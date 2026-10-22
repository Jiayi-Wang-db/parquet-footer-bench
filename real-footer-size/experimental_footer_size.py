#!/usr/bin/env python3
"""Estimate the experimental footer stored as ordinary Parquet data pages."""

import argparse
import csv
import importlib.util
import json
import math
import os
import struct
import sys
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "real-footer-size"))
import compact  # noqa: E402


BLOCK_SIZE = 1024
DATA_PAGE = 0
DICTIONARY_PAGE = 2
DATA_PAGE_V2 = 3
PLAIN_DICTIONARY = 2
RLE_DICTIONARY = 8


def load_converter():
    path = ROOT / "modular-footer" / "parquet_to_modular_addendum.py"
    spec = importlib.util.spec_from_file_location("modular_addendum_converter", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def uleb_size(value):
    size = 1
    while value >= 0x80:
        value >>= 7
        size += 1
    return size


def zigzag_size(value):
    return uleb_size((value << 1) ^ (value >> 63))


def frame_block_size(values):
    if not values:
        return 0
    base = min(values)
    residuals = [value - base for value in values]
    max_width = max(residuals, default=0).bit_length()
    best = None
    for width in range(max_width + 1):
        limit = 1 << width
        exceptions = [value for value in residuals if value >= limit]
        patch_bytes = sum(2 + uleb_size(value) for value in exceptions)
        size = (
            2
            + uleb_size(len(values))
            + zigzag_size(base)
            + 1
            + uleb_size(len(exceptions))
            + math.ceil(len(values) * width / 8)
            + patch_bytes
        )
        best = size if best is None else min(best, size)
    return best


def delta_block_size(values):
    if not values:
        return 0
    if len(values) == 1:
        return 2 + zigzag_size(values[0])
    deltas = [values[index] - values[index - 1] for index in range(1, len(values))]
    encoded_deltas = [(value << 1) ^ (value >> 63) for value in deltas]
    return 2 + zigzag_size(values[0]) + frame_block_size(encoded_deltas)


def pfor_size(values, allow_delta=True):
    total = 0
    for start in range(0, len(values), BLOCK_SIZE):
        block = values[start:start + BLOCK_SIZE]
        frame = frame_block_size(block)
        delta = delta_block_size(block) if allow_delta else frame
        total += min(frame, delta)
    return total


def dbp_size(values):
    if not values:
        return 0
    block_size = 128
    miniblock_size = 32
    miniblocks = block_size // miniblock_size
    deltas = [current - previous for previous, current in zip(values, values[1:])]
    size = (
        uleb_size(block_size)
        + uleb_size(miniblocks)
        + uleb_size(len(values))
        + zigzag_size(values[0])
    )
    for start in range(0, len(deltas), block_size):
        block = deltas[start:start + block_size]
        minimum = min(block)
        size += zigzag_size(minimum) + miniblocks
        for mini_start in range(0, block_size, miniblock_size):
            mini = block[mini_start:mini_start + miniblock_size]
            width = max((value - minimum for value in mini), default=0).bit_length()
            size += miniblock_size * width // 8
    return size


def data_pages(chunk):
    return [
        page for page in chunk.pages
        if page.page_type in (DATA_PAGE, DATA_PAGE_V2)
    ]


def extract_statistics(
    converter, chunks, row_group_num_rows, page_level, suffix_limit=16
):
    row_group_starts = []
    row_start = 0
    for num_rows in row_group_num_rows:
        row_group_starts.append(row_start)
        row_start += num_rows
    output = []
    for column in range(len(chunks[0])):
        statistics = []
        range_starts = []
        boundary_orders = []
        for group_chunks, group_start in zip(chunks, row_group_starts):
            chunk = group_chunks[column]
            chunk_pages = data_pages(chunk)
            if not page_level or chunk.column_index is None:
                stats = converter.statistic(chunk.metadata)
                if stats is None:
                    statistics = []
                    break
                range_starts.append(group_start)
                statistics.append(stats)
                boundary_orders.append(0)
            else:
                boundary_orders.append(chunk.column_index["boundary_order"])
                for page, data_page in enumerate(chunk_pages):
                    range_starts.append(data_page.first_row)
                    statistics.append(
                        converter.column_index_stat(chunk.column_index, page)
                    )
        if not statistics or not any(
            stats["null"] is not None
            or stats["nan"] is not None
            or (stats["min"] is not None and stats["max"] is not None)
            for stats in statistics
        ):
            output.append(None)
            continue
        null_counts = [stats["null"] for stats in statistics]
        nan_counts = [stats["nan"] for stats in statistics]
        if any(value is None for value in null_counts):
            null_counts = []
        if any(value is None for value in nan_counts):
            nan_counts = []
        minmax_presence = []
        prefix_lengths = []
        prefixes = bytearray()
        suffix_lengths = []
        suffixes = bytearray()
        exactness = []
        for stats in statistics:
            present = stats["min"] is not None and stats["max"] is not None
            minmax_presence.append(int(present))
            if not present:
                continue
            prefix = converter.common_prefix(stats["min"], stats["max"])
            minimum, maximum, min_exact, max_exact = converter.truncate_bounds(
                stats["min"][len(prefix):],
                stats["max"][len(prefix):],
                suffix_limit,
            )
            prefix_lengths.append(len(prefix))
            prefixes.extend(prefix)
            suffix_lengths.extend([len(minimum), len(maximum)])
            suffixes.extend(minimum)
            suffixes.extend(maximum)
            exactness.extend([
                int(stats["min_exact"] and min_exact),
                int(stats["max_exact"] and max_exact),
            ])
        output.append({
            "range_start_rows": range_starts,
            "null_counts": null_counts,
            "nan_counts": nan_counts,
            "min_max_prefix_lengths": prefix_lengths,
            "min_max_suffix_lengths": suffix_lengths,
            "min_max_presence": (
                b"" if all(minmax_presence)
                else converter.pack_bits(minmax_presence, 1)
            ),
            "min_max_prefixes": bytes(prefixes),
            "min_max_suffixes": bytes(suffixes),
            "min_max_is_exact": (
                b"" if all(exactness) else converter.pack_bits(exactness, 1)
            ),
            "boundary_order": boundary_orders[0] if len(boundary_orders) == 1 else 0,
        })
    return output


def extract_placement(converter, path):
    unused_size, footer_start, unused_footer_bytes, footer = converter.read_footer(path)
    chunks, pages = converter.scan_pages(path, footer)
    offsets = converter.initial_addendum_region_offsets(chunks, footer_start)
    region_by_offset = {offset: index for index, offset in enumerate(offsets[:-1])}
    for group_chunks in chunks:
        for chunk in group_chunks:
            chunk.region = region_by_offset[chunk.data_offset]
            for page in chunk.pages:
                if page.page_type == DICTIONARY_PAGE:
                    page.region = region_by_offset[page.offset]
    row_group_ordinals = converter.build_addendum_row_group_ordinals(
        chunks, offsets, footer_start
    )
    groups = converter.row_groups(footer)
    row_group_num_rows = [
        converter.required(group, 3, compact.I64, "RowGroup.num_rows")
        for group in groups
    ]
    num_columns = len(chunks[0])
    columns = []
    for column in range(num_columns):
        values = {
            "region_ordinals": [],
            "first_row_indexes": [],
            "codecs": [],
            "dictionary_regions": [],
            "num_values": [],
            "total_uncompressed_sizes": [],
            "page_counts": [],
            "relative_page_offsets": [],
            "page_first_row_indexes": [],
        }
        fully_dictionary_encoded = True
        for group_chunks in chunks:
            chunk = group_chunks[column]
            chunk_pages = data_pages(chunk)
            dictionary = next(
                (page for page in chunk.pages if page.page_type == DICTIONARY_PAGE),
                None,
            )
            uses_dictionary = [
                page.encoding in (PLAIN_DICTIONARY, RLE_DICTIONARY)
                for page in chunk_pages
            ]
            values["region_ordinals"].append(chunk.region)
            values["first_row_indexes"].append(chunk_pages[0].first_row)
            values["codecs"].append(chunk.codec)
            values["dictionary_regions"].append(
                0 if not any(uses_dictionary) else dictionary.region + 1
            )
            values["num_values"].append(chunk.num_values)
            values["total_uncompressed_sizes"].append(sum(
                page.total_uncompressed_size for page in chunk_pages
            ))
            values["page_counts"].append(len(chunk_pages))
            values["relative_page_offsets"].extend(
                page.offset - chunk.data_offset for page in chunk_pages[1:]
            )
            values["page_first_row_indexes"].extend(
                page.first_row for page in chunk_pages[1:]
            )
            fully_dictionary_encoded &= all(uses_dictionary)
        values["fully_dictionary_encoded"] = fully_dictionary_encoded
        columns.append(values)
    coarse_statistics = extract_statistics(
        converter, chunks, row_group_num_rows, page_level=False
    )
    full_statistics = extract_statistics(
        converter, chunks, row_group_num_rows, page_level=True
    )
    schema_blob = converter.thrift_schema_module(footer)
    file_metadata_blob = converter.thrift_file_metadata_module(footer)
    return {
        "footer_start": footer_start,
        "region_offsets": offsets,
        "row_group_region_ordinals": row_group_ordinals,
        "row_group_num_rows": row_group_num_rows,
        "columns": columns,
        "coarse_statistics": coarse_statistics,
        "full_statistics": full_statistics,
        "source_metadata": (
            struct.pack("<II", len(schema_blob), len(file_metadata_blob))
            + schema_blob
            + file_metadata_blob
        ),
    }


INTEGER_TYPES = {
    "source_column": pa.int32(),
    "i": pa.int64(),
    "values": pa.int64(),
    "core_values": pa.int64(),
    "region_offsets": pa.int64(),
    "row_group_region_ordinals": pa.int64(),
    "row_group_num_rows": pa.int64(),
    "chunk_column_starts": pa.int32(),
    "region_ordinals": pa.int64(),
    "first_row_indexes": pa.int64(),
    "codecs": pa.int32(),
    "dictionary_regions": pa.int64(),
    "num_values": pa.int64(),
    "total_uncompressed_sizes": pa.int64(),
    "page_counts": pa.int32(),
    "offset_index_column_starts": pa.int32(),
    "relative_page_offsets": pa.int64(),
    "page_first_row_indexes": pa.int64(),
    "stats_boundary_orders": pa.int32(),
    "range_start_rows": pa.int64(),
    "null_counts": pa.int64(),
    "nan_counts": pa.int64(),
    "min_max_prefix_lengths": pa.int32(),
    "min_max_suffix_lengths": pa.int32(),
    "min_max_exactness": pa.int32(),
}

CHUNK_FIELDS = (
    "region_ordinals",
    "first_row_indexes",
    "codecs",
    "dictionary_regions",
    "num_values",
    "total_uncompressed_sizes",
    "page_counts",
)

OFFSET_FIELDS = ("relative_page_offsets", "page_first_row_indexes")

STATISTICS_INTEGER_FIELDS = (
    "range_start_rows",
    "null_counts",
    "nan_counts",
    "min_max_prefix_lengths",
    "min_max_suffix_lengths",
)

STATISTICS_BINARY_FIELDS = (
    "min_max_presence",
    "min_max_prefixes",
    "min_max_suffixes",
    "min_max_is_exact",
)


def list_array(rows, value_type):
    return pa.array(rows, type=pa.list_(value_type))


def selected_statistics(metadata, statistics_kind):
    if statistics_kind is None:
        return [None] * len(metadata["columns"])
    return metadata[statistics_kind + "_statistics"]


def unpack_default_true_bits(values, count):
    if not values:
        return [1] * count
    return [int(bool(values[index // 8] & (1 << (index % 8)))) for index in range(count)]


def pack_default_true_bits(values):
    if all(values):
        return b""
    output = bytearray((len(values) + 7) // 8)
    for index, value in enumerate(values):
        if value:
            output[index // 8] |= 1 << (index % 8)
    return bytes(output)


def parquet_statistics(stats):
    if stats is None:
        return None
    num_ranges = len(stats["range_start_rows"])
    presence = unpack_default_true_bits(stats["min_max_presence"], num_ranges)
    prefix_lengths = iter(stats["min_max_prefix_lengths"])
    return {
        "boundary_order": stats["boundary_order"],
        "range_start_rows": stats["range_start_rows"],
        "null_counts": stats["null_counts"] or [None] * num_ranges,
        "nan_counts": stats["nan_counts"] or [None] * num_ranges,
        "min_max_prefix_lengths": [
            next(prefix_lengths) if present else None for present in presence
        ],
        "min_max_suffix_lengths": stats["min_max_suffix_lengths"],
        "min_max_prefixes": stats["min_max_prefixes"],
        "min_max_suffixes": stats["min_max_suffixes"],
        "min_max_is_exact": stats["min_max_is_exact"],
    }


def flat_table(metadata, include_offset_index, statistics_kind):
    columns = metadata["columns"]
    chunk_starts = [0]
    offset_starts = [0]
    flattened = {name: [] for name in CHUNK_FIELDS + OFFSET_FIELDS}
    for column in columns:
        for name in CHUNK_FIELDS:
            flattened[name].extend(column[name])
        chunk_starts.append(len(flattened["region_ordinals"]))
        for name in OFFSET_FIELDS:
            if include_offset_index:
                flattened[name].extend(column[name])
        offset_starts.append(len(flattened["relative_page_offsets"]))
    values = {
        "region_offsets": metadata["region_offsets"],
        "row_group_region_ordinals": metadata["row_group_region_ordinals"],
        "row_group_num_rows": metadata["row_group_num_rows"],
        "chunk_column_starts": chunk_starts,
        "offset_index_column_starts": offset_starts,
    }
    values.update(flattened)
    arrays = {
        name: list_array([values[name]], INTEGER_TYPES[name])
        for name in values
    }
    arrays["source_metadata"] = pa.array(
        [metadata["source_metadata"]], type=pa.binary()
    )
    arrays["fully_dictionary_encoded"] = list_array(
        [[column["fully_dictionary_encoded"] for column in columns]], pa.bool_()
    )
    if statistics_kind is not None:
        statistics = [
            parquet_statistics(stats)
            for stats in selected_statistics(metadata, statistics_kind)
        ]
        range_starts = [0]
        boundary_orders = [
            stats["boundary_order"] for stats in statistics if stats is not None
        ]
        flattened_stats = {name: [] for name in STATISTICS_INTEGER_FIELDS}
        min_max_is_exact = []
        min_max_prefixes = bytearray()
        min_max_suffixes = bytearray()
        for stats in statistics:
            if stats is None:
                range_starts.append(range_starts[-1])
                continue
            num_ranges = len(stats["range_start_rows"])
            range_starts.append(range_starts[-1] + num_ranges)
            flattened_stats["range_start_rows"].extend(stats["range_start_rows"])
            for name in ("null_counts", "nan_counts"):
                field = stats[name] if stats[name] else [None] * num_ranges
                flattened_stats[name].extend(field)
            flattened_stats["min_max_prefix_lengths"].extend(
                stats["min_max_prefix_lengths"]
            )
            flattened_stats["min_max_suffix_lengths"].extend(
                stats["min_max_suffix_lengths"]
            )
            num_present = sum(
                length is not None for length in stats["min_max_prefix_lengths"]
            )
            min_max_is_exact.extend(
                unpack_default_true_bits(stats["min_max_is_exact"], 2 * num_present)
            )
            min_max_prefixes.extend(stats["min_max_prefixes"])
            min_max_suffixes.extend(stats["min_max_suffixes"])
        arrays["stats_range_column_starts"] = list_array([range_starts], pa.int32())
        values["stats_range_column_starts"] = range_starts
        INTEGER_TYPES["stats_range_column_starts"] = pa.int32()
        arrays["stats_boundary_orders"] = list_array(
            [boundary_orders], INTEGER_TYPES["stats_boundary_orders"]
        )
        values["stats_boundary_orders"] = boundary_orders
        for name in STATISTICS_INTEGER_FIELDS:
            arrays[name] = list_array([flattened_stats[name]], INTEGER_TYPES[name])
            values[name] = [value for value in flattened_stats[name] if value is not None]
        arrays["min_max_prefixes"] = pa.array(
            [bytes(min_max_prefixes)], type=pa.binary()
        )
        arrays["min_max_suffixes"] = pa.array(
            [bytes(min_max_suffixes)], type=pa.binary()
        )
        arrays["min_max_is_exact"] = pa.array(
            [pack_default_true_bits(min_max_is_exact)], type=pa.binary()
        )
    return pa.table(arrays), values


def flat_range_values_table(metadata, include_offset_index, statistics_kind):
    table, values = flat_table(metadata, include_offset_index, None)
    if statistics_kind is None:
        return table, values, {}

    statistics = selected_statistics(metadata, statistics_kind)
    range_column_starts = [0]
    boundary_orders = []
    range_start_rows = []
    null_counts = []
    nan_counts = []
    prefixes = []
    min_suffixes = []
    max_suffixes = []
    exactness = []
    for stats in statistics:
        if stats is None:
            range_column_starts.append(range_column_starts[-1])
            continue
        boundary_orders.append(stats["boundary_order"])
        num_ranges = len(stats["range_start_rows"])
        range_column_starts.append(range_column_starts[-1] + num_ranges)
        range_start_rows.extend(stats["range_start_rows"])
        null_counts.extend(stats["null_counts"] or [None] * num_ranges)
        nan_counts.extend(stats["nan_counts"] or [None] * num_ranges)

        presence = unpack_default_true_bits(stats["min_max_presence"], num_ranges)
        num_present = sum(presence)
        exact_bits = iter(
            unpack_default_true_bits(stats["min_max_is_exact"], 2 * num_present)
        )
        prefix_lengths = iter(stats["min_max_prefix_lengths"])
        suffix_lengths = iter(stats["min_max_suffix_lengths"])
        prefix_offset = 0
        suffix_offset = 0
        for present in presence:
            if not present:
                prefixes.append(None)
                min_suffixes.append(None)
                max_suffixes.append(None)
                exactness.append(None)
                continue
            prefix_length = next(prefix_lengths)
            min_length = next(suffix_lengths)
            max_length = next(suffix_lengths)
            prefixes.append(
                stats["min_max_prefixes"][prefix_offset:prefix_offset + prefix_length]
            )
            min_suffixes.append(
                stats["min_max_suffixes"][suffix_offset:suffix_offset + min_length]
            )
            suffix_offset += min_length
            max_suffixes.append(
                stats["min_max_suffixes"][suffix_offset:suffix_offset + max_length]
            )
            prefix_offset += prefix_length
            suffix_offset += max_length
            exactness.append(next(exact_bits) | (next(exact_bits) << 1))

    integer_fields = {
        "stats_range_column_starts": (range_column_starts, pa.int32()),
        "stats_boundary_orders": (boundary_orders, pa.int32()),
        "range_start_rows": (range_start_rows, pa.int64()),
        "null_counts": (null_counts, pa.int64()),
        "nan_counts": (nan_counts, pa.int64()),
        "min_max_exactness": (exactness, pa.int32()),
    }
    for name, (field_values, value_type) in integer_fields.items():
        table = table.append_column(name, list_array([field_values], value_type))
        values[name] = [value for value in field_values if value is not None]
        INTEGER_TYPES[name] = value_type
    for name, field_values in (
        ("min_max_prefixes", prefixes),
        ("min_suffixes", min_suffixes),
        ("max_suffixes", max_suffixes),
    ):
        table = table.append_column(name, list_array([field_values], pa.binary()))
    binary_values = {
        "min_max_prefixes": [value for value in prefixes if value is not None],
        "min_suffixes": [value for value in min_suffixes if value is not None],
        "max_suffixes": [value for value in max_suffixes if value is not None],
    }
    return table, values, binary_values


def range_value_statistics(stats):
    if stats is None:
        return None
    num_ranges = len(stats["range_start_rows"])
    output = {
        "boundary_order": stats["boundary_order"],
        "range_start_rows": stats["range_start_rows"],
        "null_counts": stats["null_counts"] or [None] * num_ranges,
        "nan_counts": stats["nan_counts"] or [None] * num_ranges,
        "min_max_prefixes": [],
        "min_suffixes": [],
        "max_suffixes": [],
        "min_max_exactness": [],
    }
    presence = unpack_default_true_bits(stats["min_max_presence"], num_ranges)
    num_present = sum(presence)
    exact_bits = iter(
        unpack_default_true_bits(stats["min_max_is_exact"], 2 * num_present)
    )
    prefix_lengths = iter(stats["min_max_prefix_lengths"])
    suffix_lengths = iter(stats["min_max_suffix_lengths"])
    prefix_offset = 0
    suffix_offset = 0
    for present in presence:
        if not present:
            for name in (
                "min_max_prefixes",
                "min_suffixes",
                "max_suffixes",
                "min_max_exactness",
            ):
                output[name].append(None)
            continue
        prefix_length = next(prefix_lengths)
        min_length = next(suffix_lengths)
        max_length = next(suffix_lengths)
        output["min_max_prefixes"].append(
            stats["min_max_prefixes"][prefix_offset:prefix_offset + prefix_length]
        )
        output["min_suffixes"].append(
            stats["min_max_suffixes"][suffix_offset:suffix_offset + min_length]
        )
        suffix_offset += min_length
        output["max_suffixes"].append(
            stats["min_max_suffixes"][suffix_offset:suffix_offset + max_length]
        )
        prefix_offset += prefix_length
        suffix_offset += max_length
        output["min_max_exactness"].append(
            next(exact_bits) | (next(exact_bits) << 1)
        )
    return output


def column_range_values_table(metadata, include_offset_index, statistics_kind):
    table, values = column_rows_table(metadata, include_offset_index, None)
    if statistics_kind is None:
        return table, values, {}
    statistics = [
        range_value_statistics(stats)
        for stats in selected_statistics(metadata, statistics_kind)
    ]
    boundary_orders = [
        None if stats is None else stats["boundary_order"] for stats in statistics
    ]
    table = table.append_column(
        "stats_boundary_orders",
        pa.array([None] + boundary_orders, type=pa.int32()),
    )
    values["stats_boundary_orders"] = [
        value for value in boundary_orders if value is not None
    ]
    for name, value_type in (
        ("range_start_rows", pa.int64()),
        ("null_counts", pa.int64()),
        ("nan_counts", pa.int64()),
        ("min_max_exactness", pa.int32()),
    ):
        rows = [[]] + [([] if stats is None else stats[name]) for stats in statistics]
        table = table.append_column(name, list_array(rows, value_type))
        values[name] = [
            value for row in rows for value in row if value is not None
        ]
        INTEGER_TYPES[name] = value_type
    binary_values = {}
    for name in ("min_max_prefixes", "min_suffixes", "max_suffixes"):
        rows = [[]] + [([] if stats is None else stats[name]) for stats in statistics]
        table = table.append_column(name, list_array(rows, pa.binary()))
        binary_values[name] = [
            value for row in rows for value in row if value is not None
        ]
    return table, values, binary_values


def column_range_struct_table(metadata, include_offset_index, statistics_kind):
    table, values = column_rows_table(metadata, include_offset_index, None)
    if statistics_kind is None:
        return table, values, {}
    statistics = [
        range_value_statistics(stats)
        for stats in selected_statistics(metadata, statistics_kind)
    ]
    boundary_orders = [
        None if stats is None else stats["boundary_order"] for stats in statistics
    ]
    table = table.append_column(
        "stats_boundary_orders",
        pa.array([None] + boundary_orders, type=pa.int32()),
    )
    values["stats_boundary_orders"] = [
        value for value in boundary_orders if value is not None
    ]

    min_max_type = pa.struct([
        pa.field("prefix", pa.binary(), nullable=False),
        pa.field("min_suffix", pa.binary(), nullable=False),
        pa.field("max_suffix", pa.binary(), nullable=False),
        pa.field("exactness", pa.int32(), nullable=False),
    ])
    range_type = pa.struct([
        pa.field("start_row", pa.int64(), nullable=False),
        pa.field("null_count", pa.int64()),
        pa.field("nan_count", pa.int64()),
        pa.field("min_max", min_max_type),
    ])
    rows = [[]]
    for stats in statistics:
        if stats is None:
            rows.append([])
            continue
        ranges = []
        for index, start_row in enumerate(stats["range_start_rows"]):
            exactness = stats["min_max_exactness"][index]
            min_max = None
            if exactness is not None:
                min_max = {
                    "prefix": stats["min_max_prefixes"][index],
                    "min_suffix": stats["min_suffixes"][index],
                    "max_suffix": stats["max_suffixes"][index],
                    "exactness": exactness,
                }
            ranges.append({
                "start_row": start_row,
                "null_count": stats["null_counts"][index],
                "nan_count": stats["nan_counts"][index],
                "min_max": min_max,
            })
        rows.append(ranges)
    table = table.append_column(
        "row_range_statistics",
        pa.array(rows, type=pa.list_(range_type)),
    )
    for name, field_values in (
        (
            "range_start_rows",
            [item["start_row"] for row in rows for item in row],
        ),
        (
            "null_counts",
            [
                item["null_count"]
                for row in rows
                for item in row
                if item["null_count"] is not None
            ],
        ),
        (
            "nan_counts",
            [
                item["nan_count"]
                for row in rows
                for item in row
                if item["nan_count"] is not None
            ],
        ),
        (
            "min_max_exactness",
            [
                item["min_max"]["exactness"]
                for row in rows
                for item in row
                if item["min_max"] is not None
            ],
        ),
    ):
        values[name] = field_values
    binary_values = {
        name: [
            item["min_max"][name]
            for row in rows
            for item in row
            if item["min_max"] is not None
        ]
        for name in ("prefix", "min_suffix", "max_suffix")
    }
    return table, values, binary_values


def segmented_values(rows, batch_size):
    if batch_size is None:
        return [value for row in rows for value in row]
    return {
        "#{:04d}".format(start // batch_size): [
            value for row in rows[start:start + batch_size] for value in row
        ]
        for start in range(0, len(rows), batch_size)
    }


def add_segmented_values(output, name, rows, batch_size):
    segmented = segmented_values(rows, batch_size)
    if isinstance(segmented, list):
        output[name] = segmented
    else:
        for suffix, values in segmented.items():
            output[name + suffix] = values


def column_rows_table(metadata, include_offset_index, statistics_kind, batch_size=None):
    columns = metadata["columns"]
    arrays = {
        "source_column": pa.array([-1] + list(range(len(columns))), type=pa.int32()),
        "source_metadata": pa.array(
            [metadata["source_metadata"]] + [None for unused in columns],
            type=pa.binary(),
        ),
        "region_offsets": list_array(
            [metadata["region_offsets"]] + [[] for unused in columns], pa.int64()
        ),
        "row_group_region_ordinals": list_array(
            [metadata["row_group_region_ordinals"]] + [[] for unused in columns],
            pa.int64(),
        ),
        "row_group_num_rows": list_array(
            [metadata["row_group_num_rows"]] + [[] for unused in columns],
            pa.int64(),
        ),
    }
    values = {}
    add_segmented_values(
        values,
        "source_column",
        [[value] for value in [-1] + list(range(len(columns)))],
        batch_size,
    )
    add_segmented_values(
        values,
        "region_offsets",
        [metadata["region_offsets"]] + [[] for unused in columns],
        batch_size,
    )
    add_segmented_values(
        values,
        "row_group_region_ordinals",
        [metadata["row_group_region_ordinals"]] + [[] for unused in columns],
        batch_size,
    )
    add_segmented_values(
        values,
        "row_group_num_rows",
        [metadata["row_group_num_rows"]] + [[] for unused in columns],
        batch_size,
    )
    for name in CHUNK_FIELDS + OFFSET_FIELDS:
        rows = [[]]
        for column in columns:
            rows.append(column[name] if include_offset_index or name not in OFFSET_FIELDS else [])
        arrays[name] = list_array(rows, INTEGER_TYPES[name])
        add_segmented_values(values, name, rows, batch_size)
    arrays["fully_dictionary_encoded"] = pa.array(
        [None] + [column["fully_dictionary_encoded"] for column in columns],
        type=pa.bool_(),
    )
    if statistics_kind is not None:
        statistics = [
            parquet_statistics(stats)
            for stats in selected_statistics(metadata, statistics_kind)
        ]
        boundary_rows = [[]] + [
            [] if stats is None else [stats["boundary_order"]] for stats in statistics
        ]
        arrays["stats_boundary_orders"] = pa.array(
            [None] + [None if not row else row[0] for row in boundary_rows[1:]],
            type=pa.int32(),
        )
        add_segmented_values(
            values, "stats_boundary_orders", boundary_rows, batch_size
        )
        for name in STATISTICS_INTEGER_FIELDS:
            rows = [[]] + [
                [] if stats is None else stats[name] for stats in statistics
            ]
            arrays[name] = list_array(rows, INTEGER_TYPES[name])
            encoded_rows = [[value for value in row if value is not None] for row in rows]
            add_segmented_values(values, name, encoded_rows, batch_size)
        for name in (
            "min_max_prefixes",
            "min_max_suffixes",
            "min_max_is_exact",
        ):
            arrays[name] = pa.array(
                [None] + [None if stats is None else stats[name] for stats in statistics],
                type=pa.binary(),
            )
    return pa.table(arrays), values


def wide_table(metadata, include_offset_index, statistics_kind):
    global_type = pa.struct([
        pa.field("region_offsets", pa.list_(pa.int64())),
        pa.field("row_group_region_ordinals", pa.list_(pa.int64())),
        pa.field("row_group_num_rows", pa.list_(pa.int64())),
        pa.field("source_metadata", pa.binary()),
    ])
    arrays = {
        "global": pa.array([{
            "region_offsets": metadata["region_offsets"],
            "row_group_region_ordinals": metadata["row_group_region_ordinals"],
            "row_group_num_rows": metadata["row_group_num_rows"],
            "source_metadata": metadata["source_metadata"],
        }], type=global_type),
    }
    values = {
        "global.region_offsets": metadata["region_offsets"],
        "global.row_group_region_ordinals": metadata["row_group_region_ordinals"],
        "global.row_group_num_rows": metadata["row_group_num_rows"],
    }
    fields = [
        pa.field(name, pa.list_(INTEGER_TYPES[name]))
        for name in CHUNK_FIELDS + OFFSET_FIELDS
    ] + [
        pa.field("fully_dictionary_encoded", pa.bool_()),
    ]
    if statistics_kind is not None:
        fields.extend([
            pa.field("stats_present", pa.bool_()),
            pa.field("stats_boundary_orders", pa.int32()),
        ])
        fields.extend(
            pa.field(name, pa.list_(INTEGER_TYPES[name]))
            for name in STATISTICS_INTEGER_FIELDS
        )
        fields.extend(
            pa.field(name, pa.binary()) for name in STATISTICS_BINARY_FIELDS
        )
    column_type = pa.struct(fields)
    statistics = selected_statistics(metadata, statistics_kind)
    for ordinal, (column, stats) in enumerate(zip(metadata["columns"], statistics)):
        item = {
            name: column[name] if include_offset_index or name not in OFFSET_FIELDS else []
            for name in CHUNK_FIELDS + OFFSET_FIELDS
        }
        item["fully_dictionary_encoded"] = column["fully_dictionary_encoded"]
        if statistics_kind is not None:
            item["stats_present"] = stats is not None
            item["stats_boundary_orders"] = (
                None if stats is None else stats["boundary_order"]
            )
            for name in STATISTICS_INTEGER_FIELDS:
                item[name] = [] if stats is None else stats[name]
            for name in STATISTICS_BINARY_FIELDS:
                item[name] = b"" if stats is None else stats[name]
        field_name = "column_{:04d}".format(ordinal)
        arrays[field_name] = pa.array([item], type=column_type)
        for name in CHUNK_FIELDS + OFFSET_FIELDS:
            values["{}.{}".format(field_name, name)] = item[name]
        if statistics_kind is not None:
            if stats is not None:
                values["{}.stats_boundary_orders".format(field_name)] = [
                    stats["boundary_order"]
                ]
            for name in STATISTICS_INTEGER_FIELDS:
                values["{}.{}".format(field_name, name)] = item[name]
    return pa.table(arrays), values


def column_pages_table(metadata, include_offset_index, statistics_kind):
    global_values = [
        len(metadata["region_offsets"]),
        len(metadata["row_group_region_ordinals"]),
        len(metadata["row_group_num_rows"]),
    ]
    global_values.extend(metadata["region_offsets"])
    global_values.extend(metadata["row_group_region_ordinals"])
    global_values.extend(metadata["row_group_num_rows"])
    pages = [global_values]
    byte_pages = [metadata["source_metadata"]]
    statistics = selected_statistics(metadata, statistics_kind)
    for column, stats in zip(metadata["columns"], statistics):
        byte_values = b""
        values = [len(column["region_ordinals"])]
        for name in CHUNK_FIELDS:
            values.extend(column[name])
        values.append(int(column["fully_dictionary_encoded"]))
        offset_count = len(column["relative_page_offsets"]) if include_offset_index else 0
        values.append(offset_count)
        if include_offset_index:
            for name in OFFSET_FIELDS:
                values.extend(column[name])
        if statistics_kind is not None:
            values.append(int(stats is not None))
            if stats is not None:
                values.append(stats["boundary_order"])
                for name in STATISTICS_INTEGER_FIELDS:
                    values.append(len(stats[name]))
                    values.extend(stats[name])
                stats_bytes = bytearray()
                for name in STATISTICS_BINARY_FIELDS:
                    values.append(len(stats[name]))
                    stats_bytes.extend(stats[name])
                byte_values = bytes(stats_bytes)
        byte_pages.append(byte_values)
        pages.append(values)
    integer_values = {
        "values#{:04d}".format(index): values
        for index, values in enumerate(pages)
    }
    arrays = {"values": list_array(pages, pa.int64())}
    arrays["byte_values"] = pa.array(byte_pages, type=pa.binary())
    return pa.table(arrays), integer_values


def single_stream_table(metadata, include_offset_index, statistics_kind):
    columns = metadata["columns"]
    values = [
        len(columns),
        len(metadata["source_metadata"]),
        len(metadata["region_offsets"]),
        len(metadata["row_group_region_ordinals"]),
        len(metadata["row_group_num_rows"]),
    ]
    values.extend(metadata["region_offsets"])
    values.extend(metadata["row_group_region_ordinals"])
    values.extend(metadata["row_group_num_rows"])

    chunk_starts = [0]
    for column in columns:
        chunk_starts.append(chunk_starts[-1] + len(column["region_ordinals"]))
    values.extend(chunk_starts)
    for name in CHUNK_FIELDS:
        for column in columns:
            values.extend(column[name])
    values.extend(int(column["fully_dictionary_encoded"]) for column in columns)

    values.append(int(include_offset_index))
    if include_offset_index:
        offset_starts = [0]
        for column in columns:
            offset_starts.append(
                offset_starts[-1] + len(column["relative_page_offsets"])
            )
        values.extend(offset_starts)
        for name in OFFSET_FIELDS:
            for column in columns:
                values.extend(column[name])

    binary_values = bytearray(metadata["source_metadata"])
    values.append(int(statistics_kind is not None))
    if statistics_kind is not None:
        statistics = selected_statistics(metadata, statistics_kind)
        values.extend(int(stats is not None) for stats in statistics)
        values.extend(
            stats["boundary_order"] for stats in statistics if stats is not None
        )
        for name in STATISTICS_INTEGER_FIELDS:
            starts = [0]
            flattened_values = []
            for stats in statistics:
                if stats is not None:
                    flattened_values.extend(stats[name])
                starts.append(len(flattened_values))
            values.extend(starts)
            values.extend(flattened_values)
        for name in STATISTICS_BINARY_FIELDS:
            starts = [0]
            blob = bytearray()
            for stats in statistics:
                if stats is not None:
                    blob.extend(stats[name])
                starts.append(len(blob))
            values.extend(starts)
            binary_values.extend(blob)

    arrays = {
        "i": list_array([values], pa.int64()),
        "b": pa.array([bytes(binary_values)], type=pa.binary()),
    }
    return pa.table(arrays), {"i": values}


def partitioned_stream_table(
    metadata, include_offset_index, statistics_kind, split_offset, split_statistics
):
    columns = metadata["columns"]
    core_values = [
        len(columns),
        len(metadata["source_metadata"]),
        len(metadata["region_offsets"]),
        len(metadata["row_group_region_ordinals"]),
        len(metadata["row_group_num_rows"]),
    ]
    core_values.extend(metadata["region_offsets"])
    core_values.extend(metadata["row_group_region_ordinals"])
    core_values.extend(metadata["row_group_num_rows"])
    chunk_starts = [0]
    for column in columns:
        chunk_starts.append(chunk_starts[-1] + len(column["region_ordinals"]))
    core_values.extend(chunk_starts)
    for name in CHUNK_FIELDS:
        for column in columns:
            core_values.extend(column[name])
    core_values.extend(int(column["fully_dictionary_encoded"]) for column in columns)

    arrays = {}
    integer_values = {"i": core_values}
    core_values.append(int(include_offset_index))
    if include_offset_index:
        offset_starts = [0]
        for column in columns:
            offset_starts.append(
                offset_starts[-1] + len(column["relative_page_offsets"])
            )
        core_values.extend(offset_starts)
        for index, name in enumerate(OFFSET_FIELDS):
            flattened_values = [
                value for column in columns for value in column[name]
            ]
            if split_offset:
                stream_name = "p{}".format(index)
                arrays[stream_name] = list_array(
                    [flattened_values], INTEGER_TYPES[name]
                )
                integer_values[stream_name] = flattened_values
                INTEGER_TYPES[stream_name] = INTEGER_TYPES[name]
            else:
                core_values.extend(flattened_values)

    binary_values = bytearray(metadata["source_metadata"])
    core_values.append(int(statistics_kind is not None))
    if statistics_kind is not None:
        statistics = selected_statistics(metadata, statistics_kind)
        core_values.extend(int(stats is not None) for stats in statistics)
        core_values.extend(
            stats["boundary_order"] for stats in statistics if stats is not None
        )
        for index, name in enumerate(STATISTICS_INTEGER_FIELDS):
            starts = [0]
            flattened_values = []
            for stats in statistics:
                if stats is not None:
                    flattened_values.extend(stats[name])
                starts.append(len(flattened_values))
            stream_values = starts + flattened_values
            if split_statistics:
                stream_name = "s{}".format(index)
                arrays[stream_name] = list_array(
                    [stream_values], INTEGER_TYPES[name]
                )
                integer_values[stream_name] = stream_values
                INTEGER_TYPES[stream_name] = INTEGER_TYPES[name]
            else:
                core_values.extend(stream_values)
        for name in STATISTICS_BINARY_FIELDS:
            starts = [0]
            blob = bytearray()
            for stats in statistics:
                if stats is not None:
                    blob.extend(stats[name])
                starts.append(len(blob))
            core_values.extend(starts)
            binary_values.extend(blob)
    arrays["i"] = list_array([core_values], pa.int64())
    arrays["b"] = pa.array([bytes(binary_values)], type=pa.binary())
    return pa.table(arrays), integer_values


def offset_split_stream_table(metadata, include_offset_index, statistics_kind):
    return partitioned_stream_table(
        metadata,
        include_offset_index,
        statistics_kind,
        split_offset=True,
        split_statistics=False,
    )


def statistics_split_stream_table(metadata, include_offset_index, statistics_kind):
    return partitioned_stream_table(
        metadata,
        include_offset_index,
        statistics_kind,
        split_offset=False,
        split_statistics=True,
    )


def hybrid_stream_table(metadata, include_offset_index, statistics_kind):
    return partitioned_stream_table(
        metadata,
        include_offset_index,
        statistics_kind,
        split_offset=True,
        split_statistics=True,
    )


def write_parquet(table, row_group_size=None):
    handle, name = tempfile.mkstemp(suffix=".metadata.parquet")
    os.close(handle)
    path = Path(name)
    try:
        pq.write_table(
            table,
            path,
            row_group_size=row_group_size,
            compression="NONE",
            use_dictionary=False,
            write_statistics=False,
            version="2.0",
            data_page_version="1.0",
        )
        size = path.stat().st_size
        with path.open("rb") as stream:
            stream.seek(-8, os.SEEK_END)
            trailer = stream.read(8)
        if trailer[4:] != b"PAR1":
            raise ValueError("generated metadata file has no Parquet trailer")
        footer_size = struct.unpack("<I", trailer[:4])[0]
        parquet_metadata = pq.ParquetFile(path).metadata
        leaf_chunks = sum(
            parquet_metadata.row_group(group).num_columns
            for group in range(parquet_metadata.num_row_groups)
        )
        with path.open("rb") as stream:
            stream.seek(-8 - footer_size, os.SEEK_END)
            footer = compact.decode(stream.read(footer_size))
        key_values = next((item for item in footer if item[0] == 5), None)
        if key_values is not None:
            retained = []
            for entry in key_values[2][1]:
                key = next(item[2] for item in entry if item[0] == 1)
                if key != b"ARROW:schema":
                    retained.append(entry)
            if retained:
                key_values[2] = (key_values[2][0], retained)
            else:
                footer.remove(key_values)
        adjusted_footer = compact.encode(footer)
        return size - footer_size + len(adjusted_footer), len(adjusted_footer), leaf_chunks
    finally:
        if path.exists():
            path.unlink()


def primitive_width(value_type):
    return value_type.bit_width // 8


def estimate_layout(
    table, integer_values, layout, footer_start, row_group_size=None, binary_values=None
):
    parquet_size, footer_size, leaf_chunks = write_parquet(table, row_group_size)
    plain_bytes = 0
    encoded_bytes = 0
    frame_bytes = 0
    integer_count = 0
    pfor_blocks = 0
    block_directory_bytes = 0
    for name, values in integer_values.items():
        leaf_name = name.split(".")[-1].split("#")[0]
        value_type = INTEGER_TYPES[leaf_name]
        plain = len(values) * primitive_width(value_type)
        frame = pfor_size(values, allow_delta=False)
        auto = pfor_size(values, allow_delta=True)
        plain_bytes += plain
        frame_bytes += min(plain, frame)
        encoded_bytes += min(plain, auto)
        integer_count += len(values)
        blocks = math.ceil(len(values) / BLOCK_SIZE)
        pfor_blocks += blocks
        block_directory_bytes += max(0, blocks - 1) * 4
    binary_values = binary_values or {}
    plain_binary_bytes = sum(
        4 + len(value) for field_values in binary_values.values() for value in field_values
    )
    encoded_binary_bytes = sum(
        dbp_size([len(value) for value in field_values])
        + sum(len(value) for value in field_values)
        for field_values in binary_values.values()
    )
    framing = parquet_size - plain_bytes - plain_binary_bytes
    proposed_embedded = framing + encoded_bytes + encoded_binary_bytes - 8
    root = compact.encode([
        [1, compact.I32, 1],
        [2, compact.I32, layout],
        [3, compact.I64, footer_start],
        [4, compact.I64, proposed_embedded],
    ])
    outer_trailer = 12
    return {
        "actual_plain_bytes": parquet_size,
        "thrift_footer_bytes": footer_size,
        "leaf_chunks": leaf_chunks,
        "plain_integer_bytes": plain_bytes,
        "pfor_frame_integer_bytes": frame_bytes,
        "pfor_auto_integer_bytes": encoded_bytes,
        "integer_values": integer_count,
        "decoded_integer_bytes": plain_bytes,
        "pfor_blocks": pfor_blocks,
        "block_directory_bytes": block_directory_bytes,
        "decode_all_at_20_gbps_us": plain_bytes / 20000,
        "embedded_bytes": proposed_embedded,
        "root_bytes": len(root),
        "estimated_bytes": proposed_embedded + len(root) + outer_trailer,
    }


def offset_module_table(metadata):
    columns = metadata["columns"]
    starts = [0]
    for column in columns:
        starts.append(starts[-1] + len(column["relative_page_offsets"]))
    values = [len(columns)] + starts
    for name in OFFSET_FIELDS:
        for column in columns:
            values.extend(column[name])
    return pa.table({"i": list_array([values], pa.int64())}), {"i": values}


def offset_module_split_table(metadata):
    columns = metadata["columns"]
    starts = [0]
    for column in columns:
        starts.append(starts[-1] + len(column["relative_page_offsets"]))
    arrays = {"i": list_array([[len(columns)] + starts], pa.int64())}
    integer_values = {"i": [len(columns)] + starts}
    for index, name in enumerate(OFFSET_FIELDS):
        stream_name = "p{}".format(index)
        values = [value for column in columns for value in column[name]]
        arrays[stream_name] = list_array([values], INTEGER_TYPES[name])
        integer_values[stream_name] = values
        INTEGER_TYPES[stream_name] = INTEGER_TYPES[name]
    return pa.table(arrays), integer_values


def core_module_split_table(metadata):
    columns = metadata["columns"]
    values = [
        len(columns),
        len(metadata["source_metadata"]),
        len(metadata["region_offsets"]),
        len(metadata["row_group_region_ordinals"]),
        len(metadata["row_group_num_rows"]),
    ]
    values.extend(metadata["region_offsets"])
    values.extend(metadata["row_group_region_ordinals"])
    values.extend(metadata["row_group_num_rows"])
    chunk_starts = [0]
    for column in columns:
        chunk_starts.append(chunk_starts[-1] + len(column["region_ordinals"]))
    values.extend(chunk_starts)
    arrays = {
        "i": list_array([values], pa.int64()),
        "b": pa.array([metadata["source_metadata"]], type=pa.binary()),
    }
    integer_values = {"i": values}
    for index, name in enumerate(CHUNK_FIELDS):
        stream_name = "c{}".format(index)
        stream_values = [value for column in columns for value in column[name]]
        arrays[stream_name] = list_array([stream_values], INTEGER_TYPES[name])
        integer_values[stream_name] = stream_values
        INTEGER_TYPES[stream_name] = INTEGER_TYPES[name]
    dictionary_values = [int(column["fully_dictionary_encoded"]) for column in columns]
    arrays["c7"] = list_array([dictionary_values], pa.int32())
    integer_values["c7"] = dictionary_values
    INTEGER_TYPES["c7"] = pa.int32()
    return pa.table(arrays), integer_values


def statistics_module_table(metadata, statistics_kind):
    statistics = selected_statistics(metadata, statistics_kind)
    values = [len(statistics)]
    values.extend(int(stats is not None) for stats in statistics)
    values.extend(
        stats["boundary_order"] for stats in statistics if stats is not None
    )
    for name in STATISTICS_INTEGER_FIELDS:
        starts = [0]
        flattened_values = []
        for stats in statistics:
            if stats is not None:
                flattened_values.extend(stats[name])
            starts.append(len(flattened_values))
        values.extend(starts)
        values.extend(flattened_values)
    binary_values = bytearray()
    for name in STATISTICS_BINARY_FIELDS:
        starts = [0]
        blob = bytearray()
        for stats in statistics:
            if stats is not None:
                blob.extend(stats[name])
            starts.append(len(blob))
        values.extend(starts)
        binary_values.extend(blob)
    return pa.table({
        "i": list_array([values], pa.int64()),
        "b": pa.array([bytes(binary_values)], type=pa.binary()),
    }), {"i": values}


def statistics_module_split_table(metadata, statistics_kind):
    statistics = selected_statistics(metadata, statistics_kind)
    values = [len(statistics)]
    values.extend(int(stats is not None) for stats in statistics)
    values.extend(
        stats["boundary_order"] for stats in statistics if stats is not None
    )
    arrays = {}
    integer_values = {"i": values}
    for index, name in enumerate(STATISTICS_INTEGER_FIELDS):
        starts = [0]
        flattened_values = []
        for stats in statistics:
            if stats is not None:
                flattened_values.extend(stats[name])
            starts.append(len(flattened_values))
        stream_name = "s{}".format(index)
        stream_values = starts + flattened_values
        arrays[stream_name] = list_array([stream_values], INTEGER_TYPES[name])
        integer_values[stream_name] = stream_values
        INTEGER_TYPES[stream_name] = INTEGER_TYPES[name]
    binary_values = bytearray()
    for name in STATISTICS_BINARY_FIELDS:
        starts = [0]
        blob = bytearray()
        for stats in statistics:
            if stats is not None:
                blob.extend(stats[name])
            starts.append(len(blob))
        values.extend(starts)
        binary_values.extend(blob)
    arrays["i"] = list_array([values], pa.int64())
    arrays["b"] = pa.array([bytes(binary_values)], type=pa.binary())
    return pa.table(arrays), integer_values


def smaller_module(metadata, candidates):
    results = []
    for table, integer_values in candidates:
        results.append(estimate_layout(
            table, integer_values, 4, metadata["footer_start"]
        ))
    return min(results, key=lambda result: result["embedded_bytes"])


def estimate_module_stream(metadata, tier):
    components = []
    core_table, core_values = single_stream_table(metadata, False, None)
    split_core_table, split_core_values = core_module_split_table(metadata)
    components.append((0, smaller_module(metadata, [
        (core_table, core_values),
        (split_core_table, split_core_values),
    ])))
    if tier in ("placement", "full"):
        offset_table, offset_values = offset_module_table(metadata)
        split_offset_table, split_offset_values = offset_module_split_table(metadata)
        components.append((1, smaller_module(metadata, [
            (offset_table, offset_values),
            (split_offset_table, split_offset_values),
        ])))
    if tier in ("core_stats", "full"):
        statistics_kind = "coarse" if tier == "core_stats" else "full"
        stats_table, stats_values = statistics_module_table(
            metadata, statistics_kind
        )
        split_stats_table, split_stats_values = statistics_module_split_table(
            metadata, statistics_kind
        )
        components.append((2, smaller_module(metadata, [
            (stats_table, stats_values),
            (split_stats_table, split_stats_values),
        ])))

    cursor = metadata["footer_start"]
    entries = []
    for kind, result in components:
        entries.append([
            [1, compact.I32, kind],
            [2, compact.I64, cursor],
            [3, compact.I64, result["embedded_bytes"]],
        ])
        cursor += result["embedded_bytes"]
    root = compact.encode([
        [1, compact.I32, 1],
        [2, compact.I32, 4],
        [3, compact.LIST, (compact.STRUCT, entries)],
    ])
    output = {
        "name": "module_stream",
        "estimated_bytes": (
            sum(result["embedded_bytes"] for unused, result in components)
            + len(root)
            + 12
        ),
        "actual_plain_bytes": sum(
            result["actual_plain_bytes"] for unused, result in components
        ),
        "thrift_footer_bytes": sum(
            result["thrift_footer_bytes"] for unused, result in components
        ),
        "leaf_chunks": sum(
            result["leaf_chunks"] for unused, result in components
        ),
        "integer_values": sum(
            result["integer_values"] for unused, result in components
        ),
        "plain_integer_bytes": sum(
            result["plain_integer_bytes"] for unused, result in components
        ),
        "pfor_frame_integer_bytes": sum(
            result["pfor_frame_integer_bytes"] for unused, result in components
        ),
        "pfor_auto_integer_bytes": sum(
            result["pfor_auto_integer_bytes"] for unused, result in components
        ),
        "decoded_integer_bytes": sum(
            result["decoded_integer_bytes"] for unused, result in components
        ),
        "pfor_blocks": sum(
            result["pfor_blocks"] for unused, result in components
        ),
        "block_directory_bytes": sum(
            result["block_directory_bytes"] for unused, result in components
        ),
        "core_module_bytes": next(
            result["embedded_bytes"] for kind, result in components if kind == 0
        ),
        "offset_index_module_bytes": next(
            (result["embedded_bytes"] for kind, result in components if kind == 1),
            0,
        ),
        "statistics_module_bytes": next(
            (result["embedded_bytes"] for kind, result in components if kind == 2),
            0,
        ),
        "module_root_bytes": len(root) + 12,
    }
    output["decode_all_at_20_gbps_us"] = output["decoded_integer_bytes"] / 20000
    return output


def benchmark(path, converter, include_offset_index, statistics_kind):
    metadata = extract_placement(converter, path)
    layouts = []
    for name, layout, builder, row_group_size in (
        ("wide", 0, wide_table, None),
        ("flat", 1, flat_table, None),
        ("flat_range_values", 1, flat_range_values_table, None),
        ("column_rows", 2, column_rows_table, None),
        ("column_range_values", 2, column_range_values_table, None),
        ("column_range_struct", 2, column_range_struct_table, None),
        ("column_rows_8", 2, column_rows_table, 8),
        ("column_rows_16", 2, column_rows_table, 16),
        ("column_rows_32", 2, column_rows_table, 32),
        ("column_pages", 3, column_pages_table, 1),
        ("single_stream", 4, single_stream_table, None),
        ("hybrid_stream", 4, hybrid_stream_table, None),
        ("offset_split_stream", 4, offset_split_stream_table, None),
        ("statistics_split_stream", 4, statistics_split_stream_table, None),
    ):
        if builder == column_rows_table:
            result = builder(
                metadata, include_offset_index, statistics_kind, row_group_size
            )
        else:
            result = builder(
                metadata, include_offset_index, statistics_kind
            )
        table, integer_values = result[:2]
        binary_values = result[2] if len(result) == 3 else None
        result = estimate_layout(
            table,
            integer_values,
            layout,
            metadata["footer_start"],
            row_group_size=row_group_size,
            binary_values=binary_values,
        )
        result["name"] = name
        layouts.append(result)
    tier = (
        "full" if include_offset_index and statistics_kind == "full"
        else "placement" if include_offset_index
        else "core_stats" if statistics_kind == "coarse"
        else "core"
    )
    layouts.append(estimate_module_stream(metadata, tier))
    return layouts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("names", nargs="*")
    parser.add_argument(
        "--manifest", type=Path, default=ROOT / "real-footer-size" / "corpus.json"
    )
    parser.add_argument(
        "--data",
        type=Path,
        default=ROOT / "real-footer-size" / "data" / "raincloud-parquet-java",
    )
    args = parser.parse_args()
    entries = json.loads(args.manifest.read_text())["datasets"]
    by_name = {entry["name"]: entry for entry in entries}
    unknown = sorted(set(args.names) - set(by_name))
    if unknown:
        parser.error("unknown dataset(s): {}".format(", ".join(unknown)))
    selected = [by_name[name] for name in args.names] if args.names else entries
    converter = load_converter()

    writer = csv.writer(sys.stdout, lineterminator="\n")
    writer.writerow([
        "name",
        "tier",
        "experimental_layout",
        "estimated_bytes",
        "actual_plain_bytes",
        "thrift_footer_bytes",
        "leaf_chunks",
        "integer_values",
        "plain_integer_bytes",
        "pfor_frame_integer_bytes",
        "pfor_auto_integer_bytes",
        "decoded_integer_bytes",
        "pfor_blocks",
        "block_directory_bytes",
        "decode_all_at_20_gbps_us",
        "core_module_bytes",
        "offset_index_module_bytes",
        "statistics_module_bytes",
        "module_root_bytes",
    ])
    for entry in selected:
        path = args.data / (entry["name"] + ".parquet")
        for tier, include_offset_index, statistics_kind in (
            ("core", False, None),
            ("core_stats", False, "coarse"),
            ("placement", True, None),
            ("full", True, "full"),
        ):
            for result in benchmark(
                path, converter, include_offset_index, statistics_kind
            ):
                writer.writerow([
                    entry["name"],
                    tier,
                    result["name"],
                    result["estimated_bytes"],
                    result["actual_plain_bytes"],
                    result["thrift_footer_bytes"],
                    result["leaf_chunks"],
                    result["integer_values"],
                    result["plain_integer_bytes"],
                    result["pfor_frame_integer_bytes"],
                    result["pfor_auto_integer_bytes"],
                    result["decoded_integer_bytes"],
                    result["pfor_blocks"],
                    result["block_directory_bytes"],
                    "{:.4f}".format(result["decode_all_at_20_gbps_us"]),
                    result.get("core_module_bytes", ""),
                    result.get("offset_index_module_bytes", ""),
                    result.get("statistics_module_bytes", ""),
                    result.get("module_root_bytes", ""),
                ])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
