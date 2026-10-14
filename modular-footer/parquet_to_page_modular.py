#!/usr/bin/env python3
"""Convert a PAR1 file to the PAR1-addendum or PAR3-full page-first footer."""

import argparse
import bisect
import copy
import gzip
import os
import struct
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "real-footer-size"))
import compact  # noqa: E402


PAGE_ARRAY_BITPACKED = 0
PAGE_ARRAY_DELTA_BINARY_PACKED = 1

K_SCHEMA = 0
K_PAGE_DIRECTORY = 1
K_ROW_RANGE_STATISTICS = 2
K_FILE_METADATA = 3

ADD_K_SCHEMA = 0
ADD_K_PAGE_DIRECTORY = 1
ADD_K_ROW_GROUP_STATISTICS = 2
ADD_K_FLAT_COLUMN_INDEX = 3
ADD_K_FILE_METADATA = 4

DATA_PAGE = 0
DICTIONARY_PAGE = 2
DATA_PAGE_V2 = 3
PLAIN_DICTIONARY = 2
RLE_DICTIONARY = 8


def field(fields, field_id):
    for item in fields:
        if item[0] == field_id:
            return item
    return None


def required(fields, field_id, value_type, name):
    item = field(fields, field_id)
    if item is None or item[1] != value_type:
        raise ValueError("{} is missing".format(name))
    return item[2]


def optional_int(fields, field_id, default=None):
    item = field(fields, field_id)
    return default if item is None else item[2]


def uleb(value):
    output = bytearray()
    while value >= 0x80:
        output.append((value & 0x7f) | 0x80)
        value >>= 7
    output.append(value)
    return bytes(output)


def zigzag(value):
    return uleb((value << 1) ^ (value >> 63))


def pack_bits(values, width):
    output = bytearray((len(values) * width + 7) // 8)
    bit_offset = 0
    for value in values:
        for bit in range(width):
            if value & (1 << bit):
                absolute = bit_offset + bit
                output[absolute // 8] |= 1 << (absolute % 8)
        bit_offset += width
    return bytes(output)


def bit_width(values):
    return max(values, default=0).bit_length()


def dense_values(values):
    width = bit_width(values)
    return bytes([width]) + pack_bits(values, width)


def presence_bitmap(domain, positions):
    values = bytearray((domain + 7) // 8)
    for position in positions:
        values[position // 8] |= 1 << (position % 8)
    return bytes(values)


def delta_values(values):
    constant = not values or min(values) == max(values)
    delta_count = max(1, len(values) - 1)
    block_size = ((delta_count + 127) // 128) * 128 if constant else 128
    mini_block_count = 1 if constant else 4
    mini_block_size = block_size // mini_block_count
    deltas = [values[index] - values[index - 1] for index in range(1, len(values))]
    output = bytearray()
    output.extend(uleb(block_size))
    output.extend(uleb(mini_block_count))
    output.extend(uleb(len(values)))
    output.extend(zigzag(values[0] if values else 0))
    for block_start in range(0, len(deltas), block_size):
        block = deltas[block_start:block_start + block_size]
        minimum = min(block)
        output.extend(zigzag(minimum))
        widths = []
        mini_blocks = []
        for mini_start in range(0, block_size, mini_block_size):
            mini_block = block[mini_start:mini_start + mini_block_size]
            residuals = [value - minimum for value in mini_block]
            width = bit_width(residuals)
            widths.append(width)
            mini_blocks.append(residuals + [0] * (mini_block_size - len(residuals)))
        output.extend(bytes(widths))
        for residuals, width in zip(mini_blocks, widths):
            output.extend(pack_bits(residuals, width))
    return bytes(output)


def adaptive_values(values):
    bitpacked = bytes([PAGE_ARRAY_BITPACKED]) + uleb(len(values)) + dense_values(values)
    delta = bytes([PAGE_ARRAY_DELTA_BINARY_PACKED]) + delta_values(values)
    return min((bitpacked, delta), key=len)


def adaptive_array(values):
    return [[1, compact.BINARY, adaptive_values(values)]]


def encoded_field(field_id, array):
    return [field_id, compact.STRUCT, array]


def modular_dense_int_array(values):
    return [
        [1, compact.I32, len(values)],
        [2, compact.BINARY, dense_values(values)],
    ]


def modular_sparse_int_array(domain, positions, values):
    position_width = bit_width(positions)
    presence = uleb(len(positions)) + bytes([position_width]) + pack_bits(
        positions, position_width
    )
    return [
        [1, compact.I32, domain],
        [2, compact.BINARY, dense_values(values)],
        [3, compact.I32, 1],
        [4, compact.BINARY, presence],
    ]


def modular_sparse_bytes_array(domain, positions, values):
    position_width = bit_width(positions)
    presence = uleb(len(positions)) + bytes([position_width]) + pack_bits(
        positions, position_width
    )
    offsets = [0]
    data = bytearray()
    for value in values:
        data.extend(value)
        offsets.append(len(data))
    encoded_values = dense_values(offsets) + bytes(data)
    return [
        [1, compact.I32, domain],
        [2, compact.BINARY, encoded_values],
        [3, compact.I32, 1],
        [4, compact.BINARY, presence],
    ]


def modular_int_array(domain, positions, values):
    if positions == list(range(domain)):
        return modular_dense_int_array(values)
    return modular_sparse_int_array(domain, positions, values)


def modular_dense_bytes_array(values):
    offsets = [0]
    data = bytearray()
    for value in values:
        data.extend(value)
        offsets.append(len(data))
    return [
        [1, compact.I32, len(values)],
        [2, compact.BINARY, dense_values(offsets) + bytes(data)],
    ]


def modular_bytes_array(domain, positions, values):
    if positions == list(range(domain)):
        return modular_dense_bytes_array(values)
    return modular_sparse_bytes_array(domain, positions, values)


def read_footer(path):
    file_size = path.stat().st_size
    with path.open("rb") as stream:
        if stream.read(4) != b"PAR1":
            raise ValueError("missing leading PAR1 magic")
        stream.seek(-8, os.SEEK_END)
        trailer = stream.read(8)
        if trailer[4:] != b"PAR1":
            raise ValueError("missing trailing PAR1 magic")
        footer_size = struct.unpack("<I", trailer[:4])[0]
        footer_start = file_size - 8 - footer_size
        stream.seek(footer_start)
        footer_bytes = stream.read(footer_size)
    footer = compact.decode(footer_bytes)
    if compact.encode(footer) != footer_bytes:
        raise ValueError("compact-Thrift footer fidelity check failed")
    return file_size, footer_start, footer_bytes, footer


def row_groups(footer):
    value = required(footer, 4, compact.LIST, "FileMetaData.row_groups")
    if value[0] != compact.STRUCT:
        raise ValueError("FileMetaData.row_groups has the wrong element type")
    return value[1]


def column_chunks(row_group):
    value = required(row_group, 1, compact.LIST, "RowGroup.columns")
    if value[0] != compact.STRUCT:
        raise ValueError("RowGroup.columns has the wrong element type")
    return value[1]


def column_metadata(chunk):
    return required(chunk, 3, compact.STRUCT, "ColumnChunk.meta_data")


def schema_leaf_max_repetition_levels(footer):
    schema = required(footer, 2, compact.LIST, "FileMetaData.schema")[1]
    leaves = []

    def visit(index, inherited_level):
        element = schema[index]
        repetition_type = optional_int(element, 3)
        level = inherited_level + int(repetition_type == 2)
        num_children = optional_int(element, 5)
        next_index = index + 1
        if num_children is None:
            leaves.append(level)
            return next_index
        for unused in range(num_children):
            next_index = visit(next_index, level)
        return next_index

    if visit(0, 0) != len(schema):
        raise ValueError("schema traversal did not consume every element")
    return leaves


class Page:
    def __init__(
        self, column, row_group, offset, size, page_type, encoding, first_row,
        first_row_in_group, codec, num_values, total_uncompressed_size
    ):
        self.column = column
        self.row_group = row_group
        self.offset = offset
        self.size = size
        self.page_type = page_type
        self.encoding = encoding
        self.first_row = first_row
        self.first_row_in_group = first_row_in_group
        self.codec = codec
        self.num_values = num_values
        self.total_uncompressed_size = total_uncompressed_size
        self.region = None
        self.dictionary_region = None
        self.starts_with_continuation = False


class Chunk:
    def __init__(self, column, row_group, thrift, metadata):
        self.column = column
        self.row_group = row_group
        self.thrift = thrift
        self.metadata = metadata
        self.codec = required(metadata, 4, compact.I32, "ColumnMetaData.codec")
        self.num_values = required(metadata, 5, compact.I64, "ColumnMetaData.num_values")
        self.total_uncompressed = required(
            metadata, 6, compact.I64, "ColumnMetaData.total_uncompressed_size"
        )
        self.total_compressed = required(
            metadata, 7, compact.I64, "ColumnMetaData.total_compressed_size"
        )
        self.data_offset = required(
            metadata, 9, compact.I64, "ColumnMetaData.data_page_offset"
        )
        self.dictionary_offset = optional_int(metadata, 11)
        self.offset_index_offset = optional_int(thrift, 4)
        self.offset_index_length = optional_int(thrift, 5)
        self.column_index_offset = optional_int(thrift, 6)
        self.column_index_length = optional_int(thrift, 7)
        self.offset_index = None
        self.column_index = None
        self.pages = []


def parse_page_header(fields):
    page_type = required(fields, 1, compact.I32, "PageHeader.type")
    uncompressed_size = required(fields, 2, compact.I32, "PageHeader.uncompressed_page_size")
    compressed_size = required(fields, 3, compact.I32, "PageHeader.compressed_page_size")
    encoding = None
    num_values = None
    num_rows = None
    repetition_encoding = None
    if page_type == DATA_PAGE:
        header = required(fields, 5, compact.STRUCT, "PageHeader.data_page_header")
        num_values = required(header, 1, compact.I32, "DataPageHeader.num_values")
        encoding = required(header, 2, compact.I32, "DataPageHeader.encoding")
        repetition_encoding = required(
            header, 4, compact.I32, "DataPageHeader.repetition_level_encoding"
        )
    elif page_type == DATA_PAGE_V2:
        header = required(fields, 8, compact.STRUCT, "PageHeader.data_page_header_v2")
        num_values = required(header, 1, compact.I32, "DataPageHeaderV2.num_values")
        num_rows = required(header, 3, compact.I32, "DataPageHeaderV2.num_rows")
        encoding = required(header, 4, compact.I32, "DataPageHeaderV2.encoding")
    elif page_type == DICTIONARY_PAGE:
        header = required(fields, 7, compact.STRUCT, "PageHeader.dictionary_page_header")
        num_values = required(header, 1, compact.I32, "DictionaryPageHeader.num_values")
        encoding = required(header, 2, compact.I32, "DictionaryPageHeader.encoding")
    return (
        page_type,
        uncompressed_size,
        compressed_size,
        encoding,
        num_values,
        num_rows,
        repetition_encoding,
    )


def read_page_header(stream, offset, remaining):
    read_size = min(4096, remaining)
    while read_size:
        stream.seek(offset)
        data = stream.read(read_size)
        try:
            fields, used = compact.decode_prefix(data)
            return fields, used
        except compact.DecodeError:
            if read_size == remaining:
                raise
            read_size = min(read_size * 2, remaining)
    raise ValueError("empty page header")


def read_uleb(data, offset):
    value = 0
    shift = 0
    while shift < 70:
        if offset >= len(data):
            raise ValueError("truncated RLE/bit-packed run header")
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7f) << shift
        if not byte & 0x80:
            return value, offset
        shift += 7
    raise ValueError("invalid RLE/bit-packed run header")


def read_zigzag(data, offset):
    value, offset = read_uleb(data, offset)
    return (value >> 1) ^ -(value & 1), offset


def unpack_bits(data, width, count):
    values = []
    for index in range(count):
        value = 0
        bit_offset = index * width
        for bit in range(width):
            absolute = bit_offset + bit
            if data[absolute // 8] & (1 << (absolute % 8)):
                value |= 1 << bit
        values.append(value)
    return values


def decode_delta_values(payload):
    block_size, offset = read_uleb(payload, 0)
    mini_block_count, offset = read_uleb(payload, offset)
    count, offset = read_uleb(payload, offset)
    first, offset = read_zigzag(payload, offset)
    if block_size % 128 or not mini_block_count or block_size % mini_block_count:
        raise ValueError("invalid DELTA_BINARY_PACKED block shape")
    mini_block_size = block_size // mini_block_count
    if mini_block_size % 32:
        raise ValueError("invalid DELTA_BINARY_PACKED miniblock size")
    values = [] if count == 0 else [first]
    while len(values) < count:
        minimum, offset = read_zigzag(payload, offset)
        if offset + mini_block_count > len(payload):
            raise ValueError("truncated DELTA_BINARY_PACKED bit widths")
        widths = payload[offset:offset + mini_block_count]
        offset += mini_block_count
        remaining = count - len(values)
        for width in widths:
            byte_count = mini_block_size * width // 8
            if offset + byte_count > len(payload):
                raise ValueError("truncated DELTA_BINARY_PACKED miniblock")
            residuals = unpack_bits(
                payload[offset:offset + byte_count], width, mini_block_size
            )
            offset += byte_count
            used = min(remaining, mini_block_size)
            for residual in residuals[:used]:
                values.append(values[-1] + minimum + residual)
            remaining -= used
    if offset != len(payload):
        raise ValueError("DELTA_BINARY_PACKED stream has the wrong decoded length")
    return values


def unpack_dense_values(data, count):
    if not data:
        raise ValueError("packed array is empty")
    width = data[0]
    byte_count = (count * width + 7) // 8
    if width > 64 or len(data) != byte_count + 1:
        raise ValueError("packed array has the wrong length")
    return unpack_bits(data[1:], width, count)


def decode_rle_levels(data, width, count):
    if width == 0:
        return [0] * count
    values = []
    offset = 0
    while len(values) < count:
        header, offset = read_uleb(data, offset)
        if header & 1:
            run_count = (header >> 1) * 8
            byte_count = (run_count * width + 7) // 8
            if offset + byte_count > len(data):
                raise ValueError("truncated bit-packed levels")
            values.extend(unpack_bits(data[offset:offset + byte_count], width, run_count))
            offset += byte_count
        else:
            run_count = header >> 1
            byte_count = (width + 7) // 8
            if offset + byte_count > len(data):
                raise ValueError("truncated repeated level")
            value = int.from_bytes(data[offset:offset + byte_count], "little")
            values.extend([value] * run_count)
            offset += byte_count
    return values[:count]


def snappy_decompress(data, expected_size):
    declared_size, offset = read_uleb(data, 0)
    output = bytearray()
    while offset < len(data):
        tag = data[offset]
        offset += 1
        kind = tag & 3
        if kind == 0:
            length = tag >> 2
            if length < 60:
                length += 1
            else:
                length_bytes = length - 59
                if offset + length_bytes > len(data):
                    raise ValueError("truncated Snappy literal length")
                length = 1 + int.from_bytes(data[offset:offset + length_bytes], "little")
                offset += length_bytes
            if offset + length > len(data):
                raise ValueError("truncated Snappy literal")
            output.extend(data[offset:offset + length])
            offset += length
            continue
        if kind == 1:
            length = 4 + ((tag >> 2) & 7)
            if offset >= len(data):
                raise ValueError("truncated Snappy copy")
            copy_offset = ((tag & 0xe0) << 3) | data[offset]
            offset += 1
        elif kind == 2:
            length = 1 + (tag >> 2)
            if offset + 2 > len(data):
                raise ValueError("truncated Snappy copy")
            copy_offset = int.from_bytes(data[offset:offset + 2], "little")
            offset += 2
        else:
            length = 1 + (tag >> 2)
            if offset + 4 > len(data):
                raise ValueError("truncated Snappy copy")
            copy_offset = int.from_bytes(data[offset:offset + 4], "little")
            offset += 4
        if copy_offset <= 0 or copy_offset > len(output):
            raise ValueError("invalid Snappy copy offset")
        for unused in range(length):
            output.append(output[-copy_offset])
    if declared_size != len(output) or expected_size != len(output):
        raise ValueError(
            "Snappy size mismatch: declared {}, expected {}, decoded {}".format(
                declared_size, expected_size, len(output)
            )
        )
    return bytes(output)


def decompress(codec, data, uncompressed_size):
    if codec == 0:
        return data
    if codec == 1:
        return snappy_decompress(data, uncompressed_size)
    if codec == 2:
        result = gzip.decompress(data)
        if len(result) != uncompressed_size:
            raise ValueError("GZIP size mismatch")
        return result
    raise ValueError(
        "decoding nested V1 pages with compression codec {} is not implemented".format(codec)
    )


def v1_page_rows(
    stream, body_offset, compressed_size, uncompressed_size, codec, num_values, max_rep
):
    stream.seek(body_offset)
    compressed = stream.read(compressed_size)
    if len(compressed) != compressed_size:
        raise ValueError("truncated data-page body")
    body = decompress(codec, compressed, uncompressed_size)
    if len(body) < 4:
        raise ValueError("V1 data page has no repetition-level length")
    repetition_bytes = struct.unpack("<I", body[:4])[0]
    if 4 + repetition_bytes > len(body):
        raise ValueError("V1 repetition levels exceed the page body")
    levels = decode_rle_levels(body[4:4 + repetition_bytes], max_rep.bit_length(), num_values)
    if not levels:
        return 0, False
    return sum(level == 0 for level in levels), levels[0] != 0


def read_thrift_range(stream, offset, length, name):
    if offset is None or length is None or length <= 0:
        return None
    stream.seek(offset)
    blob = stream.read(length)
    if len(blob) != length:
        raise ValueError("{} is truncated".format(name))
    return compact.decode(blob)


def parse_offset_index(fields):
    locations = required(fields, 1, compact.LIST, "OffsetIndex.page_locations")
    if locations[0] != compact.STRUCT:
        raise ValueError("OffsetIndex.page_locations has the wrong element type")
    return [
        {
            "offset": required(location, 1, compact.I64, "PageLocation.offset"),
            "size": required(location, 2, compact.I32, "PageLocation.compressed_page_size"),
            "first_row": required(location, 3, compact.I64, "PageLocation.first_row_index"),
        }
        for location in locations[1]
    ]


def parse_column_index(fields):
    null_pages = required(fields, 1, compact.LIST, "ColumnIndex.null_pages")[1]
    minimums = required(fields, 2, compact.LIST, "ColumnIndex.min_values")[1]
    maximums = required(fields, 3, compact.LIST, "ColumnIndex.max_values")[1]
    boundary_order = required(fields, 4, compact.I32, "ColumnIndex.boundary_order")
    if len(null_pages) != len(minimums) or len(null_pages) != len(maximums):
        raise ValueError("ColumnIndex arrays have different lengths")
    null_counts_field = field(fields, 5)
    null_counts = None if null_counts_field is None else null_counts_field[2][1]
    if null_counts is not None and len(null_counts) != len(null_pages):
        raise ValueError("ColumnIndex.null_counts has the wrong length")
    return {
        "null_pages": [bool(value) for value in null_pages],
        "minimums": minimums,
        "maximums": maximums,
        "boundary_order": boundary_order,
        "null_counts": null_counts,
    }


def scan_pages(path, footer):
    groups = row_groups(footer)
    if not groups:
        raise ValueError("footer contains no row groups")
    num_columns = len(column_chunks(groups[0]))
    chunks = [[None for unused in range(num_columns)] for unused in groups]
    pages = []
    max_repetition_levels = schema_leaf_max_repetition_levels(footer)
    if len(max_repetition_levels) != num_columns:
        raise ValueError("schema leaf count does not match row-group column count")
    row_base = 0
    with path.open("rb") as stream:
        for group_ordinal, group in enumerate(groups):
            group_rows = required(group, 3, compact.I64, "RowGroup.num_rows")
            group_chunks = column_chunks(group)
            if len(group_chunks) != num_columns:
                raise ValueError("ragged row groups")
            for column, thrift_chunk in enumerate(group_chunks):
                metadata = column_metadata(thrift_chunk)
                chunk = Chunk(column, group_ordinal, thrift_chunk, metadata)
                chunks[group_ordinal][column] = chunk
                offset_index_fields = read_thrift_range(
                    stream,
                    chunk.offset_index_offset,
                    chunk.offset_index_length,
                    "OffsetIndex",
                )
                if offset_index_fields is not None:
                    chunk.offset_index = parse_offset_index(offset_index_fields)
                column_index_fields = read_thrift_range(
                    stream,
                    chunk.column_index_offset,
                    chunk.column_index_length,
                    "ColumnIndex",
                )
                if column_index_fields is not None:
                    chunk.column_index = parse_column_index(column_index_fields)
                    if chunk.offset_index is None:
                        raise ValueError("ColumnIndex is present without OffsetIndex")
                start = chunk.data_offset
                if chunk.dictionary_offset is not None:
                    start = min(start, chunk.dictionary_offset)
                end = start + chunk.total_compressed
                offset = start
                first_row = 0
                data_page_ordinal = 0
                while offset < end:
                    header, header_size = read_page_header(stream, offset, end - offset)
                    (
                        page_type,
                        uncompressed_size,
                        body_size,
                        encoding,
                        num_values,
                        num_rows,
                        repetition_encoding,
                    ) = parse_page_header(header)
                    size = header_size + body_size
                    if size <= 0 or offset + size > end:
                        raise ValueError(
                            "page at {} exceeds chunk [{}, {})".format(offset, start, end)
                        )
                    starts_with_continuation = False
                    if page_type in (DATA_PAGE, DATA_PAGE_V2) and chunk.offset_index is not None:
                        if data_page_ordinal >= len(chunk.offset_index):
                            raise ValueError("OffsetIndex has too few page locations")
                        location = chunk.offset_index[data_page_ordinal]
                        if location["offset"] != offset or location["size"] != size:
                            raise ValueError("OffsetIndex placement disagrees with scanned page")
                        first_row = location["first_row"]
                        next_first_row = (
                            chunk.offset_index[data_page_ordinal + 1]["first_row"]
                            if data_page_ordinal + 1 < len(chunk.offset_index)
                            else group_rows
                        )
                        num_rows = next_first_row - first_row
                    elif page_type == DATA_PAGE and max_repetition_levels[column] > 0:
                        if repetition_encoding != 3:
                            raise ValueError(
                                "nested V1 page uses unsupported repetition-level encoding "
                                "{}".format(repetition_encoding)
                            )
                        num_rows, starts_with_continuation = v1_page_rows(
                            stream,
                            offset + header_size,
                            body_size,
                            uncompressed_size,
                            chunk.codec,
                            num_values,
                            max_repetition_levels[column],
                        )
                    page_first_row = first_row if page_type in (DATA_PAGE, DATA_PAGE_V2) else None
                    if starts_with_continuation:
                        page_first_row = max(0, page_first_row - 1)
                    page = Page(
                        column, group_ordinal, offset, size, page_type, encoding,
                        None if page_first_row is None else row_base + page_first_row,
                        page_first_row, chunk.codec, num_values or 0,
                        header_size + uncompressed_size
                    )
                    page.starts_with_continuation = starts_with_continuation
                    chunk.pages.append(page)
                    pages.append(page)
                    if page_type in (DATA_PAGE, DATA_PAGE_V2):
                        if num_rows is None:
                            num_rows = num_values
                        first_row += num_rows
                        data_page_ordinal += 1
                    offset += size
                if offset != end:
                    raise ValueError("column chunk scan did not end at its declared size")
                if first_row != group_rows:
                    raise ValueError(
                        "page rows {} do not match row-group rows {} for chunk ({}, {})".format(
                            first_row, group_rows, column, group_ordinal
                        )
                    )
                if chunk.offset_index is not None and data_page_ordinal != len(chunk.offset_index):
                    raise ValueError("OffsetIndex page count disagrees with scanned pages")
                if (
                    chunk.column_index is not None
                    and len(chunk.column_index["null_pages"]) != data_page_ordinal
                ):
                    raise ValueError("ColumnIndex page count disagrees with scanned pages")
            row_base += group_rows
    return chunks, pages


def common_prefix(left, right):
    size = min(len(left), len(right))
    index = 0
    while index < size and left[index] == right[index]:
        index += 1
    return left[:index]


def round_up(value):
    output = bytearray(value)
    for index in range(len(output) - 1, -1, -1):
        if output[index] != 0xff:
            output[index] += 1
            return bytes(output[:index + 1])
    return None


def truncate_bounds(minimum, maximum, limit):
    min_exact = True
    max_exact = True
    if limit and len(minimum) > limit:
        minimum = minimum[:limit]
        min_exact = False
    if limit and len(maximum) > limit:
        rounded = round_up(maximum[:limit])
        if rounded is not None:
            maximum = rounded
            max_exact = False
    return minimum, maximum, min_exact, max_exact


def statistic(metadata):
    item = field(metadata, 12)
    if item is None:
        return None
    values = item[2]
    minimum = field(values, 6) or field(values, 2)
    maximum = field(values, 5) or field(values, 1)
    return {
        "null": optional_int(values, 3),
        "nan": optional_int(values, 9),
        "min": None if minimum is None else minimum[2],
        "max": None if maximum is None else maximum[2],
        "min_exact": bool(optional_int(values, 8, True)),
        "max_exact": bool(optional_int(values, 7, True)),
    }


def column_index_stat(column_index, page):
    all_null = column_index["null_pages"][page]
    null_counts = column_index["null_counts"]
    return {
        "null": None if null_counts is None else null_counts[page],
        "nan": None,
        "min": None if all_null else column_index["minimums"][page],
        "max": None if all_null else column_index["maximums"][page],
        "min_exact": True,
        "max_exact": True,
    }


def build_modular_column_statistics(column_chunks, suffix_limit):
    domain = len(column_chunks)
    null_positions = []
    null_values = []
    minmax_positions = []
    prefixes = []
    min_suffixes = []
    max_suffixes = []
    min_exact = []
    max_exact = []
    for position, chunk in enumerate(column_chunks):
        stats = statistic(chunk.metadata)
        if stats is None:
            continue
        if stats["null"] is not None:
            null_positions.append(position)
            null_values.append(stats["null"])
        if stats["min"] is None or stats["max"] is None:
            continue
        prefix = common_prefix(stats["min"], stats["max"])
        minimum, maximum, min_was_exact, max_was_exact = truncate_bounds(
            stats["min"][len(prefix):],
            stats["max"][len(prefix):],
            suffix_limit,
        )
        minmax_positions.append(position)
        prefixes.append(prefix)
        min_suffixes.append(minimum)
        max_suffixes.append(maximum)
        min_exact.append(int(stats["min_exact"] and min_was_exact))
        max_exact.append(int(stats["max_exact"] and max_was_exact))
    if not null_positions and not minmax_positions:
        return b""
    fields = []
    if null_positions:
        fields.append(encoded_field(
            1,
            modular_sparse_int_array(domain, null_positions, null_values),
        ))
    if minmax_positions:
        fields.extend([
            encoded_field(
                2,
                modular_sparse_bytes_array(domain, minmax_positions, prefixes),
            ),
            encoded_field(
                3,
                modular_sparse_bytes_array(domain, minmax_positions, min_suffixes),
            ),
            encoded_field(
                4,
                modular_sparse_bytes_array(domain, minmax_positions, max_suffixes),
            ),
        ])
        if not all(min_exact) or not all(max_exact):
            fields.extend([
                encoded_field(
                    5,
                    modular_sparse_int_array(domain, minmax_positions, min_exact),
                ),
                encoded_field(
                    6,
                    modular_sparse_int_array(domain, minmax_positions, max_exact),
                ),
            ])
    return compact.encode(fields)


def build_flat_column_index(column_chunks, suffix_limit):
    page_domain = sum(
        sum(page.page_type in (DATA_PAGE, DATA_PAGE_V2) for page in chunk.pages)
        for chunk in column_chunks
    )
    boundary_positions = []
    boundary_values = []
    page_positions = []
    null_pages = []
    null_count_positions = []
    null_counts = []
    minmax_positions = []
    prefixes = []
    min_suffixes = []
    max_suffixes = []
    min_exact = []
    max_exact = []
    page_base = 0
    for group, chunk in enumerate(column_chunks):
        page_count = sum(
            page.page_type in (DATA_PAGE, DATA_PAGE_V2) for page in chunk.pages
        )
        column_index = chunk.column_index
        if column_index is not None:
            boundary_positions.append(group)
            boundary_values.append(column_index["boundary_order"])
            for page in range(page_count):
                position = page_base + page
                page_positions.append(position)
                all_null = column_index["null_pages"][page]
                null_pages.append(int(all_null))
                if column_index["null_counts"] is not None:
                    null_count_positions.append(position)
                    null_counts.append(column_index["null_counts"][page])
                if all_null:
                    continue
                minimum = column_index["minimums"][page]
                maximum = column_index["maximums"][page]
                prefix = common_prefix(minimum, maximum)
                minimum, maximum, min_was_exact, max_was_exact = truncate_bounds(
                    minimum[len(prefix):], maximum[len(prefix):], suffix_limit
                )
                minmax_positions.append(position)
                prefixes.append(prefix)
                min_suffixes.append(minimum)
                max_suffixes.append(maximum)
                min_exact.append(int(min_was_exact))
                max_exact.append(int(max_was_exact))
        page_base += page_count
    if not boundary_positions:
        return b""
    fields = [
        encoded_field(
            1,
            modular_int_array(
                len(column_chunks), boundary_positions, boundary_values
            ),
        ),
        encoded_field(
            2,
            modular_int_array(page_domain, page_positions, null_pages),
        ),
    ]
    if null_count_positions:
        fields.append(encoded_field(
            3,
            modular_int_array(
                page_domain, null_count_positions, null_counts
            ),
        ))
    if minmax_positions:
        fields.extend([
            encoded_field(
                4,
                modular_bytes_array(page_domain, minmax_positions, prefixes),
            ),
            encoded_field(
                5,
                modular_bytes_array(page_domain, minmax_positions, min_suffixes),
            ),
            encoded_field(
                6,
                modular_bytes_array(page_domain, minmax_positions, max_suffixes),
            ),
        ])
        if not all(min_exact) or not all(max_exact):
            fields.extend([
                encoded_field(
                    7,
                    modular_sparse_int_array(page_domain, minmax_positions, min_exact),
                ),
                encoded_field(
                    8,
                    modular_sparse_int_array(page_domain, minmax_positions, max_exact),
                ),
            ])
    return compact.encode(fields)


def build_row_range_statistics_descriptor(statistics, range_start_rows, suffix_limit):
    if len(statistics) != len(range_start_rows):
        raise ValueError("statistics ranges do not match column chunks")
    if not range_start_rows or range_start_rows[0] != 0 or any(
        current <= previous
        for previous, current in zip(range_start_rows, range_start_rows[1:])
    ):
        raise ValueError("statistics ranges must start at zero and increase")
    null_positions = []
    null_values = []
    nan_positions = []
    nan_values = []
    minmax_positions = []
    prefixes = []
    min_suffixes = []
    max_suffixes = []
    min_exact = []
    max_exact = []
    for position, stats in enumerate(statistics):
        if stats is None:
            return b""
        has_minmax = stats["min"] is not None and stats["max"] is not None
        if stats["null"] is None and stats["nan"] is None and not has_minmax:
            return b""
        if stats["null"] is not None:
            null_positions.append(position)
            null_values.append(stats["null"])
        if stats["nan"] is not None:
            nan_positions.append(position)
            nan_values.append(stats["nan"])
        if not has_minmax:
            continue
        prefix = common_prefix(stats["min"], stats["max"])
        minimum = stats["min"][len(prefix):]
        maximum = stats["max"][len(prefix):]
        minimum, maximum, min_was_exact, max_was_exact = truncate_bounds(
            minimum, maximum, suffix_limit
        )
        minmax_positions.append(position)
        prefixes.append(prefix)
        min_suffixes.append(minimum)
        max_suffixes.append(maximum)
        min_exact.append(int(stats["min_exact"] and min_was_exact))
        max_exact.append(int(stats["max_exact"] and max_was_exact))
    domain = len(range_start_rows)
    fields = [
        encoded_field(1, adaptive_array(range_start_rows)),
    ]
    if len(null_positions) == domain:
        fields.append(encoded_field(2, adaptive_array(null_values)))
    if len(nan_positions) == domain:
        fields.append(encoded_field(3, adaptive_array(nan_values)))
    if minmax_positions:
        if len(minmax_positions) != domain:
            fields.append([
                4, compact.BINARY, presence_bitmap(domain, minmax_positions),
            ])
        prefix_lengths = [len(prefix) for prefix in prefixes]
        suffix_lengths = []
        suffixes = bytearray()
        prefix_bytes = bytearray()
        for prefix, minimum, maximum in zip(prefixes, min_suffixes, max_suffixes):
            prefix_bytes.extend(prefix)
            suffix_lengths.extend([len(minimum), len(maximum)])
            suffixes.extend(minimum)
            suffixes.extend(maximum)
        fields.extend([
            encoded_field(5, adaptive_array(prefix_lengths)),
            [6, compact.BINARY, bytes(prefix_bytes)],
            encoded_field(7, adaptive_array(suffix_lengths)),
            [8, compact.BINARY, bytes(suffixes)],
        ])
        if not all(min_exact) or not all(max_exact):
            exactness = [value for pair in zip(min_exact, max_exact) for value in pair]
            fields.append([
                9, compact.BINARY, pack_bits(exactness, 1),
            ])
    return compact.encode(fields)


def build_statistics_descriptor(column_chunks, range_start_rows, suffix_limit):
    return build_row_range_statistics_descriptor(
        [statistic(chunk.metadata) for chunk in column_chunks],
        range_start_rows,
        suffix_limit,
    )


def build_full_column_statistics(column_chunks, row_group_starts, suffix_limit):
    statistics = []
    range_starts = []
    boundary_orders = []
    for chunk, group_start in zip(column_chunks, row_group_starts):
        data_pages = [
            page for page in chunk.pages
            if page.page_type in (DATA_PAGE, DATA_PAGE_V2)
        ]
        if chunk.column_index is None:
            stats = statistic(chunk.metadata)
            if stats is None:
                return b"", 0
            range_starts.append(group_start)
            statistics.append(stats)
            boundary_orders.append(0)
            continue
        boundary_orders.append(chunk.column_index["boundary_order"])
        for page, data_page in enumerate(data_pages):
            range_starts.append(data_page.first_row)
            statistics.append(column_index_stat(chunk.column_index, page))
    if not range_starts or range_starts[0] != 0:
        raise ValueError("row-range statistics do not start at row zero")
    if any(current <= previous for previous, current in zip(range_starts, range_starts[1:])):
        raise ValueError("page-level row-range statistics do not strictly increase")
    descriptor = build_row_range_statistics_descriptor(
        statistics, range_starts, suffix_limit
    )
    if not descriptor:
        return b"", 0
    boundary_order = boundary_orders[0] if len(boundary_orders) == 1 else 0
    return descriptor, boundary_order


def build_legacy_offset_index(chunk):
    locations = []
    for page in chunk.pages:
        if page.page_type not in (DATA_PAGE, DATA_PAGE_V2):
            continue
        if page.starts_with_continuation:
            raise ValueError(
                "legacy OffsetIndex cannot describe a page beginning inside a top-level row"
            )
        locations.append([
            [1, compact.I64, page.offset],
            [2, compact.I32, page.size],
            [3, compact.I64, page.first_row_in_group],
        ])
    return compact.encode([
        [1, compact.LIST, (compact.STRUCT, locations)],
    ])


def add_index_locators(footer, chunks, footer_start):
    converted = copy.deepcopy(footer)
    converted_groups = row_groups(converted)
    offset = footer_start
    index_bytes = 0
    for group_ordinal, group in enumerate(converted_groups):
        for column, thrift_chunk in enumerate(column_chunks(group)):
            blob = build_legacy_offset_index(chunks[group_ordinal][column])
            thrift_chunk[:] = [item for item in thrift_chunk if item[0] not in (4, 5)]
            thrift_chunk.extend([
                [4, compact.I64, offset],
                [5, compact.I32, len(blob)],
            ])
            thrift_chunk.sort(key=lambda item: item[0])
            offset += len(blob)
            index_bytes += len(blob)
    footer_bytes = compact.encode(converted)
    return index_bytes, len(footer_bytes), index_bytes + len(footer_bytes)


class Layout:
    def __init__(self, footer_start, initial_offsets):
        self.footer_start = footer_start
        self.offsets = list(initial_offsets)
        self.output = bytearray()
        self.regions = {}

    @property
    def cursor(self):
        return self.footer_start + len(self.output)

    def add(self, name, blob):
        if not blob:
            raise ValueError("cannot create an empty region")
        if self.offsets[-1] != self.cursor:
            raise ValueError("region layout is not contiguous")
        region = len(self.offsets) - 1
        start = self.cursor
        self.output.extend(blob)
        self.offsets.append(self.cursor)
        self.regions[name] = (region, start, len(blob))
        return region


def initial_region_offsets(pages, footer_start):
    boundaries = {0, footer_start}
    intervals = []
    for page in pages:
        boundaries.add(page.offset)
        boundaries.add(page.offset + page.size)
        intervals.append((page.offset, page.offset + page.size))
    ordered = sorted(boundaries)
    for start, end in intervals:
        index = bisect.bisect_left(ordered, start)
        if index + 1 >= len(ordered) or ordered[index + 1] != end:
            raise ValueError("a page does not occupy exactly one region")
    return ordered


def build_row_group_region_ordinals(chunks, offsets, footer_start):
    end_border = bisect.bisect_left(offsets, footer_start)
    if end_border == len(offsets) or offsets[end_border] != footer_start:
        raise ValueError("footer start is not a region boundary")
    borders = [None] * len(chunks)
    for group_ordinal, group_chunks in enumerate(chunks):
        pages = [page for chunk in group_chunks for page in chunk.pages]
        if pages:
            borders[group_ordinal] = min(page.region for page in pages)
    next_border = end_border
    for group_ordinal in range(len(borders) - 1, -1, -1):
        border = borders[group_ordinal]
        if border is None:
            borders[group_ordinal] = next_border
        elif border > next_border:
            raise ValueError("row-group pages are not in physical row-group order")
        next_border = borders[group_ordinal]
    borders.append(end_border)
    for group_ordinal, group_chunks in enumerate(chunks):
        for chunk in group_chunks:
            for page in chunk.pages:
                if not borders[group_ordinal] <= page.region < borders[group_ordinal + 1]:
                    raise ValueError("row-group pages do not form contiguous physical spans")
    return borders


def thrift_schema_module(footer):
    schema = required(footer, 2, compact.LIST, "FileMetaData.schema")
    fields = [[1, compact.LIST, schema]]
    column_orders = field(footer, 7)
    if column_orders is not None:
        fields.append([2, compact.LIST, column_orders[2]])
    return compact.encode(fields)


def thrift_file_metadata_module(footer):
    fields = []
    created_by = field(footer, 6)
    key_values = field(footer, 5)
    if created_by is not None:
        fields.append([1, compact.BINARY, created_by[2]])
    if key_values is not None:
        fields.append([2, compact.LIST, key_values[2]])
    return b"" if not fields else compact.encode(fields)


def build_page_footer(
    path, footer, footer_start, chunks, pages, suffix_limit, footer_format, tier
):
    if footer_format not in ("addendum", "full"):
        raise ValueError("unknown page-first footer format {}".format(footer_format))
    if tier not in ("core", "stats", "full"):
        raise ValueError("unknown metadata tier {}".format(tier))
    groups = row_groups(footer)
    num_columns = len(chunks[0])
    offsets = initial_region_offsets(pages, footer_start)
    region_by_offset = {offset: index for index, offset in enumerate(offsets[:-1])}
    for page in pages:
        page.region = region_by_offset[page.offset]
    row_group_ordinals = build_row_group_region_ordinals(
        chunks, offsets, footer_start
    )
    row_group_num_rows = [
        required(group, 3, compact.I64, "RowGroup.num_rows") for group in groups
    ]
    layout = Layout(footer_start, offsets)
    modules = []
    page_descriptor_bytes = 0
    stats_descriptor_bytes = 0
    stats_directory_bytes = 0
    column_index_descriptor_bytes = 0
    column_index_directory_bytes = 0

    column_pages = [[] for unused in range(num_columns)]
    for group_chunks in chunks:
        for chunk in group_chunks:
            column_pages[chunk.column].extend(
                page for page in chunk.pages
                if page.page_type in (DATA_PAGE, DATA_PAGE_V2)
            )

    page_regions = []
    for column in range(num_columns):
        for group_chunks in chunks:
            chunk = group_chunks[column]
            dictionary = next(
                (page for page in chunk.pages if page.page_type == DICTIONARY_PAGE), None
            )
            dictionary_region = None if dictionary is None else dictionary.region
            for page in chunk.pages:
                if page.page_type not in (DATA_PAGE, DATA_PAGE_V2):
                    continue
                uses_dictionary = page.encoding in (PLAIN_DICTIONARY, RLE_DICTIONARY)
                if uses_dictionary and dictionary_region is None:
                    raise ValueError("dictionary-encoded page has no dictionary page")
                page.dictionary_region = dictionary_region if uses_dictionary else None

        column_page_values = column_pages[column]
        page_blob = compact.encode([
            encoded_field(1, adaptive_array([
                page.region for page in column_page_values
            ])),
            encoded_field(2, adaptive_array([
                page.first_row for page in column_page_values
            ])),
            [3, compact.BOOL_TRUE, bool(column_page_values) and all(
                page.dictionary_region is not None for page in column_page_values
            )],
            encoded_field(4, adaptive_array([
                page.codec for page in column_page_values
            ])),
            encoded_field(5, adaptive_array([
                0 if page.dictionary_region is None else page.dictionary_region + 1
                for page in column_page_values
            ])),
            encoded_field(6, adaptive_array([
                page.num_values for page in column_page_values
            ])),
            encoded_field(7, adaptive_array([
                page.total_uncompressed_size for page in column_page_values
            ])),
        ])
        page_regions.append(layout.add("pages_{}".format(column), page_blob))
        page_descriptor_bytes += len(page_blob)

    page_directory = compact.encode([
        [1, compact.BINARY, dense_values(page_regions)],
    ])
    page_directory_region = layout.add("page_directory", page_directory)
    modules.append((K_PAGE_DIRECTORY, page_directory_region))

    stats_positions = []
    stats_values = []
    boundary_orders = []
    row_start = 0
    range_starts = []
    for group in groups:
        num_rows = required(group, 3, compact.I64, "RowGroup.num_rows")
        range_starts.append(row_start)
        row_start += num_rows
    if tier != "core":
        for column in range(num_columns):
            column_chunks = [group_chunks[column] for group_chunks in chunks]
            if footer_format == "addendum":
                descriptor = build_modular_column_statistics(
                    column_chunks, suffix_limit
                )
                boundary_order = None
            else:
                descriptor, boundary_order = build_full_column_statistics(
                    column_chunks, range_starts, suffix_limit
                )
            if not descriptor:
                continue
            region = layout.add("statistics_{}".format(column), descriptor)
            stats_positions.append(column)
            stats_values.append(region)
            if boundary_order is not None:
                boundary_orders.append(boundary_order)
            stats_descriptor_bytes += len(descriptor)
    if stats_positions:
        stats_directory_fields = []
        if len(stats_positions) != num_columns:
            stats_directory_fields.append([
                1, compact.BINARY, presence_bitmap(num_columns, stats_positions),
            ])
        stats_directory_fields.append([
            2, compact.BINARY, dense_values(stats_values),
        ])
        if footer_format == "full":
            stats_directory_fields.append([
                3,
                compact.BINARY,
                bytes([2]) + pack_bits(boundary_orders, 2),
            ])
        stats_directory = compact.encode(stats_directory_fields)
        stats_directory_bytes = len(stats_directory)
        stats_name = (
            "row_group_statistics"
            if footer_format == "addendum"
            else "row_range_statistics"
        )
        stats_directory_region = layout.add(stats_name, stats_directory)
        modules.append((
            ADD_K_ROW_GROUP_STATISTICS
            if footer_format == "addendum"
            else K_ROW_RANGE_STATISTICS,
            stats_directory_region,
        ))

    column_index_positions = []
    column_index_values = []
    if footer_format == "addendum" and tier == "full":
        for column in range(num_columns):
            descriptor = build_flat_column_index(
                [group_chunks[column] for group_chunks in chunks], suffix_limit
            )
            if not descriptor:
                continue
            region = layout.add("column_index_{}".format(column), descriptor)
            column_index_positions.append(column)
            column_index_values.append(region)
            column_index_descriptor_bytes += len(descriptor)
        if column_index_positions:
            fields = []
            if len(column_index_positions) != num_columns:
                fields.append([
                    1,
                    compact.BINARY,
                    presence_bitmap(num_columns, column_index_positions),
                ])
            fields.append([2, compact.BINARY, dense_values(column_index_values)])
            directory = compact.encode(fields)
            column_index_directory_bytes = len(directory)
            region = layout.add("flat_column_index", directory)
            modules.append((ADD_K_FLAT_COLUMN_INDEX, region))

    file_metadata_blob = thrift_file_metadata_module(footer)
    if file_metadata_blob:
        file_metadata_region = layout.add("file_metadata", file_metadata_blob)
        modules.append((
            ADD_K_FILE_METADATA if footer_format == "addendum" else K_FILE_METADATA,
            file_metadata_region,
        ))

    schema_blob = thrift_schema_module(footer)
    schema_region = layout.add("schema", schema_blob)
    modules.append((K_SCHEMA, schema_region))

    region_map_offset = layout.cursor
    encoded_region_offsets = delta_values(layout.offsets)
    region_map_fields = [[1, compact.BINARY, encoded_region_offsets]]
    if footer_format == "addendum":
        region_map_fields.extend([
            [2, compact.BINARY, delta_values(row_group_ordinals)],
            [3, compact.BINARY, delta_values(row_group_num_rows)],
        ])
    region_map_blob = compact.encode(region_map_fields)
    root_offset = region_map_offset + len(region_map_blob)
    version = required(footer, 1, compact.I32, "FileMetaData.version")
    num_rows = required(footer, 3, compact.I64, "FileMetaData.num_rows")
    if footer_format == "addendum" and (
        len(row_group_ordinals) != len(groups) + 1
        or len(row_group_num_rows) != len(groups)
        or sum(row_group_num_rows) != num_rows
    ):
        raise ValueError("PAR1 row-group arrays disagree with FileMetaData")
    module_list = (
        compact.STRUCT,
        [
            [
                [1, compact.I32, kind],
                [2, compact.I64, region],
            ]
            for kind, region in modules
        ],
    )
    if footer_format == "addendum":
        root_fields = [
            [1, compact.I32, version],
            [2, compact.I32, len(groups)],
            [3, compact.I32, num_columns],
            [4, compact.I64, num_rows],
            [5, compact.LIST, module_list],
        ]
    else:
        root_fields = [
            [1, compact.I32, version],
            [2, compact.I32, num_columns],
            [3, compact.I64, num_rows],
            [4, compact.LIST, module_list],
        ]
    root_blob = compact.encode(root_fields)
    decoded_region_map = compact.decode(region_map_blob)
    decoded_offsets = decode_delta_values(
        required(decoded_region_map, 1, compact.BINARY, "RegionMap.region_offsets")
    )
    if decoded_offsets != layout.offsets:
        raise ValueError("RegionMap round-trip changed an offset")
    if footer_format == "addendum":
        decoded_row_group_ordinals = decode_delta_values(required(
            decoded_region_map,
            2,
            compact.BINARY,
            "RegionMap.row_group_region_ordinals",
        ))
        if decoded_row_group_ordinals != row_group_ordinals:
            raise ValueError("RegionMap round-trip changed a row-group ordinal")
        decoded_row_counts = decode_delta_values(required(
            decoded_region_map,
            3,
            compact.BINARY,
            "RegionMap.row_group_num_rows",
        ))
        if decoded_row_counts != row_group_num_rows:
            raise ValueError("RegionMap round-trip changed a row-group row count")
    decoded_root = compact.decode(root_blob)
    if compact.encode(decoded_root) != root_blob:
        raise ValueError("page-first root failed compact-Thrift round-trip")
    metadata = bytes(layout.output) + region_map_blob + root_blob
    addressing_bytes = page_descriptor_bytes + len(page_directory) + len(region_map_blob)
    placement_bytes = addressing_bytes
    details = {
        "footer_format": footer_format,
        "tier": tier,
        "metadata": metadata,
        "region_map_offset": region_map_offset,
        "region_map_bytes": len(region_map_blob),
        "root_offset": root_offset,
        "root_bytes": len(root_blob),
        "placement_bytes": placement_bytes,
        "addressing_bytes": addressing_bytes,
        "page_descriptor_bytes": page_descriptor_bytes,
        "stats_descriptor_bytes": stats_descriptor_bytes,
        "statistics_bytes": (
            stats_descriptor_bytes
            + stats_directory_bytes
            + column_index_descriptor_bytes
            + column_index_directory_bytes
        ),
        "row_statistics_bytes": stats_descriptor_bytes + stats_directory_bytes,
        "column_index_bytes": (
            column_index_descriptor_bytes + column_index_directory_bytes
        ),
        "indexed_column_chunks": sum(
            chunk.column_index is not None
            for group_chunks in chunks
            for chunk in group_chunks
        ),
        "offset_indexed_column_chunks": sum(
            chunk.offset_index is not None
            for group_chunks in chunks
            for chunk in group_chunks
        ),
        "module_regions": layout.regions,
        "num_regions": len(layout.offsets) - 1,
    }
    return details


def write_output(path, output, footer_start, details, full_file):
    magic = b"PMA1" if details["footer_format"] == "addendum" else b"PMF3"
    trailer = struct.pack(
        "<qqqq4s",
        details["region_map_offset"],
        details["region_map_bytes"],
        details["root_offset"],
        details["root_bytes"],
        magic,
    )
    with output.open("wb") as destination:
        if full_file:
            with path.open("rb") as source:
                remaining = footer_start
                while remaining:
                    block = source.read(min(1024 * 1024, remaining))
                    if not block:
                        raise ValueError("unexpected end while copying data prefix")
                    destination.write(block)
                    remaining -= len(block)
        destination.write(details["metadata"])
        destination.write(trailer)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path, nargs="?")
    parser.add_argument("--full-file", action="store_true")
    parser.add_argument(
        "--format", choices=("addendum", "full"), default="full"
    )
    parser.add_argument(
        "--tier", choices=("core", "stats", "full"), default="full"
    )
    parser.add_argument("--truncate-minmax", type=int, default=16)
    args = parser.parse_args()
    output = args.output or args.input.with_suffix(
        args.input.suffix + ".page-" + args.format
    )

    unused_file_size, footer_start, footer_bytes, footer = read_footer(args.input)
    chunks, pages = scan_pages(args.input, footer)
    details = build_page_footer(
        args.input,
        footer,
        footer_start,
        chunks,
        pages,
        args.truncate_minmax,
        args.format,
        args.tier,
    )
    write_output(args.input, output, footer_start, details, args.full_file)

    num_chunks = sum(len(group_chunks) for group_chunks in chunks)
    data_pages = sum(
        page.page_type in (DATA_PAGE, DATA_PAGE_V2) for page in pages
    )
    dictionary_pages = sum(page.page_type == DICTIONARY_PAGE for page in pages)
    offset_index_bytes = sum(
        chunk.offset_index_length or 0
        for group_chunks in chunks
        for chunk in group_chunks
    )
    column_index_bytes = sum(
        chunk.column_index_length or 0
        for group_chunks in chunks
        for chunk in group_chunks
    )
    print("column_chunks {}".format(num_chunks))
    print("data_pages {}".format(data_pages))
    print("dictionary_pages {}".format(dictionary_pages))
    print("page_regions {}".format(data_pages + dictionary_pages))
    print("regions {}".format(details["num_regions"]))
    print("standard_footer_bytes {}".format(len(footer_bytes)))
    print("source_offset_index_bytes {}".format(offset_index_bytes))
    print("source_column_index_bytes {}".format(column_index_bytes))
    print("source_footer_and_page_index_bytes {}".format(
        len(footer_bytes) + offset_index_bytes + column_index_bytes
    ))
    print("offset_indexed_column_chunks {}".format(
        details["offset_indexed_column_chunks"]
    ))
    print("column_indexed_column_chunks {}".format(
        details["indexed_column_chunks"]
    ))
    print("page_addressing_bytes {}".format(details["addressing_bytes"]))
    print("page_placement_bytes {}".format(details["placement_bytes"]))
    print("page_statistics_bytes {}".format(details["statistics_bytes"]))
    print("page_row_statistics_bytes {}".format(details["row_statistics_bytes"]))
    print("page_column_index_bytes {}".format(details["column_index_bytes"]))
    print("page_modular_total_bytes {}".format(len(details["metadata"])))
    for name, (region, offset, length) in details["module_regions"].items():
        print("{} region={} off={} len={}".format(name, region, offset, length))
    print("region_map off={} len={}".format(
        details["region_map_offset"], details["region_map_bytes"]
    ))
    print("root off={} len={}".format(details["root_offset"], details["root_bytes"]))
    print("output {}".format(output))
    return 0


if __name__ == "__main__":
    sys.exit(main())
