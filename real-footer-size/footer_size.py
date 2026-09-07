#!/usr/bin/env python3
"""Read exact standard-footer sizes from downloaded Parquet files."""

import argparse
import json
import os
import struct
import sys
from pathlib import Path

import compact


ROOT = Path(__file__).resolve().parent


def footer_size(path):
    file_bytes = path.stat().st_size
    if file_bytes < 12:
        raise ValueError("file is too short")
    with path.open("rb") as stream:
        if stream.read(4) != b"PAR1":
            raise ValueError("missing leading PAR1 magic")
        stream.seek(-8, os.SEEK_END)
        trailer = stream.read(8)
    if trailer[4:] != b"PAR1":
        raise ValueError("missing trailing PAR1 magic")
    footer_bytes = struct.unpack("<I", trailer[:4])[0]
    if footer_bytes + 12 > file_bytes:
        raise ValueError("footer length exceeds file size")
    return file_bytes, footer_bytes


def read_footer(path):
    file_bytes, footer_bytes = footer_size(path)
    with path.open("rb") as stream:
        stream.seek(-8 - footer_bytes, os.SEEK_END)
        footer = stream.read(footer_bytes)
    return file_bytes, footer


def field(fields, field_id):
    for item in fields:
        if item[0] == field_id:
            return item
    return None


def column_metadata(footer):
    row_groups = field(footer, 4)
    if row_groups is None or row_groups[1] != compact.LIST:
        raise ValueError("FileMetaData.row_groups is missing")
    for row_group in row_groups[2][1]:
        columns = field(row_group, 1)
        if columns is None or columns[1] != compact.LIST:
            raise ValueError("RowGroup.columns is missing")
        for column_chunk in columns[2][1]:
            metadata = field(column_chunk, 3)
            if metadata is None or metadata[1] != compact.STRUCT:
                raise ValueError("external ColumnChunk.file_path is not supported")
            yield metadata[2]


def without_paths(footer):
    removed = 0
    for metadata in column_metadata(footer):
        before = len(metadata)
        metadata[:] = [item for item in metadata if item[0] != 3]
        removed += before - len(metadata)
    return removed


def common_prefix(left, right):
    size = min(len(left), len(right))
    offset = 0
    while offset < size and left[offset] == right[offset]:
        offset += 1
    return left[:offset]


def prefix_statistics(footer, suffix_limit):
    converted = 0
    prefix_bytes = 0
    suffix_bytes = 0
    truncated = 0
    for metadata in column_metadata(footer):
        statistics_field = field(metadata, 12)
        if statistics_field is None:
            continue
        statistics = statistics_field[2]
        by_id = {item[0]: item for item in statistics}
        min_field = by_id.get(6) or by_id.get(2)
        max_field = by_id.get(5) or by_id.get(1)
        if min_field is None or max_field is None:
            continue
        if min_field[1] != compact.BINARY or max_field[1] != compact.BINARY:
            raise ValueError("statistics bounds are not binary")
        minimum = min_field[2]
        maximum = max_field[2]
        prefix = common_prefix(minimum, maximum)
        min_remainder = minimum[len(prefix):]
        max_remainder = maximum[len(prefix):]
        min_suffix = min_remainder[:suffix_limit]
        max_suffix = max_remainder[:suffix_limit]
        was_truncated = len(min_remainder) > suffix_limit or len(max_remainder) > suffix_limit

        # Experimental Statistics representation:
        #   1: common prefix, 2: min suffix, 5: max suffix.
        # Standard min/max fields 1, 2, 5, and 6 are removed first. Other
        # statistics fields (null count, distinct count, exactness) remain.
        statistics[:] = [item for item in statistics if item[0] not in (1, 2, 5, 6)]
        statistics.extend([
            [1, compact.BINARY, prefix],
            [2, compact.BINARY, min_suffix],
            [5, compact.BINARY, max_suffix],
        ])
        statistics.sort(key=lambda item: item[0])
        if was_truncated:
            for exactness_id in (7, 8):
                exactness = field(statistics, exactness_id)
                if exactness is not None:
                    exactness[2] = False
        converted += 1
        prefix_bytes += len(prefix)
        suffix_bytes += len(min_suffix) + len(max_suffix)
        truncated += int(was_truncated)
    return converted, prefix_bytes, suffix_bytes, truncated


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("names", nargs="*", help="dataset names; default: all")
    parser.add_argument("--manifest", type=Path, default=ROOT / "corpus.json")
    parser.add_argument("--data", type=Path, default=ROOT / "data")
    parser.add_argument("--suffix-limit", type=int, default=16)
    args = parser.parse_args()
    entries = json.loads(args.manifest.read_text())["datasets"]
    by_name = {entry["name"]: entry for entry in entries}
    selected = [by_name[name] for name in args.names] if args.names else entries

    print("{:<32} {:>12} {:>12} {:>12} {:>10} {:>10}".format(
        "name", "standard", "no_path", "prefix_16", "path save", "stat save"
    ))
    for entry in selected:
        path = args.data / (entry["name"] + ".parquet")
        unused_file_bytes, standard = read_footer(path)
        decoded = compact.decode(standard)
        if compact.encode(decoded) != standard:
            raise ValueError("compact-Thrift fidelity check failed for {}".format(entry["name"]))
        without_paths(decoded)
        no_path = compact.encode(decoded)
        converted, prefix_bytes, suffix_bytes, truncated = prefix_statistics(
            decoded, args.suffix_limit
        )
        prefix_stats = compact.encode(decoded)
        print("{:<32} {:>12,d} {:>12,d} {:>12,d} {:>9.2%} {:>9.2%}".format(
            entry["name"],
            len(standard),
            len(no_path),
            len(prefix_stats),
            1 - len(no_path) / len(standard),
            1 - len(prefix_stats) / len(no_path),
        ))
        if len(standard) != entry["footer_bytes"]:
            raise ValueError("{} footer differs from manifest".format(entry["name"]))
        print("  stats: {} converted, {} truncated, {:,} prefix bytes, "
              "{:,} suffix bytes".format(converted, truncated, prefix_bytes, suffix_bytes))
    return 0


if __name__ == "__main__":
    sys.exit(main())
