#!/usr/bin/env python3
"""Measure the modular footer addendum on Java-written Raincloud files."""

import argparse
import csv
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def parse_report(output):
    values = {}
    for line in output.splitlines():
        pieces = line.split()
        if len(pieces) == 2 and pieces[1].isdigit():
            values[pieces[0]] = int(pieces[1])
    return values


def run_converter(command, suffix):
    handle, output_name = tempfile.mkstemp(suffix=suffix)
    os.close(handle)
    output = Path(output_name)
    try:
        result = subprocess.run(
            command + [str(output)],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        return parse_report(result.stdout)
    finally:
        output.unlink()


def run_addendum(converter, path, tier, suffix_limit):
    return run_converter([
        sys.executable,
        str(converter),
        "--tier",
        tier,
        "--truncate-minmax",
        str(suffix_limit),
        str(path),
    ], ".modular-addendum-" + tier)


def run_modular(converter, path, tier, suffix_limit):
    command = [str(converter)]
    if tier == "core":
        command.append("--no-statistics")
    elif tier == "full":
        command.append("--page-index")
    command.extend(["--truncate-minmax={}".format(suffix_limit), str(path)])
    return run_converter(command, ".modular-" + tier)["modular_total_bytes"]


def run_experimental(path):
    import experimental_footer_size

    converter = experimental_footer_size.load_converter()
    metadata = experimental_footer_size.extract_placement(converter, path)
    output = {}
    for tier, include_offset_index, statistics_kind in (
        ("core", False, None),
        ("stats", False, "coarse"),
        ("full", True, "full"),
    ):
        result = experimental_footer_size.flat_range_values_table(
            metadata, include_offset_index, statistics_kind
        )
        table, integer_values = result[:2]
        binary_values = result[2] if len(result) == 3 else None
        estimate = experimental_footer_size.estimate_layout(
            table,
            integer_values,
            1,
            metadata["footer_start"],
            binary_values=binary_values,
        )
        output[tier] = estimate["estimated_bytes"]
    return output


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
    parser.add_argument(
        "--page-converter",
        type=Path,
        default=ROOT / "modular-footer" / "parquet_to_modular_addendum.py",
    )
    parser.add_argument(
        "--modular-converter",
        type=Path,
        default=ROOT / "build" / "modular_footer_convert",
    )
    parser.add_argument("--suffix-limit", type=int, default=16)
    parser.add_argument("--experimental", action="store_true")
    args = parser.parse_args()

    entries = json.loads(args.manifest.read_text())["datasets"]
    by_name = {entry["name"]: entry for entry in entries}
    unknown = sorted(set(args.names) - set(by_name))
    if unknown:
        parser.error("unknown dataset(s): {}".format(", ".join(unknown)))
    selected = [by_name[name] for name in args.names] if args.names else entries

    writer = csv.writer(sys.stdout, lineterminator="\n")
    writer.writerow([
        "name",
        "thrift_core_bytes",
        "thrift_core_plus_rg_stats_bytes",
        "thrift_core_plus_page_indexes_bytes",
        "modular_core_bytes",
        "modular_core_plus_rg_stats_bytes",
        "modular_core_plus_rg_stats_page_indexes_bytes",
        "addendum_core_bytes",
        "addendum_core_with_rg_stats_as_row_ranges_bytes",
        "addendum_core_with_page_stats_as_row_ranges_rg_fallback_offset_index_bytes",
    ] + ([
        "experimental_flat_arrays_core_bytes",
        "experimental_flat_arrays_with_rg_stats_as_row_ranges_bytes",
        "experimental_flat_arrays_with_page_stats_as_row_ranges_rg_fallback_offset_index_bytes",
    ] if args.experimental else []))
    for entry in selected:
        path = args.data / (entry["name"] + ".parquet")
        addendum = {
            tier: run_addendum(
                args.page_converter, path, tier, args.suffix_limit
            )
            for tier in ("core", "stats", "full")
        }
        modular = {
            tier: run_modular(args.modular_converter, path, tier, args.suffix_limit)
            for tier in ("core", "stats", "full")
        }
        experimental = run_experimental(path) if args.experimental else None
        source = addendum["full"]
        if source["offset_indexed_column_chunks"] != source["column_chunks"]:
            raise ValueError("{} does not have Offset Index for every chunk".format(path))
        row = [
            entry["name"],
            source["standard_footer_bytes"],
            source["standard_footer_bytes"],
            source["source_footer_and_page_index_bytes"],
            modular["core"],
            modular["stats"],
            modular["full"],
            addendum["core"]["modular_addendum_total_bytes"],
            addendum["stats"]["modular_addendum_total_bytes"],
            addendum["full"]["modular_addendum_total_bytes"],
        ]
        if experimental is not None:
            row.extend([
                experimental["core"],
                experimental["stats"],
                experimental["full"],
            ])
        writer.writerow(row)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
