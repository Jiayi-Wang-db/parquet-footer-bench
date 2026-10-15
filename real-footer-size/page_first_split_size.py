#!/usr/bin/env python3
"""Measure current and split page-first metadata on Java-written Raincloud files."""

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
    modules = {}
    for line in output.splitlines():
        pieces = line.split()
        if len(pieces) == 2 and pieces[1].isdigit():
            values[pieces[0]] = int(pieces[1])
        if (
            len(pieces) == 3
            and pieces[1].startswith("off=")
            and pieces[2].startswith("len=")
        ):
            modules[pieces[0]] = int(pieces[2].split("=", 1)[1])
    return values, modules


def run_page(converter, path, footer_format, tier, suffix_limit):
    handle, output_name = tempfile.mkstemp(
        suffix=".page-{}-{}".format(footer_format, tier)
    )
    os.close(handle)
    output = Path(output_name)
    try:
        result = subprocess.run(
            [
                sys.executable,
                str(converter),
                "--format",
                footer_format,
                "--tier",
                tier,
                "--truncate-minmax",
                str(suffix_limit),
                str(path),
                str(output),
            ],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        return parse_report(result.stdout)[0]
    finally:
        if output.exists():
            output.unlink()


def run_modular(converter, path, tier, suffix_limit):
    handle, output_name = tempfile.mkstemp(suffix=".modular-" + tier)
    os.close(handle)
    output = Path(output_name)
    try:
        command = [str(converter)]
        if tier == "core":
            command.append("--no-statistics")
        elif tier == "full":
            command.append("--page-index")
        elif tier != "stats":
            raise ValueError("unknown modular tier {}".format(tier))
        command.extend([
            "--truncate-minmax={}".format(suffix_limit),
            str(path),
            str(output),
        ])
        result = subprocess.run(
            command,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        values, modules = parse_report(result.stdout)
        return values["modular_total_bytes"], modules
    finally:
        if output.exists():
            output.unlink()


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
        default=ROOT / "modular-footer" / "parquet_to_page_modular.py",
    )
    parser.add_argument(
        "--modular-converter",
        type=Path,
        default=ROOT / "build" / "modular_footer_convert",
    )
    parser.add_argument("--suffix-limit", type=int, default=16)
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
        "column_chunks_count",
        "offset_indexed_chunks_count",
        "column_indexed_chunks_count",
        "data_pages_count",
        "dictionary_pages_count",
        "current_thrift_footer_bytes",
        "current_thrift_full_bytes",
        "current_modular_core_bytes",
        "current_modular_core_stats_bytes",
        "current_modular_full_bytes",
        "par1_addendum_core_bytes",
        "par1_addendum_core_stats_bytes",
        "par1_addendum_full_bytes",
        "par3_full_core_bytes",
        "par3_full_core_stats_bytes",
        "par3_full_full_bytes",
    ])
    for entry in selected:
        path = args.data / (entry["name"] + ".parquet")
        addendum = {
            tier: run_page(
                args.page_converter, path, "addendum", tier, args.suffix_limit
            )
            for tier in ("core", "stats", "full")
        }
        full = {
            tier: run_page(
                args.page_converter, path, "full", tier, args.suffix_limit
            )
            for tier in ("core", "stats", "full")
        }
        modular = {
            tier: run_modular(
                args.modular_converter, path, tier, args.suffix_limit
            )[0]
            for tier in ("core", "stats", "full")
        }
        source = addendum["full"]
        if source["offset_indexed_column_chunks"] != source["column_chunks"]:
            raise ValueError("{} does not have Offset Index for every chunk".format(path))
        if (
            full["stats"]["page_modular_total_bytes"]
            != full["full"]["page_modular_total_bytes"]
        ):
            raise ValueError("PAR3 stats and full tiers must be identical")
        writer.writerow([
            entry["name"],
            source["column_chunks"],
            source["offset_indexed_column_chunks"],
            source["column_indexed_column_chunks"],
            source["data_pages"],
            source["dictionary_pages"],
            source["standard_footer_bytes"],
            source["source_footer_and_page_index_bytes"],
            modular["core"],
            modular["stats"],
            modular["full"],
            addendum["core"]["page_modular_total_bytes"],
            addendum["stats"]["page_modular_total_bytes"],
            addendum["full"]["page_modular_total_bytes"],
            full["core"]["page_modular_total_bytes"],
            full["stats"]["page_modular_total_bytes"],
            full["full"]["page_modular_total_bytes"],
        ])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
