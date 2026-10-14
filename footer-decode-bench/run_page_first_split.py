#!/usr/bin/env python3
"""Generate and benchmark current, PAR1-addendum, and PAR3-full metadata."""

import argparse
import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def run(command):
    return subprocess.run(
        [str(value) for value in command],
        check=True,
        stdout=subprocess.PIPE,
        universal_newlines=True,
    ).stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("names", nargs="*")
    parser.add_argument("--build-dir", type=Path, default=ROOT / "build")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=ROOT / "real-footer-size" / "data" / "raincloud-parquet-java",
    )
    parser.add_argument(
        "--results-dir", type=Path, default=ROOT / "footer-decode-bench" / "results"
    )
    parser.add_argument("--reuse", action="store_true")
    args = parser.parse_args()

    entries = json.loads((ROOT / "real-footer-size" / "corpus.json").read_text())[
        "datasets"
    ]
    by_name = {entry["name"]: entry for entry in entries}
    unknown = sorted(set(args.names) - set(by_name))
    if unknown:
        parser.error("unknown dataset(s): {}".format(", ".join(unknown)))
    selected = [by_name[name] for name in args.names] if args.names else entries
    args.results_dir.mkdir(parents=True, exist_ok=True)
    derived = args.data_dir / "decode-bench"
    derived.mkdir(parents=True, exist_ok=True)

    for entry in selected:
        name = entry["name"]
        source = args.data_dir / (name + ".parquet")
        jump_table = derived / (name + ".jt.parquet")
        modular = derived / (name + ".modular")
        addendum = derived / (name + ".addendum")
        full = derived / (name + ".full")
        if not args.reuse or not jump_table.exists():
            subprocess.run(
                [args.build_dir / "jumptable_footer_convert", source, jump_table],
                check=True,
            )
        if not args.reuse or not modular.exists():
            subprocess.run(
                [
                    args.build_dir / "modular_footer_convert",
                    "--page-index",
                    "--truncate-minmax=16",
                    source,
                    modular,
                ],
                check=True,
            )
        for footer_format, output in (("addendum", addendum), ("full", full)):
            if not args.reuse or not output.exists():
                subprocess.run(
                    [
                        "python3",
                        ROOT / "modular-footer" / "parquet_to_page_modular.py",
                        "--format",
                        footer_format,
                        "--truncate-minmax",
                        "16",
                        source,
                        output,
                    ],
                    check=True,
                )
        base = [
            args.build_dir / "footer_decode_bench",
            jump_table,
            "--sweep",
            modular,
            addendum,
            full,
        ]
        placement = run(base + ["--placement-only"])
        statistics = run(base)
        placement_path = args.results_dir / (name + "-split-placement.csv")
        statistics_path = args.results_dir / (name + "-split-statistics.csv")
        placement_path.write_text(placement)
        statistics_path.write_text(statistics)
        print("wrote {} and {}".format(placement_path, statistics_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
