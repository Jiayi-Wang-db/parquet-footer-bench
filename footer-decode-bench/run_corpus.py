#!/usr/bin/env python3
"""Run and plot projection sweeps for the real-footer-size corpus."""

import argparse
import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
BENCH_DIR = ROOT / "footer-decode-bench"
DATA_DIR = ROOT / "real-footer-size" / "data"
RESULTS_DIR = BENCH_DIR / "results"


def run(command):
    return subprocess.run(
        [str(value) for value in command],
        check=True,
        stdout=subprocess.PIPE,
        universal_newlines=True,
    ).stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, default=ROOT / "build")
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    parser.add_argument("--results-dir", type=Path, default=RESULTS_DIR)
    args = parser.parse_args()

    manifest = json.loads((ROOT / "real-footer-size" / "corpus.json").read_text())
    args.results_dir.mkdir(parents=True, exist_ok=True)
    derived_dir = args.data_dir / "decode-bench"
    derived_dir.mkdir(parents=True, exist_ok=True)
    for dataset in manifest["datasets"]:
        name = dataset["name"]
        source = args.data_dir / (name + ".parquet")
        jump_table = derived_dir / (name + ".jt.parquet")
        modular = derived_dir / (name + ".modular")
        if not jump_table.exists():
            subprocess.run(
                [args.build_dir / "jumptable_footer_convert", source, jump_table],
                check=True,
            )
        if not modular.exists():
            # Lossless statistics are required by the benchmark's fidelity check.
            subprocess.run(
                [args.build_dir / "modular_footer_convert", source, modular],
                check=True,
            )

        csv_text = run([args.build_dir / "footer_decode_bench", jump_table, "--sweep", modular])
        csv_path = args.results_dir / (name + ".csv")
        csv_path.write_text(csv_text)
        svg_text = subprocess.run(
            ["python3", BENCH_DIR / "plot_sweep.py", name],
            check=True,
            input=csv_text,
            stdout=subprocess.PIPE,
            universal_newlines=True,
        ).stdout
        svg_path = args.results_dir / (name + ".svg")
        svg_path.write_text(svg_text)
        print("wrote {} and {}".format(csv_path, svg_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
