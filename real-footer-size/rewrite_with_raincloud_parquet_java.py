#!/usr/bin/env python3
"""Rewrite the pinned corpus through Raincloud's parquet@java sidecar."""

import argparse
import json
import subprocess
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parents[1]


def write_canonical(source, destination):
    parquet = pq.ParquetFile(source)
    options = pa.ipc.IpcWriteOptions(compression="zstd")
    with pa.OSFile(str(destination), "wb") as sink:
        with pa.ipc.new_file(sink, parquet.schema_arrow, options=options) as writer:
            for batch in parquet.iter_batches(batch_size=65536):
                writer.write_batch(batch)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--writer", type=Path, required=True)
    parser.add_argument(
        "--source", type=Path, default=ROOT / "real-footer-size" / "data"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "real-footer-size" / "data" / "raincloud-parquet-java",
    )
    parser.add_argument("names", nargs="*")
    args = parser.parse_args()

    entries = json.loads(
        (ROOT / "real-footer-size" / "corpus.json").read_text()
    )["datasets"]
    by_name = {entry["name"]: entry for entry in entries}
    unknown = sorted(set(args.names) - set(by_name))
    if unknown:
        parser.error("unknown dataset(s): {}".format(", ".join(unknown)))
    selected = [by_name[name] for name in args.names] if args.names else entries
    args.output.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="raincloud-parquet-java-") as temp_name:
        temp = Path(temp_name)
        for entry in selected:
            name = entry["name"]
            source = args.source / (name + ".parquet")
            output = args.output / (name + ".parquet")
            report = args.output / (name + ".report.json")
            if output.exists() or report.exists():
                raise FileExistsError("refusing to overwrite output for {}".format(name))
            canonical = temp / (name + ".arrow.zstd")
            write_canonical(source, canonical)
            subprocess.run(
                [
                    str(args.writer),
                    "--input",
                    str(canonical),
                    "--output",
                    str(output),
                    "--report",
                    str(report),
                ],
                check=True,
            )
            result = json.loads(report.read_text())
            if not result.get("roundtrip"):
                raise ValueError("Raincloud self-verification failed for {}".format(name))
            print("wrote {}".format(output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
