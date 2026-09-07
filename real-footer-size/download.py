#!/usr/bin/env python3
"""Download and verify the real-file Parquet corpus."""

import argparse
import hashlib
import json
import os
import shutil
import sys
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parent
MAGIC = b"PAR1"


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def matches(path, entry):
    if not path.is_file() or path.stat().st_size != entry["bytes"]:
        return False
    with path.open("rb") as stream:
        if stream.read(4) != MAGIC:
            return False
        stream.seek(-4, os.SEEK_END)
        if stream.read(4) != MAGIC:
            return False
    return sha256(path) == entry["sha256"]


def fetch(entry, output_dir, force):
    destination = output_dir / (entry["name"] + ".parquet")
    if not force and matches(destination, entry):
        print("cached  {} ({:,} bytes)".format(destination, destination.stat().st_size))
        return
    temporary = destination.with_suffix(".parquet.part")
    destination.parent.mkdir(parents=True, exist_ok=True)
    request = urllib.request.Request(
        entry["url"], headers={"User-Agent": "parquet-footer-bench/1"}
    )
    print("fetch   {}".format(entry["url"]), flush=True)
    try:
        with urllib.request.urlopen(request) as response, temporary.open("wb") as output:
            shutil.copyfileobj(response, output, length=1024 * 1024)
        temporary.replace(destination)
        if not matches(destination, entry):
            destination.unlink()
            raise ValueError("download does not match pinned size or SHA-256")
    except Exception:
        if temporary.exists():
            temporary.unlink()
        raise
    print("saved   {} ({:,} bytes)".format(destination, destination.stat().st_size))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("names", nargs="*", help="dataset names; default: all")
    parser.add_argument("--manifest", type=Path, default=ROOT / "corpus.json")
    parser.add_argument("--output", type=Path, default=ROOT / "data")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    entries = json.loads(args.manifest.read_text())["datasets"]
    by_name = {entry["name"]: entry for entry in entries}
    unknown = sorted(set(args.names) - set(by_name))
    if unknown:
        parser.error("unknown dataset(s): {}".format(", ".join(unknown)))
    selected = [by_name[name] for name in args.names] if args.names else entries
    for entry in selected:
        fetch(entry, args.output, args.force)
    return 0


if __name__ == "__main__":
    sys.exit(main())
