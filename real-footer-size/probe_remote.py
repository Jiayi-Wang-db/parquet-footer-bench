#!/usr/bin/env python3
"""Read only the eight-byte trailer to report remote Parquet footer sizes."""

import argparse
import json
import re
import struct
import sys
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parent
CONTENT_RANGE = re.compile(r"^bytes (\d+)-(\d+)/(\d+)$")


def probe(url):
    request = urllib.request.Request(
        url,
        headers={
            "Range": "bytes=-8",
            "Accept-Encoding": "identity",
            "User-Agent": "parquet-footer-bench/1",
        },
    )
    with urllib.request.urlopen(request) as response:
        if response.getcode() != 206:
            raise ValueError("server ignored Range (HTTP {})".format(response.getcode()))
        content_range = response.headers.get("Content-Range", "")
        match = CONTENT_RANGE.match(content_range)
        if match is None:
            raise ValueError("invalid Content-Range: {!r}".format(content_range))
        start, end, file_bytes = (int(value) for value in match.groups())
        trailer = response.read(9)
    if end - start + 1 != 8 or end + 1 != file_bytes or len(trailer) != 8:
        raise ValueError("server returned an unexpected byte range")
    if trailer[4:] != b"PAR1":
        raise ValueError("missing trailing PAR1 magic")
    footer_bytes = struct.unpack("<I", trailer[:4])[0]
    if footer_bytes + 12 > file_bytes:
        raise ValueError("footer length exceeds file size")
    return file_bytes, footer_bytes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("names", nargs="*", help="dataset names; default: all")
    parser.add_argument("--manifest", type=Path, default=ROOT / "corpus.json")
    args = parser.parse_args()
    entries = json.loads(args.manifest.read_text())["datasets"]
    by_name = {entry["name"]: entry for entry in entries}
    selected = [by_name[name] for name in args.names] if args.names else entries

    print("{:<32} {:>14} {:>14} {:>10}".format(
        "name", "file bytes", "footer bytes", "footer %"
    ))
    for entry in selected:
        file_bytes, footer_bytes = probe(entry["url"])
        print("{:<32} {:>14,d} {:>14,d} {:>9.4%}".format(
            entry["name"], file_bytes, footer_bytes, footer_bytes / file_bytes
        ))
        if file_bytes != entry["bytes"] or footer_bytes != entry["footer_bytes"]:
            raise ValueError("{} changed from its pinned metadata".format(entry["name"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
