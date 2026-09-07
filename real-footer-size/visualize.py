#!/usr/bin/env python3
"""Generate a normalized SVG comparison of the measured footer variants."""

import argparse
import html
import json
from pathlib import Path

import footer_size


ROOT = Path(__file__).resolve().parent
COLORS = ("#64748b", "#2563eb", "#16a34a")
LABELS = ("Standard", "No path_in_schema", "Prefix + suffix16")


def human_bytes(value):
    if value >= 1024 * 1024:
        return "{:.2f} MiB".format(value / (1024 * 1024))
    return "{:.1f} KiB".format(value / 1024)


def display_name(name):
    names = {
        "us-accidents-00004-of-00007": "US Accidents",
        "fineweb-10bt-000": "FineWeb 10BT",
        "hacker-news-00000-of-00039": "Hacker News",
        "yellow-tripdata-2025-01": "Yellow Taxi",
    }
    return names.get(name, name)


def svg(entries, data_dir, suffix_limit):
    width = 1120
    height = 690
    left = 205
    chart_width = 790
    top = 145
    group_height = 125
    bar_height = 22
    parts = [
        '<svg xmlns="http://www.w3.org/2000/svg" width="{}" height="{}" '
        'viewBox="0 0 {} {}">'.format(width, height, width, height),
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        '<style>text{font-family:system-ui,-apple-system,sans-serif;fill:#172033}'
        '.title{font-size:25px;font-weight:700}.subtitle{font-size:14px;fill:#526071}'
        '.dataset{font-size:16px;font-weight:650}.label{font-size:12px;fill:#526071}'
        '.value{font-size:12px;font-weight:600}.axis{font-size:11px;fill:#64748b}</style>',
        '<text x="40" y="42" class="title">Real Parquet footer size by representation</text>',
        '<text x="40" y="68" class="subtitle">Each dataset is normalized to its standard '
        'footer; labels show measured serialized bytes.</text>',
    ]
    for index, label in enumerate(LABELS):
        x = 40 + index * 205
        parts.append('<rect x="{}" y="91" width="15" height="15" rx="3" fill="{}"/>'.format(
            x, COLORS[index]
        ))
        parts.append('<text x="{}" y="103" class="label">{}</text>'.format(x + 22, label))

    for group, entry in enumerate(entries):
        result = footer_size.measure(
            data_dir / (entry["name"] + ".parquet"), suffix_limit
        )
        standard = result["standard"]
        values = (standard, result["no_path"], result["prefix_suffix"])
        y = top + group * group_height
        parts.append('<text x="40" y="{}" class="dataset">{}</text>'.format(
            y + 25, html.escape(display_name(entry["name"]))
        ))
        for variant, value in enumerate(values):
            bar_y = y + variant * 31
            bar_width = chart_width * value / standard
            parts.append('<rect x="{}" y="{}" width="{}" height="{}" rx="3" '
                         'fill="{}"/>'.format(
                             left, bar_y, bar_width, bar_height, COLORS[variant]
                         ))
            percent = 100 * value / standard
            parts.append('<text x="{}" y="{}" class="value">{} ({:.1f}%)</text>'.format(
                left + bar_width + 8, bar_y + 16, human_bytes(value), percent
            ))
        if group < len(entries) - 1:
            parts.append('<line x1="40" y1="{}" x2="1080" y2="{}" stroke="#e5e7eb"/>'.format(
                y + 105, y + 105
            ))
    parts.append('</svg>')
    return "\n".join(parts) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=ROOT / "corpus.json")
    parser.add_argument("--data", type=Path, default=ROOT / "data")
    parser.add_argument("--output", type=Path, default=ROOT / "footer-size.svg")
    parser.add_argument("--suffix-limit", type=int, default=16)
    args = parser.parse_args()
    entries = json.loads(args.manifest.read_text())["datasets"]
    args.output.write_text(svg(entries, args.data, args.suffix_limit))
    print("wrote {}".format(args.output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
