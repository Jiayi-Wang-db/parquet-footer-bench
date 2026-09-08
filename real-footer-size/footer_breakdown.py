#!/usr/bin/env python3
"""Generate a stacked OSS-versus-modular footer component graph."""

import argparse
import csv
import html
import json
import sys
from pathlib import Path

import footer_size


ROOT = Path(__file__).resolve().parent
COMPONENTS = (
    "path_in_schema",
    "statistics",
    "schema",
    "placement",
    "key_value_metadata",
    "other",
)
COLORS = {
    "path_in_schema": "#dc2626",
    "statistics": "#16a34a",
    "schema": "#2563eb",
    "placement": "#f59e0b",
    "key_value_metadata": "#8b5cf6",
    "other": "#94a3b8",
}
LABELS = {
    "path_in_schema": "path_in_schema",
    "statistics": "Row-group stats",
    "schema": "Schema",
    "placement": "Placement",
    "key_value_metadata": "Key/value metadata",
    "other": "Other/framing",
}


def display_name(name):
    return {
        "us-accidents-00004-of-00007": "US Accidents",
        "fineweb-10bt-000": "FineWeb 10BT",
        "hacker-news-00000-of-00039": "Hacker News",
        "yellow-tripdata-2025-01": "Yellow Taxi",
    }.get(name, name)


def human_bytes(value):
    if value >= 1024 * 1024:
        return "{:.2f} MiB".format(value / (1024 * 1024))
    if value >= 1024:
        return "{:.1f} KiB".format(value / 1024)
    return "{} B".format(value)


def measurements(entries, data_dir, converter, suffix_limit):
    rows = []
    for entry in entries:
        path = data_dir / (entry["name"] + ".parquet")
        rows.append((
            entry,
            footer_size.oss_breakdown(path),
            footer_size.modular_breakdown(path, converter, suffix_limit),
        ))
    return rows


def svg(rows, suffix_limit):
    width = 1160
    height = 830
    label_x = 40
    left = 220
    chart_width = 790
    top = 180
    group_height = 150
    bar_height = 28
    parts = [
        '<svg xmlns="http://www.w3.org/2000/svg" width="{}" height="{}" '
        'viewBox="0 0 {} {}">'.format(width, height, width, height),
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        '<style>text{font-family:system-ui,-apple-system,sans-serif;fill:#172033}'
        '.title{font-size:25px;font-weight:700}.subtitle{font-size:14px;fill:#526071}'
        '.dataset{font-size:16px;font-weight:650}.variant{font-size:12px;fill:#526071}'
        '.legend{font-size:13px;fill:#334155}.inside{font-size:11px;font-weight:650;fill:#fff}'
        '.total{font-size:12px;font-weight:650;fill:#334155}</style>',
        '<text x="40" y="42" class="title">What occupies each real Parquet footer?</text>',
        '<text x="40" y="68" class="subtitle">OSS original versus modular footer with '
        'min/max suffixes capped at {} bytes; each dataset uses its own OSS scale.</text>'.format(
            suffix_limit
        ),
    ]
    for index, component in enumerate(COMPONENTS):
        row = index // 3
        column = index % 3
        x = 40 + column * 360
        y = 91 + row * 28
        parts.append('<rect x="{}" y="{}" width="16" height="16" rx="3" fill="{}"/>'.format(
            x, y, COLORS[component]
        ))
        parts.append('<text x="{}" y="{}" class="legend">{}</text>'.format(
            x + 23, y + 13, LABELS[component]
        ))

    for group, (entry, oss, modular) in enumerate(rows):
        y = top + group * group_height
        parts.append('<text x="{}" y="{}" class="dataset">{}</text>'.format(
            label_x, y - 17, html.escape(display_name(entry["name"]))
        ))
        for variant_index, (variant, values) in enumerate((("OSS", oss), ("Modular", modular))):
            bar_y = y + variant_index * 44
            parts.append('<text x="{}" y="{}" class="variant">{}</text>'.format(
                left - 72, bar_y + 19, variant
            ))
            offset = left
            for component in COMPONENTS:
                value = values[component]
                segment_width = chart_width * value / oss["total"]
                parts.append('<rect x="{}" y="{}" width="{}" height="{}" fill="{}"/>'.format(
                    offset, bar_y, segment_width, bar_height, COLORS[component]
                ))
                if segment_width >= 68:
                    parts.append('<text x="{}" y="{}" class="inside">{}</text>'.format(
                        offset + 6, bar_y + 19, human_bytes(value)
                    ))
                offset += segment_width
            parts.append('<text x="{}" y="{}" class="total">{} ({:.1f}%)</text>'.format(
                offset + 8,
                bar_y + 19,
                human_bytes(values["total"]),
                100 * values["total"] / oss["total"],
            ))
        if group < len(rows) - 1:
            parts.append('<line x1="40" y1="{}" x2="1120" y2="{}" '
                         'stroke="#e5e7eb"/>'.format(y + 105, y + 105))
    parts.append('</svg>')
    return "\n".join(parts) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=ROOT / "corpus.json")
    parser.add_argument("--data", type=Path, default=ROOT / "data")
    parser.add_argument("--output", type=Path, default=ROOT / "footer-breakdown.svg")
    parser.add_argument("--suffix-limit", type=int, default=16)
    parser.add_argument(
        "--modular-converter",
        type=Path,
        default=ROOT.parent / "build" / "modular_footer_convert",
    )
    parser.add_argument("--csv", action="store_true")
    args = parser.parse_args()
    entries = json.loads(args.manifest.read_text())["datasets"]
    rows = measurements(entries, args.data, args.modular_converter, args.suffix_limit)
    if args.csv:
        writer = csv.writer(sys.stdout)
        writer.writerow(("name", "representation") + COMPONENTS + ("total",))
        for entry, oss, modular in rows:
            for representation, values in (("oss", oss), ("modular", modular)):
                writer.writerow(
                    (entry["name"], representation)
                    + tuple(values[name] for name in COMPONENTS)
                    + (values["total"],)
                )
    else:
        args.output.write_text(svg(rows, args.suffix_limit))
        print("wrote {}".format(args.output))
    return 0


if __name__ == "__main__":
    sys.exit(main())
