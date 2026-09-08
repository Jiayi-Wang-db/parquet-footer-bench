#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Render a name-resolution chart (SVG, log-log) from a CSV whose first
# column is the swept variable and whose remaining columns are walk_us /
# hash_us. Dependency-free. Used for both:
#   name_resolve_bench wide.jt.parquet --sweep   (x = queried names)
#   the width sweep                              (x = schema columns)
#
#   python3 plot_name_resolve.py "schema columns" \
#       "Name resolution: 1 of N columns" < width_sweep.csv > out.svg

import html
import math
import sys

xlabel = sys.argv[1] if len(sys.argv) > 1 else "x"
title = sys.argv[2] if len(sys.argv) > 2 else "Name resolution: walk vs persisted hash"

lines = [ln.strip() for ln in sys.stdin if ln.strip()]
rows = [ln.split(",") for ln in lines[1:]]
xs = [int(r[0]) for r in rows]
header = lines[0].split(",")
palette = {"walk": "#64748b", "hash": "#9333ea"}
cols = []
for j, h in enumerate(header[1:], start=1):
    label = h[:-3] if h.endswith("_us") else h
    disp = "walk schema (no hash)" if label == "walk" else "persisted name hash"
    cols.append((disp, palette.get(label, "#0891b2"), [float(r[j]) for r in rows]))

W, H = 900, 560
L, R, T, B = 90, 260, 70, 70
PW, PH = W - L - R, H - T - B

xmin, xmax = min(xs), max(xs)
allv = [v for _, _, vals in cols for v in vals if v > 0]
ylo = 10 ** math.floor(math.log10(min(allv)))
yhi = 10 ** math.ceil(math.log10(max(allv)))
lxmin, lxmax = math.log10(xmin), math.log10(xmax)
lylo, lyhi = math.log10(ylo), math.log10(yhi)

def px(x):
    return L + (math.log10(x) - lxmin) / (lxmax - lxmin) * PW if lxmax > lxmin else L
def py(v):
    return T + (1 - (math.log10(v) - lylo) / (lyhi - lylo)) * PH

p = []
p.append('<svg xmlns="http://www.w3.org/2000/svg" width="{}" height="{}" viewBox="0 0 {} {}">'.format(W, H, W, H))
p.append('<rect width="100%" height="100%" fill="#ffffff"/>')
p.append('<style>text{font-family:system-ui,-apple-system,sans-serif;fill:#172033}'
         '.title{font-size:20px;font-weight:700}.sub{font-size:13px;fill:#526071}'
         '.ax{font-size:12px;fill:#526071}.leg{font-size:13px;font-weight:600}</style>')
p.append('<text class="title" x="{}" y="30">{}</text>'.format(L, html.escape(title)))
p.append('<text class="sub" x="{}" y="50">us/op (log-log) — resolve query column names to ordinals</text>'.format(L))

v = ylo
while v <= yhi + 1e-9:
    y = py(v)
    p.append('<line x1="{}" y1="{:.1f}" x2="{}" y2="{:.1f}" stroke="#e5e9f0"/>'.format(L, y, L + PW, y))
    lab = ("{:g} ms".format(v / 1000.0)) if v >= 1000 else ("{:g} us".format(v))
    p.append('<text class="ax" x="{}" y="{:.1f}" text-anchor="end">{}</text>'.format(L - 8, y + 4, lab))
    v *= 10

for x in xs:
    xx = px(x)
    p.append('<line x1="{:.1f}" y1="{}" x2="{:.1f}" y2="{}" stroke="#eef1f6"/>'.format(xx, T, xx, T + PH))
    p.append('<text class="ax" x="{:.1f}" y="{}" text-anchor="middle">{}</text>'.format(xx, T + PH + 20, x))
p.append('<text class="ax" x="{:.1f}" y="{}" text-anchor="middle">{}</text>'.format(L + PW / 2, T + PH + 46, html.escape(xlabel)))

p.append('<line x1="{}" y1="{}" x2="{}" y2="{}" stroke="#94a3b8"/>'.format(L, T, L, T + PH))
p.append('<line x1="{}" y1="{}" x2="{}" y2="{}" stroke="#94a3b8"/>'.format(L, T + PH, L + PW, T + PH))

for name, color, vals in cols:
    pts = " ".join("{:.1f},{:.1f}".format(px(x), py(v)) for x, v in zip(xs, vals) if v > 0)
    p.append('<polyline points="{}" fill="none" stroke="{}" stroke-width="2.5"/>'.format(pts, color))
    for x, v in zip(xs, vals):
        if v > 0:
            p.append('<circle cx="{:.1f}" cy="{:.1f}" r="3.2" fill="{}"/>'.format(px(x), py(v), color))

lx = L + PW + 30
ly = T + 10
p.append('<text class="ax" x="{}" y="{}">resolve  (us/op @ {} / {})</text>'.format(lx, ly - 14, xmin, xmax))
for name, color, vals in cols:
    p.append('<line x1="{}" y1="{:.1f}" x2="{}" y2="{:.1f}" stroke="{}" stroke-width="3"/>'.format(lx, ly, lx + 26, ly, color))
    p.append('<circle cx="{}" cy="{:.1f}" r="3.2" fill="{}"/>'.format(lx + 13, ly, color))
    p.append('<text class="leg" x="{}" y="{:.1f}" fill="{}">{}</text>'.format(lx + 34, ly + 4, color, html.escape(name)))
    p.append('<text class="ax" x="{}" y="{:.1f}">{:.2f} → {:.2f} us</text>'.format(lx + 34, ly + 20, vals[0], vals[-1]))
    ly += 46

p.append('</svg>')
sys.stdout.write("\n".join(p) + "\n")
