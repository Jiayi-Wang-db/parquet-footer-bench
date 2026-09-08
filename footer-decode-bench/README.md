<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
# footer-decode-bench — footer decoding speed across four layouts

A **self-contained** micro-benchmark (depends on nothing else in this repo). It
times four footer layouts resolving the **same information** for a column
projection: per projected column chunk, `{data_page_offset, total_compressed_size,
null_count, min, max}` — what a scan + row-group-pruning reader needs. No I/O, no
page decode ("plan-only").

The parser is shared and held constant across all four, so the measured
difference is the footer *layout*, not parser quality. min/max are returned as
**spans** (zero-copy views) into each reader's own buffer — the lean readers never
copy or reconstruct stat bytes, so the numbers reflect decode work, not the
allocator.

| layout | what it does | cost |
|---|---|---|
| `standard` | full Thrift materialization — decode the *entire* FileMetaData into an object tree, then read placement+stats off it (models a generated-Thrift reader like parquet-java's). Copies everything; projection-independent. | O(whole footer) |
| `walk` | today's nested footer: walk every row group and column chunk, decode the projected ones, skip the rest. | O(all chunks) |
| `index` | jump table (`FileMetadataFooterIndex.column_chunk_offsets`): seek straight to each projected chunk. | O(projected) |
| `modular` | `ModularFooter`: column-major bit-packed placement + a separate `ColumnStatistics` module (present-index; min/max are prefix+suffix spans). | O(projected) |

## Layout

The code is split into a parser layer and a format layer, with one uniform
interface for the four resolvers:

- **`thrift_codec.h`** — the parser: Thrift-compact `Reader`, bit unpacking, a
  zero-copy `Span`, and a generic value tree. No footer-format knowledge.
- **`footer_formats.h`** — the format decoding: the `Resolver` interface and the
  four implementations (`StandardResolver`, `WalkResolver`, `IndexResolver`,
  `ModularResolver`), each owning its own per-file setup.
- **`footer_decode_bench.cc`** — the driver: builds the resolvers and runs the
  fidelity cross-check, timing, and sweep over them uniformly.

All four are cross-checked to produce identical placement+stats (modular's
segmented min/max compared logically) before any timing.

## Build & run

```sh
c++ -std=c++17 -O2 footer_decode_bench.cc -o footer_decode_bench

# make the two footer variants of any Parquet file:
../jumptable-footer/parquet_to_jumptable input.parquet input.jt.parquet
../modular-footer/parquet_to_modular     input.parquet input.modular

# benchmark (project the first K columns; default K = 1; modular file optional)
./footer_decode_bench input.jt.parquet 1 input.modular
```

## What it shows

hits (105 columns x 226 row groups = 23,730 chunks), project 1, us/op:

```
resolve      projected_us         all_us   proj vs walk
standard        56476.407      58460.343           0.1x
walk             3071.948       3346.048           1.0x
index              27.813       3051.793         110.4x
modular            33.028       3372.619          93.0x
```

The full ladder, and it matches the reference (`hardwood` / Will's footer-parsing
doc) while isolating the format effect:

1. **`standard` ~= 16–18x slower than `walk`** — the *parser-quality* rung.
   Materializing the whole footer into objects is what makes a generated-Thrift
   reader (parquet-java) slow; it has nothing to do with the layout, and it's
   projection-independent (it decodes everything regardless). This reproduces the
   reference's "legacy Thrift vs fast parser" gap.
2. **`walk` pays for the whole footer** — it visits all 23,730 chunks, so it is
   ~100x slower than a seek even at a 1-column projection.
3. **`index` and `modular` are O(projected)** — ~100x faster at a narrow
   projection, converging toward the walk as the projection widens.
4. **`index` and `modular` are close** (both zero-copy). `index` is marginally
   ahead because modular's compact stats encoding must decode a present-index to
   *locate* each min/max, whereas the nested layouts read `min_value`/`max_value`
   as contiguous fields. Placement-only, modular is fastest *and* the footer is
   ~7x smaller (299 KB vs 2.14 MB); with full stats the two are neck and neck.

## Projection sweep

`--sweep` times projected resolution across a geometric range of column counts
(1, 2, 4, … C); `plot_sweep.py` renders it to a log-log SVG:

```sh
./footer_decode_bench input.jt.parquet --sweep input.modular \
    | python3 plot_sweep.py "hits: 105 cols x 226 row groups" > projection_sweep.svg
```

`projection_sweep.svg` (hits) shows the scaling: `standard` and `walk` are flat
lines near the top (projection-independent / O(all chunks)); `index` and `modular`
are straight rising lines (O(projected)) whose gap to the walk fans from ~100x at
a 1-column projection down to ~1x at full projection.

On a **narrow** footer (e.g. yellow_tripdata, 19 cols x 3 rg) all the lean readers
are within a few microseconds — fixed overhead dominates, so narrow footers are
not where footer decoding matters.

## Name resolution: does a persisted schema hash earn its place?

Resolving query column *names* to ordinals is the step *before* placement/stats
resolution, and it has the same shape. `name_resolve_bench` times it two ways on a
footer that carries a persisted name hash — either a jump-table footer
(`SchemaLayout.NameHashTable`) or a modular footer with the optional `SCHEMA_INDEX`
module (`parquet_to_modular --schema-index`); the format is auto-detected:

- **`walk`** — no persisted hash: parse the whole schema (every `SchemaElement`) to
  build name → ordinal, then look up the K queried names. O(all columns).
- **`hash`** — persisted table: FNV-1a-64 probe per queried name, confirming the
  candidate by reconstructing its full dotted path from the schema (per-element
  offsets, plus the parent chain for nested schemas). O(projected).

Both return identical ordinals (cross-checked before timing). `make_wide_footer`
synthesizes flat footers of N named columns to reach the wide-schema regime the real
corpus never hits.

```sh
cmake --build build -j --target make_wide_footer modular_footer_convert name_resolve_bench
./build/make_wide_footer --columns 5000 --row-groups 4 wide.parquet
./build/modular_footer_convert --schema-index wide.parquet wide.si.modular
./build/name_resolve_bench wide.si.modular --sweep | \
  python3 footer-decode-bench/plot_name_resolve.py "queried names (of 5000)" "Name resolution" > sweep.svg
```

Width sweep (resolve 1 name; walk grows O(columns), hash stays flat):

| columns | walk | hash | speedup |
|--------:|------:|------:|--------:|
| 500 | 44 us | 0.09 us | ~475x |
| 5,000 | 439 us | 0.09 us | ~4,800x |
| 20,000 | 1,976 us | 0.13 us | ~15,000x |

The projection sweep (`results/name_resolve_wide5000_ksweep.svg`) shows the walk flat
across K (it always parses the whole schema) and the hash rising with K, crossing over
only near full projection — so the persisted hash wins decisively exactly in the common
case, a selective projection of a wide schema. The `SCHEMA_INDEX` module that backs the
`hash` path costs ~3% of a (lean) footer at 5,000 columns and is written only on demand;
see `../modular-footer/`. `modular_schema_index_check` is a reference reader that
verifies every leaf resolves correctly (flat and nested schemas).

| Sweep | SVG |
|---|---|
| Width (1 name of N) | [SVG](results/name_resolve_width_sweep.svg) |
| Projection (K of 5000) | [SVG](results/name_resolve_wide5000_ksweep.svg) |

## Real corpus sweeps

`run_corpus.py` converts and sweeps all four files from `real-footer-size/corpus.json`, then writes
one CSV and SVG per dataset under `results/`:

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j --target jumptable_footer_convert modular_footer_convert footer_decode_bench
python3 footer-decode-bench/run_corpus.py
```

The checked-in results were measured on one machine and should be used for relative trends, not as
portable absolute latency. The modular inputs intentionally preserve full statistics because the
benchmark verifies that all four layouts reconstruct identical min/max values before timing.

| Dataset | Sweep |
|---|---|
| US Accidents | [SVG](results/us-accidents-00004-of-00007.svg) |
| FineWeb 10BT | [SVG](results/fineweb-10bt-000.svg) |
| Hacker News | [SVG](results/hacker-news-00000-of-00039.svg) |
| Yellow Taxi | [SVG](results/yellow-tripdata-2025-01.svg) |
