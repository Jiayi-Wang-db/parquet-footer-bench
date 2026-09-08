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
# footer-decode-bench

Two self-contained, plan-only micro-benchmarks (no I/O, no page decode). Outputs
(CSV + SVG) are written to [`results/`](results/).

- **decode** — resolve `{data_page_offset, total_compressed_size, null_count, min,
  max}` for a column projection across four footer layouts.
- **name-resolve** — resolve column *names* to ordinals, walking the schema vs. a
  persisted name hash.

The Thrift parser (`thrift_codec.h`) is shared and held constant, so measured gaps
are the *layout*, not parser quality. min/max are returned as zero-copy spans, so
numbers reflect decode work, not the allocator.

## 1. Decode bench

| layout | what it does | cost |
|---|---|---|
| `standard` | materialize the entire FileMetaData into an object tree, then read off it (models parquet-java). | O(whole footer) |
| `walk` | today's nested footer: visit every column chunk, decode the projected ones. | O(all chunks) |
| `index` | jump table: seek straight to each projected chunk. | O(projected) |
| `modular` | `ModularFooter`: column-major bit-packed placement + a separate `ColumnStatistics` module. | O(projected) |

Files: `footer_decode_bench.cc` (driver), `footer_formats.h` (the four resolvers),
`plot_sweep.py`, `run_corpus.py`. All four are cross-checked to produce identical
placement+stats before timing.

```sh
cmake --build build -j --target jumptable_footer_convert modular_footer_convert footer_decode_bench
../build/jumptable_footer_convert input.parquet input.jt.parquet
../build/modular_footer_convert   input.parquet input.modular
../build/footer_decode_bench      input.jt.parquet 1 input.modular     # project first K; default 1
../build/footer_decode_bench      input.jt.parquet --sweep input.modular | \
  python3 plot_sweep.py "hits: 105 cols x 226 rg" > results/projection_sweep.svg
```

hits (105 cols x 226 rg = 23,730 chunks), project 1, us/op:

```
resolve      projected_us         all_us   proj vs walk
standard        56476.407      58460.343           0.1x
walk             3071.948       3346.048           1.0x
index              27.813       3051.793         110.4x
modular            33.028       3372.619          93.0x
```

- `standard` is ~16-18x slower than `walk` — the parser-quality rung (matches the
  `hardwood` / Will's footer-parsing reference), projection-independent.
- `walk` visits all chunks, so it is ~100x a seek even at 1 column.
- `index` and `modular` are O(projected) and close (both zero-copy); `index` is
  marginally ahead because modular decodes a present-index to locate each min/max.
  Placement-only, modular is fastest and ~7x smaller (299 KB vs 2.14 MB).
- Narrow footers (e.g. yellow_tripdata, 19 cols) sit within a few us across all lean
  readers — fixed overhead dominates, so they are not where decoding matters.

Corpus sweeps ([`run_corpus.py`](run_corpus.py), from `real-footer-size/corpus.json`):
[projection_sweep](results/projection_sweep.svg) (hits) ·
[us-accidents](results/us-accidents-00004-of-00007.svg) ·
[fineweb](results/fineweb-10bt-000.svg) ·
[hacker-news](results/hacker-news-00000-of-00039.svg) ·
[yellow-taxi](results/yellow-tripdata-2025-01.svg).
Checked-in numbers are one machine's — read them as trends, not absolute latency.

## 2. Name-resolve bench

Resolve query column names to ordinals two ways on a footer with a persisted name
hash (jump-table `SchemaLayout.NameHashTable`, or a modular `SCHEMA_INDEX` module via
`parquet_to_modular --schema-index`; auto-detected):

- `walk` — parse the whole schema, build name -> ordinal, look up K. O(all columns).
- `hash` — FNV-1a-64 probe per name, confirm by reconstructing its full path
  (per-element offsets + parent chain for nested schemas). O(projected).

Both return identical ordinals (cross-checked). Files: `name_resolve_bench.cc`,
`make_wide_footer.cc` (wide fixture generator), `plot_name_resolve.py`.
`../modular-footer/schema_index_check.cc` verifies every leaf resolves (flat + nested).

```sh
cmake --build build -j --target make_wide_footer modular_footer_convert name_resolve_bench
../build/make_wide_footer --columns 5000 --row-groups 4 wide.parquet
../build/modular_footer_convert --schema-index wide.parquet wide.si.modular
../build/name_resolve_bench wide.si.modular --sweep | \
  python3 plot_name_resolve.py "queried names (of 5000)" "Name resolution" > results/name_resolve.svg
```

Resolve 1 name; walk grows O(columns), hash stays flat:

| columns | walk | hash | speedup |
|--------:|------:|------:|--------:|
| 500 | 44 us | 0.09 us | ~475x |
| 5,000 | 439 us | 0.09 us | ~4,800x |
| 20,000 | 1,976 us | 0.13 us | ~15,000x |

The hash rises with K and only crosses over near full projection, so it wins exactly
in the common case: a selective projection of a wide schema. The `SCHEMA_INDEX` module
costs ~3% of a lean footer at 5,000 columns and is written only on demand
(see `../modular-footer/`). Sweeps:
[width](results/name_resolve_width_sweep.svg) ·
[projection](results/name_resolve_wide5000_ksweep.svg).
