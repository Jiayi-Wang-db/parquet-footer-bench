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
  max}` for a column projection across legacy, jump-table, current modular, and page-first
  layouts. `--placement-only` measures the offset-and-size workload from the proposal.
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
| `par1_addendum` | decode mandatory region/row-group DBP streams and projected page arrays; optionally decode row-group stats and flat Column Index. | O(all regions + projected pages) |
| `par3_full` | decode the mandatory region DBP stream and projected page arrays; optionally decode row-range statistics. | O(all regions + projected pages) |

Files: `footer_decode_bench.cc` (driver), `footer_formats.h` (the resolvers),
`plot_sweep.py`, and `run_page_first_split.py`. Placement-only runs cross-check every resolver
against the current footer before timing.

```sh
cd /path/to/parquet-footer-bench
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j --target jumptable_footer_convert modular_footer_convert footer_decode_bench
../build/jumptable_footer_convert input.parquet input.jt.parquet
../build/modular_footer_convert   input.parquet input.modular
python3 ../modular-footer/parquet_to_page_modular.py \
  --format addendum --truncate-minmax 16 input.parquet input.addendum
python3 ../modular-footer/parquet_to_page_modular.py \
  --format full --truncate-minmax 16 input.parquet input.full
../build/footer_decode_bench input.jt.parquet 1 --placement-only \
  input.modular input.addendum input.full
../build/footer_decode_bench input.jt.parquet --sweep --placement-only \
  input.modular input.addendum input.full | \
  python3 plot_sweep.py "hits: 105 cols x 226 rg" > results/projection_sweep.svg
```

The DBP throughput bridge calls the production SIMD decoder in `runtime/dbl/parquet` (`ReadHeader`,
`ReadBlockHeader`, and `DecodeMiniblock32`). The benchmark does not contain a separate DBP decoder.

The `dbp_throughput` tool accepts current page-first footers. Its timed loop uses a preallocated
output buffer and includes DBP page and block header parsing, but excludes file reads, Compact
Thrift parsing, and allocation.

```sh
cmake --build build -j --target dbp_throughput
build/dbp_throughput input.addendum
build/dbp_throughput input.full
```

Across the five measured maps, median throughput over three runs is 1.61-1.65 billion offsets/s,
equivalent to 12.9-13.2 GB/s of decoded `int64_t` output. Encoded-input throughput is 2.40-4.39 GB/s
because the maps have different packed bit widths. See
`results/dbp-region-map-throughput.csv`.

### Adaptive page-first result

The proposal's shape is a flat table with 2,000 columns and 10 row groups. The page-first
comparison uses a real Parquet fixture with that shape so the converter can scan actual pages. Each
operation is calibrated to roughly 200 ms, all files are already in memory, and every resolver is
fidelity-checked before timing. Results below are microseconds per operation on an Intel Xeon 6975P:

| layout | project 1 (µs/op) | project all 2,000 (µs/op) |
|---|---:|---:|
| standard Thrift materialization | 26,098.066 | 27,398.410 |
| footer walk | 2,007.720 | 1,989.003 |
| jump table | 2.084 | 1,830.445 |
| current modular | 1.521 | 571.808 |
| page-first, cold map | 47.889 | 783.746 |
| page-first, retained map | 1.567 | 735.496 |

This reproduces the proposal's projection scaling for the existing layouts. Page-first decode is
essentially tied with current modular for one projected column after the map is retained. With the
runtime SIMD decoder, the mandatory `DELTA_BINARY_PACKED` region-map decode adds roughly 46 µs/op on
this fixture. The warm path is slower at wide projection because it visits every projected page to
reconstruct the legacy chunk-shaped benchmark result, although each page field is directly
addressable. See `results/wide-2000x10-placement.csv`.

hits (105 columns × 226 row groups = 23,730 chunks), project 1, µs/op:

```
resolve      projected_us/op      all_us/op   vs_walk_x
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
- Narrow footers (e.g. yellow_tripdata, 19 columns) sit within a few µs/op across all lean
  readers — fixed overhead dominates, so they are not where decoding matters.

The split benchmark uses files emitted by Raincloud's actual parquet-java target. Run it with:

```sh
python3 footer-decode-bench/run_page_first_split.py
```

One projected column, placement only:

| Dataset | Thrift full | Current modular full | PAR1 addendum full | PAR3 full |
| --- | ---: | ---: | ---: | ---: |
| US Accidents | 71.695 µs/op | 0.786 µs/op | 0.736 µs/op | 3.313 µs/op |
| FineWeb | 518.593 µs/op | 35.475 µs/op | 9.613 µs/op | 25.629 µs/op |
| Hacker News | 52.317 µs/op | 0.789 µs/op | 0.955 µs/op | 2.322 µs/op |
| Yellow Taxi | 53.838 µs/op | 2.178 µs/op | 0.997 µs/op | 3.833 µs/op |

One projected column, placement plus statistics:

| Dataset | Thrift full | Current modular full | PAR1 addendum full | PAR3 full |
| --- | ---: | ---: | ---: | ---: |
| US Accidents | 77.803 µs/op | 2.542 µs/op | 1.630 µs/op | 4.603 µs/op |
| FineWeb | 519.678 µs/op | 35.710 µs/op | 9.943 µs/op | 25.645 µs/op |
| Hacker News | 57.972 µs/op | 2.403 µs/op | 1.700 µs/op | 2.591 µs/op |
| Yellow Taxi | 65.721 µs/op | 6.601 µs/op | 2.776 µs/op | 5.259 µs/op |

Placement-only decodes projected Offset Index for current Thrift/current modular. PAR1 decodes its
projected chunk directory and flat relative Offset Index; PAR3 decodes projected page placement.
The second workload also decodes projected Column Index; current modular and PAR1 decode row-group
statistics, while PAR3 decodes row-range statistics. FineWeb's first column has no page statistics,
so its one-column statistics timing is nearly its placement timing.
Every checked-in `*-split-{placement,statistics}.csv` contains projection sweeps through all
columns. Files are already in memory and each operation is calibrated to roughly 200 ms; interpret
the absolute latency as machine-specific.

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

| columns (count) | walk (µs/op) | hash (µs/op) | speedup (×) |
|--------:|------:|------:|--------:|
| 500 | 44 | 0.09 | ~475 |
| 5,000 | 439 | 0.09 | ~4,800 |
| 20,000 | 1,976 | 0.13 | ~15,000 |

The hash rises with K and only crosses over near full projection, so it wins exactly
in the common case: a selective projection of a wide schema. The `SCHEMA_INDEX` module
costs ~3% of a lean footer at 5,000 columns and is written only on demand
(see `../modular-footer/`). Sweeps:
[width](results/name_resolve_width_sweep.svg) ·
[projection](results/name_resolve_wide5000_ksweep.svg).
