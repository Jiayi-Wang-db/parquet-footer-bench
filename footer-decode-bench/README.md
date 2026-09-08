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
# footer-decode-bench — footer decoding speed: walk vs. jump table

A **self-contained** micro-benchmark (depends on nothing else in this repo). It
takes a **jump-table** Parquet footer and times two ways to resolve the projected
columns' chunk placement, with the **same Thrift codec held constant** — so the
measured difference is the *format* effect (random access vs. walking), not
parser quality:

- **walk** — today's footer: to resolve a projection you must walk every row group
  and every column chunk (Thrift structs are self-delimited; no way to seek). A
  projected read skips unselected chunk *bodies* but still pays the walk —
  **O(all chunks)**.
- **index** — use the `FileMetadataFooterIndex.column_chunk_offsets` jump table to
  seek straight to each projected chunk and decode only those — **O(projected)**.

It is "plan-only": resolve each projected chunk's `{offset, size}` (what a reader
hands its fetch scheduler). No I/O, no page decode. Both paths are cross-checked
to produce identical placement before any timing.

## Build & run

```sh
c++ -std=c++17 -O2 footer_decode_bench.cc -o footer_decode_bench

# 1. make a jump-table footer from any Parquet file (see ../jumptable-footer)
../jumptable-footer/parquet_to_jumptable input.parquet input.jt.parquet

# 2. benchmark it (project the first K columns; default K = 1)
./footer_decode_bench input.jt.parquet 1
```

## What it shows

On a **wide** footer the jump table collapses projected resolution, while the
walk cannot exploit projection at all:

```
# hits: 105 columns x 226 row groups (23,730 chunks), project 1
resolve                       us/op
walk   (projected)         2994.157
index  (projected)           27.344   <- 109.5x vs walk
walk   (all cols)          3227.782
index  (all cols)          3341.135   <- 1.0x vs walk
projected walk/full = 0.93  (≈1 means the walk cannot exploit projection)
```

Two findings, both matching the reference (`hardwood` / Will's footer-parsing doc):

1. **The walk cannot exploit projection** — reading 1 of 105 columns costs the
   same as reading all 105 (`walk projected/full ≈ 1`), because every chunk must
   be walked to reach the next. The reference shows the same: legacy `54,320` ≈
   `52,836` µs.
2. **The jump table collapses projected resolution** — `27 µs` vs `2,994 µs` here.

A useful calibration: the plain C++ **walk (~2,994 µs)** lands right on the
reference's *hand-optimized fast parser* on the unindexed footer (~2,953 µs). So
the reference's large "fast-parser vs legacy-Thrift" speedup is a parser-quality
artifact of that JVM stack — **not** a format effect. The format effect is the
index factor, which this bench isolates by holding the codec constant.

On a **narrow** footer (e.g. yellow_tripdata, 19 cols x 3 rg) the index is still
faster (~20x) but the absolute times are microseconds — fixed overhead dominates,
so narrow footers are not where footer decoding matters.
