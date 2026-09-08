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
# footer-decode-bench — footer decoding speed: walk vs. jump table vs. modular

A **self-contained** micro-benchmark (depends on nothing else in this repo). It
times three footer layouts resolving the **same information** for the projected
columns — placement *and* statistics: `{data_page_offset, total_compressed_size,
null_count, min, max}`, i.e. what a scan + row-group-pruning reader needs. The
same vendored Thrift codec is used for all three, so the measured difference is
the footer *layout*, not parser quality:

- **walk** — today's nested footer: walk every row group and every column chunk;
  decode the projected `ColumnMetaData` (placement and `Statistics` live in the
  same struct), skip the rest. Must visit every chunk — **O(all chunks)**.
- **index** — the jump-table footer's `FileMetadataFooterIndex.column_chunk_offsets`:
  seek straight to each projected `ColumnChunk` and decode it (placement + stats
  together). **O(projected)**.
- **modular** — the `ModularFooter`: placement from the column-major bit-packed
  `PlacementModule`, and statistics from a **separate** `ColumnStatistics` module
  (present-index arrays; `min = prefix + suffix`). **O(projected)**, but two modules.

No I/O, no page decode. All three are cross-checked to produce identical
`{offset, size, null_count, min, max}` (the modular min/max are reconstructed from
prefix+suffix) before any timing.

## Build & run

```sh
c++ -std=c++17 -O2 footer_decode_bench.cc -o footer_decode_bench

# make the two footer variants of any Parquet file:
../jumptable-footer/parquet_to_jumptable input.parquet input.jt.parquet
../modular-footer/parquet_to_modular     input.parquet input.modular

# benchmark (project the first K columns; default K = 1; modular file optional)
./footer_decode_bench input.jt.parquet 1 input.modular
```

(The modular reader needs the `MFT1` navigability trailer `parquet_to_modular`
writes — the root directory is written last, and the trailer records its offset.)

## What it shows

On a **wide** footer, resolving placement **+ stats** for a projection:

```
# hits: 105 columns x 226 row groups (23,730 chunks), project 1
# footer size: jump-table 2.14 MB   vs   modular 299 KB
resolve                       us/op
walk    (projected)        3274.223
index   (projected)          38.446   <- 85.2x vs walk
modular (projected)          44.767   <- 73.1x vs walk
walk    (all cols)         8445.647
index   (all cols)         5622.890   <-  1.5x vs walk
modular (all cols)         4554.893   <-  1.9x vs walk
projected walk/full = 0.39
```

1. **The walk pays for the whole footer.** Reading 1 of 105 columns still visits
   all 23,730 chunks, so it is ~85x slower than a seek — even though it can now
   skip *decoding* the unselected columns' statistics (hence projected < full).
2. **index and modular are both O(projected)** and both ~75–85x faster than the
   walk.
3. **With stats required, the jump table edges out modular** (38 vs 45 µs): the
   nested layouts co-locate placement and stats in one struct, while the modular
   footer must open a *second* module (`ColumnStatistics`) to gather stats. This
   is the flip side of the placement-only case — where the modular footer is
   fastest *and* ~7x smaller (299 KB vs 2.14 MB), because it can ignore the fat
   structs entirely. So the modular split wins big when you need placement alone
   (or stats alone), and is a mild cost when you need both together.

A calibration point: the plain C++ **walk** on the unindexed footer lands on the
reference's *hand-optimized fast parser* (`hardwood` / Will's footer-parsing doc),
so the reference's large "fast-parser vs legacy-Thrift" speedup is a parser-quality
artifact of that JVM stack — **not** a format effect. This bench isolates the
format effect by holding the codec constant.

On a **narrow** footer (e.g. yellow_tripdata, 19 cols x 3 rg) all three are within
a few microseconds — fixed overhead dominates, so narrow footers are not where
footer decoding matters.

## Projection sweep

`--sweep` times projected resolution across a geometric range of column counts
(1, 2, 4, … C) and prints CSV; `plot_sweep.py` renders it to a log-log SVG:

```sh
./footer_decode_bench input.jt.parquet --sweep input.modular \
    | python3 plot_sweep.py "hits: 105 cols x 226 row groups" > projection_sweep.svg
```

`projection_sweep.svg` (hits) makes the scaling visible: **walk** is a nearly flat
line near the top (it visits every chunk regardless of projection), while **index**
and **modular** are straight rising lines (O(projected)). The gap is a fan that
opens wide at a 1-column projection (~85x) and closes toward full projection
(~1.5x) — both layouts turn footer decode from O(all columns) into O(projected).

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
benchmark verifies that all three layouts reconstruct identical min/max values before timing.

| Dataset | Sweep |
|---|---|
| US Accidents | [SVG](results/us-accidents-00004-of-00007.svg) |
| FineWeb 10BT | [SVG](results/fineweb-10bt-000.svg) |
| Hacker News | [SVG](results/hacker-news-00000-of-00039.svg) |
| Yellow Taxi | [SVG](results/yellow-tripdata-2025-01.svg) |
