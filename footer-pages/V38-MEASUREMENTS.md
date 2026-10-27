# V38 size and decoding measurements

Fresh comparison of current Parquet Compact Thrift, the modular footer, and V38. V38 stores direct chunk offsets/lengths, a dictionary offset/length table, nullable chunk dictionary IDs, and shared cumulative-end indexes. Both hierarchy indexes omit the fixed leading zero; relative page offsets retain one value per page, including each chunk’s first zero.

All metadata is encoded without Zstd. Size is the complete retained metadata, including schema, directories, page headers, required external indexes, and trailer. Integer and dictionary encodings remain enabled. Sizes and decoding timings cover all 12 parquet-java files described in [EXPERIMENT-RESULTS.md](EXPERIMENT-RESULTS.md#2-java-written-inputs).

## Core size

| Dataset | Current bytes | Modular bytes | V38 bytes | V38 − V35 bytes |
|---|---:|---:|---:|---:|
| US Accidents | 3,963 | 1,937 | 2,099 | — |
| FineWeb | 6,209 | 1,966 | 2,359 | — |
| Hacker News | 1,990 | 936 | 1,314 | — |
| NYC Taxi | 2,042 | 1,034 | 1,371 | — |
| UCI Adult | 1,370 | 728 | 1,061 | +0 |
| Household Power | 935 | 540 | 912 | +0 |
| Kepler Search | 13,307 | 5,850 | 5,405 | +0 |
| AMPds | 913 | 520 | 873 | +0 |
| ClickBench | 510,422 | 151,028 | 119,001 | +1 |
| BI-USCensus | 750,267 | 226,675 | 185,889 | +0 |
| BI-Wins | 333,040 | 106,014 | 88,590 | +0 |
| Kepler Time Series | 221,661 | 91,407 | 60,084 | +0 |
| **Total** | **1,846,119** | **588,635** | **468,958** | **+1 (8 matched)** |

Removing two logical values does not guarantee fewer encoded bytes. For the eight matched V35 datasets, V38 re-encodes only the affected hierarchy streams; all other streams match V35 byte-for-byte. Changed first values and encoding parameters can offset the removed zeros. A dash means no V35 measurement.

The four original shards are converted from their saved V21 artifacts using the same V38 layout, direct-location encoding sweep, and column-aligned metadata page policy. Core/chunk-statistics scopes omit the optional page index. Statistics value streams are preserved byte-for-byte; their row-range tags and shared indexes are updated.

FineWeb has no page statistics for one column. Its page-statistics set uses the supported ARBITRARY form to preserve that sparse coverage; complete chunk/page sets reuse the shared indexes.

## Size at every scope

### Core

| Dataset | Current Parquet Thrift | Modular footer | V38 |
|---|---:|---:|---:|
| US Accidents | 3,963 | 1,937 | 2,099 |
| FineWeb | 6,209 | 1,966 | 2,359 |
| Hacker News | 1,990 | 936 | 1,314 |
| NYC Taxi | 2,042 | 1,034 | 1,371 |
| UCI Adult | 1,370 | 728 | 1,061 |
| Household Power | 935 | 540 | 912 |
| Kepler Search | 13,307 | 5,850 | 5,405 |
| AMPds | 913 | 520 | 873 |
| ClickBench | 510,422 | 151,028 | 119,001 |
| BI-USCensus | 750,267 | 226,675 | 185,889 |
| BI-Wins | 333,040 | 106,014 | 88,590 |
| Kepler Time Series | 221,661 | 91,407 | 60,084 |
| **Total** | **1,846,119** | **588,635** | **468,958** |

### Core + chunk statistics

| Dataset | Current Parquet Thrift | Modular footer | V38 |
|---|---:|---:|---:|
| US Accidents | 6,030 | 4,846 | 3,714 |
| FineWeb | 23,553 | 17,878 | 17,690 |
| Hacker News | 3,323 | 2,281 | 2,324 |
| NYC Taxi | 2,850 | 2,004 | 1,838 |
| UCI Adult | 1,666 | 1,431 | 1,454 |
| Household Power | 1,119 | 944 | 1,251 |
| Kepler Search | 19,281 | 12,922 | 7,937 |
| AMPds | 1,273 | 1,085 | 1,187 |
| ClickBench | 734,357 | 262,202 | 192,476 |
| BI-USCensus | 1,090,768 | 371,313 | 224,400 |
| BI-Wins | 500,148 | 197,470 | 109,519 |
| Kepler Time Series | 362,345 | 246,575 | 85,501 |
| **Total** | **2,746,713** | **1,120,951** | **649,291** |

### Core + chunk statistics + page locations

| Dataset | Current Parquet Thrift | Modular footer | V38 |
|---|---:|---:|---:|
| US Accidents | 41,612 | 25,854 | 14,453 |
| FineWeb | 76,446 | 49,437 | 32,203 |
| Hacker News | 17,638 | 10,946 | 7,847 |
| NYC Taxi | 51,741 | 29,105 | 14,832 |
| UCI Adult | 2,319 | 2,099 | 1,724 |
| Household Power | 13,917 | 7,732 | 5,297 |
| Kepler Search | 22,498 | 17,467 | 8,071 |
| AMPds | 10,089 | 5,974 | 3,966 |
| ClickBench | 8,497,329 | 4,898,177 | 2,196,131 |
| BI-USCensus | 4,701,379 | 2,581,306 | 1,039,989 |
| BI-Wins | 1,513,060 | 856,156 | 330,195 |
| Kepler Time Series | 432,650 | 346,199 | 85,700 |
| **Total** | **15,380,678** | **8,830,452** | **3,740,408** |

### Core + chunk statistics + page locations + page statistics

| Dataset | Current Parquet Thrift | Modular footer | V38 |
|---|---:|---:|---:|
| US Accidents | 101,244 | 79,147 | 48,251 |
| FineWeb | 145,670 | 112,171 | 84,197 |
| Hacker News | 140,991 | 132,831 | 122,836 |
| NYC Taxi | 113,762 | 82,850 | 34,590 |
| UCI Adult | 3,313 | 3,354 | 2,346 |
| Household Power | 27,324 | 19,596 | 13,135 |
| Kepler Search | 28,443 | 25,952 | 10,640 |
| AMPds | 20,541 | 14,758 | 8,990 |
| ClickBench | 21,868,253 | 16,887,807 | 9,794,399 |
| BI-USCensus | 7,986,845 | 5,564,773 | 1,511,418 |
| BI-Wins | 2,563,144 | 1,819,241 | 493,400 |
| Kepler Time Series | 554,166 | 530,149 | 111,111 |
| **Total** | **33,553,696** | **25,272,629** | **12,235,313** |

## Decoding by query

Times are microseconds per query. Each cell is **one column / all columns**. One-column values are the median over nine selected positions, after taking each position’s median across three fresh processes. Every process runs seven shuffled trials per case. All three formats are interleaved on CPU 16 (Intel Xeon Platinum 8375C); input bytes are resident, and each query constructs a new reader. The original eight-dataset timings are retained; the four original shards were timed in three additional processes using the unchanged executable and protocol.

### Chunk metadata

| Dataset | Current Parquet Thrift | Modular footer | V38 |
|---|---:|---:|---:|
| US Accidents | 90.49 / 100.61 | 18.48 / 22.82 | 5.20 / 19.80 |
| FineWeb | 61.08 / 80.33 | 5.69 / 13.66 | 6.83 / 20.16 |
| Hacker News | 35.59 / 41.23 | 8.06 / 10.43 | 5.43 / 12.20 |
| NYC Taxi | 40.49 / 45.10 | 8.96 / 10.95 | 4.56 / 10.97 |
| UCI Adult | 28.99 / 32.69 | 8.37 / 9.87 | 4.64 / 9.53 |
| Household Power | 18.91 / 20.68 | 4.69 / 5.54 | 4.28 / 7.25 |
| Kepler Search | 304.01 / 337.88 | 57.73 / 73.28 | 8.29 / 57.75 |
| AMPds | 22.59 / 25.04 | 5.71 / 6.89 | 4.52 / 8.34 |
| ClickBench | 3,647.20 / 5,362.82 | 68.71 / 767.42 | 65.82 / 1,373.65 |
| BI-USCensus | 5,948.85 / 8,551.72 | 300.17 / 1,441.27 | 56.37 / 2,050.25 |
| BI-Wins | 3,154.57 / 4,369.26 | 331.13 / 857.37 | 45.26 / 918.10 |
| Kepler Time Series | 5,524.50 / 6,122.29 | 969.08 / 1,252.68 | 54.45 / 1,000.27 |

### Chunk statistics

| Dataset | Current Parquet Thrift | Modular footer | V38 |
|---|---:|---:|---:|
| US Accidents | 95.98 / 125.66 | 19.14 / 37.07 | 6.94 / 20.83 |
| FineWeb | 83.49 / 160.16 | 9.14 / 47.94 | 11.68 / 39.30 |
| Hacker News | 39.85 / 56.47 | 8.76 / 16.83 | 6.67 / 13.10 |
| NYC Taxi | 42.85 / 54.57 | 9.53 / 16.66 | 5.42 / 11.52 |
| UCI Adult | 31.19 / 40.49 | 8.94 / 14.19 | 6.02 / 10.29 |
| Household Power | 20.05 / 25.88 | 5.14 / 8.16 | 5.86 / 8.77 |
| Kepler Search | 321.52 / 413.07 | 58.33 / 110.11 | 9.47 / 50.17 |
| AMPds | 24.09 / 31.38 | 6.28 / 10.41 | 5.11 / 8.05 |
| ClickBench | 4,528.19 / 9,150.52 | 74.61 / 1,467.76 | 59.75 / 841.47 |
| BI-USCensus | 7,156.08 / 14,133.80 | 304.20 / 2,450.77 | 75.06 / 1,037.32 |
| BI-Wins | 3,709.84 / 6,798.86 | 335.35 / 1,389.92 | 45.14 / 603.07 |
| Kepler Time Series | 5,857.33 / 7,648.12 | 971.57 / 2,109.15 | 76.62 / 778.84 |

### Chunk metadata + page locations

| Dataset | Current Parquet Thrift | Modular footer | V38 |
|---|---:|---:|---:|
| US Accidents | 101.04 / 302.55 | 21.40 / 149.66 | 15.07 / 147.97 |
| FineWeb | 92.64 / 376.67 | 11.59 / 194.05 | 19.68 / 166.32 |
| Hacker News | 44.18 / 127.26 | 11.15 / 62.68 | 11.46 / 57.31 |
| NYC Taxi | 53.52 / 258.53 | 16.17 / 148.21 | 20.49 / 162.84 |
| UCI Adult | 32.11 / 49.99 | 9.04 / 17.45 | 7.72 / 17.71 |
| Household Power | 26.66 / 81.00 | 9.17 / 44.56 | 13.64 / 52.40 |
| Kepler Search | 326.06 / 482.14 | 58.23 / 140.57 | 10.47 / 95.06 |
| AMPds | 27.86 / 73.57 | 8.40 / 36.94 | 11.05 / 42.55 |
| ClickBench | 5,021.06 / 45,670.40 | 310.79 / 26,810.80 | 298.26 / 21,339.50 |
| BI-USCensus | 7,511.31 / 35,473.40 | 332.08 / 17,188.10 | 110.04 / 12,906.70 |
| BI-Wins | 3,857.56 / 13,428.50 | 340.94 / 5,904.12 | 74.21 / 4,320.37 |
| Kepler Time Series | 5,927.22 / 9,074.75 | 968.84 / 2,542.47 | 59.24 / 1,748.64 |

### Chunk + page statistics

| Dataset | Current Parquet Thrift | Modular footer | V38 |
|---|---:|---:|---:|
| US Accidents | 110.42 / 795.23 | 30.96 / 613.72 | 33.65 / 340.94 |
| FineWeb | 120.53 / 584.78 | 27.30 / 356.59 | 36.85 / 214.07 |
| Hacker News | 53.85 / 460.84 | 19.36 / 363.15 | 18.99 / 274.62 |
| NYC Taxi | 72.51 / 620.85 | 39.53 / 613.07 | 36.50 / 305.11 |
| UCI Adult | 34.96 / 84.65 | 10.46 / 32.92 | 12.07 / 26.21 |
| Household Power | 40.57 / 198.84 | 25.98 / 188.28 | 30.93 / 118.46 |
| Kepler Search | 333.23 / 765.90 | 59.55 / 236.42 | 14.76 / 104.26 |
| AMPds | 35.94 / 157.91 | 16.04 / 119.98 | 17.80 / 69.20 |
| ClickBench | 5,934.74 / 132,204.00 | 983.43 / 110,325.00 | 570.01 / 121,880.00 |
| BI-USCensus | 7,839.25 / 78,071.50 | 398.75 / 49,487.90 | 252.77 / 21,116.10 |
| BI-Wins | 4,002.30 / 28,874.80 | 359.79 / 15,954.90 | 177.64 / 6,879.32 |
| Kepler Time Series | 6,006.29 / 14,106.80 | 972.46 / 4,623.39 | 92.23 / 1,644.44 |

For US Accidents, FineWeb, Hacker News, and NYC Taxi, V38 takes 24–50% less time than modular for all-column chunk/page statistics. Selective reads are mixed; FineWeb’s one-column queries take 20–70% more time across the four projections.

V38 is faster than modular for all four ClickBench one-column projections. All-column results are mixed: chunk metadata takes 79% more time and combined chunk/page statistics 10% more time, while chunk statistics and chunk metadata with page locations are faster. These results do not establish a universal decoding advantage.

## Scope and validation

Each projection constructs and checksums the same logical output. Chunk metadata includes data start/end, dictionary start, codec, value count, uncompressed size, and dictionary-use flag; the page projection adds byte extents and first-row positions. Statistics projections return row starts, min/max bounds, and pruning flags. Dictionary lengths and row-group byte hints are stored in the artifacts but are not part of the timed output. Exact null counts, encryption, and cold-storage latency are not benchmarked.

Release and ASan/UBSan each pass **2,508 cross-format queries** against the independent source oracle and **88 fixture checks**. Fixtures cover unequal/empty columns, zero-page chunks, same-column dictionary reuse, absent dictionaries, zero-column files, chunk/page/arbitrary statistics, missing bounds, statistics without page offsets, and malformed indexes/locations/statistics. Output hashes agree across formats. The real corpus uses regular row-group-aligned chunks; ragged layouts are correctness fixtures, not timing workloads.

The four original shards pass another **960 cross-format queries** in both release and ASan/UBSan, using the unchanged native executable. All 16 additional V38 artifacts are parsed through the current Thrift definition and checked against source chunk/dictionary extents; retained statistics streams are verified byte-for-byte. Each timing process also validates its selected projections against the source oracle before measuring.

These are prototype readers using the existing external codec harness, including repository-local experimental encodings. They are not a production implementation or an interoperable public encoding profile. Selective validation checks the accessed ranges; preparation and full validation check the complete source outputs.

## Reproduction

The checked-in writer/orchestrator is [v38_benchmark.py](v38_benchmark.py); native V38 addressing is in [v38_reader.h](v38_reader.h), and the matched driver/fixtures are in [v38_decode.cc](v38_decode.cc). This run reuses the saved V35 artifacts, frozen `work/refined-hierarchy` reader support/build commands, `work/list-statistics/request.bin`, and the existing external footer/codec harness. It also uses local helpers from earlier experiments, including `tagged_row_ranges.py`, `fast_statistics_reader.h`, and `refined_statistics_rows.h`. Those historical work directories, helper dependencies, and external Runtime codecs are not packaged here; this is a local-harness runner, not a self-contained public benchmark.

```sh
/home/jiayi.wang/raincloud/.venv/bin/python footer-pages/v38_benchmark.py --stage all
```

The [CSV](proposal-comparison.csv) contains size and timing values for all 48 dataset/scope combinations and all three formats. Scope 0 is core; 1 adds chunk statistics; 2 adds page locations; 3 adds page statistics. The [result archive](v38-results.json.gz) records all trial samples, per-position/process medians, validation results, artifact/source hashes, CPU information, and exact build commands. Its `runs` arrays combine the two timing campaigns by seed; `provenance.timing_campaigns` identifies the six separate process outputs.
