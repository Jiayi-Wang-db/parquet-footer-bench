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

Times are microseconds per query. Each cell is **one column / all columns**. One-column values are the median over nine selected positions, after taking each position’s median across three fresh processes. Every process runs seven shuffled trials per case. All three formats are interleaved on CPU 16 (Intel Xeon Platinum 8375C); input bytes are resident, and each query constructs a new reader. All timings were rerun with the optimized reader, using the same executable for the eight-dataset and four-dataset suites.

### Chunk metadata

| Dataset | Current Parquet Thrift | Modular footer | V38 |
|---|---:|---:|---:|
| US Accidents | 90.53 / 100.89 | 18.84 / 23.29 | 5.26 / 17.67 |
| FineWeb | 61.47 / 81.19 | 5.78 / 14.03 | 6.39 / 15.13 |
| Hacker News | 35.57 / 41.56 | 8.28 / 10.76 | 5.43 / 10.76 |
| NYC Taxi | 40.36 / 44.81 | 9.26 / 11.12 | 4.61 / 10.12 |
| UCI Adult | 28.87 / 32.32 | 8.61 / 10.04 | 4.67 / 8.83 |
| Household Power | 18.93 / 20.83 | 4.77 / 5.65 | 4.30 / 7.01 |
| Kepler Search | 305.64 / 336.58 | 58.87 / 75.49 | 8.30 / 50.28 |
| AMPds | 22.65 / 24.89 | 5.83 / 7.07 | 4.56 / 7.74 |
| ClickBench | 3,642.97 / 5,421.59 | 70.43 / 790.02 | 62.13 / 877.93 |
| BI-USCensus | 5,901.06 / 8,581.93 | 307.76 / 1,485.41 | 55.33 / 1,324.76 |
| BI-Wins | 3,150.92 / 4,292.93 | 339.07 / 884.57 | 44.93 / 612.05 |
| Kepler Time Series | 5,512.22 / 6,168.77 | 979.92 / 1,291.62 | 54.80 / 885.10 |

### Chunk statistics

| Dataset | Current Parquet Thrift | Modular footer | V38 |
|---|---:|---:|---:|
| US Accidents | 96.18 / 125.64 | 19.47 / 36.85 | 7.00 / 21.48 |
| FineWeb | 83.57 / 159.58 | 9.14 / 47.27 | 11.83 / 39.07 |
| Hacker News | 39.89 / 57.88 | 8.87 / 16.67 | 6.77 / 13.24 |
| NYC Taxi | 42.93 / 55.35 | 9.72 / 16.37 | 5.51 / 11.77 |
| UCI Adult | 31.29 / 40.53 | 9.07 / 14.22 | 6.07 / 10.47 |
| Household Power | 20.13 / 25.74 | 5.21 / 8.20 | 5.88 / 8.80 |
| Kepler Search | 322.14 / 412.46 | 59.66 / 108.01 | 9.59 / 51.00 |
| AMPds | 24.24 / 30.92 | 6.36 / 10.32 | 5.15 / 8.31 |
| ClickBench | 4,450.27 / 8,872.48 | 75.88 / 1,456.34 | 59.22 / 808.72 |
| BI-USCensus | 7,168.80 / 14,051.50 | 312.89 / 2,433.40 | 75.40 / 987.38 |
| BI-Wins | 3,701.80 / 6,756.69 | 343.59 / 1,436.93 | 45.15 / 572.35 |
| Kepler Time Series | 5,853.66 / 7,593.44 | 987.40 / 2,061.62 | 77.36 / 818.22 |

### Chunk metadata + page locations

| Dataset | Current Parquet Thrift | Modular footer | V38 |
|---|---:|---:|---:|
| US Accidents | 100.92 / 302.51 | 21.76 / 150.27 | 14.10 / 107.53 |
| FineWeb | 92.38 / 378.13 | 11.64 / 194.50 | 17.70 / 104.58 |
| Hacker News | 44.20 / 128.13 | 11.32 / 62.75 | 10.50 / 40.43 |
| NYC Taxi | 53.23 / 253.86 | 16.38 / 147.22 | 18.31 / 117.50 |
| UCI Adult | 32.15 / 49.26 | 9.31 / 17.76 | 7.70 / 15.90 |
| Household Power | 26.48 / 80.44 | 9.27 / 44.23 | 12.20 / 39.72 |
| Kepler Search | 324.58 / 483.21 | 59.43 / 142.40 | 10.47 / 84.18 |
| AMPds | 27.99 / 73.29 | 8.56 / 37.28 | 10.30 / 32.85 |
| ClickBench | 5,015.37 / 45,312.30 | 310.69 / 26,561.20 | 221.21 / 13,703.20 |
| BI-USCensus | 7,518.76 / 35,344.60 | 340.88 / 17,101.40 | 102.36 / 8,527.32 |
| BI-Wins | 3,843.25 / 13,306.10 | 349.50 / 5,920.63 | 71.43 / 2,947.91 |
| Kepler Time Series | 5,912.71 / 9,039.31 | 982.01 / 2,583.70 | 59.83 / 1,556.25 |

### Chunk + page statistics

| Dataset | Current Parquet Thrift | Modular footer | V38 |
|---|---:|---:|---:|
| US Accidents | 110.62 / 792.29 | 31.11 / 609.93 | 33.43 / 333.62 |
| FineWeb | 120.85 / 588.24 | 27.37 / 357.35 | 37.21 / 211.66 |
| Hacker News | 54.04 / 463.20 | 19.26 / 362.31 | 18.91 / 271.33 |
| NYC Taxi | 72.49 / 617.53 | 39.10 / 604.63 | 36.24 / 288.78 |
| UCI Adult | 35.07 / 83.37 | 10.56 / 32.37 | 12.21 / 26.14 |
| Household Power | 40.65 / 199.49 | 26.05 / 186.96 | 31.05 / 115.31 |
| Kepler Search | 332.18 / 764.10 | 60.58 / 232.30 | 14.85 / 106.92 |
| AMPds | 36.03 / 157.10 | 15.83 / 119.15 | 17.76 / 66.94 |
| ClickBench | 5,852.40 / 131,028.00 | 979.20 / 109,196.00 | 564.88 / 120,365.00 |
| BI-USCensus | 7,852.65 / 77,866.60 | 404.21 / 48,788.40 | 252.03 / 19,963.10 |
| BI-Wins | 3,984.63 / 28,851.80 | 369.53 / 15,846.70 | 176.53 / 6,599.38 |
| Kepler Time Series | 5,983.62 / 14,082.70 | 988.72 / 4,544.56 | 93.94 / 1,720.51 |

V38 takes less decoding time than current Parquet Thrift in **96/96** measured cases and less than modular in **77/96**. Each case is one dataset, query, and projection; these prototype results do not establish a universal decoding advantage.

This rerun optimizes the V38 reader on unchanged footer bytes: core scalar callbacks avoid copying value wrappers, full core reads reuse a decoded dictionary-offset table, and range reads visit only overlapping metadata pages and skip redundant bulk rank scans. The dictionary cache adds 8 bytes per dictionary entry for full core reads. The schema, stored encodings, and sizes are unchanged.

### Cases more than 10% slower than modular

Percentages compare decoding time using unrounded medians.

| Query | One column | All columns |
|---|---|---|
| Chunk metadata | FineWeb +10.7% | Household Power +24.0%; ClickBench +11.1% |
| Chunk statistics | FineWeb +29.4%; Household Power +12.7% | None |
| Chunk metadata + page locations | FineWeb +52.0%; Household Power +31.6%; AMPds +20.3%; NYC Taxi +11.8% | None |
| Chunk + page statistics | FineWeb +35.9%; Household Power +19.2%; UCI Adult +15.7%; AMPds +12.2% | ClickBench +10.2% |

## Scope and validation

Each projection constructs and checksums the same logical output. Chunk metadata includes data start/end, dictionary start, codec, value count, uncompressed size, and dictionary-use flag; the page projection adds byte extents and first-row positions. Statistics projections return row starts, min/max bounds, and pruning flags. Dictionary lengths and row-group byte hints are stored in the artifacts but are not part of the timed output. Exact null counts, encryption, and cold-storage latency are not benchmarked.

Release and ASan/UBSan each pass **2,508 cross-format queries** against the independent source oracle and **88 fixture checks**. Fixtures cover unequal/empty columns, zero-page chunks, same-column dictionary reuse, absent dictionaries, zero-column files, chunk/page/arbitrary statistics, missing bounds, statistics without page offsets, and malformed indexes/locations/statistics. Output hashes agree across formats. The real corpus uses regular row-group-aligned chunks; ragged layouts are correctness fixtures, not timing workloads.

The four original shards pass another **960 cross-format queries** in both release and ASan/UBSan, using the same optimized executable. All 16 additional V38 artifacts were parsed through the current Thrift definition and checked against source chunk/dictionary extents during preparation; retained statistics streams were verified byte-for-byte. This reader-only rerun verifies the saved hashes of all 48 unchanged artifacts. Each timing process also validates its selected projections against the source oracle before measuring.

These are prototype readers using the existing external codec harness, including repository-local experimental encodings. They are not a production implementation or an interoperable public encoding profile. Selective validation checks the accessed ranges; preparation and full validation check the complete source outputs.

## Reproduction

The checked-in writer/orchestrator is [v38_benchmark.py](v38_benchmark.py); native V38 addressing is in [v38_reader.h](v38_reader.h), and the matched driver/fixtures are in [v38_decode.cc](v38_decode.cc). This run reuses the saved V35 artifacts, frozen `work/refined-hierarchy` reader support/build commands, `work/list-statistics/request.bin`, and the existing external footer/codec harness. It also uses local helpers from earlier experiments, including `tagged_row_ranges.py`, `fast_statistics_reader.h`, and `refined_statistics_rows.h`. Those historical work directories, helper dependencies, and external Runtime codecs are not packaged here; this is a local-harness runner, not a self-contained public benchmark.

```sh
/home/jiayi.wang/raincloud/.venv/bin/python footer-pages/v38_benchmark.py --stage all
```

The [CSV](proposal-comparison.csv) contains size and timing values for all 48 dataset/scope combinations and all three formats. Scope 0 is core; 1 adds chunk statistics; 2 adds page locations; 3 adds page statistics. The [result archive](v38-results.json.gz) records all trial samples, per-position/process medians, validation results, artifact/source hashes, CPU information, and exact build commands. Its `runs` arrays combine the two timing campaigns by seed; `provenance.timing_campaigns` identifies the six separate process outputs.
