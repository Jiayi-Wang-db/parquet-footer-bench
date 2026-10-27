# V38 size and decoding measurements

Fresh comparison of current Parquet Compact Thrift, the modular footer, and V38. V38 stores direct chunk offsets/lengths, a dictionary offset/length table, nullable chunk dictionary IDs, and shared cumulative-end indexes. Both hierarchy indexes omit the fixed leading zero; relative page offsets retain one value per page, including each chunk’s first zero.

All metadata is encoded without Zstd. Size is the complete retained metadata, including schema, directories, page headers, required external indexes, and trailer. Integer and dictionary encodings remain enabled. The source corpus is the same eight parquet-java files described in [EXPERIMENT-RESULTS.md](EXPERIMENT-RESULTS.md#2-java-written-inputs).

## Core size

| Dataset | Current bytes | Modular bytes | V38 bytes | V38 − V35 bytes |
|---|---:|---:|---:|---:|
| UCI Adult | 1,370 | 728 | 1,061 | +0 |
| Household Power | 935 | 540 | 912 | +0 |
| Kepler Search | 13,307 | 5,850 | 5,405 | +0 |
| AMPds | 913 | 520 | 873 | +0 |
| ClickBench | 510,422 | 151,028 | 119,001 | +1 |
| BI-USCensus | 750,267 | 226,675 | 185,889 | +0 |
| BI-Wins | 333,040 | 106,014 | 88,590 | +0 |
| Kepler Time Series | 221,661 | 91,407 | 60,084 | +0 |
| **Total** | **1,831,915** | **582,762** | **461,815** | **+1** |

Removing two logical values does not guarantee fewer encoded bytes. V38 re-encodes only the affected hierarchy streams with the existing encoding/capacity sweep; all other streams match V35 byte-for-byte. Changed first values and encoding parameters can offset the removed zeros.

## Decoding by query

Times are microseconds per query. Each cell is **one column / all columns**. One-column values are the median over nine selected positions, after taking each position’s median across three fresh processes. Every process runs seven shuffled trials per case. All three formats are interleaved on CPU 16 (Intel Xeon Platinum 8375C); input bytes are resident, and each query constructs a new reader.

### Chunk metadata

| Dataset | Current µs | Modular µs | V38 µs |
|---|---:|---:|---:|
| UCI Adult | 28.99 / 32.69 | 8.37 / 9.87 | 4.64 / 9.53 |
| Household Power | 18.91 / 20.68 | 4.69 / 5.54 | 4.28 / 7.25 |
| Kepler Search | 304.01 / 337.88 | 57.73 / 73.28 | 8.29 / 57.75 |
| AMPds | 22.59 / 25.04 | 5.71 / 6.89 | 4.52 / 8.34 |
| ClickBench | 3,647.20 / 5,362.82 | 68.71 / 767.42 | 65.82 / 1,373.65 |
| BI-USCensus | 5,948.85 / 8,551.72 | 300.17 / 1,441.27 | 56.37 / 2,050.25 |
| BI-Wins | 3,154.57 / 4,369.26 | 331.13 / 857.37 | 45.26 / 918.10 |
| Kepler Time Series | 5,524.50 / 6,122.29 | 969.08 / 1,252.68 | 54.45 / 1,000.27 |

### Chunk statistics

| Dataset | Current µs | Modular µs | V38 µs |
|---|---:|---:|---:|
| UCI Adult | 31.19 / 40.49 | 8.94 / 14.19 | 6.02 / 10.29 |
| Household Power | 20.05 / 25.88 | 5.14 / 8.16 | 5.86 / 8.77 |
| Kepler Search | 321.52 / 413.07 | 58.33 / 110.11 | 9.47 / 50.17 |
| AMPds | 24.09 / 31.38 | 6.28 / 10.41 | 5.11 / 8.05 |
| ClickBench | 4,528.19 / 9,150.52 | 74.61 / 1,467.76 | 59.75 / 841.47 |
| BI-USCensus | 7,156.08 / 14,133.80 | 304.20 / 2,450.77 | 75.06 / 1,037.32 |
| BI-Wins | 3,709.84 / 6,798.86 | 335.35 / 1,389.92 | 45.14 / 603.07 |
| Kepler Time Series | 5,857.33 / 7,648.12 | 971.57 / 2,109.15 | 76.62 / 778.84 |

### Chunk metadata + page locations

| Dataset | Current µs | Modular µs | V38 µs |
|---|---:|---:|---:|
| UCI Adult | 32.11 / 49.99 | 9.04 / 17.45 | 7.72 / 17.71 |
| Household Power | 26.66 / 81.00 | 9.17 / 44.56 | 13.64 / 52.40 |
| Kepler Search | 326.06 / 482.14 | 58.23 / 140.57 | 10.47 / 95.06 |
| AMPds | 27.86 / 73.57 | 8.40 / 36.94 | 11.05 / 42.55 |
| ClickBench | 5,021.06 / 45,670.40 | 310.79 / 26,810.80 | 298.26 / 21,339.50 |
| BI-USCensus | 7,511.31 / 35,473.40 | 332.08 / 17,188.10 | 110.04 / 12,906.70 |
| BI-Wins | 3,857.56 / 13,428.50 | 340.94 / 5,904.12 | 74.21 / 4,320.37 |
| Kepler Time Series | 5,927.22 / 9,074.75 | 968.84 / 2,542.47 | 59.24 / 1,748.64 |

### Chunk + page statistics

| Dataset | Current µs | Modular µs | V38 µs |
|---|---:|---:|---:|
| UCI Adult | 34.96 / 84.65 | 10.46 / 32.92 | 12.07 / 26.21 |
| Household Power | 40.57 / 198.84 | 25.98 / 188.28 | 30.93 / 118.46 |
| Kepler Search | 333.23 / 765.90 | 59.55 / 236.42 | 14.76 / 104.26 |
| AMPds | 35.94 / 157.91 | 16.04 / 119.98 | 17.80 / 69.20 |
| ClickBench | 5,934.74 / 132,204.00 | 983.43 / 110,325.00 | 570.01 / 121,880.00 |
| BI-USCensus | 7,839.25 / 78,071.50 | 398.75 / 49,487.90 | 252.77 / 21,116.10 |
| BI-Wins | 4,002.30 / 28,874.80 | 359.79 / 15,954.90 | 177.64 / 6,879.32 |
| Kepler Time Series | 6,006.29 / 14,106.80 | 972.46 / 4,623.39 | 92.23 / 1,644.44 |

V38 is faster than modular for all four ClickBench one-column projections. All-column results are mixed: chunk metadata takes 79% more time and combined chunk/page statistics 10% more time, while chunk statistics and chunk metadata with page locations are faster. These results do not establish a universal decoding advantage.

## Scope and validation

Each projection constructs and checksums the same logical output. Chunk metadata includes data start/end, dictionary start, codec, value count, uncompressed size, and dictionary-use flag; the page projection adds byte extents and first-row positions. Statistics projections return row starts, min/max bounds, and pruning flags. Dictionary lengths and row-group byte hints are preserved from the validated V35 artifacts but are not part of the timed output. Exact null counts, encryption, and cold-storage latency are not benchmarked.

Release and ASan/UBSan each pass **2,508 cross-format queries** against the independent source oracle and **88 fixture checks**. Fixtures cover unequal/empty columns, zero-page chunks, same-column dictionary reuse, absent dictionaries, zero-column files, chunk/page/arbitrary statistics, missing bounds, statistics without page offsets, and malformed indexes/locations/statistics. Output hashes agree across formats. The real corpus uses regular row-group-aligned chunks; ragged layouts are correctness fixtures, not timing workloads.

These are prototype readers using the existing external codec harness, including repository-local experimental encodings. They are not a production implementation or an interoperable public encoding profile. Selective validation checks the accessed ranges; preparation and full validation check the complete source outputs.

## Reproduction

The checked-in writer/orchestrator is [v38_benchmark.py](v38_benchmark.py); native V38 addressing is in [v38_reader.h](v38_reader.h), and the matched driver/fixtures are in [v38_decode.cc](v38_decode.cc). This run reuses the saved V35 artifacts, frozen `work/refined-hierarchy` reader support/build commands, `work/list-statistics/request.bin`, and the existing external footer/codec harness. It also uses local helpers from earlier experiments, including `tagged_row_ranges.py`, `fast_statistics_reader.h`, and `refined_statistics_rows.h`. Those historical work directories, helper dependencies, and external Runtime codecs are not packaged here; this is a local-harness runner, not a self-contained public benchmark.

```sh
/home/jiayi.wang/raincloud/.venv/bin/python footer-pages/v38_benchmark.py --stage all
```

The [CSV](proposal-comparison.csv) contains all 32 dataset/scope combinations for all three formats. Scope 0 is core; 1 adds chunk statistics; 2 adds page locations; 3 adds page statistics. The [result archive](v38-results.json.gz) records all trial samples, per-position/process medians, validation results, artifact/source hashes, CPU information, and exact build commands.
