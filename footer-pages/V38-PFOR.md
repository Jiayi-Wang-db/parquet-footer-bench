# V38 ordinary-frame PFOR experiment

**Preferred benchmark configuration: `offsets_frame`.** Retain ordinary-frame PFOR for relative page offsets and adaptive encoding for the hierarchy indexes. We accept the modest size increase over size-minimized V38 because it buys selective-read speed while remaining smaller than modular on every tested dataset at the page-location and full-metadata scopes. This is a benchmark encoding policy; the proposed format does not mandate PFOR.

Forcing frame PFOR on relative page offsets changes geometric-mean decoding time for chunk metadata plus page locations by **-4.0% for one column** and **-1.1% for all columns**. Retained metadata through page locations grows **+2.3%**. Negative timing changes mean less time. The tables separate index changes from page-offset changes.

Encoding-only controls on all 12 saved V38 datasets. No Zstd. The baseline is remeasured in the same executable and timing processes as the controls; historical proposal timings are not used as the denominator.

`indexes_frame` forces ordinary-frame PFOR on `column_chunk_index` and `page_index` when present. `offsets_frame` forces it on relative page offsets (V38 field 4.3). `both_frame` combines them. Every DATA_PAGE_V2 retains its original logical boundaries and value count. PFOR uses the existing 1,024-value blocks; no delta blocks are allowed. A dictionary page is removed if its target stream is converted. All other streams remain byte-identical. Root offsets/lengths are updated.

The V38 reader, bulk/selected access policy, datasets and queries are unchanged. Timings include opening/parsing metadata, decoding, materializing outputs and hashing, with resident bytes and fresh readers per operation. Nine distributed single-column positions and an all-column read are measured. Each dataset is measured in three processes, each running seven randomized trials with all four policies interleaved on the same pinned CPU. The two dataset suites run separately, for six timing processes total. Each timed batch follows one untimed warmup and contains at least four operations. Single-column results are medians across positions after taking medians across trials/processes. Small changes may be timing noise.

An initial run without per-batch warmup produced an apparent 27% ClickBench full-statistics difference, including in the page-offset-only negative control. A diagnostic using identical footer bytes reproduced the roughly 90/120 ms timing modes. The tables below use the subsequent warmup/four-operation-minimum campaign; the relevant initial and identical-artifact samples are preserved in the result archive.

The size tables are cumulative stored metadata. Timing queries read only the named projection: statistics queries do not read relative page offsets. Page-offset-only controls on statistics queries and all scope-0/1 queries are therefore negative controls.

## Preferred configuration versus modular size

Sizes below use the same saved datasets and cumulative metadata scopes as the modular measurements. Timing improvements above compare with the matched V38 baseline, not with modular timings from a different campaign. Core-only sizes are unchanged by page-offset encoding; some small core footers remain larger than modular.

| Dataset | Page-location scope: preferred / modular bytes | Full metadata: preferred / modular bytes |
|---|---:|---:|
| AMPds | 3,990 / 5,974 | 9,014 / 14,758 |
| BI-USCensus | 1,051,776 / 2,581,306 | 1,523,205 / 5,564,773 |
| BI-Wins | 337,235 / 856,156 | 500,440 / 1,819,241 |
| ClickBench Hits | 2,257,417 / 4,898,177 | 9,855,685 / 16,887,807 |
| FineWeb | 34,183 / 49,437 | 86,177 / 112,171 |
| Hacker News | 8,515 / 10,946 | 123,504 / 132,831 |
| Kepler | 8,076 / 17,467 | 10,645 / 25,952 |
| Kepler Time Series | 85,729 / 346,199 | 111,140 / 530,149 |
| UCI Adult | 1,724 / 2,099 | 2,346 / 3,354 |
| Household Power | 5,629 / 7,732 | 13,467 / 19,596 |
| US Accidents | 15,117 / 25,854 | 48,915 / 79,147 |
| NYC Taxi | 16,711 / 29,105 | 36,469 / 82,850 |
| **Total** | 3,826,102 / 8,830,452 | 12,321,007 / 25,272,629 |

The selected artifact directory and all 48 hashes are recorded in `preferred.json` in the work directory and `preferred_configuration` in the result archive.

## Decoding overview

Geometric mean time change over 12 datasets. Negative means less time. These aggregates are descriptive, not significance tests.

| Query | Read | Indexes only | Page offsets only | Both |
|---|---|---:|---:|---:|
| Chunk metadata | One column | +0.1% | -0.3% | +0.1% |
| Chunk metadata | All columns | +0.6% | +0.0% | +0.5% |
| Chunk statistics | One column | +0.4% | +0.0% | +0.5% |
| Chunk statistics | All columns | +0.7% | +0.3% | +0.9% |
| Chunk metadata + page locations | One column | -0.5% | -4.0% | -4.5% |
| Chunk metadata + page locations | All columns | +0.1% | -1.1% | -1.0% |
| Chunk + page statistics | One column | -0.3% | -0.3% | -0.4% |
| Chunk + page statistics | All columns | -0.1% | -0.0% | +0.2% |

## Core size

| Dataset | V38 bytes | Indexes only bytes | Page offsets only bytes | Both bytes |
|---|---:|---:|---:|---:|
| AMPds | 873 | 882 | 873 | 882 |
| BI-USCensus | 185,889 | 186,797 | 185,889 | 186,797 |
| BI-Wins | 88,590 | 89,642 | 88,590 | 89,642 |
| ClickBench Hits | 119,001 | 119,176 | 119,001 | 119,176 |
| FineWeb | 2,359 | 2,370 | 2,359 | 2,370 |
| Hacker News | 1,314 | 1,326 | 1,314 | 1,326 |
| Kepler | 5,405 | 5,562 | 5,405 | 5,562 |
| Kepler Time Series | 60,084 | 64,033 | 60,084 | 64,033 |
| UCI Adult | 1,061 | 1,072 | 1,061 | 1,072 |
| Household Power | 912 | 920 | 912 | 920 |
| US Accidents | 2,099 | 2,138 | 2,099 | 2,138 |
| NYC Taxi | 1,371 | 1,387 | 1,371 | 1,387 |
| **Total** | 468,958 | 475,305 | 468,958 | 475,305 |

## Chunk metadata decoding

Each cell is **one column / all columns**. Baseline values are microseconds; control values are percentage changes versus the matched baseline.

| Dataset | V38 µs | Indexes only Δ | Page offsets only Δ | Both Δ |
|---|---:|---:|---:|---:|
| AMPds | 4.57 / 7.62 | +1.1% / +1.3% | +1.0% / +0.4% | +1.2% / +1.3% |
| BI-USCensus | 53.92 / 1295.06 | +0.1% / -0.3% | -0.2% / +0.4% | +0.1% / -0.6% |
| BI-Wins | 43.91 / 586.52 | -0.2% / -0.2% | -0.3% / -0.2% | -0.5% / -0.1% |
| ClickBench Hits | 59.52 / 848.85 | -0.1% / +0.2% | +0.1% / +0.1% | -0.0% / -0.1% |
| FineWeb | 6.42 / 15.09 | +2.6% / +0.8% | -0.1% / -0.1% | +2.2% / +0.7% |
| Hacker News | 5.53 / 10.83 | +1.8% / +0.1% | -0.2% / -0.6% | +2.1% / +0.2% |
| Kepler | 8.31 / 46.57 | -0.6% / +0.0% | +0.0% / +0.2% | -1.0% / -0.2% |
| Kepler Time Series | 57.74 / 809.47 | -0.7% / -0.3% | -1.6% / +0.1% | -1.4% / -0.3% |
| UCI Adult | 4.76 / 8.74 | +0.4% / +1.8% | -0.7% / +0.5% | +0.5% / +1.7% |
| Household Power | 4.39 / 6.84 | +1.1% / +2.2% | +0.2% / -0.4% | +1.4% / +1.4% |
| US Accidents | 5.46 / 16.84 | -1.8% / +0.3% | -1.2% / -0.0% | -1.2% / +0.7% |
| NYC Taxi | 4.76 / 9.98 | -2.7% / +1.3% | -0.7% / -0.3% | -2.4% / +1.0% |

## Core + chunk statistics size

| Dataset | V38 bytes | Indexes only bytes | Page offsets only bytes | Both bytes |
|---|---:|---:|---:|---:|
| AMPds | 1,187 | 1,196 | 1,187 | 1,196 |
| BI-USCensus | 224,400 | 225,308 | 224,400 | 225,308 |
| BI-Wins | 109,519 | 110,571 | 109,519 | 110,571 |
| ClickBench Hits | 192,476 | 192,651 | 192,476 | 192,651 |
| FineWeb | 17,690 | 17,701 | 17,690 | 17,701 |
| Hacker News | 2,324 | 2,336 | 2,324 | 2,336 |
| Kepler | 7,937 | 8,094 | 7,937 | 8,094 |
| Kepler Time Series | 85,501 | 89,450 | 85,501 | 89,450 |
| UCI Adult | 1,454 | 1,465 | 1,454 | 1,465 |
| Household Power | 1,251 | 1,259 | 1,251 | 1,259 |
| US Accidents | 3,714 | 3,753 | 3,714 | 3,753 |
| NYC Taxi | 1,838 | 1,854 | 1,838 | 1,854 |
| **Total** | 649,291 | 655,638 | 649,291 | 655,638 |

## Chunk statistics decoding

Each cell is **one column / all columns**. Baseline values are microseconds; control values are percentage changes versus the matched baseline.

| Dataset | V38 µs | Indexes only Δ | Page offsets only Δ | Both Δ |
|---|---:|---:|---:|---:|
| AMPds | 5.24 / 8.47 | +1.2% / +0.9% | +0.3% / -0.0% | +1.5% / +1.4% |
| BI-USCensus | 71.14 / 951.52 | -0.0% / -0.1% | +0.1% / +0.4% | +0.6% / -0.0% |
| BI-Wins | 43.69 / 553.29 | +0.0% / +0.5% | +0.0% / +0.3% | -0.1% / -0.0% |
| ClickBench Hits | 56.48 / 775.03 | +0.3% / +0.5% | +0.1% / +0.1% | +0.1% / -0.0% |
| FineWeb | 11.62 / 38.69 | +0.9% / +0.4% | +0.0% / +0.1% | +0.9% / +0.1% |
| Hacker News | 6.73 / 13.05 | +0.5% / +2.7% | +0.4% / +0.4% | +0.6% / +1.6% |
| Kepler | 9.34 / 48.68 | +0.9% / +0.5% | +0.6% / +0.6% | +0.8% / +1.1% |
| Kepler Time Series | 74.37 / 781.16 | -2.3% / +0.4% | -0.7% / +0.3% | -0.5% / +0.7% |
| UCI Adult | 6.05 / 10.37 | +1.3% / +2.1% | +0.2% / +0.4% | +0.2% / +2.2% |
| Household Power | 5.84 / 8.77 | +0.0% / -0.5% | +0.0% / +0.3% | -0.3% / +0.8% |
| US Accidents | 7.03 / 21.07 | +0.7% / +0.2% | -0.3% / -0.0% | +0.6% / +0.9% |
| NYC Taxi | 5.65 / 11.87 | +1.1% / +0.8% | -0.3% / +0.7% | +1.4% / +2.1% |

## Core + chunk statistics + page locations size

| Dataset | V38 bytes | Indexes only bytes | Page offsets only bytes | Both bytes |
|---|---:|---:|---:|---:|
| AMPds | 3,966 | 3,993 | 3,990 | 4,017 |
| BI-USCensus | 1,039,989 | 1,055,690 | 1,051,776 | 1,067,477 |
| BI-Wins | 330,195 | 337,026 | 337,235 | 344,066 |
| ClickBench Hits | 2,196,131 | 2,205,271 | 2,257,417 | 2,266,557 |
| FineWeb | 32,203 | 32,286 | 34,183 | 34,266 |
| Hacker News | 7,847 | 7,864 | 8,515 | 8,532 |
| Kepler | 8,071 | 8,385 | 8,076 | 8,390 |
| Kepler Time Series | 85,700 | 93,598 | 85,729 | 93,627 |
| UCI Adult | 1,724 | 1,750 | 1,724 | 1,750 |
| Household Power | 5,297 | 5,318 | 5,629 | 5,650 |
| US Accidents | 14,453 | 14,554 | 15,117 | 15,218 |
| NYC Taxi | 14,832 | 14,879 | 16,711 | 16,758 |
| **Total** | 3,740,408 | 3,780,614 | 3,826,102 | 3,866,308 |

## Chunk metadata + page locations decoding

Each cell is **one column / all columns**. Baseline values are microseconds; control values are percentage changes versus the matched baseline.

| Dataset | V38 µs | Indexes only Δ | Page offsets only Δ | Both Δ |
|---|---:|---:|---:|---:|
| AMPds | 10.28 / 31.71 | -0.6% / +0.9% | -0.5% / -0.8% | -0.5% / -0.5% |
| BI-USCensus | 98.91 / 8371.21 | -0.7% / +0.1% | -7.5% / -1.2% | -8.2% / -0.9% |
| BI-Wins | 68.89 / 2777.51 | -1.2% / -0.2% | -6.0% / -1.1% | -6.5% / -1.2% |
| ClickBench Hits | 215.24 / 13639.40 | -2.1% / -0.2% | -0.2% / -2.1% | -2.3% / -2.0% |
| FineWeb | 17.40 / 104.01 | +1.4% / +0.1% | -12.0% / -1.9% | -12.0% / -1.6% |
| Hacker News | 10.93 / 40.50 | -0.1% / -0.1% | -3.0% / -1.3% | -3.7% / -1.9% |
| Kepler | 10.34 / 80.76 | +0.1% / -0.6% | -1.1% / +0.1% | -1.0% / -0.4% |
| Kepler Time Series | 62.77 / 1493.54 | -2.8% / -0.4% | -3.7% / -0.8% | -5.6% / -0.7% |
| UCI Adult | 7.70 / 15.46 | +0.5% / +1.0% | +0.7% / +0.6% | +1.0% / +0.9% |
| Household Power | 12.14 / 38.97 | -1.8% / +0.0% | +0.3% / -0.9% | +0.0% / -0.8% |
| US Accidents | 14.04 / 103.99 | +0.7% / +0.3% | -5.3% / -0.9% | -6.1% / -1.2% |
| NYC Taxi | 18.07 / 115.66 | +0.8% / -0.0% | -9.1% / -2.7% | -8.6% / -2.2% |

## Full metadata size

| Dataset | V38 bytes | Indexes only bytes | Page offsets only bytes | Both bytes |
|---|---:|---:|---:|---:|
| AMPds | 8,990 | 9,017 | 9,014 | 9,041 |
| BI-USCensus | 1,511,418 | 1,527,119 | 1,523,205 | 1,538,906 |
| BI-Wins | 493,400 | 500,231 | 500,440 | 507,271 |
| ClickBench Hits | 9,794,399 | 9,803,539 | 9,855,685 | 9,864,825 |
| FineWeb | 84,197 | 84,280 | 86,177 | 86,260 |
| Hacker News | 122,836 | 122,853 | 123,504 | 123,521 |
| Kepler | 10,640 | 10,954 | 10,645 | 10,959 |
| Kepler Time Series | 111,111 | 119,009 | 111,140 | 119,038 |
| UCI Adult | 2,346 | 2,372 | 2,346 | 2,372 |
| Household Power | 13,135 | 13,156 | 13,467 | 13,488 |
| US Accidents | 48,251 | 48,352 | 48,915 | 49,016 |
| NYC Taxi | 34,590 | 34,637 | 36,469 | 36,516 |
| **Total** | 12,235,313 | 12,275,519 | 12,321,007 | 12,361,213 |

## Chunk + page statistics decoding

Each cell is **one column / all columns**. Baseline values are microseconds; control values are percentage changes versus the matched baseline.

| Dataset | V38 µs | Indexes only Δ | Page offsets only Δ | Both Δ |
|---|---:|---:|---:|---:|
| AMPds | 17.29 / 64.93 | +0.7% / -0.1% | -1.0% / -0.3% | -0.1% / -0.2% |
| BI-USCensus | 236.23 / 19342.30 | -0.4% / +0.2% | -0.4% / -0.1% | -0.2% / +0.1% |
| BI-Wins | 163.79 / 6227.96 | -0.4% / -0.4% | -0.2% / +0.0% | -0.2% / -0.1% |
| ClickBench Hits | 531.26 / 90307.00 | -1.2% / +0.8% | +0.5% / +0.6% | -1.0% / +4.1% |
| FineWeb | 36.05 / 206.95 | +0.5% / -0.1% | +0.2% / +0.1% | +0.0% / +0.3% |
| Hacker News | 18.48 / 259.00 | -0.3% / -0.1% | -0.4% / -0.6% | -0.7% / -0.2% |
| Kepler | 14.43 / 102.86 | +0.5% / -0.8% | -0.1% / +0.1% | -0.0% / -0.5% |
| Kepler Time Series | 91.01 / 1650.11 | -3.7% / +0.3% | -2.8% / +0.9% | -2.7% / -0.0% |
| UCI Adult | 12.06 / 25.54 | +0.3% / +0.0% | +0.5% / -0.7% | +0.2% / -0.4% |
| Household Power | 29.92 / 112.32 | +0.3% / -0.6% | +0.3% / -0.3% | +0.9% / -0.5% |
| US Accidents | 32.29 / 326.03 | -0.4% / +0.1% | -0.4% / +0.0% | -0.7% / -0.1% |
| NYC Taxi | 34.94 / 280.17 | +0.2% / +0.0% | -0.2% / +0.2% | +0.4% / +0.0% |

## Validation and reproduction

All 48 source artifacts match their recorded V38 SHA-256 hashes. Preparation round-trips every converted value, checks frame-mode flags, preserves logical page boundaries and verifies all unchanged streams byte-for-byte. All 192 resulting directories parse through the measured V38 Thrift definition. Release and ASan/UBSan each validate 4,624 queries against source oracles, with matching outputs across the four policies. Sanitizers halt on error. Each timing process also validates its selected projections before measuring.

This uses the measured V38 wire layout, before the subsequent review-draft struct reorganization. Runtime PFOR is an experimental encoding. The runner requires the existing local harness, frozen artifacts, generated reader support and external Runtime codecs described in [V38-MEASUREMENTS.md](V38-MEASUREMENTS.md); it is not a self-contained public benchmark.

```sh
/home/jiayi.wang/raincloud/.venv/bin/python footer-pages/v38_pfor.py --stage all
/home/jiayi.wang/raincloud/.venv/bin/python footer-pages/v38_pfor.py --stage report
```

[Absolute sizes/timings and deltas](v38-pfor-comparison.csv) · [Samples, stream decisions and provenance](v38-pfor-results.json.gz) · [Runner](v38_pfor.py) · [Native driver](v38_pfor_decode.cc)
