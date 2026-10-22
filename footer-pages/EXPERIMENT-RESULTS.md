# Footer experiment results

Saved-results reference, snapshot **2026-10-22 18:02 UTC**. This file consolidates the experiments; no benchmarks were rerun to produce it. Use the [playground guide](PLAYGROUND.md) for setup and the [Thrift definition](ModularFooterV2.thrift) for the current format.

- **Size:** uncompressed V2 is smaller than modular in **40/48** matched dataset/level comparisons, and smaller than current in **47/48**. On the four wide datasets it is **33.0–79.8% smaller than modular** before Zstd. Small core-only footers are an exception.
- **Speed:** V2 often improves on current Thrift, but is not consistently faster than modular. Encoding choice, projection, and reader strategy all matter.
- **Full projection:** decoding needed streams once in bulk removed a large selective-access penalty. PFOR alone did not establish a universal speed advantage over DBP.
- **Completion:** all 48 matched size results are complete. Eight datasets have complete three-process timing results. Three more have preliminary one-process results; ClickBench and the final wide timing campaign remain pending.

[Comparison scope](#1-what-the-comparisons-include) · [Datasets](#2-java-written-inputs) · [Sizes](#3-complete-metadata-sizes) · [Decoding](#4-metadata-decoding--access-time) · [Design experiments](#5-layout-and-encoding-experiments) · [Pending work](#6-what-remains-unmeasured) · [Source records](#7-how-to-double-check-a-number)

## 1. What the comparisons include

All primary sizes below are **complete metadata bytes**, including schema/root, stream directories, real page headers and bodies, levels, dictionaries, required external indexes, and trailers. They exclude source data pages. No fixed-size page padding or estimated encoding size is substituted for serialized bytes.

Current is normalized Compact Thrift. Modular uses packed arrays, shared min/max prefixes, and the smaller valid dense/sparse representation. V2 uses ordinary dictionary/Data Page V2 streams and the smallest tested encoding/page-capacity choices. The region-map PR is a separate baseline in section 5.

The four levels are cumulative:

| Level | Metadata retained |
| --- | --- |
| L1 | Core: source schema, row/column counts, row-group/chunk descriptors and chunk placement. No statistics or page indexes. |
| L2 | L1 + row-group statistics. |
| L3 | L2 + OffsetIndex: page byte positions/lengths and first-row positions. |
| L4 | L3 + ColumnIndex/page statistics, retaining row-group statistics. |

The matched runs retain exact source bounds without additional truncation. Current/modular retain null counts; V2 retains the equivalent supported pruning flags. These are comparisons of placement and pruning, not exact COUNT queries. Missing source statistics remain unavailable; FineWeb text has no page-level bounds.

**Raw** means encoded but without general-purpose metadata compression. **Zstd** means independently size-selected V2 with selective Zstd, requiring at least 256 bytes and 10% saving under the recorded cutoff policy. Raw and Zstd may choose different encodings/page boundaries, so their difference is not an isolated compression ablation. Current/modular are uncompressed in these tables.

**Revision distinction:** cohort A predates making `DataLayout.page_index` optional; its L1/L2 V2 artifacts still contain that stream. Cohorts B/C omit it at L1/L2. All are called version 21 in the saved records, so use the definition/artifact hashes rather than the version alone to identify a run.

## 2. Java-written inputs

All primary inputs were freshly exported with Raincloud **`parquet@java`**, parquet-java **1.17.1**, using normal row-group settings. Cohort A contains all rows of four pinned public shards; B/C contain complete selected catalog recipes. Each dataset produces one Parquet file with internal row groups. Source pages may be compressed; all timing tables in section 4 use uncompressed metadata.

Dataset labels below map to exact input IDs in section 7. Kepler Time Series is the recipe's training file, not the separate Kepler Search dataset.

| Set | Dataset | Rows | Columns | Row groups | Data pages | Java file bytes |
| --- | --- | --- | --- | --- | --- | --- |
| A | US Accidents | 1,104,056 | 46 | 1 | 2,620 | 95,648,671 |
| A | FineWeb | 1,048,581 | 9 | 11 | 3,513 | 1,337,489,282 |
| A | Hacker News | 1,072,139 | 14 | 2 | 1,022 | 144,068,514 |
| A | NYC Taxi | 3,475,226 | 20 | 1 | 3,480 | 54,965,852 |
| B | Adult | 48,842 | 15 | 1 | 45 | 373,085 |
| B | Household Power | 2,075,259 | 9 | 1 | 936 | 9,443,498 |
| B | Kepler Search | 9,564 | 153 | 1 | 153 | 2,962,455 |
| B | AMPds | 1,051,200 | 12 | 1 | 636 | 17,467,853 |
| C | Kepler Time Series | 5,087 | 3,198 | 1 | 3,198 | 74,087,086 |
| C | BI-Wins | 2,115,449 | 648 | 8 | 69,336 | 589,769,626 |
| C | BI-USCensus | 9,398,385 | 519 | 23 | 244,968 | 2,354,846,136 |
| C | ClickBench | 99,997,497 | 105 | 70 | 542,277 | 9,226,506,093 |

## 3. Complete metadata sizes

All numbers are **bytes**. Compare formats within the same row; L1–L4 refer to section 1. Sources: S1–S3 in section 7.

### A. Initial four public shards

| Dataset | Level | Current | Modular | V2 raw | V2 Zstd |
| --- | --- | --- | --- | --- | --- |
| US Accidents | L1 | 3,963 | 1,937 | 1,857 | 1,857 |
| US Accidents | L2 | 6,030 | 4,846 | 3,510 | 3,102 |
| US Accidents | L3 | 41,612 | 25,854 | 14,196 | 9,231 |
| US Accidents | L4 | 101,244 | 79,147 | 51,974 | 22,364 |
| FineWeb | L1 | 6,209 | 1,966 | 1,998 | 1,754 |
| FineWeb | L2 | 23,553 | 17,878 | 17,368 | 8,842 |
| FineWeb | L3 | 76,446 | 49,437 | 31,755 | 22,416 |
| FineWeb | L4 | 145,670 | 112,171 | 83,747 | 35,619 |
| Hacker News | L1 | 1,990 | 936 | 1,181 | 1,181 |
| Hacker News | L2 | 3,323 | 2,281 | 2,230 | 2,230 |
| Hacker News | L3 | 17,638 | 10,946 | 7,678 | 5,895 |
| Hacker News | L4 | 140,991 | 132,831 | 125,342 | 63,264 |
| NYC Taxi | L1 | 2,042 | 1,034 | 1,192 | 1,192 |
| NYC Taxi | L2 | 2,850 | 2,004 | 1,697 | 1,697 |
| NYC Taxi | L3 | 51,741 | 29,105 | 14,649 | 9,821 |
| NYC Taxi | L4 | 113,762 | 82,850 | 39,380 | 19,555 |

### B. Four additional catalog datasets

| Dataset | Level | Current | Modular | V2 raw | V2 Zstd |
| --- | --- | --- | --- | --- | --- |
| Adult | L1 | 1,370 | 728 | 922 | 922 |
| Adult | L2 | 1,666 | 1,431 | 1,352 | 1,352 |
| Adult | L3 | 2,319 | 2,099 | 1,622 | 1,597 |
| Adult | L4 | 3,313 | 3,354 | 2,365 | 2,258 |
| Household Power | L1 | 935 | 540 | 776 | 776 |
| Household Power | L2 | 1,119 | 944 | 1,153 | 1,153 |
| Household Power | L3 | 13,917 | 7,732 | 5,198 | 3,380 |
| Household Power | L4 | 27,324 | 19,596 | 14,783 | 6,241 |
| Kepler Search | L1 | 13,307 | 5,850 | 4,785 | 4,775 |
| Kepler Search | L2 | 19,281 | 12,922 | 7,359 | 6,715 |
| Kepler Search | L3 | 22,498 | 17,467 | 7,493 | 6,849 |
| Kepler Search | L4 | 28,443 | 25,952 | 10,148 | 8,847 |
| AMPds | L1 | 913 | 520 | 743 | 743 |
| AMPds | L2 | 1,273 | 1,085 | 1,095 | 1,095 |
| AMPds | L3 | 10,089 | 5,974 | 3,874 | 2,857 |
| AMPds | L4 | 20,541 | 14,758 | 9,896 | 6,175 |

### C. Four wide / many-page catalog datasets

| Dataset | Level | Current | Modular | V2 raw | V2 Zstd |
| --- | --- | --- | --- | --- | --- |
| Kepler Time Series | L1 | 221,661 | 91,407 | 55,787 | 55,136 |
| Kepler Time Series | L2 | 362,345 | 246,575 | 81,279 | 78,450 |
| Kepler Time Series | L3 | 432,650 | 346,199 | 81,478 | 78,641 |
| Kepler Time Series | L4 | 554,166 | 530,149 | 107,024 | 102,009 |
| BI-Wins | L1 | 333,040 | 106,014 | 69,434 | 46,572 |
| BI-Wins | L2 | 500,148 | 197,470 | 90,412 | 54,618 |
| BI-Wins | L3 | 1,513,060 | 856,156 | 310,892 | 160,579 |
| BI-Wins | L4 | 2,563,144 | 1,819,241 | 535,899 | 215,100 |
| BI-USCensus | L1 | 750,267 | 226,675 | 151,810 | 90,377 |
| BI-USCensus | L2 | 1,090,768 | 371,313 | 190,370 | 101,724 |
| BI-USCensus | L3 | 4,701,379 | 2,581,306 | 1,004,023 | 510,990 |
| BI-USCensus | L4 | 7,986,845 | 5,564,773 | 1,755,510 | 708,432 |
| ClickBench | L1 | 510,422 | 151,028 | 97,828 | 55,772 |
| ClickBench | L2 | 734,357 | 262,202 | 171,345 | 85,853 |
| ClickBench | L3 | 8,497,329 | 4,898,177 | 2,183,539 | 1,005,603 |
| ClickBench | L4 | 21,868,253 | 16,887,807 | 10,604,568 | 3,435,869 |

### Supplement: core + OffsetIndex, without any statistics

Size-only experiment, distinct from L3 because row-group statistics are also absent. Source S6; no separate decoding run for this scope.

| Dataset | Current | Modular | V2 raw | V2 Zstd |
| --- | --- | --- | --- | --- |
| Kepler Time Series | 291,966 | 191,031 | 55,986 | 55,327 |
| BI-Wins | 1,345,952 | 764,700 | 289,914 | 152,533 |
| BI-USCensus | 4,360,878 | 2,436,668 | 965,463 | 499,643 |
| ClickBench | 8,273,394 | 4,787,003 | 2,110,022 | 975,522 |

## 4. Metadata decoding / access time

Each cell is **one column / all columns**, in **microseconds per operation**; smaller is faster. These include fresh root/schema/index parsing, required stream decoding, selected output construction/checksum, and teardown from resident serialized bytes. They exclude storage I/O, source-data decoding, and predicate execution. Current parses the whole Thrift footer but still does more selected-output work at larger projections, so its line need not be flat.

For A/B, results are the median of three process medians, each with nine randomized interleaved trials pinned to CPU 16. C is a completed nine-trial **single-process preliminary batch**. One-column numbers aggregate the run's selected windows; they are not a separately timed first/middle/last-column sweep.

A uses the earlier reader. B/C use the projection-aware reader: at full projection it bulk-decodes each needed stream once. All metadata, including metadata dictionaries, is uncompressed. Do not compare these times with older compressed or bootstrap-excluded experiments in section 5 as if only the layout changed.

### A. Complete, earlier reader

| Dataset | Level | Current: one / all | Modular: one / all | V2: one / all |
| --- | --- | --- | --- | --- |
| US Accidents | L1 | 90.55 / 100.84 | 19.24 / 24.13 | 21.64 / 40.07 |
| US Accidents | L2 | 97.42 / 139.55 | 20.07 / 46.75 | 29.20 / 73.33 |
| US Accidents | L3 | 103.03 / 337.91 | 23.22 / 178.78 | 40.39 / 208.30 |
| US Accidents | L4 | 118.40 / 883.43 | 37.66 / 798.66 | 75.44 / 724.29 |
| FineWeb | L1 | 63.11 / 80.65 | 6.06 / 15.09 | 17.42 / 59.53 |
| FineWeb | L2 | 90.43 / 184.05 | 11.64 / 62.86 | 30.98 / 113.42 |
| FineWeb | L3 | 125.38 / 476.92 | 30.75 / 249.06 | 58.88 / 281.16 |
| FineWeb | L4 | 165.85 / 808.22 | 64.23 / 544.49 | 96.84 / 501.53 |
| Hacker News | L1 | 37.70 / 43.19 | 8.50 / 11.25 | 15.41 / 28.49 |
| Hacker News | L2 | 42.61 / 66.60 | 9.55 / 22.05 | 23.28 / 48.02 |
| Hacker News | L3 | 50.23 / 153.51 | 13.43 / 75.57 | 30.71 / 98.67 |
| Hacker News | L4 | 77.64 / 505.95 | 39.92 / 440.59 | 70.36 / 438.81 |
| NYC Taxi | L1 | 42.44 / 46.92 | 9.32 / 11.62 | 13.71 / 21.97 |
| NYC Taxi | L2 | 45.95 / 62.94 | 10.15 / 20.67 | 20.12 / 38.56 |
| NYC Taxi | L3 | 57.70 / 285.29 | 17.31 / 160.54 | 38.24 / 197.97 |
| NYC Taxi | L4 | 81.23 / 721.95 | 50.80 / 816.09 | 84.53 / 695.57 |

### B. Complete, projection-aware reader

| Dataset | Level | Current: one / all | Modular: one / all | V2: one / all |
| --- | --- | --- | --- | --- |
| Adult | L1 | 30.89 / 34.01 | 8.37 / 10.04 | 13.40 / 17.32 |
| Adult | L2 | 33.30 / 46.39 | 9.25 / 17.84 | 19.64 / 29.65 |
| Adult | L3 | 34.94 / 62.49 | 9.84 / 26.21 | 24.91 / 39.40 |
| Adult | L4 | 37.94 / 91.35 | 11.24 / 41.78 | 34.55 / 62.31 |
| Household Power | L1 | 19.98 / 21.60 | 4.72 / 5.82 | 9.46 / 11.82 |
| Household Power | L2 | 21.61 / 29.00 | 5.55 / 10.08 | 14.77 / 20.96 |
| Household Power | L3 | 29.51 / 94.30 | 10.06 / 51.86 | 25.88 / 71.85 |
| Household Power | L4 | 45.82 / 237.36 | 33.53 / 258.86 | 59.11 / 352.60 |
| Kepler Search | L1 | 291.94 / 323.46 | 56.66 / 75.63 | 54.33 / 88.16 |
| Kepler Search | L2 | 309.19 / 442.16 | 59.43 / 141.19 | 62.71 / 151.05 |
| Kepler Search | L3 | 314.88 / 577.91 | 59.25 / 209.64 | 66.27 / 182.94 |
| Kepler Search | L4 | 321.44 / 790.03 | 61.22 / 288.25 | 76.26 / 248.99 |
| AMPds | L1 | 24.02 / 25.94 | 5.82 / 7.37 | 10.66 / 14.07 |
| AMPds | L2 | 25.90 / 35.93 | 6.62 / 13.09 | 16.54 / 23.95 |
| AMPds | L3 | 30.52 / 86.73 | 9.43 / 45.62 | 25.22 / 61.90 |
| AMPds | L4 | 39.66 / 185.65 | 20.20 / 169.94 | 44.87 / 175.32 |

### C. Preliminary, projection-aware reader

| Dataset | Level | Current: one / all | Modular: one / all | V2: one / all |
| --- | --- | --- | --- | --- |
| Kepler Time Series | L1 | 5,304.15 / 5,936.98 | 928.44 / 1,295.42 | 727.38 / 1,378.15 |
| Kepler Time Series | L2 | 5,637.16 / 8,200.47 | 928.84 / 2,674.13 | 772.47 / 2,197.30 |
| Kepler Time Series | L3 | 5,672.54 / 10,858.59 | 931.91 / 4,117.53 | 786.74 / 2,731.52 |
| Kepler Time Series | L4 | 5,826.18 / 15,053.38 | 934.06 / 5,800.87 | 816.99 / 3,633.32 |
| BI-Wins | L1 | 3,017.10 / 4,155.23 | 312.54 / 958.83 | 315.29 / 1,510.91 |
| BI-Wins | L2 | 3,575.12 / 7,961.06 | 313.32 / 2,316.47 | 350.76 / 2,538.01 |
| BI-Wins | L3 | 3,735.61 / 16,689.00 | 324.47 / 7,780.54 | 487.04 / 6,606.69 |
| BI-Wins | L4 | 3,858.76 / 32,519.55 | 345.62 / 22,723.68 | 711.34 / 21,142.92 |
| BI-USCensus | L1 | 5,752.69 / 8,229.35 | 288.33 / 1,705.15 | 346.44 / 2,788.67 |
| BI-USCensus | L2 | 6,940.04 / 16,841.19 | 293.51 / 4,488.85 | 424.09 / 4,925.58 |
| BI-USCensus | L3 | 7,263.65 / 42,477.48 | 324.81 / 21,076.31 | 563.13 / 18,793.43 |
| BI-USCensus | L4 | 7,584.46 / 90,239.27 | 421.79 / 71,619.65 | 879.12 / 88,610.76 |

**ClickBench timing is pending.** C's preliminary file contains 1,056 configurations for the other three datasets. The final four-dataset run has passed release correctness validation for 152,336 matched and 38,084 document-workload queries, but has no final timing summary at this snapshot. Sanitizers were skipped for this wide campaign as requested. The earlier six-trial progress view is a partial view of this same preliminary batch, not an independent replication.

### Projection percentages and the bulk-read fix

The synthetic 2,000-column × 10-row-group fixture isolates the proposal's placement workload: schema plus selected chunk offsets/compressed sizes. It is synthetic metadata, not a Raincloud file and not the all-fields workload above. The table uses the same uncompressed, bootstrap-inclusive run (S7). “Planned” switches to whole-stream bulk decoding at full projection; partial projections use selective access. Encoding controls change only `column_chunk_index` and `region_offsets`.

| Projection | Current µs | Modular µs | V2 original planned µs | PFOR auto planned µs | PFOR frame planned µs |
| --- | --- | --- | --- | --- | --- |
| one | 8,947.71 | 379.69 | 439.99 | 427.51 | 417.31 |
| 10% | 9,252.29 | 413.19 | 589.98 | 604.58 | 614.81 |
| 20% | 9,512.13 | 445.20 | 723.44 | 750.19 | 760.25 |
| 30% | 9,814.27 | 476.59 | 853.68 | 890.98 | 901.09 |
| 40% | 10,021.98 | 508.73 | 981.94 | 1,039.05 | 1,045.30 |
| 50% | 10,213.40 | 539.09 | 1,113.14 | 1,180.53 | 1,187.71 |
| 60% | 10,324.89 | 571.00 | 1,241.82 | 1,330.29 | 1,333.27 |
| 70% | 10,609.23 | 600.01 | 1,370.58 | 1,464.33 | 1,472.79 |
| 80% | 10,776.77 | 626.38 | 1,502.15 | 1,607.93 | 1,610.90 |
| 90% | 11,046.69 | 653.60 | 1,629.92 | 1,752.98 | 1,764.23 |
| 100% | 11,187.49 | 681.57 | 865.34 | 860.71 | 846.50 |

In the paired control, full-projection V2 original fell **1,759.89 → 865.34 µs** and PFOR auto fell **1,895.58 → 860.71 µs** when the reader switched to bulk. With the same bulk path, PFOR auto is only 0.5% lower than original; the direction changes across processes, so this does not establish an encoding winner. Bulk means decoding the needed metadata streams once, then indexing the decoded arrays; it does not mean decoding source data.

Saved projection sweeps cover one column and intermediate percentages through all columns. The table shows the complete percentage sweep for this representative control; the primary tables show the mandatory endpoints for every completed dataset/level.

## 5. Layout and encoding experiments

These are representative results from **each experiment family**, with the original scope retained. They use different revisions, source geometries, compression policies, and workloads; the rows are not a cumulative optimization sequence. Unless a row says “component” or “stats-only,” size is the complete metadata total for that experiment. Evidence filenames identify saved local reports under `footer-pages/`; section 7 explains their availability.

### Page streams, region maps, and flattening

| Experiment | Measured result | Scope / evidence |
| --- | --- | --- |
| Initial page-stream layout | US Accidents: improved modular 1,510.41 KiB → page streams + Zstd 426.85 KiB. Across four inputs: 7.0–71.7% smaller. | Footer-only, no external page indexes; stress row-group inputs, 64 KiB metadata-page target. Compression and grouping both change. `RESULTS.md`. |
| Page capacity / grouping / whole-footer compression | US footer-only Zstd: 16 KiB pages 430.43 KiB; 64 KiB 426.33 KiB; grouping 16 columns 426.85 KiB. Original whole-footer Zstd: 676.19 KiB. | One column per stats stream unless grouped. Grouping reduces US metadata pages from 150 to 25. Whole-footer compression loses selective reads. `RESULTS.md`. |
| Dictionary and prefix controls | Dictionaries change total size by −3.0% / +2.0% / +1.8% / −0.2% for US / FineWeb / HN / Taxi. US custom prefix/suffix streams → raw min/max streams: 451,065 → 440,827 B. | 16 KiB, one column per stream; dictionary and prefix experiments are separate controls. `RESULTS.md`. |
| Normalize index streams and count real framing | US footer + indexes: one stream per original chunk-array takes 16,673,909 B / 355,984 metadata pages; normalized 64 KiB layout takes 773,771 B / 66 pages. | Descriptor repetition dominates the naive layout. Taxi footer-only has 2,130 B payload + 427 B headers + 2,481 B schema/metadata + 245 B framing = 5,283 B across 16 pages. `RESULTS.md`. |
| Why bitpacking still shrinks | US Accidents: 1,510.41 → 699.70 → 430.43 KiB for modular / encoded pages raw / pages + Zstd. | Footer-only, no external page indexes; 16 KiB target, one column per statistics stream. Placement components: 636.55 → 238.91 → 219.86 KiB; stats components: 865.92 → 452.84 → 202.63 KiB. `ENCODINGS.md`. |
| Adaptive region-map baseline | On the stress US Accidents input, region-map core is 209,367 B; placement component is 201,098 B versus 651,826 B in the older packed model. | PR #1 already selects DBP/bitpacking. Partial-statistics omissions prevent an equal-coverage total ranking; its checked-in CSV has different sources. `REGION-MAP-PR1.md`. |
| LIST → flat scalar arrays | Zstd totals: US 384,532 → 383,812 B; FineWeb 1,537,889 → 1,682,233 B; HN 360,185 → 372,636 B; Taxi 25,899 → 25,073 B. | Corrected LIST control, stress inputs. Flat can grow when page splitting resets compression. Earlier LIST headers had invalid null counts; use the corrected comparison. `FLAT-REGION-PAGES-RESULTS.md` / `REGION-PAGES-RESULTS.md`. |
| Random-column access after flattening | Seven chunk fields, LIST → flat: US 4,016.1 → 3,692.8 µs; FineWeb 878.1 → 801.3; HN 1,339.2 → 1,222.8; Taxi 37.7 → 37.6. | Java, compressed, fresh page-header/boundary handling; root bootstrap excluded. Flat indexes identify value ranges, not physical page offsets. FineWeb stats access became slower, 1,635.9 → 1,706.9 µs. `RANDOM-COLUMN-ACCESS.md`. |

Fresh Java rerun with normal row-group settings, five stored designs (**bytes**):

| Dataset | Current | Modular | Region map | Pages raw | Pages Zstd |
| --- | --- | --- | --- | --- | --- |
| US Accidents | 112,114 | 85,951 | 65,933 | 64,622 | 28,160 |
| FineWeb | 159,597 | 112,649 | 75,996 | 56,941 | 35,668 |
| Hacker News | 145,918 | 135,111 | 130,545 | 129,619 | 68,074 |
| NYC Taxi | 116,255 | 84,691 | 53,816 | 49,973 | 23,048 |

Source: `FRESH-JAVA-RESULTS.md` / `fresh-java-comparison.json`. These older full totals have different coverage: current/modular retain separate chunk/page statistics; region/page designs unify ranges. The region-map converter omits four available FineWeb bound pairs. Use section 3 for the matched comparison.

### PFOR, compression, and skipping work

| Experiment | Measured result | Scope / evidence |
| --- | --- | --- |
| Force PFOR versus adaptive choice | US Zstd: existing 28,160 B → forced frame 31,345 B → adaptive 28,055 B. Taxi: 23,048 → 27,963 → 23,038 B. | Forced frame was larger; adaptive savings across four files were only 0.04–0.65%. `PFOR-RESULTS.md`. |
| Java decoding controls | US one-column: existing 7.778 µs; PFOR frame 4.737, delta 4.503, adaptive 5.744. | Java scalar prototype; raw metadata, fresh headers, root/schema excluded. `decode-speed-results.json`. |
| Native C++ decoder | One-column existing → frame: US 17.09 → 3.89 µs; Taxi 12.96 → 3.76 µs. | Zstd fresh-header control, root bootstrap excluded; raw/cached and tree-index controls also saved. `DECODE-SPEED-RESULTS.md`. |
| Add root/schema and baseline readers | Taxi one-column: current 47.21, modular 7.11, existing pages 16.95, PFOR frame 11.65 µs. | Bootstrap included, Zstd page variants; different workload from the small codec control. `LAYOUT-DECODE-RESULTS.md`. |
| Profile known-row lookup | Hacker News: Zstd accounts for 112.7 of 180.8 µs in the instrumented lookup; 54 requested maximum bounds require expanding a 59,342 B payload. Separate Taxi context-reuse control: 222.90 → 209.67 µs. | Phase timers include allocation/setup and perturb total latency. This broader lookup also reads predicate bounds. `LAYOUT-PROFILE-RESULTS.md`. |
| Compression after PFOR | US full metadata: raw 66,546 B; Zstd on any saving 31,345 B; cutoff Zstd 31,607 B. Taxi: 57,544 / 27,963 / 27,963 B. | Compression still helps some encoded arrays; integer-only savings ranged 2.8–55.4%. Cutoff requires both 256 B and 10%. `work/compression-threshold/sizes.json`. |
| Cutoff decoding control | FineWeb broader point lookup: raw 114.28 µs, compress-any-saving 180.83, cutoff 170.13. Taxi: 141.97 / 202.92 / 202.72. | Same encoded values/boundaries; cutoff skips low-saving decompression but retains useful compression. Complete metadata-access time includes other work. `LAYOUT-PROFILE-RESULTS.md`. |
| Skip unneeded streams for a known row | Eager → selective: US 75.03 → 64.12 µs; FineWeb 59.66 → 29.70; HN 35.49 → 30.22; Taxi 70.67 → 53.79. | Same artifacts, cutoff Zstd. Finds the page for an already-known row in each output column; does not find the predicate match. Modular: 37.09 / 7.63 / 13.45 / 17.45 µs respectively. `SKIP-LOOKUP-RESULTS.md`. |
| PFOR encoding + selective access | Synthetic raw original 253,352 B → PFOR auto 254,029 B (+677 B); frame 297,009 B. One-column original 443.81 → auto/selective 427.67 µs; all-column 1,669.00 → 1,842.04 µs. | Only two placement streams change encoding. The original-to-auto/selective comparison also changes the reader. The all-column regression prompted the bulk fix in section 4. `PFOR-ACCESS.md`. |
| Updated PR #267100 decoder | Synthetic PFOR auto all-column direct path 8,029.96 µs versus hybrid 1,805.61 µs. Bulk-kernel-only update: 1,912.93 → 1,898.20 µs. | Byte-identical artifacts, updated f9219aef decoder. Repeated direct delta-prefix work is expensive; element access is not automatically faster than bulk. `PFOR-PR-UPDATE.md`. |
| Projection-aware full reads | Original full projection 1,759.89 → 865.34 µs; auto 1,895.58 → 860.71; frame 1,901.56 → 846.50. | Same-process selective/bulk controls, all metadata uncompressed, unchanged artifacts. Full percentage table in section 4. `PFOR-PROJECTION.md`. |

### Statistics representation and granularity

| Experiment | Measured result | Scope / evidence |
| --- | --- | --- |
| Nullable typed bound arrays | Taxi full Zstd footer 27,963 → 26,846 B; one-column bound access 104.43 → 39.48 µs. | One stream per physical type, null at wrong-type positions. CPU improved; size results varied across datasets. `TYPED-STATS-RESULTS.md`. |
| Use optimized floating-point encodings | Taxi typed PLAIN → ALP: raw 69,519 → 53,920 B; Zstd 26,846 → 26,496 B. BSS kept raw size but grew Zstd to 29,289 B. | DBL encoding experiment; ALP is useful here, BSS is not an automatic win. `DBL-STATS-RESULTS.md`. |
| Page-granularity row ranges | Taxi has 20 row-group ranges versus 3,480 page ranges. Full totals: current row-group-only 5,191 B versus typed ALP/Zstd page-range footer 26,496 B. | Stats-only components were 853 versus 12,299 B; these are NOT full-footer totals. FineWeb text retains chunk-level fallback where page bounds are absent. `PAGE-STATS-GRANULARITY.md`, `STATS-GRANULARITY-SIZE.md`. |
| Broader smart encoding search | Taxi full new page-statistics footer 21,494 B with Zstd, versus current row-group-only 4,871 B. | Different information coverage. Better encodings cannot make this an apples-to-apples size ranking. `FULL-FOOTER-SMART.md`. |
| Conservative 16-byte bounds | Full Zstd totals: US 27,301 → 23,022 B; FineWeb 35,849 → 21,621; HN 67,655 → 17,911; Taxi unchanged at 21,494. | Earlier truncation experiment only. Main matched results keep exact source bounds. `FULL-FOOTER-TRUNCATE16.md`. |
| Pair min/max in one typed stream | Row-group stats + relative offsets: US 11,102 → 10,941 B, FineWeb 17,169 → 16,965, HN 6,771 → 6,624, Taxi 10,854 → 10,705. | Zstd. Both-bound one-column CPU improved 6–24%; e.g. Taxi 9.45 → 8.86 µs. At page granularity pairing saved −281 to +100 B, so it can grow. `PAIRED-BOUNDS.md`. |
| Remove histograms / use presence and paired exactness flags | Thrift definition and wire fixtures passed; no isolated size or CPU delta was measured for each removal. | Included in later matched V2. Earlier paired-bound results use the measured version-12 definition. `encrypted-row-range-stats-definition-validation.json`. |

### Page versus chunk regions, indexes, and field ordering

| Experiment | Measured result | Scope / evidence |
| --- | --- | --- |
| One region per data page | Taxi full Zstd footer: relative-offset layout 21,494 B → page-map-only 19,283 B → derived/shared-range variant 18,971 B. | Same earlier stats policy; progressively different derived-field choices. `PAGE-REGIONS.md`. |
| Explicit page IDs, arbitrary other regions | Taxi 18,971 → 19,189 B; US 20,068 → 20,273 B. | No explicit gap-region markers. All data pages are referenced; arbitrary regions need no classification. `PAGE-REGIONS-EXPLICIT.md`. |
| Reorder field IDs by decoding dependency | US Zstd 20,273 → 20,269 B; other three totals unchanged. | Root serialization only; not a meaningful compression improvement. `PAGE-REGIONS-ORDERED.md`. |
| DataLayout/DataLocation, INT32 indexes | Taxi raw 37,296 → 37,299 B; Zstd 19,189 → 19,192 B. | Grouping and narrowing compared together; encoded widths already adapt, so narrowing is not a large size saving. `DATA-LAYOUT-LOCATION.md`. |
| Compare full versus no-index baselines | Taxi current/modular with indexes: 116,255 / 84,691 B; without indexes: 4,871 / 3,884 B. New page-region footer: 19,192 B with Zstd. | New footer retains page-level information and truncated typed bounds; smaller than full baselines but much larger than no-index modular. Coverage differs. `DATA-LAYOUT-BASELINE-COMPARISON.md`, `NO-PAGE-INDEX-BASELINES.md`. |
| Expand standard/Runtime encoding candidates | Full Zstd totals: US 20,272 → 19,305 B; FineWeb 18,520 → 17,634; HN 16,440 → 14,836; Taxi 19,192 → 17,456. | DBP geometries, dictionary, byte-array delta, ALP/BSS, and PFOR candidates; size-selected, not latency-selected. `ENCODING-SWEEP.md`. |
| Return to chunk regions + optional relative offsets | Taxi page-statistics footer: 19,592 B with relative offsets; 11,617 B without. | Zstd; omission requires scanning page headers to locate interior pages. `CHUNK-REGIONS.md`. |
| Use row-group ranges instead | Taxi row-group-statistics footer: 10,909 B with offsets; 2,931 B without, versus no-index modular 3,884 B. | Zstd; new variant still retains page rows/indexes and earlier truncation policy, so this is not pure L2. `CHUNK-ROW-GROUP-STATS.md`. |
| Make page_index optional | B/C L1/L2 omit the stream; their measured totals are in section 3. | No separate same-input before/after timing ablation. Do not subtract cohort A from B/C. `additional-raincloud-results.json`, `wide-raincloud-results.json`. |

### Proposal cases, validation, and actual codec choices

| Experiment | Measured result | Scope / evidence |
| --- | --- | --- |
| Synthetic wide footer and jump tables | 2,000 columns × 10 groups: current 1,313,914 B; compatible jump 1,394,317; lean jump 1,054,317; modular 646,627; V2 Zstd 70,564; V2 raw 253,352. | Retains row-group stats; timed workload requests placement only. Raw one/all µs: current 8,896.01/11,177.49, modular 372.80/934.58, V2 437.29/1,680.62 with earlier reader. `DOC-CASES.md`, `UNCOMPRESSED-DECODE.md`. |
| Remove paths / lossless prefix sharing | US full row-group footer: current 6,030 B → no paths 5,427 → lossless prefix 5,099. Literal suffix-16 control: 4,368 B. | Literal suffix cutting is size-only and can make pruning unsafe; distinct from conservative truncation above. `DOC-CASES.md`. |
| Tail-fetch dependency replay | 512 KiB initial tail: synthetic current needs 789,626 extra bytes; modular/V2 need none. All four public files fit. | Also replayed 4 KiB and 64 KiB tails. Actual artifact byte dependencies, not measured cloud/SQL latency. `DOC-CASES.md`. |
| All metadata uncompressed | Audit: 330 V2 streams, 385 metadata pages, zero decompressed bytes across 151 V2 projection/scenario checks. | Raw rerun also changes size-selected encodings; not an isolated Zstd latency test. Complete endpoint results in section 4A. `UNCOMPRESSED-DECODE.md`. |
| Additional and wide Raincloud campaigns | 16 sizes each; all 1,760 additional-dataset timing configurations complete. Wide: 16 sizes complete, first-three-dataset 1,056-configuration timing batch preliminary. | Sections 2–4 give results, source geometry, and completion. `ADDITIONAL-RAINCLOUD.md`, `WIDE-RAINCLOUD-COMPLETED-BATCH.md`. |
| Audit selected metadata encodings | Wide raw region offsets choose DBP in all four datasets. Chunk/page indexes choose DBP for Kepler/ClickBench and PFOR for the BI datasets. Kepler DOUBLE bounds use ALP; BI-Wins uses dictionary. | Selections vary by field/page/scenario. PFOR controls replace only two streams; no claim that every stream uses PFOR. `WIDE-RAINCLOUD-ENCODINGS.md`. |
| Footer/column-key encryption contract | Six Thrift roundtrip categories plus crypto fixtures passed, including dictionary + multiple V2 pages and authentication/identity failure cases. | Plaintext/encrypted FileInfo alternatives, protected streams, signed directory, same PAR3 trailer. No encryption performance benchmark or production reader interoperability test. `encrypted-row-range-stats-definition-validation.json`. |

The repository also contains separate baseline studies: the [synthetic placement harness](../pfb-harness/README.md) compares flat/bitpacked/delta layouts, and the [footer decoder benchmark](../footer-decode-bench/README.md) measures name lookup and baseline readers. These are different workloads from the Java-source matched campaign. Its 20,000-column test reports 1,976 µs to resolve one name by schema walk versus 0.13 µs with a hash index; this measures name lookup only.

## 6. What remains unmeasured

| Item | Status at this snapshot |
| --- | --- |
| Final wide decoding, including ClickBench | Running; no final timing summary. Published C timings are preliminary. |
| Column-position impact | First/middle/last and separately timed per-column sweeps are required by the guide but have not run. Averaged projection windows do not answer this. |
| End-to-end query / cloud latency | Not measured. Known-row lookup starts with a known row ordinal; fetch replay reports bytes/dependencies. |
| Encryption size / speed / production interoperability | Not measured; only definition and crypto fixtures validated. |
| Globally best encoding or reader | Not established. The sweep minimizes tested stored size, and the fastest representation depends on access pattern. |
| Other suggested representations | Variant/Arrow metadata layouts, bitmap alternatives and a dedicated PLAIN+LZ4 comparison were not benchmarked. |

## 7. How to double-check a number

The tables above are self-contained. Detailed experiment JSON, raw trials, artifacts and the historical reports named in section 5 are preserved in the original experiment workspace under `footer-pages/`; **they are not bundled in this public directory**. Full reruns require the separate optimized V2 harness dependencies identified in the [playground guide](PLAYGROUND.md).

Primary tables were extracted directly from these saved JSON records:

| ID | Saved file | Records |
| --- | --- | --- |
| S1 | `matched-v21-results.json` | A sizes: sizes[] |
| S2 | `additional-raincloud-results.json` | B sizes and complete timing.summaries[] |
| S3 | `wide-raincloud-results.json` | C sizes and validation; no final timing |
| S4 | `uncompressed-decode-results.json` | A raw timings: timing.summaries[], suite=matched |
| S5 | `wide-raincloud-early-timing-results.json` | C preliminary timings: timing.trials[], suite=matched |
| S6 | `work/wide-raincloud-20261022/core-offsets-no-stats-results.json` | Supplementary sizes[] |
| S7 | `pfor-projection-results.json` | Synthetic planned projection timings |

For sizes, select `dataset` and `scenario_id` (0–3 correspond to L1–L4), then read `artifacts.current.bytes`, `artifacts.modular.bytes`, `v2_uncompressed_control.bytes`, and `artifacts.v2.bytes`. The last is the selective-Zstd variant, not raw.

For times, select the matched suite, dataset, scenario, variant and `projection` (`one` or `100%`); divide `median_ns` by 1,000. V2 is `v2` for S4 and `v2_size_planned` for S2/S5. S7 uses `suite=document`, `dataset=synthetic-2000x10`. The table rounds to two decimal places; underlying nanoseconds and trial distributions are retained.

Untouched Java metadata is larger than some normalized “current” artifacts because descriptive metadata outside the shared workload is removed. Original source totals, **bytes**, remain available for checking:

| Dataset | Java footer + trailer | Java footer + page indexes + trailer |
| --- | --- | --- |
| US Accidents | 14,087 | 112,114 |
| FineWeb | 26,701 | 159,597 |
| Hacker News | 6,388 | 145,918 |
| NYC Taxi | 5,191 | 116,255 |
| Adult | 3,294 | 4,854 |
| Household Power | 2,268 | 30,534 |
| Kepler Search | 34,714 | 41,819 |
| AMPds | 2,557 | 21,633 |
| Kepler Time Series | 669,058 | 816,107 |
| BI-Wins | 655,445 | 2,641,975 |
| BI-USCensus | 1,371,633 | 8,061,159 |
| ClickBench | 898,577 | 22,240,634 |

<details>
<summary>Exact dataset IDs and source-file SHA-256</summary>

| Label | Input ID | Java source SHA-256 |
| --- | --- | --- |
| US Accidents | `us-accidents-00004-of-00007` | `84c3f80256387ea131abbdebcfc1058d96da44a419d8af9a8fce8ce67e344db3` |
| FineWeb | `fineweb-10bt-000` | `0e86a2f86fc87e2478091ced0527e417f3430af2c60552096db176103471f6b4` |
| Hacker News | `hacker-news-00000-of-00039` | `1da9c42134c971ae8c602fd0dd0d49c1b602eae277c360b664d6be8516b12a40` |
| NYC Taxi | `yellow-tripdata-2025-01` | `ea5fa01a908b361b7d1e08a817943480228bcbd0ee924bfb65650778f5269c65` |
| Adult | `uci-adult` | `10460f3ddabdac254beaa1fd0ea74e1b90f5588aafd276aa66ed861f30ce65f8` |
| Household Power | `uci-individual-household-electric-power-consumption` | `f201ed0dcb14d6c6a1030bead53dcd3ceedc386e08cfb10fa2dfbf16810fa702` |
| Kepler Search | `kepler-exoplanet-search-results` | `0e5ab56b1d93652f717aa12318c683c2b425af8b32f5da7f2cd6f58eca574cb4` |
| AMPds | `ampds-whe` | `8e50be3dce6abdee98d6cfd4b71d1585155ce4a1452a5529a58f357475e1f685` |
| Kepler Time Series | `kepler-labelled-time-series-data` | `710f6ac817e670f5cd00c9948a5d570df71fe9beb06efbfb837f49e851e2ee3a` |
| BI-Wins | `bi-wins` | `55081b6678dc6add4c36fdf884b8d6b5b43f08c7cf83b668448bee652f56700f` |
| BI-USCensus | `bi-uscensus` | `154122f16841a7206b980007927e1d279415ebe88edbc3a8a97979defd40a62e` |
| ClickBench | `clickbench-hits` | `d53a14ddccf899a295e741374804f1e75ea28351509ede764df8e893f4dbecb3` |

</details>

<details>
<summary>Result-file SHA-256 at this snapshot</summary>

| ID | SHA-256 |
| --- | --- |
| S1 | `0cf2a34d273dc57c816688fd186634f0e47f7e381318a0f1d60263a86c15012b` |
| S2 | `a2fceee4c9e2e275edba8793dedf0fe0720ba0e63ebf91d1517ff69386f2ab60` |
| S3 | `d874540b085b79964182c6774fad6faa92b03bf5cce87b468c3a1c35b5a60750` |
| S4 | `19f0b50b2971e0e0e00de7e996333890342e35db7053ac879a6c203aa26b319a` |
| S5 | `b412a984d03763026c6b95273196be0177815421ac5950e7f95f78cefbf5fe24` |
| S6 | `e620466612df8b6bbff8efc69c4333f86bb28d76b6f081cae36242ed59c2a2d8` |
| S7 | `de3d2530f111984c177f7dd70ed69168f6f3cf1b3af48a6f70df0535d75dfd9b` |

S3 may later acquire final timing records; its hash identifies the size/validation snapshot used here.

</details>

Codec references: [Runtime DBL Parquet encodings](https://github.com/databricks-eng/runtime/tree/ce6aeb66fdfb1362888367c2535bc85ef7c7b7ae/dbl/parquet), [PFOR PR #267100](https://github.com/databricks-eng/runtime/pull/267100), and the pinned revisions/build instructions in the [playground guide](PLAYGROUND.md#5-encoding-and-decoding-implementations). Runtime access is required for the optimized implementations. “Best encoding” throughout means the smallest serialized candidate tested; experimental encoding IDs require the matching readers.
