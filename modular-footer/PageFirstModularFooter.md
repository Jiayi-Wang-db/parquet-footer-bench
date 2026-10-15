# Page-First Modular Footer Metadata

**Status: design sketch and prototype**

The transition is split into two schemas:

- [`PageFirstModularFooterAddendum.thrift`](PageFirstModularFooterAddendum.thrift) describes PAR1
  files. It is a small addendum to the current modular-footer proposal: row groups remain, while
  placement uses region ordinals and page indexes become flat per-column modules.
- [`PageFirstModularFooterFull.thrift`](PageFirstModularFooterFull.thrift) describes PAR3 files.
  It removes row groups and column chunks, permits non-contiguous page layouts, and replaces both
  row-group and page statistics with one row-range statistics model.

The two schemas deliberately make PAR1 versus PAR3 semantics explicit in the file magic. There is
no optional field whose absence silently changes the interpretation of a PAR1 file.

## Common region map

A region is an independently addressable physical byte interval. In PAR1, every complete column
chunk is exactly one region and its pages are addressed relative to that region. In PAR3, every
complete serialized Parquet page, including its `PageHeader` and body, is exactly one region.
Gaps, extensions, and metadata modules may also be regions.

For `R` regions, the common region map contains `R + 1` absolute file offsets:

```text
region r = [region_offsets[r], region_offsets[r + 1])
```

`region_offsets` is one `binary` containing strict Parquet `DELTA_BINARY_PACKED`. The fixed trailer
locates the root and region map directly; every other physical reference is a region ordinal.
Readers decode and retain the region map before resolving page or module regions.

PAR3's page directory contains one descriptor region ordinal per leaf column. Selecting a column
therefore does not read another column's page metadata. Within a selected column, its sequential
arrays align one-for-one with logical data pages:

```text
region_ordinals[]
first_row_indexes[]
codecs[]
dictionary_regions[]
num_values[]
total_uncompressed_sizes[]
```

Each uses the smaller of fixed-width bit-packing and strict DBP. The encoding and element count
are self-describing inside the `EncodedArray.values` binary. `dictionary_regions` stores a region
ordinal plus one, with zero meaning no dictionary. Dictionary pages are regions but do not occupy
logical data-page entries. Exact page encodings remain in page headers;
`is_fully_dictionary_encoded` is one summary boolean for the complete column.

A schema-present flat column with zero page entries is all-null over the file's `num_rows`. This is
a convention, not another metadata field. Nested or repeated leaves may be omitted only when their
definition and repetition structure remains reconstructible.

PAR3 needs no Offset Index module: region ordinals provide physical page ranges and
`first_row_indexes` provide logical page locations.

## PAR1 addendum

PAR1 retains row groups as an explicit physical and logical contract. Its `RegionMap` requires:

```text
row_group_region_ordinals[N + 1]
row_group_num_rows[N]
```

Both are strict DBP streams. Row group `i` spans regions
`[row_group_region_ordinals[i], row_group_region_ordinals[i + 1])` and contains
`row_group_num_rows[i]` top-level rows. The root retains `num_row_groups`; readers validate both
array counts and the sum of row counts. A traditional PAR1 file therefore has a direct unified
representation without deriving row counts from page metadata.

The addendum otherwise keeps the current modular proposal's schema, file-metadata, schema-index,
and row-group `ColumnStatistics` structures. Statistics descriptors are addressed by region
ordinal instead of byte offset.

Each PAR1 column chunk is one region. The page directory contains one independently addressable
descriptor per leaf column, with parallel chunk-region, page-count, uncompressed-size, value-count,
codec, and dictionary-encoding arrays indexed by row-group ordinal. This keeps the always-read core
at one entry per column chunk rather than one entry per page.

The chunk-oriented Offset Index is replaced with one `FlatOffsetIndex` descriptor per participating
leaf column. Its arrays contain data-page offsets relative to each chunk-region start and file-global
first-row indexes. Prefix-summing the page counts gives each chunk's slice of these arrays. Page
sizes are derived from the next relative offset or the chunk-region end.

The chunk-oriented Column Index is replaced with one `FlatColumnIndex` descriptor per participating
leaf column. Page arrays align with that column's `FlatOffsetIndex` sequence. Existing PAR1 chunks
without a Column Index create sparse positions in those arrays. `boundary_orders` retains one value
per row group because ordering may restart at every row-group boundary.

To locate a page for `(column, row)`, prefix-sum the top-level row-group counts to select the row
group, decode that column's page directory, and read the selected chunk region and page count. The
prefix sum of preceding page counts gives the chunk's first ordinal in the flat Offset Index. A
search over that chunk's first-row slice selects the page; adding its relative offset to the chunk
region start gives the physical page offset.

In short:

```text
current modular
  - column-chunk placement
  - chunk-oriented Offset Index
  - chunk-oriented Column Index
  + one region per column chunk
  + per-column chunk placement and page counts
  + flat per-column Offset Index
  + flat per-column Column Index
```

Row-group statistics remain unchanged.

## PAR3 full footer

PAR3 removes `num_row_groups`, both row-group arrays, row-group statistics, and Column Index. It
uses independently selected, per-column row-range statistics. A participating column has one
descriptor whose `range_start_rows` begins at zero, is strictly increasing, and defines a dense
partition ending at file `num_rows`.

Each range may publish null counts, NaN counts, and/or min/max. Min and max share one presence bit:
both are present or both are absent. Common-prefix lengths, interleaved min/max suffix lengths,
concatenated prefix and suffix bytes, and interleaved exactness bits avoid per-value Thrift
framing. Statistics sparsity across columns is localized to the top-level directory with an
optional presence bitmap and a packed list of descriptor region ordinals.

`boundary_orders` is one flat two-bit value per participating column. It describes the complete
row-range sequence for that column and is `UNORDERED` unless global ascending or descending order
holds. It cannot preserve row-group-local ordering because PAR3 has no row-group boundaries.

A writer that wants page-level pruning publishes row ranges that tile data pages. Placement maps a
matching row range to pages through absolute `first_row_indexes`, so PAR3 needs no separate Column
Index.

## Raincloud parquet-java benchmark corpus

The benchmark rewrites the four pinned Raincloud inputs through Raincloud commit `7357517`'s actual
`parquet@java` sidecar target (parquet-java 1.17.1), with the target defaults: ZSTD, 128 MiB encoded
row-group target, 1 MiB pages, 20,000-row page limit, dictionaries, statistics, page checksums, and
page indexes. All four sidecar self-verifications round-trip to their canonical Arrow input.

Offset Index is present for every column chunk. Column Index is present for every chunk except the
11 FineWeb chunks for a column without publishable page statistics.

| Dataset | Chunks | Data pages | Dictionary pages | Offset Index chunks | Column Index chunks |
| --- | ---: | ---: | ---: | ---: | ---: |
| US Accidents | 46 | 2,620 | 26 | 46 | 46 |
| FineWeb | 99 | 3,513 | 44 | 99 | 88 |
| Hacker News | 28 | 1,022 | 12 | 28 | 28 |
| Yellow Taxi | 20 | 3,480 | 20 | 20 | 20 |

Sizes use a 16-byte min/max suffix limit. Each modular tier is serialized independently, including
its own directories, region map, root, and framing. Fixed magic/length trailers are excluded.
The source page-index ranges are non-overlapping and form one contiguous span immediately before
the Thrift footer in all four files.

| Dataset | Thrift core + stats | Thrift core + stats + indexes | Modular core | Modular core + stats | Modular core + stats + indexes | PAR1 core | PAR1 core + stats | PAR1 core + stats + indexes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| US Accidents | 14,079 B | 112,106 B | 8,919 B | 12,229 B | 82,490 B | 10,997 B | 14,276 B | 60,145 B |
| FineWeb | 26,693 B | 159,589 B | 2,826 B | 6,653 B | 69,860 B | 2,698 B | 6,508 B | 45,416 B |
| Hacker News | 6,380 B | 145,910 B | 3,317 B | 4,736 B | 38,058 B | 3,868 B | 5,266 B | 30,013 B |
| Yellow Taxi | 5,183 B | 116,247 B | 2,942 B | 4,352 B | 98,573 B | 3,827 B | 5,241 B | 67,016 B |

| Dataset | Thrift footer | Thrift full |
| --- | ---: | ---: |
| US Accidents | 14,079 B | 112,106 B |
| FineWeb | 26,693 B | 159,589 B |
| Hacker News | 6,380 B | 145,910 B |
| Yellow Taxi | 5,183 B | 116,247 B |

| Dataset | Current modular core | Core + RG stats | vs Thrift footer | Current modular full | vs Thrift full |
| --- | ---: | ---: | ---: | ---: | ---: |
| US Accidents | 8,919 B | 12,229 B | −13.1% | 82,490 B | −26.4% |
| FineWeb | 2,826 B | 6,653 B | −75.1% | 69,860 B | −56.2% |
| Hacker News | 3,317 B | 4,736 B | −25.8% | 38,058 B | −73.9% |
| Yellow Taxi | 2,942 B | 4,352 B | −16.0% | 98,573 B | −15.2% |

| Dataset | PAR1 addendum core | Core + RG stats | vs Thrift footer | PAR1 addendum full | vs Thrift full |
| --- | ---: | ---: | ---: | ---: | ---: |
| US Accidents | 10,997 B | 14,276 B | +1.4% | 60,145 B | −46.4% |
| FineWeb | 2,698 B | 6,508 B | −75.6% | 45,416 B | −71.5% |
| Hacker News | 3,868 B | 5,266 B | −17.5% | 30,013 B | −79.4% |
| Yellow Taxi | 3,827 B | 5,241 B | +1.1% | 67,016 B | −42.4% |

PAR1 core uses one region per column chunk and per-column chunk descriptors. Full adds flat
per-column Offset Index and Column Index modules.

| Dataset | PAR3 core | Core + row-range stats | PAR3 full | vs Thrift full |
| --- | ---: | ---: | ---: | ---: |
| US Accidents | 26,286 B | 61,586 B | 61,586 B | −45.1% |
| FineWeb | 29,071 B | 52,466 B | 52,466 B | −67.1% |
| Hacker News | 12,561 B | 32,148 B | 32,148 B | −78.0% |
| Yellow Taxi | 18,366 B | 61,647 B | 61,647 B | −47.0% |

PAR3 core + stats and full are byte-identical: page placement subsumes Offset Index and
page-granular row-range statistics subsume Column Index. It is compared with Thrift full.

Machine-readable size results are in
[`page-first-split-size.csv`](../real-footer-size/page-first-split-size.csv).

## Decode benchmark

The C++ harness uses the production `runtime/dbl/parquet` DBP decoder. Inputs are already in
memory, each operation runs long enough to accumulate roughly 200 ms, and every placement result is
fidelity-checked against the current footer before timing. These numbers are from an Intel Xeon
6975P-C and should be read as trends rather than portable absolute latency.

For one projected column, placement-only decode is:

| Dataset | Thrift full | Current modular full | PAR1 addendum full | PAR3 full |
| --- | ---: | ---: | ---: | ---: |
| US Accidents | 71.695 µs/op | 0.786 µs/op | 0.736 µs/op | 3.313 µs/op |
| FineWeb | 518.593 µs/op | 35.475 µs/op | 9.613 µs/op | 25.629 µs/op |
| Hacker News | 52.317 µs/op | 0.789 µs/op | 0.955 µs/op | 2.322 µs/op |
| Yellow Taxi | 53.838 µs/op | 2.178 µs/op | 0.997 µs/op | 3.833 µs/op |

Placement plus applicable statistics is:

| Dataset | Thrift full | Current modular full | PAR1 addendum full | PAR3 full |
| --- | ---: | ---: | ---: | ---: |
| US Accidents | 77.803 µs/op | 2.542 µs/op | 1.630 µs/op | 4.603 µs/op |
| FineWeb | 519.678 µs/op | 35.710 µs/op | 9.943 µs/op | 25.645 µs/op |
| Hacker News | 57.972 µs/op | 2.403 µs/op | 1.700 µs/op | 2.591 µs/op |
| Yellow Taxi | 65.721 µs/op | 6.601 µs/op | 2.776 µs/op | 5.259 µs/op |

The placement-only workload decodes projected Offset Index for current Thrift/current modular.
PAR1 decodes its projected chunk directory and flat relative Offset Index; PAR3 decodes projected
page placement. The statistics workload additionally decodes projected Column Index; current
modular and PAR1 also decode row-group statistics, while PAR3 decodes its row-range descriptor.
FineWeb's first projected column has no Column Index or row-range descriptor, so its one-column
statistics timing is nearly identical to placement. The checked-in CSVs include projection sweeps
through all columns.

## Open questions

- Whether the complete region map always fits the speculative-tail budget or needs block-level
  framing while remaining mandatory.
- Representation of data pages that begin in the same repeated top-level row.
- Whether optional column summaries are needed for resource planning in addition to per-page
  values.
- Page and module encryption identities, integrity digests, and unknown-module preservation.
