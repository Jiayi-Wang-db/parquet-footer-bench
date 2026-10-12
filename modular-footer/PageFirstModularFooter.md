# Page-First Modular Footer Metadata

**Status: design sketch**

This sketch supersedes the earlier encoding-domain prototype. It adapts the modular footer to the
page-first data model by separating physical byte addressing from logical page metadata. Readers
can address a projected column directly, then sequentially decode compact metadata for that column.

## Design principles

- Every serialized Parquet page, including its `PageHeader` and body, is exactly one physical
  region. A region never contains more than one page.
- Other independently addressable objects, including metadata modules, extensions, padding, and
  stashed compatibility data, may also be regions.
- The region map contains physical boundaries only. It does not assign columns, rows, page kinds,
  encodings, or other semantics to a region.
- All structures other than the fixed bootstrap trailer and root refer to file bytes by region
  ordinal rather than repeating byte offsets and lengths.
- Page ownership, logical order, row coverage, compression, and dictionary dependencies live in
  independently addressable column metadata.
- Cross-column directories use fixed-width bit-packing. After selecting a column, arrays may be
  decoded sequentially and use whichever of bit-packing or DBP is smaller.
- Statistics sparsity is local to the statistics structures; the generic `EncodedArray` carries no
  presence or random-access machinery.
- `PageFirstModularFooter.thrift` includes only the standard `parquet.thrift`; it does not include
  or reference the current modular-footer definition.

## Regions

The region map is an ordered array of absolute file offsets:

```text
region r = [region_offsets[r], region_offsets[r + 1])
```

For `R` regions, `region_offsets` contains `R + 1` strictly increasing values. Absolute offsets use
Parquet's standard `DELTA_BINARY_PACKED` encoding with 128-value blocks and four 32-value
miniblocks. Typical page and module sizes therefore become small deltas. The encoded stream is
sequential; decoded entries are absolute boundaries.

Every page boundary must appear in the array. An unreferenced region represents padding, a gap, or
an object unknown to the reader. Several entities may refer to the same complete region. Partial
sharing requires splitting at every referenced endpoint and representing the entity as a sequence
of regions.

Regions are metadata addressing units, not I/O request units. A reader may fetch a byte range that
covers several adjacent regions, but page identity, validation, dependencies, and decoding remain
separate.

The fixed trailer is the bootstrap exception. It locates the root and region map directly. Once the
region map is available, module-directory entries and all other locators use region ordinals.

## Required modules

```text
PageFirstModularFooter
├── Schema
├── RegionMap
├── PageDirectory
└── optional modules
    ├── RowRangeStatistics
    ├── FileMetadata
    ├── SchemaIndex
    ├── BloomFilters
    └── extensions
```

The root retains `version`, `num_columns`, `num_rows`, and the module directory. It no longer
contains `num_row_groups`.

### Region map

```thrift
struct RegionMap {
  /** UINT64 absolute offsets; num_regions + 1 strictly increasing entries. */
  1: required binary region_offsets
}
```

`region_offsets` uses strict Parquet `DELTA_BINARY_PACKED`. Readers decode and retain the complete
array before resolving region ordinals.

### Page directory

The page directory locates an independently serialized page descriptor for each leaf column.
Readers can therefore load page metadata in proportion to projection rather than decoding every
column's logical page arrays. Its locator field is a fixed-width bit-packed binary with one region
ordinal per leaf column, supporting direct access by column ordinal.

Each column descriptor contains one dictionary-encoding summary and six self-describing encoded
arrays:

```text
is_fully_dictionary_encoded
region_ordinals[]
first_row_indexes[]
codecs[]
dictionary_regions[]
num_values[]
total_uncompressed_sizes[]
```

Entries are in logical data-page order. Row indexes are absolute, zero-based top-level file row
indexes. `dictionary_regions` stores the directly referenced dictionary region plus one, with zero
meaning no dictionary. Dictionary pages remain physical regions but do not occupy descriptor rows.
A dictionary is compressed with the codec of the referring data page; all references to the same
dictionary must therefore agree on codec. Dictionary encoding, value count, and uncompressed size
remain available in its `PageHeader`.

Each array independently uses the smaller of fixed-width bit-packing and Parquet
`DELTA_BINARY_PACKED`. Its single binary starts with an encoding byte. A bit-packed payload then
stores a ULEB128 value count, its bit width, and LSB-first values; a DBP payload contains the
standard self-counting DBP stream. The selected column is decoded sequentially. There is no RLE,
sparse presence, default value, domain table, or other coalescing. Value count and uncompressed
size remain physically repeated once per data page. Physical region ordinals need not be monotonic.

`is_fully_dictionary_encoded` summarizes the complete column rather than each column chunk. Exact
per-page encodings remain in page headers; direct dictionary-region references identify the pages
that use a dictionary. Physical type remains in the schema. Total compressed bytes are derived from
region boundaries; value and uncompressed-byte totals are derived from the per-page arrays.

A schema-present column whose page arrays contain zero values represents an all-null column over
the root's `num_rows`; it is not a missing column. Its packed arrays are empty and
`is_fully_dictionary_encoded` is false. This convention permits writers to omit physical pages for
flat all-null columns. A nested or repeated leaf may be omitted only when its repetition and
definition structure can be reconstructed without that leaf's pages; otherwise it still requires
physical pages.

## Optional metadata

### Column-range statistics

The current row-group statistics module is replaced by independently selected per-column row-range
statistics. Ranges use the same absolute top-level row coordinate as page metadata. Statistics
boundaries do not close pages or dictionaries.

Each present leaf column has one independently addressable descriptor. The statistics directory
has an optional LSB-first presence bitmap and a compact fixed-width packed array of descriptor
region ordinals in column order. The bitmap is omitted when every column is present.
A reader can test membership directly and obtain a compact-array index using popcount over the
preceding bitmap bits. It may materialize the complete column-to-region lookup once rather than
store rank checkpoints on disk. A column without statistics has no descriptor entry.

A present column must cover every row in the file with a dense partition of statistics blocks. Its
descriptor stores only absolute `range_start_rows`, beginning with zero and strictly increasing.
Block `i` ends at start `i + 1`; the final block ends at the root's `num_rows`.

For example, a 10,000-row column partitioned at rows 1,000, 3,000, and 5,000 stores:

```text
range_start_rows = [0, 1000, 3000, 5000]
```

This describes `[0, 1000)`, `[1000, 3000)`, `[3000, 5000)`, and `[5000, 10000)`.
`range_start_rows` uses the local adaptive `EncodedArray`, so locating a column descriptor is O(1)
but decoding ranges within that column is sequential. Null and NaN counts are optional adaptive
arrays containing one value per range when present. Min/max shares one presence bit per range.
Prefix lengths and interleaved min/max suffix lengths are adaptive arrays; prefix and suffix bytes
are stored in separate concatenated binaries. Exactness is an interleaved min/max bitset and is
omitted when every published bound is exact. Every block must publish at least one statistic. Each
column chooses its own dense partition, without a shared granularity, preamble, or row-group
identity.

There is no separate page-level `ColumnIndex` module. A writer that wants page-granular pruning
publishes row-range blocks that tile its pages. Placement maps matching row ranges to pages using
absolute `first_row_indexes`. This keeps one statistics model for both legacy row-group files and
page-first layouts.

### Other modules

Bloom filters, file metadata, schema indexes, compatibility metadata, and future extensions are
independently serialized regions. Their directory entries contain region ordinals rather than
absolute offset/length pairs.

## Replacement of current modular placement

| Current `PlacementModule` field | Page-first representation |
| --- | --- |
| `data_page_offsets` | Data-page region ordinals plus `RegionMap` |
| `first_dictionary_pages` | Direct per-page dictionary-region references |
| `dictionary_page_offsets` | Direct dictionary region ordinals plus `RegionMap` |
| `total_compressed_sizes` | Derived from referenced region boundaries |
| `total_uncompressed_sizes` | Sum of per-page uncompressed sizes |
| `num_values` | Sum of per-page value counts for data pages |
| `codecs` | Per-column adaptive codec stream |
| `physical_types` | Schema |
| `is_fully_dictionary_encoded` | One boolean for the complete column |
| `row_group_num_rows` | Removed; pages use absolute file row indexes |

The optional `OFFSET_INDEX` module also disappears. The region map supplies page byte ranges, while
the page directory supplies logical order and first-row indexes.

## Compatibility

This design requires a new footer/version boundary. The legacy footer cannot express independent
page placement, absolute page row coordinates, or multiple dictionaries. A legacy footer may
be retained as an opaque region during a transition, but it is authoritative only when the physical
layout is representable by the legacy row-group and column-chunk model.

Encryption AADs must identify pages by stable logical identity, such as `(column ordinal, logical
page ordinal)`, rather than by row-group ordinal or physical region ordinal.

## Open questions

- Whether `region_offsets` needs a separate bounded-random-access index; standard Parquet
  `DELTA_BINARY_PACKED` is sequential.
- Whether the complete region map always fits the speculative-tail budget or needs block-level
  framing while remaining mandatory.
- Representation of data pages that begin in the same repeated top-level row.
- Whether optional column summaries are required for resource planning in addition to per-page
  values.
- Page and module encryption identities, integrity digests, and unknown-module preservation.
- Representation of sorting guarantees after removing row groups.

## Flat-array Raincloud size results

The prototype in the local `parquet-footer-bench` clone scans every real page header in the four
files used by the modular-footer proposal. It synthesizes a current-format Thrift `OffsetIndex` for
every column chunk, adds the corresponding offset/length fields to the current footer, and converts
the same file to the page-first modules above.

| Dataset | Column chunks (count) | Data pages (count) | Dictionary pages (count) | Total pages (count) | Current footer (bytes) | Current footer + offset indexes (bytes) | Column-chunk placement (bytes) | Page placement (bytes) | Placement change | Complete column-chunk footer (bytes) | Complete page footer (bytes) | Complete change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| US Accidents | 50,830 | 50,830 | 36,465 | 87,295 | 4,747,144 | 5,872,211 | 694,618 | 277,795 | −60.0% | 1,764,414 | 892,344 | −49.4% |
| FineWeb 10BT | 9,441 | 9,441 | 9,441 | 18,882 | 3,971,883 | 4,187,395 | 158,380 | 63,643 | −59.8% | 530,722 | 337,076 | −36.5% |
| Hacker News | 15,022 | 15,022 | 12,876 | 27,898 | 1,840,138 | 2,177,310 | 223,029 | 115,141 | −48.4% | 598,045 | 356,204 | −40.4% |
| Yellow Taxi | 80 | 145 | 80 | 225 | 11,212 | 13,868 | 1,404 | 3,289 | +134.3% | 5,877 | 7,461 | +27.0% |

| Dataset | Current modular row-group statistics (bytes) | Page-first row-range statistics (bytes) | Change |
| --- | ---: | ---: | ---: |
| US Accidents | 1,059,755 | 604,393 | −43.0% |
| FineWeb 10BT | 371,353 | 272,436 | −26.6% |
| Hacker News | 371,678 | 237,702 | −36.0% |
| Yellow Taxi | 2,265 | 1,921 | −15.2% |

`Current footer + offset indexes` includes the serialized index blobs and the growth of the Thrift
footer from adding each chunk's index offset and length. The index blobs alone are 718,427 bytes,
139,984 bytes, 216,996 bytes, and 2,090 bytes respectively.

`Column-chunk placement` is the current modular proposal's serialized `placement` module alone;
`Complete column-chunk footer` includes all modules emitted by Jiayi's converter. `Page placement`
includes the DBP region-offset map, page directory, one per-column dictionary-encoding boolean,
and six per-data-page arrays that independently select bit-packing or DBP. Physical types remain in
the shared schema and `num_rows` remains in the shared root, so neither whole structure is charged
to page placement. `Complete page footer` additionally includes schema, converted row-range
statistics, file metadata, and the root. It covers the same module categories used in the current
modular-footer comparison.

The three large files have exactly one data page per column chunk. A Thrift offset index adds 211
KiB to 1.07 MiB after locator growth. Adaptive per-page streams make page placement 48% to 60%
smaller than column-chunk placement on the three large files. Yellow Taxi has only a few pages per
column; selecting bit-packing cuts its placement increase from 418% to 134%. Adaptive count and
length arrays plus split prefix/suffix framing make page-first statistics 15% to 43% smaller than
current modular on these files.

These measurements use the legacy physical layout. Each of the six per-column arrays emits the
smaller of its bit-packed and strict DBP forms. There are no runs, defaults, or shared dictionaries
outside the values represented by those streams.

## Placement decode results

The C++ footer-decode harness has a fidelity-checked page-first resolver. `Page cold` decodes the
mandatory DBP region offsets on each operation; `Page warm` retains the absolute offsets and
measures projected column descriptors. Inputs are already in
memory; each case runs for roughly 200 ms. Absolute timings are specific to an Intel Xeon 6975P.

At a one-column projection, the placement-only Raincloud workload is:

| Dataset | Current Thrift (µs/op) | Current modular (µs/op) | Page cold (µs/op) | Page warm (µs/op) |
| --- | ---: | ---: | ---: | ---: |
| US Accidents | 77,139.652 | 28.500 | 115.041 | 11.633 |
| FineWeb 10BT | 11,711.905 | 26.519 | 25.176 | 11.408 |
| Hacker News | 18,720.695 | 27.143 | 43.727 | 11.223 |
| Yellow Taxi | 101.450 | 0.128 | 0.514 | 0.220 |

Statistics are intentionally excluded from the page-first timing comparison. The row-range layout
above is included in footer-size accounting, but its decode workload has not been implemented or
measured.

`Current Thrift` fully materializes the current Thrift footer into an object tree. `Page cold`
includes the mandatory DBP region-offset decode; `Page warm` retains the resulting offsets. In the
placement-only workload, retained-map page-first takes 0.41-0.43x current modular time on the three
large files and 1.72x on Yellow Taxi. These results do not include I/O, page-header decoding,
codec-stream decoding, or statistics. `Current Thrift + OffsetIndex` is a size comparison, not a
separate resolver here: the index is stored outside the footer and this workload still requires
parsing current Thrift metadata.
