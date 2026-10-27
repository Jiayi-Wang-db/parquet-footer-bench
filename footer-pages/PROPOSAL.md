# Proposal: extending the modular footer with flexible layouts and typed metadata streams

Status: design draft for discussion, based on the [current modular footer](../modular-footer/ModularFooter.thrift). The [complete proposed Thrift definition](proposal/ModularFooterV2.thrift) accompanies this document and imports only standard Parquet types from `parquet.thrift`.

## Changes from the current modular footer, and why

The current modular footer separates placement, statistics, page indexes, and descriptive metadata into independently readable modules. It already uses typed arrays, direct chunk offsets, and positional indexing. This proposal keeps those foundations and changes the following:

| Area | Current modular footer | Proposed change | Why |
|---|---|---|---|
| **Column/chunk layout** | Chunk positions use `column * num_row_groups + row_group`, with one chunk per column per row group. | Use one shared `column_chunk_index`; row-group metadata becomes optional. | Allow each column to have its own chunk count, including zero, so dense and sparse columns can be organized independently. |
| **Page metadata** | Each chunk has separate offset-index and column-index descriptors, located through `PageIndexModule.chunk_offsets`. Page locations store offsets and sizes. | Flatten page fields under one shared `page_index`. Store chunk-relative page offsets, derive page lengths from the next offset or chunk end, and use file-relative row positions. | Reuse the same page positions across locations, row positions, and statistics; remove per-chunk descriptors and support layouts without row groups. |
| **Statistics ranges** | Separate `ColumnStatistics` and `ColumnIndexChunk` structures describe row-group and page statistics. | Use one statistics structure tagged `COLUMN_CHUNK`, `PAGE`, or `ARBITRARY`; allow multiple range sets. | Support statistics whose row ranges differ from physical chunk/page boundaries. Chunk/page tags reuse existing indexes and row starts instead of storing them again. |
| **Dictionary locations** | `first_dictionary_pages` indexes a flattened list of dictionary offsets; placement also stores per-chunk total sizes. | Keep direct data offsets, give data and dictionaries separate lengths, and reference a distinct dictionary table by nullable chunk ID. | Store a dictionary extent once for reuse within a column, and locate data and dictionary bytes independently. This draft supports at most one dictionary per chunk. |
| **Array encoding** | Inline `EncodedArray` values use seekable `BITSET` or `PRESENT_INDEX` encodings, without compression or cross-value dependence. | Store arrays as typed Parquet metadata page streams, with existing encodings and optional compression. | Reuse Parquet encoding implementations and exploit repeated or regular metadata values to reduce size. The tradeoff is decoding a block or page to reach some values. |
| **Statistics values** | Min/max bounds use byte-array prefixes and suffixes with dedicated common-prefix handling. | Store paired min/max values in streams of their physical type. | Apply ordinary typed encodings to bounds and avoid a separate min/max prefix representation. |
| **Metadata directory** | The root locates modules; statistics and page-index modules locate further descriptors containing inline arrays. | Put shared stream locations directly in the footer directory. | Reduce descriptor lookups and let readers fetch individual fields. This moves more descriptors into the initial directory. |

Direct chunk offsets are retained from the current modular footer. The source schema is still stored once, and placement remains readable without statistics.

The main tradeoff is size versus access granularity. Explicit indexes locate logical positions without reconstructing list boundaries, but encoded streams do not guarantee the current modular footer's direct positional access to packed values. Shared streams may also require reading metadata for unselected columns. [Optional column sets](#optional-future-extension-column-sets) could limit that work for exceptionally wide files; the base proposal requires no column-set wrapper or membership array.

## Representation

The footer directory contains `FileInfo`, `DataLayout`, `DictionaryMetadata`, `ColumnChunkMetadata`, and optional page, row-group, statistics, and key/value metadata. `DictionaryMetadata` stores dictionary offsets and lengths; `PageMetadata` stores independently optional page offsets and row positions. The source schema is stored once in `FileInfo`. Each array field contains a `StreamLocation`:

```thrift
struct StreamLocation {
  1: required i64 offset;
  2: required i64 length;
  3: required parquet.CompressionCodec codec;
}

struct DataLayout {
  1: required StreamLocation column_chunk_index;
  2: optional StreamLocation page_index;
}
```

Each stream contains ordinary Parquet `PageHeader`/body pairs: an optional dictionary page followed by `DATA_PAGE_V2` pages. These are **metadata pages**, separate from the source-data pages they describe. The directory points directly to streams; there is no intermediate module descriptor for each shared array.

A field's ID defines its physical type, nullability, and logical domain. No additional metadata schema tree is serialized. In this proposal, `list<T>` describes an array stored in a stream, not an inline Thrift list or a nested Parquet LIST. Thrift `optional` controls whether the entire stream exists; nullable elements independently represent missing values.

Streams of non-null scalar values have no repetition or definition levels. Nullable scalar streams use definition levels for presence and no repetition levels. Their page headers count positions in the metadata array, including nulls, rather than rows in the source data. Encodings and compression are chosen per the standard page/stream rules; this proposal does not require a new value encoding.

## Two shared hierarchy indexes

Let `C` be the number of physical columns, `K` the total number of chunks, and `P` the total number of source-data pages.

| Field | Type and number of values | Nullability | Meaning |
|---|---|---|---|
| `column_chunk_index` | `list<i32>`, `C` | Non-null | Column `c` ends at `index[c]`; starts at `0` if `c = 0`, otherwise `index[c-1]`. |
| `page_index` | `list<i32>`, `K` | Non-null; stream optional | Chunk `k` ends at `index[k]`; starts at `0` if `k = 0`, otherwise `index[k-1]`. |
| Chunk-level fields | `list<T>`, `K` | Defined per field | Ordered by column, then chunk. |
| Page-level fields | `list<T>`, `P` | Defined per field | Ordered by flattened chunk, then page. |

Both indexes store nondecreasing cumulative ends, with the initial zero implicit. When nonempty, their final entries are `K` and `P`. An empty column index means `C = K = 0`; an empty, present page index means `K = P = 0`. An absent page index does not imply zero pages. Repeated ends represent an empty column or a chunk with no pages; a first entry of zero represents an empty first column or chunk and must still be stored. Counts are the current end minus the previous end, taking the previous end as zero for the first entry. `K` and `P` must fit in signed `i32`.

Columns may have different chunk counts, including zero. There is no requirement that every column have one chunk per row group. Optional row-group metadata describes layouts that use row groups; it does not define the chunk index.

All fields in a domain preserve the same logical positions. For example, a chunk without a dictionary has a null entry in `dictionary_ids`; it does not disappear from that array. Metadata-page boundaries may differ between streams because the indexes address logical positions, not encoded byte offsets.

Example: two columns with two and one chunks, containing five pages in total:

```text
column_chunk_index:    [2, 3]
page_index:            [2, 3, 5]
data_offsets:          [1000, 1400, 2000]
data_lengths:          [200, 100, 300]
dictionary_ids:        [0, 0, null]
dictionary_offsets:    [800]
dictionary_lengths:    [100]
relative_page_offsets: [0, 100, 0, 0, 200]
chunk_first_rows:      [0, 100, 0]
page_first_rows:       [0, 50, 100, 0, 80]
```

The equivalent logical page-offset grouping is `[[[0, 100], [0]], [[0, 200]]]`. Its hierarchy is stored once in the two prefix arrays, rather than repeated as RL/DL in each leaf.

## Byte locations

`ColumnChunkMetadata.data_offsets` and `data_lengths` are non-null `list<i64>` streams with `K` values each. Chunk `k` occupies `[data_offsets[k], data_offsets[k] + data_lengths[k])`, containing its contiguous complete data pages, including headers and excluding dictionary bytes. Offsets are absolute and lengths are explicit. No region map, chunk adjacency, fixed row-group layout, or file-order chunk traversal is required.

`DictionaryMetadata.dictionary_offsets` and `dictionary_lengths` form a table with `D` non-null `i64` values in each stream. Entry `d` locates one complete dictionary page, including its header. `ColumnChunkMetadata.dictionary_ids` has `K` nullable `i32` values: null means no dictionary; otherwise the ID is in `[0,D)`. Several chunks of the same physical column may reference one entry, provided their dictionary decoding is compatible. Cross-column dictionary sharing is not permitted. Table entries are stored once and need not follow file order; assigning IDs in first-reference order is a writer choice that encoded well in the prototype. With no dictionaries, both table streams are empty and all chunk IDs are null.

`PageMetadata.relative_page_offsets` contains one non-null `i64` per page, relative to its chunk's `data_offsets[k]`. The first offset of each nonempty chunk is zero. A page ends at the next page offset in that chunk, or at `data_offsets[k] + data_lengths[k]` for the last page.

The page-offset and page-row streams are independently optional. Each present stream has exactly `P` positions; the offset stream includes the zero for a single-page chunk. A reader that knows a chunk has one page can derive its byte extent from the chunk's offset and length without reading that offset. Omitting the first offset from storage would require remapping page positions for the shorter offset array, including handling empty chunks; this draft keeps those positions aligned with other page fields. `page_index` is required whenever `PageMetadata` is present. PAGE statistics require `page_first_rows`; omit `PageMetadata` when both streams are absent.

Optional row-group metadata retains row counts. Its optional `row_group_offsets` stream holds `G + 1` absolute byte boundaries only when complete row groups occupy contiguous intervals in row-group order. Interleaved layouts or dictionaries shared across row groups can omit this byte-range hint; chunk and dictionary locations remain sufficient.

## Statistics reuse the hierarchy

Each statistics set has one tag, ordered as follows:

```thrift
enum RowRangeKind {
  COLUMN_CHUNK = 0,
  PAGE = 1,
  ARBITRARY = 2
}
```

| Tag | Number of range positions | Column ownership | Row starts |
|---|---|---|---|
| `COLUMN_CHUNK` | `K` | `column_chunk_index` | Shared `chunk_first_rows` |
| `PAGE` | `P` | `column_chunk_index` and `page_index` | Shared `page_first_rows` |
| `ARBITRARY` | `S` | Separate `column_range_starts` | Set-specific `range_start_rows` |

Only `ARBITRARY` stores the additional index and row starts. `column_range_starts` has `C + 1` non-null `i64` entries; equal adjacent entries mean a column has no ranges in that set. Chunk statistics require `chunk_first_rows`, so reading them does not require page metadata. Page statistics require page-row metadata but do not require page offsets.

For the positional tags, missing statistics retain their positions as unknown values. A column with unavailable or independently encrypted statistics must not shift another column's positions. An entire unavailable leaf stream can be omitted. Scalar flags and distinct counts have one nullable value per range. Typed min/max streams contain paired values `[min_0, max_0, min_1, max_1, ...]`, with `2S` positions; exactness flags use the same pairing. A range's entries in other physical-type streams are null.

Row positions are absolute within the file. For a column with usable statistics, each set describes nonempty, row-aligned ranges partitioning `[0, num_rows)`. A range ends at the next start in that column, or at `num_rows`. Chunk membership and value counts do not establish row coverage. The positional tags apply only when the chunk/page boundaries satisfy this rule. A page splitting a source row cannot use the `PAGE` tag under this contract. Sparse coverage needs valid row-range statistics with unknown entries covering gaps, or an omitted set; changing the tag alone does not make existing bounds valid.

PAR1 row-group statistics normally map to `COLUMN_CHUNK`; row-aligned page statistics map to `PAGE`. Multiple sets may describe different granularities. Readers match them by row interval, not by set position, and must not add counts across overlapping granularities.

## Reader access

A reader first obtains the footer directory and resolves the requested columns from the source schema. For zero-based column `c`, local chunk `j`, and local page `q`, it uses:

```text
boundary(index, n) = 0 if n == 0 else index[n - 1]

a, b = boundary(column_chunk_index, c), column_chunk_index[c]
k = a + j                         # require 0 <= j < b - a
x, y = boundary(page_index, k), page_index[k]
p = x + q                         # require 0 <= q < y - x
start = data_offsets[k] + relative_page_offsets[p]
end = data_offsets[k] + relative_page_offsets[p + 1]  if p + 1 < y
      data_offsets[k] + data_lengths[k]              otherwise

d = dictionary_ids[k]
dictionary = none if d is null else (dictionary_offsets[d], dictionary_lengths[d])
```

Here `boundary(index, n)` accepts `0 <= n <= length(index)` and never reads `index[-1]` for `n = 0`. For valid column `c`, chunk statistics occupy `[a,b)` and page statistics occupy `[boundary(page_index,a), boundary(page_index,b))`, including empty columns. The same chunk/page positions address codecs, row positions, and tagged statistics. No hierarchy levels need to be scanned or reconstructed.

These formulas identify logical positions after the necessary index values have been decoded. The format does not store a byte-offset directory for every metadata page. A reader can scan page headers, accumulate their value counts, and cache a directory for locating an encoded range. The selected encoding may still require decoding preceding values within a block or decompressing a page. Protected streams must also be authenticated according to their protection unit before use. Explicit indexes therefore remove hierarchy reconstruction, but do not promise constant-time file access.

## Framing, protection, and compatibility

The proposed plaintext tail is `[metadata streams][Compact-Thrift directory][u64 LE absolute directory offset][PAR3]`, following the source data. This is a new footer format, not a backward-compatible extension readable by PAR1-only readers. The complete IDL defines the root fields and optional row-group, key/value, and encryption structures.

Protected modes authenticate the visible directory and independently protect shared streams with the footer key. Separate column-key modules protect a column's statistics. Authenticate indexes and row metadata before interpreting dependent statistics. Missing statistics or a missing column key means unknown information; failed authentication is an error. The exact envelope and AAD rules are in the IDL; public module identifiers and interoperability validation remain to be completed.

Readers validate index lengths, monotonicity, terminal counts, data/dictionary extents, dictionary ID bounds and ownership, and the positional invariants before use. Dictionary references must belong to one physical column and use compatible decoding. Check offset/length sums without integer overflow. Unsupported required format versions or encodings cannot be guessed. Unknown optional fields can be skipped; an unknown statistics tag makes that set unsupported, not equivalent to another tag.

Compared with the current modular footer, the proposed statistics schema uses null/NaN presence and all-null flags instead of exact null/NaN counts, and does not yet carry `SchemaModule.column_orders` or an equivalent of the optional `SchemaIndexModule`. Its single nullable dictionary ID also narrows the current placement schema's ability to list multiple dictionaries per chunk. These are compatibility decisions to resolve before adoption. The draft is not a lossless replacement for every current modular or PAR1 metadata field; histograms are also outside its present scope.

The public encoding profile, format version, and protection identifiers require agreement. Repository-local experimental encodings and version numbers are not public allocations.

## Optional future extension: column sets

For exceptionally wide files, such as 30,000 physical columns, location metadata alone may be large even when a query needs only a few columns. Optional column sets would partition metadata into groups of complete columns, allowing a reader to fetch and interpret the selected groups' location streams. This grouping would not constrain where source data is placed.

The [column-set draft](proposal/ColumnSetFooter.thrift) lists each set's physical `column_ordinals`, allowing noncontiguous groups in any order. The list length gives the local column count, so neither `first_column` nor `num_columns` is stored. Every physical column occurs in exactly one set, including columns with no chunks; the total count is checked against the shared source schema. Each set reuses the current base types with local cumulative-end chunk/page indexes and dictionary IDs. Since dictionaries are reused only within a column, keeping complete columns together avoids cross-set dictionary references or duplication. Chunk/page statistics reuse the set's indexes and row starts; arbitrary ranges retain their own boundaries.

A column set is a logical grouping. Every stream would retain its own offset, length, and codec, with no requirement for a set's metadata to be contiguous. Readers could fetch location streams first and statistics leaves later. Decoding a leaf's values is independent of sibling leaves; interpreting its positions still requires the shared indexes and applicable row metadata.

Smaller sets reduce metadata fetched for narrow projections but add descriptors and may weaken encoding. This optional draft uses explicit membership lists and independently located streams; group size and protection rules remain open. It is a future extension outside the base format. The revised column-set directory has not been benchmarked; historical contiguous-set experiments use different definitions.

## Size and decoding comparison

V38 is compared with the existing Parquet Compact-Thrift footer (current) and the modular footer on the same inputs. Size and decoding measurements cover all 12 datasets, with all three formats interleaved in each timing run. Metadata is encoded without Zstd. V38 uses direct offsets, a dictionary table, and cumulative-end indexes with an implicit initial zero.

These results use the [V38 benchmark definition](ExplicitIndexFooter.thrift), before the review draft split out `DictionaryMetadata` and moved page offsets into `PageMetadata`. The reorganized directory requires a new wire version; its sizes and decoding timings have not been remeasured.

### Size

All values are bytes of complete metadata at each cumulative scope, including schema, directories, page headers, required external indexes, and trailer. Each table includes every dataset in the corpus.

#### Core

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

#### Core + chunk statistics

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

#### Core + chunk statistics + page locations

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

#### Core + chunk statistics + page locations + page statistics

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

Across all 12 datasets, core metadata totals **1,846,119 bytes** for current Thrift, **588,635** for modular, and **468,958** for V38: reductions of 74.6% and 20.3%, respectively. Small core footers can still be larger than modular. For the eight datasets with V35 measurements, omitting the initial zeros changes the core total by only +1 byte; encoding headers and first-value encoding can offset the removed values.

FineWeb has no page statistics for one column. Its page-statistics set uses the supported ARBITRARY form to preserve that sparse coverage; complete chunk/page sets reuse the shared indexes.

### Decoding time

Microseconds per query; lower is better. Each cell is **one column / all columns**. All 12 datasets are shown for each query.

#### Chunk metadata

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

#### Chunk statistics

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

#### Chunk metadata + page locations

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

#### Chunk + page statistics

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

Queries use fresh prototype readers over memory-resident bytes, including parsing, decoding, allocation, constructing projected values, and checksumming; file I/O is excluded. One-column results are medians across nine column positions and read every chunk/page needed for that column. Each format ran seven trials in each of three processes per dataset, pinned to CPU 16 on an Intel Xeon Platinum 8375C. All timings were rerun with the optimized reader; the eight-dataset and four-dataset suites use the same executable and protocol. Queried fields can be a subset of the retained metadata. Comparisons cover placement and supported pruning information, rather than exact null-count queries.

See the [full measurements and validation](V38-MEASUREMENTS.md), [comparison CSV](proposal-comparison.csv), and [saved samples and provenance](v38-results.json.gz). Sizes and timings here both measure V38; they do not reuse V31 timings.

## Supporting material

The [full IDL](proposal/ModularFooterV2.thrift) defines each field's type, value count, nullability, and dependencies; the [column-set extension](proposal/ColumnSetFooter.thrift) reuses those types. [V38 measurements](V38-MEASUREMENTS.md) compare the preceding benchmark definition with current Parquet Thrift and the modular footer. Historical [V35 direct-offset results](DIRECT-OFFSETS.md) and [V31 explicit-index](legacy/ExplicitIndexFooterV31.thrift)/[V32 nested-layout](ImplicitLevelsFooter.thrift) experiments describe earlier revisions. Production encryption and a public interoperability suite remain outside the validated scope.
