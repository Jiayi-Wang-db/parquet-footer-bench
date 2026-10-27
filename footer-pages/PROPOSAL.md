# Proposal: extending the modular footer with flexible layouts and typed metadata streams

Status: design draft for discussion, based on the [current modular footer](../modular-footer/ModularFooter.thrift). The [complete proposed Thrift definition](ExplicitIndexFooter.thrift) accompanies this document and imports only standard Parquet types from `parquet.thrift`.

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

The footer directory contains `FileInfo`, `DataLayout`, `DataLocation`, `ColumnChunkMetadata`, and optional page, row-group, statistics, and key/value metadata. The source schema is stored once in `FileInfo`. Each array field contains a `StreamLocation`:

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

`DataLocation.dictionary_offsets` and `dictionary_lengths` form a table with `D` non-null `i64` values in each stream. Entry `d` locates one complete dictionary page, including its header. `ColumnChunkMetadata.dictionary_ids` has `K` nullable `i32` values: null means no dictionary; otherwise the ID is in `[0,D)`. Several chunks of the same physical column may reference one entry, provided their dictionary decoding is compatible. Cross-column dictionary sharing is not permitted. Table entries are stored once and need not follow file order; assigning IDs in first-reference order is a writer choice that encoded well in the prototype. With no dictionaries, both table streams are empty and all chunk IDs are null.

`relative_page_offsets` contains one non-null `i64` per page, relative to its chunk's `data_offsets[k]`. The first offset of each nonempty chunk is zero. A page ends at the next page offset in that chunk, or at `data_offsets[k] + data_lengths[k]` for the last page.

The page-offset stream is optional. When present, it has exactly `P` positions, including the zero for a single-page chunk. A reader that knows a chunk has one page can derive its byte extent from the chunk's offset and length without reading that offset. Omitting the first offset from storage would require remapping page positions for the shorter offset array, including handling empty chunks; this draft keeps those positions aligned with other page fields. `page_index` is required whenever page offsets or page-row metadata is present.

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

Each set would reuse the base representation with local chunk/page indexes and dictionary IDs; the source schema would remain shared. Since dictionaries are reused only within a column, keeping complete columns together avoids cross-set dictionary references or duplication. Chunk/page statistics would reuse the set's indexes and row starts; arbitrary ranges would retain their own boundaries.

A column set is a logical grouping. Every stream would retain its own offset, length, and codec, with no requirement for a set's metadata to be contiguous. Readers could fetch location streams first and statistics leaves later. Decoding a leaf's values is independent of sibling leaves; interpreting its positions still requires the shared indexes and applicable row metadata.

Smaller sets reduce metadata fetched for narrow projections but add descriptors and may weaken encoding. Membership encoding (contiguous ranges versus explicit column ordinals), group size, and the extension's directory and protection rules remain open. The [column-set prototype](ColumnSetFooter.thrift) explores contiguous ranges with independently located streams; it is supporting work, not part of the base format or a commitment to that membership representation.

## Size and decoding comparison

V38 is compared with the existing Parquet Compact-Thrift footer (current) and the modular footer on the same inputs. Size and decoding measurements cover all 12 datasets, with all three formats interleaved in each timing run. Metadata is encoded without Zstd. V38 uses direct offsets, a dictionary table, and cumulative-end indexes with an implicit initial zero.

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

#### Chunk statistics

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

#### Chunk metadata + page locations

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

#### Chunk + page statistics

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

Queries use fresh prototype readers over memory-resident bytes, including parsing, decoding, allocation, constructing projected values, and checksumming; file I/O is excluded. One-column results are medians across nine column positions and read every chunk/page needed for that column. Each format ran seven trials in each of three processes per dataset, pinned to CPU 16 on an Intel Xeon Platinum 8375C. The original eight-dataset timings are retained; the four remaining datasets were timed separately with the same executable and protocol. Queried fields can be a subset of the retained metadata. Comparisons cover placement and supported pruning information, rather than exact null-count queries.

See the [full measurements and validation](V38-MEASUREMENTS.md), [comparison CSV](proposal-comparison.csv), and [saved samples and provenance](v38-results.json.gz). Sizes and timings here both measure V38; they do not reuse V31 timings.

## Supporting material

The [full IDL](ExplicitIndexFooter.thrift) defines each field's type, value count, nullability, and dependencies. [V38 measurements](V38-MEASUREMENTS.md) compare the current draft with current Parquet Thrift and the modular footer. Historical [V35 direct-offset results](DIRECT-OFFSETS.md) and [V31 explicit-index](legacy/ExplicitIndexFooterV31.thrift)/[V32 nested-layout](ImplicitLevelsFooter.thrift) experiments describe earlier revisions. Production encryption and a public interoperability suite remain outside the validated scope.
