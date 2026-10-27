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

Fresh measurements of V38 against the existing Parquet Compact-Thrift footer (current) and the modular footer. All three formats use the same eight input files and run interleaved in one benchmark executable. Metadata is encoded without Zstd. V38 uses direct offsets, a dictionary table, and cumulative-end indexes with an implicit initial zero.

### Size

ClickBench, bytes of complete metadata at each cumulative scope, including schema, directories, page headers, and required external indexes:

| Metadata retained | Current Parquet Thrift | Modular footer | V38 |
|---|---:|---:|---:|
| Core | 510,422 | 151,028 | 119,001 |
| Core + chunk statistics | 734,357 | 262,202 | 192,476 |
| Core + chunk statistics + page locations | 8,497,329 | 4,898,177 | 2,196,131 |
| Core + chunk statistics + page locations + page statistics | 21,868,253 | 16,887,807 | 9,794,399 |

Across all eight datasets, core metadata totals **1,831,915 bytes** for current Thrift, **582,762** for modular, and **461,815** for V38: reductions of 74.8% and 20.8%, respectively. Small core footers can still be larger than modular. Omitting the initial zeros changes the total by only +1 byte versus V35; encoding headers and first-value encoding can offset the removed values.

### Decoding time

ClickBench, microseconds per query; lower is better:

| Query | Columns | Current Parquet Thrift | Modular footer | V38 |
|---|---|---:|---:|---:|
| Chunk metadata | one | 3,647.20 | 68.71 | 65.82 |
| Chunk metadata | all | 5,362.82 | 767.42 | 1,373.65 |
| Chunk statistics | one | 4,528.19 | 74.61 | 59.75 |
| Chunk statistics | all | 9,150.52 | 1,467.76 | 841.47 |
| Chunk metadata + page locations | one | 5,021.06 | 310.79 | 298.26 |
| Chunk metadata + page locations | all | 45,670.40 | 26,810.80 | 21,339.50 |
| Chunk + page statistics | one | 5,934.74 | 983.43 | 570.01 |
| Chunk + page statistics | all | 132,204.00 | 110,325.00 | 121,880.00 |

V38 is faster than modular for all four ClickBench one-column projections. All-column results are mixed: chunk metadata takes 79% more time and combined chunk/page statistics 10% more time, while chunk statistics and chunk metadata with page locations are faster. These results do not establish a universal decoding advantage.

Queries use fresh prototype readers over memory-resident bytes, including parsing, decoding, allocation, constructing projected values, and checksumming; file I/O is excluded. One-column results are medians across nine column positions and read every chunk/page needed for that column. Each format ran seven trials in each of three processes, pinned to CPU 16 on an Intel Xeon Platinum 8375C. Queried fields can be a subset of the retained metadata. Comparisons cover placement and supported pruning information, rather than exact null-count queries.

See the [full measurements and validation](V38-MEASUREMENTS.md), [comparison CSV](proposal-comparison.csv), and [saved samples and provenance](v38-results.json.gz). Sizes and timings here both measure V38; they do not reuse V31 timings.

## Supporting material

The [full IDL](ExplicitIndexFooter.thrift) defines each field's type, value count, nullability, and dependencies. [V38 measurements](V38-MEASUREMENTS.md) compare the current draft with current Parquet Thrift and the modular footer. Historical [V35 direct-offset results](DIRECT-OFFSETS.md) and [V31 explicit-index](legacy/ExplicitIndexFooterV31.thrift)/[V32 nested-layout](ImplicitLevelsFooter.thrift) experiments describe earlier revisions. Production encryption and a public interoperability suite remain outside the validated scope.
