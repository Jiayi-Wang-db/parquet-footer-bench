# Proposal: direct locations and typed metadata streams with shared indexes

Status: design draft for discussion. The [complete Thrift definition](ExplicitIndexFooter.thrift) accompanies this proposal; all proposal-specific types are defined in that file. It imports only standard Parquet types from `parquet.thrift`.

## Summary

This proposal extends the modular footer in five areas:

- **More flexible data layouts.** Columns can have different numbers of chunks, including zero, so dense and sparse columns can be laid out independently. Row groups are optional; chunk placement does not require one chunk per column per row group.
- **Direct chunk locations with reusable dictionaries.** Each chunk stores its absolute data offset and length. A separate table stores dictionary offsets and lengths, referenced by nullable chunk dictionary IDs. Chunks in the same column can reuse a dictionary. Data access needs no region-map lookup.
- **Statistics over different row ranges.** Statistics can describe column chunks, pages, or arbitrary row ranges. Multiple granularities can coexist, and their boundaries need not nest. Tags let chunk/page statistics reuse existing row positions; only arbitrary ranges store their own boundaries.
- **Shared indexes across metadata fields.** One column-to-chunk prefix index groups all chunk fields, and one chunk-to-page prefix index groups all page fields. Aligned statistics reuse these indexes, with unknown slots preserving positions when statistics are missing.
- **Typed metadata streams using Parquet encodings.** A small Thrift directory points directly to independently encoded arrays. Existing page encodings and compression can be used for metadata, while the source schema is stored once.

The base proposal uses shared indexes and streams across the file, with no column-set wrapper or membership array. [Optional column sets](#optional-future-extension-column-sets) could extend this design for exceptionally wide files; their representation is deferred.

The design retains separate access to placement, statistics, and descriptive metadata. Accessing an encoded value can require decoding its containing block or page; the format does not guarantee the original modular proposal's direct positional access to packed values.

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
| `column_chunk_index` | `list<i32>`, `C + 1` | Non-null | Column `c` owns chunks `[index[c], index[c+1])`. |
| `page_index` | `list<i32>`, `K + 1` | Non-null | Chunk `k` owns pages `[index[k], index[k+1])`. |
| Chunk-level fields | `list<T>`, `K` | Defined per field | Ordered by column, then chunk. |
| Page-level fields | `list<T>`, `P` | Defined per field | Ordered by flattened chunk, then page. |

Both indexes are nondecreasing prefixes starting at zero; their final entries are `K` and `P`. Equal adjacent entries represent an empty column or a chunk with no pages. Counts follow from adjacent differences, so separate count arrays are unnecessary. `K` and `P` must fit in signed `i32`.

Columns may have different chunk counts, including zero. There is no requirement that every column have one chunk per row group. Optional row-group metadata describes layouts that use row groups; it does not define the chunk index.

All fields in a domain preserve the same logical positions. For example, a chunk without a dictionary has a null entry in `dictionary_ids`; it does not disappear from that array. Metadata-page boundaries may differ between streams because the indexes address logical positions, not encoded byte offsets.

Example: two columns with two and one chunks, containing five pages in total:

```text
column_chunk_index:    [0, 2, 3]
page_index:            [0, 2, 3, 5]
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
a, b = column_chunk_index[c], column_chunk_index[c + 1]
k = a + j                         # require 0 <= j < b - a
x, y = page_index[k], page_index[k + 1]
p = x + q                         # require 0 <= q < y - x
start = data_offsets[k] + relative_page_offsets[p]
end = data_offsets[k] + relative_page_offsets[p + 1]  if p + 1 < y
      data_offsets[k] + data_lengths[k]              otherwise

d = dictionary_ids[k]
dictionary = none if d is null else (dictionary_offsets[d], dictionary_lengths[d])
```

The same chunk/page positions address codecs, row positions, and tagged statistics. No hierarchy levels need to be scanned or reconstructed.

These formulas identify logical positions after the necessary index values have been decoded. The format does not store a byte-offset directory for every metadata page. A reader can scan page headers, accumulate their value counts, and cache a directory for locating an encoded range. The selected encoding may still require decoding preceding values within a block or decompressing a page. Protected streams must also be authenticated according to their protection unit before use. Explicit indexes therefore remove hierarchy reconstruction, but do not promise constant-time file access.

## Alternatives considered

| Alternative | Benefit | Reason for the proposed choice |
|---|---|---|
| Region map | Infers lengths from shared file-order boundaries and is smaller in the measured corpus. | Direct chunk offsets/lengths remove the data-region lookup. Accept the measured size increase; keep dictionary indirection to support reuse. |
| Dictionary offset/length repeated per chunk | Avoids dictionary ID lookup. | A separate dictionary table stores each dictionary extent once and directly expresses reuse within a column. |
| Nested RL/DL in every chunk/page leaf | Expresses column → chunk → page directly. | Repeats hierarchy structure; selective reads must recover boundaries from levels. Shared prefixes expose those boundaries directly. |
| One RL/DL carrier, other leaves flattened | Avoids repeating hierarchy levels. | Still requires boundary reconstruction and a dependency on the carrier. Two explicit prefixes make that shared dependency easier to use. |
| Per-column/per-chunk counts | Describes each list's length. | Locating a later list requires a prefix sum. Store the prefixes and derive counts by subtraction. |
| Omit the first page offset | Saves a redundant zero, including the sole offset in a single-page chunk. | Offset positions would no longer match page-row and statistics positions. Keep one offset per page when the stream exists. |

The recommendation is shared explicit prefixes: the reader's addressing rules are simple, all sibling fields reuse them, and selective access avoids reconstructing list structure. Nested lists remain a useful way to explain the logical model.

## Framing, protection, and compatibility

The proposed plaintext tail is `[metadata streams][Compact-Thrift directory][u64 LE absolute directory offset][PAR3]`, following the source data. This is a new footer format, not a backward-compatible extension readable by PAR1-only readers. The complete IDL defines the root fields and optional row-group, key/value, and encryption structures.

Protected modes authenticate the visible directory and independently protect shared streams with the footer key. Separate column-key modules protect a column's statistics. Authenticate indexes and row metadata before interpreting dependent statistics. Missing statistics or a missing column key means unknown information; failed authentication is an error. The exact envelope and AAD rules are in the IDL; public module identifiers and interoperability validation remain to be completed.

Readers validate index lengths, monotonicity, terminal counts, data/dictionary extents, dictionary ID bounds and ownership, and the positional invariants before use. Dictionary references must belong to one physical column and use compatible decoding. Check offset/length sums without integer overflow. Unsupported required format versions or encodings cannot be guessed. Unknown optional fields can be skipped; an unknown statistics tag makes that set unsupported, not equivalent to another tag.

The current statistics schema preserves pruning information through null/NaN presence and all-null flags, but does not preserve exact null/NaN counts, histograms, or column-order descriptors. These are compatibility decisions to resolve before adoption; the draft is not a lossless replacement for every PAR1 metadata field. The public encoding profile, format version, and protection identifiers also require agreement. Repository-local experimental encodings and version numbers are not public allocations.

## Optional future extension: column sets

For exceptionally wide files, such as 30,000 physical columns, location metadata alone may be large even when a query needs only a few columns. Optional column sets would partition metadata into groups of complete columns, allowing a reader to fetch and interpret the selected groups' location streams. This grouping would not constrain where source data is placed.

Each set would reuse the base representation with local chunk/page indexes and dictionary IDs; the source schema would remain shared. Since dictionaries are reused only within a column, keeping complete columns together avoids cross-set dictionary references or duplication. Chunk/page statistics would reuse the set's indexes and row starts; arbitrary ranges would retain their own boundaries.

A column set is a logical grouping. Every stream would retain its own offset, length, and codec, with no requirement for a set's metadata to be contiguous. Readers could fetch location streams first and statistics leaves later. Decoding a leaf's values is independent of sibling leaves; interpreting its positions still requires the shared indexes and applicable row metadata.

Smaller sets reduce metadata fetched for narrow projections but add descriptors and may weaken encoding. Membership encoding (contiguous ranges versus explicit column ordinals), group size, and the extension's directory and protection rules remain open. The [column-set prototype](ColumnSetFooter.thrift) explores contiguous ranges with independently located streams; it is supporting work, not part of the base format or a commitment to that membership representation.

## Supporting material

The [full IDL](ExplicitIndexFooter.thrift) defines every field's type, value count, nullability, and dependencies. Local historical measurements compare [V31 explicit indexes](legacy/ExplicitIndexFooterV31.thrift) and [V32 nested layouts](ImplicitLevelsFooter.thrift), both using region maps, alongside the [original modular footer](../modular-footer/ModularFooter.thrift). They support the selective-read recommendation for explicit indexes; their timings do not measure this direct-offset revision. The [direct-offset size results](DIRECT-OFFSETS.md) compare the chosen representation with both region-map and original modular cores. Production encryption, direct-offset decoding-speed measurements, and a public interoperability test suite remain outside the validated scope.
