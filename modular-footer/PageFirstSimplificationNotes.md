# Page-First Footer Simplification Notes

## Access invariant

Metadata must support direct access to a column, not random access within a column. A reader can
project column `foo` without reading column `bar`; after selecting `foo`, it sequentially decodes
that column's metadata.

## Shared decisions

- `RegionMap.region_offsets` is one required strict-DBP `binary`, without an encoding enum or
  wrapper struct.
- Page-directory descriptor ordinals are one fixed-width packed `binary`, indexed by leaf column
  ordinal.
- Sequential integer arrays use a local, self-counting `EncodedArray`. The writer emits the smaller
  of fixed-width bit-packing and strict DBP. There is no RLE or default-value encoding.
- Exact page encodings remain in page headers.
- Module directories are ordinary Compact-Thrift lists. Schema and file metadata use ordinary
  Compact-Thrift structures.

## PAR1-only decisions

- `row_group_region_ordinals` and `row_group_num_rows` are required strict-DBP binaries. The former
  has `num_row_groups + 1` values and the latter has `num_row_groups` values.
- Every column chunk is one region. Each leaf column has an independently addressed
  `ColumnPageDirectory` with chunk-region ordinals, page counts, uncompressed sizes, value counts,
  codecs, and dictionary flags indexed by row-group ordinal.
- A flat Offset Index stores data-page offsets relative to each chunk-region start and file-global
  first-row indexes. Prefix-summing page counts identifies each chunk's slice. It is emitted for a
  column only when every chunk participates.
- Existing row-group `ColumnStatistics` and its sparse `ModularFooter.EncodedArray` fields remain
  unchanged to keep the addendum small.
- Flat Column Index descriptors also retain `ModularFooter.EncodedArray`, because a PAR1 file may
  have page indexes for only some row groups or statistics for only some pages.

## PAR3-only decisions

- There are no row-group arrays or `num_row_groups`.
- Statistics sparsity across columns is localized to `RowRangeStatisticsDirectory`: an optional
  presence bitmap plus packed descriptor region ordinals.
- Within one participating column, row ranges densely cover all file rows. Range starts and count
  arrays use the local adaptive `EncodedArray`; min/max has its own range-presence bitmap.
- Min/max prefixes and suffixes use adaptive length arrays plus concatenated byte payloads.
  Exactness is an interleaved min/max bitset.
- Boundary order is one flat two-bit value per participating column.

## Representation audit

| Area | Representation | Access |
| --- | --- | --- |
| Region boundaries | Strict DBP binary | Decode mandatory global map once |
| PAR1 row groups | Two strict DBP binaries | Decode mandatory PAR1 arrays once |
| Column page directory | Fixed-width packed binary | Direct leaf-column lookup |
| PAR1 per-column chunks | Adaptive bit-packed/DBP arrays | Sequential after column selection |
| PAR1 flat Offset Index | Relative offsets and file-global rows | Page slice from page-count prefix sum |
| PAR3 per-column pages | Adaptive bit-packed/DBP arrays | Sequential after column selection |
| PAR1 row-group statistics | Current modular sparse arrays | Compatibility with current proposal |
| PAR1 flat Column Index | Current modular sparse arrays | Supports missing chunk/page statistics |
| PAR3 statistics presence | Bitmap plus packed region ordinals | Direct membership, popcount for rank |
| PAR3 per-column statistics | Sequential adaptive arrays and binaries | Sequential after column selection |
| Schema index | Fixed-width packed binaries | Optional direct schema lookup |

No cross-column lookup uses adaptive DBP. No PAR3 within-column array pays for random access that a
reader does not need.
