# Page-First Footer Simplification Notes

## Goal

Simplify the page-first modular footer by reducing opaque encoded `binary` arrays and generic
`EncodedArray` usage.

The access invariant is random access to a column, not random access within a column. A reader must
be able to project metadata for column `foo` without reading metadata for column `bar`. Once the
reader has selected `foo`, it is acceptable to read or decode all metadata for `foo` sequentially.

Prefer ordinary Thrift fields and lists where they satisfy this invariant. DBP, packed binaries,
`EncodedArray`, or another specialized representation should remain only when justified by
cross-column access, sparsity, size, or another explicit requirement.

Review each remaining case individually before editing the Thrift definition.

## Decisions

- `RegionMap.region_offsets`: keep as one required DBP-encoded `binary`. Do not add an encoding
  enum, array wrapper, or generic `EncodedArray` framing.
- `PageDirectory.column_data_page_regions`: use one required fixed-width bit-packed `binary`. The
  root's `num_columns` supplies the count; every leaf column has a descriptor region.
- `ColumnDataPages`: encode `region_ordinals`, `first_row_indexes`, `codecs`,
  `dictionary_regions`, `num_values`, and `total_uncompressed_sizes` as the smaller of fixed-width
  bit-packing and strict DBP for each array. The local `EncodedArray` contains only a binary whose
  first byte identifies the encoding. Exact page encodings remain in page headers, with only
  `is_fully_dictionary_encoded` summarized in the column descriptor.
- `RowRangeStatisticsDirectory`: localize column sparsity here with an optional LSB-first
  participation bitmap and a compact fixed-width bit-packed array of descriptor region ordinals in
  column order. Omit the bitmap when every column participates. Readers may materialize a dense
  column-to-region lookup once; do not add on-disk rank checkpoints initially.
- `RowRangeStatistics`: keep all data sequential within a selected column. Range starts use the
  local adaptive `EncodedArray`. Null and NaN counts are optional dense
  Compact-Thrift lists. Min/max shares one range-presence bitmap, while prefixes and suffixes each
  use a length list plus concatenated bytes. One interleaved bitset carries min/max exactness.
- `SchemaIndexModule`: define it locally and keep every cross-schema lookup array fixed-width
  bit-packed. It is the optional random-access path into an otherwise sequential Compact-Thrift
  `SchemaModule`.
- `PageFirstModularFooter.thrift` includes only `parquet.thrift`. It defines its own
  `EncodedArray`, schema module, file-metadata module, and schema-index module and has no dependency
  on `ModularFooter.thrift`.

The preceding all-DBP `ColumnDataPages` experiment reduced page-placement bytes by 50% to 60% on
the three large Raincloud files. It increased Yellow Taxi page placement from 2,927 bytes to 7,267
bytes (+148%) because its columns contain only a few pages and strict DBP pads partial miniblocks.

Selecting the smaller encoding per array produces 277,907 bytes for US Accidents, 63,668 bytes for
FineWeb, 115,175 bytes for Hacker News, and 3,211 bytes for Yellow Taxi. Relative to all-DBP, the
three large files grow by 0.1% to 0.3% from the encoding tag and Thrift struct framing, while Yellow
Taxi shrinks by 55.8%. It remains 128.7% larger than current modular placement rather than 417.6%.

## Representation audit

| Area | Representation | Why |
| --- | --- | --- |
| Region boundaries | Strict DBP `binary` | Mandatory global data; decode once for maximum compactness |
| Module directory | Ordinary Compact-Thrift list | Always read and contains only one entry per module kind |
| Schema | Ordinary Compact-Thrift list | Read fully unless the optional schema index is present |
| Schema index | Fixed-width packed binaries | Direct path, element, leaf, and parent lookup |
| Column page directory | Fixed-width packed binary | Direct leaf-column ordinal lookup |
| Per-column page metadata | Adaptive bit-packed/DBP `EncodedArray` | Sequential after selecting a column; writer emits smaller form |
| Statistics participation | Bitmap plus packed region ordinals | Compact column sparsity; reader may materialize lookup once |
| Per-column range starts | Adaptive `EncodedArray` | Sequential after selecting a column |
| Null and NaN counts | Optional dense Compact-Thrift lists | Simple sequential representation; absent means unavailable for the column |
| Min/max statistics | Shared presence, length lists, concatenated bytes | One sparse domain and no per-value Thrift binary framing |
| Min/max exactness | Interleaved packed bits | Shares min/max presence and disappears when every bound is exact |
| File metadata | Ordinary Compact-Thrift fields | Optional descriptive data consumed as a whole |

No cross-column lookup uses adaptive DBP. No within-column array pays for random access that the
reader does not need.
