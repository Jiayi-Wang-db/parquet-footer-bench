/** Optional extension for very wide files, e.g. 30,000 columns.
 * Sets group complete columns by explicit ordinal, including noncontiguous groups.
 * Descriptors are inline; streams are independently located, with no set-level
 * contiguity requirement. Reuses ModularFooterV2 cumulative-end indexes.
 * Row-group descriptors and encryption are outside this plaintext draft.
 * Wire version and magic allocation are pending.
 */
include "ModularFooterV2.thrift"
include "../../modular-footer/parquet.thrift"
namespace cpp parquet.columnsets

/** Stored once. N = physical leaves in schema = total column_ordinals entries.
 * Field 1 (former num_columns) is reserved. Reused types refer to this num_rows.
 */
struct FileInfo {
  2: required i64 num_rows;
  3: required list<parquet.SchemaElement> schema;
  4: optional string created_by;
}

/** C = column_ordinals.size(); K, P and D are local to this set.
 * Local C replaces FileInfo.num_columns in reused types.
 * Stream and source-data offsets are absolute; page offsets remain chunk-relative.
 * Store only referenced dictionaries and rebase IDs to this set's table.
 * Dictionaries belong to one physical column, hence one set; no cross-set lookup.
 * Null slots and empty columns/chunks follow ModularFooterV2 rules.
 */
struct ColumnSetMetadata {
  /** Inline list<i32>; number of values = C > 0; list and values non-null.
   * Source ordinals in [0,N), distinct across all sets; any order is allowed.
   * List order defines local column order. Retain ordinal 0 when included.
   * Field 2 (former num_columns) is reserved.
   */
  1: required list<i32> column_ordinals;
  3: required ModularFooterV2.DataLayout data_layout;
  4: required ModularFooterV2.DictionaryMetadata dictionary_metadata;
  6: required ModularFooterV2.ColumnChunkMetadata column_chunk_metadata;
  7: optional ModularFooterV2.PageMetadata page_metadata;

  /** Independent granularities over local columns, using SharedRangeStatistics rules.
   * Leaves can be fetched/decoded separately; interpreting ranges requires the
   * shared hierarchy/row metadata. Omission means unknown statistics.
   * Location reads do not require statistics.
   */
  8: optional list<ModularFooterV2.SharedRangeStatistics> row_range_statistics;
}

/** Tail: [independently located streams][Compact-Thrift root]
 * [u64 LE absolute root offset][column-set magic; allocation pending].
 * Validate extents: within file, before root, nonoverlapping; interleaving/gaps allowed.
 * Set size/order is unrestricted. Every physical column belongs to exactly one
 * set, including empty columns; a zero-column file has no sets. Validate membership
 * against schema: union is [0,N), with no duplicate or out-of-range ordinals.
 * New layout/version; incompatible with historical contiguous-range/leading-zero drafts.
 */
struct ColumnSetFooter {
  1: required i32 version; // Allocation pending.
  2: required FileInfo file_info;
  3: required list<ColumnSetMetadata> column_sets;
}
