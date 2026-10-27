/** Direct-offset column-set draft V36; no public wire-version allocation.
 * Location and statistics are separately fetchable for the same column range.
 * Reuses the explicit-index proposal's fields and encodings. Row-group
 * descriptors and encryption are outside this experiment.
 */
include "ExplicitIndexFooterV35.thrift"
namespace cpp parquet.columnsets

/** One contiguous range of physical columns, independently fetchable.
 * Entries are ordered and partition [0, FileInfo.num_columns).
 */
struct ColumnSetLocation {
  1: required i32 first_column;
  2: required i32 num_columns;
  3: required i64 offset; // Absolute file position of ColumnSetMetadata.
  4: required i64 length; // Complete Compact-Thrift block size.

  /** Both present or both absent. Absolute position and complete block size of
   * ColumnSetStatistics for exactly this column range. Absent means unknown
   * statistics, not an empty column or a set of all-null values.
   * Reading ColumnSetMetadata never requires fetching this block.
   */
  5: optional i64 statistics_offset;
  6: optional i64 statistics_length;
}

/** C, K, P and D in the reused definitions are local to this set.
 * StreamLocation offsets are relative to page_streams. Source-data offsets
 * in data_offsets and dictionary_offsets are absolute; page offsets stay relative.
 * Store only dictionaries referenced by this set and rebase IDs to its table.
 * Dictionary reuse is confined to one physical column. Sets contain complete
 * columns, so all references to a dictionary belong to one set: no global
 * lookup, cross-set references, or duplicated dictionary entries are needed.
 * Null slots and empty columns/chunks keep the existing explicit-index rules.
 */
struct ColumnSetMetadata {
  3: required ExplicitIndexFooterV35.DataLayout data_layout;
  4: required ExplicitIndexFooterV35.DataLocation data_location;
  6: required ExplicitIndexFooterV35.ColumnChunkMetadata column_chunk_metadata;
  7: optional ExplicitIndexFooterV35.PageMetadata page_metadata;
  8: required binary page_streams;
}

/** Statistics for the SAME contiguous column range as ColumnSetMetadata.
 * Stream offsets are relative to this block's page_streams; C/K/P are local.
 * COLUMN_CHUNK/PAGE keep unknown slots and reuse the associated location
 * block's indexes and chunk/page row starts. They store neither a separate
 * column index nor row starts. Only ARBITRARY stores those two streams here.
 * Any number of granularities is allowed; align them by row interval.
 * Omit an entirely unavailable block via ColumnSetLocation's optional fields.
 */
struct ColumnSetStatistics {
  1: required list<ExplicitIndexFooterV35.ColumnRangeStatistics> row_range_statistics;
  2: required binary page_streams;
}

/** Tail: [all location blocks][all statistics blocks][Compact-Thrift root]
 * [u64 LE root offset][CST3]. V36/CST3 are local experiment identifiers.
 * V33/CST1 and V34/CST2 use region maps; see legacy/ColumnSetFooterV34.thrift.
 * They must not be interpreted with the direct-offset field definitions.
 * FileInfo/schema is stored once. Set size is a writer choice, not a new
 * per-column rule or a requirement for equal chunk counts.
 */
struct ColumnSetFooter {
  1: required i32 version;
  2: required ExplicitIndexFooterV35.FileInfo file_info;
  3: required list<ColumnSetLocation> column_sets;
}
