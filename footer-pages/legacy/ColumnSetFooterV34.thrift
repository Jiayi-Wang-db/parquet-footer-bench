/** Column-set experiment; no public wire-version allocation.
 * Location and statistics are separately fetchable for the same column range.
 * Reuses the explicit-index proposal's fields and encodings. Row-group
 * descriptors and encryption are outside this experiment.
 */
include "ExplicitIndexFooterV31.thrift"
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

/** C, K, P and R in the reused definitions are local to this set.
 * StreamLocation offsets are relative to page_streams. Source-data offsets
 * inside region_offsets remain absolute; page offsets remain chunk-relative.
 * Only referenced region boundaries are retained, in file order. Gaps can
 * form unreferenced local regions; no global region map is needed.
 * Null slots and empty columns/chunks keep the existing explicit-index rules.
 */
struct ColumnSetMetadata {
  3: required ExplicitIndexFooterV31.DataLayout data_layout;
  4: required ExplicitIndexFooterV31.DataLocation data_location;
  6: required ExplicitIndexFooterV31.ColumnChunkMetadata column_chunk_metadata;
  7: optional ExplicitIndexFooterV31.PageMetadata page_metadata;
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
  1: required list<ExplicitIndexFooterV31.ColumnRangeStatistics> row_range_statistics;
  2: required binary page_streams;
}

/** Tail: [all location blocks][all statistics blocks][Compact-Thrift root]
 * [u64 LE root offset][CST2]. V34/CST2 are local experiment identifiers.
 * The historical location-only experiment uses V33/CST1 and omits fields 5/6.
 * FileInfo/schema is stored once. Set size is a writer choice, not a new
 * per-column rule or a requirement for equal chunk counts.
 */
struct ColumnSetFooter {
  1: required i32 version;
  2: required ExplicitIndexFooterV31.FileInfo file_info;
  3: required list<ColumnSetLocation> column_sets;
}
