/** Optional future-extension prototype V37; outside the base footer proposal.
 * Membership encoding remains open; contiguous ranges below are one experiment.
 * No public wire-version allocation.
 * Every set's stream descriptors are inline in the common footer directory.
 * Each stream is independently located; sets impose no byte-contiguity rule.
 * Pinned to V35's leading-zero indexes; the current base draft uses cumulative
 * ends with an implicit initial zero. Row-group descriptors and encryption
 * are outside this experiment.
 */
include "../legacy/ExplicitIndexFooterV35.thrift"
namespace cpp parquet.columnsets

/** One contiguous range of complete physical columns. Entries are ordered and
 * partition [0, FileInfo.num_columns); first_column >= 0 and num_columns > 0.
 * C = num_columns; K, P and D in reused definitions are local to this set.
 * Column c's source-schema ordinal is first_column + c.
 * Every StreamLocation has an ABSOLUTE file offset, including statistics streams.
 * Source-data offsets in data_offsets and dictionary_offsets are also absolute;
 * relative_page_offsets values remain relative to their data chunk's start.
 * Store only dictionaries referenced by this set and rebase IDs to its table.
 * Dictionary reuse is confined to one physical column. Sets contain complete
 * columns, so all references to a dictionary belong to one set: no global
 * lookup, cross-set references, or duplicated dictionary entries are needed.
 * Null slots and empty columns/chunks keep the existing explicit-index rules.
 */
struct ColumnSetMetadata {
  1: required i32 first_column;
  2: required i32 num_columns;
  3: required ExplicitIndexFooterV35.DataLayout data_layout;
  4: required ExplicitIndexFooterV35.DataLocation data_location;
  6: required ExplicitIndexFooterV35.ColumnChunkMetadata column_chunk_metadata;
  7: optional ExplicitIndexFooterV35.PageMetadata page_metadata;

  /** Statistics descriptors for this same column range, inline in the directory.
   * Each leaf is an independently fetchable stream. COLUMN_CHUNK/PAGE keep
   * unknown slots and reuse this set's indexes and chunk/page row starts;
   * only ARBITRARY stores its own column index and row starts.
   * Any number of granularities is allowed; align them by row interval.
   * Omission means unknown statistics. Reading location streams does not
   * require fetching statistics payloads. Decoding one leaf's values does
   * not require sibling leaves; interpreting ranges requires shared metadata.
   */
  8: optional list<ExplicitIndexFooterV35.ColumnRangeStatistics> row_range_statistics;
}

/** Tail: [independently located streams][Compact-Thrift root]
 * [u64 LE absolute root offset][CST4]. V37/CST4 are local experiment identifiers.
 * Stream extents must lie before the root, within the file and not overlap.
 * Streams from different sets or fields may be interleaved or separated by gaps.
 * No set-level offset/length, embedded payload, or ordering by set is required.
 * Readers fetch the directory, then only the streams needed for their projection.
 * Validate stream extents and column partitions before dependent reads.
 * FileInfo/schema is stored once; set size is a writer choice. Column chunk
 * counts may differ, including zero. A zero-column file has an empty set list.
 * V36/CST3 used contiguous blocks; see ../legacy/ColumnSetFooterV36.thrift.
 * V33/CST1 and V34/CST2 used region maps. Reject these versions with this schema.
 */
struct ColumnSetFooter {
  1: required i32 version;
  2: required ExplicitIndexFooterV35.FileInfo file_info;
  3: required list<ColumnSetMetadata> column_sets;
}
