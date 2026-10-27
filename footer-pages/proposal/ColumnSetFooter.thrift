/** Optional extension for very wide files, such as 30,000 physical columns.
 * Each set lists its physical-column ordinals, allowing noncontiguous groups.
 * Every set's stream descriptors are inline in the common footer directory.
 * Each stream is independently located; sets impose no byte-contiguity rule.
 * Reuses the current ModularFooterV2 types and cumulative-end indexes with an
 * implicit initial zero. Row-group descriptors and encryption are outside this
 * plaintext draft. Wire version and magic allocation are pending.
 */
include "ModularFooterV2.thrift"
include "../../modular-footer/parquet.thrift"
namespace cpp parquet.columnsets

/** File metadata stored once. N is the number of physical leaves in schema,
 * equal to the sum of column_ordinals.size() across all sets. No separate
 * column count is stored. Field 1 is reserved for the former num_columns.
 * References to FileInfo.num_rows in reused types mean this file's num_rows.
 */
struct FileInfo {
  2: required i64 num_rows;
  3: required list<parquet.SchemaElement> schema;
  4: optional string created_by;
}

/** One logical group of complete physical columns, in any source-ordinal order.
 * C = column_ordinals.size(); K, P and D in reused definitions are local to this set.
 * Local column c maps to source-schema physical ordinal column_ordinals[c].
 * Every StreamLocation has an ABSOLUTE file offset, including statistics streams.
 * Source-data offsets in ColumnChunkMetadata.offsets and DictionaryMetadata.offsets
 * are also absolute. relative_page_offsets values remain relative to their
 * data chunk's start.
 * Store only dictionaries referenced by this set and rebase IDs to its table.
 * Dictionary reuse is confined to one physical column. Sets contain complete
 * columns, so all references to a dictionary belong to one set: no global
 * lookup, cross-set references, or duplicated dictionary entries are needed.
 * Null slots and empty columns/chunks keep the existing explicit-index rules.
 */
struct ColumnSetMetadata {
  /** Inline list<i32>; one source-schema physical ordinal per local column.
   * Nullability: list and values are non-null; list must be nonempty.
   * Ordinals are in [0,N), distinct within and across sets, and need not be
   * consecutive or sorted. List order defines local column order in all streams.
   * Ordinal 0 is real membership and must be stored when included.
   * List length gives C; no first_column or num_columns field is needed.
   * column_chunk_index has C cumulative ends; page_index, when present, has K.
   * Both indexes omit only their fixed leading zero; genuine zero ends remain.
   * Field 2 is reserved for the former num_columns.
   */
  1: required list<i32> column_ordinals;
  3: required ModularFooterV2.DataLayout data_layout;
  4: required ModularFooterV2.DictionaryMetadata dictionary_metadata;
  6: required ModularFooterV2.ColumnChunkMetadata column_chunk_metadata;
  7: optional ModularFooterV2.PageMetadata page_metadata;

  /** Statistics descriptors for these same local columns, inline in the directory.
   * Each leaf is an independently fetchable stream. COLUMN_CHUNK/PAGE keep
   * unknown slots and reuse this set's indexes and chunk/page row starts;
   * only ARBITRARY stores its own column index and row starts.
   * Any number of granularities is allowed; align them by row interval.
   * Omission means unknown statistics. Reading location streams does not
   * require fetching statistics payloads. Decoding one leaf's values does
   * not require sibling leaves; interpreting ranges requires shared metadata.
   */
  8: optional list<ModularFooterV2.ColumnRangeStatistics> row_range_statistics;
}

/** Tail: [independently located streams][Compact-Thrift root]
 * [u64 LE absolute root offset][column-set magic; allocation pending].
 * Stream extents must lie before the root, within the file and not overlap.
 * Streams from different sets or fields may be interleaved or separated by gaps.
 * No set-level offset/length, embedded payload, or ordering by set is required.
 * Readers fetch the directory, then location streams for the selected sets.
 * Statistics leaves can be fetched later using the same local hierarchy.
 * Validate stream extents and column membership before dependent reads.
 * FileInfo/schema is stored once; set size is a writer choice. Column chunk
 * counts may differ, including zero. Every physical column occurs in exactly
 * one set, including columns with no chunks. A zero-column file has no sets.
 * Set order is unrestricted; the union of column_ordinals must equal [0,N).
 * Validate against the schema; reject duplicate, missing or out-of-range ordinals.
 * This directory layout requires a new version; it does not read historical
 * column-set artifacts that used contiguous column ranges or leading-zero indexes.
 */
struct ColumnSetFooter {
  1: required i32 version; // Allocation pending.
  2: required FileInfo file_info;
  3: required list<ColumnSetMetadata> column_sets;
}
