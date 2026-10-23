/** V29 schema draft: store chunk/page list structure once per group.
 * Encoder, decoder and measurements are pending; V28 results do not apply.
 * These are directories of ordinary Parquet page streams. Sharing structure
 * between streams is a footer-specific mapping, not standard LIST<STRUCT>.
 *
 * C = columns; K_c = chunks in column c; K = sum(K_c); R = regions.
 * P_k = data pages in chunk k; P = sum(P_k); M = key/value pairs.
 * K_c may differ between columns and may be zero. There is no fixed chunk
 * count per column and no implied correspondence to row-group ordinal.
 * Chunk membership does not describe row coverage or the meaning of absent data.
 * Row positions must come from row metadata, not chunk ordinal or num_values.
 *
 * list<T> denotes a flat stream; list<list<T>> denotes a stream of lists.
 * The outer collection is the stream's row sequence; only inner lists use
 * Parquet repetition/definition levels. Counts include null value positions
 * but exclude the level markers representing empty lists.
 * Thrift optional controls stream presence, separately from value nullability.
 *
 * region_ordinals carries the structure for all chunk fields: C non-null lists
 * of non-null ordinals. Flattening these lists yields K chunk positions.
 * Each other chunk stream has K positions in exactly that order. An empty
 * column list contributes no position, including in nullable chunk streams.
 *
 * relative_page_offsets carries the page structure when present: K non-null
 * lists of non-null offsets. page_first_rows then has P flat positions.
 * If relative_page_offsets is absent, page_first_rows carries the K lists.
 * If both are absent, no page structure is stored. Selection depends only on
 * field presence in the directory, never on which stream a query requested.
 *
 * Structure streams use max RL=1, max DL=1: DL=0 marks an empty list,
 * DL=1 a value; RL=0 starts a list and RL=1 continues it. DATA_PAGE_V2 pages
 * contain complete lists, with num_rows counting columns or chunks.
 * Flat non-null streams use max RL=DL=0 and store no levels. Flat nullable
 * streams use max RL=0, max DL=1 (0 null, 1 present), with no repetition levels.
 * Flat page num_rows counts value positions, including nulls. Stream page
 * boundaries need not align; alignment uses flattened positions, not page IDs.
 * Readers validate C/K/P counts and construct boundaries from the structure
 * stream; no separate column_chunk_index, page_index or count array is stored.
 *
 * Stream protection and region meanings follow ModularFooterV2. Readers must
 * authenticate/decrypt a structure stream before using it to interpret other
 * streams. An unavailable structure stream does not mean empty lists and must
 * not cause the reader to choose another structure stream. Use V29 in AAD.
 */
include "ModularFooterV2.thrift"
include "../modular-footer/parquet.thrift"
namespace cpp parquet.sharedstructure

struct SharedDataLocation {
  /** list<i64>; number of values = R+1.
   * Nullability: list and values are non-null.
   * Strictly increasing absolute region boundaries; unchanged region map.
   */
  1: required ModularFooterV2.StreamLocation region_offsets;

  /** list<list<i32>>; number of lists = C.
   * Number of values = K_c in column c's list; K total across all lists.
   * Nullability: lists and values are non-null; lists may be empty.
   * Data-region ordinals. This is the only chunk stream with LIST levels.
   */
  2: required ModularFooterV2.StreamLocation region_ordinals;

  /** list<list<i64>>; number of lists = K.
   * Number of values = P_k in chunk k's list; P total across all lists.
   * Nullability: lists and values are non-null; lists may be empty.
   * Carries page structure whenever present. Offsets remain RELATIVE to the
   * chunk's data-region start; nonempty lists start at 0 and strictly increase.
   * The final page ends at the region end.
   */
  3: optional ModularFooterV2.StreamLocation relative_page_offsets;
}

/** Flat values grouped by SharedDataLocation.region_ordinals. */
struct SharedChunkFields {
  /** list<i32>; number of values = K.
   * Nullability: list and values are non-null.
   * Values are parquet.CompressionCodec enum values.
   */
  1: required ModularFooterV2.StreamLocation codecs;

  /** list<i32>; number of values = K.
   * Nullability: list is non-null; values are nullable.
   * Null means no dictionary region. Only this chunk field stores null levels;
   * they describe dictionary presence, not column/chunk list boundaries.
   */
  2: required ModularFooterV2.StreamLocation dictionary_regions;

  /** list<bool>; number of values = K.
   * Nullability: list and values are non-null.
   * True when all chunk data pages use its dictionary; false without one.
   */
  3: required ModularFooterV2.StreamLocation is_fully_dictionary_encoded;

  /** list<i64>; number of values = K.
   * Nullability: list and values are non-null.
   * Uncompressed sizes retain V23 semantics; omission means sizes are unknown.
   */
  4: optional ModularFooterV2.StreamLocation total_uncompressed_sizes;

  /** list<i64>; number of values = K.
   * Nullability: list and values are non-null.
   * Data-value counts including nulls; not a source of chunk row ranges.
   */
  5: required ModularFooterV2.StreamLocation num_values;
}

struct SharedPageFields {
  /** When relative_page_offsets is present: list<i64>; number of values = P.
   * Nullability: list and values are non-null. No repetition/definition levels.
   * Grouped by the lists in relative_page_offsets.
   *
   * Otherwise: list<list<i64>>; number of lists = K.
   * Number of values = P_k in chunk k's list; P total across all lists.
   * Nullability: lists and values are non-null; lists may be empty.
   * Carries page LIST levels in this case.
   *
   * Values are absolute first-row indexes, in chunk order then page order.
   */
  1: required ModularFooterV2.StreamLocation page_first_rows;
}

/** V29 overrides layout references in inherited statistics definitions:
 * COLUMN_CHUNK starts come from each participating chunk's first page row;
 * page_first_rows must exist and each participating chunk must have a page.
 * PAGE starts come from page_first_rows in the shared page structure's order.
 * Neither kind derives starts from row-group ordinal, even if row-group
 * metadata exists. In particular, COLUMN_CHUNK does not require K_c = G.
 * Both kinds retain V23's nonempty, row-aligned partition requirement. Use
 * explicit ARBITRARY statistics ranges when those requirements do not hold;
 * no statistics may be inferred for gaps solely from chunk list membership.
 * These rules also apply to inherited independently encrypted statistics.
 * Statistics retain their own range indexes and field nulls; only core
 * chunk/page structure is shared by this draft.
 */
struct SharedStructureFooter {
  1: required i32 version; // 29; older readers must reject this version.
  2: optional ModularFooterV2.FileInfo file_info;
  // Field 3 (DataLayout) retired: neither hierarchy index is stored.
  4: required SharedDataLocation data_location;
  5: optional ModularFooterV2.RowGroupMetadata row_group_metadata;
  6: required SharedChunkFields column_chunk_metadata;
  7: optional SharedPageFields page_metadata;
  8: optional list<ModularFooterV2.ColumnIndexedRangeStatistics> row_range_statistics;

  /** list<binary>; number of values = M.
   * Nullability: list and values are non-null. Physical type: BYTE_ARRAY.
   */
  9: optional ModularFooterV2.StreamLocation key_value_metadata_keys;

  /** list<binary>; number of values = M, aligned with key_value_metadata_keys.
   * Nullability: list is non-null; values are nullable. Physical type: BYTE_ARRAY.
   */
  10: optional ModularFooterV2.StreamLocation key_value_metadata_values;
  11: optional parquet.FileCryptoMetaData crypto_metadata;
  12: optional binary encrypted_file_info;
  13: optional ModularFooterV2.EncryptedRowRangeStatistics encrypted_row_range_statistics;
}
