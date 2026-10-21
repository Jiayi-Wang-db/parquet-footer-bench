/** Modular Footer V2 stores metadata arrays as typed Parquet page streams.
 * A compact FileMetaData directory locates streams for data placement, page indexes,
 * statistics and key/value metadata. Field IDs define each stream's physical type
 * and nullability, so metadata streams need no separate schema.
 *
 * FileInfo holds the source-data schema, row/column counts and writer information,
 * stored either in plaintext or encrypted with the footer key. Shared streams are
 * protected independently, allowing readers to fetch only the metadata they need.
 * Column statistics can use separate column keys and multiple row-range granularities.
 *
 * The directory remains readable and is authenticated in protected files.
 * A fixed trailer locates FileMetaData; all protection modes use PAR3.
 */
include "../modular-footer/parquet.thrift"

namespace java org.apache.parquet.experimental.pairedboundsfooterpages
namespace cpp parquet.pairedboundsfooterpages
namespace py paired_bounds_footer_pages

/** Standard PageHeader + body pairs: [optional DICTIONARY_PAGE][DATA_PAGE_V2 ...].
 * No specialized page header. Field IDs imply physical types and nullability.
 * Shared streams inherit FileMetaData protection: plaintext, independently signed,
 * or independently encrypted with the footer key, including headers and dictionaries.
 * Inside ColumnRowRangeStatistics, streams are plaintext within its encrypted module.
 */
struct StreamLocation {
  /** Absolute file offset for shared streams; relative to page_streams inside a column module. */
  1: required i64 offset;

  /** Complete stored extent: page headers/bodies plus any outer protection bytes.
    * Shared signed streams append a 28-byte signature; shared encrypted streams
    * add a 4-byte length prefix, 12-byte nonce and 16-byte GCM tag.
    * Streams inside a column module have no individual protection overhead.
    */
  2: required i64 length;

  /** Dictionary bodies use this codec; V2 values use it when is_compressed is true.
    * Levels are uncompressed. Encode and compress before protection.
    * Independent of source-data ColumnChunkMetadata.codecs.
    */
  3: required parquet.CompressionCodec codec;
}

/** C columns, K column chunks, P data pages, R regions, G row groups.
 * Index values are positions in flattened metadata arrays, not byte offsets.
 * K and P must be <= 2147483647, including terminal index entries.
 */
struct DataLayout {
  /** INT32, required, C+1 entries, starting at 0 and ending at K.
    * Column-chunk metadata is flattened in column order.
    * Column c's metadata occupies: [column_chunk_index[c], column_chunk_index[c+1]).
    */
  1: required StreamLocation column_chunk_index;

  /** INT32, required, K+1 entries, starting at 0 and ending at P.
    * Data-page metadata is flattened in column-chunk order, then logical page order within each chunk.
    * Column chunk cc's metadata occupies: [page_index[cc], page_index[cc+1]).
    */
  2: required StreamLocation page_index;
}

struct DataLocation {
  /** INT64, required, R+1 entries, storing strictly increasing absolute file offsets.
    * Region r occupies: [region_offsets[r], region_offsets[r+1]).
    * A data region contains one column chunk's contiguous data pages, including headers and bodies.
    * Dictionary pages occupy separate regions; other regions may contain arbitrary data.
    */
  1: required StreamLocation region_offsets;

  /** INT32, required, K entries, storing one data-region ordinal per column chunk.
    * Column-chunk metadata is flattened in column order.
    * Column chunk cc's data region is: region_ordinals[cc].
    * Ordinals are unique, in [0,R), and disjoint from dictionary-region ordinals.
    */
  2: required StreamLocation region_ordinals;

  /** INT64, required, P entries, storing page-header offsets relative to the column chunk's region start.
    * Data-page metadata is flattened in column-chunk order, then logical page order within each chunk.
    * Column chunk cc's page offsets occupy: [page_index[cc], page_index[cc+1]).
    * Offsets start at 0 and strictly increase within each chunk; the last page ends at the region end.
    * If the stream is absent, locating interior pages requires scanning page headers.
    */
  3: optional StreamLocation relative_page_offsets;
}

// In physical-type comments, required means non-null values inside the stream.
// Thrift required/optional controls whether the field itself is present.

struct RowGroupMetadata {
  /** INT64, required, G entries, storing row counts in row-group order.
    * Counts sum to FileInfo.num_rows.
    */
  1: required StreamLocation row_group_num_rows;

  /** INT32, required, G+1 entries, storing region-boundary ordinals.
    * Row group g occupies regions: [row_group_ordinals[g], row_group_ordinals[g+1]).
    * Byte endpoints follow from DataLocation.region_offsets; dictionaries are included.
    */
  2: required StreamLocation row_group_ordinals;
}

/** K entries per stream, flattened in column order.
 * Column c's metadata occupies: [column_chunk_index[c], column_chunk_index[c+1]).
 */
struct ColumnChunkMetadata {
  /** INT32, required, K entries, storing parquet.CompressionCodec values. */
  1: required StreamLocation codecs;

  /** INT32, nullable, K entries, storing one dictionary-region ordinal per chunk.
    * Null means no dictionary; multiple chunks may reference the same dictionary region.
    * Each referenced region contains one complete source dictionary page.
    */
  2: required StreamLocation dictionary_regions;

  /** BOOLEAN, required, K entries, aligned with dictionary_regions.
    * True means every data page in the chunk uses the referenced dictionary.
    * False when dictionary_regions is null or any data page uses another encoding.
    */
  3: required StreamLocation is_fully_dictionary_encoded;

  /** INT64, required, K entries, storing all page headers plus uncompressed bodies.
    * Includes the referenced dictionary once per chunk, even if shared with other chunks.
    * Omission means sizes are unknown; values are non-null when the stream is present.
    */
  4: optional StreamLocation total_uncompressed_sizes;

  /** INT64, required, K entries, storing data-value counts including nulls. */
  5: required StreamLocation num_values;
}

struct PageMetadata {
  /** INT64, required, P entries, storing absolute first-row indexes.
    * Data-page metadata is flattened in column-chunk order, then logical page order within each chunk.
    * Column chunk cc's page rows occupy: [page_index[cc], page_index[cc+1]).
    */
  1: required StreamLocation page_first_rows;
}

/** S ranges for one granularity; the containing struct determines column ownership.
 * Boundaries need not align with pages, column chunks or row groups.
 * Missing statistics mean unknown, including whether the data is all null.
 */
struct RangeStatistics {
  /** INT64, required, S entries, storing absolute first-row indexes.
    * For a column with ranges, starts begin at 0 and strictly increase below FileInfo.num_rows.
    * A range ends at the next start for the same column, or FileInfo.num_rows.
    */
  1: required StreamLocation range_start_rows;

  /** BOOLEAN, nullable, S entries; true means the range contains at least one null.
    * False means no nulls; null or an omitted stream means unknown.
    */
  2: optional StreamLocation contains_null;

  /** BOOLEAN, nullable, S entries; true means the range contains at least one NaN.
    * False means no NaNs; null or an omitted stream means unknown. NaN is not null.
    */
  3: optional StreamLocation contains_nan;

  /** BOOLEAN, nullable, S entries; true means the range contains only null values.
    * False means at least one non-null value; null or an omitted stream means unknown.
    */
  4: optional StreamLocation is_all_null;

  /** INT64, nullable, S entries; null or an omitted stream means unknown. */
  5: optional StreamLocation distinct_counts;

  /** Each bound stream contains 2S nullable scalar entries, ordered as
    * [min_0, max_0, min_1, max_1, ...]. Range r occupies [2r, 2r+2).
    * Wrong physical type or unavailable bound is null; each bound is independently nullable.
    * A pair may cross a metadata-page boundary. Missing stream means unknown bounds.
    */
  6: optional StreamLocation min_max_boolean; // BOOLEAN, nullable, 2S entries.
  7: optional StreamLocation min_max_int32; // INT32, nullable, 2S entries.
  8: optional StreamLocation min_max_int64; // INT64, nullable, 2S entries.
  9: optional StreamLocation min_max_float; // FLOAT, nullable, 2S entries.
  10: optional StreamLocation min_max_double; // DOUBLE, nullable, 2S entries.
  11: optional StreamLocation min_max_byte_array; // BYTE_ARRAY, nullable, 2S entries.

  /** BOOLEAN, nullable, 2S entries, ordered as [min_exact_0, max_exact_0, ...].
    * Positions match the paired bounds. True means exact, false means inexact;
    * null means unknown. An unavailable bound has a null exactness entry.
    */
  12: optional StreamLocation min_max_is_exact;
}

/** One shared granularity across C columns, with S ranges in total.
 * Referenced from FileMetaData; each shared stream inherits its protection mode.
 * A column with ranges partitions [0, FileInfo.num_rows).
 * A column may have ranges in one granularity and none in another.
 * Columns with independent encrypted statistics have no ranges in any shared granularity.
 */
struct ColumnIndexedRangeStatistics {
  /** INT64, required, C+1 entries, starting at 0 and ending at S.
    * Statistics metadata is flattened in column order.
    * Column c's statistics occupy: [column_range_starts[c], column_range_starts[c+1]).
    * Values are non-decreasing; equal adjacent values mean this column has no statistics.
    * Such a column contributes no entries to any per-range stream.
    */
  1: required StreamLocation column_range_starts;

  /** S ranges; column c's paired bounds occupy
    * [2*column_range_starts[c], 2*column_range_starts[c+1]).
    */
  2: required RangeStatistics statistics;

  /** INT32, required, C entries, storing parquet.BoundaryOrder values.
    * Describes bound ordering across each column's ranges in this granularity.
    * Columns without shared statistics must be UNORDERED; omission means all UNORDERED.
    */
  3: optional StreamLocation boundary_orders;
}

/** Plaintext of one encrypted column-statistics module; Compact-Thrift serialized.
 * Every range belongs to the column identified by EncryptedColumnRowRangeStatistics.column_ordinal.
 * Each list entry has its own S ranges partitioning [0, FileInfo.num_rows).
 * There is no column_range_starts; range_start_rows uses absolute file row indexes.
 * Only the column's physical-type bound stream may be present, with 2S entries.
 * Every StreamLocation.offset below is relative to page_streams, not the footer or file.
 * All referenced streams, dictionaries and page headers are contained in page_streams.
 */
struct ColumnRowRangeStatistics {
  /** Independent granularities; nonempty, with the same semantics as the root list.
    * List positions are local to this payload, not shared with other columns or the root.
    * All granularities are encrypted together in one column module.
    */
  1: required list<RangeStatistics> statistics;

  /** Concatenated ordinary metadata page streams, encoded and compressed before encryption.
    * Each stream permits an optional DICTIONARY_PAGE followed by DATA_PAGE_V2 pages.
    * StreamLocation supplies each stream's offset, length and compression codec.
    */
  2: required binary page_streams;

  /** One value per statistics list entry, describing bound ordering within that granularity.
    * Omission means all UNORDERED; an unavailable order is represented as UNORDERED.
    */
  3: optional list<parquet.BoundaryOrder> boundary_orders;
}

/** One independently encrypted module for one physical column across the file. */
struct EncryptedColumnRowRangeStatistics {
  /** Physical leaf-column ordinal in FileInfo.schema; 0 <= column_ordinal < C. */
  1: required i32 column_ordinal;

  /** Exactly one PME key selector; no raw key material.
    * ENCRYPTION_WITH_COLUMN_KEY: path_in_schema must match column_ordinal;
    * resolve that column's DEK using its key metadata.
    * ENCRYPTION_WITH_FOOTER_KEY: use the footer DEK resolved from FileMetaData.crypto_metadata.
    * One selected key applies across all granularities and row groups for this column.
    */
  2: required parquet.ColumnCryptoMetaData crypto_metadata;

  /** Absolute file offset of the encrypted module's length prefix.
    * The block precedes FileMetaData and is disjoint from shared streams.
    */
  3: required i64 offset;

  /** Total module bytes: 4-byte LE length + 12-byte nonce + ciphertext + 16-byte GCM tag.
    * The length prefix excludes its own four bytes. The plaintext is ColumnRowRangeStatistics.
    * This is a metadata block; PME encrypts metadata with AES-GCM under both
    * AES_GCM_V1 and AES_GCM_CTR_V1.
    */
  4: required i64 length;
}

/** E independently encrypted column-statistics modules; omit this struct when E is zero.
 * Each listed column has zero ranges in every shared ColumnIndexedRangeStatistics entry.
 * In ENCRYPTED mode, footer-key statistics may stay in the shared arrays.
 * Statistics requiring a separate column key must use an independent block.
 * In SIGNED mode, any protected statistics require an independent block.
 * Missing key means unavailable statistics, not all-null or an empty data column.
 * A failed authentication or malformed decrypted payload is an error, not missing statistics.
 * See ENCRYPTED-ROW-RANGE-STATS.md for module AAD, signing and reader rules.
 */
struct EncryptedRowRangeStatistics {
  /** E entries, sorted by column_ordinal, with no duplicate columns or overlapping blocks. */
  1: required list<EncryptedColumnRowRangeStatistics> columns;
}

/** Small inline metadata; stored once, either plaintext or encrypted with the footer key. */
struct FileInfo {
  1: required i32 num_columns; // Physical columns.
  2: required i64 num_rows;
  3: required list<parquet.SchemaElement> schema;
  4: optional string created_by;
}

/** Stream directory, with two alternatives for the inline file information.
 * The directory and column-key descriptors remain visible in every mode.
 * Optional groups contain their required shared indexes and arrays together.
 * PageMetadata contains row positions for all P data pages when present.
 * If absent, read data pages to determine their row positions.
 * Relative byte offsets remain independently optional in DataLocation.
 *
 * File layout: [source data][metadata streams and column-statistics blocks]
 * [Compact-Thrift FileMetaData][28-byte signature when crypto_metadata is present]
 * [u64 LE absolute FileMetaData offset][PAR3].
 *
 * Authenticate the exact serialized FileMetaData bytes before trusting this directory
 * or decrypting encrypted_file_info. Its signature is nonce12 + GCM-tag16, computed
 * by encrypting these bytes and discarding the ciphertext, as in PME signing.
 * Every signing/encryption invocation uses a fresh nonce under its key.
 *
 * AAD uses the PME file AAD (prefix + aad_file_unique) and distinct module types:
 *   directory: file AAD + NEW_FOOTER_DIRECTORY_TYPE + LE32(version).
 *   FileInfo: file AAD + NEW_FILE_INFO_TYPE + LE32(version).
 *   stream: file AAD + NEW_METADATA_STREAM_TYPE + LE32(version) + LE32(path length)
 *           + LE32(each path component) + LE64(offset) + LE64(length) + LE32(codec).
 *   column stats: file AAD + NEW_ROW_RANGE_STATISTICS_TYPE + LE32(version) + LE32(column ordinal).
 * Stream paths contain Thrift field IDs and zero-based list positions from FileMetaData;
 * for example [3,1] identifies column_chunk_index, and [8,g,2,7] identifies
 * granularity g's min_max_int32. The schema distinguishes field IDs from list positions.
 * Numeric module-type allocations are pending; do not reuse existing PME module IDs.
 *
 * Shared streams and column blocks must precede this struct and be nonoverlapping.
 * Verify each selected stream before parsing its pages. Enforce the caller's expected
 * protection policy; stripping crypto metadata must not downgrade an encrypted file.
 */
struct FileMetaData {
  1: required i32 version; // 21.

  /** Exactly one of file_info and encrypted_file_info must be present. */
  2: optional FileInfo file_info;

  3: required DataLayout data_layout;
  4: required DataLocation data_location;

  5: optional RowGroupMetadata row_group_metadata;
  6: required ColumnChunkMetadata column_chunk_metadata;
  7: optional PageMetadata page_metadata;
  /** Independent granularities, usually row-group and page-level statistics.
    * Each entry has its own indexes, row boundaries and S; there is no two-entry limit.
    * Writers should order coarse to fine; boundaries need not nest across entries.
    * Readers use row intervals, not list positions, to match ranges across entries.
    * Counts from overlapping granularities must not be added together.
    * Omit entries with no ranges, and omit the list when no shared statistics exist.
    */
  8: optional list<ColumnIndexedRangeStatistics> row_range_statistics;

  9: optional StreamLocation key_value_metadata_keys; // BYTE_ARRAY, required.
  10: optional StreamLocation key_value_metadata_values; // BYTE_ARRAY, nullable.

  /** Plaintext algorithm/AAD parameters and opaque footer-key retrieval metadata, never a raw key.
    * Absent: file_info and shared streams are plaintext; no footer signature.
    * Present with file_info: shared streams each have a 28-byte signature.
    * Present with encrypted_file_info: shared streams each use footer-key AES-GCM.
    * In BOTH protected modes, a 28-byte signature follows serialized FileMetaData,
    * authenticating the complete directory, crypto metadata and inline alternative.
    * Missing key_metadata means external key resolution; aad_file_unique is required.
    * Metadata uses AES-GCM under both PME algorithms; supply any requested external AAD prefix.
    */
  11: optional parquet.FileCryptoMetaData crypto_metadata;

  /** Serialized FileInfo encrypted with the footer key; requires crypto_metadata.
    * PME module: 4-byte LE length (excluding itself), 12-byte nonce, ciphertext, 16-byte GCM tag.
    * Thrift's binary field delimits this inline value; no root offset or length is needed.
    */
  12: optional binary encrypted_file_info;

  /** Independently encrypted statistics; disjoint from shared row_range_statistics. */
  13: optional EncryptedRowRangeStatistics encrypted_row_range_statistics;
}
