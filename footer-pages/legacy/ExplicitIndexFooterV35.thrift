/** Standalone draft: explicit indexes for the column -> chunk -> page hierarchy.
 * All proposal types are local; parquet.thrift supplies standard Parquet types.
 * V35 combines direct chunk offsets/lengths, a dictionary table, and the V31
 * shared indexes and tagged statistics. It replaces V31's region map; these
 * versions are not wire-compatible. See legacy/ExplicitIndexFooterV31.thrift
 * for the historical measurements. Encryption remains schema-only.
 * V35 is a repository-local experiment, not a public format allocation.
 *
 * C = physical columns; K_c = chunks in column c; K = sum(K_c).
 * P_k = data pages in chunk k; P = sum(P_k); D = dictionaries; G = row groups.
 * M = key/value pairs. Each column has its own chunk count, including zero.
 * K_c need not equal G. Chunk membership does not imply row coverage.
 *
 * list<T> in comments means a flat Parquet page stream, not an inline Thrift
 * list or a Parquet LIST. i32/i64/bool/binary map to INT32/INT64/BOOLEAN/BYTE_ARRAY;
 * float/double map to FLOAT/DOUBLE. Types are defined by field IDs; no extra
 * metadata SchemaElement tree is serialized. FileInfo.schema is the source schema.
 * Thrift optional controls stream presence, separately from value nullability.
 *
 * Every chunk field has K positions, ordered by column then chunk.
 * Every page field has P positions, ordered by flattened chunk then page.
 * DataLayout.column_chunk_index groups every chunk field; DataLayout.page_index
 * groups every page field. Nulls retain their positions. Metadata-page boundaries
 * may differ across streams; the shared indexes address logical positions.
 * These streams have different row counts and are not one ordinary Parquet row group.
 *
 * Non-null streams have max RL=DL=0. Nullable streams have max RL=0, max DL=1:
 * DL=0 is null; DL=1 is present. No hierarchy repetition levels are stored.
 * COLUMN_CHUNK/PAGE statistics reuse these indexes and omit their own index.
 * ARBITRARY statistics retain a separate column-to-range index and row starts.
 * Paired min/max streams have two values per range; their positions are 2r and 2r+1.
 *
 * Example: C=2, K=3, P=5; columns have 2 and 1 chunks.
 *   column_chunk_index:    [0, 2, 3]
 *   data_offsets:          [1000, 1400, 2000]
 *   data_lengths:          [200, 100, 300]
 *   dictionary_ids:        [0, 0, null]
 *   dictionary_offsets:    [800]
 *   dictionary_lengths:    [100]
 *   page_index:            [0, 2, 3, 5]
 *   relative_page_offsets: [0, 100, 0, 0, 200]
 *   page_first_rows:       [0, 50, 100, 0, 80]
 * C = column_chunk_index.length - 1, checked against FileInfo.num_columns.
 * K = column_chunk_index[C] = page_index.length - 1.
 * P = page_index[K]. Equal adjacent index entries represent an empty list.
 * Each nonempty chunk retains its first relative page offset, 0.
 *
 * Equivalent per-stream Parquet schemas (illustrative, not serialized):
 *   message column_chunk_index { required int32 value; }
 *   message page_index { required int32 value; }
 *   message data_offsets { required int64 value; }
 *   message data_lengths { required int64 value; }
 *   message dictionary_ids { optional int32 value; }
 *   message dictionary_offsets { required int64 value; }
 *   message dictionary_lengths { required int64 value; }
 *   message relative_page_offsets { required int64 value; }
 *   message page_first_rows { required int64 value; }
 *
 * Readers validate lengths, monotonic indexes and terminal counts. Authenticate
 * or decrypt an index stream before using it to interpret dependent streams.
 * An unavailable index is not an empty hierarchy.
 */
include "../../modular-footer/parquet.thrift"
namespace cpp parquet.explicitindex

/** Standard PageHeader + body pairs: [optional DICTIONARY_PAGE][DATA_PAGE_V2 ...].
 * Field IDs imply physical types and nullability; no separate stream schema.
 * Shared streams inherit root protection: plaintext, independently signed,
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

/** Shared hierarchy indexes; values are element positions, not file offsets.
 * K and P must fit in signed i32. All index values are non-negative.
 */
struct DataLayout {
  /** list<i32>; number of values = C+1.
   * Nullability: list and values are non-null.
   * Non-decreasing, starting at 0 and ending at K.
   * Column c owns chunks [column_chunk_index[c], column_chunk_index[c+1]).
   */
  1: required StreamLocation column_chunk_index;

  /** list<i32>; number of values = K+1.
   * Nullability: list and values are non-null.
   * Non-decreasing, starting at 0 and ending at P.
   * Chunk k owns pages [page_index[k], page_index[k+1]).
   * Required when relative_page_offsets or page_metadata is present.
   * PAGE statistics require page_metadata and therefore this index.
   * Otherwise it may be omitted; source page headers then supply page grouping.
   */
  2: optional StreamLocation page_index;
}

/** Dictionary table and optional page locations. Chunk extents are stored
 * directly in ColumnChunkMetadata. Dictionary IDs address table positions;
 * no additional dictionary-to-column index or region map is stored.
 */
struct DataLocation {
  /** list<i64>; number of values = D; D must fit in signed i32.
   * Nullability: list and values are non-null.
   * Absolute offsets of complete source dictionary pages, including headers.
   * Store each referenced dictionary once. Dictionaries can be before or after
   * their data chunks; no adjacency or file-order requirement is imposed.
   * First-reference table order is recommended for compact dictionary IDs.
   * Both table streams have zero values when no chunk uses a dictionary.
   */
  1: required StreamLocation dictionary_offsets;

  /** list<i64>; number of values = D.
   * Nullability: list and values are non-null; all lengths are positive.
   * Stored byte lengths, including the dictionary page header and body.
   * Dictionary d occupies [dictionary_offsets[d], offset + dictionary_lengths[d]).
   * Extents are disjoint from data extents and other dictionary entries.
   */
  2: required StreamLocation dictionary_lengths;

  /** list<i64>; number of values = P.
   * Nullability: list and values are non-null. No repetition/definition levels.
   * Requires DataLayout.page_index, shared with page_first_rows.
   * Offsets are RELATIVE to ColumnChunkMetadata.data_offsets[k]. Each nonempty
   * chunk starts at 0 and strictly increases below data_lengths[k]; the last
   * page ends at data_offsets[k] + data_lengths[k]. Retain the first zero even
   * for a single-page chunk, so positions align with other page fields.
   * If absent, locating interior pages requires scanning source-data page headers.
   */
  3: optional StreamLocation relative_page_offsets;
}

struct RowGroupMetadata {
  /** list<i64>; number of values = G.
   * Nullability: list and values are non-null.
   * Row counts in row-group order; counts sum to FileInfo.num_rows.
   */
  1: required StreamLocation row_group_num_rows;

  /** list<i64>; number of values = G+1.
   * Nullability: list and values are non-null.
   * Non-decreasing absolute byte boundaries, if row groups occupy contiguous
   * intervals in row-group order. Omit for interleaved layouts or dictionaries
   * shared across row groups when a complete row-group extent cannot be given.
   * Readers use chunk and dictionary locations independently of this hint.
   */
  2: optional StreamLocation row_group_offsets;
}

/** Flat fields grouped by DataLayout.column_chunk_index. */
struct ColumnChunkMetadata {
  /** list<i32>; number of values = K.
   * Nullability: list and values are non-null.
   * Values are parquet.CompressionCodec enum values.
   */
  1: required StreamLocation codecs;

  /** list<i32>; number of values = K.
   * Nullability: list is non-null; values are nullable.
   * Null means no dictionary. Present values are table IDs in [0,D).
   * Only this chunk field stores null levels;
   * they describe dictionary presence, not column/chunk list boundaries.
   * Multiple chunks OF THE SAME PHYSICAL COLUMN may reference the same ID.
   * Cross-column dictionary sharing is not permitted. Column ownership follows
   * from column_chunk_index; it does not require another mapping array.
   * Dictionary bytes and decoding must be compatible with every referencing
   * chunk, including its codec. Every table entry must be referenced.
   */
  2: required StreamLocation dictionary_ids;

  /** list<bool>; number of values = K.
   * Nullability: list and values are non-null.
   * True when all chunk data pages use its dictionary; false without one.
   */
  3: required StreamLocation is_fully_dictionary_encoded;

  /** list<i64>; number of values = K.
   * Nullability: list and values are non-null.
   * All page headers plus uncompressed bodies, including the referenced
   * dictionary once per chunk even when shared. Omission means sizes are unknown.
   */
  4: optional StreamLocation total_uncompressed_sizes;

  /** list<i64>; number of values = K.
   * Nullability: list and values are non-null.
   * Data-value counts including nulls; not a source of chunk row ranges.
   */
  5: required StreamLocation num_values;

  /** list<i64>; number of values = K.
   * Nullability: list and values are non-null. No repetition/definition levels.
   * Absolute first-row indexes, aligned with flattened data_offsets.
   * Required when any COLUMN_CHUNK statistics set is present, including
   * independently encrypted statistics. May be omitted otherwise.
   * Counts include chunks in every column; an empty column adds no position.
   * Starts are independent of row-group ordinal and num_values. If page-row
   * metadata is present for a nonempty chunk, its first row must agree.
   */
  6: optional StreamLocation chunk_first_rows;

  /** list<i64>; number of values = K.
   * Nullability: list and values are non-null.
   * Absolute offsets of each chunk's contiguous data pages, including headers.
   * Dictionary bytes are excluded. Ordered by column then chunk, with no
   * requirement that byte offsets increase or that chunks be adjacent.
   */
  7: required StreamLocation data_offsets;

  /** list<i64>; number of values = K.
   * Nullability: list and values are non-null; lengths are non-negative.
   * Complete stored data-page bytes, excluding the referenced dictionary.
   * Chunk k occupies [data_offsets[k], data_offsets[k] + data_lengths[k]).
   * Zero exactly when the chunk has no data pages; nonempty extents are disjoint.
   * Validate offsets and sums against file bounds without integer overflow.
   * Length is explicit; never infer it from another chunk or a dictionary.
   */
  8: required StreamLocation data_lengths;
}

struct PageMetadata {
  /** list<i64>; number of values = P.
   * Nullability: list and values are non-null. No repetition/definition levels.
   * Absolute first-row indexes, grouped by DataLayout.page_index.
   * This stream may exist without relative_page_offsets; it still requires
   * page_index. Chunk membership and num_values do not supply row positions.
   */
  1: required StreamLocation page_first_rows;
}

/** How one statistics set obtains its row ranges. */
enum RowRangeKind {
  COLUMN_CHUNK = 0,
  PAGE = 1,
  ARBITRARY = 2
}

/** S ranges for one granularity; the containing struct determines column ownership.
 * ARBITRARY boundaries need not align with pages, column chunks or row groups.
 * Missing statistics mean unknown, including whether the data is all null.
 */
struct RangeStatistics {
  /** list<i64>; number of values = S.
   * Nullability: list and values are non-null.
   * Absolute first-row indexes; present exactly when range_kind is ARBITRARY.
   * For a column with ranges, starts begin at 0 and strictly increase below
   * FileInfo.num_rows. A range ends at the next start for the same column,
   * or FileInfo.num_rows.
   */
  1: optional StreamLocation range_start_rows;

  /** list<bool>; number of values = S.
   * Nullability: list is non-null; values are nullable.
   * True means at least one null; false means no nulls.
   * Null or an omitted stream means unknown.
   */
  2: optional StreamLocation contains_null;

  /** list<bool>; number of values = S.
   * Nullability: list is non-null; values are nullable.
   * True means at least one NaN; false means no NaNs. NaN is not null.
   * Null or an omitted stream means unknown.
   */
  3: optional StreamLocation contains_nan;

  /** list<bool>; number of values = S.
   * Nullability: list is non-null; values are nullable.
   * True means only null values; false means at least one non-null value.
   * Null or an omitted stream means unknown.
   */
  4: optional StreamLocation is_all_null;

  /** list<i64>; number of values = S.
   * Nullability: list is non-null; values are nullable.
   * Null or an omitted stream means unknown.
   */
  5: optional StreamLocation distinct_counts;

  /** Each bound stream has 2S values, ordered [min_0, max_0, min_1, max_1, ...].
   * Nullability: lists are non-null; values are independently nullable.
   * Range r occupies [2r, 2r+2). Wrong physical type or unavailable bound is null.
   * A pair may cross a metadata-page boundary. Omitted stream means unknown bounds.
   */
  6: optional StreamLocation min_max_boolean; // list<bool>; number of values = 2S.
  7: optional StreamLocation min_max_int32; // list<i32>; number of values = 2S.
  8: optional StreamLocation min_max_int64; // list<i64>; number of values = 2S.
  9: optional StreamLocation min_max_float; // list<float>; number of values = 2S.
  10: optional StreamLocation min_max_double; // list<double>; number of values = 2S.
  11: optional StreamLocation min_max_byte_array; // list<binary>; number of values = 2S.

  /** list<bool>; number of values = 2S.
   * Nullability: list is non-null; values are nullable.
   * Ordered [min_exact_0, max_exact_0, ...], matching the paired bounds.
   * True means exact, false means inexact, null means unknown.
   * An unavailable bound has a null exactness entry.
   */
  12: optional StreamLocation min_max_is_exact;

  /** One tag for this entire statistics set. Writers emit it explicitly.
   * Omission means ARBITRARY. Unknown tags are unsupported and must not be
   * interpreted as another kind. Only ARBITRARY stores range_start_rows.
   *
   * COLUMN_CHUNK: one range per chunk of each participating column, in chunk
   * order. Starts come from ColumnChunkMetadata.chunk_first_rows, which must
   * be present. No page-row or row-group metadata is needed, and K_c can vary
   * independently per column. Never derive row counts from num_values.
   *
   * PAGE: one range per data page of each participating column, in page order.
   * Requires PageMetadata.page_first_rows and DataLayout.page_index.
   * Column membership comes from DataLayout.column_chunk_index.
   * Each participating chunk has at least one page.
   *
   * Both positional tags require nonempty, row-aligned ranges partitioning
   * [0, FileInfo.num_rows): starts begin at zero and strictly increase.
   * The next range start, or FileInfo.num_rows, is the end. Use ARBITRARY when
   * chunk/page coverage cannot satisfy this requirement; explicit ranges must
   * still partition the row interval. Starts alone do not establish gap coverage.
   *
   * Keep positions for unavailable statistics, using unknown values. In shared
   * COLUMN_CHUNK/PAGE sets, this includes columns with no available statistics
   * and independently encrypted columns; all their statistics slots are unknown.
   * ARBITRARY sets may instead omit a column via equal column_range_starts.
   * Referenced structure/row streams must be authenticated/decrypted under
   * their own protection policy before use. These rules also apply to
   * independently encrypted statistics.
   */
  13: optional RowRangeKind range_kind = RowRangeKind.ARBITRARY;
}

/** One shared granularity across C columns, with S range positions in total.
 * Each stream inherits root protection. COLUMN_CHUNK uses S=K; PAGE uses S=P.
 * Both tags keep all positions, including unknown statistics and columns whose
 * statistics are independently encrypted. Do not compact away these positions.
 * ARBITRARY uses its own per-column index and permits zero ranges for a column.
 * A column's usable ranges partition [0, FileInfo.num_rows).
 */
struct ColumnRangeStatistics {
  /** list<i64>; number of values = C+1.
   * Nullability: list and values are non-null. No repetition/definition levels.
   * Present exactly when statistics.range_kind is ARBITRARY.
   * Non-decreasing positions, starting at 0 and ending at S, in column order.
   * Column c's arbitrary ranges occupy [index[c], index[c+1]).
   * Equal adjacent entries mean this column has no arbitrary ranges.
   * COLUMN_CHUNK and PAGE omit this stream and reuse the core hierarchy.
   */
  1: optional StreamLocation column_range_starts;

  /** S range positions. Scalar statistics have S entries; paired bounds and
   * exactness have 2S entries. Range r's bounds occupy [2r, 2r+2).
   * Unknown statistics retain their positions as nulls or omitted leaf streams.
   * Let a=column_chunk_index[c], b=column_chunk_index[c+1].
   * COLUMN_CHUNK: column c owns ranges [a,b), with S=K.
   * PAGE: column c owns ranges [page_index[a],page_index[b]), with S=P.
   * ARBITRARY: use column_range_starts; S is its last entry.
   */
  2: required RangeStatistics statistics;

  /** list<i32>; number of values = C.
   * Nullability: list and values are non-null.
   * parquet.BoundaryOrder values describing bound ordering across each column's
   * ranges. Columns without shared statistics must be UNORDERED;
   * omission means all UNORDERED.
   */
  3: optional StreamLocation boundary_orders;
}

/** Plaintext of one encrypted column-statistics module; Compact-Thrift serialized.
 * Every range belongs to EncryptedColumnRowRangeStatistics.column_ordinal.
 * Each list entry has its own S ranges partitioning [0, FileInfo.num_rows).
 * Here COLUMN_CHUNK has S=K_c; PAGE has S equal to this column's page count.
 * There is no column_range_starts. Positional tags use this column's shared
 * structure and row metadata; ARBITRARY stores range_start_rows locally.
 * Only the column's physical-type bound stream may be present, with 2S entries.
 * All StreamLocation offsets here are relative to page_streams, which contains
 * every explicitly located stream. Positional tags additionally depend on
 * authenticated shared metadata, including chunk_first_rows for COLUMN_CHUNK.
 */
struct ColumnRowRangeStatistics {
  /** Independent granularities; nonempty, with the same semantics as the root list.
   * List positions are local to this payload, not shared with other columns or
   * the root. All granularities are encrypted together in one column module.
   */
  1: required list<RangeStatistics> statistics;

  /** Concatenated ordinary metadata page streams, encoded and compressed before
   * encryption. Each permits an optional DICTIONARY_PAGE followed by DATA_PAGE_V2
   * pages. StreamLocation supplies offset, length and compression codec.
   */
  2: required binary page_streams;

  /** One value per statistics list entry, describing bound ordering within that
   * granularity. Omission means all UNORDERED; unavailable orders are UNORDERED.
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
   * ENCRYPTION_WITH_FOOTER_KEY: use the footer DEK from root crypto_metadata.
   * The selected key applies across all granularities and row groups for this column.
   */
  2: required parquet.ColumnCryptoMetaData crypto_metadata;

  /** Absolute file offset of the encrypted module's length prefix.
   * The block precedes the root directory and is disjoint from shared streams.
   */
  3: required i64 offset;

  /** Total bytes: 4-byte LE length + 12-byte nonce + ciphertext + 16-byte GCM tag.
   * Length prefix excludes itself. The plaintext is ColumnRowRangeStatistics.
   * PME encrypts this metadata with AES-GCM under both AES_GCM_V1 and AES_GCM_CTR_V1.
   */
  4: required i64 length;
}

/** E independently encrypted column-statistics modules; omit when E is zero.
 * In shared ARBITRARY sets, each listed column has zero ranges.
 * In shared COLUMN_CHUNK/PAGE sets, its existing positions contain only unknown
 * statistics; an encrypted module must not cause shared positions to be removed.
 * With encrypted shared streams, footer-key statistics may stay in shared arrays;
 * statistics requiring a separate column key must use an independent block.
 * With signed shared streams, protected statistics require an independent block.
 * Missing key means unavailable statistics, not all-null or an empty data column.
 * Failed authentication or a malformed decrypted payload is an error.
 */
struct EncryptedRowRangeStatistics {
  /** E entries, sorted by column_ordinal; no duplicate columns or overlapping blocks. */
  1: required list<EncryptedColumnRowRangeStatistics> columns;
}

/** Small inline metadata; stored once, either plaintext or encrypted with the footer key. */
struct FileInfo {
  1: required i32 num_columns; // Physical columns.
  2: required i64 num_rows;
  3: required list<parquet.SchemaElement> schema;
  4: optional string created_by;
}

/** Stream directory. The directory and column-key descriptors remain visible.
 * Page metadata, when present, contains row positions for all P data pages;
 * otherwise read source-data pages to determine their row positions.
 * Relative page offsets are independently optional.
 *
 * File layout: [source data][metadata streams and column-statistics blocks]
 * [Compact-Thrift ExplicitIndexFooter][28-byte signature when crypto_metadata is present]
 * [u64 LE absolute root offset][PAR3].
 *
 * Authenticate the exact serialized root bytes before trusting the directory
 * or decrypting encrypted_file_info. The signature is nonce12 + GCM-tag16,
 * computed by encrypting these bytes and discarding the ciphertext, as in PME
 * signing. Every signing/encryption invocation uses a fresh nonce under its key.
 *
 * AAD uses the PME file AAD (prefix + aad_file_unique) and distinct module types:
 *   directory: file AAD + NEW_FOOTER_DIRECTORY_TYPE + LE32(version).
 *   FileInfo: file AAD + NEW_FILE_INFO_TYPE + LE32(version).
 *   stream: file AAD + NEW_METADATA_STREAM_TYPE + LE32(version) + LE32(path length)
 *           + LE32(each path component) + LE64(offset) + LE64(length) + LE32(codec).
 *   column stats: file AAD + NEW_ROW_RANGE_STATISTICS_TYPE + LE32(version) + LE32(column ordinal).
 * Stream paths contain Thrift field IDs and zero-based list positions from the
 * root: [6,7] identifies data_offsets; [8,g,2,7] identifies granularity g's
 * min_max_int32. The schema distinguishes field IDs from list positions.
 * Numeric module-type allocations are pending; do not reuse existing PME IDs.
 *
 * Shared streams and column blocks must precede this struct and be nonoverlapping.
 * Verify each selected stream before parsing its pages. Enforce the caller's
 * expected protection policy; stripping crypto metadata must not downgrade a file.
 */
struct ExplicitIndexFooter {
  1: required i32 version; // Experiment V35; reject V31 region-map interpretation.

  /** Exactly one of file_info and encrypted_file_info must be present. */
  2: optional FileInfo file_info;
  3: required DataLayout data_layout;
  4: required DataLocation data_location;
  5: optional RowGroupMetadata row_group_metadata;
  6: required ColumnChunkMetadata column_chunk_metadata;
  7: optional PageMetadata page_metadata;

  /** Independent granularities, each with its own range_kind and S.
   * COLUMN_CHUNK/PAGE reuse the core hierarchy; only ARBITRARY stores its own
   * column index and row boundaries. There is no two-entry limit.
   * Writers should order coarse to fine; boundaries need not nest across entries.
   * Readers use row intervals, not list positions, to match ranges across entries.
   * Counts from overlapping granularities must not be added together.
   * Omit sets with no available statistics; omit the list when none exist.
   */
  8: optional list<ColumnRangeStatistics> row_range_statistics;

  /** list<binary>; number of values = M.
   * Nullability: list and values are non-null. Physical type: BYTE_ARRAY.
   */
  9: optional StreamLocation key_value_metadata_keys;

  /** list<binary>; number of values = M, aligned with key_value_metadata_keys.
   * Nullability: list is non-null; values are nullable. Physical type: BYTE_ARRAY.
   */
  10: optional StreamLocation key_value_metadata_values;

  /** Plaintext algorithm/AAD parameters and opaque footer-key retrieval metadata.
   * Absent: file_info and shared streams are plaintext; no footer signature.
   * Present with file_info: shared streams each have a 28-byte signature.
   * Present with encrypted_file_info: shared streams each use footer-key AES-GCM.
   * Both protected modes append a 28-byte signature after the serialized root,
   * authenticating the directory, crypto metadata and inline alternative.
   * Missing key_metadata means external key resolution; aad_file_unique is required.
   * Metadata uses AES-GCM under both PME algorithms; supply any requested external AAD prefix.
   */
  11: optional parquet.FileCryptoMetaData crypto_metadata;

  /** Serialized FileInfo encrypted with the footer key; requires crypto_metadata.
   * PME module: 4-byte LE length (excluding itself), nonce12, ciphertext, GCM-tag16.
   * Thrift's binary field delimits the value; no root offset or length is needed.
   */
  12: optional binary encrypted_file_info;

  /** Independently encrypted statistics; disjoint from shared row_range_statistics. */
  13: optional EncryptedRowRangeStatistics encrypted_row_range_statistics;
}
