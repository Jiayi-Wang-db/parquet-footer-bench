/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Modular footer: Parquet file metadata stored as independently readable
 * metadata streams. A small root struct, ModularFooter (defined at the end of
 * this file), sits at the end of the file and records where each stream is.
 *
 * The modular footer uses these counts. Field docs refer to them by name,
 * e.g. "INT64, num_chunks values" means one value per chunk.
 *   num_columns         = |physical (leaf) columns| = FileInfo.num_columns
 *   chunks_in_column(c) = |chunks of column c|, where a chunk is one
 *                         contiguous run of data pages; may be zero
 *   num_chunks          = |chunks| = sum of chunks_in_column(c)
 *   pages_in_chunk(k)   = |data pages of chunk k|
 *   num_pages           = |data pages| = sum of pages_in_chunk(k)
 *   num_dictionaries    = |dictionary pages|
 *   num_row_groups      = |row groups|
 *   num_ranges          = |row ranges in one RangeStatistics|
 *
 * Reader requirements: before using a stream, check its value count, the
 * indexes it contains, and the byte ranges it points to. When the file is
 * signed or encrypted, authenticate or decrypt every stream a read depends on
 * before using it. A missing or unreadable index stream never means that the
 * columns or chunks it describes are empty.
 */
include "parquet.thrift"
namespace cpp parquet.modular
namespace java org.apache.parquet.format.modular

// Shared metadata structures.

/**
 * Location of a metadata stream. A stream is a sequence of Parquet pages that
 * encodes one flat column of footer metadata, for example the byte offset of
 * every column chunk. Its layout matches a non-nested column chunk:
 *   [optional DICTIONARY_PAGE][DATA_PAGE_V2 ...]
 * where each page is a PageHeader followed by its body. Data pages must be
 * DATA_PAGE_V2; DATA_PAGE (V1) and other page types are not allowed.
 * Allowing more page types requires a new ModularFooter.version.
 *
 * The stream schema is not serialized. The documentation of the field that
 * holds a StreamLocation gives the stream's physical type, its number of
 * values, and whether values can be null. Repetition levels are always 0.
 * Non-nullable streams have no definition levels (max level 0); nullable
 * streams use definition level 0 for null and 1 for a present value. A null
 * value still occupies its position in the stream. Thrift `optional` on a
 * field controls whether the stream exists, not whether its values can be null.
 *
 * Streams are independent of each other: they need not have the same number
 * of values, their page boundaries need not line up, and together they do not
 * form a Parquet row group.
 *
 * When the footer is signed or encrypted, each shared stream is protected the
 * same way as the footer, including its page headers and dictionary page; see
 * the PME section. Streams inside ColumnRowRangeStatistics are the exception:
 * they are protected as part of their column's encrypted module.
 */
struct StreamLocation {
  /** Byte offset of the stream's first page header, from the start of the
   * file. Inside ColumnRowRangeStatistics, offsets are relative to the start of
   * page_streams instead.
   */
  1: required i64 offset;

  /** Number of bytes the stream occupies in the file, including page headers
   * and any signature or encryption overhead described in the PME section.
   */
  2: required i64 length;

  /** Codec for dictionary page bodies, and for data page values when
   * DataPageHeaderV2.is_compressed is true. Levels are never compressed.
   * Compression happens before signing or encryption. The codec is chosen
   * independently of the codecs used for the file's data.
   */
  3: required parquet.CompressionCodec codec;
}

/**
 * Maps columns to their chunks, and chunks to their pages.
 *
 * Both fields are INT32 streams of range ends. Entry i is the exclusive end of
 * item i's range, and the range starts at entry i-1 (or at 0 for i = 0).
 * Entries count chunks or pages; they are not byte offsets. Because the first
 * range always starts at 0, that 0 is not stored. Entries equal to 0 or to the
 * previous entry are stored, and mean an empty range.
 *
 * Example: three columns with 2, 0 and 3 chunks give
 *   column_chunk_index = [2, 2, 5]
 * Column 0 owns chunks [0, 2), column 1 owns none ([2, 2)), and column 2 owns
 * chunks [2, 5).
 *
 * Elsewhere in this file, start(index, i) means 0 if i = 0, otherwise
 * index[i-1], for 0 <= i <= the number of entries in index.
 */
struct DataLayout {
  /** INT32, num_columns non-null values. Column c owns chunks
   * [start(column_chunk_index, c), column_chunk_index[c]). Values never
   * decrease, and the last one is num_chunks (the stream is empty when the
   * file has no columns).
   */
  1: required StreamLocation column_chunk_index;

  /** INT32, num_chunks non-null values. Chunk k owns pages
   * [start(page_index, k), page_index[k]). Values never decrease, and the last
   * one is num_pages (the stream is empty when there are no chunks).
   * Required when page_metadata is present, otherwise optional. If this stream
   * is absent, readers find pages from the page headers in the column data;
   * absence does not mean that the chunks have no pages.
   */
  2: optional StreamLocation page_index;
}

/** Locations of the file's dictionary pages. A dictionary ID (see
 * ColumnChunkMetadata.dictionary_ids) is a position in these streams.
 */
struct DictionaryMetadata {
  /** INT64, num_dictionaries non-null values; num_dictionaries must fit in a
   * signed i32.
   * Byte offset of each dictionary page header from the start of the file.
   * A dictionary used by several chunks is stored once. Dictionaries may be
   * stored anywhere and in any order; ordering them by first use is
   * recommended. Both streams are empty when the file has no dictionaries.
   */
  1: required StreamLocation offsets;

  /** INT64, num_dictionaries non-null values. Size in bytes of each dictionary
   * page as stored, including its header; always positive. Dictionary d
   * occupies [offsets[d], offsets[d] + lengths[d]), which must not overlap data
   * pages or other dictionaries.
   */
  2: required StreamLocation lengths;
}

struct RowGroupMetadata {
  /** INT64, num_row_groups non-null values. Number of rows in each row
   * group, in row-group order. The values sum to FileInfo.num_rows.
   */
  1: required StreamLocation num_rows;

  /** INT64, num_row_groups + 1 non-null values. Optional hint for files whose
   * row groups are stored contiguously in file order: row group g occupies
   * bytes [offsets[g], offsets[g+1]). Values never decrease. Omit this stream
   * when interleaved chunks or shared dictionaries mean row groups do not have
   * a contiguous byte range. Readers still locate chunks and dictionaries
   * through ColumnChunkMetadata and DictionaryMetadata.
   */
  2: optional StreamLocation offsets;
}

/**
 * Per-chunk metadata. A column chunk is one contiguous run of data pages for
 * a single column. Chunks are not tied to row groups: each column can have its
 * own number of chunks, including zero.
 *
 * Every stream has num_chunks values, one per chunk, ordered by column and
 * then by chunk within the column; DataLayout.column_chunk_index says which
 * positions belong to which column.
 */
struct ColumnChunkMetadata {
  /** INT32, num_chunks non-null values. The parquet.CompressionCodec of
   * each chunk.
   */
  1: required StreamLocation codecs;

  /** INT32, num_chunks nullable values. Value k is chunk k's dictionary ID,
   * in [0, num_dictionaries);
   * null means chunk k has no dictionary. Chunks of the same physical column
   * may share a dictionary; chunks of different columns may not. A dictionary
   * must be compatible with every chunk that uses it; in particular, those
   * chunks must use the same codec, because the dictionary page is compressed
   * with it. Every dictionary must be used by at least one chunk.
   */
  2: required StreamLocation dictionary_ids;

  /** BOOLEAN, num_chunks non-null values.
   * True if all data pages use the chunk's dictionary; false when the chunk
   * doesn't have a dictionary.
   */
  3: required StreamLocation is_fully_dictionary_encoded;

  /** INT64, num_chunks non-null values. Encoded but uncompressed size of each
   * chunk: page headers plus uncompressed page bodies. A shared dictionary is
   * counted once for each chunk that uses it. If this stream is absent, the
   * sizes are unknown.
   */
  4: optional StreamLocation total_uncompressed_sizes;

  /** INT64, num_chunks non-null values. Number of values in each chunk,
   * including nulls. This equals the number of definition/repetition levels
   * written for the chunk. It is not a row count, except for a non-nested,
   * required column, where the two are equal.
   */
  5: required StreamLocation num_values;

  /** INT64, num_chunks non-null values. Index of each chunk's first row within
   * the file. A chunk's position in its column does not say which rows it
   * covers; this stream does. Required if any statistics use COLUMN_CHUNK
   * ranges, including encrypted statistics. Independent of row groups and of
   * num_values. When PageMetadata.first_row_indexes is present, the value
   * must equal the first row of the chunk's first page.
   */
  6: optional StreamLocation first_row_indexes;

  /** INT64, num_chunks non-null values. Byte offset, from the start of the
   * file, of each chunk's first data page header. A chunk's data pages are
   * contiguous. The dictionary page is not part of this range; it is located
   * through DictionaryMetadata. Chunks need not be adjacent or stored in offset
   * order.
   */
  7: required StreamLocation offsets;

  /** INT64, num_chunks non-null values. Size in bytes of each chunk's data
   * pages as stored in the file: compressed, including page headers. The
   * dictionary page is not included. Zero if and only if the chunk has no data
   * pages. Non-empty chunks occupy
   * [offsets[k], offsets[k] + compressed_sizes[k]), and these ranges must not
   * overlap. Readers must check
   * each range against the file size and for integer overflow, and must not
   * infer a chunk's size from the position of another chunk or dictionary.
   */
  8: required StreamLocation compressed_sizes;
}

/**
 * Per-page metadata. Every stream has num_pages values, one per data page,
 * ordered by chunk and then by page within the chunk; DataLayout.page_index
 * says which positions belong to which chunk. The two streams are independently
 * optional. Omit this struct when both are absent.
 */
struct PageMetadata {
  /** INT64, num_pages non-null values. Index of each page's first row within
   * the file. Required if any statistics use PAGE ranges, including encrypted
   * statistics. If absent, readers get row positions from the data page
   * headers.
   */
  1: optional StreamLocation first_row_indexes;

  /** INT64, num_pages non-null values. Byte offset of each page header relative
   * to its chunk's ColumnChunkMetadata.offsets[k]. In each non-empty chunk the
   * first value is 0 and is stored, even for single-page chunks. Values
   * strictly increase and stay below compressed_sizes[k]; the last page ends at
   * the end of the chunk. If absent, readers scan page headers to find pages
   * after the first.
   */
  2: optional StreamLocation relative_offsets;
}

/** How a statistics set divides each column's rows into ranges.
 * COLUMN_CHUNK: one range per column chunk.
 * PAGE: one range per data page.
 * ARBITRARY: ranges chosen by the writer and listed explicitly through
 *   RangeStatistics.first_row_indexes, for example fixed 100,000-row ranges.
 *   They need not line up with chunks, pages or row groups.
 */
enum RowRangeKind {
  COLUMN_CHUNK = 0,
  PAGE = 1,
  ARBITRARY = 2
}

/**
 * Statistics for num_ranges row ranges. The struct that contains a
 * RangeStatistics defines which ranges belong to which column.
 *
 * A null value, or an absent stream, means the statistic is unknown for that
 * range. This includes the case where every value in the range is null.
 * Counts are never negative. When all three are known,
 * null_count + nan_count <= num_values.
 */
struct RangeStatistics {
  /** INT64, num_ranges non-null values. Index of each range's first row within
   * the file. Present if and only if range_kind is ARBITRARY. For each column,
   * the first value is 0 and values strictly increase while staying below
   * FileInfo.num_rows. A range ends where the next one starts, or at
   * FileInfo.num_rows for the last range. Ranges need not line up with chunks,
   * pages or row groups.
   */
  1: optional StreamLocation first_row_indexes;

  /** INT64, num_ranges nullable values. Number of null values in each range. */
  2: optional StreamLocation null_count;

  /** INT64, num_ranges nullable values. Number of NaN values in each range.
   * NaNs are counted as non-null values.
   */
  3: optional StreamLocation nan_count;

  /** INT64, num_ranges nullable values. Number of values in each range,
   * including nulls and NaNs. This is not a row count.
   */
  4: optional StreamLocation num_values;

  /** INT64, num_ranges nullable values. Number of distinct values in each
   * range.
   */
  5: optional StreamLocation distinct_counts;

  /** Min/max bounds, one stream per physical type. Each stream stores two
   * values per range: [min_0, max_0, min_1, max_1, ...]. Each value is
   * independently nullable. A bound is null when it is unavailable or when the
   * column does not have the stream's physical type. A min/max pair may span
   * two metadata pages.
   */
  6: optional StreamLocation min_max_boolean; // BOOLEAN, 2 * num_ranges values.
  7: optional StreamLocation min_max_int32; // INT32, 2 * num_ranges values.
  8: optional StreamLocation min_max_int64; // INT64, 2 * num_ranges values.
  9: optional StreamLocation min_max_float; // FLOAT, 2 * num_ranges values.
  10: optional StreamLocation min_max_double; // DOUBLE, 2 * num_ranges values.
  // BYTE_ARRAY, 2 * num_ranges values.
  11: optional StreamLocation min_max_byte_array;

  /** BOOLEAN, 2 * num_ranges nullable values, aligned with the min/max values
   * above. True means the bound is exact (it is the actual minimum or maximum
   * of the range), false means it is not exact, and null means unknown. If a
   * bound is unavailable, its entry here must be null.
   */
  12: optional StreamLocation min_max_is_exact;

  /** How the ranges are defined. Writers always set this field; when it is
   * absent, readers treat it as ARBITRARY. Readers do not support values they
   * do not recognize.
   *
   * COLUMN_CHUNK: one range per chunk. Requires
   *   ColumnChunkMetadata.first_row_indexes.
   * PAGE: one range per page. Requires PageMetadata.first_row_indexes and both
   *   DataLayout indexes. Every chunk covered by these statistics must have at
   *   least one page.
   * ARBITRARY: ranges listed in first_row_indexes. Used when ranges based on
   *   chunk or page positions do not fit.
   *
   * For each column covered by these statistics, the ranges must be non-empty,
   * start and end on row boundaries, and together cover [0, FileInfo.num_rows)
   * without gaps or overlaps: the first range starts at 0, starts strictly
   * increase, and each range ends where the next one starts (the last ends at
   * num_rows). Readers must check this; increasing starts alone do not prove
   * there are no gaps. The same rules apply to encrypted statistics. Before
   * using the chunk/page index streams and first_row_indexes streams these
   * statistics depend on, readers must authenticate or decrypt them according
   * to their own protection.
   */
  13: optional RowRangeKind range_kind = RowRangeKind.ARBITRARY;
}

/**
 * One set of row-range statistics covering all columns. Its streams are
 * protected the same way as the footer.
 *
 * With COLUMN_CHUNK ranges (num_ranges = num_chunks) or PAGE ranges
 * (num_ranges = num_pages), every chunk or page keeps its position. Statistics
 * that are unavailable, or that are stored in a column's separately encrypted
 * module, are written as null (unknown).
 * With ARBITRARY ranges, a column may have zero ranges here, shown by two equal
 * consecutive column_range_starts entries.
 */
struct SharedRangeStatistics {
  /** INT64, num_columns + 1 non-null values. Present if and only if
   * statistics.range_kind is ARBITRARY. Column c owns ranges
   * [column_range_starts[c], column_range_starts[c+1]). Values never decrease,
   * the first is 0 and the last is num_ranges. Equal consecutive values mean
   * the column has no ranges.
   */
  1: optional StreamLocation column_range_starts;

  /** The statistics. Column c owns these positions, where
   * a = start(column_chunk_index, c) and b = column_chunk_index[c]
   * (start() is defined in DataLayout):
   *   COLUMN_CHUNK: [a, b).
   *   PAGE: [start(page_index, a), start(page_index, b)).
   *   ARBITRARY: the positions given by column_range_starts.
   */
  2: required RangeStatistics statistics;

  /** INT32, num_columns non-null values. The parquet.BoundaryOrder of each
   * column's min/max values. Columns without statistics in this set must be
   * UNORDERED. If the stream is absent, every column is UNORDERED.
   */
  3: optional StreamLocation boundary_orders;
}

/** Basic file metadata: column count, row count and schema. */
struct FileInfo {
  /** Number of physical (leaf) columns. */
  1: required i32 num_columns;
  2: required i64 num_rows;
  /** Schema of the file's data, as in FileMetaData.schema. */
  3: required list<parquet.SchemaElement> schema;
  4: optional string created_by;
}

// Parquet Modular Encryption (PME) structures.

/**
 * Signing and encryption follow Parquet Modular Encryption (PME). Metadata is
 * always protected with AES-GCM, under either PME algorithm. Every encryption
 * or signature uses a fresh nonce for its key.
 *
 * Signing a module means encrypting it with AES-GCM and keeping only the
 * 12-byte nonce and 16-byte GCM tag (28 bytes); the ciphertext is discarded.
 * A signed stream is followed by its 28-byte signature. An encrypted module or
 * stream is stored as a 4-byte little-endian length (not counting itself),
 * a 12-byte nonce, the ciphertext, and a 16-byte GCM tag. These bytes are
 * included in StreamLocation.length.
 *
 * Readers verify the root struct's signature, which covers its exact
 * serialized bytes, before trusting the root or decrypting FileInfo. Readers
 * verify each stream before parsing its pages. Readers apply the caller's
 * protection policy: a file whose crypto metadata was removed must not be read
 * with weaker protection than the policy requires.
 *
 * The AAD of each module is the PME file AAD (prefix + aad_file_unique)
 * followed by:
 *   root struct: NEW_FOOTER_DIRECTORY_TYPE + LE32(version).
 *   FileInfo: NEW_FILE_INFO_TYPE + LE32(version).
 *   stream: NEW_METADATA_STREAM_TYPE + LE32(version) + LE32(path length)
 *           + LE32(each path component) + LE64(offset) + LE64(length) + LE32(codec).
 *   column statistics: NEW_ROW_RANGE_STATISTICS_TYPE + LE32(version)
 *           + LE32(column ordinal).
 * A stream's path lists the Thrift field IDs and zero-based list positions
 * from the root struct down to the StreamLocation. For example, [8, g, 2, 7]
 * identifies min_max_int32 in statistics set g of row_range_statistics. Path
 * components follow the schema. The module type values are not yet allocated;
 * they must not reuse existing PME module type IDs.
 */

/**
 * Plaintext contents of one encrypted column-statistics module, serialized
 * with the Thrift compact protocol. All ranges belong to the module's column
 * (EncryptedColumnRowRangeStatistics.column_ordinal).
 *
 * COLUMN_CHUNK statistics have one range per chunk of this column
 * (chunks_in_column(c)), and PAGE statistics one range per page of this column.
 * Both reuse the shared, authenticated DataLayout and first_row_indexes
 * streams. ARBITRARY statistics store their own first_row_indexes for this
 * column, and no column_range_starts. Only the min/max stream for the column's
 * physical type may be present (2 * num_ranges values).
 */
struct ColumnRowRangeStatistics {
  /** One or more independent statistics sets, with RangeStatistics semantics.
   * Positions are local to this column. All entries are encrypted together.
   */
  1: required list<RangeStatistics> statistics;

  /** The pages of every stream referenced by `statistics`, concatenated,
   * encoded and compressed before the module is encrypted. Each stream uses
   * the page format described on StreamLocation, and StreamLocation offsets in
   * this struct are relative to the start of this field. Streams here are not
   * signed or encrypted individually; the whole module is encrypted once.
   *
   * Example: if statistics[0] has null_count and min_max_int64, this field is
   *   [null_count pages][min_max_int64 pages]
   * with null_count.offset = 0 and min_max_int64.offset = null_count.length.
   */
  2: required binary page_streams;

  /** One BoundaryOrder per statistics entry. Missing entries, or an absent
   * list, mean UNORDERED.
   */
  3: optional list<parquet.BoundaryOrder> boundary_orders;
}

/** Location and key of one column's encrypted statistics module. */
struct EncryptedColumnRowRangeStatistics {
  /** Position of the column among the physical (leaf) columns in
   * FileInfo.schema; 0 <= column_ordinal < num_columns.
   */
  1: required i32 column_ordinal;

  /** Which key encrypts the module; it never contains the key itself. Exactly
   * one option is set and applies to the whole module.
   * ENCRYPTION_WITH_COLUMN_KEY: path_in_schema must match column_ordinal.
   *   The reader resolves the column's data encryption key from key_metadata.
   * ENCRYPTION_WITH_FOOTER_KEY: the module uses the footer's data encryption
   *   key from ModularFooter.crypto_metadata.
   */
  2: required parquet.ColumnCryptoMetaData crypto_metadata;

  /** Byte offset of this module (its 4-byte length prefix) from the start of
   * the file. Modules are stored before the ModularFooter root struct.
   */
  3: required i64 offset;

  /** Size of the module in bytes: the 4-byte length prefix (whose value does
   * not count itself), 12-byte nonce, ciphertext and 16-byte GCM tag. The
   * plaintext is a ColumnRowRangeStatistics.
   */
  4: required i64 length;
}

/**
 * Statistics stored in separately encrypted per-column modules. Omit this
 * struct when there are none.
 *
 * Columns listed here keep their positions in the shared statistics: with
 * ARBITRARY ranges they have zero shared ranges, and with COLUMN_CHUNK or PAGE
 * ranges their shared values are null (unknown).
 *
 * Statistics encrypted with the footer key can instead stay in the shared
 * streams, encrypted like the rest of the footer. Statistics that use a column
 * key need their own module. In files whose streams are signed but not
 * encrypted, statistics that need encryption also need their own module.
 *
 * A reader without the key treats the column's statistics as unavailable. A
 * failed authentication or malformed plaintext is an error.
 */
struct EncryptedRowRangeStatistics {
  /** Sorted by column_ordinal, with no duplicate columns and no overlapping
   * byte ranges.
   */
  1: required list<EncryptedColumnRowRangeStatistics> columns;
}

// Footer directory.

/**
 * Root of the modular footer. It records where every metadata stream is,
 * including the column-key descriptors for encrypted statistics. Readers find
 * it from the end of the file:
 *
 *   +----------------------------------------------+
 *   | data and dictionary pages                    |
 *   +----------------------------------------------+
 *   | metadata streams and encrypted column-       |
 *   | statistics modules (must not overlap)        |
 *   +----------------------------------------------+ <- file_size - 8 - directory_length
 *   | ModularFooter (Thrift compact protocol)      |
 *   | 28-byte signature (only if crypto_metadata)  |
 *   +----------------------------------------------+
 *   | directory_length: u32, little-endian         |  covers ModularFooter + signature
 *   | magic: "PAR3"                                |
 *   +----------------------------------------------+ <- file_size
 *
 * directory_length counts the ModularFooter struct and its optional signature,
 * not the final 8 bytes. Readers must check directory_length against the file
 * size before computing the start position. See the PME section for
 * signature and AAD rules.
 */
struct ModularFooter {
  1: required i32 version; // Allocation pending.

  /** Exactly one of file_info and encrypted_file_info is present. */
  2: optional FileInfo file_info;
  3: required DataLayout data_layout;
  4: required DictionaryMetadata dictionary_metadata;
  5: optional RowGroupMetadata row_group_metadata;
  6: required ColumnChunkMetadata column_chunk_metadata;
  7: optional PageMetadata page_metadata;

  /** Any number of independent statistics sets, preferably ordered from
   * coarse to fine ranges. Range boundaries in different sets need not nest;
   * readers match ranges across sets by row interval, not by list position,
   * and must not add up counts from overlapping ranges. Omit sets that have
   * no available statistics, and omit the list when no sets remain.
   */
  8: optional list<SharedRangeStatistics> row_range_statistics;

  /** BYTE_ARRAY, one non-null value per key/value metadata entry. */
  9: optional StreamLocation key_value_metadata_keys;

  /** BYTE_ARRAY, nullable, one value per entry in key_value_metadata_keys. */
  10: optional StreamLocation key_value_metadata_values;

  /** PME algorithm, AAD parameters and footer key metadata (opaque to the
   * format). Absent in plaintext files: FileInfo and all streams are plaintext,
   * and there is no signature.
   * With file_info: each stream is individually signed.
   * With encrypted_file_info: each stream is individually encrypted with the
   * footer key.
   * In both cases the root struct is signed. aad_file_unique is required; the
   * reader must supply the AAD prefix if the file requires one. If key_metadata
   * is absent, the reader must obtain the footer key some other way.
   */
  11: optional parquet.FileCryptoMetaData crypto_metadata;

  /** FileInfo, serialized and encrypted with the footer key, using the PME
   * encrypted module format (see the PME section). Requires crypto_metadata.
   */
  12: optional binary encrypted_file_info;

  /** Statistics in separately encrypted column modules. A column's statistics
   * are stored either here or in row_range_statistics, not both.
   */
  13: optional EncryptedRowRangeStatistics encrypted_row_range_statistics;
}
