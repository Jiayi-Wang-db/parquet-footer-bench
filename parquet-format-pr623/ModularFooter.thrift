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
 * The modular footer uses these counts:
 *   num_columns         = |physical (leaf) columns| = FileInfo.num_columns
 *   chunks_in_column(c) = |chunks of column c|, where a chunk is one
 *                         contiguous run of data pages; may be zero if
 *                         every value in the column is null
 *   num_chunks          = |chunks| = sum of chunks_in_column(c)
 *   pages_in_chunk(k)   = |data pages of chunk k|
 *   num_pages           = |data pages| = sum of pages_in_chunk(k)
 *   num_dictionaries    = |dictionary pages|
 *   num_row_groups      = |row groups|
 *   num_ranges          = |row ranges in one RangeStatistics|
 *
 * The doc of each stream field starts with the stream's type and how many
 * values it has, e.g. "INT64, one value per chunk". Values are never null
 * unless the doc says "nullable".
 *
 * Reader requirements: before using a stream, check its value count, the
 * positions it refers to, and the byte ranges it points to. When the file is
 * signed or encrypted, authenticate or decrypt every stream a read depends on
 * before using it.
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
 * The stream's schema is not written to the file. Instead, the doc of the
 * field that holds the StreamLocation says the stream's type, how many values
 * it has, and whether values can be null.
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
   * file. Streams in a column's encrypted statistics module are the
   * exception: they are stored inside the module, so their offset is counted
   * from the start of the module's decrypted stream bytes
   * (ColumnRowRangeStatistics.page_streams).
   */
  1: required i64 offset;

  /** Number of bytes the stream takes up in the file, including page headers
   * and any signature or encryption bytes described in the PME section.
   */
  2: required i64 length;

  /** Compression codec of the stream's pages, used as in any Parquet column
   * chunk. Metadata stream compression is independent of column data
   * compression. Pages are compressed before they are signed or encrypted.
   */
  3: required parquet.CompressionCodec codec;
}

/**
 * Maps columns to their chunks, and chunks to their pages.
 *
 * Chunks are numbered in column order: column 0's chunks first, then column
 * 1's, and so on. Pages are numbered the same way, in chunk order.
 *
 * Both fields store running totals. column_chunk_index[c] is the number of
 * chunks in columns 0 through c, and page_index[k] is the number of pages in
 * chunks 0 through k. So item i owns the range [start(index, i), index[i]),
 * where start(index, i) is 0 for i = 0 and index[i-1] otherwise. With n
 * entries, start(index, n) is the last entry: the total number of chunks or
 * pages. An item with no chunks or pages repeats the previous total.
 *
 * Example: three columns with 2, 0 and 3 chunks give
 *   column_chunk_index = [2, 2, 5]
 * Column 0 owns chunks [0, 2), column 1 owns none ([2, 2)), and column 2 owns
 * chunks [2, 5).
 *
 * Every column has an entry, even a column with no chunks, like column 1
 * above. column_chunk_index must have exactly num_columns values, and
 * page_index, when present, must have exactly num_chunks values.
 */
struct DataLayout {
  /** INT32, one value per column: the inclusive prefix sum of
   * chunks_in_column(c).
   */
  1: required StreamLocation column_chunk_index;

  /** INT32, one value per chunk: the inclusive prefix sum of
   * pages_in_chunk(k).
   *
   * Required when page_metadata is present. Without it, readers find pages by
   * reading the page headers in the column data; a missing page_index does not
   * mean the chunks have no pages.
   */
  2: optional StreamLocation page_index;
}

/**
 * Locations of the file's dictionary pages. A dictionary ID
 * (ColumnChunkMetadata.dictionary_ids) is a position in these streams. Each
 * dictionary is stored once, even when chunks share it. Dictionary pages can
 * be stored anywhere in the file, not necessarily next to the chunks that use
 * them.
 */
struct DictionaryMetadata {
  /** INT64, one value per dictionary: the byte offset of the dictionary page
   * header from the start of the file.
   */
  1: required StreamLocation offsets;

  /** INT64, one value per dictionary: the size in bytes of the dictionary
   * page, including its header. Dictionary pages must not overlap each other
   * or any data pages.
   */
  2: required StreamLocation lengths;

  /** INT32, one value per dictionary: the codec of the dictionary page, as a
   * parquet.CompressionCodec value. This is the only source of a dictionary
   * page's codec; the codecs of the chunks that use it do not apply.
   */
  3: required StreamLocation codecs;
}

/**
 * Optional row-group boundaries, kept for compatibility. If present, the data
 * is laid out in row groups, each stored as one contiguous byte range in file
 * order. If absent, the file does not record row groups, though its data may
 * still be laid out in them.
 */
struct RowGroupMetadata {
  /** INT64, one value per row group: the number of rows in the row group. The
   * values sum to FileInfo.num_rows.
   */
  1: required StreamLocation num_rows;

  /** INT64, one value per row group plus one: the byte boundaries of the row
   * groups. Row group g occupies bytes [offsets[g], offsets[g+1]).
   */
  2: required StreamLocation offsets;
}

/**
 * Per-chunk metadata. Each stream has one value per chunk, in the chunk order
 * defined by DataLayout.column_chunk_index. Chunk boundaries are not tied to
 * row groups.
 */
struct ColumnChunkMetadata {
  /** INT32, one value per chunk: the codec of the chunk's data pages, as a
   * parquet.CompressionCodec value. Dictionary pages use
   * DictionaryMetadata.codecs instead.
   */
  1: required StreamLocation codecs;

  /** INT32, one nullable value per chunk: the chunk's dictionary ID, which is
   * its position in DictionaryMetadata, or null if the chunk has no dictionary.
   *
   * Several chunks can share one dictionary, with these rules:
   * - The chunks must belong to the same column.
   * - Every dictionary must be used by at least one chunk.
   */
  2: required StreamLocation dictionary_ids;

  /** BOOLEAN, one value per chunk: true if every data page in the chunk is
   * encoded with the chunk's dictionary. Always false for a chunk without a
   * dictionary.
   */
  3: required StreamLocation is_fully_dictionary_encoded;

  /** INT64, one value per chunk: the size of the chunk's data pages after
   * encoding but before compression, i.e. page headers plus uncompressed page
   * bodies. The dictionary page is not included. If this stream is absent, the
   * sizes are unknown.
   */
  4: optional StreamLocation total_uncompressed_sizes;

  /** INT64, one value per chunk: the number of values in the chunk, as in
   * ColumnMetaData.num_values. This includes nulls and equals the number of
   * definition/repetition levels written for the chunk. It is not a row count:
   * a nested column can have more values than rows.
   */
  5: required StreamLocation num_values;

  /** INT64, one value per chunk: the row number, within the file, of the
   * chunk's first row.
   *
   * Required if any statistics, including encrypted statistics, use
   * COLUMN_CHUNK ranges. When PageMetadata.first_row_indexes is present, each
   * chunk's value must equal the first row of its first page.
   */
  6: optional StreamLocation first_row_indexes;

  /** INT64, one value per chunk: the byte offset of the chunk's first data
   * page header from the start of the file. A chunk's data pages are stored
   * contiguously.
   */
  7: required StreamLocation offsets;

  /** INT64, one value per chunk: the size in bytes of the chunk's data pages as
   * stored, i.e. compressed and including page headers. The dictionary page is
   * not included. Chunks must not overlap.
   */
  8: required StreamLocation compressed_sizes;
}

/**
 * Per-page metadata. Each stream has one value per data page, in the page
 * order defined by DataLayout.page_index.
 */
struct PageMetadata {
  /** INT64, one value per page: the row number, within the file, of the page's
   * first row. Required if any statistics, including encrypted statistics, use
   * PAGE ranges. If absent, readers get row positions from the data page
   * headers.
   */
  1: optional StreamLocation first_row_indexes;

  /** INT64, one value per page: the byte offset of the page header from the
   * start of its chunk (ColumnChunkMetadata.offsets), so each chunk's first
   * page is at 0. If absent, readers find pages by reading page headers.
   */
  2: optional StreamLocation relative_offsets;
}

/** How a statistics set divides each column's rows into ranges; see
 * RangeStatistics.range_kind.
 */
enum RowRangeKind {
  COLUMN_CHUNK = 0,
  PAGE = 1,
  ARBITRARY = 2
}

/**
 * Statistics for a list of row ranges: each stream has one value per range
 * (num_ranges in total). The struct that contains a RangeStatistics says which
 * ranges belong to which column.
 *
 * A null value or a missing stream means the statistic is unknown for that
 * range. In particular, null min/max values do not mean that every value in
 * the range is null; use null_count and num_values for that. Counts are never
 * negative. When null_count, nan_count and num_values are all known,
 * null_count + nan_count <= num_values.
 */
struct RangeStatistics {
  /** How the ranges are defined:
   * COLUMN_CHUNK: statistics over each column chunk, one entry per chunk.
   *   ColumnChunkMetadata.first_row_indexes must be present.
   * PAGE: statistics over each data page, one entry per page.
   *   PageMetadata.first_row_indexes and both DataLayout streams must be
   *   present, and every chunk covered must have at least one page.
   * ARBITRARY: statistics over row ranges chosen by the writer and listed in
   *   first_row_indexes, for example every 100,000 rows. The ranges need not
   *   line up with chunks, pages or row groups.
   *
   * Writers always set this field; if it is absent, readers treat it as
   * ARBITRARY. Readers cannot use statistics whose range_kind they do not
   * recognize.
   *
   * For each column covered by these statistics, the ranges must cover every
   * row from 0 to FileInfo.num_rows exactly once, with no empty ranges. Ranges
   * start and end on row boundaries, which matters for nested columns. In other
   * words, the first range starts at row 0, starts strictly increase, and each
   * range ends where the next one starts (the last at num_rows). Readers must
   * check these rules; increasing starts alone are not enough to prove there
   * are no gaps.
   *
   * The same rules apply to encrypted statistics. Before using the DataLayout
   * and first_row_indexes streams that these statistics depend on, readers
   * must authenticate or decrypt those streams under their own protection.
   */
  13: optional RowRangeKind range_kind = RowRangeKind.ARBITRARY;

  /** INT64, one value per range: the row number, within the file, of the
   * range's first row. Present if and only if range_kind is ARBITRARY.
   *
   * For each column, the first range starts at row 0, and starts strictly
   * increase and stay below FileInfo.num_rows. Each range ends where the next
   * one starts; the last one ends at FileInfo.num_rows. Ranges do not have to
   * line up with chunks, pages or row groups.
   */
  1: optional StreamLocation first_row_indexes;

  /** INT64, one nullable value per range: the number of null values in the
   * range.
   */
  2: optional StreamLocation null_count;

  /** INT64, one nullable value per range: the number of NaN values in the
   * range. NaNs are non-null values, so they are not part of null_count.
   */
  3: optional StreamLocation nan_count;

  /** INT64, one nullable value per range: the number of values in the range,
   * including nulls and NaNs. For a nested column this can be more than the
   * number of rows.
   */
  4: optional StreamLocation num_values;

  /** INT64, one nullable value per range: the number of distinct values in the
   * range.
   */
  5: optional StreamLocation distinct_counts;

  /** Minimum and maximum values, in one stream per physical type. A column's
   * bounds are stored in the stream for its physical type. Each stream has two
   * nullable values per range, min then max: [min_0, max_0, min_1, max_1, ...].
   * A value is null when that bound is unavailable, or when the column has a
   * different physical type. A min/max pair may be split across two pages of
   * the stream.
   */
  6: optional StreamLocation min_max_boolean; // BOOLEAN
  7: optional StreamLocation min_max_int32; // INT32
  8: optional StreamLocation min_max_int64; // INT64
  9: optional StreamLocation min_max_float; // FLOAT
  10: optional StreamLocation min_max_double; // DOUBLE
  11: optional StreamLocation min_max_byte_array; // BYTE_ARRAY

  /** BOOLEAN, two nullable values per range, matching the min/max values
   * above. True means the bound is exact (it is the actual minimum or maximum
   * of the range), false means it is not, and null means unknown. The value
   * must be null wherever the bound itself is unavailable.
   */
  12: optional StreamLocation min_max_is_exact;
}

/**
 * One set of row-range statistics for all columns. Its streams are protected
 * the same way as the rest of the footer.
 *
 * With COLUMN_CHUNK or PAGE ranges there is one range per chunk or per page,
 * so every chunk or page has a slot, and num_ranges equals num_chunks or
 * num_pages. A slot is null (unknown) when the statistic is unavailable or is
 * stored in the column's separately encrypted module. With ARBITRARY ranges,
 * each column has its own ranges, and a column may have none.
 */
struct SharedRangeStatistics {
  /** INT64, one value per column plus one: where each column's ranges start.
   * Present if and only if statistics.range_kind is ARBITRARY. Column c owns
   * ranges [column_range_starts[c], column_range_starts[c+1]). Values never
   * decrease; the first is 0 and the last is num_ranges. Two equal consecutive
   * values mean the column has no ranges.
   */
  1: optional StreamLocation column_range_starts;

  /** The statistics for all columns. Which positions belong to column c
   * depends on range_kind, with a = start(column_chunk_index, c) and
   * b = column_chunk_index[c] (start() is defined in DataLayout):
   *   COLUMN_CHUNK: [a, b).
   *   PAGE: [start(page_index, a), start(page_index, b)).
   *   ARBITRARY: the positions given by column_range_starts.
   */
  2: required RangeStatistics statistics;

  /** INT32, one value per column: whether the column's min/max values are
   * sorted across its ranges, as a parquet.BoundaryOrder. Columns with no
   * statistics in this set must be UNORDERED. If this stream is absent, every
   * column is UNORDERED.
   */
  3: optional StreamLocation boundary_orders;
}

/** Basic file metadata: column count, row count and schema. */
struct FileInfo {
  /** Number of physical (leaf) columns. */
  1: required i32 num_columns;
  /** Number of rows in the file. */
  2: required i64 num_rows;
  /** Schema of the file's data, as in FileMetaData.schema. */
  3: required list<parquet.SchemaElement> schema;
  /** Application that wrote the file, as in FileMetaData.created_by. */
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
 * physical type may be present.
 */
struct ColumnRowRangeStatistics {
  /** One or more independent statistics sets for this column, with the same
   * meaning as RangeStatistics elsewhere. Range positions count only this
   * column's chunks, pages or ranges. All sets are encrypted together.
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

  /** One BoundaryOrder per entry in statistics. If the list is absent or
   * shorter than statistics, the missing entries are UNORDERED.
   */
  3: optional list<parquet.BoundaryOrder> boundary_orders;
}

/** Location and key of one column's encrypted statistics module. */
struct EncryptedColumnRowRangeStatistics {
  /** Which column the module belongs to: its position among the physical
   * (leaf) columns in FileInfo.schema, from 0 to num_columns - 1.
   */
  1: required i32 column_ordinal;

  /** Which key encrypts the module. It identifies the key but never contains
   * it. Exactly one option is set, and it applies to the whole module:
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
  /** One entry per column with encrypted statistics, sorted by column_ordinal.
   * Each column appears at most once, and the modules' byte ranges must not
   * overlap.
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
  /** Format version of the modular footer. The value is not allocated yet. */
  1: required i32 version;

  /** Basic file metadata, in plaintext. Exactly one of file_info and
   * encrypted_file_info is present.
   */
  2: optional FileInfo file_info;
  3: required DataLayout data_layout;
  4: required DictionaryMetadata dictionary_metadata;
  5: optional RowGroupMetadata row_group_metadata;
  6: required ColumnChunkMetadata column_chunk_metadata;
  7: optional PageMetadata page_metadata;

  /** Any number of independent statistics sets, for example one with a range
   * per chunk and one with a range per page. Where possible, list sets with
   * larger ranges first. Ranges in different sets do not have to nest; readers
   * match ranges across sets by row interval, not by position in the list, and
   * must not add up counts from overlapping ranges. Leave out sets that have no
   * statistics, and leave out the list if no sets remain.
   */
  8: optional list<SharedRangeStatistics> row_range_statistics;

  /** BYTE_ARRAY, one value per key/value metadata entry: the key. */
  9: optional StreamLocation key_value_metadata_keys;

  /** BYTE_ARRAY, one nullable value per entry in key_value_metadata_keys: the
   * value for that key.
   */
  10: optional StreamLocation key_value_metadata_values;

  /** Encryption settings: the PME algorithm, AAD parameters and footer key
   * metadata. The key metadata is opaque to the format.
   * - Absent: the file is plaintext. FileInfo and all streams are plaintext,
   *   and nothing is signed.
   * - Present with file_info: FileInfo is plaintext, and each stream is
   *   individually signed.
   * - Present with encrypted_file_info: FileInfo and each stream are
   *   individually encrypted with the footer key.
   * When present, the root struct is always signed. aad_file_unique is
   * required. If the file needs an AAD prefix, the reader must supply it. If
   * key_metadata is absent, the reader must get the footer key some other way.
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
