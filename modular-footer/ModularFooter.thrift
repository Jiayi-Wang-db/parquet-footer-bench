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
 * Modular Footer: typed Thrift modules whose array fields hold their encoded values inline.
 *
 * ArrayPage carries the encoded array bytes inline in its data field, with the encoding, value
 * count, and parameters beside it as typed fields. The typed field containing ArrayPage defines the
 * values' meaning, type, and logical domain; ArrayPage defines only their physical encoding.
 *
 * Modules preserve independent read lifecycles. A reader can fetch placement without fetching
 * row-group statistics, and can fetch per-page indexes only for projected column chunks. Schema
 * and descriptive file metadata remain ordinary Thrift data because readers consume them in full.
 *
 * The outer file framing that locates ModularFooter from the end of a file is specified separately.
 */

include "parquet.thrift"

namespace cpp parquet.modular
namespace java org.apache.parquet.format.modular

/**
 * Raw array encodings. Neither applies general-purpose compression. Values are always bit-packed;
 * the encodings differ in how presence is recorded and whether absent positions take a value slot.
 * There is no separate dense encoding: a fully populated array is BITSET with no stored bitmap
 * (num_present == num_values), which is just a plain full-length packed array.
 */
enum ArrayEncoding {
  /**
   * A full-length value stream with one bit-packed value per position, optionally preceded by a
   * validity bitmap. Value i is read directly at bit offset i * value_bit_width, with no rank step;
   * the bitmap only says whether that value is present, and an absent position still occupies a
   * slot holding an unspecified placeholder. When num_present == num_values no bitmap is stored
   * (the dense case). Best from fully dense to moderately sparse.
   */
  BITSET = 0,
  /**
   * A bit-packed sorted list of present logical positions followed by the bit-packed present-only
   * values; absent positions take no slot. Position lookup is O(log num_present) by binary search.
   * Smaller than BITSET only in the very-sparse tail, where BITSET's full-length value stream would
   * spend most of its slots on absent positions.
   */
  PRESENT_INDEX = 1
}

/** Parameters for a BITSET payload. */
struct BitsetParameters {
  /**
   * Width of each value in the full-length value stream, or each BYTE_ARRAY cumulative offset,
   * in bits.
   */
  1: required i8 value_bit_width,
  /**
   * Number of present positions. When num_present == ArrayPage.num_values every position is
   * present and no validity bitmap is stored (the dense case). Otherwise a plain
   * ceil(num_values / 8)-byte validity bitmap precedes the full-length value stream, with bit i set
   * when position i is present.
   */
  2: required i32 num_present
}

/** Parameters for a PRESENT_INDEX payload. */
struct PresentIndexParameters {
  /** Number of logical positions that have a value. */
  1: required i32 num_present,
  /** Width of each entry in the sorted logical-position stream, in bits. */
  2: required i8 position_bit_width,
  /** Width of each present integer value, or each BYTE_ARRAY cumulative offset, in bits. */
  3: required i8 value_bit_width
}

/** Exactly one member MUST be set, matching ArrayPage.encoding. */
union ArrayEncodingParameters {
  1: BitsetParameters bitset,
  2: PresentIndexParameters present_index
}

/**
 * One encoded array, stored inline.
 *
 * data holds the encoded array bytes directly in the module, rather than an offset and length
 * pointing to a payload elsewhere in the file. num_values is the size of the complete logical
 * domain, including absent positions. Under BITSET the value stream has one entry per position (an
 * absent position holds an unspecified placeholder the reader must not use); under PRESENT_INDEX
 * only present positions have an entry. The containing typed module field defines whether values
 * are BOOLEAN, UINT32, UINT64, or BYTE_ARRAY and defines the logical indexing domain.
 */
struct ArrayPage {
  1: required binary data,
  2: required ArrayEncoding encoding,
  3: required i32 num_values,
  4: required ArrayEncodingParameters parameters
}

/** Absolute location of one independently compact-Thrift serialized module. */
struct ModuleLocation {
  1: required i64 offset,
  2: required i64 length
}

/** Schema is tree-shaped and read in full, so it remains ordinary Thrift data. */
struct SchemaModule {
  1: required list<parquet.SchemaElement> schema,
  2: optional list<parquet.ColumnOrder> column_orders
}

/**
 * Placement for every column chunk.
 *
 * Unless noted otherwise, pages contain num_columns * num_row_groups UINT64 values in column-major
 * chunk space: chunk (column c, row group g) is at c * num_row_groups + g. Every column chunk has a
 * value, so these required arrays use the all-ones case of BITSET.
 */
struct PlacementModule {
  /** UINT64: first data-page byte offset. */
  1: required ArrayPage data_page_offsets,
  /** UINT64: num_chunks + 1 cumulative indexes into dictionary_page_offsets. */
  2: required ArrayPage first_dictionary_pages,
  /** UINT64: flattened byte offsets of all dictionary pages. */
  3: required ArrayPage dictionary_page_offsets,
  /** UINT64: total compressed bytes in each column chunk. */
  4: required ArrayPage total_compressed_sizes,
  /** UINT64: total uncompressed bytes in each column chunk. */
  5: required ArrayPage total_uncompressed_sizes,
  /** UINT64: value count in each column chunk. */
  6: required ArrayPage num_values,
  /** UINT32: parquet.CompressionCodec value for each column chunk. */
  7: required ArrayPage codecs,
  /** UINT32: parquet.Type value; num_columns entries, one per leaf column. */
  8: required ArrayPage physical_types,
  /** BOOLEAN: true when every data page in the column chunk is dictionary encoded. */
  9: required ArrayPage is_fully_dictionary_encoded
}

/**
 * Row-group statistics for one leaf column; array positions are row-group ordinals.
 *
 * A row group's min and max often share a leading run of bytes (timestamps, sorted keys). That
 * longest common prefix is stored once in minmax_prefixes, and min_suffixes / max_suffixes carry
 * only the differing tails, so a long shared prefix is never written twice. The three arrays share
 * present positions: a row group that has a min/max has an entry in all three.
 */
struct ColumnStatistics {
  /** UINT64: optional null count for each row group. */
  1: optional ArrayPage null_counts,
  /** BYTE_ARRAY: longest common prefix of each row group's min and max (empty when none). */
  2: optional ArrayPage minmax_prefixes,
  /** BYTE_ARRAY: each present minimum with its minmax_prefixes entry stripped (suffix only). */
  3: optional ArrayPage min_suffixes,
  /** BYTE_ARRAY: each present maximum with its minmax_prefixes entry stripped (suffix only). */
  4: optional ArrayPage max_suffixes,
  /** BOOLEAN: 1 when the minimum is exact, 0 when it is a truncated (rounded-down) lower bound. */
  5: optional ArrayPage min_is_exact,
  /** BOOLEAN: 1 when the maximum is exact, 0 when it is a truncated (rounded-up) upper bound. */
  6: optional ArrayPage max_is_exact,
  /** UINT64: optional NaN count. */
  7: optional ArrayPage nan_counts
}

/**
 * Directory of independently serialized ColumnStatistics descriptors.
 *
 * column_offsets contains num_columns + 1 dense UINT64 absolute file offsets. Entries c and c+1
 * delimit the descriptor for leaf column c. Equal offsets mean that the column has no row-group
 * statistics. A per-column encryption envelope may cover the descriptor and its inline arrays so
 * one column key protects the column's statistics as a unit.
 */
struct RowGroupStatisticsModule {
  1: required ArrayPage column_offsets
}

/**
 * Per-page placement for one (leaf column, row group) column chunk. Array pages use that column
 * chunk's data-page ordinal as their logical position.
 */
struct OffsetIndexChunk {
  /** UINT64: page byte offset. */
  1: required ArrayPage offsets,
  /** UINT32: compressed page bytes including its page header. */
  2: required ArrayPage compressed_page_sizes,
  /** UINT64: first row index within the row group. */
  3: required ArrayPage first_row_indexes
}

/**
 * Per-page statistics for one (leaf column, row group) column chunk.
 *
 * Min and max reuse the same common-prefix stripping as the row-group statistics: each page's
 * longest common prefix is stored once in minmax_prefixes, and min_suffixes / max_suffixes carry
 * only the differing tails. The three arrays share present positions.
 */
struct ColumnIndexChunk {
  1: required parquet.BoundaryOrder boundary_order,
  /** BOOLEAN: true when the page contains only null values. */
  2: required ArrayPage null_pages,
  /** UINT64: optional null count. */
  3: optional ArrayPage null_counts,
  /** BYTE_ARRAY: longest common prefix of each page's min and max (empty when none). */
  4: optional ArrayPage minmax_prefixes,
  /** BYTE_ARRAY: each present minimum with its minmax_prefixes entry stripped (suffix only). */
  5: optional ArrayPage min_suffixes,
  /** BYTE_ARRAY: each present maximum with its minmax_prefixes entry stripped (suffix only). */
  6: optional ArrayPage max_suffixes,
  /** BOOLEAN: 1 when the minimum is exact, 0 when it is a truncated (rounded-down) lower bound. */
  7: optional ArrayPage min_is_exact,
  /** BOOLEAN: 1 when the maximum is exact, 0 when it is a truncated (rounded-up) upper bound. */
  8: optional ArrayPage max_is_exact,
  /** UINT64: optional NaN count. */
  9: optional ArrayPage nan_counts
}

/**
 * Directory for independently serialized per-column-chunk index descriptors.
 *
 * chunk_offsets contains num_columns * num_row_groups + 1 dense UINT64 absolute file offsets in
 * column-major chunk space. Entries k and k+1 delimit one compact-Thrift OffsetIndexChunk or
 * ColumnIndexChunk. Equal offsets mean that the chunk has no corresponding index.
 */
struct PageIndexModule {
  1: required ArrayPage chunk_offsets
}

/** Descriptive metadata is read in full, so it remains ordinary Thrift data. */
struct FileMetadataModule {
  1: optional string created_by,
  2: optional list<parquet.KeyValue> key_value_metadata
}

/** Kinds of module the directory can locate. Older readers skip kinds they do not understand. */
enum ModuleKind {
  SCHEMA = 0,
  PLACEMENT = 1,
  ROW_GROUP_STATISTICS = 2,
  OFFSET_INDEX = 3,
  COLUMN_INDEX = 4,
  FILE_METADATA = 5
}

/** One directory entry: the location of the module of the given kind. */
struct ModuleDirectoryEntry {
  1: required ModuleKind kind,
  2: required ModuleLocation location
}

/**
 * The always-read root. modules is a directory mapping each present module to its independently
 * compact-Thrift serialized location. SCHEMA and PLACEMENT MUST be present; other kinds are
 * optional. A new module kind is added to ModuleKind and slotted into the directory without
 * changing this struct, and a reader ignores entries whose kind it does not understand.
 */
struct ModularFooter {
  1: required i32 version,
  2: required i32 num_row_groups,
  3: required i32 num_columns,
  4: required i64 num_rows,
  5: required list<i64> row_group_num_rows,
  6: required list<ModuleDirectoryEntry> modules
}
