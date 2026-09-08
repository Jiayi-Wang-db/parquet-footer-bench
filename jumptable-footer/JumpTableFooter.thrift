// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

/**
 * Jump-table footer variation of parquet.thrift.
 *
 * The backward-compatible "inline footer index" design: today's nested
 * FileMetaData is left exactly as it is, but an optional index is emitted that
 * lets a reader seek straight to any column chunk's metadata (and resolve names)
 * instead of walking the footer. A reader that does not understand the pointer
 * reads the file exactly as a pre-extension Parquet file.
 *
 * The delta to parquet.thrift is:
 *   1. one new optional field on FileMetaData:
 *        10: optional binary footer_index_pointer   (documented below)
 *   2. the index structs defined here, reached via that pointer.
 *
 * Definitions copied directly from the reference jump-table parquet.thrift.
 */

// ---------------------------------------------------------------------------
// Addition to parquet.thrift's FileMetaData. This is a new optional field on
// the existing struct -- not a new struct -- so it is shown here for reference:
//
//   struct FileMetaData {
//     // ... existing fields 1..9 unchanged ...
//     10: optional binary footer_index_pointer
//   }
//
//  /**
//   * Presence signals that a {@link FileMetadataFooterIndex} struct has been emitted
//   * somewhere in the file. Raw binary with a versioned layout — the first byte is a
//   * {@code version} tag, and the remaining bytes version dependent:
//   *
//   *   Version 1 — uncompressed Thrift FileMetadataFooterIndex.
//   *     Payload: i64 index_start, i64 index_length, i64 file_meta_data_length.
//   *
//   *   Version 2 — zstd-compressed Thrift FileMetadataFooterIndex.
//   *     Same payload shape as v1. Reader decompresses
//   *     bytes[index_start .. index_start + index_length) and Thrift-decodes the
//   *     result
//   *
//   *   Version 3 — LZ4_RAW-compressed Thrift FileMetadataFooterIndex.
//   *     Same payload shape as v1/v2.
//   *
//   * To be effective, this field should be emitted in the footer first before all other Thrift fields.
//   *
//   * Readers that do not recognise this field (or an unknown version
//   * byte) ignore it and read the file exactly as a pre-extension Parquet file.
//   */
//  10: optional binary footer_index_pointer
// ---------------------------------------------------------------------------

/**
 * Optional open-addressed hash table mapping FNV-1a-64 of (ASCII-lowercase-folded,
 * NUL-separated) schema path → SchemaElement ordinal. Keys all schema elements —
 * interior groups as well as leaves — so name resolution works for both column
 * paths and struct paths. The ordinal can be looked up in SchemaLayout.offsets
 * to reach the SchemaElement's bytes.
 *
 * Slot encoding:
 *   bytes_per_slot = table.length / num_slots
 *   ordinal_bits   = bytes_per_slot * 8 - discriminator_bits
 *   slot_value     = (discriminator << ordinal_bits) | (element_ordinal + 1)
 * Empty slots are all-zero bytes.
 *
 * Readers probe by hashing the query path, filter candidate slots by the top
 * discriminator_bits (if any), then confirm by walking the encoded element's
 * parent chain (via a stack walk of num_children, or decoded SchemaElements'
 * names) byte-comparing each segment against the query. This struct is only
 * useful when SchemaLayout is also present.
 */
struct NameHashTable {
  /** Number of slots in the table. Load-bearing: readers derive bytes_per_slot
   *  as table.length / num_slots. Writers pick any value (typically a power of
   *  two >= 2 * num_items for load factor <= 0.5, but the reader doesn't care). */
  1: required i32 num_slots

  /** Number of discriminator bits at the top of each slot; the remainder holds
   *  (element_ordinal + 1) in the low bits. Omit (or set to 0) if the writer
   *  declined to use a discriminator — probes then always fall through to the
   *  byte-compare confirm step. */
  2: optional i32 discriminator_bits

  /** The packed slot bytes. See surrounding struct comment for slot encoding. */
  3: required binary table
}

/** Marker: no SchemaElement has a field_id set. */
struct NoFieldIds {}

/** Marker: for every leaf, field_id equals column_ordinal — the trivial 1:1 mapping. */
struct FieldIdIsColumnOrdinal {}

/**
 * How Parquet field_id values (on leaf SchemaElements) relate to the column
 * ordinals used to index into column_chunk_offsets. Lets readers resolve a
 * field_id to its column without walking the schema list.
 */
union FieldIdMapping {
  /** No leaf in this file has field_id set; field_id lookup is not applicable. */
  1: NoFieldIds none

  /** Every leaf's field_id equals its column_ordinal — no table needed. */
  2: FieldIdIsColumnOrdinal identity

  /** Explicit field_id → column_ordinal map. */
  3: map<i32, i32> explicit
}

/**
 * Optional side arrays giving each SchemaElement's byte offset (and optionally
 * child count) indexed by element ordinal in DFS pre-order. Lets readers
 * reconstruct the whole schema tree — parent of each element, column ordinal of
 * each leaf, full path by walking parents — in a single O(num_schema_elements)
 * linear pass at file open, with O(1) random access to any element's bytes
 * thereafter.
 *
 * Tree reconstruction: scan elements in order, maintain a parent stack using
 * num_children as the child-count-remaining counter (push on encountering a
 * group, pop when remaining hits zero). Leaf column_ordinals fall out as a
 * monotone counter over non-group elements.
 */
struct SchemaLayout {
  /** Total SchemaElement count, matching FileMetaData.schema.size. Load-bearing:
   *  readers derive bytes_per_entry of offsets and num_children as
   *  offsets.length / num_schema_elements. */
  1: required i32 num_schema_elements

  /** Packed array of byte offsets (relative to FileMetaData start) of each
   *  SchemaElement, indexed by element ordinal in DFS pre-order. Entry 0 is the
   *  root. Writer picks the whole-byte width; readers recover it as
   *  offsets.length / num_schema_elements. */
  2: required binary offsets

  /** Packed array of num_children per element, indexed by element ordinal.
   *  Omit when the schema is flat (one root with all-leaves directly under it) —
   *  in that case readers assume element 0 is the root and elements
   *  1..num_schema_elements-1 are leaves with column_ordinal = ordinal - 1.
   *  Bytes per entry = num_children.length / num_schema_elements; 1 byte covers
   *  any realistic schema. */
  3: optional binary num_children

  /** Optional path → SchemaElement-ordinal hash table. Ordinals index into
   *  offsets above. Writers that don't produce one omit this field; readers
   *  fall back to walking the tree. */
  4: optional NameHashTable name_hash_table

  /** Optional field_id → column_ordinal resolution hint. Writers that don't
   *  produce one omit this field; readers that need field_id lookups walk the
   *  leaves checking each SchemaElement.field_id. */
  5: optional FieldIdMapping field_id_mapping

  /** {@code true} if any SchemaElement name contains a byte outside the ASCII range
   *  (0x00..0x7F). Advisory hint for name-search callers. */
  6: optional bool has_non_ascii_names
}

/**
 * Supplementary footer index — optional. Pre-computed hash and offset tables let
 * readers answer name lookups and seek directly to column metadata without walking
 * FileMetaData. Existence, location, length, and optional compression are signalled
 * via {@link FileMetaData#footer_index_pointer}; that field's layout is
 * format-level and the index blob itself has no implied position in the file.
 * Readers that do not understand the pointer read the file exactly as before.
 */
struct FileMetadataFooterIndex {
  /** Number of leaf columns (primitive-typed nodes in the schema tree). Load-bearing:
   *  together with num_row_groups lets readers derive column_chunk_offsets'
   *  bytes_per_entry as column_chunk_offsets.length / (num_row_groups * num_leaf_columns).
   */
  1: required i32 num_leaf_columns

  /** Number of row groups — matches FileMetaData.row_groups.size. */
  2: required i32 num_row_groups

  /**
   * Packed byte offsets into FileMetaData for every (row_group, leaf_column) cell's
   * ColumnChunk, plus one trailing end-offset per row group. Layout per row group
   * has num_leaf_columns + 1 entries: entries 0..num_leaf_columns-1 are the starts
   * of each ColumnChunk struct; the final entry is the byte after the last
   * ColumnChunk's T_STOP (i.e. the byte immediately after RowGroup.fid=1's list
   * body). The end offset lets readers jump past the columns list without a
   * skipStruct on the last column.
   *
   * Indexed rg * (num_leaf_columns + 1) + col, where col ∈ [0, num_leaf_columns]
   * and col == num_leaf_columns gives the row-group end offset. col is the
   * leaf_column ordinal available on leaf SchemaElements as column_ordinal.
   *
   * Readers derive bytes_per_entry from arithmetic on counts the struct already
   * provides:
   *   bytes_per_entry =
   *     column_chunk_offsets.length / (num_row_groups * (num_leaf_columns + 1))
   * Writer picks the width (typically the minimum whole-byte width that covers
   * FileMetaData's length).
   */
  3: required binary column_chunk_offsets

  /**
   * Optional: byte offset within FileMetaData of each top-level field that is present
   * in the serialized footer, keyed by Thrift field id. Readers that want to abort the
   * parseLoop at fid 10 and seek to specific fields can use this; readers that just
   * continue the walk (column_chunk_offsets provides the only genuine shortcut) don't
   * need it, and writers can omit it.
   */
  4: optional map<i16, i64> field_offsets

  /** Optional per-SchemaElement offset + num_children arrays, plus (nested
   *  within) the optional name hash table and field_id mapping. When present,
   *  readers can reconstruct the whole schema tree and random-access any element
   *  by ordinal. */
  5: optional SchemaLayout schema_layout
}
