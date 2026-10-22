/**
 * Modular footer addendum for PAR1 and PAR3. Placement is organized as row groups, column chunks,
 * and an optional Offset Index while statistics use row ranges.
 */

include "parquet.thrift"

namespace cpp parquet.modular.addendum
namespace java org.apache.parquet.format.modular.addendum

/**
 * A compact sequential integer array. Byte zero selects fixed-width bit-packing (zero) or Parquet
 * DELTA_BINARY_PACKED (one). Both encodings include their element count in the binary payload.
 */
struct EncodedArray {
  1: required binary values
}

/**
 * For R regions, region_offsets contains R + 1 strictly increasing absolute file offsets encoded
 * with Parquet DELTA_BINARY_PACKED. Region r is [offsets[r], offsets[r + 1]).
 *
 * The row-group fields must either both be present or both be absent. For N row groups,
 * row_group_region_ordinals has N + 1 values and row_group_num_rows has N values. Row group i
 * spans [ordinals[i], ordinals[i + 1]). PAR1 requires these fields; PAR3 may include them for a
 * PAX-like physical layout or omit them for a row-group-free layout.
 */
struct RegionMap {
  1: required binary region_offsets,
  2: optional binary row_group_region_ordinals,
  3: optional binary row_group_num_rows
}

/**
 * Parallel arrays for every column chunk of one leaf column, in ascending first-row order. A
 * column chunk is one contiguous region containing one or more data pages. Dictionary pages are
 * separate regions. Split a column chunk when its data pages use different dictionaries.
 */
struct ColumnChunks {
  1: required EncodedArray region_ordinals,
  2: required EncodedArray first_row_indexes,
  3: required bool is_fully_dictionary_encoded,
  4: required EncodedArray codecs,
  /** Dictionary region + 1; zero means that the column chunk does not use a dictionary. */
  5: required EncodedArray dictionary_regions,
  6: required EncodedArray num_values,
  7: required EncodedArray total_uncompressed_sizes,
  8: required EncodedArray page_counts
}

/** One fixed-width packed ColumnChunks region ordinal per leaf column. */
struct ColumnChunkDirectory {
  1: required binary column_chunk_regions
}

/**
 * Child-page metadata for one leaf column. The arrays concatenate page_count - 1 entries for each
 * column chunk in chunk order. Relative offsets reset at each column-chunk region start; first-row
 * indexes are absolute within the file.
 */
struct ColumnOffsetIndex {
  1: required EncodedArray relative_page_offsets,
  2: required EncodedArray first_row_indexes
}

/**
 * present_columns marks columns with an Offset Index descriptor and is omitted when every column
 * is present. Columns whose chunks all have page_count one need no descriptor.
 * column_offset_index_regions has one fixed-width packed region ordinal per present column.
 */
struct OffsetIndexDirectory {
  1: optional binary present_columns,
  2: required binary column_offset_index_regions
}

/**
 * Per-column statistics over a dense partition of all top-level rows. Range i ends at start i + 1,
 * or at file num_rows for the final range. All arrays are sequential within the selected column.
 */
struct RowRangeStatistics {
  1: required EncodedArray range_start_rows,
  2: optional EncodedArray null_counts,
  3: optional EncodedArray nan_counts,
  /** LSB-first bit per range; omitted when every range has min/max. */
  4: optional binary min_max_presence,
  5: optional EncodedArray min_max_prefix_lengths,
  6: optional binary min_max_prefixes,
  /** Interleaved [min length, max length] for each present min/max pair. */
  7: optional EncodedArray min_max_suffix_lengths,
  8: optional binary min_max_suffixes,
  /** Interleaved [min exact, max exact] bits; omitted when every published bound is exact. */
  9: optional binary min_max_is_exact
}

/** Compact descriptors for the leaf columns that publish row-range statistics. */
struct RowRangeStatisticsDirectory {
  /** LSB-first bit per leaf column; omitted when every column is present. */
  1: optional binary present_columns,
  /** [u8 bit_width][one packed region ordinal per present column, in column order]. */
  2: required binary column_statistics_regions,
  /** One 2-bit parquet.BoundaryOrder per present column, aligned with field 2. */
  3: required binary boundary_orders
}

struct SchemaModule {
  1: required list<parquet.SchemaElement> schema,
  2: optional list<parquet.ColumnOrder> column_orders
}

struct FileMetadataModule {
  1: optional string created_by,
  2: optional list<parquet.KeyValue> key_value_metadata
}

struct SchemaIndexModule {
  1: required i32 num_slots,
  2: required binary hash_table,
  3: required i8 discriminator_bits,
  4: required i8 ordinal_bits,
  5: required i32 num_schema_elements,
  6: required binary element_offsets,
  7: optional binary leaf_element_indexes,
  8: optional binary parent_ordinals,
  9: required bool has_non_ascii_names
}

enum ModuleKind {
  SCHEMA = 0,
  COLUMN_CHUNK_DIRECTORY = 1,
  OFFSET_INDEX = 2,
  ROW_RANGE_STATISTICS = 3,
  FILE_METADATA = 4,
  SCHEMA_INDEX = 5
}

struct ModuleDirectoryEntry {
  1: required ModuleKind kind,
  2: required i64 region_ordinal
}

struct ModularFooterAddendum {
  1: required i32 version,
  2: required i32 num_columns,
  3: required i64 num_rows,
  4: required list<ModuleDirectoryEntry> modules
}
