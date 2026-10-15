/**
 * Complete page-first modular footer for PAR3 files. PAR3 has no row groups or column chunks.
 */

include "parquet.thrift"

namespace cpp parquet.modular.page.full
namespace java org.apache.parquet.format.modular.page.full

/**
 * For R regions, region_offsets contains R + 1 strictly increasing absolute file offsets encoded
 * with Parquet DELTA_BINARY_PACKED. Region r is [offsets[r], offsets[r + 1]). Every serialized
 * Parquet page is exactly one region.
 */
struct RegionMap {
  1: required binary region_offsets
}

/**
 * A compact sequential integer array. Byte zero selects fixed-width bit-packing (zero) or Parquet
 * DELTA_BINARY_PACKED (one). A bit-packed payload continues with a ULEB128 count, one bit-width
 * byte, and LSB-first values. A DBP payload is the standard self-counting DBP stream.
 */
struct EncodedArray {
  1: required binary values
}

/** Every per-page array is decoded for the whole selected column. */
struct ColumnDataPages {
  1: required EncodedArray region_ordinals,
  2: required EncodedArray first_row_indexes,
  3: required bool is_fully_dictionary_encoded,
  4: required EncodedArray codecs,
  /** Dictionary region + 1; zero means that the data page does not use a dictionary. */
  5: required EncodedArray dictionary_regions,
  6: required EncodedArray num_values,
  7: required EncodedArray total_uncompressed_sizes
}

/** One fixed-width packed ColumnDataPages region ordinal per leaf column. */
struct PageDirectory {
  1: required binary column_data_page_regions
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
  PAGE_DIRECTORY = 1,
  ROW_RANGE_STATISTICS = 2,
  FILE_METADATA = 3,
  SCHEMA_INDEX = 4
}

struct ModuleDirectoryEntry {
  1: required ModuleKind kind,
  2: required i64 region_ordinal
}

struct PageFirstModularFooterFull {
  1: required i32 version,
  2: required i32 num_columns,
  3: required i64 num_rows,
  4: required list<ModuleDirectoryEntry> modules
}
