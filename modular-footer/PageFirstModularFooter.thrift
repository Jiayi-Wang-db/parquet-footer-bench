/**
 * Page-first modular footer experiment.
 *
 * The fixed trailer locates RegionMap and PageFirstModularFooter directly. Every other
 * independently addressable object is exactly one region and is located by region ordinal.
 */

include "parquet.thrift"

namespace cpp parquet.modular.page
namespace java org.apache.parquet.format.modular.page

/**
 * Physical byte boundaries only. For R regions, region_offsets has R + 1 strictly increasing
 * absolute file offsets and region r is [region_offsets[r], region_offsets[r + 1]). Every complete
 * serialized Parquet page is exactly one region. region_offsets uses DELTA_BINARY_PACKED from the
 * Parquet encodings specification verbatim, with 128-value blocks and four 32-value miniblocks.
 */
struct RegionMap {
  1: required binary region_offsets
}

/**
 * A compact sequential integer array. Byte zero is the encoding: zero for fixed-width bit-packing,
 * one for Parquet DELTA_BINARY_PACKED. A bit-packed payload continues with its bit width byte and
 * LSB-first values. A DBP payload continues with the standard DBP stream. The containing structure
 * supplies the expected value count.
 */
struct EncodedArray {
  1: required binary values
}

/** Every per-page array is decoded for the whole selected column. */
struct ColumnDataPages {
  /** Number of aligned data-page positions. Zero means the complete column is null. */
  1: required i32 num_data_pages,
  /** UINT64 region ordinals containing each complete PageHeader and body. */
  2: required EncodedArray region_ordinals,
  /** UINT64 absolute, zero-based top-level first rows. */
  3: required EncodedArray first_row_indexes,
  /** True when every data page references a dictionary page; false when num_data_pages is zero. */
  4: required bool is_fully_dictionary_encoded,
  /** CompressionCodec values. */
  5: required EncodedArray codecs,
  /** UINT64 dictionary region + 1; zero when unused. The dictionary uses this codec. */
  6: required EncodedArray dictionary_regions,
  /** UINT64 page value counts. */
  7: required EncodedArray num_values,
  /** UINT64 uncompressed bytes including PageHeader and body. */
  8: required EncodedArray total_uncompressed_sizes
}

/** One region ordinal per leaf column, pointing to its independently serialized ColumnDataPages. */
struct PageDirectory {
  /** [u8 bit_width][num_columns region ordinals packed LSB-first]. */
  1: required binary column_data_page_regions
}

/**
 * Per-column statistics over a dense partition of all top-level rows. Block i ends at start i + 1,
 * or file num_rows for the final block. All arrays are sequential within this selected column.
 */
struct RowRangeStatistics {
  1: required i32 num_ranges,
  /** Absolute top-level starting rows; starts at zero and is strictly increasing. */
  2: required EncodedArray range_start_rows,
  /** Exactly num_ranges values when present. */
  3: optional list<i64> null_counts,
  /** Exactly num_ranges values when present. */
  4: optional list<i64> nan_counts,
  /** LSB-first bit per range; omitted when every range has min/max. */
  5: optional binary min_max_presence,
  /** One prefix byte length per present min/max pair; absence means no min/max statistics. */
  6: optional list<i32> min_max_prefix_lengths,
  /** Concatenated prefixes in present-range order. */
  7: optional binary min_max_prefixes,
  /** Interleaved [min length, max length] for each present min/max pair. */
  8: optional list<i32> min_max_suffix_lengths,
  /** Concatenated [min suffix, max suffix] bytes in present-range order. */
  9: optional binary min_max_suffixes,
  /** Interleaved [min exact, max exact] bits; omitted when every published bound is exact. */
  10: optional binary min_max_is_exact
}

/** Compact descriptors for the leaf columns that publish statistics. */
struct RowRangeStatisticsDirectory {
  /** LSB-first bit per leaf column; omitted when every column participates. */
  1: optional binary participating_columns,
  /** [u8 bit_width][one packed region ordinal per participating column, in column order]. */
  2: required binary column_statistics_regions
}

/** Schema is Compact-Thrift encoded and read in full unless SchemaIndexModule is present. */
struct SchemaModule {
  1: required list<parquet.SchemaElement> schema,
  2: optional list<parquet.ColumnOrder> column_orders
}

/** Descriptive metadata is Compact-Thrift encoded and read in full. */
struct FileMetadataModule {
  1: optional string created_by,
  2: optional list<parquet.KeyValue> key_value_metadata
}

/**
 * Optional direct-access index for projected schema columns. Every binary is fixed-width bit-packed
 * as [u8 bit_width][values packed LSB-first].
 */
struct SchemaIndexModule {
  /** Power-of-two open-addressed table size. */
  1: required i32 num_slots,
  /**
   * num_slots packed entries; zero means empty. A nonzero slot is
   * (hash_discriminator << ordinal_bits) | (leaf_ordinal + 1). Paths use FNV-1a-64 over
   * lowercased ASCII segments joined by NUL, excluding the root. The home slot is
   * hash & (num_slots - 1), with ascending linear probing.
   */
  2: required binary hash_table,
  3: required i8 discriminator_bits,
  4: required i8 ordinal_bits,
  5: required i32 num_schema_elements,
  /** One SchemaModule-relative byte offset per SchemaElement. */
  6: required binary element_offsets,
  /** One schema-element index per leaf column; omitted for a flat schema. */
  7: optional binary leaf_element_indexes,
  /** One parent schema-element index per element; omitted for a flat schema. */
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

/** One module occupies one complete region. */
struct ModuleDirectoryEntry {
  1: required ModuleKind kind,
  2: required i64 region_ordinal
}

/**
 * Always-read logical root. RegionMap is located independently by the fixed trailer. The module
 * list contains at most one entry per kind and is scanned in full. SCHEMA and PAGE_DIRECTORY are
 * required; every other module kind is optional.
 */
struct PageFirstModularFooter {
  1: required i32 version,
  2: required i32 num_columns,
  3: required i64 num_rows,
  4: required list<ModuleDirectoryEntry> modules
}
