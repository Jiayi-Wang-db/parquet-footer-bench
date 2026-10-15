/**
 * Experimental fully page-first modular footer for PAR1 files. Every serialized page is one
 * region; row groups remain part of the PAR1 logical contract.
 */

include "parquet.thrift"
include "ModularFooter.thrift"

namespace cpp parquet.modular.pages_only
namespace java org.apache.parquet.format.modular.pages_only

/**
 * For R regions, region_offsets contains R + 1 strictly increasing absolute file offsets encoded
 * with Parquet DELTA_BINARY_PACKED. Region r is [offsets[r], offsets[r + 1]). Every serialized
 * Parquet page is exactly one region. The row-group arrays also use DELTA_BINARY_PACKED.
 */
struct RegionMap {
  1: required binary region_offsets,
  2: required binary row_group_region_ordinals,
  3: required binary row_group_num_rows
}

/** Sequential fixed-width bit-packed or DELTA_BINARY_PACKED integer array. */
struct EncodedArray {
  1: required binary values
}

/** Every per-page array is decoded for the complete selected leaf column. */
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
 * Page statistics for one leaf column across the complete file. Page arrays align with the
 * corresponding ColumnDataPages order. boundary_orders uses row-group ordinals as its domain.
 */
struct FlatColumnIndex {
  1: required ModularFooter.EncodedArray boundary_orders,
  2: required ModularFooter.EncodedArray null_pages,
  3: optional ModularFooter.EncodedArray null_counts,
  4: optional ModularFooter.EncodedArray minmax_prefixes,
  5: optional ModularFooter.EncodedArray min_suffixes,
  6: optional ModularFooter.EncodedArray max_suffixes,
  7: optional ModularFooter.EncodedArray min_is_exact,
  8: optional ModularFooter.EncodedArray max_is_exact,
  9: optional ModularFooter.EncodedArray nan_counts
}

struct FlatColumnIndexDirectory {
  1: optional binary present_columns,
  2: required binary column_index_regions
}

struct RowGroupStatisticsDirectory {
  1: optional binary present_columns,
  2: required binary column_statistics_regions
}

enum ModuleKind {
  SCHEMA = 0,
  PAGE_DIRECTORY = 1,
  ROW_GROUP_STATISTICS = 2,
  FLAT_COLUMN_INDEX = 3,
  FILE_METADATA = 4,
  SCHEMA_INDEX = 5
}

struct ModuleDirectoryEntry {
  1: required ModuleKind kind,
  2: required i64 region_ordinal
}

struct PagesOnlyModularFooter {
  1: required i32 version,
  2: required i32 num_row_groups,
  3: required i32 num_columns,
  4: required i64 num_rows,
  5: required list<ModuleDirectoryEntry> modules
}
