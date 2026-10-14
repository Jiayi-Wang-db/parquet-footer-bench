/**
 * Page-first modular-footer addendum for PAR1 files. Row groups remain part of the physical and
 * logical contract; only column-chunk placement and the chunk-oriented page index are replaced.
 */

include "parquet.thrift"
include "ModularFooter.thrift"

namespace cpp parquet.modular.page.addendum
namespace java org.apache.parquet.format.modular.page.addendum

/**
 * The three arrays use Parquet DELTA_BINARY_PACKED. For N row groups,
 * row_group_region_ordinals has N + 1 values and row group i spans
 * [ordinals[i], ordinals[i + 1]). row_group_num_rows has N values.
 */
struct RegionMap {
  1: required binary region_offsets,
  2: required binary row_group_region_ordinals,
  3: required binary row_group_num_rows
}

/** Sequential bit-packed or DELTA_BINARY_PACKED integer array, as in the full PAR3 definition. */
struct EncodedArray {
  1: required binary values
}

struct ColumnDataPages {
  1: required EncodedArray region_ordinals,
  2: required EncodedArray first_row_indexes,
  3: required bool is_fully_dictionary_encoded,
  4: required EncodedArray codecs,
  5: required EncodedArray dictionary_regions,
  6: required EncodedArray num_values,
  7: required EncodedArray total_uncompressed_sizes
}

struct PageDirectory {
  1: required binary column_data_page_regions
}

/**
 * One descriptor for a leaf column across the complete file. Page arrays use the corresponding
 * ColumnDataPages order. Sparse positions represent pages whose PAR1 column chunk has no
 * ColumnIndex. boundary_orders uses row-group ordinals as its logical domain.
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

/** Compact descriptors for the leaf columns that have at least one source ColumnIndex. */
struct FlatColumnIndexDirectory {
  1: optional binary present_columns,
  2: required binary column_index_regions
}

/**
 * Current modular ColumnStatistics descriptors are retained unchanged. This directory changes
 * only their addressing from byte offsets to region ordinals.
 */
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

/** PAR1 magic supplies the interpretation of the required row-group arrays in RegionMap. */
struct PageFirstModularFooterAddendum {
  1: required i32 version,
  2: required i32 num_row_groups,
  3: required i32 num_columns,
  4: required i64 num_rows,
  5: required list<ModuleDirectoryEntry> modules
}
