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

/** Parallel arrays indexed by row-group ordinal for one leaf column. */
struct ColumnPageDirectory {
  1: required EncodedArray chunk_region_ordinals,
  /** Number of data pages in the chunk; dictionary pages are excluded. */
  2: required EncodedArray page_counts,
  3: required EncodedArray total_uncompressed_sizes,
  4: required EncodedArray num_values,
  5: required EncodedArray codecs,
  6: required EncodedArray is_fully_dictionary_encoded
}

/** One independently addressable ColumnPageDirectory region ordinal per leaf column. */
struct PageDirectory {
  1: required binary column_page_directory_regions
}

/**
 * Offset Index for one leaf column across the complete file. Relative offsets reset at each chunk
 * boundary identified by ColumnPageDirectory.page_counts. Page sizes are derived from the next
 * relative offset or the chunk-region end. first_row_indexes are absolute within the file.
 */
struct FlatOffsetIndex {
  1: required EncodedArray relative_data_page_offsets,
  2: required EncodedArray first_row_indexes
}

/** Compact descriptors for the leaf columns that have at least one source OffsetIndex. */
struct FlatOffsetIndexDirectory {
  1: optional binary present_columns,
  2: required binary offset_index_regions
}

/**
 * One descriptor for a leaf column across the complete file. Page arrays align with that column's
 * FlatOffsetIndex data-page order. Sparse positions represent pages whose PAR1 column chunk has no
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
  FLAT_OFFSET_INDEX = 3,
  FLAT_COLUMN_INDEX = 4,
  FILE_METADATA = 5,
  SCHEMA_INDEX = 6
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
