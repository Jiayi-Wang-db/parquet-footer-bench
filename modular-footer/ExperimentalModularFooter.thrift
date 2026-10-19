/**
 * Bootstrap for an experimental modular footer whose module payloads are embedded Parquet data.
 * Each module uses ordinary Parquet schema, page, encoding, and FileMetaData structures.
 */

namespace cpp parquet.modular.experimental
namespace java org.apache.parquet.format.modular.experimental

enum MetadataLayout {
  /** One struct containing repeated metadata leaves per source leaf column. */
  WIDE_COLUMN_STRUCTS = 0,
  /** Constant-schema arrays concatenated across source columns with column-start directories. */
  FLAT_STREAMS = 1,
  /** Constant schema with one list-valued row per source leaf column. */
  COLUMN_ROWS = 2,
  /** Constant schema with one packed integer data page per source leaf column. */
  COLUMN_PAGES = 3,
  /** One or a few packed PFOR integer streams per independently readable metadata module. */
  PACKED_STREAMS = 4
}

enum ExperimentalModuleKind {
  /** Source schema, file metadata, region map, and column-chunk placement. */
  CORE = 0,
  /** Hierarchical Offset Index entries beyond each column chunk's implicit first page. */
  OFFSET_INDEX = 1,
  /** Coarse or page-shaped row-range statistics. */
  ROW_RANGE_STATISTICS = 2
}

struct ExperimentalModule {
  1: required ExperimentalModuleKind kind,
  2: required i64 offset,
  3: required i64 length
}

/**
 * A fixed outer trailer locates this root. Each entry locates an independently readable embedded
 * Parquet module whose standard Thrift footer locates that module's metadata data pages.
 */
struct ExperimentalModularFooter {
  1: required i32 version,
  2: required MetadataLayout layout,
  3: required list<ExperimentalModule> modules
}

/**
 * FLAT_STREAMS uses the following logical Parquet leaves. Integer leaves use PFOR with frame or
 * delta mode selected independently for every 1,024-value block.
 *
 * Global placement:
 *   region_offsets, row_group_region_ordinals, row_group_num_rows
 *
 * Column-chunk placement:
 *   chunk_column_starts, chunk_region_ordinals, chunk_first_row_indexes, chunk_codecs,
 *   chunk_dictionary_regions, chunk_num_values, chunk_total_uncompressed_sizes, chunk_page_counts,
 *   fully_dictionary_encoded
 *
 * Offset Index:
 *   offset_index_column_starts, relative_page_offsets, page_first_row_indexes
 *
 * A source column c owns each half-open slice [column_starts[c], column_starts[c + 1]). The first
 * page of every column chunk is implicit, so Offset Index slices contain page_count - 1 values per
 * chunk.
 */

/**
 * The readable COLUMN_ROWS statistics schema has one row per source leaf column:
 *
 *   boundary_order: optional int32
 *   row_range_statistics: list<struct<
 *     start_row: int64,
 *     null_count: optional int64,
 *     nan_count: optional int64,
 *     min_max: optional struct<
 *       prefix: binary,
 *       min_suffix: binary,
 *       max_suffix: binary,
 *       exactness: int32
 *     >
 *   >>
 *
 * source_column identifies the source leaf column; the combined-file experiment reserves -1 for
 * its global metadata row. An empty row_range_statistics list means that the source column does
 * not publish statistics. exactness uses bit 0 for the minimum and bit 1 for the maximum. Standard
 * Parquet null definition levels encode unavailable fields, list repetition levels delimit
 * columns and ranges, and DELTA_LENGTH_BYTE_ARRAY encodes the binary leaves. No statistics-specific
 * directories, presence bitmaps, or byte-length arrays are required.
 */

/**
 * PACKED_STREAMS retains the hierarchy by storing CORE, OFFSET_INDEX, and ROW_RANGE_STATISTICS as
 * separate embedded Parquet modules. Each module normally has a packed integer leaf named `i` and
 * an optional binary leaf named `b`. The integer grammar is fixed by module kind and version.
 * Writers may split large Offset Index or statistics arrays into additional leaves (`p0`, `p1`,
 * or `sN`) when independently PFOR-encoding those leaves makes the serialized module smaller.
 *
 * CORE uses field-major arrays and one shared column-chunk start directory. OFFSET_INDEX uses one
 * shared source-column start directory followed by relative page offsets and first-row indexes.
 * ROW_RANGE_STATISTICS uses a participating-column bitmap, boundary orders, and one source-column
 * start directory for each integer or binary statistics field.
 *
 * PFOR frame or delta mode is selected independently for every 1,024-value block. Without an
 * optional block-offset directory, readers decode a complete selected integer leaf. This keeps the
 * physical representation standard and very small; the benchmark separately reports the bounded
 * decode cost and the cost of adding fixed-width block offsets.
 */
