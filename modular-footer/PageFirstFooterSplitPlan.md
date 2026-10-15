# Page-First Footer Split Plan

## Goal

Publish a small PAR1 addendum to the current modular-footer proposal, then describe the complete
PAR3 metadata model separately. Both use page placement, but only PAR1 retains row groups.

## Current local state

Implemented experimentally in the two schemas below. The converter, size benchmark, decode
benchmark, design notes, and machine-readable results are updated locally. Nothing in this split
has been committed or pushed.

## PAR1: `PageFirstModularFooterAddendum.thrift`

- Start from the current modular-footer proposal and reuse unchanged schema, file-metadata,
  schema-index, and row-group-statistics structures.
- Make each column chunk one region. Address one per-column descriptor containing parallel chunk
  region ordinals, page counts, uncompressed sizes, value counts, codecs, and dictionary flags.
- Require DBP `row_group_region_ordinals` (`N + 1` region borders) and DBP
  `row_group_num_rows` (`N` row counts). Retain `num_row_groups` and validate all counts and the sum
  of row counts.
- Replace chunk-oriented Offset Index with one flat descriptor per fully participating column.
  Store data-page offsets relative to each chunk-region start and file-global first-row indexes.
  Prefix-sum page counts to locate each chunk's page slice and derive page sizes from the next
  relative offset or chunk-region end.
- Replace chunk-oriented Column Index with one descriptor per column. Its page arrays align with
  `ColumnDataPages`; sparse positions represent row groups without a Column Index. Preserve
  `boundary_order` per `(column, row group)`, since PAR1 ordering may restart at every row group.

## PAR3: `PageFirstModularFooterFull.thrift`

- Keep the complete page-first placement model but remove `num_row_groups`,
  `row_group_region_ordinals`, and `row_group_num_rows`.
- Use per-column row-range statistics and remove both Offset Index and Column Index. Writers publish
  page-granular row ranges when they need page-level pruning.
- Add one flat, two-bit `boundary_orders` value per present statistics column to
  `RowRangeStatisticsDirectory`. It describes ordering across that column's complete row-range
  sequence; use `UNORDERED` unless global ascending or descending order holds.

## Implementation and validation

1. Define the two public Thrift files, reusing current modular types in the addendum where possible
   and keeping the full PAR3 definition independent.
2. Update the PAR1 converter to emit required row-group arrays, existing row-group statistics, and
   flat Offset Index and Column Index modules when source indexes exist. Add count, ordering, and
   page-alignment checks.
3. Retain or add a PAR3 conversion path for row-range statistics and global per-column boundary
   order.
4. Regenerate the benchmark corpus using Raincloud's parquet-java target rather than the existing
   files. Confirm and record that this target emits Offset Index and Column Index, then use those
   real indexes to validate flat Column Index fidelity and size.
5. Report independently serialized size tiers instead of comparing a bare Thrift footer with a
   modular footer that contains page-index payloads:
   - **Thrift footer:** literal `FileMetaData`, including page-index pointers but excluding the
     external index blobs;
   - **Thrift full:** the Thrift footer plus its external Offset Index and Column Index blobs;
   - **modular core:** framing, file metadata, schema, and placement;
   - **modular core + stats:** core plus row-group statistics for current modular/PAR1, or
     row-range statistics for PAR3;
   - **modular full:** current modular adds Offset Index and Column Index; PAR1 adds flat Offset
     Index and Column Index; PAR3 is identical to core + stats because placement subsumes Offset
     Index and row-range statistics subsume Column Index.
   Serialize each modular tier independently so its directory, region map, root, and other framing
   are measured exactly. Compare core + stats with the Thrift footer only where they carry the same
   coarse statistics; compare full with Thrift full. PAR3's page-granular row-range statistics make
   its combined core + stats/full tier comparable to Thrift full.
6. Report decode timings as two full-metadata workloads for current Thrift, current modular, PAR1
   addendum, and PAR3:
   - page placement (external Offset Index for current Thrift/current modular, intrinsic page
     placement for page-first);
   - page placement plus page statistics (Column Index plus row-group statistics where separate,
     or PAR3 row-range statistics).
7. Regenerate size tables and machine-readable benchmark outputs for both page-first variants.
8. Update the design notes to present the addendum as the non-breaking modular-footer transition
   and the full definition as the PAR3 breaking change. Do not push until explicitly requested.

## Completion record

- Added `PageFirstModularFooterAddendum.thrift` and `PageFirstModularFooterFull.thrift`; removed the
  superseded single schema.
- Added `--format addendum|full` conversion, real page-index parsing, required PAR1 row-group
  arrays, flat PAR1 Column Index, and PAR3 row-range statistics with per-column boundary order.
- Generated the corpus with Raincloud's actual parquet-java 1.17.1 sidecar target. Every Offset
  Index is present; Column Index coverage is 46/46, 88/99, 28/28, and 20/20 chunks respectively.
- Verified that all page-index ranges are non-overlapping and form one contiguous span immediately
  before the Thrift footer in every benchmark file.
- Added independently serialized core, core + stats, and full tiers to both modular converters.
- Recorded sizes in `real-footer-size/page-first-split-size.csv` and decode sweeps in
  `footer-decode-bench/results/*-split-{placement,statistics}.csv`.
- The exact commands and summary tables are in `PageFirstModularFooter.md` and the two benchmark
  READMEs.
