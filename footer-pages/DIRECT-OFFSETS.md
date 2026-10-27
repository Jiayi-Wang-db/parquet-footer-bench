# Direct offsets, a dictionary table, and shared explicit indexes

The chosen draft is [ExplicitIndexFooter.thrift](ExplicitIndexFooter.thrift), local experiment V38. It retains V35's direct data extents, dictionary table, and statistics tags, and omits the fixed leading zero from both hierarchy indexes. [PROPOSAL.md](PROPOSAL.md) describes the complete format for review. The measurements below use the archived [V35 definition](legacy/ExplicitIndexFooterV35.thrift).

| Field | Type | Values | Nullability |
|---|---|---:|---|
| `DataLayout.column_chunk_index` | `list<i32>` | `C` | Non-null |
| `DataLayout.page_index` | `list<i32>` | `K` | Non-null; stream optional |
| `ColumnChunkMetadata.data_offsets` | `list<i64>` | `K` | Non-null |
| `ColumnChunkMetadata.data_lengths` | `list<i64>` | `K` | Non-null |
| `ColumnChunkMetadata.dictionary_ids` | `list<i32>` | `K` | Nullable values |
| `DataLocation.dictionary_offsets` | `list<i64>` | `D` | Non-null |
| `DataLocation.dictionary_lengths` | `list<i64>` | `D` | Non-null |
| `DataLocation.relative_page_offsets` | `list<i64>` | `P` | Non-null; stream optional |

Here `C`, `K`, `P`, and `D` count columns, chunks, data pages, and distinct referenced dictionaries. Lists are encoded page streams, not inline Thrift lists. The two hierarchy indexes store cumulative ends: range `i` starts at zero for `i=0`, otherwise at `index[i-1]`, and ends at `index[i]`. Repeated ends preserve empty columns/chunks, including actual zero ends. Every chunk field uses the same `K` positions; every page field uses the same `P` positions. Page offsets remain relative to the chunk's data offset, including the first zero. No P−1 representation is introduced.

A dictionary can be reused by multiple chunks of the same physical column, with compatible decoding. Cross-column sharing is not permitted. The table stores each dictionary offset/length once; a null chunk ID means no dictionary. Table order is a writer choice; first-reference order was smaller than file-offset order in the measured corpus.

Column sets are an [optional future extension](PROPOSAL.md#optional-future-extension-column-sets) for exceptionally wide files, outside the chosen base schema. The [V37/CST4 prototype](ColumnSetFooter.thrift) explores groups of complete columns with local indexes and dictionary IDs. Each stream has an independent absolute location; location and statistics leaves can be fetched separately without a contiguous set-level block. Keeping complete columns together avoids cross-set dictionary references or duplication. The prototype's contiguous-range membership is not a settled part of the proposal.

Optional row-group metadata supplies row counts and, where applicable, absolute byte boundaries. Its byte-boundary stream can be omitted for layouts that do not provide contiguous row-group extents. This does not constrain chunk placement or dictionary reuse.

## Core size without Zstd

Complete V35 core bytes, including schema, directories, metadata page headers, null levels, and trailer. V35 stored the leading zero in both hierarchy indexes; these numbers have not been remeasured for V38. Integer encodings remain enabled. Direct variants allow metadata pages to end at column boundaries, choosing the original stream encoding if smaller. These are whole-file core sizes, not column-set measurements.

| Dataset | Original modular core | Region-map core | Direct + dictionary table | vs modular |
|---|---:|---:|---:|---:|
| UCI Adult | 728 | 922 | 1,061 | +45.7% |
| Household Power | 540 | 776 | 912 | +68.9% |
| Kepler Search | 5,850 | 4,785 | 5,405 | −7.6% |
| AMPds | 520 | 743 | 873 | +67.9% |
| ClickBench | 151,028 | 97,828 | 119,000 | −21.2% |
| BI-USCensus | 226,675 | 151,810 | 185,889 | −18.0% |
| BI-Wins | 106,014 | 69,434 | 88,590 | −16.4% |
| Kepler Time Series | 91,407 | 55,787 | 60,084 | −34.3% |
| **Total** | **582,762** | **382,085** | **461,814** | **−20.8%** |

The selected design removes data-region indirection and retains dictionary reuse. Its total core size is 20.9% larger than the region-map baseline. The eight real files have 23,546 dictionary references to 23,546 distinct locations, so these size results contain no savings from dictionary reuse. The region control is V21; V31 differs only by an additional 35 bytes in USCensus's old dictionary-ordinal stream, which is replaced here.

## Validation and historical comparisons

The V35/V36 and archived V31/V34 schemas passed parsing checks when the direct-offset draft was introduced. Thirty-two V31 artifacts (eight datasets, four scopes) were converted to V35 and read through its now-archived Thrift definition. Decoded data extents and dictionary starts match the source oracle. Hierarchy indexes, relative page offsets, row metadata and statistics streams are preserved byte-for-byte except for the row-group byte boundaries converted from ordinals to absolute offsets. Chunk/page statistics still omit separate indexes and row ranges. The eight V35 core sizes match the measured direct-table candidates exactly.

Three historical V36 fixtures validate independent location blocks with same-column dictionary reuse, irregular and empty columns, an empty chunk, absent dictionaries, and an empty table. Splitting all columns into individual sets preserves dictionary extents without duplicating entries. Negative checks reject cross-column dictionary references, invalid dictionary IDs, incompatible codecs for a shared dictionary, and a nonzero first page offset. The V36 block definition is preserved in [ColumnSetFooterV36.thrift](legacy/ColumnSetFooterV36.thrift).

The local reproduction script is `work/direct-explicit-20261026/validate.py`; it uses the existing external encoding harness and saved `work/refined-hierarchy` and `work/direct-offset-dictionary-table-20261026` inputs. Its `results.json` records the generated V35 artifact sizes and hashes. The earlier dictionary-table size audit remains in `work/direct-offset-dictionary-table-20261026`; its broader sharing fixtures predate the same-column constraint. The new validation enforces that constraint.

The V37 logical-set check, `work/logical-column-sets-20261027/validate.py`, reuses that local harness and reads real encoded page streams through absolute `StreamLocation` descriptors. Ten files deliberately interleave streams across sets, place statistics before locations, and insert gaps between streams. The 414 guarded stream reads cover same-column dictionary reuse, unequal and empty columns, absent dictionaries/statistics, a zero-column file, and COLUMN_CHUNK/PAGE/ARBITRARY statistics. Location reads fetch no statistics payloads; later statistics reads request individual leaves and reuse the set's indexes. Four negative cases reject invalid column partitions, out-of-bounds stream extents, overlapping streams, and the old block-layout version. These are correctness checks, not new size or speed measurements.

V35/V36 change field meanings and must not be decoded as V31/V34. V37 inlines set descriptors and replaces relative stream locations with absolute locations, so it must not be decoded as V36. V36/V37 remain pinned to V35's leading-zero indexes; V38's cumulative-end indexes require the revised base schema. Historical generators use [ExplicitIndexFooterV31.thrift](legacy/ExplicitIndexFooterV31.thrift) and [ColumnSetFooterV34.thrift](legacy/ColumnSetFooterV34.thrift). Existing native speed measurements apply to those historical region-map formats. No V35/V36/V37/V38 decoding-speed or storage-I/O benchmark has been run; encryption remains a schema draft.
