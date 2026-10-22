# Modular Footer V2 playground

This playground compares three representations of the same Java-written Parquet file's metadata: the current Parquet footer, the modular footer, and Modular Footer V2. It measures complete serialized metadata size and the CPU time to access metadata for selected columns.

Start with UCI Adult to check the source-data pipeline. Use Kepler Time Series to explore wide schemas, BI-Wins and BI-USCensus for wide schemas with multiple row groups, and ClickBench for many data pages. Source generation uses Raincloud's explicit **`parquet@java`** exporter. Metadata decoding benchmarks use **uncompressed metadata**; compressed sizes are a separate comparison.

This repository provides the V2 definition, a [layout visualization](data-location-visualization.html), and baseline benchmark tools. The Raincloud instructions below produce fresh Java-written inputs. The optimized V2 benchmark harness is not packaged in this repository; its codec requirements and measurement protocol are specified below.

## 1. What we have defined

The authoritative definition is [ModularFooterV2.thrift](ModularFooterV2.thrift), version **21**.

V2 stores typed metadata arrays in ordinary Parquet page streams. Each stream contains an optional `DICTIONARY_PAGE`, followed by `DATA_PAGE_V2` pages. There is **no specialized footer page header**. A field's ID defines its physical type and nullability, so we do not serialize a separate `SchemaElement` tree for the metadata arrays. The user's source-data schema is still stored once in `FileInfo`.

`StreamLocation` stores a stream's byte offset, byte length, and compression codec. Length includes page headers, bodies, and any protection framing. A stream can contain multiple metadata pages; these are different from the source file's data pages. Standard V2 page headers carry their own value/null counts and encoding IDs. Nullable scalar streams use definition levels.

In field comments, `INT32, required` means **the values inside the stream are non-null**. Thrift's `optional StreamLocation` separately means the entire stream may be absent.

| Structure | Information |
|---|---|
| `FileInfo` | Physical leaf-column count, row count, source schema, optional writer information. |
| `DataLayout` | Column-to-chunk index and optional chunk-to-page index into flattened metadata arrays. |
| `DataLocation` | Region byte boundaries, one data-region ID per chunk, optional relative page offsets. |
| `RowGroupMetadata` | Row counts and region boundaries for the original row-group layout. |
| `ColumnChunkMetadata` | Codecs, dictionary-region IDs, fully-dictionary flags, optional total uncompressed sizes, and value counts including nulls. Uncompressed sizes include dictionary pages and headers. |
| `PageMetadata` | Absolute first-row indexes for every source data page. |
| `ColumnIndexedRangeStatistics` | Column-to-range index, `RangeStatistics`, and ordering per column for one granularity. |
| `RangeStatistics` | Range row starts, null/NaN flags, optional distinct counts, paired typed min/max bounds, and paired exactness flags. |

### Column → chunk → page → bytes

Let C be physical columns, K column chunks, P data pages, and R regions. Metadata is flattened in column order, then chunk order, then logical page order.

| Stream | Type and length | Meaning |
|---|---|---|
| `column_chunk_index` | INT32, C+1 | Column c owns chunks `[index[c], index[c+1])`; first entry 0, last K. |
| `page_index` | INT32, K+1 | Chunk cc owns pages `[index[cc], index[cc+1])`; first entry 0, last P. Optional unless page offsets or page-row metadata is present. |
| `region_offsets` | INT64, R+1 | Region r occupies byte interval `[offsets[r], offsets[r+1])`. |
| `region_ordinals` | INT32, K | One data-region ID per column chunk, not one per column or page. |
| `relative_page_offsets` | INT64, P | Each page header's offset relative to its chunk's data-region start. The first offset in each chunk is zero. |

One data region contains a chunk's contiguous complete data pages. Dictionary pages occupy separate regions; unreferenced regions can hold arbitrary bytes. There is no need to classify those other regions to decode the footer. Index values are **array positions**, while region/page offsets are **byte positions**. K and P must fit the definition's signed INT32 limit.

For column c, local chunk j, local page q:

```text
cc = column_chunk_index[c] + j
p  = page_index[cc] + q
r  = region_ordinals[cc]
start = region_offsets[r] + relative_page_offsets[p]
end   = region_offsets[r] + relative_page_offsets[p+1]  if another page follows in cc
        region_offsets[r+1]                            otherwise
```

The index differences give chunk/page counts; separate count arrays are unnecessary. For example, `column_chunk_index = [0,2,3]` means column 0 has two chunks and column 1 has one. `page_index = [0,3,4,6]` means those chunks have three, one, and two pages. Finding a page containing a known row also needs page-row positions; byte locations alone do not answer that question.

### Statistics and encryption

Each statistics-list entry describes one independent granularity. The main experiment uses row-group ranges, then adds page ranges in the most complete scenario. The format permits other boundaries and more than two granularities. Within one granularity, a column's ranges partition its rows. Equal adjacent `column_range_starts` entries mean the column has no statistics in that granularity and contributes no per-range values. Missing statistics mean unknown.

Bounds are `[min_0, max_0, min_1, max_1, ...]`, in separate nullable streams by physical type. A range contributes nulls to other physical-type streams. Null-presence, NaN-presence, and all-null flags support pruning; they do not retain exact null counts for aggregate queries. Histograms and column-order descriptors are absent from V2. Boundary order belongs to each column within each granularity.

The plaintext layout is:

```text
[source data][metadata page streams][Compact-Thrift FileMetaData][u64 LE root offset][PAR3]
```

The root is a small directory, not another page stream. Its trailer offset locates it without scanning the streams. Shared stream offsets are absolute file offsets. Saved benchmark artifacts contain only the replacement metadata tail, so their recorded source-data base is needed to translate absolute offsets into positions in an artifact.

The definition also supports plaintext/encrypted `FileInfo` alternatives, independently protected shared streams using the footer key, and separate column-key statistics modules. Protected modes retain `PAR3` and authenticate the visible directory. **The size and decoding comparisons use plaintext metadata.** Production encryption module-ID allocation and reader integration remain separate work; the protection contract is described in the Thrift definition.

## 2. The four cumulative comparison levels

Generate a separate complete artifact for each level and format. Do not measure a full artifact and merely skip requesting its statistics when reporting a core size.

| Level | Script ID | Name | Contents | V2 additions |
|---|---:|---|---|---|
| 1 | 0 | **Core** | Schema, row/column counts, row-group/chunk descriptors and chunk placement. No statistics or page indexes. | `FileInfo`, chunk index, region map, row-group and chunk metadata. `page_index` absent. |
| 2 | 1 | **Core + row-group stats** | Level 1 plus row-group min/max and supported pruning information. | Row-group-granularity range statistics. `page_index` still absent. |
| 3 | 2 | **Core + stats + offset index** | Level 2 plus each data page's byte offset, length, and first-row index. | `page_index`, `relative_page_offsets`, and `PageMetadata`. Page length follows from boundaries. |
| 4 | 3 | **Core + stats + offset index + page stats** | Level 3 plus available page-level min/max and pruning information. | A second statistics granularity, retaining the row-group ranges. |

In standard Parquet, “page index” can mean both `OffsetIndex` and `ColumnIndex`. In our fourth level, “add page index” specifically means **add `ColumnIndex` / page statistics**, since `OffsetIndex` was added at level 3. `DataLayout.page_index` is a third, distinct term: it is V2's chunk-to-page array-position index.

The supplementary **core + page offsets, without any statistics** comparison is size-only and is not one of these four cumulative levels.

### What counts toward size

Count the complete replacement metadata tail: source schema, structural indexes, root/directories, dictionaries, definition levels, every metadata page header and body, and the format's trailer. Include external current-format `OffsetIndex` and `ColumnIndex` bytes at the corresponding levels. Exclude source data-page bytes. Report exact bytes; KiB means 1,024 bytes.

“Current core” removes optional statistics fields entirely; it does not leave empty statistics objects. All three formats start from the same fresh Java file, retain the same source schema, and preserve source bounds without additional truncation. Source-writer truncation, if any, is preserved. A column lacking page statistics must not acquire fabricated page bounds.

| Format | Representation in the matched experiment |
|---|---|
| Current | Standard Compact-Thrift metadata, reserialized for each scope; required external page-index payloads included. |
| Modular | Schema/placement/statistics/index modules, packed arrays and shared min/max prefixes, choosing the smaller valid dense-bitmap or sparse-present-index representation per array. |
| Modular Footer V2 | Version-21 typed page streams, with encodings/page capacities selected by measured stored size. Report uncompressed and selective-Zstd variants separately. |

Current/modular retain source column-order descriptors and exact null counts; V2 retains equivalent pruning flags. The comparison covers placement and pruning capability. Descriptive key/value metadata, `created_by`, histograms, and optional unencoded BYTE_ARRAY byte counts are excluded from the matched scopes. Record untouched Java footer totals separately. Exact aggregate-count capability is outside this comparison.

## 3. Tested datasets

### Wide schemas and multiple row groups

These are complete Raincloud recipe outputs, with one fresh Java Parquet file per dataset. Counts below describe the generated files, not downloaded upstream Parquet files.

| Raincloud slug | Rows | Physical columns | Row groups | Data pages | Java file bytes |
|---|---:|---:|---:|---:|---:|
| `kepler-labelled-time-series-data` | 5,087 | 3,198 | 1 | 3,198 | 74,087,086 |
| `bi-wins` | 2,115,449 | 648 | 8 | 69,336 | 589,769,626 |
| `bi-uscensus` | 9,398,385 | 519 | 23 | 244,968 | 2,354,846,136 |
| `clickbench-hits` | 99,997,497 | 105 | 70 | 542,277 | 9,226,506,093 |

Kepler Time Series deliberately consumes `exoTrain.csv`, as specified by its original Raincloud recipe. It is different from the 153-column Kepler dataset below. ClickBench's upstream is [hits.parquet](https://datasets.clickhouse.com/hits_compatible/hits.parquet); Raincloud reads it into canonical Arrow and rewrites it with Java before measurement.

All four canonical-to-Java comparisons passed over every row and column. BI-Wins's Java self-check reported an unsupported seconds-versus-milliseconds time comparison; the independent streaming check passed with reversible logical-type normalization. The original self-check result remains in provenance.

### Smaller and medium-width datasets

| Raincloud slug | Rows | Physical columns | Row groups | Data pages | Java file bytes |
|---|---:|---:|---:|---:|---:|
| `uci-adult` | 48,842 | 15 | 1 | 45 | 373,085 |
| `uci-individual-household-electric-power-consumption` | 2,075,259 | 9 | 1 | 936 | 9,443,498 |
| `ampds-whe` | 1,051,200 | 12 | 1 | 636 | 17,467,853 |
| `kepler-exoplanet-search-results` | 9,564 | 153 | 1 | 153 | 2,962,455 |

These also use complete recipe outputs.

### Pinned public shards

These are all rows of **pinned input shards**, not the entire upstream US Accidents, FineWeb, or Hacker News collections. Each shard was freshly rewritten with Java using normal Raincloud row-group settings.

| Benchmark input name | Raincloud recipe | Rows | Columns | Java row groups | Data pages |
|---|---|---:|---:|---:|---:|
| `us-accidents-00004-of-00007` | `us-accidents` | 1,104,056 | 46 | 1 | 2,620 |
| `fineweb-10bt-000` | `fineweb-sample-10bt` | 1,048,581 | 9 | 11 | 3,513 |
| `hacker-news-00000-of-00039` | `hacker-news` | 1,072,139 | 14 | 2 | 1,022 |
| `yellow-tripdata-2025-01` | `yellow_tripdata_2025` | 3,475,226 | 20 | 1 | 3,480 |

Source URLs and hashes are in [corpus.json](../real-footer-size/corpus.json). FineWeb's text column has no page statistics. Keep normal Raincloud row-group settings when reproducing this table.

## 4. Fetch and write fresh data with Raincloud Java

Use Linux, Git, `uv`, Python 3.11 or newer, and JDK 17. Commands below use Bash and create sibling checkouts. Start in a new directory and a shell without inherited experimental `RAINCLOUD_*` settings. Keep raw downloads, canonical Arrow, scratch space, and Java outputs; storage needs exceed the final Parquet sizes. Start small before fetching ClickBench.

```bash
set -euo pipefail
export FOOTER_PLAYGROUND_ROOT="$PWD/footer-playground"
mkdir -p "$FOOTER_PLAYGROUND_ROOT"
cd "$FOOTER_PLAYGROUND_ROOT"
git clone https://github.com/Jiayi-Wang-db/parquet-footer-bench.git
git clone https://github.com/spiraldb/raincloud.git
export FOOTER_BENCH="$FOOTER_PLAYGROUND_ROOT/parquet-footer-bench"
export FOOTER_RAINCLOUD="$FOOTER_PLAYGROUND_ROOT/raincloud"

# Raincloud revision used by the recorded campaigns.
git -C "$FOOTER_RAINCLOUD" checkout 73575174111e3f173b492681d7decb12ae3c4e8c
cd "$FOOTER_RAINCLOUD"
uv sync --extra build --inexact
git submodule update --init sidecars/java/parquet-arrow-java

# Adjust this path for your JDK 17 installation.
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
bash sidecars/java/gradlew -p sidecars/java :parquet-java:installDist
export RAINCLOUD_SIDECAR_PARQUET_JAVA="$FOOTER_RAINCLOUD/sidecars/java/parquet-java/build/install/raincloud-export-parquet-java/bin/raincloud-export-parquet-java"
export JAVA_OPTS='-Xmx8g -XX:MaxDirectMemorySize=8g'
test -x "$RAINCLOUD_SIDECAR_PARQUET_JAVA"
export FOOTER_PYTHON="$FOOTER_RAINCLOUD/.venv/bin/python"
```

Use `--inexact` on later `uv sync` commands as well, so separately installed benchmark dependencies are retained. Source-specific extras, such as `--extra kaggle`, may be needed. The recorded Java writer is parquet-java **1.17.1**; its Arrow adapter submodule is pinned by Raincloud.

Create a new output directory and an isolated manifest. This copies the selected recipes and changes only the export writer, preserving transformations and row-group settings:

```bash
mkdir -p "$FOOTER_BENCH/footer-pages/work"
export FOOTER_RUN_DIR
FOOTER_RUN_DIR=$(mktemp -d "$FOOTER_BENCH/footer-pages/work/playground-XXXXXXXX")
export FOOTER_DATASETS='uci-adult'
# For the wide campaign instead:
# export FOOTER_DATASETS='kepler-labelled-time-series-data bi-wins bi-uscensus clickbench-hits'

export RAINCLOUD_NO_CONFIG=1
export RAINCLOUD_CATALOG=local
export RAINCLOUD_MANIFEST="$FOOTER_RUN_DIR/sources.json"
export RAINCLOUD_OUTPUTS="$FOOTER_RUN_DIR/raincloud"
export RAINCLOUD_WORKDIR="$FOOTER_RUN_DIR/scratch"
export RAINCLOUD_KEEP_RAW=1
export RAINCLOUD_KEEP_CANONICAL=1

"$FOOTER_PYTHON" - <<'PY'
import copy, json, os
from pathlib import Path
source = json.loads((Path(os.environ['FOOTER_RAINCLOUD']) / 'sources.json').read_text())
wanted = os.environ['FOOTER_DATASETS'].split()
recipes = {d['slug']: d for d in source['datasets']}
manifest = {k: copy.deepcopy(v) for k, v in source.items() if k != 'datasets'}
manifest['datasets'] = [copy.deepcopy(recipes[name]) for name in wanted]
for recipe in manifest['datasets']:
    recipe['export'] = {'formats': ['parquet'], 'priority': {'parquet': ['java']}}
Path(os.environ['RAINCLOUD_MANIFEST']).write_text(json.dumps(manifest, indent=2) + '\n')
PY

cd "$FOOTER_RAINCLOUD"
"$FOOTER_PYTHON" -m raincloud.pipeline.validate_manifest
for slug in $FOOTER_DATASETS; do
  "$FOOTER_PYTHON" -m raincloud.pipeline.build "$slug" --format arrow --strict
  "$FOOTER_PYTHON" -m raincloud.pipeline.export "$slug" --format parquet@java
done
```

`--format parquet@java` is essential: plain `parquet` can choose the default Python/PyArrow writer. An upstream Parquet download is an input, not the measured Java output. The resulting paths are:

```text
$RAINCLOUD_OUTPUTS/v2/<slug>/arrow/<slug>.arrow.zstd
$RAINCLOUD_OUTPUTS/v2/<slug>/parquet/<slug>.parquet
$RAINCLOUD_OUTPUTS/builds.json
```

Raincloud writes **one file per dataset per format**. Row-group splitting happens inside that file; it does not create multiple files. Normal settings use a 128 MiB encoded-byte row-group target and the recipe's row limit, 10 million rows for these datasets. Java's byte accounting includes compressed closed pages and encoded open pages, so neither physical file size nor uncompressed bytes divided by 128 MiB predicts the exact row-group count. Keep those defaults for the matched campaign. If testing other settings, record a separate experiment.

For parallel source fetching/conversion, give each worker its own manifest, output store, and scratch directory. A shared store has locking and may serialize work. Merge provenance afterward; do not mix timings with concurrent conversion jobs.

**Kepler Time Series:** the recipe uses Kaggle and selects only `exoTrain.csv`. Install its extra with `uv sync --extra build --extra kaggle --inexact` and configure Kaggle access. The measured run used the same public ZIP from `https://www.kaggle.com/api/v1/datasets/download/keplersmachines/kepler-labelled-time-series-data`, staged with Raincloud's `fetch_url` under a filename ending in `.zip`, then referenced by an isolated HTTP/file recipe. Preserve the original parsing, transformations, and fetch receipts if using this transport.

**Pinned public shards:** the repository includes a downloader that verifies their hashes:

```bash
cd "$FOOTER_BENCH"
"$FOOTER_PYTHON" real-footer-size/download.py
```

Downloads land in `real-footer-size/data/<benchmark-input-name>.parquet`. To reproduce the shard table, create a separate Raincloud manifest from `templates/minimal_spec.json`: use the benchmark input name as its slug, the downloaded file's URI and pinned hash as the fetch source, `parse.reader = "parquet"`, and `transform.handler = "identity"`. Copy `license` and `write` from the corresponding catalog recipe, and set `export.priority.parquet = ["java"]`. Run the same canonical Arrow build and `parquet@java` export commands. The measured input is the newly generated Java file. Selecting the full catalog recipe instead may fetch more data than these pinned shards.

### Verify writer and full roundtrip

Check the build record and `created_by`, hash the file, and compare every row/column to canonical Arrow. A successful export process alone is insufficient: the writer's self-check can report an unsupported comparison. The streaming comparator handles reversible logical-type normalization without loading the whole dataset at once.

```bash
cd "$FOOTER_RAINCLOUD"
"$FOOTER_PYTHON" - <<'PY'
import hashlib, json, os, subprocess
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from raincloud.pipeline.export.compare import stream_equal

def sha256(path):
    with path.open('rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()

store = Path(os.environ['RAINCLOUD_OUTPUTS'])
builds = json.loads((store / 'builds.json').read_text())['artifacts']
records = []
for slug in os.environ['FOOTER_DATASETS'].split():
    key = f'v2/{slug}/parquet/{slug}.parquet'
    path = store / key
    canonical = store / f'v2/{slug}/arrow/{slug}.arrow.zstd'
    record = builds[key]
    parquet = pq.ParquetFile(path)
    md = parquet.metadata
    assert record['writer'] == 'java', record
    assert md.created_by.startswith('parquet-mr version 1.17.1'), md.created_by
    digest = sha256(path)
    assert digest == record['sha256']
    with pa.memory_map(str(canonical), 'r') as mapped:
        arrow = pa.ipc.open_file(mapped)
        ok, reason = stream_equal(parquet.schema_arrow, parquet.iter_batches(batch_size=8192),
                                  arrow.schema, (arrow.get_batch(i) for i in range(arrow.num_record_batches)))
        assert ok, (slug, reason)
    item = dict(dataset=slug, name=slug, path=str(path), sha256=digest,
                bytes=path.stat().st_size, rows=md.num_rows, columns=md.num_columns,
                row_groups=md.num_row_groups, created_by=md.created_by,
                canonical_sha256=sha256(canonical), roundtrip='passed', build_record=record)
    records.append(item)
    print(slug, item['rows'], item['columns'], item['row_groups'], item['bytes'], 'verified')
provenance = dict(datasets=records, writer='parquet@java', fresh=True,
    manifest_sha256=sha256(Path(os.environ['RAINCLOUD_MANIFEST'])),
    raincloud_commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip())
Path(os.environ['FOOTER_RUN_DIR'], 'source-provenance.json').write_text(json.dumps(provenance, indent=2) + '\n')
PY
```

Keep the manifest, `builds.json`, source-provenance record, raw fetch receipts, and canonical files together. Page counts are obtained later by the metadata/page scan, not from file size.

## 5. Build and run the benchmarks

### Public baseline tools

From the bench checkout, the existing C++17 targets need no third-party libraries:

```bash
cd "$FOOTER_BENCH"
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j2
ctest --test-dir build --output-on-failure

export FOOTER_INPUT="$RAINCLOUD_OUTPUTS/v2/uci-adult/parquet/uci-adult.parquet"
./build/modular_footer_convert "$FOOTER_INPUT" "$FOOTER_RUN_DIR/adult.modular"
./build/modular_footer_convert --page-index "$FOOTER_INPUT" "$FOOTER_RUN_DIR/adult.with-index.modular"
./build/jumptable_footer_convert "$FOOTER_INPUT" "$FOOTER_RUN_DIR/adult.jt.parquet"
./build/footer_decode_bench "$FOOTER_RUN_DIR/adult.jt.parquet" --sweep "$FOOTER_RUN_DIR/adult.modular" > "$FOOTER_RUN_DIR/baseline-sweep.csv"
```

These commands provide a current/jump-table/modular playground. The converter includes row-group statistics by default. Its `--page-index` option adds both OffsetIndex and ColumnIndex; it does not produce all four matched scopes individually. `footer_decode_bench` has its own reader/workload, described in [its README](../footer-decode-bench/README.md). Label its output separately from the V2 protocol below. The matched modular baseline additionally chooses the smaller dense/sparse representation per array.

### Optimized V2 benchmark requirements

Reproducing the optimized V2 measurements requires the separate experiment harness and Runtime codec sources. That harness uses C++20 release readers, Arrow/Parquet libraries from PyArrow, optimized Runtime `dbl/parquet` encodings, Runtime PFOR, and DBL floating-point codecs. Rust is used for the BSS adapter. Python also needs `zstandard` and `thriftpy2`. Building the repository's CMake targets does not build this harness.

The recorded codec sources are pinned independently:

| Component | Recorded revision |
|---|---|
| Raincloud | `73575174111e3f173b492681d7decb12ae3c4e8c` |
| parquet-arrow-java adapter | `9f98247cf2129dcd71e8e755c04b330cf7ed62d7` |
| DBL integer encoding sweep, including PFOR encoder | `ce6aeb66fdfb1362888367c2535bc85ef7c7b7ae` |
| Updated PFOR decoder, [PR #267100](https://github.com/databricks-eng/runtime/pull/267100) | `f9219aef669a0b71dae56678e3e410f792c858e7` |
| DBL floating-point codecs | `e8481f067b42f97e6e978c4435642daa0f742811` |

The recorded native linkage uses PyArrow 26.0.0 (`libarrow.so.2600` / `libparquet.so.2600`). If changing versions or machines, regenerate compiler/link paths and record the new build rather than reusing a binary compiled with `-march=native` elsewhere.

An equivalent harness needs a source-page scanner and logical metadata oracle, serializers for all three formats and four scopes, encoding adapters, and native readers that produce the same selected-column output. Validate page locations and statistics against the Java source before timing. Build and validate first; run timings without competing conversion or compilation jobs. Record the exact codec source hashes and compiler arguments alongside the results.

## 6. Encodings and decoding protocol

Encoding selection searches actual serialized sizes across metadata-page capacities of **1,024, 4,096, 16,384, and 65,536 scalars**. Candidates include PLAIN, dictionary, DBL DELTA_BINARY_PACKED geometries, PFOR auto/frame, BYTE_STREAM_SPLIT, ALP for floating-point bounds, and byte-array delta encodings where applicable. BOOLEAN PLAIN is already bit-packed. Nullable levels use RLE/bit-packing.

“Best encoding” here means **smallest among the tested candidates**, not a proof of optimal size or fastest decoding. Uncompressed and compressed variants choose independently. Zstd is retained only when it saves **at least 256 bytes and at least 10%** of the encoded payload. PFOR and ALP use experimental encoding contracts/IDs; ordinary page framing alone does not make them readable by an unmodified Parquet implementation.

Forced-PFOR controls change only `column_chunk_index` and `region_offsets`, preserving other streams. The updated PFOR decoder supports selective access; the planned reader uses selective access for partial projection and bulk-decodes each needed stream once for full projection. Bulk results are reused only within that operation.

For the main matched timing workload:

1. Use resident, **uncompressed** serialized metadata for all layouts, including dictionary bodies. Verify page flags/codecs and runtime zero-decompression counters. Source data files may still use Zstd.
2. Start each operation with a fresh root/schema reader. Include structural-index access, selected metadata decoding, consumer checksumming, and reader destruction. No parsed/decoded cache survives an operation.
3. Always include explicit **one-column** and **all-column** cases for every dataset, scope, and format. Sweep one-column reads across every physical leaf-column ordinal and retain a separate timing for each position. Also test 10%, 20%, …, 90%, rounding up to whole columns. Record actual column counts and selected ordinals; a percentage that rounds up to all columns does not replace the explicitly labeled all-column case.
4. Consume the same placement/statistics/page information for each projected column in the selected scope. Validate all readers against the Java-source oracle before timing.
5. Use three separate CPU-pinned processes per suite, 20 ms calibration per configuration, and nine randomized interleaved trials targeting 5 ms each. Report the median of the three process medians and retain every trial. A single operation/window can exceed the target duration on large inputs.

These are **metadata access timings**, not isolated deserialization timings or end-to-end SQL query times. Selected-column extraction can make current Thrift's line slope even though it parses the whole footer. Column names have already been resolved to physical ordinals. Storage I/O, source data-page decoding, predicate evaluation, and query execution are excluded. Smaller bytes do not imply lower decoding CPU time.

### Required column-position tests

The selected columns' physical positions can affect schema traversal, shared-index decoding, and how many encoded blocks or metadata pages a reader visits. A rotating-position average alone cannot show this effect. Apply the following matrix to all four metadata scopes and all three footer formats, using the same selected ordinals for each format:

| Case | Selected columns | Report |
|---|---|---|
| One column, position sweep | `{c}` for every ordinal `c` from 0 to C−1. Each position is a separate timing configuration. | Latency by ordinal, with first (`0`), middle (`floor(C/2)`), and last (`C−1`) highlighted. Retain every position's raw trials. |
| All columns | Every ordinal from 0 to C−1 in one operation. | An explicit all-column result; V2 uses bulk decoding for each needed stream. |
| Partial, contiguous | For each 10%–90% projection, take k adjacent columns starting at `0`, `floor((C−k)/2)`, and `C−k`. | Separate first, middle, and last window timings at the same selected-column count. |
| Partial, scattered | For each 10%–90% projection, use a fixed seeded shuffle and rotating windows, with up to 32 distinct sets. | Per-set timings and an aggregate distribution, retaining the seed and exact ordinals. |

Deduplicate identical sets when rounding or a narrow schema makes cases coincide, but retain their labels. For C=1, the one-column and all-column labels refer to the same measured selection. Randomize/interleave position cases during timing so ordinal does not track benchmark execution order. Use a fresh reader for every operation, as in the main protocol.

Real columns differ in type, chunk/page count, statistics availability, and encoded size. Save those properties with each position's timing, together with decoded-value/block/page and byte counters where available. Also run a uniform synthetic metadata control with the same type and chunk/page/statistics counts per column; use it to separate position effects from differences in the amount of metadata requested. Include positions on both sides of encoded-block and metadata-page boundaries in the analysis.

Plot one-column latency against physical column ordinal, alongside the separate all-column result. Summaries may report median and worst-position latency, but must not replace the per-position measurements. Position-specific correctness checks alone do not count as position-specific speed measurements.

Useful additional workloads are placement-only access while statistics remain in the artifact; locating a known row's page in every output column; a synthetic 2,000-column × 10-row-group metadata fixture; and dependent-fetch replay with 4 KiB, 64 KiB, and 512 KiB initial reads. Label them separately. Known-row lookup excludes finding the match. Fetch replay measures bytes, requests, and dependent rounds; it does not measure cloud latency or end-to-end query execution.

## 7. Record and extend an experiment

For a new experiment, start from one verified Java source and keep the four scopes fixed. Change one variable: dataset, metadata encoding candidates, page capacity, compression policy, or selective/bulk access policy. Save a new run directory instead of replacing the reference artifacts. If changing information coverage, label the new scope explicitly.

Each run should retain source/manifest hashes and writer records; the Thrift-definition hash; compiler/codec revisions; complete artifact bytes and hashes; per-stream/page encoding choices; source and artifact audits; CPU/environment details; raw trials; and size/decoding CSVs. Preserve an untouched source-footer size reference beside the normalized comparisons. Record correctness validation, sanitizer status, size completion, and timing completion separately.

A useful result table has `dataset`, `scope`, `current bytes`, `modular bytes`, `V2 raw bytes`, and `V2 cutoff-Zstd bytes`. A separate timing table has `dataset`, `scope`, `case`, `selected column count`, `selected ordinals`, `position/window`, and uncompressed microseconds for each layout. Include explicit one-column and all-column results, plus the per-position sweep. Keep single-process results explicitly labeled until the complete timing protocol finishes.
