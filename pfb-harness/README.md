# pfb — footer-layout harness

A small, dependency-free harness for **experimenting with Parquet footer layouts** and
measuring them -- not estimating them. It synthesizes footer shapes in memory (it does
not read real Parquet files -- see `../footer-decode-bench/` for that).

A Parquet reader, before it can fetch any column data, needs the *placement* of every
column chunk it will read: where the chunk starts and how big it is. Today that lives in
the Thrift `FileMetaData` footer. There are many ways one *could* encode placement, and
they trade wire size against how much work the reader does to resolve it. This harness
makes those trade-offs concrete: you define a file shape with a few parameters, and for
each candidate layout it

1. **builds** the real Thrift-compact blob,
2. **parses** it back, and
3. **resolves** it into the exact structure a reader hands to its fetch scheduler -- the
   list of `{offset, size}` for the projected column chunks,

then reports the measured wire size and the build / resolve timings. Every number comes
from a real serialize -> parse -> resolve round-trip; nothing is hand-estimated.

The load-bearing invariant is a **fidelity cross-check**: every layout must resolve to the
*identical* set of chunk locators for the same file. A layout's size and speed are only
ever compared after it is proven to produce the correct answer.

## Build & run

No third-party dependencies (the Thrift Compact codec is vendored). Any C++17 compiler;
run from the repository root (the build is driven by the top-level `CMakeLists.txt`):

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build            # fidelity cross-check
./build/pfb_bench                 # run the built-in shapes, CSV to stdout
```

Custom shape (N columns x R row groups, P data pages per chunk, K columns projected):

```sh
./build/pfb_bench --columns 5000 --row-groups 10 --pages 1 --projected 4
./build/pfb_bench --list          # list registered layouts
```

Output is CSV (`bytes` is the measured wire size; `*_us` are per-call microseconds):

```
shape,columns,row_groups,pages,projected,layout,bytes,build_us,resolve_project_us,resolve_full_us
```

## Layouts included

| Layout | Encoding | Idea |
|---|---|---|
| `soa_flat` | two parallel `list<i64>` (offsets, sizes) | the baseline "placement core": store exactly what the scheduler needs |
| `soa_bitpack` | offsets & sizes each fixed-bit-width packed | smaller than varint *and* O(1) random access -- a projection extracts only its chunks |
| `soa_delta_bitpack` | offset deltas + sizes bit-packed | smallest (deltas are tiny) but no random access: every query prefix-sums the whole array |
| `indexed_struct` | one Thrift struct per chunk + an `i64` byte-offset directory | random-access a projected chunk without decoding the rest |
| `offset_index` | per-page block lengths + rows + each chunk's first-block index | model "make the page index *the* placement core"; placement is *derived* by prefix-sum |
| `placement_plus_pageindex` | bit-packed placement core **+** a separate length-delimited page-index blob | page index is first-class but optional: a placement-only query skips it in O(1) without decoding |

Offsets in these scenarios are real file positions (200 MB - 2.5 GB), so they cost
**5-byte zig-zag varints** in `soa_flat`/`offset_index` and ~30-32 bits when bit-packed; page
lengths are 2-3 bytes. Those costs are in the measured numbers, not estimated -- the harness
serializes real Thrift.

## Example results

From `./build/pfb_bench` (Release, one machine -- your absolute numbers will differ; the
*relationships* are the point). `analytics_8m` is the concrete case: 256 columns x 8 row
groups of 1,000,000 rows (8M rows), 64 pages/chunk, 16 projected.

| shape | layout | bytes | resolve_project (us) | resolve_full (us) |
|---|---|---:|---:|---:|
| analytics_8m (256c, 8M rows, 64pg) | soa_flat | 16 KB | 12.0 | 23.7 |
| | soa_bitpack | 13 KB | 2.8 | 29.5 |
| | soa_delta_bitpack | **10 KB** | 16.0 | 35.7 |
| | indexed_struct | 28 KB | 5.4 | 20.8 |
| | offset_index | 755 KB | 1023 | 1048 |
| | placement_plus_pageindex | 496 KB | **2.7** | 30.1 |
| many_pages (10c x 5rg, 5120pg) | soa_flat | 0.45 KB | 0.48 | 0.66 |
| | soa_bitpack | **0.38 KB** | 0.49 | 0.90 |
| | offset_index | 1450 KB | 1975 | 2022 |
| | placement_plus_pageindex | 961 KB | **0.50** | 0.91 |

What the measurements show:

- **Bit-packing beats varint on both axes for placement.** `soa_bitpack` is smaller than
  `soa_flat` *and* gives O(1) random access, so a selective projection extracts only the
  chunks it needs (analytics: 2.8 us vs 12 us). `soa_delta_bitpack` is the smallest of all
  (offset deltas are tiny) but trades away random access -- every query prefix-sums the
  whole array, so its project time matches its full time.
- **Deriving placement from a page index scales with pages, not chunks.** `offset_index` is
  the largest blob whenever there is more than one page per chunk (up to ~3000x on
  `many_pages`) and its resolve is a full prefix-sum sweep -- O(total blocks), no shortcut
  for a projection. Making the page index *the* placement core makes the common
  placement-only query pay for page detail it doesn't use.
- **You can keep the page index first-class without paying to decode it.**
  `placement_plus_pageindex` carries the entire page index as a separate length-delimited
  section, yet a placement-only query skips it in O(1) (pointer bump from the list header)
  and resolves as fast as `soa_bitpack` -- e.g. `many_pages` 0.5 us vs `offset_index`'s
  1975 us, despite both holding the same page data. A writer could place that section in a
  separate footer byte-range so a query that never needs page skipping pays nothing for it
  at all -- not even the bytes.
- **A directory buys random access at a size cost.** `indexed_struct` resolves a small
  projection fast (it decodes only the selected structs) but is larger (directory +
  per-struct framing).

## Row selection: what the page index buys

The `kRowRange` query reads only the rows in a window within each row group (a
row-selective scan), set with `--row-select FIRST:COUNT` (default: 1000 rows at each
row group's midpoint). A layout that carries a page index (`page_aware`) resolves it to
the dictionary page plus the data pages overlapping the window; a layout without page
info can only fetch whole chunks. The headline metric is `rowsel_bytes_fetched` -- the
column data the query would actually read.

Selecting 1000 rows (`rowsel_bytes_fetched`, whole-chunk vs page-aware):

| shape | pages/chunk | without page index | with page index | reduction |
|---|---:|---:|---:|---:|
| many_pages | 5120 | 1.05 GB | 282 KB | ~3700x |
| analytics_8m | 64 | 84 MB | 1.6 MB | ~52x |
| many_rg | 25 | 1.03 GB | 50.9 MB | ~20x |
| wide_1pg | 1 | 501 KB | 501 KB | **1x (nothing to skip)** |

- The page index pays off in proportion to pages-per-chunk: it lets a selective scan skip
  to the covering pages instead of reading whole chunks (up to ~3700x less I/O here).
- The cost is real and on the footer path: a page-aware layout must *decode* the page
  index for a row range (`rowsel_resolve_us` is ~1-4.5 ms vs microseconds for placement),
  and the index inflates the footer. That trade -- milliseconds of footer decode against
  skipping megabytes-to-gigabytes of column I/O -- is the whole argument for the page
  index, and it only wins when there is more than one page per chunk (contrast `wide_1pg`).
- This is the complement to `placement_plus_pageindex`: a placement-only query skips the
  index for free, and a row-selective query decodes it to skip column data. Same footer,
  two regimes.

## Adding a layout

A layout is a name plus two functions -- `build` (model -> blob) and `resolve` (blob, model,
query -> chunk locators). Append one entry to `Layouts()` in `layouts.cc`:

```cpp
{"my_layout", BuildMine, ResolveMine},
```

The fidelity test and the benchmark pick it up automatically. The test will fail loudly if
`ResolveMine` disagrees with the model, so you get correctness for free before you trust a
number.

**[ADDING_A_LAYOUT.md](ADDING_A_LAYOUT.md)** is a full worked example -- it adds a
real layout (`aos_flat`) end to end: the `Build`/`Resolve` code, the `Model` contract, the
encoding helpers available (`bitpack.h`, raw `list<i8>` sections), and how to tune the
workload.

## Caveats

- Timings are single-threaded, in-process resolve cost; they do **not** include the network
  fetch of the footer bytes (size is the proxy for that).
- The synthetic model uses fixed-seed uniform page/chunk sizes; real files vary. Adjust the
  distributions in `BuildModel` (`layouts.cc`) to match a workload you care about.
- The vendored codec implements the subset of Thrift Compact the harness needs and is
  wire-compatible with it; it is not a general-purpose Thrift library.

## License

Apache License 2.0. See [LICENSE](../LICENSE).
