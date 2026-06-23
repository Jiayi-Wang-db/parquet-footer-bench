# parquet-footer-bench

A small, dependency-free harness for **experimenting with Parquet footer layouts** and
measuring them -- not estimating them.

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

No third-party dependencies (the Thrift Compact codec is vendored). Any C++17 compiler:

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
| `indexed_struct` | one Thrift struct per chunk + an `i64` byte-offset directory | random-access a projected chunk without decoding the rest |
| `offset_index` | per-page block lengths + each chunk's first-block index | model "make the page index the placement core"; placement is *derived* by prefix-sum |

## Example results

From `./build/pfb_bench` on the four built-in shapes (Release, one machine -- your absolute
numbers will differ; the *relationships* are the point):

| shape | layout | bytes | resolve_project (us) | resolve_full (us) |
|---|---|---:|---:|---:|
| wide (5000c x 10rg, 1pg) | soa_flat | 383 KB | 297 | 1067 |
| | indexed_struct | 682 KB | **116** | 602 |
| | offset_index | 379 KB | 578 | 706 |
| many_rg (10c x 1000rg, 25pg) | soa_flat | **79 KB** | 76 | 117 |
| | indexed_struct | 138 KB | 60 | 107 |
| | offset_index | 716 KB | 2595 | 2671 |
| many_pages (10c x 5rg, 5120pg) | soa_flat | **0.45 KB** | 0.48 | 0.66 |
| | indexed_struct | 0.70 KB | 0.44 | 0.67 |
| | offset_index | 682 KB | 2467 | 2563 |

What the measurements show:

- **Deriving placement from a page index scales with pages, not chunks.** `offset_index`
  is the largest blob whenever there is more than one page per chunk (up to ~1500x on
  `many_pages`) and its resolve is a full prefix-sum sweep -- O(total blocks), with no
  shortcut for a selective projection.
- **A varint list has no random access.** `soa_flat` is the smallest, but resolving a
  projection still decodes the whole offset/size lists, so it pays for columns it won't read.
- **A directory buys random access at a size cost.** `indexed_struct` resolves a small
  projection fastest (it decodes only the selected structs) but is ~1.8x larger (directory
  + per-struct framing). For a full scan it converges with `soa_flat`.

## Adding a layout

A layout is a name plus two functions -- `build` (model -> blob) and `resolve` (blob, model,
query -> chunk locators). Append one entry to `Layouts()` in `src/layouts.cc`:

```cpp
{"my_layout", BuildMine, ResolveMine},
```

The fidelity test and the benchmark pick it up automatically. The test will fail loudly if
`ResolveMine` disagrees with the model, so you get correctness for free before you trust a
number.

## Caveats

- Timings are single-threaded, in-process resolve cost; they do **not** include the network
  fetch of the footer bytes (size is the proxy for that).
- The synthetic model uses fixed-seed uniform page/chunk sizes; real files vary. Adjust the
  distributions in `BuildModel` (`src/layouts.cc`) to match a workload you care about.
- The vendored codec implements the subset of Thrift Compact the harness needs and is
  wire-compatible with it; it is not a general-purpose Thrift library.

## License

Apache License 2.0. See [LICENSE](LICENSE).
