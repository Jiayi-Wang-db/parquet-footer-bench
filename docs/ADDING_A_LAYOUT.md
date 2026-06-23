<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Adding a new footer layout

A layout is one entry in the registry: a name plus two functions. Everything
else -- the benchmark, the fidelity test, the CSV output -- picks it up
automatically. There are no base classes to implement and no files to register;
you edit one file, `src/layouts.cc`.

## The contract

A layout implements two halves with these exact signatures (see
`include/pfb/harness.h`):

```cpp
// Serialize the placement information in your encoding. The Model is the ground
// truth for one synthetic file (see below). Return the wire bytes.
std::string Build(const Model& m);

// Parse your bytes back and return the chunk locators the query needs --
// {offset, size} for every column chunk the reader would fetch.
Resolved Resolve(const std::string& blob, const Model& m, Query q);
```

What you get from the `Model` (`harness.h`):

| field | meaning |
|---|---|
| `m.chunks()` | number of column chunks = `columns * row_groups` |
| `m.chunk_off[cc]`, `m.chunk_size[cc]` | the truth your `Build` encodes, indexed `cc = column*row_groups + row_group` |
| `m.block_len[]`, `m.block_rows[]` | per-page byte lengths and row counts (only if you model a page index) |
| `m.selected` | the projected column ids (for `Query::kProject`) |

`Query` is `kProject` (read `m.selected` columns) or `kFull` (read all). Most
layouts share an iteration helper already in the file:

```cpp
ForEachSelected(m, q, [&](int cc) { /* emit locator for chunk cc */ });
```

**You do not validate anything yourself.** The harness asserts that your
`Resolve` returns exactly `m.Expected(q)` for every shape; if your encoding or
your parsing is wrong, the fidelity test fails and names your layout. That is the
point of the tool: you get correctness for free before you trust a size or a
timing.

## Worked example: `aos_flat`

This is a real layout added to the repo. It stores placement array-of-structs --
one interleaved `[off, size, off, size, ...]` list -- instead of `soa_flat`'s two
parallel lists. Three steps, all in `src/layouts.cc`.

### Step 1 -- write `Build` (Model -> bytes)

Use the vendored `Writer` (`include/pfb/thrift_compact.h`). It writes valid
Thrift Compact: field headers, list headers, and `i8/i32/i64`.

```cpp
// struct { 1: list<i64> pairs; }   // [off0, size0, off1, size1, ...]
std::string BuildAos(const Model& m) {
  Writer w;
  w.Field(1, Type::kList);
  w.ListHeader(Type::kI64, m.chunks() * 2);
  for (int cc = 0; cc < m.chunks(); ++cc) {
    w.I64(m.chunk_off[cc]);
    w.I64(m.chunk_size[cc]);
  }
  w.Stop();
  return w.bytes();
}
```

### Step 2 -- write `Resolve` (bytes + query -> locators)

Use the `Reader`. Walk fields until `kStop`, read your data, then emit one
`ChunkLocator` per chunk the query touches.

```cpp
Resolved ResolveAos(const std::string& blob, const Model& m, Query q) {
  Reader r(blob);
  std::vector<int64_t> pairs;
  for (;;) {
    auto f = r.NextField();
    if (f.type == Type::kStop) break;
    auto lh = r.ListHeader();
    pairs.resize(lh.size);
    r.I64List(pairs.data(), lh.size);
  }
  Resolved out;
  ForEachSelected(m, q, [&](int cc) { out.push_back({pairs[cc * 2], pairs[cc * 2 + 1]}); });
  return out;
}
```

### Step 3 -- register it

Add one line to `Layouts()` at the bottom of `src/layouts.cc`:

```cpp
const std::vector<Layout>& Layouts() {
  static const std::vector<Layout> kLayouts = {
      {"soa_flat", BuildSoa, ResolveSoa},
      {"aos_flat", BuildAos, ResolveAos},   // <-- new
      ...
  };
  return kLayouts;
}
```

### Build, verify, measure

```sh
cmake --build build -j
ctest --test-dir build          # fidelity: "all checks passed (N shapes x M layouts)"
./build/pfb_bench --list        # your layout now appears
./build/pfb_bench               # and shows up in every shape's CSV
```

That is the whole loop. If you got the encoding right, you immediately have its
wire size and resolve timing next to every other layout; if you got it wrong, the
fidelity test tells you which shape and query broke before any number is reported.

## Toolbox

- **Encoding helpers** beyond plain varints: `include/pfb/bitpack.h` (`BitWidth`,
  `PackBits`, `ExtractBits`) for fixed-width packing with O(1) random access; see
  `soa_bitpack` and `soa_delta_bitpack` for how they are used.
- **Raw byte sections**: write a `list<i8>` (`WriteByteList`) and skip it in O(1)
  with the list header's length (`Reader::TakeBytes`); see
  `placement_plus_pageindex` for a section a query can choose not to decode.
- **Per-chunk structs + a directory** for random access without bit-packing: see
  `indexed_struct`.

## Tuning the workload

A layout is only as meaningful as the shapes it runs on. Edit `kBuiltinShapes`
in `src/main.cc`, or run a one-off:

```sh
./build/pfb_bench --columns 1000 --row-groups 4 --pages 128 --projected 32 --rows 2000000
```

To change page/chunk size distributions (and thus how big offsets and varints
get), edit `BuildModel` in `src/layouts.cc`.
