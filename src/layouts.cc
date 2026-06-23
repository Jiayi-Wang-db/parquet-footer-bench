// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "pfb/harness.h"

#include <algorithm>
#include <numeric>
#include <random>

#include "pfb/thrift_compact.h"

namespace pfb {
namespace {

// Iterate the chunk ids a query touches, in resolution order.
template <typename Fn>
void ForEachSelected(const Model& m, Query q, Fn fn) {
  if (q == Query::kFull) {
    for (int cc = 0; cc < m.chunks(); ++cc) fn(cc);
  } else {
    for (int c : m.selected) {
      for (int r = 0; r < m.row_groups; ++r) fn(c * m.row_groups + r);
    }
  }
}

// =================================================================== SOA_FLAT
//
// Two parallel lists across all chunks: offsets and sizes. Random access by
// chunk id is O(1) once both lists are materialized. This is the baseline
// "placement core": store exactly the offset+size the scheduler needs.
//   struct { 1: list<i64> offsets; 2: list<i64> sizes; }
std::string BuildSoa(const Model& m) {
  Writer w;
  int32_t n = m.chunks();
  w.Field(1, Type::kList);
  w.ListHeader(Type::kI64, n);
  for (int64_t v : m.chunk_off) w.I64(v);
  w.Field(2, Type::kList);
  w.ListHeader(Type::kI64, n);
  for (int64_t v : m.chunk_size) w.I64(v);
  w.Stop();
  return w.bytes();
}
Resolved ResolveSoa(const std::string& blob, const Model& m, Query q) {
  Reader r(blob);
  std::vector<int64_t> off, sz;
  for (;;) {
    auto f = r.NextField();
    if (f.type == Type::kStop) break;
    auto lh = r.ListHeader();
    if (f.id == 1) {
      off.resize(lh.size);
      r.I64List(off.data(), lh.size);
    } else {
      sz.resize(lh.size);
      r.I64List(sz.data(), lh.size);
    }
  }
  Resolved out;
  ForEachSelected(m, q, [&](int cc) { out.push_back({off[cc], sz[cc]}); });
  return out;
}

// ============================================================== INDEXED_STRUCT
//
// One self-contained thrift struct per chunk, plus an i64 directory of byte
// offsets into the struct blob. Projection decodes only the K*R selected structs
// (random access via the directory) instead of the whole blob -- at the cost of
// the directory plus per-struct framing overhead.
//   structs: [ { 1: i64 offset; 2: i64 size; } ... ]
//   directory: list<i64> byte_offset_of_struct[chunk]
std::string BuildIndexed(const Model& m) {
  Writer structs;
  std::vector<int64_t> dir(m.chunks());
  for (int cc = 0; cc < m.chunks(); ++cc) {
    dir[cc] = static_cast<int64_t>(structs.size());
    int16_t s = structs.StructBegin();
    structs.Field(1, Type::kI64);
    structs.I64(m.chunk_off[cc]);
    structs.Field(2, Type::kI64);
    structs.I64(m.chunk_size[cc]);
    structs.Stop();
    structs.StructEnd(s);
  }
  // Wire form: 1: list<i64> directory, 2: the concatenated struct bytes (as a
  // length-prefixed blob we model with an i32 length + raw bytes via i8 list).
  Writer w;
  w.Field(1, Type::kList);
  w.ListHeader(Type::kI64, m.chunks());
  for (int64_t v : dir) w.I64(v);
  w.Field(2, Type::kList);
  const std::string& sb = structs.bytes();
  w.ListHeader(Type::kI8, static_cast<int32_t>(sb.size()));
  std::string out = w.bytes();
  out.append(sb);  // raw struct bytes follow the i8-list header
  out.push_back(0);  // struct stop
  return out;
}
Resolved ResolveIndexed(const std::string& blob, const Model& m, Query q) {
  Reader r(blob);
  std::vector<int64_t> dir;
  size_t structs_at = 0;
  for (;;) {
    auto f = r.NextField();
    if (f.type == Type::kStop) break;
    auto lh = r.ListHeader();
    if (f.id == 1) {
      dir.resize(lh.size);
      r.I64List(dir.data(), lh.size);
    } else {
      // The i8-list body is the raw struct region; record where it starts.
      structs_at = blob.size() - 1 - static_cast<size_t>(lh.size);
      break;
    }
  }
  const char* structs = blob.data() + structs_at;
  size_t structs_len = blob.size() - 1 - structs_at;
  Resolved out;
  ForEachSelected(m, q, [&](int cc) {
    Reader sr(structs + dir[cc], structs_len - dir[cc]);
    int64_t off = 0, sz = 0;
    for (;;) {
      auto f = sr.NextField();
      if (f.type == Type::kStop) break;
      if (f.id == 1)
        off = sr.I64();
      else
        sz = sr.I64();
    }
    out.push_back({off, sz});
  });
  return out;
}

// =============================================================== OFFSET_INDEX
//
// Page-index-style: store per-page block lengths plus, per chunk, the index of
// its first block. Chunk placement is *derived* by prefix-summing block lengths.
// This mirrors the proposal of making the page index the placement core; the
// harness exists partly to measure what that costs versus storing placement
// directly (it scales with pages, not chunks, and resolution is a full sweep).
//   struct { 1: i64 base; 2: list<i64> block_lengths; 3: list<i32> first_block[chunk]; }
std::string BuildOffsetIndex(const Model& m) {
  Writer w;
  w.Field(1, Type::kI64);
  w.I64(m.base);
  w.Field(2, Type::kList);
  w.ListHeader(Type::kI64, static_cast<int32_t>(m.block_len.size()));
  for (int64_t v : m.block_len) w.I64(v);
  w.Field(3, Type::kList);
  w.ListHeader(Type::kI32, m.chunks());
  int32_t blk = 0;
  int per = m.pages_per_chunk + 1;
  for (int cc = 0; cc < m.chunks(); ++cc) {
    w.I32(blk);
    blk += per;
  }
  w.Stop();
  return w.bytes();
}
Resolved ResolveOffsetIndex(const std::string& blob, const Model& m, Query q) {
  Reader r(blob);
  int64_t base = 0;
  std::vector<int64_t> blocks;
  std::vector<int32_t> first_block;
  for (;;) {
    auto f = r.NextField();
    if (f.type == Type::kStop) break;
    if (f.id == 1) {
      base = r.I64();
    } else if (f.id == 2) {
      auto lh = r.ListHeader();
      blocks.resize(lh.size);
      r.I64List(blocks.data(), lh.size);
    } else {
      auto lh = r.ListHeader();
      first_block.resize(lh.size);
      r.I32List(first_block.data(), lh.size);
    }
  }
  // Derive each chunk's offset by prefix-summing block lengths -- O(total
  // blocks), with no shortcut for a selective projection.
  std::vector<int64_t> block_off(blocks.size() + 1);
  block_off[0] = base;
  for (size_t i = 0; i < blocks.size(); ++i) block_off[i + 1] = block_off[i] + blocks[i];
  int per = m.pages_per_chunk + 1;
  Resolved out;
  ForEachSelected(m, q, [&](int cc) {
    int fb = first_block[cc];
    int64_t off = block_off[fb];
    int64_t sz = 0;
    for (int b = 0; b < per; ++b) sz += blocks[fb + b];
    out.push_back({off, sz});
  });
  return out;
}

}  // namespace

Model BuildModel(const Shape& shape) {
  Model m;
  m.columns = shape.columns;
  m.row_groups = shape.row_groups;
  m.pages_per_chunk = shape.pages_per_chunk;
  std::mt19937_64 rng(7);
  std::uniform_int_distribution<int64_t> page(4096, 16384), dict(1024, 4096);
  int chunks = m.chunks();
  m.chunk_off.resize(chunks);
  m.chunk_size.resize(chunks);
  int64_t pos = m.base;
  for (int cc = 0; cc < chunks; ++cc) {
    m.chunk_off[cc] = pos;
    int64_t csz = dict(rng);
    m.block_len.push_back(csz);
    for (int p = 0; p < m.pages_per_chunk; ++p) {
      int64_t l = page(rng);
      m.block_len.push_back(l);
      csz += l;
    }
    m.chunk_size[cc] = csz;
    pos += csz;
  }
  std::vector<int> idx(m.columns);
  std::iota(idx.begin(), idx.end(), 0);
  std::shuffle(idx.begin(), idx.end(), rng);
  idx.resize(std::min(shape.projected, m.columns));
  std::sort(idx.begin(), idx.end());
  m.selected = idx;
  return m;
}

const std::vector<Layout>& Layouts() {
  static const std::vector<Layout> kLayouts = {
      {"soa_flat", BuildSoa, ResolveSoa},
      {"indexed_struct", BuildIndexed, ResolveIndexed},
      {"offset_index", BuildOffsetIndex, ResolveOffsetIndex},
  };
  return kLayouts;
}

}  // namespace pfb
