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
#include <vector>

#include "pfb/bitpack.h"
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

// =================================================================== AOS_FLAT
//
// Array-of-structs: a single list<i64> of interleaved (offset, size) pairs,
// instead of soa_flat's two parallel lists. Same data, different memory order;
// useful for asking whether interleaving helps the full-scan decode. Like any
// varint list it has no random access, so a projection still decodes everything.
//   struct { 1: list<i64> pairs; }   // [off0, size0, off1, size1, ...]
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
// A faithful page index also carries a per-page row count: Parquet's OffsetIndex
// stores first_row_index per PageLocation. We model it as a per-page row list so
// the blob size reflects what a real page-index-as-core would actually pay.
//   struct { 1: i64 base; 2: list<i64> block_lengths; 3: list<i32> first_block[chunk];
//            4: list<i64> page_rows; }
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
  w.Field(4, Type::kList);
  w.ListHeader(Type::kI64, static_cast<int32_t>(m.block_rows.size()));
  for (int64_t v : m.block_rows) w.I64(v);
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
    } else if (f.id == 3) {
      auto lh = r.ListHeader();
      first_block.resize(lh.size);
      r.I32List(first_block.data(), lh.size);
    } else {
      // page_rows: not needed to resolve placement, skip it.
      r.Skip(f.type);
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

// =============================================================== SOA_BITPACK
//
// Like soa_flat, but offsets and sizes are each stored as a fixed-bit-width
// packed array instead of a varint list. The width is the minimum to hold the
// largest value. This keeps random access -- element i is at bit i*width -- so a
// projection extracts only the selected chunks (O(K*R)), while staying compact.
//   struct { 1: i8 off_bits; 2: i8 size_bits; 3: i32 count;
//            4: list<i8> packed_offsets; 5: list<i8> packed_sizes; }
std::vector<uint64_t> AsU64(const std::vector<int64_t>& v) {
  return std::vector<uint64_t>(v.begin(), v.end());
}
uint64_t MaxU64(const std::vector<int64_t>& v) {
  uint64_t mx = 0;
  for (int64_t x : v) mx = std::max(mx, static_cast<uint64_t>(x));
  return mx;
}
void WriteByteList(Writer& w, int16_t id, const std::string& bytes) {
  w.Field(id, Type::kList);
  w.ListHeader(Type::kI8, static_cast<int32_t>(bytes.size()));
  for (char c : bytes) w.I8(static_cast<int8_t>(c));
}
// Read a list<i8> body into a buffer padded with 16 trailing zero bytes so
// ExtractBits can always over-read safely.
std::vector<uint8_t> ReadPaddedByteList(Reader& r) {
  auto lh = r.ListHeader();
  const char* p = r.TakeBytes(lh.size);
  std::vector<uint8_t> buf;
  if (p != nullptr) buf.assign(p, p + lh.size);
  buf.resize(buf.size() + 16, 0);
  return buf;
}

std::string BuildSoaBitpack(const Model& m) {
  int ob = BitWidth(MaxU64(m.chunk_off));
  int sb = BitWidth(MaxU64(m.chunk_size));
  std::string po = PackBits(AsU64(m.chunk_off), ob);
  std::string ps = PackBits(AsU64(m.chunk_size), sb);
  Writer w;
  w.Field(1, Type::kI8);
  w.I8(static_cast<int8_t>(ob));
  w.Field(2, Type::kI8);
  w.I8(static_cast<int8_t>(sb));
  w.Field(3, Type::kI32);
  w.I32(m.chunks());
  WriteByteList(w, 4, po);
  WriteByteList(w, 5, ps);
  w.Stop();
  return w.bytes();
}
Resolved ResolveSoaBitpack(const std::string& blob, const Model& m, Query q) {
  Reader r(blob);
  int ob = 0, sb = 0;
  std::vector<uint8_t> ob_buf, sb_buf;
  for (;;) {
    auto f = r.NextField();
    if (f.type == Type::kStop) break;
    if (f.id == 1)
      ob = r.I8();
    else if (f.id == 2)
      sb = r.I8();
    else if (f.id == 3)
      r.I32();  // count -- not needed, the model drives iteration
    else if (f.id == 4)
      ob_buf = ReadPaddedByteList(r);
    else
      sb_buf = ReadPaddedByteList(r);
  }
  Resolved out;
  ForEachSelected(m, q, [&](int cc) {
    out.push_back({static_cast<int64_t>(ExtractBits(ob_buf.data(), cc, ob)),
                   static_cast<int64_t>(ExtractBits(sb_buf.data(), cc, sb))});
  });
  return out;
}

// ============================================================ SOA_DELTA_BITPACK
//
// Offsets are monotonically increasing, so their consecutive deltas (= the span
// each chunk occupies) are small and pack to a narrow width -- the smallest
// placement encoding here. The catch: deltas are not randomly accessible, so any
// query must prefix-sum the whole array to recover offsets (O(total chunks)),
// regardless of how few columns it projects. Sizes are still fixed-bit-packed.
//   struct { 1: i64 base; 2: i8 delta_bits; 3: i8 size_bits; 4: i32 count;
//            5: list<i8> packed_deltas; 6: list<i8> packed_sizes; }
std::string BuildSoaDeltaBitpack(const Model& m) {
  int n = m.chunks();
  std::vector<uint64_t> deltas(n);
  int64_t prev = m.base;
  for (int i = 0; i < n; ++i) {
    deltas[i] = static_cast<uint64_t>(m.chunk_off[i] - prev);
    prev = m.chunk_off[i];
  }
  uint64_t maxd = 0;
  for (uint64_t d : deltas) maxd = std::max(maxd, d);
  int db = BitWidth(maxd);
  int sb = BitWidth(MaxU64(m.chunk_size));
  std::string pd = PackBits(deltas, db);
  std::string ps = PackBits(AsU64(m.chunk_size), sb);
  Writer w;
  w.Field(1, Type::kI64);
  w.I64(m.base);
  w.Field(2, Type::kI8);
  w.I8(static_cast<int8_t>(db));
  w.Field(3, Type::kI8);
  w.I8(static_cast<int8_t>(sb));
  w.Field(4, Type::kI32);
  w.I32(n);
  WriteByteList(w, 5, pd);
  WriteByteList(w, 6, ps);
  w.Stop();
  return w.bytes();
}
Resolved ResolveSoaDeltaBitpack(const std::string& blob, const Model& m, Query q) {
  Reader r(blob);
  int64_t base = 0;
  int db = 0, sb = 0;
  std::vector<uint8_t> d_buf, s_buf;
  for (;;) {
    auto f = r.NextField();
    if (f.type == Type::kStop) break;
    if (f.id == 1)
      base = r.I64();
    else if (f.id == 2)
      db = r.I8();
    else if (f.id == 3)
      sb = r.I8();
    else if (f.id == 4)
      r.I32();  // count
    else if (f.id == 5)
      d_buf = ReadPaddedByteList(r);
    else
      s_buf = ReadPaddedByteList(r);
  }
  // Deltas are not randomly accessible: reconstruct every offset by prefix sum.
  int n = m.chunks();
  std::vector<int64_t> off(n);
  int64_t pos = base;
  for (int i = 0; i < n; ++i) {
    pos += static_cast<int64_t>(ExtractBits(d_buf.data(), i, db));
    off[i] = pos;
  }
  Resolved out;
  ForEachSelected(m, q, [&](int cc) {
    out.push_back({off[cc], static_cast<int64_t>(ExtractBits(s_buf.data(), cc, sb))});
  });
  return out;
}

// ========================================================= PLACEMENT_PLUS_PAGEINDEX
//
// Placement core (bitpacked offsets+sizes, random access) PLUS the page index as
// a SEPARATE, length-delimited section that a placement-only query skips in O(1)
// without decoding. The page index is stored as a single list<i8> blob, so its
// thrift list header carries the byte length: the reader advances past it with a
// pointer bump rather than walking per-page varints. This is the "page index is
// first-class but optional" design -- you pay its bytes in the footer, but pay
// nothing to decode it unless a query actually needs page skipping. (A writer
// could go further and place the blob in a separate footer byte-range, so a
// query that never needs pages pays nothing at all -- not even the bytes -- on
// the placement path. This layout models the in-footer, decode-free variant.)
//   struct { 1: i8 off_bits; 2: i8 size_bits; 3: i32 count;
//            4: list<i8> packed_offsets; 5: list<i8> packed_sizes;
//            6: list<i8> page_index; }  // <- skipped unless page skipping is needed
std::string BuildPageIndexBlob(const Model& m) {
  // Bitpack per-page lengths and rows; concatenate. Format detail is immaterial
  // here -- this section is only decoded by a page-skipping query, not by
  // placement resolution -- but its size reflects a real per-page index.
  int lb = BitWidth(MaxU64(m.block_len));
  int rb = BitWidth(MaxU64(m.block_rows));
  std::string blob;
  blob.push_back(static_cast<char>(lb));
  blob.push_back(static_cast<char>(rb));
  blob += PackBits(AsU64(m.block_len), lb);
  blob += PackBits(AsU64(m.block_rows), rb);
  return blob;
}
std::string BuildPlacementPlusPageIndex(const Model& m) {
  int ob = BitWidth(MaxU64(m.chunk_off));
  int sb = BitWidth(MaxU64(m.chunk_size));
  Writer w;
  w.Field(1, Type::kI8);
  w.I8(static_cast<int8_t>(ob));
  w.Field(2, Type::kI8);
  w.I8(static_cast<int8_t>(sb));
  w.Field(3, Type::kI32);
  w.I32(m.chunks());
  WriteByteList(w, 4, PackBits(AsU64(m.chunk_off), ob));
  WriteByteList(w, 5, PackBits(AsU64(m.chunk_size), sb));
  WriteByteList(w, 6, BuildPageIndexBlob(m));
  w.Stop();
  return w.bytes();
}
Resolved ResolvePlacementPlusPageIndex(const std::string& blob, const Model& m, Query q) {
  Reader r(blob);
  int ob = 0, sb = 0;
  std::vector<uint8_t> ob_buf, sb_buf;
  for (;;) {
    auto f = r.NextField();
    if (f.type == Type::kStop) break;
    if (f.id == 1) {
      ob = r.I8();
    } else if (f.id == 2) {
      sb = r.I8();
    } else if (f.id == 3) {
      r.I32();  // count
    } else if (f.id == 4) {
      ob_buf = ReadPaddedByteList(r);
    } else if (f.id == 5) {
      sb_buf = ReadPaddedByteList(r);
    } else {
      // Page index: skip in O(1) using the list header's byte length -- no
      // per-page decode on the placement path.
      auto lh = r.ListHeader();
      r.TakeBytes(lh.size);
    }
  }
  Resolved out;
  ForEachSelected(m, q, [&](int cc) {
    out.push_back({static_cast<int64_t>(ExtractBits(ob_buf.data(), cc, ob)),
                   static_cast<int64_t>(ExtractBits(sb_buf.data(), cc, sb))});
  });
  return out;
}

}  // namespace

Model BuildModel(const Shape& shape) {
  Model m;
  m.columns = shape.columns;
  m.row_groups = shape.row_groups;
  m.pages_per_chunk = shape.pages_per_chunk;
  m.rows_per_group = shape.rows_per_group;
  std::mt19937_64 rng(7);
  std::uniform_int_distribution<int64_t> page(4096, 16384), dict(1024, 4096);
  int chunks = m.chunks();
  m.chunk_off.resize(chunks);
  m.chunk_size.resize(chunks);
  // Rows are split evenly across the P data pages of a chunk (remainder on the
  // last page); the dictionary page carries no rows.
  int64_t rows_per_page = m.pages_per_chunk > 0 ? m.rows_per_group / m.pages_per_chunk : 0;
  int64_t pos = m.base;
  for (int cc = 0; cc < chunks; ++cc) {
    m.chunk_off[cc] = pos;
    int64_t csz = dict(rng);
    m.block_len.push_back(csz);
    m.block_rows.push_back(0);  // dictionary page
    int64_t rows_left = m.rows_per_group;
    for (int p = 0; p < m.pages_per_chunk; ++p) {
      int64_t l = page(rng);
      m.block_len.push_back(l);
      int64_t pr = (p == m.pages_per_chunk - 1) ? rows_left : rows_per_page;
      m.block_rows.push_back(pr);
      rows_left -= pr;
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
      {"aos_flat", BuildAos, ResolveAos},
      {"soa_bitpack", BuildSoaBitpack, ResolveSoaBitpack},
      {"soa_delta_bitpack", BuildSoaDeltaBitpack, ResolveSoaDeltaBitpack},
      {"indexed_struct", BuildIndexed, ResolveIndexed},
      {"offset_index", BuildOffsetIndex, ResolveOffsetIndex},
      {"placement_plus_pageindex", BuildPlacementPlusPageIndex, ResolvePlacementPlusPageIndex},
  };
  return kLayouts;
}

}  // namespace pfb
