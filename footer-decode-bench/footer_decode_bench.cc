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

// footer_decode_bench -- footer decoding speed: legacy walk vs jump-table seek.
//
// Input is a jump-table Parquet footer (produced by the jumptable-footer/
// converter): it carries both the ordinary nested FileMetaData AND a
// FileMetadataFooterIndex with column_chunk_offsets. That lets one file be
// resolved two ways with the SAME codec held constant, so the measured
// difference is the format effect (random access vs. walking), not parser
// quality:
//
//   walk   -- today's footer: to resolve a projection's chunk placement you must
//             walk every row group and every column chunk (Thrift structs are
//             self-delimited; no way to seek). A projected read skips unselected
//             chunk bodies but still pays the walk -- O(all chunks).
//   index  -- use column_chunk_offsets to seek straight to each projected chunk
//             and decode only those -- O(projected chunks).
//
// This is a "plan-only" measure: resolve each projected column chunk's
// {offset, size} (what a reader hands its fetch scheduler). No IO, no page decode.
//
// Self-contained -- depends on nothing else in this repo:
//   c++ -std=c++17 -O2 footer_decode_bench.cc -o footer_decode_bench
//   ./footer_decode_bench file.jumptable.parquet [num_projected]
//
// Produce the input with:  jumptable-footer/parquet_to_jumptable input.parquet

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

enum : uint8_t {
  T_STOP = 0, T_TRUE = 1, T_FALSE = 2, T_I8 = 3, T_I16 = 4, T_I32 = 5,
  T_I64 = 6, T_DOUBLE = 7, T_BINARY = 8, T_LIST = 9, T_SET = 10, T_MAP = 11,
  T_STRUCT = 12,
};

class Reader {
 public:
  Reader(const char* d, size_t n) : p_(d), begin_(d), end_(d + n) {}
  int64_t offset() const { return static_cast<int64_t>(p_ - begin_); }

  struct Field { int16_t id; uint8_t type; };
  Field NextField() {
    if (!Avail(1)) return {0, T_STOP};
    uint8_t b = static_cast<uint8_t>(*p_++);
    if (b == 0) return {0, T_STOP};
    uint8_t type = b & 0x0F;
    int delta = b >> 4;
    int16_t id = delta ? static_cast<int16_t>(last_id_ + delta) : static_cast<int16_t>(ZigZag());
    last_id_ = id;
    return {id, type};
  }
  struct ListHdr { uint8_t elem; int32_t size; };
  ListHdr List() {
    uint8_t b = static_cast<uint8_t>(*p_++);
    int32_t size = b >> 4;
    if (size == 15) size = static_cast<int32_t>(Varint());
    return {static_cast<uint8_t>(b & 0x0F), size};
  }
  int32_t I32() { return static_cast<int32_t>(ZigZag()); }
  int64_t I64() { return ZigZag(); }
  std::string Binary() {
    uint64_t n = Varint();
    std::string s(p_, static_cast<size_t>(n));
    p_ += n;
    return s;
  }
  int16_t StructBegin() { int16_t s = last_id_; last_id_ = 0; return s; }
  void StructEnd(int16_t s) { last_id_ = s; }

  void Skip(uint8_t type) {
    switch (type) {
      case T_TRUE: case T_FALSE: break;
      case T_I8: p_ += 1; break;
      case T_I16: case T_I32: case T_I64: Varint(); break;
      case T_DOUBLE: p_ += 8; break;
      case T_BINARY: p_ += static_cast<size_t>(Varint()); break;
      case T_LIST: case T_SET: {
        ListHdr h = List();
        if (h.elem == T_TRUE || h.elem == T_FALSE) { p_ += h.size; break; }
        for (int32_t i = 0; i < h.size; ++i) Skip(h.elem);
        break;
      }
      case T_MAP: {
        uint64_t n = Varint();
        if (n) { uint8_t kv = static_cast<uint8_t>(*p_++);
                 for (uint64_t i = 0; i < n; ++i) { Skip(kv >> 4); Skip(kv & 0x0F); } }
        break;
      }
      case T_STRUCT: {
        int16_t s = StructBegin();
        for (Field f = NextField(); f.type != T_STOP; f = NextField()) Skip(f.type);
        StructEnd(s);
        break;
      }
      default: break;
    }
  }

 private:
  bool Avail(size_t n) const { return static_cast<size_t>(end_ - p_) >= n; }
  uint64_t Varint() {
    uint64_t v = 0; int shift = 0;
    for (;;) { uint8_t b = static_cast<uint8_t>(*p_++); v |= static_cast<uint64_t>(b & 0x7F) << shift;
               if (!(b & 0x80)) return v; shift += 7; }
  }
  int64_t ZigZag() { uint64_t v = Varint(); return static_cast<int64_t>(v >> 1) ^ -static_cast<int64_t>(v & 1); }
  const char* p_;
  const char* begin_;
  const char* end_;
  int16_t last_id_ = 0;
};

enum { FMD_ROW_GROUPS = 4, FMD_FOOTER_INDEX_POINTER = 10, RG_COLUMNS = 1,
       CC_META_DATA = 3, CM_TOTAL_COMPRESSED = 7, CM_DATA_PAGE_OFFSET = 9,
       CM_DICT_PAGE_OFFSET = 11, IDX_NUM_LEAF = 1, IDX_NUM_RG = 2, IDX_CHUNK_OFFSETS = 3 };

struct Chunk { int64_t off; int64_t size; };

// Decode one ColumnChunk's placement (cursor at the ColumnChunk struct start):
// chunk start = dictionary_page_offset if present else data_page_offset; size =
// total_compressed_size.
Chunk ReadPlacement(Reader& r) {
  int64_t dpo = -1, dict = -1, tcs = 0;
  int16_t s = r.StructBegin();
  for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
    if (f.id == CC_META_DATA && f.type == T_STRUCT) {
      int16_t s2 = r.StructBegin();
      for (Reader::Field g = r.NextField(); g.type != T_STOP; g = r.NextField()) {
        if (g.id == CM_TOTAL_COMPRESSED) tcs = r.I64();
        else if (g.id == CM_DATA_PAGE_OFFSET) dpo = r.I64();
        else if (g.id == CM_DICT_PAGE_OFFSET) dict = r.I64();
        else r.Skip(g.type);
      }
      r.StructEnd(s2);
    } else {
      r.Skip(f.type);
    }
  }
  r.StructEnd(s);
  return {dict >= 0 ? dict : dpo, tcs};
}

// Legacy: walk every row group and every column chunk; decode the projected
// ones, skip the rest. Cost scales with the total chunk count.
std::vector<Chunk> WalkResolve(const std::string& footer, const std::vector<char>& want,
                               int C, int G) {
  std::vector<Chunk> place(static_cast<size_t>(C) * G, {0, 0});
  Reader r(footer.data(), footer.size());
  for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
    if (f.id != FMD_ROW_GROUPS || f.type != T_LIST) { r.Skip(f.type); continue; }
    Reader::ListHdr rgs = r.List();
    for (int32_t g = 0; g < rgs.size; ++g) {
      int16_t sg = r.StructBegin();
      for (Reader::Field gf = r.NextField(); gf.type != T_STOP; gf = r.NextField()) {
        if (gf.id != RG_COLUMNS || gf.type != T_LIST) { r.Skip(gf.type); continue; }
        Reader::ListHdr cols = r.List();
        for (int32_t c = 0; c < cols.size; ++c) {
          if (want[c]) place[static_cast<size_t>(c) * G + g] = ReadPlacement(r);
          else r.Skip(T_STRUCT);
        }
      }
      r.StructEnd(sg);
    }
  }
  std::vector<Chunk> out;
  for (int c = 0; c < C; ++c)
    if (want[c]) for (int g = 0; g < G; ++g) out.push_back(place[static_cast<size_t>(c) * G + g]);
  return out;
}

// Jump table: seek straight to each projected chunk via column_chunk_offsets and
// decode only those. Cost scales with the projected chunk count.
std::vector<Chunk> IndexResolve(const std::string& footer, const std::vector<char>& want,
                                int C, int G, const uint8_t* cco, int bpe) {
  int stride = C + 1;
  std::vector<Chunk> out;
  for (int c = 0; c < C; ++c) {
    if (!want[c]) continue;
    for (int g = 0; g < G; ++g) {
      int64_t bo = 0;
      const uint8_t* p = cco + (static_cast<size_t>(g) * stride + c) * bpe;
      for (int b = 0; b < bpe; ++b) bo |= static_cast<int64_t>(p[b]) << (8 * b);
      Reader r(footer.data() + bo, footer.size() - static_cast<size_t>(bo));
      out.push_back(ReadPlacement(r));
    }
  }
  return out;
}

bool Equal(const std::vector<Chunk>& a, const std::vector<Chunk>& b) {
  if (a.size() != b.size()) return false;
  for (size_t i = 0; i < a.size(); ++i)
    if (a[i].off != b[i].off || a[i].size != b[i].size) return false;
  return true;
}

// Run fn enough times to accumulate ~200ms; return per-call microseconds.
template <typename Fn>
double TimeUs(Fn fn) {
  using clock = std::chrono::steady_clock;
  long iters = 1;
  for (;;) {
    auto t0 = clock::now();
    for (long i = 0; i < iters; ++i) fn();
    double ns = std::chrono::duration<double, std::nano>(clock::now() - t0).count();
    if (ns > 200e6 || iters > (1L << 30)) return ns / iters / 1000.0;
    iters *= 2;
  }
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    std::fprintf(stderr, "usage: %s file.jumptable.parquet [num_projected]\n", argv[0]);
    return 2;
  }
  const std::string path = argv[1];
  const int proj = argc >= 3 ? std::atoi(argv[2]) : 1;
  try {
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("cannot open " + path);
    in.seekg(0, std::ios::end);
    std::streamoff fsize = in.tellg();
    if (fsize < 8) throw std::runtime_error("file too small");
    char tail[8];
    in.seekg(fsize - 8); in.read(tail, 8);
    if (std::memcmp(tail + 4, "PAR1", 4) != 0) throw std::runtime_error("missing PAR1 magic");
    uint32_t flen = static_cast<uint8_t>(tail[0]) | (static_cast<uint8_t>(tail[1]) << 8) |
                    (static_cast<uint8_t>(tail[2]) << 16) |
                    (static_cast<uint32_t>(static_cast<uint8_t>(tail[3])) << 24);
    std::string footer(flen, '\0');
    in.seekg(fsize - 8 - static_cast<std::streamoff>(flen)); in.read(&footer[0], flen);

    // Locate the footer_index_pointer (field 10) and read fmd_length; the index
    // blob sits at footer offset fmd_length.
    int64_t fmd_length = -1;
    {
      Reader r(footer.data(), footer.size());
      for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
        if (f.id == FMD_FOOTER_INDEX_POINTER && f.type == T_BINARY) {
          std::string p = r.Binary();
          if (p.size() < 25 || static_cast<uint8_t>(p[0]) != 1)
            throw std::runtime_error("unsupported footer_index_pointer version");
          std::memcpy(&fmd_length, p.data() + 17, 8);  // 3rd LE i64: file_meta_data_length
          break;
        }
        r.Skip(f.type);
      }
    }
    if (fmd_length < 0)
      throw std::runtime_error("no footer_index_pointer -- run jumptable-footer/parquet_to_jumptable first");

    // Decode FileMetadataFooterIndex: num_leaf_columns, num_row_groups,
    // column_chunk_offsets (raw bytes + bytes-per-entry).
    int C = 0, G = 0, bpe = 0;
    std::string cco_bytes;  // owns the column_chunk_offsets bytes for the run
    {
      Reader r(footer.data() + fmd_length, footer.size() - static_cast<size_t>(fmd_length));
      for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
        if (f.id == IDX_NUM_LEAF && f.type == T_I32) C = r.I32();
        else if (f.id == IDX_NUM_RG && f.type == T_I32) G = r.I32();
        else if (f.id == IDX_CHUNK_OFFSETS && f.type == T_BINARY) cco_bytes = r.Binary();
        else r.Skip(f.type);
      }
    }
    if (cco_bytes.empty() || C == 0 || G == 0)
      throw std::runtime_error("failed to decode FileMetadataFooterIndex");
    bpe = static_cast<int>(cco_bytes.size() / (static_cast<size_t>(G) * (C + 1)));
    const uint8_t* cco = reinterpret_cast<const uint8_t*>(cco_bytes.data());

    const int K = std::max(1, std::min(proj, C));
    std::vector<char> want_proj(C, 0), want_all(C, 1);
    for (int c = 0; c < K; ++c) want_proj[c] = 1;  // project the first K columns

    // Fidelity: both paths must resolve to identical placement.
    if (!Equal(WalkResolve(footer, want_proj, C, G), IndexResolve(footer, want_proj, C, G, cco, bpe)) ||
        !Equal(WalkResolve(footer, want_all, C, G), IndexResolve(footer, want_all, C, G, cco, bpe)))
      throw std::runtime_error("walk and index disagree on placement");

    double wp = TimeUs([&] { auto v = WalkResolve(footer, want_proj, C, G); if (v.empty()) std::abort(); });
    double ip = TimeUs([&] { auto v = IndexResolve(footer, want_proj, C, G, cco, bpe); if (v.empty()) std::abort(); });
    double wa = TimeUs([&] { auto v = WalkResolve(footer, want_all, C, G); if (v.empty()) std::abort(); });
    double ia = TimeUs([&] { auto v = IndexResolve(footer, want_all, C, G, cco, bpe); if (v.empty()) std::abort(); });

    std::printf("file            %s\n", path.c_str());
    std::printf("footer_bytes    %u\n", flen);
    std::printf("columns         %d\n", C);
    std::printf("row_groups      %d\n", G);
    std::printf("column_chunks   %d\n", C * G);
    std::printf("projected       %d of %d columns\n\n", K, C);
    std::printf("%-22s %12s\n", "resolve", "us/op");
    std::printf("%-22s %12.3f\n", "walk   (projected)", wp);
    std::printf("%-22s %12.3f   <- %.1fx vs walk\n", "index  (projected)", ip, ip > 0 ? wp / ip : 0);
    std::printf("%-22s %12.3f\n", "walk   (all cols)", wa);
    std::printf("%-22s %12.3f   <- %.1fx vs walk\n", "index  (all cols)", ia, ia > 0 ? wa / ia : 0);
    std::printf("\nprojected walk/full = %.2f  (≈1 means the walk cannot exploit projection)\n",
                wa > 0 ? wp / wa : 0);
    return 0;
  } catch (const std::exception& e) {
    std::fprintf(stderr, "error: %s\n", e.what());
    return 1;
  }
}
