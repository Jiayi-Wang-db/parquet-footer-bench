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

// footer_decode_bench -- footer decoding speed: legacy walk vs jump table vs
// modular footer, extracting the SAME information from each.
//
// For each projected column chunk it resolves both placement AND statistics --
// {data_page_offset, total_compressed_size, null_count, min, max} -- which is
// what a scan+pruning reader needs from the footer. No IO, no page decode. The
// same vendored Thrift codec is used for all three, so the difference is the
// footer layout, not parser quality:
//
//   walk    today's nested footer -- walk every row group / column chunk and
//           decode each projected ColumnMetaData (placement + Statistics live in
//           the same struct). O(all chunks) to reach the projected ones.
//   index   the jump table -- seek to each projected ColumnChunk via
//           column_chunk_offsets and decode it (placement + Statistics together).
//           O(projected).
//   modular the ModularFooter -- placement from the PlacementModule (column-major
//           bit-packed) and statistics from a SEPARATE ColumnStatistics module
//           (present-index arrays; min = prefix+suffix). O(projected), but two
//           modules.
//
// Inputs (produced by the sibling converters):
//   jumptable-footer/parquet_to_jumptable input.parquet input.jt.parquet
//   modular-footer/parquet_to_modular     input.parquet input.modular
//
// Build & run:
//   c++ -std=c++17 -O2 footer_decode_bench.cc -o footer_decode_bench
//   ./footer_decode_bench input.jt.parquet [num_projected] [input.modular]

#include <algorithm>
#include <cctype>
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
  uint8_t U8() { return static_cast<uint8_t>(*p_++); }
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

// LSB-first fixed-width bit unpack, matching pfb::PackBits (>= 9 pad bytes past
// the last requested bit; callers pad).
uint64_t LowMask(int w) { return w >= 64 ? ~0ULL : ((1ULL << w) - 1); }
uint64_t ExtractBits(const uint8_t* buf, size_t idx, int width) {
  if (width == 0) return 0;
  size_t bit = idx * static_cast<size_t>(width);
  size_t byte = bit >> 3;
  int off = bit & 7;
  unsigned __int128 acc = 0;
  for (int k = 0; k < 9; ++k) acc |= static_cast<unsigned __int128>(buf[byte + k]) << (8 * k);
  return static_cast<uint64_t>((acc >> off) & LowMask(width));
}

// Field ids.
enum { FMD_ROW_GROUPS = 4, FMD_FOOTER_INDEX_POINTER = 10, RG_COLUMNS = 1,
       CC_META_DATA = 3, CM_TOTAL_COMPRESSED = 7, CM_DATA_PAGE_OFFSET = 9, CM_STATISTICS = 12,
       ST_MAX_DEP = 1, ST_MIN_DEP = 2, ST_NULL_COUNT = 3, ST_MAX_VALUE = 5, ST_MIN_VALUE = 6,
       IDX_NUM_LEAF = 1, IDX_NUM_RG = 2, IDX_CHUNK_OFFSETS = 3 };
enum { K_PLACEMENT = 1, K_ROW_GROUP_STATISTICS = 2,
       MOD_NUM_RG = 2, MOD_NUM_COLS = 3, MOD_DIRECTORY = 6,
       DIR_KIND = 1, DIR_LOCATION = 2, LOC_OFFSET = 1,
       PLACE_DATA_PAGE_OFFSETS = 1, PLACE_TOTAL_COMPRESSED = 4,
       CS_NULL_COUNTS = 1, CS_MINMAX_PREFIXES = 2, CS_MIN_SUFFIXES = 3, CS_MAX_SUFFIXES = 4,
       RGS_COLUMN_OFFSETS = 1,
       AP_DATA = 1, AP_ENCODING = 2, AP_PARAMS = 4, PARAMS_BITSET = 1, PARAMS_PRESENT_INDEX = 2,
       BITSET_WIDTH = 1, PI_NUM_PRESENT = 1, PI_POS_WIDTH = 2, PI_VAL_WIDTH = 3 };

// What each resolver returns per projected chunk: placement + statistics.
struct Loc {
  int64_t off = 0, size = 0;
  bool has_null = false; int64_t null_count = 0;
  bool has_mm = false; std::string minv, maxv;
  bool operator==(const Loc& o) const {
    return off == o.off && size == o.size && has_null == o.has_null &&
           (!has_null || null_count == o.null_count) && has_mm == o.has_mm &&
           (!has_mm || (minv == o.minv && maxv == o.maxv));
  }
};

// ---- nested-footer decode (walk & index): ColumnMetaData -> placement + stats
void ReadStatistics(Reader& r, Loc& L) {
  std::string mnd, mxd, mnv, mxv;
  bool hmnd = false, hmxd = false, hmnv = false, hmxv = false;
  int16_t s = r.StructBegin();
  for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
    if (f.id == ST_MAX_DEP) { mxd = r.Binary(); hmxd = true; }
    else if (f.id == ST_MIN_DEP) { mnd = r.Binary(); hmnd = true; }
    else if (f.id == ST_NULL_COUNT) { L.null_count = r.I64(); L.has_null = true; }
    else if (f.id == ST_MAX_VALUE) { mxv = r.Binary(); hmxv = true; }
    else if (f.id == ST_MIN_VALUE) { mnv = r.Binary(); hmnv = true; }
    else r.Skip(f.type);
  }
  r.StructEnd(s);
  if ((hmnv || hmnd) && (hmxv || hmxd)) {
    L.has_mm = true;
    L.minv = hmnv ? mnv : mnd;
    L.maxv = hmxv ? mxv : mxd;
  }
}
Loc ReadColumnInfo(Reader& r) {
  Loc L;
  int16_t s = r.StructBegin();
  for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
    if (f.id == CC_META_DATA && f.type == T_STRUCT) {
      int16_t s2 = r.StructBegin();
      for (Reader::Field g = r.NextField(); g.type != T_STOP; g = r.NextField()) {
        if (g.id == CM_TOTAL_COMPRESSED) L.size = r.I64();
        else if (g.id == CM_DATA_PAGE_OFFSET) L.off = r.I64();
        else if (g.id == CM_STATISTICS && g.type == T_STRUCT) ReadStatistics(r, L);
        else r.Skip(g.type);
      }
      r.StructEnd(s2);
    } else {
      r.Skip(f.type);
    }
  }
  r.StructEnd(s);
  return L;
}

std::vector<Loc> WalkResolve(const std::string& footer, const std::vector<char>& want, int C, int G) {
  std::vector<Loc> place(static_cast<size_t>(C) * G);
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
          if (want[c]) place[static_cast<size_t>(c) * G + g] = ReadColumnInfo(r);
          else r.Skip(T_STRUCT);
        }
      }
      r.StructEnd(sg);
    }
  }
  std::vector<Loc> out;
  for (int c = 0; c < C; ++c)
    if (want[c]) for (int g = 0; g < G; ++g) out.push_back(place[static_cast<size_t>(c) * G + g]);
  return out;
}

std::vector<Loc> IndexResolve(const std::string& footer, const std::vector<char>& want,
                              int C, int G, const uint8_t* cco, int bpe) {
  int stride = C + 1;
  std::vector<Loc> out;
  for (int c = 0; c < C; ++c) {
    if (!want[c]) continue;
    for (int g = 0; g < G; ++g) {
      int64_t bo = 0;
      const uint8_t* p = cco + (static_cast<size_t>(g) * stride + c) * bpe;
      for (int b = 0; b < bpe; ++b) bo |= static_cast<int64_t>(p[b]) << (8 * b);
      Reader r(footer.data() + bo, footer.size() - static_cast<size_t>(bo));
      out.push_back(ReadColumnInfo(r));
    }
  }
  return out;
}

// ---- modular ArrayPage decode
struct AP { std::string data; int encoding = -1; int np = 0; int wpos = 0; int wval = 0; };
AP ParseArrayPage(Reader& r) {
  AP ap;
  int16_t s = r.StructBegin();
  for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
    if (f.id == AP_DATA && f.type == T_BINARY) {
      ap.data = r.Binary();
    } else if (f.id == AP_ENCODING && f.type == T_I32) {
      ap.encoding = r.I32();
    } else if (f.id == AP_PARAMS && f.type == T_STRUCT) {
      int16_t s2 = r.StructBegin();
      for (Reader::Field g = r.NextField(); g.type != T_STOP; g = r.NextField()) {
        if (g.id == PARAMS_BITSET && g.type == T_STRUCT) {  // {1 value_bit_width, 2 num_present}
          int16_t s3 = r.StructBegin();
          for (Reader::Field h = r.NextField(); h.type != T_STOP; h = r.NextField()) {
            if (h.id == BITSET_WIDTH) ap.wval = r.U8();
            else if (h.id == 2) ap.np = r.I32();
            else r.Skip(h.type);
          }
          r.StructEnd(s3);
        } else if (g.id == PARAMS_PRESENT_INDEX && g.type == T_STRUCT) {  // {1 num_present, 2 wpos, 3 wval}
          int16_t s3 = r.StructBegin();
          for (Reader::Field h = r.NextField(); h.type != T_STOP; h = r.NextField()) {
            if (h.id == PI_NUM_PRESENT) ap.np = r.I32();
            else if (h.id == PI_POS_WIDTH) ap.wpos = r.U8();
            else if (h.id == PI_VAL_WIDTH) ap.wval = r.U8();
            else r.Skip(h.type);
          }
          r.StructEnd(s3);
        } else {
          r.Skip(g.type);
        }
      }
      r.StructEnd(s2);
    } else {
      r.Skip(f.type);
    }
  }
  r.StructEnd(s);
  ap.data.append(9, '\0');  // pad for ExtractBits over-read
  return ap;
}
int64_t BitsetAt(const AP& ap, size_t i) {
  return static_cast<int64_t>(ExtractBits(reinterpret_cast<const uint8_t*>(ap.data.data()), i, ap.wval));
}
// present-index integer: fill has[G]/val[G] at present logical positions.
void PresentInt(const AP& ap, int G, std::vector<char>& has, std::vector<int64_t>& val) {
  const uint8_t* d = reinterpret_cast<const uint8_t*>(ap.data.data());
  size_t pos_bytes = (static_cast<size_t>(ap.np) * ap.wpos + 7) / 8;
  const uint8_t* vd = d + pos_bytes;
  for (int i = 0; i < ap.np; ++i) {
    int p = static_cast<int>(ExtractBits(d, i, ap.wpos));
    if (p >= 0 && p < G) { has[p] = 1; val[p] = static_cast<int64_t>(ExtractBits(vd, i, ap.wval)); }
  }
}
// present-index byte array: fill has[G]/out[G]. Layout: positions | cumulative
// offsets (np+1) | concatenated bytes.
void PresentBytes(const AP& ap, int G, std::vector<char>& has, std::vector<std::string>& out) {
  const uint8_t* d = reinterpret_cast<const uint8_t*>(ap.data.data());
  size_t pos_bytes = (static_cast<size_t>(ap.np) * ap.wpos + 7) / 8;
  const uint8_t* cd = d + pos_bytes;
  size_t cum_bytes = ((static_cast<size_t>(ap.np) + 1) * ap.wval + 7) / 8;
  const char* bytes = ap.data.data() + pos_bytes + cum_bytes;
  for (int i = 0; i < ap.np; ++i) {
    int p = static_cast<int>(ExtractBits(d, i, ap.wpos));
    uint64_t c0 = ExtractBits(cd, i, ap.wval), c1 = ExtractBits(cd, i + 1, ap.wval);
    if (p >= 0 && p < G) { has[p] = 1; out[p].assign(bytes + c0, static_cast<size_t>(c1 - c0)); }
  }
}

// One column's ColumnStatistics decoded to per-row-group arrays.
struct ColStats {
  std::vector<char> has_null; std::vector<int64_t> null_count;
  std::vector<char> has_mm; std::vector<std::string> minv, maxv;
};
ColStats DecodeColumnStatistics(const std::string& mod, int64_t off, int64_t len, int G) {
  ColStats cs;
  cs.has_null.assign(G, 0); cs.null_count.assign(G, 0);
  cs.has_mm.assign(G, 0); cs.minv.resize(G); cs.maxv.resize(G);
  if (len <= 0) return cs;
  std::vector<std::string> pref(G), msuf(G), xsuf(G);
  std::vector<char> hp(G, 0), hms(G, 0), hxs(G, 0);
  Reader r(mod.data() + off, static_cast<size_t>(len));
  for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
    if (f.type != T_STRUCT) { r.Skip(f.type); continue; }
    if (f.id == CS_NULL_COUNTS) { AP ap = ParseArrayPage(r); PresentInt(ap, G, cs.has_null, cs.null_count); }
    else if (f.id == CS_MINMAX_PREFIXES) { AP ap = ParseArrayPage(r); PresentBytes(ap, G, hp, pref); }
    else if (f.id == CS_MIN_SUFFIXES) { AP ap = ParseArrayPage(r); PresentBytes(ap, G, hms, msuf); }
    else if (f.id == CS_MAX_SUFFIXES) { AP ap = ParseArrayPage(r); PresentBytes(ap, G, hxs, xsuf); }
    else r.Skip(f.type);
  }
  for (int g = 0; g < G; ++g)
    if (hp[g] && hms[g] && hxs[g]) { cs.has_mm[g] = 1; cs.minv[g] = pref[g] + msuf[g]; cs.maxv[g] = pref[g] + xsuf[g]; }
  return cs;
}

// Precomputed modular navigation (untimed, like decoding the index directory).
struct ModularCtx {
  const std::string* mod;
  AP dpo, tcs;                    // placement bitsets
  std::vector<int64_t> col_off;   // column_offsets directory (C+1)
  bool have_stats = false;
};
ModularCtx ModularSetup(const std::string& mod, int C, int G) {
  if (mod.size() < 12 || std::memcmp(mod.data() + mod.size() - 4, "MFT1", 4) != 0)
    throw std::runtime_error("modular file missing MFT1 navigability trailer -- rebuild parquet_to_modular");
  int64_t root_off = 0;
  std::memcpy(&root_off, mod.data() + mod.size() - 12, 8);
  int64_t place_off = -1, stats_off = -1;
  {
    Reader r(mod.data() + root_off, mod.size() - static_cast<size_t>(root_off));
    for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
      if (f.id != MOD_DIRECTORY || f.type != T_LIST) { r.Skip(f.type); continue; }
      Reader::ListHdr d = r.List();
      for (int32_t i = 0; i < d.size; ++i) {
        int32_t kind = -1; int64_t off = 0;
        int16_t s = r.StructBegin();
        for (Reader::Field g = r.NextField(); g.type != T_STOP; g = r.NextField()) {
          if (g.id == DIR_KIND) kind = r.I32();
          else if (g.id == DIR_LOCATION && g.type == T_STRUCT) {
            int16_t s2 = r.StructBegin();
            for (Reader::Field h = r.NextField(); h.type != T_STOP; h = r.NextField()) {
              if (h.id == LOC_OFFSET) off = r.I64(); else r.Skip(h.type);
            }
            r.StructEnd(s2);
          } else r.Skip(g.type);
        }
        r.StructEnd(s);
        if (kind == K_PLACEMENT) place_off = off;
        else if (kind == K_ROW_GROUP_STATISTICS) stats_off = off;
      }
    }
  }
  if (place_off < 0) throw std::runtime_error("modular footer has no placement module");
  ModularCtx ctx;
  ctx.mod = &mod;
  {
    Reader r(mod.data() + place_off, mod.size() - static_cast<size_t>(place_off));
    for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
      if (f.id == PLACE_DATA_PAGE_OFFSETS && f.type == T_STRUCT) ctx.dpo = ParseArrayPage(r);
      else if (f.id == PLACE_TOTAL_COMPRESSED && f.type == T_STRUCT) ctx.tcs = ParseArrayPage(r);
      else r.Skip(f.type);
    }
  }
  ctx.col_off.assign(C + 1, 0);
  if (stats_off >= 0) {
    ctx.have_stats = true;
    Reader r(mod.data() + stats_off, mod.size() - static_cast<size_t>(stats_off));
    for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
      if (f.id == RGS_COLUMN_OFFSETS && f.type == T_STRUCT) {
        AP co = ParseArrayPage(r);
        for (int c = 0; c <= C; ++c) ctx.col_off[c] = BitsetAt(co, c);
      } else r.Skip(f.type);
    }
  }
  return ctx;
}
std::vector<Loc> ModularResolve(const ModularCtx& ctx, const std::vector<char>& want, int C, int G) {
  const std::string& mod = *ctx.mod;
  std::vector<Loc> out;
  for (int c = 0; c < C; ++c) {
    if (!want[c]) continue;
    ColStats cs;
    if (ctx.have_stats)
      cs = DecodeColumnStatistics(mod, ctx.col_off[c], ctx.col_off[c + 1] - ctx.col_off[c], G);
    for (int g = 0; g < G; ++g) {
      size_t cc = static_cast<size_t>(c) * G + g;
      Loc L;
      L.off = BitsetAt(ctx.dpo, cc);
      L.size = BitsetAt(ctx.tcs, cc);
      if (ctx.have_stats) {
        L.has_null = cs.has_null[g]; L.null_count = cs.null_count[g];
        L.has_mm = cs.has_mm[g]; if (L.has_mm) { L.minv = cs.minv[g]; L.maxv = cs.maxv[g]; }
      }
      out.push_back(std::move(L));
    }
  }
  return out;
}

bool Equal(const std::vector<Loc>& a, const std::vector<Loc>& b) {
  if (a.size() != b.size()) return false;
  for (size_t i = 0; i < a.size(); ++i) if (!(a[i] == b[i])) return false;
  return true;
}

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

std::string ReadWhole(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) throw std::runtime_error("cannot open " + path);
  return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    std::fprintf(stderr, "usage: %s file.jumptable.parquet [num_projected|--sweep] [file.modular]\n",
                 argv[0]);
    return 2;
  }
  const std::string jt_path = argv[1];
  bool sweep = false;
  int proj = 1;
  std::string mod_path;
  for (int i = 2; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--sweep") sweep = true;
    else if (!a.empty() && std::isdigit(static_cast<unsigned char>(a[0]))) proj = std::atoi(a.c_str());
    else mod_path = a;
  }
  try {
    std::string jt = ReadWhole(jt_path);
    if (jt.size() < 8 || std::memcmp(jt.data() + jt.size() - 4, "PAR1", 4) != 0)
      throw std::runtime_error(jt_path + ": missing PAR1 magic");
    uint32_t flen = 0;
    std::memcpy(&flen, jt.data() + jt.size() - 8, 4);
    std::string footer = jt.substr(jt.size() - 8 - flen, flen);

    int64_t fmd_length = -1;
    {
      Reader r(footer.data(), footer.size());
      for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
        if (f.id == FMD_FOOTER_INDEX_POINTER && f.type == T_BINARY) {
          std::string p = r.Binary();
          if (p.size() < 25 || static_cast<uint8_t>(p[0]) != 1)
            throw std::runtime_error("unsupported footer_index_pointer version");
          std::memcpy(&fmd_length, p.data() + 17, 8);
          break;
        }
        r.Skip(f.type);
      }
    }
    if (fmd_length < 0)
      throw std::runtime_error("no footer_index_pointer -- run jumptable-footer/parquet_to_jumptable first");

    int C = 0, G = 0, bpe = 0;
    std::string cco_bytes;
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

    bool have_mod = !mod_path.empty();
    std::string mod;
    ModularCtx ctx;
    if (have_mod) { mod = ReadWhole(mod_path); ctx = ModularSetup(mod, C, G); }

    // Sweep mode: time projected resolution across a geometric range of column
    // counts (1, 2, 4, ... C) and emit CSV for plotting.
    if (sweep) {
      std::vector<int> ks;
      for (int k = 1; k < C; k *= 2) ks.push_back(k);
      ks.push_back(C);
      std::printf("projected,walk_us,index_us,modular_us\n");
      for (int K : ks) {
        std::vector<char> want(C, 0);
        for (int c = 0; c < K; ++c) want[c] = 1;
        double w = TimeUs([&] { auto v = WalkResolve(footer, want, C, G); if (v.empty()) std::abort(); });
        double i = TimeUs([&] { auto v = IndexResolve(footer, want, C, G, cco, bpe); if (v.empty()) std::abort(); });
        double m = 0;
        if (have_mod) m = TimeUs([&] { auto v = ModularResolve(ctx, want, C, G); if (v.empty()) std::abort(); });
        std::printf("%d,%.3f,%.3f,%.3f\n", K, w, i, m);
      }
      return 0;
    }

    const int K = std::max(1, std::min(proj, C));
    std::vector<char> want_proj(C, 0), want_all(C, 1);
    for (int c = 0; c < K; ++c) want_proj[c] = 1;

    // Fidelity: all resolvers must agree on placement + stats.
    auto wp = WalkResolve(footer, want_proj, C, G);
    auto wa = WalkResolve(footer, want_all, C, G);
    if (!Equal(wp, IndexResolve(footer, want_proj, C, G, cco, bpe)) ||
        !Equal(wa, IndexResolve(footer, want_all, C, G, cco, bpe)))
      throw std::runtime_error("walk and index disagree");
    if (have_mod && (!Equal(wp, ModularResolve(ctx, want_proj, C, G)) ||
                     !Equal(wa, ModularResolve(ctx, want_all, C, G))))
      throw std::runtime_error("modular disagrees (mismatched input files, or truncated stats?)");

    double t_wp = TimeUs([&] { auto v = WalkResolve(footer, want_proj, C, G); if (v.empty()) std::abort(); });
    double t_ip = TimeUs([&] { auto v = IndexResolve(footer, want_proj, C, G, cco, bpe); if (v.empty()) std::abort(); });
    double t_wa = TimeUs([&] { auto v = WalkResolve(footer, want_all, C, G); if (v.empty()) std::abort(); });
    double t_ia = TimeUs([&] { auto v = IndexResolve(footer, want_all, C, G, cco, bpe); if (v.empty()) std::abort(); });
    double t_mp = 0, t_ma = 0;
    if (have_mod) {
      t_mp = TimeUs([&] { auto v = ModularResolve(ctx, want_proj, C, G); if (v.empty()) std::abort(); });
      t_ma = TimeUs([&] { auto v = ModularResolve(ctx, want_all, C, G); if (v.empty()) std::abort(); });
    }

    std::printf("jumptable file  %s  (footer %u B)\n", jt_path.c_str(), flen);
    if (have_mod) std::printf("modular file    %s  (%zu B)\n", mod_path.c_str(), mod.size());
    std::printf("columns         %d\n", C);
    std::printf("row_groups      %d\n", G);
    std::printf("column_chunks   %d\n", C * G);
    std::printf("projected       %d of %d columns\n", K, C);
    std::printf("info per chunk  {data_page_offset, total_compressed_size, null_count, min, max}\n\n");
    std::printf("%-22s %12s\n", "resolve", "us/op");
    std::printf("%-22s %12.3f\n", "walk    (projected)", t_wp);
    std::printf("%-22s %12.3f   <- %.1fx vs walk\n", "index   (projected)", t_ip, t_ip > 0 ? t_wp / t_ip : 0);
    if (have_mod)
      std::printf("%-22s %12.3f   <- %.1fx vs walk\n", "modular (projected)", t_mp, t_mp > 0 ? t_wp / t_mp : 0);
    std::printf("%-22s %12.3f\n", "walk    (all cols)", t_wa);
    std::printf("%-22s %12.3f   <- %.1fx vs walk\n", "index   (all cols)", t_ia, t_ia > 0 ? t_wa / t_ia : 0);
    if (have_mod)
      std::printf("%-22s %12.3f   <- %.1fx vs walk\n", "modular (all cols)", t_ma, t_ma > 0 ? t_wa / t_ma : 0);
    std::printf("\nprojected walk/full = %.2f  (≈1 means the walk cannot exploit projection)\n",
                t_wa > 0 ? t_wp / t_wa : 0);
    return 0;
  } catch (const std::exception& e) {
    std::fprintf(stderr, "error: %s\n", e.what());
    return 1;
  }
}
