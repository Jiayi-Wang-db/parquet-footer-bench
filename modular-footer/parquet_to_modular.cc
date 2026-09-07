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

// parquet_to_modular: read a Parquet file's footer and translate it, in full,
// into the modular-footer layout (see ModularFooter.thrift), writing the modular
// bytes and a per-module size breakdown.
//
// Dependency-free, like the rest of this repo: a small Thrift Compact codec is
// vendored below (a superset of include/pfb/thrift_compact.h -- it also handles
// the binary/bool/i16/double/map/set wire types a real FileMetaData contains),
// and the bit packing comes from include/pfb/bitpack.h. Builds with the repo
// (CMakeLists.txt target modular_footer_convert) and any C++17 compiler.
//
//   cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j
//   ./build/modular_footer_convert input.parquet [output.modular]
//
// Translates every module the FileMetaData footer carries:
//   * schema             (copied verbatim -- read in full anyway)
//   * placement          (per-chunk offsets/sizes/codecs/types, dense BITSET)
//   * row-group stats     (per-column ColumnStatistics via a column_offsets
//                          directory; null counts and min/max as PRESENT_INDEX,
//                          min/max with common-prefix stripping)
//   * file metadata      (created_by + key_value_metadata)
// The OFFSET_INDEX / COLUMN_INDEX page index lives in a separate file region
// (not in the footer), so it is not part of a footer-only translation.

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "pfb/bitpack.h"  // BitWidth, PackBits (fixed-width LSB-first packing)

namespace {

// ------------------------------------------------- Thrift Compact wire types
enum : uint8_t {
  T_STOP = 0, T_TRUE = 1, T_FALSE = 2, T_I8 = 3, T_I16 = 4, T_I32 = 5,
  T_I64 = 6, T_DOUBLE = 7, T_BINARY = 8, T_LIST = 9, T_SET = 10, T_MAP = 11,
  T_STRUCT = 12,
};

// Modular-footer ArrayEncoding values (ModularFooter.thrift).
enum : int32_t { ENC_BITSET = 0, ENC_PRESENT_INDEX = 1 };

// A forward cursor that can read every compact type (so it can navigate/skip a
// real FileMetaData) and hand back the raw byte span of a value it does not
// decode (used to copy the schema and key/value lists verbatim).
class Reader {
 public:
  Reader(const char* d, size_t n) : p_(d), end_(d + n) {}
  const char* cur() const { return p_; }
  bool ok() const { return ok_; }

  struct Field { int16_t id; uint8_t type; };
  Field NextField() {
    if (!Avail(1)) return Fail();
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
    if (!Avail(1)) { ok_ = false; return {0, 0}; }
    uint8_t b = static_cast<uint8_t>(*p_++);
    int32_t size = b >> 4;
    if (size == 15) size = static_cast<int32_t>(Varint());
    return {static_cast<uint8_t>(b & 0x0F), size};
  }
  int32_t I32() { return static_cast<int32_t>(ZigZag()); }
  int64_t I64() { return ZigZag(); }
  std::string Binary() {
    uint64_t n = Varint();
    if (!Avail(n)) { ok_ = false; return {}; }
    std::string s(p_, static_cast<size_t>(n));
    p_ += n;
    return s;
  }
  int16_t StructBegin() { int16_t s = last_id_; last_id_ = 0; return s; }
  void StructEnd(int16_t s) { last_id_ = s; }

  void Skip(uint8_t type) {
    switch (type) {
      case T_TRUE: case T_FALSE: break;
      case T_I8: Advance(1); break;
      case T_I16: case T_I32: case T_I64: Varint(); break;
      case T_DOUBLE: Advance(8); break;
      case T_BINARY: Advance(static_cast<size_t>(Varint())); break;
      case T_LIST: case T_SET: SkipList(); break;
      case T_MAP: {
        uint64_t n = Varint();
        if (n) {
          if (!Avail(1)) { ok_ = false; break; }
          uint8_t kv = static_cast<uint8_t>(*p_++);
          for (uint64_t i = 0; i < n && ok_; ++i) { Skip(kv >> 4); Skip(kv & 0x0F); }
        }
        break;
      }
      case T_STRUCT: {
        int16_t s = StructBegin();
        for (Field f = NextField(); f.type != T_STOP && ok_; f = NextField()) Skip(f.type);
        StructEnd(s);
        break;
      }
      default: ok_ = false; break;
    }
  }
  void SkipList() {
    ListHdr h = List();
    for (int32_t i = 0; i < h.size && ok_; ++i) Skip(h.elem);
  }

 private:
  bool Avail(size_t n) const { return static_cast<size_t>(end_ - p_) >= n; }
  void Advance(size_t n) { if (Avail(n)) p_ += n; else ok_ = false; }
  Field Fail() { ok_ = false; return {0, T_STOP}; }
  uint64_t Varint() {
    uint64_t v = 0; int shift = 0;
    while (Avail(1) && shift < 64) {
      uint8_t b = static_cast<uint8_t>(*p_++);
      v |= static_cast<uint64_t>(b & 0x7F) << shift;
      if (!(b & 0x80)) return v;
      shift += 7;
    }
    ok_ = false; return v;
  }
  int64_t ZigZag() { uint64_t v = Varint(); return static_cast<int64_t>(v >> 1) ^ -static_cast<int64_t>(v & 1); }

  const char* p_;
  const char* end_;
  int16_t last_id_ = 0;
  bool ok_ = true;
};

// Compact writer: field framing, i8/i32/i64, binary, lists, nested structs.
class Writer {
 public:
  void Field(int16_t id, uint8_t type) {
    int delta = id - last_id_;
    if (delta > 0 && delta <= 15) Byte(static_cast<uint8_t>((delta << 4) | type));
    else { Byte(type); ZigZag(id); }
    last_id_ = id;
  }
  void I8(int8_t v) { buf_.push_back(static_cast<char>(v)); }
  void I32(int32_t v) { ZigZag(v); }
  void I64(int64_t v) { ZigZag(v); }
  void Binary(int16_t id, const std::string& s) {
    Field(id, T_BINARY); Varint(s.size()); buf_.append(s);
  }
  void ListField(int16_t id, uint8_t elem, int32_t size) {
    Field(id, T_LIST);
    if (size <= 14) Byte(static_cast<uint8_t>((size << 4) | elem));
    else { Byte(static_cast<uint8_t>(0xF0 | elem)); Varint(static_cast<uint32_t>(size)); }
  }
  void AppendRaw(const char* d, size_t n) { buf_.append(d, n); }
  int16_t StructBegin() { int16_t s = last_id_; last_id_ = 0; return s; }
  void StructEnd(int16_t s) { last_id_ = s; }
  void Stop() { buf_.push_back(0); }
  const std::string& bytes() const { return buf_; }

 private:
  void Byte(uint8_t b) { buf_.push_back(static_cast<char>(b)); }
  void Varint(uint64_t v) {
    while (v >= 0x80) { buf_.push_back(static_cast<char>((v & 0x7F) | 0x80)); v >>= 7; }
    buf_.push_back(static_cast<char>(v));
  }
  void ZigZag(int64_t v) { Varint((static_cast<uint64_t>(v) << 1) ^ static_cast<uint64_t>(v >> 63)); }

  std::string buf_;
  int16_t last_id_ = 0;
};

// ------------------------------------------------- ArrayPage emitters
static int Width(uint64_t maxval) { return pfb::BitWidth(maxval); }
static uint64_t Max(const std::vector<uint64_t>& v) {
  uint64_t m = 0;
  for (uint64_t x : v) m = x > m ? x : m;
  return m;
}

// Write the ArrayEncodingParameters union body for BITSET.
static void BitsetParams(Writer& w, int width, int32_t num_present) {
  w.Field(4, T_STRUCT);
  int16_t s = w.StructBegin();
  w.Field(1, T_STRUCT);                  // union member 1: bitset
  int16_t s2 = w.StructBegin();
  w.Field(1, T_I8); w.I8(static_cast<int8_t>(width));
  w.Field(2, T_I32); w.I32(num_present);
  w.Stop(); w.StructEnd(s2);
  w.Stop(); w.StructEnd(s);
}
static void PresentIndexParams(Writer& w, int32_t num_present, int wpos, int wval) {
  w.Field(4, T_STRUCT);
  int16_t s = w.StructBegin();
  w.Field(2, T_STRUCT);                  // union member 2: present_index
  int16_t s2 = w.StructBegin();
  w.Field(1, T_I32); w.I32(num_present);
  w.Field(2, T_I8); w.I8(static_cast<int8_t>(wpos));
  w.Field(3, T_I8); w.I8(static_cast<int8_t>(wval));
  w.Stop(); w.StructEnd(s2);
  w.Stop(); w.StructEnd(s);
}

// Dense BITSET integer ArrayPage: every position present (no bitmap).
static void PutIntDense(Writer& w, int16_t fid, const std::vector<uint64_t>& vals) {
  int width = Width(Max(vals));
  std::string data = pfb::PackBits(vals, width);
  w.Field(fid, T_STRUCT);
  int16_t s = w.StructBegin();
  w.Binary(1, data);
  w.Field(2, T_I32); w.I32(ENC_BITSET);
  w.Field(3, T_I32); w.I32(static_cast<int32_t>(vals.size()));
  BitsetParams(w, width, static_cast<int32_t>(vals.size()));
  w.Stop(); w.StructEnd(s);
}

// PRESENT_INDEX integer ArrayPage: [packed positions][packed values].
static void PutIntSparse(Writer& w, int16_t fid, int32_t domain,
                         const std::vector<uint64_t>& positions,
                         const std::vector<uint64_t>& values) {
  int wpos = Width(domain > 0 ? static_cast<uint64_t>(domain - 1) : 0);
  int wval = Width(Max(values));
  std::string data = pfb::PackBits(positions, wpos) + pfb::PackBits(values, wval);
  w.Field(fid, T_STRUCT);
  int16_t s = w.StructBegin();
  w.Binary(1, data);
  w.Field(2, T_I32); w.I32(ENC_PRESENT_INDEX);
  w.Field(3, T_I32); w.I32(domain);
  PresentIndexParams(w, static_cast<int32_t>(positions.size()), wpos, wval);
  w.Stop(); w.StructEnd(s);
}

// PRESENT_INDEX BYTE_ARRAY ArrayPage:
//   [packed positions][num_present+1 packed cumulative offsets][concatenated bytes]
static void PutBytesSparse(Writer& w, int16_t fid, int32_t domain,
                           const std::vector<uint64_t>& positions,
                           const std::vector<std::string>& values) {
  int wpos = Width(domain > 0 ? static_cast<uint64_t>(domain - 1) : 0);
  std::vector<uint64_t> cum(values.size() + 1, 0);
  for (size_t i = 0; i < values.size(); ++i) cum[i + 1] = cum[i] + values[i].size();
  int woff = Width(cum.back());
  std::string data = pfb::PackBits(positions, wpos) + pfb::PackBits(cum, woff);
  for (const std::string& v : values) data += v;
  w.Field(fid, T_STRUCT);
  int16_t s = w.StructBegin();
  w.Binary(1, data);
  w.Field(2, T_I32); w.I32(ENC_PRESENT_INDEX);
  w.Field(3, T_I32); w.I32(domain);
  PresentIndexParams(w, static_cast<int32_t>(positions.size()), wpos, woff);
  w.Stop(); w.StructEnd(s);
}

// ------------------------------------------------- parsed FileMetaData subset
struct Stat {
  bool has_null = false; int64_t null_count = 0;
  bool has_minmax = false; std::string minv, maxv;
  bool min_exact = true, max_exact = true;
};
struct Chunk {
  int64_t data_page_offset = 0, dict_offset = 0;
  int64_t total_compressed = 0, total_uncompressed = 0, num_values = 0;
  int32_t codec = 0, type = 0;
  bool has_dict = false;
  bool fully_dict = false;   // every data page dictionary-encoded (from encoding_stats)
  Stat stat;
};
struct RowGroup { int64_t num_rows = 0; std::vector<Chunk> columns; };
struct FileMeta {
  int32_t version = 1;
  int64_t num_rows = 0;
  const char* schema_span = nullptr; size_t schema_len = 0;   // list<SchemaElement> verbatim
  const char* kv_span = nullptr; size_t kv_len = 0;           // list<KeyValue> verbatim
  bool has_created_by = false; std::string created_by;
  std::vector<RowGroup> row_groups;
};

static Stat ParseStatistics(Reader& r) {
  Stat st;
  std::string min_dep, max_dep, min_val, max_val;
  bool has_min_dep = false, has_max_dep = false, has_min_val = false, has_max_val = false;
  int16_t s = r.StructBegin();
  for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
    switch (f.id) {
      case 1: max_dep = r.Binary(); has_max_dep = true; break;    // deprecated max
      case 2: min_dep = r.Binary(); has_min_dep = true; break;    // deprecated min
      case 3: st.null_count = r.I64(); st.has_null = true; break;
      case 5: max_val = r.Binary(); has_max_val = true; break;    // max_value (current)
      case 6: min_val = r.Binary(); has_min_val = true; break;    // min_value (current)
      case 7: st.max_exact = (f.type == T_TRUE); break;           // bool: value in header
      case 8: st.min_exact = (f.type == T_TRUE); break;
      default: r.Skip(f.type); break;
    }
    if (!r.ok()) break;
  }
  r.StructEnd(s);
  const bool have_min = has_min_val || has_min_dep;
  const bool have_max = has_max_val || has_max_dep;
  if (have_min && have_max) {
    st.has_minmax = true;
    st.minv = has_min_val ? min_val : min_dep;
    st.maxv = has_max_val ? max_val : max_dep;
  }
  return st;
}

static Chunk ParseColumnMetaData(Reader& r) {
  Chunk c;
  int16_t s = r.StructBegin();
  for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
    switch (f.id) {
      case 1:  c.type = r.I32(); break;
      case 4:  c.codec = r.I32(); break;
      case 5:  c.num_values = r.I64(); break;
      case 6:  c.total_uncompressed = r.I64(); break;
      case 7:  c.total_compressed = r.I64(); break;
      case 9:  c.data_page_offset = r.I64(); break;
      case 11: c.dict_offset = r.I64(); c.has_dict = true; break;
      case 12: c.stat = ParseStatistics(r); break;
      case 13: {  // encoding_stats: list<PageEncodingStats{1:page_type,2:encoding,3:count}>
        Reader::ListHdr h = r.List();
        bool any_data = false, all_dict = true;
        for (int32_t i = 0; i < h.size && r.ok(); ++i) {
          int32_t pt = 0, enc = 0;
          int16_t ss = r.StructBegin();
          for (Reader::Field g = r.NextField(); g.type != T_STOP; g = r.NextField()) {
            if (g.id == 1) pt = r.I32();
            else if (g.id == 2) enc = r.I32();
            else r.Skip(g.type);
          }
          r.StructEnd(ss);
          if (pt == 0 || pt == 3) {                 // DATA_PAGE / DATA_PAGE_V2
            any_data = true;
            if (enc != 2 && enc != 8) all_dict = false;  // PLAIN_DICTIONARY=2, RLE_DICTIONARY=8
          }
        }
        c.fully_dict = any_data && all_dict;
        break;
      }
      default: r.Skip(f.type); break;
    }
    if (!r.ok()) break;
  }
  r.StructEnd(s);
  return c;
}

static Chunk ParseColumnChunk(Reader& r) {
  Chunk c;
  int16_t s = r.StructBegin();
  for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
    if (f.id == 3 && f.type == T_STRUCT) c = ParseColumnMetaData(r);  // meta_data
    else r.Skip(f.type);
    if (!r.ok()) break;
  }
  r.StructEnd(s);
  return c;
}

static RowGroup ParseRowGroup(Reader& r) {
  RowGroup rg;
  int16_t s = r.StructBegin();
  for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
    if (f.id == 1 && f.type == T_LIST) {
      Reader::ListHdr h = r.List();
      rg.columns.reserve(h.size);
      for (int32_t i = 0; i < h.size && r.ok(); ++i) rg.columns.push_back(ParseColumnChunk(r));
    } else if (f.id == 3 && f.type == T_I64) {
      rg.num_rows = r.I64();
    } else {
      r.Skip(f.type);
    }
    if (!r.ok()) break;
  }
  r.StructEnd(s);
  return rg;
}

static FileMeta ParseFileMetaData(Reader& r) {
  FileMeta fm;
  int16_t s = r.StructBegin();
  for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
    if (f.id == 1 && f.type == T_I32) {
      fm.version = r.I32();
    } else if (f.id == 2 && f.type == T_LIST) {                  // schema (verbatim)
      fm.schema_span = r.cur(); r.SkipList(); fm.schema_len = static_cast<size_t>(r.cur() - fm.schema_span);
    } else if (f.id == 3 && f.type == T_I64) {
      fm.num_rows = r.I64();
    } else if (f.id == 4 && f.type == T_LIST) {                  // row_groups
      Reader::ListHdr h = r.List();
      fm.row_groups.reserve(h.size);
      for (int32_t i = 0; i < h.size && r.ok(); ++i) fm.row_groups.push_back(ParseRowGroup(r));
    } else if (f.id == 5 && f.type == T_LIST) {                  // key_value_metadata (verbatim)
      fm.kv_span = r.cur(); r.SkipList(); fm.kv_len = static_cast<size_t>(r.cur() - fm.kv_span);
    } else if (f.id == 6 && f.type == T_BINARY) {                // created_by
      fm.created_by = r.Binary(); fm.has_created_by = true;
    } else {
      r.Skip(f.type);
    }
    if (!r.ok()) break;
  }
  r.StructEnd(s);
  return fm;
}

static size_t CommonPrefix(const std::string& a, const std::string& b) {
  size_t n = a.size() < b.size() ? a.size() : b.size(), i = 0;
  while (i < n && a[i] == b[i]) ++i;
  return i;
}

// Build one leaf column's ColumnStatistics descriptor across row groups; empty
// string if the column has no statistics at all.
static std::string BuildColumnStatistics(const FileMeta& fm, int c, int G) {
  std::vector<uint64_t> null_pos, null_val;
  std::vector<uint64_t> mm_pos, min_ex, max_ex;
  std::vector<std::string> pref, min_suf, max_suf;
  for (int g = 0; g < G; ++g) {
    const Stat& st = fm.row_groups[g].columns[c].stat;
    if (st.has_null) { null_pos.push_back(g); null_val.push_back(static_cast<uint64_t>(st.null_count)); }
    if (st.has_minmax) {
      size_t lcp = CommonPrefix(st.minv, st.maxv);
      mm_pos.push_back(g);
      pref.push_back(st.minv.substr(0, lcp));
      min_suf.push_back(st.minv.substr(lcp));
      max_suf.push_back(st.maxv.substr(lcp));
      min_ex.push_back(st.min_exact ? 1 : 0);
      max_ex.push_back(st.max_exact ? 1 : 0);
    }
  }
  if (null_pos.empty() && mm_pos.empty()) return {};
  Writer d;
  if (!null_pos.empty()) PutIntSparse(d, 1, G, null_pos, null_val);          // null_counts
  if (!mm_pos.empty()) {
    PutBytesSparse(d, 2, G, mm_pos, pref);                                    // minmax_prefixes
    PutBytesSparse(d, 3, G, mm_pos, min_suf);                                 // min_suffixes
    PutBytesSparse(d, 4, G, mm_pos, max_suf);                                 // max_suffixes
    PutIntSparse(d, 5, G, mm_pos, min_ex);                                    // min_is_exact
    PutIntSparse(d, 6, G, mm_pos, max_ex);                                    // max_is_exact
  }
  d.Stop();
  return d.bytes();
}

static std::string ReadFooter(const std::string& path, size_t* oss_footer_bytes) {
  std::ifstream in(path, std::ios::binary);
  if (!in) throw std::runtime_error("cannot open " + path);
  in.seekg(0, std::ios::end);
  std::streamoff size = in.tellg();
  if (size < 8) throw std::runtime_error("file too small to be Parquet");
  char tail[8];
  in.seekg(size - 8);
  in.read(tail, 8);
  if (std::memcmp(tail + 4, "PAR1", 4) != 0)
    throw std::runtime_error("missing PAR1 magic (encrypted or not a Parquet file)");
  uint32_t len = static_cast<uint8_t>(tail[0]) | (static_cast<uint8_t>(tail[1]) << 8) |
                 (static_cast<uint8_t>(tail[2]) << 16) |
                 (static_cast<uint32_t>(static_cast<uint8_t>(tail[3])) << 24);
  if (static_cast<std::streamoff>(len) + 8 > size)
    throw std::runtime_error("footer length exceeds file size");
  std::string footer(len, '\0');
  in.seekg(size - 8 - static_cast<std::streamoff>(len));
  in.read(&footer[0], len);
  if (!in) throw std::runtime_error("short read of footer");
  *oss_footer_bytes = len;
  return footer;
}

// Modular-footer ModuleKind values (ModularFooter.thrift).
enum : int32_t { K_SCHEMA = 0, K_PLACEMENT = 1, K_ROW_GROUP_STATISTICS = 2,
                 K_OFFSET_INDEX = 3, K_COLUMN_INDEX = 4, K_FILE_METADATA = 5 };
struct DirEntry { int32_t kind; int64_t off; int64_t len; };

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    std::fprintf(stderr, "usage: %s input.parquet [output.modular]\n", argv[0]);
    return 2;
  }
  const std::string in_path = argv[1];
  const std::string out_path = argc >= 3 ? argv[2] : in_path + ".modular";
  try {
    size_t oss_footer_bytes = 0;
    std::string footer = ReadFooter(in_path, &oss_footer_bytes);
    Reader r(footer.data(), footer.size());
    FileMeta fm = ParseFileMetaData(r);
    if (!r.ok()) throw std::runtime_error("failed to parse FileMetaData footer");
    if (fm.row_groups.empty()) throw std::runtime_error("footer has no row groups");
    const int G = static_cast<int>(fm.row_groups.size());
    const int C = static_cast<int>(fm.row_groups[0].columns.size());
    const int64_t N = static_cast<int64_t>(C) * G;

    // ---- placement, column-major: chunk (column c, row group g) is at c*G + g.
    std::vector<uint64_t> dpo(N), tcs(N), tus(N), nv(N), cod(N), fdict_flag(N, 0);
    std::vector<uint64_t> ptypes(C), first_dict(N + 1, 0), dict_off;
    for (int c = 0; c < C; ++c) {
      ptypes[c] = static_cast<uint64_t>(fm.row_groups[0].columns[c].type);
      for (int g = 0; g < G; ++g) {
        if (c >= static_cast<int>(fm.row_groups[g].columns.size()))
          throw std::runtime_error("ragged row groups (column count differs)");
        const Chunk& ch = fm.row_groups[g].columns[c];
        int64_t idx = static_cast<int64_t>(c) * G + g;
        dpo[idx] = static_cast<uint64_t>(ch.data_page_offset);
        tcs[idx] = static_cast<uint64_t>(ch.total_compressed);
        tus[idx] = static_cast<uint64_t>(ch.total_uncompressed);
        nv[idx] = static_cast<uint64_t>(ch.num_values);
        cod[idx] = static_cast<uint64_t>(ch.codec);
        first_dict[idx + 1] = first_dict[idx] + (ch.has_dict ? 1 : 0);
        if (ch.has_dict) dict_off.push_back(static_cast<uint64_t>(ch.dict_offset));
        fdict_flag[idx] = ch.fully_dict ? 1 : 0;
      }
    }
    Writer placement;
    PutIntDense(placement, 1, dpo);
    PutIntDense(placement, 2, first_dict);
    PutIntDense(placement, 3, dict_off);
    PutIntDense(placement, 4, tcs);
    PutIntDense(placement, 5, tus);
    PutIntDense(placement, 6, nv);
    PutIntDense(placement, 7, cod);
    PutIntDense(placement, 8, ptypes);
    PutIntDense(placement, 9, fdict_flag);  // is_fully_dictionary_encoded
    placement.Stop();

    // ---- schema: copy the parsed list<SchemaElement> verbatim under field 1.
    Writer schema;
    schema.Field(1, T_LIST);
    if (fm.schema_span) schema.AppendRaw(fm.schema_span, fm.schema_len);
    else schema.AppendRaw("\x0c", 1);        // empty list<struct>
    schema.Stop();

    // ---- file metadata: created_by + key_value_metadata (verbatim list).
    Writer filemeta;
    bool have_filemeta = false;
    if (fm.has_created_by) { filemeta.Binary(1, fm.created_by); have_filemeta = true; }
    if (fm.kv_span) { filemeta.Field(2, T_LIST); filemeta.AppendRaw(fm.kv_span, fm.kv_len); have_filemeta = true; }
    if (have_filemeta) filemeta.Stop();

    // ---- row-group statistics: per-column ColumnStatistics descriptors located
    // by a column_offsets directory (absolute offsets into the output buffer).
    std::vector<std::string> col_desc(C);
    bool have_stats = false;
    for (int c = 0; c < C; ++c) {
      col_desc[c] = BuildColumnStatistics(fm, c, G);
      if (!col_desc[c].empty()) have_stats = true;
    }

    // ---- lay out the output and collect the directory.
    std::string out;
    std::vector<DirEntry> dir;
    auto place = [&](const std::string& blob) {
      int64_t off = static_cast<int64_t>(out.size());
      out.append(blob);
      return off;
    };
    dir.push_back({K_SCHEMA, place(schema.bytes()), static_cast<int64_t>(schema.bytes().size())});
    dir.push_back({K_PLACEMENT, place(placement.bytes()), static_cast<int64_t>(placement.bytes().size())});
    if (have_filemeta)
      dir.push_back({K_FILE_METADATA, place(filemeta.bytes()), static_cast<int64_t>(filemeta.bytes().size())});
    if (have_stats) {
      std::vector<uint64_t> col_off(C + 1, 0);
      for (int c = 0; c < C; ++c) {
        col_off[c] = static_cast<uint64_t>(out.size());
        out.append(col_desc[c]);                 // empty descriptor => col_off[c]==col_off[c+1]
      }
      col_off[C] = static_cast<uint64_t>(out.size());
      Writer rgstats;
      PutIntDense(rgstats, 1, col_off);           // column_offsets
      rgstats.Stop();
      dir.push_back({K_ROW_GROUP_STATISTICS, place(rgstats.bytes()),
                     static_cast<int64_t>(rgstats.bytes().size())});
    }

    // ---- ModularFooter directory root, appended last.
    Writer root;
    root.Field(1, T_I32); root.I32(fm.version);
    root.Field(2, T_I32); root.I32(G);
    root.Field(3, T_I32); root.I32(C);
    root.Field(4, T_I64); root.I64(fm.num_rows);
    root.ListField(5, T_I64, G);
    for (const RowGroup& rg : fm.row_groups) root.I64(rg.num_rows);
    root.ListField(6, T_STRUCT, static_cast<int32_t>(dir.size()));
    for (const DirEntry& e : dir) {
      int16_t s = root.StructBegin();
      root.Field(1, T_I32); root.I32(e.kind);
      root.Field(2, T_STRUCT);
      int16_t s2 = root.StructBegin();
      root.Field(1, T_I64); root.I64(e.off);
      root.Field(2, T_I64); root.I64(e.len);
      root.Stop(); root.StructEnd(s2);
      root.Stop(); root.StructEnd(s);
    }
    root.Stop();
    place(root.bytes());

    std::ofstream out_file(out_path, std::ios::binary | std::ios::trunc);
    if (!out_file) throw std::runtime_error("cannot open " + out_path + " for writing");
    out_file.write(out.data(), out.size());
    out_file.close();

    // stats region = per-column descriptors + the RowGroupStatisticsModule.
    const size_t known = schema.bytes().size() + placement.bytes().size() +
                         (have_filemeta ? filemeta.bytes().size() : 0) + root.bytes().size();
    const size_t stats_region = out.size() - known;

    std::printf("input                 %s\n", in_path.c_str());
    std::printf("oss_footer_bytes      %zu\n", oss_footer_bytes);
    std::printf("columns               %d\n", C);
    std::printf("row_groups            %d\n", G);
    std::printf("column_chunks         %lld\n", static_cast<long long>(N));
    std::printf("rows                  %lld\n", static_cast<long long>(fm.num_rows));
    std::printf("modular_total_bytes   %zu\n", out.size());
    std::printf("  schema_module       %zu\n", schema.bytes().size());
    std::printf("  placement_module    %zu\n", placement.bytes().size());
    if (have_stats)     std::printf("  rowgroup_stats      %zu\n", stats_region);
    if (have_filemeta)  std::printf("  file_metadata       %zu\n", filemeta.bytes().size());
    std::printf("  directory_root      %zu\n", root.bytes().size());
    std::printf("wrote                 %s (%zu bytes)\n", out_path.c_str(), out.size());
    return 0;
  } catch (const std::exception& e) {
    std::fprintf(stderr, "error: %s\n", e.what());
    return 1;
  }
}
