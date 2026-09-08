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

// parquet_to_jumptable -- read a Parquet file's footer and emit its jump-table
// version, fully per JumpTableFooter.thrift:
//
//   * FileMetaData.footer_index_pointer (field 10, v1) is emitted FIRST, before
//     all other fields, so a reader can grab it without walking the footer. The
//     original first field's delta header is rewritten to long form so the rest
//     of FileMetaData is preserved verbatim (shifted by a fixed prefix).
//   * FileMetadataFooterIndex carries num_leaf_columns, num_row_groups,
//     column_chunk_offsets (rg*(num_leaf_columns+1)+col, whole-byte packed), and
//     a full SchemaLayout: num_schema_elements; per-element byte offsets and
//     num_children (packed); a NameHashTable (FNV-1a-64 of the lowercase,
//     NUL-separated path -> element ordinal); a FieldIdMapping; has_non_ascii_names.
//
// NameHashTable conventions where the spec leaves a choice (documented for
// transparency): discriminator_bits = 0 (no discriminator; a reader confirms by
// byte-comparing the path), slot values are little-endian holding element_ordinal+1
// (0 = empty), open addressing is linear probing over a power-of-two table.
//
// Self-contained -- depends on nothing else in this repo:
//   c++ -std=c++17 -O2 parquet_to_jumptable.cc -o parquet_to_jumptable
//   ./parquet_to_jumptable input.parquet [output.parquet]

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace {

// ------------------------------------------------- Thrift Compact wire types
enum : uint8_t {
  T_STOP = 0, T_TRUE = 1, T_FALSE = 2, T_I8 = 3, T_I16 = 4, T_I32 = 5,
  T_I64 = 6, T_DOUBLE = 7, T_BINARY = 8, T_LIST = 9, T_SET = 10, T_MAP = 11,
  T_STRUCT = 12,
};

// Forward cursor over the footer; offset() records where a struct begins.
class Reader {
 public:
  Reader(const char* d, size_t n) : p_(d), begin_(d), end_(d + n) {}
  int64_t offset() const { return static_cast<int64_t>(p_ - begin_); }
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
    if (h.elem == T_TRUE || h.elem == T_FALSE) { Advance(static_cast<size_t>(h.size)); return; }
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
  const char* begin_;
  const char* end_;
  int16_t last_id_ = 0;
  bool ok_ = true;
};

// Compact writer with nested-struct framing.
class Writer {
 public:
  void Field(int16_t id, uint8_t type) {
    int delta = id - last_id_;
    if (delta > 0 && delta <= 15) Byte(static_cast<uint8_t>((delta << 4) | type));
    else { Byte(type); ZigZag(id); }
    last_id_ = id;
  }
  void I32Field(int16_t id, int32_t v) { Field(id, T_I32); ZigZag(v); }
  void BoolField(int16_t id, bool v) { Field(id, v ? T_TRUE : T_FALSE); }
  void Binary(int16_t id, const std::string& s) { Field(id, T_BINARY); Varint(s.size()); buf_.append(s); }
  void MapI32I32Field(int16_t id, const std::vector<std::pair<int32_t, int32_t>>& kv) {
    Field(id, T_MAP);
    Varint(kv.size());
    if (!kv.empty()) {
      Byte(static_cast<uint8_t>((T_I32 << 4) | T_I32));
      for (const auto& e : kv) { ZigZag(e.first); ZigZag(e.second); }
    }
  }
  void EmptyStructField(int16_t id) { Field(id, T_STRUCT); Stop(); }  // struct {} with no fields
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

// FileMetaData / RowGroup / ColumnChunk / SchemaElement field ids we navigate.
enum {
  FMD_SCHEMA = 2, FMD_ROW_GROUPS = 4, FMD_FOOTER_INDEX_POINTER = 10,
  RG_COLUMNS = 1, SE_NAME = 4, SE_NUM_CHILDREN = 5, SE_FIELD_ID = 9,
};

struct SchemaElem {
  int64_t offset = 0;      // byte offset of the SchemaElement within FileMetaData
  int32_t num_children = 0;
  int32_t field_id = -1;   // -1 = absent
  std::string name;
};

struct Walk {
  int row_groups = 0;
  int columns = 0;                     // leaf columns (= columns in each row group)
  std::vector<int64_t> chunk_offsets;  // (columns + 1) per row group
  std::vector<SchemaElem> schema;      // DFS pre-order, element 0 = root
  bool already_indexed = false;
};

SchemaElem ParseSchemaElement(Reader& r) {
  SchemaElem e;
  int16_t s = r.StructBegin();
  for (Reader::Field f = r.NextField(); f.type != T_STOP && r.ok(); f = r.NextField()) {
    if (f.id == SE_NAME && f.type == T_BINARY) e.name = r.Binary();
    else if (f.id == SE_NUM_CHILDREN && f.type == T_I32) e.num_children = r.I32();
    else if (f.id == SE_FIELD_ID && f.type == T_I32) e.field_id = r.I32();
    else r.Skip(f.type);
  }
  r.StructEnd(s);
  return e;
}

Walk WalkFooter(const std::string& footer) {
  Walk w;
  Reader r(footer.data(), footer.size());
  for (Reader::Field f = r.NextField(); f.type != T_STOP && r.ok(); f = r.NextField()) {
    if (f.id == FMD_FOOTER_INDEX_POINTER) { w.already_indexed = true; r.Skip(f.type); continue; }
    if (f.id == FMD_SCHEMA && f.type == T_LIST) {
      Reader::ListHdr sl = r.List();  // list<SchemaElement>, DFS pre-order
      for (int32_t i = 0; i < sl.size && r.ok(); ++i) {
        int64_t off = r.offset();
        SchemaElem e = ParseSchemaElement(r);
        e.offset = off;
        w.schema.push_back(std::move(e));
      }
      continue;
    }
    if (f.id == FMD_ROW_GROUPS && f.type == T_LIST) {
      Reader::ListHdr rgs = r.List();  // list<RowGroup>
      w.row_groups = rgs.size;
      for (int32_t g = 0; g < rgs.size && r.ok(); ++g) {
        int16_t sg = r.StructBegin();
        for (Reader::Field gf = r.NextField(); gf.type != T_STOP && r.ok(); gf = r.NextField()) {
          if (gf.id != RG_COLUMNS || gf.type != T_LIST) { r.Skip(gf.type); continue; }
          Reader::ListHdr cols = r.List();  // list<ColumnChunk>, in schema order
          if (g == 0) w.columns = cols.size;
          for (int32_t c = 0; c < cols.size && r.ok(); ++c) {
            w.chunk_offsets.push_back(r.offset());  // ColumnChunk struct start
            r.Skip(T_STRUCT);
          }
          w.chunk_offsets.push_back(r.offset());    // row-group end offset
        }
        r.StructEnd(sg);
      }
      continue;
    }
    r.Skip(f.type);
  }
  if (!r.ok()) throw std::runtime_error("failed to parse FileMetaData footer");
  return w;
}

// ------------------------------------------------- packing / hashing helpers
int ByteWidth(uint64_t maxv) {
  int w = 1;
  while (w < 8 && maxv >= (1ULL << (8 * w))) ++w;
  return w;
}
std::string PackBytes(const std::vector<int64_t>& vals, int bw) {
  std::string out;
  out.reserve(vals.size() * bw);
  for (int64_t v : vals) {
    uint64_t u = static_cast<uint64_t>(v);
    for (int b = 0; b < bw; ++b) { out.push_back(static_cast<char>(u & 0xFF)); u >>= 8; }
  }
  return out;
}
void PutLE(std::string& s, size_t at, uint64_t u, int nbytes) {
  for (int b = 0; b < nbytes; ++b) s[at + b] = static_cast<char>((u >> (8 * b)) & 0xFF);
}
uint64_t Fnv1a64(const std::string& data) {
  uint64_t h = 0xcbf29ce484222325ULL;
  for (unsigned char c : data) { h ^= c; h *= 0x100000001b3ULL; }
  return h;
}
uint32_t NextPow2(uint32_t x) { uint32_t p = 1; while (p < x) p <<= 1; return p; }
int BitsFor(uint64_t v) { int b = 0; while (v) { ++b; v >>= 1; } return b < 1 ? 1 : b; }

}  // namespace

int main(int argc, char** argv) {
  std::vector<std::string> pos;
  for (int i = 1; i < argc; ++i) pos.push_back(argv[i]);
  if (pos.empty()) {
    std::fprintf(stderr, "usage: %s input.parquet [output.parquet]\n", argv[0]);
    return 2;
  }
  const std::string in_path = pos[0];
  const std::string out_path = pos.size() >= 2 ? pos[1] : in_path + ".jumptable.parquet";
  try {
    std::ifstream in(in_path, std::ios::binary);
    if (!in) throw std::runtime_error("cannot open " + in_path);
    in.seekg(0, std::ios::end);
    std::streamoff fsize = in.tellg();
    if (fsize < 8) throw std::runtime_error("file too small to be Parquet");
    char head[4] = {0};
    in.seekg(0); in.read(head, 4);
    const bool full_file = std::memcmp(head, "PAR1", 4) == 0;  // else a bare footer tail
    char tail[8];
    in.seekg(fsize - 8); in.read(tail, 8);
    if (std::memcmp(tail + 4, "PAR1", 4) != 0)
      throw std::runtime_error("missing PAR1 magic (encrypted or not a Parquet file)");
    uint32_t footer_len = static_cast<uint8_t>(tail[0]) | (static_cast<uint8_t>(tail[1]) << 8) |
                          (static_cast<uint8_t>(tail[2]) << 16) |
                          (static_cast<uint32_t>(static_cast<uint8_t>(tail[3])) << 24);
    if (static_cast<std::streamoff>(footer_len) + 8 > fsize)
      throw std::runtime_error("footer length exceeds file size");
    const int64_t footer_start = fsize - 8 - static_cast<std::streamoff>(footer_len);
    std::string footer(footer_len, '\0');
    in.seekg(footer_start); in.read(&footer[0], footer_len);
    if (!in) throw std::runtime_error("short read of footer");
    if (footer.empty() || static_cast<uint8_t>(footer.back()) != T_STOP)
      throw std::runtime_error("FileMetaData does not end in a STOP byte");

    // 1. Walk the footer: chunk offsets, schema elements, index presence.
    Walk w = WalkFooter(footer);
    if (w.already_indexed)
      throw std::runtime_error("footer already carries a footer_index_pointer (field 10)");
    if (w.row_groups == 0) throw std::runtime_error("footer has no row groups");
    if (w.schema.empty()) throw std::runtime_error("footer has no schema");
    const int N = static_cast<int>(w.schema.size());
    const int64_t chunks = static_cast<int64_t>(w.columns) * w.row_groups;

    // 2. Compute the prefix that emits footer_index_pointer FIRST, then rewrites
    //    the original first field's header to long form, then keeps the rest
    //    verbatim. Everything after the original first header shifts by `shift`.
    std::string pointer(27, '\0');
    pointer[0] = static_cast<char>(0xA8);  // field 10 (delta 10 from a fresh struct), binary
    pointer[1] = static_cast<char>(0x19);  // binary length = 25
    pointer[2] = 0x01;                     // version 1

    uint8_t b0 = static_cast<uint8_t>(footer[0]);
    uint8_t type0 = b0 & 0x0F;
    int id0;
    size_t orig_hdr_len;
    if ((b0 >> 4) != 0) {
      id0 = b0 >> 4;
      orig_hdr_len = 1;
    } else {  // long-form original first field: zigzag id follows
      uint64_t v = 0; int sh = 0; size_t k = 1;
      for (;; ++k) {
        uint8_t b = static_cast<uint8_t>(footer[k]);
        v |= static_cast<uint64_t>(b & 0x7F) << sh;
        if (!(b & 0x80)) break;
        sh += 7;
      }
      id0 = static_cast<int>(static_cast<int64_t>(v >> 1) ^ -static_cast<int64_t>(v & 1));
      orig_hdr_len = k + 1;
    }
    std::string new_first_hdr;
    new_first_hdr.push_back(static_cast<char>(type0));  // long form: delta 0
    {
      uint64_t zz = (static_cast<uint64_t>(id0) << 1) ^ static_cast<uint64_t>(static_cast<int64_t>(id0) >> 63);
      while (zz >= 0x80) { new_first_hdr.push_back(static_cast<char>((zz & 0x7F) | 0x80)); zz >>= 7; }
      new_first_hdr.push_back(static_cast<char>(zz));
    }
    const int64_t shift = static_cast<int64_t>(pointer.size() + new_first_hdr.size()) -
                          static_cast<int64_t>(orig_hdr_len);
    const int64_t fmd_length = static_cast<int64_t>(footer.size()) + shift;

    // 3. column_chunk_offsets (shifted into the new FileMetaData).
    std::vector<int64_t> cco;
    cco.reserve(w.chunk_offsets.size());
    for (int64_t o : w.chunk_offsets) cco.push_back(o + shift);
    uint64_t cmax = 0; for (int64_t v : cco) cmax = std::max<uint64_t>(cmax, static_cast<uint64_t>(v));
    const int cbw = ByteWidth(cmax);

    // 4. SchemaLayout arrays.
    std::vector<int64_t> se_off(N), se_nc(N);
    for (int i = 0; i < N; ++i) { se_off[i] = w.schema[i].offset + shift; se_nc[i] = w.schema[i].num_children; }
    uint64_t omax = 0; for (int64_t v : se_off) omax = std::max<uint64_t>(omax, static_cast<uint64_t>(v));
    const int obw = ByteWidth(omax);
    uint64_t ncmax = 0; for (int64_t v : se_nc) ncmax = std::max<uint64_t>(ncmax, static_cast<uint64_t>(v));
    const int ncbw = ByteWidth(ncmax);

    // Parent chain (num_children stack) -> per-element path for the name hash.
    std::vector<int> parent(N, -1);
    {
      std::vector<std::pair<int, int>> st;
      for (int i = 0; i < N; ++i) {
        if (!st.empty()) { parent[i] = st.back().first; st.back().second--; }
        if (w.schema[i].num_children > 0) st.push_back({i, w.schema[i].num_children});
        while (!st.empty() && st.back().second == 0) st.pop_back();
      }
    }
    bool non_ascii = false;
    for (int i = 0; i < N; ++i)
      for (unsigned char c : w.schema[i].name) if (c > 0x7F) non_ascii = true;
    std::vector<std::string> path(N);  // path[0] (root) stays empty
    for (int i = 1; i < N; ++i) {
      std::string seg = w.schema[i].name;
      for (char& c : seg) if (c >= 'A' && c <= 'Z') c = static_cast<char>(c + 32);
      int p = parent[i];
      if (p <= 0) path[i] = seg;                 // top-level field (parent is root)
      else path[i] = path[p] + '\0' + seg;
    }

    // NameHashTable: open-addressed, linear probe, LE slot = element_ordinal + 1.
    const uint32_t num_slots = NextPow2(static_cast<uint32_t>(std::max(2, 2 * N)));
    const int slot_bytes = (BitsFor(static_cast<uint64_t>(N)) + 7) / 8;
    std::string table(static_cast<size_t>(num_slots) * slot_bytes, '\0');
    for (int i = 0; i < N; ++i) {
      uint32_t slot = static_cast<uint32_t>(Fnv1a64(path[i]) & (num_slots - 1));
      for (uint32_t probe = 0; probe < num_slots; ++probe) {
        uint32_t s = (slot + probe) & (num_slots - 1);
        size_t at = static_cast<size_t>(s) * slot_bytes;
        bool empty = true;
        for (int b = 0; b < slot_bytes; ++b) if (table[at + b] != 0) { empty = false; break; }
        if (empty) { PutLE(table, at, static_cast<uint64_t>(i + 1), slot_bytes); break; }
      }
    }

    // FieldIdMapping: NoFieldIds / FieldIdIsColumnOrdinal / explicit map.
    std::vector<std::pair<int32_t, int32_t>> fid_map;  // field_id -> column_ordinal
    bool any_field_id = false, identity = true;
    {
      int col = 0;
      for (int i = 0; i < N; ++i) {
        if (w.schema[i].num_children != 0) continue;  // leaves, in column order
        int32_t fid = w.schema[i].field_id;
        if (fid >= 0) { any_field_id = true; fid_map.push_back({fid, col}); if (fid != col) identity = false; }
        else identity = false;
        ++col;
      }
    }

    // 5. Serialize FileMetadataFooterIndex.
    Writer idx;
    idx.I32Field(1, w.columns);                  // num_leaf_columns
    idx.I32Field(2, w.row_groups);               // num_row_groups
    idx.Binary(3, PackBytes(cco, cbw));          // column_chunk_offsets
    idx.Field(5, T_STRUCT);                      // schema_layout
    int16_t s_sl = idx.StructBegin();
      idx.I32Field(1, N);                        // num_schema_elements
      idx.Binary(2, PackBytes(se_off, obw));     // offsets
      idx.Binary(3, PackBytes(se_nc, ncbw));     // num_children
      idx.Field(4, T_STRUCT);                    // name_hash_table
      int16_t s_nht = idx.StructBegin();
        idx.I32Field(1, static_cast<int32_t>(num_slots));  // num_slots
        idx.Binary(3, table);                    // table (discriminator_bits omitted = 0)
      idx.Stop(); idx.StructEnd(s_nht);
      idx.Field(5, T_STRUCT);                    // field_id_mapping (union)
      int16_t s_fim = idx.StructBegin();
        if (!any_field_id) idx.EmptyStructField(1);        // none
        else if (identity) idx.EmptyStructField(2);        // identity
        else idx.MapI32I32Field(3, fid_map);               // explicit
      idx.Stop(); idx.StructEnd(s_fim);
      idx.BoolField(6, non_ascii);               // has_non_ascii_names
    idx.Stop(); idx.StructEnd(s_sl);
    idx.Stop();
    const std::string& index_blob = idx.bytes();

    // 6. Patch the pointer, assemble the footer, write the output.
    const int64_t footer_start_out = full_file ? footer_start : 0;
    const int64_t index_start = footer_start_out + fmd_length;
    PutLE(pointer, 3, static_cast<uint64_t>(index_start), 8);
    PutLE(pointer, 11, static_cast<uint64_t>(index_blob.size()), 8);
    PutLE(pointer, 19, static_cast<uint64_t>(fmd_length), 8);

    std::string new_fmd = pointer + new_first_hdr + footer.substr(orig_hdr_len);
    std::string new_footer = new_fmd + index_blob;

    std::ofstream out(out_path, std::ios::binary | std::ios::trunc);
    if (!out) throw std::runtime_error("cannot open " + out_path + " for writing");
    if (full_file) {
      std::string prefix(static_cast<size_t>(footer_start), '\0');
      in.seekg(0); in.read(&prefix[0], footer_start);
      out.write(prefix.data(), prefix.size());
    }
    out.write(new_footer.data(), new_footer.size());
    char trailer[8];
    uint32_t nf = static_cast<uint32_t>(new_footer.size());
    trailer[0] = static_cast<char>(nf & 0xFF);
    trailer[1] = static_cast<char>((nf >> 8) & 0xFF);
    trailer[2] = static_cast<char>((nf >> 16) & 0xFF);
    trailer[3] = static_cast<char>((nf >> 24) & 0xFF);
    std::memcpy(trailer + 4, "PAR1", 4);
    out.write(trailer, 8);
    out.close();

    const char* fim = !any_field_id ? "none" : (identity ? "identity" : "explicit");
    std::printf("input                  %s\n", in_path.c_str());
    std::printf("oss_footer_bytes       %u\n", footer_len);
    std::printf("columns (leaf)         %d\n", w.columns);
    std::printf("row_groups             %d\n", w.row_groups);
    std::printf("column_chunks          %lld\n", static_cast<long long>(chunks));
    std::printf("schema_elements        %d\n", N);
    std::printf("jumptable_footer_bytes %zu\n", new_footer.size());
    std::printf("  file_metadata        %lld (verbatim + 27-byte pointer, emitted first)\n",
                static_cast<long long>(fmd_length));
    std::printf("  index_blob           %zu\n", index_blob.size());
    std::printf("    column_chunk_offsets %d B/entry x %zu entries\n", cbw, cco.size());
    std::printf("    schema offsets       %d B/entry x %d elements\n", obw, N);
    std::printf("    name_hash_table      %u slots x %d B (FNV-1a-64, linear probe)\n",
                num_slots, slot_bytes);
    std::printf("    field_id_mapping     %s\n", fim);
    std::printf("    has_non_ascii_names  %s\n", non_ascii ? "true" : "false");
    std::printf("footer_growth          +%zu bytes (%.1f%%)\n", new_footer.size() - footer_len,
                100.0 * (new_footer.size() - footer_len) / footer_len);
    std::printf("wrote                  %s\n", out_path.c_str());
    return 0;
  } catch (const std::exception& e) {
    std::fprintf(stderr, "error: %s\n", e.what());
    return 1;
  }
}
