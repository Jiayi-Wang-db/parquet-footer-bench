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

// make_wide_footer -- synthesize a Parquet footer (bare tail) with a flat schema
// of N named DOUBLE columns over R row groups, with realistic per-chunk
// ColumnMetaData (placement + min/max/null_count statistics). No column data is
// written -- only the footer -- so it feeds the footer converters and benchmarks
// directly. Used to study wide schemas (thousands of columns), the regime our
// real corpus doesn't reach.
//
//   c++ -std=c++17 -O2 make_wide_footer.cc -o make_wide_footer
//   ./make_wide_footer --columns 5000 --row-groups 4 wide.parquet

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

namespace {

enum : uint8_t { T_I32 = 5, T_I64 = 6, T_BINARY = 8, T_LIST = 9, T_STRUCT = 12 };

class Writer {
 public:
  void Field(int16_t id, uint8_t type) {
    int delta = id - last_;
    if (delta > 0 && delta <= 15) buf_.push_back(static_cast<char>((delta << 4) | type));
    else { buf_.push_back(static_cast<char>(type)); PutVar(ZigZag(id)); }
    last_ = id;
  }
  void I32(int32_t v) { PutVar(ZigZag(v)); }
  void I64(int64_t v) { PutVar(ZigZag(v)); }
  void Binary(int16_t id, const std::string& s) { Field(id, T_BINARY); PutVar(s.size()); buf_.append(s); }
  void ListHeader(uint8_t elem, int32_t n) {
    if (n <= 14) buf_.push_back(static_cast<char>((n << 4) | elem));
    else { buf_.push_back(static_cast<char>(0xF0 | elem)); PutVar(static_cast<uint32_t>(n)); }
  }
  void I32List(int16_t id, const std::vector<int32_t>& v) {
    Field(id, T_LIST); ListHeader(T_I32, static_cast<int32_t>(v.size()));
    for (int32_t x : v) I32(x);
  }
  void StringList(int16_t id, const std::vector<std::string>& v) {
    Field(id, T_LIST); ListHeader(T_BINARY, static_cast<int32_t>(v.size()));
    for (const std::string& s : v) { PutVar(s.size()); buf_.append(s); }
  }
  int16_t StructBegin() { int16_t s = last_; last_ = 0; return s; }
  void StructEnd(int16_t s) { last_ = s; }
  void Stop() { buf_.push_back(0); }
  std::string& bytes() { return buf_; }

 private:
  void PutVar(uint64_t v) { while (v >= 0x80) { buf_.push_back(static_cast<char>((v & 0x7F) | 0x80)); v >>= 7; } buf_.push_back(static_cast<char>(v)); }
  static uint64_t ZigZag(int64_t v) { return (static_cast<uint64_t>(v) << 1) ^ static_cast<uint64_t>(v >> 63); }
  std::string buf_;
  int16_t last_ = 0;
};

std::string ColName(int i) {
  char b[16];
  std::snprintf(b, sizeof(b), "col_%06d", i);
  return std::string(b);
}

// 8-byte big-endian double bound, so min/max compare in lexicographic byte order.
std::string Bound(double d) {
  uint64_t bits;
  std::memcpy(&bits, &d, 8);
  std::string s(8, '\0');
  for (int i = 0; i < 8; ++i) s[i] = static_cast<char>((bits >> (8 * (7 - i))) & 0xFF);
  return s;
}

void WriteColumnMetaData(Writer& w, int col, int rg, int64_t rows) {
  int64_t off = 4 + (static_cast<int64_t>(rg) * 100000 + col) * 137;  // plausible distinct offsets
  int64_t size = 4096 + (col % 97);
  int16_t s = w.StructBegin();
  w.Field(1, T_I32); w.I32(5);                       // type = DOUBLE
  w.I32List(2, {0, 8});                              // encodings = PLAIN, RLE_DICTIONARY
  w.StringList(3, {ColName(col)});                   // path_in_schema
  w.Field(4, T_I32); w.I32(1);                       // codec = SNAPPY
  w.Field(5, T_I64); w.I64(rows);                    // num_values
  w.Field(6, T_I64); w.I64(size + 64);               // total_uncompressed_size
  w.Field(7, T_I64); w.I64(size);                    // total_compressed_size
  w.Field(9, T_I64); w.I64(off);                     // data_page_offset
  w.Field(12, T_STRUCT);                             // statistics
  int16_t st = w.StructBegin();
  w.Field(3, T_I64); w.I64(rg);                      // null_count
  w.Binary(5, Bound(1000.0 * rg + col + 0.5));       // max_value
  w.Binary(6, Bound(1.0 * rg + col * 0.001));        // min_value
  w.Stop(); w.StructEnd(st);
  w.Stop(); w.StructEnd(s);
}

}  // namespace

int main(int argc, char** argv) {
  int columns = 5000, row_groups = 4;
  int64_t rows_per_group = 100000;
  std::string out_path;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    auto next = [&] { return std::atoll(argv[++i]); };
    if (a == "--columns") columns = static_cast<int>(next());
    else if (a == "--row-groups") row_groups = static_cast<int>(next());
    else if (a == "--rows") rows_per_group = next();
    else out_path = a;
  }
  if (out_path.empty()) { std::fprintf(stderr, "usage: %s [--columns N --row-groups R --rows M] out.parquet\n", argv[0]); return 2; }

  Writer w;
  w.Field(1, T_I32); w.I32(2);                                       // version
  // schema: root group (num_children = columns) + N leaf DOUBLE columns
  w.Field(2, T_LIST); w.ListHeader(T_STRUCT, columns + 1);
  {
    int16_t s = w.StructBegin();
    w.Binary(4, "root"); w.Field(5, T_I32); w.I32(columns);          // name, num_children
    w.Stop(); w.StructEnd(s);
  }
  for (int c = 0; c < columns; ++c) {
    int16_t s = w.StructBegin();
    w.Field(1, T_I32); w.I32(5);                                     // type = DOUBLE
    w.Field(3, T_I32); w.I32(1);                                     // repetition_type = REQUIRED
    w.Binary(4, ColName(c));                                         // name
    w.Stop(); w.StructEnd(s);
  }
  w.Field(3, T_I64); w.I64(rows_per_group * row_groups);             // num_rows
  w.Field(4, T_LIST); w.ListHeader(T_STRUCT, row_groups);            // row_groups
  for (int rg = 0; rg < row_groups; ++rg) {
    int16_t sg = w.StructBegin();
    w.Field(1, T_LIST); w.ListHeader(T_STRUCT, columns);             // columns
    for (int c = 0; c < columns; ++c) {
      int16_t sc = w.StructBegin();
      w.Field(1, T_I64); w.I64(4);                                   // file_offset
      w.Field(3, T_STRUCT);                                          // meta_data
      WriteColumnMetaData(w, c, rg, rows_per_group);
      w.Stop(); w.StructEnd(sc);
    }
    w.Field(2, T_I64); w.I64(1 << 20);                               // total_byte_size
    w.Field(3, T_I64); w.I64(rows_per_group);                        // num_rows
    w.Stop(); w.StructEnd(sg);
  }
  w.Stop();

  std::string& fmd = w.bytes();
  std::ofstream out(out_path, std::ios::binary | std::ios::trunc);
  if (!out) { std::fprintf(stderr, "cannot open %s\n", out_path.c_str()); return 1; }
  out.write(fmd.data(), fmd.size());
  char trailer[8];
  uint32_t len = static_cast<uint32_t>(fmd.size());
  for (int b = 0; b < 4; ++b) trailer[b] = static_cast<char>((len >> (8 * b)) & 0xFF);
  std::memcpy(trailer + 4, "PAR1", 4);
  out.write(trailer, 8);
  std::printf("wrote %s: %d columns x %d row groups, footer %zu bytes\n",
              out_path.c_str(), columns, row_groups, fmd.size());
  return 0;
}
