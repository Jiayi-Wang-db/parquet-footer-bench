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

// thrift_codec.h -- the parser layer, with no knowledge of any footer format.
//
// Just the Thrift Compact Protocol reader, the fixed-width bit unpack used by
// bit-packed arrays, and a generic Thrift value tree. Everything format-specific
// (FileMetaData / jump-table index / ModularFooter field layouts) lives in
// footer_formats.h and is built on top of this.

#ifndef PFB_THRIFT_CODEC_H_
#define PFB_THRIFT_CODEC_H_

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace fdb {

// Thrift Compact Protocol wire types.
enum : uint8_t {
  T_STOP = 0, T_TRUE = 1, T_FALSE = 2, T_I8 = 3, T_I16 = 4, T_I32 = 5,
  T_I64 = 6, T_DOUBLE = 7, T_BINARY = 8, T_LIST = 9, T_SET = 10, T_MAP = 11,
  T_STRUCT = 12,
};

// A non-owning view into some buffer. Used so resolvers can hand back min/max
// (and packed arrays) without copying -- the referenced buffer must outlive it.
struct Span {
  const char* data = nullptr;
  uint32_t size = 0;
};

// Forward cursor over a Thrift Compact blob. offset() exposes the read position
// (used to record where a struct begins).
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
  // Zero-copy binary: a Span into this reader's buffer (which must outlive it).
  Span BinarySpan() {
    uint64_t n = Varint();
    Span s{p_, static_cast<uint32_t>(n)};
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

// LSB-first fixed-width bit unpack (matches the writers' pack). The buffer must
// have >= 9 readable bytes past the last requested bit -- callers pad.
inline uint64_t LowMask(int w) { return w >= 64 ? ~0ULL : ((1ULL << w) - 1); }
inline uint64_t ExtractBits(const uint8_t* buf, size_t idx, int width) {
  if (width == 0) return 0;
  size_t bit = idx * static_cast<size_t>(width);
  size_t byte = bit >> 3;
  int off = bit & 7;
  unsigned __int128 acc = 0;
  for (int k = 0; k < 9; ++k) acc |= static_cast<unsigned __int128>(buf[byte + k]) << (8 * k);
  return static_cast<uint64_t>((acc >> off) & LowMask(width));
}

// A fully materialized Thrift value (what a generic deserializer produces): every
// field allocated. Layout-agnostic -- navigation by field id is up to the caller.
struct Value {
  uint8_t type = 0;
  int64_t num = 0;                                // int / bool
  std::string bin;                                // binary
  std::vector<Value> items;                       // list / set entries
  std::vector<std::pair<int16_t, Value>> fields;  // struct fields
  const Value* field(int16_t id) const {
    for (const auto& f : fields) if (f.first == id) return &f.second;
    return nullptr;
  }
};
inline Value DecodeValue(Reader& r, uint8_t type) {
  Value v; v.type = type;
  switch (type) {
    case T_TRUE: v.num = 1; break;
    case T_FALSE: v.num = 0; break;
    case T_I8: v.num = r.U8(); break;
    case T_I16: case T_I32: case T_I64: v.num = r.I64(); break;
    case T_DOUBLE: for (int k = 0; k < 8; ++k) r.U8(); break;
    case T_BINARY: v.bin = r.Binary(); break;
    case T_LIST: case T_SET: {
      Reader::ListHdr h = r.List();
      v.items.reserve(h.size);
      for (int32_t i = 0; i < h.size; ++i) v.items.push_back(DecodeValue(r, h.elem));
      break;
    }
    case T_MAP: r.Skip(T_MAP); break;  // absent on the FileMetaData hot path
    case T_STRUCT: {
      int16_t s = r.StructBegin();
      for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField())
        v.fields.push_back({f.id, DecodeValue(r, f.type)});
      r.StructEnd(s);
      break;
    }
    default: break;
  }
  return v;
}

}  // namespace fdb

#endif  // PFB_THRIFT_CODEC_H_
