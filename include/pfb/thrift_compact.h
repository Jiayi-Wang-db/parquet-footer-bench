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

// Minimal, self-contained Thrift Compact Protocol codec -- just the subset the
// footer-layout harness needs (struct/field framing, i8/i32/i64, and lists of
// those). It is wire-compatible with the Apache Thrift Compact Protocol, so the
// blobs this produces can be cross-checked against a real Thrift deserializer.
//
// This intentionally vendors a tiny codec instead of depending on Apache Thrift:
// the harness is about measuring the *shape* of a footer, and a 200-line header
// keeps it buildable anywhere with a C++17 compiler and no third-party deps.

#ifndef PFB_THRIFT_COMPACT_H_
#define PFB_THRIFT_COMPACT_H_

#include <cstdint>
#include <string>
#include <vector>

namespace pfb {

// Compact-protocol type ids (a subset of the spec).
enum class Type : uint8_t {
  kStop = 0,
  kI8 = 3,
  kI32 = 5,
  kI64 = 6,
  kList = 9,
  kStruct = 12,
};

// ----------------------------------------------------------------- Writer
//
// Appends to an owned byte buffer. Field ids use compact delta encoding relative
// to the last field id written in the current struct; StructBegin/StructEnd save
// and restore that running id so nested structs encode correctly.
class Writer {
 public:
  // Field header: short form when the id delta is in [1, 15], else long form.
  void Field(int16_t id, Type type) {
    int delta = id - last_id_;
    if (delta > 0 && delta <= 15) {
      buf_.push_back(static_cast<char>((delta << 4) | static_cast<int>(type)));
    } else {
      buf_.push_back(static_cast<char>(static_cast<int>(type)));
      PutVarint(ZigZag(id));
    }
    last_id_ = id;
  }

  void I8(int8_t v) { buf_.push_back(static_cast<char>(v)); }
  void I32(int32_t v) { PutVarint(ZigZag(v)); }
  void I64(int64_t v) { PutVarint(ZigZag(v)); }

  // List header: short form when size in [0, 14], else long form.
  void ListHeader(Type elem, int32_t size) {
    if (size <= 14) {
      buf_.push_back(static_cast<char>((size << 4) | static_cast<int>(elem)));
    } else {
      buf_.push_back(static_cast<char>(0xF0 | static_cast<int>(elem)));
      PutVarint(static_cast<uint64_t>(static_cast<uint32_t>(size)));
    }
  }

  // Save/restore the running field id around a nested struct.
  int16_t StructBegin() {
    int16_t saved = last_id_;
    last_id_ = 0;
    return saved;
  }
  void StructEnd(int16_t saved) { last_id_ = saved; }

  void Stop() { buf_.push_back(0); }

  const std::string& bytes() const { return buf_; }
  size_t size() const { return buf_.size(); }

 private:
  void PutVarint(uint64_t v) {
    while (v >= 0x80) {
      buf_.push_back(static_cast<char>((v & 0x7F) | 0x80));
      v >>= 7;
    }
    buf_.push_back(static_cast<char>(v));
  }
  static uint64_t ZigZag(int64_t v) {
    return (static_cast<uint64_t>(v) << 1) ^ static_cast<uint64_t>(v >> 63);
  }

  std::string buf_;
  int16_t last_id_ = 0;
};

// ----------------------------------------------------------------- Reader
//
// A forward cursor over a serialized blob. On any malformed read it latches an
// error (ok() == false) instead of aborting, so callers can report cleanly.
class Reader {
 public:
  Reader(const char* data, size_t len) : p_(data), end_(data + len) {}
  explicit Reader(const std::string& s) : Reader(s.data(), s.size()) {}

  struct FieldHeader {
    int16_t id;
    Type type;
  };
  struct ListHdr {
    Type elem;
    int32_t size;
  };

  // Reads the next field header; type == kStop marks end of struct.
  FieldHeader NextField() {
    if (!Avail(1)) return Fail();
    uint8_t b = static_cast<uint8_t>(*p_++);
    if (b == 0) return {0, Type::kStop};
    Type type = static_cast<Type>(b & 0x0F);
    int delta = b >> 4;
    int16_t id;
    if (delta != 0) {
      id = static_cast<int16_t>(last_id_ + delta);
    } else {
      id = static_cast<int16_t>(UnZigZag(GetVarint()));
    }
    last_id_ = id;
    return {id, type};
  }

  int8_t I8() { return Avail(1) ? static_cast<int8_t>(*p_++) : (ok_ = false, 0); }
  int32_t I32() { return static_cast<int32_t>(UnZigZag(GetVarint())); }
  int64_t I64() { return UnZigZag(GetVarint()); }

  ListHdr ListHeader() {
    if (!Avail(1)) return {Type::kStop, 0};
    uint8_t b = static_cast<uint8_t>(*p_++);
    Type elem = static_cast<Type>(b & 0x0F);
    int32_t size = b >> 4;
    if (size == 15) size = static_cast<int32_t>(GetVarint());
    return {elem, size};
  }

  // Bulk-read a list<i64> body (header already consumed) into out.
  void I64List(int64_t* out, int32_t n) {
    for (int32_t i = 0; i < n; ++i) out[i] = I64();
  }
  void I32List(int32_t* out, int32_t n) {
    for (int32_t i = 0; i < n; ++i) out[i] = I32();
  }

  // Take n raw bytes and advance (a list<i8> body is stored contiguously).
  // Returns nullptr and latches an error if fewer than n bytes remain.
  const char* TakeBytes(int32_t n) {
    if (!Avail(static_cast<size_t>(n))) {
      ok_ = false;
      return nullptr;
    }
    const char* r = p_;
    p_ += n;
    return r;
  }

  // Skip a value of the given type (used to ignore fields a layout doesn't read).
  void Skip(Type type) {
    switch (type) {
      case Type::kI8:
        Avail(1) ? (void)(p_++) : (void)(ok_ = false);
        break;
      case Type::kI32:
      case Type::kI64:
        GetVarint();
        break;
      case Type::kList: {
        ListHdr h = ListHeader();
        for (int32_t i = 0; i < h.size; ++i) Skip(h.elem);
        break;
      }
      case Type::kStruct: {
        int16_t saved = last_id_;
        last_id_ = 0;
        for (;;) {
          FieldHeader f = NextField();
          if (f.type == Type::kStop) break;
          Skip(f.type);
        }
        last_id_ = saved;
        break;
      }
      case Type::kStop:
        break;
    }
  }

  // Enter/leave a nested struct (save/restore running id like the writer).
  int16_t StructBegin() {
    int16_t saved = last_id_;
    last_id_ = 0;
    return saved;
  }
  void StructEnd(int16_t saved) { last_id_ = saved; }

  bool ok() const { return ok_; }

 private:
  bool Avail(size_t n) const { return static_cast<size_t>(end_ - p_) >= n; }
  FieldHeader Fail() {
    ok_ = false;
    return {0, Type::kStop};
  }
  uint64_t GetVarint() {
    uint64_t v = 0;
    int shift = 0;
    while (Avail(1) && shift < 64) {
      uint8_t b = static_cast<uint8_t>(*p_++);
      v |= static_cast<uint64_t>(b & 0x7F) << shift;
      if (!(b & 0x80)) return v;
      shift += 7;
    }
    ok_ = false;
    return v;
  }
  static int64_t UnZigZag(uint64_t v) {
    return static_cast<int64_t>(v >> 1) ^ -static_cast<int64_t>(v & 1);
  }

  const char* p_;
  const char* end_;
  int16_t last_id_ = 0;
  bool ok_ = true;
};

}  // namespace pfb

#endif  // PFB_THRIFT_COMPACT_H_
