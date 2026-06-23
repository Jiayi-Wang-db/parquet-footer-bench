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

// Fixed-width, LSB-first bit packing. Unlike a varint list, a fixed-width packed
// array supports O(1) random access: element i lives at bit offset i*width, so a
// layout can read just the chunks a projection needs without decoding the rest.

#ifndef PFB_BITPACK_H_
#define PFB_BITPACK_H_

#include <cstdint>
#include <string>
#include <vector>

namespace pfb {

// Minimum number of bits to represent every value in [0, maxval]; 0 if maxval==0.
inline int BitWidth(uint64_t maxval) { return maxval == 0 ? 0 : 64 - __builtin_clzll(maxval); }

inline uint64_t LowMask(int width) { return width >= 64 ? ~0ULL : ((1ULL << width) - 1); }

// Pack values at a fixed bit width, least-significant-bit first.
inline std::string PackBits(const std::vector<uint64_t>& vals, int width) {
  std::string out;
  if (width == 0) return out;
  unsigned __int128 acc = 0;
  int nbits = 0;
  for (uint64_t v : vals) {
    acc |= static_cast<unsigned __int128>(v & LowMask(width)) << nbits;
    nbits += width;
    while (nbits >= 8) {
      out.push_back(static_cast<char>(static_cast<uint64_t>(acc) & 0xFF));
      acc >>= 8;
      nbits -= 8;
    }
  }
  if (nbits > 0) out.push_back(static_cast<char>(static_cast<uint64_t>(acc) & 0xFF));
  return out;
}

// Extract element idx. `buf` must have at least 9 readable bytes past the byte
// holding the last requested bit (callers pad the buffer accordingly).
inline uint64_t ExtractBits(const uint8_t* buf, size_t idx, int width) {
  if (width == 0) return 0;
  size_t bit = idx * static_cast<size_t>(width);
  size_t byte = bit >> 3;
  int off = bit & 7;
  unsigned __int128 acc = 0;
  for (int k = 0; k < 9; ++k) acc |= static_cast<unsigned __int128>(buf[byte + k]) << (8 * k);
  return static_cast<uint64_t>((acc >> off) & LowMask(width));
}

}  // namespace pfb

#endif  // PFB_BITPACK_H_
