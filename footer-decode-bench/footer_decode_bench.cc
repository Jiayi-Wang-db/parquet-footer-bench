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

// footer_decode_bench -- footer decoding speed across four footer layouts, all
// resolving the SAME information for a projection: per projected column chunk,
// {data_page_offset, total_compressed_size, null_count, min, max} -- what a
// scan + row-group-pruning reader needs. No IO, no page decode.
//
//   standard  full Thrift materialization (a generated-Thrift reader, e.g.
//             parquet-java's): decode the whole FileMetaData into objects. Cost is
//             projection-independent -- it decodes everything.
//   walk      today's nested footer: walk every row group / column chunk, decode
//             the projected ones. O(all chunks).
//   index     jump table (FileMetadataFooterIndex.column_chunk_offsets): seek to
//             each projected chunk. O(projected).
//   modular   ModularFooter: column-major bit-packed placement + a separate
//             ColumnStatistics module. O(projected).
//
// The parser is shared (thrift_codec.h) and each layout is a Resolver
// (footer_formats.h), so the difference measured here is the footer layout.
//
// Inputs (from the sibling converters):
//   jumptable-footer/parquet_to_jumptable input.parquet input.jt.parquet
//   modular-footer/parquet_to_modular     input.parquet input.modular
//
// Build & run:
//   c++ -std=c++17 -O2 footer_decode_bench.cc -o footer_decode_bench
//   ./footer_decode_bench input.jt.parquet [num_projected|--sweep] [input.modular]

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "footer_formats.h"

namespace {

using fdb::Placement;
using fdb::Resolver;

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

std::string ReadWhole(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) throw std::runtime_error("cannot open " + path);
  return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

std::vector<char> Mask(int C, int K) {
  std::vector<char> w(C, 0);
  for (int c = 0; c < K && c < C; ++c) w[c] = 1;
  return w;
}

double Time(const Resolver& r, const std::vector<char>& want) {
  return TimeUs([&] { Placement p = r.Resolve(want); if (p.empty()) std::abort(); });
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

    fdb::FooterIndex fi = fdb::ReadFooterIndex(footer);
    const int C = fi.columns, G = fi.row_groups;

    bool have_mod = !mod_path.empty();
    std::string mod;
    if (have_mod) mod = ReadWhole(mod_path);

    // Build the resolvers -- one uniform interface each.
    std::vector<std::unique_ptr<Resolver>> rs;
    rs.push_back(std::make_unique<fdb::StandardResolver>(footer, C, G));
    rs.push_back(std::make_unique<fdb::WalkResolver>(footer, C, G));   // index 1 = walk baseline
    rs.push_back(std::make_unique<fdb::IndexResolver>(footer, fi));
    if (have_mod) rs.push_back(std::make_unique<fdb::ModularResolver>(mod, C, G));
    const size_t WALK = 1;

    // Fidelity: every resolver must agree with the walk on placement + stats.
    for (const std::vector<char>& want : {Mask(C, std::min(std::max(proj, 1), C)), Mask(C, C)}) {
      Placement ref = rs[WALK]->Resolve(want);
      for (const auto& r : rs)
        if (r->Resolve(want) != ref)
          throw std::runtime_error(r->name() + " disagrees with walk on placement+stats");
    }

    if (sweep) {
      std::vector<int> ks;
      for (int k = 1; k < C; k *= 2) ks.push_back(k);
      ks.push_back(C);
      std::printf("projected");
      for (const auto& r : rs) std::printf(",%s_us", r->name().c_str());
      std::printf("\n");
      for (int K : ks) {
        std::vector<char> want = Mask(C, K);
        std::printf("%d", K);
        for (const auto& r : rs) std::printf(",%.3f", Time(*r, want));
        std::printf("\n");
      }
      return 0;
    }

    const int K = std::max(1, std::min(proj, C));
    std::vector<char> want_proj = Mask(C, K), want_all = Mask(C, C);
    std::vector<double> tp(rs.size()), ta(rs.size());
    for (size_t i = 0; i < rs.size(); ++i) { tp[i] = Time(*rs[i], want_proj); ta[i] = Time(*rs[i], want_all); }

    std::printf("jumptable file  %s  (footer %u B)\n", jt_path.c_str(), flen);
    if (have_mod) std::printf("modular file    %s  (%zu B)\n", mod_path.c_str(), mod.size());
    std::printf("columns %d   row_groups %d   column_chunks %d   projected %d of %d\n",
                C, G, C * G, K, C);
    std::printf("info per chunk  {data_page_offset, total_compressed_size, null_count, min, max}\n\n");
    std::printf("%-10s %14s %14s %14s\n", "resolve", "projected_us", "all_us", "proj vs walk");
    for (size_t i = 0; i < rs.size(); ++i)
      std::printf("%-10s %14.3f %14.3f %13.1fx\n", rs[i]->name().c_str(), tp[i], ta[i],
                  tp[i] > 0 ? tp[WALK] / tp[i] : 0.0);
    return 0;
  } catch (const std::exception& e) {
    std::fprintf(stderr, "error: %s\n", e.what());
    return 1;
  }
}
