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

// Driver for the footer-layout harness.
//
//   pfb_bench                     # run the built-in shapes, CSV to stdout
//   pfb_bench --columns 5000 --row-groups 10 --pages 1 --projected 4
//   pfb_bench --list             # list registered layouts
//
// For every (shape x layout) it cross-checks that the layout resolves to the
// exact placement the model says is correct (for both a projected and a full
// scan), then reports the measured wire size and the build / resolve timings.
// Resolve is the read-path cost a reader pays; build is the writer-side cost.

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "pfb/harness.h"

namespace {

using pfb::ChunkLocator;
using pfb::Layout;
using pfb::Model;
using pfb::Query;
using pfb::Resolved;
using pfb::Shape;

// Built-in shapes spanning the regimes that distinguish the layouts: many narrow
// chunks, few wide chunks, many row groups, and many pages per chunk.
const std::vector<Shape> kBuiltinShapes = {
    {5000, 10, 1, 4, "wide_1pg"},
    {200, 5, 20, 4, "latemat_20pg"},
    {10, 1000, 25, 4, "many_rg_25pg"},
    {10, 5, 5120, 4, "many_pages"},
};

bool Equal(const Resolved& a, const Resolved& b) {
  if (a.size() != b.size()) return false;
  for (size_t i = 0; i < a.size(); ++i)
    if (!(a[i] == b[i])) return false;
  return true;
}

// Run fn enough times to accumulate ~50ms, return per-call microseconds.
template <typename Fn>
double TimeUs(Fn fn) {
  using clock = std::chrono::steady_clock;
  int iters = 1;
  for (;;) {
    auto t0 = clock::now();
    for (int i = 0; i < iters; ++i) fn();
    double ns = std::chrono::duration<double, std::nano>(clock::now() - t0).count();
    if (ns > 50e6 || iters > (1 << 24)) return ns / iters / 1000.0;
    iters *= 2;
  }
}

struct Row {
  std::string shape, layout;
  int columns, row_groups, pages, projected;
  size_t bytes;
  double build_us, resolve_project_us, resolve_full_us;
};

// Returns false if any layout fails the fidelity cross-check.
bool RunShape(const Shape& shape, std::vector<Row>& rows) {
  Model m = pfb::BuildModel(shape);
  Resolved want_project = m.Expected(Query::kProject);
  Resolved want_full = m.Expected(Query::kFull);
  bool ok = true;
  for (const Layout& L : pfb::Layouts()) {
    std::string blob = L.build(m);
    Resolved got_project = L.resolve(blob, m, Query::kProject);
    Resolved got_full = L.resolve(blob, m, Query::kFull);
    if (!Equal(got_project, want_project) || !Equal(got_full, want_full)) {
      std::fprintf(stderr, "FIDELITY FAIL: shape=%s layout=%s\n", shape.name, L.name.c_str());
      ok = false;
      continue;
    }
    Row row;
    row.shape = shape.name;
    row.layout = L.name;
    row.columns = shape.columns;
    row.row_groups = shape.row_groups;
    row.pages = shape.pages_per_chunk;
    row.projected = shape.projected;
    row.bytes = blob.size();
    row.build_us = TimeUs([&] { volatile size_t s = L.build(m).size(); (void)s; });
    row.resolve_project_us = TimeUs([&] {
      Resolved r = L.resolve(blob, m, Query::kProject);
      if (r.empty()) std::abort();
    });
    row.resolve_full_us = TimeUs([&] {
      Resolved r = L.resolve(blob, m, Query::kFull);
      if (r.empty()) std::abort();
    });
    rows.push_back(std::move(row));
  }
  return ok;
}

}  // namespace

int main(int argc, char** argv) {
  std::vector<Shape> shapes = kBuiltinShapes;
  int columns = -1, row_groups = 1, pages = 1, projected = 4;
  for (int i = 1; i < argc; ++i) {
    auto next = [&](const char* flag) -> const char* {
      if (i + 1 >= argc) {
        std::fprintf(stderr, "missing value for %s\n", flag);
        std::exit(2);
      }
      return argv[++i];
    };
    if (!std::strcmp(argv[i], "--list")) {
      for (const Layout& L : pfb::Layouts()) std::printf("%s\n", L.name.c_str());
      return 0;
    } else if (!std::strcmp(argv[i], "--columns")) {
      columns = std::atoi(next("--columns"));
    } else if (!std::strcmp(argv[i], "--row-groups")) {
      row_groups = std::atoi(next("--row-groups"));
    } else if (!std::strcmp(argv[i], "--pages")) {
      pages = std::atoi(next("--pages"));
    } else if (!std::strcmp(argv[i], "--projected")) {
      projected = std::atoi(next("--projected"));
    } else if (!std::strcmp(argv[i], "--help") || !std::strcmp(argv[i], "-h")) {
      std::printf(
          "usage: pfb_bench [--columns N --row-groups R --pages P --projected K] [--list]\n"
          "with no shape flags, runs the built-in shapes.\n");
      return 0;
    } else {
      std::fprintf(stderr, "unknown arg: %s\n", argv[i]);
      return 2;
    }
  }
  if (columns > 0) shapes = {{columns, row_groups, pages, projected, "custom"}};

  std::vector<Row> rows;
  bool ok = true;
  for (const Shape& s : shapes) ok &= RunShape(s, rows);

  std::printf("shape,columns,row_groups,pages,projected,layout,bytes,build_us,resolve_project_us,resolve_full_us\n");
  for (const Row& r : rows) {
    std::printf("%s,%d,%d,%d,%d,%s,%zu,%.3f,%.3f,%.3f\n", r.shape.c_str(), r.columns, r.row_groups,
                r.pages, r.projected, r.layout.c_str(), r.bytes, r.build_us, r.resolve_project_us,
                r.resolve_full_us);
  }
  if (!ok) {
    std::fprintf(stderr, "\none or more layouts failed the fidelity cross-check\n");
    return 1;
  }
  return 0;
}
