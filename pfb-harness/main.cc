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

#include "harness.h"

namespace {

using pfb::ChunkLocator;
using pfb::Layout;
using pfb::Model;
using pfb::Query;
using pfb::Resolved;
using pfb::Shape;

// Built-in shapes spanning the regimes that distinguish the layouts. Fields:
// {columns, row_groups, pages_per_chunk, projected, rows_per_group, name}.
//
// `analytics_8m` is a concrete, realistic case: a 256-column table written as a
// single file of 8 row groups x 1,000,000 rows (8M rows total), each column
// chunk split into 64 ~16K-row data pages, with a 16-column projection -- the
// kind of wide analytics scan where footer resolution shows up. The others are
// stress regimes: many narrow chunks, many row groups, and many pages per chunk.
const std::vector<Shape> kBuiltinShapes = {
    {256, 8, 64, 16, 1'000'000, "analytics_8m"},
    {5000, 10, 1, 4, 20'000, "wide_1pg"},
    {200, 5, 20, 4, 400'000, "latemat_20pg"},
    {10, 1000, 25, 4, 500'000, "many_rg_25pg"},
    {10, 5, 5120, 4, 100'000'000, "many_pages"},
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

// Total column-data bytes a query would fetch (sum of locator sizes).
int64_t BytesFetched(const Resolved& r) {
  int64_t n = 0;
  for (const auto& c : r) n += c.size;
  return n;
}

struct Row {
  std::string shape, layout;
  int columns, row_groups, pages, projected;
  int64_t rows_per_group, total_rows;
  size_t bytes;
  double build_us, resolve_project_us, resolve_full_us;
  bool page_aware;
  int64_t rowsel_num_rows, rowsel_bytes_fetched;
  double resolve_rowsel_us;
};

// Optional row-window override from --row-select (negative = use the default).
int64_t g_sel_first = -1, g_sel_num = -1;

// Returns false if any layout fails the fidelity cross-check.
bool RunShape(const Shape& shape, std::vector<Row>& rows) {
  Model m = pfb::BuildModel(shape);
  if (g_sel_num >= 0) {
    m.sel_first_row = g_sel_first;
    m.sel_num_rows = g_sel_num;
  }
  Resolved want_project = m.Expected(Query::kProject);
  Resolved want_full = m.Expected(Query::kFull);
  Resolved want_rows_pages = m.ExpectedRowsPages();  // truth for page-aware layouts
  bool ok = true;
  for (const Layout& L : pfb::Layouts()) {
    std::string blob = L.build(m);
    Resolved got_project = L.resolve(blob, m, Query::kProject);
    Resolved got_full = L.resolve(blob, m, Query::kFull);
    Resolved got_rows = L.resolve(blob, m, Query::kRowRange);
    // A page-aware layout must resolve a row range to pages; a layout without
    // page info can only fetch whole chunks, so its truth is the projection.
    const Resolved& want_rows = L.page_aware ? want_rows_pages : want_project;
    if (!Equal(got_project, want_project) || !Equal(got_full, want_full) ||
        !Equal(got_rows, want_rows)) {
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
    row.rows_per_group = shape.rows_per_group;
    row.total_rows = m.total_rows();
    row.bytes = blob.size();
    row.page_aware = L.page_aware;
    row.rowsel_num_rows = m.sel_num_rows;
    row.rowsel_bytes_fetched = BytesFetched(got_rows);
    row.build_us = TimeUs([&] { volatile size_t s = L.build(m).size(); (void)s; });
    row.resolve_project_us = TimeUs([&] {
      Resolved r = L.resolve(blob, m, Query::kProject);
      if (r.empty()) std::abort();
    });
    row.resolve_full_us = TimeUs([&] {
      Resolved r = L.resolve(blob, m, Query::kFull);
      if (r.empty()) std::abort();
    });
    row.resolve_rowsel_us = TimeUs([&] {
      Resolved r = L.resolve(blob, m, Query::kRowRange);
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
  int64_t rows = 1'000'000;
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
    } else if (!std::strcmp(argv[i], "--rows")) {
      rows = std::atoll(next("--rows"));
    } else if (!std::strcmp(argv[i], "--row-select")) {
      // FIRST:COUNT -- select COUNT rows starting at FIRST within each row group.
      const char* v = next("--row-select");
      const char* colon = std::strchr(v, ':');
      g_sel_first = std::atoll(v);
      g_sel_num = colon ? std::atoll(colon + 1) : 1;
    } else if (!std::strcmp(argv[i], "--help") || !std::strcmp(argv[i], "-h")) {
      std::printf(
          "usage: pfb_bench [--columns N --row-groups R --pages P --projected K --rows ROWS]\n"
          "                 [--row-select FIRST:COUNT] [--list]\n"
          "  --rows        rows per row group (default 1000000); total = ROWS * row-groups.\n"
          "  --row-select  row window within each row group for the row-range query\n"
          "                (default: 1000 rows at the row-group midpoint).\n"
          "with no shape flags, runs the built-in shapes.\n");
      return 0;
    } else {
      std::fprintf(stderr, "unknown arg: %s\n", argv[i]);
      return 2;
    }
  }
  if (columns > 0) shapes = {{columns, row_groups, pages, projected, rows, "custom"}};

  std::vector<Row> result_rows;
  bool ok = true;
  for (const Shape& s : shapes) ok &= RunShape(s, result_rows);

  std::printf(
      "shape,columns,row_groups,pages,projected,rows_per_group,total_rows,layout,bytes,build_us,"
      "resolve_project_us,resolve_full_us,page_aware,rowsel_rows,rowsel_resolve_us,"
      "rowsel_bytes_fetched\n");
  for (const Row& r : result_rows) {
    std::printf("%s,%d,%d,%d,%d,%lld,%lld,%s,%zu,%.3f,%.3f,%.3f,%d,%lld,%.3f,%lld\n", r.shape.c_str(),
                r.columns, r.row_groups, r.pages, r.projected,
                static_cast<long long>(r.rows_per_group), static_cast<long long>(r.total_rows),
                r.layout.c_str(), r.bytes, r.build_us, r.resolve_project_us, r.resolve_full_us,
                r.page_aware ? 1 : 0, static_cast<long long>(r.rowsel_num_rows),
                r.resolve_rowsel_us, static_cast<long long>(r.rowsel_bytes_fetched));
  }
  if (!ok) {
    std::fprintf(stderr, "\none or more layouts failed the fidelity cross-check\n");
    return 1;
  }
  return 0;
}
