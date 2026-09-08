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

// Dependency-free fidelity test: for a spread of shapes, every layout must
// build->resolve back to the exact placement the model says is correct, for both
// a projected and a full scan. This is the invariant that makes the harness's
// size/timing numbers meaningful -- a layout is only ever compared after it is
// proven to produce the right answer. Exits non-zero on any mismatch.

#include <cstdio>
#include <vector>

#include "harness.h"

namespace {

using pfb::Layout;
using pfb::Model;
using pfb::Query;
using pfb::Resolved;
using pfb::Shape;

bool Equal(const Resolved& a, const Resolved& b) {
  if (a.size() != b.size()) return false;
  for (size_t i = 0; i < a.size(); ++i)
    if (!(a[i] == b[i])) return false;
  return true;
}

int g_failures = 0;

void Fail(const Shape& shape, const Layout& L, const char* query) {
  std::fprintf(stderr, "FAIL: shape=%s layout=%s query=%s\n", shape.name, L.name.c_str(), query);
  ++g_failures;
}

void Check(const Shape& shape) {
  Model m = pfb::BuildModel(shape);
  for (const Layout& L : pfb::Layouts()) {
    std::string blob = L.build(m);
    if (!Equal(L.resolve(blob, m, Query::kProject), m.Expected(Query::kProject)))
      Fail(shape, L, "project");
    if (!Equal(L.resolve(blob, m, Query::kFull), m.Expected(Query::kFull))) Fail(shape, L, "full");
    // Row-range: page-aware layouts must resolve to pages; the rest to whole chunks.
    const Resolved want_rows = L.page_aware ? m.ExpectedRowsPages() : m.Expected(Query::kProject);
    if (!Equal(L.resolve(blob, m, Query::kRowRange), want_rows)) Fail(shape, L, "row-range");
  }
}

}  // namespace

int main() {
  // {columns, row_groups, pages_per_chunk, projected, rows_per_group, name}
  const std::vector<Shape> shapes = {
      {1, 1, 0, 1, 0, "degenerate"},
      {3, 1, 1, 1, 1000, "tiny"},
      {5000, 10, 1, 4, 20'000, "wide"},
      {200, 5, 20, 4, 400'000, "latemat"},
      {10, 1000, 25, 4, 500'000, "many_rg"},
      {10, 5, 5120, 4, 1'000'000, "many_pages"},
      {64, 8, 3, 64, 90'000, "project_all"},
      {100, 1, 0, 7, 0, "no_data_pages"},
      {256, 8, 64, 16, 1'000'000, "analytics_8m"},
  };
  for (const Shape& s : shapes) Check(s);
  if (g_failures) {
    std::fprintf(stderr, "%d fidelity check(s) failed\n", g_failures);
    return 1;
  }
  std::printf("all fidelity checks passed (%zu shapes x %zu layouts)\n", shapes.size(),
              pfb::Layouts().size());
  return 0;
}
