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

// Core of the footer-layout harness.
//
// A *layout* is a candidate physical encoding of the placement information a
// Parquet reader needs before it can fetch column data: for every column chunk,
// where it starts and how big it is. Different layouts trade wire size against
// the cost of resolving that placement at read time.
//
// The harness measures every candidate the same way, with no estimates:
//
//   params (Shape) --Build--> serialized thrift blob --Resolve--> Resolved
//
// where `Resolved` is exactly what a reader hands to its fetch scheduler: the
// list of {offset, size} for the projected column chunks. Because every layout
// produces the *same* Resolved for the same file, a cross-check can assert they
// all agree -- so a layout is only compared after it is proven correct.

#ifndef PFB_HARNESS_H_
#define PFB_HARNESS_H_

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace pfb {

// What the reader resolves a chunk to: a byte range to fetch.
struct ChunkLocator {
  int64_t offset;
  int64_t size;
  bool operator==(const ChunkLocator& o) const {
    return offset == o.offset && size == o.size;
  }
};
using Resolved = std::vector<ChunkLocator>;

enum class Query { kProject, kFull };

// The parameters that define a synthetic file: a grid of N columns x R row
// groups (so N*R chunks), each chunk holding 1 dictionary page + P data pages.
// A query projects K of the N columns.
struct Shape {
  int columns;          // N
  int row_groups;       // R
  int pages_per_chunk;  // P (data pages; a dictionary page is always added)
  int projected;        // K
  const char* name;     // human label
};

// Ground truth derived from a Shape: the real placement every layout must
// reproduce, plus the per-page lengths a page-index-style layout would store.
struct Model {
  int columns, row_groups, pages_per_chunk;
  int64_t base = 4;  // first byte after the Parquet magic header

  std::vector<int64_t> chunk_off;   // [columns*row_groups], index = c*R + r
  std::vector<int64_t> chunk_size;  // [columns*row_groups]
  std::vector<int64_t> block_len;   // [(P+1) per chunk]: dict page, then P data pages
  std::vector<int> selected;        // projected column ids, sorted

  int chunks() const { return columns * row_groups; }

  // The truth a Resolve must match, for a given query.
  Resolved Expected(Query q) const {
    Resolved out;
    if (q == Query::kFull) {
      for (int cc = 0; cc < chunks(); ++cc) out.push_back({chunk_off[cc], chunk_size[cc]});
    } else {
      for (int c : selected) {
        for (int r = 0; r < row_groups; ++r) {
          int cc = c * row_groups + r;
          out.push_back({chunk_off[cc], chunk_size[cc]});
        }
      }
    }
    return out;
  }
};

// Build a deterministic Model from a Shape (fixed seed -> reproducible blobs).
Model BuildModel(const Shape& shape);

// A candidate layout: serialize placement to a thrift blob, then resolve a blob
// back to the chunk locators a query needs. The two halves are deliberately
// separate so the harness can time them independently (Resolve is the read-path
// cost a reader actually pays; Build is the writer-side cost).
struct Layout {
  std::string name;
  std::function<std::string(const Model&)> build;
  std::function<Resolved(const std::string& blob, const Model&, Query)> resolve;
};

// All registered layouts. Add a candidate by appending one Layout here.
const std::vector<Layout>& Layouts();

}  // namespace pfb

#endif  // PFB_HARNESS_H_
