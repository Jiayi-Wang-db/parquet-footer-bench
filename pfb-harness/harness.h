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

// kProject: read m.selected columns (all row groups). kFull: read everything.
// kRowRange: read m.selected columns, but only the rows in [sel_first_row,
// sel_first_row + sel_num_rows) within each row group -- a row-selective scan.
// Only layouts that carry a page index can act on it: they resolve to the dict
// page plus the data pages overlapping the window; layouts without page info can
// only fetch the whole chunk.
enum class Query { kProject, kFull, kRowRange };

// The parameters that define a synthetic file: a grid of N columns x R row
// groups (so N*R chunks), each chunk holding 1 dictionary page + P data pages.
// A query projects K of the N columns.
struct Shape {
  int columns;             // N
  int row_groups;          // R
  int pages_per_chunk;     // P (data pages; a dictionary page is always added)
  int projected;           // K
  int64_t rows_per_group;  // rows in each row group (total rows = rows_per_group * R)
  const char* name;        // human label
};

// Ground truth derived from a Shape: the real placement every layout must
// reproduce, plus the per-page lengths a page-index-style layout would store.
struct Model {
  int columns, row_groups, pages_per_chunk;
  int64_t rows_per_group = 0;  // rows in each row group
  int64_t base = 4;            // first byte after the Parquet magic header

  // Row window for kRowRange queries, applied within each row group.
  int64_t sel_first_row = 0;
  int64_t sel_num_rows = 0;

  std::vector<int64_t> chunk_off;   // [columns*row_groups], index = c*R + r
  std::vector<int64_t> chunk_size;  // [columns*row_groups]
  std::vector<int64_t> block_len;   // [(P+1) per chunk]: dict page, then P data pages
  std::vector<int64_t> block_rows;  // [(P+1) per chunk]: rows in each page (dict page = 0)
  std::vector<int> selected;        // projected column ids, sorted

  int chunks() const { return columns * row_groups; }
  int64_t total_rows() const { return rows_per_group * row_groups; }

  // The truth a Resolve must match for kProject/kFull (whole column chunks).
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

  // The truth a page-aware Resolve must match for kRowRange: per selected chunk,
  // the dictionary page plus every data page overlapping the row window.
  Resolved ExpectedRowsPages() const {
    Resolved out;
    int per = pages_per_chunk + 1;
    int64_t lo = sel_first_row, hi = sel_first_row + sel_num_rows;
    for (int c : selected) {
      for (int r = 0; r < row_groups; ++r) {
        int cc = c * row_groups + r;
        int fb = cc * per;
        int64_t pos = chunk_off[cc];
        out.push_back({pos, block_len[fb]});  // dictionary page (always fetched)
        pos += block_len[fb];
        int64_t cursor = 0;
        for (int p = 1; p < per; ++p) {
          int b = fb + p;
          if (cursor < hi && lo < cursor + block_rows[b]) out.push_back({pos, block_len[b]});
          pos += block_len[b];
          cursor += block_rows[b];
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
  // True if the layout carries a page index and can resolve kRowRange to pages.
  // Others fall back to whole chunks for kRowRange (no row skipping possible).
  bool page_aware = false;
};

// All registered layouts. Add a candidate by appending one Layout here.
const std::vector<Layout>& Layouts();

}  // namespace pfb

#endif  // PFB_HARNESS_H_
