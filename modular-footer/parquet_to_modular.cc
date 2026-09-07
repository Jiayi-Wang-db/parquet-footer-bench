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

// parquet_to_modular: read a Parquet file's footer and translate it into the
// modular-footer layout (see ModularFooter.thrift), writing the modular footer
// bytes and a per-module size breakdown.
//
// It uses the standard Apache Thrift compact parser end to end: the current
// footer (parquet.thrift FileMetaData) is deserialized with TCompactProtocol,
// and each modular-footer module is serialized back with TCompactProtocol. The
// only hand-encoded bytes are the ArrayPage payloads, which are the format's own
// bit-packed arrays (pfb/bitpack.h).
//
// This translates the always-present modules -- schema and placement -- plus the
// ModularFooter directory root. Row-group statistics and the page indexes are
// separate optional modules; translating them is left as a follow-up (marked
// TODO below), since placement is the size- and decode-critical core.
//
// Build (needs the Apache Thrift compiler + C++ runtime, e.g. `apt install
// thrift-compiler libthrift-dev` or a source build):
//
//   thrift --gen cpp -out gen modular-footer/parquet.thrift
//   thrift --gen cpp -out gen modular-footer/ModularFooter.thrift
//   g++ -std=c++17 -O2 -Igen -Iinclude modular-footer/parquet_to_modular.cc \
//       gen/parquet_types.cpp gen/ModularFooter_types.cpp -lthrift -o parquet_to_modular
//
// Run:
//   ./parquet_to_modular input.parquet [output.modular]

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include "pfb/bitpack.h"        // BitWidth, PackBits (fixed-width LSB-first packing)
#include "parquet_types.h"      // generated from parquet.thrift        (namespace parquet)
#include "ModularFooter_types.h" // generated from ModularFooter.thrift (namespace parquet::modular)

namespace mf = parquet::modular;
using apache::thrift::protocol::TCompactProtocol;
using apache::thrift::transport::TMemoryBuffer;

// -------------------------------------------------------------- footer I/O

// Read the FileMetaData footer bytes from the tail of a Parquet file. The last 8
// bytes are a little-endian u32 footer length followed by the "PAR1" magic.
static std::string ReadFooter(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) throw std::runtime_error("cannot open " + path);
  in.seekg(0, std::ios::end);
  const std::streamoff size = in.tellg();
  if (size < 8) throw std::runtime_error("file too small to be Parquet");

  char tail[8];
  in.seekg(size - 8);
  in.read(tail, 8);
  if (std::memcmp(tail + 4, "PAR1", 4) != 0)
    throw std::runtime_error("missing PAR1 magic (encrypted or not a Parquet file)");
  const uint32_t footer_len = static_cast<uint8_t>(tail[0]) |
                              (static_cast<uint8_t>(tail[1]) << 8) |
                              (static_cast<uint8_t>(tail[2]) << 16) |
                              (static_cast<uint32_t>(static_cast<uint8_t>(tail[3])) << 24);
  if (static_cast<std::streamoff>(footer_len) + 8 > size)
    throw std::runtime_error("footer length exceeds file size");

  std::string footer(footer_len, '\0');
  in.seekg(size - 8 - static_cast<std::streamoff>(footer_len));
  in.read(&footer[0], footer_len);
  if (!in) throw std::runtime_error("short read of footer");
  return footer;
}

// -------------------------------------------------------------- Thrift helpers

// Deserialize a compact-Thrift struct from raw bytes using the standard parser.
template <typename T>
static void ReadCompact(const std::string& bytes, T& out) {
  auto buf = std::make_shared<TMemoryBuffer>();
  buf->resetBuffer(reinterpret_cast<uint8_t*>(const_cast<char*>(bytes.data())),
                   static_cast<uint32_t>(bytes.size()), TMemoryBuffer::OBSERVE);
  TCompactProtocol proto(buf);
  out.read(&proto);
}

// Serialize a compact-Thrift struct to bytes using the standard serializer.
template <typename T>
static std::string WriteCompact(const T& in) {
  auto buf = std::make_shared<TMemoryBuffer>();
  TCompactProtocol proto(buf);
  in.write(&proto);
  uint8_t* p = nullptr;
  uint32_t n = 0;
  buf->getBuffer(&p, &n);
  return std::string(reinterpret_cast<char*>(p), n);
}

// -------------------------------------------------------------- array pages

// One integer array, encoded as a dense BITSET ArrayPage: every position is
// present (num_present == num_values), so no validity bitmap is stored and the
// payload is just the bit-packed values -- the same bytes as a plain packed
// array, addressable in O(1).
static mf::ArrayPage MakeDenseBitset(const std::vector<uint64_t>& vals) {
  uint64_t maxval = 0;
  for (uint64_t v : vals) maxval = v > maxval ? v : maxval;
  const int width = pfb::BitWidth(maxval);

  mf::BitsetParameters bp;
  bp.__set_value_bit_width(static_cast<int8_t>(width));
  bp.__set_num_present(static_cast<int32_t>(vals.size()));  // == num_values => no bitmap

  mf::ArrayEncodingParameters params;
  params.__set_bitset(bp);

  mf::ArrayPage ap;
  ap.__set_data(pfb::PackBits(vals, width));
  ap.__set_encoding(mf::ArrayEncoding::BITSET);
  ap.__set_num_values(static_cast<int32_t>(vals.size()));
  ap.__set_parameters(params);
  return ap;
}

// -------------------------------------------------------------- translation

int main(int argc, char** argv) {
  if (argc < 2) {
    std::fprintf(stderr, "usage: %s input.parquet [output.modular]\n", argv[0]);
    return 2;
  }
  const std::string in_path = argv[1];
  const std::string out_path = argc >= 3 ? argv[2] : in_path + ".modular";

  try {
    const std::string footer = ReadFooter(in_path);
    parquet::FileMetaData fmd;
    ReadCompact(footer, fmd);

    if (fmd.row_groups.empty()) throw std::runtime_error("footer has no row groups");
    const int G = static_cast<int>(fmd.row_groups.size());
    const int C = static_cast<int>(fmd.row_groups[0].columns.size());  // leaf column chunks / rg
    const int64_t N = static_cast<int64_t>(C) * G;                     // total column chunks

    // ---- placement, column-major: chunk (column c, row group g) is at c*G + g.
    std::vector<uint64_t> data_page_offsets(N), total_compressed(N), total_uncompressed(N),
        num_values(N), codecs(N), is_fully_dict(N, 0);
    std::vector<uint64_t> physical_types(C);                 // one per column (uniform across rgs)
    std::vector<uint64_t> first_dict(N + 1);                 // cumulative index into dict_offsets
    std::vector<uint64_t> dict_offsets;                      // flat, only for chunks that have one
    first_dict[0] = 0;

    for (int c = 0; c < C; ++c) {
      physical_types[c] = static_cast<uint64_t>(
          static_cast<int>(fmd.row_groups[0].columns[c].meta_data.type));
      for (int g = 0; g < G; ++g) {
        const parquet::ColumnChunk& cc = fmd.row_groups[g].columns[c];
        const parquet::ColumnMetaData& m = cc.meta_data;
        const int64_t idx = static_cast<int64_t>(c) * G + g;
        data_page_offsets[idx] = static_cast<uint64_t>(m.data_page_offset);
        total_compressed[idx] = static_cast<uint64_t>(m.total_compressed_size);
        total_uncompressed[idx] = static_cast<uint64_t>(m.total_uncompressed_size);
        num_values[idx] = static_cast<uint64_t>(m.num_values);
        codecs[idx] = static_cast<uint64_t>(static_cast<int>(m.codec));

        const bool has_dict = m.__isset.dictionary_page_offset;
        first_dict[idx + 1] = first_dict[idx] + (has_dict ? 1 : 0);
        if (has_dict) dict_offsets.push_back(static_cast<uint64_t>(m.dictionary_page_offset));

        // "fully dictionary encoded" iff every data page is dictionary-encoded.
        if (m.__isset.encoding_stats && !m.encoding_stats.empty()) {
          bool any_data = false, all_dict = true;
          for (const auto& es : m.encoding_stats) {
            const int pt = static_cast<int>(es.page_type);       // DATA_PAGE=0, DATA_PAGE_V2=3
            if (pt == 0 || pt == 3) {
              any_data = true;
              const int enc = static_cast<int>(es.encoding);     // dict: PLAIN_DICTIONARY=2, RLE_DICTIONARY=8
              if (enc != 2 && enc != 8) all_dict = false;
            }
          }
          is_fully_dict[idx] = (any_data && all_dict) ? 1 : 0;
        }
      }
    }

    mf::PlacementModule placement;
    placement.__set_data_page_offsets(MakeDenseBitset(data_page_offsets));
    placement.__set_first_dictionary_pages(MakeDenseBitset(first_dict));
    placement.__set_dictionary_page_offsets(MakeDenseBitset(dict_offsets));
    placement.__set_total_compressed_sizes(MakeDenseBitset(total_compressed));
    placement.__set_total_uncompressed_sizes(MakeDenseBitset(total_uncompressed));
    placement.__set_num_values(MakeDenseBitset(num_values));
    placement.__set_codecs(MakeDenseBitset(codecs));
    placement.__set_physical_types(MakeDenseBitset(physical_types));
    placement.__set_is_fully_dictionary_encoded(MakeDenseBitset(is_fully_dict));

    // ---- schema: read in full, so it stays ordinary Thrift (reuse the parsed
    // SchemaElement list unchanged; both specs share parquet.SchemaElement).
    mf::SchemaModule schema;
    schema.__set_schema(fmd.schema);
    if (fmd.__isset.column_orders) schema.__set_column_orders(fmd.column_orders);

    // TODO(follow-up): RowGroupStatisticsModule and the OFFSET_INDEX/COLUMN_INDEX
    // page-index modules. They are optional and translate the same way -- per
    // (leaf column, row group) arrays, sparse ones as PRESENT_INDEX, min/max with
    // common-prefix stripping -- but are omitted here to keep the tool focused on
    // the always-present placement core.

    // ---- serialize the modules, then lay them out and build the directory root.
    const std::string schema_blob = WriteCompact(schema);
    const std::string placement_blob = WriteCompact(placement);

    auto entry = [](mf::ModuleKind::type kind, int64_t off, int64_t len) {
      mf::ModuleLocation loc;
      loc.__set_offset(off);
      loc.__set_length(len);
      mf::ModuleDirectoryEntry e;
      e.__set_kind(kind);
      e.__set_location(loc);
      return e;
    };

    const int64_t schema_off = 0;
    const int64_t placement_off = static_cast<int64_t>(schema_blob.size());

    mf::ModularFooter root;
    root.__set_version(fmd.version);
    root.__set_num_row_groups(G);
    root.__set_num_columns(C);
    root.__set_num_rows(fmd.num_rows);
    std::vector<int64_t> rg_rows;
    rg_rows.reserve(G);
    for (const auto& rg : fmd.row_groups) rg_rows.push_back(rg.num_rows);
    root.__set_row_group_num_rows(rg_rows);
    root.__set_modules({
        entry(mf::ModuleKind::SCHEMA, schema_off, static_cast<int64_t>(schema_blob.size())),
        entry(mf::ModuleKind::PLACEMENT, placement_off,
              static_cast<int64_t>(placement_blob.size())),
    });
    const std::string root_blob = WriteCompact(root);

    // Output layout: [schema module][placement module][ModularFooter root]. The
    // directory offsets above are absolute within this buffer; the outer file
    // framing that locates the root is specified separately.
    std::ofstream out(out_path, std::ios::binary | std::ios::trunc);
    if (!out) throw std::runtime_error("cannot open " + out_path + " for writing");
    out.write(schema_blob.data(), schema_blob.size());
    out.write(placement_blob.data(), placement_blob.size());
    out.write(root_blob.data(), root_blob.size());
    out.close();

    const size_t modular_total = schema_blob.size() + placement_blob.size() + root_blob.size();
    std::printf("input                 %s\n", in_path.c_str());
    std::printf("oss_footer_bytes      %zu\n", footer.size());
    std::printf("columns               %d\n", C);
    std::printf("row_groups            %d\n", G);
    std::printf("column_chunks         %lld\n", static_cast<long long>(N));
    std::printf("rows                  %lld\n", static_cast<long long>(fmd.num_rows));
    std::printf("modular_total_bytes   %zu\n", modular_total);
    std::printf("  schema_module       %zu\n", schema_blob.size());
    std::printf("  placement_module    %zu\n", placement_blob.size());
    std::printf("  directory_root      %zu\n", root_blob.size());
    std::printf("wrote                 %s (%zu bytes)\n", out_path.c_str(), modular_total);
    return 0;
  } catch (const std::exception& e) {
    std::fprintf(stderr, "error: %s\n", e.what());
    return 1;
  }
}
