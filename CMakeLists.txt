# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

cmake_minimum_required(VERSION 3.16)
project(parquet_footer_bench CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

add_compile_options(-Wall -Wextra)

# No third-party dependencies: the Thrift Compact codec is vendored in
# include/pfb/thrift_compact.h so this builds with any C++17 compiler.
add_library(pfb_layouts STATIC src/layouts.cc)
target_include_directories(pfb_layouts PUBLIC include)

add_executable(pfb_bench src/main.cc)
target_link_libraries(pfb_bench PRIVATE pfb_layouts)

# Footer -> modular-footer converter (reads a real Parquet footer). Uses a
# vendored compact codec + include/pfb/bitpack.h; no third-party dependencies.
add_executable(modular_footer_convert modular-footer/parquet_to_modular.cc)
target_include_directories(modular_footer_convert PRIVATE include)

add_executable(jumptable_footer_convert jumptable-footer/parquet_to_jumptable.cc)
add_executable(footer_decode_bench footer-decode-bench/footer_decode_bench.cc)

enable_testing()
add_executable(pfb_fidelity_test test/fidelity_test.cc)
target_link_libraries(pfb_fidelity_test PRIVATE pfb_layouts)
add_test(NAME fidelity COMMAND pfb_fidelity_test)
