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

// Writes empty_data_pages_v1.parquet and empty_data_pages_v2.parquet, see
// empty_data_pages.md

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/io/file.h"
#include "parquet/column_page.h"
#include "parquet/column_writer.h"
#include "parquet/exception.h"
#include "parquet/file_writer.h"
#include "parquet/metadata.h"
#include "parquet/schema.h"

using namespace parquet;

using Pages = std::vector<std::shared_ptr<DataPage>>;

// DataPageV1 of PLAIN encoded values of a required INT64 column
auto Int64PageV1(std::vector<int64_t> values) {
  auto num_values = static_cast<int32_t>(values.size());
  auto buffer = ::arrow::Buffer::FromVector(std::move(values));
  return std::make_shared<DataPageV1>(buffer, num_values, Encoding::PLAIN,
                                      /*definition_level_encoding=*/Encoding::RLE,
                                      /*repetition_level_encoding=*/Encoding::RLE,
                                      buffer->size());
}

// DataPageV2 of PLAIN encoded values of a required INT64 column, so without levels
auto Int64PageV2(std::vector<int64_t> values) {
  auto num_values = static_cast<int32_t>(values.size());
  auto buffer = ::arrow::Buffer::FromVector(std::move(values));
  return std::make_shared<DataPageV2>(buffer, num_values, /*num_nulls=*/0,
                                      /*num_rows=*/num_values, Encoding::PLAIN,
                                      /*definition_levels_byte_length=*/0,
                                      /*repetition_levels_byte_length=*/0,
                                      buffer->size());
}

// Writes the pages of a column chunk as they are, without any further encoding, and
// returns the number of values written
auto WriteColumnChunk(const std::shared_ptr<ArrowOutputStream>& sink,
                      ColumnChunkMetaDataBuilder* metadata, const Pages& pages) {
  auto pager = PageWriter::Open(sink, Compression::UNCOMPRESSED, metadata);
  auto num_values = int64_t{0};
  for (const auto& page : pages) {
    pager->WriteDataPage(*page);
    num_values += page->num_values();
  }
  pager->Close(/*has_dictionary=*/false, /*fallback=*/false);
  return num_values;
}

// Writes a file with a single required INT64 column, stored in a single row group as a
// column chunk of the given pages
void WriteInt64ColumnFile(const std::string& path, const Pages& pages) {
  auto descriptor = SchemaDescriptor();
  descriptor.Init(schema::GroupNode::Make(
      "schema", Repetition::REQUIRED, {schema::Int64("int64", Repetition::REQUIRED)}));

  PARQUET_ASSIGN_OR_THROW(auto sink, ::arrow::io::FileOutputStream::Open(path));
  PARQUET_THROW_NOT_OK(sink->Write(kParquetMagic, sizeof(kParquetMagic)));

  auto file_metadata =
      FileMetaDataBuilder::Make(&descriptor, default_writer_properties());
  auto row_group_metadata = file_metadata->AppendRowGroup();
  // Each value of a required, non-nested column is a row
  auto num_rows = WriteColumnChunk(sink, row_group_metadata->NextColumnChunk(), pages);
  row_group_metadata->set_num_rows(num_rows);
  PARQUET_ASSIGN_OR_THROW(auto position, sink->Tell());
  row_group_metadata->Finish(/*total_bytes_written=*/position - sizeof(kParquetMagic));

  WriteFileMetaData(*file_metadata->Finish(), sink.get());
  PARQUET_THROW_NOT_OK(sink->Close());
}

int main() {
  WriteInt64ColumnFile(
      "empty_data_pages_v1.parquet",
      {Int64PageV1({}), Int64PageV1({1, 2}), Int64PageV1({}), Int64PageV1({3})});
  WriteInt64ColumnFile(
      "empty_data_pages_v2.parquet",
      {Int64PageV2({}), Int64PageV2({1, 2}), Int64PageV2({}), Int64PageV2({3})});
}
