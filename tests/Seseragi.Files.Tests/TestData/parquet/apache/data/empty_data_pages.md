<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

# `empty_data_pages_v1.parquet` and `empty_data_pages_v2.parquet`

A single required INT64 column of three rows, whose column chunk contains data pages without
values (`num_values = 0`): the first page and one in the middle. Readers must skip these pages
instead of treating them as the end of the column chunk, and read `1, 2, 3`.

## Schema

```
message schema {
  required int64 int64;
}
```

## Pages

The column chunk has four uncompressed, PLAIN encoded data pages with empty statistics, DataPageV1
in `empty_data_pages_v1.parquet` and DataPageV2 in `empty_data_pages_v2.parquet`:

| Page | `num_values` | Values |
|------|--------------|--------|
| 0    | 0            |        |
| 1    | 2            | 1, 2   |
| 2    | 0            |        |
| 3    | 1            | 3      |

In the DataPageV2 headers, `num_rows` equals `num_values` and `num_nulls` is 0.

The files have no page index, since the OffsetIndex requires strictly increasing
`first_row_index` values, while a page without values has the same one as the next page.

## How it was generated

Arrow C++ writers only emit data pages holding values, so [empty_data_pages.cc](empty_data_pages.cc)
writes the pages one by one with its `PageWriter` API. Later Arrow C++ versions may reject such
pages, so compile it against an Arrow C++ 26.0.0 installation in `$ARROW_HOME` and run it in this
directory:

```
c++ -std=c++20 empty_data_pages.cc -I$ARROW_HOME/include -L$ARROW_HOME/lib -lparquet -larrow -Wl,-rpath,$ARROW_HOME/lib -o empty_data_pages
./empty_data_pages
```
