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
# "Bad Data" files

These are files used for reproducing various bugs that have been reported.

* PARQUET-1481.parquet: tests a case where a schema Thrift value has been
  corrupted.
* ARROW-RS-GH-6229-DICTHEADER.parquet: tests a case where the number of values
  stored in dictionary page header is negative.
* ARROW-RS-GH-6229-LEVELS.parquet: tests a case where a page has insufficient
  repetition levels.
* ARROW-RS-GH-11261-FLBA-DICT.parquet: test case of https://github.com/apache/arrow-rs/issues/11261
  where Arrow dictionary values were written using BYTE_ARRAY encodings despite
  a FIXED_LEN_BYTE_ARRAY schema.
* ARROW-GH-41321.parquet: test case of https://github.com/apache/arrow/issues/41321
  where decoded rep / def levels is less than num_values in page_header.
* ARROW-GH-41317.parquet: test case of https://github.com/apache/arrow/issues/41317
  where all columns have not the same size.
* ARROW-GH-43605.parquet: dictionary index page uses rle encoding but 0 as rle bit-width.
* ARROW-GH-45185.parquet: test case of https://github.com/apache/arrow/issues/45185
  where repetition levels start with a 1 instead of 0.
* ARROW-GH-47662.parquet: test case identified in https://github.com/apache/arrow/issues/47662
  where a required column contains null values (an incorrect version of data/fixed_length_byte_array.parquet).


## Directory `variants`

This subdirectory contains files with malformed variant structures.

Robust implementations of variant decoders SHOULD reject these.

| File                                                          | Malformed Structure                                                        |
|---------------------------------------------------------------|----------------------------------------------------------------------------|
| `variant/int_overflow_in_bounds_check.parquet`                | Triggers an overflow if 32 bit multiplication is used to calculate ranges. |
| `variant/out_of_range_dictionary_size.parquet`                | The dictionary is declared as larger than the data                         |
| `variant/malformed_child_inside_well_formed_parent.parquet`   | Parent is well formed; child is malformed                                  |
| `variant/out_of_range_child_offset.parquet`                   | The offset of an child element is out of range                             |
| `variant/out_of_range_element_count.parquet`                  | The number of declared array elements is larger than the data              |
| `variant/bad_data/variants/over_deep_nested_children.parquet` | The hierarchy is excessively deep                                          |

The first of these is the most critical, as this can trigger a memory allocation of many GiB, which may affect the operations of other worker threads in a shared process; an oversized dictionary may also trigger excessive memory allocation.

The out of range child and element files contain metadata referring to content past the end of the actual data field.
On languages with strict range check, this will fail on read; extra verification simply changes when the failure is detected.
For languages where range checks are not automatically, there is a risk of variant data referencing other data on the stack/in the heap.
As this data is read only, there's no _direct_ threat to the integrity of the process, but it is still highly dangerous.

One notable file is `bad_data/variants/over_deep_nested_children.parquet`, which verifies that nested variant children over 500 levels deep is rejected. This number is subjective; it was chosen to be consistent with the JSON parser `org.apache.parquet.variant.VariantJsonParser`.

Currently excluded from these tests is any with an explicit limit on the size of a variant.
Apache Spark places a limit on 128 MiB on each of the metadata and value fields here.