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

# `int96_timestamp_order.parquet`

A single `required int96` column written with the `INT96_TIMESTAMP_ORDER` column order added in
[parquet-format #584](https://github.com/apache/parquet-format/pull/584). It exercises a reader's
ability to honor the new order: the column carries min/max statistics and a column index, and the
footer's `column_orders[0]` is set to `INT96_TIMESTAMP_ORDER` (union field 3) rather than
`TYPE_ORDER`.

INT96 timestamps are 12 little-endian bytes: an 8-byte nanoseconds-within-the-day followed by a
4-byte Julian day. The defined order compares the Julian day (as a signed int32) first, then the
nanoseconds (as a signed int64) — i.e. chronological order.

## Why this file is non-trivial

The values are deliberately chosen so that a **byte-wise (lexicographic) comparison disagrees with
the chronological order**. Because the low-order nanosecond bytes come first in the little-endian
layout, a reader that compares the raw 12 bytes (or that ignores the new order) computes the wrong
min/max. A reader must implement the chronological comparison to pass.

| Value          | Julian day | nanos-of-day      | Timestamp                          | first byte |
|----------------|------------|-------------------|------------------------------------|------------|
| EARLY          | 2440000    | 123               | 1968-05-23 00:00:00.000000123      | `0x7B`     |
| SAME_DAY_EARLY | 2440588    | 1000              | 1970-01-01 00:00:00.000001000      | `0xE8`     |
| LATE_IN_DAY    | 2440588    | 86399999999999    | 1970-01-01 23:59:59.999999999      | `0xFF`     |
| NEXT_DAY       | 2440589    | 0                 | 1970-01-02 00:00:00.000000000      | `0x00`     |

Values are written to the file out of order: `LATE_IN_DAY, NEXT_DAY, EARLY, SAME_DAY_EARLY` (so that
the correct min/max are also neither the first nor the last value).

- Correct (`INT96_TIMESTAMP_ORDER`) min/max: **EARLY / NEXT_DAY**
- Byte-wise (incorrect) min/max would be: **NEXT_DAY / LATE_IN_DAY** (ordered by the leading
  nanosecond byte `0x00 < 0x7B < 0xE8 < 0xFF`)

The min/max written to the statistics (and the column index) are therefore:

```
min = 0x 7B 00 00 00 00 00 00 00 40 3B 25 00   (EARLY:    nanos 123,  Julian day 2440000)
max = 0x 00 00 00 00 00 00 00 00 8D 3D 25 00   (NEXT_DAY:  nanos 0,    Julian day 2440589)
```

## How it was generated

Written by parquet-java (parquet-mr 1.18.0-SNAPSHOT) via the
`TestInt96TimestampStatistics#writeInt96TimestampOrderInteropFile` test:

```
mvn -pl parquet-hadoop test \
  -Dtest='TestInt96TimestampStatistics#writeInt96TimestampOrderInteropFile' \
  -Dparquet.testing.data.dir=<parquet-testing>/data
```

## Schema

```
message int96_timestamp_order {
  required int96 ts;
}
```
