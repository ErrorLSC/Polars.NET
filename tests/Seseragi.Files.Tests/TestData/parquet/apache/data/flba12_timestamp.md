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

# `flba12_timestamp.parquet`

Three columns with physical type `FIXED_LEN_BYTE_ARRAY(12)` and logical type `TIMESTAMP`.

## Schema

```
message flba12_timestamp {
  optional fixed_len_byte_array(12) timestamp_millis (TIMESTAMP(MILLIS,true));
  optional fixed_len_byte_array(12) timestamp_micros (TIMESTAMP(MICROS,true));
  optional fixed_len_byte_array(12) timestamp_nanos  (TIMESTAMP(NANOS,true));
}
```

All columns use `TYPE_DEFINED_ORDER`. For the `timestamp_nanos` column, two out of the six rows
require more than 64 bits to represent.

## Values

The same six timestamp values appear in every column, only the unit scale differs. The values
below are interpreted as ISO-8601 timestamps in the Proleptic Gregorian calendar.

| Row | Timestamp (UTC)       | Epoch Seconds    | Note                                        |
|-----|-----------------------|------------------|---------------------------------------------|
| 0   | 1970-01-01T00:00:00Z  | 0                | Epoch (all-zero bytes)                      |
| 1   | 1970-01-01T00:00:01Z  | 1                | Small positive                              |
| 2   | 1969-12-31T23:59:59Z  | -1               | Small negative (exercises two's-complement) |
| 3   | 2262-04-11T23:47:16Z  | 9,223,372,036    | Near INT64-nanos max                        |
| 4   | 9999-12-31T23:59:59Z  | 253,402,300,799  | Far future — NANOS exceeds INT64            |
| 5   | 0001-01-01T00:00:00Z  | -62,135,596,800  | Far past — NANOS is below INT64 minimum     |
