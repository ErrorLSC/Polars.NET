# Kaminari.Seseragi.Files

Managed file-format readers and writers. It holds **Avro** (object container files, specification 1.12, streaming
Apache Arrow record batches), **Excel** (xlsx workbooks, streaming Arrow record batches both ways) and the **Parquet**
pieces the lakehouse kernels need (footer metadata, a minimal writer). Pieces shared by formats, such as the compression codecs, live at the package root, each format in its own
folder.

Polars.NET's `ReadAvro`, `ScanAvro`, `WriteAvro*`, `SinkAvro`, and for xlsx `ReadExcel`, `ScanExcel`, `WriteExcel` and
`SinkExcel` run on this package.

## Design rules

* Depends on `Kaminari.Sazanami` (Arrow infrastructure) and pure managed codec packages (`ZstdSharp.Port`,
  `Snappier`, `System.IO.Hashing`), never on Polars.NET or Seseragi.Lakehouse; the sources never mention the engine.
* Off the managed heap where it matters: file blocks, decompression output and Arrow buffers are native memory.
* Values a format cannot hold exactly are refused with an exception, never changed silently.
* No reflection JSON (AOT): schemas are parsed with `JsonDocument` and written with `Utf8JsonWriter`.
* Generic native infrastructure (builders, buffers, the row view) belongs in Sazanami, not here.
* Modern .NET only (see `AGENTS.md`).

## Parquet

`ParquetMetadata` reads what a lakehouse needs from a footer (schema, row counts, per-column statistics) without
reading data pages. `MinimalParquetWriter` writes the fixed-schema files a Delta table keeps beside its log
(checkpoints); it is internal and stands in until a full Parquet writer exists.

## Avro features

**Reading** (`AvroReader`, `AvroReadOptions`): from a path or a stream, synchronously or asynchronously, as record
batches of `BatchSize` rows backed by native memory. Columns can be projected (`Columns`, `ColumnIndices`; others are
skipped without decoding), rows limited (`RowLimit`), and blocks decoded in parallel (`MaxDegreeOfParallelism`).
`WriterSchema`, `Schema`, `Codec` and `Metadata` describe the file.

**Reading with a reader schema** (`ReaderSchema`, `FieldMatching`): Avro schema resolution. Records match fields by
name or by Iceberg field id (`AvroFieldMatching`), never by record name; writer-only fields are skipped; a reader
field the writer lacks takes its default. Numbers promote (lossy promotions are checked per value), decimals may widen
their precision, logical types must mean the same. Mismatches raise `AvroResolutionException`.

**Schema cache** (`AvroReadCache`, `AvroReadOptions.Cache`): readers of many files with one schema (Iceberg manifests)
share the parsed writer schema, the projection, the Arrow schema and the resolution plan.

**Writing** (`AvroWriter`, `AvroWriteOptions`): Arrow batches to an object container file, with a codec
(`NoCompression`, `DeflateCompression`, `SnappyCompression`, `ZstandardCompression`), a block size and extra header
metadata. By default the Avro schema is mapped from the Arrow schema; with `WriterSchema` the file is written as
exactly the given Avro schema (record names, defaults, field ids, encodings).

**Schema API** (`AvroSchema` and subclasses, `AvroField`): `AvroSchema.Parse`, `ToJson`, and `ToArrowSchema` (the Arrow
schema a schema reads as, and the one a `WriterSchema` accepts). Attributes the package does not interpret are kept in
`Properties` and written back; Iceberg's ids and `adjust-to-utc` have typed accessors. Schemas are built in code with
factory methods, validated as they are built:

```csharp
AvroRecordSchema dataFile = AvroSchema.Record("r2",
[
    AvroField.Required("file_path", AvroSchema.String, fieldId: 100),
    AvroField.Required("record_count", AvroSchema.Long, fieldId: 103),
    AvroField.Optional("column_sizes",
        AvroSchema.ArrayMap("k117_v118", AvroSchema.Int, AvroSchema.Long, keyId: 117, valueId: 118), fieldId: 108),
    AvroField.Optional("created", AvroSchema.TimestampMicros(adjustToUtc: true), fieldId: 200),
]);

await using var reader = await AvroReader.OpenAsync(path, new()
{
    ReaderSchema = dataFile,
    FieldMatching = AvroFieldMatching.ByFieldId,
    Cache = cache,
});
```

**Field ids**: a schema's `field-id`, `element-id`, `key-id` and `value-id` appear in the Arrow schema as
`PARQUET:field_id` field metadata (`ArrowFieldIds` in Sazanami), and Arrow field ids are written back as Avro ids.
Sazanami's `ArrowRows` reads the batches row by row, by name or by field id.

## Excel features

**Workbooks** (`ExcelWorkbook`): opened from a path or a stream; the format is detected from the content (a zip with an
xlsx workbook part, transitional or strict OOXML). `Sheets` (name, tab position, visibility, kind: worksheet, chart sheet or other), `Tables` (name, sheet,
range, header and totals rows), `Date1904`. A stream that cannot seek is copied into native memory first. OLE2 files
(.xls, encrypted workbooks), .xlsb and .ods are recognised and refused by name.

**Reading** (`ExcelReader`, `ExcelReadOptions`, or `ExcelWorkbook.OpenSheet` for several sheets of one open
workbook): one sheet or table, synchronously or asynchronously, as record batches of `BatchSize` rows in native
memory. Opening reads the header and the inference window, so `Schema` is known at once; the rest of the sheet is
decompressed and parsed as the batches are read.

* Sheet by `SheetName` (case-insensitive) or `SheetIndex` (tab position, chart sheets counted; reading a chart sheet
  raises `ArgumentException`); `TableName` reads a table's range with its own header row
  and without its totals row.
* Header: the first row with a value (a table's first row); blank names become `__UNNAMED__i`, repeated names
  `name_1`, `name_2`… (as Python Polars); without a header, `column_1`, `column_2`…
* Columns span from the leftmost value to the rightmost cell of the header, the window or the declared dimension.
  `DropEmptyColumns` leaves out columns without a name and without values in the window; `Columns` /
  `ColumnIndices` project (sheet order). A value outside the known columns later raises `ExcelValueException`, as
  does a value in a dropped column.
* Types are inferred over `InferSchemaLength` rows (null: the whole sheet, buffered; 0: all text): whole numbers
  Int64, other numbers Float64, booleans Boolean, text Utf8; by number format, dates Date32, dates with times
  Timestamp(µs), times of day Time64(µs) (a time format over a whole serial is a date-time), elapsed times
  Duration(µs); any other mix, or no value, Utf8. String columns take any value as text (integral numbers without
  a fraction, shortest round-trip numbers, `true`/`false`, ISO 8601 with a space). A later value that does not fit an
  inferred type (including text in a numeric column) raises `ExcelValueException` with the sheet and cell address.
* `SchemaOverrides` (by column name): numbers to integer types (integral and in range), floating point, Decimal128
  (exact at the scale), Utf8, Date32/Date64, Timestamp (any unit; finer parts refused; a time zone reads the clock
  times as that zone's local time), Time32/Time64, Duration; text parsed (invariant culture, ISO 8601); booleans to
  Boolean or Utf8.
* Error values (`#N/A`, `#DIV/0!`, `#SPILL!`, any code) and empty text (a formula's `""`) read as empty cells. `DropEmptyRows` skips rows without values;
  kept empty rows lie between rows with values. `RaiseIfEmpty` raises `ExcelFormatException` for a sheet without
  data rows.
* Cells: numbers, shared strings, inline strings (rich runs concatenated, phonetic runs left out), formula results
  (`t="str"` and cached values), booleans, errors, ISO 8601 dates (`t="d"`); rows and cells without `r` take the
  next position; `_xHHHH_` escapes, entities and CDATA decoded.
* Dates: the 1900 system with Lotus's fictitious 1900-02-29 (serial 60 reads as 1900-02-28), the 1904 system;
  times rounded to the nearest microsecond.

**Writing** (`ExcelWriter`, `ExcelWriteOptions`): Arrow batches as one sheet, streaming. Rows are encoded as they
arrive and compressed into the sheet's zip entry; shared strings, styles, the workbook and the package parts follow
on `Complete`. `SheetName` (validated as Excel requires), `IncludeHeader`, number formats per temporal kind
(`DateFormat`, `DatetimeFormat`, `TimeFormat`, `DurationFormat`), `SharedStrings` (default on; off writes inline
strings and keeps nothing in memory), `Autofit` (widths from the header and the first `AutofitRows` rows, held back
until then), `FreezeHeader`, `CompressionLevel`. Asynchronous writes go through the entry stream; the zip directory is
written synchronously on completion.

## Excel type mapping (writing)

* Boolean → boolean cell; Int8..Int32, UInt8..UInt32 → number; Int64, UInt64 → number up to 2^53 in magnitude, text
  beyond; Float16/Float32 → number in the shortest single-precision form; Float64 → shortest round-trip number;
  NaN and infinities → `#NUM!` error cell.
* Decimal32..256 → number when the shortest round-trip text of the nearest double is the same decimal value, text
  otherwise.
* Utf8, LargeUtf8, Utf8View, dictionaries of strings → string cells (leading or trailing whitespace preserved,
  characters XML cannot carry and literal `_xHHHH_` escaped).
* Date32/Date64 (midnight), Timestamp of any unit (a zoned value as the wall-clock time in its zone), Time32/Time64,
  Duration → serial numbers with the format of their kind; the nearest double of the microsecond value.
* Null type → empty cells. Refused (`NotSupportedException`): binary, nested and interval types, sub-microsecond
  parts, dates before 1900-01-01 or after 9999-12-31, strings over 32,767 UTF-16 units, more than 1,048,576 rows,
  more than 16,384 columns.

## Code map

| Where | What |
| :--- | :--- |
| `CompressionType.cs` | Codec objects shared by formats. Which format accepts which codec is a compile-time fact: marker interfaces (`IAvroCompression`, with an `internal` member so only this package implements it). |
| `Avro/AvroSchema.cs` | The schema model (`AvroSchema` and subclasses, `AvroField`, `AvroType`, `AvroLogicalKind`; internal constructors) and the parser: named types and namespaces, recursion, logical types (an invalid or unknown one falls back to the base type and stays in `Properties`), Spark's `spark.sql.catalyst.type` intervals, the `kaminari.timezone` attribute, every other attribute kept in `Properties`. Attributes on a named-type reference or a `{"type": {…}}` wrapper have no node of their own and are dropped. |
| `Avro/AvroSchema.Construction.cs` | Factory methods (`AvroSchema.Record`, `Array`, `ArrayMap`, `Map`, `Union`, `Nullable`, `TimestampMicros`, `DecimalFixed`, `UuidFixed`, …; `AvroField.Required`, `Optional`). Every node is validated as built (names, logical types, unions, defaults); a built schema parses back to the same model; built schemas are not recursive. |
| `Avro/AvroDefaults.cs` | A field's JSON `default` encoded as Avro binary for its type: validates defaults, fills missing fields on read, writes them on write. |
| `Avro/AvroSchemaJsonWriter.cs` | Schema to JSON: named types written once, then by full name; two different types of one name are refused. |
| `Avro/AvroBinary.cs` | `AvroSpanReader` (bounds-checked, SWAR varint fast path) and `AvroBufferWriter` (native buffer, SWAR varint encoding). `AvroFormatException` for malformed input. |
| `Avro/AvroCodecs.cs` | Block codecs: null, deflate, snappy (with CRC32), zstandard; bzip2 and xz are refused by name. |
| `Avro/AvroReadSchema.cs` | Avro → Arrow type mapping and union shapes. |
| `Avro/AvroDecoders.cs` | Decoders compiled once per schema, appending to native builders; skippers for projected-away values; fast paths for arrays of long, int, float and double. |
| `Avro/AvroResolution.cs` | Schema resolution: `AvroResolver` turns (writer, reader) into an immutable plan (`ResolvedNode` tree; every schema check happens at open), compiled for each decoder set; identity subtrees use the reader's own decoders. Decoders for promotions, defaults, enum remaps and unions; `AvroResolutionException`. |
| `Avro/AvroReader.cs` | Container reader: header, block loop, batching, projection, row limit, parallel decoding, reader schema. |
| `Avro/AvroReadCache.cs` | `AvroReadCache`: the immutable result of opening a schema (`PreparedRead`), keyed by the writer-schema bytes, the reader schema's content, the matching and the projection; thread-safe, least recently used dropped. |
| `Avro/AvroWriteSchema.cs` | Arrow → Avro mapping and encoder compilation; `SameEncoding` (per-batch type check). |
| `Avro/AvroGivenSchema.cs` | Encoders for a given writer schema: fields matched to columns by name or id, defaults for missing fields, the reading rules in reverse, `[T, "null"]` unions, fixed decimals, enums, array-encoded maps. |
| `Avro/AvroEncoders.cs` | Encoders over pinned Arrow buffers; `EncodeRange` loops for list items. |
| `Avro/AvroWriter.cs` | Container writer: blocks by size, header with the first block, failure handling. |
| `Excel/ExcelWorkbook.cs`, `ExcelReader.cs`, `ExcelWriter.cs`, `ExcelTypes.cs` | Public Excel API: workbook (format detection, sheets, tables), reader and options, writer and options, sheet and table records, `ExcelFormatException`, `ExcelValueException`. |
| `Excel/ExcelNumberFormat.cs` | Number format classification (Number, Date, DateTime, Time, Duration): built-in ids and format codes (first section; quotes, escapes, fills, brackets skipped except elapsed-time brackets; `m` as minutes or months by its neighbours). |
| `Excel/ExcelDates.cs`, `ExcelCellReference.cs` | Serial numbers ↔ Unix days and microseconds (1900 leap bug, 1904 system); A1 references and ranges. |
| `Excel/Xml/` | `XmlSpanScanner` (allocation-free tokenizer over complete UTF-8, local names, no entity expansion), `XmlText` (attributes, entities, `_xHHHH_`, numbers), `XmlChunkReader` (a part decompressed into a native buffer, refilled synchronously or asynchronously between complete chunks). |
| `Excel/Xlsx/` | `XlsxPackage` (zip, case-insensitive part names, relationships), `XlsxWorkbook` (workbook, styles, tables; shared strings on first sheet), `XlsxSharedStrings` (one native UTF-8 buffer plus offsets), `XlsxSheetSource` (rows parsed once buffered whole). |
| `Excel/Reading/` | The format-neutral half: `SheetCells` (cell model, row buffer, `SheetSource`), `WorkbookSource`, `SheetReader` (header, window, inference, projection, batches), `ColumnAppenders` (cells to native builders per Arrow type), `CellText` (cells as text). |
| `Excel/Writing/` | `XmlOut` (native UTF-8 output with escaping), `SharedStringTable` (open-addressing hash over a native buffer), `CellEncoders` (Arrow values to cells), `XlsxParts` (styles, workbook, relationships, content types). |
| `Parquet/ParquetMetadata.cs` | Parquet footer metadata (`ParquetMetadata`: schema elements, row groups, column statistics) parsed from the Thrift compact encoding: `Parse` (a footer), `FromFile` (a whole file in memory), `FooterRange` (where the footer lies, from a file's last bytes; reading them is the caller's, e.g. Lakehouse's `ParquetFooters` over its file systems). |
| `Parquet/MinimalParquetWriter.cs` | A small, dependency-free Parquet writer for fixed schemas (Delta checkpoints): optional and required fields, structs, maps, three-level lists, plain encoding, one row group, no compression. Internal; visible to Seseragi.Lakehouse. |

In Sazanami (generic, reusable by other formats):

| Where | What |
| :--- | :--- |
| `Arrow/NativeGrowBuffer.cs` | Growable 64-byte-aligned native buffer and bitmap. |
| `Arrow/NativeColumnBuilders.cs` | Arrow column builders in native memory (fixed width, boolean, binary, fixed-size binary, dictionary, list/map, struct, null) with lazy validity. |
| `Arrow/NativeArrayExport.cs` | Builders to a `RecordBatch` without copying: buffers are detached into an Arrow C data interface `ArrowArray` (`NativeCArray`) and imported with `CArrowArrayImporter`; the root release callback frees everything. |
| `Helpers/NativeByteBuffer.cs`, `NativeByteBufferStream.cs` | Native I/O buffers (block bytes, decompression, writer output) and a write stream over them. |
| `Arrow/ArrowFieldIds.cs` | The field-id metadata key (`PARQUET:field_id`) and helpers to read or set an Arrow field's id. |
| `Arrow/ArrowRows.cs`, `ArrowRow.cs`, `ArrowList.cs`, `ArrowRowNode.cs` | Row view over a `RecordBatch` or `StructArray`: typed getters by ordinal, name or field id, nested structs, lists and maps, no allocation per row. |

In Polars.NET (the engine integration):

| Where | What |
| :--- | :--- |
| `Polars.NET.Core/Wrappers/Wrappers.Excel.cs` | `ReadExcel` (path, stream), `ScanExcel`, `WriteExcel` (path, stream), `ExcelBatchSink` (for `SinkExcel`); schema overrides from the engine's schema; .xlsb, .xls and .ods paths still go to the native reader. |
| `api/Polars.CSharp/IO/Excel/*.cs`, `api/Polars.FSharp/IO/Excel.fs` | Public Excel API. |
| `Polars.NET.Core/Wrappers/Wrappers.Avro.cs` | `ReadAvro` (path, span, stream), `ScanAvro`, `WriteAvro*`, `AvroBatchSink` (for `SinkAvro`), and the option mapping. |
| `Polars.NET.Core/Arrow/PolarsArrowCompat.cs` | Rewrites Arrow types the engine cannot import: Decimal256 is refused with the column name; a month-day-nano interval becomes `struct{months, days, nanoseconds: Duration(ns)}`. |
| `api/Polars.CSharp/IO/Avro/*.cs`, `api/Polars.FSharp/IO/Avro.fs` | Public API (F# names the parallel switch `paralleling`; `parallel` is reserved in F#). |

## Data flow

**Read.** `AvroReader.Open` reads the header (magic, metadata, sync marker), parses the schema (or takes it from the
cache), builds the projection and the resolution plan, and compiles the decoders over a root `NativeStructBuilder`.
Each block is read whole into a native buffer, its sync marker checked, decompressed and decoded row by row; batches
do not follow block bounds. `ExportBatch` detaches the builders' buffers into a batch, so nothing is copied after
decoding. A block whose records leave bytes over is an error.

**Parallel read** (`MaxDegreeOfParallelism > 1`): the reading thread copies whole raw blocks into per-chunk buffers,
grouping consecutive blocks up to `BatchSize` rows; each chunk is decoded on the thread pool by a pooled worker with
its own builders, decoders and codec state; batches are returned in file order and hold at most `BatchSize` rows.

**Write.** The encoders are compiled from the Arrow schema (default mapping) or from the Arrow schema and the given
writer schema. `Write(batch)` checks the batch against the Arrow schema, binds the encoders (pinning buffers), encodes
row by row into the block buffer and flushes a block whenever it reaches `BlockSize`. The header is written with the
first block, or on completion of an empty file. A failed write marks the writer faulted, so disposing it does not
complete a short file; the path overloads delete the partial file (Avro has no footer).

## Type mapping

Reading (Avro → Arrow):

* Unions: `["null", T]` → nullable `T`; `[int, long]` → Int64; `[float, double]` → Float64 (each ± null); any other
  union → struct of nullable `member0..n` (null branch → null struct); `["null"]` alone → Null.
* `enum` → Dictionary(Int32, Utf8) with the symbols; `fixed` → FixedSizeBinary; `map` → Map(Utf8 key); an `array`
  with `logicalType: map` over a two-field record (key `T` or `["null", T]`) → Map(key, value), a null key refused;
  `null` → Null; `uuid` → Utf8 (or the fixed(16) bytes).
* `decimal` → Decimal128 (≤ 38 digits) or Decimal256 (≤ 76), else raw bytes.
* `duration` → Interval(MonthDayNano).
* `timestamp-*` → Timestamp(unit, "UTC" or the `kaminari.timezone` attribute), without a zone when `adjust-to-utc` is
  `false`; `local-timestamp-*` → Timestamp without a zone; `date`, `time-millis`, `time-micros` → Date32, Time32(ms),
  Time64(us); Spark intervals → Duration(us) and Interval(YearMonth).
* A non-record top-level schema → one column `value`.
* Ids → `PARQUET:field_id` field metadata (on the field, the list item, the map key and value), only when present.
* Recursive records are refused unless projected away; strings and map keys must be valid UTF-8.

Writing (Arrow → Avro, default mapping): nullable field → `["null", T]`; narrow integers → `int`, UInt32 and UInt64 →
`long` (UInt64 above Int64.MaxValue refused); Float16 → `float`; Decimal32..256 → `bytes` decimal; Utf8, LargeUtf8,
Utf8View → `string`; dictionaries → their values; FixedSizeBinary → `fixed`; lists → `array`; Map with string keys →
`map`, with other keys → an `array` of key/value records with `logicalType: map`; Struct → record named after the
field in the parent's namespace; Date64 → `date` (midnight only); Time → `time-millis` or `time-micros` (sub-µs
refused); zoned Timestamp → `timestamp-*` + `kaminari.timezone`; naive → `local-timestamp-*`; Duration → Spark
day-time `long` (sub-µs refused); Interval → `duration` (negative or sub-ms refused) or Spark year-month `int`; Arrow
field ids → Avro ids. Default record name `topLevelRecord`; field names are written as given.

Resolution (reader schema) and given writer schemas follow the same rules in both directions: lossless widening
(int → long, float → double, string ↔ bytes, decimal precision at an equal scale) is free; lossy conversions
(long → float/double, int → float, double → float, long → int, sub-unit time rescaling, narrower decimals) are checked
per value; different meanings are refused (an instant against a wall-clock time, where `timestamp-*` with
`adjust-to-utc: false` counts as wall clock; a date against a timestamp; different logical types), while a plain
value against a logical type is allowed. General unions are not written against a given schema.

## Testing

* `tests/Seseragi.Files.Tests` (xUnit): read fixtures; write round trips; hand-built containers for encodings
  reference writers do not produce (negative block counts, corrupt data, DuckDB-shaped maps); schema parsing,
  construction and resolution; given writer schemas; the cache; parallel = sequential.
* Fixtures: `TestData/avro/generate_fixtures.py` writes them with fastavro (the C# tests hard-code the values it
  writes). `TestData/avro/apache/` holds the Apache Avro sample files (`README.md` there).
* Interop (opt-in): `SESERAGI_AVRO_PYTHON=<python with fastavro, cramjam, polars[, duckdb]> dotnet test
  tests/Seseragi.Files.Tests --filter Category=Interop` checks our files with fastavro, Python Polars and DuckDB
  (`interop_check.py`).
* Row view: `tests/Sazanami.Tests/ArrowRowsTests.cs`, and over Avro batches `AvroRowViewTests.cs`.
* Parquet: `tests/Seseragi.Lakehouse.Tests` (`ParquetFootersTests`, `CheckpointStatisticsTests`, `FileTypesTests`) and
  the F# `SeseragiFooterStatisticsTests`, through the Delta kernel that uses them.
* Engine level: `tests/Polars.CSharp.Tests/IO/AvroManagedTests.cs`, F# `AvroManagedTests.fs`.
* Excel (`tests/Seseragi.Files.Tests`): `ExcelReadTests` (fixtures from openpyxl, xlsxwriter and a hand-written
  strict package, `TestData/excel/generate_fixtures.py`; error paths), `ExcelWriteTests` (round trips of every type,
  refusals, limits), `ExcelFormatTests` (number formats, ported from calamine's and openpyxl's cases; serial dates).
  Interop (opt-in): `SESERAGI_EXCEL_PYTHON=<python with openpyxl, polars[, fastexcel]> dotnet test
  tests/Seseragi.Files.Tests --filter Category=Interop` reads our workbooks with openpyxl, Python Polars and, when
  `soffice` is installed, LibreOffice (`interop_check.py`). Engine level: `tests/Polars.CSharp.Tests/IO/ExcelTests.cs`,
  F# `ExcelTests.fs`.
* Benchmarks: `Polars.Benchmarks/ExcelBenchmarks.cs` (`--filter '*ExcelBenchmarks*'`), `Polars.Benchmarks/AvroBenchmarks.cs` (`--filter '*AvroBenchmarks*'`) and
  `AvroSmallFileBenchmarks.cs` (`--filter '*AvroSmallFileBenchmarks*'`).

## Extending

* **A new Arrow type on read:** map it in `AvroReadSchema.ToArrowType`, give `NativeColumnBuilder.Create` a builder
  if the layout is new, add a decoder case in `AvroDecoder.Compile`, a fixture row, and (if the engine cannot import
  it) a rewrite in `PolarsArrowCompat`.
* **A new type on write:** a case in `AvroWriteSchema.MapType` and in `AvroGivenSchema`, an encoder (pin buffers in
  `Bind`; override `EncodeRange` if it can appear as list items), a `SameEncoding` case if parameters change the
  encoding, a round trip in `AvroWriteTests.Wide`, a lossy-value case in `LossyValues`.
* **A new resolution rule:** a case in `AvroResolver.Context.Resolve` (checks at open) and, if values convert, a
  decoder; mirror it in `AvroGivenSchema` for writing.
* **A new codec:** a record in `CompressionType.cs` implementing the format's marker interface, an `AvroCodec`
  subclass, a name in `AvroCodec.ForName`.
* **A new Excel container (xlsb, xls, ods):** a `WorkbookSource` and a `SheetSource` producing `RowBuffer`s (cell
  types, number-format kinds, strings inline or through `ISharedStrings`); detection in `ExcelWorkbook`; everything from
  the header on is shared.
* **A new format:** a folder beside `Avro/`, its own codec marker interface, the Sazanami native builders and buffers.
