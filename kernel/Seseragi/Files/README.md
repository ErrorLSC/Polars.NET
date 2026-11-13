# Kaminari.Seseragi.Files

Managed file-format readers and writers. It holds **Avro** (object container files, specification 1.12, streaming
Apache Arrow record batches), **Excel** (xlsx, xlsb, xls and OpenDocument workbooks read, xlsx written, streaming Arrow record batches) and
**Parquet** (files and their full footer metadata; a minimal checkpoint writer). Pieces shared by formats, such as the compression codecs, live at the package root, each format in its own
folder.

Polars.NET's `ReadAvro`, `ScanAvro`, `WriteAvro*`, `SinkAvro`, and `ReadExcel`, `ScanExcel`, `WriteExcel` and
`SinkExcel` run on this package (xlsx, xlsb, xls, ods and flat ods).

## Design rules

* Depends on `Kaminari.Sazanami` (Arrow infrastructure) and pure managed codec packages (`ZstdSharp.Port`,
  `Snappier`, `System.IO.Hashing`), never on Polars.NET or Seseragi.Lakehouse; the sources never mention the engine.
* Off the managed heap where it matters: file blocks, decompression output and Arrow buffers are native memory.
* Values a format cannot hold exactly are refused with an exception, never changed silently.
* No reflection JSON (AOT): schemas are parsed with `JsonDocument` and written with `Utf8JsonWriter`.
* Generic native infrastructure (builders, buffers, the row view) belongs in Sazanami, not here.
* Modern .NET only (see `AGENTS.md`).

## Parquet

**Files** (`ParquetFile`): opened from a path, a stream (one that cannot seek is copied into native memory first) or
any `IRandomAccess` source (data read at offsets, such as an object in a store), synchronously or asynchronously.
The footer is read with one ranged read for most files (two when it is longer than 64 KiB); a file of up to 64 KiB is
held whole by that read and never read again. The footer is checked: the schema
tree must account for every element and each row group must hold a chunk per leaf. Encrypted files (modular
encryption, `PARE` or a plaintext footer) are refused by name; damaged footers fail with `ParquetFormatException`.

**Metadata** (`ParquetMetadata`): the footer as a read-only view (schema elements with their annotations, row count,
row groups, per-chunk statistics, `CreatedBy`, `KeyValueMetadata`), also parsed from footer bytes alone
(`FooterRange`, `Parse`) for the lakehouse kernels. Inside, the full `parquet.thrift` model (footer, page headers,
column and offset indexes, bloom filter headers) is read and written with one Thrift compact protocol layer
(`Parquet/Thrift`, `Parquet/Metadata`); fields, enum values and logical types the model does not know are kept or
skipped, never refused at this level.

**Schema** (`ParquetFile.Schema`): the Arrow schema of the file. Leaves map by their logical type, else their
converted type, else their physical type (an annotation that does not fit the physical type is ignored, as the
specification asks): integers of every width and sign, decimals (INT32, INT64, fixed or variable big-endian bytes;
Decimal128, Decimal256 above 38 digits), dates, times, timestamps (UTC-adjusted ones with the zone `UTC`; INT96 and
12-byte carriers too), FLOAT16, INTERVAL (month-day-nanosecond), UUID and other fixed-length bytes, strings (STRING,
ENUM, JSON), binaries (BSON, GEOMETRY, GEOGRAPHY), UNKNOWN as null. Groups map to structs, lists and maps, with the
backward-compatibility rules for old list and map layouts. An Arrow schema stored by the writer (`ARROW:schema`)
refines the mapping where Parquet cannot say it: time zone names of UTC-adjusted timestamps, durations, timestamp units
Parquet stores in another (seconds, nanoseconds in format 1.0, INT96; rescaled exactly or refused), integers narrower
than an unannotated INT64 or INT32 (checked), fixed-size lists, dictionary-encoded columns and field metadata. It never
makes a local timestamp an instant or an instant local, and never changes an annotated integer or decimal type. Parquet
field ids become `PARQUET:field_id` field metadata.

**Reading** (`ParquetFile.OpenReader`, `ParquetReader`, `ParquetReadOptions`): every column, flat or nested, as record
batches of `BatchSize` rows in native memory, synchronously or asynchronously, in row group order. Row groups are
decoded side by side, and the columns of a batch too, on `MaxDegreeOfParallelism` threads (all cores by default; 1
streams on the calling thread); up to that many decoded row groups are held ahead of the consumer. `ReadArrowStream`
hands the batches out as an Arrow stream that owns them (each disposed at the next read; an export keeps its memory),
for Polars' or another library's import. Strings and binaries come in Arrow's view layout by default (`ViewTypes`:
`Utf8View`, `BinaryView`; dictionary pages shared, not copied per value; any size per batch), or as `Utf8` / `Binary`
(at most 2 GiB of values per batch). Nested columns
are assembled from their repetition and definition levels into structs, lists (the old two-level and unannotated
layouts included), maps (a map without values as the list of its keys) and fixed-size lists (from the stored Arrow
schema or FINITE_VECTOR, when their items are leaves; others read as lists; a null entry holds placeholders); leaves under one repeated field must agree, a null map key
or a fixed-size list of another length is an error. Variants read as the struct they are stored as (unshredded:
`metadata` and `value`; shredded: with `typed_value`), marked `arrow.parquet.variant`. Columns are projected whole
(`Columns`) or by field path (`ColumnPaths`: only the struct fields on the paths, through lists and map keys and
values, a map keeping its key and value whatever the path; other leaves are not read), and row groups chosen (`RowGroups`); each row group's chunks are fetched with few
ranged reads (nearby chunks coalesced). Pages: data pages v1 and v2, dictionary pages; PLAIN, dictionary (RLE and the
deprecated PLAIN_DICTIONARY), RLE booleans; levels in the RLE / bit-packing hybrid or the deprecated BIT_PACKED;
codecs uncompressed, Snappy, GZIP (several members), Brotli, Zstandard, LZ4_RAW and the deprecated Hadoop-framed LZ4;
page CRCs checked on request (`VerifyChecksums`). `BinaryAsString` reads unannotated binaries as strings; INT96
timestamps read in microseconds exactly as Spark reads them (its wrapping 64-bit arithmetic, which brings back the
dates beyond 2262 Spark wrote wrapped; nanoseconds below a microsecond are dropped), or in nanoseconds (`Int96Unit`,
checked). Strings are validated as UTF-8; values that do not fit their Arrow type (narrowed integers, decimals over
their precision, timestamps beyond 64 bits) are an error, never truncated. The DELTA_* and BYTE_STREAM_SPLIT
encodings are not read yet; ALP and LZO, and column chunks in other files, are refused.

`MinimalParquetWriter` writes the fixed-schema files a Delta table keeps beside its log (checkpoints); it is internal
and stands in until a full Parquet writer exists. Page codecs compress as well as decompress, for the writer to come.

Layout of `Parquet/`: `Thrift/` (compact protocol), `Metadata/` (the `parquet.thrift` structs), `Schema/` (schema tree,
leaf types, Arrow mapping with each node's level rules, projection by path), `Codecs/` (page codecs; Snappy blocks are
decoded by Sazanami's own decoder), `Encodings/` (RLE / bit-packing hybrid, BIT_PACKED levels; LSB bit unpacking is
Sazanami's), `Reading/` (pages, leaf readers by value kind, value converters, the nested assembler); `ParquetFile`, `ParquetReader` and the options at the top.

`IO/` holds what the formats share for reading at offsets: `IRandomAccess`, the stream-backed source and the native
copy of streams that cannot seek.

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

**Workbooks** (`ExcelWorkbook`): opened from a path or a stream; the format is detected from the content (a zip whose
workbook part is XML: xlsx, transitional or strict OOXML; or BIFF12 records: xlsb; an OLE2 compound file with a BIFF8
workbook stream: xls; a zip whose `mimetype` (or manifest) says OpenDocument spreadsheet: ods; XML whose root is
`office:document` of that type: flat ods). `Sheets` (name, tab position, visibility, kind: worksheet, chart sheet or
other), `Tables` (name, sheet, range, header and totals rows; none in xls and ods), `Date1904` (in ods, a 1904-01-01
null date). A stream that cannot seek is copied into native memory first. Encrypted workbooks (password-protected xlsx
and xlsb in an OLE2 file; xls with a FILEPASS record, which Excel also writes for workbooks that are only structure-
or sheet-protected; ods with manifest encryption data), BIFF2–BIFF7 xls and other OpenDocument types are recognised
and refused by name.

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
* xlsb cells ([MS-XLSB]): RK numbers (integers, doubles, either divided by 100), doubles, booleans, errors, inline,
  rich and shared strings, and the cached values of formulas; number formats come from `styles.bin`, so dates stored
  as RK integers read as dates. Records the reader does not use (formatting, alternate-content and future-record
  blocks) are skipped; the one-byte types 12 to 18, unassigned among the cell records, raise `ExcelFormatException`.
* xls cells ([MS-XLS], BIFF8): Number, RK and MulRk numbers, LabelSst and Label strings, BoolErr booleans and
  errors, and the cached values of formulas (string results from the String record that follows, which may run on
  into Continue records); Blank and MulBlank add nothing. Shared strings may break across Continue records
  (characters, formatting runs, phonetic data); their declared counts are not trusted. Number formats come from the
  Format and XF records (XF indexes count style XFs too). Cells must come in increasing row order and, within a row,
  increasing column order, as Excel writes them: others raise `ExcelFormatException`, as does an RString cell
  (BIFF5/7, not described by [MS-XLS]). Embedded charts' substreams inside a sheet are skipped, and other records
  (formatting, layout, objects) too. Dialog and macro sheets are listed as `Other`.
* Compound files ([MS-CFB]): versions 3 and 4, the DIFAT, the mini stream; the workbook stream named "Workbook", or
  "Book" (some writers give BIFF8 the BIFF5 name). Counts and sector numbers are checked before anything is
  allocated, chains are followed once (a cycle or a link past the allocation table raises `ExcelFormatException`),
  and a file cut short fails only when a missing sector is read. Streams are read at offsets, so several sheets can
  be open at once.
* OpenDocument cells ([ODF 1.3] part 3): values by `office:value-type` — float, percentage and currency numbers;
  dates and date-times (a column is a date column when no value has a time of day; a zone suffix is ignored);
  times, read as times of day, or as durations when the cell's data style does not wrap at 24 hours
  (`number:truncate-on-overflow="false"`, found through parent styles and column and row default cell styles) or
  the value lies outside a day; booleans; strings (`office:string-value`, else the paragraphs joined by line feeds,
  their text as written, as LibreOffice reads it, with `text:s`, `text:tab` and `text:line-break` expanded; notes,
  annotations, ruby annotations and drawing objects left out). LibreOffice's error results
  (`calcext:value-type="error"`) are errors; cells without a value type are empty. Rows and cells repeat
  (`number-rows-repeated`, `number-columns-repeated`): empty repeats only move the position, others are emitted;
  values past 1,048,576 rows or 16,384 columns raise `ExcelFormatException`. Covered cells read like cells; row and
  column groups are entered; shapes, forms and tables nested in cells are skipped. Sheets hidden by their table
  style (`table:display="false"`) are `Hidden`.
* OpenDocument containers: the sheets follow one another in one document (content.xml, or the flat file), so they
  are found on demand: opening reads the styles and stops at the first sheet's start tag; reading a sheet goes on
  from there on the same stream, skipping the sheets before it, so the document is decompressed once; only
  `Sheets` (or an error that lists them) scans to the end. A sheet already passed is read by starting the document
  again at its offset (decompressing what comes before it, or seeking in a flat file). Structural errors past the
  sheets read surface only when the rest is scanned.
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
| `Excel/Xml/` | `XmlSpanScanner` (allocation-free tokenizer over complete UTF-8: one scalar pass per tag, local names, no entity expansion), `XmlAttributeEnumerator` (attributes in one walk), `XmlText` (entities, `_xHHHH_`, numbers). |
| `Excel/Packaging/OpcPackage.cs` | The zip package of xlsx and xlsb: case-insensitive part names, relationships, small parts read whole. |
| `Excel/Packaging/CompoundFile.cs`, `CompoundFileStream.cs`, `RandomAccessSource.cs` | OLE2 compound files: header, DIFAT, FAT, directory (the root's children), mini FAT and mini stream; a stream over a sector chain (consecutive sectors read at once); reads at offsets of a file handle, native memory or a gated stream. |
| `Excel/Xlsx/` | `XlsxWorkbook` (workbook, styles, tables; shared strings on first sheet), `XlsxSharedStrings` (`<si>` by `<si>`), `XlsxSheetSource` (rows parsed once buffered whole). |
| `Excel/Xlsb/` | `Biff12` (record headers, the record types used, bounds-checked field reads, UTF-16 to UTF-8), `XlsbWorkbook` (workbook.bin, styles.bin, table parts), `XlsbSharedStrings`, `XlsbSheetSource` (records parsed once buffered whole; a row handed out at the next row header). |
| `Excel/Xls/` | `Biff8` (record types, `BiffRecords` pulling records from a buffer, `BiffText` for strings across Continue records), `XlsWorkbook` (globals: sheets, 1904, formats, XFs, where the SST sits; dialog sheets by WsBool), `XlsSharedStrings` (SST and Continue records, a push parser), `XlsSheetSource` (worksheet substream to rows; formula strings, nested substreams). |
| `Excel/Ods/` | `OdsWorkbook` (detection, encryption check, the on-demand scan of the document, sheets by offset), `OdsStyles` (hidden table styles, elapsed-time cell styles through parents, the null date), `OdsValues` (xsd dates and durations to microseconds), `OdsSheetSource` (a table to rows: repeats, value types, paragraph text). |
| `Excel/Reading/` | The format-neutral half: `SheetCells` (cell model, row buffer, `SheetSource`), `WorkbookSource`, `ChunkReader` (a part decompressed into a native buffer, refilled between complete chunks), `SharedStringStore` (native UTF-8 plus offsets), `SheetReader` (header, window, inference, projection, batches), `ColumnAppenders` (cells to native builders per Arrow type), `CellText` (cells as text). |
| `Excel/Writing/` | `CellSink` (the format-neutral cell writer the encoders use) and `XlsxCellSink`, `XmlOut` (native UTF-8 output with escaping), `SharedStringTable` (open-addressing hash over native memory), `CellEncoders` (Arrow values to cells), `XlsxParts` (styles, workbook, relationships, content types). |
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
| `Polars.NET.Core/Wrappers/Wrappers.Excel.cs` | `ReadExcel` (path, stream), `ScanExcel`, `WriteExcel` (path, stream), `ExcelBatchSink` (for `SinkExcel`); schema overrides from the engine's schema. |
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
  refusals, limits), `ExcelFormatTests` (number formats, ported from calamine's and openpyxl's cases; serial dates),
  `XlsbReadTests` (calamine's Excel-written .xlsb files in `TestData/excel/calamine`, MIT; and workbooks built record
  by record with `XlsbBuilder` for records Excel's files do not show, and malformed ones), `XlsReadTests` (calamine's
  .xls files, LibreOffice's .xls copies of the fixtures, and workbooks built with `XlsBuilder`, whose
  `CompoundFileBuilder` also writes version 4, mini-stream, DIFAT and broken compound files), `OdsReadTests`
  (calamine's .ods files, LibreOffice's .ods and .fods copies of the fixtures, and spreadsheets written from markup
  by `OdsBuilder` as packages and flat documents).
  Interop (opt-in): `SESERAGI_EXCEL_PYTHON=<python with openpyxl, polars, fastexcel, pyxlsb, xlrd, olefile> dotnet test
  tests/Seseragi.Files.Tests --filter Category=Interop` reads our workbooks with openpyxl, Python Polars and, when
  `soffice` is installed, LibreOffice (`interop_check.py`); pyxlsb and Python Polars check the .xlsb fixtures, and
  LibreOffice's .xlsx copies of them must read as our xlsb reader reads the originals. `ExcelXlsInteropTests`
  compares every cell of the .xls fixtures and of built workbooks with xlrd (`xls_check.py`; olefile hands xlrd the
  workbook stream of version 4 files), and LibreOffice's .xlsx copies with our xls reader; `ExcelOdsInteropTests`
  compares LibreOffice's .xlsx copies of OpenDocument spreadsheets with our reading of the originals. Engine level: `tests/Polars.CSharp.Tests/IO/ExcelTests.cs`,
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
* **A new Excel container:** a `WorkbookSource` and a `SheetSource` producing `RowBuffer`s (cell
  types, number-format kinds, strings inline or through `ISharedStrings`); detection in `ExcelWorkbook`; everything from
  the header on is shared.
* **A new format:** a folder beside `Avro/`, its own codec marker interface, the Sazanami native builders and buffers.
