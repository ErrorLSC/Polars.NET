# Polars.NET 1.3.0 Release Note (draft)

1.3.0 moves file formats off the engine onto Polars.NET's own managed implementations, in a new package, `Kaminari.Seseragi.Files` (referenced by `Polars.NET.Core`, usable on its own): Avro, Excel, Parquet, JSON and CSV, read and written as Apache Arrow record batches in native memory. Polars.NET's Avro, Excel, JSON and CSV functions now run on it; Delta tables write their data files and read their checkpoints with its Parquet reader and writer. The engine keeps computing the rows.

## Engine: Polars 2.2

The native library moves from Polars 0.55 to the Rust code of Python Polars 2.2.

### Changed

* Lazy queries (`LazyFrame.Collect` with `Engine.Auto`, LINQ, SQL) run on the streaming engine, as in Polars 2.x. The row order of an operation that does not promise one (`Join` without `maintainOrder`, `JoinWhere`, the rows `Merge` inserts) is no longer kept by chance: ask for it (`maintainOrder`, a sort). LINQ queries keep the order LINQ to Objects gives, as before.
* The asynchronous streams of a query (`CollectBatchesAsync`, `ToBatchAsyncEnumerable`, `ToAsyncEnumerable<T>` and their `WithBackpressure` forms; F# alike) give the rows in the query's order by default (`maintainOrder = true`, Polars' default); `false` lets the streaming engine hand out batches as they are done.
* A function given to `Expr.MapBatches` (one or several columns) is called with batches of the column on the streaming engine, not with the whole column: one whose result depends on the other rows belongs in `MapGroups`. The functions run in the culture (and UI culture) of the thread that built the expression, also on the engine's threads.
* Expression arguments that are lists of values take a list: `Pl.Lit(values).Implode()` for `List.Gather`, `Str.ContainsAny`, `Str.ReplaceMany` and `Str.FindMany` (the overloads taking arrays, spans, series or dictionaries implode for you).
* `Str.Zfill` counts characters and `Str.FindMany` gives character positions (both counted bytes).
* Arithmetic between a number and a string (in `Fold`, for instance) is refused: cast one side first.
* `Expr.Hash` hashes with one seed: `seed1` to `seed3` are accepted and ignored, and the hash values differ from earlier versions.
* `Arr.ToStruct(names)` takes one name per element (missing names were filled as `field_{i}`); `Expr.Rechunk()` does nothing (Polars 2.x has no rechunk expression).
* Eager `DataFrame` and `Series` methods (`Select`, `WithColumns`, `Filter`, `Join`, `GroupBy(...).Agg`, ...) and eager reads (`ReadParquet`, `ReadIpc`, `ReadDelta`) run on the in-memory engine, as Python Polars runs its eager methods; `Merge`, which builds a plan of several steps, runs on the default engine.
* `ReadParquet` / `ScanParquet` (the engine's reader) take Parquet's own types over a conflicting Arrow schema stored in the file, as pyarrow does: nanoseconds a format 1.0 writer stored as microseconds read as microseconds, an unsigned integer stored as a plain INT64 reads as `Int64`. Seseragi.Files' Parquet reader follows the same rule (also: seconds stored as milliseconds read as milliseconds). The engine now reads files with INT96 timestamp column orders and concatenated gzip members, and refuses maps whose keys are optional.
* `TopK` and `BottomK` give their values in no particular order, as Polars documents; sort the result to order it.
* `Expr.Cast` and `Series.Cast` no longer turn text into `Date`, `Datetime` or `Time` (strict or not): the engine refuses it since Polars 2.0, with an error that names the fix. Parse the text with `Str.ToDate()`, `Str.ToDatetime()` or `Str.ToTime()`, which also take a format. LINQ queries and SQL `CAST` / `TRY_CAST` keep converting ISO 8601 text: a date, or a date and a time after `T` or a space (seconds optional); as a `DATE`, a date-time gives its date, as PostgreSQL and Spark do. `TRY_CAST` of a literal that is no date is NULL (it failed the query's translation).

## Map type

Polars 2.2's `map[key, value]` type in both APIs: a column of key-value entries, its keys never null.

### New

* `DataType.Map(key, value)`, `DataTypeKind.Map` (F#: `DataTypeKind.Map of DataType * DataType`, `pl.map`), `MapKeyType` and `MapValueType`; shown as `map[str, i64]`. Selectors `Cs.Map()` / `pl.cs.map()`.
* The `MapOps` namespace on `Expr` and `Series` (C# and F#): `Entries()`, `Keys()`, `Values()`, `Len()`, `ContainsKey(key)`, `Get(key)` (null where the map lacks the key; a key given as text is a literal, not a column name), and `List.ToMap()` from a list of `{key, value}` structs. (`Map` itself names the released UDF methods.)
* `Pl.Lit(IReadOnlyDictionary<K, V>)` and F# `pl.litMap`: one map value.
* Map columns read back as `Dictionary<K, V>`, `IReadOnlyDictionary<K, V>`, `IDictionary<K, V>` and F# `Map` (`GetValue`, `ToArray`, `Rows<T>`, the indexer, `ToDicts`); a key repeated in a map keeps its first value, as a lookup finds it. Maps keyed by lists, structs or maps are refused as .NET dictionaries.
* LINQ: dictionary members of map columns run in the engine (in `Where` too): `d[k]`, `GetValueOrDefault(k)` and `GetValueOrDefault(k, value)`, `ContainsKey`, `Count`, `Keys`, `Values`; F# `m.[k]`, `ContainsKey`, `Count`, `TryFind`. Where .NET throws on a missing key, the query reads null (as Polars, DuckDB and Spark do); a null dictionary gives null.
* Delta: map columns are created, written, read and widened (a key or value widened in older files is read and cast); CHECK constraints and generated columns read maps with Spark's functions (`m[k]`, `element_at`, `try_element_at`, `map_contains_key`, `map_keys`, `map_values`, `size`, `cardinality`, `map(k, v, ...)` with literal keys when it has several entries), as Spark 4.2 and Delta 4.4 allow them; a violation shows a map as `{k -> v, ...}`. Unity Catalog columns of Map type are `MAP`.
* ML.NET (`AsDataView`): a map of numbers is a fixed-size sparse vector over the column's keys (sorted, as slot names; or given, `AsDataView(mapKeys)`, so prediction data has the training slots); a map of text or booleans is two variable-length vectors, `{name}.Keys` and `{name}.Values`.
* Formats: Parquet, IPC and Avro read and write maps; JSON writes maps keyed by text as objects (read back with a schema); CSV, Excel and tensors refuse them, naming the column.

### Changed

* .NET dictionaries are Map columns: `DataFrame.From` / `FromRows`, `Series.From`, `PolarsSchema.From<T>()` and `Pl.Lit` gave a list of `{Key, Value}` structs (`list[struct[Key, Value]]`). `DataType.FromArrowType` of an Arrow map is a Map (it was a list of `{key, value}` structs), and Arrow, Parquet and IPC maps from the engine come as Map.
* `DataFrame.FromDicts`: a nested dictionary of object values (`Dictionary<string, object?>`) is a record, a struct of its keys, as Python Polars' `from_dicts` makes it (it was a map of the values' text); a typed dictionary (`Dictionary<string, long>`) is a Map; a column the schema (or `schemaOverrides`) makes a Map is built from the dictionaries' values in their own types. F# `DataFrame.ofMaps` alike.
* LINQ: a projection reading a dictionary member ran in .NET and threw `KeyNotFoundException` on a missing key; it runs in the engine and reads null (a member that cannot hold null fails to materialize, saying so).

### Fixed

* `DataType.Struct(fields)` (C#) took over the fields' types: a field's `DataType` could not be used again. It copies them.
* F#: a naive `Datetime` nested in a list, a struct or a map read back as a time zone named "" (`datetime[μs, ]`).

## LINQ provider: part of Polars.NET.Core

The LINQ provider, written in F# as the separate package `Polars.NET.Linq`, is now C# inside `Polars.NET.Core`, which `Polars.CSharp` and `Polars.FSharp` already reference. Remove any `Polars.NET.Linq` package reference: the package is no longer published. The public API is unchanged (`AsQueryable` and the F# query support give the same `IQueryable<T>`), queries compile to the same plans and run as fast. The query compiler, `Kaminari.Samidare`, gains a public entry API for C# callers.

## Avro: managed reader and writer

Avro now runs on Seseragi.Files, a managed implementation of the Avro 1.12 object container format (new package `Kaminari.Seseragi.Files`, referenced by `Polars.NET.Core`), instead of the engine's Avro reader and writer. Files stream block by block through Arrow record batches; blocks live in native memory and decode straight into native Arrow buffers. Behaviour is checked against fastavro, Python Polars and the Apache Avro sample files. The native library drops its `avro` feature and seven crates (`avro-schema`, `libflate`, `libflate_lz77`, `crc`, `crc-catalog`, `adler32`, `rle-decode-fast`).

### New

* `LazyFrame.ScanAvro(path, nRows, columns, projection, parallel)` scans lazily: the header is read when the frame is created, the data each time the query runs, block by block, also on the streaming engine.
* `LazyFrame.SinkAvro(path | Stream, compression, name)` writes the result of the streaming engine as its batches arrive. A failed write to a path deletes the partial file.
* `ReadAvro` and `ScanAvro` decode the file's blocks on several threads, keeping row order (`parallel`, default `true`; F#: `paralleling`). The five `ReadAvro` overloads get it as a new trailing optional parameter: recompile code built against an earlier version.
* `AvroCompression.Zstandard`, for reading and writing.
* `ReadAvro(Stream)` reads the stream block by block instead of copying it whole into memory first; `ReadAvro(byte[])` and `ReadAvro(ReadOnlySpan<byte>)` read the bytes in place.
* More of Avro reads: unions as Apache Spark reads them (`[int, long]` as `Int64`, `[float, double]` as `Float64`, other unions as a struct of `member0`, `member1`, ...), maps (as Map columns), `null` columns, `timestamp-nanos` and `local-timestamp-*`, `duration` (as a struct of months, days and nanoseconds), files whose schema is not a record (one column named `value`), records with a recursive field when that field is not read, and the `zstandard` codec. Strings are checked to be valid UTF-8.
* More types write: integers of every width (`UInt64` above `Int64.MaxValue` is refused), `Float16`, `Time`, `Duration` (as Spark's day-time interval), `Datetime` with a time zone (as `timestamp-*`, the zone kept in the schema attribute `kaminari.timezone` and restored on reading; other readers see the UTC instant), `Categorical` / `Enum` (as their strings), `Array`. Values Avro cannot hold exactly are refused with an error instead of being changed.

### Changed

* An empty record name now writes `topLevelRecord` (the engine wrote an empty name, which Avro does not allow).
* Decimals of more than 38 digits are refused with an error naming the column (the engine supports at most 38).

### Fixed

* `LazyFrame.ScanRecordBatches` took batches in standard Arrow layouts as the engine's own physical layouts: `Utf8` strings came out empty and `Time32` values wrong. Every column now goes through the engine's Arrow import.
* `DataFrame.FromArrow` and `Series.FromArrow` crashed the process (access violation) when the engine refused the data, e.g. a decimal of more than 38 digits: the Arrow array was released twice. The engine now takes the array as the Arrow C data interface specifies, and the call throws. The Arrow schema and the C structs are also freed after a successful import (they leaked).

## Excel: managed reader and writer

`ReadExcel`, `ScanExcel`, `WriteExcel` and `SinkExcel` (C# and F#) run on Seseragi.Files instead of the engine's calamine and rust_xlsxwriter, which are gone from the native library.

### New

* Reads xlsx (transitional and strict), xlsb, xls (BIFF8), OpenDocument spreadsheets (ods) and flat ods (.fods); the format is found from the content, not the file name. Writes xlsx. Both directions stream: rows are read and written batch by batch, never the whole sheet in memory.
* The package's own API: `ExcelWorkbook` (sheets of one workbook, opened once), `ExcelReader` / `ExcelReadOptions` and `ExcelWriter` / `ExcelWriteOptions`.

### Changed

* Values are written by value, never rounded silently: integers within ±2^53 and decimals that a double holds exactly as numbers, other values as text; `NaN` and infinities as `#NUM!`; date-times with a time zone as wall clock. Values Excel cannot hold (dates before 1900, sub-microsecond times, binary and nested columns) are refused, naming the column.
* Encrypted workbooks are refused with an error saying so; xls files older than BIFF8 (Excel 95 and before) are refused by name.
* Column types are inferred from the first 100 rows by default (as Python Polars); a later cell that does not fit fails, naming the cell.

## Parquet: managed reader and writer (Kaminari.Seseragi.Files)

A Parquet implementation of its own, for .NET code that reads or writes Parquet without the engine (`DataFrame.ReadParquet`, `ScanParquet`, `WriteParquet` and `SinkParquet` still run on the engine).

### New

* **Files** (`ParquetFile`): from a path, memory (read in place), a stream, or any source of ranged reads, synchronously or asynchronously; the footer as `ParquetMetadata` and the schema as Arrow's. Memory mapping is chosen automatically. Encrypted files are refused by name.
* **Reading** (`ParquetReader`, `ParquetReadOptions`): every column, flat or nested, as Arrow record batches, on all cores by default; every encoding Parquet writers use (dictionary, DELTA_*, BYTE_STREAM_SPLIT, RLE booleans) and codecs (Snappy, GZIP, Brotli, Zstandard, LZ4_RAW and the deprecated Hadoop LZ4; not LZO). Strings in Arrow's view layout by default. Row selection, a row index column and `MaxRows`.
* **Pushdown**: a row filter prunes row groups by their statistics, Bloom filters and page indexes, and filters the rows read (late materialization: other columns decoded only for rows that pass). Strings are filtered by prefix, suffix and substring too (`StartsWith`, `EndsWith`, `Contains`, ordinal).
* **Rows as objects**: `ParquetFile.OpenReader<T>(where)` / `ReadList<T>` read rows as records or classes (F# records and tuples too), the `where` lambda pushed into the read as the LINQ provider translates it.
* **Datasets** (`ParquetDataset`): many files as one table, from directories, globs or file lists; Hive partitions and the file path column (both pruned by the filter: `file == "…"`, prefixes, suffixes); schemas from the first file, united by name, or given, with lossless widening between files (a map also reads as the list of its key/value entries, for engines without a map type); INT96 timestamps as UTC instants on request (`ParquetReadOptions.Int96Timezone`, as Spark stores them); local files, memory, HTTP and the cloud stores, read ahead and in parallel. Typed rows too (`ParquetDataset.OpenReader<T>`).
* **Writing** (`ParquetWriter`, `ParquetWriteOptions`): Arrow record batches to a stream or a path, synchronously or asynchronously; flat and nested columns of every Arrow type Parquet holds (maps as MAP), field ids, statistics, page indexes, size statistics, Bloom filters, every encoding, the codecs Snappy, Zstandard, GZIP, Brotli and LZ4_RAW, data page v1 and v2; columns encoded and pages compressed on several threads. Values Parquet cannot hold exactly are refused, never changed. Files are read by pyarrow, DuckDB, Polars and Spark (verified).

## JSON: managed reader and writer (Kaminari.Seseragi.Files)

A JSON reader and writer of its own, newline-delimited and documents. Polars.NET's JSON functions (`ReadJson`, `ScanNdjson`, `WriteJson`, `WriteNdJson`, `SinkJson`, the partitioned sinks) now run on it; the native library no longer contains JSON input and output (the expression functions `Str.JsonDecode`, `Str.JsonPathMatch` and `Struct.JsonEncode` stay in the engine). Output is byte for byte what the engine wrote.

### New in Polars.NET

* `LazyFrame.ScanNdjson(path)` reads a directory of files, a glob, or any of them on a cloud store (`s3://`, `az://`, `gs://`, through the managed object store clients) as one table: the files are listed and the columns inferred when the frame is created, the data read each time the query runs (only the columns and leading rows it uses). The row index numbers rows across the files.
* New trailing optional parameters: `schemaOverrides` on `ReadJson` and `ScanNdjson` (the types of some inferred columns, or of some of a schema's columns), `hivePartitioning` on `ScanNdjson(path)` (off by default), `existingData` on `SinkJsonPartitioned` / `WriteJsonPartitioned` with the new `ExistingData` enum (`Error`, the default, `Append`, `ReplacePartitions`). Recompile code built against an earlier version.
* Types the engine's reader refused read now: `Int8`, `Int16`, `UInt8`, `UInt16`, `Time`, `Duration`, `Decimal`, `Array`, `Binary` (base64), and in documents datetimes with an offset; binaries are written (as base64) where the engine's writer panicked.
* Faster (16 threads): writing 5–8× (the engine wrote on one thread), reading documents 9–17×, reading and scanning nested lines 3–7×; flat lines within 10 % of the engine. `ReadJson(Stream)` reads the stream as it arrives instead of copying it whole first; `ReadJson(byte[])` reads the bytes in place.
* `JsonReadOptions.RowIndexColumn` (Seseragi.Files): an Int64 column numbering the records before any filter, across a dataset's files.

### Changed in Polars.NET

* `schema` is the whole schema, as in Python Polars: nothing is inferred, fields it does not name are passed over. It was passed to the engine as overrides (a partial schema inferred the rest, and a full one could still fail: "expected null in json value, got object"). Pass `schemaOverrides` to type some columns only.
* `ignoreErrors`: a value that does not fit its column's type becomes a null (syntax errors are still errors).
* A `String` column takes any other JSON value as its JSON text; a `Binary` column takes base64 strings.
* Errors of `ReadJson` are `JsonFormatException` (not JSON of the layout read) and `JsonValueException` (a value its column cannot hold), naming the line, instead of `PolarsException`; a scan reports them through the query that runs it.
* Partitioned sinks write Hive directories of files named `part-{index:D5}-{write id}.jsonl` (`.gz`, `.zst` when compressed), each published whole, and refuse files already under the path unless `existingData` says otherwise. `keysPreGrouped` is a hint: keys that are not grouped write more files, never wrong ones.
* A sink with no rows and compression writes one empty gzip member or Zstandard frame (a file of no bytes is not compressed data).
* `lowMemory` and `rechunk` of `ScanNdjson` are accepted and change nothing.

### New in Seseragi.Files

* **Reading** (`JsonReader`, `JsonReadOptions`): one object per line, as Polars' `read_ndjson` reads it, from a path (memory-mapped), a stream or any source of ranged reads, synchronously or asynchronously, as Arrow record batches in native memory, decoded on all cores and handed over in order; gzip, zlib and zstd input is decompressed on the way. A projection decodes only the columns asked for, a row limit stops early. Each line is checked to be UTF-8 and one JSON object; failures name their line.
* **Columns**: given (a full schema: nothing inferred), inferred from the first records (or all) with Polars' types and column order, or inferred with some types overridden. Values convert to every Arrow type JSON holds: integers of every width, floats, decimals (exact), strings, base64 binaries, dates, timestamps, times and durations in ISO 8601, lists, fixed-size lists, structs, maps and dictionary-encoded strings.
* **Documents** (`Format = JsonFormat.Document`): an array of objects (indented or compact) or a single object, as Polars' `read_json` reads them, cut between elements by a vectorized scan in parallel (a store's object or an unmapped file read in parallel too) and decoded on all cores like lines (Polars reads a document on one thread); a field first seen after the inferred records is an error, as in Polars. Broken documents (elements that are not objects, missing elements, data after the document, an unclosed array) are refused at their line and array index.
* **No columns** (`Columns = []`): batches that only count the rows, every record still checked.
* **Memory and text**: `JsonReader.Open(ReadOnlyMemory<byte>)` reads UTF-8 in place, `JsonReader.Parse(string)` reads JSON text.
* **Writing** (`JsonWriter`, `JsonWriter<T>`): record batches or rows of records written as Polars' `write_ndjson` / `write_json` write them, byte for byte (time zones, years beyond 9999 and floats included), to a path, a stream, an `IBufferWriter<byte>` (a `PipeWriter` flushed as it goes), a `TextWriter` or a string, encoded on all cores (documents several times faster than Polars, which writes them on one thread); gzip and Zstandard compression side by side. Binaries are written as base64 and maps as objects (Polars fails on both); strings that are not UTF-8 are refused.
* **Typed rows** (`JsonReader<T>`, `JsonReader.ReadList<T>` / `ParseList<T>`): JSON read as rows of records, the columns being the members (nothing inferred), names matched ignoring case, GUIDs from strings, enums from names or numbers, dictionaries keyed by strings from objects, time spans also as .NET writes them: what System.Text.Json writes reads as it is. A `where` predicate is evaluated by the reader on each chunk, in parallel. `JsonWriter<T>` writes GUIDs as strings and dictionaries keyed by strings as objects, so the two round-trip.
* **Row filter and key matching** (`JsonReadOptions.Filter` / `FilterRows`, `KeysIgnoreCase`): batches filtered by an IR predicate as they are decoded; fields matched to columns ignoring case. A column that is not nullable takes no null.
* **Datasets** (`JsonDataset`, `JsonDatasetWriter`): many JSON files read as one table (paths, directories, globs, file lists, stores, Hive partitions pruned by the filter, the file path column; columns given, inferred from the first file as Polars' `scan_ndjson` infers them, or united from every file; files decoded side by side, compressed ones too), and written as one (Hive partitions, `.jsonl` / `.json`, gzip / Zstandard, files cut by rows or size). Typed rows both ways (`JsonDataset.ReadList<T>`, `JsonDatasetWriter<T>`).
* **Memory in, buffer writers out** (Seseragi.Files): Excel workbooks and Avro files open from `ReadOnlyMemory<byte>` (read in place, like Parquet, IPC and JSON); the Parquet, IPC, Avro and Excel writers write to an `IBufferWriter<byte>` too (an array or native buffer writer, or a `PipeWriter` flushed as it goes, so writing waits for its reader).
* Where Polars changes or drops values, the reader refuses them instead: fractions of a second finer than a column's unit (Polars cuts them), lone surrogate escapes (Polars reads `\0`), values of mixed kinds in an array without strings (Polars sometimes makes them strings). Unlike Polars, a leading byte order mark is accepted, and integers into Int8 / UInt8, times, durations, decimals and binaries are read (Polars fails or panics on them). A column of empty objects only is refused (Apache Arrow for .NET has no struct type without fields). In documents, booleans mixed with integers and fractions into integer columns are refused (Polars' `read_json` reads `true` as 1 and cuts 2.5 to 2), objects and arrays into string columns are read as their text (Polars refuses them), and `[{}]` or `{}` is one row (Polars reads none).

## CSV: managed reader and writer (Kaminari.Seseragi.Files)

A CSV reader and writer of their own. Polars.NET's CSV functions (`ReadCsv`, `ReadCsvAsync`, `ScanCsv`, `WriteCsv`, `WriteCsvAsync`, `SinkCsv`, `SinkCsvAsync`, the memory and partitioned forms; C# and F#) now run on them; the native library no longer contains CSV input and output. What the engine read reads the same (checked on 45 corpus files, legacy encodings included) and what it wrote is written byte for byte, except where listed below.

### New in Polars.NET

* **Encodings**: `CsvEncoding` is now a struct naming any encoding .NET knows, legacy code pages included, with nothing to register: `CsvEncoding.UTF8` (the default), `ShiftJis` (code page 932), `Gbk`, `Big5`, `EucKr`, `Windows1252`, `Latin1`, `Utf16LE` / `Utf16BE`, a code page (`932`), a name (`"shift_jis"`) or a `System.Text.Encoding`, strict or `.Lossy()`. Reads honour a byte order mark; writes take an `encoding` too. In F#, `CsvEncoding.CodePage 932`, `CsvEncoding.Named "shift_jis"`, `CsvEncoding.Of encoding`. Code that passes `CsvEncoding.UTF8` / `LossyUTF8` compiles unchanged; code that matches on the old F# union cases does not.
* `ReadCsv(path)` and `ScanCsv(path)` read a directory of files, a glob, or any of them on a cloud store as one table: columns matched by name across the files, the types inferred from the first files, the row index numbering rows across them.
* New trailing optional parameters (recompile code built against an earlier version): `trueValues`, `falseValues` (localized booleans such as `VERDADEIRO` / `FALSO`) and `thousandsSeparator` on reads; `allowMissingColumns`, `ignoreExtraColumns` and `hivePartitioning` (off by default) on path reads; `encoding` and `thousandsSeparator` on writes; `existingData` on the partitioned writes.
* Faster (16 threads; median, same query and file): lazy queries over a scan 1.5–7× (a filter keeping 1 % of Chicago's crimes 2.5×, of the 2.9 GB US Accidents file 7.6×; group-by 2–3×), full reads 1.5–7×, writes about 1.4×. `ReadCsv(Stream)` reads the stream as it arrives instead of copying it whole first; `ReadCsv(byte[])` reads the bytes in place.

### Changed in Polars.NET

* A `schema` with a header is matched to the columns **by name** (the frame in the schema's order); without a header, by position. The engine renamed the file's columns by position, silently swapping columns whose order differed.
* `inferSchemaFiles` defaults to 10 files, as dataframe engines take (`ulong.MaxValue`, the old default, also means 10): types are inferred from the first ten files of a dataset.
* Errors are `CsvFormatException` (malformed CSV) and `CsvValueException` (a value its column cannot hold), naming the file, the row and the column, with the fix where there is one, instead of `PolarsException`; a scan reports them through the query that runs it. A value more precise than its time column's unit (nine fraction digits into the microseconds inferred from the first rows) is refused and names the finer unit to give (`datetime[ns]`); the engine truncated it silently.
* A column holding both dates and datetimes is read as datetimes. Records may end with a CR alone. `ignoreErrors` reads a value its column cannot hold as null.
* Empty input follows `raiseIfEmpty`: an error by default, an empty frame when it is off. `ScanCsv(byte[])` refused an empty array with `ArgumentException`.
* Writes: a null float written with `decimalComma` is an empty field (not `""`), `Float32` values are written from their own shortest digits, durations as ISO 8601 (`PT1.5S`) instead of being refused.
* Partitioned writes produce Hive directories of files named `part-{index:D5}-{write id}.csv` (`.gz`, `.zst` when compressed), each with its header and published whole, the key columns kept in the files (`includeKeys`), and refuse files already under the path unless `existingData` says otherwise.
* `lowMemory`, `cache`, `chunkSize` and `batchSize` are accepted and change nothing; `nThreads` sets the reader's threads.
* Scans run as .NET sources of the engine (see Streaming over .NET sources): a query's filter runs on the batches read, not in the reader; an in-memory collect reads the columns the query uses whole first (filter a projection, not the whole file, when memory matters); a sink spools the rows to a temporary file first, so scan → sink of a file that parses fast is up to 1.4× slower than the engine's native CSV, and the spool goes to `POLARS_TEMP_DIR` (RAM on a tmpfs `/tmp`: point it at a disk for large files).

### New in Seseragi.Files

* **Reading** (`CsvReader`, `CsvReadOptions`): from a path (memory-mapped), memory (in place), a stream or any source of ranged reads, synchronously or asynchronously, as Arrow record batches in native memory, decoded on all cores and handed over in order; gzip, zlib and zstd decompressed on the way; any encoding, transcoded to UTF-8 on all cores. Types inferred as Polars infers them (or given, or some overridden), separators, quotes, comment prefixes, skipped lines and rows, null values, decimal comma, thousands separators, true and false values, ragged lines, a row index, a projection that decodes only the columns asked for, a row limit that stops early. A row filter (`CsvReadOptions.Filter`, an IR predicate) decodes the columns it tests first and the others only for the rows kept.
* **Writing** (`CsvWriter`, `CsvWriteOptions`): Arrow record batches written as Polars writes them, to a path, a stream or an `IBufferWriter<byte>`, encoded on all cores, in any encoding, gzip or Zstandard compressed.
* **Datasets** (`CsvDataset`, `CsvDatasetWriter`): many CSV files read as one table (paths, directories, globs, stores, Hive partitions pruned by the filter, the file path column) and written as one (Hive partitions, files cut by rows or size).
* **Typed rows** (`CsvReader<T>`, `CsvWriter<T>`, `CsvDataset.ReadList<T>` / `OpenReader<T>`, `CsvDatasetWriter<T>`): CSV read as rows of records and records written as CSV, one column per member. The header is matched by name, in any case; other columns are passed over; a member the file has no column for is an error naming it. GUIDs, enums (names or numbers), time spans and decimals (without trailing zeros) round-trip; a `where` predicate is pushed into the reader. Members a CSV field cannot hold (records, collections, bytes) are refused by name.

## Delta Lake: managed Parquet writer

Delta data files are written by Polars.NET's own Parquet writer (Seseragi.Files) instead of the engine's Parquet sink. The engine still computes the rows, batch by batch; the writer encodes them and the store takes the bytes as they come: a temporary file renamed on a local disk, parts uploaded on a cloud store.

### New

* **Map columns are written as Parquet maps.** Tables with map columns (made by Spark or Databricks) can be appended to, updated, merged into, optimized, and have deletion vectors, type widening or column mapping dropped; Spark reads the files back. Appends and merges take a list of two-field structs (the engine's map) as the table's map, by position, whatever the fields' names.
* `POLARS_NET_DELTA_SINK_MEMORY` sets how much memory the files being written may hold together: a size (`2GiB`, `512MiB`) or a share of the memory the process may use (`25%`). By default an eighth of the memory available to the process (a container's limit, not the machine's), at least 256 MB.
* The engine's Parquet sink is no longer used for Delta data files (`POLARS_NET_DELTA_SINK` is gone). Where the managed storage clients do not serve the store, the files go through the engine's object store, each held whole in native memory until it closes, within `POLARS_NET_DELTA_SINK_MEMORY`; tables with map columns are written there too.
* Checkpoints are read with the managed Parquet reader and written in native memory, streamed to the store; a large checkpoint no longer passes through the managed heap.
* Deletion vectors are decoded into native bitmaps (roaring), off the managed heap.
* Writes into many partitions hold less memory: each open file keeps its levels a byte each and takes its buffers only when it needs them (10,000 partitions of 10M rows: about 1 GB less at the peak; there, twice as fast as the engine's sink in our measurements). Beyond `POLARS_NET_DELTA_SINK_MEMORY`, row groups end early, as many as it takes, before any file closes early.
* On S3, Azure and Google Cloud Storage, data files and checkpoints are uploaded in parts while they are written (S3 multipart uploads, Azure blocks, GCS multipart and resumable uploads), never held whole; a write that fails or is given up aborts its upload.
* S3: the `aws_unsigned_payload` setting is honoured. By default, uploads over HTTPS no longer hash their data into the request signature (TLS keeps it whole), as the AWS SDKs do; `false` signs it.

### Changed

* `approxBytesPerFile` and `delta.targetFileSize` are sizes on disk: a file closes when one more row would take it past the target, as far as its compressed bytes so far tell. They were the rows' size in memory, and files split only between row groups, so tables of wide rows got files far beyond the target.
* Partitioned writes group rows by their partition values without sorting each batch in the engine. A partition column of floats takes -0.0 and 0.0 as one partition, and every NaN as one, also with several partition columns.

### Fixed

* Float and double partition values `NaN`, `Infinity` and `-Infinity` were logged as null, and read back as null. They are now written as Spark writes them.

## Delta Lake: SQL expressions and table operations

### New

* **Check constraints, column invariants, generated columns and column defaults** are evaluated as Spark 4 evaluates them (ANSI semantics, Spark's session time zone) on appends, merges, updates and optimize; tables that use them were read only. A violating row fails the write with Spark's message, nothing committed.
* **`Delta.AddConstraint` / `DropConstraint`** (Spark's `ADD` / `DROP CONSTRAINT`): the table's rows are checked first. **`CreateTable`** takes `generatedColumns` and `columnDefaults`; **`SetColumnDefault` / `DropColumnDefault`** change a default. Each declaration is checked as Spark checks it, and brings its table feature.
* **`Delta.SessionTimeZone`**: the zone those expressions read instants in, process-wide, UTC by default.
* **`Delta.AddColumns`** (`ADD COLUMNS`, nested fields and positions too), **`UnsetTableProperties`** (`UNSET TBLPROPERTIES`), **`Detail`** (`DESCRIBE DETAIL`), **`ReplaceTable`** (`REPLACE TABLE`, `CREATE OR REPLACE TABLE`), **`Clone`** (`SHALLOW CLONE`) and **`ConvertToDelta`** (`CONVERT TO DELTA`, Parquet directories, Hive partitions), as delta-spark 4.4 commits them. Unity Catalog: `AddCatalogConstraint`, `DropCatalogConstraint`, `SetCatalogColumnDefault`, `DropCatalogColumnDefault`, `AddCatalogColumns`, `UnsetCatalogTableProperties`, `CatalogTableDetail`.
* **`UnityCatalog.ReplaceCatalogTable`** (`REPLACE TABLE` / `CREATE OR REPLACE TABLE` on a managed, catalog-managed table) as Delta Spark commits it through Unity Catalog: the table keeps its id and its `delta.*` properties, unchanged columns keep their column ids, and the catalog gets the new columns.

### Changed

* `delta.constraints.*` properties are refused by `CreateTable` and by writes that create a table, as by `SetTableProperties`: constraints are added with `AddConstraint`, which checks them.
* A commit to a catalog-managed table that would change or remove a property the catalog requires of a managed table (deletion vectors, in-commit timestamps, V2 checkpoints and their statistics, the catalog's table id) is refused before the catalog sees it; Unity Catalog 0.7 would take it and then refuse the table's next protocol change.

### Fixed

* Shallow clones made by Spark (data files and deletion vectors at absolute `file:/…` paths) could not be read.
* `ReplaceTable` with column mapping numbered the columns from 1 again with new physical names; it now goes on after the old highest id and keeps the id of a column whose name and type stay, as Spark. With rows, column mapping and the change data feed on, a replacement that drops, renames or retypes a column is refused, as Spark refuses it.

## Arrow interop

* Arrow data handed between the engine and .NET (exports to Arrow, record-batch scans) no longer reports its native memory to the .NET garbage collector as memory pressure. Apache.Arrow's importer did so for every buffer, which forced a full (gen2) collection for nearly every batch.

## Rows as objects

### Changed

* A row (or a nested record) whose mapped columns are all null is an object of nulls in `DataFrame.Rows<T>()` (and what is built on it) and every reader of typed rows (Parquet, Arrow IPC, JSON); it was `null`, which a `List<T>` of records then held as a null element. Query results keep folding such an object to `null`, as an outer join leaves a missing side (`from c in g.DefaultIfEmpty()`).
* A null a member cannot hold names its row counted from the first row read (it named the row within the batch being mapped).

## LINQ

### New

* Checked sums per group (`g.Sum(x => x.L)` of int / long, distinct values too) take one exact sum per group in the engine, checked by a native kernel: about twice as fast (10M rows, 1000 groups: long 4.6× → 2.1× the plain sum, int 3.2× → 1.5×).
* `Average` of long values is the exact checked sum over the count, as .NET computes it, taken per group as one exact sum and a count: about 1.5× faster (10M rows, 1000 groups: 3.1× → 2.2× the plain mean), and exact where large values cancel (the engine's floating sum lost them: 2^62, 1, -2^62 averaged to 0, not 1/3).

### Changed

* Checked conversions (`checked((int)x)`) give null where .NET throws `OverflowException` (the value leaves the target type's range or is NaN; floating values are truncated toward zero). They wrapped before. A conversion cannot fail the query: the engine evaluates both sides of `&&` and `?:` and merges `Where` filters, so it would fail on rows the query guards against.
* Conversions between `decimal` and `double` / `float` follow .NET 11 on every runtime. `(double)m` and `(float)m` are correctly rounded (the engine's cast was one unit in the last place off for about a quarter of the values, and `(float)m` now rounds once, not through a double). `(decimal)x` converts the exact binary value of `x`, as .NET 11 does (`(decimal)0.1f` is `0.100000001490116119`, where .NET 10 gave `0.1`; the engine's cast, computed in floating point, could be off in the last digits), kept to the provider's 18 fractional digits as `Math.Round(value, 18)`. `NaN`, infinities and values of `1e20` or more in magnitude give null (the provider's decimals hold 20 integer digits, where `System.Decimal` holds up to 7.9e28).

### Fixed

* A sum over a group's distinct values (`g.Select(x => x.L).Distinct().Sum()`) wrapped on overflow instead of throwing `OverflowException`.
* A `decimal` cast to an integer (`(int)m`, `checked((int)m)`, `decimal.ToInt32(m)`) rounded (`(int)2.9m` was 3, `(int)-2.9m` was -3); it truncates, as in .NET (out of range it still gives null, where .NET throws).
* `Convert.ToXxx` between numbers and `bool` (also of boxed values, and `Convert.ToString(bool)`), `double.IsNaN` and its kin (`IsInfinity`, `IsFinite`, `IsNormal`, `IsInteger`, ... on `double`, `float` and `Half`) and `char.IsDigit` and its kin (`IsLetter`, `IsUpper`, `IsWhiteSpace`, `IsAsciiXxx`, ...) ran as row-by-row .NET callbacks; they are native expressions now, with .NET's results (`Convert.ToInt32(2.5)` is 2; out of range or `NaN`, where .NET throws, null). `Convert.ToInt32((string)null)` is 0, as in .NET (it was null); `Convert.ToDateTime(string)` is `DateTime.Parse`.
* `DateTime.Parse` (and `DateOnly.Parse`, `TimeOnly.Parse`) applied the format of the column's first value to every row: after a date-only first row (`"2024-01-02"`), `"2023-12-31 10:00:00"` lost its time. ISO 8601 text (`T` or a space before the time, seconds and fraction optional, surrounding white space ignored) is read row by row; other text is parsed as before.
* `bool.Parse(s)` and `Convert.ToBoolean(s)` failed the query (the engine casts no string to a boolean); they read `true` / `false` ignoring case and white space, as .NET does, and give null for other text.
* A value's text in a string concatenation, interpolation, `string.Format`, `string.Concat` or `string.Join` was the engine's (`"v=" + 2.0` gave `v=2.0`, `1e16` gave `1e+16`, `true` gave `true`, a date `2023-12-31 10:05:00.000000`); it is .NET's under the current culture (`v=2`, `10000000000000000`, `True`, `12/31/2023 10:05:00 AM` in en-US), and `x.ToString()`, `x.ToString(culture)` and `Convert.ToString(x)` of numbers, booleans, dates and times run natively with the same text (they were row-by-row .NET callbacks). Doubles and floats take .NET's shortest round-trip form (`1E+17`, `1E-05`, `-0`, `∞`, ties to the even digit); the current culture is part of the compiled query's cache key. A `decimal` keeps its column's scale.

## Native memory

* The managed readers and writers (Avro, Excel, Parquet, Arrow IPC, Delta's data files) take their buffers from a native memory pool that keeps freed blocks for reuse. It now keeps no more than the process had in use at once lately (the peak fades within seconds once the load drops), and a request may take a kept block up to half as large again instead of mapping fresh pages: writers hold 5–12 % less memory at their peak in our measurements, at about the same speed. Blocks unused for a second go back to the system, as before.
* `POLARS_NET_POOL_RETAIN` sets the most the pool ever keeps: a size (`512MiB`, `2GiB`; `0` keeps nothing) or a share of the memory the process may use (`10%`). By default 1 GiB. A value that cannot be read leaves the default.

---

# Polars.NET 1.2.0 Release Note

![icon](assets/icon_lite.png)

Polars.NET 1.2.0

For full architectural guides, API references, and benchmarks, please visit documentation site:
**[Polars.NET Documentation Site](https://errorlsc.github.io/Polars.NET)**

---

## Delta Lake: managed kernel

Delta Lake now runs on Seseragi.Lakehouse, a managed implementation of the Delta protocol, instead of the native `delta-rs` bindings. The log, checkpoints, deletion vectors, column mapping, type widening, in-commit timestamps and the change data feed are handled in .NET; the Polars engine reads and writes the Parquet data files. Behaviour is checked against Apache Spark 4.2 with delta-spark 4.4, in both directions. The native library drops `delta-rs` and its dependencies.

### New

* **File skipping for LINQ queries**: a LINQ query over the frame `ScanDelta` or `ScanCatalogTable` returns (C# and F#) reads only the data files that may hold a row its filter keeps, judged by the log's partition values and statistics, at the version the scan read. The filter is the LINQ compiler's IR as it is, judged by the statistics pruning of Polars.NET's Parquet reader: comparisons, ranges, `IN` lists, null checks, `StartsWith`, `IsNaN`, integer arithmetic and column pairs, on columns and struct fields (column mapping and type widening included). Scans with `nRows` or `rowIndexName` read every file; the DataFrame API's `Filter` leaves no file out.
* Writes stream: `WriteDelta` / `SinkDelta` no longer collect the frame. Files are split by partition, `maxRowsPerFile` and size. The sizes and statistics of the written files are read from each file's end, several files at once (usually one request per file on S3 and Google Cloud Storage, two on Azure), instead of listing the table's directories.
* Delete, merge and optimize stream their files the same way, change data files included: rows go from the scan straight to the engine's Parquet sink and never pass through managed memory as file bytes. New data and change data files are split by partition and by `delta.targetFileSize` (they used to be one file per partition), and their statistics come from the file footers. Optimize writes one file per compacted bin; Z-ordered rows are cut into files of about the target size. Merge plans in two passes, as Spark does: the first pass keeps only which target rows change and how, the second runs the join again and streams the new rows into files, so changed rows are no longer held in memory whole.
* **Identity columns** (`GENERATED ALWAYS / BY DEFAULT AS IDENTITY`, as Spark and Databricks create them) can be written: appends, overwrites and merge inserts generate their values after the high water mark, and the commit records the new mark. Spark's rules apply: a GENERATED ALWAYS column takes no values from the data, a GENERATED BY DEFAULT column keeps the values the data brings (nulls included) without moving the mark, and identity columns are never updated. Values of one write are consecutive steps (Spark leaves gaps between its tasks). A restore keeps the current high water mark, as Spark does. `Delta.CreateTable` creates a table with identity columns (`DeltaIdentityColumn`: start, step, GENERATED ALWAYS or BY DEFAULT).
* **Row tracking** (`delta.enableRowTracking`): new files get their base row ids and default row commit versions when the commit lands, after the ids of concurrent commits. Deletes, merges and optimize keep each row's id, and updates keep it too. Rows that are not changed also keep their commit version; updated rows take the version of the update. The ids are kept in the table's materialized columns, as Spark keeps them, and change data files carry them too. The property can be set when a table is created (`Delta.CreateTable(..., properties)`) or on an existing table. On an existing table it backfills ids into the files that have none, in one commit (Spark takes three). `ScanDelta` / `ReadDelta` / `ScanCatalogTable` return each row's id and commit version with `rowIdColumn` / `rowCommitVersionColumn` (Spark's `_metadata.row_id` / `_metadata.row_commit_version`). `delta.rowTrackingSuspended` (`DeltaTableProperties.RowTrackingSuspended`) stops assigning ids to new files, as Spark; it is refused together with `delta.enableRowTracking`. Unlike Spark, lifting the suspension and turning tracking on can be one change, and a table that has both set (which Spark cannot repair) can be fixed by setting either to false.
* **Liquid clustering** (`CLUSTER BY`, the `clustering` table feature): `Delta.CreateTable(…, clusterBy)` and writes that create a table take up to four clustering columns (nested fields too); `Delta.Optimize` clusters the data by them (a Hilbert curve over each column's sampled range ids, every range of curve keys written to its own files; one streaming pass, nothing collected), with `full: true` also reclustering data clustered by earlier columns (`OPTIMIZE FULL`); `Delta.ClusterBy` changes the columns (`ALTER TABLE … CLUSTER BY`, none for `NONE`). Spark's rules apply (no partitioning, no Z-order, columns with statistics, at most four), files and commits are recorded as delta-spark 4.4 records them (`clusteringProvider: liquid`, ZCube tags), and tables Spark or Databricks clustered are written and optimized incrementally (unclustered files merged into small cubes, data beyond about 150 GiB cut into further cubes, as Spark's). Unity Catalog: `SinkCatalogTable(clusterBy)`, `OptimizeCatalogTable(full)`, `ClusterCatalogTable`.
* **Column changes** (`ALTER TABLE … RENAME COLUMN` / `DROP COLUMNS` / `ALTER COLUMN … TYPE`): `Delta.RenameColumn`, `Delta.DropColumns` and `Delta.ChangeColumnType` (Unity Catalog: `RenameCatalogColumn`, `DropCatalogColumns`, `ChangeCatalogColumnType`) change the schema in one metadata commit, no data rewritten, committed as delta-spark 4.4 commits them. Columns are named as in Spark: dotted for nested fields, `element` / `key` / `value` inside arrays and maps. Renaming and dropping need column mapping; partition columns and `delta.dataSkippingStatsColumns` follow a rename, and a dropped column's entries leave the stats columns. Partition and clustering columns cannot be dropped; unlike Spark, neither can a struct that holds a clustering column (Spark allows it and then fails on later changes to the table). Type changes need type widening and record the change as Spark does. `Delta.DropColumnNotNull`, `Delta.SetColumnComment` and `Delta.MoveColumn` (`ALTER COLUMN … DROP NOT NULL` / `COMMENT` / `FIRST` / `AFTER`; Unity Catalog: `DropCatalogColumnNotNull`, `SetCatalogColumnComment`, `MoveCatalogColumn`) let a column, nested field, array element or map value hold nulls, set a column's comment and move a column within its struct, each one `CHANGE COLUMN` commit as delta-spark 4.4 records it.
* **Purge** (`REORG TABLE … APPLY (PURGE)`): `Delta.Purge` (Unity Catalog: `PurgeCatalogTable`) rewrites the files that still hold dropped columns (at any depth, found from their Parquet footers) and the files with deletion vectors, packed per partition (on a clustered table, into new cubes), in one commit that changes no data, choosing the files as delta-spark 4.4 chooses them; `partitionFilters` limits it to some partitions (`REORG TABLE … WHERE`); it returns the number of files rewritten. Purge and then vacuum to remove dropped columns and deleted rows from storage. Dropping deletion vectors now rewrites the files with dropped columns too, as Spark does. Catalog-managed tables refuse it, as Spark does.
* **Update**: `Delta.Update(path, predicate, set)` sets columns of the rows matching a predicate (all rows without one), as SQL `UPDATE`; `DataFrame.UpdateDelta` / `LazyFrame.UpdateDelta` update the matched rows of a merge by key, optionally filtered by a condition, and insert nothing. Identity columns cannot be updated. On Unity Catalog tables: `UnityCatalog.UpdateCatalogRecords(catalog, schema, table, predicate, set)` and `DataFrame` / `LazyFrame.UpdateCatalogRecords(uc, catalog, schema, table, mergeKeys, condition)` (catalog-managed tables commit through the catalog).
* **Column mapping with nested columns** can be written (appends, overwrites, deletes, updates, merges, optimize, change data files): every struct field, at any depth (inside array elements and map values too), is written under its physical name, as Spark writes it. Schema evolution adds nested fields to such tables, with the next column ids and fresh physical names; new tables number nested fields as Spark does.
* **Type widening at any depth:** tables whose struct fields, array elements or map keys and values Spark widened (`ALTER COLUMN ... TYPE`: wider integers, float to double, wider decimals, integers to decimal or double, date to timestamp_ntz) are read, older files cast up to the current types, and rewritten in the new types by deletes, updates, merges and optimize. Writes with schema evolution on a table with `delta.enableTypeWidening` widen nested types too, recording the change as Spark does (on the struct field, or on the column with the path to the array element or map key or value).
* Tables with the change data feed can be written (writes, deletes, merges and restores feed it). Previously writers failed with `Unsupported table features required`.
* Deletion vectors follow the protocol and are read and written by Spark and Databricks.
* `AddFeature` and `SetTableProperties` follow Spark's `ALTER TABLE ... SET TBLPROPERTIES`: properties that turn a feature on bring it into the protocol, `delta.feature.<name>` adds a feature, `delta.minReaderVersion` / `delta.minWriterVersion` raise the versions. Column mapping can be turned on (`none` to `name`), and in-commit timestamps for an existing table.
* The table properties `delta.dataSkippingNumIndexedCols` and `delta.dataSkippingStatsColumns` choose the columns that get statistics.
* **Statistics of nested fields:** data files get the minimum, maximum and null count of struct fields at any depth, nested under their structs as Spark writes them (and selected as Spark selects them: struct fields count as leaves, dotted names in `delta.dataSkippingStatsColumns`), so filters on nested fields (and liquid clustering by them) skip files. Arrays and maps get none: their null counts cannot be read from a Parquet footer.
* **Checkpoints carry typed statistics**, as Spark writes them: `add.stats_parsed` (each file's minimum, maximum and null counts in the table's own types, nested fields nested, physical names under column mapping, `tightBounds` with deletion vectors) and `add.partitionValues_parsed` (typed partition values), so readers skip files without parsing JSON. `delta.checkpoint.writeStatsAsStruct` and `delta.checkpoint.writeStatsAsJson` turn the typed and the text statistics off, as in Spark; a table checkpointed with typed statistics only is read and skipped by Spark and by Polars.NET.
* **Dropping table features** (`ALTER TABLE … DROP FEATURE`): `Delta.DropFeature` (Unity Catalog: `UnityCatalog.DropFeature`) drops `deletionVectors`, `typeWidening`, `columnMapping`, `v2Checkpoint`, `vacuumProtocolCheck`, `inCommitTimestamp`, `rowTracking`, `domainMetadata` and `checkConstraints`, committing what delta-spark 4.4 commits: the feature's traces removed step by step (properties turned off; deletion vectors purged, files in older types rewritten in the table's types, files rewritten under logical column names, row ids kept), then a reader feature dropped behind three checkpointed empty commits under `checkpointProtection`, a writer feature in one commit. An interrupted drop is finished by running it again. Unlike Spark, dropping column mapping from a clustered table renames the clustering columns too (Spark leaves names its own `OPTIMIZE` then fails on).
* **Checkpoint protection** (`checkpointProtection`, which Spark adds when it drops a reader feature): such tables are written, and log cleanup keeps the history before the protected version until it can all go at once, commits before checkpoints. A restore keeps the protection (Spark's restore loses it).
* **Truncating history** (`DROP FEATURE … TRUNCATE HISTORY`): `Delta.DropFeature(…, truncateHistory: true)` (Unity Catalog: `UnityCatalog.DropFeature(…, truncateHistory)`; a new trailing optional parameter, recompile code built against an earlier version) drops a reader feature by deleting the history instead of protecting it, and drops `checkpointProtection` itself, committing and refusing as delta-spark 4.4 does: the traces removed and an empty commit checkpointed, a wait when traces had to be removed or the history still uses the feature, and once the history is older than `delta.dropFeatureTruncateHistory.retentionDuration` (24 hours) the log before the newest old checkpoint deleted and the feature dropped. The deletion cannot be undone.
* **Variant columns** (`variantType`, as Spark 4 and Databricks write them) are read, as stored (a struct of the `value` and `metadata` binaries, the Parquet Variant encoding), or as JSON text with the new `variantAsJson` option of `ScanDelta` / `ReadDelta` / `ScanCatalogTable` (C# and F#; a new trailing optional parameter, recompile code built against an earlier version), rendered byte for byte as Spark's `to_json` renders them (timestamps in UTC, where Spark uses the session time zone; times and nanosecond timestamps, which Spark does not write but Iceberg and DuckDB do, in the same style, as DuckDB renders them), at any depth (struct fields, array elements, map values). Such tables are read only. Polars refuses Parquet files with a VARIANT annotation (pola-rs/polars#28627), so the data files of such tables are read by Seseragi.Files, Polars.NET's managed Parquet reader: only the columns and leading rows a query uses, by ranged reads, when the query runs; the files of a partitioned table are read together, as one dataset whose partition columns hold the files' partition values, and the filter of a LINQ query also leaves out row groups, pages and rows in the reader. Tables without variant columns are read by the engine as before.
* **Random file prefixes** (`delta.randomizeFilePrefixes`, `delta.randomPrefixLength`; `DeltaTableProperties.RandomizeFilePrefixes` / `RandomPrefixLength`): new data, change data and deletion vector files go into directories of random letters and digits (`Ab/part-…`) instead of the partition directories and `_change_data`, as Spark places them (Spark-created managed tables have it on), spreading them over an object store's prefixes. Bad values are refused when the table is created or the property set, as in Spark.
* **Faster table loading and file skipping on large tables.** Commit files are read token by token (large commits on several threads), each file's statistics are kept as UTF-8 and read for skipping without a JSON document per file, files of a partition share one map of partition values, the actions of a checkpoint are made on several threads, a filter on partition columns alone reads no statistics, and file skipping runs on several threads. On a table of 500,000 files (16 threads, our measurements): loading from JSON commits and skipping files 2.8 s → 0.37 s, from a checkpoint 1.9 s → 0.38 s, from a checkpoint with typed statistics only (`stats_parsed`) 2.7 s → 0.41 s; a table of 5,000 files loads 3× faster and its snapshot holds a third of the memory.
* Delete, merge and optimize are retried as a whole when another writer's commit conflicts with them (`POLARS_DELTA_MAX_RETRIES`, 5 by default); files of a failed attempt are removed.
* On S3, Azure Blob Storage and Google Cloud Storage, the kernel's own file access (log, checkpoints, deletion vectors, vacuum listings) goes through managed clients. They read the same configuration keys and environment variables (`AWS_*`, `AZURE_*`, `GOOGLE_*`, `~/.aws/credentials`, gcloud's application default credentials) and choose credentials in the same order as before: static keys, SAS and bearer tokens, service account keys, web identity (EKS), container credentials (ECS, EKS Pod Identity), the instance metadata services of AWS, Azure (managed identity) and Google Cloud, Microsoft Entra ID client secrets and workload identity, the Azure CLI, gcloud user logins, and credentials vended by Unity Catalog. S3 server-side encryption (including customer-provided keys), requester pays, `http://` proxies (`proxy_url`, `proxy_excludes`, `proxy_ca_certificate`) and `allow_invalid_certificates` behave as before. Setups the managed clients do not implement (`https://` and SOCKS proxies, S3 Express, checksums, conditional puts other than ETags, Microsoft Fabric) keep using the native object store. `POLARS_NET_OBJECT_STORE=native` always uses the native store; `managed` makes a setup the managed clients cannot serve an error instead of a fallback.
* Unity Catalog tables on Google Cloud Storage use the OAuth token the catalog vends (`google_oauth_token`), for scans and writes as well. It used to be ignored, so access needed a service account or the metadata server of a GCP machine.
* **Unity Catalog credentials are renewed.** The temporary credentials a catalog vends (AWS, Azure SAS, GCS token) used to be fetched once per operation, so a lazy scan collected later, or a long write, failed once they expired. They are now fetched again before they expire (half their life before, at most five minutes), by the managed clients and by the engine's own stores reading and writing the data files, for scans, writes and every table operation. A `LazyFrame` from `ScanCatalogTable` keeps working after the `UnityCatalog` is disposed until its credentials expire; renewing them needs the catalog open.
* **Catalog-managed tables** (`catalogManaged`, Unity Catalog's managed Delta tables) are read and written through `UnityCatalog`, as Delta Spark 4.4 does with the Unity Catalog connector: scans include the commits the catalog ratified but nobody published yet, and appends, deletes, merges, schema evolution, clustering, column drops, type changes, properties and restores are committed through the catalog's Delta API (staged, ratified, then published in order; the catalog's copy of properties, columns, protocol and clustering is kept up to date). Verified against open-source Unity Catalog 0.7 together with Spark. Such tables are refused by their path, and `OptimizeCatalogTable`, `PurgeCatalogTable`, `DeltaVacuum`, `RenameCatalogColumn` and `DropFeature` are refused on them, as Spark refuses them. `CreateCatalogTable` (managed) creates them as Spark does through the catalog's Delta API: staged, version 0 written at the location the catalog gives (with the protocol and properties it requires, and those it suggests unless `properties` overrides them), then registered; new trailing optional parameters `partitionBy`, `clusterBy`, `properties`, `identityColumns` and `cloudOptions` (recompile code built against an earlier version). Writing to a catalog table that does not exist (`SinkCatalogTable`, `WriteCatalogTable`) creates it that way with the rows as version 0, in every mode, as Spark's `saveAsTable` / `CREATE TABLE AS SELECT` (an overwrite records `CREATE OR REPLACE TABLE AS SELECT`); it used to fail. The staged table's credentials are renewed through the catalog while the rows are written. `LazyFrame.SinkCatalogTable` and `DataFrame.WriteCatalogTable` take `clusterBy` too (a new trailing optional parameter). Unity Catalog tables on local storage (`file://`) no longer fail for want of vended credentials. Every commit now records a `txnId` in its `commitInfo`, as Spark's do.

### Behaviour changes

* **Versions:** creating a table is one commit, as in Spark. The first write to a new table is version 0 (it used to be version 1, after an empty `CREATE TABLE` version 0). Existing tables keep their history.
* **MERGE follows Spark:** a source row with a null key matches nothing (was an error); several source rows matching one target row are an error only when that row is updated or deleted; source values are cast to the table's column types (keys needed identical types).
* **Schema evolution follows Spark:** an append with `canEvolve` adds the new columns and never drops existing ones (it used to replace the whole schema); an overwrite with `canEvolve` replaces the schema and the partitioning, unpartitioned when `partitionBy` is not given.
* **Change data feed:** readable only for versions where `delta.enableChangeDataFeed` was on (a range that starts earlier is refused); `_commit_timestamp` is `Datetime(us, UTC)` from the protocol's commit time, `_commit_version` is `Int64`. The changes are read with the schema Spark reads them with (the end version's for column-mapped tables, else the latest), and a range across a column rename, drop or type change is refused, as in Spark, since the feed does not record the change; `ReadChangeDataFeed(…, allowSchemaChanges: true)` reads it anyway (C#, F#, Unity Catalog; a new trailing optional parameter: recompile code built against an earlier version).
* **Interval properties are read and checked as Spark reads and checks them.** `delta.logRetentionDuration`, `delta.deletedFileRetentionDuration` and the other retentions are read in every form Spark stores (`7 days`, `interval 1 week`, `2 weeks 1 day`); values without `interval` used to be ignored for the default. Setting one (or creating a table with one) refuses what Spark refuses: text that is no interval, negative values, months or years, and a log retention shorter than the deleted file retention set with it (or its default, 1 week). `delta.checkpointRetentionDuration` and `delta.sampleRetentionDuration` can be set.
* **Log cleanup follows Spark** in where it cuts the log: the retention's cutoff is rounded down to midnight UTC, and the log is cut at the newest checkpoint whose earlier files (by modification time) are all past it, rather than the newest checkpoint whose own commit is. Cleanup may run up to a day later, and then cut up to one checkpoint further, than before.
* **Time travel by time** uses the protocol's commit time: the log file's modification time, or the in-commit timestamp. A time without an offset is UTC.
* **`AddFeature`** of `deletionVectors`, `changeDataFeed` or `appendOnly` also sets the property that turns it on (`delta.enableDeletionVectors`, ...), unless the table sets it already. `appendOnly` therefore makes the table append-only.
* **`SetTableProperties(raiseIfNotExists: true)`** refuses unknown `delta.` properties only; other keys are stored. `delta.constraints.*` is refused (check constraints are added with `Delta.AddConstraint`, in 1.3.0).
* **Optimize:** partition filters may name partition columns only, Z-order columns may not be partition columns (as Spark).
* **Unity Catalog** scans take the schema from the Delta log.
* **Deletion vectors written by earlier Polars.NET versions** did not follow the protocol and are refused with a hint: run `Delta.Optimize` with the earlier version first, so that no file carries a deletion vector.
* **Tables with map columns** (created by Spark or Databricks) are read, but their data files are not written: updates, deletes that rewrite files, merges, optimize, change data files and the rewrites of `DropFeature` (deletion vectors, type widening, column mapping; refused before anything is committed) are refused with a clear error. The engine has no map type and would write the maps as Parquet lists, which Spark then cannot read, leaving the table unreadable to it. Deletes on tables with deletion vectors need no rewrite and work. This restriction is lifted once the engine writes Parquet maps.
* **Column-mapped tables** put new data and change data files under random directory prefixes (`Ab/part-…`), as Spark does, instead of partition directories named after physical column names (`col-…=value/`) and `_change_data`. Readers take file paths from the log, so existing files and other readers are not affected. Data files of partitioned tables are named as Spark names them (`part-00000-<uuid>.c000.snappy.parquet`; unpartitioned tables keep `-c000`).
* **`CreateCatalogTable` with `CatalogTableType.Managed`** creates a catalog-managed table through Unity Catalog's Delta API. It used the catalog's older table API, which Unity Catalog 0.7 refuses for managed tables. External tables are registered as before; partitioning, clustering, properties and identity columns are refused for them (create the Delta table at its location first).
* **V2 checkpoints** are written only when `delta.checkpointPolicy` is `v2`, as Spark writes them; a table that only has the `v2Checkpoint` feature gets classic checkpoints (they used to be V2).
* **Operations on tables the kernel cannot write are refused when they start**, as Spark checks a write: an optimize or restore with nothing to do used to return without an error on such a table.
* **`delta.dataSkippingStatsColumns` set to an empty value** (as Spark leaves it after the only named column is dropped) now means no column statistics, as in Spark; it used to fall back to the first 32 columns.
* **A commit file that is not UTF-8 is refused** (`DeltaException`: "A log line is not valid JSON"). Some invalid bytes inside strings used to be let through.
* `History` renames an operation parameter or metric that collides with a top-level column (a RESTORE's `version` becomes `operationParameters.version`).
* `ScanDelta` / `ReadDelta` / `UnityCatalog.ScanCatalogTable` (C# and F#) take two more optional parameters, `rowIdColumn` and `rowCommitVersionColumn`. Source-compatible, but code compiled against an earlier version must be recompiled.
* `DeltaTableFeatures.IcebergCompatV1` was removed (it never named a protocol feature); `TimestampWithoutTimezone` now maps to `timestampNtz`.
* Engine.Gpu will throw NotSupportedException.

### Ignored parameters (removed in 2.0)

* `SinkDelta` / `WriteDelta` / catalog sinks: `includeKeys` (partition columns are never written into data files), `statistics` (always written), `compatLevel`, `mkdir` (directories are always created).
* `ScanDelta` / `ReadDelta` / catalog scans: `glob`, `hivePartitioning`, `hivePartitionSchema`, `tryParseHiveDates` (partition values come from the log). `schema` now selects and casts the table's columns; an unknown column is an error.
* `approxBytesPerFile` and `delta.targetFileSize` (128 MB when unset; byte counts or sizes such as `64mb`) are measured as the engine estimates rows in memory: files on disk are smaller. A file holds at least one row group (122,880 rows unless `rowGroupSize` is set), so tables with rows wider than about 1 KB in memory get larger files (see the Delta limitations page).

---

## Streaming over .NET sources

Scans over .NET data (`ScanDatabase` / `ScanDb`, `ScanEnumerable` / `ScanSeq`, `ScanRecordBatches`) now work on the streaming engine. Polars's streaming engine cannot run these scans, so the source is replaced just before the query runs.

### New

* A scan over .NET data reads only the columns and the leading rows the query uses, on every engine (in memory, `Collect(Engine.Streaming)`, sinks): the engine's optimizer finds them (`Select`, `Head` / `Limit`, also through joins and `CollectAll`) and the source is asked for them. `ScanAvro` decodes only those columns and stops after those rows, `ScanExcel` stops parsing the sheet after those rows; the temporary file of a sink holds only those columns. Your own sources get the request through `ArrowStreamInterop.ScanStream(Func<StreamScanRequest, IEnumerable<RecordBatch>>, schema, strict)` (Polars.NET.Core); a source may ignore it.

### Fixed

* `SinkCsv`, `SinkIpc`, `SinkNdJson`, the partitioned sinks and `Collect(Engine.Streaming)` over a .NET source failed with `not yet implemented: AnonymousScan`.
* `SinkParquet` over a .NET source no longer prints a Rust panic to stderr and no longer falls back to an eager write that held the whole table in memory (#38).
* Sinks, `SinkBatches` / `SinkTo` and `CollectBatchesAsync(engine: Engine.Streaming)` write the source batch by batch to a temporary Arrow IPC file and stream from it, so memory stays bounded by a batch. On 40M rows, `SinkParquet` peaks at about 215 MB of anonymous memory, where it used to hold the whole table (about 1 GB). A source used twice in one query (a self-join, `CollectAll`) is read once, so a single-use `IDataReader` works too.
* `Collect(Engine.Streaming)` reads the source into memory first, then runs the rest of the query on the streaming engine.
* `SinkBatches` / `SinkTo` run on the streaming engine and hand batches over as they are produced. They used to collect the whole frame first: `Engine.Auto` is the in-memory engine in this Polars version.
* `SinkBatches` / `SinkTo` hand over logical Arrow types (`Decimal128`, dictionaries) instead of the engine's physical arrays. `decimal` columns used to fail with `Data type is not yet supported in import` on the streaming path.
* `CollectBatchesAsync` / `RowsAsync` (and LINQ async enumeration) no longer end silently when the engine fails mid-stream: the error is thrown.
* `CollectBatchesAsync` starts the query off the caller's thread.
* The engine releases a .NET source's stream when it stops reading, which disposes the source's enumerator, also when a query needs only the first rows. The stream's handle used to stay allocated after every read.
* A .NET source that fails while it is read fails the query with its error. Its stream used to end there instead: the query returned the rows read so far, and the error went only to the console.

### Behaviour changes

* The temporary file goes in Polars's temporary directory (`POLARS_TEMP_DIR`, by default under the system temp directory). It has no name in the directory and disappears when the query ends. Where `/tmp` is a RAM-backed tmpfs (Fedora, Arch), point `POLARS_TEMP_DIR` at a disk to keep large sinks out of RAM.
* `ScanDatabaseBuffered` and `ScanEnumerable(useBuffered: true)` / `ScanSeq(useBuffered = true)` buffer through the native engine. The file lives until the last frame that scans it is released, so there is nothing extra to dispose. `ScanDatabaseBuffered` now returns `LazyFrame`; `ScopedLazyFrame` was removed, and `LazyFrame` is sealed.
* `CollectBatchesAsync` on the default `Engine.Auto` still reads a .NET source into memory. Pass `Engine.Streaming` for bounded memory.
* The eager fallback of `SinkParquet` was removed: streaming sink errors on local paths are thrown, as on cloud paths.

---

## CSV and JSON schema inference

`inferSchemaLength` (CSV) and `inferSchemaLen` / `inferSchemaLength` (JSON, NDJSON) follow Python Polars's `infer_schema_length` (#36).

### Behaviour changes

* The parameter defaults to 100 rows instead of null. Calls that leave it out infer from 100 rows, as before.
* Passing `null` infers the types from the whole input. The input is read into memory for this, which is slow. A null used to mean 100 rows for CSV, although part of the documentation said it scanned the whole file. For JSON it was 100 rows in some readers and the whole input in others.
* Values above 16,777,216 also infer from the whole input. Polars preallocates the rows it samples: `ulong.MaxValue` used to fail with `capacity overflow` and print a Rust panic, and a value such as `int.MaxValue` could abort the process. In F#, where an omitted optional parameter cannot be told apart from `None`, pass `UInt64.MaxValue` for a whole-file inference.
* `0` reads every CSV column as String; for JSON it infers from the whole input. JSON readers used to disagree on `0`.

---

## In-memory scans without copies

### New

* `ScanParquet`, `ScanIpc`, `ScanCsv`, `ScanNdjson` and `ReadParquet`, `ReadIpc`, `ReadCsv` take a `ReadOnlyMemory<byte>`, which the engine reads in place without a copy (#35). The memory stays pinned until the last frame reading it is released, clones and collected frames included, and must not change meanwhile. Arrays, slices of arrays, and unmanaged or memory-mapped memory wrapped in a `MemoryManager<byte>` all work. Free unmanaged memory only once the engine unpins it (`MemoryManager<byte>.Unpin`): a query can still be finishing on a background thread right after its frames are released, for example after an early `break` out of `CollectBatchesAsync`. The F# `ScanParquet` / `ScanIpc` / `ScanCsv` / `ScanNdjson` take a `ReadOnlyMemory<byte>` too.
* The same methods, plus `ReadJson` and `ReadAvro`, take a `ReadOnlySpan<byte>`, which is copied once into the engine. Stack-allocated buffers and pointers work. `ReadJson` and `ReadAvro` also take a `ReadOnlyMemory<byte>`; they parse during the call and copy nothing.
* The `byte[]` overloads still copy, so changing the array after the call doesn't affect the frame.

### Fixed

* `ScanParquet(byte[])` passed `rechunk` as `cache` and `cache` as `rechunk`.
* `ScanParquet(byte[])` and `ScanNdjson(byte[])` refuse an empty buffer with `ArgumentException`, as the IPC scans did (CSV: see its section, empty input follows `raiseIfEmpty`).

---

## Schemas given as variables

### Fixed

* An optional schema parameter (`IntoSchema?`: `schema`, `dtypeOverride`, `schemaOverrides`, …) given a variable that is null (a `PolarsSchema`, `DataFrame`, `LazyFrame`, dictionary, array, list, Arrow schema or `Type` holding null) means no schema, as passing `null` itself does. It threw `ArgumentNullException`.

---

## Packaging

* `native_shim.dll` (`Polars.NET.Native.win-x64`) links the C runtime statically and no longer needs the Visual C++ Redistributable (`VCRUNTIME140.dll`), which a clean Windows install lacks. It used to fail on first use with "Dll was not found" (#37). The release build now checks the DLL's imports.
* `libnative_shim.so` in `Polars.NET.Native.linux-arm64` needs glibc 2.28, as the linux-x64 one does (it is now built in the manylinux 2.28 image on an arm64 runner). It used to need glibc 2.39, so it failed on Debian 12 and Raspberry Pi OS Bookworm, Ubuntu 22.04, RHEL 9 and Amazon Linux 2023 with "`GLIBC_2.38` not found" (#40). The release build now checks the glibc versions both Linux glibc libraries need, and the arm64 package is smoke-tested on Debian 12.
