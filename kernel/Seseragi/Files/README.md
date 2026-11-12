# Kaminari.Seseragi.Files — maintainer handoff

Managed file-format readers and writers that stream Apache Arrow record batches. Today it holds one format,
**Avro** (object container files, specification 1.12.2). The package is meant to hold more formats (the managed
Parquet pieces in Seseragi.Lakehouse are the first candidates), so shared pieces such as the compression codecs
live at the package root.

State (2026-11): Avro is complete for reading and writing; Polars.NET's `ReadAvro` / `ScanAvro` / `WriteAvro*` /
`SinkAvro` run on it and the engine's own Avro support was removed. This file is the starting point for anyone
adding features.

## Rules of the package

* Depends on `Kaminari.Sazanami` (Arrow infrastructure) plus pure managed codec packages (`ZstdSharp.Port`,
  `Snappier`, `System.IO.Hashing`). Never on Polars.NET or Seseragi.Lakehouse; sources never mention the engine.
* Off the managed heap where it matters: file blocks, decompression output and Arrow buffers are native memory.
  A full read allocates a few hundred KiB of managed memory regardless of file size.
* Values a format cannot hold exactly are refused with an exception, never changed silently.
* No reflection JSON (AOT): schemas are parsed with `JsonDocument` and written with `Utf8JsonWriter`.
* Generic native infrastructure goes into Sazanami, not here (see below).
* Modern .NET only (see `AGENTS.md`): switch expressions, collection expressions, `FrozenDictionary`, `SearchValues`.

## Code map

| Where | What |
| :--- | :--- |
| `CompressionType.cs` | Codec objects shared by formats: `NoCompression`, `DeflateCompression(Level)`, `SnappyCompression`, `ZstandardCompression(Level)`. Which format accepts which codec is a compile-time fact: marker interfaces (`IAvroCompression`, with an `internal` member so only this package implements it). Add `IParquetCompression` etc. when a format needs it. |
| `Avro/AvroSchema.cs` | Schema model (public, read-only: `AvroSchema` and subclasses, `AvroField`, `AvroLogicalKind`; internal constructors) and the parser. Named types and namespaces, recursion, logical types (an invalid or unknown one falls back to the base type, as the spec requires, and stays in `Properties`), Spark's `spark.sql.catalyst.type` intervals, our `kaminari.timezone` attribute. Every attribute a node or field does not interpret is kept in `Properties` (JSON) and written back; `FieldId` / `ElementId` / `KeyId` / `ValueId` read Iceberg's ids. Attributes on a named-type reference or a `{"type": {…}}` wrapper are dropped (no node of their own). |
| `Avro/AvroSchemaJsonWriter.cs` | Schema to JSON (named types written once, then by full name). |
| `Avro/AvroBinary.cs` | `AvroSpanReader` (bounds-checked, SWAR varint fast path) and `AvroBufferWriter` (native buffer, SWAR varint encode). `AvroFormatException` for malformed input. |
| `Avro/AvroCodecs.cs` | Block codecs (null, deflate, snappy with CRC32, zstandard); bzip2 / xz are refused by name. |
| `Avro/AvroReadSchema.cs` | Avro → Arrow type mapping, union shapes. |
| `Avro/AvroDecoders.cs` | Decoders compiled once per schema, appending to native builders; skippers for projected-away columns; fast paths for arrays of long / int / float / double. |
| `Avro/AvroReader.cs` | Container reader: header, block loop, batching, projection, row limit, opt-in parallel decoding. |
| `Avro/AvroWriteSchema.cs` | Arrow → Avro mapping and encoder compilation; `SameEncoding` (per-batch type check). |
| `Avro/AvroEncoders.cs` | Encoders over pinned Arrow buffers; `EncodeRange` loops for list items. |
| `Avro/AvroWriter.cs` | Container writer (blocks by size, header on first block, failure handling). |

In Sazanami (generic, reusable by other formats):

| Where | What |
| :--- | :--- |
| `Arrow/NativeGrowBuffer.cs` | Growable 64-byte-aligned native buffer and bitmap (capacity hint so each batch does not regrow from zero). |
| `Arrow/NativeColumnBuilders.cs` | Arrow column builders in native memory (fixed width, boolean, binary, fixed-size binary, dictionary, list/map, struct, null), lazy validity. |
| `Arrow/NativeArrayExport.cs` | Turns builders into a `RecordBatch` without copying: buffers are detached into an Arrow C data interface `ArrowArray` (`NativeCArray`, layout-identical because Apache.Arrow keeps `CArrowArray.release` internal) and imported with `CArrowArrayImporter`; the root release callback frees everything. |
| `Helpers/NativeByteBuffer.cs`, `NativeByteBufferStream.cs` | Native I/O scratch buffers (block bytes, decompression, writer output) and a write stream over them. |
| `Arrow/ArrowFieldIds.cs` | The field-id metadata key (`PARQUET:field_id`) and helpers to read or set an id on an Arrow field. |
| `Arrow/ArrowRows.cs`, `ArrowRow.cs`, `ArrowList.cs`, `ArrowRowNode.cs` | Row view over a `RecordBatch` / `StructArray`: `ArrowRows.Over(batch, fieldIdKey)`, `row.Get<T>(ordinal or name)`, `GetStruct`, `GetList<T>`, `GetMap<K, V>`, `GetBytes`; field ids from a metadata key; no allocation per row (tests in `Sazanami.Tests/ArrowRowsTests.cs`, over Avro batches in `AvroRowViewTests.cs`). |

In Polars.NET (the engine integration):

| Where | What |
| :--- | :--- |
| `Polars.NET.Core/Wrappers/Wrappers.Avro.cs` | `ReadAvro` (path / span / stream), `ScanAvro`, `WriteAvro*`, `AvroBatchSink` (for `SinkAvro`), option mapping (`parallel` → `MaxDegreeOfParallelism = -1`, empty record name → `topLevelRecord`, `PlAvroCompression` → codec objects). |
| `Polars.NET.Core/Arrow/PolarsArrowCompat.cs` | Rewrites Arrow types the engine cannot import: Decimal256 refused with the column name; month-day-nano interval → `struct{months, days, nanoseconds: Duration(ns)}`. |
| `api/Polars.CSharp/IO/Avro/*.cs`, `api/Polars.FSharp/IO/Avro.fs` | Public API (F# names the parallel switch `paralleling`; `parallel` is reserved in F#). |

## Data flow

**Read.** `AvroReader.Open` reads the header (magic, metadata map, sync marker), parses the schema, builds the
projection and compiles one `RowDecoder` over a root `NativeStructBuilder`. Each block is read whole into a native
buffer (`EnsureAsync`), its sync marker checked, decompressed, and decoded row by row; batches do not follow block
bounds (a batch can span blocks; decoding resumes mid-block). `ExportBatch` detaches the builders' buffers into a
batch, so nothing is copied after decoding. A block whose records leave bytes over is an error.

**Parallel read** (`MaxDegreeOfParallelism > 1`, default 1): the reading thread copies whole raw blocks into
per-chunk buffers, grouping consecutive blocks until the next would exceed `BatchSize`; each chunk is decoded by a
`Worker` (own builders, decoders, codec state, pooled in a `ConcurrentBag`) on the thread pool; results are returned
in order. Batches then hold *at most* `BatchSize` rows. A single-block file gains nothing.

**Write.** `AvroWriteSchema.Build` maps the Arrow schema and compiles encoders. `Write(batch)` checks the batch
against the schema (`SameEncoding`), binds the encoders (pinning buffers), encodes row by row into the block buffer
and flushes a block whenever it reaches `BlockSize`. The header is written with the first block, or on completion of
an empty file. A failed write marks the writer faulted, so disposing it does not complete a short file; the path
overloads delete the partial file (Avro has no footer: a file cut at a block boundary would look complete).

## Type mapping (and where each rule comes from)

Rules were set with the user after probing Python Polars 2.1, Apache Spark 4.2 (spark-avro) and DuckDB 2.0.
Priority on the write side: the reference dataframe writer's rule where it has one, else spark-avro, else refuse
when data could change.

Reading (Avro → Arrow):

* Unions as spark-avro: `["null", T]` → nullable `T`; `[int, long]` → Int64; `[float, double]` → Float64 (each ± null);
  anything else → struct of nullable `member0..n` (null branch → null struct). `["null"]` alone → Null type (Spark: empty struct).
* `enum` → Dictionary(Int32, Utf8) with the symbols; `fixed` → FixedSizeBinary; `map` → Map(Utf8 key);
  an `array` with `logicalType: map` over a record of exactly two fields (Iceberg's rule; key `T` or `["null", T]`
  as DuckDB writes it) → Map(key, value), a null key refused; any other `logicalType: map` array stays a list;
  `null` → Null; `uuid` → Utf8 (or the fixed(16) bytes).
* `decimal` → Decimal128 (≤ 38 digits) / Decimal256 (≤ 76), else raw bytes.
* `duration` → Interval(MonthDayNano) (Spark reads binary; DuckDB refuses; the engine adapter turns it into a struct).
* `timestamp-*` → Timestamp(unit, "UTC" or the `kaminari.timezone` attribute), but without a zone when Iceberg's
  `adjust-to-utc` is `false` (its wall-clock `timestamp`; only a JSON boolean counts, and it wins over
  `kaminari.timezone`); `local-timestamp-*` → Timestamp without zone. DuckDB writes a naive `TIMESTAMP` as plain
  `timestamp-micros` without the attribute, so it reads as UTC (the file does not say otherwise);
  `date`, `time-millis`, `time-micros` → Date32, Time32(ms), Time64(us); Spark intervals → Duration(us) / Interval(YearMonth).
* A non-record top-level schema → one column `value` (Spark and Polars refuse; DuckDB names it `avro_schema`).
* Ids: `field-id` → the field's `PARQUET:field_id` metadata (`ArrowFieldIds.MetadataKey` in Sazanami, the key pyarrow,
  pyiceberg and our Parquet writer use); `element-id` → the list item field; `key-id` / `value-id` → the map key and
  value fields. Only present ids are attached, so files without ids read exactly as before. Writing does the reverse.
* Recursive records are refused unless projected away (skippers handle recursion).
* `string` and map keys must be valid UTF-8.

Writing (Arrow → Avro): nullable field → `["null", T]`; narrow integers → `int`, UInt32/UInt64 → `long` (UInt64
above Int64.MaxValue refused); Float16 → `float`; Decimal32..256 → `bytes` decimal (user chose bytes over Spark's
fixed); Utf8/LargeUtf8/Utf8View → `string`; dictionaries → their values; FixedSizeBinary → `fixed`; lists of every
kind → `array`; Map with string keys → `map`, with other keys → an `array` of key/value records with
`logicalType: map` (the Iceberg and DuckDB encoding; required key, entry record named after the field); Struct → record named after the field in the parent's namespace
(Spark); Date64 → `date` (midnight only); Time → `time-millis` / `time-micros` (sub-µs refused); zoned Timestamp →
`timestamp-*` + `kaminari.timezone`; naive → `local-timestamp-*`; Duration → Spark day-time `long` (µs, sub-µs
refused); Interval → `duration` (negative or sub-ms refused) or Spark year-month `int`. Default record name
`topLevelRecord`; field names are written as given.

## Engine integration: traps already paid for

* Stream imports into the engine must be **strict** for foreign Arrow data. The non-strict path takes arrays as the
  engine's own physical layout (Utf8View, Time64[ns]): Utf8 became empty strings and Time32 garbage. Eager reads use
  `ArrowStreamInterop.ImportEagerStrict`; scans pass `strict: true` (the Rust `CSharpStreamScanner` flag).
* Batches handed to the engine are **not disposed** by us: exporting pins their imported native buffers and
  Apache.Arrow refuses to dispose pinned memory; the GC reclaims them after the engine releases. A cleaner future
  path is handing our `NativeCArray` to the engine directly (no Apache.Arrow round trip, deterministic release).
* `CArrowArrayImporter` calls `GC.AddMemoryPressure` per buffer (induced gen2 GCs; measured ~1.3% of a read). Kept:
  it makes the GC finalize undisposed batches. Only an upstream opt-out could remove it.
* Take a `NativeByteBuffer`'s `Span` only after any call that may grow it.
* `StructArray.Fields` are already sliced to the parent's offset (child index = parent index); the decimals derive
  from `FixedSizeBinaryType` (order type switches accordingly).

## Testing

* `tests/Seseragi.Files.Tests` (xUnit, ~100 tests): read fixtures, write round trips through our reader,
  hand-built containers for encodings reference writers do not produce (negative block counts, corrupt data),
  parallel = sequential, varint boundaries.
* Fixtures: `TestData/avro/generate_fixtures.py` writes them with fastavro (run with a venv that has fastavro,
  cramjam, zstandard; the C# tests hard-code the values it writes). `TestData/avro/apache/` holds the Apache Avro
  sample files (`README.md` there).
* Interop (opt-in): `SESERAGI_AVRO_PYTHON=<python with fastavro, cramjam, polars> dotnet test tests/Seseragi.Files.Tests --filter Category=Interop`
  writes files with our writer and checks them with fastavro and Python Polars (`interop_check.py`).
* Engine level: `tests/Polars.CSharp.Tests/IO/AvroManagedTests.cs` (links the fixtures), F# `AvroManagedTests.fs`.
* Benchmarks: `Polars.Benchmarks/AvroBenchmarks.cs` (`--filter '*AvroBenchmarks*'`). Reference (1M rows × 7 columns,
  16 cores): read 79 ms (27 ms parallel), write 63 ms uncompressed; Python Polars reads the same file in 150 ms,
  DuckDB in 90 ms on 8 threads.

## Extending

* **A new Arrow type on read:** map it in `AvroReadSchema.ToArrowType`, give `NativeColumnBuilder.Create` a builder
  if the layout is new, add a decoder case in `AvroDecoder.Compile`, a fixture row, and (if the engine cannot import
  it) a rewrite in `PolarsArrowCompat`.
* **A new type on write:** a case in `AvroWriteSchema.MapType`, an encoder (pin buffers in `Bind`; override
  `EncodeRange` if it can appear as list items), a `SameEncoding` case if parameters change the encoding, a round
  trip in `AvroWriteTests.Wide`, a lossy-value case in `LossyValues`.
* **A new codec:** a record in `CompressionType.cs` implementing the format's marker interface, an `AvroCodec`
  subclass, a name in `AvroCodec.ForName`.
* **A new format:** a folder beside `Avro/`, its own marker interface for codecs, reuse the Sazanami native builders
  and buffers.

## Iceberg roadmap (next)

The Avro module exists to back a managed Iceberg kernel in Seseragi.Lakehouse: Iceberg's metadata layer (manifest
lists `snap-*.avro` and manifest files) is Avro. Agreed plan (2026-11), in order; the Iceberg specifics are to be
verified against real files first:

1. **Reference files.** Generate v1 / v2 / v3 manifest lists and manifests with pyiceberg and with Spark's Iceberg
   runtime, and check each item below against them (Iceberg spec, Appendix A, Avro). *Generated (2026-11-13) with
   Iceberg 1.12.0 on Spark 4.1.1 (no Iceberg runtime runs on Spark 4.2 yet) and pyiceberg 0.12.0 (v1/v2 only;
   it cannot write v3). Findings so far: maps are key/value record arrays with `logicalType: map` and field ids on
   key/value (no `key-id` / `value-id`); `element-id` on lists; unions always `["null", T]`; decimal partitions are
   minimal `fixed`; codec deflate. Timestamp partitions: `timestamp-micros` with `adjust-to-utc` true (timestamptz) /
   false (timestamp); `days` → `int` + `date`, `hours` / `months` → plain `int`. uuid partitions: `fixed(16)` named
   `uuid_fixed` with `logicalType: uuid`, big-endian bytes; decimal(38, 10) → `fixed(16)`. An identity partition on a
   `fixed(5)` column (table made with the Iceberg 1.12 Java API, rows inserted from Spark 4.1) stored its value in
   Avro `bytes` encoding although the schema says `fixed(5)` (verified: decoding it as `bytes` parses every record).
   Two-record files then fail in every reader (fastavro, Java, ours); one-record files decode silently as garbage,
   since Avro has no checks. Cause (Iceberg core or that write path) not determined.*
2. **Public schema API.** *Done (2026-11-13) as a read-only model (`AvroSchema.Parse`, `AvroReader.WriterSchema`,
   `Properties`, id accessors, `ToJson`); a construction API waits for step 6. Parse → `ToJson` reproduces all 150
   reference schemas exactly (order-insensitive).* Attributes the parser used to drop (`field-id`, `element-id`,
   `key-id` / `value-id`, other custom properties) are kept; ids are exposed on read as Arrow field metadata and
   written back.
3. **Maps with non-string keys.** *Done (2026-11-13): read as Arrow maps (ids from the entry fields); written for
   non-string keys; all 1794 stats maps of the reference manifests match fastavro; DuckDB reads ours as `MAP`.*
   Iceberg writes `map<int, …>` (`column_sizes`, `value_counts`, `lower_bounds`, …) as an `array` of key/value records
   annotated `logicalType: map`. Read it as an Arrow Map; write Arrow Maps with non-string keys that way.
4. **`adjust-to-utc`.** *Read side done (2026-11-13): `false` reads zone-less, `AvroSchema.AdjustToUtc`; the
   attribute round-trips in `Properties`. The default Arrow mapping still writes zone-less timestamps as
   `local-timestamp-*` (the reference writers never emit the attribute); exact Iceberg schemas come from step 6.*
   Iceberg distinguishes `timestamp` and `timestamptz` by this attribute on `timestamp-micros` / `-nanos` (verified
   on Spark and pyiceberg partition tuples: `true` for timestamptz, `false` for timestamp).
5. **Reader-schema resolution.** Read with an expected schema: match fields by id (else name), fill missing fields
   with their default or null, skip extra fields (the skippers exist), promote int → long and float → double (and the
   rest of Avro's promotions), match union branches. Manifest schemas differ across format versions and partition
   specs.
6. **Write against a given Avro schema** (absorbs per-field encodings; includes the public construction API). Manifests
   need record names (`r2`, `k117_v118`, `manifest_entry`), required vs optional and `"default": null`, which the Arrow
   mapping cannot produce. It also covers per-field encodings: Iceberg needs decimals as `fixed` of the minimal size
   for the precision and uuids as `fixed(16)`; the default mapping keeps a `bytes` decimal. Its encoder must accept a
   zone-less Arrow timestamp for a `timestamp-*` with `"adjust-to-utc": false`.
7. **Small files.** Cache compiled decoders by (writer schema, reader schema); a synchronous whole-buffer fast path.
8. **Row view in Sazanami.** *Done (2026-11-13): `ArrowRows` in Sazanami's `Arrow/` (see the code map); it already
   reads all 59 generated v2 / v3 manifests and lists from the Avro reader (maps read as maps since step 3). Wide decimals read as `SqlDecimal` or unscaled bytes; a lossy `decimal` read is refused.* The kernel reads
   manifests as Arrow batches through a generic, allocation-free row cursor with typed getters by name or field id
   (scalars, structs, lists, `GetMap<K, V>`, nulls). This is preferred over a separate Avro typed-record reader: one
   decoding path. The same view can later replace the Delta kernel's JSON detour (checkpoints are read today as
   Parquet → engine → JSON lines via `IParquetReader.ReadAsJsonLines`).
9. **Interop both ways.** Read manifests written by Java Iceberg and pyiceberg; have Spark and pyiceberg read ours.

## Open items

* Not planned yet: single-object encoding, the Confluent / Kafka framing (could feed a Kafka source later), Avro
  aliases, Avro data files inside Iceberg tables (field-id projection plus `schema.name-mapping.default`).
* Hand `NativeCArray` batches to the engine directly instead of through Apache.Arrow (see traps).
* Remaining write cost is per-row virtual dispatch; a specialised loop for flat schemas would help. A file written
  as one huge block (as the engine's old writer did) cannot be decoded in parallel.
* Cloud paths: the plan is for Polars.NET.Core to open the object through Seseragi.Lakehouse's object stores and hand
  this package a `Stream`; the package itself stays storage-agnostic.
* Moving the managed Parquet pieces (`MinimalParquetWriter`, `ParquetFooters`) here, with Lakehouse referencing Files.
