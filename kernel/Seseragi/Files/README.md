# Kaminari.Seseragi.Files

Managed file-format readers and writers. It holds **Avro** (object container files, specification 1.12, streaming
Apache Arrow record batches), **Excel** (xlsx, xlsb, xls and OpenDocument workbooks read, xlsx written, streaming Arrow record batches) and
**Parquet** (files and their full footer metadata read and written) and **Arrow IPC** (the file format, Feather v2, and
the stream format, read and written). Pieces shared by formats, such as the compression codecs, live at the package root, each format in its own
folder.

Polars.NET's `ReadAvro`, `ScanAvro`, `WriteAvro*`, `SinkAvro`, and `ReadExcel`, `ScanExcel`, `WriteExcel` and
`SinkExcel` run on this package (xlsx, xlsb, xls, ods and flat ods).

## Design rules

* Depends on `Kaminari.Sazanami` (Arrow infrastructure) and pure managed codec packages (`ZstdSharp.Port`,
  `Snappier`; Zstandard and XxHash come through Sazanami), never on Polars.NET or Seseragi.Lakehouse; the sources never mention the engine.
* Off the managed heap where it matters: file blocks, decompression output and Arrow buffers are native memory.
* Values a format cannot hold exactly are refused with an exception, never changed silently.
* No reflection JSON (AOT): schemas are parsed with `JsonDocument` and written with `Utf8JsonWriter`.
* Generic native infrastructure (builders, buffers, the row view) belongs in Sazanami, not here.
* Modern .NET only (see `AGENTS.md`).

## Parquet

**Files** (`ParquetFile`): opened from a path, memory (`ReadOnlyMemory<byte>`, read in place), a stream (memory
streams, visible buffer or not, and unmanaged memory streams over a pointer are read in place; one that cannot seek is
copied into native memory first) or any `IRandomAccess` source (data read at offsets, such as an object in a store),
synchronously or asynchronously.
The footer is read with one ranged read for most files (two when it is longer than 64 KiB); a file of up to 64 KiB is
held whole by that read and never read again. The footer is checked: the schema
tree must account for every element and each row group must hold a chunk per leaf. Encrypted files (modular
encryption, `PARE` or a plaintext footer) are refused by name; damaged footers fail with `ParquetFormatException`.
One stream holds one file: data the footer does not describe before it (beyond the metadata copy some writers leave
there, or zero padding) is refused as files joined together, a source cheap to read at its start must begin with
`PAR1`, and column chunks reaching into the footer are refused when read.

**Datasets** (`ParquetDataset`, `ParquetDatasetOptions`, `ParquetDatasetReader`): many files read as one table, from
paths, directories (every file below them), globs (`*`, `?`, `[...]`, `**` as a whole segment) or a list of files
known beforehand (`ParquetDatasetFile`: size, rows to read, footer read before), expanded as Polars expands them (each
glob's or directory's files in path order, inputs in the order given, empty files left out, hidden and `_` files
read unless `IgnorePrefixes` says otherwise, a directory given alone holding one extension). The columns (nullable)
are the first file's (`DatasetSchemaMode.FirstFile`, the default), every file's united by name in first-appearance
order with the narrowest type holding each (`UnionByName`: every footer read when the dataset opens), or given
(`Schema`: no file read to open; string layouts follow the reader). Files' columns convert to the dataset's types:
as they are, by lossless widenings (wider integers, floats holding the integers, decimals of 32 to 256 bits into
decimals of no fewer integer digits and no smaller scale — united as wide as the widest, wider when the digits need
it —, integers into decimals, dates into timestamps, finer time units — values beyond the range fail —,
timestamps of another zone, struct fields matched by name, list and map elements, dictionaries to and from their
values and into wider indices, plain, large and view layouts of strings, binaries and lists), or with `AllowLossyCasts` by approximating ones (integers and decimals into floats,
narrower floats, coarser time units rounded down); wall-clock times and instants, strings and numbers never mix.
Columns and struct fields beyond the dataset's are not read; those a file lacks are an error naming the file and the
way out, or nulls (`AllowMissingColumns`; always when united). Polars Enums (at any depth) must have the same
categories in every file, and an Enum never meets a column that is none, unless the given `Schema` declares the column
as plain values. `UnifyDictionaries` reads every dictionary column (at any depth) with one dictionary for the dataset:
values numbered as first seen, indices of the type the given `Schema` declares (else Int32; more distinct values than
it reaches are an error), each batch carrying the dictionary as it then is (row groups' and files' dictionaries alike). Filters prune each file on its own types; columns it
lacks become nulls folded into the filter; conjuncts over lossily converted columns do not prune; where a file's own
filtering cannot be exact the dataset filters its rows after converting, so `FiltersExactly` holds across files.
Files are read in order with the reader options; with several threads the row groups of several files decode side
by side ahead of the consumer, on one scheduler and within one reader's in-flight memory (the row group waited for
always starts), files opened and planned ahead (`MaxFilesInFlight`; by default enough to keep the threads busy,
more for stores other than local files and memory, so that the files after one whose request is slow keep being
read), columns converted and filtered on those threads; requests to such stores are bounded apart
(`MaxRequestsInFlight`, those of the files read first let in first). With one thread, file after file on the
caller's from local files and memory, and from other stores fetched ahead as with several, decoded on one. The row index numbers rows across files and comes first, `MaxRows` counts
across files, `FilePathColumn` adds each row's file; `RowGroups` and `Rows` (per-file numbers) are refused. Errors
name their file and surface after the rows of the files before it. Rows come typed too
(`ParquetDataset.OpenReader<T>`, `ParquetDatasetReader<T>`, `ParquetDataset.ReadList<T>`): members map columns and
partitions by name, a `where` predicate is pushed down (conjuncts over partitions decide the files read), the rest
evaluated on the rows as the query compiler's client evaluation policy allows. Files come from an `IFileStore`
(`IO/`): the local file system by default (`LocalFileStore`), HTTP(S) URLs
(`HttpFileStore`, chosen for http(s) URLs when no store is given), Hugging Face Hub repositories (`HuggingFaceFileStore`,
chosen for `hf://datasets/…` and `hf://spaces/…`), object stores (`CloudFileStore.Open("s3://bucket")` of the lakehouse
library: S3, Azure Blob Storage, Google Cloud Storage through its managed clients, read asynchronously, files opened by
their ends and read at the version first seen; a path without a trailing '/' names a file), or a store of the caller.
Over HTTP a file is opened with one ranged request for its last bytes (its size and footer; explicit ranges where a
server has no suffix ranges), never a HEAD (presigned URLs are signed for GET only); later ranges require the version
first seen (`If-Match`, else `If-Unmodified-Since`), so a file changed while read fails; answers must not be compressed;
a server ignoring ranges gives small files whole (`HttpFileStoreOptions.WholeFileLimit`). A plain server cannot list
(files are named by URL); the Hub lists through its tree API, so globs and directories work there. Footers of large
files on such stores are kept between reads with their sizes and versions: a dataset read again opens them without a
request. A store's failure names the file and surfaces in order; a hung request ends with the caller's cancellation.
`Glob = false` reads wildcard characters as names (Polars' `glob=False`).

**Writing datasets** (`ParquetDatasetWriter`, `ParquetDatasetWriteOptions`, `ParquetWrittenFile`; `DatasetWriteOptions`,
`DatasetWriteResult`, shared with the IPC writer): record batches written as files under a path (a directory, or a
prefix of an `IWritableFileStore`), sync or async, or in one call (`Write`, `WriteAsync`). `PartitionBy` splits the
rows into Hive partitions: a directory `key=value/` per column, in order, values rendered as Polars renders them
(floats by their shortest digits, timestamps with their unit's digits and their zone's offset, decimals with their
scale's digits, times with their fraction), escaped as Hive and Spark escape them, a null `__HIVE_DEFAULT_PARTITION__`
and an empty string an empty value (two partitions); integers, floats, decimals, booleans, strings, dates, times,
timestamps and dictionaries of them. The partition columns stay out of the files unless `IncludePartitionColumns`.
Each batch's rows are grouped by their keys and gathered once (a large batch in slices that stay in the caches), and
the partitions' files are written side by side; a file written alone, not cut by size, encodes several row groups
side by side (holding references to the caller's batches, which may be disposed once written).
Files are cut by rows
(`MaxRowsPerFile`, exactly) and by size on disk (`ApproxBytesPerFile`: rows cut to fit, the size of the rows to come
estimated from those written), named `part-00000-<write id>.parquet` and so on (the index sorts the files of one write
in the order written; the write's id keeps them apart from any other write's; `FileName` names them otherwise) and
published whole (a file appears with its footer or not at all). Files under the path already fail the write
(`ExistingData.Error`, the default; a store that cannot list cannot be checked), are kept (`Append`) or replaced
(`ReplacePartitions`: those of the partitions written — or every file without partitions — deleted once every new
file is published; the other partitions stay). A failed write gives its open files up and deletes
the files it published, so the path is as it was; `DatasetWriteException` lists what it could not delete. Each file
takes the writer's options (`Writer`: compression, row groups, encodings, statistics); the dataset decides the threads
and owns the streams, batches stay the caller's. The files open at once hold their bytes within a memory budget
(`MemoryBudget`: an eighth of the memory the process may use by default): files being published are waited for, then
the largest files' row groups ended early, then the largest files closed early. The result lists each file with its
partition's values, rows, size and footer.

**Rows as objects, written as a dataset** (`ParquetDatasetWriter<T>.Create(path, options)`,
`ParquetDatasetWriter.WriteRows<T>` / `WriteRowsAsync`; IPC: `IpcDatasetWriter<T>`, `IpcDatasetWriter.WriteRows<T>`):
rows of `T` (`WriteRow`, `WriteRows`, `WriteRowsAsync` over sequences or async streams) laid out as `ParquetWriter<T>`
lays them out (dictionaries as MAP groups), gathered into native batches (an array's or a list's made in parallel) and
written as the record-batch writer writes them; `PartitionBy` names members (an enum's partition is its member name;
a `Guid`, bytes or collections cannot partition). The files depend on the rows alone. `PartitionSchema` gives the
partition columns' types for reading the dataset back as `T` (`ParquetDatasetReader<T>`). A row the layout refuses
gives the write up (what it published deleted); a null row is refused alone. Disposing an uncompleted writer gives the
write up.

**Partitions** (`HivePartitioning`, `PartitionSchema`, `ParseHiveDates`, `ParquetDatasetFile.PartitionValues`): Hive
`key=value` directories give partition columns as Polars reads them — by default for a directory given alone, on
request for globs and files (their whole path); values percent-decoded, `__HIVE_DEFAULT_PARTITION__` null, an empty
value null (an empty string for strings); the keys are the first path's and every path must hold them all. A key's
type is the partition schema's, else the files' column of that name, else inferred over all values with Polars' CSV
rules (booleans, floats, integers — beyond Int64 a decimal(38, 0) — dates and timestamps, strings; dates parsed by
default; times of day and mixed date orders stay strings: a partition schema reads times too). Values may also come with a list of files (typed by the
partition schema; strings read as Hive values). Partition columns follow the files' columns; a key the files hold
takes that column's place with the path's value. A filter's conjuncts over partition columns only decide which files
are read (the IR's semantics, evaluated on a table of one row per file); conjuncts over both partition and file
columns get each file's values as literals, folded (`FilterShapes.foldConstants`) before the file is read. Row
numbers count the rows of files left out.

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
batches of `BatchSize` rows in native memory, synchronously or asynchronously, in row group order. The unit of work is
a column of a row group (a nested column's leaves together), decoded from its first page to its last; units run side
by side on `MaxDegreeOfParallelism` threads (all cores by default; 1 reads on the calling thread), and up to that many
decoded row groups (fewer when large) are held ahead of the consumer. `ReadArrowStream`
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
values, a map keeping its key and value whatever the path; other leaves are not read), and row groups chosen (`RowGroups`).
Column chunks are read as the source suits: sliced when it lives in memory, page by page from a local file, and fetched
with few ranged reads (nearby chunks coalesced) from other sources; a local file can also be read through a memory
mapping (`MemoryMap`: automatic by default — Snappy-compressed and uncompressed fixed-width chunks —, on, or off; only
files opened by path are mapped, never a stream; a mapped file must not be truncated while read). Pages: data pages v1 and v2, dictionary pages; PLAIN, dictionary (RLE and the
deprecated PLAIN_DICTIONARY), RLE booleans, DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY and
BYTE_STREAM_SPLIT (on every physical type the spec allows them for); levels in the RLE / bit-packing hybrid or the deprecated BIT_PACKED;
codecs uncompressed, Snappy, GZIP (several members), Brotli, Zstandard, LZ4_RAW and the deprecated Hadoop-framed LZ4;
page CRCs checked on request (`VerifyChecksums`). `BinaryAsString` reads unannotated binaries as strings; INT96
timestamps read in microseconds exactly as Spark reads them (its wrapping 64-bit arithmetic, which brings back the
dates beyond 2262 Spark wrote wrapped; nanoseconds below a microsecond are dropped), or in nanoseconds (`Int96Unit`,
checked). Strings are validated as UTF-8; values that do not fit their Arrow type (narrowed integers, decimals over
their precision, timestamps beyond 64 bits) are an error, never truncated. ALP and LZO, and column chunks in other
files, are refused. Damaged files are refused with `ParquetFormatException`, never another exception; a damaged
stored Arrow schema is ignored (it only refines the mapping). The reader and its unsafe kernels are fuzzed with guard
pages after every buffer (`tests/Seseragi.Files.Fuzz`).

**Pushdown**: `Filter` takes a row filter in the query compiler's IR (Samidare `ScalarExpr`, its semantics: Kleene
logic, NaN above every number, strings in code point order) as a hint. Row groups whose statistics show that no row can
pass are not read; the statistics are used as their column order allows (with column orders only `min_value` /
`max_value`, without only the signed legacy bounds of fixed-width types), literals are compared in the stored
representation (converted by Samidare's `LiteralConversion`, shared with its filter lowering), and whatever cannot be
decided keeps the row group. Equality tests also ask the chunks' split block
Bloom filters (`BloomFilters`, on by default); column indexes prune pages, and the offset indexes of the chunks read
then skip the pages without a selected row unread on every way of reading chunks (fetches take only the dictionary
page and the pages needed). Indexes and filters that do not parse or do not describe the chunk are ignored; a page
read must agree with its offset index. `Rows` selects rows by their number in the file (Sazanami `RowRanges`, as a
deletion vector's complement); pages wholly between selected rows are passed over by their headers when there is no
offset index. `RowIndexColumn` puts each row's number first, `MaxRows` stops after that many rows, and
`ParquetFile.Open(source, metadata)` reopens a file with a footer read before. By default rows are not filtered: the
batches hold the rows of what is kept, for the caller's filter. With `FilterRows` the reader filters them itself (late
materialization): the filter is lowered by Samidare's `FilterLowering` into a Sazanami `FilterProgram` over the
columns as read, the columns it tests are decoded first (struct fields by path), the program is evaluated batch by
batch, and the selected columns are then read for the rows kept only: masked (the filter's mask goes into decoding,
so the columns' readers append only the rows kept; the filter's own columns are reused, filtered with `ArrowFilter`,
as are columns of booleans or nulls) or sparsely (by row ranges, skipping pages by the offset index): masked when
nearly every page read holds a kept row (by the offset indexes; without them, by how scattered the rows kept are).
Conjuncts that cannot be evaluated here are left to the caller (`FiltersExactly` is then false); `MaxRows` then
counts the rows delivered.

**Rows as objects**: `ParquetFile.OpenReader<T>(where, options)` gives a `ParquetReader<T>` that reads the rows as `T`
(records, classes, structs, F# records and tuples, mapped by member name ignoring case as `DataFrame.Rows<T>()` maps
them: Sazanami's `ArrowRowMapper<T>`), only the columns its members map, batch by batch (`ReadRows`,
`ReadRowsAsync`; batches turned into rows on up to `MaxDegreeOfParallelism` threads at once, in order, through
Sazanami's `OrderedParallel`); `ParquetFile.ReadList<T>(path, where)` / `ReadListAsync` read a file in one call. The `where`
predicate (`Expression<Func<T, bool>>`) is translated by Samidare as the LINQ provider translates it (captured values
evaluated first) and pushed into the read as `Filter` with `FilterRows`; the conjuncts it cannot translate, or that
the reader cannot evaluate, are evaluated on the rows as `T` when Samidare's client evaluation policy allows it
(Strict, the default, refuses them). Filters reach pruning and lowering in canonical shapes (`FilterShapes.canonical`:
a starts-with call as the operator, an IN over a collection as an IN over literals), whoever built them.

**Writing** (`ParquetWriter`, `ParquetWriteOptions`): Arrow record batches as a Parquet file, to a stream or a path
(`Create`, then `Write` / `WriteAsync` batch by batch and `Complete`; or the static `Write` / `WriteAsync` of a whole
sequence, which delete a partial file on failure). Disposing an incomplete writer completes the file unless writing
failed or the caller gave it up with `Abort` (a failure outside the writer: then nothing buffered is encoded or written,
and the stream keeps no footer). Flat columns of every Arrow type Parquet holds: booleans, integers
of every width and sign (annotated), half, single and double floats, dates (date64 in whole days), times, timestamps
(seconds stored in milliseconds, a zone as UTC-adjusted; `TimestampUnit` coerces exactly, `Int96Timestamps` writes
the legacy layout), durations, decimals of 32 to 256 bits (in INT32 up to 9 digits, INT64 up to 18, else the shortest
fixed-length bytes), strings and binary in every layout (large, view), fixed-size binary, dictionary columns (by their
values), intervals (as INTERVAL when exact) and the null type. Nested columns at any depth: structs as groups, lists
of every layout (list, large, fixed-size, list views) as three-level LIST groups (`list` / `element`), maps as MAP
groups (`key_value` / `key` / `value`, keys required), run-end encoded arrays and dictionaries of nested values by
the values they stand for (the stored Arrow schema keeps their layout); unions are refused (Parquet has no form for
them). Definition and repetition levels follow the Dremel rules (as pyarrow and Spark write them, null counts
included); pages break only between rows. Field ids are written from the Arrow fields' `PARQUET:field_id` (struct
children, list elements, map keys and values; never the `list` / `key_value` wrappers). Values Parquet
cannot hold exactly (a date64 that is not a whole day, a decimal with more digits than its precision, an inexact
interval or timestamp coercion) and nulls where the schema says non-nullable, at any depth, are refused before
anything is written, never changed. Each row group is
buffered until `RowGroupRows` or `RowGroupBytes` (checked after each segment of a batch the columns take at once, at
most 16 384 rows; or `EndRowGroup`), then written column chunk by column chunk.
With `MaxDegreeOfParallelism` above 1 (the processor count by default) the columns of a segment are encoded on several
threads, pages are compressed by helper threads (a few in flight a column, the total of busy threads kept to the
degree), a full row group is sealed and finished (last pages, dictionary pages, indexes) by a task while the next one
fills another set of column writers, and goes to the stream while the next one is encoded; the file is the same, byte
for byte, whatever the degree. `BatchOwnership` is a choice of speed or memory: with `Writer` the writer takes the batches (reading them after `Write` returns,
disposing each once its rows are encoded, also on failure) and, on several threads, encodes several row groups at once
straight from them (each by column writers of its own, encoding its rows as they come; the rows waiting for encoders
and the row groups at once bounded by their bytes, two row groups at least), writing them in order; row groups are then closed on rows or on the rows' logical Arrow bytes (what a compact copy of them
takes, however their buffers are shared) on any number of threads, so that the file is again the same whatever the
degree, and the one the caller's batches give whenever that bytes rule does not decide. An explicit `EndRowGroup` returns once its row group is in the stream. Pages
close at `DataPageRows` or `DataPageBytes`, in format 1 or 2 (`DataPageVersion`), compressed by any codec of
`IParquetCompression` (none, Snappy, Zstandard, Gzip, Brotli, LZ4_RAW) and checksummed (`WriteChecksums`). Columns are
dictionary-encoded (PLAIN dictionary page, RLE_DICTIONARY pages) unless the first page holding values shows the dictionary does not
pay (smaller than the plain values, judged as it is mapped; for fixed-width values compressed afterwards, at most three quarters of them, as
bit-packed indices hardly compress where plain values do); one outgrowing `DictionaryPageBytes` goes on plainly. Statistics follow the type-defined order of every column
(signed, unsigned, IEEE floats with NaN counts and zeros as -0 / +0, decimals as numbers), with the deprecated fields
for signed orders; string and binary bounds are cut to `StatisticsTruncateLength` (the upper bound rounded up, marked
inexact); floating chunks holding NaN get no bounds (Spark orders NaN above every number and ignores NaN counts).
Chunks carry a page index (`PageIndex`; the offset index always, the column index with statistics, except for chunks
holding a NaN or of a type without order) and size statistics (`SizeStatistics`: the value bytes of strings and
binary, level histograms where they say more than the null count). `Columns` sets columns apart by their dotted Parquet
path (`s.x`, `l.list.element`, `m.key_value.key`; a struct, list or map path applies to every leaf under it, the
longest path winning): the encoding of pages written without the dictionary (`ParquetValueEncoding`:
DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY, BYTE_STREAM_SPLIT, RLE booleans), the dictionary on or
off, and a Bloom filter (split blocks of XXH64 hashes, sized for each chunk's distinct values, at most `DistinctValues`,
cut to `MaxBytes`); settings a leaf's type cannot take are refused. Bloom filters, then column and offset indexes, follow
the row groups. The footer stores the Arrow schema (`ARROW:schema`, `StoreArrowSchema`) and extra key-value metadata; `Metadata` is the
footer written, `BytesWritten` and `PendingBytes` what is in the stream and what the row groups not there yet will take
(sealed row groups exactly, waiting for them; with the writer owning batches on several threads, those still being encoded too: exactly once encoded, else
estimated from the compression of those laid out), so that their sum can drive splitting files by size. View columns
(`string_view`, `binary_view`, as Polars exports strings) are copied from their views and data buffers in runs, like
plain and large strings.

**Rows as objects, written**: `ParquetWriter<T>.Create(path | stream, options)` writes rows of `T` (`WriteRow`,
`WriteRows` / `WriteRowsAsync` of a sequence or an async sequence, `EndRowGroup`, `Complete`, `Abort`);
`ParquetWriter.WriteRows<T>(path | stream, rows)` / `WriteRowsAsync` write a whole sequence (a partial file deleted on
failure). One column per public readable property, laid out as `DataFrame.From<T>` lays them out (Sazanami's
`ArrowRowBuilder<T>`: strings as string views, collections as large lists, nested records as structs, enums as
dictionaries of their member names carrying the engine's Enum metadata, decimals as decimal(38, 18), `Guid` as 16 bytes,
`Int128` / `UInt128` as decimal(38, 0) marked as 128-bit integers (exact everywhere; values of more than 38 digits are
refused — for bit patterns such as hashes use `Guid` or `byte[]`; Apache.Arrow has no 128-bit integer type yet, and once
it has one this layout will change to it),
value-type columns non-nullable, list items and members of nested records nullable); dictionaries (`Dictionary`, `IDictionary`, `IReadOnlyDictionary`, F# `Map`, at any depth) are
written as Parquet MAP groups, and stored as Arrow maps in `ARROW:schema` (Spark, DuckDB, pyarrow and Python Polars read
maps); any other `T` is one column named `value`. Rows are gathered into batches of 16 384 in native
memory, each written and freed. Speed or memory is the options' `BatchOwnership`: `Caller` (the default) encodes
each batch before the next, the least memory; `Writer` hands the batches over and encodes several row groups side by
side, up to twice as fast where the columns are few or cheap, for some hundreds of MiB more. Either way the file
depends on the rows alone, not on how they were passed nor on the threads. The batches of an array or a list are made on up to `MaxDegreeOfParallelism` threads at once and handed
over in order (rows passed one by one or by a lazy sequence are read as they come, on the caller's thread: a producer
may reuse one object for every row). A null
row is refused (the writer goes on); a value the layout refuses (a decimal that does not fit exactly, an undefined enum
value) aborts the file. `ParquetReader<T>` reads the rows back. UUID columns (FIXED_LEN_BYTE_ARRAY(16) annotated UUID,
as DuckDB and pyarrow write them) are read as the `Guid` those engines show; maps (and lists of key / value structs) are
read into dictionary members (RFC 4122 byte order); an Arrow
`arrow.uuid` field is written annotated UUID.

Everything the writer holds per page and per chunk lives in native memory (`NativeMemoryPool`, through
`NativeBufferWriter` and `NativeArray<T>`): page values, levels and dictionary indices, shredded levels, string arenas,
dictionaries and their hash tables, Bloom hashes and bitsets, page bytes, compressed bodies and dictionary pages. The
column writers and shredders are freed when the writer is disposed; only metadata (headers, statistics, indexes) is
managed. For many writers open at once (one per partition of a sink), internal hooks let them share one budget of
threads (and then not seal row groups, which would double what each holds), tell what they hold without waiting for
pages being compressed (pending pages at their largest, plus the scratch their column writers keep), and give that
scratch back to the pool between row groups (grown again as rows come; the file is the same).

Delta checkpoints are written by `ParquetWriter` from Arrow batches the lakehouse builds straight into native column
builders; their schema, described by `ParquetNode`s, becomes the Arrow schema through `ParquetNodeArrow`, whose types
write back to the same Parquet types. `MinimalParquetWriter`, the small writer they used before, stays internal for
tests that need such files.

Layout of `Parquet/`: `Thrift/` (compact protocol), `Metadata/` (the `parquet.thrift` structs), `Schema/` (schema tree,
leaf types, Arrow mapping with each node's level rules, projection by path), `Codecs/` (page codecs; Snappy blocks are
decoded by Sazanami's own decoder), `Encodings/` (RLE / bit-packing hybrid, BIT_PACKED levels, DELTA_BINARY_PACKED,
the delta byte arrays, and their encoders; LSB bit packing and unpacking and byte stream split are Sazanami's), `Reading/` (pages, leaf readers by
value kind, value converters, the nested assembler), `Writing/` (Arrow → Parquet schema mapping with value sources,
column writers by physical type with their dictionaries and statistics, page assembly and page index entries,
page compression jobs and the thread budget, column settings, Bloom filter builders), `Pruning/` (the IR filter compiled to outcome sets over statistics,
literals in the stored representation, page pruning), `PageIndexes.cs` and `BloomFilters.cs` (per-file caches of
checked page indexes and Bloom filters); `ParquetFile`, `ParquetReader`, `ParquetWriter` and the options at the top.
Pages in the non-PLAIN value encodings are decoded whole when entered, into PLAIN layout, so every value kind reads
them as PLAIN; string readers take delta byte arrays as data, starts and lengths instead.

`IO/` holds what the formats share for reading at offsets: `IRandomAccess`, the stream-backed source (in-memory
streams sliced without a lock), the native copy of streams that cannot seek, and the file stores datasets list and
open (`IFileStore`, `StoredFile`, `LocalFileStore`, `HttpFileStore` over Sazanami's `HttpSender`,
`HuggingFaceFileStore`); and for writing, the stores files are written to (`IWritableFileStore`: local files here,
cloud file systems through the lakehouse's `FileSystemStore`; an internal memory store for tests) and `FileWriteStream`, a file that appears whole
at its commit or not at all (locally a hidden temporary file renamed into place; create-only commits atomic against
other writers). `Datasets/` holds what datasets of every format share: their options and files
(`DatasetOptions`, `DatasetFile`, `HivePartitioning`, `DatasetSchemaMode`), path expansion, stores, partitions and
schemas united by name (`DatasetFiles`), partition values (`Partitions`: Hive paths or given values as a typed table of
one row per file), the columns, filter split and batch conversion around each file's reader (`DatasetShape`), footers
kept between reads (`KeptFooters`) and how far readers read ahead (`DatasetLimits`); for writing, the options and
results (`DatasetWriteOptions`), the sink every format's dataset writer writes through (`DatasetSink`: rows split by their keys' values, files cut, written side by side within a memory budget and
published while the next rows are written), the format seam (`DatasetFileFormat`), the write around it
(`DatasetWriting`: partitions, existing files, names, undoing a failed write), typed rows written through it
(`DatasetRows`: `RowBatcher` batches handed to the write, a refused row giving it up) and Hive partition paths (`HivePaths`:
values rendered and escaped, shared with the Delta writer). `Parquet/Datasets/` holds the
Parquet dataset (`ParquetDataset`; `ParquetDatasetReader`: shape plans per file schema, filters per file, the window
of row groups across files; `ParquetDatasetReader<T>`: typed rows); `Ipc/Datasets/` the Arrow IPC one (`IpcDataset`;
`IpcDatasetReader`: the window of record batches across files; `IpcDatasetReader<T>`; `IpcDatasetTypes`: layouts
united by the first file's).

## Arrow IPC

**Files** (`IpcFile`): the IPC file format (`.arrow`, Feather v2), opened from a path, memory, a stream (one that
cannot seek is copied into native memory first) or an `IRandomAccess` source, sync or async. The footer is read and
checked on open (every block inside the data); `Schema` (with the schema's custom metadata), `FileMetadata` (the
footer's), `BatchCount`, `ReadBatch(index)`, `GetBatchMetadata(index)` (a batch message's custom metadata). Only the
footer locates messages: the stream embedded after the magic is never parsed (Polars writes its schema message there
without a prefix). Data in the stream format (`.arrows`, the `.arrow` files of Hugging Face datasets) opens too
(`IsStream`): its schema message is read on open, its other messages scanned as far as reading needs them (their
prefixes and metadata, bodies passed over by their length: in place in memory, through the mapping of a file opened by
path — the mapping its reader then shares; a dataset reader with `MemoryMap.Off` does not map —, else 64 KiB reads
locally, 1 MiB on a store; open a `Stream` for no mapping at all), the whole stream when `BatchCount` is asked, giving
the blocks a footer would; its batches are read at offsets as a file's, each with the dictionaries the stream sent
before it. A read that stops early leaves the rest unscanned. A missing end-of-stream marker ends it; a damaged
message, or data ending inside one, is an error once reading reaches it (the batches before it read). Feather v1 files and other data are refused by name.

**Reading** (`IpcFile.OpenReader`, `IpcReader`, `IpcReadOptions`): `ReadBatch(index)`, `ReadBatches` /
`ReadBatchesAsync`, `ReadArrowStream`. `Columns` selects top-level columns (the buffers of the others are never read,
decompressed or checked; byte gaps up to 1 MiB between the needed buffers are read rather than skipped); `MaxRows`
cuts the rows; `MemoryMap` (`IpcMemoryMap`: `Auto` by default, as `On` today) reads a local file opened by path
through a mapping: messages are parsed in it, uncompressed batches point into it, compressed ones are decompressed out
of it; the mapping lives while a batch does. Dictionaries are read on the first batch, in footer order (deltas appended; a second non-delta dictionary of an
id is refused, as the format says), only those the selected columns reach (dictionary values may themselves be
dictionary-encoded); a stream's in order as far as each batch needs (replacements and deltas), the dictionaries of the
batches delivered let go as a read goes on (read again from the stream's start for a batch read at random before them). `ValidateUtf8` checks strings (off by default, as pyarrow and Polars read).
`MaxDegreeOfParallelism` (all cores by default): batches read and decoded side by side ahead of the consumer, in order
(bounded by their body sizes; a consumer that stops early frees what was decoded ahead), and the compressed buffers of
a large batch decompressed side by side.

**Row filters** (`Filter`, `FilterRows`): a filter in Samidare's IR (as Parquet's), evaluated within each batch (IPC
data has no statistics to skip batches by, so without `FilterRows` it does nothing): the columns it reads are decoded
first (read even when not selected), the other selected columns only for batches where it keeps a row, then filtered
(Sazanami's `ArrowFilter`); batches keeping no row are passed over, `MaxRows` counts the rows delivered.
`FiltersExactly` tells whether every conjunct was evaluated. List views, unions and run-end encoded columns cannot be
selected with `FilterRows` (refused on open).

**Row index** (`IpcReadOptions.RowIndexColumn`): an Int64 column put first, numbering every row in the order of the data
before any filter, in both readers (a single batch read alone numbered after the batches before it).

**Datasets** (`IpcDataset`, `IpcDatasetOptions`, `IpcDatasetReader`, `IpcDatasetFile`): many IPC files read as one
table, as Parquet datasets are (the same `DatasetOptions`: paths, directories, globs, stores, Hive partitions pruned
by the filter, schema modes, missing columns, lossy casts, Enums, unified dictionaries, file path column, files and
requests in flight). Strings, binaries and lists in several layouts (plain, large, view) unite to the first file's;
dictionaries unite to dictionaries with indices holding both (a dictionary beside plain values to plain values);
columns sharing a name are refused. Batches of several files are read and decoded side by side ahead of the consumer within a byte budget, files
opened and their dictionaries read ahead, requests to remote stores bounded apart; on one thread from local files, file
after file. Rows are numbered across the files (files left out by their partitions counted, their batches' messages
read only then); on a store a file opens with one read of its tail, a large file's footer kept between reads. Files in
the stream format join any dataset, beside files (told apart by their content, not their extension): scanned in the
background once opened, a chunk at a time, their batches joining the read as they are found (a read that stops early
stops their scans), their whole scans kept on stores between reads (with their versions), counted from their scans for
the row index. Typed
rows: `IpcDataset.OpenReader<T>(where)`, `ReadList<T>` / `ReadListAsync`. Written by `IpcDatasetWriter`
(`IpcDatasetWriteOptions`) as Parquet datasets are (see Parquet's "Writing datasets"): files in the file format
(`.arrow`) or the stream format (`.arrows`); runs of 64 Ki rows or more are written as record batches as they are,
shorter ones (a partition's rows of each batch, small batches) gathered into record batches of 64 Ki rows or 8 MiB.
Rows of `T` are written by `IpcDatasetWriter<T>` as by `ParquetDatasetWriter<T>` (dictionaries as lists of key / value
structs).

**Typed rows** (`IpcReader<T>`, `IpcFile.OpenReader<T>(where)`, `IpcFile.ReadList<T>` / `ReadListAsync`;
`IpcStreamReader<T>.Open(stream, where)`: a stream's rows, read once, its columns chosen once its schema is read;
`IpcWriter<T>`, `IpcWriter.WriteRows<T>` / `WriteRowsAsync`): as Parquet's typed API: members mapped by name over
Sazanami's `ArrowRowMapper<T>` / `ArrowRowBuilder<T>`, the `where` predicate translated by Samidare and evaluated by
the reader (the rest on the rows, as the client evaluation policy allows), batches turned into rows in parallel; rows
written in record batches of 16 Ki rows (an array's or a list's batches made in parallel), the output depending on the
rows alone; a failed write to a path deletes the file.

**Streams** (`IpcStreamReader`): the stream format from any `Stream`, forward only, sync or async (`ReadNext`,
`ReadBatches`, `ReadBatchesAsync`, `ReadArrowStream`); dictionary replacements and deltas apply from the next batch;
the end-of-stream marker or the end of the data ends it; the buffers of unselected columns are skipped (seeked past, or
read through a small scratch); `SchemaMessageMetadata` and `BatchMetadata`. Above one thread the stream is read ahead
by a task of the reader's own (messages in order; dictionary batches applied there, each record batch decoded on the
thread pool against a snapshot of the dictionaries): a batch is delivered as soon as it is decoded, never waiting for
later messages, so the stream's position runs ahead of the batches delivered.

**What is read**: metadata V4 (unions with their old validity buffer, refused when it holds nulls) and V5; the
pre-0.15 framing (no continuation marker); every type of the columnar format 1.5: null, booleans, integers, floats
(half to double), decimals of 32 to 256 bits, dates, times, timestamps (with zones), durations, the three interval
kinds, binary and strings plain, large and as views (any number of variadic buffers), fixed-size binary, list, large
list, list view, large list view, fixed-size list, struct, map, sparse and dense unions, run-end encoding, dictionaries
of every index type (a dictionary a stream sends after an all-null batch reads as empty), extension types (as their
storage, the extension kept in the field metadata). Polars' Int128 / UInt128 (an Arrow `Int` of 128 bits) read as
`decimal128(38, 0)` / `decimal256(39, 0)`. Bodies compressed with LZ4 frames (linked or independent blocks, checksums
verified) or Zstandard, buffers stored raw (`-1`) or empty without a length. Refused by name: big-endian data, metadata
before V4, tensor messages, buffers over 2 GiB, types Apache.Arrow cannot hold (a struct without fields, fixed-size
binary of width 0). Apache.Arrow names map fields canonically (`entries`, `key`, `value`).

**Writing** (`IpcWriter`, `IpcWriteOptions`): record batches as the file format (default) or the stream format
(`IpcFormat`), to a stream or a path, sync or async (`Write`, `WriteAsync`, a batch's message metadata as an extra
argument, `Complete`, `Abort`, `BytesWritten`; `IpcWriter.Write(path | stream, schema, batches)` / `WriteAsync` in one
call; disposing completes unless aborted or failed, `LeaveOpen`). Metadata V5, little-endian; schema, field, message and
footer (`FileMetadata`) metadata; extension types written as their storage with the `ARROW:extension:*` keys. Buffers
64-byte aligned (bodies too, from where writing began); compression `NoCompression` (default), `Lz4FrameCompression`
(independent 64 KiB blocks, no checksums, as Arrow C++ writes) or `ZstandardCompression(level)`, each buffer on its own;
the work runs on up to `MaxDegreeOfParallelism` threads in units (an LZ4 buffer's 64 KiB blocks, a Zstandard buffer's
frames of 32 MiB: a larger buffer is several frames one after the other, which every reader decompresses whole) with
the same bytes for any degree; a buffer that does not shrink is stored as it is (`-1`). With compression a batch is
compressed before `Write` returns and written to the stream while the next one is compressed (a failure of the stream
then surfaces from the next call). With `BatchOwnership = Writer` (the shared `BatchOwnership` enum, as Parquet's
writer takes it) the writer takes every batch: `Write` returns once the batch is checked and its dictionaries settled,
up to `MaxDegreeOfParallelism` batches (512 MiB of their buffers) are compressed side by side and written in order,
each disposed once written (also on failure, abort or dispose); the bytes are the same. `IpcWriter<T>` always hands
its batches over this way. Arrays are written from where they are: a slice as its own rows (offsets rebased, bitmaps shifted, run ends
cut; view data buffers and list-view children whole). Dictionaries: each written with the first batch that uses it
(after the dictionaries its values use), then compared with the last one written: the same is not written again, one
whose first values are the last one's is written as a delta, another is a replacement in the stream format and refused
in the file format (which allows none). A batch that does not match the schema is refused before anything is written.

**Validation**: every array is checked before it is handed out, in O(n), vectorized where the data allows: buffers
inside the body and long enough for their lengths, null counts against validity bitmaps, offsets non-decreasing and
within their data, views (length, buffer index, offset, inline prefix), list views, union type ids and dense offsets,
run ends increasing, dictionary indices within their dictionary, map keys not null, exactly the nodes, buffers and
variadic counts the schema takes, decompressed sizes as declared (and declared sizes within what the codec can expand
to, so a forged length allocates nothing). Values themselves (decimal precision, date64 whole days) are not checked.
Damaged or invalid data fails with `IpcFormatException`.

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
| `Ipc/IpcFile.cs`, `IpcReader.cs`, `IpcStreamReader.cs`, `IpcReadOptions.cs`, `IpcFormatException.cs` | Public Arrow IPC API: the file (footer, blocks, format detection), its reader (dictionaries, a stream's by batch, projection, mapping), the stream reader (framing, skipping unselected buffers), options, the exception. |
| `Ipc/IpcStreamIndex.cs` | The scan of data in the stream format read at offsets: blocks, rows and dictionary batches before each record batch, kept as bytes by datasets. |
| `Ipc/IpcWriter.cs`, `IpcWriteOptions.cs` | Public IPC writer and its options (`IpcFormat`); messages written with their bodies' padding, the footer's blocks. The codec's writing half is Sazanami's `Arrow/Ipc`: `FlatBufferWriter` (front to back), `IpcMetadataWriter` (schema, messages, footer), `IpcBatchEncoder` (arrays to nodes and buffers, compression, layout), `IpcMessageEncoder` (batches to messages, dictionaries as written, framing), `IpcSchemaMessage` (Parquet's `ARROW:schema`). |
| `Datasets/` | What datasets of every format share (see the structure above): `DatasetOptions`, `DatasetFiles`, `Partitions`, `DatasetShape`, `KeptFooters`, `DatasetLimits`. |
| `Ipc/Datasets/` | The Arrow IPC dataset: `IpcDataset` (opening, schema modes, footers kept), `IpcDatasetReader` (sequential reading, the window of record batches across files, the row index across files), `IpcDatasetReader<T>`, `IpcDatasetTypes`, `IpcDatasetOptions` / `IpcDatasetFile`. |
| `Ipc/IpcDecoding.cs` | What a reader decodes from each batch: the selection, the row filter lowered by Samidare (its columns decoded first, the others when it keeps a row, then filtered), the dictionaries reached; shared by both readers and immutable (batches decode on several threads). |
| `Ipc/IpcReaderOfT.cs`, `IpcStreamReaderOfT.cs`, `IpcWriterOfT.cs` | `IpcReader<T>`, `IpcStreamReader<T>` and `IpcWriter<T>`: typed rows over the shared `TypedRows` and `RowBatcher`. |
| `Ipc/IpcIO.cs` | Messages and bodies read at offsets (the needed ranges into native memory, or the mapping), decoding batches and dictionaries, errors as `IpcFormatException`. The codec itself is Sazanami's `Arrow/Ipc`: FlatBuffer metadata (`IpcMetadata`, `FlatBufferTable`), framing and ranges (`IpcFraming`), bodies (`IpcBody`: compressed buffers of a large batch decompressed side by side before the walk), the decoder (`IpcBatchDecoder`), checks (`IpcValidation`), dictionaries (`IpcDictionaries`, with snapshots for batches decoded ahead), arrays as native slices (`IpcArray`); LZ4 frames and Zstandard in `Compression`. |
| `Parquet/ParquetMetadata.cs` | Parquet footer metadata (`ParquetMetadata`: schema elements, row groups, column statistics) parsed from the Thrift compact encoding: `Parse` (a footer), `FromFile` (a whole file in memory), `FooterRange` (where the footer lies, from a file's last bytes; reading them is the caller's, e.g. Lakehouse's `ParquetFooters` over its file systems). |
| `Parquet/ParquetReaderOfT.cs`, `Parquet/TypedRows.cs`, `TypedRows.cs` | `ParquetReader<T>`: the rows as objects over Sazanami's `ArrowRowMapper<T>`; `TypedRows` (shared with `ParquetDatasetReader<T>` and `IpcReader<T>`; `ParquetTypedRows` maps Parquet's options onto it): the `where` predicate translated by Samidare and pushed into the read, the rest evaluated on the rows by the client policy; batches turned into rows in parallel (`OrderedParallel`), `ReadList` copying the batches' rows once into an exact-size list. |
| `Parquet/ReadBudget.cs` | The threads (one scheduler), scratch buffers and in-flight bytes several readers share when a dataset drives them unit by unit. |
| `BatchOwnership.cs` | Who owns the batches given to a writer (Parquet's and Arrow IPC's): the caller, or the writer, which then encodes several side by side and disposes them. |
| `Parquet/ParquetWriterOfT.cs`, `RowBatcher.cs` | `ParquetWriter<T>` over `RowBatcher<T>` (shared with `IpcWriter<T>`): rows gathered into native batches by Sazanami's `ArrowRowBuilder<T>` (an array's or a list's whole batches made in parallel by a pool of builders, up to its first null row, handed in order to the writer, which takes them with `BatchOwnership.Writer`); a refused value aborts the output. |
| `Parquet/ParquetWriter.cs`, `ParquetWriteOptions.cs` | The writer: batches checked (types, lengths, nulls of required columns) before anything is written; segments of a batch written column by column in parallel (`MaxDegreeOfParallelism`, the first column's error reported), row groups closed on rows or (per segment) bytes, finished in parallel and written chunk by chunk while the next group is encoded (with several threads sealed: finished by a task on a second set of column writers); with the writer owning the batches, row groups encoded side by side (sets of column writers, one column scheduler, reference-counted batches, laid out and written in order); Bloom filters, column indexes, offset indexes and the footer (schema, row groups, column orders, `ARROW:schema`) at completion; failure handling as `AvroWriter`'s. |
| `Parquet/ParquetColumnOptions.cs` | Settings by column (`ParquetWriteOptions.Columns`): the encoding of pages written without the dictionary (`ParquetValueEncoding`), the dictionary on or off, Bloom filters (`ParquetBloomFilterOptions`: false positive rate, a cap on the distinct values sized for, the largest size). |
| `Parquet/Writing/WriteSchema.cs` | The Arrow schema as a Parquet schema tree (groups, LIST and MAP nodes, field ids) with each leaf's levels and shredding steps; Arrow fields to Parquet leaves: physical type, annotations, the value source of each Arrow type (conversions checked only on valid rows; dictionary columns through their values), statistics order. |
| `Parquet/Writing/Shredding.cs` | The Dremel shredder: the steps from a top-level array to a leaf (struct, list in every layout, map, remapping layouts — run-end encoding, dictionaries of nested values — and the leaf), walked row by row into definition and repetition levels and the positions of the values present (each step's array built once per call; the validating walk run only when a non-nullable node's array holds nulls); children from the parents' raw data (offsets of parents applied, nulls of parents hiding their children); the same walk validates nullability. |
| `Parquet/Writing/ColumnSettings.cs` | Which `Columns` entry applies to each leaf (dotted Parquet paths, an ancestor's applying to the leaves below, the longest winning) and what each physical type takes (encodings, Bloom filters); unknown paths and impossible settings refused. |
| `Parquet/Writing/PageJobs.cs` | Encoded pages waiting for compression (compressed and checksummed by a helper thread or by their column writer, whichever comes first) and the budget of busy threads. |
| `Parquet/Writing/BloomBuilder.cs` | A chunk's Bloom filter: XXH64 of each value's PLAIN bytes as it enters a page, the distinct hashes kept, the filter sized for them at the chunk's end. |
| `Parquet/Writing/ColumnWriter.cs`, `TypedColumnWriters.cs` | A leaf column per row group: levels and values page by page, dictionary judged on the first page holding values (in steps, dropped as soon as it cannot pay) and dropped when it outgrows its budget, scratch given back between row groups on request, pages encoded then handed to compression jobs (`PageJobs.cs`) and kept in order, the dictionary page first; fixed-width, byte-array and boolean values (runs copied straight from Arrow buffers where no conversion is needed, validity read by words) with their statistics (vector bounds for numbers; string bounds at page end, from the distinct dictionary entries of dictionary pages) and bound truncation. |
| `Parquet/Encodings/RleBitPackedEncoder.cs` | The RLE / bit-packing hybrid written: runs of 8 or more as RLE, the rest in bit-packed groups of 8. |
| `Parquet/Encodings/DeltaEncoders.cs` | DELTA_BINARY_PACKED written (blocks of 128 in four miniblocks, INT32 deltas wrapped to 32 bits), DELTA_LENGTH_BYTE_ARRAY and DELTA_BYTE_ARRAY (prefixes shared with the value before). |
| `Parquet/MinimalParquetWriter.cs` | A small, dependency-free Parquet writer for fixed schemas (the `ParquetNode` model): optional and required fields, structs, maps, three-level lists, plain encoding, one row group, no compression. Internal; tests write fixtures with it. |
| `Parquet/ParquetNodeArrow.cs` | `ParquetNode`s as the Arrow schema (and rows as a batch) `ParquetWriter` writes back to the same Parquet types (the Delta checkpoint schema; test fixtures): annotated leaves (strings, int8/int16, dates, decimals as numbers or big-endian bytes, UTC and local microsecond timestamps), structs, lists, string maps. |

In Sazanami (generic, reusable by other formats):

| Where | What |
| :--- | :--- |
| `Arrow/Builders/NativeGrowBuffer.cs` | Growable 64-byte-aligned native buffer and bitmap. |
| `Arrow/Builders/` (`NativeColumnBuilder.cs`, `FixedWidthBuilders.cs`, `BinaryBuilders.cs`, `DictionaryBuilder.cs`, `NestedBuilders.cs`) | Arrow column builders in native memory (fixed width, boolean, binary, fixed-size binary, dictionary, list/map, struct, null) with lazy validity. |
| `Arrow/Interop/NativeArrayExport.cs` | Builders to a `RecordBatch` without copying: buffers are detached into an Arrow C data interface `ArrowArray` (`NativeCArray`) and made into Apache.Arrow arrays over that memory, freed with the last buffer. |
| `Arrow/Interop/ArrowCImport.cs` | Imports Arrow C data interface arrays and streams (from the engine or any producer) as Apache.Arrow arrays over the producer's memory, released with the last buffer; replaces Apache.Arrow's importer, which reports every buffer to the GC as memory pressure (banned: `BannedSymbols.txt`). |
| `Arrow/Interop/ArrowCExport.cs` | Exports Apache.Arrow arrays and record batches through the Arrow C data interface without copying: each buffer retained (shared with the caller, who may dispose at once) and pinned until the consumer releases it; replaces Apache.Arrow's exporter (banned). |
| `Arrow/Builders/PooledArrowAllocator.cs` | The allocator Apache.Arrow builders build with: zeroed `NativeMemoryPool` blocks, without the GC memory pressure Apache.Arrow's default allocator reports. |
| `Arrow/Compute/ArrowFilter.cs` | Keeps the rows a row mask selects from an Arrow array (flat, or structs, lists, maps and fixed-size lists of them), into native memory of its own; long view values and dictionaries are shared with a native input when enough rows are kept (the result then holds its memory), copied otherwise. |
| `Buffers/NativeByteBuffer.cs`, `NativeByteBufferStream.cs` | Native I/O buffers (block bytes, decompression, writer output) and a write stream over them. |
| `Buffers/NativeBufferWriter.cs`, `NativeStreamWriter.cs` | `IBufferWriter<byte>` in native memory: a growable one shaped like `ArrayBufferWriter` (Parquet pages, levels, scratch), and a small window that hands its bytes to a stream (JSON written line by line). |
| `Buffers/NativeMemoryPool.cs` | The process's native memory for large buffers: blocks kept by size class and released when idle; `Poison` (the tests and the fuzzer) hands every block out filled with 0xCC, so code counting on zeroed memory fails. |
| `Buffers/NativeArray.cs` | A growable array of unmanaged values in pooled native memory (page values, levels, indices, hash tables): growth keeps what it is told to keep, memory is not zeroed. |
| `Text/GlobPattern.cs` | Globs over '/'-separated paths as the `glob` crate (Polars' local expansion) matches them: the literal prefix to list from, whether to list recursively, segment-wise matching with `**` backtracking. |
| `Arrow/Builders/ConstantArrays.cs` | Arrays of one value on every row (a dataset's file path and partition columns): string views over one copy, repeated strings, any flat type repeated from one element of an array. |
| `Text/TextValues.cs` | Text values inferred and parsed as Polars' CSV reader does: booleans, floats, integers (Int128 beyond Int64), dates and date-times with offsets; the kind of a column from the kinds of its values. |
| `Arrow/TypeSystem/ArrowTypes.cs` | Whether two Arrow types hold values the same way (parameters compared, nested nullability not); decimal parameters; types as messages show them. |
| `Arrow/Compute/ArrowWidening.cs`, `ArrowConversion.cs` | Conversions between Arrow types as datasets need them: which compile (lossless widenings, lossy ones on request, struct fields by name with missing ones null on request), the narrowest common type of two, and the conversions themselves into native memory (unchanged parts shared with a native source; portable vector paths for the common numeric widenings); arrays of nulls of any type. |
| `Buffers/MemoryStreams.cs`, `PointerMemory.cs` | A memory stream's bytes read in place (its buffer visible or, for a plain `MemoryStream`, hidden: read through `UnsafeAccessor`); native bytes another owner holds (a mapping, an unmanaged memory stream) as `ReadOnlyMemory<byte>`. |
| `Arrow/TypeSystem/ArrowFieldIds.cs` | The field-id metadata key (`PARQUET:field_id`) and helpers to read or set an Arrow field's id. |
| `Arrow/Ipc/IpcSchemaMessage.cs` | Parquet's `ARROW:schema`: an Arrow IPC schema message written and read by the IPC codec (every FlatBuffer position checked; damaged or unknown messages read as absent). |
| `Encodings/BitUnpacking.cs`, `BitUnpack.cs` | LSB-first bit unpacking of 0–64-bit values (above 32 into 64-bit integers): per-width group kernels and a bounds-checked path. |
| `Encodings/ByteStreamSplit.cs` | Joins byte streams back into values (portable widening for 2-, 4- and 8-byte values). |
| `Rows/RowRanges.cs` | Sets of row numbers as sorted, disjoint ranges: union, intersection, difference, complement, slice, take; from indices or a bitmap. |
| `Filters/SplitBlockBloomFilter.cs` | Split block Bloom filters (Parquet's): insert and look up by a 64-bit hash (insert also into a bitset held elsewhere, such as native memory), size for a count and a false positive rate. |
| `Filters/FilterProgram.cs`, `FilterNode.cs`, `LeafTests.cs`, `CompareKernels.cs`, `BinaryKernels.cs`, `Masks.cs` | Physical row predicates over Arrow columns (comparisons, IN, prefix, null and NaN tests, Kleene AND/OR/NOT/COALESCE; values in the column's stored representation), checked once and evaluated per batch into a row mask: vector compares on fixed-width columns, view-prefix shortcuts on strings, dictionaries tested per entry; the mask algebra. |
| `Network/RetrySettings.cs`, `HttpSender.cs`, `RequestGate.cs` | HTTP requests with retries shared by the file stores and the lakehouse's object store clients: network failures (a failed body read included), 408, 429 and 5xx retried with jittered backoff (`Retry-After` honoured up to the cap), failures an answer decided never retried, caller cancellations never retried; synchronous and asynchronous; requests counted per flow; a bound on requests at once that lets waiters in by their order (the dataset reader's request budget). |
| `Buffers/GuardedMemory.cs` | Fuzzing only: blocks that end at an inaccessible page; with it on, `NativeMemoryPool` and the Parquet page reader put one after every buffer. |
| `Encodings/BitPacker.cs` | LSB-first packing of 0–32-bit values (the inverse of bit unpacking). |
| `Encodings/ValueDictionary.cs` | Dictionaries of fixed-width values and of byte strings (open addressing, values in first-appearance order, native memory) for dictionary encoding. |
| `Data/Building/` (`ArrowRowBuilder.cs`, `RowLayout.cs`, `ValueWriters.cs`, `ValueConverters.cs`) | Rows of `T` as Arrow batches in native memory (the mirror of `ArrowRowMapper<T>`): the engine's layout per .NET type, appended by code compiled once per type (sealed writers, struct converters, no boxing); records refused when null; a value the layout refuses faults the builder; the paths of its dictionary columns (`MapColumns`); `ArrowColumnBuilder<T>` (one array); `WideIntegers` (128-bit integers as marked decimal(38, 0) until Apache.Arrow has 128-bit integer types). |
| `Data/Mapping/` accessors (`NestedAccessors.cs`: `MapAccessor`; `ScalarAccessors.cs`: `WideIntegerAccessor`) | Decimal(p, 0) columns into `Int128` / `UInt128` members (a column marked unsigned by its bits). Map columns (Arrow maps, lists of two-field structs: key / value by name, else by position) read into `Dictionary`, `IDictionary`, `IReadOnlyDictionary` and F# `Map` members; null keys refused, a repeated key keeps its last value. |
| `Concurrency/OrderedParallel.cs` | Maps a sequence's items on the thread pool, a window opening 1, 2, 4 … at a time, results in the source's order; failures where a sequential loop raises them; running maps waited for when the consumer stops. |
| `Concurrency/OrderedWindow.cs` | Numbered work items run ahead of a consumer taking them in order, a window opening 1, 2, 4 … at a time within a size budget; results that own resources disposed when never taken. |
| `Arrow/Builders/ArrowBatchAppender.cs` | Rows of many record batches appended as ranges, copied once into growable native buffers, taken as one batch (flat layouts, views with their long values copied, structs, lists, maps, fixed-size lists, dictionaries while they stay the same): the IPC dataset writer's gathering of short runs. |
| `Concurrency/OrderedUnits.cs` | The datasets' read window, for any format: groups (files) planned ahead in order, their units (row groups, record batches) run ahead of a consumer taking the results in order within unit, byte and group bounds; units of a group found as it is read join as they are found; failures in order; a group freed when its last unit is taken (or kept for a result that still needs it). |
| `Compression/Lz4Block.cs`, `Lz4Frame.cs`, `ZstdBlock.cs`, `SnappyBlock.cs` | Block codecs shared by Parquet, Arrow IPC and Avro. LZ4: the decoder copies 16 bytes at a time where the output leaves room (each sequence checked whole first, exact copies near the end, runs and short periods expanded); the encoder is LZ4's fast compressor (8192-entry table reused across calls without clearing, the step growing over data without matches, matches extended backward and compared 8 bytes at a time), its output depending on the input alone; frames of independent 64 KiB blocks, which writers compress side by side. |
| `Arrow/RowViews/ArrowRows.cs`, `ArrowRow.cs`, `ArrowList.cs`, `ArrowRowNode.cs` | Row view over a `RecordBatch` or `StructArray`: typed getters by ordinal, name or field id, nested structs, lists and maps, no allocation per row. |

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
* Row predicates and filtering: `tests/Sazanami.Tests/FilterProgramTests.cs` (random programs on every column type and layout against a row-by-row model of the IR's semantics) and `ArrowFilterTests.cs` (against a model gather; shared memory and its lifetime).
* Arrow IPC (`tests/Seseragi.Files.Tests/Ipc`): every corpus file against pyarrow and Python Polars value by value
  and batch by batch (`IpcValueTests`, `TestData/ipc/values.json`), its structure against an independent FlatBuffer
  reading (`IpcStructureTests`, `messages.json`; the corpus and how it is written: `TestData/ipc/README.md`), every
  reading mode alike (mapped or not, sync or async, memory), projection and row limits, Apache.Arrow's reader on the
  uncompressed files, the Arrow C++ fuzz reproducers, random damage, arrays breaking each checked rule
  (`IpcRobustnessTests`, `IpcDecoderTests`, `IpcEdgeCaseTests`), lifetimes, metadata and errors (`IpcApiTests`). The
  writer: every corpus file written again in both formats and every codec reads back the same, slices, dictionaries,
  bytes independent of parallelism, Apache.Arrow's reader on what is written (`IpcWriterTests`); interop (opt-in:
  `SESERAGI_IPC_PYTHON=<python with pyarrow, polars> dotnet test --filter Category=Interop`): pyarrow and Python Polars
  read our files as they read the originals (`IpcWriterInteropTests`, `TestData/ipc/writer_interop.py`; slices of views
  validated fully by pyarrow). Row numbers in every way of reading (`IpcRowIndexTests`); datasets: expansion, every
  corpus file as a one-file dataset, partitions and pruning, row index across files, schema modes and layouts, errors
  naming files, footers on stores (`IpcDatasetTests`), the window against one thread, remote stores, early stops,
  failures in order, requests bounded, typed rows (`IpcDatasetWindowTests`), guarded memory (`GuardedMemoryTests`). Parallel
  reading gives every corpus file's batches as one thread does, row limits, early stops, a live pipe never stalled,
  failures in place, batch metadata with its batch (`IpcParallelTests`); row filters against the IR evaluator over the
  corpus (`IpcFilterTests`); typed rows in both formats and every codec, the output independent of how rows are passed
  (`IpcTypedTests`).
* Parquet (`tests/Seseragi.Files.Tests/Parquet`): every corpus file against pyarrow and DuckDB oracles
  (`ParquetValueTests`, `TestData/parquet/values.json`, `footers.json`; the corpus and how it is written:
  `TestData/parquet/README.md`), readers and options (`ParquetReaderTests`), nested data, kernels against their
  reference paths (`ParquetKernelTests`), the value encodings against reference encoders (`ParquetEncodingTests`),
  what the fuzzer found (`ParquetFuzzRegressionTests`, `TestData/parquet-fuzz/`), pushdown (`ParquetPruningTests`:
  an evaluator of the IR's semantics checks that no matching row is lost on the corpus and the fixtures, that single
  comparisons on every type keep exactly the row groups with matches, and that lying statistics, indexes and Bloom
  filters are used or ignored as intended; `ParquetRowSelectionTests`: selected rows equal the full read's in every
  way of reading; `ParquetFilterRowsTests`: with `FilterRows`, the rows delivered are those the filter keeps, with the
  full read's values, densely, sparsely and masked in every way of reading, on the fixtures and the corpus; the fixtures and
  how they are written: `TestData/parquet-pushdown/write_pushdown.py`; `ParquetTypedRowsTests`: `ParquetReader<T>` skips
what its predicate cannot match, and gives the rows of one thread on several). Sources and datasets:
  `ParquetMemorySourceTests` (memory, memory streams and unmanaged streams read in place), `ParquetSingleFileTests`
  (joined, prefixed and cut files refused; padding and metadata copies accepted), `FileStoreTests`, `HttpFileStoreTests`
  (ranges, versions, servers without ranges or suffix ranges, compression, the Hub's listing; a local server for
  datasets of URLs), `ParquetDatasetFaultTests` (stores failing, hanging and answering late),
  `ParquetDatasetTests` (expansion, order, schema across files, row index, limits, file paths, stores),
  `ParquetDatasetSchemaTests` (schema modes, conversions, missing and extra columns and fields, filters over converted
  and missing columns),
  `ParquetDatasetWindowTests` (several threads read the rows one thread reads, failures in order, the in-flight
  budget, files open at once, typed rows), `ParquetDatasetRequestTests` (requests to remote stores bounded apart from
  the files open, a slow file not stopping the files after it, early stops with requests waiting),
  `ParquetHiveTests` (keys, types, values, collisions, given values, the glob switch, partition pruning),
  `FilterConstantFoldingTests` and, opt-in with `SESERAGI_PARQUET_PYTHON`, `ParquetDatasetInteropTests` (the same
  datasets, plain and partitioned, read by Python Polars, `TestData/parquet/dataset_polars.py`; files of mixed types
  read by Polars, DuckDB and pyarrow where the rules agree, `TestData/parquet/dataset_schema_oracle.py`). Writing: `ParquetWriterTests` (every type in every
  page layout, codec and dictionary setting read back by our reader with checksums verified; slices, empty batches,
  row groups, the footer's chunks and encodings, the async API, refusals), `ParquetWriterStatisticsTests` (bounds of
  every order, truncation, null and NaN counts; filtered reads at the bounds with statistics against without),
  `ParquetWriterOwnershipTests` (the writer owning its batches: the caller's file whatever the threads, each batch
  disposed once after its rows are encoded — watched memory poisoned on disposal — also on failures and abandoned
  writers; row groups in flight bounded by count and bytes; row groups by the batches' bytes), `ParquetNodeArrowTests`
  (checkpoint-shaped rows through Arrow and the writer: the minimal writer's schema and values),
  `ParquetWriterOfTTests` (typed rows back through `ParquetReader<T>`; the same file whatever the way rows are passed
  or the threads; row groups, null rows, refused values, failing sources), `ParquetGuidTests` (UUID columns of pyarrow
  and DuckDB read as their Guids; `arrow.uuid` fields written annotated),
  `ParquetWriterInteropTests` (opt-in, `Category=Interop`: pyarrow, DuckDB, Python Polars and Spark — vectorized and
  not — read our files, `TestData/parquet/writer_interop.py` and `writer_spark.py`, each also filtering at the bounds
  with statistics and without; the readers' own limits are listed there); `ParquetNestedWriterTests` (random nested columns —
  every list layout, maps, run-end encoded and dictionary-encoded nested values, nulls at every level, slices —
  checked against the generator's own model of each row; the schema and levels pyarrow and Spark write; field ids;
  refusals); the interop test also reads nested files with the four readers and compares the footer of a nested
  file with the one pyarrow writes for the same table; the fuzzer's writer cases, flat and nested. The evaluator of the IR's semantics these share
  is `IrEvaluator.cs`; `FilterLoweringTests.cs` checks Samidare's filter lowering against it. Process-wide state (memory pool,
  guard pages, early stops, `ParquetReader<T>` under each client evaluation policy) is tested in the serial
`tests/Polars.Config.Tests`. Through the engine:
  `tests/Polars.CSharp.Tests/IO/ParquetRowReaderTests.cs` (`ParquetReader<T>` rows and predicates against
`DataFrame.Rows<T>()` and the LINQ provider), `tests/Polars.CSharp.Tests/IO/ParquetManagedPipelineTests.cs` (managed read imported into Polars against Polars'
  own read). Through the Delta kernel: `tests/Seseragi.Lakehouse.Tests` (`ParquetFootersTests`,
  `CheckpointStatisticsTests`, `FileTypesTests`) and the F# `SeseragiFooterStatisticsTests`.
* Fuzzing (by hand): `tests/Seseragi.Files.Fuzz` (`README.md` there: contract, guard pages, how to run it capped).
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
