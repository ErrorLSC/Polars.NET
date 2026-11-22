using System.Buffers;
using System.IO.Pipelines;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Builders;
using Kaminari.Sazanami.Arrow.Compute;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Data.Building;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Json;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Delta;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Wrappers;
using JsonFormat = Kaminari.Seseragi.Files.Json.JsonFormat;

namespace Polars.NET.Core.Json;

/// <summary>
/// JSON read and written by the managed reader and writer (Seseragi.Files): frames come in and go out as Arrow record
/// batches. Reads decode the input on every core (a file mapped, a stream as it arrives); scans infer the columns once,
/// when made, and read the data each time the plan runs (the columns and leading rows the query needs, a row index
/// numbered by the reader); writes and sinks encode on every core, sinks batch by batch as the streaming engine produces
/// them. Paths with a scheme (<c>s3://</c>, <c>az://</c>, <c>gs://</c>, …) go through the managed object store clients
/// (or the native store where they cannot serve the location).
/// </summary>
/// <remarks>
/// Batches read are handed to the engine without being disposed here: exporting them pins their native buffers until
/// the engine releases them. Handles given to sinks are consumed (also when the sink fails).
/// </remarks>
public static class ManagedJson
{
    /// <summary>The inference lengths from which the whole input is taken (as the engine's reader did).</summary>
    private const ulong WholeInputFrom = 16_777_216;

    // ================================================================= options

    private static JsonFormat FormatOf(PlJsonFormat format) => format switch
    {
        PlJsonFormat.Json => JsonFormat.Document,
        PlJsonFormat.JsonLines => JsonFormat.Lines,
        _ => throw new ArgumentOutOfRangeException(nameof(format), format, "Unknown JSON format."),
    };

    /// <summary>Records the columns are inferred from: null (and 0, and very many) take the whole input.</summary>
    private static int? InferLength(ulong? rows) => rows is null or 0 or > WholeInputFrom ? null : (int)rows.Value;

    private static int BatchRows(ulong? rows) => rows is null or 0 ? JsonReadOptions.DefaultBatchSize : (int)Math.Min(rows.Value, int.MaxValue);

    private static long? RowLimit(ulong? rows) => rows is { } n ? (long)Math.Min(n, long.MaxValue) : null;

    /// <summary>The Arrow schema the engine exports for a schema (borrowed): what its frames hold.</summary>
    public static Schema ArrowSchemaOf(SchemaHandle schema)
    {
        ArgumentNullException.ThrowIfNull(schema);
        using DataFrameHandle empty = PolarsWrapper.DataFrameFromSchema(schema, 0);
        using IArrowArrayStream exported = ArrowStreamInterop.ExportToStream(empty, wideIntegers: true);
        return exported.Schema;
    }

    /// <summary>
    /// The types given (a schema, overrides; overrides with a schema replace its columns' types) as the reader decodes
    /// them: as the engine exports them, except columns it cannot take as they are, read otherwise and cast back to the
    /// types given by the engine once read (<see cref="CastBack(LazyFrameHandle)"/>; with errors ignored a value the cast
    /// refuses becomes null): columns holding dictionaries (categoricals, enums) at any depth, read with their values
    /// (strings), and overridden 128-bit integers (decimals: overrides carry no field metadata).
    /// </summary>
    private sealed class GivenTypes : IDisposable
    {
        private readonly List<(string Name, DataTypeHandle Type)> _casts = [];
        private readonly bool _strict;

        public GivenTypes(SchemaHandle? schema, SchemaHandle? overrides, bool ignoreErrors)
        {
            _strict = !ignoreErrors;
            try
            {
                List<Field>? fields = schema is null ? null : Readable(schema, overriding: false);
                List<Field>? replacing = overrides is null ? null : Readable(overrides, overriding: true);
                if (fields is not null && replacing is not null)
                {
                    Dictionary<string, Field> by = replacing.ToDictionary(f => f.Name, StringComparer.Ordinal);
                    if (by.Keys.FirstOrDefault(name => !fields.Any(f => f.Name == name)) is { } unknown)
                        throw new ArgumentException($"The schema has no column '{unknown}' to override.", nameof(overrides));
                    fields = [.. fields.Select(f => by.GetValueOrDefault(f.Name, f))];
                    replacing = null;
                }
                if (fields is not null) Schema = new Schema(fields, null);
                if (replacing is not null) Overrides = replacing.ToDictionary(f => f.Name, f => f.DataType, StringComparer.Ordinal);
            }
            catch
            {
                Dispose();
                throw;
            }
        }

        /// <summary>The schema the reader takes, or null.</summary>
        public Schema? Schema { get; }

        /// <summary>The overrides the reader takes, or null.</summary>
        public IReadOnlyDictionary<string, IArrowType>? Overrides { get; }

        // The fields of a schema (borrowed) as the reader decodes them; the columns read otherwise noted for a cast
        // (a column of the schema overridden afterwards loses its own).
        private List<Field> Readable(SchemaHandle schema, bool overriding)
        {
            Schema exported = ArrowSchemaOf(schema);
            List<Field> fields = new(exported.FieldsList.Count);
            for (int i = 0; i < exported.FieldsList.Count; i++)
            {
                Field field = exported.FieldsList[i];
                IArrowType read = Values(field.DataType);
                // An override replaces the schema's column, its cast with it.
                if (overriding) Forget(field.Name);
                bool wide = overriding && WideIntegers.KindOf(field) is not null;
                if (!ReferenceEquals(read, field.DataType) || wide)
                {
                    PolarsWrapper.GetSchemaFieldAt(schema, (ulong)i, out string name, out DataTypeHandle type);
                    _casts.Add((name, type));
                    field = new Field(field.Name, read, field.IsNullable);
                }
                fields.Add(field);
            }
            return fields;

            void Forget(string name)
            {
                foreach (var (_, type) in _casts.Where(c => c.Name == name)) type.Dispose();
                _casts.RemoveAll(c => c.Name == name);
            }
        }

        // A type with its dictionaries replaced by their values (the same instance when it holds none).
        private static IArrowType Values(IArrowType type) => type switch
        {
            DictionaryType d => Values(d.ValueType),
            StructType s when s.Fields.Select(f => Values(f.DataType)).ToArray() is var types && types.Where((t, i) => !ReferenceEquals(t, s.Fields[i].DataType)).Any() =>
                new StructType([.. s.Fields.Select((f, i) => ReferenceEquals(types[i], f.DataType) ? f : new Field(f.Name, types[i], f.IsNullable))]),
            ListType l when Values(l.ValueDataType) is var item && !ReferenceEquals(item, l.ValueDataType) =>
                new ListType(new Field(l.ValueField.Name, item, l.ValueField.IsNullable)),
            LargeListType l when Values(l.ValueDataType) is var item && !ReferenceEquals(item, l.ValueDataType) =>
                new LargeListType(new Field(l.ValueField.Name, item, l.ValueField.IsNullable)),
            FixedSizeListType l when Values(l.ValueDataType) is var item && !ReferenceEquals(item, l.ValueDataType) =>
                new FixedSizeListType(new Field(l.ValueField.Name, item, l.ValueField.IsNullable), l.ListSize),
            _ => type,
        };

        /// <summary>The query with the columns read otherwise cast back to the types given (the query consumed).</summary>
        public LazyFrameHandle CastBack(LazyFrameHandle lf)
        {
            if (_casts.Count == 0) return lf;
            ExprHandle[] casts = [.. _casts.Select(c => PolarsWrapper.ExprCast(PolarsWrapper.Col(c.Name), PolarsWrapper.DataTypeExprFromDataType(c.Type), _strict, false))];
            return PolarsWrapper.LazyWithColumns(lf, casts);
        }

        /// <summary>The frame with the columns read otherwise cast back (the frame consumed).</summary>
        public DataFrameHandle CastBack(DataFrameHandle df)
        {
            if (_casts.Count == 0) return df;
            LazyFrameHandle lf;
            using (df) lf = PolarsWrapper.DataFrameToLazy(df);
            return PolarsWrapper.LazyCollect(CastBack(lf), PlEngine.Auto);
        }

        public void Dispose()
        {
            foreach (var (_, type) in _casts) type.Dispose();
            _casts.Clear();
        }
    }

    private static JsonReadOptions ReadOptions(string[]? columns, GivenTypes given, ulong? inferSchemaLength, ulong? batchSize, bool ignoreErrors,
        PlJsonFormat format) => new()
    {
        Format = FormatOf(format),
        Columns = columns,
        Schema = given.Schema,
        SchemaOverrides = given.Overrides,
        InferSchemaLength = InferLength(inferSchemaLength),
        BatchSize = BatchRows(batchSize),
        IgnoreErrors = ignoreErrors,
        LeaveOpen = true,
    };

    // ================================================================= read

    /// <summary>Reads a JSON file (a document, or newline-delimited) into a frame.</summary>
    /// <param name="path">The file.</param>
    /// <param name="columns">The columns read (all when null).</param>
    /// <param name="schema">Every column and its type (borrowed): nothing inferred; null infers them.</param>
    /// <param name="schemaOverrides">The types of some inferred columns (borrowed).</param>
    /// <param name="inferSchemaLength">The records the columns are inferred from (null or 0: all).</param>
    /// <param name="batchSize">Rows per batch read.</param>
    /// <param name="ignoreErrors">Values that do not fit their column's type are nulls.</param>
    /// <param name="format">The layout.</param>
    public static DataFrameHandle ReadJson(string path, string[]? columns, SchemaHandle? schema, SchemaHandle? schemaOverrides, ulong? inferSchemaLength,
        ulong? batchSize, bool ignoreErrors, PlJsonFormat format)
    {
        ArgumentNullException.ThrowIfNull(path);
        if (!File.Exists(path)) throw new FileNotFoundException($"JSON file not found: {path}", path);
        using var given = new GivenTypes(schema, schemaOverrides, ignoreErrors);
        using JsonReader reader = JsonReader.Open(path, ReadOptions(columns, given, inferSchemaLength, batchSize, ignoreErrors, format));
        return given.CastBack(Import(reader));
    }

    /// <summary>Reads JSON bytes in place (they stay unchanged during the call).</summary>
    /// <inheritdoc cref="ReadJson(string, string[], SchemaHandle, SchemaHandle, ulong?, ulong?, bool, PlJsonFormat)"/>
    public static DataFrameHandle ReadJson(ReadOnlyMemory<byte> bytes, string[]? columns, SchemaHandle? schema, SchemaHandle? schemaOverrides,
        ulong? inferSchemaLength, ulong? batchSize, bool ignoreErrors, PlJsonFormat format)
    {
        using var given = new GivenTypes(schema, schemaOverrides, ignoreErrors);
        using JsonReader reader = JsonReader.Open(bytes, ReadOptions(columns, given, inferSchemaLength, batchSize, ignoreErrors, format));
        return given.CastBack(Import(reader));
    }

    /// <summary>Reads JSON bytes in place, pinned during the call.</summary>
    /// <inheritdoc cref="ReadJson(string, string[], SchemaHandle, SchemaHandle, ulong?, ulong?, bool, PlJsonFormat)"/>
    public static unsafe DataFrameHandle ReadJson(ReadOnlySpan<byte> bytes, string[]? columns, SchemaHandle? schema, SchemaHandle? schemaOverrides,
        ulong? inferSchemaLength, ulong? batchSize, bool ignoreErrors, PlJsonFormat format)
    {
        // The read is eager: it ends before this returns, the span pinned throughout.
        fixed (byte* p = bytes)
            return ReadJson(PointerMemory.Of(p, bytes.Length), columns, schema, schemaOverrides, inferSchemaLength, batchSize, ignoreErrors, format);
    }

    /// <summary>Reads a stream (from its position; left open) as it arrives.</summary>
    /// <inheritdoc cref="ReadJson(string, string[], SchemaHandle, SchemaHandle, ulong?, ulong?, bool, PlJsonFormat)"/>
    public static DataFrameHandle ReadJson(Stream stream, string[]? columns, SchemaHandle? schema, SchemaHandle? schemaOverrides,
        ulong? inferSchemaLength, ulong? batchSize, bool ignoreErrors, PlJsonFormat format)
    {
        ArgumentNullException.ThrowIfNull(stream);
        using var given = new GivenTypes(schema, schemaOverrides, ignoreErrors);
        using JsonReader reader = JsonReader.Open(stream, ReadOptions(columns, given, inferSchemaLength, batchSize, ignoreErrors, format));
        return given.CastBack(Import(reader));
    }

    /// <summary>
    /// The reader's batches into a frame of contiguous columns (one chunk each, as eager reads return them; the reader's
    /// output owns its memory: the input may go once this returns). The batches are concatenated into one first, on
    /// every core (the long values of views shared, not copied); columns of a type that cannot be (dictionaries) or past
    /// 2 GB are imported as they come and rechunked by the engine. A read failing throws as it is
    /// (<see cref="JsonFormatException"/>, <see cref="JsonValueException"/>, …).
    /// </summary>
    /// <remarks>Batches handed to the engine are not disposed here: their buffers stay pinned until the engine releases them.</remarks>
    private static DataFrameHandle Import(JsonReader reader)
    {
        Schema schema = PolarsArrowCompat.Adapt(reader.Schema, out bool convert);
        List<RecordBatch> batches = [];
        try
        {
            foreach (RecordBatch batch in reader.ReadBatches()) batches.Add(convert ? PolarsArrowCompat.Convert(batch, schema) : batch);
            if (batches.Count > 1 && Concatenated(batches) is { } one)
            {
                foreach (RecordBatch batch in batches) batch.Dispose(); // shared view data stays with the concatenated batch
                batches = [one];
            }
        }
        catch
        {
            foreach (RecordBatch batch in batches) batch.Dispose();
            throw;
        }
        if (batches.Count <= 1) return ArrowStreamInterop.ImportEagerStrict(batches, schema);
        using DataFrameHandle chunked = ArrowStreamInterop.ImportEagerStrict(batches, schema);
        return PolarsWrapper.DataFrameRechunk(chunked);

        static RecordBatch? Concatenated(List<RecordBatch> batches)
        {
            try
            {
                return ArrowConcat.Concatenate(batches, Environment.ProcessorCount);
            }
            catch (Exception e) when (e is NotSupportedException or OverflowException)
            {
                return null;
            }
        }
    }

    // ================================================================= scan

    /// <summary>
    /// A lazy scan of newline-delimited JSON files: a file, a directory, a glob, or a URL of any of them on a store. The
    /// files are listed and the columns inferred now (from the first file holding records); the data is read each time
    /// the plan runs.
    /// </summary>
    /// <param name="path">The file, directory or glob (a URL for a store).</param>
    /// <param name="schema">Every column and its type (borrowed): nothing inferred; null infers them.</param>
    /// <param name="schemaOverrides">The types of some inferred columns (borrowed).</param>
    /// <param name="batchSize">Rows per batch read.</param>
    /// <param name="inferSchemaLength">The records the columns are inferred from (null or 0: all of the first file's).</param>
    /// <param name="nRows">The rows read at most.</param>
    /// <param name="ignoreErrors">Values that do not fit their column's type are nulls.</param>
    /// <param name="rowIndexName">A UInt32 column put first numbering the rows across the files; null for none.</param>
    /// <param name="rowIndexOffset">The row index's first number.</param>
    /// <param name="includePathColumn">A string column put last holding each row's file path; null for none.</param>
    /// <param name="hivePartitioning">Whether <c>key=value</c> directories of the paths are columns.</param>
    /// <param name="cloud">The store's settings (a local path takes none).</param>
    public static LazyFrameHandle ScanNdjson(string path, SchemaHandle? schema, SchemaHandle? schemaOverrides, ulong? batchSize, ulong? inferSchemaLength,
        ulong? nRows, bool ignoreErrors, string? rowIndexName, uint rowIndexOffset, string? includePathColumn, bool hivePartitioning, CloudArgs? cloud)
    {
        ArgumentNullException.ThrowIfNull(path);
        bool url = ManagedStores.IsUrl(path);
        if (!url && !File.Exists(path) && !Directory.Exists(path) && path.IndexOfAny(['*', '?', '[']) < 0)
            throw new FileNotFoundException($"NDJSON file not found: {path}", path);
        var (store, owner) = ManagedStores.Open([path], (cloud ?? CloudArgs.Local).WithProviderFrom(path));
        try
        {
            using var given = new GivenTypes(schema, schemaOverrides, ignoreErrors);
            var options = new JsonDatasetOptions
            {
                Format = JsonFormat.Lines,
                Store = store,
                Schema = given.Schema,
                SchemaOverrides = given.Overrides,
                InferSchemaLength = InferLength(inferSchemaLength),
                FilePathColumn = includePathColumn,
                Partitioning = hivePartitioning ? HivePartitioning.On : HivePartitioning.Off,
            };
            JsonDataset dataset = JsonDataset.Open(path, options);
            Schema read;
            using (JsonDatasetReader probe = dataset.OpenReader(new JsonReadOptions { RowIndexColumn = rowIndexName })) read = probe.Schema;
            var scan = new Scan(read, rowIndexName, rowIndexOffset, RowLimit(nRows));
            return given.CastBack(ArrowStreamInterop.ScanStream(Batches, scan.Schema, strict: true, new Owners(dataset, owner)));

            IEnumerable<RecordBatch> Batches(StreamScanRequest request)
            {
                var (columns, indexed, limit) = scan.Ask(request);
                var read = new JsonReadOptions
                {
                    Columns = columns,
                    MaxRows = limit,
                    BatchSize = BatchRows(batchSize),
                    IgnoreErrors = ignoreErrors,
                    RowIndexColumn = indexed ? rowIndexName : null,
                };
                using JsonDatasetReader reader = dataset.OpenReader(read);
                foreach (RecordBatch batch in reader.ReadBatches()) yield return scan.Deliver(batch, indexed);
            }
        }
        catch
        {
            owner?.Dispose();
            throw;
        }
    }

    /// <summary>
    /// A lazy scan of newline-delimited JSON in memory, read in place each time the plan runs: <paramref name="owner"/>
    /// (what keeps the memory: a copy, a pin) is disposed once the engine releases the scan. The columns are inferred now.
    /// </summary>
    /// <inheritdoc cref="ScanNdjson(string, SchemaHandle, SchemaHandle, ulong?, ulong?, ulong?, bool, string, uint, string, bool, CloudArgs)"/>
    public static LazyFrameHandle ScanNdjson(ReadOnlyMemory<byte> bytes, IDisposable owner, SchemaHandle? schema, SchemaHandle? schemaOverrides,
        ulong? batchSize, ulong? inferSchemaLength, ulong? nRows, bool ignoreErrors, string? rowIndexName, uint rowIndexOffset)
    {
        ArgumentNullException.ThrowIfNull(owner);
        try
        {
            // The columns, decided once: every run reads with them (nothing inferred again).
            Schema full;
            using var given = new GivenTypes(schema, schemaOverrides, ignoreErrors);
            var first = new JsonReadOptions { Schema = given.Schema, SchemaOverrides = given.Overrides, InferSchemaLength = InferLength(inferSchemaLength) };
            using (JsonReader probe = JsonReader.Open(bytes, first)) full = probe.Schema;
            Schema read;
            using (JsonReader probe = JsonReader.Open(bytes, new JsonReadOptions { Schema = full, RowIndexColumn = rowIndexName })) read = probe.Schema;
            var scan = new Scan(read, rowIndexName, rowIndexOffset, RowLimit(nRows));
            return given.CastBack(ArrowStreamInterop.ScanStream(Batches, scan.Schema, strict: true, owner));

            IEnumerable<RecordBatch> Batches(StreamScanRequest request)
            {
                var (columns, indexed, limit) = scan.Ask(request);
                var options = new JsonReadOptions
                {
                    Schema = full,
                    Columns = columns,
                    MaxRows = limit,
                    BatchSize = BatchRows(batchSize),
                    IgnoreErrors = ignoreErrors,
                    RowIndexColumn = indexed ? rowIndexName : null,
                };
                using JsonReader reader = JsonReader.Open(bytes, options);
                foreach (RecordBatch batch in reader.ReadBatches()) yield return scan.Deliver(batch, indexed);
            }
        }
        catch
        {
            owner.Dispose();
            throw;
        }
    }

    /// <summary>A lazy scan of newline-delimited JSON bytes: copied once into native memory the scan keeps.</summary>
    /// <inheritdoc cref="ScanNdjson(string, SchemaHandle, SchemaHandle, ulong?, ulong?, ulong?, bool, string, uint, string, bool, CloudArgs)"/>
    public static LazyFrameHandle ScanNdjson(ReadOnlySpan<byte> bytes, SchemaHandle? schema, SchemaHandle? schemaOverrides, ulong? batchSize,
        ulong? inferSchemaLength, ulong? nRows, bool ignoreErrors, string? rowIndexName, uint rowIndexOffset)
    {
        var copy = new NativeBufferWriter(Math.Max(1, bytes.Length));
        try
        {
            bytes.CopyTo(copy.GetSpan(bytes.Length));
            copy.Advance(bytes.Length);
        }
        catch
        {
            copy.Dispose();
            throw;
        }
        return ScanNdjson(copy.WrittenMemory, copy, schema, schemaOverrides, batchSize, inferSchemaLength, nRows, ignoreErrors, rowIndexName, rowIndexOffset);
    }

    /// <summary>
    /// A lazy scan of newline-delimited JSON in memory read in place: pinned until the engine releases the scan
    /// (<see cref="MemoryManager{T}.Unpin"/> tells), unchanged meanwhile.
    /// </summary>
    /// <inheritdoc cref="ScanNdjson(string, SchemaHandle, SchemaHandle, ulong?, ulong?, ulong?, bool, string, uint, string, bool, CloudArgs)"/>
    public static LazyFrameHandle ScanNdjson(ReadOnlyMemory<byte> bytes, SchemaHandle? schema, SchemaHandle? schemaOverrides, ulong? batchSize,
        ulong? inferSchemaLength, ulong? nRows, bool ignoreErrors, string? rowIndexName, uint rowIndexOffset) =>
        ScanNdjson(bytes, new Pinned(bytes.Pin()), schema, schemaOverrides, batchSize, inferSchemaLength, nRows, ignoreErrors, rowIndexName, rowIndexOffset);

    /// <summary>A lazy scan of a stream (from its position; left open): read now, once, into native memory the scan keeps.</summary>
    /// <inheritdoc cref="ScanNdjson(string, SchemaHandle, SchemaHandle, ulong?, ulong?, ulong?, bool, string, uint, string, bool, CloudArgs)"/>
    public static LazyFrameHandle ScanNdjson(Stream stream, SchemaHandle? schema, SchemaHandle? schemaOverrides, ulong? batchSize,
        ulong? inferSchemaLength, ulong? nRows, bool ignoreErrors, string? rowIndexName, uint rowIndexOffset)
    {
        ArgumentNullException.ThrowIfNull(stream);
        var copy = new NativeBufferWriter(stream.CanSeek ? (int)Math.Clamp(stream.Length - stream.Position, 1, int.MaxValue) : 1 << 16);
        try
        {
            copy.ReadFrom(stream);
        }
        catch
        {
            copy.Dispose();
            throw;
        }
        return ScanNdjson(copy.WrittenMemory, copy, schema, schemaOverrides, batchSize, inferSchemaLength, nRows, ignoreErrors, rowIndexName, rowIndexOffset);
    }

    /// <summary>
    /// What a scan declares and how a run reads: the columns asked (the row index apart: the reader numbers rows only
    /// when it is asked for), the leading rows, and each batch as the engine takes it (the row index as UInt32 from the
    /// offset, as the engine numbers rows).
    /// </summary>
    private sealed class Scan
    {
        private readonly Schema _read;
        private readonly string? _rowIndex;
        private readonly uint _offset;
        private readonly long? _limit;
        private readonly Schema _adapted; // the reader's columns as the engine takes them (the row index aside)
        private readonly bool _convert;

        public Scan(Schema read, string? rowIndex, uint offset, long? limit)
        {
            _read = read;
            _rowIndex = rowIndex;
            _offset = offset;
            _limit = limit;
            Schema data = rowIndex is null ? read : new Schema([.. read.FieldsList.Skip(1)], read.Metadata);
            _adapted = PolarsArrowCompat.Adapt(data, out _convert);
            Schema = rowIndex is null ? _adapted : new Schema([new Field(rowIndex, UInt32Type.Default, nullable: false), .. _adapted.FieldsList], read.Metadata);
        }

        /// <summary>The scan's schema: the row index (UInt32) first when asked, then the columns as the engine takes them.</summary>
        public Schema Schema { get; }

        /// <summary>The columns a run reads (null: all), whether it numbers rows, and the rows it reads at most.</summary>
        public (string[]? Columns, bool Indexed, long? Limit) Ask(StreamScanRequest request)
        {
            bool indexed = _rowIndex is not null && (request.Columns is null || request.Columns.Contains(_rowIndex));
            string[]? columns = request.Columns is { } names && _rowIndex is not null ? [.. names.Where(n => n != _rowIndex)] : request.Columns;
            long? limit = (_limit, request.RowLimit) switch
            {
                ({ } a, { } b) => Math.Min(a, b),
                var (a, b) => a ?? b,
            };
            return (columns, indexed, limit);
        }

        /// <summary>A batch read as the engine takes it (the reader's disposed when replaced).</summary>
        public RecordBatch Deliver(RecordBatch batch, bool indexed)
        {
            if (indexed) batch = Renumbered(batch);
            return _convert ? PolarsArrowCompat.Convert(batch, Adapted(batch.Schema, indexed)) : batch;
        }

        // The schema a converted batch takes: its fields (the asked ones) adapted.
        private Schema Adapted(Schema schema, bool indexed) =>
            new([.. schema.FieldsList.Select((f, i) => indexed && i == 0 ? f : _adapted.GetFieldByName(f.Name) ?? f)], schema.Metadata);

        // The reader's Int64 row numbers (from 0) as UInt32 from the offset.
        private RecordBatch Renumbered(RecordBatch batch)
        {
            var numbers = (Int64Array)batch.Column(0);
            IArrowArray index;
            try
            {
                using var builder = new NativeFixedWidthBuilder<uint>(UInt32Type.Default);
                Span<uint> values = builder.AppendSlots(numbers.Length);
                ReadOnlySpan<long> read = numbers.Values;
                for (int i = 0; i < values.Length; i++)
                {
                    long n = read[i] + _offset;
                    if (n > uint.MaxValue) throw new OverflowException($"The row index '{_rowIndex}' passes {uint.MaxValue}: more rows than it can number.");
                    values[i] = (uint)n;
                }
                index = builder.ExportArray();
            }
            catch
            {
                batch.Dispose();
                throw;
            }
            numbers.Dispose();
            Field field = new(_rowIndex!, UInt32Type.Default, nullable: false);
            return new RecordBatch(new Schema([field, .. batch.Schema.FieldsList.Skip(1)], batch.Schema.Metadata), [index, .. batch.Arrays.Skip(1)], batch.Length);
        }
    }

    /// <summary>Memory pinned for a scan, unpinned once the engine releases it.</summary>
    private sealed class Pinned(MemoryHandle handle) : IDisposable
    {
        private MemoryHandle _handle = handle;

        public void Dispose() => _handle.Dispose();
    }

    /// <summary>A dataset and its store, released with the scan.</summary>
    private sealed class Owners(JsonDataset dataset, IDisposable? store) : IDisposable
    {
        public void Dispose()
        {
            dataset.Dispose();
            store?.Dispose();
        }
    }

    // ================================================================= write a frame

    private static JsonWriteOptions WriteOptions(JsonFormat format, PlExternalCompression compression = PlExternalCompression.Uncompressed, int level = -1) => new()
    {
        Format = format,
        Compression = compression switch
        {
            PlExternalCompression.Uncompressed => JsonCompression.None,
            PlExternalCompression.Gzip => JsonCompression.Gzip,
            PlExternalCompression.ZSTD => JsonCompression.Zstd,
            _ => throw new ArgumentOutOfRangeException(nameof(compression), compression, "Unknown compression."),
        },
        CompressionLevel = level < 0 || compression == PlExternalCompression.Uncompressed ? null : level,
        // The batches exported are handed over: encoded side by side, each released once encoded.
        BatchOwnership = BatchOwnership.Writer,
        LeaveOpen = true,
    };

    /// <summary>Exports the frame batch by batch (chunk by chunk, nothing copied) into a writer made with its schema.</summary>
    private static async Task WriteFrameAsync(DataFrameHandle df, Func<Schema, JsonWriter> create, bool useAsync, CancellationToken ct)
    {
        using IArrowArrayStream exported = ArrowStreamInterop.ExportToStream(df, wideIntegers: true);
        JsonWriter writer = create(exported.Schema);
        try
        {
            while (await exported.ReadNextRecordBatchAsync(ct).ConfigureAwait(false) is { } batch)
            {
                if (useAsync) await writer.WriteAsync(batch, ct).ConfigureAwait(false);
                else writer.Write(batch);
            }
            if (useAsync) await writer.CompleteAsync(ct).ConfigureAwait(false);
            else writer.Complete();
        }
        finally
        {
            if (useAsync) await writer.DisposeAsync().ConfigureAwait(false);
            else writer.Dispose();
        }
    }

    /// <summary>Writes a frame to a file (replaced; a failed write leaves none).</summary>
    public static void WriteJson(DataFrameHandle df, string path, PlJsonFormat format)
    {
        ArgumentNullException.ThrowIfNull(path);
        using FileWriteStream file = LocalWrite(path, mkdir: false, sync: false);
        WriteJson(df, file, format);
        file.Commit();
    }

    /// <summary>Writes a frame to a stream (flushed, left open).</summary>
    public static void WriteJson(DataFrameHandle df, Stream stream, PlJsonFormat format)
    {
        ArgumentNullException.ThrowIfNull(stream);
        WriteFrameAsync(df, schema => JsonWriter.Create(stream, schema, WriteOptions(FormatOf(format))), false, default).GetAwaiter().GetResult();
    }

    /// <summary>Writes a frame to a stream with asynchronous writes only (flushed, left open).</summary>
    public static Task WriteJsonAsync(DataFrameHandle df, Stream stream, PlJsonFormat format, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);
        return WriteFrameAsync(df, schema => JsonWriter.Create(stream, schema, WriteOptions(FormatOf(format))), true, cancellationToken);
    }

    /// <summary>Writes a frame to a buffer writer (a <see cref="PipeWriter"/> is flushed as it fills, never completed).</summary>
    public static void WriteJson(DataFrameHandle df, IBufferWriter<byte> output, PlJsonFormat format)
    {
        ArgumentNullException.ThrowIfNull(output);
        WriteFrameAsync(df, schema => JsonWriter.Create(output, schema, WriteOptions(FormatOf(format))), false, default).GetAwaiter().GetResult();
    }

    /// <summary>Writes a frame to a pipe with asynchronous flushes (flushed, not completed).</summary>
    public static Task WriteJsonAsync(DataFrameHandle df, PipeWriter writer, PlJsonFormat format, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(writer);
        return WriteFrameAsync(df, schema => JsonWriter.Create(writer, schema, WriteOptions(FormatOf(format))), true, cancellationToken);
    }

    /// <summary>A frame as JSON bytes.</summary>
    public static byte[] WriteJsonToMemory(DataFrameHandle df, PlJsonFormat format)
    {
        using var output = new NativeBufferWriter();
        WriteJson(df, output, format);
        return output.WrittenSpan.ToArray();
    }

    /// <summary>A frame as JSON in native memory, handed over without a copy.</summary>
    public static NativeBuffer WriteJsonNativeMemory(DataFrameHandle df, PlJsonFormat format) =>
        ToNative(stream => WriteJson(df, stream, format));

    // ================================================================= sink a query

    /// <summary>
    /// Runs the query on the streaming engine (consumed) and writes its batches as they come into a writer made with
    /// the first's schema (without rows, with no columns: newline-delimited JSON has no header).
    /// </summary>
    private static async Task SinkAsync(LazyFrameHandle lf, Func<Schema, JsonWriter> create, bool maintainOrder, bool useAsync, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(lf);
        JsonWriter? writer = null;
        try
        {
            using CollectBatchesHandle batches = useAsync
                ? await Task.Run(() => PolarsWrapper.LazyCollectBatches(lf, PlEngine.Streaming, maintainOrder), ct).ConfigureAwait(false)
                : PolarsWrapper.LazyCollectBatches(lf, PlEngine.Streaming, maintainOrder);
            while (true)
            {
                ct.ThrowIfCancellationRequested();
                var (frame, done) = useAsync ? await Task.Run(() => Next(batches), ct).ConfigureAwait(false) : Next(batches);
                using (frame)
                {
                    if (done) break;
                    using IArrowArrayStream chunks = ArrowStreamInterop.ExportToStream(frame, wideIntegers: true);
                    while (await chunks.ReadNextRecordBatchAsync(ct).ConfigureAwait(false) is { } chunk)
                    {
                        try
                        {
                            writer ??= create(chunk.Schema);
                        }
                        catch
                        {
                            chunk.Dispose();
                            throw;
                        }
                        if (useAsync) await writer.WriteAsync(chunk, ct).ConfigureAwait(false);
                        else writer.Write(chunk);
                    }
                }
            }
            // Without rows no line is written: nothing, or an empty compressed member (a file sink still publishes its file).
            writer ??= create(new Schema([], null));
            if (useAsync) await writer.CompleteAsync(ct).ConfigureAwait(false);
            else writer.Complete();
        }
        finally
        {
            lf.Dispose(); // consumed (a no-op once the engine took it)
            if (writer is not null)
            {
                if (useAsync) await writer.DisposeAsync().ConfigureAwait(false);
                else writer.Dispose();
            }
        }

        static (DataFrameHandle Frame, bool Done) Next(CollectBatchesHandle batches)
        {
            DataFrameHandle frame = PolarsWrapper.CollectBatchesNext(batches, out bool done);
            return (frame, done || frame.IsInvalid);
        }
    }

    private static Func<Schema, JsonWriter> Lines(Stream stream, PlExternalCompression compression, int level) =>
        schema => JsonWriter.Create(stream, schema, WriteOptions(JsonFormat.Lines, compression, level));

    /// <summary>
    /// Sinks a query (consumed) as newline-delimited JSON into a file, local or on a store: published whole once
    /// written (a failed sink leaves none).
    /// </summary>
    /// <param name="lf">The query (consumed).</param>
    /// <param name="path">The file (a URL for a store).</param>
    /// <param name="compression">The file's compression.</param>
    /// <param name="compressionLevel">The level (-1: the codec's default).</param>
    /// <param name="checkExtension">Whether the path's extension must match the compression (as the engine checks it).</param>
    /// <param name="maintainOrder">Whether the rows keep the query's order.</param>
    /// <param name="syncOnClose">Whether a local file is synced to disk before it takes its name.</param>
    /// <param name="mkdir">Whether the parent directories of a local file are created.</param>
    /// <param name="cloud">The store's settings (a local path takes none).</param>
    public static void SinkJson(LazyFrameHandle lf, string path, PlExternalCompression compression, int compressionLevel, bool checkExtension,
        bool maintainOrder, PlSyncOnClose syncOnClose, bool mkdir, CloudArgs? cloud)
    {
        ArgumentNullException.ThrowIfNull(path);
        FileWriteStream file;
        IDisposable? owner = null;
        try
        {
            if (checkExtension) CheckExtension(path, compression);
            if (ManagedStores.IsUrl(path))
            {
                (IWritableFileStore store, owner) = OpenStore(path, cloud);
                file = store.OpenWrite(path, createOnly: false);
            }
            else file = LocalWrite(path, mkdir, syncOnClose != PlSyncOnClose.None);
        }
        catch
        {
            lf.Dispose();
            owner?.Dispose();
            throw;
        }
        try
        {
            using (file)
            {
                SinkAsync(lf, Lines(file, compression, compressionLevel), maintainOrder, false, default).GetAwaiter().GetResult();
                file.Commit();
            }
        }
        finally
        {
            owner?.Dispose();
        }
    }

    /// <summary>Sinks a query (consumed) as newline-delimited JSON into a stream (flushed, left open).</summary>
    public static void SinkJson(LazyFrameHandle lf, Stream stream, PlExternalCompression compression, int compressionLevel, bool maintainOrder)
    {
        ArgumentNullException.ThrowIfNull(stream);
        SinkAsync(lf, Lines(stream, compression, compressionLevel), maintainOrder, false, default).GetAwaiter().GetResult();
    }

    /// <summary>Sinks a query (consumed) into a stream with asynchronous writes only, the engine read off the caller's thread.</summary>
    public static Task SinkJsonAsync(LazyFrameHandle lf, Stream stream, PlExternalCompression compression, int compressionLevel, bool maintainOrder,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);
        return SinkAsync(lf, Lines(stream, compression, compressionLevel), maintainOrder, true, cancellationToken);
    }

    /// <summary>Sinks a query (consumed) into a pipe with asynchronous flushes (flushed, not completed).</summary>
    public static Task SinkJsonAsync(LazyFrameHandle lf, PipeWriter writer, PlExternalCompression compression, int compressionLevel, bool maintainOrder,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(writer);
        return SinkAsync(lf, schema => JsonWriter.Create(writer, schema, WriteOptions(JsonFormat.Lines, compression, compressionLevel)), maintainOrder, true,
            cancellationToken);
    }

    /// <summary>A query's rows (consumed) as newline-delimited JSON bytes.</summary>
    public static byte[] SinkJsonToMemory(LazyFrameHandle lf, PlExternalCompression compression, int compressionLevel, bool maintainOrder)
    {
        using var output = new NativeBufferWriter();
        SinkAsync(lf, schema => JsonWriter.Create(output, schema, WriteOptions(JsonFormat.Lines, compression, compressionLevel)), maintainOrder, false, default)
            .GetAwaiter().GetResult();
        return output.WrittenSpan.ToArray();
    }

    /// <summary>A query's rows (consumed) as newline-delimited JSON in native memory, handed over without a copy.</summary>
    public static NativeBuffer SinkJsonNativeMemory(LazyFrameHandle lf, PlExternalCompression compression, int compressionLevel, bool maintainOrder) =>
        ToNative(stream => SinkJson(lf, stream, compression, compressionLevel, maintainOrder));

    // ================================================================= partitioned

    /// <summary>
    /// Sinks a query (consumed) as a dataset of newline-delimited JSON files under <paramref name="path"/>, in Hive
    /// partitions of <paramref name="partitionBy"/> (<c>key=value</c> directories), files named
    /// <c>part-{index:D5}-{write id}.jsonl</c>, cut by rows and size, each published whole.
    /// </summary>
    /// <param name="lf">The query (consumed).</param>
    /// <param name="path">The dataset's directory (a URL for a store).</param>
    /// <param name="partitionBy">The partition columns.</param>
    /// <param name="includeKeys">Whether the partition columns are written into the files too.</param>
    /// <param name="keysPreGrouped">Whether the rows come grouped by their keys (not gathered; a partition's file closed once the next one's rows come).</param>
    /// <param name="maxRowsPerFile">The rows of a file at most (0: no limit).</param>
    /// <param name="approxBytesPerFile">The size a file closes at (0: no limit).</param>
    /// <param name="compression">The files' compression.</param>
    /// <param name="compressionLevel">The level (-1: the codec's default).</param>
    /// <param name="maintainOrder">Whether the rows keep the query's order.</param>
    /// <param name="syncOnClose">Whether local files are synced to disk before they take their names.</param>
    /// <param name="existingData">What happens to files there already.</param>
    /// <param name="cloud">The store's settings (a local path takes none).</param>
    public static void SinkJsonPartitioned(LazyFrameHandle lf, string path, string[] partitionBy, bool includeKeys, bool keysPreGrouped,
        ulong maxRowsPerFile, ulong approxBytesPerFile, PlExternalCompression compression, int compressionLevel, bool maintainOrder,
        PlSyncOnClose syncOnClose, ExistingData existingData, CloudArgs? cloud)
    {
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(partitionBy);
        IDisposable? owner = null;
        JsonDatasetWriter? writer = null;
        try
        {
            IWritableFileStore? store = null;
            if (ManagedStores.IsUrl(path)) (store, owner) = OpenStore(path, cloud);
            var options = new JsonDatasetWriteOptions
            {
                Store = store,
                PartitionBy = partitionBy,
                IncludePartitionColumns = includeKeys,
                KeysPreGrouped = keysPreGrouped,
                ExistingData = existingData,
                MaxRowsPerFile = maxRowsPerFile == 0 ? null : (long)Math.Min(maxRowsPerFile, long.MaxValue),
                ApproxBytesPerFile = approxBytesPerFile == 0 ? null : (long)Math.Min(approxBytesPerFile, long.MaxValue),
                SyncOnCommit = syncOnClose != PlSyncOnClose.None,
                Writer = WriteOptions(JsonFormat.Lines, compression, compressionLevel) with { BatchOwnership = BatchOwnership.Caller },
            };
            using CollectBatchesHandle batches = PolarsWrapper.LazyCollectBatches(lf, PlEngine.Streaming, maintainOrder);
            while (true)
            {
                using DataFrameHandle frame = PolarsWrapper.CollectBatchesNext(batches, out bool done);
                if (done || frame.IsInvalid) break;
                using IArrowArrayStream chunks = ArrowStreamInterop.ExportToStream(frame, wideIntegers: true);
                while (chunks.ReadNextRecordBatchAsync().AsTask().GetAwaiter().GetResult() is { } chunk)
                {
                    using (chunk)
                    {
                        writer ??= JsonDatasetWriter.Create(path, chunk.Schema, options);
                        writer.Write(chunk);
                    }
                }
            }
            writer?.Complete();
        }
        catch
        {
            writer?.Abort();
            throw;
        }
        finally
        {
            lf.Dispose();
            writer?.Dispose();
            owner?.Dispose();
        }
    }

    /// <summary>The partition columns a selector picks from a query's schema (the query and the selector borrowed).</summary>
    public static string[] SelectedColumns(LazyFrameHandle lf, SelectorHandle selector)
    {
        // The expression takes its own copy of the selector (making one consumes the selector given to it).
        using LazyFrameHandle selected = PolarsWrapper.LazySelect(PolarsWrapper.LazyClone(lf),
            [PolarsWrapper.SelectorToExpr(PolarsWrapper.CloneSelector(selector))]);
        using SchemaHandle schema = PolarsWrapper.GetLazySchema(selected);
        ulong count = PolarsWrapper.GetSchemaLen(schema);
        string[] names = new string[count];
        for (ulong i = 0; i < count; i++)
        {
            PolarsWrapper.GetSchemaFieldAt(schema, i, out string name, out DataTypeHandle type);
            type.Dispose();
            names[i] = name;
        }
        return names;
    }

    // ================================================================= places written

    // A store's writable files and what to dispose once done.
    private static (IWritableFileStore Store, IDisposable? Owner) OpenStore(string url, CloudArgs? cloud)
    {
        var (store, owner) = ManagedStores.Open([url], (cloud ?? CloudArgs.Local).WithProviderFrom(url));
        return (store!, owner);
    }

    /// <summary>
    /// A local file written through a temporary file renamed into place once committed; its directory must exist
    /// unless <paramref name="mkdir"/>.
    /// </summary>
    private static FileWriteStream LocalWrite(string path, bool mkdir, bool sync)
    {
        string full = Path.GetFullPath(path);
        string? directory = Path.GetDirectoryName(full);
        if (directory is not null && !Directory.Exists(directory))
        {
            if (!mkdir) throw new DirectoryNotFoundException($"The directory of '{path}' does not exist.");
            Directory.CreateDirectory(directory);
        }
        FileWriteStream file = new LocalFileStore().OpenWrite(full, createOnly: false);
        file.SyncOnCommit = sync;
        return file;
    }

    /// <summary>The path's extension checked against the compression, as the engine checks a sink's.</summary>
    internal static void CheckExtension(string path, PlExternalCompression compression)
    {
        string extension = Path.GetExtension(path) is { Length: > 1 } dotted ? dotted[1..] : "";
        string? suffix = compression switch
        {
            PlExternalCompression.Gzip => "gz",
            PlExternalCompression.ZSTD => "zst",
            _ => null,
        };
        if (suffix is not null && extension.Length > 0 && extension != suffix)
            throw new ArgumentException(
                $"The path ({path}) does not conform to standard naming, expected suffix: (.{suffix}); set checkExtension to false if you don't want this behavior.", nameof(path));
        if (suffix is null && extension is "gz" or "zst" or "zstd")
            throw new ArgumentException(
                "Use the compression parameter to control compression, or set checkExtension to false if you want to suffix an uncompressed filename with an ending intended for compression.", nameof(path));
    }

    // Bytes written into native memory, handed over without a copy.
    private static unsafe NativeBuffer ToNative(Action<Stream> write)
    {
        var buffer = new NativeByteBuffer(64 * 1024);
        try
        {
            var sink = new NativeByteBufferStream(buffer);
            write(sink);
            int length = (int)sink.Length;
            return NativeBuffer.TakeNativeMemory(buffer.Detach(), length);
        }
        finally
        {
            buffer.Release();
        }
    }
}
