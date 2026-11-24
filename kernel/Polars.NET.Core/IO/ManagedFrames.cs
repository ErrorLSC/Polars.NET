using System.Buffers;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Builders;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Data.Building;
using Kaminari.Seseragi.Files.IO;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Delta;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Wrappers;

namespace Polars.NET.Core.IO;

/// <summary>
/// What the managed file formats (JSON, CSV) share around their readers and writers: the types given as the readers
/// decode them (and cast back), a lazy scan's request turned into a read and its batches into the engine's, a frame
/// exported batch by batch into a writer, a query sunk batch by batch, the places written (local files published whole,
/// stores) and native memory handed over.
/// </summary>
public static class ManagedFrames
{
    /// <summary>A format's writer as the shared loops drive it.</summary>
    internal sealed class FrameWriter(Action<RecordBatch> write, Func<RecordBatch, CancellationToken, ValueTask> writeAsync, Action complete,
        Func<CancellationToken, ValueTask> completeAsync, IAsyncDisposable owner) : IAsyncDisposable, IDisposable
    {
        public void Write(RecordBatch batch) => write(batch);

        public ValueTask WriteAsync(RecordBatch batch, CancellationToken ct) => writeAsync(batch, ct);

        public void Complete() => complete();

        public ValueTask CompleteAsync(CancellationToken ct) => completeAsync(ct);

        public ValueTask DisposeAsync() => owner.DisposeAsync();

        public void Dispose() => owner.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

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
    internal sealed class GivenTypes : IDisposable
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



    /// <summary>
    /// What a scan declares and how a run reads: the columns asked (the row index apart: the reader numbers rows only
    /// when it is asked for), the leading rows, and each batch as the engine takes it (the row index as UInt32 from the
    /// offset, as the engine numbers rows).
    /// </summary>
    internal sealed class Scan
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
    internal sealed class Pinned(MemoryHandle handle) : IDisposable
    {
        private MemoryHandle _handle = handle;

        public void Dispose() => _handle.Dispose();
    }


    // ================================================================= write a frame, sink a query


    /// <summary>Exports the frame batch by batch (chunk by chunk, nothing copied) into a writer made with its schema.</summary>
    internal static async Task WriteFrameAsync(DataFrameHandle df, Func<Schema, FrameWriter> create, bool useAsync, CancellationToken ct)
    {
        using IArrowArrayStream exported = ArrowStreamInterop.ExportToStream(df, wideIntegers: true);
        FrameWriter writer = create(exported.Schema);
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


    /// <summary>
    /// Runs the query on the streaming engine (consumed) and writes its batches as they come into a writer made with
    /// the first's schema (without rows, with <paramref name="empty"/>'s: the query's columns when the format writes them).
    /// </summary>
    internal static async Task SinkAsync(LazyFrameHandle lf, Func<Schema, FrameWriter> create, bool maintainOrder, bool useAsync, CancellationToken ct,
        Func<Schema>? empty = null)
    {
        ArgumentNullException.ThrowIfNull(lf);
        FrameWriter? writer = null;
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
            // Without rows: the format's empty output (a file sink still publishes its file).
            writer ??= create(empty?.Invoke() ?? new Schema([], null));
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
    internal static (IWritableFileStore Store, IDisposable? Owner) OpenStore(string url, CloudArgs? cloud)
    {
        var (store, owner) = ManagedStores.Open([url], (cloud ?? CloudArgs.Local).WithProviderFrom(url));
        return (store!, owner);
    }

    /// <summary>
    /// A local file written through a temporary file renamed into place once committed; its directory must exist
    /// unless <paramref name="mkdir"/>.
    /// </summary>
    internal static FileWriteStream LocalWrite(string path, bool mkdir, bool sync)
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
    internal static unsafe NativeBuffer ToNative(Action<Stream> write)
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
