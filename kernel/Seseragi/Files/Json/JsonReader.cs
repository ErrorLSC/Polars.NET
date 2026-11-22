using System.Runtime.CompilerServices;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Builders;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Concurrency;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Json.Reading;

namespace Kaminari.Seseragi.Files.Json;

/// <summary>
/// Reads JSON as Apache Arrow record batches in native memory: newline-delimited JSON (one object per line, as dataframe
/// engines' NDJSON readers read it) or one document (an array of objects, or a single object; see
/// <see cref="JsonReadOptions.Format"/>). The input is cut into chunks of whole lines or array elements, decoded on
/// several threads and handed over in order; a local file is read through a memory mapping, a store's object by ranged
/// reads, a stream in order (gzip, zlib and zstd input is decompressed on the way). Columns are given
/// (<see cref="JsonReadOptions.Schema"/>) or inferred from the first records; the input is checked to be UTF-8 and
/// JSON of the layout read.
/// </summary>
public sealed class JsonReader : IDisposable, IAsyncDisposable
{
    private readonly JsonInput _input;
    private readonly IDisposable? _owned;
    private readonly JsonReadOptions _options;
    private readonly JsonDecodeSettings _settings;
    private bool _read, _disposed;

    private JsonReader(JsonInput input, IDisposable? owned, JsonReadOptions options, JsonDecodeSettings settings)
    {
        _input = input;
        _owned = owned;
        _options = options;
        _settings = settings;
    }

    /// <summary>The columns read (after <see cref="JsonReadOptions.Columns"/>).</summary>
    public Schema Schema => _settings.Schema;

    /// <summary>Opens a file.</summary>
    public static JsonReader Open(string path, JsonReadOptions? options = null) =>
        OpenPathAsync(path, options ?? new JsonReadOptions(), useAsync: false, default).GetAwaiter().GetResult();

    /// <inheritdoc cref="Open(string, JsonReadOptions?)"/>
    public static Task<JsonReader> OpenAsync(string path, JsonReadOptions? options = null, CancellationToken cancellationToken = default) =>
        OpenPathAsync(path, options ?? new JsonReadOptions(), useAsync: true, cancellationToken).AsTask();

    /// <summary>Opens a stream (read from its position; left open with <see cref="JsonReadOptions.LeaveOpen"/>).</summary>
    public static JsonReader Open(Stream stream, JsonReadOptions? options = null) =>
        OpenStreamAsync(stream, options ?? new JsonReadOptions(), useAsync: false, default).GetAwaiter().GetResult();

    /// <inheritdoc cref="Open(Stream, JsonReadOptions?)"/>
    public static Task<JsonReader> OpenAsync(Stream stream, JsonReadOptions? options = null, CancellationToken cancellationToken = default) =>
        OpenStreamAsync(stream, options ?? new JsonReadOptions(), useAsync: true, cancellationToken).AsTask();

    /// <summary>Opens data read at offsets (an object in a store, memory); the caller keeps it open while reading.</summary>
    public static JsonReader Open(IRandomAccess source, JsonReadOptions? options = null) =>
        OpenSourceAsync(source, null, options ?? new JsonReadOptions(), mappable: false, useAsync: false, default).GetAwaiter().GetResult();

    /// <inheritdoc cref="Open(IRandomAccess, JsonReadOptions?)"/>
    public static Task<JsonReader> OpenAsync(IRandomAccess source, JsonReadOptions? options = null, CancellationToken cancellationToken = default) =>
        OpenSourceAsync(source, null, options ?? new JsonReadOptions(), mappable: false, useAsync: true, cancellationToken).AsTask();

    /// <summary>Opens bytes in memory (UTF-8), read in place: the caller keeps them unchanged while reading.</summary>
    public static JsonReader Open(ReadOnlyMemory<byte> bytes, JsonReadOptions? options = null) =>
        OpenSourceAsync(new MemoryRandomAccess(bytes), null, options ?? new JsonReadOptions(), mappable: false, useAsync: false, default).GetAwaiter().GetResult();

    /// <summary>
    /// Opens JSON text (a string): it is encoded to UTF-8 once, into native memory the reader keeps. Unpaired
    /// surrogates are refused (<see cref="JsonFormatException"/>).
    /// </summary>
    public static JsonReader Parse(ReadOnlySpan<char> json, JsonReadOptions? options = null)
    {
        int length = System.Text.Encoding.UTF8.GetByteCount(json);
        var buffer = new NativeByteBuffer(Math.Max(1, length), pooled: true);
        if (System.Text.Unicode.Utf8.FromUtf16(json, buffer.Span, out int read, out int written, replaceInvalidSequences: false) != System.Buffers.OperationStatus.Done)
        {
            buffer.Release();
            throw new JsonFormatException($"The text holds an unpaired surrogate at character {read}.");
        }
        // The reader owns the bytes from here (released with it, or when opening fails).
        var source = new MemoryRandomAccess(buffer.Memory[..written]);
        return OpenSourceAsync(source, new Releaser(buffer), options ?? new JsonReadOptions(), mappable: false, useAsync: false, default).GetAwaiter().GetResult();
    }

    private static ValueTask<JsonReader> OpenPathAsync(string path, JsonReadOptions options, bool useAsync, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(path);
        var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 1,
            useAsync ? FileOptions.RandomAccess | FileOptions.Asynchronous : FileOptions.RandomAccess);
        RandomAccessSource source;
        try { source = new RandomAccessSource(stream, leaveOpen: false, null, mappable: options.MemoryMap == JsonMemoryMap.Auto); }
        catch
        {
            stream.Dispose();
            throw;
        }
        return OpenSourceAsync(source, source, options, mappable: true, useAsync, ct);
    }

    private static async ValueTask<JsonReader> OpenStreamAsync(Stream stream, JsonReadOptions options, bool useAsync, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(stream);
        if (stream.CanSeek)
        {
            var source = new RandomAccessSource(stream, options.LeaveOpen, null);
            return await OpenSourceAsync(source, source, options, mappable: false, useAsync, ct).ConfigureAwait(false);
        }
        // A stream read in order: its first bytes tell whether it is compressed.
        byte[] head = new byte[4];
        int got = 0;
        try
        {
            while (got < head.Length)
            {
                int n = useAsync ? await stream.ReadAsync(head.AsMemory(got), ct).ConfigureAwait(false) : stream.Read(head.AsSpan(got));
                if (n == 0) break;
                got += n;
            }
        }
        catch when (!options.LeaveOpen)
        {
            await stream.DisposeAsync().ConfigureAwait(false);
            throw;
        }
        var prefixed = new PrefixedStream(head[..got], stream, options.LeaveOpen);
        Stream data = StreamInput.Decompressed(prefixed, head.AsSpan(0, got), out _);
        JsonInput input = options.Format == JsonFormat.Document
            ? DocumentInput.Of(data, leaveOpen: false, options.UnitBytes)
            : new StreamInput(data, leaveOpen: false, options.UnitBytes);
        return await CreateAsync(input, null, options, useAsync, ct).ConfigureAwait(false);
    }

    private static async ValueTask<JsonReader> OpenSourceAsync(IRandomAccess source, IDisposable? owned, JsonReadOptions options, bool mappable,
        bool useAsync, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(source);
        JsonInput input;
        try
        {
            byte[] head = new byte[(int)Math.Min(4, source.Length)];
            int got = 0;
            while (got < head.Length)
            {
                int n = useAsync ? await source.ReadAtAsync(got, head.AsMemory(got), ct).ConfigureAwait(false) : source.ReadAt(got, head.AsSpan(got));
                if (n <= 0) break;
                got += n;
            }
            Stream decompressed = StreamInput.Decompressed(new RandomAccessStream(source, 0), head.AsSpan(0, got), out bool compressed);
            bool map = mappable && options.MemoryMap == JsonMemoryMap.Auto;
            input = (compressed, options.Format) switch
            {
                (true, JsonFormat.Document) => DocumentInput.Of(decompressed, leaveOpen: false, options.UnitBytes),
                (true, _) => new StreamInput(decompressed, leaveOpen: false, options.UnitBytes),
                (false, JsonFormat.Document) => DocumentInput.Of(source, null, map, options.UnitBytes, options.MaxDegreeOfParallelism),
                _ => new BlockInput(source, null, map, options.UnitBytes),
            };
        }
        catch
        {
            owned?.Dispose();
            throw;
        }
        return await CreateAsync(input, owned, options, useAsync, ct).ConfigureAwait(false);
    }

    private static async ValueTask<JsonReader> CreateAsync(JsonInput input, IDisposable? owned, JsonReadOptions options, bool useAsync, CancellationToken ct)
    {
        try
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.BatchSize);
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.MaxDegreeOfParallelism);
            if (options.InferSchemaLength is <= 0) throw new ArgumentOutOfRangeException(nameof(options), "InferSchemaLength is at least 1 (null infers from the whole input).");
            if (options.MaxRows is < 0) throw new ArgumentOutOfRangeException(nameof(options), "MaxRows cannot be negative.");
            if (options.Schema is not null && options.SchemaOverrides is not null)
                throw new ArgumentException("Give either a schema or schema overrides, not both.", nameof(options));
            if (!Enum.IsDefined(options.Format)) throw new ArgumentOutOfRangeException(nameof(options), $"Unknown JSON format {options.Format}.");
            Schema schema = options.Schema ?? await InferAsync(input, options, useAsync, ct).ConfigureAwait(false);
            if (options.SchemaOverrides is { } overrides) schema = Override(schema, overrides);
            List<string> passedOver = [];
            if (options.Columns is { } columns) schema = Project(schema, columns, passedOver);
            // A document's fields the inference did not see are errors (lines pass them over, as NDJSON readers do).
            bool strict = options.Format == JsonFormat.Document && options.Schema is null && !options.IgnoreErrors;
            var settings = new JsonDecodeSettings(schema, options.BatchSize, options.IgnoreErrors, strict, passedOver);
            Validate(settings);
            return new JsonReader(input, owned, options, settings);
        }
        catch
        {
            input.Dispose();
            owned?.Dispose();
            throw;
        }
    }

    // The columns of the first records (all of them without a length), every record taken in in input order (as
    // dataframe engines fold record types: the fields' order depends on it, so units are not walked apart and merged).
    // A document without elements (or of empty objects only) has no columns.
    private static async ValueTask<Schema> InferAsync(JsonInput input, JsonReadOptions options, bool useAsync, CancellationToken ct)
    {
        using var root = new JsonShape("");
        bool document = options.Format == JsonFormat.Document;
        long lines = 0, limit = options.InferSchemaLength ?? long.MaxValue;
        for (int i = 0; root.Records < limit && (input.Count < 0 || i < input.Count); i++)
        {
            using JsonUnitBytes? bytes = await input.GetAsync(i, useAsync, ct).ConfigureAwait(false);
            if (bytes is null) break;
            long records = root.Records;
            var (unitLines, error) = document ? JsonDecode.ObserveDocument(root, bytes, limit) : Walk(root, bytes, limit);
            if (error is not null) throw error.At(lines, records);
            lines += unitLines;
        }
        if (root.Records == 0 && !document) throw new JsonFormatException("The input holds no records to infer the columns from (give a schema).");
        return new Schema(root.Columns(options.ViewTypes), null);
    }

    // A unit's records taken in, up to the limit.
    private static (int Lines, UnitError? Error) Walk(JsonShape root, JsonUnitBytes bytes, long limit)
    {
        int lines = 0;
        try
        {
            ReadOnlySpan<byte> data = JsonLines.WithoutMark(bytes.Memory.Span, bytes.FileStart);
            int invalid = JsonLines.FirstInvalidUtf8(data);
            int position = 0;
            while (root.Records < limit && JsonLines.Next(data, ref position, out ReadOnlySpan<byte> line))
            {
                lines++;
                try
                {
                    if (invalid >= 0 && invalid < position) throw JsonLines.NotUtf8();
                    if (JsonLines.IsBlank(line)) continue;
                    var reader = new Utf8JsonReader(line, JsonLines.ReaderOptions);
                    if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
                        throw new JsonFormatException("A line of newline-delimited JSON holds one object.");
                    root.ObserveObject(ref reader);
                    if (reader.Read()) throw new JsonFormatException("A line of newline-delimited JSON holds one object.");
                }
                catch (Exception e) when (e is JsonException or JsonFormatException or JsonValueException or InvalidOperationException)
                {
                    throw new UnitError(lines, e);
                }
                root.Records++;
            }
            return (lines, null);
        }
        catch (UnitError e)
        {
            return (lines, e);
        }
    }

    private static Schema Override(Schema schema, IReadOnlyDictionary<string, IArrowType> overrides)
    {
        var unknown = overrides.Keys.Where(k => schema.GetFieldIndex(k) < 0).ToList();
        if (unknown.Count > 0) throw new ArgumentException($"The inferred columns have none named {string.Join(", ", unknown)}.", nameof(overrides));
        return new Schema([.. schema.FieldsList.Select(f => overrides.TryGetValue(f.Name, out IArrowType? t) ? new Field(f.Name, t, true) : f)], schema.Metadata);
    }

    // The columns named, in schema order; the others are passed over.
    private static Schema Project(Schema schema, IReadOnlyList<string> columns, List<string> passedOver)
    {
        var unknown = columns.Where(c => schema.GetFieldIndex(c) < 0).ToList();
        if (unknown.Count > 0) throw new ArgumentException($"There are no columns named {string.Join(", ", unknown)}.", nameof(columns));
        var wanted = columns.ToHashSet();
        passedOver.AddRange(schema.FieldsList.Where(f => !wanted.Contains(f.Name)).Select(f => f.Name));
        return new Schema([.. schema.FieldsList.Where(f => wanted.Contains(f.Name))], schema.Metadata);
    }

    // Every column's type has a decoder (fails with NotSupportedException otherwise).
    private static void Validate(JsonDecodeSettings settings)
    {
        using NativeStructBuilder? builder = settings.Builder();
        using StructColumn? root = settings.Root(builder);
    }

    /// <summary>The records as record batches (each the caller's to dispose); a stream is read once.</summary>
    public IEnumerable<RecordBatch> ReadBatches()
    {
        Begin();
        return Batches();

        IEnumerable<RecordBatch> Batches()
        {
            long max = _options.MaxRows ?? long.MaxValue, rows = 0, lines = 0;
            using OrderedWindow<JsonUnit> window = Window(default);
            for (int i = 0; rows < max && Has(window, i); i++)
            {
                JsonUnit unit = window.Take(i).GetAwaiter().GetResult();
                try
                {
                    if (unit.Error is { } error) throw error.At(lines, rows);
                    if (unit.End) yield break;
                    if (rows + unit.Rows > max)
                    {
                        unit.Dispose();
                        unit = DecodeAsync(i, max - rows, useAsync: false, default).GetAwaiter().GetResult();
                        if (unit.Error is { } again) throw again.At(lines, rows);
                    }
                    for (int k = 0; k < unit.Batches.Count; k++)
                    {
                        RecordBatch batch = unit.Batches[k]!;
                        unit.Batches[k] = null;
                        rows += batch.Length;
                        yield return batch;
                    }
                    lines += unit.Lines;
                }
                finally
                {
                    unit.Dispose();
                    _input.Release(i);
                }
            }
        }
    }

    /// <inheritdoc cref="ReadBatches"/>
    public IAsyncEnumerable<RecordBatch> ReadBatchesAsync(CancellationToken cancellationToken = default)
    {
        Begin();
        return Batches(cancellationToken);

        async IAsyncEnumerable<RecordBatch> Batches([EnumeratorCancellation] CancellationToken ct)
        {
            long max = _options.MaxRows ?? long.MaxValue, rows = 0, lines = 0;
            using OrderedWindow<JsonUnit> window = Window(ct);
            for (int i = 0; rows < max && Has(window, i); i++)
            {
                JsonUnit unit = await window.Take(i).ConfigureAwait(false);
                try
                {
                    if (unit.Error is { } error) throw error.At(lines, rows);
                    if (unit.End) yield break;
                    if (rows + unit.Rows > max)
                    {
                        unit.Dispose();
                        unit = await DecodeAsync(i, max - rows, useAsync: true, ct).ConfigureAwait(false);
                        if (unit.Error is { } again) throw again.At(lines, rows);
                    }
                    for (int k = 0; k < unit.Batches.Count; k++)
                    {
                        RecordBatch batch = unit.Batches[k]!;
                        unit.Batches[k] = null;
                        rows += batch.Length;
                        yield return batch;
                    }
                    lines += unit.Lines;
                }
                finally
                {
                    unit.Dispose();
                    _input.Release(i);
                }
            }
        }
    }

    private void Begin()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_input.ReadOnce && _read) throw new InvalidOperationException("A stream is read once.");
        _read = true;
    }

    // Units decoded ahead of the reader, as many as the threads, within a budget of input bytes.
    private OrderedWindow<JsonUnit> Window(CancellationToken ct)
    {
        int threads = _options.MaxDegreeOfParallelism;
        long max = _options.MaxRows ?? long.MaxValue;
        int unit = _options.UnitBytes;
        return new OrderedWindow<JsonUnit>(Math.Max(0, _input.Count), (i, token) => DecodeAsync(i, max, useAsync: false, token), threads,
            _ => unit, 2L * threads * unit, ct, opened: _options.MaxRows is null);
    }

    // Whether unit i may exist: below the count, or (a stream) found as it is read, the window grown to it.
    private bool Has(OrderedWindow<JsonUnit> window, int i)
    {
        if (_input.Count >= 0) return i < _input.Count;
        window.Grow(i + 2 * _options.MaxDegreeOfParallelism);
        return true;
    }

    private async Task<JsonUnit> DecodeAsync(int index, long maxRows, bool useAsync, CancellationToken ct)
    {
        using JsonUnitBytes? bytes = await _input.GetAsync(index, useAsync, ct).ConfigureAwait(false);
        return bytes is null ? JsonUnit.EndOfInput
            : _options.Format == JsonFormat.Document ? JsonDecode.DecodeDocument(bytes, _settings, maxRows)
            : JsonDecode.Decode(bytes.Memory.Span, bytes.FileStart, _settings, maxRows);
    }

    /// <summary>Releases the input (and closes a file or stream the reader owns).</summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _input.Dispose();
        _owned?.Dispose();
    }

    /// <inheritdoc cref="Dispose"/>
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }
}
