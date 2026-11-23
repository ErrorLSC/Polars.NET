using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Arrow.Builders;
using Kaminari.Sazanami.Arrow.Compute;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Concurrency;
using Kaminari.Sazanami.Json;
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
    private readonly TextUnits _input;
    private readonly IDisposable? _owned;
    private readonly JsonReadOptions _options;
    private readonly JsonDecodeSettings _settings;
    private readonly JsonRowFilter? _filter;
    private bool _read, _disposed;

    private JsonReader(TextUnits input, IDisposable? owned, JsonReadOptions options, JsonDecodeSettings settings, JsonRowFilter? filter, bool exact)
    {
        _input = input;
        _owned = owned;
        _options = options;
        _settings = settings;
        _filter = filter;
        FiltersExactly = exact;
    }

    /// <summary>The columns read (after <see cref="JsonReadOptions.Columns"/>), the row index first when asked.</summary>
    public Schema Schema => _settings.Indexed ?? _filter?.Selected ?? _settings.Schema;

    /// <summary>
    /// Whether the rows delivered are exactly those <see cref="JsonReadOptions.Filter"/> keeps: true without a filter
    /// (or without <see cref="JsonReadOptions.FilterRows"/>, when every row is delivered as asked), false when a
    /// conjunct of it cannot be evaluated by the reader (the rows then include some the filter drops).
    /// </summary>
    public bool FiltersExactly { get; }

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

    /// <summary>Reads the records of the file at <paramref name="path"/> as rows of <typeparamref name="T"/> (see <see cref="JsonReader{T}"/>).</summary>
    /// <inheritdoc cref="JsonReader{T}.Open(string, Expression{Func{T, bool}}?, JsonReadOptions?)" path="/exception"/>
    public static List<T> ReadList<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(string path, Expression<Func<T, bool>>? where = null,
        JsonReadOptions? options = null)
    {
        using JsonReader<T> reader = JsonReader<T>.Open(path, where, options);
        return reader.ReadList();
    }

    /// <inheritdoc cref="ReadList{T}(string, Expression{Func{T, bool}}?, JsonReadOptions?)"/>
    public static async Task<List<T>> ReadListAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(string path,
        Expression<Func<T, bool>>? where = null, JsonReadOptions? options = null, CancellationToken cancellationToken = default)
    {
        await using JsonReader<T> reader = await JsonReader<T>.OpenAsync(path, where, options, cancellationToken).ConfigureAwait(false);
        return await reader.ReadListAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads JSON text as rows of <typeparamref name="T"/> (see <see cref="JsonReader{T}"/>; named apart from
    /// <see cref="ReadList{T}(string, Expression{Func{T, bool}}?, JsonReadOptions?)"/>, which takes a path).
    /// </summary>
    /// <inheritdoc cref="JsonReader{T}.Open(string, Expression{Func{T, bool}}?, JsonReadOptions?)" path="/exception"/>
    public static List<T> ParseList<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(ReadOnlySpan<char> json,
        Expression<Func<T, bool>>? where = null, JsonReadOptions? options = null)
    {
        using JsonReader<T> reader = JsonReader<T>.Parse(json, where, options);
        return reader.ReadList();
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
        TextUnits input = options.Format == JsonFormat.Document
            ? DocumentInput.Of(data, leaveOpen: false, options.UnitBytes)
            : new StreamInput(data, leaveOpen: false, options.UnitBytes, JsonLinesCut.Instance);
        return await CreateAsync(input, null, options, useAsync, ct).ConfigureAwait(false);
    }

    private static async ValueTask<JsonReader> OpenSourceAsync(IRandomAccess source, IDisposable? owned, JsonReadOptions options, bool mappable,
        bool useAsync, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(source);
        TextUnits input;
        try
        {
            input = await InputAsync(source, options.Format, mappable && options.MemoryMap == JsonMemoryMap.Auto, options.UnitBytes,
                options.MaxDegreeOfParallelism, useAsync, ct).ConfigureAwait(false);
        }
        catch
        {
            owned?.Dispose();
            throw;
        }
        return await CreateAsync(input, owned, options, useAsync, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// The input of <paramref name="source"/> (which the caller keeps open while it is read): gzip, zlib and zstd found by
    /// the first bytes and read in order; else lines cut into blocks, a document scanned (in parallel, by
    /// <paramref name="threads"/>); a local file mapped when <paramref name="map"/> says so.
    /// </summary>
    internal static async ValueTask<TextUnits> InputAsync(IRandomAccess source, JsonFormat format, bool map, int unitBytes, int threads,
        bool useAsync, CancellationToken ct)
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
        return (compressed, format) switch
        {
            (true, JsonFormat.Document) => DocumentInput.Of(decompressed, leaveOpen: false, unitBytes),
            (true, _) => new StreamInput(decompressed, leaveOpen: false, unitBytes, JsonLinesCut.Instance),
            (false, JsonFormat.Document) => DocumentInput.Of(source, null, map, unitBytes, threads),
            _ => new BlockInput(source, null, map, unitBytes, JsonLinesCut.Instance),
        };
    }

    private static async ValueTask<JsonReader> CreateAsync(TextUnits input, IDisposable? owned, JsonReadOptions options, bool useAsync, CancellationToken ct)
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
            // A document's fields the inference did not see are errors (lines pass them over, as NDJSON readers do).
            bool strict = options.Format == JsonFormat.Document && options.Schema is null && !options.IgnoreErrors;
            var (settings, filter, exact) = Prepare(schema, options.Columns, options.FilterRows ? options.Filter : null, options, strict);
            return new JsonReader(input, owned, options, settings, filter, exact);
        }
        catch
        {
            input.Dispose();
            owned?.Dispose();
            throw;
        }
    }

    /// <summary>
    /// What decoding the columns <paramref name="columns"/> of <paramref name="full"/> takes (all when null), and the row
    /// filter of <paramref name="filter"/> (null: none) with whether it is exact; the columns the filter reads are decoded
    /// too, the others passed over (strictly: only those, see <see cref="JsonDecodeSettings"/>). Fails when a column's
    /// type has no decoder.
    /// </summary>
    internal static (JsonDecodeSettings Settings, JsonRowFilter? Filter, bool Exact) Prepare(Schema full, IReadOnlyList<string>? columns, ScalarExpr? filter,
        JsonReadOptions options, bool strict)
    {
        Schema schema = full;
        List<string> passedOver = [];
        if (columns is not null) schema = Project(full, columns, passedOver);
        JsonRowFilter? rows = null;
        bool exact = true;
        if (filter is not null)
        {
            rows = JsonRowFilter.Create(full, schema, filter, out exact);
            schema = rows.Decoded;
            passedOver = [.. full.FieldsList.Where(f => schema.GetFieldIndex(f.Name) < 0).Select(f => f.Name)];
        }
        var settings = new JsonDecodeSettings(schema, options.BatchSize, options.IgnoreErrors, strict, passedOver, options.KeysIgnoreCase)
        {
            Indexed = Indexed(rows?.Selected ?? schema, options.RowIndexColumn),
        };
        Validate(settings);
        return (settings, rows, exact);
    }

    // The batches' schema with the row index column first (null without one).
    private static Schema? Indexed(Schema delivered, string? rowIndex)
    {
        if (rowIndex is null) return null;
        if (rowIndex.Length == 0) throw new ArgumentException("The row index column needs a name.", "options");
        if (delivered.GetFieldIndex(rowIndex) >= 0)
            throw new ArgumentException($"The row index column '{rowIndex}' has the name of another column read.", "options");
        return new Schema([new Field(rowIndex, Int64Type.Default, nullable: false), .. delivered.FieldsList], delivered.Metadata);
    }

    // The columns of the first records (all of them without a length), every record taken in in input order (as
    // dataframe engines fold record types: the fields' order depends on it, so units are not walked apart and merged).
    // A document without elements (or of empty objects only) has no columns.
    private static async ValueTask<Schema> InferAsync(TextUnits input, JsonReadOptions options, bool useAsync, CancellationToken ct)
    {
        using var root = new JsonShape("");
        bool document = options.Format == JsonFormat.Document;
        await ObserveAsync(root, input, document, options.InferSchemaLength, useAsync, ct).ConfigureAwait(false);
        if (root.Records == 0 && !document) throw new JsonFormatException("The input holds no records to infer the columns from (give a schema).");
        return new Schema(root.Columns(options.ViewTypes), null);
    }

    /// <summary>
    /// Takes the first <paramref name="records"/> records of <paramref name="input"/> (all when null) into
    /// <paramref name="root"/>, in input order; errors name the line (and the array index) in the input.
    /// </summary>
    internal static async ValueTask ObserveAsync(JsonShape root, TextUnits input, bool document, int? records, bool useAsync, CancellationToken ct)
    {
        long before = root.Records, lines = 0, limit = records is int count ? before + count : long.MaxValue;
        for (int i = 0; root.Records < limit && (input.Count < 0 || i < input.Count); i++)
        {
            using TextUnitBytes? bytes = await input.GetAsync(i, useAsync, ct).ConfigureAwait(false);
            if (bytes is null) break;
            long taken = root.Records;
            var (unitLines, error) = document ? JsonDecode.ObserveDocument(root, bytes, limit) : Walk(root, bytes, limit);
            if (error is not null) throw error.At(lines, taken - before);
            lines += unitLines;
        }
    }

    // A unit's records taken in, up to the limit: the unit's lines read, or its failure.
    private static (int Lines, UnitError? Error) Walk(JsonShape root, TextUnitBytes bytes, long limit)
    {
        ReadOnlySpan<byte> data = JsonLines.WithoutMark(bytes.Memory.Span, bytes.FileStart);
        var reader = JsonTokenReader.Lines(data);
        int start = 0;
        bool read = false;
        try
        {
            while (root.Records < limit && reader.NextRecord())
            {
                read = true;
                start = reader.BytesConsumed;
                if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject) throw new JsonFormatException(JsonLines.OneObject);
                root.ObserveObject(ref reader);
                if (reader.Read()) throw new JsonFormatException(JsonLines.OneObject);
                root.Records++;
            }
            return (read || root.Records < limit ? reader.Line : 0, null);
        }
        catch (Exception e) when (e is JsonSyntaxException or JsonFormatException or JsonValueException or InvalidOperationException)
        {
            return (reader.Line, new UnitError(reader.Line, JsonLines.OnLine(data, start, e)));
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
            long max = _options.MaxRows ?? long.MaxValue, rows = 0, records = 0, lines = 0;
            using OrderedWindow<JsonUnit> window = Window(default);
            for (int i = 0; rows < max && Has(window, i); i++)
            {
                JsonUnit unit = window.Take(i).GetAwaiter().GetResult();
                try
                {
                    // A failure past the rows asked is not the reader's (the unit decoded up to MaxRows of its own, not
                    // of the whole input): the unit is decoded again up to them.
                    if (unit.Error is not null && _filter is null && rows + unit.Rows >= max)
                    {
                        unit.Dispose();
                        unit = DecodeAsync(i, max - rows, useAsync: false, default).GetAwaiter().GetResult();
                    }
                    if (unit.Error is { } error) throw error.At(lines, records);
                    if (unit.End) yield break;
                    if (_filter is null && rows + unit.Rows > max)
                    {
                        unit.Dispose();
                        unit = DecodeAsync(i, max - rows, useAsync: false, default).GetAwaiter().GetResult();
                        if (unit.Error is { } again) throw again.At(lines, records);
                    }
                    for (int k = 0; k < unit.Batches.Count && rows < max; k++)
                    {
                        RecordBatch batch = unit.Batches[k]!;
                        unit.Batches[k] = null;
                        if (_settings.Indexed is not null) RowNumbers.Shift(batch.Column(0), records);
                        // Filtered rows are counted as they are delivered: the last batch is cut to the limit.
                        if (rows + batch.Length > max) batch = JsonRowFilter.First(batch, (int)(max - rows));
                        rows += batch.Length;
                        yield return batch;
                    }
                    lines += unit.Lines;
                    records += unit.Rows;
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
            long max = _options.MaxRows ?? long.MaxValue, rows = 0, records = 0, lines = 0;
            using OrderedWindow<JsonUnit> window = Window(ct);
            for (int i = 0; rows < max && Has(window, i); i++)
            {
                JsonUnit unit = await window.Take(i).ConfigureAwait(false);
                try
                {
                    // A failure past the rows asked is not the reader's (the unit decoded up to MaxRows of its own, not
                    // of the whole input): the unit is decoded again up to them.
                    if (unit.Error is not null && _filter is null && rows + unit.Rows >= max)
                    {
                        unit.Dispose();
                        unit = await DecodeAsync(i, max - rows, useAsync: true, ct).ConfigureAwait(false);
                    }
                    if (unit.Error is { } error) throw error.At(lines, records);
                    if (unit.End) yield break;
                    if (_filter is null && rows + unit.Rows > max)
                    {
                        unit.Dispose();
                        unit = await DecodeAsync(i, max - rows, useAsync: true, ct).ConfigureAwait(false);
                        if (unit.Error is { } again) throw again.At(lines, records);
                    }
                    for (int k = 0; k < unit.Batches.Count && rows < max; k++)
                    {
                        RecordBatch batch = unit.Batches[k]!;
                        unit.Batches[k] = null;
                        if (_settings.Indexed is not null) RowNumbers.Shift(batch.Column(0), records);
                        // Filtered rows are counted as they are delivered: the last batch is cut to the limit.
                        if (rows + batch.Length > max) batch = JsonRowFilter.First(batch, (int)(max - rows));
                        rows += batch.Length;
                        yield return batch;
                    }
                    lines += unit.Lines;
                    records += unit.Rows;
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
        // A filter decides the rows delivered after decoding: every record of a unit is decoded.
        if (_filter is not null) max = long.MaxValue;
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
        using TextUnitBytes? bytes = await _input.GetAsync(index, useAsync, ct).ConfigureAwait(false);
        return bytes is null ? JsonUnit.EndOfInput : Decode(bytes, _settings, _filter, _options.Format == JsonFormat.Document, maxRows);
    }

    /// <summary>
    /// A unit's records decoded (up to <paramref name="maxRows"/>), filtered, and numbered from the unit's first record
    /// when a row index is asked (<see cref="JsonDecodeSettings.Indexed"/>: the numbers are shifted by the records before
    /// the unit as it is delivered).
    /// </summary>
    internal static JsonUnit Decode(TextUnitBytes bytes, JsonDecodeSettings settings, JsonRowFilter? filter, bool document, long maxRows)
    {
        JsonUnit unit = document ? JsonDecode.DecodeDocument(bytes, settings, maxRows) : JsonDecode.Decode(bytes.Memory.Span, bytes.FileStart, settings, maxRows);
        if (unit.Error is null && (filter is not null || settings.Indexed is not null)) Deliverable(unit, filter, settings.Indexed);
        return unit;
    }

    // A unit's batches filtered and numbered (on the thread that decoded it); batches left without rows are dropped.
    private static void Deliverable(JsonUnit unit, JsonRowFilter? rowFilter, Schema? indexed)
    {
        try
        {
            int kept = 0;
            long first = 0; // the unit's records before the batch
            for (int k = 0; k < unit.Batches.Count; k++)
            {
                RecordBatch batch = unit.Batches[k]!;
                unit.Batches[k] = null;
                int length = batch.Length;
                ulong[]? mask = null;
                RecordBatch? output = rowFilter is null ? batch : rowFilter.Apply(batch, out mask);
                if (output is not null && indexed is not null) output = Numbered(output, indexed, first, mask);
                first += length;
                if (output is not null) unit.Batches[kept++] = output;
            }
            unit.Batches.RemoveRange(kept, unit.Batches.Count - kept);
        }
        catch
        {
            unit.Dispose();
            throw;
        }

        // The batch with its rows' numbers first: from the batch's first record, the rows a filter kept.
        static RecordBatch Numbered(RecordBatch batch, Schema indexed, long first, ulong[]? kept)
        {
            IArrowArray numbers;
            try
            {
                numbers = kept is null ? RowNumbers.Range(first, batch.Length) : RowNumbers.Selected(first, kept, batch.Length);
            }
            catch
            {
                batch.Dispose();
                throw;
            }
            return new RecordBatch(indexed, [numbers, .. batch.Arrays], batch.Length);
        }
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
