using System.Text.Json;
using System.Text.Unicode;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Builders;
using Kaminari.Sazanami.Json;
using Kaminari.Seseragi.Files.IO;

namespace Kaminari.Seseragi.Files.Json.Reading;

/// <summary>The lines of newline-delimited JSON.</summary>
internal static class JsonLines
{
    /// <summary>The failure of a line holding something else than one object.</summary>
    public const string OneObject = "A line of newline-delimited JSON holds one object.";

    /// <summary>The UTF-8 byte order mark, skipped at the start of the input.</summary>
    public static ReadOnlySpan<byte> ByteOrderMark => [0xEF, 0xBB, 0xBF];

    /// <summary>
    /// The failure of the record that starts at <paramref name="start"/>: the input not being UTF-8 when the record's line
    /// is not (as when the whole input is checked first), else <paramref name="failure"/>.
    /// </summary>
    public static Exception OnLine(ReadOnlySpan<byte> bytes, int start, Exception failure)
    {
        ReadOnlySpan<byte> rest = bytes[Math.Min(start, bytes.Length)..];
        int newline = rest.IndexOf((byte)'\n');
        return Utf8.IsValid(newline < 0 ? rest : rest[..newline]) ? failure : NotUtf8();
    }

    /// <summary>
    /// Whether to reserve rows after the record that made <paramref name="inBatch"/> rows in the batch and
    /// <paramref name="rows"/> in the unit, and how many more at most: 16 after its first, 256 after its 16th, the rest of
    /// the batch after its 256th or once a batch measured the unit's records; null in between.
    /// </summary>
    public static long? ReserveNow(int inBatch, long rows) => inBatch switch
    {
        0 => long.MaxValue,
        1 when rows == 1 => 16,
        16 when rows == 16 => 256,
        256 when rows == 256 => long.MaxValue,
        _ => null,
    };

    /// <summary>
    /// The rows a unit of <paramref name="length"/> bytes is expected to hold after the first <paramref name="read"/>
    /// bytes gave <paramref name="rows"/> records (a sixteenth more, at least one), at most <paramref name="limit"/>.
    /// </summary>
    public static long ExpectedRows(int length, int read, long rows, long limit) =>
        Math.Max(0, Math.Min(limit, (long)((length - read) * (double)rows / Math.Max(1, read) * 1.0625) + 1));

    /// <summary>The input's bytes with a leading byte order mark left out.</summary>
    public static ReadOnlySpan<byte> WithoutMark(ReadOnlySpan<byte> bytes, bool fileStart) =>
        fileStart && bytes.StartsWith(ByteOrderMark) ? bytes[ByteOrderMark.Length..] : bytes;

    /// <summary>Where the first byte that is not UTF-8 is, or -1 (lines read before it fail first, in input order).</summary>
    public static int FirstInvalidUtf8(ReadOnlySpan<byte> bytes)
    {
        if (Utf8.IsValid(bytes)) return -1;
        int at = 0;
        while (at < bytes.Length)
        {
            if (System.Text.Rune.DecodeFromUtf8(bytes[at..], out _, out int consumed) != System.Buffers.OperationStatus.Done) return at;
            at += consumed;
        }
        return -1;
    }

    /// <summary>The failure of a line holding the first byte that is not UTF-8.</summary>
    public static JsonFormatException NotUtf8() => new("The input is not UTF-8.");
}

/// <summary>
/// A failure inside a unit, at a line counted from the unit's start and (in a document's array) at an element counted
/// from the unit's first one (the reader adds the lines and elements before it).
/// </summary>
internal sealed class UnitError(int line, Exception inner, long element = -1) : Exception(inner.Message, inner)
{
    public int Line { get; } = line;

    /// <summary>The index of the failing array element in the unit, or -1 (lines, a document's frame).</summary>
    public long Element { get; } = element;

    /// <summary>The failure as the reader reports it, at its line (and element) in the whole input.</summary>
    public Exception At(long linesBefore, long elementsBefore = 0)
    {
        string at = Element < 0 ? $"Line {linesBefore + Line}" : $"Line {linesBefore + Line} (array index {elementsBefore + Element})";
        return InnerException switch
        {
            JsonValueException v => new JsonValueException($"{at}: {v.Message}"),
            JsonFormatException f => new JsonFormatException($"{at}: {f.Message}", f.InnerException),
            JsonSyntaxException j => new JsonFormatException($"{at}: {j.Message}", j),
            InvalidOperationException o => new JsonFormatException($"{at}: {o.Message}", o),
            _ => InnerException!,
        };
    }
}

/// <summary>What a unit decoded: record batches, its rows and lines, or its failure; or the end of a stream.</summary>
internal sealed class JsonUnit : IDisposable
{
    public static JsonUnit EndOfInput => new() { End = true };

    public List<RecordBatch?> Batches { get; } = [];

    public long Rows { get; set; }

    public int Lines { get; set; }

    public UnitError? Error { get; set; }

    public bool End { get; private init; }

    public void Dispose()
    {
        foreach (RecordBatch? batch in Batches) batch?.Dispose();
        Batches.Clear();
    }
}

/// <summary>What decoding takes: the columns, rows per batch, and how mismatched values and unknown fields are treated.</summary>
internal sealed class JsonDecodeSettings
{
    private readonly HashSet<string> _passedOver;

    /// <param name="schema">The columns read.</param>
    /// <param name="batchSize">Rows per record batch.</param>
    /// <param name="ignoreErrors">Values that do not fit their type are nulls.</param>
    /// <param name="strict">An object's field its (inferred) struct type lacks is an error.</param>
    /// <param name="passedOver">Top-level fields passed over without an error (columns left out of the read).</param>
    /// <param name="keysIgnoreCase">Fields match columns ignoring case when none matches exactly.</param>
    public JsonDecodeSettings(Schema schema, int batchSize, bool ignoreErrors, bool strict, IReadOnlyList<string> passedOver, bool keysIgnoreCase = false)
    {
        Schema = schema;
        BatchSize = batchSize;
        IgnoreErrors = ignoreErrors;
        Strict = strict;
        PassedOver = passedOver;
        KeysIgnoreCase = keysIgnoreCase;
        _passedOver = [.. passedOver];
        Type = schema.FieldsList.Count == 0 ? null : new StructType(schema.FieldsList);
    }

    public Schema Schema { get; }

    /// <summary>
    /// The schema of the batches delivered with a row index: its column first, then the columns delivered (the
    /// selected ones when filtered); null without a row index.
    /// </summary>
    public Schema? Indexed { get; init; }

    public int BatchSize { get; }

    public bool IgnoreErrors { get; }

    public bool Strict { get; }

    public IReadOnlyList<string> PassedOver { get; }

    public bool KeysIgnoreCase { get; }

    /// <summary>The records' struct type; null without columns (Apache Arrow has no struct type without fields).</summary>
    public StructType? Type { get; }

    /// <summary>
    /// Whether a unit's newlines are counted before it is decoded, so that the builders take exactly its rows at once:
    /// for many leaf columns (more than <see cref="CountedFrom"/>). Otherwise the rows are estimated from the records
    /// read, in a few steps; each step grows every leaf's buffers, which costs more than the pass over the unit's bytes
    /// once there are hundreds of leaves (GH Archive's 600: +9 % time, +17 % memory), and less below (one column: the
    /// pass costs 30 %). Between 64 and 256 leaves both cost the same (measured 2026-11-22).
    /// </summary>
    public bool CountsLines => Type is not null && Leaves(Type) > CountedFrom;

    /// <summary>The most leaf columns decoded with rows estimated instead of counted.</summary>
    public const int CountedFrom = 64;

    private static int Leaves(IArrowType type) => type is NestedType nested ? nested.Fields.Sum(f => Leaves(f.DataType)) : 1;

    /// <summary>The builder of a unit's records (null without columns).</summary>
    public NativeStructBuilder? Builder() => Type is null ? null : (NativeStructBuilder)NativeColumnBuilder.Create(Type);

    /// <summary>The decoder of a unit's records over <paramref name="builder"/>.</summary>
    public StructColumn? Root(NativeStructBuilder? builder) =>
        builder is null ? null : new StructColumn(Type!, builder, "", IgnoreErrors, Strict, PassedOver, KeysIgnoreCase);

    /// <summary>The batch of the rows built (a batch without columns has only its length).</summary>
    public RecordBatch Export(NativeStructBuilder? builder, int rows) => builder?.ExportBatch(Schema) ?? new RecordBatch(Schema, [], rows);

    /// <summary>
    /// An object passed over when there are no columns (the reader on its start, left on its end); strict, its fields
    /// must be ones passed over.
    /// </summary>
    public void SkipObject(ref JsonTokenReader reader)
    {
        if (!Strict)
        {
            reader.Skip();
            return;
        }
        while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
        {
            string name = reader.GetString()!;
            if (!_passedOver.Contains(name)) throw StructColumn.Extra("", name);
            reader.Read();
            reader.Skip();
        }
    }
}

/// <summary>Decodes a unit of lines, or of a document's elements, into record batches of a schema.</summary>
internal static class JsonDecode
{
    /// <summary>
    /// The records of <paramref name="bytes"/> (whole lines) as batches of the settings' schema, at most
    /// <paramref name="maxRows"/> rows; a failure is the unit's <see cref="JsonUnit.Error"/>.
    /// </summary>
    public static JsonUnit Decode(ReadOnlySpan<byte> bytes, bool fileStart, JsonDecodeSettings settings, long maxRows)
    {
        var unit = new JsonUnit();
        int batchSize = settings.BatchSize;
        NativeStructBuilder? builder = settings.Builder();
        StructColumn? root = settings.Root(builder);
        bytes = JsonLines.WithoutMark(bytes, fileStart);
        var reader = JsonTokenReader.Lines(bytes);
        int start = 0, inBatch = 0;
        // Many columns: the lines counted first, the rows reserved exactly (see JsonDecodeSettings.CountsLines).
        long lines = root is not null && settings.CountsLines ? bytes.Count((byte)'\n') + (bytes.Length > 0 && bytes[^1] != '\n' ? 1 : 0) : -1;
        try
        {
            if (lines >= 0) root!.Reserve(Math.Min(Math.Min(lines, batchSize), maxRows), bytes.Length);
            while (unit.Rows < maxRows && reader.NextRecord())
            {
                start = reader.BytesConsumed;
                if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
                    throw new JsonFormatException(JsonLines.OneObject);
                if (root is null) settings.SkipObject(ref reader);
                else root.ReadObject(ref reader);
                if (reader.Read()) throw new JsonFormatException(JsonLines.OneObject);
                unit.Rows++;
                if (++inBatch == batchSize)
                {
                    unit.Batches.Add(settings.Export(builder, inBatch));
                    root?.EndBatch();
                    inBatch = 0;
                }
                if (root is null) continue;
                if (lines >= 0)
                {
                    if (inBatch == 0) root.Reserve(Math.Min(Math.Min(lines - reader.Line, batchSize), maxRows - unit.Rows), bytes.Length - reader.BytesConsumed);
                }
                // Otherwise the builders take the rows the batch is expected to hold (and their bytes), as the unit's
                // records so far measure them: a few at first (records differ in size, and a reservation never shrinks),
                // all of them once 256 records (or a batch) measured them; they grow past it if need be.
                else if (JsonLines.ReserveNow(inBatch, unit.Rows) is long most)
                {
                    long rows = JsonLines.ExpectedRows(bytes.Length, reader.BytesConsumed, unit.Rows, Math.Min(Math.Min(batchSize - inBatch, maxRows - unit.Rows), most));
                    root.Reserve(rows, (long)(rows * (double)reader.BytesConsumed / unit.Rows));
                }
            }
            if (inBatch > 0)
            {
                unit.Batches.Add(settings.Export(builder, inBatch));
                root?.EndBatch();
            }
            unit.Lines = unit.Rows == 0 && maxRows <= 0 ? 0 : reader.Line;
        }
        catch (Exception e) when (e is JsonSyntaxException or JsonFormatException or JsonValueException or InvalidOperationException)
        {
            unit.Dispose();
            unit.Error = new UnitError(reader.Line, JsonLines.OnLine(bytes, start, e));
        }
        finally
        {
            root?.Dispose();
            builder?.Dispose();
        }
        return unit;
    }

    /// <summary>
    /// The elements of a document's unit as batches of the settings' schema, at most <paramref name="maxRows"/> rows; a
    /// failure is the unit's <see cref="JsonUnit.Error"/>, and <see cref="JsonUnit.Lines"/> counts the unit's newlines.
    /// </summary>
    public static JsonUnit DecodeDocument(TextUnitBytes input, JsonDecodeSettings settings, long maxRows)
    {
        ReadOnlySpan<byte> bytes = input.Memory.Span;
        var unit = new JsonUnit();
        int batchSize = settings.BatchSize;
        NativeStructBuilder? builder = settings.Builder();
        StructColumn? root = settings.Root(builder);
        try
        {
            int invalid = JsonLines.FirstInvalidUtf8(bytes), inBatch = 0;
            var elements = new JsonElements(bytes, input.FileStart, input.FileEnd);
            // The builders take each batch's rows at once: as many as the scan counted, or as the first element's size
            // makes the unit hold.
            long count = input.Elements;
            long Expected() => Math.Max(1, Math.Min(Math.Min(count - unit.Rows, batchSize - inBatch), maxRows - unit.Rows));
            if (count > 0) root?.Reserve(Expected(), bytes.Length);
            while (unit.Rows < maxRows)
            {
                int start;
                try
                {
                    if (!elements.Next(out start)) break;
                }
                catch (JsonFormatException e)
                {
                    return Failed(unit, bytes, elements.Position, e, -1, invalid);
                }
                int end;
                try
                {
                    end = start + ReadElement(bytes[start..], settings, root, null);
                }
                catch (ElementFailure f)
                {
                    return Failed(unit, bytes, start + f.Offset, f.InnerException!, elements.InArray ? unit.Rows : -1, invalid);
                }
                if (invalid >= 0 && invalid < end) return Failed(unit, bytes, invalid, JsonLines.NotUtf8(), elements.InArray ? unit.Rows : -1, invalid);
                elements.Advance(end);
                unit.Rows++;
                if (++inBatch == batchSize)
                {
                    unit.Batches.Add(settings.Export(builder, inBatch));
                    root?.EndBatch();
                    inBatch = 0;
                    root?.Reserve(Expected(), bytes.Length);
                }
                else if (count <= 0)
                {
                    count = unit.Rows + (bytes.Length - end) / Math.Max(1, end - start) + 1;
                    root?.Reserve(Expected(), bytes.Length);
                }
            }
            if (inBatch > 0)
            {
                unit.Batches.Add(settings.Export(builder, inBatch));
                root?.EndBatch();
            }
            unit.Lines = bytes.Count((byte)'\n');
            return unit;
        }
        finally
        {
            root?.Dispose();
            builder?.Dispose();
        }
    }

    /// <summary>
    /// A document's unit's elements taken in by the inference, up to <paramref name="limit"/> records in all: its
    /// newlines, or its failure.
    /// </summary>
    public static (int Lines, UnitError? Error) ObserveDocument(JsonShape shape, TextUnitBytes input, long limit)
    {
        ReadOnlySpan<byte> bytes = input.Memory.Span;
        var unit = new JsonUnit();
        int invalid = JsonLines.FirstInvalidUtf8(bytes);
        var elements = new JsonElements(bytes, input.FileStart, input.FileEnd);
        while (shape.Records < limit)
        {
            int start;
            try
            {
                if (!elements.Next(out start)) break;
            }
            catch (JsonFormatException e)
            {
                return (0, Failed(unit, bytes, elements.Position, e, -1, invalid).Error);
            }
            int end;
            try
            {
                end = start + ReadElement(bytes[start..], null, null, shape);
            }
            catch (ElementFailure f)
            {
                return (0, Failed(unit, bytes, start + f.Offset, f.InnerException!, elements.InArray ? unit.Rows : -1, invalid).Error);
            }
            if (invalid >= 0 && invalid < end) return (0, Failed(unit, bytes, invalid, JsonLines.NotUtf8(), elements.InArray ? unit.Rows : -1, invalid).Error);
            elements.Advance(end);
            unit.Rows++;
            shape.Records++;
        }
        return (bytes.Count((byte)'\n'), null);
    }

    // One element (an object) from the start of rest, decoded (root, or passed over without columns) or taken in by the
    // inference (shape): the bytes it took, or an ElementFailure at its offset in rest.
    private static int ReadElement(ReadOnlySpan<byte> rest, JsonDecodeSettings? settings, StructColumn? root, JsonShape? shape)
    {
        var reader = JsonTokenReader.Value(rest);
        try
        {
            reader.Read();
            if (reader.TokenType != JsonTokenType.StartObject)
                throw new JsonFormatException($"An array element is {Kind(reader.TokenType)}, not an object.");
            if (shape is not null) shape.ObserveObject(ref reader);
            else if (root is not null) root.ReadObject(ref reader);
            else settings!.SkipObject(ref reader);
            return reader.BytesConsumed;
        }
        catch (JsonSyntaxException e)
        {
            throw new ElementFailure(e.Position, new JsonFormatException(e.Message, e));
        }
        catch (Exception e) when (e is JsonFormatException or JsonValueException or InvalidOperationException)
        {
            throw new ElementFailure(reader.TokenStartIndex, e);
        }

        static string Kind(JsonTokenType token) => token switch
        {
            JsonTokenType.Null => "null",
            JsonTokenType.True or JsonTokenType.False => "a boolean",
            JsonTokenType.Number => "a number",
            JsonTokenType.String => "a string",
            JsonTokenType.StartArray => "an array",
            _ => token.ToString(),
        };
    }

    // The unit failed at offset at (the first byte that is not UTF-8 when it comes earlier).
    private static JsonUnit Failed(JsonUnit unit, ReadOnlySpan<byte> bytes, int at, Exception error, long element, int invalid)
    {
        if (invalid >= 0 && invalid < at)
        {
            error = JsonLines.NotUtf8();
            at = invalid;
        }
        unit.Dispose();
        unit.Error = new UnitError(bytes[..Math.Min(at, bytes.Length)].Count((byte)'\n') + 1, error, element);
        return unit;
    }

    /// <summary>A failure inside an array element, at an offset from its start.</summary>
    private sealed class ElementFailure(int offset, Exception inner) : Exception(inner.Message, inner)
    {
        public int Offset { get; } = offset;
    }
}

/// <summary>
/// The elements of a document's unit, in order: the document's start (in the first unit: a byte order mark,
/// whitespace, then '[' or a single object), commas between elements, and its end (in the last unit: ']' unless a
/// single object, then whitespace only) are checked; a unit not first starts right after a comma, one not last ends
/// right before one. Failures are <see cref="JsonFormatException"/>s at <see cref="Position"/>.
/// </summary>
internal ref struct JsonElements(ReadOnlySpan<byte> bytes, bool fileStart, bool fileEnd)
{
    private enum State : byte { Head, Element, After, Tail, Done }

    private const string Missing = "An array element is missing (a comma after '[', before ']' or after another comma).";

    private readonly ReadOnlySpan<byte> _bytes = bytes;
    private int _position;
    private State _state = fileStart ? State.Head : State.Element;

    /// <summary>Where the walk stands (where a failure is).</summary>
    public readonly int Position => _position;

    /// <summary>Whether the elements are an array's (not a single object's).</summary>
    public bool InArray { get; private set; } = !fileStart;

    /// <summary>The next element's start; false when the unit has no more (its end checked).</summary>
    public bool Next(out int start)
    {
        start = 0;
        while (true)
        {
            switch (_state)
            {
                case State.Head:
                    _position = _bytes.StartsWith(JsonLines.ByteOrderMark) ? JsonLines.ByteOrderMark.Length : 0;
                    SkipWhitespace();
                    if (_position == _bytes.Length) throw new JsonFormatException("The input holds no JSON document.");
                    if (_bytes[_position] == '[')
                    {
                        InArray = true;
                        _position++;
                        SkipWhitespace();
                        if (_position < _bytes.Length && _bytes[_position] == ']')
                        {
                            _position++;
                            _state = State.Tail;
                        }
                        else _state = State.Element;
                        continue;
                    }
                    if (_bytes[_position] != '{') throw new JsonFormatException("A JSON document to read as rows is an array of objects or one object.");
                    start = _position;
                    _state = State.Tail;
                    return true;
                case State.Element:
                    SkipWhitespace();
                    if (_position == _bytes.Length)
                        throw new JsonFormatException(fileEnd ? "The document ends before its array closes." : Missing);
                    if (_bytes[_position] is (byte)',' or (byte)']')
                        throw new JsonFormatException(Missing);
                    start = _position;
                    _state = State.After;
                    return true;
                case State.After:
                    SkipWhitespace();
                    if (_position == _bytes.Length)
                    {
                        if (fileEnd) throw new JsonFormatException("The document ends before its array closes.");
                        _state = State.Done;
                        return false;
                    }
                    byte c = _bytes[_position++];
                    if (c == ',') _state = State.Element;
                    else if (c == ']') _state = State.Tail;
                    else
                    {
                        _position--;
                        throw new JsonFormatException("An array element is followed by something other than ',' or ']'.");
                    }
                    continue;
                case State.Tail:
                    SkipWhitespace();
                    if (_position < _bytes.Length) throw new JsonFormatException("Data follows the end of the JSON document.");
                    _state = State.Done;
                    return false;
                default:
                    return false;
            }
        }
    }

    /// <summary>The element that started at the last <see cref="Next"/> ends at <paramref name="end"/>.</summary>
    public void Advance(int end) => _position = end;

    private void SkipWhitespace()
    {
        int at = _bytes[_position..].IndexOfAnyExcept(" \t\r\n"u8);
        _position = at < 0 ? _bytes.Length : _position + at;
    }
}
