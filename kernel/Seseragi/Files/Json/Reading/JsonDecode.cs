using System.Text.Json;
using System.Text.Unicode;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Builders;

namespace Kaminari.Seseragi.Files.Json.Reading;

/// <summary>The lines of newline-delimited JSON.</summary>
internal static class JsonLines
{
    /// <summary>The reader's options for one line: one value, strict JSON, deep nesting allowed.</summary>
    public static readonly JsonReaderOptions ReaderOptions = new() { MaxDepth = 512 };

    /// <summary>The UTF-8 byte order mark, skipped at the start of the input.</summary>
    public static ReadOnlySpan<byte> ByteOrderMark => [0xEF, 0xBB, 0xBF];

    /// <summary>The line from <paramref name="position"/> (without its newline), moving past it; false at the end.</summary>
    public static bool Next(ReadOnlySpan<byte> bytes, ref int position, out ReadOnlySpan<byte> line)
    {
        if (position >= bytes.Length)
        {
            line = default;
            return false;
        }
        int nl = bytes[position..].IndexOf((byte)'\n');
        int end = nl < 0 ? bytes.Length : position + nl;
        line = bytes[position..end];
        position = nl < 0 ? bytes.Length : end + 1;
        return true;
    }

    /// <summary>A line of whitespace only (skipped, as dataframe engines skip it).</summary>
    public static bool IsBlank(ReadOnlySpan<byte> line) => line.IndexOfAnyExcept(" \t\r"u8) < 0;

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
            JsonException j => new JsonFormatException($"{at}: {j.Message}", j),
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

    public int BatchSize { get; }

    public bool IgnoreErrors { get; }

    public bool Strict { get; }

    public IReadOnlyList<string> PassedOver { get; }

    public bool KeysIgnoreCase { get; }

    /// <summary>The records' struct type; null without columns (Apache Arrow has no struct type without fields).</summary>
    public StructType? Type { get; }

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
    public void SkipObject(ref Utf8JsonReader reader)
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
    // One record (the reader on its object's start); false when the line holds more after it.
    private static bool ReadRecord(JsonDecodeSettings settings, StructColumn? root, ref Utf8JsonReader reader, ReadOnlySpan<byte> line)
    {
        if (root is null) settings.SkipObject(ref reader);
        else root.ReadObject(ref reader, line);
        return !reader.Read();
    }

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
        try
        {
            bytes = JsonLines.WithoutMark(bytes, fileStart);
            int invalid = JsonLines.FirstInvalidUtf8(bytes);
            int position = 0, inBatch = 0;
            // Lines bound the rows: the builders take each batch's rows at once.
            long lines = bytes.Count((byte)'\n') + (bytes.Length > 0 && bytes[^1] != '\n' ? 1 : 0);
            long Expected() => Math.Min(Math.Min(lines - unit.Lines, batchSize), maxRows - unit.Rows);
            root?.Reserve(Expected(), bytes.Length);
            try
            {
                while (unit.Rows < maxRows && JsonLines.Next(bytes, ref position, out ReadOnlySpan<byte> line))
                {
                    unit.Lines++;
                    if (invalid >= 0 && invalid < position) throw JsonLines.NotUtf8();
                    if (JsonLines.IsBlank(line)) continue;
                    var reader = new Utf8JsonReader(line, JsonLines.ReaderOptions);
                    if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject || !ReadRecord(settings, root, ref reader, line))
                        throw new JsonFormatException("A line of newline-delimited JSON holds one object.");
                    unit.Rows++;
                    if (++inBatch == batchSize)
                    {
                        unit.Batches.Add(settings.Export(builder, inBatch));
                        root?.EndBatch();
                        inBatch = 0;
                        root?.Reserve(Expected(), bytes.Length);
                    }
                }
            }
            catch (Exception e) when (e is JsonException or JsonFormatException or JsonValueException or InvalidOperationException)
            {
                throw new UnitError(unit.Lines, e);
            }
            if (inBatch > 0)
            {
                unit.Batches.Add(settings.Export(builder, inBatch));
                root?.EndBatch();
            }
        }
        catch (UnitError e)
        {
            unit.Dispose();
            unit.Error = e;
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
    public static JsonUnit DecodeDocument(JsonUnitBytes input, JsonDecodeSettings settings, long maxRows)
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
    public static (int Lines, UnitError? Error) ObserveDocument(JsonShape shape, JsonUnitBytes input, long limit)
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
        var reader = new Utf8JsonReader(rest, JsonLines.ReaderOptions);
        try
        {
            reader.Read();
            if (reader.TokenType != JsonTokenType.StartObject)
                throw new JsonFormatException($"An array element is {Kind(reader.TokenType)}, not an object.");
            if (shape is not null) shape.ObserveObject(ref reader);
            else if (root is not null) root.ReadObject(ref reader, rest);
            else settings!.SkipObject(ref reader);
            return (int)reader.BytesConsumed;
        }
        catch (JsonException e)
        {
            // The reader's own position is the element's: the failure says where it is in the whole input instead.
            string message = e.Message;
            int cut = message.IndexOf(" LineNumber:", StringComparison.Ordinal);
            throw new ElementFailure(Offset(rest, e.LineNumber ?? 0, e.BytePositionInLine ?? 0),
                new JsonFormatException(Literal(cut < 0 ? message : message[..cut]), e));
        }
        catch (Exception e) when (e is JsonFormatException or JsonValueException or InvalidOperationException)
        {
            throw new ElementFailure((int)reader.TokenStartIndex, e);
        }

        // A broken literal is quoted to the end of the reader's input (the rest of the unit here, which depends on
        // where the units were cut): quoted up to the first delimiter instead.
        static string Literal(string message)
        {
            const string Invalid = "' is an invalid JSON literal.";
            int end = message.IndexOf(Invalid, StringComparison.Ordinal);
            if (!message.StartsWith('\'') || end < 0) return message;
            int stop = message.AsSpan(1, end - 1).IndexOfAny(",}] \t\r\n");
            return stop < 0 ? message : $"'{message[1..(stop + 1)]}{message[end..]}";
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

    // The offset of a line's byte (both counted from 0) in bytes.
    private static int Offset(ReadOnlySpan<byte> bytes, long line, long column)
    {
        int at = 0;
        for (long i = 0; i < line; i++)
        {
            int nl = bytes[at..].IndexOf((byte)'\n');
            if (nl < 0) return bytes.Length;
            at += nl + 1;
        }
        return (int)Math.Min(bytes.Length, at + column);
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
