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

/// <summary>A failure inside a unit, at a line counted from the unit's start (the reader adds the lines before it).</summary>
internal sealed class UnitError(int line, Exception inner) : Exception(inner.Message, inner)
{
    public int Line { get; } = line;

    /// <summary>The failure as the reader reports it, at its line in the whole input.</summary>
    public Exception At(long linesBefore)
    {
        long line = linesBefore + Line;
        return InnerException switch
        {
            JsonValueException v => new JsonValueException($"Line {line}: {v.Message}"),
            JsonFormatException f => new JsonFormatException($"Line {line}: {f.Message}", f.InnerException),
            JsonException j => new JsonFormatException($"Line {line}: {j.Message}", j),
            InvalidOperationException o => new JsonFormatException($"Line {line}: {o.Message}", o),
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

/// <summary>Decodes a unit of lines into record batches of a schema.</summary>
internal static class JsonDecode
{
    // One record (the reader on its object's start); false when the line holds more after it.
    private static bool ReadRecord(StructColumn root, ref Utf8JsonReader reader, ReadOnlySpan<byte> line)
    {
        root.ReadObject(ref reader, line);
        return !reader.Read();
    }

    /// <summary>
    /// The records of <paramref name="bytes"/> (whole lines) as batches of <paramref name="schema"/>, at most
    /// <paramref name="maxRows"/> rows; a failure is the unit's <see cref="JsonUnit.Error"/>.
    /// </summary>
    public static JsonUnit Decode(ReadOnlySpan<byte> bytes, bool fileStart, Schema schema, int batchSize, bool ignoreErrors, long maxRows)
    {
        var unit = new JsonUnit();
        var type = new StructType(schema.FieldsList);
        var builder = (NativeStructBuilder)NativeColumnBuilder.Create(type);
        var root = new StructColumn(type, builder, "", ignoreErrors);
        try
        {
            bytes = JsonLines.WithoutMark(bytes, fileStart);
            int invalid = JsonLines.FirstInvalidUtf8(bytes);
            int position = 0, inBatch = 0;
            // Lines bound the rows: the builders take each batch's rows at once.
            long lines = bytes.Count((byte)'\n') + (bytes.Length > 0 && bytes[^1] != '\n' ? 1 : 0);
            long Expected() => Math.Min(Math.Min(lines - unit.Lines, batchSize), maxRows - unit.Rows);
            root.Reserve(Expected(), bytes.Length);
            try
            {
                while (unit.Rows < maxRows && JsonLines.Next(bytes, ref position, out ReadOnlySpan<byte> line))
                {
                    unit.Lines++;
                    if (invalid >= 0 && invalid < position) throw JsonLines.NotUtf8();
                    if (JsonLines.IsBlank(line)) continue;
                    var reader = new Utf8JsonReader(line, JsonLines.ReaderOptions);
                    if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject || !ReadRecord(root, ref reader, line))
                        throw new JsonFormatException("A line of newline-delimited JSON holds one object.");
                    unit.Rows++;
                    if (++inBatch == batchSize)
                    {
                        unit.Batches.Add(builder.ExportBatch(schema));
                        root.EndBatch();
                        inBatch = 0;
                        root.Reserve(Expected(), bytes.Length);
                    }
                }
            }
            catch (Exception e) when (e is JsonException or JsonFormatException or JsonValueException or InvalidOperationException)
            {
                throw new UnitError(unit.Lines, e);
            }
            if (inBatch > 0)
            {
                unit.Batches.Add(builder.ExportBatch(schema));
                root.EndBatch();
            }
        }
        catch (UnitError e)
        {
            unit.Dispose();
            unit.Error = e;
        }
        finally
        {
            root.Dispose();
            builder.Dispose();
        }
        return unit;
    }
}
