using System.Text;
using System.Text.Json;
using System.Text.Unicode;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Json;

namespace Seseragi.Files.Fuzz;

/// <summary>
/// Sazanami's <see cref="JsonTokenReader"/> against <see cref="Utf8JsonReader"/> on a (mutated) input held in guarded
/// memory: newline-delimited records read line by line by the oracle (blank lines passed over, a line that is not UTF-8
/// refused), a document as the one value at its start. The tokens (kind, position, length, escaped, depth), numbers,
/// unescaped strings, skips and the refusal (its line) must agree; the tokens a failing line gave before it failed are
/// not compared (a reader of records drops the unit then).
/// </summary>
internal static class JsonTokenDiff
{
    private static readonly JsonReaderOptions Options = new() { MaxDepth = 512 };

    public static Outcome Run(byte[] bytes, bool lines)
    {
        using var block = GuardedMemory.Block.Copy(bytes);
        ReadOnlySpan<byte> input = block.GetSpan();
        List<string> oracle = lines ? OracleLines(bytes) : OracleValue(bytes);
        List<string> ours = lines ? OursLines(input) : OursValue(input);
        // (A case that is no finding has a one-word signature: the parent splits its line at spaces.)
        if (oracle.SequenceEqual(ours)) return oracle[^1].StartsWith("ERR", StringComparison.Ordinal) ? new Outcome(false, "refused", "tokens") : Outcome.Read;
        int at = oracle.Zip(ours).TakeWhile(p => p.First == p.Second).Count();
        return new Outcome(true, "different tokens", $"{(lines ? "lines" : "value")}, token {at}: oracle {oracle.ElementAtOrDefault(at)} | ours {ours.ElementAtOrDefault(at)}");
    }

    private static List<string> OracleLines(byte[] unit)
    {
        List<string> tokens = [];
        int position = 0, line = 0;
        while (position < unit.Length)
        {
            int nl = unit.AsSpan(position).IndexOf((byte)'\n');
            int end = nl < 0 ? unit.Length : position + nl;
            line++;
            byte[] text = unit[position..end];
            List<string> read = [];
            try
            {
                if (!Utf8.IsValid(text)) throw new InvalidDataException();
                if (text.AsSpan().IndexOfAnyExcept(" \t\r"u8) >= 0)
                {
                    var reader = new Utf8JsonReader(text, Options);
                    while (reader.Read()) read.Add(Describe(ref reader, position));
                }
            }
            catch (Exception e) when (e is JsonException or InvalidDataException)
            {
                tokens.Add($"ERR line {line}");
                return tokens;
            }
            tokens.AddRange(read);
            position = nl < 0 ? unit.Length : end + 1;
        }
        tokens.Add($"lines {line}");
        return tokens;
    }

    private static List<string> OursLines(ReadOnlySpan<byte> unit)
    {
        List<string> tokens = [];
        var reader = JsonTokenReader.Lines(unit);
        int recordStart = 0;
        try
        {
            while (reader.NextRecord())
            {
                recordStart = tokens.Count;
                while (reader.Read()) tokens.Add(Describe(ref reader));
            }
        }
        catch (JsonSyntaxException)
        {
            tokens.RemoveRange(recordStart, tokens.Count - recordStart);
            tokens.Add($"ERR line {reader.Line}");
            return tokens;
        }
        tokens.Add($"lines {reader.Line}");
        return tokens;
    }

    private static List<string> OracleValue(byte[] bytes)
    {
        List<string> tokens = [];
        var reader = new Utf8JsonReader(bytes, Options);
        try
        {
            while (reader.Read())
            {
                tokens.Add(Describe(ref reader, 0));
                if (reader.CurrentDepth == 0 && reader.TokenType is not (JsonTokenType.StartObject or JsonTokenType.StartArray)) break;
            }
        }
        catch (JsonException)
        {
            return ["ERR"];
        }
        if (!Utf8.IsValid(bytes.AsSpan(0, (int)reader.BytesConsumed))) return ["ERR"];
        tokens.Add($"end {reader.BytesConsumed}");
        return tokens;
    }

    private static List<string> OursValue(ReadOnlySpan<byte> bytes)
    {
        List<string> tokens = [];
        var reader = JsonTokenReader.Value(bytes);
        try
        {
            while (reader.Read()) tokens.Add(Describe(ref reader));
        }
        catch (JsonSyntaxException)
        {
            return ["ERR"];
        }
        tokens.Add($"end {reader.BytesConsumed}");
        return tokens;
    }

    private static string Describe(ref Utf8JsonReader reader, int offset)
    {
        var text = new StringBuilder($"{reader.TokenType}@{offset + reader.TokenStartIndex}+{reader.ValueSpan.Length}{(reader.ValueIsEscaped ? "e" : "")}/{reader.CurrentDepth}");
        switch (reader.TokenType)
        {
            case JsonTokenType.Number:
                text.Append(reader.TryGetInt64(out long l) ? $" l{l}" : " l-").Append(reader.TryGetUInt64(out ulong u) ? $" u{u}" : " u-")
                    .Append(reader.TryGetDouble(out double d) ? $" d{BitConverter.DoubleToInt64Bits(d)}" : " d-");
                break;
            case JsonTokenType.String or JsonTokenType.PropertyName:
                try
                {
                    byte[] buffer = new byte[reader.ValueSpan.Length];
                    string hex = Convert.ToHexString(buffer, 0, reader.CopyString(buffer));
                    text.Append(' ').Append(hex);
                }
                catch (InvalidOperationException)
                {
                    text.Append(" !");
                }
                break;
        }
        if (reader.TokenType is JsonTokenType.StartObject or JsonTokenType.StartArray or JsonTokenType.PropertyName)
        {
            Utf8JsonReader copy = reader;
            try
            {
                copy.Skip();
                text.Append($" skip {offset + copy.BytesConsumed} {copy.TokenType}");
            }
            catch (JsonException)
            {
                text.Append(" skip ERR");
            }
        }
        return text.ToString();
    }

    private static string Describe(ref JsonTokenReader reader)
    {
        var text = new StringBuilder($"{reader.TokenType}@{reader.TokenStartIndex}+{reader.ValueSpan.Length}{(reader.ValueIsEscaped ? "e" : "")}/{reader.CurrentDepth}");
        switch (reader.TokenType)
        {
            case JsonTokenType.Number:
                text.Append(reader.TryGetInt64(out long l) ? $" l{l}" : " l-").Append(reader.TryGetUInt64(out ulong u) ? $" u{u}" : " u-")
                    .Append(reader.TryGetDouble(out double d) ? $" d{BitConverter.DoubleToInt64Bits(d)}" : " d-");
                break;
            case JsonTokenType.String or JsonTokenType.PropertyName:
                try
                {
                    byte[] buffer = new byte[reader.ValueSpan.Length];
                    string hex = Convert.ToHexString(buffer, 0, reader.CopyString(buffer));
                    text.Append(' ').Append(hex);
                }
                catch (JsonSyntaxException)
                {
                    text.Append(" !");
                }
                break;
        }
        if (reader.TokenType is JsonTokenType.StartObject or JsonTokenType.StartArray or JsonTokenType.PropertyName)
        {
            JsonTokenReader copy = reader;
            try
            {
                copy.Skip();
                text.Append($" skip {copy.BytesConsumed} {copy.TokenType}");
            }
            catch (JsonSyntaxException)
            {
                text.Append(" skip ERR");
            }
        }
        return text.ToString();
    }
}
