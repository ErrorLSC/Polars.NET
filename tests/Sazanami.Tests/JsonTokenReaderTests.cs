using System.Text;
using System.Text.Json;
using System.Text.Unicode;
using Kaminari.Sazanami.Json;

namespace Sazanami.Tests;

/// <summary>
/// <see cref="JsonTokenReader"/> against <see cref="Utf8JsonReader"/> as the oracle: the same tokens (kind, position,
/// length, escaped, depth), values (integers, doubles, unescaped strings), skips and refusals, on hand-made cases,
/// deep nesting and random records (valid and mutated); newline-delimited records read line by line by the oracle as
/// the JSON reader read them (blank lines passed over, a line that is not UTF-8 refused).
/// </summary>
public class JsonTokenReaderTests
{
    private static readonly JsonReaderOptions Options = new() { MaxDepth = 512 };

    private static readonly string[] Cases =
    [
        """{"a":1}""", """{"a":1,}""", """{"a":[1,]}""", """{"a":01}""", """{"a":-}""", """{"a":1.}""", """{"a":.5}""", """{"a":1e}""",
        """{"a":1e+}""", """{"a":1E-7}""", """{"a":-0.0e10}""", """{"a":tru}""", """{"a":truex}""", """{"a":nul}""", """{"a":NaN}""",
        """{"a":"x\q"}""", """{"a":"x\u12G4"}""", """{"a":"x\u12"}""", """{"a":"\uD800"}""", """{"a":"\uDC00x"}""", """{"a":"\uD83D\uDE00"}""",
        """{"a":"\uD83D\u0041"}""", """{"a":"\uD83Dx"}""", """{"a":"x""", """{"a" 1}""", """{"a":1 "b":2}""", """{"a":1}{"b":2}""", """{"a":1} x""",
        """{"a":1}  """, """ {"a":1}""", """{a:1}""", """{"a":1}}""", """{"a":[1}""", """{"a":{]}""", """{"a":/*c*/1}""", """{"a":1}//c""",
        "{\"a\":\"tab\tin\"}", "{\"a\":\"\u0001\"}", "{\"a\":1\u0001}", "{\"a\":\"\u00e9\"}", """{"a":[[[[]]]]}""", """{"a":"\\\""}""",
        """{"a":"\\\\"}""", """{"\"q":1}""", """{"a":[1 2]}""", """{"a":[1,,2]}""", """{"":0}""", """{"a":-01}""", """{"a":1.5.3}""",
        """{"a":1-2}""", """{"a":true,"b":false,"c":null,"d":[],"e":{}}""", """{"a":"\/\b\f\n\r\t"}""", "[", "]", "{", "}", "1", "-0",
        "\"s\"", "null", "[1,2]", "{\"a\":1}\r", "{\"a\":1,\r\"b\":2}", "{\"a\":\"\r\"}", """{"a":1:2}""", """{"a":"x"1}""", """{"a":1"x"}""",
        """{"a":18446744073709551615,"b":18446744073709551616,"c":-9223372036854775808,"d":-9223372036854775809,"e":1e400,"f":-1e-400}""",
        """{"a":12345678901234567890123,"b":0.1e1,"c":1E+2,"d":-0,"e":0e0,"f":2.2250738585072014e-308}""",
        """{"a":"0123456789abcdef\n0123456789abcdef\u00e9 long enough to cross vectors twice over"}""", """{"a" : 1 , "b" : [ 1 , 2 ] }""",
    ];

    [Fact]
    public void HandCasesMatchTheOracleAsLines()
    {
        foreach (string text in Cases)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(text);
            Assert.True(Oracle(bytes).SequenceEqual(Ours(bytes)), $"{text}\n oracle: {string.Join(" ", Oracle(bytes))}\n ours:   {string.Join(" ", Ours(bytes))}");
        }
    }

    [Fact]
    public void HandCasesMatchTheOracleAsValues()
    {
        foreach (string text in Cases)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(text);
            Assert.True(OracleValue(bytes).SequenceEqual(OursValue(bytes)), $"{text}\n oracle: {string.Join(" ", OracleValue(bytes))}\n ours:   {string.Join(" ", OursValue(bytes))}");
        }
    }

    [Fact]
    public void BytesThatAreNotUtf8()
    {
        byte[][] units =
        [
            [.. "{\"a\":\""u8, 0xC3, 0x28, .. "\"}"u8], [.. "{\"a\":\""u8, 0xED, 0xA0, 0x80, .. "\"}"u8], [.. "{\"a\":1"u8, 0xC3, 0xA9, .. "}"u8],
            [.. "{\"\\u00e9\":\""u8, 0xE2, 0x82, 0xAC, 0xF0, 0x9F, 0x98, 0x80, .. "\"}\n"u8], [.. "{\"a\":\"0123456789abcdef"u8, 0xC3, .. "\"}"u8],
            [.. "{\"a\":\"0123456789abcdefghijklmn"u8, 0xF0, 0x9F, 0x98, .. "\"}"u8], [.. "{\"a\":\"é"u8, 0xFF, .. "0123456789abcdef\"}"u8],
            [.. "{\"a\":1}\n"u8, 0xFE, .. "\n{\"a\":2}"u8], [.. "{\""u8, 0xC0, 0x80, .. "\":1}"u8], [.. "{\"a\":\""u8, 0xF4, 0x90, 0x80, 0x80, .. "\"}"u8],
        ];
        foreach (byte[] unit in units) Assert.Equal(Oracle(unit), Ours(unit));
    }

    [Fact]
    public void LinesFrameRecords()
    {
        string[] units =
        [
            "{\"a\":1}\n{\"a\":2}\n", "{\"a\":1}\r\n{\"a\":2}\r\n", "\n\n  \n{\"a\":1}\n \t\n{\"a\":2}", "{\"a\":\n1}\n", "{\"a\":1}\n{\"a\"",
            "{\"a\":1} \n {\"b\":2}", "{\"a\":\"x\ny\"}", "[1]\n2\n\"s\"\n", "{}\n{}\n", "", "\n", "   ", "\n\n\n", "{\"a\":1}\n   ", "{\"a\":[1,\n2]}",
            "{\"a\"\n:1}", "{\n\"a\":1}", "{\"a\":1\n}", "{\"a\":1}\n\u0001\n", "{\"a\":1}\n\n{\"b\":tru}\n{\"c\":3}",
        ];
        foreach (string text in units)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(text);
            Assert.True(Oracle(bytes).SequenceEqual(Ours(bytes)), $"{text}\n oracle: {string.Join(" ", Oracle(bytes))}\n ours:   {string.Join(" ", Ours(bytes))}");
        }
    }

    [Theory]
    [InlineData(1), InlineData(63), InlineData(64), InlineData(65), InlineData(127), InlineData(128), InlineData(129), InlineData(200)]
    [InlineData(510), InlineData(511), InlineData(512), InlineData(513), InlineData(600)]
    public void DeepNestingOfMixedKinds(int depth)
    {
        foreach (int seed in (int[])[1, 2, 3])
        {
            var random = new Random(seed * 1000 + depth);
            var text = new StringBuilder();
            var kinds = new Stack<bool>();
            for (int d = 0; d < depth; d++)
            {
                bool isObject = d == 0 || random.Next(2) == 0;
                if (kinds.Count > 0 && kinds.Peek()) text.Append($"\"k{d}\":");
                text.Append(isObject ? '{' : '[');
                kinds.Push(isObject);
            }
            while (kinds.Count > 0)
            {
                bool isObject = kinds.Pop();
                // A value next to the closing one, so that the kind of every level after a pop matters.
                if (kinds.Count > 0) text.Append(isObject ? "}," : "],").Append(kinds.Peek() ? "\"z\":7" : "7");
                else text.Append(isObject ? '}' : ']');
            }
            byte[] bytes = Encoding.UTF8.GetBytes(text.ToString());
            Assert.Equal(Oracle(bytes), Ours(bytes));
            Assert.Equal(OracleValue(bytes), OursValue(bytes));
        }
    }

    [Fact]
    public void RandomRecordsMatchTheOracle()
    {
        var random = new Random(20261122);
        for (int n = 0; n < 20_000; n++)
        {
            byte[] unit = RandomUnit(random);
            if (random.Next(2) == 0) unit = Mutate(random, unit);
            List<string> oracle = Oracle(unit), ours = Ours(unit);
            if (!oracle.SequenceEqual(ours))
                Assert.Fail($"case {n}: {Convert.ToHexString(unit)}\n{Encoding.UTF8.GetString(unit)}\n oracle: {string.Join(" ", oracle)}\n ours:   {string.Join(" ", ours)}");
        }
    }

    [Fact]
    public void RandomValuesMatchTheOracle()
    {
        var random = new Random(20261123);
        for (int n = 0; n < 10_000; n++)
        {
            var text = new StringBuilder();
            RandomValue(random, text, 0, newlines: true);
            // What follows an element: a comma and the next one, or anything (left unread).
            text.Append(random.Next(3) switch { 0 => "", 1 => ",\n{\"next\":1}", _ => " ]x" });
            byte[] bytes = Encoding.UTF8.GetBytes(text.ToString());
            if (random.Next(2) == 0) bytes = Mutate(random, bytes);
            List<string> oracle = OracleValue(bytes), ours = OursValue(bytes);
            if (!oracle.SequenceEqual(ours))
                Assert.Fail($"case {n}: {Convert.ToHexString(bytes)}\n{Encoding.UTF8.GetString(bytes)}\n oracle: {string.Join(" ", oracle)}\n ours:   {string.Join(" ", ours)}");
        }
    }

    [Theory]
    [InlineData("""{"a":nul}""", 5, "'nul' is not a JSON literal (true, false or null).")]
    [InlineData("""{"a":01}""", 5, "Invalid number '01'.")]
    [InlineData("""{"a":"x\q"}""", 7, @"Invalid escape '\q' in a string.")]
    [InlineData("{\"a\":\"t\tb\"}", 7, "A control character (0x09) in a string must be escaped.")]
    [InlineData("""{"a":"x""", 5, "Unterminated string.")]
    [InlineData("""{"a" 1}""", 5, "Expected ':' after a property name, found '1'.")]
    [InlineData("""{"a":1 "b":2}""", 7, "Expected ',' or '}' after a property value, found '\"b\"'.")]
    [InlineData("""{"a":[1 2]}""", 8, "Expected ',' or ']' after an array element, found '2'.")]
    [InlineData("""{"a":1,}""", 7, "Expected a property name (a string), found '}'.")]
    [InlineData("""{"a":}""", 5, "Expected a value, found '}'.")]
    [InlineData("""{"a":1} x""", 8, "A line of newline-delimited JSON holds one object.")]
    [InlineData("{\"a\":\n1}", 5, "The value is not closed at its line's end.")]
    [InlineData("{\"a\":1", 6, "The value is not closed at its line's end.")]
    [InlineData("""{"a":"\uD800"}""", 6, @"The escape '\uD800' is half of a surrogate pair without its other half.")]
    public void FailuresSayWhatAndWhere(string text, int position, string message)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(text);
        var e = Assert.Throws<JsonSyntaxException>(() =>
        {
            var r = JsonTokenReader.Lines(bytes);
            while (r.NextRecord())
                while (r.Read())
                    if (r.TokenType == JsonTokenType.String) r.GetString();
        });
        Assert.Equal(message, e.Message);
        Assert.Equal(position, e.Position);
    }

    [Fact]
    public void LinesCountAsTheLineReaderCountsThem()
    {
        (string Text, int Lines)[] units = [("", 0), ("\n", 1), ("{}", 1), ("{}\n", 1), ("{}\n\n", 2), ("{}\n  ", 2), ("\n{}\n{}", 3), ("{}\r\n{}\r\n", 2)];
        foreach (var (text, lines) in units)
        {
            var reader = JsonTokenReader.Lines(Encoding.UTF8.GetBytes(text));
            while (reader.NextRecord())
                while (reader.Read()) { }
            Assert.Equal(lines, reader.Line);
        }
    }

    // The oracle's reading of a unit of lines as the JSON reader read them (a line that is not UTF-8 refused before it is
    // read): the tokens of the lines read, then their count; or, at the first failing line, its number (the tokens the
    // failing line gave before it failed are left out: a reader of records drops the whole unit then).
    private static List<string> Oracle(byte[] unit)
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

    private static List<string> Ours(byte[] unit)
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

    // The value at the start: its tokens, read until it ends (nothing after it is looked at), and where it ends.
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
        // A value that is not UTF-8 is refused (the readers check the whole input).
        if (!Utf8.IsValid(bytes.AsSpan(0, (int)reader.BytesConsumed))) return ["ERR"];
        tokens.Add($"end {reader.BytesConsumed}");
        return tokens;
    }

    private static List<string> OursValue(byte[] bytes)
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
                    int length = reader.CopyString(buffer);
                    text.Append(' ').Append(Convert.ToHexString(buffer, 0, length));
                    // (the oracle's text after its CopyString, so a refusal reads the same)
                    Assert.Equal(Encoding.UTF8.GetString(buffer, 0, length), reader.GetString());
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

    // A unit of 1 to 6 records (mostly objects), blank lines between some, CRLF or LF, a last newline or not.
    private static byte[] RandomUnit(Random random)
    {
        var text = new StringBuilder();
        int records = random.Next(1, 7);
        for (int r = 0; r < records; r++)
        {
            if (random.Next(5) == 0) text.Append(random.Next(4) switch { 0 => "", 1 => "  ", 2 => "\r", _ => " \t" }).Append('\n');
            if (random.Next(10) == 0) RandomValue(random, text, 1, newlines: false);
            else RandomObject(random, text, 0, newlines: false);
            if (random.Next(4) == 0) text.Append(random.Next(2) == 0 ? " " : "\r");
            if (r < records - 1 || random.Next(2) == 0) text.Append('\n');
        }
        return Encoding.UTF8.GetBytes(text.ToString());
    }

    private static void RandomValue(Random random, StringBuilder text, int depth, bool newlines)
    {
        int kind = depth > 6 ? random.Next(4) : random.Next(7);
        switch (kind)
        {
            case 0: RandomNumber(random, text); break;
            case 1: RandomString(random, text); break;
            case 2: text.Append(random.Next(3) switch { 0 => "true", 1 => "false", _ => "null" }); break;
            case 3: RandomNumber(random, text); break;
            case 4: RandomString(random, text); break;
            case 5: RandomObject(random, text, depth + 1, newlines); break;
            default:
                text.Append('[');
                int items = random.Next(4);
                for (int i = 0; i < items; i++)
                {
                    if (i > 0) text.Append(',');
                    White(random, text, newlines);
                    RandomValue(random, text, depth + 1, newlines);
                    White(random, text, newlines);
                }
                text.Append(']');
                break;
        }
    }

    private static void RandomObject(Random random, StringBuilder text, int depth, bool newlines)
    {
        text.Append('{');
        int fields = random.Next(depth == 0 ? 1 : 0, 6);
        for (int f = 0; f < fields; f++)
        {
            if (f > 0) text.Append(',');
            White(random, text, newlines);
            RandomString(random, text);
            White(random, text, newlines);
            text.Append(':');
            White(random, text, newlines);
            RandomValue(random, text, depth + 1, newlines);
            White(random, text, newlines);
        }
        text.Append('}');
    }

    private static void White(Random random, StringBuilder text, bool newlines)
    {
        if (random.Next(4) != 0) return;
        text.Append(random.Next(newlines ? 5 : 4) switch { 0 => " ", 1 => "\t", 2 => "\r", 3 => "  ", _ => "\n " });
    }

    private static void RandomNumber(Random random, StringBuilder text)
    {
        if (random.Next(8) == 0)
        {
            string[] edges =
            [
                "0", "-0", "18446744073709551615", "18446744073709551616", "9223372036854775807", "9223372036854775808", "-9223372036854775808",
                "-9223372036854775809", "1e400", "-1e400", "1e-400", "2.2250738585072014e-308", "1.7976931348623157e308", "0.1", "1E+2", "0e0",
                "123456789012345678901234567890", "4.35", "9007199254740993", "1.0", "-1.5e-3",
            ];
            text.Append(edges[random.Next(edges.Length)]);
            return;
        }
        if (random.Next(3) == 0) text.Append('-');
        int digits = random.Next(1, random.Next(3) == 0 ? 25 : 8);
        text.Append(digits == 1 ? (char)('0' + random.Next(10)) : (char)('1' + random.Next(9)));
        for (int d = 1; d < digits; d++) text.Append((char)('0' + random.Next(10)));
        if (random.Next(3) == 0)
        {
            text.Append('.');
            for (int d = random.Next(1, 12); d > 0; d--) text.Append((char)('0' + random.Next(10)));
        }
        if (random.Next(4) == 0)
        {
            text.Append(random.Next(2) == 0 ? 'e' : 'E');
            if (random.Next(2) == 0) text.Append(random.Next(2) == 0 ? '+' : '-');
            text.Append(random.Next(400));
        }
    }

    private static void RandomString(Random random, StringBuilder text)
    {
        text.Append('"');
        int pieces = random.Next(random.Next(4) == 0 ? 20 : 4);
        for (int p = 0; p < pieces; p++)
        {
            text.Append(random.Next(12) switch
            {
                0 => "\\\"", 1 => "\\\\", 2 => "\\/", 3 => @"\b\f\n\r\t",
                4 => $"\\u{random.Next(0x10000):x4}",           // any code unit, lone surrogates included
                5 => "\\uD83D\\uDE00",
                6 => "é中😀",
                7 => "0123456789abcdefghij",
                8 => "\u00ff\u0800\uffff",
                _ => ((char)random.Next('a', 'z' + 1)).ToString(),
            });
        }
        text.Append('"');
    }

    // One to three byte flips, insertions or deletions (structural bytes, quotes, escapes, controls, non-ASCII).
    private static byte[] Mutate(Random random, byte[] bytes)
    {
        ReadOnlySpan<byte> choices = "{}[]:,\"\\\n 0-.etnx\u0001"u8;
        byte[] extra = [0x80, 0xC3, 0xFF, 0xED, 0xF0];
        List<byte> list = [.. bytes];
        for (int m = random.Next(1, 4); m > 0 && list.Count > 0; m--)
        {
            byte b = random.Next(4) == 0 ? extra[random.Next(extra.Length)] : choices[random.Next(choices.Length)];
            int at = random.Next(list.Count);
            switch (random.Next(3))
            {
                case 0: list[at] = b; break;
                case 1: list.Insert(at, b); break;
                default: list.RemoveAt(at); break;
            }
        }
        return [.. list];
    }
}
