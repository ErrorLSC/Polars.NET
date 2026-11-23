using System.Globalization;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Text;
using Kaminari.Seseragi.Files.Csv;

namespace Seseragi.Files.Tests.Csv;

/// <summary>
/// Gate G3 of the managed CSV reader's step f1: random delimited files (quotes, doubled quotes, line ends of every
/// kind, blank lines, ragged rows, every inferred kind, nulls; some with an error planted, some with a given schema whose
/// temporal columns start late and mix formats, some without quoting or with a custom line end) read in units of a few
/// bytes from every kind of source give what the whole input read as one unit gives: the same values, or the same first
/// error (type and text). CSV_UNITS_CASES sets the number of files (the gate runs 2000).
/// </summary>
public class CsvUnitInvarianceTests
{
    private static readonly int Count = int.TryParse(Environment.GetEnvironmentVariable("CSV_UNITS_CASES"), out int n) ? n : 150;

    public static TheoryData<int> Seeds()
    {
        var data = new TheoryData<int>();
        for (int i = 0; i < Count; i++) data.Add(20261123 + i);
        return data;
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public void SameInAnyUnits(int seed)
    {
        var random = new Random(seed);
        (byte[] bytes, CsvReadOptions options) = Generate(random);
        string whole = Outcome(() => CsvBehaviourTests.Read(bytes, options with { MaxDegreeOfParallelism = 1 }, CsvBehaviourTests.CsvSource.Memory));
        int[] units = [1, 2, 3, 7, 64, 4096];
        foreach (int unit in units)
            foreach (CsvBehaviourTests.CsvSource source in Enum.GetValues<CsvBehaviourTests.CsvSource>())
            {
                if (source == CsvBehaviourTests.CsvSource.PathUnmapped && unit < 64) continue; // same path as Seekable, slower
                string got = Outcome(() => CsvBehaviourTests.Read(bytes, options with { UnitBytes = unit, MaxDegreeOfParallelism = 8, BatchSize = 1 + random.Next(50) }, source));
                if (got != whole)
                    Assert.Fail($"seed {seed}, unit {unit}, {source}:\nwhole: {Cut(whole)}\ngot:   {Cut(got)}\ninput: {Cut(Encoding.UTF8.GetString(bytes).Replace("\r", "\\r").Replace("\n", "\\n"))}");
            }

        static string Cut(string s) => s.Length > 3000 ? s[..3000] + "…" : s;
    }

    private static readonly int[] Pages = [65001, 1252, 28591, 932, 936, 949, 950, 54936, 37, 1200, 1201];

    /// <summary>
    /// Gate G3 of step f2: the generator's files in a random encoding (non-ASCII characters dropped when the page lacks
    /// them, else UTF-16), strict or lossy, a tenth with an invalid unit planted (double-byte pages, UTF-16): any units and
    /// sources give what one unit gives; without an invalid unit, what the same text in UTF-8 gives.
    /// </summary>
    [Theory]
    [MemberData(nameof(Seeds))]
    public void EncodedSameInAnyUnits(int seed)
    {
        var random = new Random(seed * 7 + 1);
        (byte[] utf8, CsvReadOptions options) = Generate(random);
        if (Encode(random, utf8, options) is not var (bytes, page, text, planted, encodedOptions)) return; // an f1 planted error: UTF-8 cases cover it
        options = encodedOptions;
        string whole = Outcome(() => CsvBehaviourTests.Read(bytes, options with { MaxDegreeOfParallelism = 1 }, CsvBehaviourTests.CsvSource.Memory));
        foreach (int unit in (int[])[1, 3, 7, 64, 4096])
            foreach (CsvBehaviourTests.CsvSource source in Enum.GetValues<CsvBehaviourTests.CsvSource>())
            {
                if (source == CsvBehaviourTests.CsvSource.PathUnmapped && unit < 64) continue;
                string got = Outcome(() => CsvBehaviourTests.Read(bytes, options with { UnitBytes = unit, MaxDegreeOfParallelism = 8 }, source));
                if (got != whole) Assert.Fail($"seed {seed}, page {page}, unit {unit}, {source}:\nwhole: {Cut(whole)}\ngot:   {Cut(got)}");
            }
        if (!planted)
        {
            string asUtf8 = Outcome(() => CsvBehaviourTests.Read(Encoding.UTF8.GetBytes(text), options with { Encoding = null, InvalidText = CsvInvalidText.Error, ForceTranscode = false }, CsvBehaviourTests.CsvSource.Memory));
            if (asUtf8 != whole) Assert.Fail($"seed {seed}, page {page}: in UTF-8\n{Cut(asUtf8)}\nin the page\n{Cut(whole)}");
        }

        static string Cut(string s) => s.Length > 2000 ? s[..2000] + "…" : s;
    }

    /// <summary>
    /// A generated file in a random encoding (non-ASCII characters dropped when the page lacks them, else UTF-16), strict
    /// or lossy, a tenth with an invalid unit planted (double-byte pages, UTF-16); null for a file holding invalid UTF-8.
    /// </summary>
    internal static (byte[] Bytes, int Page, string Text, bool Planted, CsvReadOptions Options)? Encode(Random random, byte[] utf8, CsvReadOptions options)
    {
        if (!System.Text.Unicode.Utf8.IsValid(utf8)) return null;
        string text = Encoding.UTF8.GetString(utf8);
        int page = Pages[random.Next(Pages.Length)];
        Encoding encoding = StrictEncoding(page);
        if (!Encodable(encoding, text))
        {
            string ascii = new([.. text.Select(c => c < 0x80 ? c : 'q')]);
            if (random.Next(2) == 0 && Encodable(encoding, ascii)) text = ascii;
            else (page, encoding) = (1200, StrictEncoding(1200));
        }
        byte[] bytes = encoding.GetBytes(text);
        bool marked = page is 1200 or 1201 && random.Next(2) == 0;
        if (marked) bytes = [.. encoding.GetPreamble(), .. bytes];
        bool planted = false;
        if (random.Next(10) == 0 && page is 932 or 936 or 949 or 950 or 54936 or 1200 or 1201)
        {
            // An invalid unit at a character boundary: a lead byte with no trail, a lone surrogate.
            int at = encoding.GetByteCount(text[..random.Next(text.Length + 1)].TrimEnd('\uD800', '\uDBFF')) + (marked ? 2 : 0);
            byte[] invalid = page switch { 1200 => [0x00, 0xD8], 1201 => [0xD8, 0x00], _ => [0x81, 0x20] };
            if (page is 1200 or 1201 && at % 2 == 1) at--;
            bytes = [.. bytes[..at], .. invalid, .. bytes[at..]];
            planted = true;
        }
        options = options with
        {
            Encoding = marked && random.Next(2) == 0 ? null : TextEncodings.Resolve(page),
            InvalidText = random.Next(3) == 0 ? CsvInvalidText.Replace : CsvInvalidText.Error,
            ForceTranscode = random.Next(4) == 0,
        };
        return (bytes, page, text, planted, options);
    }

    private static Encoding StrictEncoding(int page)
    {
        var encoding = (Encoding)TextEncodings.Resolve(page).Clone();
        encoding.EncoderFallback = EncoderFallback.ExceptionFallback;
        return encoding;
    }

    private static bool Encodable(Encoding encoding, string text)
    {
        try
        {
            encoding.GetByteCount(text);
            return true;
        }
        catch (EncoderFallbackException) { return false; }
    }

    /// <summary>The generator reaches every path: values, input errors, value errors, given schemas.</summary>
    [Fact]
    public void GeneratorCoversThePaths()
    {
        int ok = 0, format = 0, value = 0, given = 0;
        for (int i = 0; i < 400; i++)
        {
            (byte[] bytes, CsvReadOptions options) = Generate(new Random(20261123 + i));
            if (options.Schema is not null) given++;
            string outcome = Outcome(() => CsvBehaviourTests.Read(bytes, options with { UnitBytes = 3, MaxDegreeOfParallelism = 16 }, CsvBehaviourTests.CsvSource.Memory));
            if (outcome.StartsWith(nameof(CsvFormatException), StringComparison.Ordinal)) format++;
            else if (outcome.StartsWith(nameof(CsvValueException), StringComparison.Ordinal)) value++;
            else ok++;
        }
        Assert.True(ok >= 200 && format >= 10 && value >= 10 && given >= 40, $"ok {ok}, format {format}, value {value}, given {given}");
    }

    /// <summary>
    /// A given date column whose first value comes after the rows read at open: its format is the first value's in file
    /// order, whatever unit read it first (units that read another format are decoded again).
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void GivenTemporalFormatIsTheFirstValuesInFileOrder(bool ignoreErrors)
    {
        var sb = new StringBuilder("x,d\n");
        for (int i = 0; i < 150; i++) sb.Append("x,\n");
        sb.Append("x,2024-01-02\nx,03-01-2024\nx,2024-01-05\n");
        byte[] bytes = Encoding.UTF8.GetBytes(sb.ToString());
        var options = new CsvReadOptions
        {
            Schema = new Schema([new Field("x", StringType.Default, true), new Field("d", Date32Type.Default, true)], null),
            IgnoreErrors = ignoreErrors,
        };
        string whole = Outcome(() => CsvBehaviourTests.Read(bytes, options, CsvBehaviourTests.CsvSource.Memory));
        Assert.Contains(ignoreErrors ? "\"x\",2024-01-02 | \"x\",null | \"x\",2024-01-05" : "Row 152 (line 153): column 'd': '03-01-2024' is not a date in the column's format (year-month-day)", whole);
        // Whether a unit decodes before or after the file's format is known depends on timing: read until one did.
        long before = Interlocked.Read(ref CsvReader.Redecoded);
        for (int rep = 0; rep < 50 && Interlocked.Read(ref CsvReader.Redecoded) == before; rep++)
            for (int unit = 1; unit < 12; unit++)
                Assert.Equal(whole, Outcome(() => CsvBehaviourTests.Read(bytes, options with { UnitBytes = unit, MaxDegreeOfParallelism = 8 }, CsvBehaviourTests.CsvSource.Memory)));
        Assert.True(Interlocked.Read(ref CsvReader.Redecoded) > before, "no unit was decoded again");
    }

    internal static string Outcome(Func<string> read)
    {
        try { return read(); }
        catch (Exception e) when (e is CsvFormatException or CsvValueException) { return $"{e.GetType().Name}: {e.Message}"; }
    }

    private enum Kind { Int, Float, Bool, DateYmd, DateDmy, Datetime, Time, Text, Empty }

    /// <summary>A random file and the options to read it with.</summary>
    internal static (byte[] Bytes, CsvReadOptions Options) Generate(Random random)
    {
        bool quoting = random.Next(5) != 0;
        bool customEnd = random.Next(5) == 0;
        char separator = random.Next(4) switch { 0 => ';', 1 => '\t', 2 => '|', _ => ',' };
        if (customEnd && separator == ';') separator = ',';
        string[] ends = customEnd ? [";"] : random.Next(3) switch { 0 => ["\n"], 1 => ["\r\n"], _ => ["\n", "\r\n", "\r"] };
        int columns = random.Next(8) == 0 ? 1 + random.Next(20) : 1 + random.Next(6);
        int rows = random.Next(10) == 0 ? random.Next(2000) : random.Next(120);
        bool given = random.Next(5) == 0;
        var kinds = new Kind[columns];
        for (int c = 0; c < columns; c++) kinds[c] = (Kind)random.Next(Enum.GetValues<Kind>().Length);
        var sb = new StringBuilder();
        bool header = random.Next(6) != 0;
        string End() => ends[random.Next(ends.Length)];
        if (header) sb.Append(string.Join(separator, Enumerable.Range(0, columns).Select(c => $"c{c}"))).Append(End());
        // Given schemas: temporal columns stay null for a while, then mix formats (the first value decides).
        // Half of them past the rows the reader takes formats from at open (100), so units decide and are checked.
        if (given && random.Next(2) == 0) rows = 150 + random.Next(450);
        int late = given ? (rows > 150 ? 101 + random.Next(rows - 140) : random.Next(rows + 1)) : 0;
        int errorRow = random.Next(10) == 0 ? random.Next(Math.Max(1, rows)) : -1;
        for (int r = 0; r < rows; r++)
        {
            if (random.Next(40) == 0)
            {
                sb.Append(End()); // a blank line: a null row
                continue;
            }
            int fields = random.Next(30) == 0 ? Math.Max(1, columns - 1 - random.Next(columns)) : columns;
            var values = new string[fields];
            for (int c = 0; c < fields; c++)
            {
                bool temporal = kinds[c] is Kind.DateYmd or Kind.DateDmy or Kind.Datetime;
                values[c] = random.Next(8) == 0 || (given && temporal && r < late) ? "" : random.Next(60) == 0 ? "NA" : Value(random, kinds[c], quoting, separator, given && r > late);
            }
            string line = string.Join(separator, values);
            if (r == errorRow)
                line = random.Next(4) switch
                {
                    0 => line + separator + "extra" + separator + "more",
                    1 when quoting => line + separator + "\"open",
                    2 => "\uFFFF" + line, // replaced below by a byte that is not UTF-8
                    _ => "x" + line,
                };
            sb.Append(line);
            if (r < rows - 1 || random.Next(2) == 0) sb.Append(End());
        }
        byte[] bytes = Encoding.UTF8.GetBytes(sb.ToString());
        // U+FFFF (EF BF BF) planted as an error: its first byte made invalid.
        for (int i = 0; i + 2 < bytes.Length; i++)
            if (bytes[i] == 0xEF && bytes[i + 1] == 0xBF && bytes[i + 2] == 0xBF) bytes[i] = 0xFF;
        var options = new CsvReadOptions
        {
            Separator = separator,
            Quote = quoting ? '"' : null,
            LineEnd = customEnd ? ';' : null,
            HasHeader = header,
            InferSchemaLength = random.Next(4) == 0 ? null : 1 + random.Next(100),
            IgnoreErrors = random.Next(6) == 0,
            ViewTypes = random.Next(2) == 0,
            NullValues = random.Next(3) == 0 ? ["NA"] : null,
            MissingIsNull = random.Next(5) != 0,
            MaxRows = random.Next(4) == 0 ? random.Next(rows + 2) : null,
            RowIndexColumn = random.Next(4) == 0 ? "idx" : null,
            RowIndexOffset = random.Next(3) == 0 ? random.Next(1000) : 0,
        };
        if (given)
        {
            Field[] fields = [.. kinds.Select((k, c) => new Field(header ? $"c{c}" : $"column_{c + 1}", k switch
            {
                Kind.Int => Int64Type.Default,
                Kind.Float => DoubleType.Default,
                Kind.Bool => BooleanType.Default,
                Kind.DateYmd or Kind.DateDmy => Date32Type.Default,
                Kind.Datetime => new TimestampType(TimeUnit.Microsecond, (string?)null),
                Kind.Time => new Time64Type(TimeUnit.Nanosecond),
                _ => (IArrowType)StringType.Default,
            }, true))];
            options = options with { Schema = new Schema(fields, null) };
        }
        return (bytes, options);
    }

    private static string Value(Random random, Kind kind, bool quoting, char separator, bool otherFormat)
    {
        string text = kind switch
        {
            Kind.Int => random.Next(-100000, 100000).ToString(CultureInfo.InvariantCulture),
            Kind.Float => (random.NextDouble() * 1000 - 500).ToString("R", CultureInfo.InvariantCulture),
            Kind.Bool => random.Next(2) == 0 ? "true" : "False",
            Kind.DateYmd => otherFormat && random.Next(3) == 0 ? $"{random.Next(1, 28):D2}-{random.Next(1, 13):D2}-2024" : $"2024-{random.Next(1, 13):D2}-{random.Next(1, 28):D2}",
            Kind.DateDmy => otherFormat && random.Next(3) == 0 ? $"2023/{random.Next(1, 13):D2}/{random.Next(1, 28):D2}" : $"{random.Next(1, 28):D2}.{random.Next(1, 13):D2}.2023",
            Kind.Datetime => $"2024-01-{random.Next(1, 28):D2}{(otherFormat && random.Next(3) == 0 ? ' ' : 'T')}{random.Next(24):D2}:{random.Next(60):D2}:{random.Next(60):D2}",
            Kind.Time => random.Next(3) == 0 ? $"{random.Next(24):D2}:{random.Next(60):D2}" : $"{random.Next(24):D2}:{random.Next(60):D2}:{random.Next(60):D2}.{random.Next(1000):D3}",
            Kind.Text => Text(random, quoting),
            _ => "",
        };
        if (!quoting) return text.Replace(separator.ToString(), "").Replace("\n", "").Replace("\r", "").Replace(";", "");
        bool needs = text.IndexOfAny([separator, '"', '\n', '\r', ';']) >= 0 || random.Next(10) == 0;
        return needs ? "\"" + text.Replace("\"", "\"\"") + "\"" : text;
    }

    private static string Text(Random random, bool quoting)
    {
        const string alphabet = "abcxyz é漢字,;|\t\"\n\r  0123";
        int length = random.Next(12);
        var sb = new StringBuilder();
        for (int i = 0; i < length; i++)
        {
            char ch = alphabet[random.Next(alphabet.Length)];
            if (!quoting && ch == '"' && random.Next(2) == 0) continue;
            sb.Append(ch);
        }
        return sb.ToString();
    }
}
