using System.Text;
using System.Text.RegularExpressions;
using Kaminari.Seseragi.Files.Csv;
using static Seseragi.Files.Tests.Csv.CsvBehaviourTests;

namespace Seseragi.Files.Tests.Csv;

/// <summary>
/// Gate G3 of the managed CSV reader's step f4: the G3 generator's files decorated with comment lines between records
/// (odd quotes, separators, invalid bytes in them), junk lines for SkipLines (splitting quotes) or junk records for
/// SkipRows, junk records after the header for SkipRowsAfterHeader (refused values, grammar-breaking quotes, invalid
/// bytes, blank lines), and TruncateRaggedLines at random. (1) The decorated file reads as the plain one does (values,
/// row numbers, row index, errors; line numbers aside). (2) It reads alike in any unit size, from every kind of source,
/// on one thread or eight. CSV_SKIP_CASES sets the number of files (the gate runs 2000).
/// </summary>
public partial class CsvSkipTests
{
    private static readonly int Count = int.TryParse(Environment.GetEnvironmentVariable("CSV_SKIP_CASES"), out int n) ? n : 150;

    public static TheoryData<int> Seeds()
    {
        var data = new TheoryData<int>();
        for (int i = 0; i < Count; i++) data.Add(20261125 + i);
        return data;
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public void Decorated_ReadsAsPlain_InAnyUnits(int seed)
    {
        (byte[] plain, CsvReadOptions plainOptions, byte[] decorated, CsvReadOptions options, string what) = Make(seed);
        string expected = Mask(Outcome(() => CsvBehaviourTests.Read(plain, plainOptions with { MaxDegreeOfParallelism = 1 }, CsvSource.Memory)));
        string whole = Outcome(() => CsvBehaviourTests.Read(decorated, options with { MaxDegreeOfParallelism = 1 }, CsvSource.Memory));
        if (expected.Contains("holds no records", StringComparison.Ordinal))
            Assert.True(whole.Contains("no records", StringComparison.Ordinal) || whole.Contains("passes over the whole input", StringComparison.Ordinal),
                $"seed {seed} ({what}): plain is empty, decorated gave {Cut(whole)}");
        else if (Mask(whole) != expected)
            Assert.Fail($"seed {seed} ({what}):\nplain:     {Cut(expected)}\ndecorated: {Cut(Mask(whole))}\ninput: {Cut(Show(decorated))}");
        var random = new Random(seed ^ 0x5A5A);
        foreach (int unit in (int[])[1, 3, 7, 64, 4096])
            foreach (CsvSource source in Enum.GetValues<CsvSource>())
            {
                if (source == CsvSource.PathUnmapped && unit < 64) continue;
                foreach (int threads in (int[])[1, 8])
                {
                    if (threads == 1 && unit is not (3 or 4096)) continue;
                    string got = Outcome(() => CsvBehaviourTests.Read(decorated, options with { UnitBytes = unit, MaxDegreeOfParallelism = threads, BatchSize = 1 + random.Next(50) }, source));
                    if (got != whole) Assert.Fail($"seed {seed} ({what}), unit {unit}, {source}, t{threads}:\nwhole: {Cut(whole)}\ngot:   {Cut(got)}\ninput: {Cut(Show(decorated))}");
                }
            }

        static string Cut(string s) => s.Length > 2500 ? s[..2500] + "…" : s;
        static string Show(byte[] b) => Encoding.UTF8.GetString(b).Replace("\r", "\\r").Replace("\n", "\\n");
    }

    /// <summary>The decorations reach every path: comments, each skip, truncation, failures, and junk that would fail if read.</summary>
    [Fact]
    public void DecorationsCoverThePaths()
    {
        int comments = 0, lines = 0, rows = 0, after = 0, truncate = 0, failures = 0;
        for (int i = 0; i < 300; i++)
        {
            (byte[] plain, CsvReadOptions plainOptions, _, CsvReadOptions options, _) = Make(20261125 + i);
            if (options.CommentPrefix is not null) comments++;
            if (options.SkipLines > 0) lines++;
            if (options.SkipRows > 0) rows++;
            if (options.SkipRowsAfterHeader > 0) after++;
            if (options.TruncateRaggedLines) truncate++;
            if (Outcome(() => CsvBehaviourTests.Read(plain, plainOptions)).StartsWith("Csv", StringComparison.Ordinal)) failures++;
        }
        Assert.True(comments >= 100 && lines >= 50 && rows >= 50 && after >= 80 && truncate >= 50 && failures >= 20,
            $"comments {comments}, lines {lines}, rows {rows}, after {after}, truncate {truncate}, failures {failures}");
    }

    /// <summary>
    /// SkipLines and comment lines on every input path: gzip (the lines read off the decompressed text), UTF-16 decoded
    /// whole in memory or as a stream, CP932 on its bytes (an ASCII prefix) or transcoded (another prefix).
    /// </summary>
    [Theory]
    [InlineData("utf-8", false)]
    [InlineData("gzip", false)]
    [InlineData("utf-16", false)]
    [InlineData("utf-16", true)]
    [InlineData("cp932", false)]
    [InlineData("cp932", true)]
    public void SkipsAndComments_OnEveryInputPath(string kind, bool other)
    {
        string prefix = other ? "※" : "#";
        string text = "junk \"open\nmore, junk\n" + "名前,値\n" + prefix + " a comment \"\n" + "あ,1\n" + prefix + "注, \"x\nい,2\n";
        Encoding encoding = kind switch
        {
            "utf-16" => Encoding.Unicode,
            "cp932" => Kaminari.Sazanami.Text.TextEncodings.Resolve(932),
            _ => Encoding.UTF8,
        };
        byte[] bytes = kind == "utf-16" ? [.. encoding.GetPreamble(), .. encoding.GetBytes(text)] : encoding.GetBytes(text);
        if (kind == "gzip")
        {
            using var compressed = new MemoryStream();
            using (var gzip = new System.IO.Compression.GZipStream(compressed, System.IO.Compression.CompressionLevel.Fastest, leaveOpen: true)) gzip.Write(bytes);
            bytes = compressed.ToArray();
        }
        var options = new CsvReadOptions
        {
            SkipLines = 2,
            CommentPrefix = prefix,
            Encoding = kind == "cp932" ? encoding : null,
            MaxDegreeOfParallelism = 4,
        };
        const string expected = "名前:str,値:i64 | \"あ\",1 | \"い\",2";
        foreach (CsvSource source in Enum.GetValues<CsvSource>())
            foreach (int unit in (int[])[1, 5, 1 << 20])
                Assert.Equal(expected, CsvBehaviourTests.Read(bytes, options with { UnitBytes = unit }, source));
        // A line number counts the lines skipped.
        byte[] bad = encoding.GetBytes($"junk \"open\nmore\nn\n{prefix} c\n1\nx\n");
        if (kind == "utf-16") bad = [.. encoding.GetPreamble(), .. bad];
        var typed = options with { Schema = new Apache.Arrow.Schema([new Apache.Arrow.Field("n", Apache.Arrow.Types.Int64Type.Default, true)], null) };
        if (kind != "gzip")
            Assert.StartsWith("Row 2 (line 6): column 'n'", Assert.Throws<CsvValueException>(() => CsvBehaviourTests.Read(bad, typed, CsvSource.Memory)).Message);
    }

    [Fact]
    public void Truncation_TrailingSeparatorsAndPositionalSchemas()
    {
        // An extra field that is a separator last in the input is dropped too.
        Assert.Equal("a:i64,b:str | 1,\"x\"", Read("a,b\n1,x,", new CsvReadOptions { TruncateRaggedLines = true }));
        Assert.Throws<CsvFormatException>(() => Read("a,b\n1,x,", new CsvReadOptions()));
        // Without a header a schema gives the columns: wider records are truncated when asked, else refused.
        var schema = new Apache.Arrow.Schema([new("a", Apache.Arrow.Types.Int64Type.Default, true), new("b", Apache.Arrow.Types.StringType.Default, true)], null);
        var positional = new CsvReadOptions { HasHeader = false, Schema = schema };
        Assert.Equal("a:i64,b:str | 1,\"x\" | 2,\"y\"", Read("1,x,9\n2,y\n", positional with { TruncateRaggedLines = true }));
        Assert.Contains("schema has 2 columns, the records 3", Assert.Throws<CsvFormatException>(() => Read("1,x,9\n2,y\n", positional)).Message);
        Assert.Contains("schema has 2 columns, the records 1", Assert.Throws<CsvFormatException>(() => Read("1\n2\n", positional with { TruncateRaggedLines = true })).Message);
    }

    [Fact]
    public void ANonAsciiPrefix_InASingleBytePage_IsComparedOnTheText()
    {
        Encoding cp1252 = Kaminari.Sazanami.Text.TextEncodings.Resolve(1252);
        byte[] bytes = cp1252.GetBytes("a,b\n§ x\"y\n1,é\n");
        foreach (int unit in (int[])[1, 1 << 20])
            Assert.Equal("a:i64,b:str | 1,\"é\"", CsvBehaviourTests.Read(bytes, new CsvReadOptions { Encoding = cp1252, CommentPrefix = "§", UnitBytes = unit }, CsvSource.Memory));
    }

    /// <summary>Row filters over files with comment lines (invalid bytes in them), skipped lines and rows, and truncated records.</summary>
    [Theory]
    [InlineData(1 << 20)]
    [InlineData(4)]
    public void Filters_WithCommentsSkipsAndTruncation(int unit)
    {
        var sb = new StringBuilder("junk \"x\nj2\na,b\nq,1\n");
        for (int i = 1; i <= 200; i++)
        {
            if (i % 17 == 0) sb.Append("# \uFFFF odd \"\n");
            sb.Append(i).Append(',').Append(i % 3 == 0 ? "x" : i.ToString()).Append(i % 5 == 0 ? ",extra,\"e\"" : "").Append('\n');
        }
        byte[] bytes = Encoding.UTF8.GetBytes(sb.ToString());
        for (int i = 0; i + 2 < bytes.Length; i++)
            if (bytes[i] == 0xEF && bytes[i + 1] == 0xBF && bytes[i + 2] == 0xBF) bytes[i] = 0xFF;
        var options = new CsvReadOptions
        {
            SkipLines = 2, SkipRowsAfterHeader = 1, CommentPrefix = "#", TruncateRaggedLines = true, UnitBytes = unit, RowIndexColumn = "r",
            Filter = Kaminari.Samidare.IR.ScalarExpr.NewBinary(Kaminari.Samidare.IR.BinaryOp.Eq,
                Kaminari.Samidare.IR.ScalarExpr.NewBinary(Kaminari.Samidare.IR.BinaryOp.Modulo, Kaminari.Samidare.IR.ScalarExpr.NewColumn("a"),
                    Kaminari.Samidare.IR.ScalarExpr.NewLiteral(Kaminari.Samidare.IR.LiteralValue.NewInt64(10))),
                Kaminari.Samidare.IR.ScalarExpr.NewLiteral(Kaminari.Samidare.IR.LiteralValue.NewInt64(1))),
            FilterRows = true,
        };
        // b holds "x" every third row (a string column then); kept: a % 10 == 1; the row index counts the data records.
        string expected = "r:i64,a:i64,b:str" + string.Concat(Enumerable.Range(1, 200).Where(i => i % 10 == 1).Select(i => $" | {i - 1},{i},\"{(i % 3 == 0 ? "x" : i.ToString())}\""));
        foreach (CsvSource source in Enum.GetValues<CsvSource>())
            Assert.Equal(expected, CsvBehaviourTests.Read(bytes, options, source));
        // An Int64 b: the kept rows' "x" is refused (row 21 = a 21, line counts every line), dropped rows' are not.
        var typed = options with { SchemaOverrides = new Dictionary<string, Apache.Arrow.Types.IArrowType> { ["b"] = Apache.Arrow.Types.Int64Type.Default } };
        Assert.StartsWith("Row 21 (line 26): column 'b'", Assert.Throws<CsvValueException>(() => CsvBehaviourTests.Read(bytes, typed, CsvSource.Memory)).Message);
    }

    private static string Read(string text, CsvReadOptions options) => CsvBehaviourTests.Read(Encoding.UTF8.GetBytes(text), options);

    private static string Outcome(Func<string> read) => CsvUnitInvarianceTests.Outcome(read);

    // Line numbers differ by the lines added; everything else must not.
    [GeneratedRegex(@"line \d+")]
    private static partial Regex LineNumber();

    private static string Mask(string outcome) => LineNumber().Replace(outcome, "line _");

    /// <summary>The plain file and options, the decorated file and its options, and what was added.</summary>
    internal static (byte[] Plain, CsvReadOptions PlainOptions, byte[] Decorated, CsvReadOptions Options, string What) Make(int seed)
    {
        var decor = new Random(seed * 31 + 7);
        bool comments = decor.Next(2) == 0, truncate = decor.Next(4) == 0;
        int skip = decor.Next(3); // 0 none, 1 SkipLines, 2 SkipRows
        long after = decor.Next(3) == 0 ? 0 : 1 + decor.Next(4);
        int prefixCount = 1 + decor.Next(4);
        (byte[] plain, CsvReadOptions plainOptions) = CsvUnitInvarianceTests.Generate(new Random(seed));
        string lineEnd = plainOptions.LineEnd is ';' ? ";" : "\n";
        bool quoting = plainOptions.Quote is not null, afterWritten = false;
        (byte[] body, CsvReadOptions options) = CsvUnitInvarianceTests.Generate(new Random(seed), (record, sb, _) =>
        {
            // Nothing after a CR: the plain file may pair it with an LF that comes next (one line end, not two).
            if (sb.Length > 0 && sb[^1] == '\r') return;
            if (comments && decor.Next(6) == 0)
                for (int k = 1 + decor.Next(2); k > 0; k--) sb.Append('#').Append(Junk(decor, lineEnd, balanced: false)).Append(lineEnd);
            if (record == 0 && after > 0)
            {
                for (int k = 0; k < after; k++) sb.Append(JunkRecord(decor, lineEnd, quoting, sb, k == 0)).Append(lineEnd);
                afterWritten = true;
            }
        });
        var prefix = new StringBuilder();
        if (skip == 1)
            for (int k = 0; k < prefixCount; k++) prefix.Append(Junk(decor, lineEnd, balanced: false)).Append(lineEnd);
        else if (skip == 2)
            for (int k = 0; k < prefixCount; k++) prefix.Append(JunkRecord(decor, lineEnd, quoting, prefix, k == 0)).Append(lineEnd);
        byte[] head = Encoding.UTF8.GetBytes(prefix.ToString());
        for (int i = 0; i + 2 < head.Length; i++)
            if (head[i] == 0xEF && head[i + 1] == 0xBF && head[i + 2] == 0xBF) head[i] = 0xFF;
        CsvReadOptions decorated = options with
        {
            CommentPrefix = comments ? "#" : null,
            SkipLines = skip == 1 ? prefixCount : 0,
            SkipRows = skip == 2 ? prefixCount : 0,
            SkipRowsAfterHeader = afterWritten ? after : 0,
            TruncateRaggedLines = truncate,
        };
        string what = $"comments {comments}, skip lines {decorated.SkipLines}, rows {decorated.SkipRows}, after {decorated.SkipRowsAfterHeader}, truncate {truncate}";
        return (plain, plainOptions with { TruncateRaggedLines = truncate }, [.. head, .. body], decorated, what);
    }

    private const string Alphabet = "jk,;|\t \"#é1x2漢\uFFFF";

    // One line's text: no line end; quotes balanced or not; invalid bytes (U+FFFF, made 0xFF) sometimes.
    private static string Junk(Random random, string lineEnd, bool balanced)
    {
        var sb = new StringBuilder();
        int length = random.Next(12);
        for (int i = 0; i < length; i++)
        {
            char c = Alphabet[random.Next(Alphabet.Length)];
            if (lineEnd == ";" && c == ';') continue;
            sb.Append(c);
        }
        string text = sb.ToString();
        if (balanced && text.Count(c => c == '"') % 2 == 1) text += "\"";
        return text;
    }

    // One junk record as the cut sees records: quotes balanced (a quoted field may hold a line end), grammar-breaking
    // quotes, refused values, invalid bytes; blank sometimes (never first, never after a CR: it would pair with it).
    private static string JunkRecord(Random random, string lineEnd, bool quoting, StringBuilder before, bool first)
    {
        if (!first && random.Next(6) == 0 && lineEnd == "\n" && (before.Length == 0 || before[^1] != '\r')) return "";
        string text = random.Next(4) switch
        {
            0 when quoting => $"\"{Junk(random, lineEnd, balanced: true).Replace("\"", "\"\"")}{lineEnd}more\"",
            1 => "x\"y\"z",
            _ => "q" + Junk(random, lineEnd, balanced: true),
        };
        // Without quoting a quote is a character like any other.
        return text;
    }
}
