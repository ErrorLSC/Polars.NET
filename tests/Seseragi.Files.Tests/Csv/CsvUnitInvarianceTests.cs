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

    /// <summary>A generated file in a random encoding (<see cref="CsvGenerator.Encode"/>).</summary>
    internal static (byte[] Bytes, int Page, string Text, bool Planted, CsvReadOptions Options)? Encode(Random random, byte[] utf8, CsvReadOptions options) =>
        CsvGenerator.Encode(random, utf8, options);

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

    /// <summary>A random file and the options to read it with (<see cref="CsvGenerator.Generate"/>).</summary>
    internal static (byte[] Bytes, CsvReadOptions Options) Generate(Random random, Action<int, StringBuilder, string[]>? insert = null) =>
        CsvGenerator.Generate(random, insert);
}
