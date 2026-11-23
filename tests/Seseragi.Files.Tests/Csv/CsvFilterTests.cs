using System.Text;
using System.Text.RegularExpressions;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Seseragi.Files.Csv;
using Kaminari.Seseragi.Files.IO;
using Microsoft.FSharp.Collections;
using static Seseragi.Files.Tests.Csv.CsvBehaviourTests;

namespace Seseragi.Files.Tests.Csv;

/// <summary>
/// Step f3 of the managed CSV reader: row filters decoded in two phases (the columns the filter reads for every record,
/// the delivered ones for the kept records only). A value a dropped record holds in a column the filter does not read
/// is never decoded (user decision 2026-11-23: not raised, unlike the engine); values in the columns it reads and input
/// errors fail whatever the filter keeps.
/// </summary>
public partial class CsvFilterTests
{
    private static readonly int Count = int.TryParse(Environment.GetEnvironmentVariable("CSV_FILTER_CASES"), out int n) ? n : 120;

    private static Schema SchemaOf(params (string Name, IArrowType Type)[] columns) => new([.. columns.Select(c => new Field(c.Name, c.Type, true))], null);

    private static ScalarExpr Col(string name) => ScalarExpr.NewColumn(name);

    private static ScalarExpr Lit(long value) => ScalarExpr.NewLiteral(LiteralValue.NewInt64(value));

    private static ScalarExpr Bin(BinaryOp op, ScalarExpr l, ScalarExpr r) => ScalarExpr.NewBinary(op, l, r);

    private static string Read(string text, CsvReadOptions options, CsvSource source = CsvSource.Memory) =>
        CsvBehaviourTests.Read(Encoding.UTF8.GetBytes(text), options, source);

    private static string Outcome(Func<string> read) => CsvUnitInvarianceTests.Outcome(read);

    // a = 1..rows, b = a except a bad value at row bad.
    private static string Numbers(int rows, int bad, string badValue = "x")
    {
        var sb = new StringBuilder("a,b\n");
        for (int i = 1; i <= rows; i++) sb.Append(i).Append(',').Append(i == bad ? badValue : i.ToString()).Append('\n');
        return sb.ToString();
    }

    private static readonly Schema TwoInts = SchemaOf(("a", Int64Type.Default), ("b", Int64Type.Default));

    private static CsvReadOptions Filtered(ScalarExpr filter, CsvReadOptions? options = null) =>
        (options ?? new CsvReadOptions()) with { Filter = filter, FilterRows = true };

    [Theory]
    [InlineData(1 << 20)]
    [InlineData(7)]
    [InlineData(64)]
    public void ADroppedRowsValue_InAColumnTheFilterDoesNotRead_IsNotRaised(int unit)
    {
        string text = Numbers(300, 250);
        var given = new CsvReadOptions { Schema = TwoInts, UnitBytes = unit, BatchSize = 37 };
        // Unfiltered: refused. Filtered past it: the row is never decoded, inferred or given.
        Assert.Throws<CsvValueException>(() => Read(text, given));
        foreach (CsvReadOptions options in (CsvReadOptions[])[given, new CsvReadOptions { UnitBytes = unit }])
        {
            string all = Read(text, Filtered(Bin(BinaryOp.NotEq, Col("a"), Lit(250)), options));
            Assert.Equal(299, all.Split(" | ").Length - 1);
            Assert.DoesNotContain("| 250,", all);
            Assert.Equal("a:i64,b:i64 | 1,1 | 2,2 | 3,3", Read(text, Filtered(Bin(BinaryOp.Lt, Col("a"), Lit(4)), options)));
            Assert.Equal("b:i64 | 299 | 300", Read(text, Filtered(Bin(BinaryOp.Gt, Col("a"), Lit(298)), options with { Columns = ["b"] })));
        }
        // A kept row's value is refused; a value in a column the filter reads is refused on any row.
        var kept = Assert.Throws<CsvValueException>(() => Read(text, Filtered(Bin(BinaryOp.GtEq, Col("a"), Lit(250)), given)));
        Assert.StartsWith("Row 250 (line 251): column 'b'", kept.Message);
        var tested = Assert.Throws<CsvValueException>(() => Read(text, Filtered(Bin(BinaryOp.Lt, Col("b"), Lit(10)), given)));
        Assert.StartsWith("Row 250 (line 251): column 'b'", tested.Message);
        // A filter keeping nothing: the columns, no rows.
        Assert.Equal("a:i64,b:i64", Read(text, Filtered(Bin(BinaryOp.Gt, Col("a"), Lit(1000)), given)));
    }

    [Theory]
    [InlineData(1 << 20)]
    [InlineData(5)]
    public void InputErrors_AreRaisedWhateverTheFilterKeeps(int unit)
    {
        var options = new CsvReadOptions { Schema = TwoInts, UnitBytes = unit };
        ScalarExpr first = Bin(BinaryOp.Lt, Col("a"), Lit(3));
        string ragged = Numbers(200, 0).Replace("\n150,150\n", "\n150,150,9\n");
        Assert.StartsWith("Row 150 (line 151): the record has more fields", Assert.Throws<CsvFormatException>(() => Read(ragged, Filtered(first, options))).Message);
        string quote = Numbers(200, 0).Replace("\n150,150\n", "\n150,1\"5\n");
        Assert.StartsWith("Row 150 (line 151): field 2 holds a quote", Assert.Throws<CsvFormatException>(() => Read(quote, Filtered(first, options))).Message);
        byte[] invalid = Encoding.UTF8.GetBytes(Numbers(200, 150, "￿"));
        for (int i = 0; i + 2 < invalid.Length; i++)
            if (invalid[i] == 0xEF && invalid[i + 1] == 0xBF && invalid[i + 2] == 0xBF) invalid[i] = 0xFF;
        Assert.StartsWith("Row 150 (line 151):", Assert.Throws<CsvFormatException>(() => CsvBehaviourTests.Read(invalid, Filtered(first, options), CsvSource.Memory)).Message);
        // Past the rows asked (MaxRows counts the rows kept), nothing fails.
        Assert.Equal("a:i64,b:i64 | 1,1 | 2,2", Read(ragged, Filtered(first, options with { MaxRows = 2 })));
        Assert.Equal("a:i64,b:i64 | 1,1", Read(ragged, Filtered(first, options with { MaxRows = 1 })));
    }

    [Theory]
    [InlineData(1 << 20, 1000)]
    [InlineData(6, 1000)]
    [InlineData(1 << 20, 3)]
    public void MaxRows_CountsTheRowsKept_AndStopsBeforeLaterErrors(int unit, int batch)
    {
        // Kept: even rows. A value refused on kept row 120 (the 60th kept), and a value in a dropped row (odd) before it.
        var sb = new StringBuilder("a,b\n");
        for (int i = 1; i <= 300; i++) sb.Append(i).Append(',').Append(i is 120 or 101 ? "x" : i.ToString()).Append('\n');
        string text = sb.ToString();
        var options = Filtered(Bin(BinaryOp.Eq, Bin(BinaryOp.Modulo, Col("a"), Lit(2)), Lit(0)),
            new CsvReadOptions { Schema = TwoInts, UnitBytes = unit, BatchSize = batch, RowIndexColumn = "i", RowIndexOffset = 10 });
        string upTo59 = Read(text, options with { MaxRows = 59 });
        Assert.EndsWith("| 127,118,118", upTo59);
        Assert.StartsWith("i:i64,a:i64,b:i64 | 11,2,2 | 13,4,4", upTo59);
        Assert.Equal(59, upTo59.Split(" | ").Length - 1);
        Assert.StartsWith("Row 120 (line 121): column 'b'", Assert.Throws<CsvValueException>(() => Read(text, options with { MaxRows = 60 })).Message);
        Assert.StartsWith("Row 120 (line 121): column 'b'", Assert.Throws<CsvValueException>(() => Read(text, options)).Message);
        Assert.Equal("i:i64,a:i64,b:i64", Read(text, options with { MaxRows = 0 }));
    }

    [Theory]
    [InlineData(1 << 20)]
    [InlineData(9)]
    public void TemporalFormat_IsTheFirstValuesInFileOrder_KeptOrNot(int unit)
    {
        // A given date column empty past the rows read at open; its first value (year-month-day) on a dropped row; a kept
        // row in day-month-year after it is refused, as without the filter.
        var sb = new StringBuilder("k,d\n");
        for (int i = 0; i < 150; i++) sb.Append("0,\n");
        sb.Append("0,2024-01-02\n1,03-01-2024\n1,2024-01-05\n");
        var options = new CsvReadOptions { Schema = SchemaOf(("k", Int64Type.Default), ("d", Date32Type.Default)), UnitBytes = unit };
        ScalarExpr keep = Bin(BinaryOp.Eq, Col("k"), Lit(1));
        var unfiltered = Assert.Throws<CsvValueException>(() => Read(sb.ToString(), options));
        var filtered = Assert.Throws<CsvValueException>(() => Read(sb.ToString(), Filtered(keep, options)));
        Assert.Equal(unfiltered.Message, filtered.Message);
        Assert.Contains("is not a date in the column's format (year-month-day)", filtered.Message);
        Assert.Equal("k:i64,d:date | 1,null | 1,2024-01-05", Read(sb.ToString(), Filtered(keep, options with { IgnoreErrors = true })));
        // A kept value before the first one that reads is refused without a format, as without the filter.
        var before = new StringBuilder("k,d\n");
        for (int i = 0; i < 150; i++) before.Append("0,\n");
        before.Append("1,nope\n0,2024-01-02\n1,2024-01-05\n");
        Assert.Equal(Outcome(() => Read(before.ToString(), options)), Outcome(() => Read(before.ToString(), Filtered(keep, options))));
        // The first value that reads on a dropped record after a chunk's last kept one (the chunk ends there: 151 rows a
        // batch): the next chunk reads in its format.
        var chunked = new StringBuilder("k,d\n");
        for (int i = 0; i < 150; i++) chunked.Append("1,\n");
        chunked.Append("0,2024-01-02\n1,03-01-2024\n1,2024-01-05\n");
        Assert.Equal(Outcome(() => Read(chunked.ToString(), options with { BatchSize = 151 })),
            Outcome(() => Read(chunked.ToString(), Filtered(keep, options with { BatchSize = 151 }))));
        Assert.Contains("is not a date in the column's format (year-month-day)", Outcome(() => Read(chunked.ToString(), Filtered(keep, options with { BatchSize = 151 }))));
        // The date column read by the filter and delivered.
        ScalarExpr onDate = Bin(BinaryOp.Gt, Col("d"), ScalarExpr.NewLiteral(LiteralValue.NewDate(new DateOnly(2024, 1, 3))));
        string ymd = sb.ToString().Replace("1,03-01-2024\n", "1,2024-01-03\n");
        Assert.Equal("k:i64,d:date | 1,2024-01-05", Read(ymd, Filtered(onDate, options)));
        Assert.Equal("d:date | 2024-01-05", Read(ymd, Filtered(onDate, options with { Columns = ["d"] })));
    }

    [Fact]
    public void FiltersExactly_TellsWhetherEveryConjunctIsEvaluated()
    {
        byte[] bytes = Encoding.UTF8.GetBytes("a,s\n1,x\n2,y\n3,z\n");
        ScalarExpr lowered = Bin(BinaryOp.Gt, Col("a"), Lit(1));
        ScalarExpr foreign = Bin(BinaryOp.Gt, Col("a"), ScalarExpr.NewLiteral(LiteralValue.NewString("q")));
        using (CsvReader reader = CsvReader.Open(bytes, Filtered(lowered))) Assert.True(reader.FiltersExactly);
        using (CsvReader reader = CsvReader.Open(bytes, Filtered(Bin(BinaryOp.And, lowered, foreign)))) Assert.False(reader.FiltersExactly);
        using (CsvReader reader = CsvReader.Open(bytes, new CsvReadOptions { Filter = foreign })) Assert.True(reader.FiltersExactly);
        // Only the conjunct evaluated drops rows; a filter none of which is evaluated keeps them all.
        Assert.Equal("a:i64,s:str | 2,\"y\" | 3,\"z\"", CsvBehaviourTests.Read(bytes, Filtered(Bin(BinaryOp.And, lowered, foreign))));
        Assert.Equal("a:i64,s:str | 1,\"x\" | 2,\"y\" | 3,\"z\"", CsvBehaviourTests.Read(bytes, Filtered(foreign)));
        Assert.Equal("a:i64,s:str | 1,\"x\" | 2,\"y\" | 3,\"z\"", CsvBehaviourTests.Read(bytes, new CsvReadOptions { Filter = lowered }));
    }

    [Theory]
    [InlineData(1 << 20)]
    [InlineData(5)]
    public void ErrorsInOneChunk_InRecordOrder_TheFirstColumnInOutputOrder(int unit)
    {
        var options = new CsvReadOptions { Schema = SchemaOf(("b", Int64Type.Default), ("c", Int64Type.Default), ("d", Int64Type.Default)), UnitBytes = unit };
        // c (read by the filter) refused on record 5; d (delivered only) on kept record 7 after it: record 5's comes first,
        // though b alone keeps the records after it.
        string text = "b,c,d\n1,1,1\n1,1,1\n1,1,1\n1,1,1\n1,x,1\n1,1,1\n1,1,x\n1,1,1\n";
        ScalarExpr either = Bin(BinaryOp.Or, Bin(BinaryOp.Gt, Col("b"), Lit(0)), Bin(BinaryOp.Gt, Col("c"), Lit(0)));
        Assert.StartsWith("Row 5 (line 6): column 'c'", Assert.Throws<CsvValueException>(() => Read(text, Filtered(either, options with { Columns = ["b", "d"] }))).Message);
        Assert.StartsWith("Row 5 (line 6): column 'c'", Assert.Throws<CsvValueException>(() => Read(text, Filtered(either, options))).Message);
        // Two columns the filter reads refused in one record: the first in output order (as unfiltered), whatever order
        // the filter names them in.
        string both = "b,c,d\n1,1,1\n1,1,1\nx,x,1\n";
        ScalarExpr reversed = Bin(BinaryOp.And, Bin(BinaryOp.Gt, Col("c"), Lit(0)), Bin(BinaryOp.Gt, Col("b"), Lit(0)));
        Assert.StartsWith("Row 3 (line 4): column 'b'", Assert.Throws<CsvValueException>(() => Read(both, Filtered(reversed, options))).Message);
        Assert.StartsWith("Row 3 (line 4): column 'b'", Assert.Throws<CsvValueException>(() => Read(both, options)).Message);
    }

    [Fact]
    public void WithoutAHeader_TheFilterNamesTheColumns_AsGivenOrNumbered()
    {
        string text = "1,x,10\n2,y,20\n3,z,30\n";
        var given = new CsvReadOptions { HasHeader = false, Schema = SchemaOf(("a", Int64Type.Default), ("s", StringType.Default), ("b", Int64Type.Default)) };
        Assert.Equal("s:str | \"y\" | \"z\"", Read(text, Filtered(Bin(BinaryOp.Gt, Col("b"), Lit(15)), given with { Columns = ["s"] })));
        Assert.Equal("column_2:str | \"x\"", Read(text, Filtered(Bin(BinaryOp.Lt, Col("column_3"), Lit(15)), new CsvReadOptions { HasHeader = false, Columns = ["column_2"] })));
    }

    [Theory]
    [InlineData(1 << 20, 1000)]
    [InlineData(11, 2)]
    public void Strings_Categoricals_AndRowIndex(int unit, int batch)
    {
        var sb = new StringBuilder("id,s,c\n");
        for (int i = 0; i < 400; i++) sb.Append(i).Append(',').Append(i % 7 == 0 ? "\"q,\"\"x\"" : $"v{i % 5}").Append(',').Append($"k{i % 3}").Append('\n');
        var options = new CsvReadOptions
        {
            Schema = SchemaOf(("id", Int64Type.Default), ("s", StringType.Default), ("c", new DictionaryType(UInt32Type.Default, StringType.Default, false))),
            UnitBytes = unit, BatchSize = batch, RowIndexColumn = "r",
        };
        ScalarExpr filter = Bin(BinaryOp.And, Bin(BinaryOp.Eq, Col("c"), ScalarExpr.NewLiteral(LiteralValue.NewString("k1"))),
            Bin(BinaryOp.Eq, Col("s"), ScalarExpr.NewLiteral(LiteralValue.NewString("q,\"x"))));
        string got = Read(sb.ToString(), Filtered(filter, options with { Columns = ["id", "s"] }));
        // i % 7 == 0 and i % 3 == 1: 7, 28, 49, …
        string expected = "r:i64,id:i64,s:str" + string.Concat(Enumerable.Range(0, 400).Where(i => i % 7 == 0 && i % 3 == 1).Select(i => $" | {i},{i},\"q,\\\"x\""));
        Assert.Equal(expected, got);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // G1: the generator's files, random predicates: the filtered read equals the unfiltered read filtered afterwards.

    public static TheoryData<int> Seeds()
    {
        var data = new TheoryData<int>();
        for (int i = 0; i < Count; i++) data.Add(20261124 + i);
        return data;
    }

    /// <summary>Partial cases (a dropped row's value refused unfiltered; only the rows before it compared) seen in the run.</summary>
    private static int s_partial, s_cases;

    [Theory]
    [MemberData(nameof(Seeds))]
    public void FilteredRead_EqualsTheReadFilteredAfterwards(int seed)
    {
        var random = new Random(seed);
        (byte[] bytes, CsvReadOptions options) = CsvUnitInvarianceTests.Generate(random);
        if (random.Next(3) == 0 && CsvUnitInvarianceTests.Encode(random, bytes, options) is var (encoded, _, _, _, encodedOptions))
            (bytes, options) = (encoded, encodedOptions);
        Interlocked.Increment(ref s_cases);
        // The file's columns as read without a filter (an input failing at open fails filtered alike).
        CsvReadOptions plain = options with { MaxRows = null, Columns = null, RowIndexColumn = null };
        Schema full;
        try
        {
            using CsvReader reader = CsvReader.Open(bytes, plain);
            full = reader.Schema;
        }
        catch (Exception e) when (e is CsvFormatException or CsvValueException)
        {
            string failed = $"{e.GetType().Name}: {e.Message}";
            Assert.Equal(failed, Outcome(() => CsvBehaviourTests.Read(bytes, options with { Filter = Bin(BinaryOp.Eq, Col(plain.HasHeader ? "c0" : "column_1"), Lit(1)), FilterRows = true }, CsvSource.Memory)));
            return;
        }
        if (full.FieldsList.Count == 0) return;
        string[] names = [.. full.FieldsList.Select(f => f.Name)];
        if (random.Next(3) == 0) options = options with { Columns = [.. names.Where(_ => random.Next(2) == 0).DefaultIfEmpty(names[0])] };
        ScalarExpr filter = Predicate(random, full, 2);
        options = options with { Filter = filter, FilterRows = true };
        (string expected, long expectedRows, bool exact, long partialAt) = Expected(bytes, options, full, filter);
        if (partialAt >= 0) Interlocked.Increment(ref s_partial);
        foreach (int unit in (int[])[1, 3, 7, 64, 4096])
            foreach (CsvSource source in Enum.GetValues<CsvSource>())
            {
                if (source == CsvSource.PathUnmapped && unit < 64) continue;
                foreach (int threads in (int[])[1, 8])
                {
                    if (threads == 1 && unit is not (7 or 4096)) continue;
                    CsvReadOptions o = options with { UnitBytes = unit, MaxDegreeOfParallelism = threads, BatchSize = 1 + random.Next(60) };
                    if (partialAt < 0)
                    {
                        string got = Outcome(() => CsvBehaviourTests.Read(bytes, o, source));
                        if (got != expected)
                            Assert.Fail($"seed {seed}, unit {unit}, {source}, t{threads}, filter {Show(filter)}:\nexpected: {Cut(expected)}\ngot:      {Cut(got)}");
                    }
                    else
                    {
                        // The rows before the refused one, then no failure at or before it.
                        string got = Outcome(() => CsvBehaviourTests.Read(bytes, o with { MaxRows = Math.Min(o.MaxRows ?? long.MaxValue, expectedRows) }, source));
                        if (got != expected) Assert.Fail($"seed {seed}, unit {unit}, {source}, partial before {partialAt}:\nexpected: {Cut(expected)}\ngot:      {Cut(got)}");
                        string rest = Outcome(() => CsvBehaviourTests.Read(bytes, o, source));
                        if (RowOf(rest) is long at && at <= partialAt) Assert.Fail($"seed {seed}, unit {unit}, {source}: failed at {at}, at or before the dropped row {partialAt}: {Cut(rest)}");
                    }
                }
            }
        using (CsvReader reader = CsvReader.Open(bytes, options)) Assert.Equal(exact, reader.FiltersExactly);

        static string Cut(string s) => s.Length > 2500 ? s[..2500] + "…" : s;
    }

    /// <summary>The generator and the predicates reach the paths: kept and dropped rows, failures, partial cases few.</summary>
    [Fact]
    public void OracleCoversThePaths()
    {
        int failures = 0, someKept = 0, partial = 0, inexact = 0, cases = 0;
        for (int i = 0; i < 300; i++)
        {
            var random = new Random(20261124 + i);
            (byte[] bytes, CsvReadOptions options) = CsvUnitInvarianceTests.Generate(random);
            if (random.Next(3) == 0 && CsvUnitInvarianceTests.Encode(random, bytes, options) is var (encoded, _, _, _, encodedOptions))
                (bytes, options) = (encoded, encodedOptions);
            Schema full;
            try
            {
                using CsvReader reader = CsvReader.Open(bytes, options with { MaxRows = null, Columns = null, RowIndexColumn = null });
                full = reader.Schema;
            }
            catch (Exception e) when (e is CsvFormatException or CsvValueException) { continue; }
            if (full.FieldsList.Count == 0) continue;
            string[] names = [.. full.FieldsList.Select(f => f.Name)];
            if (random.Next(3) == 0) options = options with { Columns = [.. names.Where(_ => random.Next(2) == 0).DefaultIfEmpty(names[0])] };
            ScalarExpr filter = Predicate(random, full, 2);
            (string expected, long rows, bool exact, long partialAt) = Expected(bytes, options with { Filter = filter, FilterRows = true }, full, filter);
            cases++;
            if (partialAt >= 0) partial++;
            else if (expected.StartsWith("Csv", StringComparison.Ordinal)) failures++;
            else if (rows > 0) someKept++;
            if (!exact) inexact++;
        }
        Assert.True(failures >= 10 && someKept >= cases / 3 && partial * 10 <= cases && inexact >= 10,
            $"cases {cases}, failures {failures}, some kept {someKept}, partial {partial}, inexact {inexact}");
    }

    private static string Show(ScalarExpr e) => e.ToString().Replace("\n", " ");

    [GeneratedRegex(@"^Csv\w+Exception: Row (\d+) ")]
    private static partial Regex ErrorRow();

    private static long? RowOf(string outcome) => ErrorRow().Match(outcome) is { Success: true } m ? long.Parse(m.Groups[1].Value) : null;

    /// <summary>
    /// What the filtered read gives, from unfiltered reads of the columns it decodes, filtered afterwards (RowFilter on
    /// whole batches, the JSON-proven path): the rows kept up to MaxRows, or the first failure in record order that the
    /// filtered read meets. When the unfiltered read refuses a value of a dropped row in a column the filter does not read,
    /// only the rows before it are known (partialAt = that row, 1-based); else partialAt = -1.
    /// </summary>
    private static (string Expected, long Rows, bool Exact, long PartialAt) Expected(byte[] bytes, CsvReadOptions options, Schema full, ScalarExpr filter)
    {
        string[] selectedNames = options.Columns is { } c ? [.. full.FieldsList.Select(f => f.Name).Where(c.Contains)] : [.. full.FieldsList.Select(f => f.Name)];
        string index = options.RowIndexColumn ?? "__i";
        long max = options.MaxRows ?? long.MaxValue;
        Schema selected = new([.. selectedNames.Select(name => full.GetFieldByName(name))], null);
        RowFilter probe = RowFilter.Create(full, selected, filter, out bool exact);
        string[] decoded = [.. probe.Decoded.FieldsList.Select(f => f.Name)];
        string[] tested = probe.Program is null ? [] : [.. probe.Program.Columns.Select(col => col.Path[0]).Distinct()];
        CsvReadOptions unfiltered = options with { Filter = null, FilterRows = false, Columns = decoded, RowIndexColumn = index, MaxRows = null };
        (string Text, long Rows) whole = Outcome(() => Filter(unfiltered, long.MaxValue, false));
        if (RowOf(whole.Text) is not long e)
            return whole.Text.StartsWith("Csv", StringComparison.Ordinal) ? (whole.Text, 0, exact, -1) : Expect(Filter(unfiltered, max, false));
        // The rows kept before the failing record.
        (string Text, long Rows) before = Filter(unfiltered with { MaxRows = e - 1 }, max, false);
        if (before.Rows >= max || probe.Program is null) return before.Rows >= max ? Expect(before) : (whole.Text, 0, exact, -1);
        bool valueOutsideTests = whole.Text.StartsWith(nameof(CsvValueException), StringComparison.Ordinal)
            && !tested.Any(t => whole.Text.Contains($": column '{t}':", StringComparison.Ordinal));
        if (!valueOutsideTests) return (whole.Text, 0, exact, -1);
        // A value refused in a column the filter does not read: failing when its record is kept (or when a tested column
        // fails there too).
        CsvReadOptions tests = options with { Filter = null, FilterRows = false, Columns = tested, RowIndexColumn = index, MaxRows = e };
        (string Text, long Rows) upTo = Outcome(() => Filter(tests, long.MaxValue, true));
        if (upTo.Text.StartsWith("Csv", StringComparison.Ordinal)) return (upTo.Text, 0, exact, -1);
        bool kept = upTo.Text.EndsWith($"| {e - 1}", StringComparison.Ordinal) || upTo.Text == $"{e - 1}";
        return kept ? (whole.Text, 0, exact, -1) : (before.Text, before.Rows, exact, e);

        (string, long, bool, long) Expect((string Text, long Rows) read) => (read.Text, read.Rows, exact, -1);

        static (string, long) Outcome(Func<(string, long)> read)
        {
            try { return read(); }
            catch (Exception e) when (e is CsvFormatException or CsvValueException) { return ($"{e.GetType().Name}: {e.Message}", 0); }
        }

        // An unfiltered read filtered afterwards: the selected columns (and the index when asked), or the kept rows' indices.
        (string, long) Filter(CsvReadOptions o, long rows, bool keptRows)
        {
            using CsvReader reader = CsvReader.Open(bytes, o with { RowIndexOffset = keptRows ? 0 : o.RowIndexOffset });
            Schema read = reader.Schema;
            // The index is always kept (every column read is then decoded, in the reader's order), dropped when not asked.
            Schema wanted = keptRows
                ? new Schema([read.GetFieldByName(index)], null)
                : new Schema([read.GetFieldByName(index), .. selectedNames.Select(read.GetFieldByName)], null);
            bool dropIndex = !keptRows && options.RowIndexColumn is null;
            Schema shown = dropIndex ? new Schema([.. wanted.FieldsList.Skip(1)], null) : wanted;
            RowFilter rowFilter = RowFilter.Create(read, wanted, filter, out _);
            List<RecordBatch> batches = [];
            try
            {
                long count = 0;
                foreach (RecordBatch batch in reader.ReadBatches())
                {
                    if (count >= rows)
                    {
                        batch.Dispose();
                        continue;
                    }
                    RecordBatch? kept = rowFilter.Apply(batch);
                    if (kept is null) continue;
                    if (count + kept.Length > rows) kept = RowFilter.First(kept, (int)(rows - count));
                    count += kept.Length;
                    if (dropIndex)
                    {
                        kept.Column(0).Dispose();
                        kept = new RecordBatch(shown, [.. kept.Arrays.Skip(1)], kept.Length);
                    }
                    batches.Add(kept);
                }
                if (!keptRows) return (Render(shown, batches), count);
                return (string.Join(" | ", batches.SelectMany(b => Enumerable.Range(0, b.Length).Select(r => ((Int64Array)b.Column(0)).GetValue(r)!.Value.ToString()))), count);
            }
            finally { batches.ForEach(b => b.Dispose()); }
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Random predicates over the columns read

    private static readonly BinaryOp[] Comparisons = [BinaryOp.Eq, BinaryOp.NotEq, BinaryOp.Lt, BinaryOp.LtEq, BinaryOp.Gt, BinaryOp.GtEq];

    private static ScalarExpr Predicate(Random random, Schema schema, int depth)
    {
        if (depth > 0 && random.Next(3) == 0)
            return random.Next(3) switch
            {
                0 => Bin(BinaryOp.And, Predicate(random, schema, depth - 1), Predicate(random, schema, depth - 1)),
                1 => Bin(BinaryOp.Or, Predicate(random, schema, depth - 1), Predicate(random, schema, depth - 1)),
                _ => ScalarExpr.NewUnary(UnaryOp.Not, Predicate(random, schema, depth - 1)),
            };
        Field field = schema.FieldsList[random.Next(schema.FieldsList.Count)];
        ScalarExpr column = Col(field.Name);
        BinaryOp op = Comparisons[random.Next(Comparisons.Length)];
        if (random.Next(8) == 0) return ScalarExpr.NewUnary(random.Next(2) == 0 ? UnaryOp.IsNull : UnaryOp.IsNotNull, column);
        if (random.Next(12) == 0)
            // A literal of another kind: the conjunct is not evaluated by the reader.
            return Bin(op, column, ScalarExpr.NewLiteral(field.DataType is StringType or StringViewType ? LiteralValue.NewDate(new DateOnly(2024, 1, 1)) : LiteralValue.NewString("q")));
        switch (field.DataType)
        {
            case Int64Type:
            {
                ScalarExpr literal = Lit(random.Next(-100_000, 100_000));
                if (random.Next(4) == 0) return Bin(BinaryOp.Eq, Bin(BinaryOp.Modulo, column, Lit(2 + random.Next(4))), Lit(random.Next(2)));
                if (random.Next(5) == 0 && Other(random, schema, field, t => t is Int64Type or DoubleType) is { } other) return Bin(op, column, Col(other));
                if (random.Next(6) == 0)
                    return ScalarExpr.NewInList(column, InListSource.NewLiterals(ListModule.OfSeq(Enumerable.Range(0, random.Next(4)).Select(_ => LiteralValue.NewInt64(random.Next(-5, 5))))),
                        random.Next(2) == 0, false);
                return Bin(op, column, literal);
            }
            case DoubleType:
                if (random.Next(5) == 0 && Other(random, schema, field, t => t is Int64Type or DoubleType) is { } otherDouble) return Bin(op, column, Col(otherDouble));
                return Bin(op, column, ScalarExpr.NewLiteral(LiteralValue.NewFloat64(random.NextDouble() * 1000 - 500)));
            case BooleanType:
                return random.Next(3) switch
                {
                    0 => column,
                    1 => ScalarExpr.NewUnary(UnaryOp.Not, column),
                    _ => Bin(BinaryOp.Eq, column, ScalarExpr.NewLiteral(LiteralValue.NewBool(random.Next(2) == 0))),
                };
            case StringType or StringViewType:
            {
                string[] pieces = ["", "a", "b", "x", "q", "abc", "漢", "NA", "1", "2024"];
                string piece = pieces[random.Next(pieces.Length)];
                return random.Next(3) switch
                {
                    0 => Bin(BinaryOp.StartsWith, column, ScalarExpr.NewLiteral(LiteralValue.NewString(piece))),
                    1 => Bin(op, column, ScalarExpr.NewLiteral(LiteralValue.NewString(piece))),
                    _ => Bin(BinaryOp.NotEq, column, ScalarExpr.NewLiteral(LiteralValue.NewString(piece))),
                };
            }
            case Date32Type:
                return Bin(op, column, ScalarExpr.NewLiteral(LiteralValue.NewDate(new DateOnly(2023 + random.Next(2), 1 + random.Next(12), 1 + random.Next(27)))));
            case TimestampType:
                return Bin(op, column, ScalarExpr.NewLiteral(LiteralValue.NewDateTime(new DateTime(2024, 1, 1 + random.Next(27), random.Next(24), 0, 0))));
            default:
                return ScalarExpr.NewUnary(random.Next(2) == 0 ? UnaryOp.IsNull : UnaryOp.IsNotNull, column);
        }

        static string? Other(Random random, Schema schema, Field field, Func<IArrowType, bool> kind)
        {
            Field[] others = [.. schema.FieldsList.Where(f => f.Name != field.Name && kind(f.DataType))];
            return others.Length == 0 ? null : others[random.Next(others.Length)].Name;
        }
    }
}
