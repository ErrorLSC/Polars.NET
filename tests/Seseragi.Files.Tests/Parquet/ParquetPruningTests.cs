using System.Text;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Rows;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet;
using Microsoft.FSharp.Collections;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// Row group pruning by <see cref="ParquetReadOptions.Filter"/> (IR filters). Soundness: a kept set never misses a
/// row group holding a row the filter keeps, checked by an evaluator of the IR's semantics that compares the values
/// read (it shares nothing with the compiler's literal conversion). Effectiveness: on TestData/parquet-pushdown
/// (written by write_pushdown.py) single comparisons keep exactly the row groups with matches, and statistics that
/// lie are used or ignored as pyarrow uses or ignores them.
/// </summary>
public class ParquetPruningTests
{
    private static string Fixture(string name) => Path.Combine(AppContext.BaseDirectory, "TestData", "parquet-pushdown", name);

    // ------------------------------------------------------------------
    // Statistics rules: lying statistics, against pyarrow
    // ------------------------------------------------------------------

    private static readonly Lazy<JsonDocument> Expected = new(() => JsonDocument.Parse(File.ReadAllBytes(Fixture("expected.json"))));

    public static TheoryData<string> LyingCases => [.. Expected.Value.RootElement.GetProperty("rows").EnumerateObject().Select(p => p.Name)];

    [Theory]
    [MemberData(nameof(LyingCases))]
    public void LyingStatistics_AreUsedOrIgnored_AsPyarrow(string name)
    {
        string[] parts = name.Split(' ');
        ScalarExpr filter = parts[1] switch
        {
            "i<5" => Bin(BinaryOp.Lt, Col("i"), Lit(LiteralValue.NewInt64(5))),
            "s<s05" => Bin(BinaryOp.Lt, Col("s"), Lit(LiteralValue.NewString("s05"))),
            "i=3" => Bin(BinaryOp.Eq, Col("i"), Lit(LiteralValue.NewInt64(3))),
            _ => Bin(BinaryOp.Eq, Col("s"), Lit(LiteralValue.NewString("s03"))),
        };
        using var file = ParquetFile.Open(Fixture(parts[0] + ".parquet"));
        using var reader = file.OpenReader(new ParquetReadOptions { Filter = filter, MaxDegreeOfParallelism = 1 });
        int rows = 0;
        foreach (RecordBatch batch in reader.ReadBatches())
        {
            using (batch)
            {
                for (int i = 0; i < batch.Length; i++)
                {
                    if (IrEvaluator.Eval(filter, Row(batch, i)) is true) rows++;
                }
            }
        }
        Assert.Equal(Expected.Value.RootElement.GetProperty("rows").GetProperty(name).GetInt32(), rows);
    }

    // ------------------------------------------------------------------
    // Single comparisons on every type: exactly the row groups with matches
    // ------------------------------------------------------------------

    private static readonly Lazy<FileRows> Types = new(() => FileRows.Read(Fixture("types.parquet"))!);

    private static readonly string[] Leaves =
    [
        "i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64", "f32", "f64", "f16", "d9", "d18", "d30", "b", "s", "dt",
        "ts_ms", "ts_us", "ts_ns", "ts_utc", "t_ms", "t_us", "t_ns", "n", "st.a", "st.b",
    ];

    public static TheoryData<string> TypeLeaves => [.. Leaves];

    [Theory]
    [MemberData(nameof(TypeLeaves))]
    public void SingleComparisons_KeepExactlyTheRowGroupsWithMatches(string leaf)
    {
        FileRows rows = Types.Value;
        ScalarExpr column = Leaf(leaf);
        int checkedFilters = 0;
        foreach (LiteralValue literal in rows.Literals(column))
        {
            foreach (BinaryOp op in Comparisons)
            {
                foreach (ScalarExpr filter in (ScalarExpr[])[Bin(op, column, Lit(literal)), Bin(op, Lit(literal), column)])
                {
                    int[] matching = rows.Matching(filter), kept = rows.Kept(filter);
                    // pyarrow writes no nan_count: NaN may lie beyond any maximum, so where a NaN row could pass,
                    // floating leaves keep more than the matches (only soundness is checked there).
                    bool nanMayPass = Floating.Contains(leaf) && (NaNPasses(op, flipped: filter is ScalarExpr.Binary { left: ScalarExpr.Literal })
                                                                  || literal.BoxedValue is double.NaN or float.NaN || literal.BoxedValue is Half h && Half.IsNaN(h));
                    if (nanMayPass) Assert.True(matching.All(kept.Contains), $"{Show(filter)}: kept [{string.Join(",", kept)}], matches in [{string.Join(",", matching)}]");
                    else Assert.True(matching.SequenceEqual(kept), $"{Show(filter)}: kept [{string.Join(",", kept)}], matches in [{string.Join(",", matching)}]");
                    checkedFilters++;
                }
            }
        }
        Assert.True(checkedFilters >= 24, $"only {checkedFilters} filters for {leaf}");
    }

    private static readonly string[] Floating = ["f32", "f64", "f16"];

    /// <summary>Whether a NaN value passes <c>NaN op literal</c> (or <c>literal op NaN</c> when flipped) for a number literal.</summary>
    private static bool NaNPasses(BinaryOp op, bool flipped) =>
        op.IsNotEq || (flipped ? op.IsLt || op.IsLtEq : op.IsGt || op.IsGtEq);

    [Fact]
    public void TheTypesFile_PrunesSomething_ForEveryLeaf()
    {
        // Guards the effectiveness test against a compiler that keeps everything (kept == matching would then fail
        // only where some row group has no match; this asserts such filters exist for each leaf).
        FileRows rows = Types.Value;
        foreach (string leaf in Leaves)
        {
            ScalarExpr column = Leaf(leaf);
            bool pruned = rows.Literals(column).Any(l => rows.Kept(Bin(BinaryOp.Eq, column, Lit(l))).Length < 3);
            Assert.True(pruned, $"no equality prunes on {leaf}");
        }
    }

    // ------------------------------------------------------------------
    // Compositions: never a matching row group lost
    // ------------------------------------------------------------------

    [Fact]
    public void RandomFilters_NeverDropARowGroupWithMatches()
    {
        FileRows rows = Types.Value;
        var random = new Random(20261114);
        ScalarExpr[] columns = [.. Leaves.Select(Leaf)];
        int pruning = 0;
        for (int n = 0; n < 1500; n++)
        {
            ScalarExpr filter = RandomFilter(random, rows, columns, depth: 3);
            int[] matching = rows.Matching(filter), kept = rows.Kept(filter);
            Assert.True(matching.All(kept.Contains), $"{Show(filter)}: kept [{string.Join(",", kept)}], matches in [{string.Join(",", matching)}]");
            if (kept.Length < 3) pruning++;
        }
        Assert.True(pruning > 150, $"only {pruning} of the random filters pruned anything");
    }

    private static ScalarExpr RandomFilter(Random random, FileRows rows, ScalarExpr[] columns, int depth)
    {
        int pick = random.Next(depth > 0 ? 10 : 6);
        ScalarExpr column = columns[random.Next(columns.Length)];
        LiteralValue[] literals = rows.Literals(column);
        LiteralValue literal = literals[random.Next(literals.Length)];
        return pick switch
        {
            0 or 1 or 2 => Bin(Comparisons[random.Next(Comparisons.Length)], column, Lit(literal)),
            3 => ScalarExpr.NewUnary(random.Next(2) == 0 ? UnaryOp.IsNull : UnaryOp.IsNotNull, column),
            4 => Bin(random.Next(2) == 0 ? BinaryOp.EqMissing : BinaryOp.NotEqMissing, column,
                     Lit(random.Next(3) == 0 ? LiteralValue.NewNull(Kaminari.Samidare.IR.LogicalType.Unknown) : literal)),
            5 => ScalarExpr.NewInList(column, InListSource.NewLiterals(ListModule.OfSeq<LiteralValue>(
                    [.. Enumerable.Range(0, random.Next(4)).Select(_ => literals[random.Next(literals.Length)]),
                     .. random.Next(3) == 0 ? [LiteralValue.NewNull(Kaminari.Samidare.IR.LogicalType.Unknown)] : (LiteralValue[])[]])),
                    random.Next(2) == 0, random.Next(2) == 0),
            6 => Bin(BinaryOp.And, RandomFilter(random, rows, columns, depth - 1), RandomFilter(random, rows, columns, depth - 1)),
            7 => Bin(BinaryOp.Or, RandomFilter(random, rows, columns, depth - 1), RandomFilter(random, rows, columns, depth - 1)),
            8 => Not(RandomFilter(random, rows, columns, depth - 1)),
            _ => ScalarExpr.NewCoalesce(ListModule.OfSeq<ScalarExpr>(
                    [RandomFilter(random, rows, columns, depth - 1), Lit(random.Next(3) switch
                    {
                        0 => LiteralValue.NewBool(false),
                        1 => LiteralValue.NewBool(true),
                        _ => LiteralValue.NewNull(Kaminari.Samidare.IR.LogicalType.Unknown),
                    })])),
        };
    }

    // ------------------------------------------------------------------
    // Particular shapes
    // ------------------------------------------------------------------

    [Fact]
    public void FloatGreaterThan_PrunesOnlyUnderTheFrontEndsNaNGuard()
    {
        // pyarrow writes no nan_count, so NaN (greater than every number) may hide behind every maximum.
        FileRows rows = Types.Value;
        ScalarExpr greater = Bin(BinaryOp.Gt, Col("f64"), Lit(LiteralValue.NewFloat64(5.0)));
        Assert.Equal([0, 1, 2], rows.Kept(greater));
        Assert.Equal([1, 2], rows.Matching(greater)); // the NaN in row group 1
        ScalarExpr guarded = Bin(BinaryOp.And, greater, Bin(BinaryOp.LtEq, Col("f64"), Lit(LiteralValue.NewFloat64(double.PositiveInfinity))));
        Assert.Equal([2], rows.Kept(guarded));
        ScalarExpr notNaN = Bin(BinaryOp.And, Not(ScalarExpr.NewUnary(UnaryOp.IsNan, Col("f64"))), greater);
        Assert.Equal([2], rows.Kept(notNaN));
        // Less-than needs no guard: NaN is never below a number.
        Assert.Equal([0], rows.Kept(Bin(BinaryOp.Lt, Col("f64"), Lit(LiteralValue.NewFloat64(-5.0)))));
    }

    [Fact]
    public void LiteralsBetweenStoredValues_CompareExactly()
    {
        FileRows rows = Types.Value;
        // i8 holds -15..-12 | -5..-2 | 5..8; d9 -10.00..-7.00 | 0.00..3.00 | 10.00..13.00.
        Assert.Equal([0], rows.Kept(Bin(BinaryOp.Lt, Col("i8"), Lit(LiteralValue.NewDecimal(-11.5m)))));
        Assert.Equal([0, 1], rows.Kept(Bin(BinaryOp.Lt, Col("i8"), Lit(LiteralValue.NewFloat64(-4.5)))));
        Assert.Empty(rows.Kept(Bin(BinaryOp.Eq, Col("i8"), Lit(LiteralValue.NewDecimal(-13.5m)))));
        Assert.Equal([2], rows.Kept(Bin(BinaryOp.GtEq, Col("d9"), Lit(LiteralValue.NewDecimal(3.005m)))));
        Assert.Equal([1, 2], rows.Kept(Bin(BinaryOp.Gt, Col("d9"), Lit(LiteralValue.NewInt32(-7)))));
        // ts_ms holds whole seconds + 500 ms; a literal 1 tick later lies between two stored values.
        var at = new DateTime(2020, 1, 1, 0, 0, 13, 500).AddTicks(1);
        Assert.Equal([2], rows.Kept(Bin(BinaryOp.Gt, Col("ts_ms"), Lit(LiteralValue.NewDateTime(at)))));
    }

    [Fact]
    public void UnmodelledColumnsAndExpressions_KeepEveryRowGroup()
    {
        FileRows rows = Types.Value;
        LiteralValue five = LiteralValue.NewInt32(5);
        ScalarExpr element = ScalarExpr.NewItemAccess(Col("li"), Lit(LiteralValue.NewInt32(0)));
        foreach (ScalarExpr filter in (ScalarExpr[])
                 [
                     Bin(BinaryOp.Eq, Col("missing"), Lit(five)),                     // no such column
                     Bin(BinaryOp.Eq, Col("li"), Lit(five)),                          // a list, not a leaf
                     Bin(BinaryOp.Eq, element, Lit(five)),                            // list elements
                     Bin(BinaryOp.Eq, Col("i8"), Col("i16")),                         // two columns
                     Bin(BinaryOp.Lt, Bin(BinaryOp.Add, Col("i8"), Lit(five)), Lit(five)), // arithmetic
                     Bin(BinaryOp.Eq, Col("dt"), Lit(LiteralValue.NewInt32(5))),      // a date against an integer
                 ])
        {
            Assert.Equal([0, 1, 2], rows.Kept(filter));
        }
        // A comparison with null is null on every row; a constant false keeps nothing.
        Assert.Empty(rows.Kept(Bin(BinaryOp.Eq, Col("i8"), Lit(LiteralValue.NewNull(Kaminari.Samidare.IR.LogicalType.Unknown)))));
        Assert.Empty(rows.Kept(Lit(LiteralValue.NewBool(false))));
    }

    [Theory]
    [InlineData("s")]
    [InlineData("st.b")]
    public void StartsWith_KeepsExactlyTheRowGroupsWithMatches(string leaf)
    {
        FileRows rows = Types.Value;
        ScalarExpr column = Leaf(leaf);
        string[] prefixes = [.. rows.Literals(column).Select(l => (string)l.BoxedValue!)
                                .SelectMany(v => Enumerable.Range(0, v.Length + 1).Select(n => v[..n])).Append("t").Append("r").Append("s\uffff").Distinct()];
        foreach (string prefix in prefixes)
        {
            ScalarExpr starts = Bin(BinaryOp.StartsWith, column, Lit(LiteralValue.NewString(prefix)));
            foreach (ScalarExpr filter in (ScalarExpr[])[starts, Not(starts)])
            {
                Assert.True(rows.Matching(filter).SequenceEqual(rows.Kept(filter)),
                    $"{Show(filter)}: kept [{string.Join(",", rows.Kept(filter))}], matches in [{string.Join(",", rows.Matching(filter))}]");
            }
        }
        Assert.Equal([1], rows.Kept(Bin(BinaryOp.StartsWith, column, Lit(LiteralValue.NewString(leaf == "s" ? "s1" : "b1")))));
    }

    [Fact]
    public void NullOutcomes_Propagate_ThroughAndOrAndCoalesce()
    {
        // n is null in all of row group 2: comparisons there are null, and coalesce(…, true) turns null into true.
        FileRows rows = Types.Value;
        ScalarExpr nIs5 = Bin(BinaryOp.Eq, Col("n"), Lit(LiteralValue.NewInt64(5)));
        ScalarExpr always = Bin(BinaryOp.Lt, Col("i8"), Lit(LiteralValue.NewInt64(100)));
        ScalarExpr never = Bin(BinaryOp.Gt, Col("i8"), Lit(LiteralValue.NewInt64(100)));
        Coalesce(Bin(BinaryOp.And, nIs5, always), [0, 1, 2]);   // null AND true is null
        Coalesce(Bin(BinaryOp.Or, nIs5, never), [0, 1, 2]);     // null OR false is null
        Coalesce(Bin(BinaryOp.And, nIs5, never), []);           // anything AND false is false
        Coalesce(Not(nIs5), [0, 1, 2]);                         // NOT null is null

        void Coalesce(ScalarExpr inner, int[] expected)
        {
            ScalarExpr filter = ScalarExpr.NewCoalesce(ListModule.OfSeq<ScalarExpr>([inner, Lit(LiteralValue.NewBool(true))]));
            Assert.True(rows.Matching(filter).All(rows.Kept(filter).Contains), Show(filter));
            Assert.Equal(expected, rows.Kept(filter));
        }
    }

    [Fact]
    public void AbsentNullCounts_AreNotZero()
    {
        // Every row group of nulls_unknown.parquet holds a null, but none says how many.
        FileRows rows = FileRows.Read(Fixture("nulls_unknown.parquet"))!;
        ScalarExpr isNull = ScalarExpr.NewUnary(UnaryOp.IsNull, Col("n"));
        Assert.Equal([0, 1, 2], rows.Matching(isNull));
        Assert.Equal([0, 1, 2], rows.Kept(isNull));
        ScalarExpr nullSafe = Bin(BinaryOp.EqMissing, Col("n"), Lit(LiteralValue.NewNull(Kaminari.Samidare.IR.LogicalType.Unknown)));
        Assert.Equal([0, 1, 2], rows.Kept(nullSafe));
        Assert.Equal([1], rows.Kept(Bin(BinaryOp.Eq, Col("n"), Lit(LiteralValue.NewInt64(12)))));
    }

    [Fact]
    public void OrderKeepingCasts_AreSeenThrough()
    {
        FileRows rows = Types.Value;
        ScalarExpr widened = ScalarExpr.NewCast(Col("i8"), LogicalTypes.Int64, false);
        Assert.Equal([2], rows.Kept(Bin(BinaryOp.Gt, widened, Lit(LiteralValue.NewInt64(0)))));
        ScalarExpr toDouble = ScalarExpr.NewCast(Col("f32"), LogicalTypes.Float64, false);
        Assert.Equal([0], rows.Kept(Bin(BinaryOp.Lt, toDouble, Lit(LiteralValue.NewFloat64(-5)))));
        // Narrowing may wrap values: not seen through.
        ScalarExpr narrowed = ScalarExpr.NewCast(Col("i16"), LogicalTypes.Int8, false);
        Assert.Equal([0, 1, 2], rows.Kept(Bin(BinaryOp.Gt, narrowed, Lit(LiteralValue.NewInt8(0)))));
    }

    // ------------------------------------------------------------------
    // Pages: column indexes prune rows, offset indexes skip pages unread
    // ------------------------------------------------------------------

    private static ScalarExpr Range(string column, long from, long to) =>
        Bin(BinaryOp.And, Bin(BinaryOp.GtEq, Col(column), Lit(LiteralValue.NewInt64(from))), Bin(BinaryOp.Lt, Col(column), Lit(LiteralValue.NewInt64(to))));

    [Theory]
    [InlineData("indexed_v1.parquet")]
    [InlineData("indexed_v2.parquet")]
    public void ColumnIndexes_KeepThePagesWithMatches_AndOnlyThoseAreRead(string name)
    {
        // 3 row groups x 1000 rows, pages of 50 rows in every column (7 leaves): key 1234..1299 lies in pages 4..5 of group 1.
        using var file = ParquetFile.OpenOnSource(Fixture(name));
        using var reader = file.OpenReader(new ParquetReadOptions { Filter = Range("key", 1234, 1300), MaxDegreeOfParallelism = 1 });
        Assert.Equal([1], reader.RowGroupsRead);
        Assert.Equal([RowRanges.Of([(200, 100)])], reader.RowsRead);
        int rows = 0;
        foreach (RecordBatch batch in reader.ReadBatches())
        {
            using (batch) rows += batch.Length;
        }
        Assert.Equal(100, rows);
        Assert.Equal(2 * 7, reader.PagesRead);
        Assert.Equal(4 * 7, reader.PagesSkipped); // pages 0..3 skipped by the offset index; pages 6..19 never reached

        // nn is null in rows 1000..1299 (whole pages, null counts given): nn > 0 cannot hold there.
        ScalarExpr nullRows = Bin(BinaryOp.And, Range("key", 1000, 1300), Bin(BinaryOp.Gt, Col("nn"), Lit(LiteralValue.NewInt64(0))));
        using var none = file.OpenReader(new ParquetReadOptions { Filter = nullRows });
        Assert.Empty(none.RowGroupsRead);
        // Rows are read whole where nothing is skipped: a full scan reads no page index.
        using var all = file.OpenReader(new ParquetReadOptions { Filter = Range("key", -5, 5000) });
        Assert.Equal([null, null, null], all.RowsRead);
    }

    [Theory]
    [InlineData("indexed_v1.parquet")]
    [InlineData("indexed_v2.parquet")]
    public void RandomFilters_OnPagedFiles_NeverLoseAMatchingRow(string name)
    {
        FileRows rows = FileRows.Read(Fixture(name))!;
        var random = new Random(name.Length * 7919);
        ScalarExpr[] columns = [Col("key"), Col("cat"), Col("nn"), Col("f"), Field(Col("st"), "a"), Field(Col("st"), "s")];
        int pruned = 0;
        for (int n = 0; n < 120; n++)
        {
            ScalarExpr filter = RandomFilter(random, rows, columns, depth: 2);
            long[] lost = rows.RowsLost(filter);
            Assert.True(lost.Length == 0, $"{Show(filter)}: rows {string.Join(",", lost.Take(10))} match but were not read");
            if (rows.RowsRead(filter).Count < 3000) pruned++;
        }
        Assert.True(pruned > 25, $"only {pruned} filters pruned rows");
    }

    [Fact]
    public void DamagedIndexes_AreUsedWhenTheyParse_AndIgnoredOtherwise_NeverShiftingRows()
    {
        ScalarExpr below10 = Bin(BinaryOp.Lt, Col("key"), Lit(LiteralValue.NewInt64(10)));
        // The column index of page 0 says 100000..200000: believed (nothing else tells), so rows 0..49 are not read.
        Assert.DoesNotContain(5L, FileRows.Read(Fixture("idx_ci_lie.parquet"))!.RowsRead(below10));
        // A column index that does not parse, an offset index that goes backwards: ignored.
        foreach (string damaged in (string[])["idx_ci_garbage.parquet", "idx_oi_bad.parquet"])
        {
            FileRows rows = FileRows.Read(Fixture(damaged))!;
            Assert.Empty(rows.RowsLost(below10));
            Assert.Empty(rows.RowsLost(Range("key", 60, 70)));
        }
        // An offset index that parses and is consistent, but page 1 starts a row late: an error or the right rows.
        using var file = ParquetFile.Open(Fixture("idx_oi_shift.parquet"));
        foreach (RowRanges selection in (RowRanges[])[RowRanges.Of([(60, 10)]), RowRanges.Of([(49, 3)]), RowRanges.Of([(120, 5)])])
        {
            try
            {
                using var reader = file.OpenReader(new ParquetReadOptions { Rows = selection, Columns = ["key"], RowIndexColumn = "row" });
                foreach (RecordBatch batch in reader.ReadBatches())
                {
                    using (batch)
                    {
                        var key = (Int64Array)batch.Column(0);
                        var row = (Int64Array)batch.Column(1);
                        for (int i = 0; i < batch.Length; i++) Assert.Equal(row.GetValue(i), key.GetValue(i));
                    }
                }
            }
            catch (ParquetFormatException e)
            {
                Assert.Contains("offset index", e.Message);
            }
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void PagesNotNeeded_AreNeverRequested(bool fetch)
    {
        ChunkReading reading = fetch ? ChunkReading.Fetch : ChunkReading.Stream;
        byte[] bytes = File.ReadAllBytes(Fixture("indexed_v1.parquet"));
        var source = new RecordingSource(bytes);
        using var file = ParquetFile.Open(source);
        using var reader = file.OpenReader(new ParquetReadOptions { Filter = Range("key", 1234, 1300), Reading = reading, FetchGap = 0, MaxDegreeOfParallelism = 1 });
        source.Reads.Clear();
        foreach (RecordBatch batch in reader.ReadBatches()) batch.Dispose();
        // Every page of every column outside group 1's pages 4..5 (and its dictionary pages) is off limits.
        List<(long Start, long End)> forbidden = [];
        for (int g = 0; g < 3; g++)
        {
            for (int leaf = 0; leaf < 7; leaf++)
            {
                if (g != 1)
                {
                    // Groups 0 and 2 are pruned whole: none of their bytes may be read.
                    var (start, length) = ParquetReader.ChunkRange(file.Thrift.RowGroups[g].Columns[leaf].MetaData!, bytes.Length, "", false);
                    forbidden.Add((start, start + length));
                    continue;
                }
                ChunkPageIndex index = file.PageIndexes.Get(g, leaf, withColumns: false)!;
                for (int page = 0; page < index.PageCount; page++)
                {
                    if (g != 1 || page is not (4 or 5)) forbidden.Add((index.Offsets[page], index.Offsets[page] + index.Sizes[page]));
                }
            }
        }
        Assert.Equal(18 * 7 + 2 * 7, forbidden.Count);
        foreach (var (at, length) in source.Reads)
        {
            foreach (var (start, end) in forbidden)
                Assert.False(at < end && start < at + length, $"read [{at}, +{length}) touches a page skipped [{start}, {end})");
        }
        Assert.Equal(2 * 7, reader.PagesRead);
    }

    [Fact]
    public void MaxRows_CountsWhatPruningLeaves_AndPlanningRunsOnce()
    {
        using var file = ParquetFile.Open(Fixture("indexed_v2.parquet"));
        // Keys 1234..1299 and 2900..2999 pass: group 1 keeps 100 rows (two pages), group 2 the last two pages.
        ScalarExpr filter = Bin(BinaryOp.Or, Range("key", 1234, 1300), Range("key", 2900, 3000));
        using var reader = file.OpenReader(new ParquetReadOptions { Filter = filter, MaxRows = 150, RowIndexColumn = "row" });
        long[] read = [.. reader.ReadBatches().SelectMany(b =>
        {
            using (b) return Enumerable.Range(0, b.Length).Select(i => ((Int64Array)b.Column(b.ColumnCount - 1)).GetValue(i)!.Value).ToArray();
        })];
        Assert.Equal([.. Enumerable.Range(1200, 100).Concat(Enumerable.Range(2900, 50)).Select(i => (long)i)], read);
        // Enumerating again gives the same plan.
        Assert.Equal(150, reader.ReadBatches().Sum(b => { using (b) return b.Length; }));
        Assert.Equal([1, 2], reader.RowGroupsRead);
    }

    // ------------------------------------------------------------------
    // Bloom filters
    // ------------------------------------------------------------------

    [Theory]
    [InlineData("pushdown:bloom.parquet")]
    [InlineData("apache/data/data_index_bloom_encoding_stats.parquet")]
    [InlineData("apache/data/data_index_bloom_encoding_with_length.parquet")]
    [InlineData("matrix/enc_duckdb_v2_uncompressed.parquet")]
    [InlineData("matrix/enc_duckdb_v2_zstd.parquet")]
    [InlineData("nested/duckdb_nested_snappy.parquet")]
    [InlineData("nested/duckdb_nested_zstd.parquet")]
    public void BloomFilters_NeverRuleOutAStoredValue(string name)
    {
        string path = name.StartsWith("pushdown:", StringComparison.Ordinal) ? Fixture(name["pushdown:".Length..]) : ParquetFixtures.FullPath(name);
        FileRows rows = FileRows.Read(path)!;
        int groups, lookups = 0, pruned = 0;
        using (var file = ParquetFile.Open(path)) groups = file.Metadata.RowGroups.Count;
        foreach (string column in rows.TopLevel)
        {
            int columnLookups = 0, columnPruned = 0;
            foreach (LiteralValue literal in rows.Literals(Col(column), most: 300))
            {
                ScalarExpr filter = Bin(BinaryOp.Eq, Col(column), Lit(literal));
                long[] lost = rows.RowsLost(filter);
                Assert.True(lost.Length == 0, $"{name} {Show(filter)}: rows {string.Join(",", lost.Take(5))} hold the value but were not read");
                columnLookups++;
                if (rows.Kept(filter).Length < groups) columnPruned++;
            }
            // In bloom.parquet every column's ranges overlap across row groups: only the Bloom filters can prune, and
            // they keep about the one group holding the value (false positives at 1 %) for every type.
            if (name == "pushdown:bloom.parquet")
                Assert.True(columnPruned > columnLookups * 9 / 10, $"{column}: {columnPruned} of {columnLookups} lookups pruned");
            lookups += columnLookups;
            pruned += columnPruned;
        }
        Assert.True(lookups > 0);
    }

    [Fact]
    public void BloomFilters_AreBelieved_WhenAsked()
    {
        FileRows rows = FileRows.Read(Fixture("bloom_zero.parquet"))!;
        LiteralValue present = rows.Literals(Col("i64"))[0];
        ScalarExpr filter = Bin(BinaryOp.Eq, Col("i64"), Lit(present));
        Assert.Empty(rows.Kept(filter)); // every bitset is empty: every lookup says absent
        using var file = ParquetFile.Open(Fixture("bloom_zero.parquet"));
        using var off = file.OpenReader(new ParquetReadOptions { Filter = filter, BloomFilters = false });
        Assert.Equal([0, 1, 2], off.RowGroupsRead);
        // Null-safe equality and IN look values up too; a range test does not.
        Assert.Empty(rows.Kept(Bin(BinaryOp.EqMissing, Col("i64"), Lit(present))));
        Assert.Empty(rows.Kept(ScalarExpr.NewInList(Col("s"), InListSource.NewLiterals(ListModule.OfSeq<LiteralValue>(rows.Literals(Col("s"))[..3])), false, false)));
        Assert.Equal([0, 1, 2], rows.Kept(Bin(BinaryOp.GtEq, Col("i64"), Lit(LiteralValue.NewInt64(long.MinValue)))));
        // A value absent everywhere makes != true on every row (an equality on the leaf has its filters loaded).
        ScalarExpr either = Bin(BinaryOp.Or, Bin(BinaryOp.NotEq, Col("i64"), Lit(present)), Bin(BinaryOp.Eq, Col("i64"), Lit(present)));
        Assert.Equal([0, 1, 2], rows.Kept(either));
    }

    [Fact]
    public void FloatingZeros_AreLookedUpInBothSigns()
    {
        // f32: +0 in group 0, -0 in group 2; f64: -0 in group 0, +0 in group 2. The IR's = holds between them.
        FileRows rows = FileRows.Read(Fixture("bloom.parquet"))!;
        foreach (var (column, zero) in (ValueTuple<string, LiteralValue>[])[("f32", LiteralValue.NewFloat32(-0f)), ("f32", LiteralValue.NewFloat32(0f)),
                                                                             ("f64", LiteralValue.NewFloat64(0.0)), ("f64", LiteralValue.NewFloat64(-0.0))])
        {
            ScalarExpr filter = Bin(BinaryOp.Eq, Col(column), Lit(zero));
            Assert.Equal([0, 2], rows.Matching(filter));
            Assert.Empty(rows.RowsLost(filter));
        }
    }

    /// <summary>Bytes read at offsets, every read recorded.</summary>
    private sealed class RecordingSource(byte[] bytes) : IRandomAccess
    {
        public List<(long Offset, int Length)> Reads { get; } = [];

        public long Length => bytes.Length;

        public int ReadAt(long offset, Span<byte> destination)
        {
            lock (Reads) Reads.Add((offset, destination.Length));
            int count = (int)Math.Clamp(bytes.Length - offset, 0, destination.Length);
            bytes.AsSpan((int)offset, count).CopyTo(destination);
            return count;
        }

        public ValueTask<int> ReadAtAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ReadAt(offset, destination.Span));
    }

    // ------------------------------------------------------------------
    // The corpus: no row group with matches dropped, whatever wrote the file
    // ------------------------------------------------------------------

    private static readonly Lazy<JsonDocument> Values = new(() => JsonDocument.Parse(File.ReadAllBytes(Path.Combine(ParquetFixtures.Root, "values.json"))));

    public static TheoryData<string> CorpusFiles => [.. Values.Value.RootElement.GetProperty("files").EnumerateObject().Select(p => p.Name)];

    [Theory]
    [MemberData(nameof(CorpusFiles))]
    public void CorpusFiles_NeverLoseARowGroupWithMatches(string name)
    {
        FileRows? rows;
        try
        {
            rows = FileRows.Read(ParquetFixtures.FullPath(name), maxRows: 20_000);
        }
        catch (Exception)
        {
            return; // refused files are covered by ParquetValueTests
        }
        if (rows is null) return; // too large to evaluate row by row
        foreach (string column in rows.TopLevel)
        {
            ScalarExpr c = Col(column);
            foreach (LiteralValue literal in rows.Literals(c, perRowGroup: 2, most: 12))
            {
                foreach (BinaryOp op in Comparisons)
                {
                    ScalarExpr filter = Bin(op, c, Lit(literal));
                    long[] lost = rows.RowsLost(filter);
                    Assert.True(lost.Length == 0, $"{name} {Show(filter)}: rows {string.Join(",", lost.Take(10))} match but were not read");
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // IR building
    // ------------------------------------------------------------------

    private static readonly BinaryOp[] Comparisons = [BinaryOp.Eq, BinaryOp.NotEq, BinaryOp.Lt, BinaryOp.LtEq, BinaryOp.Gt, BinaryOp.GtEq];

    internal static ScalarExpr Col(string name) => ScalarExpr.NewColumn(name);

    internal static ScalarExpr Lit(LiteralValue value) => ScalarExpr.NewLiteral(value);

    internal static ScalarExpr Bin(BinaryOp op, ScalarExpr left, ScalarExpr right) => ScalarExpr.NewBinary(op, left, right);

    internal static ScalarExpr Not(ScalarExpr expr) => ScalarExpr.NewUnary(UnaryOp.Not, expr);

    internal static ScalarExpr Field(ScalarExpr parent, string name) =>
        ScalarExpr.NewFunction(ScalarFunction.StructFieldByName, ListModule.OfSeq<ScalarExpr>([parent, Lit(LiteralValue.NewString(name))]),
            Kaminari.Samidare.IR.LogicalType.Unknown);

    /// <summary>A column, or a struct field by a dotted path.</summary>
    private static ScalarExpr Leaf(string dotted)
    {
        string[] parts = dotted.Split('.');
        ScalarExpr expr = Col(parts[0]);
        foreach (string part in parts[1..]) expr = Field(expr, part);
        return expr;
    }

    private static string Show(ScalarExpr e) => e switch
    {
        ScalarExpr.Column c => c.name,
        ScalarExpr.Literal l => l.value.BoxedValue switch
        {
            null => "null",
            string s => $"'{s}'",
            IFormattable f => f.ToString(null, System.Globalization.CultureInfo.InvariantCulture),
            var v => v.ToString() ?? "?",
        },
        ScalarExpr.Binary b => $"({Show(b.left)} {b.op} {Show(b.right)})",
        ScalarExpr.Unary u => $"{u.op}({Show(u.expr)})",
        ScalarExpr.Function f => $"{Show(f.args.Head)}.{Show(f.args.Tail.Head)}",
        ScalarExpr.InList i => $"{Show(i.expr)} {(i.negated ? "NOT IN" : "IN")}{(i.nullsEqual ? "~" : "")} ({string.Join(", ", ((InListSource.Literals)i.source).Item.Select(v => Show(Lit(v))))})",
        ScalarExpr.Coalesce c => $"coalesce({string.Join(", ", c.exprs.Select(Show))})",
        ScalarExpr.Cast c => $"cast({Show(c.expr)})",
        _ => e.GetType().Name,
    };

    // The IR's semantics on one row: IrEvaluator.

    private static Func<string, object?> Row(RecordBatch batch, int row) =>
        name => batch.Schema.GetFieldIndex(name) is >= 0 and var c ? Value(batch.Column(c), row) : IrEvaluator.Missing.Instance;

    /// <summary>A value read, as the IR's literal of its type would hold it; <see cref="Missing"/> for types not compared here.</summary>
    private static object? Value(IArrowArray array, int i)
    {
        if (array.IsNull(i)) return null;
        return array switch
        {
            Int8Array a => a.GetValue(i),
            Int16Array a => a.GetValue(i),
            Int32Array a => a.GetValue(i),
            Int64Array a => a.GetValue(i),
            UInt8Array a => a.GetValue(i),
            UInt16Array a => a.GetValue(i),
            UInt32Array a => a.GetValue(i),
            UInt64Array a => a.GetValue(i),
            HalfFloatArray a => a.GetValue(i),
            FloatArray a => a.GetValue(i),
            DoubleArray a => a.GetValue(i),
            Decimal128Array a => TryDecimal(a, i),
            BooleanArray a => a.GetValue(i),
            StringArray a => a.GetString(i),
            StringViewArray a => a.GetString(i),
            Date32Array a => a.GetDateOnly(i),
            // Only values a 100 ns tick holds exactly (the CLR types round finer ones).
            TimestampArray a => ((TimestampType)a.Data.DataType).Unit == Apache.Arrow.Types.TimeUnit.Nanosecond && a.GetValue(i) % 100 != 0
                ? IrEvaluator.Missing.Instance : a.GetTimestamp(i),
            Time32Array a => a.GetTime(i),
            Time64Array a => ((Time64Type)a.Data.DataType).Unit == Apache.Arrow.Types.TimeUnit.Nanosecond && a.GetValue(i) % 100 != 0
                ? IrEvaluator.Missing.Instance : a.GetTime(i),
            DictionaryArray d => Value(d.Dictionary, DictionaryIndex(d.Indices, i)),
            StructArray s => Fields(s, i),
            _ => IrEvaluator.Missing.Instance,
        };

        // Only values System.Decimal holds exactly (GetValue rounds a 38-digit value to 28 or 29 digits).
        static object TryDecimal(Decimal128Array a, int i)
        {
            string exact = a.GetString(i);
            var culture = System.Globalization.CultureInfo.InvariantCulture;
            return decimal.TryParse(exact, System.Globalization.NumberStyles.Float, culture, out decimal d)
                   && d.ToString("F" + ((Decimal128Type)a.Data.DataType).Scale, culture) == exact ? d : IrEvaluator.Missing.Instance;
        }

        static int DictionaryIndex(IArrowArray indices, int i) => indices switch
        {
            Int8Array x => x.GetValue(i)!.Value,
            Int16Array x => x.GetValue(i)!.Value,
            Int32Array x => x.GetValue(i)!.Value,
            UInt8Array x => x.GetValue(i)!.Value,
            UInt16Array x => x.GetValue(i)!.Value,
            UInt32Array x => (int)x.GetValue(i)!.Value,
            Int64Array x => (int)x.GetValue(i)!.Value,
            _ => throw new NotSupportedException(indices.GetType().Name),
        };

        static Dictionary<string, object?> Fields(StructArray s, int i)
        {
            var type = (StructType)s.Data.DataType;
            Dictionary<string, object?> fields = [];
            for (int f = 0; f < type.Fields.Count; f++) fields[type.Fields[f].Name] = Value(s.Fields[f], i);
            return fields;
        }
    }

    /// <summary>The IR literal holding a value read from a column of <paramref name="type"/>; null where none is made.</summary>
    private static LiteralValue? LiteralOf(IArrowType type, object value) => (type, value) switch
    {
        (DictionaryType d, _) => LiteralOf(d.ValueType, value),
        (_, sbyte v) => LiteralValue.NewInt8(v),
        (_, short v) => LiteralValue.NewInt16(v),
        (_, int v) => LiteralValue.NewInt32(v),
        (_, long v) => LiteralValue.NewInt64(v),
        (_, byte v) => LiteralValue.NewUInt8(v),
        (_, ushort v) => LiteralValue.NewUInt16(v),
        (_, uint v) => LiteralValue.NewUInt32(v),
        (_, ulong v) => LiteralValue.NewUInt64(v),
        (_, Half v) => LiteralValue.NewFloat16(v),
        (_, float v) => LiteralValue.NewFloat32(v),
        (_, double v) => LiteralValue.NewFloat64(v),
        (_, decimal v) => LiteralValue.NewDecimal(v),
        (_, bool v) => LiteralValue.NewBool(v),
        (_, string v) => LiteralValue.NewString(v),
        (_, DateOnly v) => LiteralValue.NewDate(v),
        (TimestampType t, DateTimeOffset v) => string.IsNullOrEmpty(t.Timezone)
            ? LiteralValue.NewDateTime(new DateTime(v.UtcTicks, DateTimeKind.Unspecified))
            : LiteralValue.NewDateTimeOffset(v),
        (_, TimeOnly v) => LiteralValue.NewTimeOnly(v),
        _ => null,
    };

    /// <summary>The rows of a file per row group, top-level columns by name, for evaluating filters and asking what a filter keeps.</summary>
    private sealed class FileRows
    {
        private readonly string _path;
        private readonly List<Dictionary<string, object?>>[] _groups;
        private readonly Dictionary<string, IArrowType> _types;

        private FileRows(string path, List<Dictionary<string, object?>>[] groups, Dictionary<string, IArrowType> types)
        {
            _path = path;
            _groups = groups;
            _types = types;
        }

        public IEnumerable<string> TopLevel => _types.Keys;

        /// <summary>Reads every row group; null when the file holds more than <paramref name="maxRows"/> rows.</summary>
        public static FileRows? Read(string path, long maxRows = long.MaxValue)
        {
            using var file = ParquetFile.Open(path);
            if (file.Metadata.RowCount > maxRows) return null;
            var groups = new List<Dictionary<string, object?>>[file.Metadata.RowGroups.Count];
            Dictionary<string, IArrowType> types = [];
            for (int g = 0; g < groups.Length; g++)
            {
                groups[g] = [];
                using var reader = file.OpenReader(new ParquetReadOptions { RowGroups = [g], MaxDegreeOfParallelism = 1 });
                foreach (Field f in reader.Schema.FieldsList) types[f.Name] = f.DataType;
                foreach (RecordBatch batch in reader.ReadBatches())
                {
                    using (batch)
                    {
                        for (int i = 0; i < batch.Length; i++)
                        {
                            Dictionary<string, object?> row = [];
                            for (int c = 0; c < batch.ColumnCount; c++) row[batch.Schema.GetFieldByIndex(c).Name] = Value(batch.Column(c), i);
                            groups[g].Add(row);
                        }
                    }
                }
            }
            return new FileRows(path, groups, types);
        }

        /// <summary>Rows the filter keeps (true) that a reader with the filter does not deliver (none, when pruning is sound).</summary>
        public long[] RowsLost(ScalarExpr filter, ParquetReadOptions? options = null)
        {
            HashSet<long> read = RowsRead(filter, options);
            List<long> lost = [];
            long first = 0;
            foreach (List<Dictionary<string, object?>> group in _groups)
            {
                for (int i = 0; i < group.Count; i++)
                {
                    if (IrEvaluator.Eval(filter, name => group[i].TryGetValue(name, out object? v) ? v : null) is true && !read.Contains(first + i)) lost.Add(first + i);
                }
                first += group.Count;
            }
            return [.. lost];
        }

        /// <summary>
        /// The numbers of the rows a reader with the filter delivers, from its plan (that a plan's rows are what is
        /// delivered, ParquetRowSelectionTests checks).
        /// </summary>
        public HashSet<long> RowsRead(ScalarExpr filter, ParquetReadOptions? options = null)
        {
            using var file = ParquetFile.Open(_path);
            using var reader = file.OpenReader((options ?? new ParquetReadOptions()) with { Filter = filter, MaxDegreeOfParallelism = 1 });
            long[] first = new long[file.Metadata.RowGroups.Count];
            for (int g = 1; g < first.Length; g++) first[g] = first[g - 1] + file.Metadata.RowGroups[g - 1].RowCount;
            HashSet<long> rows = [];
            foreach (var (g, read) in reader.RowGroupsRead.Zip(reader.RowsRead))
            {
                foreach (var (start, count) in read ?? RowRanges.All(file.Metadata.RowGroups[g].RowCount))
                {
                    for (long i = 0; i < count; i++) rows.Add(first[g] + start + i);
                }
            }
            return rows;
        }

        /// <summary>The row groups holding a row the filter keeps (true).</summary>
        public int[] Matching(ScalarExpr filter) =>
            [.. Enumerable.Range(0, _groups.Length).Where(g => _groups[g].Any(row => IrEvaluator.Eval(filter, name => row.TryGetValue(name, out object? v) ? v : null) is true))];

        /// <summary>The row groups a reader with the filter reads.</summary>
        public int[] Kept(ScalarExpr filter)
        {
            using var file = ParquetFile.Open(_path);
            using var reader = file.OpenReader(new ParquetReadOptions { Filter = filter, MaxDegreeOfParallelism = 1 });
            return [.. reader.RowGroupsRead];
        }

        /// <summary>
        /// Literals made from the values of a column or struct field: every distinct value (or the smallest and
        /// largest <paramref name="perRowGroup"/> of each row group), plus values between stored ones.
        /// </summary>
        public LiteralValue[] Literals(ScalarExpr column, int perRowGroup = int.MaxValue, int most = int.MaxValue)
        {
            List<object> values = [];
            IArrowType? type = null;
            foreach (List<Dictionary<string, object?>> group in _groups)
            {
                object[] present = [.. group.Select(row => IrEvaluator.Eval(column, name => row.TryGetValue(name, out object? v) ? v : null))
                                            .Where(v => v is not null and not IrEvaluator.Missing and not IReadOnlyDictionary<string, object?>)
                                            .Select(v => v!)];
                if (perRowGroup == int.MaxValue) values.AddRange(present);
                else
                {
                    object[] sorted = [.. present.Order(Comparer<object>.Create(IrEvaluator.Compare))];
                    values.AddRange([.. sorted.Take(perRowGroup), .. sorted.TakeLast(perRowGroup)]);
                }
            }
            type = TypeOf(column);
            if (type is null) return [];
            List<LiteralValue> literals = [];
            foreach (object v in values.Distinct().Take(most))
            {
                if (LiteralOf(type, v) is { } literal) literals.Add(literal);
                // Between stored values: half a step above (integers, decimals) or one tick (timestamps, times).
                LiteralValue? between = (type, v) switch
                {
                    (Decimal128Type d, decimal m) => LiteralValue.NewDecimal(m + 0.5m * (decimal)Math.Pow(10, -d.Scale)),
                    (_, sbyte or short or int or long or byte or ushort or uint) => LiteralValue.NewDecimal(Convert.ToDecimal(v, System.Globalization.CultureInfo.InvariantCulture) + 0.5m),
                    (TimestampType { Unit: not Apache.Arrow.Types.TimeUnit.Nanosecond } t, DateTimeOffset o) when string.IsNullOrEmpty(t.Timezone) =>
                        LiteralValue.NewDateTime(new DateTime(o.UtcTicks + 1, DateTimeKind.Unspecified)),
                    _ => null,
                };
                if (between is not null) literals.Add(between);
            }
            return [.. literals];
        }

        private IArrowType? TypeOf(ScalarExpr column) => column switch
        {
            ScalarExpr.Column c => _types.GetValueOrDefault(c.name),
            ScalarExpr.Function f when TypeOf(f.args.Head) is StructType s
                                       && f.args.Tail.Head is ScalarExpr.Literal { value: LiteralValue.String name } =>
                s.Fields.FirstOrDefault(x => x.Name == name.Item)?.DataType,
            _ => null,
        };
    }
}
