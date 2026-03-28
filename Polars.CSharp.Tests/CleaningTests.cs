using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public class CleaningTests
{
    [Fact]
    public void Test_Forward_Backward_Fill()
    {
        var content = "val\n1\n\n\n2\n\n"; 
        
        using var csv = new DisposableFile(content, ".csv");
        using var df = DataFrame.ReadCsv(csv.Path);
        
        // Forward Fill (limit=null -> 0 -> Infinite)
        using var ff = df.Select(Col("val").ForwardFill().Alias("ff"));
        
        Assert.Equal(1, ff.GetValue<int>(0,"ff"));
        Assert.Equal(1, ff.GetValue<int>(1,"ff")); 
        Assert.Equal(1, ff.GetValue<int>(2,"ff")); 
        Assert.Equal(2, ff.GetValue<int>(3,"ff"));
        Assert.Equal(2, ff.GetValue<int>(4,"ff")); 
    }
    [Fact]
    public void Test_Sampling()
    {
        var rows = Enumerable.Range(0, 100).Select(i => new { Val = i });
        using var df = DataFrame.From(rows);
        Assert.Equal(100, df.Height);

        // Sample N=10
        using var sampleN = df.Sample(n: 10, seed: 42);
        Assert.Equal(10, sampleN.Height);

        // Sample Frac=0.1 (10%)
        using var sampleFrac = df.Sample(fraction: 0.1, seed: 42);
        Assert.Equal(10, sampleFrac.Height);
    }
    [Fact]
    public void Test_Data_Cleaning_Trio()
    {
        var content = "A,B,C\n1,x,10\n,y,20\n3,,30\n";
        
        using var csv = new DisposableFile(content, ".csv");
        using var df = DataFrame.ReadCsv(csv.Path);

        // --- 1. FillNull ---
        using var filledDf = df.WithColumns(
            Col("A").FillNull(0), 
            Col("B").FillNull("unknown")
        );
        
        Assert.Equal(0, filledDf.GetValue<int>(1,"A")); // null -> 0
        Assert.Equal("unknown", filledDf.GetValue<string>(2,"B")); // null -> unknown

        // --- 2. DropNulls ---
        using var dfDirty = DataFrame.ReadCsv(csv.Path);
        using var droppedDf = dfDirty.DropNulls();
        
        Assert.Equal(1, droppedDf.Height); 
        Assert.Equal(1, droppedDf.GetValue<int>(0,"A"));
    }
    [Fact]
    public void Test_Cleaning_Dirty_Data()
    {
        var df = DataFrame.FromColumns(new 
        {
            RawData = new object[] { 100, 200.5, "NotANumber", "NaN", null }
        });

        // Clean
        var cleanExpr = Col("RawData")
            // Step A: cast to Double，strict=false
            // "100" -> 100.0
            // "200.5" -> 200.5
            // "NotANumber" -> null
            // "NaN" -> NaN 
            // null -> null
            .Cast(DataType.Float64, strict: false)
            
            // Step B: Handle NaN 
            .FillNan(0) 
            
            // Step C: Handle Null 
            .FillNull(0);

        var result = df.Select(cleanExpr.Alias("Cleaned"));

        var rows = result["Cleaned"].ToArray<double?>();
        
        Assert.Equal(100.0, rows[0]);
        Assert.Equal(200.5, rows[1]);
        Assert.Equal(0.0, rows[2]); // "NotANumber" -> null -> 0
        Assert.Equal(0.0, rows[3]); // "NaN" -> NaN -> 0
        Assert.Equal(0.0, rows[4]); // null -> 0
    }
    [Fact]
    public void Test_Series_Unique_And_Duplicated()
    {
        // Data: [1, 2, 2, 3]
        // IsUnique -> [T, F, F, T] 
        // IsDuplicated -> [F, T, T, F]
        
        using var s = Series.From("nums", [1, 2, 2, 3]);

        using var unique = s.UniqueStable();
        Assert.Equal(3, unique.Length);
        Assert.Equal(1, unique[0]);
        Assert.Equal(2, unique[1]);
        Assert.Equal(3, unique[2]);

        using var dupMask = s.IsDuplicated();
        Assert.Equal(DataTypeKind.Boolean, dupMask.DataType.Kind);
        // 1->F, 2->T, 2->T, 3->F
        Assert.False((bool)dupMask[0]);
        Assert.True((bool)dupMask[1]);
        Assert.True((bool)dupMask[2]);
        Assert.False((bool)dupMask[3]);

        //  IsUnique 
        using var uniqMask = s.IsUnique();
        Assert.True((bool)uniqMask[0]);  
        Assert.False((bool)uniqMask[1]); 
        Assert.False((bool)uniqMask[2]);
        Assert.True((bool)uniqMask[3]);  
    }

    [Fact]
    public void Test_Expr_Unique_Context()
    {
        using var df = DataFrame.FromColumns(new 
        {
            Group = new[] { "A", "A", "B", "B", "B" },
            Val = new[]   { 1,   1,   2,   3,   2 }
        });

        using var res = df.Lazy()
            .GroupBy(Col("Group"))
            .Agg(
                Col("Val").Unique().Alias("UniqueVals"),
                Col("Val").IsDuplicated().Sum().Alias("DupCount") 
            )
            .Sort("Group")
            .Collect();

        var groupA_DupCount = (uint)res["DupCount"][0]; 
        // A: [1, 1]. IsDuplicated -> [True, True]. Sum = 2.
        Assert.Equal(2u, groupA_DupCount);

        var groupB_DupCount = (uint)res["DupCount"][1];
        // B: [2, 3, 2]. IsDuplicated -> [True, False, True]. Sum = 2.
        Assert.Equal(2u, groupB_DupCount);
    }
    [Fact]
    public void TestDropNulls()
    {
        var s = Series.From("data", new int?[] { 1, null, 2, null, 3 });
        var cleanSeries = s.DropNulls();

        Assert.Equal(3, cleanSeries.Length);

        // Expr.DropNulls() via DataFrame
        var df = DataFrame.FromColumns(new
        {
            vals = new int?[] { 10, null, 20 }
        });

        var resultDf = df.Select(Col("vals").DropNulls());
        
        Assert.Equal(2, resultDf.Height);
        Assert.Equal(10, resultDf["vals"][0]);
        Assert.Equal(20, resultDf["vals"][1]);
    }

    [Fact]
    public void TestDropNans()
    {
        var s = new Series("floats", [1.5, double.NaN, 2.5, double.NaN, 3.5]);
        var cleanSeries = s.DropNans();

        Assert.Equal(3, cleanSeries.Length);
        
        var arr = cleanSeries.ToArray<double>();
        Assert.Equal(1.5, arr[0]);
        Assert.Equal(2.5, arr[1]);
        Assert.Equal(3.5, arr[2]);

        var df = DataFrame.FromSeries(
            Series.From("f",[double.NaN, 100.0, double.NaN])
        );

        var resultDf = df.Select(Col("f").DropNans());
        
        Assert.Equal(1, resultDf.Height);
        Assert.Equal(100.0, resultDf["f"][0]);
    }
    [Fact]
    [Trait("Cleaning","IsDuplicated")]
    public void Test_DataFrame_IsDuplicated_IsUnique()
    {
        using var scoresDf = DataFrame.FromColumns(new 
        {
            student = new[] { "Alice", "Alice", "Bob" },
            year    = new[] { 2023,    2023,    2023 },
            score   = new[] { 85,      85,      70 },
            note    = new[] { "Score1", "Score1", "Score3" } 
        });
        var dupDf = scoresDf.Filter(scoresDf.IsDuplicated());
        Assert.Equal(2L,dupDf.Height);

        var uniDf = scoresDf.Filter(scoresDf.IsUnique());
        Assert.Equal(1L,uniDf.Height);

    }
}