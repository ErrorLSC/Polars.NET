using Pl = Polars.CSharp.Polars;
using Cs = Polars.CSharp.Polars.Selectors;

namespace Polars.CSharp.Tests;

public class SelectorTests
{
    [Fact]
    public void Test_Selector_All_Exclude()
    {
        // Prepare Data
        var df = DataFrame.FromColumns(new 
        {
            Id = new[] { 1, 2 },
            Name = new[] { "Alice", "Bob" },
            Age = new[] { 25, 30 },
            Secret = new[] { "pass1", "pass2" }
        });

        // Use Selector: All().Exclude(...)
        // Scene：Keep data but exclude ID and Secret
        // Here implictly: Selector -> Expr
        var result = df.Select(
            Cs.All().Exclude("Id", "Secret")
        );

        // Assert Results
        Assert.Equal(2, result.Width);
        
        // Only Name and Age column left
        Assert.Equal("Name", result.Column(0).Name);
        Assert.Equal("Age", result.Column(1).Name);
        
        // Id and Secret are not there
        Assert.Throws<ArgumentException>(() => result["Id"]);
        Assert.Throws<ArgumentException>(() => result["Secret"]);
    }

    [Fact]
    public void Test_Selector_Operation()
    {
      
        var df = DataFrame.FromColumns(new 
        {
            Id = new[] { 1, 2 },
            Val1 = new[] { 10, 20 },
            Val2 = new[] { 100, 200 }
        });

        // select (All - "Id") * 2
        var result = df.Select(
            (Cs.All().Exclude("Id") * 2).Name.Suffix("_Scaled") 
        );

        Assert.Equal(20, result[0, "Val1_Scaled"]); // 10 * 2
        Assert.Equal(200, result[0, "Val2_Scaled"]); // 100 * 2
        
        Assert.Throws<ArgumentException>(() => result["Id"]);
    }
    
    [Fact]
    public void Test_Selector_Full_Capabilities()
    {
        var df = DataFrame.FromColumns(new 
        {
            Id = new[] { 1, 2 },
            Meta_Info = new[] { "A", "B" },
            Price_US = new[] { 10.5, 20.0 },
            Price_EU = new[] { 8.5, 15.0 },
            Qty_2023 = new[] { 100, 200 },
            Qty_2024 = new[] { 110, 220 },
            Timestamp = new[] { DateTime.Now, DateTime.Now }
        });

        // Target：
        // A. Choose all columns name start with Price then convert to int
        // B. Choose all columns name end with 2024
        // C. Exclude Id and Meta_Info
        // D. Choose Datetime Column
        
        var result = df.Select(
            // A. StartsWith + Math
            (Cs.StartsWith("Price") * 100).Name.Suffix("_Cents"),

            // B. EndsWith
            Cs.EndsWith("2024"),

            // C. Type Selector (Datetime)
            Cs.Datetime()
        );

        Assert.Equal(4, result.Width); 

        Assert.Contains("Price_US_Cents", result.ColumnNames);
        Assert.Contains("Qty_2024", result.ColumnNames);
        Assert.Contains("Timestamp", result.ColumnNames);
        
        Assert.DoesNotContain("Id", result.ColumnNames);

        Assert.Equal(1050.0, result[0, "Price_US_Cents"]); // 10.5 * 100
    }
    [Fact]
    [Trait("Selector", "SetOperationsProMax")]
    public void Test_Selector_Set_Operations_Pro_Max()
    {
        using var df = DataFrame.FromSeries(
            Series.From("A", [1, 2, 3]),           // Int
            Series.From("B", [1.1, 2.2, 3.3]),     // Float
            Series.From("C", ["x", "y", "z"]),     // String
            Series.From("D", [true, false, true])  // Boolean
        );

        // INTERSECTION: Numeric & Float -> B
        using var resAnd = df.Select(Cs.Numeric() & Cs.Float());
        Assert.Equal(["B"], resAnd.Columns);

        // UNION: Float | String -> B and C
        using var resOr = df.Select(Cs.Float() | Cs.String());
        Assert.Equal(["B", "C"], resOr.Columns);

        // DIFFERENCE: Numeric - Float -> A
        using var resSub = df.Select(Cs.Numeric() - Cs.Float());
        Assert.Equal(["A"], resSub.Columns);

        // NOT: ~Numeric ->  C(String) and D(Boolean)
        using var resNot = df.Select(~Cs.Numeric());
        Assert.Equal(["C", "D"], resNot.Columns);

        // SYMMETRIC DIFFERENCE (XOR): 
        // Numeric | String -> A, B, C
        // Float | Boolean -> B, D
        // XOR : A, C, D
        using var resXor = df.Select((Cs.Numeric() | Cs.String()) ^ (Cs.Float() | Cs.Boolean()));
        Assert.Equal(["A", "C", "D"], [.. resXor.Columns.OrderBy(c => c)]);
        
        using var resBang = df.Select(!Cs.Numeric());
        Assert.Equal(["C", "D"], resBang.Columns);
    }

    [Fact]
    [Trait("Selector", "Datetime")]
    public void Test_Datetime_Selectors()
    {
        using var baseDf = DataFrame.FromSeries(
            Series.From("ts_str", ["2024-01-01T00:00:00", "2024-01-02T00:00:00"])
        );

        using var df = baseDf.Lazy()
            .Select(
                Pl.Col("ts_str").Str.ToDatetime().Alias("dt_naive"), // Unset
                Pl.Col("ts_str").Str.ToDatetime().Dt.ReplaceTimeZone("UTC").Alias("dt_utc"), // UTC
                Pl.Col("ts_str").Str.ToDatetime().Dt.ReplaceTimeZone("Asia/Shanghai").Alias("dt_shanghai"), // Asia/Shanghai
                (Pl.Col("ts_str").Str.ToDatetime() - Pl.Col("ts_str").Str.ToDatetime()).Alias("duration_col") // Duration
            ).Collect();

        // Datetime()
        using var resAll = df.Select(Cs.Datetime());
        Assert.Equal(["dt_naive", "dt_shanghai","dt_utc"], [.. resAll.Columns.OrderBy(c => c)]);

        // DatetimeNaive()
        using var resNaive = df.Select(Cs.DatetimeNaive());
        Assert.Equal(["dt_naive"], resNaive.Columns);

        // DatetimeAware() 
        using var resAware = df.Select(Cs.DatetimeAware());
        Assert.Equal(["dt_shanghai", "dt_utc"], [.. resAware.Columns.OrderBy(c => c)]);

        // DatetimeExact()
        using var resExact = df.Select(Cs.DatetimeExact("Asia/Shanghai"));
        Assert.Equal(["dt_shanghai"], resExact.Columns);

        // Duration() 
        using var resDuration = df.Select(Cs.Duration());
        Assert.Equal(["duration_col"], resDuration.Columns);
    }
    [Fact]
    [Trait("Selector", "NestedTypes")]
    public void Test_List_And_Array_Selectors_FixedType()
    {
        using var df = DataFrame.FromSeries(
            Series.From("normal_int", [1, 2]),
            
            Series.From("list_int", new int[][] { [1, 2], [3] }),
            Series.From("list_str", new string[][] { ["a", "b"], ["c"] }),
            
            Series.From("array_float_w2", new float[,] { { 1.1f, 2.2f }, { 3.3f, 4.4f } }),
            Series.From("array_int_w3", new int[,] { { 1, 2, 3 }, { 4, 5, 6 } })
        );


        using var resStrList = df.Select(Cs.List(Cs.String()));
        Assert.Single(resStrList.Columns);
        Assert.Equal("list_str", resStrList.Columns[0]);

        using var resFloatArray = df.Select(Cs.Array(Cs.Float()));
        Assert.Equal(["array_float_w2"], resFloatArray.Columns);

        using var resW3Array = df.Select(Cs.Array(width: 3));
        Assert.Equal(["array_int_w3"], resW3Array.Columns);

        using var resCombined = df.Select(Cs.Array(Cs.Float(), width: 2));
        Assert.Equal(["array_float_w2"], resCombined.Columns);
    }
}
