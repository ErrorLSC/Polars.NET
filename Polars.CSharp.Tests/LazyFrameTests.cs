using static Polars.CSharp.Polars;
using Cs = Polars.CSharp.Polars.Selectors;

namespace Polars.CSharp.Tests;

public class LazyFrameTests
{
    [Fact]
    public void Test_ScanCsv_Filter_Select()
    {
        var csvContent = @"name,age,salary
Alice,25,50000
Bob,30,60000
Charlie,35,70000
David,40,80000";
        using var csv = new DisposableFile(csvContent, ".csv");

        using var lf = LazyFrame.ScanCsv(csv.Path);
        using var lf_copyed = lf.Clone();
        using var df = lf.Collect();

        Assert.Equal(4, df.Height);
        Assert.Equal(3, df.Width);
        Assert.Contains("name", df.Columns);

        using var filtered = lf_copyed
            .Filter(Col("age") > 30)
            .Select(Col("name"), Col("salary"));
        using var resultDf = filtered.Collect();

        Assert.Equal(2, resultDf.Height);
        Assert.Equal(2, resultDf.Width); // name, salary

        var nameCol = resultDf.Column("name");
        
        Assert.NotNull(nameCol);
        Assert.Equal("Charlie", nameCol.GetValue<string>(0));
        Assert.Equal("David", nameCol.GetValue<string>(1));

    }
    [Fact]
    public void Test_Lazy_Concat_Horizontal_And_Safety()
    {
        using var csv1 = new DisposableFile("id\n1\n2",".csv");
        using var lf1 = LazyFrame.ScanCsv(csv1.Path);

        using var csv2 = new DisposableFile("name\nAlice\nBob",".csv");
        using var lf2 = LazyFrame.ScanCsv(csv2.Path);

        var concatLf = LazyFrame.Concat([lf1, lf2], ConcatType.Horizontal);
        
        using var df = concatLf.Collect();
        
        Assert.Equal(2, df.Height);
        Assert.Equal(2, df.Width); // id, name

        Assert.Equal(1, df.Column("id").GetValue<long>(0));
        Assert.Equal("Alice", df.Column("name").GetValue<string>(0));
        
        using var df1_again = lf1.Select(Col("id") * Lit(10)).Collect();
        Assert.Equal(2, df1_again.Height);
        
        Assert.Equal(10, df1_again.Column("id").GetValue<long>(0));
    }
    
    [Fact]
    public void Test_Lazy_Concat_Diagonal()
    {
        // LF1: [A, B]
        using var csv1 = new DisposableFile("A,B\n1,10",".csv");
        using var lf1 = LazyFrame.ScanCsv(csv1.Path);

        // LF2: [B, C]
        using var csv2 = new DisposableFile("B,C\n20,300",".csv");
        using var lf2 = LazyFrame.ScanCsv(csv2.Path);

        // Diagonal Concat (Lazy)
        // Row 0 (From LF1): A=1,    B=10,   C=null (补位)
        // Row 1 (From LF2): A=null, B=20,   C=300
        var concatLf = LazyFrame.Concat([lf1, lf2], ConcatType.Diagonal);
        
        using var df = concatLf.Collect();
        
        Assert.Equal(2, df.Height);
        Assert.Equal(3, df.Width); // A, B, C

        // --- Row 0 ---
        Assert.Equal(1, df.GetValue<int?>(0, "A"));
        Assert.Equal(10, df.GetValue<int?>(0, "B"));

        Assert.Null(df.GetValue<int?>(0, "C"));
        
        // --- Row 1---
        Assert.Null(df.GetValue<int?>(1, "A"));
        
        Assert.Equal(20, df.GetValue<int?>(1, "B"));
        Assert.Equal(300, df.GetValue<int?>(1, "C"));
    }
    [Fact]
    public void Test_LazyFrame_Join_MultiColumn_WithParams()
    {

        using var scoresDf = DataFrame.FromColumns(new 
        {
            student = new[] { "Alice", "Alice", "Bob" },
            year    = new[] { 2023,    2024,    2023 },
            score   = new[] { 85,      90,      70 },
            note    = new[] { "L1",    "L2",    "L3" }
        });
        using var scoresLf = scoresDf.Lazy();

        using var classDf = DataFrame.FromColumns(new 
        {
            student = new[] { "Alice", "Alice", "Bob" },
            year    = new[] { 2023,    2024,    2024 },
            className = new[] { "Math", "Physics", "History" },
            note    = new[] { "R1",    "R2",    "R3" }
        });
        using var classLf = classDf.Lazy();

        using var joinedLf = scoresLf.Join(
            classLf,
            leftOn: [Col("student"), Col("year")],
            rightOn: [Col("student"), Col("year")],
            how: JoinType.Inner,
            
            suffix: "_lazy_conflict",           
            validation: JoinValidation.OneToOne, 
            coalesce: JoinCoalesce.JoinSpecific
        );

        using var joinedDf = joinedLf.Collect();

        Assert.Equal(2, joinedDf.Height);
        Assert.Equal(6, joinedDf.Width); // student, year, score, note, className, note_lazy_conflict

        var cols = joinedDf.ColumnNames;
        Assert.Contains("note", cols);
        Assert.Contains("note_lazy_conflict", cols);

        using var sorted = joinedDf.Sort("year");
        
        Assert.Equal(2023, sorted.GetValue<int>(0, "year"));
        Assert.Equal("Math", sorted.GetValue<string>(0, "className"));
        Assert.Equal("L1", sorted.GetValue<string>(0, "note"));
        Assert.Equal("R1", sorted.GetValue<string>(0, "note_lazy_conflict"));

        using var bobCheck = joinedDf.Filter(Col("student") == Lit("Bob"));
        Assert.Equal(0, bobCheck.Height);
    }
    [Fact]
    [Trait("LazyFrame","GroupBy")]
    public void Test_LazyFrame_GroupBy_Having_Agg()
    {
        string[] depts = ["IT", "IT", "HR", "HR", "Sales", "Sales"];
        long[] salaries = [100, 200, 150, 50, 20, 30]; 

        using var df = DataFrame.FromColumns(
            Series.From("dept", depts),
            Series.From("salary", salaries)
        );

        using var groupedlf = df.Lazy()
            .GroupBy("dept")
            .Having(Col("salary").Sum() > 100) 
            .Agg(Col("salary").Sum().Alias("total_salary"))
            .Sort("total_salary", descending: true); 
            
        using var grouped = groupedlf.Collect();
        
        Assert.Equal(2, grouped.Height);

        Assert.Equal("IT", grouped.Column("dept").GetValue<string>(0));
        Assert.Equal(300L, grouped.Column("total_salary").GetValue<long>(0));
        
        Assert.Equal("HR", grouped.Column("dept").GetValue<string>(1));
        Assert.Equal(200L, grouped.Column("total_salary").GetValue<long>(1));
    }
    [Fact]
    [Trait("LazyFrame", "GroupBySugar1")]
    public void Test_LazyFrame_GroupBy_Len()
    {
        string[] depts = ["IT", "IT", "HR", "HR", "HR", "Sales"];

        using var df = DataFrame.FromColumns(
            Series.From("dept", depts)
        );

        using var defaultLenLf = df.Lazy()
            .GroupBy("dept")
            .Len()
            .Sort("len", descending: true);
            
        using var defaultLenDf = defaultLenLf.Collect();
        
        Assert.Equal(3, defaultLenDf.Height);
        
        Assert.Equal("HR", defaultLenDf.Column("dept").GetValue<string>(0));
        Assert.Equal(3u, defaultLenDf.Column("len").GetValue<uint>(0));
        
        Assert.Equal("IT", defaultLenDf.Column("dept").GetValue<string>(1));
        Assert.Equal(2u, defaultLenDf.Column("len").GetValue<uint>(1));

        using var customLenLf = df.Lazy()
            .GroupBy("dept")
            .Len("employee_count")
            .Sort("employee_count", descending: true);
            
        using var customLenDf = customLenLf.Collect();
        
        Assert.Equal(3, customLenDf.Height);
        Assert.Equal(3u, customLenDf.Column("employee_count").GetValue<uint>(0)); 
    }

    [Fact]
    [Trait("LazyFrame", "GroupBySugar2")]
    public void Test_LazyFrame_GroupBy_Sugar_Aggregations()
    {
        string[] depts = ["IT", "IT", "HR", "HR", "Sales"];
        long[] salaries = [100, 200, 150, 50, 30]; 

        using var df = DataFrame.FromColumns(
            Series.From("dept", depts),
            Series.From("salary", salaries)
        );

        using var sumLf = df.Lazy()
            .GroupBy("dept")
            .Sum()
            .Sort("dept"); 
            
        using var sumDf = sumLf.Collect();
        
        Assert.Equal(3, sumDf.Height);
        Assert.Equal("HR", sumDf.Column("dept").GetValue<string>(0));
        Assert.Equal(200L, sumDf.Column("salary").GetValue<long>(0));
        Assert.Equal("IT", sumDf.Column("dept").GetValue<string>(1));
        Assert.Equal(300L, sumDf.Column("salary").GetValue<long>(1));

        using var maxLf = df.Lazy()
            .GroupBy("dept")
            .Max()
            .Sort("dept");
            
        using var maxDf = maxLf.Collect();
        
        Assert.Equal(3, maxDf.Height);
        Assert.Equal(150L, maxDf.Column("salary").GetValue<long>(0)); 
        Assert.Equal(200L, maxDf.Column("salary").GetValue<long>(1)); 

        using var headLf = df.Lazy()
            .GroupBy("dept")
            .Head(1) 
            .Sort("dept");
            
        using var headDf = headLf.Collect();

        Assert.Equal(3, headDf.Height);
        Assert.Contains("salary", headDf.ColumnNames); 
    }
    [Fact]
    public void Test_Lazy_Unpivot_With_Explain()
    {
        var content = @"date,apple,banana
2024-01-01,10,20
2024-01-02,12,22";
        
        using var csv = new DisposableFile(content,".csv");
        using var lf = LazyFrame.ScanCsv(csv.Path);

        var unpivotedLf = lf.Unpivot(
            index: ["date"],
            on: ["apple", "banana"],
            variableName: "fruit",
            valueName: "price"
        );

        // --- Explain ---
        string plan = unpivotedLf.Explain(optimized: true);
        Console.WriteLine("LazyFrame Explain Plan:");
        Console.WriteLine(plan);

        Assert.Contains("UNPIVOT", plan.ToUpper()); 

        using var df = unpivotedLf.Collect();

        Assert.Equal(4, df.Height);
        Assert.Equal(3, df.Width); // date, fruit, price

        Assert.NotNull(df.Column("fruit"));
        Assert.NotNull(df.Column("price"));
        
        var price0 = df.Column("price").GetValue<long>(0);
        Assert.True(price0 == 10 || price0 == 20);
    }
    [Fact]
    public void Test_Lazy_JoinAsOf_With_TimeSpan_Tolerance()
    {
        
        var tradesContent = @"time,sym,qty
2024-01-01 10:00:00,AAPL,10
2024-01-01 10:02:00,AAPL,20
2024-01-01 10:05:00,AAPL,5"; 
        using var tradesCsv = new DisposableFile(tradesContent, ".csv");
        
        var quotesContent = @"time,sym,bid
2024-01-01 09:59:00,AAPL,150
2024-01-01 10:01:00,AAPL,151
2024-01-01 10:06:00,AAPL,152";
        using var quotesCsv = new DisposableFile(quotesContent, ".csv");

        using var lfTrades = LazyFrame.ScanCsv(tradesCsv.Path, tryParseDates: true);
        using var lfQuotes = LazyFrame.ScanCsv(quotesCsv.Path, tryParseDates: true);

        var joinedLf = lfTrades.JoinAsOf(
            lfQuotes,
            leftOn: Col("time"),
            rightOn: Col("time"),
            
            tolerance: TimeSpan.FromMinutes(2), 
            
            strategy: AsofStrategy.Backward,
            
            leftBy: [Col("sym")],
            rightBy: [Col("sym")]
        );

        using var df = joinedLf.Collect();
        
        Assert.Equal(3, df.Height);

        // Row 0: Trade 10:00 -> Quote 09:59 (Diff: 1m <= 2m) -> Match
        Assert.Equal(150, df.Column("bid").GetValue<long?>(0));

        // Row 1: Trade 10:02 -> Quote 10:01 (Diff: 1m <= 2m) -> Match
        Assert.Equal(151, df.Column("bid").GetValue<long?>(1));

        // Row 2: Trade 10:05 -> Quote 10:01 (Diff: 4m > 2m) -> No Match
        Assert.Null(df.Column("bid").GetValue<long?>(2));
    }
    [Fact]
    public void Test_Lazy_GroupBy_Ownership()
    {
        var data = new[]
        {
            new { Dept = "A", Val = 10 },
            new { Dept = "A", Val = 20 },
            new { Dept = "B", Val = 30 }
        };
        using var df = DataFrame.From(data);
        using var lf = df.Lazy();


        using var agg1 = lf.GroupBy(Col("Dept"))
                           .Agg(Col("Val").Sum().Alias("SumVal"))
                           .Collect();
        
        Assert.Equal(2, agg1.Height); // A, B

        using var res2 = lf.Select(Col("Dept")).Collect();
        Assert.Equal(3, res2.Height);
    }
    [Fact]
    public void Test_LazyFrame_Explode()
    {
        using var s = new Series("chars", ["a,b","c"]);
        using var df = DataFrame.FromSeries(s);
        
        using var lf = df.Lazy();

        using var selector = Cs.ByName("expanded");

        using var res = lf
            .Select(Col("chars").Str.Split(",").Alias("expanded"))
            .Explode(selector)
            .Collect();

        Assert.Equal(3, res.Height);
        Assert.Equal("a", res.GetValue<string>(0, "expanded"));
        Assert.Equal("b", res.GetValue<string>(1, "expanded"));
        Assert.Equal("c", res.GetValue<string>(2, "expanded"));
    }
    [Fact]
    public void TestLazySchema_ZeroParse_Introspection()
    {
        Console.WriteLine("=== LazyFrame Schema Zero-Parse Test ===");

        // Schema: { "a": Int32, "b": Float64, "c": String }
        using var s1 = Series.From("a", [1, 2, 3]);
        using var s2 = Series.From("b", [1.1, 2.2, 3.3]);
        using var s3 = Series.From("c", ["apple", "banana", "cherry"]);
        using var df = DataFrame.FromSeries(s1, s2, s3);
        
        using var lf = df.Lazy();

        Console.WriteLine("--- 1. Initial Schema ---");
        using var schema1 = lf.Schema; 

        Assert.Equal(3, schema1.Length);
        
        Assert.Equal(DataTypeKind.Int32, schema1["a"].Kind);
        Assert.Equal(DataTypeKind.Float64, schema1["b"].Kind);
        Assert.Equal(DataTypeKind.String, schema1["c"].Kind);

        Console.WriteLine(schema1.ToString()); 

        Console.WriteLine("\n--- 2. Modified Schema (Type Inference) ---");
        
        using var lf2 = lf.Select(
            Col("a").Cast(DataType.Float64).Alias("a_cast"),
            Col("c").Implode().Alias("c_list") 
        );

        using var schema2 = lf2.Schema;

        Assert.Equal(DataTypeKind.Float64, schema2["a_cast"].Kind);
        Console.WriteLine($"[Check] a_cast is Float64: {schema2["a_cast"].Kind == DataTypeKind.Float64}");

        var listType = schema2["c_list"];
        Assert.Equal(DataTypeKind.List, listType.Kind);
        Console.WriteLine($"[Check] c_list is List: {listType.Kind == DataTypeKind.List}");

        using var innerType = listType.InnerType; 
        
        Assert.NotNull(innerType);
        Assert.Equal(DataTypeKind.String, innerType!.Kind);
        Console.WriteLine($"[Check] c_list inner type is String: {innerType.Kind == DataTypeKind.String}");

        Console.WriteLine(schema2.ToString());
        Console.WriteLine("=== SUCCESS: Validated without a single string parse! ===");
    }
    [Fact]
    public void Test_LazyFrame_Sort_FullOptions()
    {
        // val: [3, null, 1]
        // grp: [1, 1, 1]
        using var df = DataFrame.FromColumns(new 
        {
            val = new int?[] { 3, null, 1 },
            grp = new[] { 1, 1, 1 }
        });

        // Expected: [3, 1, null]
        
        using var lf = df.Lazy();
        using var sortedLf = lf.Sort(
            "val", 
            descending: true, 
            nullsLast: true 
        );
        
        using var res = sortedLf.Collect();

        Assert.Equal(3, res["val"][0]);
        Assert.Equal(1, res["val"][1]);
        Assert.Null(res["val"][2]);
    }

    // raw: List<int> -> Array(2) -> Struct { field_0, field_1 }
    private static DataFrame CreateStructDataFrame()
    {
        var data = new[] 
        { 
            new[] { 1, 2 }, 
            [10, 20],
            [100, 200]
        };

        var df = DataFrame.FromColumns(new { raw = data });
        
        return df.Select(
            Col("raw")
                .Cast(DataType.Array(DataType.Int32, 2))
                .Array.ToStruct()
                .Alias("my_struct")
        );
    }

    [Fact]
    [Trait("LazyFrame","Unnest")]
    public void Test_LazyFrame_Unnest_With_Strings()
    {
        
        using var df = CreateStructDataFrame();

        // Action
        using var res = df.Lazy()
            .Unnest(Cs.Nested())
            .Collect();

        // Assert
        Assert.True(res.Columns.Contains("field_0"), "Result should contain field_0");
        Assert.True(res.Columns.Contains("field_1"), "Result should contain field_1");

        Assert.Equal(1, (int)res["field_0"][0]);
        Assert.Equal(2, (int)res["field_1"][0]);

        Assert.Equal(10, (int)res["field_0"][1]);
        Assert.Equal(20, (int)res["field_1"][1]);
    }

    [Fact]
    [Trait("LazyFrame","UnnestReuseSelector")]
    public void Test_LazyFrame_Unnest_Reuse_Selector()
    {
        
        using var df = CreateStructDataFrame();
        using var selector = Cs.ByName("my_struct");

        using var lf1 = df.Lazy().Unnest(selector); 
        using var res1 = lf1.Collect();
        Assert.True(res1.Columns.Contains("field_0"));

        using var lf2 = df.Lazy().Unnest(selector);
        using var res2 = lf2.Collect();
        Assert.True(res2.Columns.Contains("field_0"));
    }
    
    [Fact]
    [Trait("LazyFrame","UnnestMultipleCols")]
    public void Test_LazyFrame_Unnest_Multiple_Cols()
    {
        var data1 = new[] { new[] { 1, 2 } };
        var data2 = new[] { new[] { 3, 4 } };
        
        using var df = DataFrame.FromColumns(new { raw1 = data1, raw2 = data2 })
            .Select(
                Col("raw1").Cast(DataType.Array(typeof(int), 2)).Array.ToStruct().Alias("s1"),
                
                // s2 : field_0 -> other_0, field_1 -> other_1
                Col("raw2").Cast(DataType.Array(typeof(int), 2)).Array.ToStruct()
                    .Struct.RenameFields("other_0", "other_1") 
                    .Alias("s2")
            );

        using var res = df.Lazy()
            .Unnest(["s1", "s2"],separator:null) 
            .Collect();
        // s1 : field_0, field_1
        // s2 : other_0, other_1
        Assert.True(res.Columns.Contains("field_0"));
        Assert.True(res.Columns.Contains("other_0"));
        
        Assert.Equal(1, (int)res["field_0"][0]);
        Assert.Equal(3, (int)res["other_0"][0]);
    }
    [Fact]
    public void Test_LazyFrame_TopK_BottomK()
    {
        var data = new[] { 10, 5, 8, 100, 1 };
        using var df = DataFrame.FromColumns(new { val = data });

        using var top = df.Lazy()
            .TopK(2, "val") 
            .Collect();

        Assert.Equal(2, top.Height);
        var topVals = top["val"].ToArray<int>();
        Assert.Contains(100, topVals);
        Assert.Contains(10, topVals);

        using var bottom = df.Lazy()
            .BottomK(2, "val")
            .Collect();

        Assert.Equal(2, bottom.Height);
        var bottomVals = bottom["val"].ToArray<int>();
        Assert.Contains(1, bottomVals);
        Assert.Contains(5, bottomVals);
    }
    [Fact]
    public void Test_LazyFrame_Slice()
    {
        using var csv = new DisposableFile("val\n0\n1\n2\n3\n4", ".csv");
        using var lf = LazyFrame.ScanCsv(csv.Path);

        var slicedLf = lf.Slice(-3, 2);

        using var df = slicedLf.Collect();

        Assert.Equal(2, df.Height);
        
        Assert.Equal(2, df["val"].GetValue<int>(0));
        Assert.Equal(3, df["val"].GetValue<int>(1));
    }
    [Fact]
    public void Test_Drop_ByColumnNames()
    {
        var df = DataFrame.From(
        [
            new { Name = "Alice", Age = 25, City = "New York", Salary = 5000 },
            new { Name = "Bob", Age = 30, City = "London", Salary = 6000 }
        ]);

        var lf = df.Lazy();
        var res = lf.Drop("City", "Salary")
                    .Collect();

        Assert.Equal(2, res.Width); 
        Assert.Contains("Name", res.ColumnNames);
        Assert.Contains("Age", res.ColumnNames);
        
        Assert.DoesNotContain("City", res.ColumnNames);
        Assert.DoesNotContain("Salary", res.ColumnNames);
    }

    [Fact]
    [Trait("LazyFrame","DropNulls")]
    public void Test_DropNulls()
    {
        var df = DataFrame.FromRows(
        [
            new { A = 1, B = 2.2, C = null as string },
            new { A = 2, B = double.NaN, C = "world" }
        ]);

        var lf = df.Lazy();
        var res = lf.DropNulls()
                    .Collect();

        Assert.Equal(3, res.Width);
        Assert.Equal(1, res.Len);
        
    }
    [Fact]
    [Trait("LazyFrame","DropNaNs")]
    public void Test_DropNaN()
    {
        var df = DataFrame.FromRows(
        [
            new { A = 1, B = 2.2, C = null as string },
            new { A = 2, B = double.NaN, C = "world" }
        ]);

        var lf = df.Lazy();
        var res = lf.DropNans()
                    .Collect();

        Assert.Equal(3, res.Width);
        Assert.Equal(1, res.Len);
        
    }
    [Fact]
    [Trait("LazyFrame","Unique")]
    public void Test_Lazy_Unique()
    {
        var df = DataFrame.From(
        [
            new { A = 1, B = 1 },
            new { A = 1, B = 2 },
            new { A = 2, B = 3 },
            new { A = 1, B = 1 } // Duplicate of first row
        ]);

        // 1. Test Selector (Subset=[A])
        // Keep First -> A=1(row0), A=2(row2)     
        var lf = df.Lazy();
        
        // Case A: Subset on "A", Keep First
        var res1 = lf.Unique(Cs.ByIndex(0), UniqueKeepStrategy.First)
                     .Collect();
        
        Assert.Equal(2, res1.Height); 
        
        // Case B: Subset on All (null selector), Keep None (Drop all duplicates)
        var res2 = lf.Unique(keep: UniqueKeepStrategy.None,maintainOrder:true)
                     .Collect();
                     
        Assert.Equal(2, res2.Height);
        Assert.Equal(2, (int)res2["B"][0]); 
        
        Assert.Equal(3, (int)res2["B"][1]);
        
        // Case C: String overload
        var res3 = lf.Unique(["A", "B"]).Collect();
        Assert.Equal(3, res3.Height); // (1,1), (1,2), (2,3) are kept. The last (1,1) dropped.
    }
    [Fact]
    public void Test_LazyPivot_With_Schema_Injection()
    {
        var df = DataFrame.FromColumns( new
            {
                date = new[] {"2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"},
                product = new[] {"Apple", "Banana", "Apple", "Banana"},
                sales = new[] {100, 200, 150, 300}
            });
        var lf = df.Lazy();

        var expectedColumns = new Series("product", ["Banana", "Apple"]); 

        var pivotedLf = lf.Pivot(
            index: "date",          
            on:"product",     
            values: "sales",       
            onColumns: expectedColumns,
            aggregateFunction: PivotAgg.Sum, 
            maintainOrder: true         
        );

        var schema = pivotedLf.Schema;
        Assert.Contains("Banana", schema.ColumnNames);
        Assert.Contains("Apple", schema.ColumnNames);
        
        var result = pivotedLf.Collect();

        /*
        ┌────────────┬────────┬───────┐
        │ date       ┆ Banana ┆ Apple │
        │ ---        ┆ ---    ┆ ---   │
        │ str        ┆ i32    ┆ i32   │
        ╞════════════╪════════╪═══════╡
        │ 2024-01-01 ┆ 200    ┆ 100   │
        │ 2024-01-02 ┆ 300    ┆ 150   │
        └────────────┴────────┴───────┘
        */
        
        Assert.Equal("date", result.ColumnNames[0]);
        Assert.Equal("Banana", result.ColumnNames[1]);
        Assert.Equal("Apple", result.ColumnNames[2]);

        Assert.Equal(200, result["Banana"][0]); // 01-01 Banana
        Assert.Equal(100, result["Apple"][0]);  // 01-01 Apple
        Assert.Equal(300, result["Banana"][1]); // 01-02 Banana
        Assert.Equal(150, result["Apple"][1]);  // 01-02 Apple
    }
    [Fact]
    [Trait("LazyFrame","Aggregation1")]
    public void Test_Lazy_Aggregation1()
    {
        var lf = DataFrame.FromSeries(
            Series.From("col1",[1,0,114514]),
            Series.From("col2",[5.5,4.4,double.NaN]),
            Series.From("col3",new long?[] {null,985L,211L}),
            Series.From("col4", ["",null,"nihao"])
        ).Lazy();

        var lf2 = lf.Clone();

        var result = lf.Count().Collect();
        Assert.Equal(3u,result["col1"][0]);
        Assert.Equal(3u,result["col2"][0]);
        Assert.Equal(2u,result["col3"][0]);
        Assert.Equal(2u,result["col4"][0]);

        var resultSum = lf2.Drop("col4").Sum().Collect();
        Assert.Equal(114515,resultSum["col1"][0]);
        Assert.Equal(double.NaN,resultSum["col2"][0]);
        Assert.Equal(1196L,resultSum[2][0]);
    }
    [Fact]
    [Trait("LazyFrame","Aggregation2")]
    public void Test_Lazy_Aggregation2()
    {
        var lf = DataFrame.FromSeries(
            Series.From("col1",[1,0,114514]),
            Series.From("col2",[5.5,double.MaxValue,double.NaN]),
            Series.From("col3",new long?[] {null,985L,211L})
        ).Lazy();

        var lf2 = lf.Clone();

        var result = lf.Max().Collect();

        Assert.Equal(114514,result["col1"][0]);
        Assert.Equal(double.MaxValue,result["col2"][0]);
        Assert.Equal(985L,result["col3"][0]);

        var resultSum = lf2.Min().Collect();
        Assert.Equal(0,resultSum["col1"][0]);
        Assert.Equal(5.5,resultSum["col2"][0]);
        Assert.Equal(211L,resultSum[2][0]);
    }
    [Fact]
    [Trait("LazyFrame","Aggregation3")]
    public void Test_Lazy_Aggregation3()
    {
        var lf = DataFrame.FromSeries(
            Series.From("col1",[1,0,114514]),
            Series.From("col2",[5.5,double.MaxValue,double.NaN]),
            Series.From("col3",new long?[] {null,985L,211L})
        ).Lazy();

        var lf2 = lf.Clone();

        var result = lf.Mean().Collect();

        Assert.Equal(38171.67,(double)result["col1"][0],2);
        Assert.Equal(double.NaN,result["col2"][0]);
        Assert.Equal(598.0,(double)result["col3"][0],3);

        var resultMedian = lf2.Median().Collect();
        Assert.Equal(1.0,(double)resultMedian["col1"][0],1);
        // Assert.Equal(,resultMedian["col2"][0]); 1.7977e308
        Assert.Equal(598.0,(double)resultMedian[2][0],1);
    }
    [Fact]
    [Trait("LazyFrame","Aggregation4")]
    public void Test_Lazy_Aggregation4()
    {
        var lf = DataFrame.FromSeries(
            Series.From("col1",[1,0,114514]),
            Series.From("col2",[5.5,double.MaxValue,double.NaN]),
            Series.From("col3",new long?[] {null,985L,211L})
        ).Lazy();

        var lf2 = lf.Clone();

        var result = lf.Std().Collect();

        Assert.Equal(66114.400053,(double)result["col1"][0],3);
        Assert.Equal(double.NaN,result["col2"][0]);
        Assert.Equal(547.300649,(double)result["col3"][0],3);

        var resultVar = lf2.Var().Collect();
        // Assert.Equal(1.0,(double)resultMedian["col1"][0],1);
        Assert.Equal(double.NaN,resultVar["col2"][0]);
        Assert.Equal(299538.0,(double)resultVar[2][0],1);
    }
    [Fact]
    [Trait("LazyFrame","NullCount")]
    public void Test_Lazy_NullCount()
    {
        var lf = DataFrame.FromSeries(
            Series.From("col1",[1,0,114514]),
            Series.From("col2",[5.5,4.4,double.NaN]),
            Series.From("col3",new long?[] {null,985L,211L}),
            Series.From("col4", ["",null,"nihao"])
        ).Lazy();


        var result = lf.NullCount().Collect();
        Assert.Equal(0u,result["col1"][0]);
        Assert.Equal(0u,result["col2"][0]);
        Assert.Equal(1u,result["col3"][0]);
        Assert.Equal(1u,result["col4"][0]);
    }
    [Fact]
    [Trait("LazyFrame","Quantile")]
    public void Test_Lazy_Quantile()
    {
        var lf = DataFrame.FromSeries(
            Series.From("col1",[1,0,114514,6546213]),
            Series.From("col2",[5.5,4.4,double.NaN,double.MinValue]),
            Series.From("col3",new long?[] {null,985L,211L,1919810L})
        ).Lazy();

        var result = lf.Quantile(0.4,method:QuantileMethod.Linear).Collect();

        Assert.Equal(22903.6,(double)result["col1"][0],1);
        Assert.Equal(4.62,(double)result["col2"][0],2);
        Assert.Equal(830.2,(double)result["col3"][0],2);
    }
    [Fact]
    [Trait("LazyFrame","FilterByArray")]
    public void Test_Lazy_Filter()
    {
        var lf = DataFrame.FromSeries(
            Series.From("col1",[1,0,114514,6546213]),
            Series.From("col2",[5.5,4.4,double.NaN,double.MinValue]),
            Series.From("col3",new long?[] {null,985L,211L,1919810L})
        ).Lazy();

        var result = lf.Filter([false,false,true,true]).Collect();
        Assert.Equal(2L,result.Height);
        Assert.Equal(114514,result[0][0]);
    }
    [Fact]
    [Trait("LazyFrame","FilterBySeries")]
    public void Test_Lazy_Filter_Series()
    {
        var lf = DataFrame.FromSeries(
            Series.From("col1",[1,0,114514,6546213]),
            Series.From("col2",[5.5,4.4,double.NaN,double.MinValue]),
            Series.From("col3",new long?[] {null,985L,211L,1919810L})
        ).Lazy();

        Series mask = Series.From("mask",new bool?[] {true,null,null,null});
        var result = lf.Filter(mask).Collect();
        Assert.Equal(1L,result.Height);
        Assert.Equal(1,result[0][0]);
    }

}