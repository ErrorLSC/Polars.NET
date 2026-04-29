#pragma warning disable CS8632
using System.Numerics.Tensors;
using Apache.Arrow;
using Apache.Arrow.Memory;
using Pl = Polars.CSharp.Polars;
using Cs = Polars.CSharp.Polars.Selectors;
using Polars.NET.Core;
using System.Text.Json;
namespace Polars.CSharp.Tests;

public class DataFrameTests
{
    [Fact]
    public void Test_FromArrow_RoundTrip()
    {
        var builder = new RecordBatch.Builder(new NativeMemoryAllocator())
            .Append("id", false, col => col.Int32(array => array.AppendRange([1, 2, 3])))
            .Append("value", false, col => col.Double(array => array.AppendRange([1.1, 2.2, 3.3])));

        using var originalBatch = builder.Build();

        using var df = DataFrame.FromArrow(originalBatch);
        
        Assert.Equal(3, df.Height);
        Assert.Equal(2, df.Width);

        using var resultDf = df.Select(
            "id", 
            (Pl.Col("value") * 2.0).Alias("value_doubled")
        );

        using var resultBatch = resultDf.ToArrow();
        var doubledCol = resultBatch.Column("value_doubled") as DoubleArray;

        Assert.NotNull(doubledCol);
        Assert.Equal(2.2, doubledCol.GetValue(0).Value, 4);
        Assert.Equal(4.4, doubledCol.GetValue(1).Value, 4);
        Assert.Equal(6.6, doubledCol.GetValue(2).Value, 4);
    }
    
    [Fact]
    [Trait("DataFrame","GroupBy")]
    public void Test_GroupBy_Agg()
    {
        string[] depts = ["IT", "IT", "HR", "HR", "Sales", "Sales"];
        long[] salaries = [100, 200, 150, 50, 20, 30]; 

        using var df = DataFrame.FromColumns(
            Series.From("dept", depts),
            Series.From("salary", salaries)
        );

        using var grouped = df
            .GroupBy("dept")
            .Having(Pl.Col("salary").Sum() > 100) 
            .Agg(Pl.Col("salary").Sum().Alias("total_salary"))
            .Sort("total_salary", descending: true); 

        Assert.Equal(2, grouped.Height);

        Assert.Equal("IT", grouped.Column("dept").GetValue<string>(0));
        Assert.Equal(300L, grouped.Column("total_salary").GetValue<long>(0));
        
        Assert.Equal("HR", grouped.Column("dept").GetValue<string>(1));
        Assert.Equal(200L, grouped.Column("total_salary").GetValue<long>(1));
    }
    [Fact]
    [Trait("DataFrame", "GroupBy")]
    public void Test_GroupBy_Standard_Head_And_Tail()
    {
        var df = DataFrame.FromColumns(new
        {
            Group = new[] { "A", "A", "A", "B", "B" },
            Val = new[] { 1, 2, 3, 4, 5 }
        });

        var headRes = df.GroupBy("Group").Head(2);
        
        Assert.Equal(4, headRes.Height);
        Assert.Equal(1, headRes.GetValue<int>(0, "Val"));
        Assert.Equal(2, headRes.GetValue<int>(1, "Val"));
        Assert.Equal(4, headRes.GetValue<int>(2, "Val"));
        Assert.Equal(5, headRes.GetValue<int>(3, "Val"));

        var tailRes = df.GroupBy("Group").Tail(1);
        
        Assert.Equal(2, tailRes.Height);
        Assert.Equal(3, tailRes.GetValue<int>(0, "Val"));
        Assert.Equal(5, tailRes.GetValue<int>(1, "Val"));
    }
    [Fact]
    [Trait("DataFrame", "GroupBySugar1")]
    public void Test_DataFrame_GroupBy_Len()
    {
        string[] depts = ["IT", "IT", "HR", "HR", "HR", "Sales"];

        using var df = DataFrame.FromColumns(
            Series.From("dept", depts)
        );

        using var defaultLenDf = df
            .GroupBy("dept")
            .Len()
            .Sort("len", descending: true);
            
        Assert.Equal(3, defaultLenDf.Height);
        
        Assert.Equal("HR", defaultLenDf.Column("dept").GetValue<string>(0));
        Assert.Equal(3u, defaultLenDf.Column("len").GetValue<uint>(0));
        
        Assert.Equal("IT", defaultLenDf.Column("dept").GetValue<string>(1));
        Assert.Equal(2u, defaultLenDf.Column("len").GetValue<uint>(1));

        using var customLenDf = df
            .GroupBy("dept")
            .Len("employee_count")
            .Sort("employee_count", descending: true);
            
        Assert.Equal(3, customLenDf.Height);
        Assert.Equal(3u, customLenDf.Column("employee_count").GetValue<uint>(0)); 
    }

    [Fact]
    [Trait("DataFrame", "GroupBySugar2")]
    public void Test_DataFrame_GroupBy_Sugar_Aggregations()
    {
        string[] depts = ["IT", "IT", "HR", "HR", "Sales"];
        long[] salaries = [100, 200, 150, 50, 30]; 

        using var df = DataFrame.FromColumns(
            Series.From("dept", depts),
            Series.From("salary", salaries)
        );

        using var sumDf = df
            .GroupBy(Cs.String(),maintainOrder:false)
            .Sum()
            .Sort(Cs.String()); 
            
        Assert.Equal(3, sumDf.Height);
        Assert.Equal("HR", sumDf.Column("dept").GetValue<string>(0));
        Assert.Equal(200L, sumDf.Column("salary").GetValue<long>(0));
        Assert.Equal("IT", sumDf.Column("dept").GetValue<string>(1));
        Assert.Equal(300L, sumDf.Column("salary").GetValue<long>(1));

        using var maxDf = df
            .GroupBy("dept")
            .Max()
            .Sort("dept");
            
        Assert.Equal(3, maxDf.Height);
        Assert.Equal(150L, maxDf.Column("salary").GetValue<long>(0)); 
        Assert.Equal(200L, maxDf.Column("salary").GetValue<long>(1)); 

        using var headDf = df
            .GroupBy("dept")
            .Head(1) 
            .Sort("dept");
            
        Assert.Equal(3, headDf.Height);
        Assert.Contains("salary", headDf.ColumnNames); 
    }
    [Fact]
    public void Test_GroupBy_Advanced_Aggregations()
    {
        string[] groups = ["A", "A", "B", "C", "C"];
        bool[] bools = [true, false, true, false, false];
        int[] values = [1, 2, 3, 4, 5];

        using var df = DataFrame.FromColumns(new { groups, bools, values });

        using var result = df
            .GroupBy(Cs.String())
            .Agg(
                Cs.Boolean().ToExpr().Any().Alias("is_any_true"),   
                Cs.Boolean().ToExpr().All().Alias("is_all_true"),                  
            
                Cs.Integer().ToExpr().First().Alias("v_first"),
                Cs.Integer().ToExpr().Last().Alias("v_last"),    
                
                Cs.Integer().ToExpr().Reverse().First().Alias("v_rev_first") 
            )
            .Sort(Cs.ByIndex(0));

        Assert.Equal(3, result.Height); 

        // --- Group A (Mixed) ---
        // bools: [true, false] -> Any: True, All: False
        // values: [1, 2] -> First: 1, Last: 2
        Assert.Equal("A", result.GetValue<string>(0, "groups"));
        Assert.True(result.GetValue<bool>(0, "is_any_true"));
        Assert.False(result.GetValue<bool>(0, "is_all_true"));
        Assert.Equal(1, result.GetValue<int>(0, "v_first"));
        Assert.Equal(2, result.GetValue<int>(0, "v_last"));
        Assert.Equal(2, result.GetValue<int>(0, "v_rev_first"));

        // --- Group B (Single) ---
        // bools: [true] -> Any: True, All: True
        Assert.Equal("B", result.GetValue<string>(1, "groups"));
        Assert.True(result.GetValue<bool>(1, "is_all_true")); 

        // --- Group C (All False) ---
        // bools: [false, false] -> Any: False, All: False
        Assert.Equal("C", result.GetValue<string>(2, "groups"));
        Assert.False(result.GetValue<bool>(2, "is_any_true"));
    }

    [Fact]
    public void Test_GroupBy_Item_Safe()
    {
        string[] groups = ["X", "Y"];
        int[] codes = [101, 102]; 

        using var df = DataFrame.FromColumns(new { groups, codes });

        using var res = df.GroupBy("groups")
            .Agg(
                Pl.Col("codes").Item().Alias("code_item")
            )
            .Sort("groups");

        Assert.Equal(2, res.Height);
        Assert.Equal(101, res.GetValue<int>(0, "code_item"));
        Assert.Equal(102, res.GetValue<int>(1, "code_item"));
    }

    [Fact]
    public void Test_Expr_Reverse_Standalone()
    {
        // [1, 2, 3] -> [3, 2, 1]
        
        using var df = DataFrame.FromSeries(         
            Series.From("nums",[1, 2, 3]) 
        );

        using var res = df.Select(
            Pl.Col("nums").Reverse().Alias("nums_rev")
        );

        var revArr = res["nums_rev"].ToArray<int>();
        
        Assert.Equal(3, revArr.Length);
        Assert.Equal(3, revArr[0]);
        Assert.Equal(2, revArr[1]);
        Assert.Equal(1, revArr[2]);
    }
    // ==========================================
    // Join Tests
    // ==========================================
    [Fact]
    [Trait("DataFrame","Join")]
    public void Test_DataFrame_Join_MultiColumn_WithParams()
    {
        using var scoresDf = DataFrame.FromColumns(new 
        {
            student = new[] { "Alice", "Alice", "Bob" },
            year    = new[] { 2023,    2024,    2023 },
            score   = new[] { 85,      90,      70 },
            note    = new[] { "Score1", "Score2", "Score3" } 
        });

        using var classDf = DataFrame.FromColumns(new 
        {
            student = new[] { "Alice", "Alice", "Bob" },
            year    = new[] { 2023,    2024,    2024 },
            className = new[] { "Math", "Physics", "History" }, 
            note    = new[] { "Class1", "Class2", "Class3" }
        });

        // - (Alice, 2023) -> Math
        // - (Alice, 2024) -> Physics
        // (Bob, 2023) -> discard
        // (Bob, 2024) -> discard
        using var joinedDf = scoresDf.Join(
            classDf,
            on: ["student", "year"],
            how: JoinType.Inner,
            
            suffix: "_conflict_test",          
            validation: JoinValidation.OneToOne, 
            coalesce: JoinCoalesce.JoinSpecific  
        );

        Assert.Equal(2, joinedDf.Height);
        
        var cols = joinedDf.Columns;
        Assert.Contains("note", cols);
        Assert.Contains("note_conflict_test", cols);

        using var sorted = joinedDf.Sort("year");

        // Row 0: Alice 2023
        Assert.Equal(2023, sorted.GetValue<int>(0, "year"));
        Assert.Equal("Math", sorted.GetValue<string>(0, "className"));
        
        Assert.Equal("Score1", sorted.GetValue<string>(0, "note"));              
        Assert.Equal("Class1", sorted.GetValue<string>(0, "note_conflict_test")); 

        // Row 1: Alice 2024
        Assert.Equal(2024, sorted.GetValue<int>(1, "year"));
        Assert.Equal("Physics", sorted.GetValue<string>(1, "className"));
        Assert.Equal("Score2", sorted.GetValue<string>(1, "note"));
        Assert.Equal("Class2", sorted.GetValue<string>(1, "note_conflict_test"));
    }
    // ==========================================
    // Concat Tests (Vertical, Horizontal, Diagonal)
    // ==========================================
    [Fact]
    public void Test_Concat_All_Types()
    {
        // --- Vertical ---
        {
            using var csv1 = new DisposableFile("id,name\n1,Alice","csv");
            using var df1 = DataFrame.ReadCsv(csv1.Path);

            using var csv2 = new DisposableFile("id,name\n2,Bob","csv");
            using var df2 = DataFrame.ReadCsv(csv2.Path);

            using var res = DataFrame.Concat([df1, df2]);

            Assert.Equal(2, res.Height);
            Assert.Equal(2, res.Width);

            Assert.Equal(1, res.GetValue<int>(0, "id"));
            Assert.Equal(2, res.GetValue<int>(1, "id"));
        }

        // --- Horizontal ---
        {
            using var csv1 = new DisposableFile("id\n1\n2",".csv");
            using var df1 = DataFrame.ReadCsv(csv1.Path);

            using var csv2 = new DisposableFile("name,age\nAlice,20\nBob,30",".csv");
            using var df2 = DataFrame.ReadCsv(csv2.Path);

            using var res = DataFrame.ConcatHorizontal([df1, df2]);

            Assert.Equal(2, res.Height);
            Assert.Equal(3, res.Width); // id + name + age

            Assert.NotNull(res.Columns.Contains("id") ? res.GetValue<int>(0, "id") : null);
            Assert.NotNull(res.Columns.Contains("name") ? res.GetValue<string>(0, "name") : null);
            Assert.NotNull(res.Columns.Contains("age") ? res.GetValue<int>(0, "age") : null);
            
            Assert.Equal(1, res.GetValue<int>(0, "id"));
            Assert.Equal("Alice", res.GetValue<string>(0, "name"));
        }

        // --- 3Diagonal ---
        // DF1: [A, B]
        // DF2: [B, C]
        // Result: [A, B, C]
        {
            using var csv1 = new DisposableFile("A,B\n1,10",".csv");
            using var df1 = DataFrame.ReadCsv(csv1.Path);

            using var csv2 = new DisposableFile("B,C\n20,300",".csv");
            using var df2 = DataFrame.ReadCsv(csv2.Path);

            using var res = DataFrame.ConcatDiagonal([df1, df2]);

            Assert.Equal(2, res.Height); 
            Assert.Equal(3, res.Width);  
            
            Assert.Equal(1, res.GetValue<int>(0, "A"));
            Assert.Equal(10, res.GetValue<int>(0, "B"));
            Assert.Null(res.GetValue<int?>(0, "C"));

            // Row 1: A=null, B=20, C=300
            Assert.Null(res.GetValue<int?>(1, "A"));
            Assert.Equal(20, res.GetValue<int>(1, "B"));
            Assert.Equal(300, res.GetValue<int>(1, "C"));
        }
    }
    // ==========================================
    // Reshaping Tests (Pivot & Unpivot)
    // ==========================================
    [Fact]
    [Trait("DataFrame","Pivot")]
    public void Test_Pivot_Unpivot_With_CustomExpr()
    {
        using var df = DataFrame.FromColumns(new
        {
            date = new[] { "2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02" },
            city = new[] { "NY", "LA", "NY", "LA" },
            temp = new[] { 5.0, 20.0, 2.0, 18.0 } 
        });

        // --- Step 1: Standard Pivot ---
        using var pivoted = df.Pivot(
            index: "date",
            on: "city",
            values: "temp",
            aggregateFunction: PivotAgg.First,
            maintainOrder:true
        );
        Assert.Equal(2, pivoted.Height);
        Assert.Equal(3, pivoted.Width); // date, LA, NY (Sorted)

        var cols = pivoted.ColumnNames;
        Assert.Contains("LA", cols);
        Assert.Contains("NY", cols);

        Assert.Equal(20.0, pivoted.GetValue<double>(0, "LA")); 
        Assert.Equal(5.0, pivoted.GetValue<double>(0, "NY")); 

        // --- Step 2: Custom Expr Pivot ---
        
        using var dfWithF = df.WithColumns((Pl.Col("temp") * 1.8 + 32).Alias("temp_f"));
        
        using var pivotedFahrenheit = dfWithF.Pivot(
            index: ["date"],
            on: ["city"],
            values: ["temp_f"],
            aggregateExpr: Pl.Col("").First(),
            maintainOrder:true
        );
        // NY: 5 * 1.8 + 32 = 41
        // LA: 20 * 1.8 + 32 = 68
        Assert.Equal(68.0, pivotedFahrenheit.GetValue<double>(0, "LA"));
        Assert.Equal(41.0, pivotedFahrenheit.GetValue<double>(0, "NY"));

        // --- Step 3: Unpivot/Melt ---
        using var unpivoted = pivoted.Unpivot(
            index: ["date"],
            on: ["LA", "NY"],
            variableName: "city_restored",
            valueName: "temp_restored"
        ).Sort(["date", "city_restored"]);

        Assert.Equal(4, unpivoted.Height);
        
        Assert.Equal("2024-01-01", unpivoted.GetValue<string>(0, "date"));
        Assert.Equal("LA", unpivoted.GetValue<string>(0, "city_restored"));
        Assert.Equal(20.0, unpivoted.GetValue<double>(0, "temp_restored"));
    }
    // ==========================================
    // Display Tests (Head & Show)
    // ==========================================
    [Fact]
    public void Test_Head_And_Show()
    {
        // 0..14
        using var df = DataFrame.FromArrow(
            new RecordBatch.Builder(new NativeMemoryAllocator())
                .Append("id", false, col => col.Int32(arr => arr.AppendRange(Enumerable.Range(0, 15))))
                .Append("name", false, col => col.String(arr => arr.AppendRange(Enumerable.Range(0, 15).Select(i => $"User_{i}"))))
                .Build()
        );

        Assert.Equal(15, df.Height);

        // Test Head/Tail
        using var headDf = df.Head(5);
        Assert.Equal(5, headDf.Height);
        
        Assert.Equal(0, headDf.GetValue<int>(0,"id"));
        Assert.Equal(4, headDf.GetValue<int>(4,"id"));

        using var tailDf = df.Tail(5);
        Assert.Equal(5, tailDf.Height);

        Assert.Equal(10, tailDf.GetValue<int>(0,"id"));
        Assert.Equal(14, tailDf.GetValue<int>(4,"id"));
        // Test Show (No exception should be thrown)
        Console.WriteLine("\n--- Testing DataFrame.Show() output ---");
        df.Show(); 
        
        headDf.Show();
        tailDf.Show();
    }
    [Fact]
    public void Test_Describe_Logic()
    {
        var content = "val\n1\n2\n3\n4\n5\n"; 
        using var csv = new DisposableFile(content,".csv");
        using var df = DataFrame.ReadCsv(csv.Path);

        using var summary = df.Describe();
        
        summary.Show(); 

        Assert.Equal(9, summary.Height);
        
        using var meanRow = summary.Filter(Pl.Col("statistic") == Pl.Lit("mean"));
        Assert.Equal(3.0, meanRow.GetValue<double>(0, "val"));
        
        using var minRow = summary.Filter(Pl.Col("statistic") == Pl.Lit("min"));
        Assert.Equal(1.0, minRow.GetValue<double>(0, "val"));
    }
    // ==========================================
    // Rolling & List & Name Ops Tests
    // ==========================================

    [Fact]
    public void Test_Rolling_Functions()
    {
        var content = @"date,val
2024-01-01,10
2024-01-02,20
2024-01-03,30
2024-01-04,40
2024-01-05,50";
        using var csv = new DisposableFile(content,".csv");
        using var df = DataFrame.ReadCsv(csv.Path, tryParseDates: true);

        // Rolling Mean
        // 10
        // 10,20 -> 15
        // 10,20,30 -> 20
        var rollExpr = Pl.Col("val")
            .RollingMeanBy(windowSize: new TimeSpan(3,0,0,0), by: Pl.Col("date"), closed: ClosedInterval.Left)
            .Alias("roll_mean");

        using var res = df.Select(
            "date",
            "val",
            rollExpr
        );

        // (2024-01-03):  [01, 02, 03) -> 10, 20. Mean = 15. 
        Assert.NotNull(res);
        Assert.Equal(5, res.Height); 
    }

    [Fact]
    public void Test_List_Aggregations_And_Name()
    {
        var content = @"group,val
A,1
A,2
B,3
B,4
B,5";
        using var csv = new DisposableFile(content,".csv");
        using var df = DataFrame.ReadCsv(csv.Path);

        using var res = df
            .GroupBy(Pl.Col("group"))
            .Agg(
                Pl.Col("val").Alias("val_list") 
            )
            .Select(
                Pl.Col("group"),
                Pl.Col("val_list").List.Sum().Name.Suffix("_sum"),
                Pl.Col("val_list").List.Max().Name.Suffix("_max"),
                Pl.Col("val_list").List.Contains(3).Alias("has_3")
            )
            .Sort("group");
        // A (1,2) -> Sum=3, Max=2, Has3=false
        // B (3,4,5) -> Sum=12, Max=5, Has3=true

        Assert.Equal(3, res.GetValue<int>(0,"val_list_sum"));
        Assert.Equal(2, res.GetValue<int>(0,"val_list_max"));
        Assert.False(res.GetValue<bool>(0,"has_3"));

        Assert.Equal(12, res.GetValue<int>(1,"val_list_sum"));
        Assert.Equal(5, res.GetValue<int>(1,"val_list_max"));
        Assert.True(res.GetValue<bool>(1,"has_3"));
    }
    [Fact]
    public void Test_DataFrame_From_Records_With_Decimal()
    {
        var data = new[]
        {
            new { Id = 1, Name = "A", Price = 10.5m },
            new { Id = 2, Name = "B", Price = 20.005m }, // Scale 3
            new { Id = 3, Name = "C", Price = 0m }
        };

        using var df = DataFrame.From(data);
        
        Assert.Equal(3, df.Height);
        Assert.Equal(3, df.Width);

        var priceCol = df.Column("Price");
        Assert.Equal(3, priceCol.Length); 
        Assert.Equal(10.5m, priceCol.GetValue<decimal>(0));   
        Assert.Equal(20.005m, priceCol.GetValue<decimal>(1)); 
        Assert.Equal(0m, priceCol.GetValue<decimal>(2));
    }
    [Fact]
    public void Test_Get_Column_As_Series()
    {
        var data = new[]
        {
            new { Name = "Alice", Age = 30 },
            new { Name = "Bob",   Age = 40 }
        };
        using var df = DataFrame.From(data);

        using var sName = df.Column("Name");
        Assert.Equal("Name", sName.Name);
        Assert.Equal(2, sName.Length);
        Assert.Equal("Alice", sName.GetValue<string>(0));

        using var sAge = df["Age"];
        Assert.Equal("Age", sAge.Name);
        Assert.Equal(2, sAge.Length);
        Assert.Equal(40, sAge.GetValue<int>(1));
        
        Assert.Equal(DataType.Int32, sAge.DataType); 
    }

    [Fact]
    public void Test_Get_Columns_Iterate()
    {
        using var df = DataFrame.From([new { A = 1, B = 2.0 }]);

        var columns = df.GetColumns();
        
        Assert.Equal(2, columns.Length);
        Assert.Equal("A", columns[0].Name);
        Assert.Equal("B", columns[1].Name);
        
        foreach (var col in columns) col.Dispose();
    }
    [Fact]
    public void Test_DataFrame_Explode_Eager()
    {
        // Row 0: "1,2"
        // Row 1: "3"  
        using var s = new Series("nums", ["1,2","3"]);
        using var df = DataFrame.FromSeries(s);

        // ┌──────┬───────────┐
        // │ nums ┆ list_vals │
        // ╞══════╪═══════════╡
        // │ 1,2  ┆ ["1","2"] │
        // │ 3    ┆ ["3"]     │
        // └──────┴───────────┘
        using var dfWithList = df.Select(
            Pl.Col("nums"),
            Pl.Col("nums").Str.Split(",").Alias("list_vals")
        );
        // Explode for all Int32 list 
        using var exploded = dfWithList.Explode(Cs.List(Cs.String()));

        Assert.Equal(3, exploded.Height);

        Assert.Equal("1", exploded["list_vals"][0]);
        Assert.Equal("2", exploded[1][1]);
        Assert.Equal("3", exploded.GetValue<string>("list_vals",2));

        Assert.Equal("1,2", exploded.GetValue<string>(0, "nums"));
        Assert.Equal("1,2", exploded[1,"nums"]);
        Assert.Equal("3",   exploded[2,0]);
    }
    [Fact]
    [Trait("DataFrame","SliceIndexer")]
    public void Test_DataFrame_Slice_With_Range_Indexer()
    {
        // Arrange
        using var s1 = Series.From("A", [10, 20, 30, 40, 50]);
        using var s2 = Series.From("B", ["a", "b", "c", "d", "e"]);
        using var df = new DataFrame(s1, s2);

        using var slice1 = df[1..4];
        Assert.Equal(3, slice1.Height);
        Assert.Equal([20, 30, 40], slice1["A"].ToArray<int>());

        using var slice2 = df[..^2];
        Assert.Equal(3, slice2.Height);
        Assert.Equal(["a", "b", "c"], slice2["B"].ToArray<string>());

        using var slice3 = df[1..3, ["B"]];
        Assert.Equal(2, slice3.Height);
        Assert.Equal(1, slice3.Width);
        Assert.Equal(["b", "c"], slice3["B"].ToArray<string>());
    }
    [Fact]
    [Trait("DataFrame","SliceIndexer")]
    public void Test_DataFrame_Slice_With_IntoSelector_KillerMove()
    {
        // Arrange
        using var s1 = Series.From("Id", [1, 2, 3, 4, 5]);
        using var s2 = Series.From("Score", [99.5, 88.0, 76.5, 100.0, 59.9]);
        using var s3 = Series.From("Name", ["Alice", "Bob", "Charlie", "David", "Eve"]);
        using var s4 = Series.From("IsActive", [true, false, true, true, false]);
        using var df = new DataFrame(s1, s2, s3, s4);

        using var sliceByType = df[1..4, typeof(double)];
        Assert.Equal(["Score"], sliceByType.Columns);
        Assert.Equal(3, sliceByType.Height);
        Assert.Equal([88.0, 76.5, 100.0], sliceByType["Score"].ToArray<double>());

        using Series sliceByString = df[..2, "Name"];
        Assert.Equal("Name", sliceByString.Name);
        Assert.Equal(2, sliceByString.Length);

        using var sliceByExpr = df[..^1, Pl.Col("Id")];
        Assert.Equal(["Id"], sliceByExpr.Columns);
        Assert.Equal(4, sliceByExpr.Height);

        using var sliceBySelector = df[2..4, Cs.Numeric()];
        Assert.Equal(["Id", "Score"], sliceBySelector.Columns); 
        Assert.Equal(2, sliceBySelector.Height);
    }
    [Fact]
    [Trait("DataFrame","SelectorIndexer")]
    public void Test_DataFrame_ColumnIndexer_With_IntoSelector_Sugar()
    {
        // Arrange
        using var s1 = Series.From("Id", [1, 2, 3]);
        using var s2 = Series.From("Score", [99.5, 88.0, 76.5]);
        using var s3 = Series.From("Name", ["Alice", "Bob", "Charlie"]);
        using var df = new DataFrame(s1, s2, s3);

        using var nameSeries = df["Name"];
        Assert.IsType<Series>(nameSeries);
        Assert.Equal("Name", nameSeries.Name);

        using DataFrame multiColDf = df[["Id", "Score"]];
        Assert.IsType<DataFrame>(multiColDf);
        Assert.Equal(2, multiColDf.Width);

        using var dfByType = df[typeof(double)];
        Assert.IsType<DataFrame>(dfByType);
        Assert.Equal(["Score"], dfByType.Columns);

        using var dfByExpr = df[Pl.Col("Id")];
        Assert.IsType<DataFrame>(dfByExpr);
        Assert.Equal(["Id"], dfByExpr.Columns);

        using var dfBySelector = df[Cs.Numeric()];
        Assert.IsType<DataFrame>(dfBySelector);
        Assert.Equal(["Id", "Score"], dfBySelector.Columns);
    }
    [Fact]
    [Trait("DataFrame","SelectorIndexer")]
    public void Test_DataFrame_SingleRow_MultiColumn_Indexer()
    {
        // Arrange
        using var s1 = Series.From("Id", [1, 2, 3, 4]);
        using var s2 = Series.From("Score", [99.5, 88.0, 76.5, 100.0]);
        using var s3 = Series.From("Name", ["Alice", "Bob", "Charlie", "David"]);
        using var df = new DataFrame(s1, s2, s3);

        using var row1 = df[1, ["Id", "Name"]];
        Assert.IsType<DataFrame>(row1);
        Assert.Equal(1, row1.Height);
        Assert.Equal(["Id", "Name"], row1.Columns);
        Assert.Equal(2, row1[0, "Id"]);        
        Assert.Equal("Bob", row1[0, "Name"]);

        using var lastRow = df[^1, typeof(double)];
        Assert.IsType<DataFrame>(lastRow);
        Assert.Equal(1, lastRow.Height);
        Assert.Equal(["Score"], lastRow.Columns);
        Assert.Equal(100.0, lastRow[0, "Score"]); 
    }
    [Fact]
    [Trait("DataFrame","SelectorIndexer")]
    public void Test_DataFrame_Selector_Setter()
    {
        // Arrange
        using var df = new DataFrame(
            Series.From("Id", ["A", "B"]),     // String
            Series.From("Val1", [1.0, 2.0]),   // Double
            Series.From("Val2", [10.0, 20.0])  // Double
        );
        
        df[Cs.Numeric()] *= 2;

        // Assert
        Assert.Equal(3, df.Width);
        Assert.Equal(["Id", "Val1", "Val2"], df.Columns);
        Assert.Equal([2.0, 4.0], df["Val1"].ToArray<double>());
        Assert.Equal([20.0, 40.0], df["Val2"].ToArray<double>());
        Assert.Equal(["A", "B"], df["Id"].ToArray<string>()); 
    }
    [Fact]
    [Trait("DataFrame","SelectorIndexer")]
    public void Test_DataFrame_Indexer_RowMask_And_ColumnSelector_HappyPath()
    {
        using var df = new DataFrame(
            Series.From("Name", ["Alice", "Bob", "Charlie", "David"]),
            Series.From("Age", [15, 22, 25, 17]),                        
            Series.From("Score", [88.5, 92.0, 85.5, 70.0])               
        );

        using var resultDf = df[~(df["Age"] < 18), Cs.Numeric()];

        Assert.Equal(2, resultDf.Height);
        Assert.Equal(2, resultDf.Width);

        Assert.Equal(["Age", "Score"], resultDf.Columns);

        Assert.Equal([22, 25], resultDf["Age"].ToArray<int>());
        Assert.Equal([92.0, 85.5], resultDf["Score"].ToArray<double>());
    }

    [Fact]
    [Trait("DataFrame","SelectorIndexer")]
    public void Test_DataFrame_Indexer_RowMask_And_Selector_EmptyMatch()
    {
        using var df = new DataFrame(
            Series.From("Name", ["Alice", "Bob"]),
            Series.From("City", ["New York", "London"])
        ); 

        using var mask = Series.From("mask", [true, true]); 

        using var resultDf = df[mask, Cs.Numeric()];

        Assert.NotNull(resultDf);
        Assert.True(resultDf.IsVoid);
    }
    [Fact]
    public void Test_Column_ByIndex_And_Iteration()
    {
        var df = DataFrame.FromColumns(new 
        {
            Name = new[] { "A", "B" }, // Index 0
            Age = new[] { 10, 20 },    // Index 1
            Score = new[] { 99, 88 }   // Index 2
        });

        var col0 = df.Column(0);
        Assert.Equal("Name", col0.Name);
        Assert.Equal("A", col0[0]);

        var col2 = df[2];
        Assert.Equal("Score", col2.Name);
        Assert.Equal(99, col2.Cast(DataType.Int32)[0]);

        Assert.Throws<IndexOutOfRangeException>(() => df[99]);
        Assert.Throws<IndexOutOfRangeException>(() => df[-1]);

        int count = 0;
        foreach (var series in df)
        {
            if (count == 0) Assert.Equal("Name", series.Name);
            if (count == 1) Assert.Equal("Age", series.Name);
            count++;
        }
        Assert.Equal(3, count);
    }
    [Fact]
    [Trait("DataFrame", "Cast")]
    public void Test_Cast_With_Explicit_Schema()
    {
        using var df = Pl.DataFrame(
            Pl.Series("Id", ["1", "2", "3"]), 
            Pl.Series("IsActive", [1, 0, 1])  
        );

        using var targetSchema = new PolarsSchema()
            .Add("Id", typeof(sbyte))       
            .Add("IsActive", typeof(bool)); 

        using var resultDf = df.Cast(targetSchema);

        Assert.Equal(DataType.Int8, resultDf.Schema["Id"]);
        Assert.Equal(DataType.Boolean, resultDf.Schema["IsActive"]);
        Assert.Equal((sbyte)1, resultDf[0][0]);

        Assert.True((bool)resultDf[1][0]);
    }

    [Fact]
    [Trait("DataFrame", "Cast")]
    public void Test_Cast_With_Another_DataFrame_Implicitly()
    {
        using var masterDf = Pl.DataFrame(
            Pl.Series("UserId", [101, 102]),          // Int32
            Pl.Series("Score", [99.5, 88.0]),      // Float64
            Pl.Series("Tag", ["A", "B"])           // String
        );
        using var targetDf = Pl.DataFrame(
            Pl.Series("UserId", ["103", "104"]),   
            Pl.Series("Score", [70, 60]),           
            Pl.Series("Tag", ["C", "D"])         
        );

        using var resultDf = targetDf.Cast(masterDf);

        Assert.Equal(DataType.Int32, resultDf.Schema["UserId"]);
        Assert.Equal(DataType.Float64, resultDf.Schema["Score"]);
        Assert.Equal(DataType.String, resultDf.Schema["Tag"]);

        Assert.Equal(70.0, resultDf["Score"][0]); 
    }

    [Fact]
    [Trait("DataFrame", "Cast")]
    public void Test_Cast_With_Empty_Schema_Returns_Self()
    {
        using var df = Pl.DataFrame(Pl.Series("A", [1, 2, 3]));
        using var emptySchema = new PolarsSchema();

        using var resultDf = df.Cast(emptySchema);

        Assert.Equal(DataType.Int32, resultDf.Schema["A"]);
    }
    [Fact]
    public void Test_DataFrame_Sort_Advanced()
    {
        // A: [1, 1, 2, 2]
        // B: [null, 10, null, 5]
        using var df = DataFrame.FromColumns(new 
        {
            A = new[] { 1, 1, 2, 2 },
            B = new int?[] { null, 10, null, 5 }
        });

        // 1. Sort by A asc, B desc (nulls last)
        // A=1 : B=[null, 10]。B desc nulls last -> [10, null]
        // A=2 : B=[null, 5]。 B desc nulls last -> [5, null]
        // row 1: A=1, B=10
        // row 0: A=1, B=null
        // row 3: A=2, B=5
        // row 2: A=2, B=null

        using var sorted = df.Sort(
            [Pl.Col("A"), Pl.Col("B")],
            descending: [false, true], // A asc, B desc
            nullsLast: [false, true]   // A normal, B nulls last
        );

        Assert.Equal(10, sorted["B"][0]);
        Assert.Null(sorted["B"][1]);
        Assert.Equal(5, sorted["B"][2]);
        Assert.Null(sorted["B"][3]);
    }
    [Fact]
    public void Test_DataFrame_TopK_Eager()
    {
        var data = new[] { 1, 100, 50, 2 };
        using var df = DataFrame.FromColumns(new { val = data });

        using var top = df.TopK(2, "val");

        Assert.Equal(2, top.Height);
        var arr = top["val"].ToArray<int>();
        Assert.Contains(100, arr);
        Assert.Contains(50, arr);
    }
    [Fact]
    [Trait("DataFrame","JoinAsOf")]
    public void Test_DataFrame_JoinAsOf_Eager()
    {
        // Left: [10:00, 10:02], val_l = [1, 2]
        var datesL = new[] 
        { 
            new DateTime(2023, 1, 1, 10, 0, 0),
            new DateTime(2023, 1, 1, 10, 2, 0)
        };
        using var dfLeft = DataFrame.FromColumns(new { ts = datesL, val_l = new[] { 1, 2 } });

        // Right: [09:59, 10:00, 10:01, 10:03], val_r = [10, 20, 30, 40]
        var datesR = new[] 
        { 
            new DateTime(2023, 1, 1, 9, 59, 0),
            new DateTime(2023, 1, 1, 10, 0, 0), // Match for 10:00
            new DateTime(2023, 1, 1, 10, 1, 0), // Match for 10:02 (backward)
            new DateTime(2023, 1, 1, 10, 3, 0)
        };
        using var dfRight = DataFrame.FromColumns(new { ts = datesR, val_r = new[] { 10, 20, 30, 40 } });

        // JoinAsOf (Backward strategy)
        using var res = dfLeft.JoinAsOf(
            dfRight,
            on: "ts",
            tolerance: null,
            strategy: AsofStrategy.Backward
        );

        Assert.Equal(2, res.Height);
        
        var rVals = res["val_r"];
        Assert.Equal(20, rVals[0]); // 10:00 matched 10:00
        Assert.Equal(30, rVals[1]); // 10:02 matched 10:01 (closest previous)
    }
    [Fact]
    [Trait("DataFrame","Slice")]
    public void Test_DataFrame_Slice()
    {
        var df = DataFrame.FromSeries(
            Series.From("Fruit", ["Apple", "Grape", "Grape", "Fig", "Fig"]),
            Series.From("Color", ["Green", "Red", "White", "White", "Red"])
        );

        // 0: Apple, Green
        // 1: Grape, Red
        // 2: Grape, White  <-- Start
        // 3: Fig,   White
        // 4: Fig,   Red    <-- End (Length 3)
        using var sl = df.Slice(2);

        Assert.Equal(3, sl.Height);
        Assert.Equal(2, sl.Width);

        Assert.Equal("Grape", sl["Fruit"].GetValue<string>(0));
        Assert.Equal("Fig",   sl["Fruit"].GetValue<string>(1));
        Assert.Equal("Fig",   sl["Fruit"].GetValue<string>(2));
        
        using var slOverflow = df.Slice(4, 100);
        Assert.Equal(1, slOverflow.Height); 
        Assert.Equal("Fig", slOverflow["Fruit"].GetValue<string>(0));
    }
    [Fact]
    [Trait("DataFrame", "InsertColumn")]
    public void Test_DataFrame_InsertColumn_SyntaxSugar()
    {
        // Setup initial DataFrame: 
        // a = [1, 2, 3]
        // b = [4, 5, 6]
        using var df = DataFrame.FromColumns(new 
        { 
            a = new[] { 1, 2, 3 }, 
            b = new[] { 4, 5, 6 } 
        });

        Assert.Equal(2, df.Width);
        Assert.Equal("a", df.Columns[0]);
        Assert.Equal("b", df.Columns[1]);

        // Insert at index 1 -> [a, a_times_10, b]
        using var df1 = df.InsertColumn(1, (Pl.Col("a") * 10).Alias("a_times_10"));
        
        Assert.Equal(3, df1.Width);
        Assert.Equal("a_times_10", df1.Columns[1]);
        Assert.Equal(20, df1["a_times_10"][1]); 

        // int -> IntoExpr -> pl.lit
        // Insert at index 0 -> [literal, a, a_times_10, b]
        using var df2 = df1.InsertColumn(0, 99); 
        
        Assert.Equal(4, df2.Width);
        Assert.Equal("literal", df2.Columns[0]); 
        Assert.Equal(99, df2["literal"][0]);
        Assert.Equal(99, df2["literal"][2]); 

        // Width is 4. Index -1 -> width + (-1) = 3 -> [literal, a, a_times_10, new_series, b]
        using var s = Series.From("new_series", [7, 8, 9]);
        using var df3 = df2.InsertColumn(-1, s);
        
        Assert.Equal(5, df3.Width);
        Assert.Equal("new_series", df3.Columns[3]);
        Assert.Equal("b", df3.Columns[4]);
        Assert.Equal(8, df3["new_series"][1]);

        Assert.Throws<ArgumentOutOfRangeException>(() => df3.InsertColumn(10, 100));
        Assert.Throws<ArgumentOutOfRangeException>(() => df3.InsertColumn(-10, 100));
    }
    [Fact]
    [Trait("DataFrame","Unique")]
    public void Test_Unique()
    {
        var df = DataFrame.From(
        [
            new { A = 1, B = "x" },
            new { A = 2, B = "y" },
            new { A = 1, B = "x" }, // Duplicate
            new { A = 3, B = "z" }
        ]);

        // 1. Default (All cols, Keep First)
        var res1 = df.Unique(maintainOrder:true);
        Assert.Equal(3, res1.Height);
        Assert.Equal(1, res1["A"][0]); // Order preserved
        Assert.Equal(2, res1["A"][1]);
        Assert.Equal(3, res1["A"][2]);

        // 2. Subset (Check only A)
        var df2 = DataFrame.From(
        [
            new { A = 1, B = "x" },
            new { A = 1, B = "y" } // Duplicate on A
        ]);
        
        var res2 = df2.Unique("A", UniqueKeepStrategy.Last);
        Assert.Equal(1, res2.Height);
        Assert.Equal("y", res2["B"][0]); // Should keep the last one ("y")

        // 3. Keep None (Remove all duplicates)
        var res3 = df.Unique(keep:UniqueKeepStrategy.None);
        Assert.Equal(2, res3.Height); // A=2 and A=3 are unique. A=1 appears twice so both removed.

        // Unique by selector
        var res4 = df2.Unique(Cs.Numeric());
        Assert.Equal(1, res4.Height);

    }
    [Fact]
    [Trait("DataFrame","VStack")]
    public void Test_HStack_VStack()
    {
        // --- Test HStack ---
        using var df1 = DataFrame.FromColumns(
            Series.From("a",[1,2,3])
        );
        
        // New Column:[b]
        using var sNew = new Series("b", [10, 20, 30]);

        // HStack -> [a, b]
        using var hStacked = df1.HStack(sNew);

        Assert.Equal(3, hStacked.Height);
        Assert.Equal(2, hStacked.Width);
        Assert.Equal("a", hStacked.Columns[0]);
        Assert.Equal("b", hStacked.Columns[1]);
        Assert.Equal(10, hStacked["b"][0]);

        // --- 2. Test VStack ---
        // DF2: [a, b]
        using var df2 = DataFrame.FromColumns(new 
        { 
            a = new[] { 4, 5 }, 
            b = new[] { 40, 50 } 
        });

        // VStack: hStacked (3 rows) + df2 (2 rows) -> 5 rows
        using var vStacked = hStacked.VStack(df2);

        Assert.Equal(5, vStacked.Height);
        Assert.Equal(2, vStacked.Width);
        Assert.Equal(2L,vStacked.NChunks());

        var rechunked = vStacked.Rechunk();

        Assert.Equal(1L,rechunked.NChunks());
        
        // Row 0 (from df1)
        Assert.Equal(1, vStacked["a"][0]);
        Assert.Equal(10, vStacked["b"][0]);
        
        // Row 3 (from df2, index 0)
        Assert.Equal(4, vStacked["a"][3]);
        Assert.Equal(40, vStacked["b"][3]);
    }
    [Fact]
    [Trait("DataFrame","Extend")]
    public void Test_Extend()
    {
        using var df1 = DataFrame.FromColumns(
            Series.From("a",[1,2,3]),
            Series.From("b",[10,20,30])
        );
        // DF2: [a, b]
        using var df2 = DataFrame.FromColumns(new 
        { 
            a = new[] { 4, 5 }, 
            b = new[] { 40, 50 } 
        });

        using var extended = df1.Extend(df2);

        Assert.Equal(5, extended.Height);
        Assert.Equal(2, extended.Width);
        Assert.Equal(1L,extended.NChunks());
        
        // Row 0 (from df1)
        Assert.Equal(1, extended["a"][0]);
        Assert.Equal(10, extended["b"][0]);
        
        // Row 3 (from df2, index 0)
        Assert.Equal(4, extended["a"][3]);
        Assert.Equal(40, extended["b"][3]);
    }
    [Fact]
    [Trait("DataFrame", "AsTensor")]
    public void AsTensor_AllColumnsMatch_ReturnsRowMajor2DTensor()
    {
        // Arrange
        var s1 = Series.From("feature1", [1.1f, 2.1f, 3.1f]);
        var s2 = Series.From("feature2", [1.2f, 2.2f, 3.2f]);
        using var df = new DataFrame(s1, s2);

        // Act
        Tensor<float> tensor = df.AsTensor<float>();

        // Assert
        Assert.Equal(2, tensor.Rank);
        Assert.Equal(3, tensor.Lengths[0]); // Rows
        Assert.Equal(2, tensor.Lengths[1]); // Columns

        // Row 0
        Assert.Equal(1.1f, tensor[0, 0]);
        Assert.Equal(1.2f, tensor[0, 1]);
        
        // Row 1
        Assert.Equal(2.1f, tensor[1, 0]);
        Assert.Equal(2.2f, tensor[1, 1]);
        
        // Row 2
        Assert.Equal(3.1f, tensor[2, 0]);
        Assert.Equal(3.2f, tensor[2, 1]);

        float[] expectedFlat = [1.1f, 1.2f, 2.1f, 2.2f, 3.1f, 3.2f];
        Assert.True(tensor.SequenceEqual(expectedFlat));
    }

    [Fact]
    [Trait("DataFrame", "AsTensorSelected")]
    public void AsTensor_SelectedColumns_Returns2DTensor()
    {
        // Arrange
        var s1 = Series.From("id", [1, 2]); 
        var s2 = Series.From("feature1", [0.1f, 0.2f]);
        var s3 = Series.From("feature2", [0.9f, 0.8f]);
        using var df = new DataFrame(s1, s2, s3);

        // Act
        Tensor<float> tensor = df.AsTensor<float>("feature1", "feature2");

        // Assert
        Assert.Equal(2, tensor.Rank);
        Assert.Equal(2, tensor.Lengths[0]); // 2 Rows
        Assert.Equal(2, tensor.Lengths[1]); // 2 Columns

        // Row 0
        Assert.Equal(0.1f, tensor[0, 0]); // feature1
        Assert.Equal(0.9f, tensor[0, 1]); // feature2
        
        // Row 1
        Assert.Equal(0.2f, tensor[1, 0]); // feature1
        Assert.Equal(0.8f, tensor[1, 1]); // feature2
    }

    [Fact]
    [Trait("DataFrame", "AsTensorException")]
    public void AsTensor_TypeMismatch_ThrowsInvalidOperationException()
    {
        // Arrange
        var s1 = Series.From("age", [25, 30]); // Int32
        var s2 = Series.From("salary", [5000.5f, 6000.5f]); // Single
        using var df = new DataFrame(s1, s2);

        var exception = Assert.ThrowsAny<Exception>(() => 
        {
            df.AsTensor<float>();
        });

        Assert.NotNull(exception);
    }

    [Fact]
    [Trait("DataFrame", "AsTensorEmpty")]
    public void AsTensor_EmptyDataFrame_ThrowsInvalidOperationException()
    {
        // Arrange
        using var emptyDf = new DataFrame(); 
        
        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => 
        {
            emptyDf.AsTensor<float>();
        });
        
        Assert.Contains("Cannot create a Tensor from an empty DataFrame", exception.Message);
    }
    [Fact]
    [Trait("DataFrame","HashRows")]
    public void Test_DataFrame_HashRows()
    {
        using var scoresDf = DataFrame.FromColumns(new 
        {
            student = new[] { "Alice", "Alice", "Bob" },
            year    = new[] { 2023,    2024,    2023 },
            score   = new[] { 85,      90,      70 },
            note    = new[] { "Score1", "Score2", "Score3" } 
        });

        var hashNull = scoresDf.HashRows(seed:null);
        var hash42 = scoresDf.HashRows(seed:42);
        
        Assert.False(hashNull.IsEmpty);
        Assert.False(hash42.IsEmpty);
    }
    [Fact]
    [Trait("DataFrame","ToStruct")]
    public void Test_DataFrame_To_Struct()
    {
        using var df = DataFrame.FromColumns(new 
        {
            Name = new[] { "A", "B" }, 
            Age = new[] { 10, 20 },    
            Score = new[] { 99, 88 }   
        });
        var structSeries = df.ToStruct(name:"struct");

        Assert.Equal("struct",structSeries.Name);
        Assert.Equal(DataType.Struct(["Name","Age","Score"],[DataType.String,DataType.Int32,DataType.Int32]),structSeries.DataType);
        var dfBack = structSeries.Unnest();
        Assert.Equal((2L,3L),dfBack.Shape);
    }
    [Fact]
    [Trait("DataFrame","NUnique")]
    public void Test_DataFrame_NUnique()
    {
        using var df = DataFrame.FromColumns(new 
        {
            Name = new[] { "A", "B","A" }, 
            Age = new[] { 10, 20,10 },    
            Score = new[] { 99, 88,99 }   
        });
        long unique = df.NUnique(Cs.String());
        Assert.Equal(2L,unique);
    }
    [Fact]
    [Trait("DataFrame","Clear")]
    public void Test_DataFrame_Clear()
    {
        using var df = DataFrame.FromColumns(new 
        {
            Name = new[] { "A", "B","A" }, 
            Age = new[] { 10, 20,10 },    
            Score = new[] { 99, 88,99 }   
        });
        var clearDf = df.Clear();
        Assert.Equal(clearDf.Schema,df.Schema);
        Assert.True(clearDf.IsEmpty);

        var clearDf1 = df.Clear(1);
        Assert.Equal(1L,clearDf1.Height);
        Assert.Null(clearDf1[0][0]);
    }
    [Fact]
    [Trait("DataFrame","Drop")]
    public void Test_Drop()
    {
        var df = DataFrame.From(
        [
            new { A = 1, B = 2.2, C = "hello" },
            new { A = 3, B = 4.4, C = "world" }
        ]);

        var res = df.Drop(Cs.String());
        Assert.Equal(2, res.Width);

        var res2 = df.Drop("A","B");
        Assert.Equal(1,res2.Width);
        
        Assert.Throws<ArgumentException>(() => res["C"]);
    }
    [Fact]
    [Trait("DataFrame","DropInPlace")]
    public void Test_DropInPlace()
    {
        using DataFrame df = DataFrame.FromRows(
        [
            new { A = 1, B = 2.2, C = "hello" },
            new { A = 3, B = 4.4, C = "world" }
        ]);

        using Series res = df.DropInPlace("A");
        Assert.Equal(2, res.Length);
        Assert.Equal(1,res[0]);
        Assert.Equal(2,df.Width);

    }
    [Fact]
    [Trait("DataFrame","UnstackVertical")]
    public void Unstack_Vertical_WithDefaultNullFill_ShouldWork()
    {
        // shape: (5, 2)
        var df = DataFrame.FromColumns(new
        {
            foo = new[] { 1, 2, 3, 4, 5 },
            bar = new[] { "a", "b", "c", "d", "e" }
        });

        using var unstacked = df.Unstack(step: 2, how: UnstackDirection.Vertical);

        // Assert
        Assert.Equal(2, unstacked.Height);
        Assert.Equal(6, unstacked.Width);

        var cols = unstacked.Columns;
        Assert.Contains("foo_0", cols);
        Assert.Contains("foo_1", cols);
        Assert.Contains("foo_2", cols);
        Assert.Contains("bar_2", cols);

        var foo0 = unstacked["foo_0"].ToArray<int?>();
        Assert.Equal(1, foo0[0]);
        Assert.Equal(2, foo0[1]);

        var foo2 = unstacked["foo_2"].ToArray<int?>();
        Assert.Equal(5, foo2[0]);
        Assert.Null(foo2[1]);

        var bar2 = unstacked["bar_2"].ToArray<string>();
        Assert.Equal("e", bar2[0]);
        Assert.Null(bar2[1]);
    }

    [Fact]
    [Trait("DataFrame","UnstackHorizontal")]
    public void Unstack_Horizontal_ShouldSortCorrectly()
    {
        // Arrange
        var df = DataFrame.FromColumns(new
        {
            val = new[] { 10, 20, 30, 40, 50 }
        });

        using var unstacked = df.Unstack(step: 3, how: UnstackDirection.Horizontal);

        // _0: 10, 40
        // _1: 20, 50
        // _2: 30, null
        Assert.Equal(2, unstacked.Height);
        Assert.Equal(3, unstacked.Width);

        var val0 = unstacked["val_0"].ToArray<int?>();
        Assert.Equal(10, val0[0]);
        Assert.Equal(40, val0[1]);

        var val1 = unstacked["val_1"].ToArray<int?>();
        Assert.Equal(20, val1[0]);
        Assert.Equal(50, val1[1]);

        var val2 = unstacked["val_2"].ToArray<int?>();
        Assert.Equal(30, val2[0]);
        Assert.Null(val2[1]);
    }

    [Fact]
    [Trait("DataFrame","UnstackVerticalBroadcast")]
    public void Unstack_Vertical_WithCustomFillValues_ShouldBroadcastAndFill()
    {
        // Arrange
        var df = DataFrame.FromColumns(new
        {
            id = new[] { 1, 2, 3 },
            name = new[] { "Alice", "Bob", "Charlie" }
        });


        object?[] fills = [-1, "Unknown"];
        using var unstacked = df.Unstack(step: 2, how: UnstackDirection.Vertical, fillValues: fills);

        // Assert
        Assert.Equal(2, unstacked.Height);

        var id1 = unstacked["id_1"].ToArray<int?>();
        Assert.Equal(3, id1[0]);
        Assert.Equal(-1, id1[1]);

        var name1 = unstacked["name_1"].ToArray<string>();
        Assert.Equal("Charlie", name1[0]);
        Assert.Equal("Unknown", name1[1]);
    }

    [Fact]
    [Trait("DataFrame","UnstackSelected")]
    public void Unstack_WithSpecificColumns_ShouldOnlyUnstackSelected()
    {
        // Arrange
        var df = DataFrame.FromColumns(new
        {
            A = new[] { 1, 2, 3 },
            B = new[] { 4, 5, 6 },
            C = new[] { 7, 8, 9 }
        });

        using var unstacked = df.Unstack(step: 2, columns: ["A", "C"]);

        // Assert
        Assert.Equal(4, unstacked.Width); // A_0, A_1, C_0, C_1
        Assert.Contains("A_0", unstacked.Columns);
        Assert.Contains("C_0", unstacked.Columns);
        Assert.DoesNotContain("B_0", unstacked.Columns);
    }
    [Fact]
    [Trait("DataFrame", "Remove")]
    public void Test_Remove_By_Expr_Keeps_Null_Conditions()
    {
        // Id:  [1, 2, 3, 4, 5]
        // Val: [10, 20, null, 40, 50]
        using var df = DataFrame.FromColumns(new 
        {
            Id = new[] { 1, 2, 3, 4, 5 },
            Val = new int?[] { 10, 20, null, 40, 50 }
        });

        using var resultDf = df.Remove((Pl.Col("Val") > 25) | (Pl.Col("Id")<3));
        Assert.Equal(1, resultDf.Height);
        Assert.Equal(3, resultDf["Id"][0]);
        Assert.Null(resultDf["Val"][0]); 
    }

    [Fact]
    [Trait("DataFrame", "Remove")]
    public void Test_Remove_By_Boolean_Series()
    {
        using var df = Pl.DataFrame(
            Pl.Series("Id", [1, 2, 3, 4])
        );

        using var mask = Pl.Series("mask", [true, false, true, false]);

        using var resultDf = df.Remove(mask);

        Assert.Equal(2, resultDf.Height);
        Assert.Equal(2, resultDf["Id"][0]);
        Assert.Equal(4, resultDf["Id"][1]);
    }

    [Fact]
    [Trait("DataFrame", "Remove")]
    public void Test_Remove_By_Boolean_Array()
    {
        using var df = Pl.DataFrame(
            Pl.Series("Name", ["Alice", "Bob", "Charlie"])
        );

        bool[] mask = [false, false, true];

        using var resultDf = df.Remove(mask);

        Assert.Equal(2, resultDf.Height);
        Assert.Equal("Alice", resultDf["Name"][0]);
        Assert.Equal("Bob", resultDf["Name"][1]);
    }

    [Fact]
    [Trait("DataFrame", "Remove")]
    public void Test_Remove_Throws_On_NonBoolean_Series()
    {
        using var df = Pl.DataFrame(
            Pl.Series("Id", [1, 2, 3])
        );

        using var invalidMask = Pl.Series("mask", [1, 0, 1]);

        var ex = Assert.Throws<InvalidOperationException>(() => df.Remove(invalidMask));
        Assert.Contains("non-boolean", ex.Message);
    }
    [Fact]
    [Trait("DataFrame","IterSlices")]
    public void IterSlices_ShouldYieldCorrectSlices()
    {
        var df = DataFrame.FromColumns(new
        {
            Id = Enumerable.Range(1, 25).ToArray(),
            Name = Enumerable.Range(1, 25).Select(i => $"User_{i}").ToArray()
        });

        var slices = df.IterSlices(nRows: 10).ToList();

        // Assert
        Assert.Equal(3, slices.Count); 

        Assert.Equal(10, slices[0].Height);
        Assert.Equal(10, slices[1].Height);
        Assert.Equal(5, slices[2].Height); 

        var lastSliceIdSeries = slices[2]["Id"];
        Assert.Equal(21, (int)lastSliceIdSeries[0]!); 
        Assert.Equal(25, (int)lastSliceIdSeries[4]!); 
    }

    [Fact]
    [Trait("DataFrame","IterSlices")]
    public void IterSlices_WithInvalidNRows_ShouldThrowException()
    {
        // Arrange
        var df = DataFrame.FromColumns(new { Id = new[] { 1, 2, 3 } });

        // Act & Assert
        var ex1 = Assert.Throws<ArgumentOutOfRangeException>(() => df.IterSlices(0).ToList());
        Assert.Contains("greater than zero", ex1.Message);

        var ex2 = Assert.Throws<ArgumentOutOfRangeException>(() => df.IterSlices(-5).ToList());
        Assert.Contains("greater than zero", ex2.Message);
    }
    [Fact]
    [Trait("DataFrame","Equal")]
    public void Equals_StrictAndMissing_ShouldWorkCorrectly()
    {
        // Arrange
        var df1 = DataFrame.FromColumns(new 
        { 
            A = new int?[] { 1, null, 3 }, 
            B = new[] { "x", "y", "z" } 
        });
        
        var df2 = DataFrame.FromColumns(new 
        { 
            A = new int?[] { 1, null, 3 }, 
            B = new[] { "x", "y", "z" } 
        });
        
        var df3 = DataFrame.FromColumns(new 
        { 
            A = new int?[] { 1, 2, 3 }, // 不同的数据
            B = new[] { "x", "y", "z" } 
        });

        // Act & Assert

        Assert.True(df1.Equals(df2));
        Assert.True(df1 == df2); 
        Assert.False(df1 != df2);

        Assert.False(df1.Equals(df3));
        Assert.False(df1 == df3);
        Assert.True(df1 != df3);

        Assert.False(df1.Equals(df2, nullEqual: false));
    }

    [Fact]
    [Trait("DataFrame","Equal")]
    public void GetHashCode_ShouldThrowNotSupportedException()
    {
        // Arrange
        var df = DataFrame.FromColumns(new { Id = new[] { 1, 2, 3 } });

        // Act & Assert
        var ex = Assert.Throws<NotSupportedException>(() => df.GetHashCode());
        Assert.Contains("cannot be hashed directly", ex.Message);
        
        var dict = new Dictionary<DataFrame, int>();
        Assert.Throws<NotSupportedException>(() => dict.Add(df, 1));
    }
    [Fact]
    [Trait("DataFrame","PartitionBy")]
    public void PartitionBy_ShouldReturnCorrectArraySlices()
    {
        // Arrange
        var df = DataFrame.FromColumns(new
        {
            Group = new[] { "A", "A", "B", "B", "C" },
            Value = new[] { 1, 2, 3, 4, 5 }
        });

        // Act
        var partitions = df.PartitionBy(Cs.String());

        // Assert
        Assert.Equal(3, partitions.Length);

        Assert.Equal(2, partitions[0].Height);
        Assert.Equal("A", (string)partitions[0]["Group"][0]!);

        Assert.Equal(2, partitions[1].Height);
        Assert.Equal("B", (string)partitions[1]["Group"][0]!);

        Assert.Equal(1, partitions[2].Height);
        Assert.Equal("C", (string)partitions[2]["Group"][0]!);
    }

    [Fact]
    [Trait("DataFrame","PartitionBy")]
    public void PartitionByAsDict_ShouldWorkCorrectlyWithArrayKeys()
    {
        // Arrange
        var df = DataFrame.FromColumns(new
        {
            Region = new[] { "US", "US", "CN" },
            Year   = new[] { 2023, 2023, 2024 },
            Sales  = new[] { 100,  200,  300 }
        });

        // Act: 根据两列进行 Partition
        var dict = df.PartitionByAsDict(["Region", "Year"],maintainOrder: true, includeKey: true);

        // Assert
        Assert.Equal(2, dict.Count);

        var keyUS = new object?[] { "US", 2023 };
        var keyCN = new object?[] { "CN", 2024 };

        Assert.True(dict.ContainsKey(keyUS));
        Assert.True(dict.ContainsKey(keyCN));

        Assert.Equal(2, dict[keyUS].Height); 
        Assert.Equal(1, dict[keyCN].Height); 
    }

    [Fact]
    [Trait("DataFrame","PartitionBy")]
    public void PartitionByAsDict_InvalidCombination_ShouldThrow()
    {
        // Arrange
        var df = DataFrame.FromColumns(new { A = new[] { 1, 2, 1 } });

        // Act & Assert
        var ex = Assert.Throws<ArgumentException>(() => 
            df.PartitionByAsDict(["A"],maintainOrder: false, includeKey: false));
            
        Assert.Contains("Group keys cannot be matched to partitions", ex.Message);
    }
    [Fact]
    [Trait("DataFrame","ReplaceColumn")]
    public void Replace_ByIndex_ShouldModifyInPlaceAndSupportChaining()
    {
        // Arrange
        var df = DataFrame.FromColumns(new
        {
            Col1 = new[] { 1, 2, 3 },
            Col2 = new[] { 4, 5, 6 },
            Col3 = new[] { 7, 8, 9 }
        });

        var newSeries2 = Series.From("New_Col2", [40, 50, 60]);
        var newSeries3 = Series.From("New_Col3", [70, 80, 90]);

        df.ReplaceColumn(1, newSeries2)
          .ReplaceColumn(-1, newSeries3);
        // Assert
        Assert.Equal(3, df.Width);
        
        Assert.Equal(40, df[1][0]!);
        Assert.Equal(90, df[2][2]!);

        Assert.Equal("New_Col2", df.Columns[1]);
        Assert.Equal("New_Col3", df.Columns[2]);
    }

    [Fact]
    [Trait("DataFrame","ReplaceColumn")]
    public void Replace_ByName_ShouldModifyInPlace()
    {
        // Arrange
        var df = DataFrame.FromColumns(new
        {
            A = new[] { 1, 2, 3 },
            B = new[] { 4, 5, 6 }
        });

        var newColA = Series.From("A_Modified", [10, 20, 30]);

        // Act
        df.ReplaceColumn("A", newColA,keepName:false);
        // Assert
        Assert.Equal(10, (int)df["A_Modified"][0]!); 
        Assert.DoesNotContain("A", df.Columns);
    }

    [Fact]
    [Trait("DataFrame","ReplaceColumn")]
    public void Replace_WithShapeMismatch_ShouldThrowPolarsException()
    {
        // Arrange
        var df = DataFrame.FromColumns(new { A = new[] { 1, 2, 3 } });
        var badSeries = Series.From("A", [1, 2]); 

        // Act & Assert
        var ex = Assert.Throws<PolarsException>(() => df.ReplaceColumn("A", badSeries));
        Assert.Contains("lengths don't match", ex.Message);
    }

    [Fact]
    [Trait("DataFrame","ReplaceColumn")]
    public void Replace_WithInvalidIndex_ShouldThrowArgumentOutOfRangeException()
    {
        // Arrange
        var df = DataFrame.FromColumns(new { A = new[] { 1, 2, 3 } });
        var s = Series.From("A", [10, 20, 30]);

        Assert.Throws<ArgumentOutOfRangeException>(() => df.ReplaceColumn(5, s));
        Assert.Throws<ArgumentOutOfRangeException>(() => df.ReplaceColumn(-5, s)); 
    }
    [Fact]
    [Trait("DataFrame", "WithRowIndex")]
    public void WithRowIndex_DefaultParameters_ShouldGenerateCorrectIndex()
    {
        // Arrange
        var df = DataFrame.FromColumns(new
        {
            Value = new[] { "A", "B", "C" }
        });

        // Act
        var result = df.WithRowIndex();

        // Assert
        Assert.Equal(2, result.Width);
        Assert.Equal("index", result.Columns[0]); 
        Assert.Equal("Value", result.Columns[1]);

        var indexArray = result["index"].ToArray<uint>();
        Assert.Equal(new uint[] { 0, 1, 2 }, indexArray);
    }

    [Fact]
    [Trait("DataFrame", "WithRowIndex")]
    public void WithRowIndex_CustomParameters_ShouldGenerateCorrectIndex()
    {
        // Arrange
        var df = DataFrame.FromColumns(new
        {
            Value = new[] { "X", "Y", "Z", "W" }
        });

        // Act
        // name="row_id", offset=10
        var result = df.WithRowIndex("row_id",10);

        // Assert
        Assert.Equal("row_id", result.Columns[0]);

        var indexArray = result["row_id"].ToArray<uint>();
        Assert.Equal(new uint[] { 10, 11, 12, 13 }, indexArray);
    }

    [Fact]
    [Trait("DataFrame", "Update")]
    public void Update_HowOuter_ShouldAddNewRows()
    {
        // Arrange
        var left = DataFrame.FromColumns(new
        {
            Id = new[] { 1, 2, 3 },
            ValueA = new int?[] { 10, 20, 30 },
            ValueB = new string[] { "x", "y", "z" }
        });
    
        var right =DataFrame.FromColumns(new
        {
            Id = new[] { 2, 3, 4 },
            ValueA = new int?[] { 200, null, 400 }, 
            ValueB = new string[] { "yy", "zz", "ww" }
        });
    
        // Act
        var result = left.Update(
            right, 
            on: ["Id"], 
            how: JoinType.Outer,
            maintainOrder: JoinMaintainOrder.Left
        );

        // Assert
        var idArray = result["Id"].ToArray<int>();
        var valAArray = result["ValueA"].ToArray<int?>();

        Assert.Equal([1, 2, 3, 4], idArray);

        // Id 1: 10
        // Id 2: 200 (updated)
        // Id 3: 30  (keep)
        // Id 4: 400 (added)
        Assert.Equal([10, 200, 30, 400], valAArray);
    }
    [Fact]
    [Trait("DataFrame", "Merge")]
    public void MergeBuilder_WithSelector_ShouldResolveColumnsDynamically()
    {
        // Arrange
        var targetDf = DataFrame.FromColumns(new
        {
            Id1 = new[] { 1, 2 },
            Id2 = new[] { 10, 20 },
            Name = new[] { "A", "B" },
            Value = new[] { 100.0, 200.0 }
        });

        var sourceDf = DataFrame.FromColumns(new
        {
            Id1 = new[] { 2, 3 },
            Id2 = new[] { 20, 30 },
            Name = new[] { "B_Updated", "C_New" },
            Value = new[] { 999.0, 888.0 }
        });

        var resultDf = targetDf.Merge(sourceDf, on: Cs.StartsWith("Id")).Execute();

        // Assert
        var id1Array = resultDf["Id1"].ToArray<int>();
        var nameArray = resultDf["Name"].ToArray<string>();

        Assert.Equal([1, 2, 3], id1Array);
        Assert.Equal(["A", "B_Updated", "C_New"], nameArray);
    }
    [Fact]
    [Trait("DataFrame", "MergeCondition")]
    public void MergeBuilder_WithComplexConditions_ShouldHandleFullDataLifecycle()
    {
        // Arrange: 
        var targetDf = DataFrame.FromColumns(new
        {
            Id = new[] { 1, 2, 3, 4, 5 },
            Category = new[] { "Seasonal", "Core", "Core", "Tech", "Tech" },
            Price = new[] { 10.0, 20.0, 30.0, 100.0, 200.0 },
            IsDiscontinued = new[] { false, false, false, false, false }
        });

        var sourceDf = DataFrame.FromColumns(new
        {
            Id = new[] { 3, 4, 5, 6, 7 },
            Category = new[] { "Core", "Tech", "Tech", "New", "Trash" },
            Price = new[] { 30.0, 120.0, 190.0, 50.0, 0.0 },
            IsDiscontinued = new[] { true, false, false, false, false } 
        });

        var resultDf = targetDf.Merge(sourceDf, "Id")
            // If matched as IsDiscontinued delete
            .WhenMatchedDelete(m => m.Source("IsDiscontinued") == true)
            // If matched price up then update
            .WhenMatchedUpdate(m => m.Source("Price") > m.Target("Price"))
            // Only insert price > 0 in source table
            .WhenNotMatchedInsert(m => m.Source("Price") > 0.0)
            // If record in source table is missing then delete target
            .WhenNotMatchedBySourceDelete(m => m.Target("Category") == "Seasonal")
            // Inspect plan to console
            .InspectPlan(verbose: false) 
            .Execute(); 

        resultDf = resultDf.Sort("Id");

        var idArray = resultDf["Id"].ToArray<int>();
        var priceArray = resultDf["Price"].ToArray<double>();

        // Assert: 
        // Id 1 (Target): Seasonal item missed in source table -> NotMatchedBySourceDelete -> Delete
        // Id 2 (Target): Core item missed in source table -> Keep
        // Id 3 (Both): IsDiscontinued=true -> MatchedDelete -> Delete
        // Id 4 (Both): Source Price(120) > Target Price(100) -> WhenMatchedUpdate -> Price: 120.0
        // Id 5 (Both): Source Price(190) < Target Price(200) -> Keep
        // Id 6 (Source): New Item in source and price(50) > 0 -> WhenNotMatchedInsert -> Insert
        // Id 7 (Source): New Item in source but price(0) is invalid -> Discard
        
        Assert.Equal([2, 4, 5, 6], idArray);
        Assert.Equal([20.0, 120.0, 200.0, 50.0], priceArray);
    }

    [Fact]
    [Trait("DataFrame", "MergeOrderAndExplain")]
    public void MergeBuilder_OrderSensitive_ShouldRespectFirstMatchAndExplain()
    {
        var targetDf = DataFrame.FromColumns(new
        {
            Id = new[] { 1, 2, 3 },
            Score = new[] { 10, 20, 30 }
        });

        var sourceDf = DataFrame.FromColumns(new
        {
            Id = new[] { 2, 3, 4 },
            Score = new[] { 99, 15, 100 }
        });

        var mergeBuilder = targetDf.Merge(sourceDf, "Id")
            .WhenMatchedDelete(m => m.Source("Score") > 90)
            .WhenMatchedUpdate(m => m.Source("Score") > 10)
            .WhenNotMatchedInsert().MaintainOrder(JoinMaintainOrder.Left);

        Console.WriteLine(mergeBuilder);

        var resultDf = mergeBuilder.Execute();

        var idArray = resultDf["Id"].ToArray<int>();
        var scoreArray = resultDf["Score"].ToArray<int>();

        // Id = 1 : Target only，keep (10)
        // Id = 2 : Delete(99 > 90)
        // Id = 3 : Update(15 > 10, 15)
        // Id = 4 : Insert(100)。
        Assert.Equal([1, 3, 4], idArray);
        Assert.Equal([10, 15, 100], scoreArray);
    }
    [Fact]
    [Trait("DataFrame", "MergeIncludeNulls")]
    public void MergeBuilder_IncludeNulls_ShouldControlNullOverwrite()
    {
        var targetDf = DataFrame.FromColumns(new
        {
            Id = new[] { 1, 2, 3 },
            Score = new int?[] { 10, 20, 30 }
        });

        var sourceDf = DataFrame.FromColumns(new
        {
            Id = new[] { 1, 2, 3 },
            Score = new int?[] { null, 99, null }
        });

        // Act 1: includeNulls = false
        var resultWithoutNulls = targetDf.Merge(sourceDf, "Id")
            .IncludeNulls(false)
            .Execute()
            .Sort("Id");

        // Act 2:includeNulls = true
        var resultWithNulls = targetDf.Merge(sourceDf, "Id")
            .IncludeNulls(true)
            .Execute()
            .Sort("Id");

        var scoreArrayWithoutNulls = resultWithoutNulls["Score"].ToArray<int?>();
        var scoreArrayWithNulls = resultWithNulls["Score"].ToArray<int?>();

        // includeNulls = false：
        Assert.Equal([10, 99, 30], scoreArrayWithoutNulls);

        // includeNulls = true：
        Assert.Equal([null, 99, null], scoreArrayWithNulls);
    }
    [Fact]
    [Trait("DataFrame", "MergePartialUpdate")]
    public void MergeBuilder_WithSetters_ShouldPerformPartialColumnUpdates()
    {
        var targetDf = DataFrame.FromColumns(new
        {
            Id = new[] { 1, 2, 3 },
            Name = new[] { "Apple", "Banana", "Orange" },
            Price = new[] { 1.0, 2.0, 3.0 },
            Stock = new[] { 100, 150, 200 }
        });

        var sourceDf = DataFrame.FromColumns(new
        {
            Id = new[] { 2, 3, 4 },
            Name = new[] { "Banana_Ignored", "Orange_Ignored", "Grape" }, 
            Price = new[] { 2.5, 2.5, 4.0 }, 
            RestockQty = new[] { 50, 0, 200 }
        });

        var resultDf = targetDf.Merge(sourceDf, "Id")
            .WhenMatchedUpdate(
                condition: m => m.Source("Price") > m.Target("Price"),
                set: s => s
                    .Set("Price", m => m.Source("Price")) 
                    .Set("Stock", m => m.Target("Stock") + m.Source("RestockQty")) 
            )
            .WhenNotMatchedInsert(
                condition: null, 
                set: s => s
                    .Set("Id", m => m.Source("Id"))
                    .Set("Name", m => m.Source("Name"))
                    .Set("Price", m => m.Source("Price"))
                    .Set("Stock", m => m.Source("RestockQty")) 
            )
            .InspectPlan(verbose: false) 
            .Execute();

        resultDf = resultDf.Sort("Id");

        var idArray = resultDf["Id"].ToArray<int>();
        var nameArray = resultDf["Name"].ToArray<string>();
        var priceArray = resultDf["Price"].ToArray<double>();
        var stockArray = resultDf["Stock"].ToArray<int>();

        // Id 1: Target only -> Keep (Apple, 1.0, 100)
        // Id 2: Matched & Hit(2.5 > 2.0) -> Price updated to 2.5, Stock updated (150+50)=200, Name keep "Banana"
        // Id 3: Matched & Miss(2.5 < 3.0) -> Keep (Orange, 3.0, 200)
        // Id 4: Source only -> Insert (Grape, 4.0, 200)
        
        Assert.Equal([1, 2, 3, 4], idArray);
        Assert.Equal(["Apple", "Banana", "Orange", "Grape"], nameArray);
        Assert.Equal([1.0, 2.5, 3.0, 4.0], priceArray);
        Assert.Equal([100, 200, 200, 200], stockArray);
    }
    [Fact]
    [Trait("DataFrame", "MergeSelectorUpdate")]
    public void MergeBuilder_WithSelectors_ShouldPerformBatchColumnUpdates()
    {
        var targetDf = DataFrame.FromColumns(new
        {
            Id = new[] { 1, 2, 3 },
            Name = new[] { "Hero", "Villain", "NPC" },
            Stat_HP = new[] { 100, 100, 10 },    
            Stat_MP = new[] { 50, 50, 0 },       
            Score = new[] { 1000, 500, 0 },
            Tag = new[] { "Old", "Old", "Old" }
        });

        var sourceDf = DataFrame.FromColumns(new
        {
            Id = new[] { 1, 2, 4 },
            Name = new[] { "Hero_Ignored", "Villain", "Newbie" }, 
            Stat_HP = new[] { 120, 0, 80 },
            Stat_MP = new[] { 60, 0, 40 },
            ScoreDelta = new[] { 200, 0, 50 },  
            IsBanned = new[] { false, true, false }
        });

        var resultDf = targetDf.Merge(sourceDf, "Id")
            .WhenMatchedDelete(m => m.Source("IsBanned")) 
            .WhenMatchedUpdate(
                condition: null, 
                set: s => s
                    .Set(Cs.StartsWith("Stat_"), (m, colName) => m.Source(colName))
                    .Set("Score", m => m.Target("Score") + m.Source("ScoreDelta"))
                    .Set("Tag", Pl.Lit("Updated"))
            )
            .WhenNotMatchedInsert(
                condition: null,
                set: s => s
                    .Set("Id", m => m.Source("Id"))
                    .Set("Name", m => m.Source("Name"))
                    .Set(Cs.StartsWith("Stat_"), (m, colName) => m.Source(colName))
                    .Set("Score", m => m.Source("ScoreDelta"))
                    .Set("Tag", Pl.Lit("New"))
            )
            .InspectPlan(verbose: false) 
            .Execute();

        resultDf = resultDf.Sort("Id");

        var idArray = resultDf["Id"].ToArray<int>();
        var nameArray = resultDf["Name"].ToArray<string>();
        var hpArray = resultDf["Stat_HP"].ToArray<int>();
        var scoreArray = resultDf["Score"].ToArray<int>();
        var tagArray = resultDf["Tag"].ToArray<string>();

        // Id 1 (Hero):    Matched -> NotBanned -> Bulked update HP/MP, score 1000+200=1200, Tag -> Updated, name kept as Hero
        // Id 2 (Villain): Matched -> IsBanned -> Deleted
        // Id 3 (NPC):     Target Only -> Keep
        // Id 4 (Newbie):  Source Only -> Insert

        Assert.Equal([1, 3, 4], idArray);
        Assert.Equal(["Hero", "NPC", "Newbie"], nameArray);
        Assert.Equal([120, 10, 80], hpArray);
        Assert.Equal([1200, 0, 50], scoreArray);
        Assert.Equal(["Updated", "Old", "New"], tagArray);
    }
    [Fact]
    [Trait("DataFrame", "MergeCompositeKeys")]
    public void MergeBuilder_WithMultipleKeys_ShouldPerformCompositeKeyMerge()
    {
        //  Composite Keys: TenantId, UserId
        var targetDf = DataFrame.FromColumns(new
        {
            TenantId = new[] { "T1", "T1", "T2", "T2" },
            UserId = new[] { 101, 102, 101, 999 },
            Role = new[] { "Admin", "User", "User", "Guest" },
            IsActive = new[] { true, true, true, false }
        });

        var sourceDf = DataFrame.FromColumns(new
        {
            TenantId = new[] { "T1", "T2", "T2" },
            UserId = new[] { 102, 101, 888 },
            Role = new[] { "Editor", "Admin", "User" }, 
            IsActive = new[] { true, true, true }
        });
        Selector selector = Cs.EndsWith("Id");
        var resultDf = targetDf.Merge(sourceDf, selector) 
            .WhenMatchedUpdate(
                set: s => s
                    .Set("Role", (m, c) => m.Source("Role"))
                    .Set("IsActive", true)
            )
            .WhenNotMatchedBySourceDelete()
            .WhenNotMatchedInsert(
                set: s => s
                    .Set("TenantId", m => m.Source("TenantId"))
                    .Set("UserId", m => m.Source("UserId"))
                    .Set("Role", m => m.Source("Role"))
                    .Set("IsActive", true)
            )
            .InspectPlan(verbose: false) 
            .Execute();

        resultDf = resultDf.Sort(selector); 

        var tIds = resultDf["TenantId"].ToArray<string>();
        var uIds = resultDf["UserId"].ToArray<int>();
        var roles = resultDf["Role"].ToArray<string>();

        Assert.Equal(["T1", "T2", "T2"], tIds);
        Assert.Equal([102, 101, 888], uIds);
        Assert.Equal(["Editor", "Admin", "User"], roles);
    }
    [Fact]
    [Trait("DataFrame", "MergeValidation")]
    public void MergeBuilder_Validation_ThrowsOnTypeMismatch()
    {
        var targetDf = DataFrame.FromColumns(new
        {
            Id = new[] { 1, 2, 3 },
            Value = new[] { 10, 20, 30 }
        });

        var sourceDf = DataFrame.FromColumns(new
        {
            Id = new[] { "1", "2" }, 
            Value = new[] { 100, 200 }
        });

        // Act & Assert
        var ex = Assert.Throws<ArgumentException>(() =>
        {
            targetDf.Merge(sourceDf, "Id").Execute();
        });
        Assert.Contains("Merge Key Type Mismatch", ex.Message);
        Assert.Contains("Source: str", ex.Message);
        Assert.Contains("Target: i32", ex.Message);
    }

    [Fact]
    [Trait("DataFrame", "MergeValidation")]
    public void MergeBuilder_Validation_ThrowsOnNullMergeKey()
    {
        var targetDf = DataFrame.FromColumns(new
        {
            Id = new int[] { 1, 2, 3 },
            Value = new string[] { "A", "B", "C" }
        });

        var sourceDf = DataFrame.FromColumns(new
        {
            Id = new int?[] { 1, null, 3 }, 
            Value = new string[] { "A1", "B1", "C1" }
        });

        // Act & Assert
        var ex = Assert.Throws<InvalidDataException>(() =>
        {
            targetDf.Merge(sourceDf, "Id")
                .WhenMatchedUpdate()
                .Execute();
        });
        
        Assert.Contains("CRITICAL ERROR: Null values detected in Merge Keys", ex.Message);
    }

    [Fact]
    [Trait("DataFrame", "MergeValidation")]
    public void MergeBuilder_Validation_ThrowsOnDuplicateSourceKeys()
    {
        var targetDf = DataFrame.FromColumns(new
        {
            TenantId = new[] { "T1", "T2" },
            UserId = new[] { 101, 102 },
            Score = new[] { 10, 20 }
        });

        var sourceDf = DataFrame.FromColumns(new
        {
            TenantId = new[] { "T1", "T1", "T2" }, 
            UserId = new[] { 101, 101, 102 },      
            Score = new[] { 100, 999, 200 }
        });

        // Act & Assert
        var ex = Assert.Throws<InvalidDataException>(() =>
        {
            targetDf.Merge(sourceDf, Cs.EndsWith("Id")) 
                .WhenMatchedUpdate()
                .Execute();
        });
        Assert.Contains("CRITICAL ERROR: Duplicate keys detected in SOURCE table", ex.Message);
        Assert.Contains("duplicate_count", ex.Message);
    }
    [Fact]
    [Trait("DataFrame", "Transpose")]
    public void Test_DataFrame_Transpose()
    {
        // ID="A1", Val1=10, Val2=100
        // ID="B2", Val1=20, Val2=200
        using var df = DataFrame.FromColumns(new 
        {
            ID = new[] { "A1", "B2" },
            Val1 = new[] { 10, 20 },
            Val2 = new[] { 100, 200 }
        });

        // ==============================================================
        // Set a new column name
        // ==============================================================
        using var t1 = df.Transpose(columnName: "ID", includeHeader: true, headerName: "metrics");
        
        Assert.Equal(2, t1.Height);
        Assert.Equal(3, t1.Width);

        Assert.Equal("metrics", t1.ColumnNames[0]);
        Assert.Equal("A1", t1.ColumnNames[1]);
        Assert.Equal("B2", t1.ColumnNames[2]);

        Assert.Equal("Val1", t1.GetValue<string>(0, "metrics"));
        Assert.Equal("Val2", t1.GetValue<string>(1, "metrics"));
        
        Assert.Equal(10, t1.GetValue<int>(0, "A1"));   
        Assert.Equal(100, t1.GetValue<int>(1, "A1"));  

        // ==============================================================
        // Default Column Names
        // ==============================================================
        using var t2 = df.Transpose(includeHeader: false);
        
        Assert.Equal(3, t2.Height);
        Assert.Equal(2, t2.Width);
        Assert.Equal("column_0", t2.ColumnNames[0]);
        Assert.Equal("column_1", t2.ColumnNames[1]);

        // Type Coercion: 
        Assert.Equal("A1", t2.GetValue<string>(0, "column_0"));
        Assert.Equal("10", t2.GetValue<string>(1, "column_0"));  
        Assert.Equal("100", t2.GetValue<string>(2, "column_0"));

        // ==============================================================
        // CustomNames
        // ==============================================================
        using var t3 = df.Transpose(customNames: ["Row_Alpha", "Row_Beta"], includeHeader: true, headerName: "Original_Column");

        Assert.Equal(3, t3.Height);
        Assert.Equal(3, t3.Width);

        Assert.Equal("Original_Column", t3.ColumnNames[0]);
        Assert.Equal("Row_Alpha", t3.ColumnNames[1]);
        Assert.Equal("Row_Beta", t3.ColumnNames[2]);

        Assert.Equal("ID", t3.GetValue<string>(0, "Original_Column"));
        Assert.Equal("Val1", t3.GetValue<string>(1, "Original_Column"));
    }
    [Fact]
    [Trait("DataFrame", "Upsample")]
    public void Test_DataFrame_Upsample_With_Selectors()
    {
        var dates = new[]
        {
            new DateTime(2024, 1, 1),
            new DateTime(2024, 1, 3),
            new DateTime(2024, 1, 1),
            new DateTime(2024, 1, 2)
        };
        var groups = new[] { "A", "A", "B", "B" };
        var vals = new int?[] { 10, 30, 100, 200 };

        using var df = DataFrame.FromColumns(new { ts = dates, grp = groups, val = vals });
        using var sorted = df.Sort(["grp", "ts"]);

        using var upsampled = sorted.Upsample(
            timeColumn: Cs.Temporal(),  
            every: TimeSpan.FromDays(1), 
            groupBy: Cs.String(), 
            maintainOrder: true
        );
        upsampled.Show();
        Assert.Equal(5, upsampled.Height);
        Assert.Equal(3, upsampled.Width);

        using var groupA = upsampled.Filter(Pl.Col("grp") == Pl.Lit("A"));
        Assert.Equal(3, groupA.Height);

        Assert.Equal(10, groupA.GetValue<int>(0, "val"));

        Assert.Equal(new DateTime(2024, 1, 2), groupA.GetValue<DateTime>(1, "ts"));
        Assert.Null(groupA.GetValue<int?>(1, "val"));
        
        Assert.Equal(30, groupA.GetValue<int>(2, "val"));
    }
    [Fact]
    [Trait("DataFrame", "ToDummies")]
    public void Test_DataFrame_ToDummies()
    {
        var ids = new[] { 1, 2, 3, 4 };
        var groups = new[] { "A", "B", "A", "C" };
        var colors = new[] { "Red", "Blue", "Red", "Red" };

        using var df = DataFrame.FromColumns(new 
        { 
            id = ids, 
            group = groups, 
            color = colors 
        });

        using var dummiesAll = df.ToDummies();

        Assert.Equal(6, dummiesAll.Width);
        Assert.Contains("group_A", dummiesAll.ColumnNames);
        Assert.Contains("group_C", dummiesAll.ColumnNames);
        Assert.Contains("color_Red", dummiesAll.ColumnNames);

        // 验证第一行 id=1, group=A, color=Red
        Assert.Equal(1, dummiesAll.GetValue<byte>(0, "group_A"));
        Assert.Equal(0, dummiesAll.GetValue<byte>(0, "group_B"));

        using var dummiesSingle = df.ToDummies(columns: "group", dropFirst: true);
        
        Assert.Equal(4, dummiesSingle.Width);
        Assert.Contains("color", dummiesSingle.ColumnNames); 
        Assert.Contains("group_B", dummiesSingle.ColumnNames);
        Assert.Contains("group_C", dummiesSingle.ColumnNames);
        Assert.DoesNotContain("group_A", dummiesSingle.ColumnNames);

        using var dummiesSelector = df.ToDummies(columns: Cs.String(), separator: "-");
        
        Assert.Contains("group-B", dummiesSelector.ColumnNames);
        Assert.Contains("color-Blue", dummiesSelector.ColumnNames);
    }
    [Fact]
    [Trait("DataFrame", "ShrinkToFit")]
    public void Test_DataFrame_ShrinkToFit_MemoryReduction()
    {
        using var idExpr = Pl.IntRange(0, 100_000).Alias("id");
        using var idSeries = Series.FromExpr(idExpr);

        using var valSeries = Series.FromExpr((idExpr.Cast<double>() * 1.1).Alias("value"));

        using var df = new DataFrame(idSeries, valSeries);

        using var slicedDf = df.Slice(0..10);

        slicedDf.ShrinkToFitInplace();
        
        var idArray = slicedDf["id"].ToArray<int>();
        Assert.Equal(0, idArray[0]);
        Assert.Equal(9, idArray[^1]);
    }
    [Fact]
    [Trait("DataFrame", "AlignFrames")]
    public void Test_AlignFrames_OuterJoin_And_Null_Padding()
    {
        var df1 = DataFrame.FromColumns(new 
        { 
            Time = (int[])[1, 2, 3], 
            ValA = (double[])[1.1, 2.2, 3.3] 
        });
        var df2 = DataFrame.FromColumns(new 
        { 
            Time = (int[])[2, 3, 4], 
            ValB = (double[])[20.0, 30.0, 40.0] 
        });
        var df3 = DataFrame.FromColumns(new 
        { 
            Time = (int[])[1, 3, 5], 
            ValC = (double[])[100.0, 300.0, 500.0] 
        });

        DataFrame[] aligned = Pl.AlignFrames(
            frames: [df1, df2, df3], 
            on: ["Time"], 
            how: JoinType.Outer 
        );

        Assert.Equal(3, aligned.Length);
        foreach (var df in aligned)
        {
            Assert.Equal(5u, df.Height);
            
            Assert.Equal(1, df[0, "Time"]);
            Assert.Equal(5, df[4, "Time"]);
        }

        Assert.Null(aligned[0][3, "ValA"]); 
        Assert.Null(aligned[0][4, "ValA"]); // Time = 5

        Assert.Null(aligned[1][0, "ValB"]); 
        Assert.Equal(20.0, aligned[1][1, "ValB"]); 
    }

    [Fact]
    [Trait("DataFrame", "AlignFrames")]
    public void Test_AlignFrames_Name_Collision_Resolution()
    {
        var df1 = DataFrame.FromColumns(new { Id = (int[])[1, 2], Price = (int[])[10, 20] });
        var df2 = DataFrame.FromColumns(new { Id = (int[])[2, 3], Price = (int[])[200, 300] });
        var df3 = DataFrame.FromColumns(new { Id = (int[])[3, 4], Price = (int[])[3000, 4000] }); 

        var aligned = Pl.AlignFrames(
            frames: [df1, df2, df3], 
            on: ["Id"], 
            how: JoinType.Outer
        );

        Assert.Equal(["Id", "Price"], aligned[0].Columns);
        Assert.Equal(["Id", "Price"], aligned[1].Columns);
        Assert.Equal(["Id", "Price"], aligned[2].Columns);

        Assert.Equal(20, aligned[0][1, "Price"]); 
        Assert.Equal(200, aligned[1][1, "Price"]); 
    }

    [Fact]
    [Trait("DataFrame", "AlignFramesAsync")]
    public async Task Test_AlignFramesAsync_With_Select_Projection()
    {
        var df1 = DataFrame.FromColumns(new { Date = (string[])["2023"], Open = (int[])[10], Close = (int[])[15] });
        var df2 = DataFrame.FromColumns(new { Date = (string[])["2023"], Open = (int[])[20], Close = (int[])[25] });

        DataFrame[] aligned = await Pl.AlignFramesAsync(
            frames: [df1, df2], 
            on: ["Date"], 
            select: ["Date", "Close"]
        );

        Assert.Equal(2, aligned.Length);

        Assert.Equal(["Date", "Close"], aligned[0].Columns);
        Assert.Equal(["Date", "Close"], aligned[1].Columns);

        Assert.Equal(15, aligned[0][0, "Close"]);
        Assert.Equal(25, aligned[1][0, "Close"]);
    }
    [Fact]
    [Trait("Factory", "FromDicts")]
    public void Test_FromDictionaries_BasicInference_And_MissingValues()
    {
        var data = new List<Dictionary<string, object?>>
        {
            new() { { "id", 1 }, { "name", "Alice" } },
            new() { { "id", 2 }, { "age", 25 } }, 
            new() { { "id", 3 }, { "name", "Bob" }, { "age", null } }
        };

        using var df = DataFrame.FromDicts(data);

        Assert.Equal(3, df.Height);
        Assert.Equal(3, df.Width); // id, name, age

        Assert.Equal(DataType.Int32, df.Schema["id"]);
        Assert.Equal(DataType.String, df.Schema["name"]);
        Assert.Equal(DataType.Int32, df.Schema["age"]);
    }

    [Fact]
    [Trait("Factory", "FromDicts")]
    public void Test_FromDictionaries_TypePromotion()
    {
        var data = new List<Dictionary<string, object?>>
        {
            new() { { "value", 100 } },        // typeof(int)
            new() { { "value", 99.9 } }      // typeof(double)
        };

        using var df = DataFrame.FromDicts(data);

        Assert.Equal(2, df.Height);
        Assert.Equal(DataType.Float64, df.Schema["value"]);
    }

    [Fact]
    [Trait("Factory", "FromDictionaries")]
    public void Test_FromDictionaries_StrictSchema_FastPath()
    {
        var data = new List<Dictionary<string, object?>>
        {
            new() { { "id", 1 }, { "secret_key", "xxx" }, { "score", 95.5 } }
        };

        // Dictionary -> IntoSchema
        var schema = new Dictionary<string, DataType>
        {
            { "score", DataType.Float32 }, 
            { "id", DataType.Int64 }       
        };

        using var df = DataFrame.FromDicts(data, schema: schema);

        Assert.Equal(2, df.Width);
        
        var columns = df.Columns;
        Assert.Equal("score", columns[0]);
        Assert.Equal("id", columns[1]);

        Assert.Equal(DataTypeKind.Float32, df.Schema["score"].Kind);
        Assert.Equal(DataTypeKind.Int64, df.Schema["id"].Kind);
    }

    [Fact]
    [Trait("Factory", "FromDictionaries")]
    public void Test_FromDictionaries_SchemaOverrides()
    {
        var data = new List<Dictionary<string, object?>>
        {
            new() { { "id", 1 }, { "flag", 1 } }
        };

        var overrides = new (string, DataType)[] 
        { 
            ("flag", DataType.Boolean) 
        };

        using var df = DataFrame.FromDicts(data, schemaOverrides: overrides);

        Assert.Equal(2, df.Width);
        Assert.Equal(DataType.Int32, df.Schema["id"]);
        Assert.Equal(DataType.Boolean, df.Schema["flag"]);
    }

    [Fact]
    [Trait("Factory", "FromDicts")]
    public void Test_FromDictionaries_AutoInferNamesOnly()
    {
        var data = new List<Dictionary<string, object?>>
        {
            new() { { "col_A", "text" }, { "col_B", 42 }, { "col_C", 3.14 } }
        };

        string[] names = ["col_A", "col_B"]; 

        using var df = DataFrame.FromDicts(data, schema: names);

        Assert.Equal(2, df.Width);
        
        Assert.Equal(DataType.String, df.Schema["col_A"]);
        Assert.Equal(DataType.Int32, df.Schema["col_B"]);
    }
    [Fact]
    [Trait("DataFrame", "JsonNormalize")]
    public void Test_JsonNormalize_BasicFlattening()
    {
        var data = new Dictionary<string, object?>
        {
            { "id", 1 },
            { "user", new Dictionary<string, object?>
                {
                    { "name", "Alice" },
                    { "address", new Dictionary<string, object?>
                        {
                            { "city", "Tokyo" },
                            { "zip", "100-0001" }
                        }
                    }
                }
            }
        };

        // Act 
        using var df = Pl.JsonNormalize(data);

        // Assert
        Assert.Equal(1, df.Height);
        Assert.Equal(4, df.Width);

        var columns = df.Columns;
        Assert.Contains("id", columns);
        Assert.Contains("user.name", columns);
        Assert.Contains("user.address.city", columns);
        Assert.Contains("user.address.zip", columns);

        Assert.Equal(DataType.Int32, df.Schema["id"]);
        Assert.Equal(DataType.String, df.Schema["user.address.city"]);
    }

    [Fact]
    [Trait("DataFrame", "JsonNormalize")]
    public void Test_JsonNormalize_ListAndCustomSeparator()
    {
        var data = new List<Dictionary<string, object?>>
        {
            new() { { "device", new Dictionary<string, object?> { { "sensor", 42.5 } } } },
            new() { { "device", new Dictionary<string, object?> { { "sensor", 99.9 } } } }
        };

        using var df = Pl.JsonNormalize(data, separator: "_");

        Assert.Equal(2, df.Height);
        Assert.Single(df.Columns);
        
        Assert.Equal("device_sensor", df.Columns[0]);
        Assert.Equal(DataType.Float64, df.Schema["device_sensor"]);
    }

    [Fact]
    [Trait("DataFrame", "JsonNormalize")]
    public void Test_JsonNormalize_MaxLevelCutoff()
    {
        var data = new Dictionary<string, object?>
        {
            { "L1", new Dictionary<string, object?>
                {
                    { "L2", new Dictionary<string, object?>
                        {
                            { "L3", "deep_value" }
                        }
                    }
                }
            }
        };

        // 1. MaxLevel = 0
        using var df0 = Pl.JsonNormalize(data, maxLevel: 0);
        Assert.Single(df0.Columns);
        Assert.Equal("L1", df0.Columns[0]);
        Assert.Equal(DataType.String, df0.Schema["L1"]); 

        // 2. MaxLevel = 1: Flatten L1 -> L2
        using var df1 = Pl.JsonNormalize(data, maxLevel: 1);
        Assert.Single(df1.Columns);
        Assert.Equal("L1.L2", df1.Columns[0]);
        Assert.Equal(DataType.String, df1.Schema["L1.L2"]);
    }

    [Fact]
    [Trait("DataFrame", "JsonNormalize")]
    public void Test_JsonNormalize_WithSchemaIntegration()
    {
        var data = new List<Dictionary<string, object?>>
        {
            new() {
                { "meta", new Dictionary<string, object?> { { "tag", "A" }, { "version", 1 }, { "garbage", "xyz" } } }
            },
            new() {
                { "meta", new Dictionary<string, object?> { { "tag", "B" } } }
            }
        };

        var strictSchema = new Dictionary<string, DataType>
        {
            { "meta.tag", typeof(string) },
            { "meta.version", typeof(double) }
        };

        using var df = Pl.JsonNormalize(data, schema: strictSchema);

        Assert.Equal(2, df.Height);
        Assert.Equal(2, df.Width);
        Assert.Equal(DataType.Float64, df.Schema["meta.version"]);
    }
    
    [Fact]
    [Trait("DataFrame", "JsonNormalize")]
    public void Test_JsonNormalize_EmptyData()
    {
        var emptyData = new List<Dictionary<string, object?>>();
        using var df = Pl.JsonNormalize(emptyData);
        
        Assert.True(df.IsVoid);
    }
    [Fact]
    [Trait("DataFrame", "JsonNormalize")]
    public void Test_JsonNormalize_CustomEncoder()
    {
        var data = new Dictionary<string, object?>
        {
            { "event_id", 1024 },
            { "payload", new Dictionary<string, object?>
                {
                    { "action", "click" },
                    { "timestamp", "2024-01-01T12:00:00Z" }
                }
            }
        };

        bool encoderWasCalled = false;
        string myCustomEncoder(object obj)
        {
            encoderWasCalled = true;
            return $"<CustomEncoded>{JsonSerializer.Serialize(obj)}</CustomEncoded>";
        }

        // Act: maxLevel = 0 
        using var df = Pl.JsonNormalize(data, maxLevel: 0, encoder: myCustomEncoder);

        // Assert
        Assert.Equal(1, df.Height);
        Assert.Equal(2, df.Width);

        Assert.Equal(DataType.Int32, df.Schema["event_id"]);
        Assert.Equal(DataType.String, df.Schema["payload"]);

        Assert.True(encoderWasCalled, "Custom Encoder is not called!");

        string payloadValue = df["payload"].GetValue<string>(0); 
        Assert.StartsWith("<CustomEncoded>", payloadValue);
    }
    [Fact]
    [Trait("DataFrame", "Repr")]
    public void Test_DataFrame_Repr()
    {
        using DataFrame df = [
            Pl.Series("nihao",[1,2,3]),
            Pl.Series("buhao",[new DateTime(2026,5,1,10,0,0),new DateTime(2026,5,2,12,0,0),new DateTime(2026,5,3,14,0,0)])
        ];

        // using var df1 = df.WithColumns(Pl.Col("buhao").Dt.ReplaceTimeZone("Asia/Shanghai").Alias("genghao"));

        var df2 = DataFrame.FromRepr(df.ToString());
        df2.Show();
    }    
}