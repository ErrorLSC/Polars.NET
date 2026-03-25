using System.Linq.Expressions;
using Apache.Arrow;
using Apache.Arrow.Memory;
using static Polars.CSharp.Polars;
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
            Col("id"), 
            (Col("value") * 2.0).Alias("value_doubled")
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
            .Having(Col("salary").Sum() > 100) 
            .Agg(Col("salary").Sum().Alias("total_salary"))
            .Sort("total_salary", descending: true); 
        
        Assert.Equal(2, grouped.Height);

        Assert.Equal("IT", grouped.Column("dept").GetValue<string>(0));
        Assert.Equal(300L, grouped.Column("total_salary").GetValue<long>(0));
        
        Assert.Equal("HR", grouped.Column("dept").GetValue<string>(1));
        Assert.Equal(200L, grouped.Column("total_salary").GetValue<long>(1));
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
            .GroupBy("dept")
            .Sum()
            .Sort("dept"); 
            
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
            .GroupBy("groups")
            .Agg(
                Col("bools").Any().Alias("is_any_true"),   
                Col("bools").All().Alias("is_all_true"),   
                
                
                Col("values").First().Alias("v_first"),
                Col("values").Last().Alias("v_last"),
                
                
                Col("values").Reverse().First().Alias("v_rev_first") 
            )
            .Sort("groups");

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
                Col("codes").Item().Alias("code_item")
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
            Col("nums").Reverse().Alias("nums_rev")
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
            leftOn: ["student", "year"],
            rightOn: ["student", "year"],
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
            index: ["date"],
            columns: ["city"],
            values: ["temp"],
            aggregateFunction: PivotAgg.First,
            sortColumns: true 
        );

        Assert.Equal(2, pivoted.Height);
        Assert.Equal(3, pivoted.Width); // date, LA, NY (Sorted)

        var cols = pivoted.ColumnNames;
        Assert.Equal("LA", cols[1]);
        Assert.Equal("NY", cols[2]);

        Assert.Equal(20.0, pivoted.GetValue<double>(0, "LA")); 
        Assert.Equal(5.0, pivoted.GetValue<double>(0, "NY"));  

        // --- Step 2: Custom Expr Pivot ---
        
        using var dfWithF = df.WithColumns((Col("temp") * 1.8 + 32).Alias("temp_f"));
        
        using var pivotedFahrenheit = dfWithF.Pivot(
            index: ["date"],
            columns: ["city"],
            values: ["temp_f"],
            aggregateExpr: Col("").First(), 
            sortColumns: true
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
        
        using var meanRow = summary.Filter(Col("statistic") == Lit("mean"));
        Assert.Equal(3.0, meanRow.GetValue<double>(0, "val"));
        
        using var minRow = summary.Filter(Col("statistic") == Lit("min"));
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
        var rollExpr = Col("val")
            .RollingMeanBy(windowSize: new TimeSpan(3,0,0,0), by: Col("date"), closed: ClosedWindow.Left)
            .Alias("roll_mean");

        using var res = df.Select(
            Col("date"),
            Col("val"),
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
            .GroupBy(Col("group"))
            .Agg(
                Col("val").Alias("val_list") 
            )
            .Select(
                Col("group"),
                Col("val_list").List.Sum().Name.Suffix("_sum"),
                Col("val_list").List.Max().Name.Suffix("_max"),
                Col("val_list").List.Contains(3).Alias("has_3")
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
            Col("nums"),
            Col("nums").Str.Split(",").Alias("list_vals")
        );
        using var dtype = DataType.List(DataType.String);
        // Explode for all Int32 list 
        using var exploded = dfWithList.Explode(Selectors.DType(dtype));

        Assert.Equal(3, exploded.Height);

        Assert.Equal("1", exploded["list_vals"][0]);
        Assert.Equal("2", exploded[1][1]);
        Assert.Equal("3", exploded.GetValue<string>("list_vals",2));

        Assert.Equal("1,2", exploded.GetValue<string>(0, "nums"));
        Assert.Equal("1,2", exploded["nums",1]);
        Assert.Equal("3",   exploded[2,0]);
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
            ["A", "B"],
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
    public void Test_DataFrame_GroupByDynamic()
    {
        var dates = new[]
        {
            new DateTime(2023, 1, 1, 10, 0, 0),
            new DateTime(2023, 1, 1, 10, 10, 0),
            new DateTime(2023, 1, 1, 10, 20, 0),
            new DateTime(2023, 1, 1, 11, 0, 0)
        };
        var values = new[] { 1, 2, 3, 4 };

        using var df = DataFrame.FromColumns(new { ts = dates, val = values });

        using var res = df.GroupByDynamic("ts", TimeSpan.FromHours(1))
            .Agg(Col("val").Sum());

        // 10:00:00 -> [1, 2, 3] -> Sum = 6
        // 11:00:00 -> [4]       -> Sum = 4
        
        Assert.Equal(2, res.Height);
        
        var sums = res["val"].ToArray<int>();
        Assert.Contains(6, sums);
        Assert.Contains(4, sums);
    }
    [Fact]
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
            leftOn: Col("ts"),
            rightOn: Col("ts"),
            tolerance: null,
            strategy: AsofStrategy.Backward
        );

        Assert.Equal(2, res.Height);
        
        var rVals = res["val_r"].ToArray<int>();
        Assert.Equal(20, rVals[0]); // 10:00 matched 10:00
        Assert.Equal(30, rVals[1]); // 10:02 matched 10:01 (closest previous)
    }
    [Fact]
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
        using var sl = df.Slice(2, 3);

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
    public void Test_Unique_Stable()
    {
        var df = DataFrame.From(
        [
            new { A = 1, B = "x" },
            new { A = 2, B = "y" },
            new { A = 1, B = "x" }, // Duplicate
            new { A = 3, B = "z" }
        ]);

        // 1. Default (All cols, Keep First)
        var res1 = df.Unique();
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
        
        var res2 = df2.Unique(["A"], UniqueKeepStrategy.Last);
        Assert.Equal(1, res2.Height);
        Assert.Equal("y", res2["B"][0]); // Should keep the last one ("y")

        // 3. Keep None (Remove all duplicates)
        var res3 = df.Unique(null, UniqueKeepStrategy.None);
        Assert.Equal(2, res3.Height); // A=2 and A=3 are unique. A=1 appears twice so both removed.
    }
    [Fact]
    public void Test_HStack_VStack()
    {
        // --- Test HStack ---
        using var df1 = DataFrame.FromColumns(new { a = new[] { 1, 2, 3 } });
        
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
        
        
        // Row 0 (from df1)
        Assert.Equal(1, vStacked["a"][0]);
        Assert.Equal(10, vStacked["b"][0]);
        
        // Row 3 (from df2, index 0)
        Assert.Equal(4, vStacked["a"][3]);
        Assert.Equal(40, vStacked["b"][3]);
    }
    [Fact]
    [Trait("DataFrame","ToTensor")]
    public void ToTensor_AllColumnsMatch_ConvertsToRowMajorFlatArray()
    {
        var s1 = Series.From("feature1", [1.1f, 2.1f, 3.1f]);
        var s2 = Series.From("feature2", [1.2f, 2.2f, 3.2f]);
        
        using var df = new DataFrame(s1, s2);

        float[] tensorData = df.ToTensor<float>();

        Assert.Equal(6, tensorData.Length);

        float[] expected = 
        [ 
            1.1f, 1.2f, 
            2.1f, 2.2f, 
            3.1f, 3.2f  
        ];

        Assert.True(tensorData.AsSpan().SequenceEqual(expected));
    }

    [Fact]
    [Trait("DataFrame","ToTensorSelected")]
    public void ToTensor_SelectedColumns_OnlyConvertsSpecifiedColumns()
    {
        var s1 = Series.From("id", [1, 2]); 
        var s2 = Series.From("feature1", [0.1f, 0.2f]);
        var s3 = Series.From("feature2", [0.9f, 0.8f]);
        
        using var df = new DataFrame(s1, s2, s3);

        float[] tensorData = df.ToTensor<float>("feature1", "feature2");

        Assert.Equal(4, tensorData.Length);
        float[] expected = [0.1f, 0.9f, 0.2f, 0.8f];
        Assert.True(tensorData.AsSpan().SequenceEqual(expected));
    }
    [Fact]
    [Trait("DataFrame","ToTensorException")]
    public void ToTensor_TypeMismatch_ThrowsInvalidOperationException()
    {
        var s1 = Series.From("age", [25, 30]);
        var s2 = Series.From("salary", [5000.5f, 6000.5f]);
        
        using var df = new DataFrame(s1, s2);

        var exception = Assert.Throws<InvalidOperationException>(() => 
        {
            df.ToTensor<float>();
        });

        Assert.Contains("Type mismatch on column 'age'", exception.Message);
        Assert.Contains("Expected Single, but got Int32", exception.Message);
        Assert.Contains("Col().Cast()", exception.Message);
    }

    [Fact]
    [Trait("DataFrame","ToTensorEmpty")]
    public void ToTensor_EmptyColumnList_ReturnsEmptyArray()
    {
        using var emptyDf = new DataFrame(); 
        float[] emptyTensor = emptyDf.ToTensor<float>();
        
        Assert.Empty(emptyTensor);

        var s1 = Series.From("age_float", [25f, 30f]);
        using var df = new DataFrame(s1);
        
        float[] tensorData = df.ToTensor<float>([]);

        Assert.Equal(2, tensorData.Length);
    }
}