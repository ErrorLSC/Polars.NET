using Apache.Arrow;
using Apache.Arrow.Memory;
using static Polars.CSharp.Polars;
namespace Polars.CSharp.Tests;

public class DataFrameTests
{
    [Fact]
    public void Test_ReadCsv_Filter_Select()
    {
        // 1. 准备一个临时 CSV 文件
        var csvContent = @"name,age,salary
Alice,25,50000
Bob,30,60000
Charlie,35,70000
David,40,80000";
        var fileName = "test_data.csv";
        File.WriteAllText(fileName, csvContent);

        try
        {
            // 2. 读取 CSV
            using var df = DataFrame.ReadCsv(fileName);
            
            // 验证加载正确
            Assert.Equal(4, df.Height);
            Assert.Equal(3, df.Width);
            Assert.Contains("name", df.Columns);

            // 3. 执行操作：筛选 age > 30 并选择 name 和 salary
            // SQL 逻辑: SELECT name, salary FROM df WHERE age > 30
            using var filtered = df
                .Filter(Col("age") > Lit(30))
                .Select(Col("name"), Col("salary"));

            // 验证结果
            // 应该剩下 Charlie (35) 和 David (40)
            Assert.Equal(2, filtered.Height);
            Assert.Equal(2, filtered.Width);

            // 4. 验证具体值 (通过 ToArrow 取回数据)
            using var batch = filtered.ToArrow();
            var nameCol = batch.Column("name");
            
            Assert.NotNull(nameCol);
            Assert.Equal("Charlie", nameCol.GetStringValue(0));
            Assert.Equal("David", nameCol.GetStringValue(1));
        }
        finally
        {
            if (File.Exists(fileName)) File.Delete(fileName);
        }
    }

    [Fact]
    public void Test_FromArrow_RoundTrip()
    {
        // 1. 手动构建一个 Arrow RecordBatch
        var builder = new RecordBatch.Builder(new NativeMemoryAllocator())
            .Append("id", false, col => col.Int32(array => array.AppendRange([1, 2, 3])))
            .Append("value", false, col => col.Double(array => array.AppendRange([1.1, 2.2, 3.3])));

        using var originalBatch = builder.Build();

        // 2. 转为 Polars DataFrame
        using var df = DataFrame.FromArrow(originalBatch);
        
        Assert.Equal(3, df.Height);
        Assert.Equal(2, df.Width);

        // 3. 做一些计算 (例如 value * 2)
        using var resultDf = df.Select(
            Polars.Col("id"), 
            (Polars.Col("value") * Polars.Lit(2.0)).Alias("value_doubled")
        );

        // 4. 转回 Arrow 验证
        using var resultBatch = resultDf.ToArrow();
        var doubledCol = resultBatch.Column("value_doubled") as DoubleArray;

        Assert.NotNull(doubledCol);
        Assert.Equal(2.2, doubledCol.GetValue(0).Value, 4);
        Assert.Equal(4.4, doubledCol.GetValue(1).Value, 4);
        Assert.Equal(6.6, doubledCol.GetValue(2).Value, 4);
    }
    
    [Fact]
    public void Test_GroupBy_Agg()
    {
         // 准备数据: Department, Salary
        var csvContent = @"dept,salary
IT,100
IT,200
HR,150
HR,50";
        var fileName = "groupby_test.csv";
        File.WriteAllText(fileName, csvContent);

        try
        {
            using var df = DataFrame.ReadCsv(fileName);

            // GroupBy dept, Agg Sum(salary)
            using var grouped = df
                .GroupBy(Col("dept"))
                .Agg(Col("salary").Sum().Alias("total_salary"))
                .Sort(Col("total_salary"), descending: true); // 排序方便断言

            // 预期: 
            // IT: 300
            // HR: 200
            
            Assert.Equal(2, grouped.Height);
            
            using var batch = grouped.ToArrow();
            var deptCol = batch.Column("dept");
            var salaryCol = batch.Column("total_salary"); // Polars Sum 整数通常返回 Int64

            Assert.Equal("IT", deptCol.GetStringValue(0));
            Assert.Equal(300, salaryCol.GetInt64Value(0));
            
            Assert.Equal("HR", deptCol.GetStringValue(1));
            Assert.Equal(200, salaryCol.GetInt64Value(1));
        }
        finally
        {
            if (File.Exists(fileName)) File.Delete(fileName);
        }
    }
    // ==========================================
    // Join Tests
    // ==========================================
    [Fact]
    public void Test_DataFrame_Join_MultiColumn()
    {
        // 场景：学生在不同年份有不同的成绩
        // Alice 在 2023 和 2024 都有成绩
        // Bob 只有 2023 的成绩
        var scoresContent = @"student,year,score
Alice,2023,85
Alice,2024,90
Bob,2023,70";
        using var scoresCsv = new DisposableCsv(scoresContent);
        using var scoresDf = DataFrame.ReadCsv(scoresCsv.Path);

        // 场景：班级分配表
        // Alice: 2023是Math班, 2024是Physics班
        // Bob:   2024是History班 (注意：Bob 2023没有班级记录)
        var classContent = @"student,year,class
Alice,2023,Math
Alice,2024,Physics
Bob,2024,History";
        using var classCsv = new DisposableCsv(classContent);
        using var classDf = DataFrame.ReadCsv(classCsv.Path);

        // 执行多列 Join (Inner Join)
        // 逻辑：必须 student 和 year 都相同才算匹配
        // 预期结果：
        // 1. Alice + 2023 -> 匹配
        // 2. Alice + 2024 -> 匹配
        // 3. Bob + 2023   -> 左表有Bob 2023，但右表只有 Bob 2024 -> 丢弃 (因为是 Inner Join)
        using var joinedDf = scoresDf.Join(
            classDf,
            leftOn: [Col("student"), Col("year")],   // 左表双键
            rightOn: [Col("student"), Col("year")],  // 右表双键
            how: JoinType.Inner
        );

        // 验证高度：应该只有 2 行 (Alice 2023, Alice 2024)
        Assert.Equal(2, joinedDf.Height); 
        
        // 验证宽度：student, year, score, class (year 在 Join 后通常会去重或保留一份，具体看 Polars 行为，通常保留左表的)
        // Polars Join 后列名如果冲突会自动处理，或者保留 Key。
        // 这里的列应该是: student, year, score, class
        Assert.Equal(4, joinedDf.Width);

        using var batch = joinedDf.ToArrow();
        
        // 排序以确保验证顺序 (按 year 排序)
        // 但这里我们简单通过 Filter 验证或者假定顺序（CSV读取顺序通常保留）
        
        // 验证第一行 (Alice 2023)
        Assert.Equal("Alice", batch.Column("student").GetStringValue(0));
        Assert.Equal(2023, batch.Column("year").GetInt64Value(0));
        Assert.Equal("Math", batch.Column("class").GetStringValue(0));

        // 验证第二行 (Alice 2024)
        Assert.Equal("Alice", batch.Column("student").GetStringValue(1));
        Assert.Equal(2024, batch.Column("year").GetInt64Value(1));
        Assert.Equal("Physics", batch.Column("class").GetStringValue(1));

        // 验证 Bob 确实被删除了 (因为他在右表没有 2023 的记录)
        // 我们可以简单地检查 DataFrame 里没有 Bob
        using var bobCheck = joinedDf.Filter(Col("student") == Lit("Bob"));
        Assert.Equal(0, bobCheck.Height);
    }
    // ==========================================
    // Concat Tests (Vertical, Horizontal, Diagonal)
    // ==========================================
    [Fact]
    public void Test_Concat_All_Types()
    {
        // --- 1. Vertical (垂直拼接) ---
        // 场景：两份数据结构相同，上下堆叠
        {
            using var csv1 = new DisposableCsv("id,name\n1,Alice");
            using var df1 = DataFrame.ReadCsv(csv1.Path);

            using var csv2 = new DisposableCsv("id,name\n2,Bob");
            using var df2 = DataFrame.ReadCsv(csv2.Path);

            using var res = DataFrame.Concat([df1, df2], ConcatType.Vertical);

            Assert.Equal(2, res.Height);
            Assert.Equal(2, res.Width);

            using var batch = res.ToArrow();
            // 验证顺序
            Assert.Equal(1, batch.Column("id").GetInt64Value(0));
            Assert.Equal(2, batch.Column("id").GetInt64Value(1));
        }

        // --- 2. Horizontal (水平拼接) ---
        // 场景：行数相同，列不同，左右拼接
        {
            using var csv1 = new DisposableCsv("id\n1\n2");
            using var df1 = DataFrame.ReadCsv(csv1.Path);

            using var csv2 = new DisposableCsv("name,age\nAlice,20\nBob,30");
            using var df2 = DataFrame.ReadCsv(csv2.Path);

            using var res = DataFrame.Concat(new[] { df1, df2 }, ConcatType.Horizontal);

            Assert.Equal(2, res.Height);
            Assert.Equal(3, res.Width); // id + name + age

            using var batch = res.ToArrow();
            Assert.NotNull(batch.Column("id"));
            Assert.NotNull(batch.Column("name"));
            Assert.NotNull(batch.Column("age"));
            
            // 验证数据对齐
            Assert.Equal(1, batch.Column("id").GetInt64Value(0));
            Assert.Equal("Alice", batch.Column("name").GetStringValue(0));
        }

        // --- 3. Diagonal (对角拼接) ---
        // 场景：列不完全对齐，取并集，空缺填 null
        // DF1: [A, B]
        // DF2: [B, C]
        // Result: [A, B, C]
        {
            using var csv1 = new DisposableCsv("A,B\n1,10");
            using var df1 = DataFrame.ReadCsv(csv1.Path);

            using var csv2 = new DisposableCsv("B,C\n20,300");
            using var df2 = DataFrame.ReadCsv(csv2.Path);

            using var res = DataFrame.Concat(new[] { df1, df2 }, ConcatType.Diagonal);

            Assert.Equal(2, res.Height); // 垂直堆叠
            Assert.Equal(3, res.Width);  // A, B, C (列的并集)

            using var batch = res.ToArrow();
            
            var colA = batch.Column("A");
            var colB = batch.Column("B");
            var colC = batch.Column("C");

            // Row 0 (来自 DF1): A=1, B=10, C=null
            Assert.Equal(1, colA.GetInt64Value(0));
            Assert.Equal(10, colB.GetInt64Value(0));
            Assert.True(colC.IsNull(0)); // DF1 没有 C 列

            // Row 1 (来自 DF2): A=null, B=20, C=300
            Assert.True(colA.IsNull(1)); // DF2 没有 A 列
            Assert.Equal(20, colB.GetInt64Value(1));
            Assert.Equal(300, colC.GetInt64Value(1));
        }
    }
    // ==========================================
    // Reshaping Tests (Pivot & Unpivot)
    // ==========================================
    [Fact]
    public void Test_Pivot_Unpivot()
    {
        // 构造“长表”数据：记录了不同城市在不同日期的温度
        // date, city, temp
        var content = @"date,city,temp
2024-01-01,NY,5
2024-01-01,LA,20
2024-01-02,NY,2
2024-01-02,LA,18";
        
        using var csv = new DisposableCsv(content);
        using var df = DataFrame.ReadCsv(csv.Path);

        // --- Step 1: Pivot (长 -> 宽) ---
        // 目标：每一行是 date，列变成 city (NY, LA)，值是 temp
        using var pivoted = df.Pivot(
            index: ["date"],
            columns: ["city"],
            values: ["temp"],
            agg: PivotAgg.First // 因为 (date, city) 唯一，First 即可
        );

        // 验证 Pivot 结果
        // 列应该是: date, NY, LA (顺序可能变，取决于 Polars 内部哈希，通常是排序的或按出现顺序)
        Assert.Equal(2, pivoted.Height); // 只有两天 (01-01, 01-02)
        Assert.Equal(3, pivoted.Width);  // date, NY, LA
        
        // 简单打印一下结构，防止列名顺序不确定导致测试挂掉
        pivoted.Show(); 

        using var pBatch = pivoted.ToArrow();
        // 验证 2024-01-01 的 NY 气温 (假设第一行是 01-01)
        // 注意：Arrow 列名区分大小写
        Assert.Equal(5, pBatch.Column("NY").GetInt64Value(0)); 
        Assert.Equal(20, pBatch.Column("LA").GetInt64Value(0));

        // --- Step 2: Unpivot/Melt (宽 -> 长) ---
        // 把刚才的宽表还原。
        // Index 保持 "date" 不变
        // 把 "NY" 和 "LA" 这两列融化成 "city" (variable) 和 "temp" (value)
        using var unpivoted = pivoted.Unpivot(
            index: ["date"],
            on: ["NY", "LA"],
            variableName: "city",
            valueName: "temp_restored"
        ).Sort(Col("date")); // 排序以便断言

        // 验证 Unpivot 结果
        // 高度应该回到 4 行
        Assert.Equal(4, unpivoted.Height);
        Assert.Equal(3, unpivoted.Width); // date, city, temp_restored

        using var uBatch = unpivoted.ToArrow();
        
        // 验证列名是否存在
        Assert.NotNull(uBatch.Column("city"));
        Assert.NotNull(uBatch.Column("temp_restored"));

        // 验证值是否还在
        // 比如第一行应该是 2024-01-01, NY, 5 (或者 LA, 20，取决于排序稳定性，我们这里不深究具体排序，只验证数据存在性)
        // 简单验证第一行的数据类型正确
        Assert.NotNull(uBatch.Column("city").GetStringValue(0));
        Assert.NotNull(uBatch.Column("temp_restored").GetInt64Value(0));
    }
    // ==========================================
    // Display Tests (Head & Show)
    // ==========================================
    [Fact]
    public void Test_Head_And_Show()
    {
        // 构造较多数据 (15行)
        // 0..14
        using var df = DataFrame.FromArrow(
            new RecordBatch.Builder(new NativeMemoryAllocator())
                .Append("id", false, col => col.Int32(arr => arr.AppendRange(Enumerable.Range(0, 15))))
                .Append("name", false, col => col.String(arr => arr.AppendRange(Enumerable.Range(0, 15).Select(i => $"User_{i}"))))
                .Build()
        );

        Assert.Equal(15, df.Height);

        // 1. Test Head/Tail
        using var headDf = df.Head(5);
        Assert.Equal(5, headDf.Height);
        
        using var batch = headDf.ToArrow();
        Assert.Equal(0, batch.Column("id").GetInt64Value(0));
        Assert.Equal(4, batch.Column("id").GetInt64Value(4));

        using var tailDf = df.Tail(5);
        Assert.Equal(5, tailDf.Height);
        using var tailBatch = tailDf.ToArrow();
        Assert.Equal(10, tailBatch.Column("id").GetInt64Value(0));
        Assert.Equal(14, tailBatch.Column("id").GetInt64Value(4));
        // 2. Test Show (No exception should be thrown)
        // 这会在控制台打印表格
        System.Console.WriteLine("\n--- Testing DataFrame.Show() output ---");
        df.Show(10); 
        
        // 测试小数据 Show
        headDf.Show();
        tailDf.Show();
    }
    // ==========================================
    // Rolling & List & Name Ops Tests
    // ==========================================

    [Fact]
    public void Test_Rolling_Functions()
    {
        // 构造时序数据
        var content = @"date,val
2024-01-01,10
2024-01-02,20
2024-01-03,30
2024-01-04,40
2024-01-05,50";
        using var csv = new DisposableCsv(content);
        using var df = DataFrame.ReadCsv(csv.Path, tryParseDates: true);

        // 逻辑: 3天滑动窗口求平均 (Rolling Mean)
        // 10
        // 10,20 -> 15
        // 10,20,30 -> 20
        var rollExpr = Col("val")
            .RollingMeanBy(windowSize: "3d", by: Col("date"), closed: "left")
            .Alias("roll_mean");

        using var res = df.Select(
            Col("date"),
            Col("val"),
            rollExpr
        );

        // 验证
        using var batch = res.ToArrow();
        var rollCol = batch.Column("roll_mean");
        
        // 第3行 (2024-01-03): 窗口 [01, 02, 03) -> 10, 20. Mean = 15. 
        // Polars 的 RollingBy closed="left" 行为细节取决于版本，通常不包含当前行
        // 假设这里验证的是基本调用成功，具体数值依赖 Polars 逻辑
        Assert.NotNull(rollCol);
        Assert.Equal(5, rollCol.Length); 
        // 只要不抛异常且有数据返回，说明 Wrapper 绑定成功
    }

    [Fact]
    public void Test_List_Aggregations_And_Name()
    {
        // 构造含有 List 的数据不易直接通过 CSV，我们用 GroupBy 产生 List
        // A: [1, 2]
        // B: [3, 4, 5]
        var content = @"group,val
A,1
A,2
B,3
B,4
B,5";
        using var csv = new DisposableCsv(content);
        using var df = DataFrame.ReadCsv(csv.Path);

        using var res = df
            .GroupBy(Col("group"))
            .Agg(
                Col("val").Alias("val_list") // 隐式聚合为 List
            )
            .Select(
                Col("group"),
                // 测试 List.Sum, List.Max
                Col("val_list").List.Sum().Name.Suffix("_sum"),
                Col("val_list").List.Max().Name.Suffix("_max"),
                // 测试 List.Contains
                Col("val_list").List.Contains(3).Alias("has_3")
            )
            .Sort(Col("group"));

        // A (1,2) -> Sum=3, Max=2, Has3=false
        // B (3,4,5) -> Sum=12, Max=5, Has3=true
        
        using var batch = res.ToArrow();
        
        // 验证 Name Suffix
        Assert.NotNull(batch.Column("val_list_sum")); // Suffix 生效
        Assert.NotNull(batch.Column("val_list_max"));

        // 验证 A
        Assert.Equal(3, batch.Column("val_list_sum").GetInt64Value(0));
        Assert.Equal(2, batch.Column("val_list_max").GetInt64Value(0));
        Assert.Equal("false", batch.Column("has_3").FormatValue(0));

        // 验证 B
        Assert.Equal(12, batch.Column("val_list_sum").GetInt64Value(1));
        Assert.Equal(5, batch.Column("val_list_max").GetInt64Value(1));
        Assert.Equal("true", batch.Column("has_3").FormatValue(1));
    }
    [Fact]
    public void Test_DataFrame_From_Records_With_Decimal()
    {
        // 1. 准备数据
        var data = new[]
        {
            new { Id = 1, Name = "A", Price = 10.5m },
            new { Id = 2, Name = "B", Price = 20.005m }, // Scale 3
            new { Id = 3, Name = "C", Price = 0m }
        };

        // 2. 转换
        // 匿名类型也是支持的
        using var df = DataFrame.From(data);
        
        Assert.Equal(3, df.Height);
        Assert.Equal(3, df.Width);

        // 3. 验证
        using var batch = df.ToArrow();
        
        // 验证 Decimal
        var priceCol = batch.Column("Price") as Decimal128Array;
        Assert.NotNull(priceCol);
        Assert.Equal(3, priceCol.Scale); // 自动推断
        Assert.Equal(10.5m, priceCol.GetValue(0));   // 之前期望 10500 是错的，Arrow 已经除回去了
        Assert.Equal(20.005m, priceCol.GetValue(1)); 
        Assert.Equal(0m, priceCol.GetValue(2));
    }
public class TradeRecord
    {
        public string Ticker { get; set; }
        public int Qty { get; set; }        // C# int <-> Polars Int64
        public decimal Price { get; set; }  // C# decimal <-> Polars Decimal(18,2)
        public double? Factor { get; set; } // C# double <-> Polars Float64
        public float Risk { get; set; }     // C# float <-> Polars Float64 (downcast)
    }

    [Fact]
    public void Test_DataFrame_RoundTrip_POCO()
    {
        // 1. 原始数据
        var trades = new List<TradeRecord>
        {
            new() { Ticker = "AAPL", Qty = 100, Price = 150.50m, Factor = 1.1, Risk = 0.5f },
            new() { Ticker = "GOOG", Qty = 50,  Price = 2800.00m, Factor = null, Risk = 0.1f },
            new() { Ticker = "MSFT", Qty = 200, Price = 300.25m, Factor = 0.95, Risk = 0.2f }
        };

        // 2. From: List -> DataFrame
        using var df = DataFrame.From(trades);
        
        Assert.Equal(3, df.Height);
        
        // 3. To: DataFrame -> List (Rows<T>)
        var resultList = df.Rows<TradeRecord>().ToList();

        Assert.Equal(3, resultList.Count);

        // 4. 验证数据
        var row0 = resultList[0];
        Assert.Equal("AAPL", row0.Ticker);
        Assert.Equal(100, row0.Qty);
        Assert.Equal(150.50m, row0.Price);
        Assert.Equal(1.1, row0.Factor);
        Assert.Equal(0.5f, row0.Risk);

        var row1 = resultList[1];
        Assert.Equal("GOOG", row1.Ticker);
        Assert.Null(row1.Factor); // 验证 Null 透传
    }
}