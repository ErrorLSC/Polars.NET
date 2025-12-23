using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public class LazyFrameTests
{
    [Fact]
    public void Test_ScanCsv_Filter_Select()
    {
        // 1. 准备一个临时 CSV 文件
        var csvContent = @"name,age,salary
Alice,25,50000
Bob,30,60000
Charlie,35,70000
David,40,80000";
        using var csv = new DisposableFile(csvContent, ".csv");
        // 2. Scan CSV
        using var lf = LazyFrame.ScanCsv(csv.Path);
        using var lf_copyed = lf.Clone();
        using var df = lf.Collect();
        // 验证加载正确
        Assert.Equal(4, df.Height);
        Assert.Equal(3, df.Width);
        Assert.Contains("name", df.Columns);

        // 3. 执行操作：筛选 age > 30 并选择 name 和 salary
        // SQL 逻辑: SELECT name, salary FROM df WHERE age > 30
        using var filtered = lf_copyed
            .Filter(Col("age") > Lit(30))
            .Select(Col("name"), Col("salary"));
        using var resultDf = filtered.Collect();
        // 验证结果
        // 应该剩下 Charlie (35) 和 David (40)
        Assert.Equal(2, resultDf.Height);
        Assert.Equal(2, resultDf.Width); // name, salary

        // 4. 验证具体值
        var nameCol = resultDf.Column("name");
        
        Assert.NotNull(nameCol);
        Assert.Equal("Charlie", nameCol.GetValue<string>(0));
        Assert.Equal("David", nameCol.GetValue<string>(1));

    }
    [Fact]
    public void Test_Lazy_Concat_Horizontal_And_Safety()
    {
        // 准备两个 LazyFrame
        using var csv1 = new DisposableFile("id\n1\n2",".csv");
        using var lf1 = LazyFrame.ScanCsv(csv1.Path);

        using var csv2 = new DisposableFile("name\nAlice\nBob",".csv");
        using var lf2 = LazyFrame.ScanCsv(csv2.Path);

        // 1. 执行 Horizontal Concat
        var concatLf = LazyFrame.Concat([lf1, lf2], ConcatType.Horizontal);
        
        // 收集结果
        using var df = concatLf.Collect();
        
        Assert.Equal(2, df.Height);
        Assert.Equal(2, df.Width); // id, name

        // 验证数据
        Assert.Equal(1, df.Column("id").GetValue<long>(0));
        Assert.Equal("Alice", df.Column("name").GetValue<string>(0));

        // 2. [关键] 验证 C# 对象安全性 (F# vs C# 习惯测试)
        
        // 我们复用 lf1 做另一个查询
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
        // 结果结构预期：
        // Row 0 (来自 LF1): A=1,    B=10,   C=null (补位)
        // Row 1 (来自 LF2): A=null, B=20,   C=300
        var concatLf = LazyFrame.Concat([lf1, lf2], ConcatType.Diagonal);
        
        using var df = concatLf.Collect();
        
        Assert.Equal(2, df.Height);
        Assert.Equal(3, df.Width); // A, B, C

        // --- 验证 Row 0 (来自 LF1) ---
        // A 和 B 有值
        Assert.Equal(1, df.GetValue<int?>(0, "A"));
        Assert.Equal(10, df.GetValue<int?>(0, "B"));
        
        // C 应该是 null (因为 LF1 没有 C列)
        // [重点] 使用 Assert.Null + GetValue<int?>
        Assert.Null(df.GetValue<int?>(0, "C"));
        
        // --- 验证 Row 1 (来自 LF2) ---
        // A 应该是 null (因为 LF2 没有 A列)
        Assert.Null(df.GetValue<int?>(1, "A"));
        
        // B 和 C 有值
        Assert.Equal(20, df.GetValue<int?>(1, "B"));
        Assert.Equal(300, df.GetValue<int?>(1, "C"));
    }
        [Fact]
    public void Test_LazyFrame_Join_MultiColumn()
    {
        // 场景：学生在不同年份有不同的成绩
        // Alice 在 2023 和 2024 都有成绩
        // Bob 只有 2023 的成绩
        var scoresContent = @"student,year,score
Alice,2023,85
Alice,2024,90
Bob,2023,70";
        using var scoresCsv = new DisposableFile(scoresContent,".csv");
        using var scoresLf = LazyFrame.ScanCsv(scoresCsv.Path);

        // 场景：班级分配表
        // Alice: 2023是Math班, 2024是Physics班
        // Bob:   2024是History班 (注意：Bob 2023没有班级记录)
        var classContent = @"student,year,class
Alice,2023,Math
Alice,2024,Physics
Bob,2024,History";
        using var classCsv = new DisposableFile(classContent,".csv");
        using var classLf = LazyFrame.ScanCsv(classCsv.Path);

        // 执行多列 Join (Inner Join)
        // 逻辑：必须 student 和 year 都相同才算匹配
        // 预期结果：
        // 1. Alice + 2023 -> 匹配
        // 2. Alice + 2024 -> 匹配
        // 3. Bob + 2023   -> 左表有Bob 2023，但右表只有 Bob 2024 -> 丢弃 (因为是 Inner Join)
        using var joinedLf = scoresLf.Join(
            classLf,
            leftOn: [Col("student"), Col("year")],   // 左表双键
            rightOn: [Col("student"), Col("year")],  // 右表双键
            how: JoinType.Inner
        );
        using var joinedDf = joinedLf.Collect();
        // 验证高度：应该只有 2 行 (Alice 2023, Alice 2024)
        Assert.Equal(2, joinedDf.Height); 
        
        // 验证宽度：student, year, score, class (year 在 Join 后通常会去重或保留一份，具体看 Polars 行为，通常保留左表的)
        // Polars Join 后列名如果冲突会自动处理，或者保留 Key。
        // 这里的列应该是: student, year, score, class
        Assert.Equal(4, joinedDf.Width);

        
        // 排序以确保验证顺序 (按 year 排序)
        // 但这里我们简单通过 Filter 验证或者假定顺序（CSV读取顺序通常保留）
        
        // 验证第一行 (Alice 2023)
        Assert.Equal("Alice", joinedDf.Column("student").GetValue<string>(0));
        Assert.Equal(2023, joinedDf.Column("year").GetValue<long>(0));
        Assert.Equal("Math", joinedDf.Column("class").GetValue<string>(0));

        // 验证第二行 (Alice 2024)
        Assert.Equal("Alice", joinedDf.Column("student").GetValue<string>(1));
        Assert.Equal(2024, joinedDf.Column("year").GetValue<long>(1));
        Assert.Equal("Physics", joinedDf.Column("class").GetValue<string>(1));

        // 验证 Bob 确实被删除了 (因为他在右表没有 2023 的记录)
        // 我们可以简单地检查 DataFrame 里没有 Bob
        using var bobCheck = joinedDf.Filter(Col("student") == Lit("Bob"));
        Assert.Equal(0, bobCheck.Height);
    }
        [Fact]
    public void Test_LazyFrame_GroupBy_Agg()
    {
         // 准备数据: Department, Salary
        var csvContent = @"dept,salary
IT,100
IT,200
HR,150
HR,50";
        using var scoresCsv = new DisposableFile(csvContent,".csv");


        using var lf = LazyFrame.ScanCsv(scoresCsv.Path);

        // GroupBy dept, Agg Sum(salary)
        using var groupedlf = lf
            .GroupBy(Col("dept"))
            .Agg(Col("salary").Sum().Alias("total_salary"))
            .Sort(Col("total_salary"), descending: true); // 排序方便断言
        var grouped = groupedlf.Collect();
        // 预期: 
        // IT: 300
        // HR: 200
        
        Assert.Equal(2, grouped.Height);
    
        Assert.Equal("IT", grouped.Column("dept").GetValue<string>(0));
        Assert.Equal(300, grouped.Column("total_salary").GetValue<long>(0));
        
        Assert.Equal("HR", grouped.Column("dept").GetValue<string>(1));
        Assert.Equal(200, grouped.Column("total_salary").GetValue<long>(1));
    }
    [Fact]
    public void Test_Lazy_Unpivot_With_Explain()
    {
        // 构造宽表数据: 日期, 苹果价格, 香蕉价格
        var content = @"date,apple,banana
2024-01-01,10,20
2024-01-02,12,22";
        
        using var csv = new DisposableFile(content,".csv");
        using var lf = LazyFrame.ScanCsv(csv.Path);

        // 构建 Lazy 查询: Unpivot (Melt)
        var unpivotedLf = lf.Unpivot(
            index: ["date"],
            on: ["apple", "banana"],
            variableName: "fruit",
            valueName: "price"
        );

        // --- Explain 功能测试 ---
        // 获取查询计划字符串
        string plan = unpivotedLf.Explain(optimized: true);
        Console.WriteLine("LazyFrame Explain Plan:");
        Console.WriteLine(plan);

        Assert.Contains("UNPIVOT", plan.ToUpper()); 

        // 执行查询
        using var df = unpivotedLf.Collect();

        // 验证结果
        // 原来 2 行，每行拆成 2 个水果 -> 总共 4 行
        Assert.Equal(4, df.Height);
        Assert.Equal(3, df.Width); // date, fruit, price
        
        // 简单验证第一行 (具体顺序取决于实现，但通常有序)
        // 检查列存在性
        Assert.NotNull(df.Column("fruit"));
        Assert.NotNull(df.Column("price"));
        
        // 验证值类型 (apple/banana 价格是 Int64)
        var price0 = df.Column("price").GetValue<long>(0);
        Assert.True(price0 == 10 || price0 == 20);
    }
    [Fact]
    public void Test_Lazy_JoinAsOf_With_TimeSpan_Tolerance()
    {
        // 场景: 股票交易匹配报价，但增加了 "2分钟" 的有效窗口
        // 如果报价太旧（超过2分钟），则认为数据失效，不予匹配
        
        // 使用 ISO 格式日期，配合 tryParseDates: true，让 Polars 自动识别为 Datetime
        var tradesContent = @"time,sym,qty
2024-01-01 10:00:00,AAPL,10
2024-01-01 10:02:00,AAPL,20
2024-01-01 10:05:00,AAPL,5"; // 这个交易离最近的报价(10:01)有4分钟
        using var tradesCsv = new DisposableFile(tradesContent, ".csv");
        
        var quotesContent = @"time,sym,bid
2024-01-01 09:59:00,AAPL,150
2024-01-01 10:01:00,AAPL,151
2024-01-01 10:06:00,AAPL,152";
        using var quotesCsv = new DisposableFile(quotesContent, ".csv");

        // [关键] tryParseDates: true
        // 确保 'time' 列被解析为 Datetime 类型，而不是 String
        using var lfTrades = LazyFrame.ScanCsv(tradesCsv.Path, tryParseDates: true);
        using var lfQuotes = LazyFrame.ScanCsv(quotesCsv.Path, tryParseDates: true);

        // 构建 JoinAsOf
        var joinedLf = lfTrades.JoinAsOf(
            lfQuotes,
            leftOn: Col("time"),
            rightOn: Col("time"),
            
            // [核心升级] 使用 TimeSpan 强类型容差！
            // 逻辑：只匹配最近 2 分钟内的报价
            // 我们刚才实现的 DurationFormatter 会自动将其转为 "2m"
            tolerance: TimeSpan.FromMinutes(2), 
            
            strategy: "backward",
            leftBy: [Col("sym")],
            rightBy: [Col("sym")]
        );

        using var df = joinedLf.Collect();
        
        // 验证结果
        Assert.Equal(3, df.Height);

        // Row 0: Trade 10:00 -> Quote 09:59 (Diff: 1m)
        // 1m <= 2m -> 匹配成功
        Assert.Equal(150, df.Column("bid").GetValue<long?>(0));

        // Row 1: Trade 10:02 -> Quote 10:01 (Diff: 1m)
        // 1m <= 2m -> 匹配成功
        Assert.Equal(151, df.Column("bid").GetValue<long?>(1));

        // Row 2: Trade 10:05 -> Quote 10:01 (Diff: 4m)
        // backward 找到的最近记录是 10:01
        // 但是 4m > 2m (Tolerance)
        // -> 匹配失败，bid 应该为 null
        Assert.Null(df.Column("bid").GetValue<long?>(2));
    }
    [Fact]
    public void Test_DataFrame_To_Lazy_And_Sql()
    {
        // 1. Eager DataFrame
        var data = new[]
        {
            new { Name = "A", Val = 10 },
            new { Name = "B", Val = 20 }
        };
        using var df = DataFrame.From(data);

        // 2. 转 Lazy (新功能)
        using var lf = df.Lazy();

        // 3. 验证 Lazy 操作
        using var resDf = lf
            .Filter(Col("Val") > Lit(15))
            .Collect();

        Assert.Equal(1, resDf.Height); // Only B

        // 4. 验证原 DF 是否还活着 (关键！如果 Lazy() 没 Clone，这里会崩)
        Assert.Equal(2, df.Height); 

        // 5. 验证 SQL Context (CloneHandle 修复验证)
        using var ctx = new SqlContext();
        ctx.Register("mytable", lf); // 这里调用了 lf.CloneHandle()
        
        using var sqlRes = ctx.Execute("SELECT * FROM mytable WHERE Val < 15").Collect();
        Assert.Equal(1, sqlRes.Height); // Only A
    }
    [Fact]
    public void Test_Lazy_GroupBy_Ownership()
    {
        // 1. 数据
        var data = new[]
        {
            new { Dept = "A", Val = 10 },
            new { Dept = "A", Val = 20 },
            new { Dept = "B", Val = 30 }
        };
        using var df = DataFrame.From(data);
        using var lf = df.Lazy();

        // 2. 第一次聚合
        // GroupBy 内部 Clone 了 Handle，所以 Agg 消耗的是副本
        using var agg1 = lf.GroupBy(Col("Dept"))
                           .Agg(Col("Val").Sum().Alias("SumVal"))
                           .Collect();
        
        Assert.Equal(2, agg1.Height); // A, B

        // 3. 验证原 lf 是否还活着 (如果 GroupBy 没 Clone，这里会崩)
        // 我们做个简单的 Select 操作验证
        using var res2 = lf.Select(Col("Dept")).Collect();
        Assert.Equal(3, res2.Height);
    }
    [Fact]
    public void Test_LazyFrame_Explode()
    {
        using var s = new Series("chars", ["a,b", "c"]);
        using var df = new DataFrame(s);
        
        // 转换为 Lazy 模式
        using var lf = df.Lazy();

        // 链式调用: Split -> Explode -> Collect
        // 这一步调用你的 public LazyFrame Explode(params Expr[] exprs)
        using var res = lf
            .Select(Col("chars").Str.Split(",").Alias("expanded"))
            .Explode(Col("expanded"))
            .Collect();

        // 验证
        Assert.Equal(3, res.Height);
        Assert.Equal("a", res.GetValue<string>(0, "expanded"));
        Assert.Equal("b", res.GetValue<string>(1, "expanded"));
        Assert.Equal("c", res.GetValue<string>(2, "expanded"));
    }
    [Fact]
    public void TestLazySchema_ZeroParse()
    {
        Console.WriteLine("===  LazyFrame Schema Zero-Parse Test ===");

        // 1. 准备数据
        // Schema: { "a": Int32, "b": Float64, "c": String }
        using var s1 = Series.From("a", new[] { 1, 2, 3 });
        using var s2 = Series.From("b", new[] { 1.1, 2.2, 3.3 });
        using var s3 = Series.From("c", new[] { "apple", "banana", "cherry" });
        using var df = new DataFrame(s1, s2, s3);
        
        using var lf = df.Lazy();

        // 2. 获取初始 Schema
        Console.WriteLine("--- 1. Initial Schema ---");
        var schema1 = lf.Schema; // 这里触发 Rust collect_schema

        Assert.Equal(3, schema1.Count);
        
        // 验证类型 (强类型枚举匹配)
        Assert.Equal(DataTypeKind.Int32, schema1["a"].Kind);
        Assert.Equal(DataTypeKind.Float64, schema1["b"].Kind);
        Assert.Equal(DataTypeKind.String, schema1["c"].Kind);

        PrintSchema(schema1);

        // 3. 执行 Lazy 操作并验证 Schema 变更
        // 操作：
        // - "a" 转为 Float64
        // - "c" 进行聚合 (Implode) 变成 List<String>
        Console.WriteLine("\n--- 2. Modified Schema (Type Inference) ---");
        
        using var lf2 = lf.Select(
            Col("a").Cast(DataType.Float64).Alias("a_cast"),
            Col("c").Implode().Alias("c_list") // 变成 List
        );

        var schema2 = lf2.Schema;

        // 验证 "a_cast" 变成了 Float64
        Assert.Equal(DataTypeKind.Float64, schema2["a_cast"].Kind);
        Console.WriteLine($"[Check] a_cast is Float64: {schema2["a_cast"].Kind == DataTypeKind.Float64}");

        // 验证 "c_list" 变成了 List
        var cListType = schema2["c_list"];
        Assert.Equal(DataTypeKind.List, cListType.Kind);
        Console.WriteLine($"[Check] c_list is List: {cListType.Kind == DataTypeKind.List}");

        // 4. [高光时刻] 验证嵌套类型 (Introspection)
        // 我们没有解析字符串 "list[str]"，而是直接问 Rust 内部类型是什么
        // cListType.InnerType 会触发 pl_datatype_get_inner FFI 调用
        var innerType = cListType.InnerType; 
        
        Assert.NotNull(innerType);
        Assert.Equal(DataTypeKind.String, innerType!.Kind);
        Console.WriteLine($"[Check] c_list inner type is String: {innerType.Kind == DataTypeKind.String}");

        PrintSchema(schema2);
        
        Console.WriteLine("=== NO STRING PARSE! ===");
    }

    // 辅助打印方法
    private void PrintSchema(Dictionary<string, DataType> schema)
    {
        foreach (var kvp in schema)
        {
            var dt = kvp.Value;
            string extraInfo = dt.Kind == DataTypeKind.List 
                ? $"<Inner: {dt.InnerType?.Kind}>" 
                : "";
                
            Console.WriteLine($"Column: {kvp.Key.PadRight(10)} | Kind: {dt.Kind.ToString().PadRight(10)} | {dt} {extraInfo}");
        }
    }
}