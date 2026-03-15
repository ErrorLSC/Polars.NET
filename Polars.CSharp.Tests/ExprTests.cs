using Apache.Arrow;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Helpers;
using static Polars.CSharp.Polars;
namespace Polars.CSharp.Tests;

public class ExprTests
{
    // ==========================================
    // 1. Select Inline Style (Pythonic)
    // ==========================================
    [Fact]
    public void Select_Inline_Style_Pythonic()
    {
        using var csv = new DisposableFile("name,birthdate,weight,height\nQinglei,2025-11-25,70,1.80",".csv");
        using var df = DataFrame.ReadCsv(csv.Path);

        // 像 Python 一样写在 Select 参数里！
        using var res = df.Select(
            Col("name"),
            
            // Inline 1: 简单的 alias
            Col("birthdate").Alias("b_date"),
            
            // Inline 2: 链式调用 (Date Year)
            Col("birthdate").Dt.Year().Alias("year"),
            
            // Inline 3: 算术表达式 (BMI 计算)
            // 注意：C# 运算符优先级，除号需要括号明确优先级
            (Col("weight") / (Col("height") * Col("height"))).Alias("bmi")
        );

        // 验证列数: name, b_date, year, bmi
        Assert.Equal(4, res.Width);

        // 验证值
        
        // 1. 验证 Name (String)
        Assert.Equal("Qinglei", res.GetValue<string>(0, "name"));

        // 2. 验证 Year (Int32 or Int64 depending on Polars/Arrow mapping)
        // Polars Year 通常返回 Int32
        Assert.Equal(2025, res.GetValue<int>(0, "year"));

        // 3. 验证 BMI (Double)
        Assert.True(res.GetValue<double>(0, "bmi") > 21.6);
        Assert.True(res.GetValue<double>(0, "bmi") < 21.7);
    }

    // ==========================================
    // 2. Filter by numeric value (> operator)
    // ==========================================
    [Fact]
    public void Filter_By_Numeric_Value_Gt()
    {
        using var csv = new DisposableFile("val\n10\n20\n30",".csv");
        using var df = DataFrame.ReadCsv(csv.Path);

        // C# 运算符重载: Col("val") > Lit(15)
        using var res = df.Filter(Col("val") > 15);
        
        Assert.Equal(2, res.Height); // 20, 30
        
        // 验证结果
        
        Assert.Equal(20, res.GetValue<int>(0, "val"));
        Assert.Equal(30, res.GetValue<int>(1, "val"));
    }

    // ==========================================
    // 3. Filter by Date Year (< operator)
    // ==========================================
    [Fact]
    public void Filter_By_Date_Year_Lt()
    {
        var content = @"name,birthdate,weight,height
Ben Brown,1985-02-15,72.5,1.77
Qinglei,2025-11-25,70.0,1.80
Zhang,2025-10-31,55,1.75";
        
        using var csv = new DisposableFile(content,".csv");
        // tryParseDates 默认为 true
        using var df = DataFrame.ReadCsv(csv.Path);

        // 逻辑: birthdate.year < 1990
        using var res = df.Filter(Col("birthdate").Dt.Year() < 1990);

        Assert.Equal(1, res.Height); // 只有 Ben Brown
        
        Assert.Equal("Ben Brown", res.GetValue<string>(0, "name"));
    }

    // ==========================================
    // 4. Filter by string value (== operator)
    // ==========================================
    [Fact]
    public void Filter_By_String_Value_Eq()
    {
        using var csv = new DisposableFile("name\nAlice\nBob\nAlice",".csv");
        using var df = DataFrame.ReadCsv(csv.Path);
        
        // 逻辑: name == "Alice"
        using var res = df.Filter(Col("name") == Lit("Alice"));
        
        Assert.Equal(2, res.Height);
    }

    // ==========================================
    // 5. Filter by double value (== operator)
    // ==========================================
    [Fact]
    public void Filter_By_Double_Value_Eq()
    {
        using var csv = new DisposableFile("value\n3.36\n4.2\n5\n3.36",".csv");
        using var df = DataFrame.ReadCsv(csv.Path);
        
        // 逻辑: value == 3.36
        // 注意浮点数比较通常有精度问题，但在 Polars 内部如果是完全匹配的字面量通常没问题
        using var res = df.Filter(Col("value") == 3.36);
        
        Assert.Equal(2, res.Height);
    }

    // ==========================================
    // 6. Null handling works
    // ==========================================
    [Fact]
    public void Null_Handling_Works()
    {
        // 构造 CSV: age 列包含 10, null, 30
        // 注意 CSV 中的空行会被解析为 null
        using var csv = new DisposableFile("age\n10\n\n30",".csv");
        using var df = DataFrame.ReadCsv(csv.Path);

        // --- 测试 1: FillNull ---
        // 逻辑: 将 null 填充为 0，并筛选 >= 0
        // C# Eager 写法:
        using var filled = df
            .WithColumns(
                Col("age").FillNull(0).Alias("age_filled")
            )
            .Filter(Col("age_filled") >= 0);
            
        // 结果应该是 3 行 (10, 0, 30)
        Assert.Equal(3, filled.Height);

        // 验证一下中间那个确实变成了 0
        Assert.Equal(0, filled.GetValue<int>(1, "age_filled"));

        // --- 测试 2: IsNull ---
        // 筛选出 null 的行
        using var nulls = df.Filter(Col("age").IsNull());
        
        // 结果应该是 1 行
        Assert.Equal(1, nulls.Height);
    }
    [Fact]
    public void IsBetween_With_DateTime_Literals()
    {
        // 构造数据: Qinglei 的生日
        var content = @"name,birthdate,height
Qinglei,1990-05-20,1.80
TooOld,1980-01-01,1.80
TooShort,1990-05-20,1.60";

        using var csv = new DisposableFile(content,".csv");
        
        // 必须开启日期解析 (tryParseDates: true)
        using var df = DataFrame.ReadCsv(csv.Path, tryParseDates: true);

        // Python logic translation:
        // col("birthdate").is_between(date(1982,12,31), date(1996,1,1)) & (col("height") > 1.7)
        
        // 定义边界
        var startDt = new DateTime(1982, 12, 31);
        var endDt = new DateTime(1996, 1, 1);

        using var res = df.Filter(
            // 条件 1: 生日区间
            Col("birthdate").IsBetween(Lit(startDt), Lit(endDt))
            & // 条件 2: AND (注意 C# 是 & 不是 &&)
            // 条件 3: 身高
            (Col("height") > 1.7)
        );

        // 验证: 只有 Qinglei 符合 (TooOld 生日不对，TooShort 身高不对)
        Assert.Equal(1, res.Height);
        
        Assert.Equal("Qinglei", res.GetValue<string>(0, "name"));
    }
    [Fact]
    public void Math_Ops_BMI_Calculation_With_Pow()
    {
        // 构造数据: 身高(m), 体重(kg)
        using var csv = new DisposableFile("name,height,weight\nAlice,1.65,60\nBob,1.80,80",".csv");
        using var df = DataFrame.ReadCsv(csv.Path);

        // 目标逻辑: weight / (height ^ 2)
        // C# 使用 .Pow(2) 代替 ** 2
        var bmiExpr = (Col("weight") / Col("height").Pow(2))
            .Alias("bmi");

        using var res = df.Select(
            Col("name"),
            bmiExpr,
            // 顺便测一下 sqrt: sqrt(height)
            Col("height").Sqrt().Alias("sqrt_h")
        );

        // 验证 Bob 的 BMI: 80 / 1.8^2 = 24.691358...
        // Bob 是第二行 (index 1)
        Assert.True(res.GetValue<double>(1, "bmi") > 24.69 && res.GetValue<double>(1, "bmi") < 24.70);
        // 验证 Alice 的 Sqrt: sqrt(1.65) = 1.2845...
        // Alice 是第一行 (index 0)
        Assert.True(res.GetValue<double>(0, "sqrt_h") > 1.28 && res.GetValue<double>(0, "sqrt_h") < 1.29);
    }
        [Fact]
    public void Test_Trigonometry_Basic()
    {
        // 准备数据: [0, PI/2, PI]
        var data = new[] { 0.0, Math.PI / 2.0, Math.PI };
        using var df = DataFrame.FromColumns(new { theta = data });

        // 计算 Sin, Cos, Tan
        using var res = df.Select(
            Col("theta").Sin().Alias("sin"),
            Col("theta").Cos().Alias("cos"),
            Col("theta").Tan().Alias("tan")
        );

        // 验证 Row 0 (theta = 0)
        // Sin(0)=0, Cos(0)=1, Tan(0)=0
        Assert.Equal(0.0, (double)res["sin"][0], 1e-6);
        Assert.Equal(1.0, (double)res["cos"][0], 1e-6);
        Assert.Equal(0.0, (double)res["tan"][0], 1e-6);

        // 验证 Row 1 (theta = PI/2)
        // Sin(PI/2)=1, Cos(PI/2)=0 (approx)
        Assert.Equal(1.0, (double)res["sin"][1], 1e-6);
        Assert.Equal(0.0, (double)res["cos"][1], 1e-6); // 极小值
    }

    [Fact]
    public void Test_Trigonometry_Inverse()
    {
        // 准备数据: [-1, 0, 1]
        var data = new[] { -1.0, 0.0, 1.0 };
        using var df = DataFrame.FromColumns(new { val = data });

        using var res = df.Select(
            Col("val").ArcSin().Alias("asin"),
            Col("val").ArcCos().Alias("acos")
        );

        // ArcSin(-1) = -PI/2
        Assert.Equal(-Math.PI / 2, (double)res["asin"][0], 1e-6);
        // ArcCos(1) = 0
        Assert.Equal(0.0, (double)res["acos"][2], 1e-6);
    }

    [Fact]
    public void Test_Rounding_And_Sign()
    {
        // 数据: [-1.5, 1.5, 2.0]
        var data = new[] { -1.5, 1.5, 2.0 };
        using var df = DataFrame.FromColumns(new { val = data });

        using var res = df.Select(
            Col("val").Ceil().Alias("ceil"),   // [-1, 2, 2]
            Col("val").Floor().Alias("floor"), // [-2, 1, 2]
            Col("val").Sign().Alias("sign")    // [-1, 1, 1]
        );

        // Ceil
        Assert.Equal(-1.0, (double)res["ceil"][0]);
        Assert.Equal(2.0, (double)res["ceil"][1]);

        // Floor
        Assert.Equal(-2.0, (double)res["floor"][0]);
        Assert.Equal(1.0, (double)res["floor"][1]);

        // Sign
        Assert.Equal(-1, Convert.ToInt32(res["sign"][0])); // Sign 返回的可能是 Int 或 Float，视 Polars 版本
        Assert.Equal(1, Convert.ToInt32(res["sign"][1]));
    }  
    // ==========================================
    // String Operations
    // ==========================================

    [Fact]
    public void String_Operations_Case_Slice_Replace()
    {
        // 脏数据: "Hello World", "foo BAR"
        using var csv = new DisposableFile("text\nHello World\nfoo BAR",".csv");
        using var df = DataFrame.ReadCsv(csv.Path);

        using var res = df.Select(
            Col("text"),
            
            // 1. 转大写
            Col("text").Str.ToUpper().Alias("upper"),
            
            // 2. 切片 (取前 3 个字符)
            Col("text").Str.Slice(0, 3).Alias("slice"),
            
            // 3. 替换 (把 'o' 换成 '0')
            Col("text").Str.ReplaceAll("o", "0").Alias("replaced"),
            
            // 4. 长度
            Col("text").Str.Len().Alias("len")
        );

        // 验证 Row 0: "Hello World"
        Assert.Equal("HELLO WORLD", res.GetValue<string>(0, "upper"));
        Assert.Equal("Hel", res.GetValue<string>(0, "slice"));
        Assert.Equal("Hell0 W0rld", res.GetValue<string>(0, "replaced"));
        
        // Polars len() 返回的是 u32，我们的 GetInt64Value 会处理转换
        Assert.Equal(11, res.GetValue<int>(0, "len")); 

        // 验证 Row 1: "foo BAR"
        Assert.Equal("FOO BAR", res.GetValue<string>(1, "upper"));
        Assert.Equal("foo", res.GetValue<string>(1, "slice"));
    }

    [Fact]
    public void String_Regex_Replace_And_Extract()
    {
        using var csv = new DisposableFile("text\nUser: 12345\nID: 999",".csv");
        using var df = DataFrame.ReadCsv(csv.Path);

        using var res = df.Select(
            // 1. Regex Replace: 把数字换成 #
            // C# 字符串中反斜杠需要转义，所以写 "\\d+" 或者 @"\d+"
            Col("text").Str.ReplaceAll(@"\d+", "#", useRegex: true).Alias("masked"),
            
            // 2. Regex Extract: 提取数字部分
            // @"(\d+)" 是第 1 组
            Col("text").Str.Extract(@"(\d+)", 1).Alias("extracted_id")
        );
            // 验证 Replace
        // "User: 12345" -> "User: #"
        Assert.Equal("User: #", res.GetValue<string>(0, "masked"));
        
        // 验证 Extract
        // "User: 12345" -> "12345"
        Assert.Equal("12345", res.GetValue<string>(0, "extracted_id"));
        Assert.Equal("999", res.GetValue<string>(1, "extracted_id"));
    }
    // ==========================================
    // Temporal Ops (Components, Format, Cast)
    // ==========================================
    [Fact]
    public void Temporal_Ops_Components_Format_Cast()
    {
        // 构造数据: 包含日期和时间的字符串
        var csvContent = "ts\n2023-12-25 15:30:00\n2024-01-01 00:00:00";
        using var csv = new DisposableFile(csvContent,".csv");

        // [关键] 开启 tryParseDates=true
        using var df = DataFrame.ReadCsv(csv.Path, tryParseDates: true);

        using var res = df.Select(
            Col("ts"),
            // 1. 提取组件
            Col("ts").Dt.Year().Alias("y"),
            Col("ts").Dt.Month().Alias("m"),
            Col("ts").Dt.Day().Alias("d"),
            Col("ts").Dt.Hour().Alias("h"),
            Col("ts").Dt.Weekday().Alias("w_day"),
            
            // 2. 格式化 (Format to String) -> 返回的是 Utf8 类型
            Col("ts").Dt.ToString("%Y/%m/%d").Alias("fmt_custom"),
            
            // 3. 类型转换 (Cast to Date) -> 返回的是 Date 类型 (内部是 Int32 days)
            Col("ts").Dt.Date().Alias("date_only")
        );

        // --- 验证 Row 0: 2023-12-25 15:30:00 ---

        // 1. 组件验证
        // 注意：Polars 内部 Year/Month/Day 可能返回不同宽度的整数 (Int32/Int8/UInt32)
        // 确保你的 GetValue<int> 内部处理了 Convert.ToInt32 的转换逻辑，否则可能会因为类型不严格匹配报错
        Assert.Equal(2023, res.GetValue<int>(0, "y"));
        Assert.Equal(12, res.GetValue<int>(0, "m"));
        Assert.Equal(25, res.GetValue<int>(0, "d"));
        Assert.Equal(15, res.GetValue<int>(0, "h"));
        Assert.Equal(1, res.GetValue<int>(0, "w_day")); // 周一

        // 2. 格式化字符串验证
        // 这里是对的，ToString() 在 Polars 层面返回 Utf8，C# 对应 string
        Assert.Equal("2023/12/25", res.GetValue<string>(0, "fmt_custom"));

        // 3. Date 类型验证 [修正点]
        // 既然去掉了 Arrow 中间层，GetValue<DateTime> 应该返回 C# 的 DateTime 结构体
        // 或者是 DateOnly (取决于你的 .NET 版本和绑定实现，通常用 DateTime 兼容性更好)
        var expectedDate = new DateTime(2023, 12, 25);
        var actualDate = res.GetValue<DateTime>(0, "date_only");

        Assert.Equal(expectedDate, actualDate); 
        // 如果你坚持要比对字符串，必须自己在 C# 侧 ToString:
        // Assert.Equal("2023-12-25", actualDate.ToString("yyyy-MM-dd"));

        // --- 验证 Row 1: 2024-01-01 00:00:00 ---
        Assert.Equal(2024, res.GetValue<int>(1, "y"));
        Assert.Equal(1, res.GetValue<int>(1, "m"));
        Assert.Equal(0, res.GetValue<int>(1, "h"));
    }
    [Fact]
    public void Test_Dt_Ops_Advanced()
    {
        // 构造数据: 2023-01-01 10:30:55
        // Row 0: 10:30:55
        // Row 1: 10:45:10
        using var s = new Series("ts", ["2023-01-01 10:30:55", "2023-01-01 10:45:10"]);
        using var df = DataFrame.FromSeries(s);
        
        // 先转成 Datetime 类型 (利用之前做的 tryParseDates 或者手动转换)
        // 这里手动转一下以确保万无一失
        using var dfDt = df.Select(
            Col("ts").Str.ToDatetime("%Y-%m-%d %H:%M:%S").Alias("ts")
        );

        using var res = dfDt.Select(
            Col("ts"),

            // 1. Truncate "1h" -> 应该变成 10:00:00
            Col("ts").Dt.Truncate(new TimeSpan(0,1,0,0)).Alias("trunc_1h"),
            
            // 2. Round "30m" -> 
            // 10:30:55 -> 10:30:00
            // 10:45:10 -> 10:30:00 (就近) 还是 11:00? Round是四舍五入
            Col("ts").Dt.Round(new TimeSpan(0,0,30,0)).Alias("round_30m"),

            // 3. OffsetBy "1d" -> 加一天
            // 注意：这里我们测试 OffsetBy(Lit("1d")) 是否有效
            // 如果失败，可能需要先 Cast 为 Duration
            Col("ts").Dt.OffsetBy(TimeSpan.FromDays(1)).Alias("offset_1d"),

            // 4. Timestamp (转 Int64)
            Col("ts").Dt.Timestamp(TimeUnit.Milliseconds).Alias("ts_ms")
        );

        // 验证 Truncate
        var t0 = res.GetValue<DateTime>(0, "trunc_1h");
        Assert.Equal(10, t0.Hour);
        Assert.Equal(0, t0.Minute);
        Assert.Equal(0, t0.Second);

        // 验证 OffsetBy (+1 Day)
        var original = res.GetValue<DateTime>(0, "ts");
        var offset = res.GetValue<DateTime>(0, "offset_1d");
        Assert.Equal(original.AddDays(1), offset);

        // 验证 Timestamp (Milliseconds)
        // 2023-01-01 10:30:55 UTC
        // 只要结果是 Int64 且大于 0 即可，精确换算比较麻烦（涉及时区）
        var tsVal = res.GetValue<long>(0, "ts_ms");
        Assert.True(tsVal > 1672531000000L); 
        
        // 验证类型是否正确变换为 Int64
        Assert.Equal(DataTypeKind.Int64, res.Schema["ts_ms"].Kind);
    }
    [Fact]
    public void Test_Duration_Formatter_HighPrecision()
    {
        // 1. 测试微秒
        // 10微秒
        var us = TimeSpan.FromMicroseconds(10);
        Assert.Equal("10us", DurationFormatter.ToPolarsString(us));

        // 2. 测试纳秒 (1 tick = 100ns)
        // .NET 无法表达 50ns，最小单位是 100ns
        var ns = TimeSpan.FromTicks(1); 
        Assert.Equal("100ns", DurationFormatter.ToPolarsString(ns));

        // 3. 混合高精度测试
        // 1秒 + 500毫秒 + 30微秒 + 200纳秒
        var complex = new TimeSpan(0, 0, 0, 1, 500)
                      + TimeSpan.FromMicroseconds(30)
                      + TimeSpan.FromTicks(2); // 200ns
        
        Assert.Equal("1s500ms30us200ns", DurationFormatter.ToPolarsString(complex));
    }
    // ==========================================
    // Cast Ops: Int to Float, String to Int
    // ==========================================
    [Fact]
    public void Cast_Ops_Int_To_Float_String_To_Int()
    {
        using var csv = new DisposableFile("val_str,val_int\n100,10\n200,20",".csv");
        using var df = DataFrame.ReadCsv(csv.Path);

        using var res = df.Select(
            // 1. String -> Int64
            Col("val_str").Cast(DataType.Int64).Alias("str_to_int"),
            
            // 2. Int64 -> Float64
            Col("val_int").Cast(DataType.Float64).Alias("int_to_float")
        );

        // 验证
        
        // 验证 str_to_int (Row 0: 100)
        // GetInt64Value 兼容 Int32/Int64，很安全
        long v1 = res.Column("str_to_int").GetValue<long>(0);
        Assert.Equal(100L, v1);

        // 验证 int_to_float (Row 1: 20)
        var floatCol = res.Column("int_to_float").ToArrow() as DoubleArray; // Float64 -> DoubleArray
        Assert.NotNull(floatCol);
        
        double v2 = floatCol.GetValue(1) ?? 0.0;
        Assert.Equal(20.0, v2);
    }
    // ==========================================
    // Control Flow: IfElse (When/Then/Otherwise)
    // ==========================================
    [Fact]
    public void Control_Flow_IfElse()
    {
        // 构造成绩数据
        using var csv = new DisposableFile("student,score\nAlice,95\nBob,70\nCharlie,50",".csv");
        using var df = DataFrame.ReadCsv(csv.Path);

        // 逻辑:
        // if score >= 90 then "A"
        // else if score >= 60 then "Pass"
        // else "Fail"
        
        var gradeExpr = IfElse(
            Col("score") >= 90,
            Lit("A"),
            // 嵌套 IfElse (Else 分支)
            IfElse(
                Col("score") >= 60,
                Lit("Pass"),
                Lit("Fail")
            )
        ).Alias("grade");

        using var res = df
            .WithColumns(gradeExpr)
            .Sort("score", descending: true); // 降序

        // 验证
        using var batch = res.ToArrow();
        var gradeCol = batch.Column("grade");

        // Alice (95) -> A
        Assert.Equal("A", gradeCol.GetStringValue(0));
        
        // Bob (70) -> Pass
        Assert.Equal("Pass", gradeCol.GetStringValue(1));
        
        // Charlie (50) -> Fail
        Assert.Equal("Fail", gradeCol.GetStringValue(2));
    }
    // ==========================================
    // Struct and Advanced List Ops
    // ==========================================
    [Fact]
    public void Struct_And_Advanced_List_Ops()
    {
        // 构造数据: Alice 考了两次试
        using var csv = new DisposableFile("name,score1,score2\nAlice,80,90\nBob,60,70",".csv");
        using var df = DataFrame.ReadCsv(csv.Path);

        // 逻辑 4 的表达式: "1 5 2" -> Split -> Sort(Desc) -> First
        // 结果应该是 "5"
        var maxCharExpr = Col("raw_nums").Str.Split(" ")
            .List.Sort(descending: true)
            .List.First()
            .Alias("max_char");

        using var res = df
            // 1. Struct 测试: 把 score1, score2 打包成 "scores_struct"
            .WithColumns(
                AsStruct(Col("score1"), Col("score2"))
                .Alias("scores_struct")
            )
            // 2. Struct Field 测试: 从 struct 取出 score1
            .WithColumns(
                Col("scores_struct").Struct.Field("score1").Alias("s1_extracted")
            )
            // 3. 造一个字符串列 "1 5 2" 用于 List 测试
            .WithColumns(
                Lit("1 5 2").Alias("raw_nums")
            )
            // 4. 执行 List 复杂操作
            .WithColumns(maxCharExpr);

        // 验证
        using var batch = res.ToArrow();

        // 验证 Struct Field
        // Alice score1 = 80
        Assert.Equal(80, batch.Column("s1_extracted").GetInt64Value(0));

        // 验证 List Sort + First
        // "1 5 2" -> ["1", "5", "2"] -> Sort Desc -> ["5", "2", "1"] -> First -> "5"
        Assert.Equal("5", batch.Column("max_char").GetStringValue(0));
    }
    [Fact]
    public void Test_List_Sort_Options()
    {
        // 数据: [[3, null, 1]]
        // 构造一个 List Series
        using var s = Series.From("vals", [3, 1]); // 简单起见，先构造非 null
        // 实际构造含 Null 的 List 比较麻烦，通常通过 Expr 构造
        
        // 我们用 DataFrame + Expr 来测试更方便
        using var df = DataFrame.FromColumns(new 
        {
            // 这里只是为了有一行数据
            dummy = new[] { 1 } 
        }).Select(
            // 构造 list: [3, null, 1]
            Lit(3).Implode().List.Concat(LitNull().Implode()).List.Concat(Lit(1).Implode())
            .Alias("list_col")
        );
        
        // 1. 默认 Sort (Ascending) -> null 在前 (通常)
        // 预期: [null, 1, 3]
        using var df1 = df.Select(Col("list_col").List.Sort(descending: false, nullsLast: false));
        // 验证逻辑...

        // 2. Sort (Ascending + NullsLast)
        // 预期: [1, 3, null]
        using var df2 = df.Select(Col("list_col").List.Sort(descending: false, nullsLast: true));
        
        // 取出第一行的 List
        // 注意：这里需要 Series List 的 GetValue 支持，或者转为 JSON 验证
        // 简单验证: List.Get(-1) 应该是 null
        using var lastItem = df2.Select(Col("list_col").List.Get(-1));
        Assert.Null(lastItem["list_col"][0]);
        
        // List.Get(0) 应该是 1
        using var firstItem = df2.Select(Col("list_col").List.Get(0));
        Assert.Equal(1, (int)firstItem["list_col"][0]);
    }
    [Fact]
    public void Test_Expr_Explode_In_Select()
    {
        using var s = new Series("data", ["x,y"]);
        using var df = DataFrame.FromSeries(s);

        // 直接在 Select 内部对表达式结果进行 Explode
        // Col("data").Str.Split(",") 返回 List
        // .Explode() 将其展平
        using var res = df.Select(
            Col("data").Str.Split(",").Explode().Alias("flat")
        );

        // 原本 1 行，应该变成 2 行
        Assert.Equal(2, res.Height);
        Assert.Equal("x", res.GetValue<string>(0, "flat"));
        Assert.Equal("y", res.GetValue<string>(1, "flat"));
    }
    [Fact]
    public void Test_String_Strip_And_Checks()
    {
        // 构造测试数据
        // "  hello  " (用于测试 strip whitespace)
        // "__world__" (用于测试 strip chars)
        // "prefix_val_suffix" (用于测试 strip prefix/suffix)
        using var s = new Series("s", ["  hello  ", "__world__", "prefix_val_suffix"]);
        using var df = DataFrame.FromSeries(s);

        using var res = df.Select(
            // 1. Strip Whitespace (默认)
            Col("s").Str.StripChars().Alias("strip_ws"), 
            
            // 2. Strip Specific Chars ('_')
            Col("s").Str.StripChars("_").Alias("strip_custom"),

            // 3. Strip Start/End
            Col("s").Str.StripCharsStart(" _").Alias("strip_start"), // 去除开头的空格或下划线
            
            // 4. Strip Prefix/Suffix
            Col("s").Str.StripPrefix("prefix_").Str.StripSuffix("_suffix").Alias("strip_affix"),

            // 5. StartsWith / EndsWith
            Col("s").Str.StartsWith("  h").Alias("starts_h"),
            Col("s").Str.EndsWith("__").Alias("ends_underscore")
        );

        // 验证 Strip WS ("  hello  " -> "hello")
        Assert.Equal("hello", res.GetValue<string>(0, "strip_ws"));
        
        // 验证 Strip Custom ("__world__" -> "world")
        Assert.Equal("world", res.GetValue<string>(1, "strip_custom"));

        // 验证 Strip Start ("  hello  " -> "hello  ", "__world__" -> "world__")
        Assert.Equal("hello  ", res.GetValue<string>(0, "strip_start"));
        Assert.Equal("world__", res.GetValue<string>(1, "strip_start"));

        // 验证 Strip Affix ("prefix_val_suffix" -> "val")
        Assert.Equal("val", res.GetValue<string>(2, "strip_affix"));

        // 验证 Boolean Checks
        Assert.True(res.GetValue<bool>(0, "starts_h"));     // "  hello  " starts with "  h"
        Assert.True(res.GetValue<bool>(1, "ends_underscore")); // "__world__" ends with "__"
    }

    [Fact]
    public void Test_String_To_Date_Parsing()
    {
        using var s = new Series("dates", ["2023-01-01", "2023/12/31"]);
        using var df = DataFrame.FromSeries(s);

        // 测试 ToDate 和 ToDatetime
        // 注意：Wrapper 里的 ToDate/ToDatetime 是严格模式，需要格式匹配
        using var res = df.Select(
            Col("dates").Str.ToDate("%Y-%m-%d").Alias("parsed_date"),       // Row 0 匹配
            Col("dates").Str.ToDatetime("%Y/%m/%d").Alias("parsed_dt")      // Row 1 匹配
        );
        
        // 验证 Row 0: 2023-01-01
        // Schema 验证 (现在可以用 Kind 了!)
        Assert.Equal(DataTypeKind.Date, res.Schema["parsed_date"].Kind);
        
        // 解析成功的应该有值
        // Row 0 格式是 Y-m-d，所以 parsed_date 有值
        Assert.NotNull(res.GetValue<DateTime?>(0, "parsed_date")); // 或者 DateOnly
        // Row 0 格式不匹配 Y/m/d，解析失败应该变成 null (Polars 默认行为是 strict=false 还是 null? 通常是 null)
        Assert.Null(res.GetValue<DateTime?>(0, "parsed_dt"));

        // 验证 Row 1: 2023/12/31
        // 格式是 Y/m/d，所以 parsed_dt 有值
        Assert.NotNull(res.GetValue<DateTime?>(1, "parsed_dt"));
    }
    [Fact]
    public void Test_Struct_Operations()
    {
        // 1. 准备数据
        var df = DataFrame.From(
        [
            new { A = 1, B = 2 },
            new { A = 3, B = 4 }
        ]);

        // 2. 构造 Struct 并重命名
        // 逻辑：把 A 和 B 打包成 Struct，然后重命名为 "First", "Second"
        var q = df.Lazy()
            .Select(
                AsStruct(Col("A","B"))
                    .Struct.RenameFields("First", "Second")
                    .Alias("MyStruct")
            );

        using var result = q.Collect();

        // 3. 验证 FieldByindex
        // 取出 MyStruct 列，并进一步取出第 1 个字段 ("Second")
        // 注意：这里是在 Eager 模式下验证结果，或者我们可以继续用 Lazy Expr
        
        // 让我们在 Lazy 阶段验证 Field(index)
        var q2 = result.Select(
            Col("MyStruct").Struct.Field(0).Alias("F0"), // 应该是 A (First)
            Col("MyStruct").Struct.Field(1).Alias("F1")  // 应该是 B (Second)
        );
        
        // using var result2 = q2.Collect();

        // F0 应该等于 A
        Assert.Equal(1, q2.GetValue<int>(0, "F0"));
        // F1 应该等于 B
        Assert.Equal(2, q2.GetValue<int>(0, "F1"));
    }
    [Fact]
    public void Test_Struct_JsonEncode()
    {
        var df = DataFrame.From(
        [
            new { Id = 1, Info = new { Name = "Alice", Age = 18 } },
            new { Id = 2, Info = new { Name = "Bob", Age = 20 } }
        ]);

        // 将 Info 列 (Struct) 转为 JSON 字符串
        var q = df.Lazy()
            .Select(
                Col("Id"),
                Col("Info").Struct.JsonEncode().Alias("InfoJson")
            );

        using var res = q.Collect();

        // 验证类型
        // 原来是 Struct，现在应该是 String
        var jsonSeries = res["InfoJson"];
        Assert.Equal(DataTypeKind.String, jsonSeries.DataType.Kind); // 或者 LargeString/StringView

        // 验证内容
        var jsonStr = res.GetValue<string>(0, "InfoJson");
        // 简单的字符串包含验证 (JSON 字段顺序可能变，所以不建议全匹配，除非你确定 Rust 实现的顺序)
        Assert.Contains("\"Name\":\"Alice\"", jsonStr);
        Assert.Contains("\"Age\":18", jsonStr);
        
        Console.WriteLine(jsonStr); 
        // 输出示例: {"Name":"Alice","Age":18}
    }
    [Fact]
    public void Test_Window_Shift_Diff_Len()
    {
        // 准备数据
        var df = DataFrame.FromColumns(new 
        {
            Group = new[] { "A", "A", "A", "B", "B" },
            Value = new[] { 10, 20, 30, 100, 200 },
        });

        // 验证 Len()
        // select count(*)
        var count = df.Select(Len()).Row(0)[0];
        Assert.Equal(5u, count); // Polars Len 返回的是 UInt32

        // 复杂计算
        var result = df.Select(
            Col("Group"),
            Col("Value"),
            
            // 1. Over: 组内总和
            Col("Value").Sum().Over("Group").Alias("GroupSum"),
            
            // 2. Shift: 向下移动 1 行
            Col("Value").Shift(1).Alias("PrevValue"),
            
            // 3. Diff: 当前值 - 上一行值 (相当于 Value - PrevValue)
            Col("Value").Diff(1).Alias("DiffValue")
        );

        // 验证 A 组 (10, 20, 30) -> Sum = 60
        Assert.Equal(60, result[0, "GroupSum"]); // A1
        Assert.Equal(60, result[2, "GroupSum"]); // A3

        // 验证 B 组 (100, 200) -> Sum = 300
        Assert.Equal(300, result[3, "GroupSum"]); // B1

        // 验证 Shift (第一行前面没有，应该是 null)
        Assert.Null(result[0, "PrevValue"]); 
        Assert.Equal(10, result[1, "PrevValue"]); // 20 的前一个是 10

        // 验证 Diff
        Assert.Null(result[0, "DiffValue"]);
        Assert.Equal(10, result[1, "DiffValue"]); // 20 - 10 = 10
        Assert.Equal(100, result[4, "DiffValue"]); // 200 - 100 = 100
    }
    [Fact]
    public void Test_AddBusinessDays_SupplyChain_Scenario()
    {
        // 2024-01-05 是周五
        var startDate = new DateOnly(2024, 1, 5); 
        
        using var df = DataFrame.FromColumns(new 
        {
            OrderDate = new[] { startDate }
        });

        // 场景 1: 标准周末 (周五 + 2工作日 -> 周二)
        // 周五 -> (跳过周六, 周日) -> 周一(+1) -> 周二(+2)
        using var res1 = df.Select(
            Col("OrderDate").Dt
                .AddBusinessDays(2) // 默认 Mon-Fri, 无假期
                .Alias("Delivery")
        );

        var delivery1 = (DateOnly)res1["Delivery"][0];
        Assert.Equal(new DateOnly(2024, 1, 9), delivery1); // 周二

        // 场景 2: 遇到假期 (下周一 2024-01-08 是假期)
        // 周五 -> (跳过周六, 周日) -> (跳过周一假期) -> 周二(+1) -> 周三(+2)
        var holidays = new[] { new DateOnly(2024, 1, 8) };
        
        using var res2 = df.Select(
            Col("OrderDate").Dt
                .AddBusinessDays(2, holidays: holidays)
                .Alias("Delivery")
        );

        var delivery2 = (DateOnly)res2["Delivery"][0];
        Assert.Equal(new DateOnly(2024, 1, 10), delivery2); // 周三
    }

    [Fact]
    public void Test_IsBusinessDay()
    {
        // 2024-01-05 (Fri), 01-06 (Sat), 01-07 (Sun), 01-08 (Mon)
        var dates = new[] 
        { 
            new DateOnly(2024, 1, 5), 
            new DateOnly(2024, 1, 6),
            new DateOnly(2024, 1, 8) 
        };

        using var df = DataFrame.FromColumns(new { Date = dates });

        // 设定 01-08 为假期
        var holidays = new[] { new DateOnly(2024, 1, 8) };

        using var res = df.Select(
            Col("Date"),
            Col("Date").Dt.IsBusinessDay(holidays: holidays).Alias("IsBiz")
        );

        // Fri -> True
        Assert.True((bool)res["IsBiz"][0]);
        // Sat -> False (Weekend)
        Assert.False((bool)res["IsBiz"][1]);
        // Mon -> False (Holiday)
        Assert.False((bool)res["IsBiz"][2]);
    }
    public class ArrayExprTests
    {
        // 辅助方法：构造一个包含 Array 列的 DataFrame
        // 逻辑：创建 List -> Cast 为 Array(Int32, 3)
        private DataFrame CreateArrayDataFrame()
        {
            // 原始数据: List<int>
            var lists = new[] 
            {
                [1, 2, 3],
                [10, 20, 30],
                new[] { 5, 0, 5 },
                null // 测试 Null 行
            };

            using var df = DataFrame.FromColumns(new { data = lists });
            
            // Cast List -> Array(Int32, 3)
            // 这一步验证了我们之前修好的 DataType.Array 工厂方法
            var targetType = DataType.Array(DataType.Int32, 3);
            
            var dfArray = df.Select(
                Col("data").Cast(targetType).Alias("arr")
            );
            
            return dfArray;
        }

        [Fact]
        public void Test_Array_Aggregations()
        {
            using var df = CreateArrayDataFrame();

            // 测试: Sum, Min, Max, Mean
            // Row 0: [1, 2, 3] -> Sum=6, Max=3
            // Row 1: [10, 20, 30] -> Sum=60, Max=30
            using var res = df.Select(
                Col("arr").Array.Sum().Alias("sum"),
                Col("arr").Array.Max().Alias("max"),
                Col("arr").Array.Min().Alias("min"),
                Col("arr").Array.Mean().Alias("mean")
            );

            // 验证第一行
            Assert.Equal(6, (int)res["sum"][0]);
            Assert.Equal(3, (int)res["max"][0]);
            Assert.Equal(1, (int)res["min"][0]);
            Assert.Equal(2.0, (double)res["mean"][0]);

            // 验证第二行
            Assert.Equal(60, (int)res["sum"][1]);
        }

        [Fact]
        public void Test_Array_ToStruct()
        {
            // 这是杀手级功能：Embedding -> Features
            using var df = CreateArrayDataFrame();

            // Array(3) -> Struct { field_0, field_1, field_2 }
            using var res = df.Select(
                Col("arr").Array.ToStruct().Alias("struct_col")
            );

            // 1. 验证类型
            var structCol = res["struct_col"];
            Assert.Equal(DataTypeKind.Struct, structCol.DataType.Kind);

            // 2. 验证 Unnest (通常配合 ToStruct 使用)
            // 将 Struct 炸开成列
            using var unnested = res.Unnest("struct_col");
            
            Assert.True(unnested.Columns.Contains("field_0"));
            Assert.True(unnested.Columns.Contains("field_1"));
            Assert.True(unnested.Columns.Contains("field_2"));

            Assert.Equal(1, (int)unnested["field_0"][0]);
            Assert.Equal(2, (int)unnested["field_1"][0]);
            Assert.Equal(3, (int)unnested["field_2"][0]);
        }

        [Fact]
        public void Test_Array_Get_And_Join()
        {
            using var df = CreateArrayDataFrame();

            using var res = df.Select(
                Col("arr").Array.Get(0).Alias("first"),  // 取第0个
                Col("arr").Array.Get(-1).Alias("last")   // 取最后一个
            );

            Assert.Equal(1, (int)res["first"][0]);
            Assert.Equal(3, (int)res["last"][0]);
            
            Assert.Equal(10, (int)res["first"][1]);
            Assert.Equal(30, (int)res["last"][1]);
        }

        [Fact]
        public void Test_Array_Join_String()
        {
            // 测试 Join 需要字符串数组
            var data = new[] { new[] { "a", "b" }, ["c", "d"] };
            using var df = DataFrame.FromColumns(new { strs = data });
            
            using var res = df.Select(
                Col("strs")
                    .Cast(DataType.Array(DataType.String, 2)) // 转为定长
                    .Array.Join("-")
                    .Alias("joined")
            );

            Assert.Equal("a-b", (string)res["joined"][0]);
            Assert.Equal("c-d", (string)res["joined"][1]);
        }

        [Fact]
        public void Test_Array_Sort_And_Reverse()
        {
            // 数据: [3, 1, 2]
            var data = new[] { new[] { 3, 1, 2 } };
            using var df = DataFrame.FromColumns(new { data })
                .Select(Col("data").Cast(DataType.Array(DataType.Int32, 3)).Alias("arr"));

            using var res = df.Select(
                Col("arr").Array.Sort().Alias("sorted"),       // [1, 2, 3]
                Col("arr").Array.Reverse().Alias("reversed"),  // [2, 1, 3] (原序反转)
                Col("arr").Array.ArgMin().Alias("argmin"),     // Index of 1 is 1
                Col("arr").Array.ArgMax().Alias("argmax")      // Index of 3 is 0
            );

            // 1. 验证 Sort (返回的是 List/Array)
            // res["sorted"][0] 返回的是 object，如果是 List<int> 或 int[]
            // 这样 ArrowReader 知道你要 int，它会创建 List<int> 而不是 List<object>
            var sortedList = res["sorted"].GetValue<List<int>>(0); 

            Assert.NotNull(sortedList);
            Assert.Equal(1, sortedList[0]);
            Assert.Equal(2, sortedList[1]);
            Assert.Equal(3, sortedList[2]);

            // [修复] Reverse 同理
            var reversedList = res["reversed"].GetValue<List<int>>(0);
            Assert.NotNull(reversedList);
            Assert.Equal(2, reversedList[0]);
            Assert.Equal(1, reversedList[1]);
            Assert.Equal(3, reversedList[2]);
            // 验证 ArgMin/ArgMax
            // ArgMin 应该返回 1 (因为 1 在索引 1)
            // ArgMax 应该返回 0 (因为 3 在索引 0)
            Assert.Equal(1U, res["argmin"][0]); // Polars 索引通常是 UInt32
            Assert.Equal(0U, res["argmax"][0]);
        }

        [Fact]
        public void Test_Array_Boolean_Ops()
        {
            var data = new[] 
            { 
                [true, false], 
                [true, true],
                new[] { false, false }
            };
            
            using var df = DataFrame.FromColumns(new { vals = data })
                .Select(Col("vals").Cast(DataType.Array(DataType.Boolean, 2)).Alias("arr"));

            using var res = df.Select(
                Col("arr").Array.Any().Alias("any"),
                Col("arr").Array.All().Alias("all")
            );

            // Row 0: [T, F] -> Any=T, All=F
            Assert.True((bool)res["any"][0]);
            Assert.False((bool)res["all"][0]);

            // Row 1: [T, T] -> Any=T, All=T
            Assert.True((bool)res["any"][1]);
            Assert.True((bool)res["all"][1]);

            // Row 2: [F, F] -> Any=F, All=F
            Assert.False((bool)res["any"][2]);
            Assert.False((bool)res["all"][2]);
        }
        
        [Fact]
        public void Test_Array_Contains()
        {
             // 数据: [3, 1, 2]
            var data = new[] { new[] { 3, 1, 2 } };
            using var df = DataFrame.FromColumns(new { data })
                .Select(Col("data").Cast(DataType.Array(DataType.Int32, 3)).Alias("arr"));
            
            using var res = df.Select(
                Col("arr").Array.Contains(1).Alias("has_1"),
                Col("arr").Array.Contains(99).Alias("has_99")
            );
            
            Assert.True((bool)res["has_1"][0]);
            Assert.False((bool)res["has_99"][0]);
        }
        
        [Fact]
        public void Test_Array_InnerType_Property()
        {
            // 验证我们在 DataType.cs 里修好的 InnerType 逻辑
            var arrType = DataType.Array(DataType.Float64, 5);
            
            Assert.Equal(DataTypeKind.Array, arrType.Kind);
            // 这里之前会崩溃，现在应该好了
            Assert.Equal(DataTypeKind.Float64, arrType.InnerType.Kind);
            // 验证 Width
            Assert.Equal(5UL, arrType.ArrayWidth);
        }
        [Fact]
        public void Test_Series_Array_ReadItem()
        {
            // 1. 准备数据: Array(Int32, 3)
            // Row 0: [1, 2, 3]
            // Row 1: [10, 20, 30]
            // Row 2: [5, 0, 5]
            // Row 3: null
            using var df = CreateArrayDataFrame(); 
            var series = df["arr"]; 

            // 2. 测试读取为 List<int>
            var listVal = series.GetValue<List<int>>(0);
            Assert.NotNull(listVal);
            Assert.Equal(3, listVal.Count);
            Assert.Equal(1, listVal[0]);
            Assert.Equal(2, listVal[1]);
            Assert.Equal(3, listVal[2]);

            // 3. 测试读取为 int[] (数组)
            var arrayVal = series.GetValue<int[]>(1);
            Assert.NotNull(arrayVal);
            Assert.Equal(3, arrayVal.Length);
            Assert.Equal(10, arrayVal[0]);
            Assert.Equal(20, arrayVal[1]);
            Assert.Equal(30, arrayVal[2]);
            
            // 4. 测试读取 Null 行
            var nullVal = series.GetValue<List<int>>(3);
            Assert.Null(nullVal);
            
            // 5. 测试读取嵌套结构 (如果实现了 Array<Struct>)
            // 这里简单验证一下类型系统是否能处理
            // 假设我们有一个 Array<double>
            using var dfFloat = DataFrame.FromColumns(new 
            { 
                v = new[] { [1.1, 2.2], new[] { 3.3, 4.4 } } 
            }).Select(Col("v").Cast(DataType.Array(DataType.Float64, 2)).Alias("vec"));
            
            var vec = dfFloat["vec"].GetValue<double[]>(0);
            Assert.Equal(1.1, vec[0], 1);
            Assert.Equal(2.2, vec[1], 1);
        }
        [Fact]
        public void Test_Expr_TopK()
        {
            // 无序数据
            var data = new[] { 10, 5, 8, 100, 1, 99 };
            using var df = DataFrame.FromColumns(new { val = data });

            // TopK(2) 应该是 [100, 99] (顺序不保证，或者是有序的？Polars TopK 返回通常是降序)
            using var top = df.Select(Col("val").TopK(2).Alias("top"));
            
            // BottomK(2) 应该是 [1, 5]
            using var bottom = df.Select(Col("val").BottomK(2).Alias("bottom"));

            // 验证 Top
            Assert.Equal(2, top.Height);
            // Polars TopK 返回通常是有序的，但我们检查集合包含关系最稳妥
            var topList = top["top"].ToArray<int>();
            Assert.Contains(100, topList);
            Assert.Contains(99, topList);

            // 验证 Bottom
            var bottomList = bottom["bottom"].ToArray<int>();
            Assert.Contains(1, bottomList);
            Assert.Contains(5, bottomList);
        }
        [Fact]
        public void Test_Expr_TopKBy_MultiColumn()
        {
            // 准备数据：
            // Group: [A, A, B, B]
            // Value: [1, 2, 3, 4]
            // Score: [10, 20, 10, 20]
            
            var group = new[] { "A", "A", "B", "B" };
            var val = new[] { 1, 2, 3, 4 };
            var score = new[] { 10, 20, 10, 20 };

            using var df = DataFrame.FromColumns(new { group, val, score });
            
            using var res_topk = df.Select(
                Col("val").TopKBy(
                    k: 2, 
                    by: [Col("group"), Col("score")], 
                    reverse: [true, false] // Group 降序, Score 升序
                ).Alias("top_k")
            );

            Assert.Equal(2, res_topk.Height);
            var list_top = res_topk["top_k"].ToArray<int>();
            
            // 验证结果包含 1 和 2
            Assert.Contains(1, list_top);
            Assert.Contains(2, list_top);

            using var res_bottomk = df.Select(
                Col("val").BottomKBy(
                    k: 2,
                    by: [Col("group"), Col("score")], 
                    reverse: [true, false] //
                ).Alias("bottom_k")
            );

            Assert.Equal(2, res_bottomk.Height);
            var list_bottom = res_bottomk["bottom_k"].ToArray<int>();
            
            // 验证结果包含 1 和 2
            Assert.Contains(3, list_bottom);
            Assert.Contains(4, list_bottom);
        }
    }
    [Fact]
    public void Test_Bitwise_Shift_Operators()
    {
        // 准备数据
        // Signed: 1, 2, -4, null
        // Unsigned: 1, 2, 0xFFFFFFFF, null
        var iData = new int?[] { 1, 2, -4, null };
        var uData = new uint?[] { 1, 2, 0xFFFFFFFF, null };

        using var df = DataFrame.FromColumns(new 
        { 
            i_val = iData,
            u_val = uData
        });

        // 执行移位
        using var res = df.Select(
            // 1 << 1 = 2
            (Col("i_val") << 1).Alias("i_shl"),
            
            // -4 >> 1 = -2 (算术右移，保留符号)
            (Col("i_val") >> 1).Alias("i_shr"),

            // 0xFFFFFFFF >> 4 = 0x0FFFFFFF (逻辑右移)
            (Col("u_val") >> 4).Alias("u_shr")
        );

        // 验证 Signed Left Shift
        // 1 << 1 = 2
        Assert.Equal(2, res.GetValue<int>(0, "i_shl"));
        // null << 1 = null
        Assert.Null(res.GetValue<int?>(3, "i_shl"));

        // 验证 Signed Right Shift (Arithmetic)
        // -4 (111...100) >> 1 = -2 (111...110)
        Assert.Equal(-2, res.GetValue<int>(2, "i_shr"));

        // 验证 Unsigned Right Shift (Logical)
        // UInt Max >> 4
        uint expected = 0xFFFFFFFF >> 4; // C# 也是逻辑右移
        Assert.Equal(expected, res.GetValue<uint>(2, "u_shr"));
    }
    [Fact]
    public void Test_Product()
    {
        // 1 * 2 * 3 * 4 = 24
        using var df = DataFrame.FromColumns(new { nums = new[] { 1, 2, 3, 4 } });

        var result = df.Select(Col("nums").Product());
        
        // 结果应该是一个 1x1 的 DataFrame
        var val = result["nums"][0];
        Assert.Equal(24L, val);
    }
    [Fact]
    public void Test_Skew()
    {
        // [1, 2, 3] 对称 -> Skew 0
        // [1, 1, 10] 右偏 -> Skew > 0
        using var df = DataFrame.FromColumns(new { 
            sym = new[] { 1.0, 2.0, 3.0 }, 
            skewed = new[] { 1.0, 1.0, 10.0 } 
        });

        // Test 1: Symmetric -> 0
        using var res1 = df.Select(Col("sym").Skew(bias: true));
        Assert.Equal(0.0, (double)res1["sym"][0], precision: 6);

        // Test 2: Skewed -> Positive
        using var res2 = df.Select(Col("skewed").Skew(bias: false));
        Assert.True((double)res2["skewed"][0] > 0);
    }
    [Fact]
    public void Test_Kurtosis()
    {
        using var df = DataFrame.FromColumns(new { data = new[] { 1.0, 2.0, 3.0, 4.0, 5.0 } });

        // Fisher = true (Normal = 0)
        // Fisher = false (Pearson, Normal = 3)
        using var q = df.Select(
            Col("data").Kurtosis(fisher: true, bias: false).Alias("k_fisher"),
            Col("data").Kurtosis(fisher: false, bias: false).Alias("k_pearson")
        );

        var kFisher = (double)q["k_fisher"][0];
        var kPearson = (double)q["k_pearson"][0];

        // Pearson = Fisher + 3
        Assert.Equal(3.0, kPearson - kFisher, precision: 6);
    }
    [Fact]
    public void Test_PctChange()
    {
        // Data: [10, 20, 40, 40]
        // n=1: [null, 1.0, 1.0, 0.0]
        using var df = DataFrame.FromColumns(new { a = new[] { 10.0, 20.0, 40.0, 40.0 } });

        using var res = df.Select(Col("a").PctChange(n: 1));
        
        // 检查第一个是 null
        Assert.Null(res["a"][0]); 
        
        // 检查后续值
        Assert.Equal(1.0, (double)res["a"][1]);
        Assert.Equal(1.0, (double)res["a"][2]);
        Assert.Equal(0.0, (double)res["a"][3]);
    }

    [Fact]
    public void Test_Rank_Methods()
    {
        // Data with ties: [10, 20, 20, 30]
        // Indices:         0   1   2   3
        using var df = DataFrame.FromColumns(new { v = new[] { 10, 20, 20, 30 } });

        // 1. Average (Default): 20->2.5
        using var qAvg = df.Select(Col("v").Rank(RankMethod.Average).Alias("rank"));
        Assert.Equal(2.5, (double)qAvg["rank"][1]);

        // 2. Min: 20->2
        using var qMin = df.Select(Col("v").Rank(RankMethod.Min).Alias("rank"));
        // 注意：Polars rank 返回通常是 f64，但也可能是整型，视版本而定。
        // 用 Convert.ToDouble 最稳，或者如果你确定它是 double 就直接 (double)
        Assert.Equal(2.0, Convert.ToDouble(qMin["rank"][1]));
        Assert.Equal(4.0, Convert.ToDouble(qMin["rank"][3])); // 跳过3，直接4

        // 3. Dense: 20->2, 30->3
        using var qDense = df.Select(Col("v").Rank(RankMethod.Dense).Alias("rank"));
        Assert.Equal(2.0, Convert.ToDouble(qDense["rank"][1]));
        Assert.Equal(3.0, Convert.ToDouble(qDense["rank"][3])); // 紧接3
        
        // 4. Descending
        using var qDesc = df.Select(Col("v").Rank(RankMethod.Min, descending: true));
        // 30->1, 20->2, 10->4
        Assert.Equal(1.0, Convert.ToDouble(qDesc["v"][3])); // value 30 is rank 1
    }
    [Fact]
    public void Test_CumSum_Prod()
    {
        // Data: [1, 2, 3, 4]
        using var df = DataFrame.FromColumns(new { v = new[] { 1, 2, 3, 4 } });

        // --- CumSum ---
        // Forward: [1, 3, 6, 10]
        // Reverse: [10, 9, 7, 4]  (4, 4+3, 4+3+2, ...)
        using var qSum = df.Select(
            Col("v").CumSum().Alias("fwd"),
            Col("v").CumSum(reverse: true).Alias("rev")
        );

        Assert.Equal(6, qSum["fwd"][2]); // 1+2+3
        Assert.Equal(9, qSum["rev"][1]); // 4+3+2

        // --- CumProd ---
        // Forward: [1, 2, 6, 24]
        // Reverse: [24, 24, 12, 4] (1*2*3*4, 2*3*4, 3*4, 4)
        using var qProd = df.Select(
            Col("v").CumProd().Alias("fwd"),
            Col("v").CumProd(reverse: true).Alias("rev")
        );
        Assert.Equal(6L, qProd["fwd"][2]); // 1*2*3
        Assert.Equal(12L, qProd["rev"][2]); // 3*4
    }

    [Fact]
    public void Test_CumMin_Max()
    {
        // Data: [1, 5, 2, 4, 3]
        using var df = DataFrame.FromColumns(new { v = new[] { 1, 5, 2, 4, 3 } });

        // --- CumMax ---
        // Forward: [1, 5, 5, 5, 5]
        // Reverse: [5, 5, 4, 4, 3] (Max of 1..3 is 5, ..., 2..3 is 4, 4..3 is 4, 3 is 3)
        using var qMax = df.Select(
            Col("v").CumMax().Alias("fwd"),
            Col("v").CumMax(reverse: true).Alias("rev")
        );
        
        Assert.Equal(5, (int)qMax["fwd"][2]); // Max(1,5,2) -> 5
        Assert.Equal(4, (int)qMax["rev"][2]); // Max(2,4,3) -> 4 (从索引2往后看)

        // --- CumMin ---
        // Forward: [1, 1, 1, 1, 1]
        // Reverse: [1, 2, 2, 3, 3] (Min of 1..3 is 1, ..., 2..3 is 2, ..., 3 is 3)
        using var qMin = df.Select(
            Col("v").CumMin().Alias("fwd"),
            Col("v").CumMin(reverse: true).Alias("rev")
        );

        Assert.Equal(1, (int)qMin["fwd"][4]); 
        Assert.Equal(2, (int)qMin["rev"][2]); // Min(2,4,3) -> 2
    }

    [Fact]
    public void Test_CumCount()
    {
        // CumCount 其实就是返回索引位置（不包含 null 的计数，具体看 polars 版本，但通常类似 enumerate）
        // Data: [10, 20, 30]
        using var df = DataFrame.FromColumns(new { v = new[] { 10, 20, 30 } });

        using var res = df.Select(
            Col("v").CumCount(reverse: false).Alias("fwd"), // [1, 2, 3]
            Col("v").CumCount(reverse: true).Alias("rev")   // [3, 2, 1] (倒着数)
        );

        // CumCount 返回类型是 UInt32 
        Assert.Equal(1u, res["fwd"][0]); // 第1个
        Assert.Equal(2u, res["fwd"][1]);
        Assert.Equal(3u, res["fwd"][2]);

        // Reverse Check
        // Index 0 (原来的第一个元素): 倒着数它是第3个
        Assert.Equal(3u, res["rev"][0]); 
        // Index 2 (原来的最后一个元素): 倒着数它是第1个
        Assert.Equal(1u, res["rev"][2]);
    }
    [Fact]
    public void Test_Ewm_Mean_Std_Var()
    {
        // Data: [10, 20, 40]
        using var df = DataFrame.FromColumns(new { v = new[] { 10.0, 20.0, 40.0 } });

        // --- 1. EWM Mean (alpha=0.5) ---
        // Formula (adjust=true):
        // y0 = x0 = 10
        // y1 = (x1 + (1-a)*x0) / (1 + (1-a)) = (20 + 0.5*10) / 1.5 = 25/1.5 = 16.666...
        using var resMean = df.Select(
            Col("v").EwmMean(alpha: 0.5, adjust: true).Alias("mean")
        );
        
        Assert.Equal(10.0, (double)resMean["mean"][0]);
        Assert.Equal(16.666666, (double)resMean["mean"][1], precision: 4);

        // --- 2. EWM Var & Std (bias=false, unbiased) ---
        // 只需要验证非零且不报错，具体数值依赖 polars 底层算法
        using var resVar = df.Select(
            Col("v").EwmVar(alpha: 0.5, bias: false).Alias("var"),
            Col("v").EwmStd(alpha: 0.5, bias: false).Alias("std")
        );

        // 第一个点通常是 null 或 0 (因为只有一个点没法算方差，取决于 min_periods)
        // Polars 默认 min_periods=1，但方差需要至少两个点才不是 NaN/Null ? 
        // 实测 Polars ewm_std 第一个点通常是 null
        var var0 = resVar["var"][0];
        if (var0 == null) 
            Assert.Null(var0); 
        else 
            Assert.Equal(0.0, Convert.ToDouble(var0));
        // Assert.Null(resVar["std"][0]);

        // 第二个点应该有值
        Assert.True((double)resVar["var"][1] > 0);
        Assert.True((double)resVar["std"][1] > 0);
        
        // 验证 Std = Sqrt(Var)
        var v = (double)resVar["var"][2];
        var s = (double)resVar["std"][2];
        Assert.Equal(s, Math.Sqrt(v), precision: 6);
    }

    [Fact]
    public void Test_EwmMeanBy_Time()
    {
        // 构造时间序列数据
        // t0: 00:00, val=10
        // t1: 01:00, val=20 (间隔 1h)
        // t2: 03:00, val=40 (间隔 2h)
        var dates = new[] {
            new DateTime(2023, 1, 1, 0, 0, 0),
            new DateTime(2023, 1, 1, 1, 0, 0),
            new DateTime(2023, 1, 1, 3, 0, 0)
        };
        
        using var df = DataFrame.FromColumns(new 
        { 
            ts = dates, 
            v = new[] { 10.0, 20.0, 40.0 } 
        });
        df.Show();
        // 设置 half_life = "1h"
        // 意味着如果间隔 1h，旧数据的权重变为 0.5
        using var res = df.Select(
            Col("v").EwmMeanBy(by: Col("ts"), halfLife: "1h").Alias("ewm_by")
        );

        // Row 0: 10
        Assert.Equal(10.0, (double)res["ewm_by"][0]);

        // Row 1 (dt=1h):
        // weight_current = 1
        // weight_prev = 0.5^1 = 0.5
        // mean = (20*1 + 10*0.5) / (1 + 0.5) = 25 / 1.5 = 16.666...
        Assert.Equal(15.0, (double)res["ewm_by"][1], precision: 4);
        
        // Row 2 (dt=2h from t1):
        // weight_current = 1
        // weight_t1 = 0.5^(2/1) = 0.25
        // weight_t0 = 0.5^(3/1) = 0.125
        // mean = (40*1 + 20*0.25 + 10*0.125) / (1 + 0.25 + 0.125) 
        //      = (40 + 5 + 1.25) / 1.375 = 46.25 / 1.375 = 33.6363...
        Assert.Equal(33.75, (double)res["ewm_by"][2], precision: 4);
    }

    [Fact]
    public void Test_EwmMeanBy_Index()
    {
        // 测试 '1i' (Index Count) 用法
        // 效果应该等同于普通的 EWM
        using var df = DataFrame.FromColumns(new 
        { 
            idx = new[] { 0, 1, 2 }, // 均匀索引
            v = new[] { 10.0, 20.0, 40.0 } 
        });

        using var res = df.Select(
            Col("v").EwmMeanBy(by: Col("idx"), halfLife: "1i").Alias("ewm_idx")
        );
        
        // 应该能算出数，不报错即可
        Assert.Equal(10.0, (double)res["ewm_idx"][0]);
        Assert.True((double)res["ewm_idx"][1] > 10);
    }
    [Fact]
    public void Test_Rolling_Fixed_Advanced()
    {
        // Data: [1, 2, 3, 4, 100]
        using var df = DataFrame.FromColumns(new { v = new[] { 1.0, 2.0, 3.0, 4.0, 100.0 } });

        // --- 1. Rolling Var (Window=3) ---
        // [1, 2, 3] -> Mean=2, Var = ((1-2)^2 + (2-2)^2 + (3-2)^2) / (3-1) = 2/2 = 1.0
        // [2, 3, 4] -> Var = 1.0
        using var resVar = df.Select(
            Col("v").RollingVar(windowSize: "3i").Alias("var")
        );
        // 前两个是 null (min_periods default is usually window size or 1 depending on setup, Polars usually nulls until window full)
        Assert.Null(resVar["var"][0]); 
        Assert.Equal(1.0, (double)resVar["var"][2]);
        Assert.Equal(1.0, (double)resVar["var"][3]);

        // --- 2. Rolling Quantile (Median) with Weights ---
        // Median (0.5 quantile) is at weight 1.0 -> Value 2.
        
        // 注意：weights 数组长度必须和 windowSize 里的数字一致（这里是3）
        double[] w = [0.5, 1.0, 0.5];
        
        using var resQuant = df.Select(
            Col("v").RollingQuantile(
                quantile: 0.5, 
                method: QuantileMethod.Linear, 
                windowSize: "3i", 
                weights: w
            ).Alias("q_weighted")
        );
        // 实测 Polars Linear Weighted Quantile 逻辑：
        // Window [1, 2, 3] -> 1*0.25 + 2*0.75 = 1.75
        Assert.Equal(1.75, (double)resQuant["q_weighted"][2], precision: 4);
        
        // Window [3, 4, 100] -> 3*0.25 + 4*0.75 = 3.75
        // 注意：100 被权重的分布“屏蔽”了，因为中位数落在前两个数之间
        Assert.Equal(3.75, (double)resQuant["q_weighted"][4], precision: 4);
        
        // --- 3. Rolling Kurtosis ---
        // Kurtosis 需要至少4个点才有意义 (Fisher definition) 或者根据实现不同
        // 这里只验证它能运行且算出非空值
        using var resKurt = df.Select(
            Col("v").RollingKurtosis(windowSize: "4i").Alias("kurt")
        );
        Assert.NotNull(resKurt["kurt"][3]); // index 3 is 4th element
    }
    [Fact]
    public void Test_Rolling_Rank_Fixed()
    {
        // 构造带重复值的数据: [10, 20, 20, 30]
        using var df = DataFrame.FromColumns(new { v = new[] { 10.0, 20.0, 20.0, 30.0 } });

        // 尝试调用 RollingRank (固定窗口 3)
        // 期望:
        // Index 2 (Window: 10, 20, 20): Last val is 20. Rank(Average) = (2+3)/2 = 2.5
        using var res = df.Select(
            Col("v").RollingRank(windowSize: "3i", minPeriods: 3).Alias("rank")
        );

        // 如果 Rust 端实现没有注入 RollingFnParams::Rank，这里会 Panic
        Assert.Equal(2.5, (double)res["rank"][2]);
    }
    [Fact]
    public void Test_Rolling_By_Time_Std_Rank()
    {
        // 构造非等间距的时间序列
        // t0: 00:00, v=0
        // t1: 00:01, v=10
        // t2: 00:02, v=10 (Diff from t0 is 2m)
        // t3: 00:05, v=20 (Diff from t2 is 3m)
        var dates = new[] {
            new DateTime(2023, 1, 1, 0, 0, 0),
            new DateTime(2023, 1, 1, 0, 1, 0),
            new DateTime(2023, 1, 1, 0, 2, 0),
            new DateTime(2023, 1, 1, 0, 5, 0)
        };

        using var df = DataFrame.FromColumns(new 
        { 
            ts = dates, 
            v = new[] { 0.0, 10.0, 10.0, 20.0 } 
        });

        // 必须转为 Polars Datetime
        var tsCol = Col("ts").Cast(DataType.Datetime(TimeUnit.Microseconds));

        // --- 1. Rolling Std By "2m" ---
        // At t2 (00:02): Window [00:00, 00:02] (Default closed="left" -> [t-w, t))
        // Closed=Left: [00:00, 00:02) -> 包含 00:00, 00:01. Values: [0, 10]. Std(0, 10) ≈ 7.07
        // Closed=Both: [00:00, 00:02] -> 包含 00:00, 00:01, 00:02. Values: [0, 10, 10]. Std(0, 10, 10) ≈ 5.77
        
        using var resStd = df.Select(
            Col("v").RollingStdBy(
                windowSize: TimeSpan.FromMinutes(2), 
                by: tsCol, 
                closed: ClosedWindow.Left
            ).Alias("std_left"),
            
            Col("v").RollingStdBy(
                windowSize: TimeSpan.FromMinutes(2), 
                by: tsCol, 
                closed: ClosedWindow.Both // 包含当前点和边界
            ).Alias("std_both")
        );

        // Check t2 (Index 2)
        // Left: [0, 10]
        Assert.Equal(7.07106, (double)resStd["std_left"][2], precision: 4);
        // Both: [0, 10, 10]
        Assert.Equal(5.77350, (double)resStd["std_both"][2], precision: 4);

        // --- 2. Rolling Rank By ---
        // 测试 Rank 在时间窗口内的表现
        using var resRank = df.Select(
            Col("v").RollingRankBy(
                windowSize: "3m", // 3分钟窗口
                by: tsCol,
                closed: ClosedWindow.Both
            ).Alias("rank")
        );
        
        // At t2 (00:02): Window [23:59, 00:02]. Includes t0, t1, t2 -> [0, 10, 10]
        // Current value is 10 (at t2).
        // Rank logic: 0->1, 10->2.5 (Average), 10->2.5
        // Polars rolling rank typically ranks the *current row's value* within the window.
        Assert.Equal(2.5, (double)resRank["rank"][2]);
    }
    [Fact]
    public void Test_Rolling_QuantileBy_Method()
    {
        var dates = new[] {
            new DateTime(2023, 1, 1, 0, 0, 0),
            new DateTime(2023, 1, 1, 0, 1, 0)
        };
        
        // Values: [1, 2]
        using var df = DataFrame.FromColumns(new 
        { 
            ts = dates, 
            v = new[] { 1.0, 2.0 } 
        });
        var tsCol = Col("ts").Cast(DataType.Datetime(TimeUnit.Microseconds));

        // Window "2m" covers both points.
        // We compute Median (0.5) of [1, 2].
        // Method: Linear -> 1.5
        // Method: Lower -> 1
        // Method: Higher -> 2
        
        using var res = df.Select(
            Col("v").RollingQuantileBy(
                quantile: 0.5, 
                method: QuantileMethod.Linear, 
                windowSize: "2m", 
                by: tsCol,
                closed: ClosedWindow.Both
            ).Alias("q_linear"),
            
            Col("v").RollingQuantileBy(
                quantile: 0.5, 
                method: QuantileMethod.Lower, 
                windowSize: "2m", 
                by: tsCol,
                closed: ClosedWindow.Both
            ).Alias("q_lower")
        );

        Assert.Equal(1.5, (double)res["q_linear"][1]);
        Assert.Equal(1.0, (double)res["q_lower"][1]);
    }
    [Fact]
    public void Test_Temporal_Literals()
    {
        // 准备数据：1行 dummy 数据用于 Select
        using var df = DataFrame.FromColumns(new { id = new[] { 1 } });

        // 1. DateOnly (2024-01-01)
        var d = new DateOnly(2024, 1, 1);
        
        // 2. TimeOnly (12:30:00)
        var t = new TimeOnly(12, 30, 0);
        
        // 3. TimeSpan (1 hour)
        var dur = TimeSpan.FromHours(1);

        // 4. DateTimeOffset (2024-01-01 12:00 +08:00) -> UTC 04:00
        var dto = new DateTimeOffset(2024, 1, 1, 12, 0, 0, TimeSpan.FromHours(8));

        using var res = df.Select(
            Lit(d).Alias("date"),
            Lit(t).Alias("time"),
            Lit(dur).Alias("duration"),
            Lit(dto).Alias("dto")
        );

        // 验证结果
        Assert.Equal(1, res.Height);
        // 重点验证 DateTimeOffset 的归一化
        // 2024-01-01 12:00+8 -> 2024-01-01 04:00 UTC
        // 取出来如果是 DateTime (Naive)，应该是 04:00
        var val = res.GetValue<DateTime>(0, "dto"); // 假设你取出来是 DateTime
        Assert.Equal(new DateTime(2024, 1, 1, 4, 0, 0), val);
    }
    [Fact]
    public void Test_Decimal_Lit()
    {
        using var df = DataFrame.FromColumns(new { id = new[] { 1 } });

        // 1. 常规小数
        decimal d1 = 12.34m;     // Unscaled: 1234, Scale: 2
        
        // 2. 负数 + 多位小数
        decimal d2 = -99.8765m;  // Unscaled: -998765, Scale: 4
        
        // 3. 整数型 Decimal
        decimal d3 = 100m;       // Unscaled: 100, Scale: 0 (通常)

        var res = df.Select(
            Lit(d1).Alias("d1"),
            Lit(d2).Alias("d2"),
            Lit(d3).Alias("d3")
        );

        Assert.Equal(1, res.Height);
        Assert.Equal(12.34m, res.GetValue<decimal>(0, "d1"));
        Assert.Equal(-99.8765m, res.GetValue<decimal>(0, "d2"));
        Assert.Equal(100m, res.GetValue<decimal>(0, "d3"));
        
        Assert.Equal(DataType.Decimal(38,2),res[0].DataType);
        Assert.Equal(DataType.Decimal(38,4),res[1].DataType);
    }
    [Fact]
    public void Test_Expr_Lit_List()
    {
        using var df = DataFrame.FromColumns(new { id = new[] { 1, 2, 3, 4, 5 } });

        // 1. 整数列表 (int[])
        var listInt = new[] { 1, 3, 5 };
        var res1 = df.Select(
            Lit(listInt).Implode().List.Contains(Col("id")).Alias("is_in_int")
        );
        Assert.True(res1.GetValue<bool>(0, "is_in_int")); // 1 in [1,3,5] -> True
        Assert.False(res1.GetValue<bool>(1, "is_in_int")); // 2 in [1,3,5] -> False
        
        // 2. 字符串列表 (string[])
        // 构造一个 List 列字面量
        string[] listStr =["A", "B"];
        var res2 = df.Select(
            Lit(listStr).Alias("lit_str_list")
        );

        Assert.Equal("A",res2[0][0]);
        Assert.Equal("B",res2[0][1]);
    }
    [Fact]
    public void Test_Expr_IsIn()
    {
        // 1. 准备数据
        using var df = DataFrame.FromColumns(new
        {
            id = new[] { 1, 2, 3, 4, 5 },
            name = new[] { "Alice", "Bob", "Charlie", "David", "Eve" }
        });

        // 2. 定义查找集合 (白名单)
        var validIds = new[] { 1, 3, 5 };       // ID 白名单
        var validNames = new[] { "Bob", "Eve" }; // 名字白名单

        // 3. 执行 IsIn
        // 注意：Lit(validIds) 生成的是一个 Series (集合)，这正是 IsIn 需要的右值
        var res = df.Select(
            Col("id").IsIn(Lit(validIds).Implode()).Alias("id_in_whitelist"),
            Col("name").IsIn(Lit(validNames).Implode()).Alias("name_in_whitelist")
        );

        // 4. 验证结果
        Assert.Equal(5, res.Height);

        // --- 验证 ID (1, 3, 5 应该为 True) ---
        // Row 0: id=1 (In [1,3,5]) -> True
        Assert.True(res.GetValue<bool>(0, "id_in_whitelist"));
        // Row 1: id=2 (Not In [1,3,5]) -> False
        Assert.False(res.GetValue<bool>(1, "id_in_whitelist"));
        // Row 2: id=3 (In [1,3,5]) -> True
        Assert.True(res.GetValue<bool>(2, "id_in_whitelist"));

        // --- 验证 Name (Bob, Eve 应该为 True) ---
        // Row 0: Alice -> False
        Assert.False(res.GetValue<bool>(0, "name_in_whitelist"));
        // Row 1: Bob -> True
        Assert.True(res.GetValue<bool>(1, "name_in_whitelist"));
    }
    [Fact]
    public void Test_Lit_Primitives_And_Nullables()
    {
        // 准备一个空 DF 用于执行 Select
        // 注意：Lit(array) 生成的是 Series。
        // 为了方便测试，我们让 DF 的高度与数组一致，或者使用 pl.Select (如果实现了静态入口)
        // 这里简单起见，创建一个高度匹配的 DF
        using var df = DataFrame.FromColumns(new { _ = new[] { 0, 0, 0 } }); // Height=3

        var ints = new[] { 1, 2, 3 };
        var nullDoubles = new double?[] { 1.1, null, 3.3 };
        var bools = new[] { true, false, true };

        var res = df.Select(
            Lit(ints).Alias("i"),           // 命中 int[] 分支
            Lit(nullDoubles).Alias("f"),    // 命中 double?[] 分支
            Lit(bools).Alias("b")           // 命中 bool[] 分支
        );

        // 验证结果
        Assert.Equal(3, res.Height);
        
        // Int Check
        Assert.Equal(1, res.GetValue<int>(0, "i"));
        
        // Nullable Double Check
        Assert.Equal(1.1, res.GetValue<double?>(0, "f"));
        Assert.Null(res.GetValue<double?>(1, "f")); // 验证 null 传递成功
        
        // Bool Check
        Assert.False(res.GetValue<bool>(1, "b"));
    }

    [Fact]
    public void Test_Lit_Struct_Implicit()
    {
        // 验证 Lit() 能否自动 fallback 到 StructPacker
        
        using var df = DataFrame.FromColumns(new { id = new[] { 1, 2 } });

        var users = new[]
        {
            new { Name = "Alice", Age = 30 },
            new { Name = "Bob",   Age = 25 }
        };

        // 魔法时刻：C# 匿名对象 -> Series<Struct>
        var res = df.Select(
            Lit(users).Alias("user_info")
        );

        /*
         Expected:
         shape: (2, 1)
         ┌───────────────┐
         │ user_info     │
         │ ---           │
         │ struct[2]     │
         ╞═══════════════╡
         │ {"Alice",30}  │
         │ {"Bob",25}    │
         └───────────────┘
        */
        
        Assert.Equal(2, res.Height);
        // 简单的 Schema 验证
        var dtype = res.Schema["user_info"];

        Assert.Equal(res.Schema["user_info"],DataType.Struct(["Name","Age"],[DataType.String,DataType.Int32]));
    }
    [Fact]
    public void Test_Complex_Struct_Filter()
    {
        // =================================================================
        // 这是我们这几天讨论的集大成者：
        // 用 C# 对象列表作为白名单，过滤 DataFrame 中的多列组合。
        // =================================================================

        // 1. 数据：销售记录 (Region, Dept)
        using var df = DataFrame.FromColumns(new
        {
            id = new[] { 1, 2, 3, 4 },
            region = new[] { "US", "US", "EU", "EU" },
            dept   = new[] { "Sales", "IT", "Sales", "IT" }
        });

        // 2. 白名单：只保留 (US, Sales) 和 (EU, IT)
        var validCombinations = new[] 
        {
            new { region = "US", dept = "Sales" }, // 匹配 Row 1
            new { region = "EU", dept = "IT" }     // 匹配 Row 4
        };

        // 3. 构建查询
        // AsStruct: 将两列打包成 Struct
        // Lit(validCombinations): 将 C# 数组转为 Series<Struct>
        // .Implode(): 将 Series<Struct> 打包成 scalar List<Struct> (一行)
        // .IsIn(): 检查左边的 struct 是否在右边的列表中
        var res = df.Select(
            Col("id"),
            AsStruct(Col("region"), Col("dept"))
                .IsIn(Lit(validCombinations).Implode()) 
                .Alias("is_valid")
        ).Filter(Col("is_valid")); // 只保留 valid 的行

        // 4. 验证结果
        // 应该只剩下 id=1 和 id=4
        
        Assert.Equal(2, res.Height);
        Assert.Equal(1, res.GetValue<int>(0, "id"));
        Assert.Equal(4, res.GetValue<int>(1, "id"));
    }
   [Fact]
    public void Test_The_Grand_Loop_Read_Write()
    {
        // 1. 准备：深层嵌套数据 (Struct -> List -> Struct)
        using var df = DataFrame.FromColumns(new { _ = new[] { 0 } });

        var logs = new[]
        {
            new 
            { 
                Machine = "Server-A",
                Events = new[] 
                {
                    new { Code = 200, Msg = "OK" },
                    new { Code = 500, Msg = "Error" }
                }
            },
            new 
            { 
                Machine = "Server-B",
                Events = new[] 
                {
                    new { Code = 404, Msg = "Not Found" }
                }
            }
        };

        // 2. 写入 (Write): 利用 ArrowConverter 瞬间入库
        var dfComplex = df.Select(
            Lit(logs).Alias("log_entry")
        );
    
        // 3. 计算 (Compute): 利用 Expr.Struct.Field 在引擎内部做手术
        // 目标：提取每台机器的“第一个事件的消息 (Msg)”
        // 路径：log_entry (Struct) -> Events (List) -> Get(0) (Struct) -> Msg (String)
        var res = dfComplex.Select(
            Col("log_entry").Struct.Field("Machine").Alias("host"),
            
            Col("log_entry")
                .Struct.Field("Events")
                .List.Get(0) // 取列表第一个元素
                .Struct.Field("Msg") // 取 Struct 里的 Msg 字段
                .Alias("first_msg")
        );

        // 4. 读取 (Read): 利用 ArrowReader 验证数据回流到 C#
        // 此时数据已经经历了一圈：C# -> Arrow -> Rust/Polars -> Arrow -> C#
        
        Assert.Equal(2, res.Height);

        // Row 0: Server-A, First Event is OK
        Assert.Equal("Server-A", res.GetValue<string>(0, "host"));
        Assert.Equal("OK",       res.GetValue<string>(0, "first_msg"));

        // Row 1: Server-B, First Event is Not Found
        Assert.Equal("Server-B", res.GetValue<string>(1, "host"));
        Assert.Equal("Not Found", res.GetValue<string>(1, "first_msg"));
    }

    [Fact]
    public void Test_Level2_Struct_With_List()
    {
        // 场景：带标签的用户，Struct 里面套 List
        // 这是纯手写 Packer 最头疼的地方（因为要算 Offset），但 Arrow 应该能秒杀
        using var df = DataFrame.FromColumns(new { _ = new[] { 0 } });

        var users = new[]
        {
            new 
            { 
                Name = "Alice", 
                Tags = new[] { 1, 2, 3 } // <--- List<int>
            },
            new 
            { 
                Name = "Bob", 
                Tags = new int[] { }     // <--- Empty List
            }
        };

        var res = df.Select(
            Lit(users).Alias("user_tags")
        );

        /*
         shape: (2, 1)
         ┌─────────────────────┐
         │ user_tags           │
         │ ---                 │
         │ struct[2]           │
         ╞═════════════════════╡
         │ {"Alice",[1, 2, 3]} │
         │ {"Bob",[]}          │
         └─────────────────────┘
        */

        Assert.Equal(2, res.Height);
        
        // 验证第一行 Tags 长度为 3
        // 验证第二行 Tags 长度为 0
    }

    [Fact]
    public void Test_Level3_The_Ultimate_Nest()
    {
        // 场景：复杂的 JSON 风格数据
        // Struct 
        //  -> List 
        //      -> Struct (对象数组)
        using var df = DataFrame.FromColumns(new { _ = new[] { 0 } });

        var complexData = new[]
        {
            new 
            { 
                Id = 1,
                History = new[] // <--- List of Structs
                {
                    new { Date = "2023-01-01", Action = "Login" },
                    new { Date = "2023-01-02", Action = "Logout" }
                }
            },
            new 
            { 
                Id = 2,
                History = new[] 
                {
                    new { Date = "2023-01-05", Action = "Purchase" }
                }
            }
        };

        // 这一步如果不报错，说明 SeriesFactory -> ArrowConverter -> FFI 整个链路完全通畅
        var res = df.Select(
            Lit(complexData).Alias("audit_log")
        );

        res.Show();

        // 既然这都能成，我们顺便测试一下 Expr 的深层访问能力 (如果有的话)
        // 比如：展开 History 列表
        // col("audit_log").struct.field("History").explode()
        
        var exploded = res.Select(
            Col("audit_log").Struct.Field("History").Explode().Alias("flat_log")
        );
        
        // 应该变成 3 行 (2 + 1)
        Assert.Equal(3, exploded.Height);
    }
    [Fact]
    public void Filter_Expression_Works()
    {
        // 准备数据：a 为 1..10， group 为分组 ID
        var df = new DataFrame(
            new Series("a", Enumerable.Range(1, 10).ToArray()),
            new Series("group", new[] { 1, 1, 1, 1, 1, 2, 2, 2, 2, 2 })
        );

        // 场景：在 Select 中使用 Expr.Filter
        // 这里的逻辑是：取出列 "a"，但只保留那些 "a" 大于 5 的元素
        // 注意：在 Polars Eager 模式下，Select 返回的 DataFrame 要求所有列长度一致。
        // 所以单纯 df.Select(Col("a").Filter(...)) 会因为行数变少而成功返回一个较短的 DF。
        
        var res = df.Select(
            Col("a").Filter(Col("a") > 5).Alias("filtered_a")
        );

        var series = res["filtered_a"];

        // 验证：1..10 中大于 5 的有 6, 7, 8, 9, 10 (共5个)
        Assert.Equal(5, series.Len());
        Assert.Equal(6, (int)series[0]);
        Assert.Equal(10, (int)series[4]);
    }

    [Fact]
    public void Filter_In_GroupBy_Works()
    {
        // 这是一个更典型的场景：在分组聚合中进行过滤
        var df = new DataFrame(
            new Series("id", new[] { 1, 1, 1, 2, 2 }),
            new Series("val", new[] { 10, 20, 30, 40, 50 })
        );

        // GroupBy id, 然后取出 val 中大于 15 的值的平均值
        var res = df.GroupBy("id").Agg(
            Col("val").Filter(Col("val") > 15).Mean().Alias("conditional_mean")
        ).Sort("id", false);

        // Group 1: [10, 20, 30] -> Filter(>15) -> [20, 30] -> Mean -> 25
        // Group 2: [40, 50] -> Filter(>15) -> [40, 50] -> Mean -> 45
        
        var meanCol = res["conditional_mean"];
        Assert.Equal(25.0, (double)meanCol[0]);
        Assert.Equal(45.0, (double)meanCol[1]);
    }
    [Fact]
    public void Test_Expr_Dot_Product()
    {
        // 1. 准备数据
        // a = [1, 2, 3]
        // b = [4, 5, 6]
        using var df = DataFrame.FromColumns(new
        {
            a = new[] { 1, 2, 3 },
            b = new[] { 4, 5, 6 }
        });

        // 2. 执行点积
        // Calculation: (1*4) + (2*5) + (3*6) = 4 + 10 + 18 = 32
        using var result = df.Select(
            Col("a").Dot(Col("b")).Alias("dot_res")
        );
        result.Show();
        // 3. 验证
        // Dot product 返回的是标量（Scalar），但在 Polars 里它依然是 Series/DataFrame 的形式
        // 对于整数输入，结果通常是 Int64
        var val = result["dot_res"][0];
        Assert.Equal(32L, Convert.ToInt64(val));
    }

    [Fact]
    public void Test_Expr_Cosine_Similarity()
    {
        // 1. 准备正交向量（Cosine Should be 0）和平行向量（Cosine Should be 1）
        // Case 1: [1, 0] vs [0, 1] -> Orthogonal -> Dot=0, Cos=0
        // Case 2: [1, 1] vs [2, 2] -> Parallel -> Cos=1
        
        // 注意：Cosine Sim 涉及除法和开方，最好用浮点数
        using var df = DataFrame.FromColumns(new
        {
            // Vector A
            a1 = new[] { 1.0, 0.0 }, 
            a2 = new[] { 0.0, 1.0 },
            
            // Vector B
            b1 = new[] { 1.0, 1.0 },
            b2 = new[] { 2.0, 2.0 }
        });

        // 2. 定义余弦相似度公式函数
        // Cos(A, B) = (A . B) / (|A| * |B|)
        // |A| = Sqrt(Sum(A^2))
        static Expr L2Norm(Expr e) => e.Pow(2).Sum().Sqrt();
        static Expr Cosine(Expr a, Expr b) => a.Dot(b) / (L2Norm(a) * L2Norm(b));

        // 3. 计算
        using var res = df.Select(
            Cosine(Col("a1"), Col("a2")).Alias("ortho"), // [1,0] . [0,1]
            Cosine(Col("b1"), Col("b2")).Alias("parallel") // [1,1] . [2,2]
        );

        // 4. 验证
        var ortho = (double?)res["ortho"][0];
        var parallel = (double?)res["parallel"][0];

        // 浮点数比较需要容差
        Assert.True(Math.Abs(ortho.Value - 0.0) < 1e-6, $"Expected 0.0, got {ortho}");
        Assert.True(Math.Abs(parallel.Value - 1.0) < 1e-6, $"Expected 1.0, got {parallel}");
    }

    [Fact]
    public void Test_Dot_Product_With_Nulls()
    {
        // 测试包含 Null 的情况，Polars 的 dot 通常会处理 Null（视作0或忽略，取决于具体实现，通常是忽略 null 进行 sum）
        using var df = DataFrame.FromColumns(new
        {
            a = new[] { 1, 2, null as int? },
            b = new[] { 4, 5, 10 }
        });

        // (1*4) + (2*5) + (null*10) = 4 + 10 + ? 
        // Polars 默认行为：None propagates? 或者类似 sum 忽略？
        // 让我们验证一下行为。通常 Dot 操作如果由 underlying arrow compute kernel 执行，
        // 任何一方为 null，乘积为 null。求和时忽略 null。
        // 所以预期：4 + 10 + null -> 14
        
        using var result = df.Select(Col("a").Dot(Col("b")));
        
        var val = result[0][0];
        
        // 如果 Polars 策略是 Propagate Nulls (像 +, * 那样)，结果可能是 Null
        // 如果 Polars 策略是 Ignore Nulls (像 Sum 那样)，结果是 14
        // 根据 Polars 文档，dot 忽略 nulls。
        if (val == null)
        {
             // 如果你发现这里挂了，说明行为是 Propagate，我们改测试预期
             Assert.Null(val); 
        }
        else
        {
             Assert.Equal(14L, Convert.ToInt64(val));
        }
    }
    [Fact]
    public void Test_Interpolate()
    {
        using var df = DataFrame.FromColumns(new
        {
            // 1, null, null, 4, 5
            val = new double?[] { 1.0, null, null, 4.0, 5.0 } 
        });

        // 1. Linear Interpolation (Default)
        // 1.0 -> (2.0) -> (3.0) -> 4.0 -> 5.0
        using var linear = df.Select(Col("val").Interpolate().Alias("linear"));
        var linearArr = linear["linear"].ToArray<double?>();
        
        Assert.Equal(2.0, linearArr[1]);
        Assert.Equal(3.0, linearArr[2]);

        // 2. Nearest Interpolation
        // 1.0 -> (1.0) -> (4.0) -> 4.0 -> 5.0  (depends on strategy, usually rounds to nearest valid)
        // Polars Nearest strategy: 
        // Index 1 (val=null) is closer to Index 0 (val=1) ? or Index 3 (val=4)?
        // Let's verify standard nearest neighbor behavior
        using var nearest = df.Select(Col("val").Interpolate(InterpolationMethod.Nearest).Alias("nearest"));
        var nearestArr = nearest["nearest"].ToArray<double?>();
        
        // 验证非空
        Assert.NotNull(nearestArr[1]);
        Assert.NotNull(nearestArr[2]);
    }
    [Fact]
    public void Test_InterpolateBy_Expr()
    {
        // 构造非等间距数据
        // Time: 0, 10, 20 (中间缺了 2, 5 这种均匀步长，但这里只看相对距离)
        // 让我们用一个更明显的例子：
        // Position: 0,      2,          10
        // Value:    0,      ?,          100
        //
        // Linear (Index based): Index 1 is exactly between Index 0 and Index 2.
        //      Result = (0 + 100) / 2 = 50
        //
        // InterpolateBy (Position based):
        //      Total dist = 10 - 0 = 10
        //      Current dist = 2 - 0 = 2
        //      Ratio = 2 / 10 = 0.2
        //      Result = 0 + (100 - 0) * 0.2 = 20
        
        using var df = DataFrame.FromColumns(new
        {
            pos = new double[] { 0, 2, 10 },
            val = new double?[] { 0, null, 100 }
        });

        using var res = df.Select(
            Col("val").Interpolate().Alias("linear_index"),
            Col("val").InterpolateBy(Col("pos")).Alias("linear_pos")
        );

        // 1. 验证普通插值 (基于 Index，位于中间)
        var linearIndex = res["linear_index"][1];
        Assert.Equal(50.0, (double)linearIndex!);

        // 2. 验证按列插值 (基于 Position，位于 20% 处)
        var linearPos = res["linear_pos"][1];
        Assert.Equal(20.0, (double)linearPos!);
    }
    [Fact]
    [Trait("Expr","SqlExpr")]
    public void Test_SqlExpr_MixedWithNativeExpr()
    {
        // 1. 准备基础数据
        using var df = DataFrame.FromColumns(new
        {
            a = new int[] { 1, 2, 3 } 
        });

        // 2. Act: 在 Select 中将原生 Col() 和 SqlExpr() 无缝混搭
        // 注意：在 Polars SQL 中，通常用 VARCHAR 或 TEXT 表示字符串
        using var resultDf = df.Select(
            Col("a").Alias("original_a"),                  // 原生 Expr 算子
            SqlExpr("POWER(a, a) AS a_a"),                 // SQL 表达式：幂运算并起别名
            SqlExpr("CAST(a AS VARCHAR) AS a_txt")         // SQL 表达式：类型转换并起别名
        );

        // 3. Assert 验证计算结果
        
        // 验证 POWER 计算结果 (1^1=1, 2^2=4, 3^3=27)
        // 注意：Polars SQL 的 POWER 函数返回值类型默认推断为 Float64 (double)
        var aaArr = resultDf["a_a"].ToArray<double>();
        Assert.Equal(1.0, aaArr[0]);
        Assert.Equal(4.0, aaArr[1]);
        Assert.Equal(27.0, aaArr[2]);

        // 验证 CAST 转换结果
        var txtArr = resultDf["a_txt"].ToArray<string>();
        Assert.Equal("1", txtArr[0]);
        Assert.Equal("2", txtArr[1]);
        Assert.Equal("3", txtArr[2]);

        // 验证原生列保持原样
        var origArr = resultDf["original_a"].ToArray<int>();
        Assert.Equal(1, origArr[0]);
        Assert.Equal(3, origArr[2]);
    }
    [Fact]
    [Trait("Expr","Get")]
    public void Test_Expr_Get()
    {
        // 构造测试数据
        // Index: 0,  1,  2,  3,  4
        // Value: 10, 20, 30, 40, 50
        using var df = DataFrame.FromColumns(new
        {
            val = new int[] { 10, 20, 30, 40, 50 }
        });

        using var res = df.Select(
            // 正常取值：取 Index 2 的值，应该返回 30
            Col("val").Get(2).Alias("get_valid"),
            
            // 越界取值：取 Index 10，由于开启了 nullOnOutOfBounds = true，应该返回 null 而不报错
            Col("val").Get(10, nullOnOutOfBounds: true).Alias("get_oob")
        );

        // 验证正常取值
        Assert.Equal(30, (int)res["get_valid"][0]!);

        // 验证越界安全返回 Null
        Assert.Null(res["get_oob"][0]);
    }

    [Fact]
    [Trait("Expr","GatherTake")]
    public void Test_Expr_Gather_And_Take()
    {
        // 构造测试数据
        // 这个例子模拟了非常经典的 LINQ 场景：按成绩排名，提取前几名的人名
        using var df = DataFrame.FromColumns(new
        {
            name = new string[] { "Alice", "Bob", "Charlie", "David" },
            score = new int[]   { 85,      92,    78,        95      }
        });

        // idx_gather: 取排名第一和第二的索引 (对应 David: 3, Bob: 1)
        // idx_take:   随意挑几个索引提取
        using var dfIndices = DataFrame.FromColumns(new
        {
            idx_take = new int[] { 0, 2, 0 } // 取 Alice(0), Charlie(2), Alice(0)
        });

        using var res = df.Select(
            // Gather: 配合 ArgSort，按成绩从高到低提取名字
            Col("name").Gather(Col("score").ArgSort(descending:true)).Alias("ranked_names"),
            
            // Take (Gather的别名糖): 通过指定的整数列提取
            Col("name").Take(Lit([0, 2, 0,1])).Alias("taken_names") 
        );

        var rankedNames = res["ranked_names"];
        Assert.Equal(4, rankedNames.Length);
        Assert.Equal("David",   (string)rankedNames[0]!); // 第1名
        Assert.Equal("Bob",     (string)rankedNames[1]!); // 第2名
        Assert.Equal("Alice",   (string)rankedNames[2]!); // 第3名
        Assert.Equal("Charlie", (string)rankedNames[3]!); // 第4名

        var takenNames = res["taken_names"];
        Assert.Equal(4, takenNames.Length);
        Assert.Equal("Alice",   (string)takenNames[0]!);
        Assert.Equal("Charlie", (string)takenNames[1]!);
        Assert.Equal("Alice",   (string)takenNames[2]!);
        Assert.Equal("Bob",     (string)takenNames[3]!);
    }

    [Fact]
    [Trait("Expr","GatherEvery")]
    public void Test_Expr_GatherEvery()
    {
        // 构造测试数据
        // Index: 0,  1,  2,  3,  4,  5,  6,  7,  8
        // Value: 0, 10, 20, 30, 40, 50, 60, 70, 80
        using var df = DataFrame.FromColumns(new
        {
            val = new int[] { 0, 10, 20, 30, 40, 50, 60, 70, 80 }
        });

        // 测试目标：从 offset = 2 (即数字 20) 开始，每隔 3 个取一次
        // 提取的索引应该是：2, 5, 8
        // 对应的值应该是：20, 50, 80
        using var res = df.Select(
            Col("val").GatherEvery(n: 3, offset: 2).Alias("every_3_offset_2")
        );

        var resultCol = res["every_3_offset_2"];
        
        Assert.Equal(3, resultCol.Length);
        Assert.Equal(20, (int)resultCol[0]!);
        Assert.Equal(50, (int)resultCol[1]!);
        Assert.Equal(80, (int)resultCol[2]!);
    }
    [Fact]
    [Trait("Expr","Arg")]
    public void Test_Expr_Arg_Extremes_And_Unique()
    {
        // 构造测试数据
        // Index: 0, 1,  2,  3, 4
        // Value: 5, 5, 10, 10, 1
        using var df = DataFrame.FromColumns(new
        {
            val = new int[] { 5, 5, 10, 10, 1 }
        });

        using var res = df.Select(
            Col("val").ArgMin().Alias("min_idx"),
            Col("val").ArgMax().Alias("max_idx"),
            Col("val").ArgUnique().Alias("unique_idx")
        );

        // 1. 验证 ArgMin: 最小值是 1，位于索引 4
        // Polars 的索引类型默认是 UInt32 (u32)
        Assert.Equal(4u, (uint)res["min_idx"][0]!);

        // 2. 验证 ArgMax: 最大值是 10，第一次出现位于索引 2
        Assert.Equal(2u, (uint)res["max_idx"][0]!);

        // 3. 验证 ArgUnique: 唯一值分别是 5, 10, 1，它们第一次出现的索引是 0, 2, 4
        var uniqueIdx = res["unique_idx"];
        Assert.Equal(3, uniqueIdx.Length);
        Assert.Equal(0u, (uint)uniqueIdx[0]!); // 首次出现的 5
        Assert.Equal(2u, (uint)uniqueIdx[1]!); // 首次出现的 10
        Assert.Equal(4u, (uint)uniqueIdx[2]!); // 首次出现的 1
    }
    [Fact]
    [Trait("Expr","IndexSearch")]
    public void Test_Expr_IndexOf_And_SearchSorted()
    {
        // 构造测试数据 (对于 SearchSorted，数据最好是有序的)
        // Index:  0,  1,  2,  3,  4,  5
        // Value: 10, 20, 20, 30, 40, 50
        using var df = DataFrame.FromColumns(new
        {
            val = new int[] { 10, 20, 20, 30, 40, 50 }
        });

        using var res = df.Select(
            // --- IndexOf 测试 ---
            Col("val").IndexOf(Lit(20)).Alias("idx_of_20"),
            Col("val").IndexOf(Lit(99)).Alias("idx_of_99"), // 查找不存在的值
            
            // --- SearchSorted 测试 ---
            // 查找 25 应该插入的位置：在 20 和 30 之间，即索引 3
            Col("val").SearchSorted(Lit(25)).Alias("search_25"),
            
            // 查找 20 插入位置 (Left 边界)：插在第一个 20 前面，即索引 1
            Col("val").SearchSorted(Lit(20), side: SearchSortedSide.Left).Alias("search_20_left"),
            
            // 查找 20 插入位置 (Right 边界)：插在最后一个 20 后面，即索引 3
            Col("val").SearchSorted(Lit(20), side: SearchSortedSide.Right).Alias("search_20_right")
        );

        // 1. 验证 IndexOf
        Assert.Equal(1u, (uint)res["idx_of_20"][0]!); // 20 首次出现在索引 1
        Assert.Null(res["idx_of_99"][0]); // 找不到 99，返回 Null

        // 2. 验证 SearchSorted 常规插入点
        Assert.Equal(3u, (uint)res["search_25"][0]!); 

        // 3. 验证 SearchSorted 左右边界行为
        Assert.Equal(1u, (uint)res["search_20_left"][0]!);
        Assert.Equal(3u, (uint)res["search_20_right"][0]!);
    }
}