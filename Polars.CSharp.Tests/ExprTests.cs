using Apache.Arrow;
using Polars.NET.Core;
using Polars.NET.Core.Arrow;
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
        using var res = df.Filter(Col("val") > Lit(15));
        
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
        using var res = df.Filter(Col("birthdate").Dt.Year() < Lit(1990));

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
        using var res = df.Filter(Col("value") == Lit(3.36));
        
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
                Col("age").FillNull(Lit(0)).Alias("age_filled")
            )
            .Filter(Col("age_filled") >= Lit(0));
            
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
            (Col("height") > Lit(1.7))
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
        using var df = new DataFrame(s);
        
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
            Col("score") >= Lit(90),
            Lit("A"),
            // 嵌套 IfElse (Else 分支)
            IfElse(
                Col("score") >= Lit(60),
                Lit("Pass"),
                Lit("Fail")
            )
        ).Alias("grade");

        using var res = df
            .WithColumns(gradeExpr)
            .Sort(Col("score"), descending: true); // 降序

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
    public void Test_Expr_Explode_In_Select()
    {
        using var s = new Series("data", ["x,y"]);
        using var df = new DataFrame(s);

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
        using var df = new DataFrame(s);

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
        using var df = new DataFrame(s);

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
                AsStruct(Col("A"), Col("B"))
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
}