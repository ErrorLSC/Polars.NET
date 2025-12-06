using Apache.Arrow;

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
        using var csv = new DisposableCsv("name,birthdate,weight,height\nQinglei,2025-11-25,70,1.80");
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
        using var batch = res.ToArrow();
        
        // 1. 验证 Name (String)
        Assert.Equal("Qinglei", batch.Column("name").GetStringValue(0));

        // 2. 验证 Year (Int32 or Int64 depending on Polars/Arrow mapping)
        // Polars Year 通常返回 Int32
        var yearCol = batch.Column("year");
        Assert.Equal(2025, yearCol.GetInt64Value(0));

        // 3. 验证 BMI (Double)
        var bmiCol = batch.Column("bmi") as DoubleArray;
        Assert.NotNull(bmiCol);
        
        double bmi = bmiCol.GetValue(0) ?? 0.0;
        // 70 / (1.8 * 1.8) = 21.6049...
        Assert.True(bmi > 21.6);
        Assert.True(bmi < 21.7);
    }

    // ==========================================
    // 2. Filter by numeric value (> operator)
    // ==========================================
    [Fact]
    public void Filter_By_Numeric_Value_Gt()
    {
        using var csv = new DisposableCsv("val\n10\n20\n30");
        using var df = DataFrame.ReadCsv(csv.Path);

        // C# 运算符重载: Col("val") > Lit(15)
        using var res = df.Filter(Col("val") > Lit(15));
        
        Assert.Equal(2, res.Height); // 20, 30
        
        // 验证结果
        using var batch = res.ToArrow();
        var valCol = batch.Column("val");
        
        Assert.Equal(20, valCol.GetInt64Value(0));
        Assert.Equal(30, valCol.GetInt64Value(1));
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
        
        using var csv = new DisposableCsv(content);
        // tryParseDates 默认为 true
        using var df = DataFrame.ReadCsv(csv.Path);

        // 逻辑: birthdate.year < 1990
        using var res = df.Filter(Col("birthdate").Dt.Year() < Lit(1990));

        Assert.Equal(1, res.Height); // 只有 Ben Brown
        
        using var batch = res.ToArrow();
        Assert.Equal("Ben Brown", batch.Column("name").GetStringValue(0));
    }

    // ==========================================
    // 4. Filter by string value (== operator)
    // ==========================================
    [Fact]
    public void Filter_By_String_Value_Eq()
    {
        using var csv = new DisposableCsv("name\nAlice\nBob\nAlice");
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
        using var csv = new DisposableCsv("value\n3.36\n4.2\n5\n3.36");
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
        using var csv = new DisposableCsv("age\n10\n\n30");
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
        using var batch = filled.ToArrow();
        var filledCol = batch.Column("age_filled"); // 通常是 Int64
        Assert.Equal(0, filledCol.GetInt64Value(1)); // 第二行索引为 1

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

        using var csv = new DisposableCsv(content);
        
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
        
        using var batch = res.ToArrow();
        Assert.Equal("Qinglei", batch.Column("name").GetStringValue(0));
    }
    [Fact]
    public void Math_Ops_BMI_Calculation_With_Pow()
    {
        // 构造数据: 身高(m), 体重(kg)
        using var csv = new DisposableCsv("name,height,weight\nAlice,1.65,60\nBob,1.80,80");
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

        using var batch = res.ToArrow();

        // 验证 Bob 的 BMI: 80 / 1.8^2 = 24.691358...
        // Bob 是第二行 (index 1)
        var bmiCol = batch.Column("bmi") as DoubleArray;
        Assert.NotNull(bmiCol);
        
        double bobBmi = bmiCol.GetValue(1) ?? 0.0;
        Assert.True(bobBmi > 24.69 && bobBmi < 24.70);

        // 验证 Alice 的 Sqrt: sqrt(1.65) = 1.2845...
        // Alice 是第一行 (index 0)
        var sqrtCol = batch.Column("sqrt_h") as DoubleArray;
        Assert.NotNull(sqrtCol);

        double aliceSqrt = sqrtCol.GetValue(0) ?? 0.0;
        Assert.True(aliceSqrt > 1.28 && aliceSqrt < 1.29);
    }
    // ==========================================
    // String Operations
    // ==========================================

    [Fact]
    public void String_Operations_Case_Slice_Replace()
    {
        // 脏数据: "Hello World", "foo BAR"
        using var csv = new DisposableCsv("text\nHello World\nfoo BAR");
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

        using var batch = res.ToArrow();

        // 验证 Row 0: "Hello World"
        Assert.Equal("HELLO WORLD", batch.Column("upper").GetStringValue(0));
        Assert.Equal("Hel", batch.Column("slice").GetStringValue(0));
        Assert.Equal("Hell0 W0rld", batch.Column("replaced").GetStringValue(0));
        
        // Polars len() 返回的是 u32，我们的 GetInt64Value 会处理转换
        Assert.Equal(11, batch.Column("len").GetInt64Value(0)); 

        // 验证 Row 1: "foo BAR"
        Assert.Equal("FOO BAR", batch.Column("upper").GetStringValue(1));
        Assert.Equal("foo", batch.Column("slice").GetStringValue(1));
    }

    [Fact]
    public void String_Regex_Replace_And_Extract()
    {
        using var csv = new DisposableCsv("text\nUser: 12345\nID: 999");
        using var df = DataFrame.ReadCsv(csv.Path);

        using var res = df.Select(
            // 1. Regex Replace: 把数字换成 #
            // C# 字符串中反斜杠需要转义，所以写 "\\d+" 或者 @"\d+"
            Col("text").Str.ReplaceAll(@"\d+", "#", useRegex: true).Alias("masked"),
            
            // 2. Regex Extract: 提取数字部分
            // @"(\d+)" 是第 1 组
            Col("text").Str.Extract(@"(\d+)", 1).Alias("extracted_id")
        );

        using var batch = res.ToArrow();

        // 验证 Replace
        // "User: 12345" -> "User: #"
        Assert.Equal("User: #", batch.Column("masked").GetStringValue(0));
        
        // 验证 Extract
        // "User: 12345" -> "12345"
        Assert.Equal("12345", batch.Column("extracted_id").GetStringValue(0));
        Assert.Equal("999", batch.Column("extracted_id").GetStringValue(1));
    }
    // ==========================================
    // Temporal Ops (Components, Format, Cast)
    // ==========================================
    [Fact]
    public void Temporal_Ops_Components_Format_Cast()
    {
        // 构造数据: 包含日期和时间的字符串
        // Row 0: 2023年圣诞节下午3点半 (周一)
        // Row 1: 2024年元旦零点 (周一)
        var csvContent = "ts\n2023-12-25 15:30:00\n2024-01-01 00:00:00";
        using var csv = new DisposableCsv(csvContent);

        // [关键] 开启 tryParseDates=true，让 Polars 自动解析为 Datetime 类型
        using var df = DataFrame.ReadCsv(csv.Path, tryParseDates: true);

        using var res = df.Select(
            Col("ts"),

            // 1. 提取组件 (Components)
            Col("ts").Dt.Year().Alias("y"),
            Col("ts").Dt.Month().Alias("m"),
            Col("ts").Dt.Day().Alias("d"),
            Col("ts").Dt.Hour().Alias("h"),
            
            // Polars 定义: Monday=1, Sunday=7
            Col("ts").Dt.Weekday().Alias("w_day"),
            
            // 2. 格式化 (Format to String)
            // 测试自定义格式: "2023/12/25"
            Col("ts").Dt.ToString("%Y/%m/%d").Alias("fmt_custom"),
            
            // 3. 类型转换 (Cast to Date)
            // Datetime (含时分秒) -> Date (只含日期)
            // 这一步依赖我们在 Wrapper.Expr.cs 中添加了 DtDate 绑定
            Col("ts").Dt.Date().Alias("date_only")
        );

        // 验证结果
        using var batch = res.ToArrow();

        // --- 验证 Row 0: 2023-12-25 15:30:00 ---
        
        // 年月日 (Polars 返回的是 Int32，我们的 GetInt64Value 会自动兼容)
        Assert.Equal(2023, batch.Column("y").GetInt64Value(0));
        Assert.Equal(12, batch.Column("m").GetInt64Value(0));
        Assert.Equal(25, batch.Column("d").GetInt64Value(0));
        
        // 小时
        Assert.Equal(15, batch.Column("h").GetInt64Value(0));
        
        // 星期 (2023-12-25 是周一，Polars weekday 1-7)
        Assert.Equal(1, batch.Column("w_day").GetInt64Value(0));

        // 格式化字符串验证
        // 这里验证 .Dt.ToString() 是否生效
        Assert.Equal("2023/12/25", batch.Column("fmt_custom").GetStringValue(0));

        // Date 类型验证
        // 这里验证 .Dt.Date() 是否成功把 Datetime 转成了 Date32
        // 我们在 ArrowExtensions.cs 里写的 FormatValue 会把 Date32 渲染为 "yyyy-MM-dd"
        // 且不包含时分秒
        var dateCol = batch.Column("date_only");
        Assert.Equal("2023-12-25", dateCol.FormatValue(0));

        // --- 验证 Row 1: 2024-01-01 00:00:00 ---
        Assert.Equal(2024, batch.Column("y").GetInt64Value(1));
        Assert.Equal(1, batch.Column("m").GetInt64Value(1));
        Assert.Equal(0, batch.Column("h").GetInt64Value(1)); // 零点
    }
    // ==========================================
    // Cast Ops: Int to Float, String to Int
    // ==========================================
    [Fact]
    public void Cast_Ops_Int_To_Float_String_To_Int()
    {
        using var csv = new DisposableCsv("val_str,val_int\n100,10\n200,20");
        using var df = DataFrame.ReadCsv(csv.Path);

        using var res = df.Select(
            // 1. String -> Int64
            Col("val_str").Cast(DataType.Int64).Alias("str_to_int"),
            
            // 2. Int64 -> Float64
            Col("val_int").Cast(DataType.Float64).Alias("int_to_float")
        );

        // 验证
        using var batch = res.ToArrow();
        
        // 验证 str_to_int (Row 0: 100)
        // GetInt64Value 兼容 Int32/Int64，很安全
        long v1 = batch.Column("str_to_int").GetInt64Value(0) ?? 0;
        Assert.Equal(100L, v1);

        // 验证 int_to_float (Row 1: 20)
        var floatCol = batch.Column("int_to_float") as DoubleArray; // Float64 -> DoubleArray
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
        using var csv = new DisposableCsv("student,score\nAlice,95\nBob,70\nCharlie,50");
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
}