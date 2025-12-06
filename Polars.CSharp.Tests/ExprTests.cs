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
}