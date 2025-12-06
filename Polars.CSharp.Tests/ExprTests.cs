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
        var yearCol = batch.Column("year") as IArrowArray;
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
}