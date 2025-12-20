using Xunit;

namespace Polars.CSharp.Tests;
public class SelectorTests
{
    [Fact]
    public void Test_Selector_All_Exclude()
    {
        // 1. 准备数据 (4列)
        var df = DataFrame.FromColumns(new 
        {
            Id = new[] { 1, 2 },
            Name = new[] { "Alice", "Bob" },
            Age = new[] { 25, 30 },
            Secret = new[] { "pass1", "pass2" }
        });

        // 2. 使用 Selector: All().Exclude(...)
        // 场景：保留所有业务数据，剔除 ID 和 敏感信息
        // 这里发生了隐式转换: Selector -> Expr
        var result = df.Select(
            Polars.All().Exclude("Id", "Secret")
        );

        // 3. 验证结果
        Assert.Equal(2, result.Width);
        
        // 应该只剩下 Name 和 Age
        Assert.Equal("Name", result.Column(0).Name);
        Assert.Equal("Age", result.Column(1).Name);
        
        // 确保排除的列不存在 (抛异常)
        Assert.Throws<ArgumentException>(() => result["Id"]);
        Assert.Throws<ArgumentException>(() => result["Secret"]);
    }

    [Fact]
    public void Test_Selector_Operation()
    {
        // Selector 不仅能筛选，还能直接由 Expr 的能力
        // 比如：选取除了 Id 以外的所有列，并把它们都乘以 2 (假设都是数值)
        
        var df = DataFrame.FromColumns(new 
        {
            Id = new[] { 1, 2 },
            Val1 = new[] { 10, 20 },
            Val2 = new[] { 100, 200 }
        });

        // select (All - "Id") * 2
        var result = df.Select(
            (Polars.All().Exclude("Id") * 2).Name.Suffix("_Scaled") 
            // 注意：Polars 里的 Selector 运算通常会保持原名，
            // 或者变成 struct，这里简单测试 broadcast 乘法
        );

        // 验证
        Assert.Equal(20, result[0, "Val1_Scaled"]); // 10 * 2
        Assert.Equal(200, result[0, "Val2_Scaled"]); // 100 * 2
        
        // Id 列不应该参与计算 (也不在结果里，因为我们没选它)
        Assert.Throws<ArgumentException>(() => result["Id"]);
    }
    
    [Fact]
    public void Test_Selector_Full_Capabilities()
    {
        // 1. 准备宽表 (模拟复杂 ETL 场景)
        var df = DataFrame.FromColumns(new 
        {
            Id = new[] { 1, 2 },
            Meta_Info = new[] { "A", "B" },
            Price_US = new[] { 10.5, 20.0 },
            Price_EU = new[] { 8.5, 15.0 },
            Qty_2023 = new[] { 100, 200 },
            Qty_2024 = new[] { 110, 220 },
            Timestamp = new[] { DateTime.Now, DateTime.Now }
        });

        // 2. 复杂选择逻辑
        // 目标：
        // A. 选取所有 Price 开头的列，转为整数 (乘以 100)
        // B. 选取所有 Qty 结尾的列
        // C. 排除 Id 和 Meta_Info
        // D. 选中 Datetime 列
        
        var result = df.Select(
            // A. StartsWith + Math
            (Polars.Selectors.StartsWith("Price") * 100).Name.Suffix("_Cents"),

            // B. EndsWith
            Polars.Selectors.EndsWith("2024"),

            // C. Type Selector (Datetime)
            Polars.Datetime()
        );

        // 3. 验证结果
        Assert.Equal(4, result.Width); 
        // 预期列: Price_US_Cents, Price_EU_Cents, Qty_2024, Timestamp

        // 验证列名
        Assert.Contains("Price_US_Cents", result.ColumnNames);
        Assert.Contains("Qty_2024", result.ColumnNames);
        Assert.Contains("Timestamp", result.ColumnNames);
        
        // 验证排除 (Id 不应该在里面)
        Assert.DoesNotContain("Id", result.ColumnNames);

        // 验证计算
        Assert.Equal(1050.0, result[0, "Price_US_Cents"]); // 10.5 * 100
    }

    [Fact]
    public void Test_Selector_Set_Operations()
    {
        var df = DataFrame.FromColumns(new 
        {
            Num1 = new[] { 1 },
            Num2 = new[] { 2 },
            Str1 = new[] { "a" },
            Str2 = new[] { "b" }
        });

        // 逻辑: (所有数字) - (Num1)
        // 应该只剩 Num2
        var sel = Polars.Numeric() - Polars.Selectors.Matches("^Num1$");
        
        var result = df.Select(sel);

        Assert.Equal(1, result.Width);
        Assert.Equal("Num2", result.Column(0).Name);
    }
}
