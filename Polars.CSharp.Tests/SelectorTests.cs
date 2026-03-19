using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public class SelectorTests
{
    [Fact]
    public void Test_Selector_All_Exclude()
    {
        // Prepare Data
        var df = DataFrame.FromColumns(new 
        {
            Id = new[] { 1, 2 },
            Name = new[] { "Alice", "Bob" },
            Age = new[] { 25, 30 },
            Secret = new[] { "pass1", "pass2" }
        });

        // Use Selector: All().Exclude(...)
        // Scene：Keep data but exclude ID and Secret
        // Here implictly: Selector -> Expr
        var result = df.Select(
            Selectors.All().Exclude("Id", "Secret")
        );

        // Assert Results
        Assert.Equal(2, result.Width);
        
        // Only Name and Age column left
        Assert.Equal("Name", result.Column(0).Name);
        Assert.Equal("Age", result.Column(1).Name);
        
        // Id and Secret are not there
        Assert.Throws<ArgumentException>(() => result["Id"]);
        Assert.Throws<ArgumentException>(() => result["Secret"]);
    }

    [Fact]
    public void Test_Selector_Operation()
    {
      
        var df = DataFrame.FromColumns(new 
        {
            Id = new[] { 1, 2 },
            Val1 = new[] { 10, 20 },
            Val2 = new[] { 100, 200 }
        });

        // select (All - "Id") * 2
        var result = df.Select(
            (Selectors.All().Exclude("Id") * 2).Name.Suffix("_Scaled") 
        );

        Assert.Equal(20, result[0, "Val1_Scaled"]); // 10 * 2
        Assert.Equal(200, result[0, "Val2_Scaled"]); // 100 * 2
        
        Assert.Throws<ArgumentException>(() => result["Id"]);
    }
    
    [Fact]
    public void Test_Selector_Full_Capabilities()
    {
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

        // Target：
        // A. Choose all columns name start with Price then convert to int
        // B. Choose all columns name end with 2024
        // C. Exclude Id and Meta_Info
        // D. Choose Datetime Column
        
        var result = df.Select(
            // A. StartsWith + Math
            (Selectors.StartsWith("Price") * 100).Name.Suffix("_Cents"),

            // B. EndsWith
            Selectors.EndsWith("2024"),

            // C. Type Selector (Datetime)
            Selectors.Datetime()
        );

        Assert.Equal(4, result.Width); 

        Assert.Contains("Price_US_Cents", result.ColumnNames);
        Assert.Contains("Qty_2024", result.ColumnNames);
        Assert.Contains("Timestamp", result.ColumnNames);
        
        Assert.DoesNotContain("Id", result.ColumnNames);

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

        var sel = Selectors.Numeric() - Selectors.Matches("^Num1$");
        
        var result = df.Select(sel);

        Assert.Equal(1, result.Width);
        Assert.Equal("Num2", result.Column(0).Name);
    }
}
