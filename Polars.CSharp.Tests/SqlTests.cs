using Xunit;
using Polars.CSharp;

namespace Polars.CSharp.Tests;

public class SqlTests
{
    [Fact]
    public void Test_Sql_Basic_Select_And_Filter()
    {
        // 1. 准备数据
        var data = new[]
        {
            new { Name = "Alice", Age = 25, Sales = 100.0 },
            new { Name = "Bob",   Age = 30, Sales = 200.0 },
            new { Name = "Charlie", Age = 35, Sales = 300.0 }
        };
        
        using var df = DataFrame.From(data);
        using var lf = df.Lazy();

        // 2. 创建 SQL Context
        using var ctx = new SqlContext();
        
        // 3. 注册表
        ctx.Register("people", lf);

        // 4. 执行 SQL
        // 语法：标准 SQL
        var query = "SELECT Name, Sales FROM people WHERE Age > 28 ORDER BY Sales DESC";
        
        using var resLf = ctx.Execute(query);
        using var resDf = resLf.Collect(); // SQL 返回的是 LazyFrame，需要 Collect

        // 5. 验证
        Assert.Equal(2, resDf.Height); // Bob, Charlie
        
        // Order By DESC -> Charlie First
        Assert.Equal("Charlie", resDf.Column("Name").GetValue<string>(0));
        Assert.Equal("Bob", resDf.Column("Name").GetValue<string>(1));
    }

    [Fact]
    public void Test_Sql_Group_By()
    {
        var data = new[]
        {
            new { Dept = "IT", Salary = 1000 },
            new { Dept = "IT", Salary = 2000 },
            new { Dept = "HR", Salary = 1500 }
        };
        
        using var df = DataFrame.From(data);
        using var ctx = new SqlContext();
        
        // 直接注册 DataFrame (测试重载方法)
        ctx.Register("employees", df);

        var query = @"
            SELECT Dept, SUM(Salary) as TotalSalary 
            FROM employees 
            GROUP BY Dept 
            ORDER BY TotalSalary";

        using var res = ctx.Execute(query).Collect();
        
        // HR: 1500, IT: 3000
        var deptCol = res.Column("Dept");
        var salaryCol = res.Column("TotalSalary"); // Polars SQL 会保留大小写或者转小写，视版本而定，通常是保持

        Assert.Equal("HR", deptCol.GetValue<string>(0));
        Assert.Equal(1500, salaryCol.GetValue<long>(0)); // Sum int -> int/long

        Assert.Equal("IT", deptCol.GetValue<string>(1));
        Assert.Equal(3000, salaryCol.GetValue<long>(1));
    }
}