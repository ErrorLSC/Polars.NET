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
    [Fact]
    public void Test_Sql_GetTables_Register_And_Unregister()
    {
        // 1. 准备数据 (随意造点数据用于生成 LazyFrame)
        var data = new[]
        {
            new { Id = 1, Name = "Alice" },
            new { Id = 2, Name = "Bob" }
        };
        
        using var df = DataFrame.From(data);
        using var lf = df.Lazy();

        // 2. 创建 SQL Context
        using var ctx = new SqlContext();
        
        // 3. 初始状态验证：应该没有注册任何表
        var initialTables = ctx.GetTables();
        Assert.Empty(initialTables);

        // 4. 注册多张表
        // 我们故意先注册 "people" 再注册 "departments"
        ctx.Register("people", lf);
        ctx.Register("departments", lf);
        ctx.Register("company", lf);

        // 5. 验证 GetTables
        // 注意：由于 Polars 底层在 Rust 中调用了 tables.sort_unstable()，
        // 所以返回的数组必须是按字母字典序排列的。
        var tables = ctx.GetTables();
        Assert.Equal(3, tables.Length);
        Assert.Equal("company", tables[0]);
        Assert.Equal("departments", tables[1]);
        Assert.Equal("people", tables[2]);

        // 6. 反注册一张表
        ctx.UnRegister("departments");

        // 7. 再次验证 GetTables
        var remainingTables = ctx.GetTables();
        Assert.Equal(2, remainingTables.Length);
        Assert.Equal("company", remainingTables[0]);
        Assert.Equal("people", remainingTables[1]);
    }
}