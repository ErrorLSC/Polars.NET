namespace Polars.CSharp.Tests;

public class SqlTests
{
    [Fact]
    public void Test_Sql_Basic_Select_And_Filter()
    {
        var data = new[]
        {
            new { Name = "Alice", Age = 25, Sales = 100.0 },
            new { Name = "Bob",   Age = 30, Sales = 200.0 },
            new { Name = "Charlie", Age = 35, Sales = 300.0 }
        };
        
        using var df = DataFrame.From(data);
        using var lf = df.Lazy();

        using var ctx = new SqlContext();
        
        ctx.Register("people", lf);

        var query = "SELECT Name, Sales FROM people WHERE Age > 28 ORDER BY Sales DESC";
        
        using var resLf = ctx.Execute(query);
        using var resDf = resLf.Collect();
        
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
        
        ctx.Register("employees", df);

        var query = @"
            SELECT Dept, SUM(Salary) as TotalSalary 
            FROM employees 
            GROUP BY Dept 
            ORDER BY TotalSalary";

        using var res = ctx.Execute(query).Collect();
        
        // HR: 1500, IT: 3000
        var deptCol = res.Column("Dept");
        var salaryCol = res.Column("TotalSalary");

        Assert.Equal("HR", deptCol.GetValue<string>(0));
        Assert.Equal(1500, salaryCol.GetValue<long>(0)); // Sum int -> int/long

        Assert.Equal("IT", deptCol.GetValue<string>(1));
        Assert.Equal(3000, salaryCol.GetValue<long>(1));
    }
    [Fact]
    public void Test_Sql_GetTables_Register_And_Unregister()
    {
        var data = new[]
        {
            new { Id = 1, Name = "Alice" },
            new { Id = 2, Name = "Bob" }
        };
        
        using var df = DataFrame.FromRows(data);
        using var lf = df.Lazy();

        using var ctx = new SqlContext();
        
        var initialTables = ctx.GetTables();
        Assert.Empty(initialTables);

        ctx.Register("people", lf);
        ctx.Register("departments", lf);
        ctx.Register("company", lf);

        var tables = ctx.GetTables();
        Assert.Equal(3, tables.Length);
        Assert.Equal("company", tables[0]);
        Assert.Equal("departments", tables[1]);
        Assert.Equal("people", tables[2]);

        ctx.UnRegister("departments");

        var remainingTables = ctx.GetTables();
        Assert.Equal(2, remainingTables.Length);
        Assert.Equal("company", remainingTables[0]);
        Assert.Equal("people", remainingTables[1]);
    }
}