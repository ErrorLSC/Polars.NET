using Apache.Arrow;
using Apache.Arrow.Memory;

namespace Polars.CSharp.Tests;

public class DataFrameTests
{
    [Fact]
    public void Test_ReadCsv_Filter_Select()
    {
        // 1. 准备一个临时 CSV 文件
        var csvContent = @"name,age,salary
Alice,25,50000
Bob,30,60000
Charlie,35,70000
David,40,80000";
        var fileName = "test_data.csv";
        File.WriteAllText(fileName, csvContent);

        try
        {
            // 2. 读取 CSV
            using var df = DataFrame.ReadCsv(fileName);
            
            // 验证加载正确
            Assert.Equal(4, df.Height);
            Assert.Equal(3, df.Width);
            Assert.Contains("name", df.Columns);

            // 3. 执行操作：筛选 age > 30 并选择 name 和 salary
            // SQL 逻辑: SELECT name, salary FROM df WHERE age > 30
            using var filtered = df
                .Filter(Polars.Col("age") > Polars.Lit(30))
                .Select(Polars.Col("name"), Polars.Col("salary"));

            // 验证结果
            // 应该剩下 Charlie (35) 和 David (40)
            Assert.Equal(2, filtered.Height);
            Assert.Equal(2, filtered.Width);

            // 4. 验证具体值 (通过 ToArrow 取回数据)
            using var batch = filtered.ToArrow();
            var nameCol = batch.Column("name");
            
            Assert.NotNull(nameCol);
            Assert.Equal("Charlie", nameCol.GetStringValue(0));
            Assert.Equal("David", nameCol.GetStringValue(1));
        }
        finally
        {
            if (File.Exists(fileName)) File.Delete(fileName);
        }
    }

    [Fact]
    public void Test_FromArrow_RoundTrip()
    {
        // 1. 手动构建一个 Arrow RecordBatch
        var builder = new RecordBatch.Builder(new NativeMemoryAllocator())
            .Append("id", false, col => col.Int32(array => array.AppendRange([1, 2, 3])))
            .Append("value", false, col => col.Double(array => array.AppendRange([1.1, 2.2, 3.3])));

        using var originalBatch = builder.Build();

        // 2. 转为 Polars DataFrame
        using var df = DataFrame.FromArrow(originalBatch);
        
        Assert.Equal(3, df.Height);
        Assert.Equal(2, df.Width);

        // 3. 做一些计算 (例如 value * 2)
        using var resultDf = df.Select(
            Polars.Col("id"), 
            (Polars.Col("value") * Polars.Lit(2.0)).Alias("value_doubled")
        );

        // 4. 转回 Arrow 验证
        using var resultBatch = resultDf.ToArrow();
        var doubledCol = resultBatch.Column("value_doubled") as DoubleArray;

        Assert.NotNull(doubledCol);
        Assert.Equal(2.2, doubledCol.GetValue(0).Value, 4);
        Assert.Equal(4.4, doubledCol.GetValue(1).Value, 4);
        Assert.Equal(6.6, doubledCol.GetValue(2).Value, 4);
    }
    
    [Fact]
    public void Test_GroupBy_Agg()
    {
         // 准备数据: Department, Salary
        var csvContent = @"dept,salary
IT,100
IT,200
HR,150
HR,50";
        var fileName = "groupby_test.csv";
        File.WriteAllText(fileName, csvContent);

        try
        {
            using var df = DataFrame.ReadCsv(fileName);

            // GroupBy dept, Agg Sum(salary)
            using var grouped = df
                .GroupBy(Polars.Col("dept"))
                .Agg(Polars.Col("salary").Sum().Alias("total_salary"))
                .Sort(Polars.Col("total_salary"), descending: true); // 排序方便断言

            // 预期: 
            // IT: 300
            // HR: 200
            
            Assert.Equal(2, grouped.Height);
            
            using var batch = grouped.ToArrow();
            var deptCol = batch.Column("dept");
            var salaryCol = batch.Column("total_salary"); // Polars Sum 整数通常返回 Int64

            Assert.Equal("IT", deptCol.GetStringValue(0));
            Assert.Equal(300, salaryCol.GetInt64Value(0));
            
            Assert.Equal("HR", deptCol.GetStringValue(1));
            Assert.Equal(200, salaryCol.GetInt64Value(1));
        }
        finally
        {
            if (File.Exists(fileName)) File.Delete(fileName);
        }
    }
    // ==========================================
    // Display Tests (Head & Show)
    // ==========================================
    [Fact]
    public void Test_Head_And_Show()
    {
        // 构造较多数据 (15行)
        // 0..14
        using var df = DataFrame.FromArrow(
            new RecordBatch.Builder(new NativeMemoryAllocator())
                .Append("id", false, col => col.Int32(arr => arr.AppendRange(Enumerable.Range(0, 15))))
                .Append("name", false, col => col.String(arr => arr.AppendRange(Enumerable.Range(0, 15).Select(i => $"User_{i}"))))
                .Build()
        );

        Assert.Equal(15, df.Height);

        // 1. Test Head/Tail
        using var headDf = df.Head(5);
        Assert.Equal(5, headDf.Height);
        
        using var batch = headDf.ToArrow();
        Assert.Equal(0, batch.Column("id").GetInt64Value(0));
        Assert.Equal(4, batch.Column("id").GetInt64Value(4));

        using var tailDf = df.Tail(5);
        Assert.Equal(5, tailDf.Height);
        using var tailBatch = tailDf.ToArrow();
        Assert.Equal(10, tailBatch.Column("id").GetInt64Value(0));
        Assert.Equal(14, tailBatch.Column("id").GetInt64Value(4));
        // 2. Test Show (No exception should be thrown)
        // 这会在控制台打印表格
        System.Console.WriteLine("\n--- Testing DataFrame.Show() output ---");
        df.Show(10); 
        
        // 测试小数据 Show
        headDf.Show();
        tailDf.Show();
    }
}