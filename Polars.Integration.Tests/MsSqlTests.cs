using Microsoft.Data.SqlClient; 
using Polars.CSharp;           
using Polars.Integration.Tests.Fixtures;
using Polars.NET.Core.Data;

namespace Polars.Integration.Tests;

public class MsSqlTests : IClassFixture<MsSqlFixture>
{
    private readonly MsSqlFixture _fixture;

    public MsSqlTests(MsSqlFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    [Trait("MSSQL","E2E")]
    public async Task Test_RealSqlServer_ETL_EndToEnd_WithNulls()
    {
        // ---------------------------------------------------------
        // 1. 准备数据库环境 (DDL)
        // ---------------------------------------------------------
        // 随机表名，防止多次运行冲突
        var tableName = "Orders_" + Guid.NewGuid().ToString("N");
        
        // Region, Amount, OrderDate 默认允许 NULL
        var setupSql = $@"
            CREATE TABLE {tableName} (
                OrderId INT PRIMARY KEY,
                Region NVARCHAR(50) NULL,
                Amount FLOAT NULL,
                OrderDate DATETIME2 NULL
            );";

        using (var conn = new SqlConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync();
            using var cmd = new SqlCommand(setupSql, conn);
            await cmd.ExecuteNonQueryAsync();
        }

        // ---------------------------------------------------------
        // 2. 准备 Polars 数据 (Source)
        // ---------------------------------------------------------
        // 构造包含 Null 的数据，模拟真实脏数据场景
        int totalRows = 10000;
        var baseTime = DateTime.UtcNow.Date;

        // 构造数据生成逻辑：
        // - Region: 每 100 行插入一个 null
        // - Amount: 每 50 行插入一个 null
        // - OrderDate: 每 10 行插入一个 null (高频 null 测试)
        using var df = DataFrame.FromColumns(new
        {
            OrderId = Enumerable.Range(0, totalRows).ToArray(),
            
            // 注意：这里必须显式使用 string?[] 等可空数组类型
            Region = Enumerable.Range(0, totalRows)
                .Select(i => i % 100 == 0 ? null : "US")
                .ToArray(),
            
            Amount = Enumerable.Range(0, totalRows)
                .Select(i => i % 50 == 0 ? (double?)null : 100.5)
                .ToArray(),

            OrderDate = Enumerable.Range(0, totalRows)
                .Select(i => i % 10 == 0 ? (DateTime?)null : baseTime)
                .ToArray()
        });

        // ---------------------------------------------------------
        // 3. 执行 ETL (SinkTo -> SqlBulkCopy)
        // ---------------------------------------------------------
        // 将 Polars 数据流式写入 SQL Server
        await Task.Run(() =>
        {
            // 定义类型映射契约
            // 虽然是可空类型，但 Type 依然传基础类型，DataWriter 会自动处理 DBNull
            var overrides = new Dictionary<string, Type>
            {
                { "OrderDate", typeof(DateTime) } 
            };

            df.Lazy().SinkTo(reader =>
            {
                using var bulk = new SqlBulkCopy(_fixture.ConnectionString);
                bulk.DestinationTableName = tableName;
                
                // 开启流式写入配置（可选，提升大字段性能）
                bulk.EnableStreaming = true; 
                bulk.BatchSize = 2000;

                // 显式映射列名，防止顺序不一致
                bulk.ColumnMappings.Add("OrderId", "OrderId");
                bulk.ColumnMappings.Add("Region", "Region");
                bulk.ColumnMappings.Add("Amount", "Amount");
                bulk.ColumnMappings.Add("OrderDate", "OrderDate");

                try
                {
                    bulk.WriteToServer(reader);
                }
                catch (Exception ex)
                {
                    throw new Exception($"Bulk Copy Failed: {ex.Message}", ex);
                }

            }, bufferSize: 100, typeOverrides: overrides);
        });

        // ---------------------------------------------------------
        // 4. 验证 (Verify)
        // ---------------------------------------------------------
        using (var conn = new SqlConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync();

            // 4.1 验证总行数
            using var cmdCount = new SqlCommand($"SELECT COUNT(*) FROM {tableName}", conn);
            var count = Convert.ToInt32(await cmdCount.ExecuteScalarAsync());
            Assert.Equal(totalRows, count);

            // 4.2 验证 Null 写入情况
            // 检查 OrderId = 0 (它是 100, 50, 10 的公倍数，所以三个字段都应该是 NULL)
            using var cmdNullCheck = new SqlCommand(
                $"SELECT Region, Amount, OrderDate FROM {tableName} WHERE OrderId = 0", conn);
            
            using var reader = await cmdNullCheck.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync(), "Should find row with OrderId = 0");

            // 断言数据库里真的是 DBNull
            Assert.True(await reader.IsDBNullAsync(0), "Region should be NULL for ID 0"); 
            Assert.True(await reader.IsDBNullAsync(1), "Amount should be NULL for ID 0");
            Assert.True(await reader.IsDBNullAsync(2), "OrderDate should be NULL for ID 0");
            reader.Close();

            // 4.3 验证非 Null 写入情况
            // 检查 OrderId = 1 (应该都有值)
            using var cmdValueCheck = new SqlCommand(
                $"SELECT Region, Amount, OrderDate FROM {tableName} WHERE OrderId = 1", conn);
            
            using var valResult = await cmdValueCheck.ExecuteReaderAsync();
            Assert.True(await valResult.ReadAsync(), "Should find row with OrderId = 1");

            Assert.Equal("US", valResult["Region"]);
            // float 对比时注意精度，这里数据是简单的 100.5，通常相等
            Assert.Equal(100.5, Convert.ToDouble(valResult["Amount"])); 
            Assert.Equal(baseTime, valResult["OrderDate"]);
        }
    }
    [Fact]
    [Trait("MSSQL","DataReader")]
    public async Task Test_RealSqlServer_ETL_EndToEnd_With_AsDataReader()
    {
        // ---------------------------------------------------------
        // 1. 准备数据库环境 (DDL)
        // ---------------------------------------------------------
        var tableName = "Orders_" + Guid.NewGuid().ToString("N");
        
        var setupSql = $@"
            CREATE TABLE {tableName} (
                OrderId INT PRIMARY KEY,
                Region NVARCHAR(50) NULL,
                Amount FLOAT NULL,
                OrderDate DATETIME2 NULL
            );";

        using (var conn = new SqlConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync();
            using var cmd = new SqlCommand(setupSql, conn);
            await cmd.ExecuteNonQueryAsync();
        }

        // ---------------------------------------------------------
        // 2. 准备 Polars 数据 (Source)
        // ---------------------------------------------------------
        int totalRows = 10000;
        var baseTime = DateTime.UtcNow.Date;

        using var df = DataFrame.FromColumns(new
        {
            OrderId = Enumerable.Range(0, totalRows).ToArray(),
            Region = Enumerable.Range(0, totalRows).Select(i => i % 100 == 0 ? null : "US").ToArray(),
            Amount = Enumerable.Range(0, totalRows).Select(i => i % 50 == 0 ? (double?)null : 100.5).ToArray(),
            OrderDate = Enumerable.Range(0, totalRows).Select(i => i % 10 == 0 ? (DateTime?)null : baseTime).ToArray()
        });

        // ---------------------------------------------------------
        // 3. 执行 ETL (🌟 AsDataReader 拉取模式 -> SqlBulkCopy 🌟)
        // ---------------------------------------------------------
        var overrides = new Dictionary<string, Type>
        {
            { "OrderDate", typeof(DateTime) } 
        };
        using (var testReader = df.AsDataReader(bufferSize: 10, typeOverrides: overrides) as PolarsDataReader)
        {
            Assert.NotNull(testReader);
            
            // 👉 读第一行 (OrderId = 0, 后三列全是 Null)
            Assert.True(testReader.Read());
            Assert.Equal(0, testReader.GetFieldValue<int>(0)); // 测试值类型
            Assert.True(testReader.IsDBNull(1)); 
            Assert.Null(testReader.GetFieldValue<string?>(1)); // 测试可空兜底
            
            // 👉 读第二行 (OrderId = 1, 有数据)
            Assert.True(testReader.Read());
            Assert.Equal(1, testReader.GetFieldValue<int>(0));
            
            // 🌟 测试零装箱强类型通道
            Assert.Equal("US", testReader.GetFieldValue<string>(1));
            Assert.Equal(100.5, testReader.GetFieldValue<double>(2));
            Assert.Equal(baseTime, testReader.GetFieldValue<DateTime>(3));

            // 🌟🌟 测试极客专属的“零拷贝” Span 后门！(测试 Region 列的 "US")
            var utf8Span = testReader.GetBytesSpan(1); 
            Assert.Equal(2, utf8Span.Length);
            Assert.Equal((byte)'U', utf8Span[0]);
            Assert.Equal((byte)'S', utf8Span[1]);
        }

        // 🔥 核心替换：一行代码直接拿到流式 Reader！
        // 后台生产者线程已经自动启动，开始源源不断地把 Arrow Chunk 塞进通道
        using var bulkReader = df.AsDataReader(bufferSize: 100, typeOverrides: overrides);

        // 纯平的代码结构，告别 Lambda 嵌套地狱！
        using var bulk = new SqlBulkCopy(_fixture.ConnectionString);
        bulk.DestinationTableName = tableName;
        bulk.EnableStreaming = true; 
        bulk.BatchSize = 2000;

        bulk.ColumnMappings.Add("OrderId", "OrderId");
        bulk.ColumnMappings.Add("Region", "Region");
        bulk.ColumnMappings.Add("Amount", "Amount");
        bulk.ColumnMappings.Add("OrderDate", "OrderDate");

        try
        {
            // 🔥 直接 await 原生的 WriteToServerAsync！
            // 主线程只负责网络 I/O 等待，而后台 Polars 线程在疯狂计算和推数据
            await bulk.WriteToServerAsync(bulkReader);
        }
        catch (Exception ex)
        {
            throw new Exception($"Bulk Copy Failed: {ex.Message}", ex);
        }

        // ---------------------------------------------------------
        // 4. 验证 (Verify) - 与原版完全一致
        // ---------------------------------------------------------
        using (var conn = new SqlConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync();

            // 4.1 验证总行数
            using var cmdCount = new SqlCommand($"SELECT COUNT(*) FROM {tableName}", conn);
            var count = Convert.ToInt32(await cmdCount.ExecuteScalarAsync());
            Assert.Equal(totalRows, count);

            // 4.2 验证 Null 写入情况
            using var cmdNullCheck = new SqlCommand($"SELECT Region, Amount, OrderDate FROM {tableName} WHERE OrderId = 0", conn);
            using var resultReader = await cmdNullCheck.ExecuteReaderAsync();
            Assert.True(await resultReader.ReadAsync(), "Should find row with OrderId = 0");

            Assert.True(await resultReader.IsDBNullAsync(0), "Region should be NULL for ID 0"); 
            Assert.True(await resultReader.IsDBNullAsync(1), "Amount should be NULL for ID 0");
            Assert.True(await resultReader.IsDBNullAsync(2), "OrderDate should be NULL for ID 0");
            resultReader.Close();

            // 4.3 验证非 Null 写入情况
            using var cmdValueCheck = new SqlCommand($"SELECT Region, Amount, OrderDate FROM {tableName} WHERE OrderId = 1", conn);
            using var valResult = await cmdValueCheck.ExecuteReaderAsync();
            Assert.True(await valResult.ReadAsync(), "Should find row with OrderId = 1");

            Assert.Equal("US", valResult["Region"]);
            Assert.Equal(100.5, Convert.ToDouble(valResult["Amount"])); 
            Assert.Equal(baseTime, valResult["OrderDate"]);
        }
    }
}