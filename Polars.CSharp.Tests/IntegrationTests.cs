using Microsoft.Data.SqlClient;
using Polars.CSharp.Tests.Fixtures;

namespace Polars.CSharp.Tests
{
    // 注入我们刚才写的 Fixture
    public class IntegrationTests : IClassFixture<MsSqlFixture>
    {
        private readonly MsSqlFixture _fixture;

        public IntegrationTests(MsSqlFixture fixture)
        {
            _fixture = fixture;
        }

        [Fact]
        public async Task Test_RealSqlServer_ETL_EndToEnd()
        {
            // 1. 准备数据库环境 (DDL)
            // 在容器里创建一张真实的表，强制定义 OrderDate 为 DATETIME2
            var tableName = "Orders_" + Guid.NewGuid().ToString("N");
            var setupSql = $@"
                CREATE TABLE {tableName} (
                    OrderId INT PRIMARY KEY,
                    Region NVARCHAR(50),
                    Amount FLOAT,
                    OrderDate DATETIME2 -- 重点：数据库里是强类型的
                );";

            using (var conn = new SqlConnection(_fixture.ConnectionString))
            {
                await conn.OpenAsync();
                using var cmd = new SqlCommand(setupSql, conn);
                await cmd.ExecuteNonQueryAsync();
            }

            // 2. 准备 Polars 数据 (Source)
            // 模拟 1万行数据
            int totalRows = 10000;
            var df = DataFrame.FromColumns(new 
            {
                OrderId = Enumerable.Range(0, totalRows).ToArray(),
                Region = Enumerable.Repeat("US", totalRows).ToArray(),
                Amount = Enumerable.Repeat(100.5, totalRows).ToArray(),
                // 构造日期 (注意：Polars 可能会把它退化成 long)
                OrderDate = Enumerable.Repeat(DateTime.UtcNow.Date, totalRows).ToArray() 
            });

            // 3. 执行 ETL (SinkTo)
            // 这一步是从 Polars 内存 -> ArrowToDbStream -> SqlBulkCopy -> Docker SQL Server
            await Task.Run(() => 
            {
                // 定义契约：强制把 OrderDate 当 DateTime 处理
                // 即使 Polars 传过来的是 long，ArrowToDbStream 也会帮我们要回来
                var overrides = new Dictionary<string, Type>
                {
                    { "OrderDate", typeof(DateTime) }
                };

                df.Lazy().SinkTo(reader => 
                {
                    using var bulk = new SqlBulkCopy(_fixture.ConnectionString);
                    bulk.DestinationTableName = tableName;
                    
                    // 必须配置 Mapping，因为 Arrow 流的列序可能和 DB 不一致（这里是一致的，但也建议写）
                    bulk.ColumnMappings.Add("OrderId", "OrderId");
                    bulk.ColumnMappings.Add("Region", "Region");
                    bulk.ColumnMappings.Add("Amount", "Amount");
                    bulk.ColumnMappings.Add("OrderDate", "OrderDate");

                    try 
                    {
                        // 见证奇迹的时刻
                        bulk.WriteToServer(reader);
                    }
                    catch(Exception ex)
                    {
                        // 方便调试：打印具体的转换错误
                        throw new Exception($"Bulk Copy Failed: {ex.Message}", ex);
                    }

                }, bufferSize: 50, typeOverrides: overrides);
            });

            // 4. 验证 (Verify)
            // 从真实数据库读回来检查
            using (var conn = new SqlConnection(_fixture.ConnectionString))
            {
                await conn.OpenAsync();
                using var cmd = new SqlCommand($"SELECT COUNT(*) FROM {tableName}", conn);
                var count = (int)await cmd.ExecuteScalarAsync();
                Assert.Equal(totalRows, count);

                // 验证日期类型是否正确写入
                using var cmd2 = new SqlCommand($"SELECT TOP 1 OrderDate FROM {tableName}", conn);
                var dbDate = await cmd2.ExecuteScalarAsync();
                
                Assert.IsType<DateTime>(dbDate); // SQL Server 返回的是 DateTime
                // 验证值 (允许一点点精度误差)
                Assert.Equal(DateTime.UtcNow.Date, ((DateTime)dbDate).Date);
            }
        }
    }
}