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

        int totalRows = 10000;
        var baseTime = DateTime.UtcNow.Date;

        using var df = DataFrame.FromColumns(new
        {
            OrderId = Enumerable.Range(0, totalRows).ToArray(),
            
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
        // SinkTo -> SqlBulkCopy)
        // ---------------------------------------------------------
        await Task.Run(() =>
        {
            var overrides = new Dictionary<string, Type>
            {
                { "OrderDate", typeof(DateTime) } 
            };

            df.Lazy().SinkTo(reader =>
            {
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
                    bulk.WriteToServer(reader);
                }
                catch (Exception ex)
                {
                    throw new Exception($"Bulk Copy Failed: {ex.Message}", ex);
                }

            }, bufferSize: 100, typeOverrides: overrides);
        });

        using (var conn = new SqlConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync();

            using var cmdCount = new SqlCommand($"SELECT COUNT(*) FROM {tableName}", conn);
            var count = Convert.ToInt32(await cmdCount.ExecuteScalarAsync());
            Assert.Equal(totalRows, count);

            using var cmdNullCheck = new SqlCommand(
                $"SELECT Region, Amount, OrderDate FROM {tableName} WHERE OrderId = 0", conn);
            
            using var reader = await cmdNullCheck.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync(), "Should find row with OrderId = 0");

            Assert.True(await reader.IsDBNullAsync(0), "Region should be NULL for ID 0"); 
            Assert.True(await reader.IsDBNullAsync(1), "Amount should be NULL for ID 0");
            Assert.True(await reader.IsDBNullAsync(2), "OrderDate should be NULL for ID 0");
            reader.Close();

            using var cmdValueCheck = new SqlCommand(
                $"SELECT Region, Amount, OrderDate FROM {tableName} WHERE OrderId = 1", conn);
            
            using var valResult = await cmdValueCheck.ExecuteReaderAsync();
            Assert.True(await valResult.ReadAsync(), "Should find row with OrderId = 1");

            Assert.Equal("US", valResult["Region"]);

            Assert.Equal(100.5, Convert.ToDouble(valResult["Amount"])); 
            Assert.Equal(baseTime, valResult["OrderDate"]);
        }
    }
    [Fact]
    [Trait("MSSQL","DataReader")]
    public async Task Test_RealSqlServer_ETL_EndToEnd_With_AsDataReader()
    {
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

        int totalRows = 10000;
        var baseTime = DateTime.UtcNow.Date;

        using var df = DataFrame.FromColumns(new
        {
            OrderId = Enumerable.Range(0, totalRows).ToArray(),
            Region = Enumerable.Range(0, totalRows).Select(i => i % 100 == 0 ? null : "US").ToArray(),
            Amount = Enumerable.Range(0, totalRows).Select(i => i % 50 == 0 ? (double?)null : 100.5).ToArray(),
            OrderDate = Enumerable.Range(0, totalRows).Select(i => i % 10 == 0 ? (DateTime?)null : baseTime).ToArray()
        });

        var overrides = new Dictionary<string, Type>
        {
            { "OrderDate", typeof(DateTime) } 
        };
        using (var testReader = df.AsDataReader(bufferSize: 10, typeOverrides: overrides) as PolarsDataReader)
        {
            Assert.NotNull(testReader);
            
            Assert.True(testReader.Read());
            Assert.Equal(0, testReader.GetFieldValue<int>(0));
            Assert.True(testReader.IsDBNull(1)); 
            Assert.Null(testReader.GetFieldValue<string?>(1)); 
            
            Assert.True(testReader.Read());
            Assert.Equal(1, testReader.GetFieldValue<int>(0));
            
            Assert.Equal("US", testReader.GetFieldValue<string>(1));
            Assert.Equal(100.5, testReader.GetFieldValue<double>(2));
            Assert.Equal(baseTime, testReader.GetFieldValue<DateTime>(3));

            var utf8Span = testReader.GetBytesSpan(1); 
            Assert.Equal(2, utf8Span.Length);
            Assert.Equal((byte)'U', utf8Span[0]);
            Assert.Equal((byte)'S', utf8Span[1]);
        }

        using var bulkReader = df.AsDataReader(bufferSize: 100, typeOverrides: overrides);

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
            await bulk.WriteToServerAsync(bulkReader);
        }
        catch (Exception ex)
        {
            throw new Exception($"Bulk Copy Failed: {ex.Message}", ex);
        }

        using (var conn = new SqlConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync();

            using var cmdCount = new SqlCommand($"SELECT COUNT(*) FROM {tableName}", conn);
            var count = Convert.ToInt32(await cmdCount.ExecuteScalarAsync());
            Assert.Equal(totalRows, count);

            using var cmdNullCheck = new SqlCommand($"SELECT Region, Amount, OrderDate FROM {tableName} WHERE OrderId = 0", conn);
            using var resultReader = await cmdNullCheck.ExecuteReaderAsync();
            Assert.True(await resultReader.ReadAsync(), "Should find row with OrderId = 0");

            Assert.True(await resultReader.IsDBNullAsync(0), "Region should be NULL for ID 0"); 
            Assert.True(await resultReader.IsDBNullAsync(1), "Amount should be NULL for ID 0");
            Assert.True(await resultReader.IsDBNullAsync(2), "OrderDate should be NULL for ID 0");
            resultReader.Close();

            using var cmdValueCheck = new SqlCommand($"SELECT Region, Amount, OrderDate FROM {tableName} WHERE OrderId = 1", conn);
            using var valResult = await cmdValueCheck.ExecuteReaderAsync();
            Assert.True(await valResult.ReadAsync(), "Should find row with OrderId = 1");

            Assert.Equal("US", valResult["Region"]);
            Assert.Equal(100.5, Convert.ToDouble(valResult["Amount"])); 
            Assert.Equal(baseTime, valResult["OrderDate"]);
        }
    }
}