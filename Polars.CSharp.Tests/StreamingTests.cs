using System.Diagnostics;
using Apache.Arrow;
using Apache.Arrow.Types;
using static Polars.CSharp.Polars;
using Polars.NET.Core.Data;
using Xunit.Abstractions;

namespace Polars.CSharp.Tests;
public class StreamingTests(ITestOutputHelper output)
{
    private readonly ITestOutputHelper _output = output;

    private class BigDataPoco
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public double Value { get; set; }
    }

    private static IEnumerable<BigDataPoco> GenerateData_1(int count)
    {
        for (int i = 0; i < count; i++)
        {
            yield return new BigDataPoco
            {
                Id = i,
                Name = $"Row_{i}",
                Value = i * 0.1
            };
        }
    }

    [Fact]
    public void Test_FromArrowStream_Integration()
    {
        int totalRows = 500_000; 
        int batchSize = 100_000; 

        using var df = DataFrame.FromEnumerable(GenerateData_1(totalRows), batchSize);

        Assert.Equal(totalRows, df.Height);

        Assert.Equal(0, df.GetValue<int>(0, "Id"));
        Assert.Equal("Row_0", df.GetValue<string>(0, "Name"));

        long midIndex = 250_000;
        Assert.Equal((int)midIndex, df.GetValue<int>(midIndex, "Id"));
        Assert.Equal($"Row_{midIndex}", df.GetValue<string>(midIndex, "Name"));

        long lastIndex = totalRows - 1;
        Assert.Equal((int)lastIndex, df.GetValue<int>(lastIndex, "Id"));
    }

    private class StreamPoco
    {
        public int Id { get; set; }
        public string Group { get; set; }
        public double Value { get; set; }
    }

    private static IEnumerable<StreamPoco> GenerateData_2(int count)
    {
        for (int i = 0; i < count; i++)
        {
            yield return new StreamPoco
            {
                Id = i,
                Group = i % 2 == 0 ? "Even" : "Odd",
                Value = i * 1.5
            };
        }
    }
    [Fact]
    public void Test_Lazy_ScanArrowStream_EndToEnd()
    {
        int totalRows = 50000;
        int batchSize = 10000; 

        var lf = LazyFrame.ScanEnumerable(GenerateData_2(totalRows),null, batchSize);

        var q = lf
            .Filter(Col("Group") == Lit("Even"))
            .Select(
                Col("Id"),
                (Col("Value") * 2).Alias("DoubleValue")
            );

        using var df1 = q.Clone().Collect();

        Assert.Equal(totalRows / 2, df1.Height);

        Assert.Equal(0, df1.GetValue<int>(0, "Id"));
        Assert.Equal(0 * 1.5 * 2, df1.GetValue<double>(0, "DoubleValue")); // 0

        var lastIdx = df1.Height - 1;
        Assert.Equal(49998, df1.GetValue<int>(lastIdx, "Id"));
        
        using var df2 = q.Collect();
        Assert.Equal(df1.Height, df2.Height);
        Assert.Equal(df1.GetValue<int>(0, "Id"), df2.GetValue<int>(0, "Id"));
    }
    
    [Fact]
    public void Test_Lazy_Stream_Empty()
    {
        var lf = LazyFrame.ScanEnumerable(new List<StreamPoco>(), batchSize: 100);
        using var df = lf.Collect();

        Assert.Equal(0, df.Height);
        Assert.Contains("Id", df.ColumnNames);
    }
    [Fact]
    public void Test_EndToEnd_Streaming_Invincible()
    {
        static IEnumerable<BigDataPoco> InfiniteStream()
        {
            int limit = 1_000_000; 
            for (int i = 0; i < limit; i++)
            {
                yield return new BigDataPoco 
                { 
                    Id = i, 
                    Name = "IgnoreMe",
                    Value = i 
                };
            }
        }

        int batchSize = 50_000;

        var lf = LazyFrame.ScanEnumerable(InfiniteStream(),null, batchSize);

        var q = lf
            .Filter(Col("Id") > 999_998) 
            .Select(Col("Id"), Col("Value"));

        using var df = q.Collect();

        Assert.Equal(1, df.Height);
        Assert.Equal(999999, df.GetValue<int>(0, "Id"));
        
        Console.WriteLine("Streaming execution completed without OOM!");
    }
    private class BenchPoco
    {
        public int Id { get; set; }
        public string Category { get; set; } 
        public double Value { get; set; }
    }

    private static IEnumerable<BenchPoco> GenerateMassiveData(int count)
    {
        var catA = "Category_A"; 
        var catB = "Category_B"; 

        for (int i = 0; i < count; i++)
        {
            yield return new BenchPoco
            {
                Id = i,

                Category = (i % 2 == 0) ? catA : catB, 
                Value = 1.0 
            };
        }
    }
    [Fact(Skip ="Too long")]
    [Trait("Stream", "StressTest")] 
    public async Task Test_100_Million_Rows_StreamingAsync()
    {

        int totalRows = 100_000_000; 
        int batchSize = 500_000;     
        
        // Pre heat GC
        GC.Collect();
        GC.WaitForPendingFinalizers();

        using var cts = new CancellationTokenSource();
        long peakPhysicalMemory = 0;
        long peakManagedMemory = 0;

        var proc = Process.GetCurrentProcess();

        var monitorTask = Task.Run(async () => 
        {
            while (!cts.IsCancellationRequested)
            {
                proc.Refresh();
                long currentPhysical = proc.PrivateMemorySize64;
                
                long currentManaged = GC.GetTotalMemory(false);

                if (currentPhysical > peakPhysicalMemory) peakPhysicalMemory = currentPhysical;
                if (currentManaged > peakManagedMemory) peakManagedMemory = currentManaged;

                // Console.WriteLine($"[Monitor] Phys: {currentPhysical/1024/1024} MB | Man: {currentManaged/1024/1024} MB");

                try { await Task.Delay(100, cts.Token); } catch { break; }
            }
        });

        _output.WriteLine($"[Start] Baseline Physical: {proc.PrivateMemorySize64 / 1024 / 1024} MB");

        var sw = Stopwatch.StartNew();

        // ====================================================
        // Core Logic
        // ====================================================
        try 
        {
            using var lf = LazyFrame.ScanEnumerable(
                    GenerateMassiveData(totalRows), 
                    batchSize: batchSize, 
                    useBuffered: true
                );
            var q = lf
                .Filter(Col("Category") == Lit("Category_A"))
                .Select(
                    Col("Id").Sum().Alias("SumId"),
                    (Col("Value") * 2).Sum().Alias("SumValue"),
                    Col("Id").Count().Alias("Count")
                );

            using var df = q.Collect(useStreaming:true);
            
            sw.Stop();
            
            Assert.Equal(1, df.Height);
            long expectedCount = totalRows / 2;
            Assert.Equal(expectedCount, df.GetValue<long>(0, "Count"));
            double expectedSumValue = expectedCount * 2.0;
            Assert.Equal(expectedSumValue, df.GetValue<double>(0, "SumValue"));
        }
        finally
        {
            cts.Cancel();
            try {await monitorTask;} catch {} 
        }

        proc.Refresh();
        long endPhysical = proc.PrivateMemorySize64;
        if (endPhysical > peakPhysicalMemory) peakPhysicalMemory = endPhysical;
        
        Console.WriteLine("--------------------------------------------------");
        Console.WriteLine($"[Result] Processed {totalRows:N0} rows");
        Console.WriteLine($"[Time]   Total Time: {sw.Elapsed.TotalSeconds:F2} s");
        Console.WriteLine($"[Speed]  Throughput: {totalRows / sw.Elapsed.TotalSeconds:N0} rows/sec");
        Console.WriteLine("--------------------------------------------------");
        Console.WriteLine($"[Memory] Peak Physical (Total): {peakPhysicalMemory / 1024 / 1024} MB");
        Console.WriteLine($"[Memory] Peak Managed  (C#):    {peakManagedMemory / 1024 / 1024} MB");
        Console.WriteLine($"[Memory] End Physical:          {endPhysical / 1024 / 1024} MB");
        Console.WriteLine("--------------------------------------------------");

        Assert.True(peakPhysicalMemory < 4L * 1024 * 1024 * 1024, 
            $"Memory Leak Detected! Peak memory usage ({peakPhysicalMemory/1024/1024} MB) exceeded 4GB limit.");
    }
    [Fact]
    public void Test_ArrowToDbStream_EndToEnd()
    {
        var today = DateOnly.FromDateTime(DateTime.Now); 

        var schema = new Schema.Builder()
            .Field(new Field("Id", Int32Type.Default, true))
            .Field(new Field("Name", StringViewType.Default, true))
            .Field(new Field("Date", Date32Type.Default, true)) 
            .Build();

        IEnumerable<RecordBatch> MockArrowStream()
        {
            // Batch 1: [1, "Alice", today]
            var dtOffset = new DateTimeOffset(today.ToDateTime(TimeOnly.MinValue), TimeSpan.Zero);
            
            yield return new RecordBatch(schema, [
                new Int32Array.Builder().Append(1).Build(),
                new StringViewArray.Builder().Append("Alice").Build(),
                new Date32Array.Builder().Append(dtOffset).Build() 
            ], 1);

            // Batch 2: [2, "Bob", null]
            yield return new RecordBatch(schema, [
                new Int32Array.Builder().Append(2).Build(),
                new StringViewArray.Builder().Append("Bob").Build(),
                new Date32Array.Builder().AppendNull().Build()
            ], 1);
        }

        using var dbReader = new ArrowToDbStream(MockArrowStream());

        var targetTable = new System.Data.DataTable();
        targetTable.Load(dbReader);

        Assert.Equal(2, targetTable.Rows.Count);
        
        // Row 1
        Assert.Equal(1, targetTable.Rows[0]["Id"]);
        Assert.Equal("Alice", targetTable.Rows[0]["Name"]);
        
        var actualDate = targetTable.Rows[0]["Date"];
        Assert.IsType<DateOnly>(actualDate);
        Assert.Equal(today, (DateOnly)actualDate);

        // Row 2
        Assert.Equal(2, targetTable.Rows[1]["Id"]);
        Assert.Equal(DBNull.Value, targetTable.Rows[1]["Date"]);
    }
    [Fact]
    public void Test_SinkTo_Generic_EndToEnd()
    {

        int totalRows = 50_000;
        
        var df = DataFrame.FromColumns(new 
        {
            Id = Enumerable.Range(0, totalRows).ToArray(),
            Value = Enumerable.Repeat("test_val", totalRows).ToArray()
        });

        var targetTable = new System.Data.DataTable();

        df.Lazy().SinkTo(reader => 
        {
            Console.WriteLine("[MockDB] Start Bulk Insert...");

            targetTable.Load(reader);
            
            Console.WriteLine($"[MockDB] Inserted {targetTable.Rows.Count} rows.");
        });

        Assert.Equal(totalRows, targetTable.Rows.Count);
        
        Assert.Equal(0, targetTable.Rows[0]["Id"]);
        Assert.Equal("test_val", targetTable.Rows[0]["Value"]);
        Assert.Equal(totalRows - 1, targetTable.Rows[totalRows - 1]["Id"]);
    }
    [Fact]
    [Trait("Stream","E2E")]
    public void Test_ETL_Stream_EndToEnd()
    {
        int totalRows = 100_000;
        
        // ---------------------------------------------------------
        // Extract
        // ---------------------------------------------------------
        var sourceTable = new System.Data.DataTable();
        sourceTable.Columns.Add("OrderId", typeof(int));
        sourceTable.Columns.Add("Region", typeof(string));
        sourceTable.Columns.Add("Amount", typeof(double));
        sourceTable.Columns.Add("OrderDate", typeof(DateTime));

        var baseDate = DateTime.Now.Date.AddHours(12);
        for (int i = 0; i < totalRows; i++)
        {
            string region = (i % 2 == 0) ? "US" : "EU";
            sourceTable.Rows.Add(i, region, i * 1.5, baseDate.AddDays(i % 10)); 
        }

        using var sourceReader = sourceTable.CreateDataReader();

        // ---------------------------------------------------------
        // Transform
        // ---------------------------------------------------------

        var lf = LazyFrame.ScanDatabase(sourceReader,50000);

        var pipeline = lf
            .Filter(Col("Region") == Lit("US"))
            .WithColumns((Col("Amount") * 1.08).Alias("TaxedAmount"))
            .Select(Col("OrderId"),Col("TaxedAmount"),
                    Col("OrderDate"));

        // ---------------------------------------------------------
        // Load
        // ---------------------------------------------------------
        
        var targetTable = new System.Data.DataTable(); 

        var schemaContract = new Dictionary<string, Type>
        {
            { "OrderDate", typeof(DateTime) }
        };
        Console.WriteLine("[ETL] Starting Pipeline...");
        var sw = Stopwatch.StartNew();

        pipeline.SinkTo(reader => 
        {

            int dateColIndex = reader.GetOrdinal("OrderDate");
            Assert.Equal(typeof(DateTime), reader.GetFieldType(dateColIndex));
            
            targetTable.Load(reader);

        }, bufferSize: 5, typeOverrides: schemaContract);

        sw.Stop();
        Console.WriteLine($"[ETL] Completed in {sw.Elapsed.TotalSeconds:F3}s. Rows written: {targetTable.Rows.Count}");

        // ---------------------------------------------------------
        // Verify
        // ---------------------------------------------------------
        
        Assert.Equal(totalRows / 2, targetTable.Rows.Count);

        // 0 * 1.5 * 1.08 = 0
        Assert.Equal(0, targetTable.Rows[0]["OrderId"]);
        Assert.Equal(0.0, (double)targetTable.Rows[0]["TaxedAmount"], 4);
        Assert.Equal(baseDate,targetTable.Rows[0]["OrderDate"]);     

        // 99998 * 1.5 * 1.08 = 161996.76
        int lastId = 99998;
        Assert.Equal(lastId, targetTable.Rows[^1]["OrderId"]);
        
        double expectedAmount = lastId * 1.5 * 1.08;
        double actualAmount = (double)targetTable.Rows[^1]["TaxedAmount"];
        Assert.Equal(expectedAmount, actualAmount, 0.001); 

        // 验证列名 (确保 Select 生效)
        Assert.True(targetTable.Columns.Contains("TaxedAmount"));
        Assert.False(targetTable.Columns.Contains("Amount"));
        Assert.False(targetTable.Columns.Contains("Region"));
    }
    [Fact]
    public void Test_ScanDatabase_Factory_Reusability()
    {
        var table = new System.Data.DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Rows.Add(1);
        table.Rows.Add(2);

        System.Data.IDataReader factory() => table.CreateDataReader();

        var lf1 = LazyFrame.ScanDatabase(factory);
        var lf2 = lf1.Clone();

        var df1 = lf1.Collect();
        Assert.Equal(2, df1.Height);
        Assert.Equal(1, df1[0, "Id"]);
        
        var df2 = lf2.Collect();
        Assert.Equal(2, df2.Height);
        Assert.Equal(2, df2[1, "Id"]);
    }
    [Fact]
    public void Test_1_Extract_CSharpToPolars_NewTypes()
    {

        int totalRows = 10; 
        var sourceTable = new System.Data.DataTable();
        sourceTable.Columns.Add("Id", typeof(int));
        sourceTable.Columns.Add("TimeVal", typeof(TimeOnly));  
        sourceTable.Columns.Add("TinyInt", typeof(sbyte));     
        sourceTable.Columns.Add("UnsignedBig", typeof(ulong)); 

        var startInfo = new TimeOnly(8, 0, 0); 

        for (int i = 0; i < totalRows; i++)
        {
            if (i == 5) 
            {
                sourceTable.Rows.Add(i, DBNull.Value, DBNull.Value, DBNull.Value);
                continue;
            }
            var time = startInfo.Add(TimeSpan.FromMinutes(i)).Add(TimeSpan.FromMilliseconds(i)); 
            sbyte tiny = (sbyte)(i * 10 - 50); 
            ulong ubig = long.MaxValue + (ulong)i; 
            
            sourceTable.Rows.Add(i, time, tiny, ubig);
        }

        using var sourceReader = sourceTable.CreateDataReader();
        
        using var lf = LazyFrame.ScanDatabase(sourceReader, batchSize: 2);
        using var df = lf.Collect();

        Assert.Equal(10, df.Height);
        
    }
    [Fact]
    [Trait("Stream","NewType")]
    public void Test_2_Load_PolarsToCSharp_NewTypes()
    {


        var sourceTable = new System.Data.DataTable();
        sourceTable.Columns.Add("Id", typeof(int));
        sourceTable.Columns.Add("TimeVal", typeof(TimeOnly));
        sourceTable.Columns.Add("UnsignedBig", typeof(ulong));

        sourceTable.Columns.Add("TimeSpanVal", typeof(TimeSpan));
        sourceTable.Columns.Add("DecimalVal", typeof(decimal));
        sourceTable.Columns.Add("DateOnlyVal", typeof(DateOnly));
        

        var sampleTimeOnly = new TimeOnly(8, 0, 0);
        var sampleTimeSpan = new TimeSpan(1, 30, 45); 
        var sampleDecimal = 123456.789m;
        var sampleDateOnly = new DateOnly(2024, 5, 20); 

        sourceTable.Rows.Add(
            1, 
            sampleTimeOnly, 
            (ulong)long.MaxValue,
            sampleTimeSpan,
            sampleDecimal,
            sampleDateOnly
        );
        
        sourceTable.Rows.Add(2, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value);

        using var sourceReader = sourceTable.CreateDataReader();
        using var sourceReader2 = sourceTable.CreateDataReader();
        var df = DataFrame.ReadDatabase(sourceReader2);
        df.Show();
        using var lf = LazyFrame.ScanDatabase(sourceReader, batchSize: 2);

        var targetTable = new System.Data.DataTable();

        var typeOverrides = new Dictionary<string, Type>
        {
            { "TimeVal", typeof(TimeOnly) }, 
            { "UnsignedBig", typeof(ulong) },
            { "TimeSpanVal", typeof(TimeSpan) },
            { "DecimalVal", typeof(decimal) },
            { "DateOnlyVal", typeof(DateOnly) }
        };

        lf.SinkTo(targetTable.Load, typeOverrides: typeOverrides); 

        Assert.Equal(2, targetTable.Rows.Count);

        var row0 = targetTable.Rows[0];
        
        Assert.Equal(sampleTimeOnly, Assert.IsType<TimeOnly>(row0["TimeVal"]));
        Assert.Equal((ulong)long.MaxValue, Assert.IsType<ulong>(row0["UnsignedBig"]));

        Assert.Equal(sampleTimeSpan, Assert.IsType<TimeSpan>(row0["TimeSpanVal"]));
        Assert.Equal(sampleDecimal, Assert.IsType<decimal>(row0["DecimalVal"]));
        Assert.Equal(sampleDateOnly, Assert.IsType<DateOnly>(row0["DateOnlyVal"]));

        var row1 = targetTable.Rows[1];
        Assert.Equal(DBNull.Value, row1["DateOnlyVal"]);

    }
    [Fact]
    [Trait("Stream", "Binary")]
    public void Test_3_BinaryType_Roundtrip()
    {

        var sourceTable = new System.Data.DataTable();
        sourceTable.Columns.Add("Id", typeof(int));
        sourceTable.Columns.Add("BlobVal", typeof(byte[]));
        
        byte[] sampleBytes = [0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01, 0xFF];
        byte[] emptyBytes = [];

        sourceTable.Rows.Add(1, sampleBytes);
        sourceTable.Rows.Add(2, emptyBytes);
        sourceTable.Rows.Add(3, DBNull.Value);

        using var sourceReader = sourceTable.CreateDataReader();
        using var lf = LazyFrame.ScanDatabase(sourceReader, batchSize: 2);

        var targetTable = new System.Data.DataTable();

        lf.SinkTo(targetTable.Load, typeOverrides: null); 

        Assert.Equal(3, targetTable.Rows.Count);

        var row0 = targetTable.Rows[0];
        var actualBytes = Assert.IsType<byte[]>(row0["BlobVal"]);
        Assert.Equal(sampleBytes, actualBytes); 

        var row1 = targetTable.Rows[1];
        var actualEmpty = Assert.IsType<byte[]>(row1["BlobVal"]);
        Assert.Empty(actualEmpty);

        var row2 = targetTable.Rows[2];
        Assert.Equal(DBNull.Value, row2["BlobVal"]);

    }
    [Fact]
    [Trait("Category", "Debug")]
    public void Test_4_Guid_FixedSizeBinary_Roundtrip()
    {

        var sourceTable = new System.Data.DataTable();
        sourceTable.Columns.Add("Id", typeof(int));
        sourceTable.Columns.Add("GuidVal", typeof(Guid));
        
        var sampleGuid = Guid.NewGuid(); // Standard Guid
        var emptyGuid = Guid.Empty;      // 00000000-0000-0000-0000-000000000000)

        sourceTable.Rows.Add(1, sampleGuid);
        sourceTable.Rows.Add(2, emptyGuid);
        sourceTable.Rows.Add(3, DBNull.Value);

        using var sourceReader = sourceTable.CreateDataReader();
        
        using var lf = LazyFrame.ScanDatabase(sourceReader, batchSize: 2);
  
        var targetTable = new System.Data.DataTable();

        var typeOverrides = new Dictionary<string, Type>
        {
            { "GuidVal", typeof(Guid) }
        };

        lf.SinkTo(targetTable.Load, typeOverrides: typeOverrides); 

        Assert.Equal(3, targetTable.Rows.Count);

        // Standard Guid
        var row0 = targetTable.Rows[0];
        var actualGuid = Assert.IsType<Guid>(row0["GuidVal"]);
        Assert.Equal(sampleGuid, actualGuid);

        // Empty Guid
        var row1 = targetTable.Rows[1];
        var actualEmpty = Assert.IsType<Guid>(row1["GuidVal"]);
        Assert.Equal(emptyGuid, actualEmpty);

        // Null
        var row2 = targetTable.Rows[2];
        Assert.Equal(DBNull.Value, row2["GuidVal"]);

    }
}
