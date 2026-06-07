using System.ComponentModel;
using System.Text;
using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public class IoTests
{
    [Fact]
    public void Test_ReadJson_File_Advanced_WithSink()
    {
        using var dfOrig = DataFrame.FromColumns(new
        {
            name = new[] { "Alice", "Bob" },
            age = new[] { 20, 30 }, // Int32
            extra = new[] { "junk", "junk" }
        });

        string tempStub = Path.GetTempFileName();
        File.Delete(tempStub);
        string path = tempStub + ".jsonl";

        try
        {
            dfOrig.Lazy().SinkNdJson(path);

            Assert.True(File.Exists(path));
            Assert.True(new FileInfo(path).Length > 0);


            using var schema = new PolarsSchema()
                .Add("age", DataType.Float64);

            using var df = DataFrame.ReadJson(
                path,
                columns: ["age"],       
                schema: schema,                 
                jsonFormat: JsonFormat.JsonLines,
                ignoreErrors: false
            );

            Assert.Equal(1, df.Width); 
            Assert.Equal("age", df.ColumnNames[0]);
            
            Assert.DoesNotContain("name", df.ColumnNames);

            Assert.Equal(DataType.Float64, df.Column("age").DataType);
            
            Assert.Equal(2, df.Height);
            Assert.Equal(20.0, df.GetValue<double>(0, "age"));
            Assert.Equal(30.0, df.GetValue<double>(1, "age"));
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }
    [Fact]
    public void Test_SinkAndReadJsonLines_Memory_Bytes()
    {

        var ndjsonContent = 
            "{\"id\": 1, \"val\": true}\n" +
            "{\"id\": 2, \"val\": false}\n";
        
        byte[] inputBuffer = Encoding.UTF8.GetBytes(ndjsonContent);

        using var originalDf = DataFrame.ReadJson(
            inputBuffer,
            jsonFormat: JsonFormat.JsonLines 
        );

        byte[] sinkBuffer = originalDf.Lazy().SinkJsonMemory();

        Assert.NotNull(sinkBuffer);
        Assert.True(sinkBuffer.Length > 0);

        using var readDf = DataFrame.ReadJson(
            sinkBuffer,
            jsonFormat: JsonFormat.JsonLines
        );

        Assert.Equal(2, readDf.Height);
        
        Assert.Equal(1, readDf.GetValue<int>(0, "id"));
        Assert.True(readDf.GetValue<bool>(0, "val"));
        
        Assert.Equal(2, readDf.GetValue<int>(1, "id"));
        Assert.False(readDf.GetValue<bool>(1, "val"));
    }
    [Fact]
    public void Test_WriteAndReadJson_Stream_And_Memory()
    {

        var jsonContent = @"[{""city"": ""New York""}, {""city"": ""London""}]";
        byte[] bytes = Encoding.UTF8.GetBytes(jsonContent);

        using var inputStream = new MemoryStream(bytes);
        using var originalDf = DataFrame.ReadJson(
            inputStream, 
            jsonFormat: JsonFormat.Json 
        );

        Assert.Equal(2, originalDf.Height);
        
        byte[] outputBytes = originalDf.WriteJsonMemory(JsonFormat.Json);

        Assert.NotNull(outputBytes);
        Assert.True(outputBytes.Length > 0);

        using var readStream = new MemoryStream(outputBytes);
        using var readDf = DataFrame.ReadJson(
            readStream, 
            jsonFormat: JsonFormat.Json
        );

        Assert.Equal(2, readDf.Height);
        Assert.Equal("New York", readDf.GetValue<string>(0, "city"));
        Assert.Equal("London", readDf.GetValue<string>(1, "city"));
    }

    [Fact]
    public void Test_Ndjson_Scan_Lazy_AllModes()
    {
        using var dfOrig = DataFrame.FromColumns(new
        {
            id = new[] { 1, 2, 3 },
            val = new[] { 100, 200, 300 }, 
            tag = new[] { "A", "B", "C" }
        });

        string tempStub = Path.GetTempFileName();
        File.Delete(tempStub);
        string path = tempStub + ".ndjson";

        try
        {
            dfOrig.WriteNdJson(path);

            Assert.True(File.Exists(path));
            Assert.True(new FileInfo(path).Length > 0);
            // File Mode
            {
                using var schema = new PolarsSchema()
                    .Add("val", DataType.Int32);

                using var lf = LazyFrame.ScanNdjson(
                    path, 
                    schema: schema,
                    nRows: 3 
                );
                
                using var df = lf.Collect();

                Assert.Equal(3, df.Height);
                
                Assert.Equal(DataType.Int32, df.Column("val").DataType);
                Assert.Equal(100, df.GetValue<int>(0, "val"));
                Assert.Equal("A", df.GetValue<string>(0, "tag"));
            }
            // Memory Mode
            {
                byte[] bytes = File.ReadAllBytes(path);

                using var lf = LazyFrame.ScanNdjson(bytes);
                using var df = lf.Collect();

                Assert.Equal(3, df.Height);
                
                Assert.Equal(DataType.Int64, df.Column("val").DataType);
                Assert.Equal(200, df.GetValue<long>(1, "val"));
                
                Assert.Equal("B", df.GetValue<string>(1, "tag"));
            }

            // Stream Mode
            {
                using var fs = File.OpenRead(path);

                using var lf = LazyFrame.ScanNdjson(fs);
                using var df = lf.Collect();

                Assert.Equal(3, df.Height);
                Assert.Equal(3, df.GetValue<long>(2, "id"));
                Assert.Equal("C", df.GetValue<string>(2, "tag"));
            }
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }
    [Fact]
    public void Test_ReadParquet_Advanced()
    {
        using var sId = new Series("id", [1, 2, 3, 4, 5]);
        using var sName = new Series("name", ["Alice", "Bob", null, "David", "Eve"]); 
        using var df = new DataFrame(sId, sName);

        using var f = new DisposableFile(".parquet");
        
        // ---------------------------------------------------------
        // Test Write Options: 
        // ---------------------------------------------------------
        // 1. ZSTD (Level 3)
        // 2. Statistics = true
        // 3. owGroupSize = 2
        df.WriteParquet(
            f.Path,
            compression: ParquetCompression.ZSTD,
            compressionLevel: 3,
            statistics: true,
            rowGroupSize: 2
        );

        Assert.True(File.Exists(f.Path));
        Assert.True(new FileInfo(f.Path).Length > 0);

        using var dfFull = DataFrame.ReadParquet(f.Path);
        Assert.Equal(5, dfFull.Height);
        Assert.Equal(2, dfFull.Width);
        Assert.Equal("Alice", dfFull.GetValue<string>(0, "name"));
        Assert.Null(dfFull.GetValue<string>(2, "name")); 


        using var dfPartial = DataFrame.ReadParquet(
            f.Path,
            columns: ["id"], 
            nRows: 3,                
            rowIndexName: "row_idx", 
            rowIndexOffset: 10   
        );
        Assert.Equal(3, dfPartial.Height); 
        Assert.Equal(2, dfPartial.Width);  

        
        Assert.True(dfPartial.ColumnNames.Contains("id"));
        Assert.False(dfPartial.ColumnNames.Contains("name"));

        Assert.Equal(10UL, dfPartial.GetValue<ulong>(0, "row_idx"));
        Assert.Equal(12UL, dfPartial.GetValue<ulong>(2, "row_idx"));
    }
    [Fact]
    [Trait("IO","ParquetMemory")]
    public void Test_ReadParquet_Memory_And_Stream()
    {
        using var dfOriginal = new DataFrame(
            new Series("timestamp", [DateTime.Now, DateTime.Now.AddSeconds(1)]),
            new Series("status", ["OK", "FAIL"])
        );

        byte[] parquetBytes = dfOriginal.Lazy().SinkParquetMemory();
        
        Assert.NotNull(parquetBytes);
        Assert.True(parquetBytes.Length > 0);
        // --- Case 1: Read from byte[] (Memory) ---
        using var dfFromBytes = DataFrame.ReadParquet(parquetBytes);
        
        Assert.Equal(2, dfFromBytes.Height);
        Assert.Equal("OK", dfFromBytes.GetValue<string>(0, "status"));
        Assert.Equal("FAIL", dfFromBytes.GetValue<string>(1, "status"));

        // --- Case 2: Read from Stream ---
        using var ms = new MemoryStream(parquetBytes);
        
        using var dfFromStream = DataFrame.ReadParquet(ms, nRows: 1);

        Assert.Equal(1, dfFromStream.Height); 
        Assert.Equal("OK", dfFromStream.GetValue<string>(0, "status"));
    }
    [Fact]
    public void Test_ScanParquet_File_Hive_Schema()
    {
        // ---------------------------------------------------
        // Hive: /data/category=sales/data.parquet
        // ---------------------------------------------------

        string baseDir = Path.Combine(Path.GetTempPath(), "polars_test_hive_" + Guid.NewGuid());
        string partitionDir = Path.Combine(baseDir, "category=sales");
        Directory.CreateDirectory(partitionDir);
        string filePath = Path.Combine(partitionDir, "data.parquet");

        try
        {
            using var dfRaw = DataFrame.FromColumns(new
            {
                id = new[] { 1, 2, 3 },
                amount = new[] { 100, 200, 300 } // Int32
            });
            dfRaw.WriteParquet(filePath);
            
            using var fileSchema = new PolarsSchema()
                .Add("id", DataType.Int32)
                .Add("amount", DataType.Int32); 

            using var hiveSchema = new PolarsSchema()
                .Add("category", DataType.Categorical());

            using var lf = LazyFrame.ScanParquet(
                path: Path.Combine(baseDir, "**/*.parquet"),
                glob: true,
                schema: fileSchema,               
                hivePartitioning: true,
                hivePartitionSchema: hiveSchema,  
                tryParseHiveDates: true
            );

            using var df = lf.Collect();

            Assert.Contains("category", df.ColumnNames);
            // Assert.Equal("sales", df.GetValue<string>(0, "category"));
            
            Assert.Equal(DataTypeKind.Categorical, df.Column("category").DataType.Kind);

            Assert.Equal(3, df.Height);
            Assert.Equal(100, df.GetValue<int>(0, "amount"));
        }
        finally
        {
            if (Directory.Exists(baseDir))
                Directory.Delete(baseDir, true);
        }
    }

    [Fact]
    public void Test_ScanParquet_Memory_WithSchema()
    {

        using var dfRaw = DataFrame.FromColumns(new
        {
            name = new[] { "Alice", "Bob" },
            score = new[] { 9.5, 8.0 }
        });

        string tmpPath = Path.GetTempFileName();
        byte[] parquetBytes;
        try
        {
            dfRaw.WriteParquet(tmpPath);
            parquetBytes = File.ReadAllBytes(tmpPath);
        }
        finally
        {
            File.Delete(tmpPath);
        }

        using var schema = new PolarsSchema()
            .Add("name", DataType.String)
            .Add("score", DataType.Float64);

        using var lf = LazyFrame.ScanParquet(
            parquetBytes, 
            schema: schema,
            tryParseHiveDates: false
        );

        using var df = lf.Collect();

        Assert.Equal(2, df.Height);
        Assert.Equal("Alice", df.GetValue<string>(0, "name"));
        Assert.Equal(9.5, df.GetValue<double>(0, "score"));
        
    }
    [Fact]
    public void Test_Ipc_Compression()
    {

        using var dfOriginal = new DataFrame(
            new Series("id", [1, 2, 3]),
            new Series("val", ["A", "B", "C"])
        );
        using var f = new DisposableFile(".ipc");

        dfOriginal.WriteIpc(
            f.Path, 
            compression: IpcCompression.LZ4
        );

        using var df = DataFrame.ReadIpc(f.Path);

        Assert.Equal(3, df.Height);
        Assert.Equal("val", df.ColumnNames[1]);
        Assert.Equal("B", df.GetValue<string>(1, "val"));
    }

    [Fact]
    public void Test_Ipc_Reader_Modes()
    {

        using var sId = new Series("id", [1, 2, 3, 4, 5]);
        using var sVal = new Series("val", ["A", "B", "C", "D", "E"]);
        using var sTs = new Series("ts", [
            new DateTime(2021,1,1), new DateTime(2022,1,1), new DateTime(2023,1,1),
            new DateTime(2024,1,1), new DateTime(2025,1,1)
        ]);

        using var dfOriginal = new DataFrame(sId, sVal, sTs);
        using var f = new DisposableFile(".ipc");

        dfOriginal.WriteIpc(
            f.Path, 
            compression: IpcCompression.None
        );

        // =================================================================
        // File Mode
        // =================================================================
        {
            using var df = DataFrame.ReadIpc(
                f.Path, 
                columns: ["id", "val"], 
                nRows: 3
            );

            Assert.Equal(3, df.Height);
            Assert.Equal(2, df.Width); 
            Assert.Equal(1, df.GetValue<int>(0, "id"));
            Assert.Equal("C", df.GetValue<string>(2, "val"));
        }

        // =================================================================
        // Memory Mode (Bytes)
        // =================================================================
        {
            byte[] bytes = File.ReadAllBytes(f.Path);
            using var df = DataFrame.ReadIpc(bytes);

            Assert.Equal(5, df.Height);
            Assert.Equal(new DateTime(2023,1,1), df.GetValue<DateTime>(2, "ts"));
        }

        // =================================================================
        // Stream Mode
        // =================================================================
        {
            using var stream = File.OpenRead(f.Path);
            using var df = DataFrame.ReadIpc(stream);

            Assert.Equal(5, df.Height);
            Assert.Equal("E", df.GetValue<string>(4, "val"));
        }
    }
    [Fact]
    public void Test_ScanIpc_Lazy_AllModes()
    {

        using var sId = new Series("id", [1, 2, 3, 4, 5]);
        using var sVal = new Series("val", ["A", "B", "C", "D", "E"]);
        using var dfOriginal = new DataFrame(sId, sVal);

        using var f = new DisposableFile(".ipc");
        dfOriginal.WriteIpc(f.Path);

        // =================================================================
        // File Mode (UnifiedScanArgs: nRows/PreSlice & RowIndex)
        // =================================================================
        {
            using var lf = LazyFrame.ScanIpc(
                f.Path, 
                nRows: 3, 
                rowIndexName: "idx_col"
            );
            
            using var df = lf.Collect();

            Assert.Equal(3, df.Height); 
            
            Assert.True(df.ColumnNames.Contains("idx_col"));
            Assert.Equal(0u, df.GetValue<uint>(0, "idx_col")); 

            Assert.Equal("C", df.GetValue<string>(2, "val"));
        }

        // =================================================================
        // Memory Mode (Bytes)
        // =================================================================
        {
            byte[] bytes = File.ReadAllBytes(f.Path);

            using var lf = LazyFrame.ScanIpc(bytes);
            
            using var df = lf.Filter(Col("id") > 3).Collect();

            Assert.Equal(2, df.Height); // id: 4, 5
            Assert.Equal(5, df.GetValue<int>(1, "id"));
        }

        // =================================================================
        // Stream Mode
        // =================================================================
        {
            using var stream = File.OpenRead(f.Path);

            using var lf = LazyFrame.ScanIpc(stream);
            using var df = lf.Collect();

            Assert.Equal(5, df.Height);
            Assert.Equal("E", df.GetValue<string>(4, "val"));
        }
    }
    [Fact]
    public void Test_Csv_Write_Advanced_RoundTrip()
    {

        using var df = DataFrame.FromColumns(new
        {
            Name = new[] { "Alice", "Bob", "Charlie" },

            Score = new[] { 99.123456, 88.56789, 77.0 }, 

            Date = new[] { new DateTime(2023, 1, 1), new DateTime(2023, 5, 20), new DateTime(2023, 12, 31) },

            Comment = new[] { "Good", null, "Excellent" } 
        });

        string tempStub = Path.GetTempFileName();
        File.Delete(tempStub); 
        string path = tempStub + ".csv";

        try
        {
            // ---------------------------------------------------
            // Full Sugar
            // ---------------------------------------------------
            df.WriteCsv(
                path,
                includeHeader: true,
                separator: '|',             
                quoteChar: '\'',             
                quoteStyle: QuoteStyle.NonNumeric, 
                nullValue: "VOID",          
                floatPrecision: 2,           
                datetimeFormat: "%Y/%m/%d",      
                lineTerminator: "\n"
            );


            string rawContent = File.ReadAllText(path);
            Console.WriteLine(rawContent);

            Assert.Contains("|", rawContent);

            Assert.Contains("'Name'|'Score'|'Date'|'Comment'", rawContent);

            Assert.Contains("VOID", rawContent);

            Assert.Contains("99.12", rawContent);
            Assert.DoesNotContain("99.123", rawContent);

            Assert.Contains("2023/01/01", rawContent);

            // ---------------------------------------------------
            // Round-Trip
            // ---------------------------------------------------
            using var dfRead = DataFrame.ReadCsv(
                path,
                separator: '|',         
                quoteChar: '\'',        
                nullValues: ["VOID"], 
                hasHeader: true,
                tryParseDates: true    
            );

            Assert.Equal(3, dfRead.Height);
            Assert.Equal(4, dfRead.Width);

            Assert.Equal(99.12, dfRead.GetValue<double>(0, "Score"));
            
            Assert.Null(dfRead.GetValue<string>(1, "Comment"));
            Assert.Equal("Good", dfRead.GetValue<string>(0, "Comment"));

            var dateVal = dfRead["Date"][0];
            Assert.Contains("2023", dateVal.ToString());
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }
    private class SinkTestPoco
    {
        public int Id { get; set; }
        public string Type { get; set; }
        public double Val { get; set; }
    }
    [Fact]
    public void Test_SinkParquet_Advanced()
    {

        using var df = DataFrame.FromColumns(new
        {
            Id = new[] { 1, 2, 3, 4, 5 },
            Name = new[] { "Alice", "Bob", "Charlie", "David", "Eve" }
        });

        string tempStub = Path.GetTempFileName();
        File.Delete(tempStub);
        string path = tempStub + ".parquet";

        try
        {
            // Lazy -> Sink 
            // - Compression: ZSTD 
            // - Statistics: true 
            // - RowGroupSize: 2 
            // - SyncOnClose: All 
            df.Lazy().SinkParquet(
                path,
                compression: ParquetCompression.ZSTD,
                compressionLevel: 3,
                statistics: true,
                rowGroupSize: 2,
                maintainOrder: true,
                syncOnClose: SyncOnClose.All
            );

            var fileInfo = new FileInfo(path);
            Assert.True(fileInfo.Exists);
            Assert.True(fileInfo.Length > 0);

            using var dfRead = LazyFrame.ScanParquet(path).Collect();
            
            Assert.Equal(5, dfRead.Height);
            Assert.Equal(2, dfRead.Width);
            
            Assert.Equal("Alice", dfRead.GetValue<string>(0, "Name"));
            Assert.Equal("Charlie", dfRead.GetValue<string>(2, "Name"));
            Assert.Equal(5, dfRead.GetValue<int>(4, "Id"));
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }
    [Fact]
    public void Test_Streaming_SinkParquet_EndToEnd()
    {
        int totalRows = 1_000_000;
        int batchSize = 100_000;
        string path = Path.Combine(Path.GetTempPath(), $"polars_stream_{Guid.NewGuid()}.parquet");

        try
        {
            IEnumerable<SinkTestPoco> GenerateData()
            {
                for (int i = 0; i < totalRows; i++)
                {
                    yield return new SinkTestPoco
                    { 
                        Id = i, 
                        Type = (i % 2 == 0) ? "A" : "B", 
                        Val = i * 0.1 
                    };
                }
            }

            var lf = LazyFrame.ScanEnumerable(GenerateData(), null,batchSize);

            var q = lf
                .Filter(Col("Type") == "A")
                .Select(Col("Id"), Col("Val"));

            Console.WriteLine("Starting Streaming Sink...");
            var sw = System.Diagnostics.Stopwatch.StartNew();

            q.SinkParquet(path);

            sw.Stop();
            Console.WriteLine($"Sink completed in {sw.Elapsed.TotalSeconds:F2}s");

            Assert.True(File.Exists(path));
            var fileInfo = new FileInfo(path);
            Console.WriteLine($"Parquet File Size: {fileInfo.Length / 1024.0 / 1024.0:F2} MB");
            
            using var dfCheck = DataFrame.ReadParquet(path);
            Assert.Equal(totalRows / 2, dfCheck.Height);
            
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }
    [Fact]
    public void Test_Streaming_SinkParquet_ComplexTypes_EndToEnd()
    {

        int totalRows = 100_000; 
        int batchSize = 10_000;
        string path = Path.Combine(Path.GetTempPath(), $"polars_complex_{Guid.NewGuid()}.parquet");

        try
        {
            IEnumerable<ComplexPoco> GenerateData()
            {
                for (int i = 0; i < totalRows; i++)
                {
                    yield return new ComplexPoco
                    {
                        Id = i,

                        Tags = [i, i * 2], 

                        Meta = new MetaInfo { Score = i * 0.5, Label = $"L_{i}" } 
                    };
                }
            }

            // ToArrowBatches -> ArrowConverter
            var lf = LazyFrame.ScanEnumerable(GenerateData(),null, batchSize);

            var q = lf.Filter(Col("Id") % Lit(2) == Lit(0));

            Console.WriteLine("Starting Complex Streaming Sink...");
            var sw = System.Diagnostics.Stopwatch.StartNew();

            q.SinkParquet(path);

            sw.Stop();
            Console.WriteLine($"Sink completed in {sw.Elapsed.TotalSeconds:F2}s");

            Assert.True(File.Exists(path));

            using var lfCheck = LazyFrame.ScanParquet(path);
            using var df = lfCheck.Collect();
            
            Assert.Equal(totalRows / 2, df.Height); 

            var tags = df.Column("Tags");
            Assert.Equal(DataTypeKind.List, tags.DataType.Kind);
            var row0Tags = tags.GetValue<List<int?>>(0); // Id=0: [0, 0]
            Assert.Equal(0, row0Tags[0]);
            
            var unnested = df.Unnest("Meta");
            Assert.True(unnested.ColumnNames.Contains("Score"));
            Assert.True(unnested.ColumnNames.Contains("Label"));
            Assert.Equal(0.0, unnested.GetValue<double>(0, "Score")); // Id=0
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }
    [Fact]
    public void Test_SinkIpc_Advanced_Options()
    {
        var df = new DataFrame(
            new Series("id", [1, 2, 3]),
            new Series("val", ["A", "B", "C"])
        );

        var lf = df.Lazy();

        using var f = new DisposableFile(".ipc");

        lf.SinkIpc(
            f.Path, 
            compression: IpcCompression.LZ4, 
            maintainOrder: true, 
            syncOnClose: SyncOnClose.All
        );

        using var dfRead = DataFrame.ReadIpc(f.Path);

        Assert.Equal(3, dfRead.Height);
        Assert.Equal("val", dfRead.ColumnNames[1]);
        Assert.Equal("B", dfRead.GetValue<string>(1, "val"));
        
        var fileInfo = new FileInfo(f.Path);
        Assert.True(fileInfo.Exists);
        Assert.True(fileInfo.Length > 0);
    }
    [Fact]
    public void Test_SinkMemoryIpc_Advanced_Options()
    {
        var df = new DataFrame(
            new Series("id", [1, 2, 3]),
            new Series("val", ["A", "B", "C"])
        );

        var lf = df.Lazy();

        byte[] ipcBytes = lf.SinkIpcMemory(
            compression: IpcCompression.LZ4, 
            maintainOrder: true
        );

        Assert.NotNull(ipcBytes);
        Assert.True(ipcBytes.Length > 0);

        using var f = new DisposableFile(".ipc");
        File.WriteAllBytes(f.Path, ipcBytes);

        using var dfRead = DataFrame.ReadIpc(f.Path);

        Assert.Equal(3, dfRead.Height);
        Assert.Equal(2, dfRead.Width);
        Assert.Equal("val", dfRead.ColumnNames[1]);
        Assert.Equal("B", dfRead.GetValue<string>(1, "val"));
        Assert.Equal(2, dfRead.GetValue<int>(1, "id"));
    }


    private class ComplexPoco
    {
        public int Id { get; set; }
        public int[] Tags { get; set; }   
        public MetaInfo Meta { get; set; } 
    }

    private class MetaInfo
    {
        public double Score { get; set; }
        public string Label { get; set; }
    }
    [Fact]  
    public void Test_ScanDataReader_Integration()
    {

        var table = new System.Data.DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("Name", typeof(string)); 
        table.Columns.Add("Value", typeof(double));
        table.Columns.Add("Date", typeof(DateTime)); 

        var now = DateTime.Now;
        for (int i = 0; i < 1000; i++)
        {
            table.Rows.Add(i, $"User_{i}", i * 0.5, now.AddSeconds(i));
        }

        using var reader = table.CreateDataReader();

        
        var lf = LazyFrame.ScanDatabase(reader, batchSize: 100);

        var q = lf.Select(Col("Id"), Col("Value")).Filter(Col("Id") > 500);

        using var df = q.Collect();
        
        Assert.Equal(499, df.Height);
    }
    [Fact]
    public void Test_ScanDataReader_Nested_Array()
    {
        var table = new System.Data.DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("Tags", typeof(int[])); 
        table.Columns.Add("Memo", typeof(string)); 

        // Row 1: [10, 20]
        table.Rows.Add(1, new int[] { 10, 20 }, "Row1");
        // Row 2: [30]
        table.Rows.Add(2, new int[] { 30 }, "Row2");
        // Row 3: [] (空数组)
        table.Rows.Add(3, new int[] { }, "Row3");
        // Row 4: null (空值)
        table.Rows.Add(4, DBNull.Value, "Row4");

        using var reader = table.CreateDataReader();

        var lf = LazyFrame.ScanDatabase(reader);

        using var df = lf.Collect();

        Assert.Equal(4, df.Height);
        
        var tagsSeries = df.Column("Tags");
        Assert.Equal(DataTypeKind.List, tagsSeries.DataType.Kind);

        var row1 = tagsSeries.GetValue<List<int?>>(0);
        
        Assert.Equal("Row1", df.GetValue<string>(0, "Memo"));
        
        var exploded = df.Explode("Tags");
        // 1(10), 1(20), 2(30), 3(null), 4(null)
        Assert.True(exploded.Height >= 3);
    }
    public class UserMeta
    {
        public int Level { get; set; }
        public double Score { get; set; }
    }

    [Fact]
    public void Test_ScanDataReader_Nested_Struct()
    {
        var table = new System.Data.DataTable();
        table.Columns.Add("Id", typeof(int));

        table.Columns.Add("Meta", typeof(UserMeta));

        table.Rows.Add(1, new UserMeta { Level = 5, Score = 99.5 });
        table.Rows.Add(2, new UserMeta { Level = 1, Score = 10.0 });
        table.Rows.Add(3, DBNull.Value); // Null Struct

        using var reader = table.CreateDataReader();

        var lf = LazyFrame.ScanDatabase(reader);

        using var df = lf.Collect();

        Assert.Equal(3, df.Height);
        
        var metaSeries = df.Column("Meta");
        Assert.Equal(DataTypeKind.Struct, metaSeries.DataType.Kind);

        var unnested = df.Unnest("Meta");
        
        Assert.True(unnested.ColumnNames.Contains("Level"));
        Assert.True(unnested.ColumnNames.Contains("Score"));
        
        Assert.Equal(5, unnested.GetValue<int>(0, "Level"));
        Assert.Equal(99.5, unnested.GetValue<double>(0, "Score"));

        Assert.Null(unnested.GetValue<int?>(2, "Level"));
    }
    [Fact]
    public void Test_DataFrame_FromDataReader_Eager_Nested()
    {
        var table = new System.Data.DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("Tags", typeof(int[])); // List
        table.Columns.Add("Meta", typeof(UserMeta)); // Struct

        table.Rows.Add(1, new int[] { 10, 20 }, new UserMeta { Level = 99, Score = 100.0 });
        table.Rows.Add(2, DBNull.Value, DBNull.Value);

        using var reader = table.CreateDataReader();

        using var df = DataFrame.ReadDatabase(reader);

        Assert.Equal(2, df.Height);

        var meta = df.Column("Meta");
        Assert.Equal(DataTypeKind.Struct, meta.DataType.Kind);
        
        var tags = df.Column("Tags");
        Assert.Equal(DataTypeKind.List, tags.DataType.Kind);

        var row1Tags = tags.GetValue<List<int?>>(0);
        Assert.Equal(10, row1Tags[0]);

        var unnested = df.Unnest("Meta");
        Assert.Equal(99, unnested.GetValue<int>(0, "Level"));
    }
    [Fact]
    public async Task Test_WriteTo_Generic_EndToEnd()
    {
        var df = DataFrame.FromColumns(new 
        {
            Id = new[] { 1, 2, 3 },

            Date = new DateTime?[] { DateTime.Now.Date, null, DateTime.Now.Date.AddDays(1) }
        });

        var targetTable = new System.Data.DataTable();


        await Task.Run(() => 
        {
            df.WriteTo(reader => 
            {
                targetTable.Load(reader);
            });
        });

        Assert.Equal(3, targetTable.Rows.Count);
        Assert.Equal(1, targetTable.Rows[0]["Id"]);
        Assert.NotNull(targetTable.Rows[0]["Date"]);
        
        Assert.Equal(DBNull.Value, targetTable.Rows[1]["Date"]);
    }
    public record Fruits(int Id, string Name,float Rate,DateOnly Date);
    [Fact]
    [Trait("IO","CSVSchema")]
    public void Test_ReadCsv_With_Explicit_Schema()
    {

        string csvContent = 
@"1,Apple,1.5,2023-01-01
2,Banana,3.7,2023-05-20
3,Cherry,,2023-10-10";

        string filePath = Path.GetTempFileName() + ".csv";
        File.WriteAllText(filePath, csvContent);

        try
        {
            using var explicitSchema = new PolarsSchema()
                .Add("Id", typeof(int))
                .Add("Name", typeof(string))
                .Add("Rate", typeof(float))
                .Add("Date", typeof(DateOnly));
 
            using var df = DataFrame.ReadCsv(filePath,hasHeader:false, schema: typeof(Fruits));
            Assert.Equal(explicitSchema,df.Schema);

            Assert.Equal(1, df["Rate"].NullCount);
        }
        finally
        {
            if (File.Exists(filePath))
                File.Delete(filePath);
        }
    }

    [Fact]
    public void Test_ScanCsv_With_Explicit_Schema()
    {
        string csvContent = "val\n100\n200";
        string filePath = Path.GetTempFileName() + ".csv";
        File.WriteAllText(filePath, csvContent);

        try
        {
            using var explicitSchema = new PolarsSchema()
                .Add("val", DataType.Float64); 

            using var lf = LazyFrame.ScanCsv(
                filePath, 
                dtypeOverride: explicitSchema,
                rowIndexName: "row_id",
                rowIndexOffset: 10      
            );
            
            var lfSchema = lf.Schema;

            Assert.Equal(DataType.Float64, lfSchema["val"]);

            Assert.Contains("row_id", lfSchema.ColumnNames);
            Assert.Equal(DataType.UInt32, lfSchema["row_id"]); 

            Console.WriteLine("[Test] LazyFrame Schema validated.");

            using var df = lf.Collect();
            
            Assert.Equal(DataTypeKind.Float64, df.Schema["val"].Kind);
            
            // "100" -> 100.0
            Assert.Equal(100.0, df["val"][0]); 

            Assert.Equal(10u, df["row_id"][0]); 
            Assert.Equal(11u, df["row_id"][1]);
        }
        finally
        {
            if (File.Exists(filePath))
                File.Delete(filePath);
        }
    }
    [Fact]
    public void Test_ReadCsv_AllOptions_EndToEnd()
    {

        string csvContent = 
@"# Metadata Line 1: Created by System X
# Metadata Line 2: Version 1.0
ID;ProductName;Weight;ReleaseDate
101;Quantum Gadget;1.55;2023-12-25
102;Hyper Widget;;2024-01-01";

        string filePath = Path.GetTempFileName();
        File.WriteAllText(filePath, csvContent);

        try
        {

            using var explicitSchema = new PolarsSchema()
                .Add("ID", DataType.Int32)      
                .Add("Weight", DataType.Float32) 
                .Add("ReleaseDate", DataType.Date); 

            using var df = DataFrame.ReadCsv(
                path: filePath,
                dtypeOverride: explicitSchema,    
                hasHeader: true,           
                separator: ';',            
                skipRows: 2,               
                tryParseDates: true        
            );

            Assert.Equal(2, df.Height); 
            Assert.Equal(4, df.Width);  

            Assert.Equal(DataType.Int32, df.Schema["ID"]);
            Assert.Equal(DataType.Float32, df.Schema["Weight"]);
            Assert.Equal(DataType.Date, df.Schema["ReleaseDate"]);
            Assert.Equal(DataType.String, df.Schema["ProductName"]); 

            
            // ID (Int32)
            Assert.Equal(101, df["ID"][0]); 
            
            // String
            Assert.Equal("Quantum Gadget", df["ProductName"][0]);
            
            // Weight (Float32)
            Assert.Equal(1.55f, (float)df["Weight"][0]!, 0.0001f);
            
            // Date
            var dateVal = df["ReleaseDate"][0];
            Assert.Equal(new DateOnly(2023, 12, 25), dateVal);

            Assert.Equal(1, df["Weight"].NullCount);
            Assert.Null(df["Weight"][1]);
        }
        finally
        {
            if (File.Exists(filePath)) 
                File.Delete(filePath);
        }
    }
    [Fact]
    public void ReadDatabase_Should_Handle_Decimal_Correctly()
    {
        var table = new System.Data.DataTable();
        table.Columns.Add("Product", typeof(string));
        table.Columns.Add("Price", typeof(decimal)); 

        table.Rows.Add("Laptop", 1234.56m);
        table.Rows.Add("Mouse", 99.99m);
        table.Rows.Add("Cable", 0.00m); 

        using var reader = table.CreateDataReader();

        using var df = DataFrame.ReadDatabase(reader);

        Console.WriteLine("=== Decimal Test Output ===");
        df.Show();

        var priceCol = df["Price"];
        Assert.Contains("decimal", priceCol.DataType.ToString());

        var val0 = priceCol[0];
        var val1 = priceCol[1];

        Assert.Equal(1234.56m, Convert.ToDecimal(val0));
        Assert.Equal(99.99m, Convert.ToDecimal(val1));
    }
    [Fact]
    public void Test_SinkCsvMemory_And_Scan()
    {
        using var dfOriginal = new DataFrame(
            new Series("id", [1L, 2L]), 
            new Series("name", ["Alice", "Bob"]),
            new Series("score", [99.5, 88.0]) 
        );

        byte[] csvBytes = dfOriginal.Lazy().SinkCsvMemory(includeHeader: true);

        Assert.NotNull(csvBytes);
        Assert.True(csvBytes.Length > 0);

        using var lf = LazyFrame.ScanCsv(
            csvBytes,
            hasHeader: true,
            rowIndexName: "row_idx" 
        );


        using var schema = lf.Schema;
        Assert.Equal(DataTypeKind.Int64, schema["id"].Kind);
        Assert.Equal(DataTypeKind.String, schema["name"].Kind);
        Assert.Equal(DataTypeKind.Float64, schema["score"].Kind);
        Assert.Equal(DataTypeKind.UInt32, schema["row_idx"].Kind);

        using var dfRead = lf.Collect();
        Assert.Equal(2, dfRead.Height);
        Assert.Equal(4, dfRead.Width); 
        
        Assert.Equal("Alice", dfRead.GetValue<string>(0, "name"));
        Assert.Equal(99.5, dfRead.GetValue<double>(0, "score"));
        Assert.Equal(0u, dfRead.GetValue<uint>(0, "row_idx")); 
    }
    [Fact]
    [Trait("IO","AvroFile")]
    public void Test_ReadWriteAvro_Advanced()
    {

        using var sId = new Series("id", [1, 2, 3, 4, 5]);
        using var sName = new Series("name", ["Alice", "Bob", null, "David", "Eve"]); 
        using var df = new DataFrame(sId, sName);

        using var f = new DisposableFile(".avro");
        
        // ---------------------------------------------------------
        // Test Write Options: 
        // ---------------------------------------------------------

        df.WriteAvro(
            f.Path,
            compression: AvroCompression.Deflate,
            name: "TestRecord"
        );

        Assert.True(File.Exists(f.Path));
        Assert.True(new FileInfo(f.Path).Length > 0);


        using var dfFull = DataFrame.ReadAvro(f.Path);
        Assert.Equal(5, dfFull.Height);
        Assert.Equal(2, dfFull.Width);
        Assert.Equal("Alice", dfFull.GetValue<string>(0, "name"));
        Assert.Null(dfFull.GetValue<string>(2, "name")); 

        using var dfPartialByName = DataFrame.ReadAvro(
            f.Path,
            columns: ["id"], 
            nRows: 3         
        );
                
        Assert.Equal(3, dfPartialByName.Height); 
        Assert.Equal(1, dfPartialByName.Width); 
        
        Assert.True(dfPartialByName.ColumnNames.Contains("id"));
        Assert.False(dfPartialByName.ColumnNames.Contains("name"));

        using var dfPartialByIndex = DataFrame.ReadAvro(
            f.Path,
            projection: [1] 
        );
        
        Assert.Equal(5, dfPartialByIndex.Height);
        Assert.Equal(1, dfPartialByIndex.Width);
        Assert.True(dfPartialByIndex.ColumnNames.Contains("name"));
        Assert.False(dfPartialByIndex.ColumnNames.Contains("id"));
    }
    [Fact]
    [Trait("IO","AvroMem")]
    public void Test_ReadWriteAvro_MemoryBuffer()
    {
        using var sId = new Series("id", [1, 2, 3, 4, 5]);
        using var sName = new Series("name", ["Alice", "Bob", null, "David", "Eve"]); 
        using var df = new DataFrame(sId, sName);

        // ---------------------------------------------------------
        // Test Write to Memory Buffer:
        // ---------------------------------------------------------
        byte[] buffer = df.WriteAvroMemory(
            compression: AvroCompression.Snappy,
            name: "MemoryRecord"
        );
        
        Assert.NotNull(buffer);
        Assert.True(buffer.Length > 0);

        using var dfFull = DataFrame.ReadAvro(buffer);
        Assert.Equal(5, dfFull.Height);
        Assert.Equal(2, dfFull.Width);
        Assert.Equal("Alice", dfFull.GetValue<string>(0, "name"));
        Assert.Null(dfFull.GetValue<string>(2, "name")); 

        using var dfPartialByName = DataFrame.ReadAvro(
            buffer,
            columns: ["name"], 
            nRows: 2           
        );
        
        Assert.Equal(2, dfPartialByName.Height); 
        Assert.Equal(1, dfPartialByName.Width);  
        
        Assert.True(dfPartialByName.ColumnNames.Contains("name"));
        Assert.False(dfPartialByName.ColumnNames.Contains("id"));

        using var dfPartialByIndex = DataFrame.ReadAvro(
            buffer,
            projection: [0]
        );
        
        Assert.Equal(5, dfPartialByIndex.Height);
        Assert.Equal(1, dfPartialByIndex.Width);
        Assert.True(dfPartialByIndex.ColumnNames.Contains("id"));
        Assert.False(dfPartialByIndex.ColumnNames.Contains("name"));
    }
    [Fact]
    [Trait("IO", "IpcStreamFile")]
    public void Test_ReadWriteIpcStream_Advanced()
    {
        using var sId = new Series("id", [10, 20, 30, 40, 50]);
        using var sName = new Series("name", ["Alice", "Bob", null, "David", "Eve"]); 
        using var df = new DataFrame(sId, sName);

        using var f = new DisposableFile(".ipc");
        
        df.WriteIpcStream(
            f.Path,
            compression: IpcCompression.ZSTD, 
            compatLevel: -1                  
        );

        Assert.True(File.Exists(f.Path));
        Assert.True(new FileInfo(f.Path).Length > 0);

        using var dfFull = DataFrame.ReadIpcStream(f.Path);
        Assert.Equal(5, dfFull.Height);
        Assert.Equal(2, dfFull.Width);
        Assert.Equal(10, dfFull.GetValue<int>(0, "id"));
        Assert.Equal("Alice", dfFull.GetValue<string>(0, "name"));
        Assert.Null(dfFull.GetValue<string>(2, "name")); 

        using var dfPartial = DataFrame.ReadIpcStream(
            f.Path,
            columns: ["name"], 
            nRows: 3         
        );
                
        Assert.Equal(3, dfPartial.Height); 
        Assert.Equal(1, dfPartial.Width); 
        Assert.True(dfPartial.ColumnNames.Contains("name"));
        Assert.False(dfPartial.ColumnNames.Contains("id"));
        Assert.Equal("Bob", dfPartial.GetValue<string>(1, "name"));

        using var dfProj = DataFrame.ReadIpcStream(
            f.Path,
            projection: [0] 
        );

        Assert.Equal(5, dfProj.Height);
        Assert.Equal(1, dfProj.Width);
        Assert.True(dfProj.ColumnNames.Contains("id"));
        Assert.False(dfProj.ColumnNames.Contains("name"));
        Assert.Equal(50, dfProj.GetValue<int>(4, "id"));

        using var dfWithIndex = DataFrame.ReadIpcStream(
            f.Path,
            rowIndexName: "custom_index_col",
            rowIndexOffset: 100, 
            rechunk: true
        );

        Assert.Equal(5, dfWithIndex.Height);
        Assert.Equal(3, dfWithIndex.Width); 
        Assert.True(dfWithIndex.ColumnNames.Contains("custom_index_col"));
        Assert.Equal(100u, dfWithIndex.GetValue<uint>(0, "custom_index_col"));
        Assert.Equal(104u, dfWithIndex.GetValue<uint>(4, "custom_index_col"));
    }

    [Fact]
    [Trait("IO", "IpcStreamMemory")]
    public void Test_ReadWriteIpcStream_Memory_Advanced()
    {
        using var sId = new Series("id", [100, 200, 300, 400, 500]);
        using var sName = new Series("name", ["Alice", "Bob", null, "David", "Eve"]); 
        using var df = new DataFrame(sId, sName);

        byte[] memoryBuffer = df.WriteIpcStreamMemory(
            compression: IpcCompression.LZ4, 
            compatLevel: -1                 
        );

        Assert.NotNull(memoryBuffer);
        Assert.True(memoryBuffer.Length > 0);

        ReadOnlySpan<byte> bufferSpan = memoryBuffer;

        using var dfFull = DataFrame.ReadIpcStream(bufferSpan);
        Assert.Equal(5, dfFull.Height);
        Assert.Equal(2, dfFull.Width);
        Assert.Equal(100, dfFull.GetValue<int>(0, "id"));
        Assert.Null(dfFull.GetValue<string>(2, "name")); 

        using var dfPartial = DataFrame.ReadIpcStream(
            bufferSpan,
            columns: ["id"], 
            nRows: 2         
        );
                
        Assert.Equal(2, dfPartial.Height); 
        Assert.Equal(1, dfPartial.Width); 
        Assert.True(dfPartial.ColumnNames.Contains("id"));
        Assert.False(dfPartial.ColumnNames.Contains("name"));
        Assert.Equal(200, dfPartial.GetValue<int>(1, "id"));

        using var dfProj = DataFrame.ReadIpcStream(
            bufferSpan,
            projection: [1]
        );

        Assert.Equal(5, dfProj.Height);
        Assert.Equal(1, dfProj.Width);
        Assert.True(dfProj.ColumnNames.Contains("name"));
        Assert.False(dfProj.ColumnNames.Contains("id"));
        Assert.Equal("Alice", dfProj.GetValue<string>(0, "name"));

        using var dfWithIndex = DataFrame.ReadIpcStream(
            bufferSpan,
            rowIndexName: "auto_row_id",
            rowIndexOffset: 50, 
            rechunk: true
        );

        Assert.Equal(5, dfWithIndex.Height);
        Assert.Equal(3, dfWithIndex.Width); 
        Assert.True(dfWithIndex.ColumnNames.Contains("auto_row_id"));
        Assert.Equal(50u, dfWithIndex.GetValue<uint>(0, "auto_row_id"));
        Assert.Equal(54u, dfWithIndex.GetValue<uint>(4, "auto_row_id"));
    }
    [Fact]
    [Trait("IO", "IpcStreamSchema")]
    public void Test_ReadIpcStreamSchema_DictionaryStyle()
    {
        using var sId = new Series("id", [1, 2]);
        using var sValue = new Series("value", [3.14, 2.71]); 
        using var df = new DataFrame(sId, sValue);

        using var f = new DisposableFile(".ipc");
        df.WriteIpcStream(f.Path);

        using var schemaFromFile = DataFrame.ReadIpcStreamSchema(f.Path);
        
        Assert.NotNull(schemaFromFile);
        Assert.Equal(2, schemaFromFile.Count);
        
        Assert.True(schemaFromFile.ContainsKey("id"));
        Assert.True(schemaFromFile.ContainsKey("value"));
        Assert.False(schemaFromFile.ContainsKey("non_exist_col"));

        byte[] buffer = df.WriteIpcStreamMemory();
        ReadOnlySpan<byte> bufferSpan = buffer;

        using var schemaFromMemory = DataFrame.ReadIpcStreamSchema(bufferSpan);
        
        Assert.NotNull(schemaFromMemory);
        Assert.Equal(2, schemaFromMemory.Count);
        
        Assert.Equal(DataType.Int32, schemaFromMemory["id"]);
        Assert.Equal(DataType.Float64, schemaFromMemory["value"]);
        
        foreach (var field in schemaFromMemory)
        {
            Assert.NotNull(field.Name);
            Assert.NotNull(field.DataType);
        }
    }
    
}
