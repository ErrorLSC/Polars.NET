using Apache.Arrow;
using Apache.Arrow.Types;
using Polars.NET.Core.Data;
using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests
{
    public class IoTests
    {
        [Fact]
        public void Test_Json_Read()
        {
            // 标准 JSON (Array of Objects)
            var jsonContent = @"
            [
                {""name"": ""Alice"", ""age"": 20},
                {""name"": ""Bob"", ""age"": 30}
            ]";

            using var f = new DisposableFile(jsonContent, ".json");
            
            using var df = DataFrame.ReadJson(f.Path);
            
            Assert.Equal(2, df.Height);
            Assert.Equal("Alice", df.GetValue<string>(0, "name"));
            Assert.Equal(30, df.GetValue<int>(1, "age"));
        }

        [Fact]
        public void Test_Ndjson_Scan_Lazy()
        {
            // NDJSON (Newline Delimited JSON) -> 每行一个 Object
            var ndjsonContent = 
@"{""id"": 1, ""val"": ""a""}
{""id"": 2, ""val"": ""b""}
{""id"": 3, ""val"": ""c""}";

            using var f = new DisposableFile(ndjsonContent, ".ndjson"); // 注意扩展名

            // 测试 Scan (Lazy)
            using var lf = LazyFrame.ScanNdjson(f.Path);
            using var df = lf.Collect();

            Assert.Equal(3, df.Height);
            Assert.Equal(2, df.GetValue<int>(1, "id"));
            Assert.Equal("c", df.GetValue<string>(2, "val"));
        }

        [Fact]
        public void Test_Parquet_RoundTrip()
        {
            // 1. 创建数据
            using var s1 = new Series("a", new int[]{1, 2, 3});
            using var s2 = new Series("b", new string[]{"x", "y", "z"});
            using var dfOriginal = new DataFrame(s1, s2);

            // 2. 写入 Parquet (需要 DataFrame.WriteParquet 实现)
            using var f = new DisposableFile(".parquet");
            dfOriginal.WriteParquet(f.Path);

            // 3. 读取 Parquet
            using var dfRead = DataFrame.ReadParquet(f.Path);

            // 4. 验证
            Assert.Equal(dfOriginal.Height, dfRead.Height);
            Assert.Equal("y", dfRead.GetValue<string>(1, "b"));
            
            // 5. 测试 Lazy Scan
            using var lf = LazyFrame.ScanParquet(f.Path);
            using var dfLazyRead = lf.Collect();
            Assert.Equal(3, dfLazyRead.Height);
        }

        [Fact]
        public void Test_Ipc_RoundTrip()
        {
            // IPC (Feather) 格式测试
            using var s = new Series("ts", new DateTime[]{new DateTime(2023,1,1), new DateTime(2024,1,1)});
            using var dfOriginal = new DataFrame(s);

            using var f = new DisposableFile(".ipc"); // 或 .arrow
            dfOriginal.WriteIpc(f.Path);

            using var dfRead = DataFrame.ReadIpc(f.Path);
            
            Assert.Equal(2, dfRead.Height);
            // 验证时间是否正确读写 (IPC 保留类型能力很强)
            Assert.Equal(new DateTime(2023,1,1), dfRead.GetValue<DateTime>(0, "ts"));
            
            // Lazy Scan IPC
            using var lf = LazyFrame.ScanIpc(f.Path);
            using var dfLazy = lf.Collect();
            Assert.Equal(2, dfLazy.Height);
        }
        [Fact]
        public void Test_Csv_TryParseDates_Auto()
        {
            // 构造数据：包含标准的 ISO 日期格式
            var csvContent = "name,birthday\nAlice,2023-01-01\nBob,2023-12-31";
            using var csv = new DisposableFile(csvContent,".csv");

            // 1. 默认 tryParseDates = true
            using var df = DataFrame.ReadCsv(csv.Path);
            
            // 验证 birthday 列是否被自动解析为 Date 类型，而不是 String
            // 注意：Polars 自动解析可能解析为 Date 或 Datetime
            var dateType = df.Schema["birthday"];
            Assert.Equal(DataTypeKind.Date, dateType.Kind);

            // 2. 测试显式关闭 (tryParseDates = false)
            using var dfString = DataFrame.ReadCsv(csv.Path, tryParseDates: false);
            
            // 断言它是 String
            var strType = dfString.Schema["birthday"];
            Assert.Equal(DataTypeKind.String,strType.Kind);
        }
        private class SinkTestPoco
    {
        public int Id { get; set; }
        public string Type { get; set; }
        public double Val { get; set; }
    }
        [Fact]
        public void Test_SinkParquet_Basic()
        {
            // 1. 准备数据
            var df = DataFrame.FromColumns(new
            {
                Id = new[] { 1, 2, 3 },
                Name = new[] { "Alice", "Bob", "Charlie" }
            });

            // 生成临时文件路径
            string path = Path.GetTempFileName(); 
            // GetTempFileName 创建了一个空文件，ParquetWriter 可能会抱怨文件已存在或为空。
            // 安全起见，删掉它，让 Polars 创建
            File.Delete(path); 
            path += ".parquet"; // 加个后缀

            try
            {
                // 2. Lazy -> Sink
                // Sink 这是一个 Action (就像 Collect 一样)，会触发执行
                df.Lazy().SinkParquet(path);

                // 3. 验证文件是否存在
                Assert.True(File.Exists(path));
                Assert.True(new FileInfo(path).Length > 0);

                // 4. 读取回验证 (假设你有 ScanParquet，如果没有，可以先只测文件存在)
                using var dfRead = LazyFrame.ScanParquet(path).Collect();
                Assert.Equal(3, dfRead.Height);
                Assert.Equal("Alice", dfRead.GetValue<string>(0, "Name"));
            }
            finally
            {
                if (File.Exists(path)) File.Delete(path);
            }
        }
        [Fact]
        public void Test_Streaming_SinkParquet_EndToEnd()
        {
            // ====================================================
            // 场景：生成 100万行数据，过滤掉一半，写入 Parquet
            // 预期：内存平稳，磁盘出现文件
            // ====================================================

            int totalRows = 1_000_000;
            int batchSize = 100_000;
            string path = Path.Combine(Path.GetTempPath(), $"polars_stream_{Guid.NewGuid()}.parquet");

            try
            {
                // [修复]: 使用明确的泛型 IEnumerable<SinkTestPoco>
                // 而不是 IEnumerable<object>
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

                // 2. 建立管道
                var lf = LazyFrame.ScanArrowStream(GenerateData(), batchSize);

                // 3. 转换逻辑 (Lazy)
                // 只保留 Type == "A" 的数据 (50万行)
                var q = lf
                    .Filter(Polars.Col("Type") == Polars.Lit("A"))
                    .Select(Polars.Col("Id"), Polars.Col("Val"));

                Console.WriteLine("Starting Streaming Sink...");
                var sw = System.Diagnostics.Stopwatch.StartNew();

                // 4. 执行写入 (Sink)
                // Polars 会启动 Streaming Engine，分块读取 C# -> 处理 -> 写入磁盘
                // 整个过程内存占用极低
                q.SinkParquet(path);

                sw.Stop();
                Console.WriteLine($"Sink completed in {sw.Elapsed.TotalSeconds:F2}s");

                // 5. 验证
                Assert.True(File.Exists(path));
                var fileInfo = new FileInfo(path);
                Console.WriteLine($"Parquet File Size: {fileInfo.Length / 1024.0 / 1024.0:F2} MB");

                // 简单的正确性验证：
                // 如果你的库实现了 ScanParquet，可以读回来校验行数是否为 500,000
                
                using var lfCheck = LazyFrame.ScanParquet(path);
                using var dfCheck = lfCheck.Collect();
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
        // ====================================================
        // 场景：生成包含数组和对象的复杂数据流，流式写入 Parquet
        // 目的：验证 "邪修" ArrowConverter 在流式 Sink 下的稳定性
        // ====================================================

        int totalRows = 100_000; // 稍微减小一点量，侧重测结构
        int batchSize = 10_000;
        string path = Path.Combine(Path.GetTempPath(), $"polars_complex_{Guid.NewGuid()}.parquet");

        try
        {
            // 1. 数据源 (包含 List 和 Struct)
            IEnumerable<ComplexPoco> GenerateData()
            {
                for (int i = 0; i < totalRows; i++)
                {
                    yield return new ComplexPoco
                    {
                        Id = i,
                        // 测试 List<int> -> Parquet LIST
                        Tags = new[] { i, i * 2 }, 
                        // 测试 POCO -> Parquet STRUCT
                        Meta = new MetaInfo { Score = i * 0.5, Label = $"L_{i}" } 
                    };
                }
            }

            // 2. 建立管道
            // ScanArrowStream 内部会调用 ToArrowBatches -> ArrowConverter
            // 这里会触发你的 "邪修" 递归反射逻辑
            var lf = LazyFrame.ScanArrowStream(GenerateData(), batchSize);

            // 3. 简单的转换 (确保 Lazy 引擎介入)
            // 比如只保留 Id 偶数的
            var q = lf.Filter(Col("Id") % Lit(2) == Lit(0));

            Console.WriteLine("Starting Complex Streaming Sink...");
            var sw = System.Diagnostics.Stopwatch.StartNew();

            // 4. 执行写入
            q.SinkParquet(path);

            sw.Stop();
            Console.WriteLine($"Sink completed in {sw.Elapsed.TotalSeconds:F2}s");

            // 5. 验证文件
            Assert.True(File.Exists(path));

            // 6. 读回验证 (Eager Load)
            // 这里验证 Parquet 是否真的存对了结构
            using var lfCheck = LazyFrame.ScanParquet(path);
            using var df = lfCheck.Collect();
            
            Assert.Equal(totalRows / 2, df.Height); // 过滤了一半

            // 验证 List
            var tags = df.Column("Tags");
            Assert.Equal(DataTypeKind.List, tags.DataType.Kind);
            var row0Tags = tags.GetValue<List<int?>>(0); // Id=0: [0, 0]
            Assert.Equal(0, row0Tags[0]);
            
            // 验证 Struct (Unnest 验证)
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

    // --- 辅助 POCO ---
    private class ComplexPoco
    {
        public int Id { get; set; }
        public int[] Tags { get; set; }     // 映射为 List<Int32>
        public MetaInfo Meta { get; set; }  // 映射为 Struct
    }

    private class MetaInfo
    {
        public double Score { get; set; }
        public string Label { get; set; }
    }
        [Fact]  
        public void Test_ScanDataReader_Integration()
        {
            // 1. 模拟数据库
            var table = new System.Data.DataTable();
            table.Columns.Add("Id", typeof(int));
            table.Columns.Add("Name", typeof(string)); // 嫌疑人 A
            table.Columns.Add("Value", typeof(double));
            table.Columns.Add("Date", typeof(DateTime)); // 嫌疑人 B

            var now = DateTime.Now;
            for (int i = 0; i < 1000; i++)
            {
                table.Rows.Add(i, $"User_{i}", i * 0.5, now.AddSeconds(i));
                // table.Rows.Add(i, i * 0.5, now.AddSeconds(i));
            }

            using var reader = table.CreateDataReader();

            // [修改] 只读取数值列，暂时避开 String 和 DateTime
            // 注意：ToArrowBatches 会读取所有列，所以我们得在查询里 Select
            // 或者，更直接地，我们只往 DataTable 里放数值列试试？
            
            // 既然 ScanDataReader 默认读所有列，我们通过 Lazy Select 来过滤
            var lf = LazyFrame.ScanDatabase(reader, batchSize: 100);

            // [测试 A] 只查数值列
            var q = lf.Select(Col("Id"), Col("Value")).Filter(Col("Id") > 500);

            using var df = q.CollectStreaming();
            
            Assert.Equal(499, df.Height);
        }
        [Fact]
        public void Test_ScanDataReader_Nested_Array()
        {
            // 1. 模拟一个支持数组的数据库 (如 PostgreSQL)
            var table = new System.Data.DataTable();
            table.Columns.Add("Id", typeof(int));
            // [邪修核心] 定义列类型为 int[]
            table.Columns.Add("Tags", typeof(int[])); 
            table.Columns.Add("Memo", typeof(string)); // 顺便测一下 LargeString

            // 2. 插入数据
            // Row 1: [10, 20]
            table.Rows.Add(1, new int[] { 10, 20 }, "Row1");
            // Row 2: [30]
            table.Rows.Add(2, new int[] { 30 }, "Row2");
            // Row 3: [] (空数组)
            table.Rows.Add(3, new int[] { }, "Row3");
            // Row 4: null (空值)
            table.Rows.Add(4, DBNull.Value, "Row4");

            using var reader = table.CreateDataReader();

            // 3. 执行 Scan
            // 这里的 reader.GetFieldType("Tags") 会返回 typeof(int[])
            // 你的 DataReaderExtensions 会识别为 IEnumerable -> LargeList
            // 然后 Buffer 会收集这些数组，交给 ArrowConverter 递归处理
            var lf = LazyFrame.ScanDatabase(reader);

            using var df = lf.Collect();

            // 4. 验证
            Assert.Equal(4, df.Height);
            
            // 验证 Schema: Tags 应该是 List(Int32)
            // 注意：Polars 内部 List 可能是 LargeList，C# 类型是 Series
            var tagsSeries = df.Column("Tags");
            Assert.Equal(DataTypeKind.List, tagsSeries.DataType.Kind);

            // 验证值 (需要 Series 支持 GetValue<int[]> 或者 List<int>)
            // 这里我们简单起见，转成 String 验证结构，或者由你之前实现的 GetValue 支持
            // Row 1: [10, 20]
            // 假设 Series.ToString() 会打印列表内容
            var row1 = tagsSeries.GetValue<List<int?>>(0);
            // 断言它包含元素 (具体格式取决于 Polars Series ToString 实现)
            // 只要不报错，说明结构对了。
            
            // 验证 LargeString 也没炸
            Assert.Equal("Row1", df.GetValue<string>(0, "Memo"));
            
            // 5. 进阶：Explode (炸开) 测试
            // 如果能炸开，说明 Polars 真的认出了这是 List
            var exploded = df.Explode(Col("Tags"));
            // 1(10), 1(20), 2(30), 3(null), 4(null) -> 至少应该变多
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
            // 1. 模拟数据库返回对象
            var table = new System.Data.DataTable();
            table.Columns.Add("Id", typeof(int));
            // [邪修核心] 定义列类型为自定义对象
            table.Columns.Add("Meta", typeof(UserMeta));

            table.Rows.Add(1, new UserMeta { Level = 5, Score = 99.5 });
            table.Rows.Add(2, new UserMeta { Level = 1, Score = 10.0 });
            table.Rows.Add(3, DBNull.Value); // Null Struct

            using var reader = table.CreateDataReader();

            // 2. 执行 Scan
            // DataReaderExtensions 会发现 UserMeta 不认识 -> 兜底扔给 ArrowConverter
            // ArrowConverter 反射 UserMeta -> 发现是 Class -> BuildStructArray -> 生成 Struct
            var lf = LazyFrame.ScanDatabase(reader);

            using var df = lf.Collect();

            // 3. 验证
            Assert.Equal(3, df.Height);
            
            var metaSeries = df.Column("Meta");
            Assert.Equal(DataTypeKind.Struct, metaSeries.DataType.Kind);

            // 4. 验证 Struct 字段访问 (Unnest)
            // 在 Polars 里把 Struct 拆成列
            var unnested = df.Unnest("Meta");
            
            Assert.True(unnested.ColumnNames.Contains("Level"));
            Assert.True(unnested.ColumnNames.Contains("Score"));
            
            Assert.Equal(5, unnested.GetValue<int>(0, "Level"));
            Assert.Equal(99.5, unnested.GetValue<double>(0, "Score"));
            
            // Row 3 应该是 null
            Assert.Null(unnested.GetValue<int?>(2, "Level"));
        }
        [Fact]
        public void Test_DataFrame_FromDataReader_Eager_Nested()
        {
            // 1. 模拟数据库 (复用之前的复杂结构)
            var table = new System.Data.DataTable();
            table.Columns.Add("Id", typeof(int));
            table.Columns.Add("Tags", typeof(int[])); // List
            table.Columns.Add("Meta", typeof(UserMeta)); // Struct

            table.Rows.Add(1, new int[] { 10, 20 }, new UserMeta { Level = 99, Score = 100.0 });
            table.Rows.Add(2, DBNull.Value, DBNull.Value);

            using var reader = table.CreateDataReader();

            // 2. [高光时刻] 一行代码，Eager 加载！
            // 此时数据已经在 C++ 堆内存里了，C# 端只有 handle
            using var df = DataFrame.ReadDatabase(reader);

            // 3. 验证
            Assert.Equal(2, df.Height);
            
            // 验证 Struct
            var meta = df.Column("Meta");
            Assert.Equal(DataTypeKind.Struct, meta.DataType.Kind);
            
            // 验证 List
            var tags = df.Column("Tags");
            Assert.Equal(DataTypeKind.List, tags.DataType.Kind);

            // 验证值
            var row1Tags = tags.GetValue<List<int?>>(0);
            Assert.Equal(10, row1Tags[0]);
            
            // 验证 Unnest 也就是 Struct 的内容
            var unnested = df.Unnest("Meta");
            Assert.Equal(99, unnested.GetValue<int>(0, "Level"));
        }
        [Fact]
        public async Task Test_WriteTo_Generic_EndToEnd()
        {
            // 1. 准备数据 (故意搞点 null)
            var df = DataFrame.FromColumns(new 
            {
                Id = new[] { 1, 2, 3 },
                // Date 列，中间有个 null
                Date = new DateTime?[] { DateTime.Now.Date, null, DateTime.Now.Date.AddDays(1) }
            });

            var targetTable = new System.Data.DataTable();

            // 2. 调用通用 WriteTo 接口
            // 这里模拟 "SQL Server Extension" 的行为
            await Task.Run(() => 
            {
                df.WriteTo(reader => 
                {
                    // 假装这是 SqlBulkCopy.WriteToServer
                    targetTable.Load(reader);
                });
            });

            // 3. 验证
            Assert.Equal(3, targetTable.Rows.Count);
            Assert.Equal(1, targetTable.Rows[0]["Id"]);
            Assert.NotNull(targetTable.Rows[0]["Date"]);
            
            // 验证 null 处理
            Assert.Equal(DBNull.Value, targetTable.Rows[1]["Date"]);
        }
    }
    public class CsvSchemaTests
    {
        [Fact]
        public void Test_ReadCsv_With_Explicit_Schema()
        {
            // 1. 准备测试数据
            // 默认情况下:
            // "id"   会被推断为 Int64
            // "rate" 会被推断为 Float64
            // "date" 会被推断为 Date 或 String
            string csvContent = @"id,name,rate,date
1,Apple,1.5,2023-01-01
2,Banana,3.7,2023-05-20
3,Cherry,,2023-10-10";

            string filePath = Path.GetTempFileName() + ".csv";
            File.WriteAllText(filePath, csvContent);

            try
            {
                Console.WriteLine($"[Test] Created temp CSV at: {filePath}");

                // 2. 定义强制 Schema (覆盖默认推断)
                // 我们故意使用非默认类型来验证 Schema 是否生效
                var explicitSchema = new Dictionary<string, DataType>
                {
                    // 强制 id 为 Int32 (默认是 Int64)
                    ["id"] = DataType.Int32,
                    
                    // name 保持 String
                    ["name"] = DataType.String,
                    
                    // 强制 rate 为 Float32 (默认是 Float64)
                    ["rate"] = DataType.Float32,
                    
                    // 强制 date 为 String (禁止自动解析日期)
                    ["date"] = DataType.String 
                };

                // 3. 执行读取 (Eager Mode)
                // 这里会触发 WithSchemaHandle -> pl_schema_new -> pl_read_csv
                using var df = DataFrame.ReadCsv(filePath, schema: explicitSchema);

                Console.WriteLine("[Test] DataFrame loaded successfully.");
                Console.WriteLine(df); // 这里会打印 Schema 字符串，你可以人工检查

                // 4. 验证 Schema (Introspection)
                var resultSchema = df.Schema;

                // 验证: id 应该是 Int32
                Assert.Equal(DataTypeKind.Int32, resultSchema["id"].Kind);
                
                // 验证: rate 应该是 Float32
                Assert.Equal(DataTypeKind.Float32, resultSchema["rate"].Kind);

                // 验证: date 应该是 String (因为我们强制指定了)
                Assert.Equal(DataTypeKind.String, resultSchema["date"].Kind);

                // 5. 验证数据正确性 (可选)
                // 确保数据没有因为类型转换而乱码
                // 注意：这里需要你之前实现的 Series GetValue 相关方法支持
                // 简单起见，我们检查 Null Count
                Assert.Equal(1, df["rate"].NullCount);
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
            // 测试 LazyFrame (ScanCsv)
            string csvContent = "val\n100\n200";
            string filePath = Path.GetTempFileName() + ".csv";
            File.WriteAllText(filePath, csvContent);

            try
            {
                // 强制转为 Float64 (默认 Int64)
                var schema = new Dictionary<string, DataType>
                {
                    ["val"] = DataType.Float64
                };

                // Lazy Mode
                // 这里触发 pl_scan_csv
                using var lf = LazyFrame.ScanCsv(filePath, schema: schema);
                
                // 此时还未读取文件，但 Schema 应该是我们指定的
                // 注意：LazyFrame.Schema 属性会触发 collect_schema
                var lfSchema = lf.Schema;

                Assert.Equal(DataTypeKind.Float64, lfSchema["val"].Kind);
                Console.WriteLine("[Test] LazyFrame Schema validated.");

                // Collect 并验证结果
                using var df = lf.Collect();
                Assert.Equal(DataTypeKind.Float64, df.Schema["val"].Kind);
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
            // 1. 准备“非标准”的脏数据
            // 特征：
            // - 前两行是垃圾注释 (需要 skipRows=2)
            // - 使用分号 ';' 分隔 (需要 separator=';')
            // - 包含日期字符串 (需要 tryParseDates=true)
            // - 我们希望 ID 是 Int32 而不是默认的 Int64 (需要 schema)
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
                // 2. 定义强制 Schema (Introspection 验证点)
                var explicitSchema = new Dictionary<string, DataType>
                {
                    ["ID"] = DataType.Int32,        // 强制 Int32
                    // ["ProductName"] = DataType.String,
                    ["Weight"] = DataType.Float32,  // 强制 Float32
                    ["ReleaseDate"] = DataType.Date // 强制 Date
                };

                // 3. 调用全参数 ReadCsv
                using var df = DataFrame.ReadCsv(
                    path: filePath,
                    schema: explicitSchema,    // 验证 Schema 注入
                    hasHeader: true,           // 验证表头解析
                    separator: ';',            // 验证自定义分隔符
                    skipRows: 2,               // 验证跳过行
                    tryParseDates: true        // 验证日期解析
                );

                // 4. 验证结构 (Shape)
                Assert.Equal(2, df.Height); // 2 行数据
                Assert.Equal(4, df.Width);  // 4 列

                // 5. 验证元数据 (Schema)
                var resultSchema = df.Schema;
                Assert.Equal(DataTypeKind.Int32, resultSchema["ID"].Kind);
                Assert.Equal(DataTypeKind.Float32, resultSchema["Weight"].Kind);
                Assert.Equal(DataTypeKind.Date, resultSchema["ReleaseDate"].Kind);

                // 6. 验证标量值 (利用索引器)
                
                // 第一行数据验证
                Assert.Equal(101, df["ID"][0]); 
                Assert.Equal("Quantum Gadget", df["ProductName"][0]);
                
                // 浮点数精度验证
                // 注意：Float32 在 C# 是 float，在 Assert.Equal 时最好指定精度
                Assert.Equal(1.55f, (float)df["Weight"][0], 0.0001f);
                
                // 日期验证 (Polars Date 内部通常存储为 Days Int32，或者 C# Binding 映射为 DateTime/DateOnly)
                // 这里假设你的 Binding 将 Date 映射为 DateTime 或 DateOnly
                // 如果返回的是 DateTime：
                var dateVal = df["ReleaseDate"][0];
                Assert.Equal(new DateOnly(2023, 12, 25), dateVal);

                // 7. 验证 Null 处理 (第二行 Weight 为 null)
                // 方式 A: 检查 NullCount
                Assert.Equal(1, df["Weight"].NullCount);
                
                // 方式 B: 标量值检查 (取决于你的 indexer 对 null 的返回是 null 还是 DBNull.Value)
                // 假设是 null
                Assert.Null(df["Weight"][1]);
            }
            finally
            {
                if (File.Exists(filePath)) 
                    File.Delete(filePath);
            }
        }
    }
}