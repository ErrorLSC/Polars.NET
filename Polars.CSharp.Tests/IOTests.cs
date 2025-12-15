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
            using var s1 = new Series("a", [1, 2, 3]);
            using var s2 = new Series("b", ["x", "y", "z"]);
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
            using var s = new Series("ts", [new DateTime(2023,1,1), new DateTime(2024,1,1)]);
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
    }
}