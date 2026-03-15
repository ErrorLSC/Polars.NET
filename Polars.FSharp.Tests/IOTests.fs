namespace Polars.FSharp.Tests

open System
open System.IO
open Xunit
open Polars.FSharp

type IOTests() =
    
    // 准备一个临时文件路径
    let tempPath = Path.Combine(Path.GetTempPath(), $"scan_test_{Guid.NewGuid()}.parquet")

    // 在构造函数中生成一个标准的 Parquet 文件
    do
        let df = 
            DataFrame.create [
                Series.create("id", [1; 2; 3; 4; 5])
                Series.create("val", [10.5; 20.5; 30.5; 40.5; 50.5])
                Series.create("cat", ["A"; "B"; "A"; "B"; "C"])
            ]
        df.WriteParquet(
            tempPath,
            compression = ParquetCompression.Zstd,
            compressionLevel = 3,
            statistics = true,
            rowGroupSize = 2
        ) |> ignore

    // 析构函数清理垃圾
    interface IDisposable with
        member _.Dispose() =
            if File.Exists tempPath then File.Delete tempPath

    [<Fact>]
    member _.``ScanParquet (File): nRows and RowIndex``() =
        // 测试核心：
        // 1. nRows (传下去是指针) -> 生效应该只剩 3 行
        // 2. rowIndexName (字符串) -> 应该多出一列 "idx"
        // 3. rowIndexOffset (uint) -> "idx" 应该从 100 开始
        let lf = LazyFrame.ScanParquet(
            tempPath, 
            nRows = 3UL, 
            rowIndexName = "idx", 
            rowIndexOffset = 100u
        )
        
        let df = lf.Collect()

        // Assert nRows
        Assert.Equal(3L, df.Height)
        
        // Assert Row Index exists and correct
        let idxCol = df.Column("idx")
        Assert.Equal(100u, idxCol.GetValue<uint32>(0))
        Assert.Equal(101u, idxCol.GetValue<uint32>(1))
        
        // Assert Data
        Assert.Equal(1, df.Column("id").GetValue<int>(0))

    [<Fact>]
    member _.``ScanParquet (Memory): Bytes and Schema Overwrite``() =
        // 1. 读取 bytes
        let bytes = File.ReadAllBytes tempPath

        // 2. 构造一个 PolarsSchema
        // 我们故意只定义两列，看看 Schema Projection 是否生效，或者验证 handle 传递是否成功
        // 注意：scan_parquet 的 schema 参数通常用于 overwrite 数据类型，
        // 这里我们主要验证 C# Wrapper 的 SafeHandleLock 逻辑没炸。
        use mySchema = new PolarsSchema([
            "id", DataType.Int32
            "val", DataType.Float64
            "cat", DataType.String
        ])

        // 3. 调用内存版 ScanParquet
        // 传入 schema，lowMemory=true 测试 bool flag
        let lf = LazyFrame.ScanParquet(
            bytes,
            schema = mySchema,
            lowMemory = true,
            useStatistics = false // 故意关掉 stats 测参数
        )

        let df = lf.Collect()

        // 验证读取完整性
        Assert.Equal(5L, df.Height)
        Assert.Equal("A", df.Column("cat").GetValue<string>(0))
        Assert.Equal(50.5, df.Column("val").GetValue<double>(4))

    [<Fact>]
    member _.``ScanParquet: Schema handle disposal safety``() =
        // 这是一个压力/安全性测试
        // 验证在 F# scope 结束销毁 Schema 后，ScanParquet 内部会不会因为持有悬空指针而崩溃
        // (你的 C# Wrapper 应该在调用 native 期间锁住 handle，这是关键)
        
        let bytes = File.ReadAllBytes tempPath
        let mutable lf = Unchecked.defaultof<LazyFrame>

        // Scope
        let runScope () =
            use schema = new PolarsSchema([
                "id", DataType.Int32
                "val", DataType.Float64
                "cat", DataType.String
                ])
            // 这里传入 schema，LazyFrame 建立 Logical Plan
            lf <- LazyFrame.ScanParquet(bytes, schema=schema)
            // schema 在这里 dispose
        
        runScope()

        let df = lf.Collect()
        Assert.True(df.Height > 0L)
    [<Fact>]
    member _.``IO: Advanced CSV Reading (Schema, Skip, Dates)`` () =
        let path = "advanced_test.csv"
        try
            // 构造带干扰项的 CSV
            let content = 
                "IGNORE_THIS_LINE\n" +
                "id;date_col;val_col\n" +
                "007;2023-01-01;99.9\n" +
                "008;2023-12-31;10.5"
            System.IO.File.WriteAllText(path, content)

            // 1. 构造 Schema (使用新的 Schema 类)
            // 显式指定 id 为 String 以保留前导零 "007"
            use mySchema = new PolarsSchema([
                "id", pl.string
                "date_col", pl.date
                "val_col", pl.float64
            ])

            // 2. 调用全参数 readCsv
            let df = DataFrame.ReadCsv(
                path,
                skipRows = 1UL,       // 注意：F# 中 int64 字面量通常带 L，或者依赖自动转换
                separator = ';',
                tryParseDates = true,
                schema = mySchema
            )

            // 3. 验证结果
            Assert.Equal(2L, df.Height) // Rows -> Height

            // 验证类型覆盖 (ID 应该是 String，而不是自动推断的 Int)
            Assert.Equal(pl.string, df.Column("id").DataType)
            
            // 验证数值
            Assert.Equal("007", df.Column("id").GetValue<string>(0))
            Assert.Equal(99.9, df.Column("val_col").GetValue<double>(0))
            
            // 验证日期解析 (依赖 tryParseDates=true 和 Schema)
            // Polars.NET 通常将 pl.Date 映射为 DateOnly
            let dateVal = df.Column("date_col").GetValue<DateOnly>(0)
            Assert.Equal(DateOnly(2023, 1, 1), dateVal)

        finally
            if File.Exists path then File.Delete path
    [<Fact>]
    member _.``ScanCsv (Memory): Basic & Options``() =
        // 1. 准备 CSV 字节流
        let csvString = 
            """name,age,active
Alice,30,true
Bob,25,false
Charlie,35,true"""
        
        let bytes = System.Text.Encoding.UTF8.GetBytes csvString

        // 2. 调用内存版 ScanCsv
        // 测试参数传递：跳过 header (假设我们要把它当数据读，或者测试 skipRows)
        // 这里我们正常读，但在 Laziness 上做文章
        let lf = LazyFrame.ScanCsv(
            bytes, 
            hasHeader = true,
            nRows = 2UL // 只读前两行
        )

        // 3. 执行
        let df = lf.Collect()

        // 4. 验证
        Assert.Equal(2L, df.Height)
        Assert.Equal("Alice", df.Column("name").GetValue<string>(0))
        Assert.Equal(30, df.Column("age").GetValue<int>(0))
        
        // 验证 Bob 也在
        Assert.Equal("Bob", df.Column("name").GetValue<string>(1))
    [<Fact>]
    member _.``Can read&write Parquet`` () =
        // 1. 准备 CSV 数据
        use csv = new DisposableFile(".csv", "a,b,c,d\n1,2,3,4")
        use df = DataFrame.ReadCsv(csv.Path, tryParseDates=false)
        
        // 2. 准备 Parquet 目标路径 (不预先创建文件)
        use parquet = new DisposableFile ".parquet"
        
        // 3. 写入
        // 注意：F# 方法返回 this，忽略它
        df.WriteParquet parquet.Path |> ignore
        
        // 4. 验证文件确实生成了
        Assert.True(File.Exists parquet.Path, $"Parquet file should exist at {parquet.Path}")

        // 5. 读回来验证内容
        use df2 = DataFrame.ReadParquet parquet.Path
        Assert.Equal(df.Rows, df2.Rows)
        Assert.Equal(4L, df2.Width)

    [<Fact>]    
    member _.``IO: Read JSON (File, Bytes, Stream)`` () =
        // 准备路径托管
        use jsonFile = new DisposableFile ".json"

        // 1. 准备数据并写入文件 (作为测试源)
        let s1 = Series.create("a", [1; 2; 3])
        let s2 = Series.create("b", ["x"; "y"; "z"])
        use df = DataFrame.create [s1; s2]
        
        df.WriteJson jsonFile.Path |> ignore
        Assert.True(File.Exists jsonFile.Path, "JSON file not found")

        // ---------------------------------------------------
        // 测试 1: 从文件路径读取 (File)
        // ---------------------------------------------------
        use dfFile = DataFrame.ReadJson jsonFile.Path
        Assert.Equal(3L, dfFile.Rows)
        Assert.Equal(1L, dfFile.Int("a", 0).Value) // 验证第一行数据

        // ---------------------------------------------------
        // 测试 2: 从内存字节读取 (Bytes)
        // ---------------------------------------------------
        let bytes = File.ReadAllBytes jsonFile.Path
        use dfBytes = DataFrame.ReadJson bytes
        
        Assert.Equal(3L, dfBytes.Rows)
        Assert.Equal("y", dfBytes.String("b", 1).Value) // 验证第二行数据

        // ---------------------------------------------------
        // 测试 3: 从流读取 (Stream)
        // ---------------------------------------------------
        use fs = File.OpenRead jsonFile.Path
        use dfStream = DataFrame.ReadJson fs
        
        Assert.Equal(3L, dfStream.Rows)
        Assert.Equal("z", dfStream.String("b", 2).Value) // 验证第三行数据
    [<Fact>]
    member _.``Lazy: Scan NDJSON (All Modes - Manual IO)`` () =
        // ---------------------------------------------------
        // 1. 手动准备环境 (避开 DisposableFile)
        // ---------------------------------------------------
        let tempStub = Path.GetTempFileName()
        let path = Path.ChangeExtension(tempStub, ".ndjson")

        try
            // 2. 准备纯净数据 (每行一个 JSON 对象，val 是数字)
            // 使用 String.concat 避免缩进和换行符问题
            let content = 
                [ "{\"id\": 1, \"val\": 100, \"tag\": \"A\"}"
                  "{\"id\": 2, \"val\": 200, \"tag\": \"B\"}"
                  "{\"id\": 3, \"val\": 300, \"tag\": \"C\"}" ]
                |> String.concat "\n"

            File.WriteAllText(path, content)

            // =================================================================
            // Mode 1: File Mode (测试 Schema Overwrite)
            // =================================================================
            // 构造 Schema: 强行将 "val" 指定为 Int32 (默认推断通常是 Int64)
            use schema = new PolarsSchema(["val", DataType.Int32])

            // 使用内部函数隔离作用域，确保对象及时 Dispose
            let testFileMode () =
                use lf = LazyFrame.ScanNdjson(path, schema=schema, nRows=3UL)
                use df = lf.Collect()

                Assert.Equal(3L, df.Rows)
                
                // [验证点] Schema Overwrite 是否生效
                // 如果是 Int32，说明 Rust 端的 with_schema_overwrite 工作正常
                Assert.Equal(DataType.Int32, df.Column("val").DataType)
                Assert.Equal(100L, df.Int("val", 0).Value)

            testFileMode()

            // =================================================================
            // Mode 2: Memory Mode (Bytes)
            // =================================================================
            let bytes = File.ReadAllBytes path

            let testMemoryMode () =
                // 不传 Schema，验证 Rust 端 ScanSources::Buffers 和默认推断
                use lf = LazyFrame.ScanNdjson bytes
                use df = lf.Collect()

                Assert.Equal(3L, df.Rows)
                
                // [验证点] 默认推断通常是 Int64
                Assert.Equal(DataType.Int64, df.Column("val").DataType)
                Assert.Equal(200L, df.Int("val", 1).Value)
                Assert.Equal("B", df.String("tag", 1).Value)

            testMemoryMode()

            // =================================================================
            // Mode 3: Stream Mode
            // =================================================================
            let testStreamMode () =
                use fs = File.OpenRead path
                
                // 验证 API 层 Stream -> MemoryStream -> Bytes 的转换逻辑
                use lf = LazyFrame.ScanNdjson(fs)
                use df = lf.Collect()

                Assert.Equal(3L, df.Rows)
                // 默认推断 id 也是 Int64
                Assert.Equal(3L, df.Int("id", 2).Value)

            testStreamMode()

        finally
            // ---------------------------------------------------
            // 清理战场
            // ---------------------------------------------------
            if File.Exists path then File.Delete path
            if File.Exists tempStub then File.Delete tempStub
    [<Fact>]
    member _.``IO: Read IPC (All Modes)`` () =
        // ---------------------------------------------------
        // 1. 准备环境 & 数据
        // ---------------------------------------------------
        let tempStub = Path.GetTempFileName()
        let path = Path.ChangeExtension(tempStub, ".ipc")

        try
            // 创建原始 DataFrame
            // 包含两列：id (Int32), val (String)
            let s1 = Series.create("id", [1; 2; 3; 4; 5])
            let s2 = Series.create("val", ["A"; "B"; "C"; "D"; "E"])
            
            // F# 的 use 绑定会在作用域结束时自动 Dispose
            use dfOrig = DataFrame.create [s1; s2]
            
            // 写入 IPC 文件
            dfOrig.WriteIpc path |> ignore

            // =================================================================
            // Mode 1: File Mode (测试 Projection & Limit)
            // =================================================================
            let testFileMode () =
                // 只读 "val" 列，且只读前 3 行
                // 验证 memoryMap=true (默认值) 是否正常工作
                use df = DataFrame.ReadIpc(
                    path, 
                    columns=["val"], 
                    nRows=3UL
                )

                Assert.Equal(3L, df.Rows)   // 验证 nRows
                Assert.Equal(1L, df.Width)   // 验证 columns (只剩 val)
                Assert.Equal("val", df.ColumnNames.[0])
                
                // 验证数据内容
                Assert.Equal("A", df.String("val", 0).Value)
                Assert.Equal("C", df.String("val", 2).Value)

            testFileMode()

            // =================================================================
            // Mode 2: Memory Mode (Bytes)
            // =================================================================
            let bytes = File.ReadAllBytes path

            let testMemoryMode () =
                use df = DataFrame.ReadIpc bytes

                Assert.Equal(5L, df.Rows) // 读取完整数据
                Assert.Equal(2L, df.Width)
                Assert.Equal(5L, df.Int("id", 4).Value) // 验证最后一行

            testMemoryMode()

            // =================================================================
            // Mode 3: Stream Mode
            // =================================================================
            let testStreamMode () =
                use fs = File.OpenRead path
                
                use df = DataFrame.ReadIpc fs

                Assert.Equal(5L, df.Rows)
                Assert.Equal("E", df.String("val", 4).Value)

            testStreamMode()

        finally
            // ---------------------------------------------------
            // 清理战场
            // ---------------------------------------------------
            if File.Exists path then File.Delete path
            if File.Exists tempStub then File.Delete tempStub
    [<Fact>]
    member _.``Lazy: Scan IPC (All Modes & UnifiedArgs)`` () =
        // ---------------------------------------------------
        // 1. 准备基准数据
        // ---------------------------------------------------
        let tempStub = Path.GetTempFileName()
        let path = Path.ChangeExtension(tempStub, ".ipc")

        try
            // 创建原始 DataFrame: id (Int32), val (String)
            let s1 = Series.create("id", [1; 2; 3; 4; 5])
            let s2 = Series.create("val", ["A"; "B"; "C"; "D"; "E"])
            
            use dfOrig = DataFrame.create [s1; s2]
            dfOrig.Lazy().SinkIpc(
                path, 
                compression = IpcCompression.LZ4, 
                maintainOrder = true
            )

            // =================================================================
            // 2. File Mode (测试 nRows/PreSlice & RowIndex)
            // =================================================================
            let testFileMode () =
                // 限制只读前 3 行，并生成行号列 "idx_col"
                use lf = LazyFrame.ScanIpc(
                    path, 
                    nRows=3UL, 
                    rowIndexName="idx_col"
                )
                
                use df = lf.Collect()

                // [验证点 1] nRows 生效 (Rust PreSlice)
                Assert.Equal(3L, df.Rows)
                
                // [验证点 2] RowIndex 列存在
                let cols = df.ColumnNames
                Assert.Contains("idx_col", cols)

                Assert.Equal(3L, df.Int("id", 2).Value)
                Assert.Equal("C", df.String("val", 2).Value)

            testFileMode()

            // =================================================================
            // 3. Memory Mode (Bytes) - 验证 Rust ScanSources
            // =================================================================
            let bytes = File.ReadAllBytes path

            let testMemoryMode () =
                // 直接对内存 Bytes 建立 Lazy Plan
                use lf = LazyFrame.ScanIpc bytes
                
                // 加一个 Filter 证明是 Lazy 的
                use df = lf.Filter(col "id" .> pl.lit 3 ).Collect()

                // id > 3 -> 4, 5 (2 rows)
                Assert.Equal(2L, df.Rows)
                Assert.Equal(5L, df.Int("id", 1).Value)

            testMemoryMode()

            // =================================================================
            // 4. Stream Mode
            // =================================================================
            let testStreamMode () =
                use fs = File.OpenRead path
                
                // API 层会将 Stream -> MemoryStream -> Bytes -> ScanSources
                use lf = LazyFrame.ScanIpc fs
                use df = lf.Collect()

                Assert.Equal(5L, df.Rows)
                Assert.Equal("E", df.String("val", 4).Value)

            testStreamMode()

        finally
            // ---------------------------------------------------
            // 清理战场
            // ---------------------------------------------------
            if File.Exists path then File.Delete path
            if File.Exists tempStub then File.Delete tempStub