namespace Polars.FSharp.Tests

open Xunit
open Polars.FSharp
open System
open System.IO

type DisposableFile (extension: string, ?content: string) =
    let ext = if extension.StartsWith(".") then extension else "." + extension
    let path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString() + ext)
    
    do
        match content with
        | Some text -> File.WriteAllText(path, text) // 模式 A: 写入内容 (CSV/JSON)
        | None -> () // 模式 B: 仅生成路径 (Parquet/IPC Write)

    member _.Path = path

    interface IDisposable with
        member _.Dispose() =
            try 
                if File.Exists(path) then File.Delete(path)
            with _ -> ()
type UserRecord = {
        name: string
        age: int          // Int64 -> Int32
        score: float option // Nullable Float
        joined: System.DateTime option // Timestamp -> DateTime
    }
[<CLIMutable>]
type SensorData = {
    Id: int
    Value: string
    Timestamp: DateTime
}

// 场景 2: 过滤测试
[<CLIMutable>]
type Student = {
    Id: int
    Group: string
    Score: double
}

// 场景 3: Join 测试 (Multi-pass)
[<CLIMutable>]
type JoinItem = {
    Key: int
    Val: int
}
type ``Basic Functionality Tests`` () =

    [<Fact>]
    member _.``Can read CSV and count rows/cols`` () =
        use csv = new TempCsv "name,age,birthday\nAlice,30,2022-11-01\nBob,25,2025-12-03"
        
        let df = DataFrame.ReadCsv (path=csv.Path)
        
        Assert.Equal(2L, df.Rows)    // 注意：现在 Rows 返回的是 long (int64)
        Assert.Equal(3L, df.Columns) // 注意：现在 Columns 返回的是 long
    [<Fact>]
    member _.``IO: Advanced CSV Reading (Schema, Skip, Dates)`` () =
        let path = "advanced_test.csv"
        try
            let content = """IGNORE_THIS_LINE
id;date_col;val_col
007;2023-01-01;99.9
008;2023-12-31;10.5"""
            System.IO.File.WriteAllText(path, content)

            // [修改] 调用 DataFrame.ReadCsv
            use df = DataFrame.ReadCsv(
                path,
                skipRows = 1,
                separator = ';',
                tryParseDates = true,
                schema = Map [("id", DataType.String)]
            )

            Assert.Equal(2L, df.Rows)
            Assert.Equal("str", df.Column("id").DtypeStr)
            Assert.Equal("007", df.String("id", 0).Value)
            Assert.Equal(99.9, df.Float("val_col", 0).Value)

        finally
            if System.IO.File.Exists path then System.IO.File.Delete path
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
        Assert.Equal(4, df2.Schema.Count)

    [<Fact>]
    member _.``IO: Write & Read IPC/JSON`` () =
        // 准备路径托管
        use ipcFile = new DisposableFile ".ipc"
        use jsonFile = new DisposableFile ".json"

        // 1. 准备数据
        let s1 = Series.create("a", [1; 2; 3])
        let s2 = Series.create("b", ["x"; "y"; "z"])
        use df = DataFrame.create [s1; s2]

        // 2. 测试 IPC (Feather)
        df.WriteIpc ipcFile.Path |> ignore
        Assert.True(File.Exists ipcFile.Path, "IPC file not found")
        
        use dfIpc = DataFrame.ReadIpc ipcFile.Path
        Assert.Equal(3L, dfIpc.Rows)
        Assert.Equal("x", dfIpc.String("b", 0).Value)

        // 3. 测试 JSON
        df.WriteJson jsonFile.Path |> ignore
        Assert.True(File.Exists jsonFile.Path, "JSON file not found")
        
        use dfJson = DataFrame.ReadJson jsonFile.Path
        Assert.Equal(3L, dfJson.Rows)
        Assert.Equal(2L, dfJson.Int("a", 1).Value)
    [<Fact(Skip = "Sink bug, will be skiped")>]
    member _.``Streaming: Debug Sink`` () =
        // 1. 准备数据
        use csv = new DisposableFile(".csv", "a,b\n1,2\n3,4")
        use parquetEager = new DisposableFile(".parquet")
        use parquetSink = new DisposableFile(".parquet")

        printfn "CSV Path: %s" csv.Path
        printfn "Target Sink Path: %s" parquetSink.Path

        // Step A: 验证 LazyFrame 能否读取数据 (Collect)
        // 如果这里挂了，说明 scanCsv 没读到文件，或者 CSV 格式 Polars 不认
        let lf = LazyFrame.ScanCsv(csv.Path)
        use df = lf.Collect()
        
        Assert.Equal(2L, df.Rows)
        printfn "Step A: Collect Success. Rows: %d" df.Rows

        // Step B: 验证 Eager WriteParquet 是否正常
        // 如果这里挂了，说明是文件系统/权限问题，与 Lazy 无关
        df.WriteParquet(parquetEager.Path) |> ignore
        Assert.True(System.IO.File.Exists parquetEager.Path, "Step B: Eager Write Failed")
        printfn "Step B: Eager Write Success"

        // Step C: 验证 Lazy Sink (使用新的 LazyFrame)
        // 注意：之前的 lf 已经被 Collect 消耗了 (虽然我们 CloneHandle 了，但为了纯净环境重新 scan)
        LazyFrame.ScanCsv(csv.Path)
            .SinkParquet parquetSink.Path
    
        Assert.True(System.IO.File.Exists parquetSink.Path, "Step C: Lazy Sink Failed")
        printfn "Step C: Lazy Sink Success"
    [<Fact>]
    member _.``Metadata: Schema and Dtype`` () =
        // 1. 创建 DataFrame
        use s1 = Series.create("id", [1; 2; 3])
        use s2 = Series.create("score", [1.1; 2.2; 3.3])
        use s3 = Series.create("is_active", [true; false; true])
        
        use df = DataFrame.create [s1; s2; s3]

        // 2. 验证 Series Dtype
        Assert.Equal("i32", s1.DtypeStr)   // F# int 是 Int32
        Assert.Equal("f64", s2.DtypeStr) // F# float 是 double (Float64)
        Assert.Equal("bool", s3.DtypeStr)

        // 3. 验证 DataFrame Schema
        let schema = df.Schema
        Assert.Equal(3, schema.Count)
        Assert.Equal(DataType.Int32, schema.["id"])
        Assert.Equal(DataType.Float64, schema.["score"])
        Assert.Equal(DataType.Boolean, schema.["is_active"])
        
        // 4. 打印看看效果
        Console.WriteLine "------Test DataFrame PrintSchema START------"
        df.PrintSchema()
        Console.WriteLine "------Test DataFrame PrintSchema END------"
    [<Fact>]
    member _.``Lazy Introspection: Schema and Explain`` () =
        use csv = new TempCsv "a,b\n1,2"
        let lf = LazyFrame.ScanCsv (path=csv.Path, tryParseDates=false)
        
        let lf2 = 
            lf 
            |> Polars.withColumnLazy (
                (Polars.col "a" * Polars.lit 2).Alias "a_double"
            )
            |> Polars.filterLazy (Polars.col "b" .> Polars.lit 0)

        // 1. 验证 Schema (使用 Map API，更加精准)
        let schema = lf2.Schema // 类型是 Map<string, string>
        
        // 验证列名是否存在 (Key)
        Assert.True(schema.ContainsKey "a")
        Assert.True(schema.ContainsKey "b")
        Assert.True(schema.ContainsKey "a_double")
        
        // 验证列类型 (Value)
        // Polars 读取 CSV 整数默认是 Int64
        Assert.Equal("i64", schema.["a"])
        Assert.Equal("i64", schema.["a_double"])

        // 2. 验证 Explain 和 Optimization
        let plan = lf2.Explain false
        printfn "\n=== Query Plan ===\n%s\n==================" plan
        Assert.Contains("FILTER", plan) 
        Assert.Contains("WITH_COLUMNS", plan)

        let planOptimized = lf2.Explain true
        printfn "\n=== Query Plan Optimized===\n%s\n==================" planOptimized
        Assert.Contains("SELECTION", planOptimized) 
    [<Fact>]
    member _.``Arrow Integration: Import C# Arrow Data to Polars`` () =
        // 1. 在 C# 端原生构建一个 RecordBatch
        // 模拟场景：数据来自 .NET 数据库或计算结果
        let builder = new Apache.Arrow.Int64Array.Builder()
        builder.Append 100L |> ignore
        builder.Append 200L |> ignore
        builder.AppendNull() |> ignore // 测试空值
        let colArray = builder.Build()

        let field = new Apache.Arrow.Field("num", new Apache.Arrow.Types.Int64Type(), true)
        let schema = new Apache.Arrow.Schema([| field |], null)
        
        use batch = new Apache.Arrow.RecordBatch(schema, [| colArray |], 3)

        // 2. 传给 Polars (C# -> Rust)
        // 这一步应该能成功，因为内存是 C# 分配的，Exporter 能够处理
        let df = DataFrame.FromArrow batch
        df |> Polars.show |> ignore
        // 3. 验证
        Assert.Equal(3L, df.Rows)
        Assert.Equal(100L, df.Int("num", 0).Value)
        Assert.Equal(200L, df.Int("num", 1).Value)
        Assert.True(df.Int("num", 2).IsNone) // 验证空值传递
    [<Fact>]
    member _.``Series: AsSeq Lifecycle & Complex Types`` () =
    // 1. 创建包含复杂类型的 Series
        let data = [
            Some(DateTime(2023, 1, 1))
            None
            Some(DateTime(2024, 1, 1))
        ]
        use s = Series.ofSeq("dt", data)

        // 2. 惰性序列 (此时 Arrow Array 还没创建)
        let seqData = s.AsSeq<DateTime>()

        // 3. 触发迭代 (Arrow Array 创建 -> 读取 -> 释放)
        let listData = seqData |> Seq.toList

        Assert.Equal(3, listData.Length)
        Assert.Equal(Some(DateTime(2023, 1, 1)), listData.[0])
        Assert.True(listData.[1].IsNone)
    [<Fact>]
    member _.``Ingestion: Create DataFrame from F# Records`` () =
        // 1. 定义数据
        let data = [
            { name = "Alice"; age = 30; score = Some 99.5; joined = Some (System.DateTime(2023,1,1)) }
            { name = "Bob"; age = 25; score = None; joined = None }
        ]

        // 2. 转换 (ofRecords)
        let df = DataFrame.ofRecords data

        // 3. 验证结构
        Assert.Equal(2L, df.Rows)
        Assert.Equal(4L, df.Columns)

        // 4. 验证数据
        
        // --- String (Alice) ---
        // [修复] df.String 返回 string option，必须用 .Value 取出里面的 string 才能和 "Alice" 比较
        Assert.Equal("Alice", df.String("name", 0).Value) 
        
        // --- Int (Alice) ---
        Assert.Equal(30L, df.Int("age", 0).Value) 
        
        // --- Float (Alice) ---
        Assert.Equal(99.5, df.Float("score", 0).Value)
        
        // --- DateTime (Alice) ---
        // [修复] 先取出 Option 里的值，再判断包含关系
        let joinedAlice = df.String("joined", 0).Value
        Assert.Contains("2023-01-01", joinedAlice)

        // --- Bob (验证 Null) ---
        Assert.Equal("Bob", df.String("name", 1).Value)
        Assert.Equal(25L, df.Int("age", 1).Value)
        
        // Score 是 None
        Assert.True(df.Float("score", 1).IsNone)
        
        // Joined 是 None
        // [修复] 不要用 = null 判断 Option，要用 .IsNone
        let joinedBob = df.String("joined", 1)
        Assert.True joinedBob.IsNone
    [<Fact>]
    member _.``DataFrame: Create from Series`` () =
        // 1. 创建两个独立的 Series
        use s1 = Series.create("id", [1; 2; 3])
        use s2 = Series.create("name", ["a"; "b"; "c"])

        // 2. 组合成 DataFrame
        use df = DataFrame.create [s1; s2]

        // 3. 验证
        Assert.Equal(3L, df.Rows)
        Assert.Equal(2L, df.Columns)
        Assert.Equal<string seq>(["id"; "name"], df.ColumnNames)
        
        // 4. [关键] 验证原来的 Series 依然可用 (未被 Move)
        // 如果 Rust 端不是 Clone 而是 Move，这里就会崩
        Assert.Equal(3L, s1.Length)
        
        // 5. 打印看看
        Polars.show df |> ignore
    [<Fact>]
    member _.``Convenience: Drop, Rename, DropNulls, Sample`` () =
        // Test DataFrame
        let s1 = Series.create("a", [Some 1; Some 2; None])
        let s2 = Series.create("b", ["x"; "y"; "z"])
        use df = DataFrame.create [s1; s2]

        // 1. Drop
        let dfDrop = df.Drop "a"
        Assert.Equal(1L, dfDrop.Columns)
        Assert.Equal<string seq>(["b"], dfDrop.ColumnNames)
        Assert.Equal(2L, df.Columns) |> ignore
        Assert.Equal<string seq>(["a"; "b"], df.ColumnNames)

        // 2. Rename
        let dfRenamed = df.Rename("b", "b_new")
        Assert.Equal<string seq>(["a"; "b_new"], dfRenamed.ColumnNames)

        // 3. DropNulls
        let dfClean = df.DropNulls()
        Assert.Equal(2L, dfClean.Rows) // 第三行 a=null 被删了
        Assert.Equal(Some 1L, dfClean.Int("a", 0))
        Assert.Equal(Some 2L, dfClean.Int("a", 1))

        // 4. Sample (n=1)
        let dfSample = df.Sample(n=1, seed=12345UL)
        Assert.Equal(1L, dfSample.Rows)
        
        // 5. Sample (frac=0.5) -> 3 * 0.5 = 1.5 -> 1 or 2 rows depending on algo, usually round/floor
        // Polars sample_frac usually works well. 3 * 0.6 = 1.8. 
        // 让我们试个明确的
        let dfSampleFrac = df.Sample(frac=1.0) // 全量乱序
        Assert.Equal(3L, dfSampleFrac.Rows)
    [<Fact>]
    member _.``Full Temporal Types: Create & Retrieve`` () =
        let date = DateOnly(2023, 1, 1)
        let time = TimeOnly(12, 30, 0)
        let dur = TimeSpan.FromHours(1.5) // 90 mins

        // 1. Series Create
        use sDate = Series.create("d", [date])
        use sTime = Series.create("t", [time])
        use sDur = Series.create("dur", [dur])

        // 2. 验证类型字符串
        Assert.Equal("date", sDate.DtypeStr)
        Assert.Equal("time", sTime.DtypeStr)
        Assert.Equal("duration[μs]", sDur.DtypeStr) // Polars 默认 Duration 是 us

        // 3. 验证读取 (Scalar Access)
        Assert.Equal(date, sDate.Date(0).Value)
        Assert.Equal(time, sTime.Time(0).Value)
        Assert.Equal(dur, sDur.Duration(0).Value)

        // 4. DataFrame.ofRecords 集成测试
        let records = [
            {| Id = 1; DoB = date; WakeUp = time; Shift = dur |}
        ]
        let df = DataFrame.ofRecords records
        
        Assert.Equal(date, df.Date("DoB", 0).Value)
        Assert.Equal(time, df.Time("WakeUp", 0).Value)
        Assert.Equal(dur, df.Duration("Shift", 0).Value)
    [<Fact>]
    member _.``Expr: TimeZone Ops (Convert & Replace)`` () =
        // 1. 创建 Naive Datetime
        let s = Series.create("ts", [DateTime(2023, 1, 1, 12, 0, 0)])
        use df = DataFrame.create [s]

        let res = 
            df
            |> Polars.select([
                Polars.col "ts"
                
                // 1. Convert (Naive -> Error, so we must Replace first)
                // 先 Replace 设置为 UTC，再 Convert 到 Shanghai
                Polars.col("ts")
                    .Dt.ReplaceTimeZone("UTC")
                    .Dt.ConvertTimeZone("Asia/Shanghai")
                    .Alias "shanghai"

                // 2. Replace with Strategy (Full signature check)
                // "raise" is default, explicitly passing it to verify API binding
                Polars.col("ts")
                    .Dt.ReplaceTimeZone("Europe/London", ambiguous="earliest", nonExistent="null")
                    .Alias "london_explicit"
                
                // 3. Unset TimeZone (Make Naive)
                Polars.col("ts")
                    .Dt.ReplaceTimeZone("UTC")
                    .Dt.ReplaceTimeZone(None) // Set back to None
                    .Alias "naive"
            ])

        // 验证 Shanghai (+08:00)
        let shRow = res.Column("shanghai").AsSeq<DateTimeOffset>() |> Seq.head |> Option.get
        Assert.Equal(TimeSpan.FromHours 8, shRow.Offset)
        Assert.Equal(20, shRow.Hour) // 12:00 UTC -> 20:00 Shanghai

        // 验证 London (Naive 12:00 -> London 12:00 +00:00 in Jan)
        let ldRow = res.Column("london_explicit").AsSeq<DateTimeOffset>() |> Seq.head |> Option.get
        Assert.Equal(0, ldRow.Offset.Hours) 
        
        // 验证 Naive (Unset)
        // 读取出来的应该是 UTC 的 DateTime,我们约定进入polars的一定是UTC
        let naiveRow = res.Column("naive").AsSeq<DateTime>() |> Seq.head |> Option.get
        Assert.Equal(DateTimeKind.Unspecified, naiveRow.Kind)
    [<Fact>]
    member _.``Conversion: DataFrame -> Lazy -> DataFrame`` () =
        // 1. 创建 Eager DF
        use df = DataFrame.ofRecords [ { name = "Qinglei"; age = 18 ; score = Some 99.5; joined = Some (System.DateTime(2023,1,1)) }; { name = "Someone"; age = 20; score = None; joined = None } ]
        
        // 2. 转 Lazy 并执行操作
        // 注意：df 在这里应该依然有效，因为 .Lazy() 是 Clone
        let lf = df.Lazy()
        
        let res = 
            lf
            |> Polars.filterLazy(Polars.col "age" .> Polars.lit 18)
            |> Polars.collect

        // 3. 验证结果
        Assert.Equal(1L, res.Rows)
        Assert.Equal(20L, res.Int("age", 0).Value)
        
        // 4. 验证原 DF 依然存活
        Assert.Equal(2L, df.Rows)
    [<Fact>]
    member _.``EDA: Describe (Manual Implementation)`` () =
        let s = Series.create("nums", [1.0; 2.0; 3.0; 4.0; 5.0])
        use df = DataFrame.create [s]

        let desc = df.Describe()
        
        Polars.show desc |> ignore
        
        // 验证行数 (9个指标)
        Assert.Equal(9L, desc.Rows)
        
        // 验证 mean (第3行，第2列)
        // 注意：我们每一行是一个单独的 Select，顺序由 metrics 列表决定
        // 0: count, 1: null_count, 2: mean
        let meanVal = desc.Float("nums", 2).Value
        Assert.Equal(3.0, meanVal)
        
        // 验证 std
        let stdVal = desc.Float("nums", 3).Value
        // 1..5 的 std 是 1.5811...
        Assert.True(abs(stdVal - 1.58113883) < 0.0001)
    [<Fact>]
    member _.``Reshaping: Concat Diagonal`` () =
        // df1: [a, b]
        use csv1 = new TempCsv "a,b\n1,2"
        // df2: [a, c] (注意：没有 b，多了 c)
        use csv2 = new TempCsv "a,c\n3,4"

        let df1 = DataFrame.ReadCsv (path=csv1.Path, tryParseDates=false)
        let df2 = DataFrame.ReadCsv (path=csv2.Path, tryParseDates=false)

        // 对角拼接
        // 结果应该包含 3 列: [a, b, c]
        // Row 1 (来自 df1): a=1, b=2, c=null
        // Row 2 (来自 df2): a=3, b=null, c=4
        let res = Polars.concatDiagonal [df1; df2]

        Assert.Equal(2L, res.Rows)
        Assert.Equal(3L, res.Columns)
        
        // 验证列名
        let cols = res.ColumnNames
        Assert.Contains("a", cols)
        Assert.Contains("b", cols)
        Assert.Contains("c", cols)

        // 验证数据
        // 第一行 (df1)
        Assert.Equal(1L, res.Int("a", 0).Value)
        Assert.Equal(2L, res.Int("b", 0).Value)
        Assert.True(res.Int("c", 0).IsNone) // c 应该是 null

        // 第二行 (df2)
        Assert.Equal(3L, res.Int("a", 1).Value)
        Assert.True(res.Int("b", 1).IsNone) // b 应该是 null
        Assert.Equal(4L, res.Int("c", 1).Value)
    [<Fact>]
    member _.``Scalar Access: IsNullAt`` () =
        // 准备数据: [1, null, 3]
        use s = Series.create("a", [Some 1; None; Some 3])
        use df = DataFrame.create [s]

        // Series 验证
        Assert.False(s.IsNullAt 0)
        Assert.True(s.IsNullAt 1)
        Assert.False(s.IsNullAt 2)
        Assert.False(s.IsNullAt 999) // 越界返回 false

        // DataFrame 验证
        Assert.False(df.IsNullAt("a", 0))
        Assert.True(df.IsNullAt("a", 1))
    [<Fact>]
    member _.``Metadata: NullCount`` () =
        // 1. 创建包含 Null 的 Series
        // 数据: 1, null, 3, null
        let s = Series.create("a", [Some 1; None; Some 3; None])
        
        // 2. 验证 Series.NullCount
        Assert.Equal(2L, s.NullCount)
        Assert.Equal(4L, s.Length)

        // 3. 验证 DataFrame Helper
        use df = DataFrame.create [s]
        Assert.Equal(2L, df.NullCount "a")
 
    [<Fact>]
    member _.``Async: Collect LazyFrame`` () =
        // 构造一个稍微大一点的计算任务
        use csv1 = new TempCsv "a,b\n1,2\n3,4"
        let df = 
            LazyFrame.ScanCsv (path=csv1.Path, tryParseDates=false)
            |> Polars.filterLazy (Polars.col "a" .> Polars.lit 0)
            |> Polars.collectAsync // 返回 Async<DataFrame>
            |> Async.RunSynchronously // 在测试里阻塞等待结果

        Assert.Equal(2L, df.Rows)
        Assert.Equal(1L, df.Int("a", 0).Value)
    [<Fact>]
    member _.``Series: Arithmetic & Aggregation (Pandas Style)`` () =
        // 1. 准备数据
        use demand = Series.create("demand", [100.0; 200.0; 300.0])
        use weight = Series.create("weight", [0.5; 1.5; 1.0])

        // 2. Pandas 风格计算：加权平均
        // weighted_mean = (demand * weight).Sum() / weight.Sum()
        
        let sProd = demand * weight    // [50.0, 300.0, 300.0]
        let sSumProd = sProd.Sum()     // [650.0]
        let sSumW = weight.Sum()       // [3.0]
        
        // Series 之间的除法 (Broadcasting: Scalar / Scalar)
        let sWeightedMean = sSumProd / sSumW 
        
        // 结果应该是一个长度为1的 Series
        Assert.Equal(1L, sWeightedMean.Length)
        
        // 验证数值: 650 / 3 = 216.666...
        let valMean = sWeightedMean.Float(0).Value
        Assert.True(abs(valMean - 216.6666) < 0.001)

        // 3. 逻辑运算与过滤
        // weeks_with_demand = (demand > 0).sum()
        
        // (demand > 0) 返回 Boolean Series
        // .Sum() 在 Boolean Series 上通常等价于 count true，但 Polars Series Sum 可能返回 Int/Float
        // 让我们看看 boolean sum 的行为
        // Polars Rust boolean.sum() returns u32/u64 usually.
        
        let mask = demand .> 0.0 // 广播比较
        // Polars.NET Sum() 返回的是 Series。对于 Bool，Rust sum 返回的是 number。
        // 我们验证一下类型
        let countPos = mask.Sum()
        // demand全是 > 0，所以应该是 3
        
        // 注意：Sum 返回的可能是 Int 或 Float，视底层实现而定
        // Polars boolean sum returns UInt32 usually.
        // 我们通过 .Float 或 .Int 尝试获取
        // 简单起见，先转 f64 再拿
        let countVal = countPos.Cast(DataType.Float64).Float(0).Value
        Assert.Equal(3.0, countVal)
        
        // zero_ratio = (demand == 0).mean()
        let zeroMask = demand .= 0.0
        let zeroRatio = zeroMask.Mean() // Mean on boolean = ratio of true
        
        // 0 / 3 = 0.0
        Assert.Equal(0.0, zeroRatio.Float(0).Value)
    [<Fact>]
    member _.``Series: Arithmetic & Aggregation (F# Pipeline Style)`` () =
        // 1. 准备数据
        use demand = Series.create("demand", [100.0; 200.0; 300.0])
        use weight = Series.create("weight", [0.5; 1.5; 1.0])

        // 2. F# 管道风格计算：加权平均
        // 逻辑流：demand 乘以 weight -> 求和 -> 除以 (weight 求和)
        
        let sWeightedMean = 
            demand
            |> Series.mul weight          // Element-wise multiplication
            |> Series.sum                 // Sum result
            |> Series.div (weight |> Series.sum) // Divide by scalar (series of len 1)

        // 验证
        Assert.Equal(1L, sWeightedMean.Length)
        let valMean = sWeightedMean.Float(0).Value
        Assert.True(abs(valMean - 216.6666) < 0.001)

        // 3. 逻辑运算与过滤
        
        // A. 统计需求大于 0 的周数
        // 逻辑流：demand -> 大于 0.0 -> 求和 -> 转 Float -> 取值
        let countVal = 
            demand
            |> Series.gtLit 0.0           // Broadcasting comparison (> 0.0)
            |> Series.sum                 // Count true values
            |> Series.cast DataType.Float64 
            |> fun s -> s.Float(0).Value  // 最后的取值也可以写个 helper

        Assert.Equal(3.0, countVal)

        // B. 统计零需求占比
        // 逻辑流：demand -> 等于 0.0 -> 求均值
        let zeroRatio = 
            demand
            |> Series.eqLit 0.0           // Broadcasting comparison (= 0.0)
            |> Series.mean                // Mean of boolean
            |> fun s -> s.Float(0).Value

        Assert.Equal(0.0, zeroRatio)
    [<Fact>]
    member _.``Series: NaN and Infinity Checks`` () =
        // 1. 准备数据: [1.0, NaN, Inf, -Inf, 5.0]
        let s = Series.create("f", [1.0; Double.NaN; Double.PositiveInfinity; Double.NegativeInfinity; 5.0])

        // 2. IsNan -> [F, T, F, F, F]
        let maskNan = s.IsNan()
        Assert.Equal(Some true, maskNan.Bool 1) // NaN
        Assert.Equal(Some false, maskNan.Bool 0)

        // 3. IsInfinite -> [F, F, T, T, F]
        let maskInf = s.IsInfinite()
        Assert.Equal(Some true, maskInf.Bool 2) // +Inf
        Assert.Equal(Some true, maskInf.Bool 3) // -Inf
        Assert.Equal(Some false, maskInf.Bool 1) // NaN is NOT Infinite

        // 4. IsFinite -> [T, F, F, F, T]
        let maskFin = s.IsFinite()
        Assert.Equal(Some true, maskFin.Bool 0)
        Assert.Equal(Some false, maskFin.Bool 1) // NaN not finite
        Assert.Equal(Some false, maskFin.Bool 2) // Inf not finite
    // ---------------------------------------------------
    // Streaming Tests
    // ---------------------------------------------------

    [<Fact>]
    member _.``Stream: Eager Ingestion (ofSeqStream)`` () =
        // 1. 模拟一个较大的数据源 (10万行)
        // 使用 Seq.init 惰性生成，不占内存
        let count = 100_000
        let data = Seq.init count (fun i -> 
            { Id = i; Value = $"Val_{i}"; Timestamp = DateTime(2023, 1, 1).AddSeconds(float i) }
        )

        // 2. 流式导入 (Batch Size = 10,000)
        // 这一步应该非常快，且内存占用极低
        use df = DataFrame.ofSeqStream(data, batchSize = 10_000)

        // 3. 验证
        Assert.Equal(int64 count, df.Rows)
        Assert.Equal("Val_99999", df.Column("Value").AsSeq<string>() |> Seq.last |> Option.get)
        
        // 验证 Schema 是否正确推断 (Timestamp)
        Assert.Equal(DataType.Datetime, df.Schema.["Timestamp"])

    [<Fact>]
    member _.``Stream: Lazy Scan (scanSeq) with Filter`` () =
        // 1. 数据源
        let data = [
            { Id = 1; Group = "A"; Score = 10.0 }
            { Id = 2; Group = "B"; Score = 20.0 }
            { Id = 3; Group = "A"; Score = 30.0 }
        ]

        // 2. Lazy Scan -> Filter -> Collect
        // Polars 应该会将 Filter 下推，并在流式读取时就应用过滤
        let res = 
            LazyFrame.scanSeq data
                |> Polars.filterLazy(Polars.col "Group" .== Polars.lit "A")
                |> Polars.collectStreaming

        // 3. 验证
        Assert.Equal(2L, res.Rows) // 只剩下 Id 1 和 3
        Assert.Equal(1L, res.Int("Id", 0).Value)
        Assert.Equal(3L, res.Int("Id", 1).Value)

    [<Fact>]
    member _.``Stream: Lazy Multi-pass Scan (Self Join)`` () =
        // 这是一个的高级测试
        // Self Join 需要扫描同一份数据源两次
        // 这将验证我们的 ArrowStreamInterop.Factory 机制是否能正确重置枚举器
        
        let data = Seq.init 10 (fun i -> { Key = i % 3; Val = i }) // Key: 0, 1, 2 重复
        
        let lf = LazyFrame.scanSeq data
        
        // Self Join: lf.Join(lf, on="Key")
        // Rust 引擎会调用两次 StreamFactoryCallback
        let res = 
            lf
            |> Polars.joinLazy lf [Polars.col "Key"] [Polars.col "Key"] Left
            |> Polars.collect

        // 简单验证行数 (笛卡尔积会膨胀)
        // 0: 4 items -> 4*4 = 16
        // 1: 3 items -> 3*3 = 9
        // 2: 3 items -> 3*3 = 9
        // Total = 34
        Assert.Equal(34L, res.Rows)