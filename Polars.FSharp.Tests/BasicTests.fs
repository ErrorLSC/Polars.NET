namespace Polars.FSharp.Tests

open Xunit
open Polars.FSharp

type UserRecord = {
        name: string
        age: int          // Int64 -> Int32
        score: float option // Nullable Float
        joined: System.DateTime option // Timestamp -> DateTime
    }
type ``Basic Functionality Tests`` () =

    [<Fact>]
    member _.``Can read CSV and count rows/cols`` () =
        use csv = new TempCsv "name,age,birthday\nAlice,30,2022-11-01\nBob,25,2025-12-03"
        
        let df = Polars.readCsv csv.Path None
        
        Assert.Equal(2L, df.Rows)    // 注意：现在 Rows 返回的是 long (int64)
        Assert.Equal(3L, df.Columns) // 注意：现在 Columns 返回的是 long

    [<Fact>]
    member _.``Can read&write Parquet`` () =
        // 这一步需要你有一个真实的 parquet 文件，或者先用 writeParquet 生成一个
        use csv = new TempCsv "a,b\n1,2"
        let df = Polars.readCsv csv.Path None
        
        let tmpParquet = System.IO.Path.GetTempFileName()
        try
            // 测试 Write -> Read 闭环
            df |> Polars.writeParquet tmpParquet |> ignore
            let df2 = Polars.readParquet tmpParquet
            Assert.Equal(df.Rows, df2.Rows)
        finally
            System.IO.File.Delete tmpParquet
    [<Fact>]
    member _.``Streaming, Sink(untested)`` () =
        // 1. 准备宽表数据 (Sales Data)
        // Year, Q1, Q2
        use csv = new TempCsv "year,Q1,Q2\n2023,100,200\n2024,300,400"

        let lf = Polars.scanCsv csv.Path None
        let tmpParquet = System.IO.Path.GetTempFileName()
        System.IO.File.Delete tmpParquet

        try
            // Lazy Unpivot -> Sink Parquet
            lf
            |> Polars.unpivotLazy ["year"] ["Q1"; "Q2"] (Some "quarter") (Some "revenue")
            |> Polars.sinkParquet tmpParquet

            // 验证文件生成
            let tmpParquet = System.IO.Path.Combine(System.IO.Path.GetTempPath(), System.Guid.NewGuid().ToString() + ".parquet")
            
            // 读回来验证行数
            // let checkDf = Polars.readParquet tmpParquet
            // Assert.Equal(4L, checkDf.Rows)

            let streamedDf = lf |> Polars.collectStreaming 
            Assert.Equal(2L, streamedDf.Rows)

        finally
            if System.IO.File.Exists tmpParquet then
                System.IO.File.Delete tmpParquet
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
        Assert.Equal("i32", schema.["id"])
        Assert.Equal("f64", schema.["score"])
        Assert.Equal("bool", schema.["is_active"])
        
        // 4. 打印看看效果
        df.PrintSchema()
    [<Fact>]
    member _.``Lazy Introspection: Schema and Explain`` () =
        use csv = new TempCsv "a,b\n1,2"
        let lf = Polars.scanCsv csv.Path None
        
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
        let df = Polars.fromArrow batch
        df |> Polars.show |> ignore
        // 3. 验证
        Assert.Equal(3L, df.Rows)
        Assert.Equal(100L, df.Int("num", 0).Value)
        Assert.Equal(200L, df.Int("num", 1).Value)
        Assert.True(df.Int("num", 2).IsNone) // 验证空值传递

    [<Fact>]
    member _.``Materialization: DataFrame to Records`` () =
        let csv = "name,age,score,joined\nAlice,30,99.5,2023-01-01\nBob,25,,\n"
        use tmp = new TempCsv(csv)
        let df = Polars.readCsv tmp.Path (Some true)

        let records = df.ToRecords<UserRecord>()

        Assert.Equal(2, records.Length)
        
        let alice = records.[0]
        Assert.Equal("Alice", alice.name)
        Assert.Equal(30, alice.age)
        Assert.Equal(Some 99.5, alice.score)
        Assert.Equal(System.DateTime(2023,1,1), alice.joined.Value)

        let bob = records.[1]
        Assert.Equal("Bob", bob.name)
        Assert.Equal(None, bob.score)

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
    member _.``Reshaping: Concat Diagonal`` () =
        // df1: [a, b]
        use csv1 = new TempCsv "a,b\n1,2"
        // df2: [a, c] (注意：没有 b，多了 c)
        use csv2 = new TempCsv "a,c\n3,4"

        let df1 = Polars.readCsv csv1.Path None
        let df2 = Polars.readCsv csv2.Path None

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
    member _.``Async: Collect LazyFrame`` () =
        // 构造一个稍微大一点的计算任务
        use csv1 = new TempCsv "a,b\n1,2\n3,4"
        let df = 
            Polars.scanCsv csv1.Path None
            |> Polars.filterLazy (Polars.col "a" .> Polars.lit 0)
            |> Polars.collectAsync // 返回 Async<DataFrame>
            |> Async.RunSynchronously // 在测试里阻塞等待结果

        Assert.Equal(2L, df.Rows)
        Assert.Equal(1L, df.Int("a", 0).Value)