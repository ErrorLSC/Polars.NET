namespace PolarsFSharp.Tests

open Xunit
open PolarsFSharp

type UserRecord = {
        name: string
        age: int          // Int64 -> Int32
        score: float option // Nullable Float
        joined: System.DateTime option // Timestamp -> DateTime
    }
type ``Basic Functionality Tests`` () =

    [<Fact>]
    member _.``Can read CSV and count rows/cols`` () =
        use csv = new TempCsv("name,age,birthday\nAlice,30,2022-11-01\nBob,25,2025-12-03")
        
        let df = Polars.readCsv csv.Path None
        
        Assert.Equal(2L, df.Rows)    // 注意：现在 Rows 返回的是 long (int64)
        Assert.Equal(3L, df.Columns) // 注意：现在 Columns 返回的是 long

    [<Fact>]
    member _.``Can read&write Parquet`` () =
        // 这一步需要你有一个真实的 parquet 文件，或者先用 writeParquet 生成一个
        use csv = new TempCsv("a,b\n1,2")
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
        use csv = new TempCsv("year,Q1,Q2\n2023,100,200\n2024,300,400")

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

            // 测试 Streaming Collect (虽然数据量小看不出优势，但验证 API 是否崩)
            let streamedDf = lf |> Polars.collectStreaming |> Polars.show
            Assert.Equal(2L, streamedDf.Rows)

        finally
            if System.IO.File.Exists tmpParquet then
                System.IO.File.Delete tmpParquet

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
        builder.Append(100L) |> ignore
        builder.Append(200L) |> ignore
        builder.AppendNull() |> ignore // 测试空值
        let colArray = builder.Build()

        let field = new Apache.Arrow.Field("num", new Apache.Arrow.Types.Int64Type(), true)
        let schema = new Apache.Arrow.Schema([| field |], null)
        
        use batch = new Apache.Arrow.RecordBatch(schema, [| colArray |], 3)

        // 2. 传给 Polars (C# -> Rust)
        // 这一步应该能成功，因为内存是 C# 分配的，Exporter 能够处理
        let df = Polars.fromArrow batch

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