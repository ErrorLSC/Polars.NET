namespace Polars.FSharp.Tests

open Xunit
open Polars.FSharp
open System.IO
open System

type ``Complex Query Tests`` () =
    
    [<Fact>]
    member _.``Join execution (Eager)`` () =
        use users = new TempCsv "id,name\n1,A\n2,B"
        use sales = new TempCsv "uid,amt\n1,100\n1,200\n3,50"

        let uDf = DataFrame.ReadCsv (path=users.Path, tryParseDates=false)
        let sDf = DataFrame.ReadCsv (path=sales.Path, tryParseDates=false)

        let res = 
            uDf 
            |> Polars.join sDf [Polars.col "id"] [Polars.col "uid"] JoinType.Left
        
        // Left join: id 1 (2 rows), id 2 (1 row null match) -> Total 3
        Assert.Equal(3L, res.Rows)

    [<Fact>]
    member _.``Lazy API Chain (Filter -> Collect)`` () =
        use csv = new TempCsv "a,b\n1,10\n2,20\n3,30"
        let lf = LazyFrame.ScanCsv csv.Path
        
        let df = 
            lf
            |> Polars.filterLazy (Polars.col "a" .> Polars.lit 1)
            |> Polars.limit 1u
            |> Polars.collect

        Assert.Equal(1L, df.Rows)

    [<Fact>]
    member _.``GroupBy Queries`` () =
        use csv = new TempCsv "name,birthdate,weight,height\nBen Brown,1985-02-15,72.5,1.77\nQinglei,2025-11-25,70.0,1.80\nZhang,2025-10-31,55,1.75"
        let lf = LazyFrame.ScanCsv csv.Path

        let res = 
            lf 
            |> Polars.groupByLazy
                [(Polars.col "birthdate").Dt.Year() / Polars.lit 10 * Polars.lit 10 |> Polars.alias "decade" ]
                [ Polars.count().Alias "cnt"] 
            |> Polars.sortLazy (Polars.col "decade") false
            |> Polars.collect

        // 验证
        // Row 0: 1980 -> 2
        Assert.Equal(1980L, res.Int("decade", 0).Value)
        // 注意：count() 返回通常是 UInt32 或 UInt64，我们用 Int64 读取是安全的
        Assert.Equal(1L, int64 (res.Int("cnt", 0).Value)) 

        // Row 1: 1990 -> 1
        Assert.Equal(2020L, res.Int("decade", 1).Value)
        Assert.Equal(2L, int64 (res.Int("cnt", 1).Value))

    [<Fact>]
    member _.``Complex Transformation (Selector Exclude)`` () =
        let csvContent = 
            "name,birthdate,weight,height\n" +
            "Zhang San,1985-01-01,70.1234,1.755\n" +
            "Li Si,1988-05-20,60.5678,1.604\n" +
            "Wang Wu,1996-12-31,80.9999,1.859"
        use csv = new TempCsv(csvContent)
        let lf = LazyFrame.ScanCsv csv.Path

        let res = 
            lf
            |> Polars.withColumnsLazy (
                // 1. String Split -> List -> First
                [
                (Polars.col "name").Str.Split(" ").List.First()
                (Polars.col "birthdate").Dt.Year() / Polars.lit 10 * Polars.lit 10 |> Polars.alias "decade"
            ])
            |> Polars.selectLazy [
                // 2. Exclude (all except "ignore_me")
                Polars.all() |> Polars.exclude ["birthdate"] |> Polars.asExpr
            ]
            |> Polars.groupByLazy 
                // Keys
                [ Polars.col "decade" ] 
                // Aggs
                [
                    // Agg A: 名字列表 (Polars 默认行为：非 Key 列在 agg 中会聚合成 List)
                    // 但 Rust 原例写的是 col("name")，如果 context 是 agg，它就是 list
                    // 这里我们显式一点，或者直接传 col "name" 让 Polars 处理
                    Polars.col "name"
                    // Agg B: Weight & Height 的均值 + 四舍五入 + 重命名
                    // Rust 原例: cols(["weight", "height"]).mean().round(2).prefix("avg_")
                    // F# 复刻: 展开写两个 Expr (效果等价)
                    (Polars.col "weight").Mean().Round(2).Name.Prefix("avg_")
                    (Polars.col "height").Mean().Round(2).Name.Prefix("avg_")
                ]
            |> Polars.collect
            |> Polars.sort (Polars.col "decade") false

    // 验证列名 (birthdate 应该没了，新增了 decade 和 avg_ 前缀)
        let cols = res.ColumnNames
        Assert.DoesNotContain("birthdate", cols)
        Assert.Contains("decade", cols)
        Assert.Contains("avg_weight", cols)
        Assert.Contains("avg_height", cols)

        // 验证 Row 0 (1980年代: Zhang, Li)
        Assert.Equal(1980L, res.Int("decade", 0).Value)
        
        // 验证数学运算 (Mean + Round)
        // Weight: (70.1234 + 60.5678) / 2 = 65.3456 -> Round(2) -> 65.35
        let w80 = res.Float("avg_weight", 0).Value
        Assert.Equal(65.35, w80)

        // 验证字符串处理
        // 在 GroupBy 结果中，"name" 列变成了 List<String>
        // 但目前我们的 C# Wrapper 转 Arrow 时，List 列会变成 String (JSON representation) 或者 ListArray
        // 这里我们可以简单验证一下 ToArrow 的行为，或者只验证 Schema
        // (由于我们还没做 List 类型的读取 API，这里暂时跳过内容验证，只要不崩就行)
        
        // 验证 Row 1 (1990年代: Wang)
        Assert.Equal(1990L, res.Int("decade", 1).Value)
        // Weight: 80.9999 -> 81.00
        let w90 = res.Float("avg_weight", 1).Value
        Assert.Equal(81.00, w90)

    [<Fact>]
    member _.``List Ops: Cols, Explode, Join and Read`` () =
        // 数据: 一个人有多个 Tag (空格分隔)
        use csv = new TempCsv "name,tags\nAlice,coding reading\nBob,gaming"
        let lf = LazyFrame.ScanCsv csv.Path

        let res = 
            lf
            |> Polars.withColumnLazy (
                // 1. Split 变成 List
                (Polars.col "tags").Str.Split(" ").Alias "tag_list"
            )
            |> Polars.withColumnLazy (
                // 2. 演示 cols([...]): 同时选中 name 和 tag_list，加上前缀
                // 虽然这里只是演示，通常用于批量数学运算
                Polars.cols ["name"; "tag_list"]
                |> fun e -> e.Name.Prefix("my_")
            )
            |> Polars.withColumnLazy (
                // 3. List Join (还原回去)
                (Polars.col "my_tag_list").List.Join("-").Alias "joined_tags"
            )
            |> Polars.collect

        // 验证 1: cols 产生的前缀
        let cols = res.ColumnNames
        Assert.Contains("my_name", cols)
        Assert.Contains("my_tag_list", cols)

        // 验证 2: List Join
        // coding reading -> coding-reading
        Assert.Equal("coding-reading", res.String("joined_tags", 0).Value)

        // 验证 3: 读取 List (使用新加的 API)
        let aliceTags = res.StringList("my_tag_list", 0)
        Assert.True aliceTags.IsSome
        Assert.Equal<string list>(["coding"; "reading"], aliceTags.Value)

        // 验证 4: Explode (炸裂)
        // Alice 有 2 个 tag，Bob 有 1 个 -> Explode 后应该是 3 行
        let exploded = 
            res 
            |> Polars.select [ Polars.col "my_name"; Polars.col "my_tag_list" ]
            // [修改] 加上列表括号 []
            |> Polars.explode [ Polars.col "my_tag_list" ] 
        
        Assert.Equal(3L, exploded.Rows)
        Assert.Equal("coding", exploded.String("my_tag_list", 0).Value)
        Assert.Equal("reading", exploded.String("my_tag_list", 1).Value)
        Assert.Equal("gaming", exploded.String("my_tag_list", 2).Value)

    [<Fact>]
    member _.``Struct and Advanced List Ops`` () =
        // 构造数据: Alice 考了两次试
        use csv = new TempCsv "name,score1,score2\nAlice,80,90\nBob,60,70"
        let lf = LazyFrame.ScanCsv csv.Path
        let maxCharExpr = 
            (Polars.col "raw_nums").Str.Split(" ")
                .List.Sort(true) // Descending
                .List.First()
                .Alias("max_char")
        let res = 
            lf
            // 1. Struct 测试: 把 score1, score2 打包成 "scores_struct"
            |> Polars.withColumnLazy (
                Polars.asStruct [Polars.col "score1"; Polars.col "score2"]
                |> Polars.alias "scores_struct"
            )
            // 2. List 测试: 
            // 假设我们把 struct 里的字段取出来，做一个计算 (演示 Struct.Field)
            |> Polars.withColumnLazy (
                (Polars.col "scores_struct").Struct.Field("score1").Alias("s1_extracted")
            )
            // 3. List Agg 测试 (既然没有 concat_list，我们造一个伪需求：如果 split 后的 list)
            // 我们手动 split 一个字符串 "1 5 2"
            |> Polars.withColumnLazy (
                Polars.lit "1 5 2"
                |> Polars.alias "raw_nums"
            )
            // 4. 处理 List: Split -> Sort(Desc) -> First
            // "1 5 2" -> ["1", "5", "2"] -> ["5", "2", "1"] -> "5"
            // 注意：Split 出来是 String，Sort 默认按字典序，"5" > "2" > "1"
            |> Polars.withColumnLazy maxCharExpr
            |> Polars.collect

        // 验证 Struct Field
        // Alice score1 = 80
        Assert.Equal(80L, res.Int("s1_extracted", 0).Value)

        // 验证 List Sort + First
        Assert.Equal("5", res.String("max_char", 0).Value)

    [<Fact>]
    member _.``Window Function (Over)`` () =
        use csv = new TempCsv "name,dept,salary\nAlice,IT,1000\nBob,IT,2000\nCharlie,HR,3000"
        let lf = LazyFrame.ScanCsv csv.Path

        let res = 
            lf
            |> Polars.withColumnLazy (
                // 逻辑: col("salary") - col("salary").mean().over([col("dept")])
                Polars.col "salary" - 
                (Polars.col "salary").Mean().Over [Polars.col "dept"]
                |> Polars.alias "diff_from_avg"
            )
            |> Polars.collect
            |> Polars.sort (Polars.col "name") false

        // 验证
        // Alice (IT): 1000 - 1500 = -500
        Assert.Equal("Alice", res.String("name", 0).Value)
        Assert.Equal(-500.0, res.Float("diff_from_avg", 0).Value)

        // Bob (IT): 2000 - 1500 = 500
        Assert.Equal("Bob", res.String("name", 1).Value)
        Assert.Equal(500.0, res.Float("diff_from_avg", 1).Value)

        // Charlie (HR): 3000 - 3000 = 0
        Assert.Equal("Charlie", res.String("name", 2).Value)
        Assert.Equal(0.0, res.Float("diff_from_avg", 2).Value)
    [<Fact>]
    member _.``Reshaping and IO: Pivot, Unpivot`` () =
        // 1. 准备宽表数据 (Sales Data)
        // Year, Q1, Q2
        use csv = new TempCsv "year,Q1,Q2\n2023,100,200\n2024,300,400"
        let df = DataFrame.ReadCsv csv.Path

        // --- Test 1: Eager Unpivot (Wide -> Long) ---
        // 结果: year, quarter, revenue
        let longDf = 
            df 
            |> Polars.unpivot ["year"] ["Q1"; "Q2"] (Some "quarter") (Some "revenue")
            |> Polars.sort (Polars.col "year") false

        Assert.Equal(4L, longDf.Rows)
        Assert.Equal("Q1", longDf.String("quarter", 0).Value)
        Assert.Equal(100L, longDf.Int("revenue", 0).Value)

        // --- Test 2: Eager Pivot (Long -> Wide) ---
        // 还原回: year, Q1, Q2
        let wideDf = 
            longDf
            |> Polars.pivot ["year"] ["quarter"] ["revenue"] PivotAgg.Sum
            |> Polars.sort (Polars.col "year") false

        Assert.Equal(2L, wideDf.Rows)
        Assert.Equal(3L, wideDf.Columns) // year, Q1, Q2
        Assert.Equal(100L, wideDf.Int("Q1", 0).Value)
        Assert.Equal(400L, wideDf.Int("Q2", 1).Value)

    [<Fact>]
    member _.``Lazy Reshaping: Concat All Types`` () =
        // lf1: [a]
        // lf2: [b]
        // lf3: [a]
        let lf1 = LazyFrame.ScanCsv (new TempCsv "a\n1").Path
        let lf2 = LazyFrame.ScanCsv (new TempCsv "b\n2").Path
        let lf3 = LazyFrame.ScanCsv (new TempCsv "a\n3").Path

        // 1. Horizontal: [a, b]
        let dfHorz = 
            Polars.concatLazyHorizontal [lf1; lf2]
            |> Polars.collect
        
        Assert.Equal(1L, dfHorz.Rows)
        Assert.Equal(2L, dfHorz.Columns)
        Assert.Equal(1L, dfHorz.Int("a", 0).Value)
        Assert.Equal(2L, dfHorz.Int("b", 0).Value)

        // 2. Vertical: [a] (rows=2)        
        let dfVert = 
            Polars.concatLazy [lf1; lf3]
            |> Polars.collect
        
        Assert.Equal(2L, dfVert.Rows)
        Assert.Equal(1L, dfVert.Columns)

        // 3. Diagonal: [a, b] (rows=2)
        // lf1 (a=1, b=null)
        // lf2 (a=null, b=2)
        let dfDiag =
            Polars.concatLazyDiagonal [lf1; lf2]
            |> Polars.collect
        
        Assert.Equal(2L, dfDiag.Rows)
        Assert.Equal(2L, dfDiag.Columns)

    [<Fact>]
    member _.``Concatenation: Eager Stack (Safety Check)`` () =
        // DF1
        use csv1 = new TempCsv "val\n1"
        let df1 = DataFrame.ReadCsv csv1.Path
        
        // DF2
        use csv2 = new TempCsv "val\n2"
        let df2 = DataFrame.ReadCsv csv2.Path

        // 1. 执行 Concat
        let bigDf = Polars.concat [df1; df2]

        // 验证结果
        Assert.Equal(2L, bigDf.Rows)

        // 2. [关键验证] 验证原 df1, df2 是否依然可用
        // 如果没有正确 Clone，这里会报 ObjectDisposedException 或 Segfault
        Assert.Equal(1L, df1.Rows)
        Assert.Equal(1L, df2.Rows)
        Assert.Equal(1L, df1.Int("val", 0).Value)
    [<Fact>]
    member _.``SQL Context: Register and Execute`` () =
        // 准备数据
        use csv = new TempCsv "name,age\nAlice,20\nBob,30"
        let lf = LazyFrame.ScanCsv csv.Path

        // 1. 创建 Context
        use ctx = Polars.sqlContext()
        
        // 2. 注册表 "people"
        ctx.Register("people", lf)

        // 3. 写 SQL
        let resLf = ctx.Execute "SELECT name, age * 2 AS age_double FROM people WHERE age > 25"
        let res = resLf |> Polars.collect

        // 验证
        Assert.Equal(1L, res.Rows)
        Assert.Equal("Bob", res.String("name", 0).Value)
        Assert.Equal(60L, res.Int("age_double", 0).Value)
    [<Fact>]
    member _.``Time Series: Shift, Diff, ForwardFill`` () =
        // 数据: 价格序列，中间有空值
        // P1: 10
        // P2: null
        // P3: 20
        use csv = new TempCsv "price\n10\n\n20"
        let df = DataFrame.ReadCsv csv.Path

        let res = 
            df 
            |> Polars.select [
                Polars.col "price"
                
                // 1. Forward Fill: null 变成 10
                (Polars.col "price").ForwardFill().Alias "price_ffill"
                
                // 2. Shift(1): 向下平移一行
                (Polars.col "price").Shift(1L).Alias "price_lag1"
            ]
            |> Polars.withColumn (
                // 3. Diff: 当前值 - 上一个值 (基于 ffill 后的数据)
                (Polars.col "price_ffill").Diff(1L).Alias "price_diff"
            )

        // 验证
        // Row 0: 10, ffill=10, lag=null, diff=null
        Assert.Equal(10L, res.Int("price_ffill", 0).Value)
        Assert.True(res.Int("price_lag1", 0).IsNone)

        // Row 1: null, ffill=10, lag=10, diff=0 (10-10)
        Assert.Equal(10L, res.Int("price_ffill", 1).Value)
        Assert.Equal(10L, res.Int("price_lag1", 1).Value)
        Assert.Equal(0L, res.Int("price_diff", 1).Value)

        // Row 2: 20, ffill=20, lag=null(原始price是null), diff=10 (20-10)
        Assert.Equal(20L, res.Int("price_ffill", 2).Value)
        Assert.Equal(10L, res.Int("price_diff", 2).Value)
    [<Fact>]
    member _.``Rolling Window (Moving Average)`` () =
        // 构造时序数据
        use csv = new TempCsv "date,price\n2024-01-01,10\n2024-01-02,20\n2024-01-03,30"
        let lf = LazyFrame.ScanCsv (path=csv.Path,tryParseDates=true)

        let res = 
            lf
            |> Polars.sortLazy (Polars.col "date") false // Rolling 必须先排序
            |> Polars.withColumnLazy (
                // 2天移动平均 (包括当前行)
                // 1.1: 10
                // 1.2: (10+20)/2 = 15
                // 1.3: (20+30)/2 = 25
                // 注意：Polars "2d" 窗口不仅看行数，还看时间列。
                // 如果没有设置 by="date"，这里其实是按行数 "2i" (2 rows) 来算的，或者依赖 Implicit Index。
                // 为了简单测试，我们假设它是按行滚动 (2i)
                (Polars.col "price").RollingMean("2i").Alias "ma_2"
            )
            |> Polars.collect

        Assert.Equal(15.0, res.Float("ma_2", 1).Value)
        Assert.Equal(25.0, res.Float("ma_2", 2).Value)
    [<Fact>]
    member _.``Time Series: Dynamic Rolling Window`` () =
        // 构造非均匀时间数据
        // 10:00 -> 10
        // 10:30 -> 20
        // 12:00 -> 30 (此时 1小时窗口内只有它自己，因为 10:30 已经是一个半小时前了)
        let csvContent = "time,val\n2024-01-01 10:00:00,10\n2024-01-01 10:30:00,20\n2024-01-01 12:00:00,30"
        use csv = new TempCsv(csvContent)
        let lf = LazyFrame.ScanCsv (path=csv.Path,tryParseDates=true)

        let res = 
            lf
            // 必须先按时间排序，虽然 Polars 有时会自动排，但显式排是好习惯
            |> Polars.sortLazy (Polars.col "time") false
            |> Polars.withColumnLazy (
                // 计算 "1h" (1小时) 内的 sum
                // 10:00: 窗口 [09:00, 10:00) -> 10
                // 10:30: 窗口 [09:30, 10:30) -> 10 + 20 = 30
                // 12:00: 窗口 [11:00, 12:00) -> 30 (前面的都过期了)
                (Polars.col "val")
                    .RollingSumBy("1h", Polars.col "time", closed="right") // closed="left" means [ )
                    .Alias "sum_1h"
            )
            |> Polars.collect

        // 验证
        Assert.Equal(10L, res.Int("sum_1h", 0).Value)
        Assert.Equal(30L, res.Int("sum_1h", 1).Value)
        Assert.Equal(30L, res.Int("sum_1h", 2).Value)
    [<Fact>]
    member _.``Lazy Join (Standard Join)`` () =
        // 左表: 用户 (id, name)
        use usersCsv = new TempCsv "id,name\n1,Alice\n2,Bob"
        // 右表: 订单 (uid, amount)
        use ordersCsv = new TempCsv "uid,amount\n1,100\n1,200\n3,50"

        let lfUsers = LazyFrame.ScanCsv usersCsv.Path
        let lfOrders = LazyFrame.ScanCsv ordersCsv.Path

        let res = 
            lfUsers
            |> Polars.joinLazy lfOrders [Polars.col "id"] [Polars.col "uid"] JoinType.Left
            |> Polars.collect
            |> Polars.sort (Polars.col "id") false

        // 验证
        // Alice (id=1) 有两单
        Assert.Equal("Alice", res.String("name", 0).Value)
        Assert.Equal(100L, res.Int("amount", 0).Value)
        
        Assert.Equal("Alice", res.String("name", 1).Value)
        Assert.Equal(200L, res.Int("amount", 1).Value)

        // Bob (id=2) 没单 -> null
        Assert.Equal("Bob", res.String("name", 2).Value)
        Assert.True(res.Int("amount", 2).IsNone) // 验证 Left Join 的空值处理
    [<Fact>]
    member _.``Join AsOf: Trades matching Quotes (with GroupBy and Tolerance)`` () =
        // 1. 交易数据 (Trades)
        // AAPL 在 10:00 有交易
        // MSFT 在 10:00 有交易
        let tradesContent = 
            "time,ticker,volume\n" +
            "1000,AAPL,10\n" +
            "1000,MSFT,20\n" +
            "1005,AAPL,10" // AAPL 在 10:05 还有一笔
        use tradesCsv = new TempCsv(tradesContent)
        
        // 2. 报价数据 (Quotes)
        // AAPL: 09:59 (99.0), 10:01 (101.0)
        // MSFT: 09:58 (50.0)
        // 注意：AsOf Join 要求数据在 Join Key 上是排序的
        let quotesContent = 
            "time,ticker,bid\n" +
            "998,MSFT,50.0\n" +
            "999,AAPL,99.0\n" +
            "1001,AAPL,101.0"
        use quotesCsv = new TempCsv(quotesContent)

        let lfTrades = LazyFrame.ScanCsv tradesCsv.Path |> Polars.sortLazy (Polars.col "time") false
        let lfQuotes = LazyFrame.ScanCsv quotesCsv.Path |> Polars.sortLazy (Polars.col "time") false

        // 3. 执行 AsOf Join
        // 逻辑：找到交易发生时刻(time)之前(backward)最近的一次报价
        // 必须匹配 ticker (by=["ticker"])
        // 容差: 2个时间单位 (tolerance="2")
        let res = 
            lfTrades
            |> Polars.joinAsOf lfQuotes 
                (Polars.col "time") (Polars.col "time") // On Time
                [Polars.col "ticker"] [Polars.col "ticker"] // By Ticker
                (Some "backward") // Strategy
                (Some "2")        // Tolerance: 只匹配最近2ms内的数据
            |> Polars.sortLazy (Polars.col "ticker") false // 排序方便断言
            |> Polars.sortLazy (Polars.col "time") false
            |> Polars.collect

        // 4. 验证结果
        // 预期：
        // Row 0: time=1000, ticker=AAPL. 匹配 999 (diff=1 <= 2). Bid=99.0
        // Row 1: time=1000, ticker=MSFT. 匹配 998 (diff=2 <= 2). Bid=50.0
        // Row 2: time=1005, ticker=AAPL. 最近是 1001 (diff=4 > 2). 匹配失败 -> null
        
        // 按 time, ticker 排序后的顺序:
        // 1000, AAPL
        Assert.Equal("AAPL", res.String("ticker", 0).Value)
        Assert.Equal(99.0, res.Float("bid", 0).Value)

        // 1000, MSFT
        Assert.Equal("MSFT", res.String("ticker", 1).Value)
        Assert.Equal(50.0, res.Float("bid", 1).Value)

        // 1005, AAPL (超时，应为 null)
        Assert.Equal("AAPL", res.String("ticker", 2).Value)
        Assert.True(res.Float("bid", 2).IsNone) // 验证 Tolerance 生效