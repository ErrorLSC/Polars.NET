namespace Polars.FSharp.Tests

open Xunit
open Polars.FSharp
open System
open System.Data
open System.Diagnostics

type ``Complex Query Tests`` () =
    
    [<Fact>]
    member _.``Join execution (Eager)`` () =
        use users = new TempCsv "id,name\n1,A\n2,B"
        use sales = new TempCsv "uid,amt\n1,100\n1,200\n3,50"

        let uDf = DataFrame.ReadCsv (path=users.Path, tryParseDates=false)
        let sDf = DataFrame.ReadCsv (path=sales.Path, tryParseDates=false)

        let res = 
            uDf 
            |> pl.join sDf [pl.col "id"] [pl.col "uid"] JoinType.Left
        
        // Left join: id 1 (2 rows), id 2 (1 row null match) -> Total 3
        Assert.Equal(3L, res.Rows)

    [<Fact>]
    member _.``Lazy API Chain (Filter -> Collect)`` () =
        use csv = new TempCsv "a,b\n1,10\n2,20\n3,30"
        let lf = LazyFrame.ScanCsv csv.Path
        
        let df = 
            lf
            |> pl.filterLazy (pl.col "a" .> pl.lit 1)
            |> pl.limit 1u
            |> pl.collectStreaming

        Assert.Equal(1L, df.Rows)

    [<Fact>]
    member _.``GroupBy Queries`` () =
        use csv = new TempCsv "name,birthdate,weight,height\nBen Brown,1985-02-15,72.5,1.77\nQinglei,2025-11-25,70.0,1.80\nZhang,2025-10-31,55,1.75"
        let lf = LazyFrame.ScanCsv(csv.Path,tryParseDates=true)

        let res = 
            lf 
            |> pl.groupByLazy
                [(pl.col "birthdate").Dt.Year() / pl.lit 10 * pl.lit 10 |> pl.alias "decade" ]
                [ pl.count().Alias "cnt"] 
            |> pl.sortLazy (pl.col "decade") false
            |> pl.collect

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
        let lf = LazyFrame.ScanCsv(csv.Path,tryParseDates=true)

        let res = 
            lf
            |> pl.withColumnsLazy (
                // 1. String Split -> List -> First
                [
                (pl.col "name").Str.Split(" ").List.First()
                (pl.col "birthdate").Dt.Year() / pl.lit 10 * pl.lit 10 |> pl.alias "decade"
            ])
            |> pl.selectLazy [
                // 2. Exclude (all except "ignore_me")
                pl.all() |> pl.exclude ["birthdate"] |> pl.asExpr
            ]
            |> pl.groupByLazy 
                // Keys
                [ pl.col "decade" ] 
                // Aggs
                [
                    // Agg A: 名字列表 (Polars 默认行为：非 Key 列在 agg 中会聚合成 List)
                    // 但 Rust 原例写的是 col("name")，如果 context 是 agg，它就是 list
                    // 这里我们显式一点，或者直接传 col "name" 让 Polars 处理
                    pl.col "name"
                    // Agg B: Weight & Height 的均值 + 四舍五入 + 重命名
                    // Rust 原例: cols(["weight", "height"]).mean().round(2).prefix("avg_")
                    // F# 复刻: 展开写两个 Expr (效果等价)
                    (pl.col "weight").Mean().Round(2).Name.Prefix("avg_")
                    (pl.col "height").Mean().Round(2).Name.Prefix("avg_")
                ]
            |> pl.collect
            |> pl.sort ((pl.col "decade"),false)

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
            |> pl.withColumnLazy (
                // 1. Split 变成 List
                (pl.col "tags").Str.Split(" ").Alias "tag_list"
            )
            |> pl.withColumnLazy (
                // 2. 演示 cols([...]): 同时选中 name 和 tag_list，加上前缀
                // 虽然这里只是演示，通常用于批量数学运算
                pl.cols ["name"; "tag_list"]
                |> fun e -> e.Name.Prefix("my_")
            )
            |> pl.withColumnLazy (
                // 3. List Join (还原回去)
                (pl.col "my_tag_list").List.Join("-").Alias "joined_tags"
            )
            |> pl.collect

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
            |> pl.select [ pl.col "my_name"; pl.col "my_tag_list" ]
            // [修改] 加上列表括号 []
            |> pl.explode ["my_tag_list"]  
        
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
            (pl.col "raw_nums").Str.Split(" ")
                .List.Sort(true) // Descending
                .List.First()
                .Alias "max_char"
        let res = 
            lf
            // 1. Struct 测试: 把 score1, score2 打包成 "scores_struct"
            |> pl.withColumnLazy (
                pl.asStruct [pl.col "score1"; pl.col "score2"]
                |> pl.alias "scores_struct"
            )
            // 2. List 测试: 
            // 假设我们把 struct 里的字段取出来，做一个计算 (演示 Struct.Field)
            |> pl.withColumnLazy (
                (pl.col "scores_struct").Struct.Field("score1").Alias("s1_extracted")
            )
            // 3. List Agg 测试 (既然没有 concat_list，我们造一个伪需求：如果 split 后的 list)
            // 我们手动 split 一个字符串 "1 5 2"
            |> pl.withColumnLazy (
                pl.lit "1 5 2"
                |> pl.alias "raw_nums"
            )
            // 4. 处理 List: Split -> Sort(Desc) -> First
            // "1 5 2" -> ["1", "5", "2"] -> ["5", "2", "1"] -> "5"
            // 注意：Split 出来是 String，Sort 默认按字典序，"5" > "2" > "1"
            |> pl.withColumnLazy maxCharExpr
            |> pl.collect

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
            |> pl.withColumnLazy (
                // 逻辑: col("salary") - col("salary").mean().over([col("dept")])
                pl.col "salary" - 
                (pl.col "salary").Mean().Over [pl.col "dept"]
                |> pl.alias "diff_from_avg"
            )
            |> pl.collect
            |> pl.sort(pl.col "name", false)

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
    member _.``Reshaping and IO: Pivot, Unpivot (In-Memory & Custom Expr)`` () =
        // 1. 准备宽表数据 (Sales Data) - 纯内存构建，告别 CSV IO
        // Year, Q1, Q2
        let df = 
            DataFrame.create [
                Series.create("year", [2023; 2024])
                Series.create("Q1",   [100;  300])
                Series.create("Q2",   [200;  400])
            ]

        // --- Test 1: Unpivot (Wide -> Long) ---
        // 结果: year, quarter, revenue
        // 使用成员方法 .Unpivot (C# 风格) 或者管道风格皆可，这里演示成员方法以配合可选参数
        let longDf = 
            df.Unpivot(
                index = ["year"], 
                on = ["Q1"; "Q2"], 
                variableName = Some "quarter", 
                valueName = Some "revenue"
            ).Sort [pl.col "year";pl.col "quarter"] // 排序保证断言顺序

        Assert.Equal(4L, longDf.Height)
        
        // 验证第一行: 2023, Q1, 100
        Assert.Equal(2023, longDf.Cell<int>(0, "year"))
        Assert.Equal("Q1", longDf.Cell<string>(0, "quarter"))
        Assert.Equal(100, longDf.Cell<int>(0, "revenue"))

        // --- Test 2: Standard Pivot (Enum Aggregation) ---
        // 还原回: year, Q1, Q2
        let wideDfEnum = 
            longDf.Pivot(
                index = ["year"], 
                columns = ["quarter"], 
                values = ["revenue"], 
                aggFn = PivotAgg.Sum,
                sortColumns = true 
            ).Sort(pl.col "year", false)

        Assert.Equal(2L, wideDfEnum.Height)
        Assert.Equal(3L, wideDfEnum.Width) // year, Q1, Q2
        Assert.Equal(100, wideDfEnum.Cell<int>(0, "Q1")) // 2023 Q1
        Assert.Equal(400, wideDfEnum.Cell<int>(1, "Q2")) // 2024 Q2

        // --- Test 3: Pivot with Custom Expr (自定义表达式) ---
        // 场景：在透视的同时，将收入翻倍 (Revenue * 2)
        // 注意：根据 Polars Pivot 机制，聚合时的上下文是匿名 Value 列，需使用 pl.col("")
        let wideDfExpr = 
            longDf.Pivot(
                index = ["year"], 
                columns = ["quarter"], 
                values = ["revenue"], 
                // [关键] 使用 Expr 重载
                // pl.col("").sum() 代表对当前分组后的 revenue 值求和
                // * pl.lit(2) 代表结果翻倍
                aggExpr = pl.col("").Sum() * pl.lit 2, 
                sortColumns = true
            ).Sort(pl.col "year", false)

        // 验证结果
        // 2023 Q1 原值为 100 -> 翻倍后应为 200
        Assert.Equal(200, wideDfExpr.Cell<int>(0, "Q1"))
        
        // 2024 Q2 原值为 400 -> 翻倍后应为 800
        Assert.Equal(800, wideDfExpr.Cell<int>(1, "Q2"))

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
            pl.concatLazy [lf1; lf2] Horizontal
            |> pl.collect
        
        Assert.Equal(1L, dfHorz.Rows)
        Assert.Equal(2L, dfHorz.Width)
        Assert.Equal(1L, dfHorz.Int("a", 0).Value)
        Assert.Equal(2L, dfHorz.Int("b", 0).Value)

        // 2. Vertical: [a] (rows=2)        
        let dfVert = 
            pl.concatLazy [lf1; lf3] Vertical
            |> pl.collect
        
        Assert.Equal(2L, dfVert.Rows)
        Assert.Equal(1L, dfVert.Width)

        // 3. Diagonal: [a, b] (rows=2)
        // lf1 (a=1, b=null)
        // lf2 (a=null, b=2)
        let dfDiag =
            pl.concatLazy [lf1; lf2] Diagonal
            |> pl.collect
        
        Assert.Equal(2L, dfDiag.Rows)
        Assert.Equal(2L, dfDiag.Width)

    [<Fact>]
    member _.``Concatenation: Eager Stack (Safety Check)`` () =
        // DF1
        use csv1 = new TempCsv "val\n1"
        let df1 = DataFrame.ReadCsv csv1.Path
        
        // DF2
        use csv2 = new TempCsv "val\n2"
        let df2 = DataFrame.ReadCsv csv2.Path

        // 1. 执行 Concat
        let bigDf = pl.concat [df1; df2]

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
        use ctx = pl.sqlContext()
        
        // 2. 注册表 "people"
        ctx.Register("people", lf)

        // 3. 写 SQL
        let resLf = ctx.Execute "SELECT name, age * 2 AS age_double FROM people WHERE age > 25"
        let res = resLf |> pl.collect

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
            |> pl.select [
                pl.col "price"
                
                // 1. Forward Fill: null 变成 10
                (pl.col "price").ForwardFill().Alias "price_ffill"
                
                // 2. Shift(1): 向下平移一行
                (pl.col "price").Shift(1L).Alias "price_lag1"
            ]
            |> pl.withColumn (
                // 3. Diff: 当前值 - 上一个值 (基于 ffill 后的数据)
                (pl.col "price_ffill").Diff(1L).Alias "price_diff"
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
            |> pl.sortLazy (pl.col "date") false // Rolling 必须先排序
            |> pl.withColumnLazy (
                // 2天移动平均 (包括当前行)
                // 1.1: 10
                // 1.2: (10+20)/2 = 15
                // 1.3: (20+30)/2 = 25
                // 注意：Polars "2d" 窗口不仅看行数，还看时间列。
                // 如果没有设置 by="date"，这里其实是按行数 "2i" (2 rows) 来算的，或者依赖 Implicit Index。
                // 为了简单测试，我们假设它是按行滚动 (2i)
                (pl.col "price").RollingMean("2i").Alias "ma_2"
            )
            |> pl.collect

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
            |> pl.sortLazy (pl.col "time") false
            |> pl.withColumnLazy (
                // 计算 "1h" (1小时) 内的 sum
                // 10:00: 窗口 [09:00, 10:00) -> 10
                // 10:30: 窗口 [09:30, 10:30) -> 10 + 20 = 30
                // 12:00: 窗口 [11:00, 12:00) -> 30 (前面的都过期了)
                (pl.col "val")
                    .RollingSumBy("1h", pl.col "time", closed= ClosedWindow.Right) // closed="left" means [ )
                    .Alias "sum_1h"
            )
            |> pl.collect

        // 验证
        Assert.Equal(10L, res.Int("sum_1h", 0).Value)
        Assert.Equal(30L, res.Int("sum_1h", 1).Value)
        Assert.Equal(30L, res.Int("sum_1h", 2).Value)
    [<Fact>]
    member _.``Lazy Join (Standard Join)`` () =
        // 1. 准备数据 (内存构建 -> Lazy)
        // 左表: 用户 (id, name, common)
        // "common" 列用于测试 Suffix 后缀冲突处理
        let users = 
            DataFrame.create [
                Series.create("id", [1; 2])
                Series.create("name", ["Alice"; "Bob"])
                Series.create("common", ["U1"; "U2"]) 
            ]
        let lfUsers = users.Lazy()

        // 右表: 订单 (uid, amount, common)
        let orders = 
            DataFrame.create [
                Series.create("uid", [1; 1; 3])
                Series.create("amount", [100; 200; 50])
                Series.create("common", ["O1"; "O2"; "O3"]) 
            ]
        let lfOrders = orders.Lazy()

        // 2. 执行 Join (Left Join)
        // 注意：这里直接调用 .Join 类方法，以便使用可选参数
        // 预期行为：
        // - Alice (1) 匹配到两单 (100, 200)
        // - Bob (2) 无匹配 (null)
        // - 列名冲突自动处理 (common -> common_right_test)
        let res = 
            lfUsers.Join(
                other = lfOrders, 
                leftOn = [pl.col "id"], 
                rightOn = [pl.col "uid"], 
                how = JoinType.Left,
                // --- 新功能测试 ---
                suffix = "_right_test",            // 自定义后缀
                validation = JoinValidation.ManyToMany // 显式指定验证模式
            )
            |> pl.collect

        let resSorted = res.Sort([pl.col "id"; pl.col "amount"], [false; false], [false;false]) // 排序保证断言顺序

        // 3. 验证
        // Row 0: Alice - 100
        Assert.Equal("Alice", resSorted.Cell<string>(0, "name"))
        Assert.Equal(100, resSorted.Cell<int>(0, "amount"))
        Assert.Equal("U1", resSorted.Cell<string>(0, "common"))             // 左表原样
        Assert.Equal("O1", resSorted.Cell<string>(0, "common_right_test"))  // 右表加后缀

        // Row 1: Alice - 200
        Assert.Equal("Alice", resSorted.Cell<string>(1, "name"))
        Assert.Equal(200, resSorted.Cell<int>(1, "amount"))

        // Row 2: Bob - null
        Assert.Equal("Bob", resSorted.Cell<string>(2, "name"))
        
        // 验证空值 (Amount 应该是 null)
        let amountNull = resSorted.Column("amount").IsNullAt(2)
        Assert.True(amountNull, "Bob should have null amount")
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

        let lfTrades = LazyFrame.ScanCsv tradesCsv.Path |> pl.sortLazy (pl.col "time") false
        let lfQuotes = LazyFrame.ScanCsv quotesCsv.Path |> pl.sortLazy (pl.col "time") false

        // 3. 执行 AsOf Join
        // 逻辑：找到交易发生时刻(time)之前(backward)最近的一次报价
        // 必须匹配 ticker (by=["ticker"])
        // 容差: 2个时间单位 (tolerance=2L)
        let res = 
            lfTrades.JoinAsOf(
                lfQuotes,
                pl.col "time",
                pl.col "time", 
                2L,                      
                strategy = Backward,       
                byLeft = [pl.col "ticker"], 
                byRight = [pl.col "ticker"]
            )
            |> pl.sortLazy (pl.col "ticker") false
            |> pl.sortLazy (pl.col "time") false
            |> pl.collect

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
    [<Fact>]
    member _.``Test_ETL_Stream_EndToEnd: DataTable -> Polars -> DataTable`` () =
        // ==================================================================================
        // 场景模拟：每日订单处理 (ETL)
        // Source DB (Mock DataTable) -> DataReader -> Polars Lazy -> Filter/Calc -> SinkTo -> Target DB
        // 目标：处理 10 万行数据，内存不积压，类型全覆盖。
        // ==================================================================================

        let totalRows = 100_000
        
        // ---------------------------------------------------------
        // 1. [Extract] 准备源数据库 (Source)
        // ---------------------------------------------------------
        let sourceTable = new DataTable()
        sourceTable.Columns.Add("OrderId", typeof<int>) |> ignore
        sourceTable.Columns.Add("Region", typeof<string>) |> ignore
        sourceTable.Columns.Add("Amount", typeof<double>) |> ignore
        sourceTable.Columns.Add("OrderDate", typeof<DateTime>) |> ignore

        // 生成模拟数据
        // 偶数行是 "US"，奇数行是 "EU"
        // 日期固定为今天中午（避开时区坑）
        let baseDate = DateTime.Now.Date.AddHours 12.0
        
        for i in 0 .. totalRows - 1 do
            let region = if i % 2 = 0 then "US" else "EU"
            let amount = float i * 1.5
            let date = baseDate.AddDays(float (i % 10))
            sourceTable.Rows.Add(i, region, amount, date) |> ignore

        // 定义 Reader 工厂
        // scanDb 需要一个工厂函数，以便在需要时(重新)创建连接
        let readerFactory = fun () -> sourceTable.CreateDataReader() :> IDataReader

        // ---------------------------------------------------------
        // 2. [Transform] 构建 Polars 流式管道
        // ---------------------------------------------------------
        
        // 这里的 batchSize=50_000 意味着 Polars 每次只从 Reader 拉取 5万行进内存
        let lf = LazyFrame.scanDb(readerFactory, batchSize=50_000)
        // Step C: 定义转换逻辑 (Transform)
        // 业务需求：
        // 1. 只保留 "US" 地区的订单
        // 2. 计算税后金额 (Amount * 1.08)
        // 3. 选取需要的列
        let pipeline = 
            lf
            |> pl.filterLazy(pl.col "Region".== pl.lit "US")
            |> pl.withColumnLazy((pl.col "Amount" * pl.lit 1.08).Alias "TaxedAmount")
            |> pl.selectLazy([
                pl.col "OrderId"
                pl.col "TaxedAmount"
                pl.col "OrderDate"
            ])

        // ---------------------------------------------------------
        // 3. [Load] 准备目标数据库 & 执行 Sink
        // ---------------------------------------------------------
        
        let targetTable = new DataTable() // 模拟目标表

        // 定义类型契约：强制要求 OrderDate 为 DateTime
        // (Arrow 默认可能识别为 Date32/Date64/Timestamp，我们需要确保 DataReader 对外暴露的是 DateTime)
        let schemaContract = dict [
            "OrderDate", typeof<DateTime>
        ]

        printfn "[ETL] Starting Pipeline..."
        let sw = Stopwatch.StartNew()

        // 执行流式写入！
        // 这一步会驱动：
        // sourceReader -> Arrow转换 -> Rust引擎(Filter/Calc) -> Buffer -> ArrowToDbStream -> targetTable.Load
        pipeline.SinkTo(
            (fun reader -> 
                // 验证点 1: Reader 敢不敢对外宣称它是 DateTime?
                let dateColIndex = reader.GetOrdinal "OrderDate"
                Assert.Equal(typeof<DateTime>, reader.GetFieldType dateColIndex)
                
                // 模拟加载 (SqlBulkCopy 的行为就是一直 Read 到结束)
                targetTable.Load reader
            ),
            bufferSize = 50000,
            typeOverrides = schemaContract
        )

        sw.Stop()
        printfn "[ETL] Completed in %.3fs. Rows written: %d" sw.Elapsed.TotalSeconds targetTable.Rows.Count

        // ---------------------------------------------------------
        // 4. [Verify] 验证结果
        // ---------------------------------------------------------
        
        // 验证行数：只保留了 US (偶数行)，应该是 50,000 行
        Assert.Equal(totalRows / 2, targetTable.Rows.Count)

        // 验证第一行 (OrderId 0)
        // 0 * 1.5 * 1.08 = 0
        let row0 = targetTable.Rows.[0]
        Assert.Equal(0, unbox<int> row0.["OrderId"])
        Assert.Equal(0.0, unbox<double> row0.["TaxedAmount"], 4)
        Assert.Equal(baseDate, unbox<DateTime> row0.["OrderDate"])

        // 验证最后一行 (OrderId 99998) -> 它是最后一个偶数
        // 99998 * 1.5 * 1.08 = 161996.76
        let lastRow = targetTable.Rows.[targetTable.Rows.Count - 1]
        let lastId = 99998
        
        Assert.Equal(lastId, unbox<int> lastRow.["OrderId"])
        
        let expectedAmount = float lastId * 1.5 * 1.08
        let actualAmount = Convert.ToDouble(lastRow.["TaxedAmount"])
        Assert.Equal(expectedAmount, actualAmount, 0.001)

        // 验证列名 (确保 Select 生效)
        Assert.True(targetTable.Columns.Contains "TaxedAmount")
        Assert.False(targetTable.Columns.Contains "Amount") // 原列被排除了
        Assert.False(targetTable.Columns.Contains "Region") // 原列被排除了
    [<Fact>]
    member _.``Lazy: GroupBy Dynamic (Rolling Window)`` () =
        // 1. 准备时序数据
        // Start: 10:00
        let start = DateTime(2023, 1, 1, 10, 0, 0)
        let data = [
            // Group A
            {| Time = start;                     Category = "A"; Value = 10 |} // 10:00
            {| Time = start.AddMinutes 30.0;    Category = "A"; Value = 20 |} // 10:30
            {| Time = start.AddHours 1.0;       Category = "A"; Value = 30 |} // 11:00
            {| Time = start.AddHours 1.5;       Category = "A"; Value = 40 |} // 11:30
            
            // Group B (验证 'by' 参数有效性)
            {| Time = start;                     Category = "B"; Value = 100 |} // 10:00
        ]
        let df = DataFrame.ofRecords data
        let lf = df.Lazy()

        // 2. 执行动态分组
        // 目标：每 1 小时为一个窗口起点 (every)，查看未来 2 小时的数据 (period)
        // 这是一个典型的滑动窗口 (Rolling Window)
        let res = 
            lf.GroupByDynamic(
                indexCol = "Time",
                every = TimeSpan.FromHours 1.0,      // 窗口步长: 1h
                period = TimeSpan.FromHours 2.0,     // 窗口大小: 2h (Overlapping)
                
                // 按 Category 分组
                by = [ !> pl.col("Category") ],
                
                // 窗口闭合方式: 左闭右开 [t, t + period)
                closedWindow = ClosedWindow.Left,
                
                // 聚合逻辑 (混合使用 Expr 和 Selector)
                aggs = [
                    // 标准 Expr 聚合
                    !> pl.col("Value").Count().Alias("Count")
                    !> pl.col("Value").Mean().Alias("Mean")
                    
                    // Selector 聚合 (验证 IColumnExpr 混写能力)
                    // 对所有数值列 (这里只有 Value) 求和，并加后缀
                    !> pl.cs.numeric().ToExpr().Sum().Name.Suffix("_Sum") 
                ]
            ).Collect().Sort([pl.col "Category"; pl.col "Time"], false) // 排序以便断言

        // 3. 验证结果
        // 预期窗口:
        // Window 1 (10:00): 范围 [10:00, 12:00) -> 包含 10:00, 10:30, 11:00, 11:30
        // Window 2 (11:00): 范围 [11:00, 13:00) -> 包含 11:00, 11:30
        
        Assert.Equal(3L, res.Rows) // A有2个窗口，B有1个窗口

        // --- Row 0: Category A, Time 10:00 ---
        Assert.Equal("A", res.Cell<string>( "Category",0))
        // Count: 4 (10, 20, 30, 40)
        Assert.Equal(4, res.Cell<int>("Count",0)) 
        // Mean: (10+20+30+40)/4 = 25.0
        Assert.Equal(25.0, res.Cell<double>("Mean",0))
        // Sum (Selector): 100
        Assert.Equal(100L, res.Cell<int64>("Value_Sum",0))

        // --- Row 1: Category A, Time 11:00 ---
        Assert.Equal(DateTime(2023, 1, 1, 11, 0, 0), res.Cell<DateTime>("Time",1))
        // Count: 2 (30, 40)
        Assert.Equal(2, res.Cell<int>("Count",1))
        // Mean: (30+40)/2 = 35.0
        Assert.Equal(35.0, res.Cell<double>("Mean",1))
        
        // --- Row 2: Category B, Time 10:00 ---
        Assert.Equal("B", res.Cell<string>("Category",2))
        Assert.Equal(1, res.Cell<int>("Count",2))
        Assert.Equal(100.0, res.Cell<double>("Mean",2))
    [<Fact>]
    member _.``LazyFrame: Unnest Struct`` () =
        // 1. 准备数据: [Struct(A=1, B=2)]
        let data = [
            {| ID = 1; Val1 = 10; Val2 = 20 |}
        ]
        
        let lf = 
            DataFrame.ofRecords(data).Lazy()
                // 先构造一个 Struct 列 "MyStruct"
                .Select([
                    pl.col "ID"
                    pl.asStruct([ pl.col "Val1"; pl.col "Val2" ]).Alias "MyStruct"
                ])
        
        // Schema Check: ID, MyStruct
        
        // 2. Unnest "MyStruct"
        let res = 
            lf.Unnest("MyStruct") // 展开 MyStruct
              .Collect()

        // 3. 验证
        // Unnest 后，列名应该是 ID, Val1, Val2

        
        // 验证数据
        Assert.Equal(10, res.Cell<int>("Val1",0))
        Assert.Equal(20, res.Cell<int>("Val2",0))

    [<Fact>]
    member _.``LazyFrame: TopK & BottomK`` () =
        // Data: 1..5
        let data = [
            {| V = 1 |}; {| V = 3 |}; {| V = 5 |}; {| V = 2 |}; {| V = 4 |}
        ]
        let lf = DataFrame.ofRecords(data).Lazy()

        // --- TopK (by V) ---
        // 取最大的 2 个 -> 5, 4
        // 注意：Polars TopK 默认行为是 "Largest k elements"，所以不需要 descending=true
        let top2 = 
            lf.TopK(2, [pl.col "V"],reverse=false) // Descending=true 确保大的在前
            |>  pl.collect
        
        // 验证: 应该是 5, 4 (顺序可能取决于具体的 TopK 算法，但数值集合是对的)
        // Polars TopK 通常返回有序结果
        Assert.Equal(2L, top2.Rows)
        Assert.Equal(5, top2.Cell<int>("V",0))
        Assert.Equal(4, top2.Cell<int>("V",1))

        // --- BottomK (by V) ---
        // 取最小的 2 个 -> 1, 2
        let bot2 = 
            lf.BottomK(2, [pl.col "V"], reverse=false) // Ascending
              |> pl.collect
        
        Assert.Equal(1, bot2.Cell<int>("V",0))
        Assert.Equal(2, bot2.Cell<int>("V",1))