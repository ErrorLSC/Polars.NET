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
    [<Trait("LazyFrame","GroupBy")>]
    member _.``GroupBy Queries With Having`` () =
        let names = [| "Ben"; "Alice"; "Qinglei"; "Zhang" |]
        let dates = [| 
            DateTime(1985, 2, 15)  // 1980s (1)
            DateTime(1992, 8, 20)  // 1990s (1)
            DateTime(2025, 11, 25) // 2020s (2)
            DateTime(2025, 10, 31)
        |]
        
        use df = DataFrame.create([|
            Series.create("name", names)
            Series.create("birthdate", dates)
        |])
        
        let lf = df.Lazy()

        let keys = [ pl.col("birthdate").Dt.Year() / pl.lit 10 * pl.lit 10 |> pl.alias "decade" ]
        let aggs = [ pl.count().Alias "cnt" ]
        let havingCond = pl.count() .> pl.lit 1
        
        use res = 
            lf.GroupBy(keys, aggs,havingCond)
              .Sort("decade")
              .Collect()

        Assert.Equal(1L, res.Height) 
        
        Assert.Equal(2020L, int64 (res.Int("decade", 0).Value))
        Assert.Equal(2L, int64 (res.Int("cnt", 0).Value))

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
                // String Split -> List -> First
                [
                (pl.col "name").Str.Split(" ").List.First()
                (pl.col "birthdate").Dt.Year() / pl.lit 10 * pl.lit 10 |> pl.alias "decade"
            ])
            |> pl.selectLazy [
                // Exclude (all except "ignore_me")
                pl.cs.all().Exclude ["birthdate"] |> pl.asExpr
            ]
            |> pl.groupByLazy 
                // Keys
                [ pl.col "decade" ] 
                // Aggs
                [
                    pl.col "name"
                    (pl.col "weight").Mean().Round(2).Name.Prefix "avg_"
                    (pl.col "height").Mean().Round(2).Name.Prefix "avg_"
                ]
            |> pl.collect
            |> pl.sort (pl.col "decade",false)

        let cols = res.ColumnNames
        Assert.DoesNotContain("birthdate", cols)
        Assert.Contains("decade", cols)
        Assert.Contains("avg_weight", cols)
        Assert.Contains("avg_height", cols)

        // Row 0 (1980: Zhang, Li)
        Assert.Equal(1980L, res.Int("decade", 0).Value)
        
        // Mean + Round
        // Weight: (70.1234 + 60.5678) / 2 = 65.3456 -> Round(2) -> 65.35
        let w80 = res.Float("avg_weight", 0).Value
        Assert.Equal(65.35, w80)
        
        // Row 1 (1990: Wang)
        Assert.Equal(1990L, res.Int("decade", 1).Value)
        // Weight: 80.9999 -> 81.00
        let w90 = res.Float("avg_weight", 1).Value
        Assert.Equal(81.00, w90)

    [<Fact>]
    member _.``List Ops: Cols, Explode, Join and Read`` () =

        use csv = new TempCsv "name,tags\nAlice,coding reading\nBob,gaming"
        let lf = LazyFrame.ScanCsv csv.Path

        let res = 
            lf
            |> pl.withColumnLazy (
                // Split 变成 List
                (pl.col "tags").Str.Split(" ").Alias "tag_list"
            )
            |> pl.withColumnLazy (
                pl.cols ["name"; "tag_list"]
                |> fun e -> e.Name.Prefix("my_")
            )
            |> pl.withColumnLazy (
                (pl.col "my_tag_list").List.Join("-").Alias "joined_tags"
            )
            |> pl.collect

        let cols = res.ColumnNames
        Assert.Contains("my_name", cols)
        Assert.Contains("my_tag_list", cols)

        // coding reading -> coding-reading
        Assert.Equal("coding-reading", res.String("joined_tags", 0).Value)

        let aliceTags = res.StringList("my_tag_list", 0)
        Assert.True aliceTags.IsSome
        Assert.Equal<string list>(["coding"; "reading"], aliceTags.Value)

        let exploded = 
            res 
            |> pl.select [ pl.col "my_name"; pl.col "my_tag_list" ]
            |> pl.explode ["my_tag_list"]  
        
        Assert.Equal(3L, exploded.Rows)
        Assert.Equal("coding", exploded.String("my_tag_list", 0).Value)
        Assert.Equal("reading", exploded.String("my_tag_list", 1).Value)
        Assert.Equal("gaming", exploded.String("my_tag_list", 2).Value)

    [<Fact>]
    member _.``Struct and Advanced List Ops`` () =

        use csv = new TempCsv "name,score1,score2\nAlice,80,90\nBob,60,70"
        let lf = LazyFrame.ScanCsv csv.Path
        let maxCharExpr = 
            (pl.col "raw_nums").Str.Split(" ")
                .List.Sort(true) // Descending
                .List.First()
                .Alias "max_char"
        let res = 
            lf

            |> pl.withColumnLazy (
                pl.asStruct [pl.col "score1"; pl.col "score2"]
                |> pl.alias "scores_struct"
            )

            |> pl.withColumnLazy (
                (pl.col "scores_struct").Struct.Field("score1").Alias("s1_extracted")
            )

            |> pl.withColumnLazy (
                pl.lit "1 5 2"
                |> pl.alias "raw_nums"
            )

            |> pl.withColumnLazy maxCharExpr
            |> pl.collect

        // Alice score1 = 80
        Assert.Equal(80L, res.Int("s1_extracted", 0).Value)

        // List Sort + First
        Assert.Equal("5", res.String("max_char", 0).Value)

    [<Fact>]
    member _.``Window Function (Over)`` () =
        use csv = new TempCsv "name,dept,salary\nAlice,IT,1000\nBob,IT,2000\nCharlie,HR,3000"
        let lf = LazyFrame.ScanCsv csv.Path

        let res = 
            lf
            |> pl.withColumnLazy (
                // col("salary") - col("salary").mean().over([col("dept")])
                pl.col "salary" - 
                (pl.col "salary").Mean().Over [pl.col "dept"]
                |> pl.alias "diff_from_avg"
            )
            |> pl.collect
            |> pl.sort(pl.col "name", false)

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
        // Year, Q1, Q2
        let df = 
            DataFrame.create [
                Series.create("year", [2023; 2024])
                Series.create("Q1",   [100;  300])
                Series.create("Q2",   [200;  400])
            ]

        let longDf = 
            df.Unpivot(
                index = ["year"], 
                on = ["Q1"; "Q2"], 
                variableName = Some "quarter", 
                valueName = Some "revenue"
            ).Sort [pl.col "year";pl.col "quarter"] 

        Assert.Equal(4L, longDf.Height)
        
        // 2023, Q1, 100
        Assert.Equal(2023, longDf.Cell<int>(0, "year"))
        Assert.Equal("Q1", longDf.Cell<string>(0, "quarter"))
        Assert.Equal(100, longDf.Cell<int>(0, "revenue"))

        // --- Standard Pivot (Enum Aggregation) ---
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

        // --- Pivot with Custom Expr ---
        let wideDfExpr = 
            longDf.Pivot(
                index = ["year"], 
                columns = ["quarter"], 
                values = ["revenue"], 
                aggExpr = pl.col("").Sum() * pl.lit 2, 
                sortColumns = true
            ).Sort(pl.col "year", false)

        Assert.Equal(200, wideDfExpr.Cell<int>(0, "Q1"))
        
        Assert.Equal(800, wideDfExpr.Cell<int>(1, "Q2"))

    [<Fact>]
    member _.``Lazy Reshaping: Concat All Types`` () =
        // lf1: [a]
        // lf2: [b]
        // lf3: [a]
        let lf1 = LazyFrame.ScanCsv (new TempCsv "a\n1").Path
        let lf2 = LazyFrame.ScanCsv (new TempCsv "b\n2").Path
        let lf3 = LazyFrame.ScanCsv (new TempCsv "a\n3").Path

        // Horizontal: [a, b]
        let dfHorz = 
            pl.concatLazy [lf1; lf2] Horizontal
            |> pl.collect
        
        Assert.Equal(1L, dfHorz.Rows)
        Assert.Equal(2L, dfHorz.Width)
        Assert.Equal(1L, dfHorz.Int("a", 0).Value)
        Assert.Equal(2L, dfHorz.Int("b", 0).Value)

        // Vertical: [a] (rows=2)        
        let dfVert = 
            pl.concatLazy [lf1; lf3] Vertical
            |> pl.collect
        
        Assert.Equal(2L, dfVert.Rows)
        Assert.Equal(1L, dfVert.Width)

        // Diagonal: [a, b] (rows=2)
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

        // Concat
        let bigDf = pl.concat [df1; df2]

        Assert.Equal(2L, bigDf.Rows)

        Assert.Equal(1L, df1.Rows)
        Assert.Equal(1L, df2.Rows)
        Assert.Equal(1L, df1.Int("val", 0).Value)
    [<Fact>]
    member _.``SQL Context: Register and Execute`` () =
        use csv = new TempCsv "name,age\nAlice,20\nBob,30"
        let lf = LazyFrame.ScanCsv csv.Path

        use ctx = pl.sqlContext()
        
        ctx.Register("people", lf)

        let resLf = ctx.Execute "SELECT name, age * 2 AS age_double FROM people WHERE age > 25"
        let res = resLf |> pl.collect

        Assert.Equal(1L, res.Rows)
        Assert.Equal("Bob", res.String("name", 0).Value)
        Assert.Equal(60L, res.Int("age_double", 0).Value)
    [<Fact>]
    member _.``Time Series: Shift, Diff, ForwardFill`` () =
        // P1: 10
        // P2: null
        // P3: 20
        use csv = new TempCsv "price\n10\n\n20"
        let df = DataFrame.ReadCsv csv.Path

        let res = 
            df 
            |> pl.select [
                pl.col "price"
                
                // Forward Fill: null into 10
                (pl.col "price").ForwardFill().Alias "price_ffill"
                
                // Shift(1)
                (pl.col "price").Shift(1L).Alias "price_lag1"
            ]
            |> pl.withColumn (

                (pl.col "price_ffill").Diff(1L).Alias "price_diff"
            )

        // Row 0: 10, ffill=10, lag=null, diff=null
        Assert.Equal(10L, res.Int("price_ffill", 0).Value)
        Assert.True(res.Int("price_lag1", 0).IsNone)

        // Row 1: null, ffill=10, lag=10, diff=0 (10-10)
        Assert.Equal(10L, res.Int("price_ffill", 1).Value)
        Assert.Equal(10L, res.Int("price_lag1", 1).Value)
        Assert.Equal(0L, res.Int("price_diff", 1).Value)

        // Row 2: 20, ffill=20, lag=null, diff=10 (20-10)
        Assert.Equal(20L, res.Int("price_ffill", 2).Value)
        Assert.Equal(10L, res.Int("price_diff", 2).Value)
    [<Fact>]
    member _.``Rolling Window (Moving Average)`` () =

        use csv = new TempCsv "date,price\n2024-01-01,10\n2024-01-02,20\n2024-01-03,30"
        let lf = LazyFrame.ScanCsv (path=csv.Path,tryParseDates=true)

        let res = 
            lf
            |> pl.sortLazy (pl.col "date") false 
            |> pl.withColumnLazy (
                // 1.1: 10
                // 1.2: (10+20)/2 = 15
                // 1.3: (20+30)/2 = 25
                (pl.col "price").RollingMean("2i").Alias "ma_2"
            )
            |> pl.collect

        Assert.Equal(15.0, res.Float("ma_2", 1).Value)
        Assert.Equal(25.0, res.Float("ma_2", 2).Value)
    [<Fact>]
    member _.``Time Series: Dynamic Rolling Window`` () =
        // 10:00 -> 10
        // 10:30 -> 20
        // 12:00 -> 30 
        let csvContent = "time,val\n2024-01-01 10:00:00,10\n2024-01-01 10:30:00,20\n2024-01-01 12:00:00,30"
        use csv = new TempCsv(csvContent)
        let lf = LazyFrame.ScanCsv (path=csv.Path,tryParseDates=true)

        let res = 
            lf

            |> pl.sortLazy (pl.col "time") false
            |> pl.withColumnLazy (
                // 10:00: [09:00, 10:00) -> 10
                // 10:30: [09:30, 10:30) -> 10 + 20 = 30
                // 12:00: [11:00, 12:00) -> 30
                (pl.col "val")
                    .RollingSumBy("1h", pl.col "time", closed= ClosedWindow.Right) // closed="left" means [ )
                    .Alias "sum_1h"
            )
            |> pl.collect

        Assert.Equal(10L, res.Int("sum_1h", 0).Value)
        Assert.Equal(30L, res.Int("sum_1h", 1).Value)
        Assert.Equal(30L, res.Int("sum_1h", 2).Value)
    [<Fact>]
    member _.``Lazy Join (Standard Join)`` () =

        let users = 
            DataFrame.create [
                Series.create("id", [1; 2])
                Series.create("name", ["Alice"; "Bob"])
                Series.create("common", ["U1"; "U2"]) 
            ]
        let lfUsers = users.Lazy()

        let orders = 
            DataFrame.create [
                Series.create("uid", [1; 1; 3])
                Series.create("amount", [100; 200; 50])
                Series.create("common", ["O1"; "O2"; "O3"]) 
            ]
        let lfOrders = orders.Lazy()

        let res = 
            lfUsers.Join(
                other = lfOrders, 
                leftOn = [pl.col "id"], 
                rightOn = [pl.col "uid"], 
                how = JoinType.Left,
                suffix = "_right_test",            
                validation = JoinValidation.ManyToMany 
            )
            |> pl.collect

        let resSorted = res.Sort([pl.col "id"; pl.col "amount"], [false; false], [false;false]) 

        // Row 0: Alice - 100
        Assert.Equal("Alice", resSorted.Cell<string>(0, "name"))
        Assert.Equal(100, resSorted.Cell<int>(0, "amount"))
        Assert.Equal("U1", resSorted.Cell<string>(0, "common"))             
        Assert.Equal("O1", resSorted.Cell<string>(0, "common_right_test")) 

        // Row 1: Alice - 200
        Assert.Equal("Alice", resSorted.Cell<string>(1, "name"))
        Assert.Equal(200, resSorted.Cell<int>(1, "amount"))

        // Row 2: Bob - null
        Assert.Equal("Bob", resSorted.Cell<string>(2, "name"))
        
        let amountNull = resSorted.Column("amount").IsNullAt 2
        Assert.True(amountNull, "Bob should have null amount")
    [<Fact>]
    member _.``Join AsOf: Trades matching Quotes (with GroupBy and Tolerance)`` () =

        let tradesContent = 
            "time,ticker,volume\n" +
            "1000,AAPL,10\n" +
            "1000,MSFT,20\n" +
            "1005,AAPL,10"
        use tradesCsv = new TempCsv(tradesContent)
        
        let quotesContent = 
            "time,ticker,bid\n" +
            "998,MSFT,50.0\n" +
            "999,AAPL,99.0\n" +
            "1001,AAPL,101.0"
        use quotesCsv = new TempCsv(quotesContent)

        let lfTrades = LazyFrame.ScanCsv tradesCsv.Path |> pl.sortLazy (pl.col "time") false
        let lfQuotes = LazyFrame.ScanCsv quotesCsv.Path |> pl.sortLazy (pl.col "time") false

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


        // Row 0: time=1000, ticker=AAPL. 999 (diff=1 <= 2). Bid=99.0
        // Row 1: time=1000, ticker=MSFT. 998 (diff=2 <= 2). Bid=50.0
        // Row 2: time=1005, ticker=AAPL. 1001 (diff=4 > 2) -> null

        // 1000, AAPL
        Assert.Equal("AAPL", res.String("ticker", 0).Value)
        Assert.Equal(99.0, res.Float("bid", 0).Value)

        // 1000, MSFT
        Assert.Equal("MSFT", res.String("ticker", 1).Value)
        Assert.Equal(50.0, res.Float("bid", 1).Value)

        // 1005, AAPL
        Assert.Equal("AAPL", res.String("ticker", 2).Value)
        Assert.True(res.Float("bid", 2).IsNone) 
    [<Fact>]
    member _.``Test_ETL_Stream_EndToEnd: DataTable -> Polars -> DataTable`` () =

        // Source DB (Mock DataTable) -> DataReader -> Polars Lazy -> Filter/Calc -> SinkTo -> Target DB
        let totalRows = 100_000
        
        // ---------------------------------------------------------
        // Extract
        // ---------------------------------------------------------
        let sourceTable = new DataTable()
        sourceTable.Columns.Add("OrderId", typeof<int>) |> ignore
        sourceTable.Columns.Add("Region", typeof<string>) |> ignore
        sourceTable.Columns.Add("Amount", typeof<double>) |> ignore
        sourceTable.Columns.Add("OrderDate", typeof<DateTime>) |> ignore

        let baseDate = DateTime.Now.Date.AddHours 12.0
        
        for i in 0 .. totalRows - 1 do
            let region = if i % 2 = 0 then "US" else "EU"
            let amount = float i * 1.5
            let date = baseDate.AddDays(float (i % 10))
            sourceTable.Rows.Add(i, region, amount, date) |> ignore

        let readerFactory = fun () -> sourceTable.CreateDataReader() :> IDataReader

        // ---------------------------------------------------------
        // Transform
        // ---------------------------------------------------------
        
        let lf = LazyFrame.scanDb(readerFactory, batchSize=50_000)

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
        // Load
        // ---------------------------------------------------------
        
        let targetTable = new DataTable() 

        let schemaContract = dict [
            "OrderDate", typeof<DateTime>
        ]

        printfn "[ETL] Starting Pipeline..."
        let sw = Stopwatch.StartNew()

        // sourceReader -> DbToArrowStream -> Rust(Filter/Calc) -> Buffer -> ArrowToDbStream -> targetTable.Load
        pipeline.SinkTo(
            (fun reader -> 

                let dateColIndex = reader.GetOrdinal "OrderDate"
                Assert.Equal(typeof<DateTime>, reader.GetFieldType dateColIndex)

                targetTable.Load reader
            ),
            bufferSize = 50000,
            typeOverrides = schemaContract
        )

        sw.Stop()
        printfn "[ETL] Completed in %.3fs. Rows written: %d" sw.Elapsed.TotalSeconds targetTable.Rows.Count


        Assert.Equal(totalRows / 2, targetTable.Rows.Count)

        // 0 * 1.5 * 1.08 = 0
        let row0 = targetTable.Rows.[0]
        Assert.Equal(0, unbox<int> row0.["OrderId"])
        Assert.Equal(0.0, unbox<double> row0.["TaxedAmount"], 4)
        Assert.Equal(baseDate, unbox<DateTime> row0.["OrderDate"])

        // 99998 * 1.5 * 1.08 = 161996.76
        let lastRow = targetTable.Rows.[targetTable.Rows.Count - 1]
        let lastId = 99998
        
        Assert.Equal(lastId, unbox<int> lastRow.["OrderId"])
        
        let expectedAmount = float lastId * 1.5 * 1.08
        let actualAmount = Convert.ToDouble(lastRow.["TaxedAmount"])
        Assert.Equal(expectedAmount, actualAmount, 0.001)

        Assert.True(targetTable.Columns.Contains "TaxedAmount")
        Assert.False(targetTable.Columns.Contains "Amount")
        Assert.False(targetTable.Columns.Contains "Region") 
    [<Fact>]
    member _.``Lazy: GroupBy Dynamic (Rolling Window)`` () =

        // Start: 10:00
        let start = DateTime(2023, 1, 1, 10, 0, 0)
        let data = [
            // Group A
            {| Time = start;                     Category = "A"; Value = 10 |} // 10:00
            {| Time = start.AddMinutes 30.0;    Category = "A"; Value = 20 |} // 10:30
            {| Time = start.AddHours 1.0;       Category = "A"; Value = 30 |} // 11:00
            {| Time = start.AddHours 1.5;       Category = "A"; Value = 40 |} // 11:30
            
            // Group B 
            {| Time = start;                     Category = "B"; Value = 100 |} // 10:00
        ]
        let df = DataFrame.ofRecords data
        let lf = df.Lazy()

        let res = 
            lf.GroupByDynamic(
                indexCol = "Time",
                every = TimeSpan.FromHours 1.0,      
                period = TimeSpan.FromHours 2.0,     
                
                by = [ !> pl.col("Category") ],
                
                // [t, t + period)
                closedWindow = ClosedWindow.Left,

                aggs = [
                    !> pl.col("Value").Count().Alias("Count")
                    !> pl.col("Value").Mean().Alias("Mean")

                    !> pl.cs.numeric().ToExpr().Sum().Name.Suffix("_Sum") 
                ]
            ).Collect().Sort([pl.col "Category"; pl.col "Time"], false) 

        // Window 1 (10:00): [10:00, 12:00) -> 10:00, 10:30, 11:00, 11:30
        // Window 2 (11:00): [11:00, 13:00) -> 11:00, 11:30
        
        Assert.Equal(3L, res.Rows) 

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

        let data = [
            {| ID = 1; Val1 = 10; Val2 = 20 |}
        ]
        
        let lf = 
            DataFrame.ofRecords(data).Lazy()

                .Select([
                    pl.col "ID"
                    pl.asStruct([ pl.col "Val1"; pl.col "Val2" ]).Alias "MyStruct"
                ])
        
        // Schema Check: ID, MyStruct
        
        // Unnest "MyStruct"
        let res = 
            lf.Unnest("MyStruct")
              .Collect()
       
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
        let top2 = 
            lf.TopK(2, [pl.col "V"],reverse=false) 
            |>  pl.collect
        
        Assert.Equal(2L, top2.Rows)
        Assert.Equal(5, top2.Cell<int>("V",0))
        Assert.Equal(4, top2.Cell<int>("V",1))

        // --- BottomK (by V) ---
        let bot2 = 
            lf.BottomK(2, [pl.col "V"], reverse=false) // Ascending
              |> pl.collect
        
        Assert.Equal(1, bot2.Cell<int>("V",0))
        Assert.Equal(2, bot2.Cell<int>("V",1))