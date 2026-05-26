namespace Polars.FSharp.Tests

open Xunit
open Polars.FSharp
open System
open System.Data
open System.Diagnostics
open Polars.NET.Core
open System.IO

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
        Assert.Equal(3L, res.Height)

    [<Fact>]
    member _.``Lazy API Chain (Filter -> Collect)`` () =
        use csv = new TempCsv "a,b\n1,10\n2,20\n3,30"
        let lf = LazyFrame.ScanCsv csv.Path
        
        let df = 
            lf
            |> pl.filterLazy (pl.col "a" .> pl.lit 1)
            |> pl.collect
            |> pl.head 1

        Assert.Equal(1L, df.Height)

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
        
        use df = pl.dataframe[|
            pl.series "name" names
            pl.series "birthdate" dates
        |]


        let keys = [ pl.col("birthdate").Dt.Year() / pl.lit 10 * pl.lit 10 |> pl.alias "decade" ]
        let aggs = [ pl.len() |> alias "cnt" ]
        let havingCond = pl.len() .> pl.lit 1
        
        use res =
            df
            |> pl.asLazy
            |> pl.groupByLazy keys
            |> pl.havingLazy havingCond
            |> pl.aggLazy aggs
            |> pl.sortLazy [pl.col "decade"] false
            |> pl.collect
        res |> pl.show |> ignore
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
            |> pl.groupByLazy [ pl.col "decade" ] 
            |> pl.aggLazy
                [
                    pl.col "name"
                    (pl.col "weight").Mean().Round(2u).Name.Prefix "avg_"
                    (pl.col "height").Mean().Round(2u).Name.Prefix "avg_"
                ]
            |> pl.collect
            |> pl.sort [pl.col "decade"] false

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
        
        Assert.Equal(3L, exploded.Height)
        Assert.Equal("coding", exploded.String("my_tag_list", 0).Value)
        Assert.Equal("reading", exploded.String("my_tag_list", 1).Value)
        Assert.Equal("gaming", exploded.String("my_tag_list", 2).Value)

    [<Fact>]
    [<Trait("Expr","StructOps")>]
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
            |> pl.sort [pl.col "name"] false

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
            ) 
            |> pl.sort [pl.col "year"] false

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
            pl.concatLazy [lf1; lf2] ConcatType.Horizontal
            |> pl.collect
        
        Assert.Equal(1L, dfHorz.Height)
        Assert.Equal(2L, dfHorz.Width)
        Assert.Equal(1L, dfHorz.Int("a", 0).Value)
        Assert.Equal(2L, dfHorz.Int("b", 0).Value)

        // Vertical: [a] (rows=2)        
        let dfVert = 
            pl.concatLazy [lf1; lf3] ConcatType.Vertical
            |> pl.collect
        
        Assert.Equal(2L, dfVert.Height)
        Assert.Equal(1L, dfVert.Width)

        // Diagonal: [a, b] (rows=2)
        // lf1 (a=1, b=null)
        // lf2 (a=null, b=2)
        let dfDiag =
            pl.concatLazy [lf1; lf2] ConcatType.Diagonal
            |> pl.collect
        
        Assert.Equal(2L, dfDiag.Height)
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

        Assert.Equal(2L, bigDf.Height)

        Assert.Equal(1L, df1.Height)
        Assert.Equal(1L, df2.Height)
        Assert.Equal(1L, df1.Int("val", 0).Value)
    [<Fact>]
    member _.``SQL Context: Register and Execute`` () =
        use csv = new TempCsv "name,age\nAlice,20\nBob,30"
        let lf = LazyFrame.ScanCsv csv.Path

        use ctx = pl.sqlContext()
        
        ctx.Register("people", lf)

        let resLf = ctx.Execute "SELECT name, age * 2 AS age_double FROM people WHERE age > 25"
        let res = resLf |> pl.collect

        Assert.Equal(1L, res.Height)
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
            |> pl.sortLazy [pl.col "date"] false 
            |> pl.withColumnLazy (
                // 1.1: 10
                // 1.2: (10+20)/2 = 15
                // 1.3: (20+30)/2 = 25
                (pl.col "price").RollingMean(Dur.String "2i").Alias "ma_2"
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

            |> pl.sortLazy [pl.col "time"] false
            |> pl.withColumnLazy (
                // 10:00: [09:00, 10:00) -> 10
                // 10:30: [09:30, 10:30) -> 10 + 20 = 30
                // 12:00: [11:00, 12:00) -> 30
                (pl.col "val")
                    .RollingSumBy(Dur.String "1h", pl.col "time", closed= ClosedWindow.Right) // closed="left" means [ )
                    .Alias "sum_1h"
            )
            |> pl.collect

        Assert.Equal(10L, res.Int("sum_1h", 0).Value)
        Assert.Equal(30L, res.Int("sum_1h", 1).Value)
        Assert.Equal(30L, res.Int("sum_1h", 2).Value)
    [<Fact>]
    member _.``Lazy Join (Standard Join)`` () =

        let lfUsers = 
            pl.dataframe [
                pl.series "id" [1; 2]
                pl.series "name"  ["Alice"; "Bob"]
                pl.series "common" ["U1"; "U2"] 
            ] 
            |> pl.asLazy 

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

        let lfTrades = LazyFrame.ScanCsv tradesCsv.Path |> pl.sortLazy [pl.col "time"] false
        let lfQuotes = LazyFrame.ScanCsv quotesCsv.Path |> pl.sortLazy [pl.col "time"] false

        let res = 
            lfTrades.JoinAsOf(
                lfQuotes,
                pl.col "time",
                pl.col "time", 
                Tolerance.Integer 2L,                      
                strategy = AsofStrategy.Backward,       
                byLeft = [pl.col "ticker"], 
                byRight = [pl.col "ticker"]
            )
            |> pl.sortLazy [pl.col "ticker"] false
            |> pl.sortLazy [pl.col "time"] false
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
        printfn "[ETL] Completed in %.3fs. Height written: %d" sw.Elapsed.TotalSeconds targetTable.Rows.Count


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

        use res = 
            lf.GroupByDynamic(
                indexCol = "Time",
                every = Dur.TimeSpan(TimeSpan.FromHours 1.0),      
                period = Dur.TimeSpan(TimeSpan.FromHours 2.0),     
                
                by = [ pl.col "Category" ],
                
                // [t, t + period)
                closedWindow = ClosedWindow.Left
            )
            |> pl.aggLazy [
                pl.col("Value").Count().Alias("Count")
                pl.col("Value").Mean().Alias("Mean")
                pl.cs.numeric().ToExpr().Sum().Name.Suffix("_Sum") 
            ]
            |> pl.sortLazy [ pl.col "Category"; pl.col "Time" ] false
            |> pl.collect

        // Window 1 (10:00): [10:00, 12:00) -> 10:00, 10:30, 11:00, 11:30
        // Window 2 (11:00): [11:00, 13:00) -> 11:00, 11:30
        
        Assert.Equal(3L, res.Height) 

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
            DataFrame.ofRecords(data) 
            |> pl.asLazy
            |> pl.selectLazy
                [
                    pl.col "ID"
                    pl.asStruct([ pl.col "Val1"; pl.col "Val2" ]).Alias "MyStruct"
                ]
        // Schema Check: ID, MyStruct
        
        // Unnest "MyStruct"
        let res = 
            lf
            |> pl.unnestColumnsLazy ["MyStruct"]
            |> pl.collect
       
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
        
        Assert.Equal(2L, top2.Height)
        Assert.Equal(5, top2.Cell<int>("V",0))
        Assert.Equal(4, top2.Cell<int>("V",1))

        // --- BottomK (by V) ---
        let bot2 = 
            lf.BottomK(2, [pl.col "V"], reverse=false) // Ascending
              |> pl.collect
        
        Assert.Equal(1, bot2.Cell<int>("V",0))
        Assert.Equal(2, bot2.Cell<int>("V",1))
    [<Fact>]
    [<Trait("DataFrame", "Upsample")>]
    member _.``Upsample with Duration and Selector resolves columns and handles missing intervals correctly`` () =
        // Arrange: Setup a simple time-series dataframe
        // Assume columns are populated with appropriate time indices and values
        let timeSeries = Series.create("datetime", [| new DateTime(2026,5,20,10,0,0); new DateTime(2026,5,20,10,2,0) |])
        let values = Series.create("metrics", [| 42.0; 45.0 |])
        let groups = Series.create("category", [| "A"; "A" |])
        let df = DataFrame.create [ timeSeries; values; groups ]

        // Create selectors based on the project core implementation
        let timeSel = pl.cs.temporal()
        let groupSel = pl.cs.string()

        // Act 1: Upsample using Duration.String
        let durationStr = Dur.String "1m" // Upsample to 1 minute intervals
        let result1 = df.Upsample(timeSel, durationStr, groupBy = groupSel)

        // Assert 1: The gap at 10:01:00 should be upsampled and filled with nulls
        // Initial rows were 2, upsampling 10:00 to 10:02 at 1m interval should create 3 rows total
        Assert.Equal(3L, result1.Height)
        Assert.Equal(3, int result1.Width)

        // Act 2: Upsample using Dur.TimeSpan
        let durationTs = Dur.TimeSpan(TimeSpan.FromMinutes(1.0))
        let result2 = df.Upsample(timeSel, durationTs, maintainOrder = true)

        // Assert 2
        Assert.Equal(3L, result2.Height)

        // Act 3: Test exception when timeColumn selector matches multiple columns
        let invalidTimeSel = pl.cs.all() // Matches all columns, which is invalid for time index
        let action = fun () -> df.Upsample(invalidTimeSel, durationStr) |> ignore
        Assert.Throws<ArgumentException>(action) |> ignore
    [<Fact>]
    [<Trait("DataFrame", "Unstack")>]
    member _.``DataFrame: Unstack vertical, no fill, all columns`` () =
        let data = [
            {| Name = "Alice"; Score = 80 |}
            {| Name = "Bob";   Score = 90 |}
            {| Name = "Cathy"; Score = 70 |}
            {| Name = "Dan";   Score = 60 |}
        ]
        let df = DataFrame.ofRecords data

        let result = df.Unstack(step = 2L)

        Assert.Equal(2L, result.Height)
        Assert.Equal(4L, result.Width)
        Assert.Equal<string[]>([| "Name_0"; "Name_1"; "Score_0"; "Score_1" |], result.Columns)

        Assert.Equal("Alice", result.String("Name_0", 0).Value)
        Assert.Equal(80L, result.Int("Score_0", 0).Value)
        Assert.Equal("Cathy", result.String("Name_1", 0).Value)
        Assert.Equal(70L, result.Int("Score_1", 0).Value)

        // Name_0=Bob, Score_0=90, Name_1=Dan, Score_1=60
        Assert.Equal("Bob", result.String("Name_0", 1).Value)
        Assert.Equal(90L, result.Int("Score_0", 1).Value)
        Assert.Equal("Dan", result.String("Name_1", 1).Value)
        Assert.Equal(60L, result.Int("Score_1", 1).Value)
    [<Fact>]
    [<Trait("DataFrame", "Unstack")>]
    member _.``DataFrame: Unstack vertical with mixed fill expressions`` () =
        let data = [
            {| Name = "Alice"; Score = 80 |}
            {| Name = "Bob";   Score = 90 |}
            {| Name = "Cathy"; Score = 70 |}
        ]
        let df = DataFrame.ofRecords data

        let fills = [pl.lit "你好"; pl.lit -1]

        let result = df.Unstack(step = 2L, fillValues = fills)

        // 3 rows -> step=2 → nRows=2, nCols=2
        Assert.Equal(2L, result.Height)
        Assert.Equal(4L, result.Width)

        // Name_0=Alice, Score_0=80, Name_1=Cathy, Score_1=70
        Assert.Equal("Alice", result.String("Name_0", 0).Value)
        Assert.Equal(80L, result.Int("Score_0", 0).Value)
        Assert.Equal("Cathy", result.String("Name_1", 0).Value)
        Assert.Equal(70L, result.Int("Score_1", 0).Value)

        //Name_0=Bob, Score_0=90, Name_1="你好", Score_1=-1
        Assert.Equal("Bob", result.String("Name_0", 1).Value)
        Assert.Equal(90L, result.Int("Score_0", 1).Value)
        Assert.Equal("你好", result.String("Name_1", 1).Value)
        Assert.Equal(-1L, result.Int("Score_1", 1).Value)
    [<Fact>]
    [<Trait("DataFrame", "Unstack")>]
    member _.``DataFrame: Unstack horizontal, no fill, all columns`` () =
        let data = [
            {| A = 1; B = 2; C = 3 |}
            {| A = 4; B = 5; C = 6 |}
            {| A = 7; B = 8; C = 9 |}
        ]
        let df = DataFrame.ofRecords data

        let result = df.Unstack(step = 2L, how = UnstackDirection.Horizontal)
        Assert.Equal(2L, result.Height)   
        Assert.Equal(6L, result.Width)    

        let expectedCols = [| "A_0"; "A_1"; "B_0"; "B_1"; "C_0"; "C_1" |]
        Assert.Equal<string[]>(expectedCols, result.Columns)

        Assert.Equal(1L, result.Int("A_0", 0).Value)
        Assert.Equal(4L, result.Int("A_1", 0).Value)
        Assert.Equal(2L, result.Int("B_0", 0).Value)
        Assert.Equal(5L, result.Int("B_1", 0).Value)
        Assert.Equal(3L, result.Int("C_0", 0).Value)
        Assert.Equal(6L, result.Int("C_1", 0).Value)

        Assert.Equal(7L, result.Int("A_0", 1).Value)
        Assert.Equal(8L, result.Int("B_0", 1).Value)
        Assert.Equal(9L, result.Int("C_0", 1).Value)
        Assert.Null(result.Int("C_1", 1))
        Assert.Null(result.Int("B_1", 1))
        Assert.Null(result.Int("A_1", 1))
    [<Fact>]
    [<Trait("DataFrame", "Unstack")>]
    member _.``DataFrame: Unstack with column selector`` () =
        let data = [
            {| A = 1; B = "x"; C = 3.0 |}
            {| A = 2; B = "y"; C = 4.0 |}
            {| A = 3; B = "z"; C = 5.0 |}
            {| A = 4; B = "w"; C = 6.0 |}
        ]
        let df = DataFrame.ofRecords data

        let result = df.Unstack(step = 2L, columns = pl.cs.numeric())

        Assert.Equal(4L, result.Width)
        Assert.Equal<string[]>([| "A_0"; "A_1"; "C_0"; "C_1" |], result.Columns)

        Assert.DoesNotContain("B_0", result.Columns)
        Assert.DoesNotContain("B_1", result.Columns)

        Assert.Equal(1L, result.Int("A_0", 0).Value)
        Assert.Equal(3.0, result.Float("C_0", 0).Value)
        Assert.Equal(3L, result.Int("A_1", 0).Value)
        Assert.Equal(5.0, result.Float("C_1", 0).Value)
    [<Fact>]
    [<Trait("DataFrame", "Update")>]
    member _.``DataFrame: Update basic - same schema, no missing keys`` () =
        let leftDf =
            [ pl.series "id" [1;2;3];
            pl.series "value" [10;20;30] ]
            |> pl.dataframe

        let rightDf =
            [ pl.series "id" [2;3];
            pl.series "value" [200;300] ]
            |> pl.dataframe

        let result = leftDf.Update(other = rightDf,on=pl.col "id")

        Assert.Equal(3L, result.Height)
        Assert.Equal(2L, result.Width)
        Assert.Equal(Some 10L,result.Int("value",0))
        Assert.Equal(Some 300L,result.Int("value",2))
    [<Fact>]
    [<Trait("DataFrame", "Update")>]
    member _.``DataFrame: Update with explicit leftOn/rightOn`` () =
        let leftDf =
            [ pl.series "pk_left" [10;20;30];
            pl.series "name" ["Alice";"Bob";"Cathy"] ]
            |> pl.dataframe

        let rightDf =
            [ pl.series "pk_right" [20;30];
            pl.series "name" ["Bobby";"Catherine"] ]
            |> pl.dataframe

        let result = leftDf.Update(other = rightDf, 
                                leftOn = pl.cs.byName ["pk_left"], 
                                rightOn = pl.cs.byName [ "pk_right"])
        Assert.Equal(Some "Alice",result.String("name",0))
        Assert.Equal(Some "Catherine",result.String("name",2))

    [<Fact>]
    [<Trait("DataFrame", "Update")>]
    member _.``DataFrame: Update with includeNulls = true`` () =
        let leftDf =
            [ pl.series "key" [1;2;3];
            pl.series "info" ["A";"B";"C"] ]
            |> pl.dataframe

        let rightDf =
            [ pl.series "key" [2;3];
            pl.series "info" [null; "C_updated"] ]  
            |> pl.dataframe

        let result = leftDf.Update(other = rightDf,on=pl.cs.numeric(),includeNulls = true)

        Assert.Equal(Some "A", result.String("info",0))
        Assert.Equal(None, result.String("info",1))
        Assert.Equal(Some "C_updated", result.String("info",2))

    [<Fact>]
    [<Trait("DataFrame", "Update")>]
    member _.``DataFrame: Update with includeNulls = false (coalesce)`` () =
        let leftDf =
            [ pl.series "key" [1;2;3];
            pl.series "val" [10;20;30] ]
            |> pl.dataframe

        let rightDf =
            [ pl.series "key" [2;3];
            pl.series "val" [None; Some 300] ] 
            |> pl.dataframe

        let result = leftDf.Update(other = rightDf,on=pl.col "key", includeNulls = false)
        
        Assert.Equal(Some 20L, result.Int("val", 1))  

    [<Fact>]
    [<Trait("DataFrame", "Update")>]
    member _.``DataFrame: Update with Inner join - drops non-matching rows`` () =
        let leftDf =
            [ pl.series "id" [1;2;3];
            pl.series "val" [10;20;30] ]
            |> pl.dataframe

        let rightDf =
            [ pl.series "id" [2];
            pl.series "val" [200] ]
            |> pl.dataframe

        let result = leftDf.Update(other = rightDf,on=pl.col "id", how = JoinType.Inner)

        Assert.Equal(1L, result.Height)
        Assert.Equal(Some 2L,  result.Int("id",0))
        Assert.Equal(Some 200L,  result.Int("val",0))

    [<Fact>]
    [<Trait("DataFrame", "Update")>]
    member _.``DataFrame: Update with Outer join - retains all rows from both`` () =
        let leftDf =
            [ pl.series "key" [1;2];
            pl.series "a" [10;20] ]
            |> pl.dataframe

        let rightDf =
            [ pl.series "key" [2;3];
            pl.series "a" [200;300] ]
            |> pl.dataframe

        let result = leftDf.Update(other = rightDf,on=pl.col "key" ,how = JoinType.Outer)
        Assert.Equal(Some 10L, result.Int("a",0))
        Assert.Equal(Some 200L, result.Int("a",1))
        Assert.Equal(Some 300L, result.Int("a",2))

    [<Fact>]
    [<Trait("DataFrame", "Update")>]
    member _.``DataFrame: Update with no common columns besides keys - early return`` () =
        let leftDf =
            [ pl.series "id" [1;2];
            pl.series "x" [10;20] ]
            |> pl.dataframe

        let rightDf = pl.dataframe [ pl.series "id" [1;2] ]

        let result = leftDf.Update(other = rightDf, on=pl.col "id" )

        Assert.True(result.Equals leftDf)
    [<Fact>]
    [<Trait("DataFrame", "JsonNormalize")>]
    member _.``DataFrame: JsonNormalize flat objects without schema`` () =
        let data = [
            Map.ofList [ "name", box "Alice"; "age", box 30 ]
            Map.ofList [ "name", box "Bob";   "age", box 25 ]
        ]
        let df = DataFrame.JsonNormalize(data)

        Assert.Equal(2L, df.Width)
        Assert.Equal(2L, df.Height)

        Assert.Equal(Some "Alice", df.String("name", 0))
        Assert.Equal(Some 30L, df.Int("age", 0))
        Assert.Equal(Some "Bob", df.String("name", 1))
        Assert.Equal(Some 25L, df.Int("age", 1))

    [<Fact>]
    [<Trait("DataFrame", "JsonNormalize")>]
    member _.``DataFrame: JsonNormalize nested objects with maxLevel and schema`` () =
        let nested = Map.ofList [
            "id", box 1
            "info", box (Map.ofList [
                "height", box 180.0
                "weight", box 75.0
            ])
        ]

        let df = DataFrame.JsonNormalize(
                    data = [nested],
                    maxLevel = 1,
                    separator = "_",
                    schema = PolarsSchema.ofList [
                        "id", DataType.Int32
                        "info_height", DataType.Float64
                        "info_weight", DataType.Float64
                    ])

        Assert.Equal(3L, df.Width)
        Assert.Equal(1L, df.Height)

        Assert.Equal(Some 1L, df.Int("id", 0))
        Assert.Equal(Some 180.0, df.Float("info_height", 0))
        Assert.Equal(Some 75.0, df.Float("info_weight", 0))

    [<Fact>]
    [<Trait("DataFrame", "JsonNormalize")>]
    member _.``DataFrame: JsonNormalize with missing fields and strict = false`` () =
        let data = [
            Map.ofList [ "a", box 1; "b", box "x" ]
            Map.ofList [ "a", box 2; "c", box true ]   
        ]

        let df = DataFrame.JsonNormalize(data, strict = false)

        Assert.Equal(3L, df.Width)
        Assert.Equal(2L, df.Height)

        Assert.Equal(Some "x", df.String("b", 0))
        Assert.Equal(None, df.String("b", 1))   
        Assert.Equal(Some true, df.Bool("c", 1))
    [<Fact>]
    [<Trait("DataFrame", "JsonNormalizeFromString")>]
    member _.``DataFrame: JsonNormalize from JSON string (array of objects)`` () =
        let json = """
        [
            {"name":"Alice","age":30,"scores":{"math":90,"eng":88}},
            {"name":"Bob","age":25,"scores":{"math":78,"eng":82}}
        ]
        """
        let df = DataFrame.JsonNormalizeFromString(json, maxLevel = 1, separator = ".")

        Assert.Equal(4L, df.Width)
        Assert.Equal(2L, df.Height)

        Assert.Equal(Some "Alice", df.String("name", 0))
        Assert.Equal(Some 30L, df.Int("age", 0))
        Assert.Equal(Some 90L, df.Int("scores.math", 0))
        Assert.Equal(Some 88L, df.Int("scores.eng", 0))

        Assert.Equal(Some "Bob", df.String("name", 1))
        Assert.Equal(Some 25L, df.Int("age", 1))
        Assert.Equal(Some 78L, df.Int("scores.math", 1))
        Assert.Equal(Some 82L, df.Int("scores.eng", 1))

    [<Fact>]
    [<Trait("DataFrame", "JsonNormalizeFromString")>]
    member _.``DataFrame: JsonNormalize from JSON string (single object)`` () =
        let json = """{"product":"Widget","price":9.99,"inStock":true}"""
        let df = DataFrame.JsonNormalizeFromString(json)

        Assert.Equal(1L, df.Height)
        Assert.Equal(3L, df.Width)
        Assert.Equal(Some "Widget", df.String("product", 0))
        Assert.Equal(Some 9.99, df.Float("price", 0))
        Assert.Equal(Some true, df.Bool("inStock", 0))
    [<Fact>]
    [<Trait("DataFrame", "Merge")>]
    member _.``DataFrame: Merge basic update (no insert)`` () =

        let target = pl.dataframe [
            pl.series "id" [1;2;3]
            pl.series "val" [10;20;30]
        ]

        let source = pl.dataframe [
            pl.series "id" [2;4]
            pl.series "val" [200;400]
        ]

        let result = 
            target.Merge(source, pl.col "id")
            |> Merge.whenMatchedUpdateAll
            |> Merge.executeEager Engine.Auto

        Assert.Equal(3L, result.Height)
        Assert.Equal(Some 1L, result.Int("id", 0))
        Assert.Equal(Some 2L, result.Int("id", 1))
        Assert.Equal(Some 3L, result.Int("id", 2))
        Assert.Equal(Some 10L, result.Int("val", 0))
        Assert.Equal(Some 200L, result.Int("val", 1))
        Assert.Equal(Some 30L, result.Int("val", 2))

    [<Fact>]
    [<Trait("DataFrame", "Merge")>]
    member _.``DataFrame: Merge insert and delete`` () =
        let target = pl.dataframe [
            pl.series "key" [1;2;3]
            pl.series "value" ["A";"B";"C"]
        ]
        let source = pl.dataframe [
            pl.series "key" [2;4]
            pl.series "value" ["B_new";"D"]
        ]

        let result = 
            target.Merge(source, pl.col "key")
            |> Merge.whenMatchedDelete (Some (fun ctx -> ctx.SourceCol "value" .== pl.lit "B_new"))
            |> Merge.whenMatchedUpdateAll
            |> Merge.whenNotMatchedInsertAll
            |> Merge.executeEager Engine.Auto

        Assert.Equal(3L, result.Height)

        Assert.Equal(Some 1L, result.Int("key", 0))
        Assert.Equal(Some "A", result.String("value", 0))
        Assert.Equal(Some 3L, result.Int("key", 1))
        Assert.Equal(Some "C", result.String("value", 1))
        Assert.Equal(Some 4L, result.Int("key", 2))
        Assert.Equal(Some "D", result.String("value", 2))

    [<Fact>]
    [<Trait("DataFrame", "Merge")>]
    member _.``DataFrame: Merge includeNulls = true`` () =
        let target = pl.dataframe [
            pl.series "id" [1;2;3];
            pl.series "score" [100;200;300]
        ]
        let source = pl.dataframe [
            pl.series "id" [2;3];
            pl.series "score" [None; Some 999]  
        ]

        let result = 
            target.Merge(source, pl.col "id")
            |> Merge.whenMatchedUpdateAll
            |> Merge.includeNulls true
            |> Merge.executeEager Engine.Auto

        Assert.Equal(3L, result.Height)
        Assert.Equal(Some 100L, result.Int("score", 0))
        Assert.Equal(None, result.Int("score", 1))          // null
        Assert.Equal(Some 999L, result.Int("score", 2))
    [<Fact>]
    [<Trait("DataFrame", "Merge")>]
    member _.``DataFrame: Merge full pipeline with all actions and parameters`` () =
        let target = pl.dataframe [
            pl.series "key"     [1; 2; 3; 5]
            pl.series "name"    ["Alice"; "Bob"; "Cathy"; "Eve"]
            pl.series "score"   [80; 90; 85; 55]
        ]
        let source = pl.dataframe [
            pl.series "key"     [1; 2; 4; 6]
            pl.series "name"    [null; "Bobby"; "Diana"; "Frank"]
            pl.series "score"   [85; 95; 75; 65]
        ]

        let result =
            target.Merge(source, pl.col "key")
            |> Merge.whenMatchedDelete (Some (fun ctx -> (ctx.SourceCol "name").Str.StartsWith "B"))
            |> Merge.whenMatchedUpdateSet (Set.build [
                Set.col "score" (fun ctx -> ctx.SourceCol "score" * pl.lit 2)
            ])
            |> Merge.whenNotMatchedInsertAll
            |> Merge.whenNotMatchedBySourceDelete
                (Some (fun ctx -> ctx.TargetCol "score" .< pl.lit 80))
            |> Merge.maintainOrder JoinMaintainOrder.Left
            |> Merge.executeEager Engine.Auto

        Assert.Equal(4L, result.Height)
        
        Assert.Equal(Some 1L,      result.Int("key", 0))
        Assert.Equal(Some "Alice",         result.String("name", 0))      // null
        Assert.Equal(Some 170L,    result.Int("score", 0))

        Assert.Equal(Some 3L,      result.Int("key", 1))
        Assert.Equal(Some "Cathy", result.String("name", 1))
        Assert.Equal(Some 85L,     result.Int("score", 1))

        Assert.Equal(Some 4L,      result.Int("key", 2))
        Assert.Equal(Some "Diana", result.String("name", 2))
        Assert.Equal(Some 75L,     result.Int("score", 2))

        Assert.Equal(Some 6L,      result.Int("key", 3))
        Assert.Equal(Some "Frank", result.String("name", 3))
        Assert.Equal(Some 65L,     result.Int("score", 3))
    [<Fact>]
    [<Trait("DataFrame", "Merge")>]
    member _.``DataFrame: Merge respects first-match-wins (Delete before Update)`` () =
        let target = pl.dataframe [
            pl.series "Id"    [1; 2; 3]
            pl.series "Score" [10; 20; 30]
        ]

        let source = pl.dataframe [
            pl.series "Id"    [2; 3; 4]
            pl.series "Score" [99; 15; 100]
        ]
        
        let result =
            target.Merge(source, pl.col "Id")
            |> Merge.whenMatchedDelete (Some (fun ctx -> ctx.SourceCol "Score" .> pl.lit 90))
            |> Merge.whenMatchedUpdate (Some (fun ctx -> ctx.SourceCol "Score" .> pl.lit 10)) None
            |> Merge.whenNotMatchedInsertAll
            |> Merge.maintainOrder JoinMaintainOrder.Left
            |> Merge.executeEager Engine.Auto

        Assert.Equal(3L, result.Height)
        Assert.Equal(Some 1L,   result.Int("Id", 0))
        Assert.Equal(Some 10L,  result.Int("Score", 0))
        Assert.Equal(Some 3L,   result.Int("Id", 1))
        Assert.Equal(Some 15L,  result.Int("Score", 1)) 
        Assert.Equal(Some 4L,   result.Int("Id", 2))
        Assert.Equal(Some 100L, result.Int("Score", 2))
    [<Fact>]
    [<Trait("DataFrame", "Merge")>]
    member _.``DataFrame: Merge partial update with setters`` () =
        let target = pl.dataframe [
            pl.series "Id" [1;2;3]
            pl.series "Name" ["Apple";"Banana";"Orange"]
            pl.series "Price" [1.0;2.0;3.0]
            pl.series "Stock" [100;150;200]
        ]
        let source = pl.dataframe [
            pl.series "Id" [2;3;4]
            pl.series "Name" ["Banana_Ignored";"Orange_Ignored";"Grape"]
            pl.series "Price" [2.5;2.5;4.0]
            pl.series "RestockQty" [50;0;200]
        ]

        let result =
            target.Merge(source, pl.col "Id")
            |> Merge.whenMatchedUpdate
                (Some (fun ctx -> ctx.SourceCol "Price" .> ctx.TargetCol "Price"))
                (Some (Set.build [
                    Set.col "Price" (fun ctx -> ctx.SourceCol "Price")
                    Set.col "Stock" (fun ctx -> ctx.TargetCol "Stock" + ctx.SourceCol "RestockQty")
                ]))
            |> Merge.whenNotMatchedInsertSet (Set.build [
                Set.col "Id"    (fun ctx -> ctx.SourceCol "Id")
                Set.col "Name"  (fun ctx -> ctx.SourceCol "Name")
                Set.col "Price" (fun ctx -> ctx.SourceCol "Price")
                Set.col "Stock" (fun ctx -> ctx.SourceCol "RestockQty")
            ])
            |> Merge.executeEager Engine.Auto
            |> pl.sort [pl.col "Id"] false

        // Id=1: Apple, 1.0, 100
        Assert.Equal(Some 1L,   result.Int("Id", 0))
        Assert.Equal(Some "Apple", result.String("Name", 0))
        Assert.Equal(Some 1.0,  result.Float("Price", 0))
        Assert.Equal(Some 100L, result.Int("Stock", 0))

        // Id=2: Banana, 2.5, 200 (Stock=150+50)
        Assert.Equal(Some 2L,   result.Int("Id", 1))
        Assert.Equal(Some "Banana", result.String("Name", 1))
        Assert.Equal(Some 2.5,  result.Float("Price", 1))
        Assert.Equal(Some 200L, result.Int("Stock", 1))

        // Id=3: Orange, 3.0, 200 (condition missed)
        Assert.Equal(Some 3L,   result.Int("Id", 2))
        Assert.Equal(Some "Orange", result.String("Name", 2))
        Assert.Equal(Some 3.0,  result.Float("Price", 2))
        Assert.Equal(Some 200L, result.Int("Stock", 2))

        // Id=4: Grape, 4.0, 200
        Assert.Equal(Some 4L,   result.Int("Id", 3))
        Assert.Equal(Some "Grape", result.String("Name", 3))
        Assert.Equal(Some 4.0,  result.Float("Price", 3))
        Assert.Equal(Some 200L, result.Int("Stock", 3))
    [<Fact>]
    [<Trait("DataFrame", "Merge")>]
    member _.``DataFrame: Merge with selector-based batch setters`` () =
        let target = pl.dataframe [
            pl.series "Id"       [1; 2; 3]
            pl.series "Name"     ["Hero"; "Villain"; "NPC"]
            pl.series "Stat_HP"  [100; 100; 10]
            pl.series "Stat_MP"  [50; 50; 0]
            pl.series "Score"    [1000; 500; 0]
            pl.series "Tag"      ["Old"; "Old"; "Old"]
        ]
        let source = pl.dataframe [
            pl.series "Id"         [1; 2; 4]
            pl.series "Name"       ["Hero_Ignored"; "Villain"; "Newbie"]
            pl.series "Stat_HP"    [120; 0; 80]
            pl.series "Stat_MP"    [60; 0; 40]
            pl.series "ScoreDelta" [200; 0; 50]
            pl.series "IsBanned"   [false; true; false]
        ]

        let result =
            target.Merge(source, pl.col "Id")
            |> Merge.whenMatchedDelete (Some (fun ctx -> ctx.SourceCol "IsBanned"))
            |> Merge.whenMatchedUpdateSet (Set.build [
                Set.selector (pl.cs.startsWith "Stat_") (fun colName ctx -> ctx.SourceCol colName)
                Set.col "Score" (fun ctx -> ctx.TargetCol "Score" + ctx.SourceCol "ScoreDelta")
                Set.col "Tag"   (fun _ -> pl.lit "Updated")
            ])
            |> Merge.whenNotMatchedInsertSet (Set.build [
                Set.col "Id"    (fun ctx -> ctx.SourceCol "Id")
                Set.col "Name"  (fun ctx -> ctx.SourceCol "Name")
                Set.selector (pl.cs.startsWith "Stat_") (fun colName ctx -> ctx.SourceCol colName)
                Set.col "Score" (fun ctx -> ctx.SourceCol "ScoreDelta")
                Set.col "Tag"   (fun _ -> pl.lit "New")
            ])
            |> Merge.printPlan
            |> Merge.execute
            |> pl.collect

        let sorted = result |> pl.sort [pl.col "Id"] false

        // Id=1: Hero, HP=120, MP=60, Score=1200, Tag=Updated
        Assert.Equal(Some 1L,       sorted.Int("Id", 0))
        Assert.Equal(Some "Hero",   sorted.String("Name", 0))
        Assert.Equal(Some 120L,     sorted.Int("Stat_HP", 0))
        Assert.Equal(Some 60L,      sorted.Int("Stat_MP", 0))
        Assert.Equal(Some 1200L,    sorted.Int("Score", 0))
        Assert.Equal(Some "Updated",sorted.String("Tag", 0))

        // Id=3: NPC (untouched)
        Assert.Equal(Some 3L,       sorted.Int("Id", 1))
        Assert.Equal(Some "NPC",    sorted.String("Name", 1))
        Assert.Equal(Some 10L,      sorted.Int("Stat_HP", 1))
        Assert.Equal(Some 0L,       sorted.Int("Stat_MP", 1))
        Assert.Equal(Some 0L,       sorted.Int("Score", 1))
        Assert.Equal(Some "Old",    sorted.String("Tag", 1))

        // Id=4: Newbie, HP=80, MP=40, Score=50, Tag=New
        Assert.Equal(Some 4L,       sorted.Int("Id", 2))
        Assert.Equal(Some "Newbie", sorted.String("Name", 2))
        Assert.Equal(Some 80L,      sorted.Int("Stat_HP", 2))
        Assert.Equal(Some 40L,      sorted.Int("Stat_MP", 2))
        Assert.Equal(Some 50L,      sorted.Int("Score", 2))
        Assert.Equal(Some "New",    sorted.String("Tag", 2))

        Assert.Equal(3L, sorted.Height)
    [<Fact>]
    [<Trait("DataFrame", "Merge")>]
    member _.``Merge composite keys with EndsWith selector`` () =
        let target = pl.dataframe [
            pl.series "TenantId" ["T1";"T1";"T2";"T2"]
            pl.series "UserId"   [101;102;101;999]
            pl.series "Role"     ["Admin";"User";"User";"Guest"]
            pl.series "IsActive" [true;true;true;false]
        ]
        let source = pl.dataframe [
            pl.series "TenantId" ["T1";"T2";"T2"]
            pl.series "UserId"   [102;101;888]
            pl.series "Role"     ["Editor";"Admin";"User"]
            pl.series "IsActive" [true;true;true]
        ]

        let result =
            target.Merge(source, pl.cs.endsWith "Id")
            |> Merge.whenMatchedUpdateSet (Set.build [
                Set.col "Role"     (fun ctx -> ctx.SourceCol "Role")
                Set.col "IsActive" (fun _   -> pl.lit true)
            ])
            |> Merge.whenNotMatchedBySourceDelete None   
            |> Merge.whenNotMatchedInsertSet (Set.build [
                Set.col "TenantId" (fun ctx -> ctx.SourceCol "TenantId")
                Set.col "UserId"   (fun ctx -> ctx.SourceCol "UserId")
                Set.col "Role"     (fun ctx -> ctx.SourceCol "Role")
                Set.col "IsActive" (fun _   -> pl.lit true)
            ])
            |> Merge.executeEager Engine.Auto

        let sorted = result |> pl.sort [pl.col "TenantId"; pl.col "UserId"] false

        Assert.Equal(3L, sorted.Height)

        // T1, 102: Editor, Active=true
        Assert.Equal(Some "T1", sorted.String("TenantId", 0))
        Assert.Equal(Some 102L, sorted.Int("UserId", 0))
        Assert.Equal(Some "Editor", sorted.String("Role", 0))
        Assert.Equal(Some true, sorted.Bool("IsActive", 0))

        // T2, 101: Admin, Active=true
        Assert.Equal(Some "T2", sorted.String("TenantId", 1))
        Assert.Equal(Some 101L, sorted.Int("UserId", 1))
        Assert.Equal(Some "Admin", sorted.String("Role", 1))
        Assert.Equal(Some true, sorted.Bool("IsActive", 1))

        // T2, 888: User, Active=true (insert)
        Assert.Equal(Some "T2", sorted.String("TenantId", 2))
        Assert.Equal(Some 888L, sorted.Int("UserId", 2))
        Assert.Equal(Some "User", sorted.String("Role", 2))
        Assert.Equal(Some true, sorted.Bool("IsActive", 2))

    [<Fact>]
    [<Trait("DataFrame", "Merge")>]
    member _.``Merge validation: type mismatch on key`` () =
        let target = pl.dataframe [
            pl.series "Id" [1;2;3]
            pl.series "Value" [10;20;30]
        ]
        let source = pl.dataframe [
            pl.series "Id" ["1";"2"]   
            pl.series "Value" [100;200]
        ]

        let ex = Assert.Throws<ArgumentException>(fun () ->
            target.Merge(source, pl.col "Id")
            |> Merge.executeEager Engine.Auto
            |> ignore
        )
        Assert.Contains("Key type mismatch for 'Id'", ex.Message)
        Assert.Contains("source: str", ex.Message)
        Assert.Contains("target: i32", ex.Message)

    [<Fact>]
    [<Trait("DataFrame", "Merge")>]
    member _.``Merge validation: null merge key`` () =
        let target = pl.dataframe [
            pl.series "Id" [1;2;3]
            pl.series "Value" ["A";"B";"C"]
        ]
        let source = pl.dataframe [
            pl.series "Id" [Nullable(1); Nullable<int>(); Nullable(3)]   // 中间为 null
            pl.series "Value" ["A1";"B1";"C1"]
        ]

        let ex = Assert.Throws<InvalidDataException>(fun () ->
            target.Merge(source, pl.col "Id")
            |> Merge.whenMatchedUpdateAll
            |> Merge.executeEager Engine.Auto
            |> ignore
        )
        Assert.Contains("Null values found in merge keys: Id", ex.Message)

    [<Fact>]
    [<Trait("DataFrame", "Merge")>]
    member _.``Merge validation: duplicate source keys`` () =
        let target = pl.dataframe [
            pl.series "TenantId" ["T1";"T2"]
            pl.series "UserId" [101;102]
            pl.series "Score" [10;20]
        ]
        let source = pl.dataframe [
            pl.series "TenantId" ["T1";"T1";"T2"]
            pl.series "UserId"   [101;101;102]
            pl.series "Score"    [100;999;200]
        ]

        let ex = Assert.Throws<InvalidDataException>(fun () ->
            target.Merge(source, pl.cs.endsWith "Id")   
            |> Merge.whenMatchedUpdateAll
            |> Merge.executeEager Engine.Auto
            |> ignore
        )
        Assert.Contains("Duplicate keys found in source for", ex.Message)
    [<Fact>]
    [<Trait("DataFrame", "Merge")>]
    member _.``Merge: includeNulls controls null overwrite in default update (no setters)`` () =
        let target = pl.dataframe [
            pl.series "key" [1;2]
            pl.series "val" ["A";"B"]
        ]
        let source = pl.dataframe [
            pl.series "key" [1;2]
            pl.series "val" [null; "new_B"]
        ]

        let resultNoNull =
            target.Merge(source, pl.col "key")
            |> Merge.whenMatchedUpdateAll
            |> Merge.includeNulls false
            |> Merge.executeEager Engine.Auto

        Assert.Equal(Some "A", resultNoNull.String("val", 0))
        Assert.Equal(Some "new_B", resultNoNull.String("val", 1))

        let resultWithNull =
            target.Merge(source, pl.col "key")
            |> Merge.whenMatchedUpdateAll
            |> Merge.includeNulls true
            |> Merge.executeEager Engine.Auto

        Assert.Equal(None, resultWithNull.String("val", 0))          // null
        Assert.Equal(Some "new_B", resultWithNull.String("val", 1))