namespace Polars.FSharp.Tests

open System
open System.IO
open Xunit
open Polars.FSharp

type IOTests() =
    
    let tempPath = Path.Combine(Path.GetTempPath(), $"scan_test_{Guid.NewGuid()}.parquet")

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

    interface IDisposable with
        member _.Dispose() =
            if File.Exists tempPath then File.Delete tempPath

    [<Fact>]
    member _.``ScanParquet (File): nRows and RowIndex``() =

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
        let bytes = File.ReadAllBytes tempPath

        use mySchema = new PolarsSchema([
            "id", DataType.Int32
            "val", DataType.Float64
            "cat", DataType.String
        ])

        let lf = LazyFrame.ScanParquet(
            bytes,
            schema = mySchema,
            lowMemory = true,
            useStatistics = false 
        )

        let df = lf.Collect()

        Assert.Equal(5L, df.Height)
        Assert.Equal("A", df.Column("cat").GetValue<string>(0))
        Assert.Equal(50.5, df.Column("val").GetValue<double>(4))

    [<Fact>]
    member _.``ScanParquet: Schema handle disposal safety``() =

        let bytes = File.ReadAllBytes tempPath
        let mutable lf = Unchecked.defaultof<LazyFrame>

        let runScope () =
            use schema = new PolarsSchema([
                "id", DataType.Int32
                "val", DataType.Float64
                "cat", DataType.String
                ])
            lf <- LazyFrame.ScanParquet(bytes, schema=schema)

        runScope()

        let df = lf.Collect()
        Assert.True(df.Height > 0L)
    [<Fact>]
    member _.``IO: Advanced CSV Reading (Schema, Skip, Dates)`` () =
        let path = "advanced_test.csv"
        try
            let content = 
                "IGNORE_THIS_LINE\n" +
                "id;date_col;val_col\n" +
                "007;2023-01-01;99.9\n" +
                "008;2023-12-31;10.5"
            System.IO.File.WriteAllText(path, content)

            use mySchema = new PolarsSchema([
                "id", pl.string
                "date_col", pl.date
                "val_col", pl.float64
            ])

            let df = DataFrame.ReadCsv(
                path,
                skipRows = 1UL,      
                separator = ';',
                tryParseDates = true,
                schema = mySchema
            )

            Assert.Equal(2L, df.Height) // Rows -> Height

            Assert.Equal(pl.string, df.Column("id").DataType)
            
            Assert.Equal("007", df.Column("id").GetValue<string>(0))
            Assert.Equal(99.9, df.Column("val_col").GetValue<double>(0))
            
            let dateVal = df.Column("date_col").GetValue<DateOnly>(0)
            Assert.Equal(DateOnly(2023, 1, 1), dateVal)

        finally
            if File.Exists path then File.Delete path
    [<Fact>]
    member _.``ScanCsv (Memory): Basic & Options``() =
        let csvString = 
            """name,age,active
Alice,30,true
Bob,25,false
Charlie,35,true"""
        
        let bytes = System.Text.Encoding.UTF8.GetBytes csvString

        let lf = LazyFrame.ScanCsv(
            bytes, 
            hasHeader = true,
            nRows = 2UL 
        )

        let df = lf.Collect()

        Assert.Equal(2L, df.Height)
        Assert.Equal("Alice", df.Column("name").GetValue<string>(0))
        Assert.Equal(30, df.Column("age").GetValue<int>(0))

        Assert.Equal("Bob", df.Column("name").GetValue<string>(1))
    [<Fact>]
    member _.``Can read&write Parquet`` () =
        use csv = new DisposableFile(".csv", "a,b,c,d\n1,2,3,4")
        use df = DataFrame.ReadCsv(csv.Path, tryParseDates=false)

        use parquet = new DisposableFile ".parquet"
        
        df.WriteParquet parquet.Path |> ignore

        Assert.True(File.Exists parquet.Path, $"Parquet file should exist at {parquet.Path}")

        use df2 = DataFrame.ReadParquet parquet.Path
        Assert.Equal(df.Height, df2.Height)
        Assert.Equal(4L, df2.Width)

    [<Fact>]    
    member _.``IO: Read JSON (File, Bytes, Stream)`` () =
        use jsonFile = new DisposableFile ".json"

        let s1 = Series.create("a", [1; 2; 3])
        let s2 = Series.create("b", ["x"; "y"; "z"])
        use df = DataFrame.create [s1; s2]
        
        df.WriteJson jsonFile.Path |> ignore
        Assert.True(File.Exists jsonFile.Path, "JSON file not found")

        // ---------------------------------------------------
        // File
        // ---------------------------------------------------
        use dfFile = DataFrame.ReadJson jsonFile.Path
        Assert.Equal(3L, dfFile.Height)
        Assert.Equal(1L, dfFile.Int("a", 0).Value)

        // ---------------------------------------------------
        // Bytes
        // ---------------------------------------------------
        let bytes = File.ReadAllBytes jsonFile.Path
        use dfBytes = DataFrame.ReadJson bytes
        
        Assert.Equal(3L, dfBytes.Height)
        Assert.Equal("y", dfBytes.String("b", 1).Value) 

        // ---------------------------------------------------
        // Stream
        // ---------------------------------------------------
        use fs = File.OpenRead jsonFile.Path
        use dfStream = DataFrame.ReadJson fs
        
        Assert.Equal(3L, dfStream.Height)
        Assert.Equal("z", dfStream.String("b", 2).Value) 
    [<Fact>]
    member _.``Lazy: Scan NDJSON (All Modes - Manual IO)`` () =

        let tempStub = Path.GetTempFileName()
        let path = Path.ChangeExtension(tempStub, ".ndjson")

        try

            let content = 
                [ "{\"id\": 1, \"val\": 100, \"tag\": \"A\"}"
                  "{\"id\": 2, \"val\": 200, \"tag\": \"B\"}"
                  "{\"id\": 3, \"val\": 300, \"tag\": \"C\"}" ]
                |> String.concat "\n"

            File.WriteAllText(path, content)

            // =================================================================
            // File Mode
            // =================================================================
            use schema = new PolarsSchema(["val", DataType.Int32])

            let testFileMode () =
                use lf = LazyFrame.ScanNdjson(path, schema=schema, nRows=3UL)
                use df = lf.Collect()

                Assert.Equal(3L, df.Height)
                
                Assert.Equal(DataType.Int32, df.Column("val").DataType)
                Assert.Equal(100L, df.Int("val", 0).Value)

            testFileMode()

            // =================================================================
            // MMemory Mode (Bytes)
            // =================================================================
            let bytes = File.ReadAllBytes path

            let testMemoryMode () =
                use lf = LazyFrame.ScanNdjson bytes
                use df = lf.Collect()

                Assert.Equal(3L, df.Height)
                
                Assert.Equal(DataType.Int64, df.Column("val").DataType)
                Assert.Equal(200L, df.Int("val", 1).Value)
                Assert.Equal("B", df.String("tag", 1).Value)

            testMemoryMode()

            // =================================================================
            // Stream Mode
            // =================================================================
            let testStreamMode () =
                use fs = File.OpenRead path
                
                use lf = LazyFrame.ScanNdjson(fs)
                use df = lf.Collect()

                Assert.Equal(3L, df.Height)

                Assert.Equal(3L, df.Int("id", 2).Value)

            testStreamMode()

        finally
            if File.Exists path then File.Delete path
            if File.Exists tempStub then File.Delete tempStub
    [<Fact>]
    member _.``IO: Read IPC (All Modes)`` () =

        let tempStub = Path.GetTempFileName()
        let path = Path.ChangeExtension(tempStub, ".ipc")

        try
            let s1 = Series.create("id", [1; 2; 3; 4; 5])
            let s2 = Series.create("val", ["A"; "B"; "C"; "D"; "E"])
            
            use dfOrig = DataFrame.create [s1; s2]
            
            dfOrig.WriteIpc path |> ignore

            // =================================================================
            // File Mode
            // =================================================================
            let testFileMode () =
                use df = DataFrame.ReadIpc(
                    path, 
                    columns=["val"], 
                    nRows=3UL
                )

                Assert.Equal(3L, df.Height)   
                Assert.Equal(1L, df.Width)  
                Assert.Equal("val", df.ColumnNames.[0])
                
                Assert.Equal("A", df.String("val", 0).Value)
                Assert.Equal("C", df.String("val", 2).Value)

            testFileMode()

            // =================================================================
            // Memory Mode (Bytes)
            // =================================================================
            let bytes = File.ReadAllBytes path

            let testMemoryMode () =
                use df = DataFrame.ReadIpc bytes

                Assert.Equal(5L, df.Height)
                Assert.Equal(2L, df.Width)
                Assert.Equal(5L, df.Int("id", 4).Value) 

            testMemoryMode()

            // =================================================================
            // Stream Mode
            // =================================================================
            let testStreamMode () =
                use fs = File.OpenRead path
                
                use df = DataFrame.ReadIpc fs

                Assert.Equal(5L, df.Height)
                Assert.Equal("E", df.String("val", 4).Value)

            testStreamMode()

        finally

            if File.Exists path then File.Delete path
            if File.Exists tempStub then File.Delete tempStub
    [<Fact>]
    member _.``Lazy: Scan IPC (All Modes & UnifiedArgs)`` () =

        let tempStub = Path.GetTempFileName()
        let path = Path.ChangeExtension(tempStub, ".ipc")

        try
            let s1 = Series.create("id", [1; 2; 3; 4; 5])
            let s2 = Series.create("val", ["A"; "B"; "C"; "D"; "E"])
            
            use dfOrig = DataFrame.create [s1; s2]
            dfOrig.Lazy().SinkIpc(
                path, 
                compression = IpcCompression.LZ4, 
                maintainOrder = true
            )

            // =================================================================
            // File Mode
            // =================================================================
            let testFileMode () =
                use lf = LazyFrame.ScanIpc(
                    path, 
                    nRows=3UL, 
                    rowIndexName="idx_col"
                )
                
                use df = lf.Collect()

                Assert.Equal(3L, df.Height)
                
                let cols = df.ColumnNames
                Assert.Contains("idx_col", cols)

                Assert.Equal(3L, df.Int("id", 2).Value)
                Assert.Equal("C", df.String("val", 2).Value)

            testFileMode()

            // =================================================================
            // Memory Mode (Bytes)
            // =================================================================
            let bytes = File.ReadAllBytes path

            let testMemoryMode () =
                use lf = LazyFrame.ScanIpc bytes
                
                use df = lf.Filter(col "id" .> pl.lit 3 ).Collect()

                // id > 3 -> 4, 5 (2 rows)
                Assert.Equal(2L, df.Height)
                Assert.Equal(5L, df.Int("id", 1).Value)

            testMemoryMode()

            // =================================================================
            // 4. Stream Mode
            // =================================================================
            let testStreamMode () =
                use fs = File.OpenRead path
                
                // Stream -> MemoryStream -> Bytes -> ScanSources
                use lf = LazyFrame.ScanIpc fs
                use df = lf.Collect()

                Assert.Equal(5L, df.Height)
                Assert.Equal("E", df.String("val", 4).Value)

            testStreamMode()

        finally
            if File.Exists path then File.Delete path
            if File.Exists tempStub then File.Delete tempStub