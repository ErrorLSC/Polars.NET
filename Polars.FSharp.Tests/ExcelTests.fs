namespace Polars.FSharp.Tests

open System
open System.IO
open Xunit
open Polars.FSharp
open MiniExcelLibs 

type TestExcelRow = {
    Id: int
    Name: string
    Score: float
    IsActive: bool
    JoinDate: DateTime
}

type FSharpExcelTests() =

    [<Fact>]
    member _.``IO: Read Excel (Native Roundtrip with MiniExcel)`` () =
        let tempFile = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString() + ".xlsx")
        
        try

            let data = [
                { Id = 1; Name = "Alice"; Score = 99.5; IsActive = true;  JoinDate = DateTime(2022, 1, 1) }
                { Id = 2; Name = "Bob";   Score = 88.0; IsActive = false; JoinDate = DateTime(2023, 5, 20) }
                { Id = 3; Name = "C";     Score = 60.0; IsActive = true;  JoinDate = DateTime(2024, 12, 31) }
            ]

            MiniExcel.SaveAs(tempFile, data) |> ignore

            let testDefaultRead() =
                use df = DataFrame.ReadExcel tempFile
                
                Assert.Equal(3L, df.Height)
                
                Assert.Equal(1.0, df.Float("Id", 0).Value) 

                Assert.Equal("Alice", df.String("Name", 0).Value)
 
                Assert.Equal(DateTime(2022, 1, 1), df.DateTime("JoinDate", 0).Value)

            testDefaultRead()

            let testSchemaRead() =
 
                use schema = PolarsSchema.ofList([
                    "Id", DataType.Int64
                    "Score", DataType.String
                ])
                use df = DataFrame.ReadExcel(tempFile, schema=schema)

                Assert.Equal(DataType.Int64, df.Schema.["Id"])
                Assert.Equal(DataType.String, df.Schema.["Score"])

                // Id: Float(1.0) -> Int64(1)
                Assert.Equal(1L, df.Int("Id", 0).Value)
                
                // Score: Float(99.5) -> String("99.5")
                Assert.Equal("99.5", df.String("Score", 0).Value)

            testSchemaRead()

        finally
            if File.Exists tempFile then File.Delete tempFile
    [<Fact>]
    member _.``IO: Excel Roundtrip (Precision & Formats)`` () =
        let tempFile = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString() + ".xlsx")
        
        try
            let bigId = 18446744073709551615UL 
            
            use sId = Series.create("Id", [| 1 |])
            use sBigId = Series.create("BigId", [| bigId |])
            use sDate = Series.create("MyDate", [| DateTime(2023, 10, 1) |])
            
            use df = DataFrame.create [sId; sBigId; sDate]

            df.WriteExcel(
                tempFile, 
                sheetName="Data", 
                dateFormat="dd-mm-yyyy"
            )

            use schema = PolarsSchema.ofList ["BigId", DataType.String]

            use dfRead = DataFrame.ReadExcel(tempFile, schema=schema)
            Assert.Equal(DataType.String, dfRead.Schema.["BigId"])
            
            let readBigIdStr = dfRead.String("BigId", 0).Value
            Assert.Equal(bigId.ToString(), readBigIdStr)

            let readDate = dfRead.DateTime("MyDate", 0).Value
            Assert.Equal(DateTime(2023, 10, 1), readDate)

        finally
            if File.Exists tempFile then File.Delete tempFile