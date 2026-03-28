namespace Polars.FSharp.Tests

open System
open Xunit
open Polars.FSharp
open Apache.Arrow
type Product = {
    Name: string 
    Price: decimal
    InStock: bool option
}
[<CLIMutable>]
    type ComplexData = {
        Id: int
        Name: string option       
        Score: float option      
        Tags: string list         
        Metadata: InnerMeta option 
        CreatedAt: DateTime
    }
    and [<CLIMutable>] InnerMeta = {
        Code: string
        Level: int
    }
type ``Extensions Tests`` () =

    [<Fact>]
    member _.``Extensions: Series <-> Seq`` () =
        // 1. Generic Create (ofOptionSeq)
        let data = [Some 10; None; Some 30]
        use s = Series.ofOptionSeq("nums", data)
        
        Assert.Equal("nums", s.Name)
        Assert.Equal(3L, s.Length)

        // 2. Generic Retrieve (AsSeq)
        let res = s.AsSeq<int>() |> Seq.toList
        
        Assert.Equal(Some 10, res.[0])
        Assert.Equal(None, res.[1])
        Assert.Equal(Some 30, res.[2])

    [<Fact>]
    member _.``Extensions: Series Map (UDF)`` () =

        use s = Series.create("val", [10; 20; 30])

        // Define UDF (Int -> Double / 2)
        let logic (arr: IArrowArray) : IArrowArray =
            let iArr = arr :?> Int32Array
            let b = new DoubleArray.Builder()
            for i in 0 .. iArr.Length - 1 do
                if iArr.IsNull i then b.AppendNull() |> ignore
                else b.Append(float (iArr.GetValue(i).Value) / 2.0) |> ignore
            b.Build()

        // Run Map directly on Series
        use sRes = s.Map(Func<_,_>(logic))

        // 4. Verify
        let res = sRes.AsSeq<double>() |> Seq.toList
        Assert.Equal(5.0, res.[0].Value)
        Assert.Equal(10.0, res.[1].Value)
        Assert.Equal(15.0, res.[2].Value)
        Assert.Equal("val", sRes.Name) 


    [<Fact>]
    [<Trait("Extension","ComplexType")>]
    member _.``Interop: Full Complex Type Roundtrip`` () =
        let data = [
            { 
                Id = 1
                Name = Some "Alice"
                Score = Some 99.5
                Tags = ["dev"; "fsharp"]
                Metadata = Some { Code = "A1"; Level = 10 }
                CreatedAt = DateTime(2023, 1, 1) 
            }
            { 
                Id = 2
                Name = None
                Score = None
                Tags = []
                Metadata = None
                CreatedAt = DateTime(2023, 1, 2) 
            }
        ]

        // Seq -> Series -> DataFrame
        use df = DataFrame.create [
            Series.ofSeq("data", data) 
        ]
        

        df.PrintSchema() 
        df.GlimpseFrame() |> pl.show |> ignore
        
        // DataFrame -> Seq
        let dfFlat =
            df |>pl.unnestColumn "data"
        
        let readBack = dfFlat.ToRecords<ComplexData>() |> Seq.toList
        Assert.Equal(2, readBack.Length)
        
        let row1 = readBack.[0]
        Assert.Equal(Some "Alice", row1.Name)
        Assert.Equal(Some 99.5, row1.Score)
        Assert.Equal(10, row1.Metadata.Value.Level)
        
        let row2 = readBack.[1]
        Assert.True row2.Name.IsNone
        Assert.True row2.Metadata.IsNone