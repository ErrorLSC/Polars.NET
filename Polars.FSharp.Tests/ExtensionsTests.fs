namespace Polars.FSharp.Tests

open System
open Xunit
open Polars.FSharp
open Apache.Arrow

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
        // 1. Create Series
        use s = Series.create("val", [10; 20; 30])

        // 2. Define UDF (Int -> Double / 2)
        // 复用之前的 Udf.intToDouble 或者手写
        let logic (arr: IArrowArray) : IArrowArray =
            let iArr = arr :?> Int32Array
            let b = new DoubleArray.Builder()
            for i in 0 .. iArr.Length - 1 do
                if iArr.IsNull i then b.AppendNull() |> ignore
                else b.Append(float (iArr.GetValue(i).Value) / 2.0) |> ignore
            b.Build()

        // 3. Run Map directly on Series
        // 注意：这里不需要传入 DataType，因为我们是从 Arrow 结果反推的
        use sRes = s.Map(Func<_,_>(logic))

        // 4. Verify
        let res = sRes.AsSeq<double>() |> Seq.toList
        Assert.Equal(5.0, res.[0].Value)
        Assert.Equal(10.0, res.[1].Value)
        Assert.Equal(15.0, res.[2].Value)
        Assert.Equal("val", sRes.Name) // 名字应该保持一致