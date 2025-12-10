namespace Polars.FSharp.Tests

open System
open Xunit
open Polars.FSharp
open Apache.Arrow
type Product = {
    Name: string // 这里对应 Categorical 列
    Price: decimal
    InStock: bool option
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

    [<Fact>]
    member _.``Extensions: DataFrame <-> Records (Decimal & Cat)`` () =

        // 1. 准备数据
        let records = [
            { Name = "Apple"; Price = 1.20m; InStock = Some true }
            { Name = "Banana"; Price = 0.85m; InStock = None }
            { Name = "Apple"; Price = 1.25m; InStock = Some false }
        ]

        // 2. Records -> DataFrame
        // 这里会自动推断 Decimal Scale (应该是 2)
        let df = DataFrame.ofRecords records
        
        // 3. 将 Name 列转为 Categorical (模拟真实场景)
        let dfCat = 
            df
            |> Polars.withColumn(
                Polars.col("Name").Cast Categorical
            )

        // 验证一下类型
        Polars.show dfCat |> ignore

        // 4. DataFrame -> Records
        // 这里测试 Categorical -> String 的自动转换
        // 以及 Decimal -> Decimal 的读取
        let results = dfCat.ToRecords<Product>() |> Seq.toList

        // 5. 断言
        Assert.Equal(3, results.Length)
        Assert.Equal("Apple", results.[0].Name)
        Assert.Equal(1.20m, results.[0].Price)
        Assert.Equal(Some true, results.[0].InStock)
        
        Assert.Equal("Banana", results.[1].Name)
        Assert.Equal(0.85m, results.[1].Price)