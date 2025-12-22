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
[<CLIMutable>]
    type ComplexData = {
        Id: int
        Name: string option       // 测试 Option<string>
        Score: float option       // 测试 Option<float>
        Tags: string list         // 测试 List<string> (递归)
        Metadata: InnerMeta option // 测试 Option<Struct> (递归)
        CreatedAt: System.DateTime
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
    member _.``Interop: Full Complex Type Roundtrip`` () =
        // 1. 准备数据
        let data = [
            { 
                Id = 1
                Name = Some "Alice"
                Score = Some 99.5
                Tags = ["dev"; "fsharp"]
                Metadata = Some { Code = "A1"; Level = 10 }
                CreatedAt = System.DateTime(2023, 1, 1) 
            }
            { 
                Id = 2
                Name = None
                Score = None
                Tags = []
                Metadata = None
                CreatedAt = System.DateTime(2023, 1, 2) 
            }
        ]

        // 2. 写入 (Seq -> Series -> DataFrame)
        // 这一步会调用 ArrowConverter，递归处理 List 和 Option
        use df = DataFrame.create [
            Series.ofSeq("data", data) // 没错，直接把 Struct 当作一列 Series 存进去！
        ]
        
        // Polars 会把它展平成 Struct 类型列
        // 验证一下 Schema
        df.PrintSchema() 
        
        // 3. 读取 (DataFrame -> Seq)
        // 如果是 Struct 列，Polars 里的列名是 "data"。
        // 这里演示的是 Unnest 后的读取，或者直接读 Struct 列。
        // 为了简单演示 Roundtrip，我们假设这一列就是 Struct
        
        // 我们需要把这一列拿出来，转回 Record
        // 目前 ToRecords 是针对 DataFrame 的（按列名匹配属性名）。
        // 上面的 create 实际上创建了一个只有一列 "data" 的 DF，列类型是 Struct<Id, Name...>
        
        // 我们需要 Unnest 才能用 ToRecords 映射回扁平的 Record
        let dfFlat =
            df |>pl.unnestColumn "data"
        
        let readBack = dfFlat.ToRecords<ComplexData>() |> Seq.toList

        // 4. 验证
        Assert.Equal(2, readBack.Length)
        
        let row1 = readBack.[0]
        Assert.Equal(Some "Alice", row1.Name)
        // Assert.Equal(["dev"; "fsharp"], row1.Tags)
        Assert.Equal(Some 99.5, row1.Score)
        Assert.Equal(10, row1.Metadata.Value.Level)
        
        let row2 = readBack.[1]
        Assert.True row2.Name.IsNone
        Assert.True row2.Metadata.IsNone