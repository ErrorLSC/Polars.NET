namespace PolarsFSharp.Tests

module UdfLogic =
    open Apache.Arrow
    open Apache.Arrow.Types

    // 场景 A: 类型转换 (Int32 -> String)
    let intToString (arr: IArrowArray) : IArrowArray =
        match arr with
        | :? Int64Array as i64Arr ->
            let builder = new StringViewArray.Builder()
            for i in 0 .. i64Arr.Length - 1 do
                if i64Arr.IsNull(i) then builder.AppendNull() |> ignore
                else 
                    let v = i64Arr.GetValue(i).Value
                    builder.Append $"Value: {v}" |> ignore
            builder.Build() :> IArrowArray

    // 为了兼容性保留 Int32
        | :? Int32Array as i32Arr ->
            let builder = new StringViewArray.Builder()
            for i in 0 .. i32Arr.Length - 1 do
                if i32Arr.IsNull(i) then 
                    builder.AppendNull() |> ignore
                else 
                    let v = i32Arr.GetValue(i).Value
                    builder.Append($"Value: {v}") |> ignore
            builder.Build() :> IArrowArray
        |_ -> failwith $"Expected Int32Array or Int64Array, but got: {arr.GetType().Name}"

    // 场景 B: 必定报错
    let alwaysFail (arr: IArrowArray) : IArrowArray =
        failwith "Boom! C# UDF Exploded!"

open Xunit
open PolarsFSharp
open Apache.Arrow
open System
open Polars.Native

type ``UDF Tests`` () =

    [<Fact>]
    member _.``Map UDF can change data type (Int -> String)`` () =
        // 1. 准备数据
        use csv = new TempCsv("num\n100\n200")
        let lf = Polars.scanCsv csv.Path None
        
        // 2. 构造 C# 委托
        let udf = Func<IArrowArray, IArrowArray>(UdfLogic.intToString)

        // 3. 执行 Polars 查询
        // 关键点：必须传入 PlDataType.String，否则 Polars 可能会把结果当成 Int 处理导致乱码
        let df = 
            lf 
            |> Polars.withColumn (
                Polars.col "num"
                |> fun e -> e.Map(udf, PlDataType.String)
                |> Polars.alias "desc"
            )
            |> Polars.selectLazy [ Polars.col "desc" ]
            |> Polars.collect

        // 4. 验证结果
        let arrowBatch = df.ToArrow()
        let strCol = arrowBatch.Column("desc") :?> StringViewArray
        
        Assert.Equal("Value: 100", strCol.GetString(0))
        Assert.Equal("Value: 200", strCol.GetString(1))

    [<Fact>]
    member _.``Map UDF error is propagated to F#`` () =
        // 1. 准备数据
        use csv = new TempCsv("num\n1")
        let lf = Polars.scanCsv csv.Path None
        
        let udf = System.Func<IArrowArray, IArrowArray>(UdfLogic.alwaysFail)

        // 2. 断言会抛出异常
        let ex = Assert.Throws<Exception>(fun () -> 
            lf 
            |> Polars.withColumn (
                Polars.col "num" 
                |> fun e -> e.Map(udf, PlDataType.SameAsInput)
            )
            // UDF 是 Lazy 执行的，只有 Collect/ToArrow 时才会触发
            |> Polars.collect 
            |> ignore
        )

        // 3. 验证异常信息
        // 我们期望看到 C# 的异常信息包含在 PolarsError 里
        Assert.Contains("Boom! C# UDF Exploded!", ex.Message)
        Assert.Contains("C# UDF Failed", ex.Message) // 这是 Rust 代码里加的前缀