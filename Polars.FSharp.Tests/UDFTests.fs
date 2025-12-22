namespace Polars.FSharp.Tests

module UdfLogic =
    open Apache.Arrow

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
                    builder.Append $"Value: {v}" |> ignore
            builder.Build() :> IArrowArray
        |_ -> failwith $"Expected Int32Array or Int64Array, but got: {arr.GetType().Name}"

    // 场景 B: 必定报错
    let alwaysFail (arr: IArrowArray) : IArrowArray =
        failwith "Boom! C# UDF Exploded!"

open Xunit
open Polars.FSharp
open Apache.Arrow
open System
open Polars.NET.Core

type ``UDF Tests`` () =

    [<Fact>]
    member _.``Map UDF can change data type (Int -> String)`` () =
        // 1. 准备数据
        use csv = new TempCsv "num\n100\n200"
        let lf = LazyFrame.ScanCsv csv.Path
        
        // 2. 构造 C# 委托
        let udf = Func<IArrowArray, IArrowArray> UdfLogic.intToString

        // 3. 执行 Polars 查询
        // 关键点：必须传入 PlDataType.String，否则 Polars 可能会把结果当成 Int 处理导致乱码
        let df = 
            lf 
            |> pl.withColumnLazy (
                pl.col "num"
                |> fun e -> e.Map(udf, DataType.String)
                |> pl.alias "desc"
            )
            |> pl.selectLazy [ pl.col "desc" ]
            |> pl.collect

        // 4. 验证结果
        let arrowBatch = df.ToArrow()
        let strCol = arrowBatch.Column "desc" :?> StringViewArray
        
        Assert.Equal("Value: 100", strCol.GetString 0)
        Assert.Equal("Value: 200", strCol.GetString 1)

    [<Fact>]
    member _.``Map UDF error is propagated to F#`` () =
        // 1. 准备数据
        use csv = new TempCsv("num\n1")
        let lf = LazyFrame.ScanCsv csv.Path
        
        let udf = System.Func<IArrowArray, IArrowArray> UdfLogic.alwaysFail

        // 2. 断言会抛出异常
        let ex = Assert.Throws<Exception>(fun () -> 
            lf 
            |> pl.withColumnLazy (
                pl.col "num" 
                |> fun e -> e.Map(udf, DataType.SameAsInput)
            )
            // UDF 是 Lazy 执行的，只有 Collect/ToArrow 时才会触发
            |> pl.collect 
            |> ignore
        )

        // 3. 验证异常信息
        // 我们期望看到 C# 的异常信息包含在 PolarsError 里
        Assert.Contains("Boom! C# UDF Exploded!", ex.Message)
        Assert.Contains("C# UDF Failed", ex.Message) // 这是 Rust 代码里加的前缀

    [<Fact>]
    member _.``Generic Map UDF with Lambda (Int -> String)`` () =
        use csv = new TempCsv "num\n100\n"
        let lf = LazyFrame.ScanCsv csv.Path
        
        // --- 用户的代码极度简化 ---
        // 1. 定义一个简单的匿名函数 (int -> string)
        let myLogic = fun (x: int) -> sprintf "Num: %d" (x + 1)

        let df = 
            lf 
            |> pl.withColumnLazy (
                pl.col "num"
                // 2. 直接调用 Udf.map
                // 泛型 'T 和 'U 会自动推断为 int 和 string
                |> fun e -> e.Map(Udf.map myLogic DataType.String, DataType.String) 
                |> pl.alias "res"
            )
            |> pl.selectLazy [ pl.col "res" ]
            |> pl.collect

        // 验证
        let arrow = df.ToArrow()
        let col = arrow.Column "res" :?> StringViewArray // 自动用了 StringView
        
        Assert.Equal("Num: 101", col.GetString 0)
        Assert.Equal(1, col.Length)
    [<Fact>]
    member _.``UDF: Map with Option (Null Handling)`` () =
        // 数据: [10, 20, null]
        // 注意: CSV 最后一行写 null 还是空行取决于解析器，这里用空行配合 Polars 默认行为
        use csv = new TempCsv "val\n10\n20\n" 
        let lf = LazyFrame.ScanCsv csv.Path

        // 逻辑: 
        // 输入是 int option
        // 如果有值且 > 15 -> 返回 Some (x * 2)
        // 否则 (<= 15 或原本就是 null) -> 返回 None (即 null)
        let logic (opt: int option) =
            match opt with
            | Some x when x > 15 -> Some (x * 2)
            | _ -> None

        let df = 
            lf 
            |> pl.withColumnLazy (
                pl.col "val"
                // 使用 mapOption，显式处理 Option 类型
                |> fun e -> e.Map(Udf.mapOption logic DataType.Int32, DataType.Int32)
                |> pl.alias "res"
            )
            |> pl.collect

        // 验证
        let arrow = df.ToArrow()
        let col = arrow.Column "res" :?> Int32Array // 注意根据 DataType.Int32 生成的是 Int32Array

        // Row 0: 10 -> (<=15) -> None
        Assert.True(col.IsNull 0)

        // Row 1: 20 -> (>15) -> 40
        Assert.Equal(40, col.GetValue(1).Value)

        // Row 2: null -> (None) -> None
        Assert.True(col.IsNull 2)
    [<Fact>]
    member _.``UDF: Decimal Map`` () =
        // 1. 准备数据: String -> Decimal
        // 源数据是字符串 "10.50", "20.25"
        let data = ["10.50"; "20.25"; null]
        use s = Series.create("str_vals", data)
        use df = s.ToFrame()

        // 2. 定义 Decimal 逻辑
        // 输入: decimal option
        // 输出: decimal option (乘以 2)
        let logic (opt: decimal option) =
            opt |> Option.map (fun d -> d * 2m)

        // 3. 执行 UDF
        // 先 Cast 成 Decimal，再跑 UDF，再输出 Decimal
        // 注意：Series.Cast 已经支持了
        use sDec = s.Cast(DataType.Decimal(Some 10, 2))
        
        // 这里的 DataType.Decimal(Some 10, 2) 会告诉 createBuilder 创建 Decimal128Array
        let res = 
            sDec.ToFrame()

            // 或者我们可以直接在 Series 上实现 Map? 目前 Series.Map 没暴露，只有 Expr.Map
            // 我们用 Expr 方式：
            |> pl.withColumn (
                pl.col("str_vals")
                    .Cast(DataType.Decimal(Some 10, 2)) // 先转 Decimal
                    .Map(Udf.mapOption logic (DataType.Decimal(Some 10, 2)), DataType.Decimal(Some 10, 2)) // 跑 UDF
                    .Alias "doubled"
            )// 这里如果源是 Eager DataFrame，withColumn 返回 Eager DataFrame
        
        let arrow = res.ToArrow()
        let col = arrow.Column "doubled" :?> Decimal128Array
        Assert.Equal(21.00m, col.GetValue(0).Value)
        Assert.Equal(40.50m, col.GetValue(1).Value)
        Assert.True(col.IsNull 2)