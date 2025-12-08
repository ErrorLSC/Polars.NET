namespace Polars.FSharp

open System
open Apache.Arrow
open Apache.Arrow.Types

module Udf =

    // 1. 定义读取器：给定索引 i，返回类型 'T
    type ColumnReader<'T> = int -> 'T

    // 2. 定义写入器：给定值 'U，写入 Builder
    // 同时返回 Builder 本身以便最后 Build (这里用 obj 弱类型持有 builder)
    type ColumnWriter<'U> = {
        Append: 'U -> unit
        AppendNull: unit -> unit
        Build: unit -> IArrowArray
    }

    // --- 内部工厂：根据数组类型创建读取器 ---
    let private createReader<'T> (arr: IArrowArray) : ColumnReader<'T> =
        // 利用 F# 的类型匹配 (Type Shape)
        // 这里需要对每种支持的输入类型写一次
        if typeof<'T> = typeof<int> then
            match arr with
            // 兼容 Int32 和 Int64 源，统一转为 int 给用户
            | :? Int32Array as a -> fun i -> unbox (a.GetValue(i).Value)
            | :? Int64Array as a -> fun i -> unbox (int (a.GetValue(i).Value)) 
            | _ -> failwith $"Array {arr.GetType().Name} cannot be read as int"
        
        else if typeof<'T> = typeof<string> then
            match arr with
            | :? StringArray as a -> fun i -> unbox (a.GetString(i))
            | :? StringViewArray as a -> fun i -> unbox (a.GetString(i))
            | _ -> failwith $"Array {arr.GetType().Name} cannot be read as string"
            
        else if typeof<'T> = typeof<double> then
            match arr with
            | :? DoubleArray as a -> fun i -> unbox (a.GetValue(i).Value)
            | _ -> failwith "Array cannot be read as double"

        else if typeof<'T> = typeof<bool> then
            match arr with
            | :? BooleanArray as a -> fun i-> unbox (a.GetValue(i).Value)
            | _ -> failwith "Array cannot be read as boolean"
            
        else
            failwith $"Unsupported input type for UDF: {typeof<'T>.Name}"

    // --- 内部工厂：根据泛型 'U 创建写入器 ---
    let private createWriter<'U> () : ColumnWriter<'U> =
        if typeof<'U> = typeof<int> then
            let b = new Int32Array.Builder()
            let writer : ColumnWriter<int> = { 
                // 显式指定 unbox<int>
                Append = (fun v -> b.Append(v) |> ignore)
                AppendNull = (fun () -> b.AppendNull() |> ignore)
                Build = (fun () -> b.Build() :> IArrowArray) 
            } 
            writer :> obj |> unbox

        else if typeof<'U> = typeof<string> then
            let b = new StringViewArray.Builder()
            let writer : ColumnWriter<string> = { 
                // 显式指定 unbox<string>
                Append = (fun v -> b.Append(v) |> ignore)
                AppendNull = (fun () -> b.AppendNull() |> ignore)
                Build = (fun () -> b.Build() :> IArrowArray) 
            } 
            writer :> obj |> unbox
            
        else if typeof<'U> = typeof<double> then // F# 的 double 就是 System.Double
            let b = new DoubleArray.Builder()
            let writer: ColumnWriter<float> = { 
                // F# 里 float 就是 64位双精度浮点数 (C# double)
                // 必须显式写 unbox<float>，否则编译器不知道你调的是 Append(double) 还是 Append(double?)
                Append = (fun v -> b.Append(v) |> ignore)
                AppendNull = (fun () -> b.AppendNull() |> ignore)
                Build = (fun () -> b.Build() :> IArrowArray) 
            } 
            writer :> obj |> unbox

        else if typeof<'U> = typeof<bool> then
            let b = new BooleanArray.Builder()
            let writer : ColumnWriter<bool> = {
                Append = (fun v -> b.Append(v) |> ignore)
                AppendNull = (fun () -> b.AppendNull() |> ignore)
                Build = (fun () -> b.Build() :> IArrowArray)
            } 
            writer :> obj |> unbox

        else
            failwith $"Unsupported output type for UDF: {typeof<'U>.Name}"


// --- 核心：万能 Map 函数 ---
    // 用户只需要传 f: 'T -> 'U
    let map (f: 'T -> 'U) : Func<IArrowArray, IArrowArray> =
        Func<_,_>(fun (arr: IArrowArray) ->
            // 1. 获取读取器 (闭包)
            let reader = createReader<'T> arr
            
            // 2. 获取写入器
            let writer = createWriter<'U> ()
            
            // 3. 通用循环 (脏活在这里只写一次)
            let len = arr.Length
            for i in 0 .. len - 1 do
                if arr.IsNull i then
                    writer.AppendNull()
                else
                    // 读取 -> 计算 -> 写入
                    let valIn = reader i
                    let valOut = f valIn
                    writer.Append valOut
            
            // 4. 返回结果
            writer.Build()
        )