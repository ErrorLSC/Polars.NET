namespace Polars.FSharp

open System
open Apache.Arrow

module Udf =

    // ==========================================
    // 1. 内部工厂：Builder 创建器
    // ==========================================
    // 职责：根据目标 DataType 创建对应的 Arrow Builder
    // 返回：(写入函数: obj option -> unit, 构建函数: unit -> IArrowArray)
    let private createBuilder (dtype: DataType) (capacity: int) : (obj option -> unit) * (unit -> IArrowArray) =
        match dtype with
        | DataType.Int32 ->
            // Reserve(capacity) 是性能关键，防止多次扩容
            let b = (new Int32Array.Builder()).Reserve capacity
            let append (v: obj option) =
                match v with
                | Some x -> b.Append(unbox<int> x) |> ignore
                | None -> b.AppendNull() |> ignore
            append, fun () -> b.Build() :> IArrowArray
            
        | DataType.Int64 ->
            let b = (new Int64Array.Builder()).Reserve capacity
            let append (v: obj option) =
                match v with
                | Some x -> b.Append(unbox<int64> x) |> ignore
                | None -> b.AppendNull() |> ignore
            append, fun () -> b.Build() :> IArrowArray

        | DataType.Float64 ->
            let b = (new DoubleArray.Builder()).Reserve capacity
            let append (v: obj option) =
                match v with
                | Some x -> b.Append(unbox<double> x) |> ignore
                | None -> b.AppendNull() |> ignore
            append, fun () -> b.Build() :> IArrowArray
        | DataType.Decimal (pOpt, s) ->
            // Arrow 需要显式的 Decimal128Type 来初始化 Builder
            // 如果用户没给 precision，我们给个默认值 (比如 38，最大值)
            let p = defaultArg pOpt 38
            let arrowType = new Apache.Arrow.Types.Decimal128Type(p, s)
            let b = (new Decimal128Array.Builder(arrowType)).Reserve(capacity)
            
            let append (v: obj option) =
                match v with 
                // C# Arrow Builder 支持直接 Append(decimal)
                | Some x -> b.Append(unbox<decimal> x) |> ignore 
                | None -> b.AppendNull() |> ignore
            append, fun () -> b.Build() :> IArrowArray
        | DataType.String ->
            let b = (new StringViewArray.Builder()).Reserve capacity
            let append (v: obj option) =
                match v with
                | Some x -> b.Append(unbox<string> x) |> ignore
                | None -> b.AppendNull() |> ignore
            append, fun () -> b.Build() :> IArrowArray
        | DataType.Categorical ->
            // 手动实现 Dictionary Builder 逻辑
            // 1. 维护一个哈希表用于去重: Value -> Index
            let lookup = System.Collections.Generic.Dictionary<string, int>()
            
            // 2. 两个 Builder: 存唯一值的 StringArray，存索引的 UInt32Array
            // Polars 默认使用 UInt32 作为物理索引
            let valuesBuilder = new StringArray.Builder()
            let indicesBuilder = (new UInt32Array.Builder()).Reserve(capacity)
            
            let append (v: obj option) =
                match v with 
                | None -> 
                    // 空值直接在索引数组里存 null
                    indicesBuilder.AppendNull() |> ignore
                | Some objVal -> 
                    let s = unbox<string> objVal
                    // 检查是否已存在
                    match lookup.TryGetValue(s) with
                    | true, idx -> 
                        // 已存在，直接存索引
                        indicesBuilder.Append(uint32 idx) |> ignore
                    | false, _ ->
                        // 新值：
                        // 1. 获取新索引 (即当前字典大小)
                        let newIdx = lookup.Count
                        // 2. 存入查找表
                        lookup.Add(s, newIdx)
                        // 3. 存入 Values 数组
                        valuesBuilder.Append(s) |> ignore
                        // 4. 存入 Indices 数组
                        indicesBuilder.Append(uint32 newIdx) |> ignore

            let build () =
                // 1. 构建 Indices 和 Values 数组
                let indices = indicesBuilder.Build()
                let values = valuesBuilder.Build()
                
                // 2. 构建 DictionaryType (Key=UInt32, Value=String, Ordered=false)
                let dictType = new Types.DictionaryType(
                    indices.Data.DataType,
                    values.Data.DataType,
                    false
                )
                
                // 3. 组装最终的 DictionaryArray
                new DictionaryArray(dictType, indices, values) :> IArrowArray

            (append, build)
        | DataType.Boolean ->
            let b = (new BooleanArray.Builder()).Reserve capacity
            let append (v: obj option) =
                match v with
                | Some x -> b.Append(unbox<bool> x) |> ignore
                | None -> b.AppendNull() |> ignore
            append, fun () -> b.Build() :> IArrowArray

        | _ -> failwithf "UDF output type %A not supported yet" dtype

    // ==========================================
    // 2. 内部工厂：Reader 创建器
    // ==========================================
    // 职责：根据输入 Array 类型和泛型 'T，生成读取闭包
    // 返回：int -> 'T option
    let private createReader<'T> (arr: IArrowArray) : int -> 'T option =
        // 使用 box Unchecked.defaultof<'T> 来进行泛型类型匹配
        match box Unchecked.defaultof<'T> with
        | :? int -> 
            // 处理输入是 Int32 或 Int64 但用户想要 int 的情况
            match arr with
            | :? Int32Array as a -> 
                fun i -> if a.IsNull(i) then None else Some (unbox<'T> (a.GetValue(i).Value))
            | :? Int64Array as a -> 
                // 注意：这里可能发生截断，但通常 F# int 是 int32
                fun i -> if a.IsNull(i) then None else Some (unbox<'T> (int (a.GetValue(i).Value)))
            | _ -> failwithf "Cannot read %s as int" (arr.GetType().Name)

        | :? int64 -> 
            match arr with
            | :? Int64Array as a -> 
                fun i -> if a.IsNull(i) then None else Some (unbox<'T> (a.GetValue(i).Value))
            | :? Int32Array as a -> 
                fun i -> if a.IsNull(i) then None else Some (unbox<'T> (int64 (a.GetValue(i).Value)))
            | _ -> failwithf "Cannot read %s as int64" (arr.GetType().Name)

        | :? double -> 
            match arr with
            | :? DoubleArray as a -> 
                fun i -> if a.IsNull(i) then None else Some (unbox<'T> (a.GetValue(i).Value))
            | :? FloatArray as a ->
                fun i -> if a.IsNull(i) then None else Some (unbox<'T> (double (a.GetValue(i).Value)))
            | _ -> failwithf "Cannot read %s as double" (arr.GetType().Name)
        | :? decimal ->
            match arr with
            | :? Decimal128Array as a -> 
                // C# Arrow 会自动应用 Scale 将 Int128 转为 System.Decimal
                fun i -> if a.IsNull(i) then None else Some (unbox<'T> (a.GetValue(i).Value))
            | _ -> failwithf "Cannot read %s as decimal" (arr.GetType().Name)
        | :? string ->
            match arr with
            | :? StringArray as sa -> 
                fun i -> if sa.IsNull(i) then None else Some (unbox<'T> (sa.GetString i))
            | :? StringViewArray as sva ->
                fun i -> if sva.IsNull(i) then None else Some (unbox<'T> (sva.GetString i))
            | :? DictionaryArray as da ->
                // 这是一个通用的 Dictionary 读取器
                // 注意：这里为了性能，我们应该在闭包外获取 Dictionary (Values) 数组
                let values = da.Dictionary
                let indices = da.Indices
                
                // 辅助函数：根据 Index 类型读取 Key
                let getKey (idx: int) : int =
                    match indices with
                    | :? UInt32Array as arr -> if arr.IsNull(idx) then -1 else int (arr.GetValue(idx).Value)
                    | :? Int32Array as arr -> if arr.IsNull(idx) then -1 else arr.GetValue(idx).Value
                    | _ -> -1 // 其他类型暂略

                // 辅助函数：根据 Dictionary 类型读取 String
                let getVal (key: int) : string =
                    match values with
                    | :? StringArray as sa -> sa.GetString(key)
                    | :? StringViewArray as sva -> sva.GetString(key)
                    | _ -> ""

                fun i -> 
                    let key = getKey i
                    if key < 0 then None else Some (unbox<'T> (getVal key))

            | _ -> failwithf "Cannot read %s as string" (arr.GetType().Name)
            
        | :? bool ->
            match arr with
            | :? BooleanArray as a ->
                 fun i -> if a.IsNull i then None else Some (unbox<'T> (a.GetValue(i).Value))
            | _ -> failwithf "Cannot read %s as bool" (arr.GetType().Name)

        | _ -> failwithf "Unsupported UDF input type: %s" typeof<'T>.Name

    // ==========================================
    // 3. 公开 API：Option 版本的 Map
    // ==========================================
    // 这是核心函数，处理所有 Null 逻辑
    let mapOption<'T, 'U> (f: 'T option -> 'U option) (outputType: DataType) : Func<IArrowArray, IArrowArray> =
        Func<IArrowArray, IArrowArray>(fun arr ->
            let len = arr.Length
            
            // 1. 准备 Writer (带预分配)
            let append, build = createBuilder outputType len

            // 2. 准备 Reader (已内联类型检查)
            let getter = createReader<'T> arr

            // 3. 循环执行
            for i in 0 .. len - 1 do
                let input = getter i
                let output = f input // 执行用户逻辑
                
                // 将 'U option 转为 obj option 以便通用 Writer 处理
                let outObj = output |> Option.map box
                append outObj

            // 4. 构建结果
            build()
        )

    // ==========================================
    // 4. 公开 API：普通版本的 Map
    // ==========================================
    // 方便用户处理非 Null 逻辑，自动处理 Some/None 包装
    let map (f: 'T -> 'U) (outputType: DataType) =
        mapOption (fun (opt: 'T option) ->
            match opt with
            | Some v -> Some (f v)
            | None -> None
        ) outputType