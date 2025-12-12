namespace Polars.FSharp

open System
open FSharp.Reflection
open Apache.Arrow
open Apache.Arrow.Types
open Polars.Native

// =========================================================================================
// MODULE: Series Extensions (Data Conversion & Computation)
// =========================================================================================
[<AutoOpen>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module SeriesExtensions =
    // -----------------------------------------------------------
    // 1. Data Conversion (Series <-> Seq)
    // -----------------------------------------------------------
    type Series with
        /// <summary>
        /// Convert Series to a typed sequence of Options.
        /// Supports: int, int64, double, bool, string, decimal.
        /// </summary>
        member this.AsSeq<'T>() : seq<'T option> =
            // 1. 转为 Arrow 以便高效读取 (Zero-Copy where possible)
            let arrow = this.ToArrow()
            let len = arrow.Length

            // 2. 根据目标类型 'T 创建特定的读取器闭包 (int -> 'T option)
            // 这样做的好处是避免了在循环内部进行类型匹配，性能更高
            let reader : int -> 'T option = 
                match box Unchecked.defaultof<'T> with
                | :? int -> 
                    match arrow with
                    | :? Int32Array as arr -> fun i -> if arr.IsNull i then None else Some(unbox (arr.GetValue(i).Value))
                    | :? Int64Array as arr -> fun i -> if arr.IsNull i then None else Some(unbox (int (arr.GetValue(i).Value)))
                    | _ -> failwithf "Type mismatch for AsSeq<int>: Underlying Arrow array is %s" (arrow.GetType().Name)
                
                | :? int64 ->
                    match arrow with
                    | :? Int64Array as arr -> fun i -> if arr.IsNull i then None else Some(unbox (arr.GetValue(i).Value))
                    | :? Int32Array as arr -> fun i -> if arr.IsNull i then None else Some(unbox (int64 (arr.GetValue(i).Value)))
                    | _ -> failwithf "Type mismatch for AsSeq<int64>: Underlying Arrow array is %s" (arrow.GetType().Name)

                | :? double ->
                    match arrow with
                    | :? DoubleArray as arr -> fun i -> if arr.IsNull i then None else Some(unbox (arr.GetValue(i).Value))
                    | :? FloatArray as arr -> fun i -> if arr.IsNull i then None else Some(unbox (double (arr.GetValue(i).Value)))
                    | _ -> failwithf "Type mismatch for AsSeq<double>: Underlying Arrow array is %s" (arrow.GetType().Name)

                | :? decimal ->
                    // Arrow 自动处理 Scale，返回 System.Decimal
                    match arrow with
                    | :? Decimal128Array as arr -> fun i -> if arr.IsNull i then None else Some(unbox (arr.GetValue(i).Value))
                    | _ -> failwithf "Type mismatch for AsSeq<decimal>: Underlying Arrow array is %s" (arrow.GetType().Name)

                | :? string ->
                    match arrow with
                    | :? StringArray as arr -> fun i -> if arr.IsNull i then None else Some(unbox (arr.GetString i))
                    | :? StringViewArray as arr -> fun i -> if arr.IsNull i then None else Some(unbox (arr.GetString i))
                    // Categorical 支持: 自动查找字典值
                    | :? DictionaryArray as arr ->
                        let indices = arr.Indices
                        let dict = arr.Dictionary :?> StringArray // 假设 Dict 是 String
                        
                        // Helper to get key from indices array
                        let getKey i = 
                            match indices with
                            | :? UInt32Array as idx -> if idx.IsNull i then -1 else int (idx.GetValue(i).Value)
                            | :? Int32Array as idx -> if idx.IsNull i then -1 else int (idx.GetValue(i).Value)
                            | :? Int8Array as idx -> if idx.IsNull i then -1 else int (idx.GetValue(i).Value)
                            | _ -> -1
                        
                        fun i -> 
                            let k = getKey i
                            if k < 0 then None else Some(unbox (dict.GetString k))
                    | _ -> failwithf "Type mismatch for AsSeq<string>: Underlying Arrow array is %s" (arrow.GetType().Name)

                | :? bool ->
                    match arrow with
                    | :? BooleanArray as arr -> fun i -> if arr.IsNull i then None else Some(unbox (arr.GetValue(i).Value))
                    | _ -> failwithf "Type mismatch for AsSeq<bool>: Underlying Arrow array is %s" (arrow.GetType().Name)

                | _ -> failwithf "Unsupported type for AsSeq: %A" typeof<'T>

            // 3. 生成序列
            seq {
                for i in 0 .. len - 1 do
                    yield reader i
            }

        /// <summary>
        /// Get values as a list (forces evaluation).
        /// </summary>
        member this.ToList<'T>() = this.AsSeq<'T>() |> Seq.toList
    // -----------------------------------------------------------
    // 2. Series Aggregatios and Arithmetic
    // -----------------------------------------------------------
    // --- Aggregations (Pipe-friendly) ---

    let sum (s: Series) = s.Sum()
    let mean (s: Series) = s.Mean()
    let min (s: Series) = s.Min()
    let max (s: Series) = s.Max()
    let count (s: Series) = s.Length // 或者 s.Length
    let nullCount (s: Series) = s.NullCount

    // --- Arithmetic (Pipe-friendly) ---
    
    let add (other: Series) (s: Series) = s + other
    let sub (other: Series) (s: Series) = s - other
    let mul (other: Series) (s: Series) = s * other
    let div (other: Series) (s: Series) = s / other

    // --- Comparison ---
    
    let eq (other: Series) (s: Series) = s .= other
    let nteq (other: Series) (s: Series) = s != other
    let gt (other: Series) (s: Series) = s .> other
    let lt (other: Series) (s: Series) = s .< other
    let gtEq (other: Series) (s: Series) = s .>= other
    let ltEq (other: Series) (s: Series) = s .<= other

    // 支持标量广播
    let addLit (value: 'T) (s: Series) = s + Series.ofSeq("lit", [value])
    let subLit (value: 'T) (s: Series) = s - Series.ofSeq("lit", [value])
    let subLitFrom (value: 'T) (s: Series) = Series.ofSeq("lit", [value]) - s
    let mulLit (value: 'T) (s: Series) = s * Series.ofSeq("lit", [value])
    let divLit (value: 'T) (s: Series) = s / Series.ofSeq("lit", [value])
    let divLitFrom (value: 'T) (s: Series) = Series.ofSeq("lit", [value]) / s
    // Comparison (用于 Filter / Mask)
    let eqLit (value: 'T) (s: Series) = s .= Series.ofSeq("lit", [value])
    let neqLit (value: 'T) (s: Series) = s != Series.ofSeq("lit", [value])
    let gtLit (value: 'T) (s: Series) = s .> Series.ofSeq("lit", [value])
    let ltLit (value: 'T) (s: Series) = s .< Series.ofSeq("lit", [value])
    let gtEqLit  (value: 'T) (s: Series) = s .>= Series.ofSeq("lit", [value])
    let ltEqLit (value: 'T) (s: Series) = s .<= Series.ofSeq("lit", [value])
    
    // --- Transformations ---
    let cast (dtype: DataType) (s: Series) = s.Cast dtype

    // -----------------------------------------------------------
    // 3. UDF Support (Direct Map on Series)
    // -----------------------------------------------------------
    type Series with
        /// <summary>
        /// Apply a C# UDF (Arrow->Arrow) directly to this Series.
        /// Returns a new Series.
        /// </summary>
        member this.Map(func: Func<IArrowArray, IArrowArray>) : Series =
            // 1. 获取输入 (Zero-Copy)
            let inputArrow = this.ToArrow()
            
            // 2. 执行运算
            let outputArrow = func.Invoke inputArrow
            
            // 3. 将结果封装回 Series
            // 这里的巧妙之处：Extensions.fs 可以看到 Polars.fromArrow
            
            // 3.1 构建 RecordBatch 包装纸
            let field = new Apache.Arrow.Field(this.Name, outputArrow.Data.DataType, true)
            let schema = new Apache.Arrow.Schema([| field |], null)
            use batch = new Apache.Arrow.RecordBatch(schema, [| outputArrow |], outputArrow.Length)
            
            // 3.2 借道 DataFrame (Zero-Copy import)
            use df = Polars.fromArrow batch
            
            // 3.3 提取 Series (Clone Handle)
            let res = df.Column 0
            
            // 显式重命名以防万一
            res.Rename this.Name

// =========================================================================================
// MODULE: DataFrame Serialization (Record <-> DataFrame)
// =========================================================================================
[<AutoOpen>]
module Serialization =

    // ==========================================
    // 1. Helpers (Option Wrapping/Unwrapping)
    // ==========================================

    let private createOptionWrapper (t: Type) : (obj -> obj) * obj =
        if t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<option<_>> then
            let cases = FSharpType.GetUnionCases(t)
            let noneCase = cases |> Array.find (fun c -> c.Name = "None")
            let someCase = cases |> Array.find (fun c -> c.Name = "Some")
            
            let mkSome (v: obj) = FSharpValue.MakeUnion(someCase, [| v |])
            let noneValue = FSharpValue.MakeUnion(noneCase, [||])
            
            mkSome, noneValue
        else
            (fun x -> x), null

    let private createOptionUnwrapper (t: Type) : (obj -> obj) =
        if t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<option<_>> then
            fun (v: obj) ->
                if isNull v then null
                else
                    let case, fields = FSharpValue.GetUnionFields(v, t)
                    if case.Name = "None" then null
                    else fields.[0]
        else
            fun x -> x

    // ==========================================
    // 2. Reading Logic (Arrow -> Record)
    // ==========================================

    // 工厂：为某一列创建一个专用的读取闭包 (int -> obj)
    let private createColumnReader (col: IArrowArray) (targetType: Type) : (int -> obj) =
        
        let isOption = targetType.IsGenericType && targetType.GetGenericTypeDefinition() = typedefof<option<_>>
        let coreType = if isOption then targetType.GetGenericArguments().[0] else targetType

        let wrapSome, valueNone = createOptionWrapper targetType

        // 内部读取器：假设非空，读取原始值
        // 这里复用 Series.AsSeq 的逻辑会更简洁，但为了避免反射开销，我们针对 obj 再次匹配
        let getRawValue : int -> obj =
            match col with
            // 数值类型
            | :? Int64Array as arr -> fun i -> box (arr.GetValue(i).GetValueOrDefault())
            | :? Int32Array as arr -> fun i -> box (arr.GetValue(i).GetValueOrDefault())
            | :? Int16Array as arr -> fun i -> box (arr.GetValue(i).GetValueOrDefault())
            | :? Int8Array  as arr -> fun i -> box (arr.GetValue(i).GetValueOrDefault())
            | :? UInt64Array as arr -> fun i -> box (arr.GetValue(i).GetValueOrDefault())
            | :? UInt32Array as arr -> fun i -> box (arr.GetValue(i).GetValueOrDefault())
            | :? DoubleArray as arr -> fun i -> box (arr.GetValue(i).GetValueOrDefault())
            | :? FloatArray  as arr -> fun i -> box (arr.GetValue(i).GetValueOrDefault())
            
            // Decimal
            | :? Decimal128Array as arr -> fun i -> box (arr.GetValue(i).GetValueOrDefault())

            // 字符串
            | :? StringArray as arr -> fun i -> let s = arr.GetString(i) in if isNull s then box "" else box s
            | :? StringViewArray as arr -> fun i -> let s = arr.GetString(i) in if isNull s then box "" else box s
            
            // 布尔
            | :? BooleanArray as arr -> fun i -> box (arr.GetValue(i).GetValueOrDefault())

            // 时间
            | :? Date32Array as arr -> 
                fun i -> 
                    let v = arr.GetValue(i).GetValueOrDefault()
                    box (DateTime(1970, 1, 1).AddDays(float v))
            | :? TimestampArray as arr ->
                fun i ->
                    let v = arr.GetValue(i).GetValueOrDefault()
                    try box (DateTime.UnixEpoch.AddTicks(v * 10L)) 
                    with _ -> box DateTime.MinValue
            | :? DictionaryArray as arr ->
            let indices = arr.Indices
            
            // 1. 准备字典值读取器 (可能是 StringArray 或 StringViewArray)
            let getDictValue = 
                if arr.Dictionary :? StringArray then
                    let sa = arr.Dictionary :?> StringArray
                    fun k -> sa.GetString(k)
                elif arr.Dictionary :? StringViewArray then
                    let sva = arr.Dictionary :?> StringViewArray
                    fun k -> sva.GetString(k)
                else
                    failwithf "Unsupported Dictionary Value Type: %s" (arr.Dictionary.GetType().Name)

            // 2. 准备索引读取器 (Key -> Int)
            // Polars Categorical 索引通常是 UInt32，但为了稳健我们多检查几种
            let getKey i = 
                match indices with
                | :? UInt32Array as idx -> if idx.IsNull i then -1 else int (idx.GetValue(i).Value)
                | :? Int32Array as idx -> if idx.IsNull i then -1 else int (idx.GetValue(i).Value)
                | :? Int8Array as idx -> if idx.IsNull i then -1 else int (idx.GetValue(i).Value)
                | _ -> -1

            // 3. 组合读取逻辑
            fun i -> 
                let k = getKey i
                if k < 0 then null // Boxed null
                else box (getDictValue k)

            | _ -> failwithf "Unsupported Arrow Type for reading: %s" (col.GetType().Name)

        // 返回闭包：处理 Null 和 类型转换
        fun (rowIndex: int) ->
            if col.IsNull rowIndex then
                if isOption then valueNone
                else if not coreType.IsValueType then null // 引用类型允许 null
                else failwithf "Column '%s' has null at row %d but record field '%s' is not Option" (col.GetType().Name) rowIndex targetType.Name
            else
                let raw = getRawValue rowIndex
                // 类型转换 (例如 Arrow Int64 -> Record Int32)
                let converted = 
                    if isNull raw then null
                    elif raw.GetType() = coreType then raw
                    else Convert.ChangeType(raw, coreType)
                
                if isOption then wrapSome converted else converted

    // ==========================================
    // 3. Writing Logic (Record -> Arrow)
    // ==========================================

    // 工厂：为某个属性创建写入闭包
    // let private createFieldWriter (prop: Reflection.PropertyInfo) 
    //     : (obj -> unit) * (unit -> Field) * (unit -> IArrowArray) =
        
    //     let t = prop.PropertyType
    //     let name = prop.Name
        
    //     let isOption = t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<option<_>>
    //     let coreType = if isOption then t.GetGenericArguments().[0] else t
        
    //     // 获取解包器
    //     let unwrap = createOptionUnwrapper t

    //     // 通用 Append 逻辑
    //     let appendWithNullCheck (f: obj -> unit) (builderNull: unit -> unit) (v: obj) =
    //         let realVal = unwrap v
    //         if isNull realVal then builderNull()
    //         else f realVal

    //     if coreType = typeof<int> then
    //         let b = new Int32Array.Builder()
    //         let append v = appendWithNullCheck (fun x -> b.Append(unbox<int> x) |> ignore) (fun () -> b.AppendNull() |> ignore) v
    //         let field () = new Field(name, Int32Type.Default, true)
    //         let build () = b.Build() :> IArrowArray
    //         append, field, build

    //     else if coreType = typeof<int64> then
    //         let b = new Int64Array.Builder()
    //         let append v = appendWithNullCheck (fun x -> b.Append(unbox<int64> x) |> ignore) (fun () -> b.AppendNull() |> ignore) v
    //         let field () = new Field(name, Int64Type.Default, true)
    //         let build () = b.Build() :> IArrowArray
    //         append, field, build

    //     else if coreType = typeof<double> then
    //         let b = new DoubleArray.Builder()
    //         let append v = appendWithNullCheck (fun x -> b.Append(unbox<double> x) |> ignore) (fun () -> b.AppendNull() |> ignore) v
    //         let field () = new Field(name, DoubleType.Default, true)
    //         let build () = b.Build() :> IArrowArray
    //         append, field, build
            
    //     else if coreType = typeof<string> then
    //         let b = new StringArray.Builder()
    //         let append v = appendWithNullCheck (fun x -> b.Append(unbox<string> x) |> ignore) (fun () -> b.AppendNull() |> ignore) v
    //         let field () = new Field(name, StringType.Default, true)
    //         let build () = b.Build() :> IArrowArray
    //         append, field, build

    //     else if coreType = typeof<bool> then
    //         let b = new BooleanArray.Builder()
    //         let append v = appendWithNullCheck (fun x -> b.Append(unbox<bool> x) |> ignore) (fun () -> b.AppendNull() |> ignore) v
    //         let field () = new Field(name, BooleanType.Default, true)
    //         let build () = b.Build() :> IArrowArray
    //         append, field, build

    //     else if coreType = typeof<DateTime> then
    //         let tsType = new TimestampType(TimeUnit.Microsecond, (null: string))
    //         let b = new TimestampArray.Builder(tsType)
            
    //         let writeValue (x: obj) = 
    //             let dt = unbox<DateTime> x
    //             let dtUtc = DateTime(dt.Ticks, DateTimeKind.Utc)
    //             let dto = DateTimeOffset dtUtc
    //             b.Append dto |> ignore

    //         let writeNull () = b.AppendNull() |> ignore

    //         let append v = appendWithNullCheck writeValue writeNull v
    //         let field () = new Field(name, new TimestampType(TimeUnit.Microsecond, (null:string)), true)
    //         let build () = b.Build() :> IArrowArray
            
    //         append, field, build

    //     // [新增] Decimal Support
    //     else if coreType = typeof<decimal> then
    //         // 注意：这里我们无法预知所有数据的 Scale，必须假定一个足够大的值 (e.g. 28, 4)
    //         // 或者抛出异常建议用户使用 Series.create
    //         // 这里为了方便，我们默认使用 (38, 18) 或者 (28, 6) 这种通用精度
    //         // Arrow 的 DecimalBuilder 需要显式 Type
    //         let p, s = 28, 6
    //         let decType = new Decimal128Type(p, s)
    //         let b = new Decimal128Array.Builder(decType)
            
    //         let append v = appendWithNullCheck (fun x -> b.Append(unbox<decimal> x) |> ignore) (fun () -> b.AppendNull() |> ignore) v
    //         let field () = new Field(name, decType, true)
    //         let build () = b.Build() :> IArrowArray
    //         append, field, build

    //     else
    //         failwithf "Unsupported type for DataFrame.ofRecords: %s" coreType.Name

    // ==========================================
    // 4. Extensions (Exposed Methods)
    // ==========================================
    
    type DataFrame with
        
        /// <summary>
        /// [ToRecords] 将 DataFrame 转换为 F# Record 列表
        /// </summary>
        member this.ToRecords<'T>() : 'T list =
            if not (FSharpType.IsRecord typeof<'T>) then
                failwithf "Type '%s' is not an F# Record" typeof<'T>.Name
            
            let props = FSharpType.GetRecordFields typeof<'T>
            
            // 全量转 Arrow
            use batch = this.ToArrow()
            let rowCount = batch.Length
            
            // 预构建读取器
            let columnReaders = 
                props 
                |> Array.map (fun prop -> 
                    let col = batch.Column prop.Name
                    if isNull col then failwithf "Column '%s' not found in DataFrame" prop.Name
                    createColumnReader col prop.PropertyType
                )

            let result = ResizeArray<'T> rowCount
            let args = Array.zeroCreate<obj> columnReaders.Length
            
            for i in 0 .. rowCount - 1 do
                for c in 0 .. columnReaders.Length - 1 do
                    args.[c] <- columnReaders.[c] i
                
                let record = FSharpValue.MakeRecord(typeof<'T>, args) :?> 'T
                result.Add record
            
            Seq.toList result

        /// <summary>
        /// [ofRecords] 从 F# Record 序列创建 DataFrame
        /// </summary>
        static member ofRecords<'T> (data: seq<'T>) : DataFrame =
            if not (FSharpType.IsRecord typeof<'T>) then
                failwithf "Type '%s' is not an F# Record" typeof<'T>.Name

            // 1. 如果是空序列，尝试创建空 Schema (略麻烦，这里简单处理)
            let items = Seq.toArray data
            if items.Length = 0 then
                // 这里可能需要更复杂的逻辑来从 Type 推断空 Schema
                // 暂时返回空 DataFrame
                DataFrame.create []
            else
                // 2. 利用 Series.ofSeq 智能构建每一列
                // 这样做的好处是 Series.ofSeq 已经处理了 Decimal Scale 推断和 Option 识别
                let props = FSharpType.GetRecordFields typeof<'T>
                
                let seriesList = 
                    props 
                    |> Array.map (fun prop ->
                        let name = prop.Name
                        let values = items |> Array.map (fun item -> prop.GetValue(item))
                        
                        // 反射调用泛型 Series.ofSeq<'PropType>
                        // 因为我们拿到的 values 是 obj[]，必须动态分发
                        
                        // 动态构造泛型方法: Series.ofSeq<T>(name, seq)
                        let method = 
                            typeof<Series>.GetMethod("ofSeq", System.Reflection.BindingFlags.Public ||| System.Reflection.BindingFlags.Static)
                                          .MakeGenericMethod prop.PropertyType
                        
                        // 准备参数: name, data (cast to proper enumerable)
                        // 注意：Series.ofSeq 接受 seq<'T>，我们需要把 obj[] 转为 IEnumerable<'T>
                        // 最简单的办法是利用 System.Linq.Enumerable.Cast
                        let castedData = 
                            typeof<System.Linq.Enumerable>
                                .GetMethod("Cast")
                                .MakeGenericMethod(prop.PropertyType)
                                .Invoke(null, [| values |])
                        
                        method.Invoke(null, [| name; castedData |]) :?> Series
                    )
                    |> Array.toList

                DataFrame.create seriesList
        member this.Describe() : DataFrame =
            // 1. 筛选数值列 (Int/Float)
            // 我们利用 Schema 来判断
            let numericCols = 
                this.Schema 
                |> Map.filter (fun _ dtype -> 
                    dtype.StartsWith "i" || dtype.StartsWith "f" || dtype.StartsWith "u"
                )
                |> Map.keys
                |> Seq.toList

            if numericCols.IsEmpty then
                failwith "No numeric columns to describe."

            // 2. 定义统计指标
            // 每个指标生成一行数据
            let metrics = [
                "count",      fun (c: string) -> Polars.col(c).Count().Cast Float64
                "null_count", fun c -> Polars.col(c).IsNull().Sum().Cast Float64
                "mean",       fun c -> Polars.col(c).Mean()
                "std",        fun c -> Polars.col(c).Std()
                "min",        fun c -> Polars.col(c).Min().Cast Float64
                "25%",        fun c -> Polars.col(c).Quantile 0.25
                "50%",        fun c -> Polars.col(c).Median().Cast Float64 
                "75%",        fun c -> Polars.col(c).Quantile 0.75
                "max",        fun c -> Polars.col(c).Max().Cast Float64
            ]

            // 3. 构建聚合查询
            // 结果将是:
            // statistic | col1 | col2 ...
            // count     | ...  | ...
            // mean      | ...  | ...
            
            // 这里的策略是：先计算所有值，然后转置？
            // 不，Polars 推荐的方式是：构建一个包含 "statistic" 列和其他列的 List of DataFrames，然后 Concat。
            // 每一行（比如 mean）是一个小的 DataFrame：[statistic="mean", col1=mean1, col2=mean2...]
            
            let rowFrames = 
                metrics 
                |> List.map (fun (statName, op) ->
                    // 构造 Select 列表: [ Lit(statName).Alias("statistic"), op(col1), op(col2)... ]
                    let exprs = 
                        [ Polars.lit(statName).Alias "statistic" ] @
                        (numericCols |> List.map (fun c -> op c))
                    
                    // 对原 DF 执行 Select -> 得到 1 行 N 列的 DF
                    this |> Polars.select exprs
                )

            // 4. 垂直拼接 (Concat Vertical)
            Polars.concat rowFrames