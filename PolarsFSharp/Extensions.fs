namespace PolarsFSharp

open System
open FSharp.Reflection
open Apache.Arrow
open Apache.Arrow.Types
open Polars.Native

// [修复] 把所有辅助逻辑放在一个独立的模块里
[<AutoOpen>]
module SerializationHelpers =

    // 定义读取器类型
    type RowReader = int -> obj

    // --- 1. 辅助：Option 包装器 ---
    let createOptionWrapper (t: Type) : (obj -> obj) * obj =
        if t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<option<_>> then
            let cases = FSharpType.GetUnionCases(t)
            let noneCase = cases |> Array.find (fun c -> c.Name = "None")
            let someCase = cases |> Array.find (fun c -> c.Name = "Some")
            
            let mkSome (v: obj) = FSharpValue.MakeUnion(someCase, [| v |])
            let noneValue = FSharpValue.MakeUnion(noneCase, [||])
            
            mkSome, noneValue
        else
            (fun x -> x), null

    // --- 2. 核心：为某一列创建一个读取器 ---
    let createColumnReader (col: IArrowArray) (targetType: Type) : RowReader =
        
        let isOption = targetType.IsGenericType && targetType.GetGenericTypeDefinition() = typedefof<option<_>>
        let coreType = if isOption then targetType.GetGenericArguments().[0] else targetType

        // 获取 Option 包装工具
        let (wrapSome, valueNone) = createOptionWrapper targetType

        // 定义原始值读取函数 (Raw Getter)
        let getRawValue : int -> obj =
            match col with
            // --- 整数 ---
            | :? Int64Array as arr -> fun i -> box (arr.GetValue(i).GetValueOrDefault())
            | :? Int32Array as arr -> fun i -> box (arr.GetValue(i).GetValueOrDefault())
            | :? Int16Array as arr -> fun i -> box (arr.GetValue(i).GetValueOrDefault())
            | :? Int8Array  as arr -> fun i -> box (arr.GetValue(i).GetValueOrDefault())
            | :? UInt64Array as arr -> fun i -> box (arr.GetValue(i).GetValueOrDefault())
            | :? UInt32Array as arr -> fun i -> box (arr.GetValue(i).GetValueOrDefault())
            | :? UInt16Array as arr -> fun i -> box (arr.GetValue(i).GetValueOrDefault())
            | :? UInt8Array  as arr -> fun i -> box (arr.GetValue(i).GetValueOrDefault())

            // --- 浮点 ---
            | :? DoubleArray as arr -> fun i -> box (arr.GetValue(i).GetValueOrDefault())
            | :? FloatArray  as arr -> fun i -> box (arr.GetValue(i).GetValueOrDefault())

            // --- 字符串 ---
            | :? StringArray as arr -> 
                fun i -> box (arr.GetString(i)) 
            | :? StringViewArray as arr -> 
                fun i -> box (arr.GetString(i))
            
            // --- 布尔 ---
            | :? BooleanArray as arr -> fun i -> box (arr.GetValue(i).GetValueOrDefault())

            // --- 时间 ---
            | :? Date32Array as arr -> 
                fun i -> 
                    let v = arr.GetValue(i).GetValueOrDefault()
                    box (DateTime(1970, 1, 1).AddDays(float v))
            
            | :? TimestampArray as arr ->
                fun i ->
                    let v = arr.GetValue(i).GetValueOrDefault()
                    try box (DateTime.UnixEpoch.AddTicks(v * 10L)) 
                    with _ -> box DateTime.MinValue

            | _ -> failwithf "Unsupported Arrow Type: %s" (col.GetType().Name)

        // 返回最终的 Reader
        fun (rowIndex: int) ->
            if col.IsNull(rowIndex) then
                if isOption then 
                    valueNone
                else 
                    if not coreType.IsValueType then null
                    else failwithf "Column '%s' has null at row %d but record field '%s' is not Option" (col.GetType().Name) rowIndex (targetType.Name)
            else
                let raw = getRawValue rowIndex
                
                let converted = 
                    if isNull raw then null
                    elif raw.GetType() = coreType then raw
                    else Convert.ChangeType(raw, coreType)
                
                if isOption then wrapSome converted else converted

// [修复] 独立的模块用于放置扩展方法
[<AutoOpen>]
module DataFrameExtensions =
    
    // 这里是对 DataFrame 类型的扩展
    type DataFrame with
        member this.ToRecords<'T>() : 'T list =
            if not (FSharpType.IsRecord(typeof<'T>)) then
                failwithf "Type '%s' is not an F# Record" (typeof<'T>.Name)
            
            let props = FSharpType.GetRecordFields(typeof<'T>)
            
            use batch = this.ToArrow()
            let rowCount = batch.Length
            
            // 调用上面定义的辅助函数
            let columnReaders = 
                props 
                |> Array.map (fun prop -> 
                    let col = batch.Column(prop.Name)
                    if isNull col then failwithf "Column '%s' not found in DataFrame" prop.Name
                    
                    // 调用外部模块的函数
                    SerializationHelpers.createColumnReader col prop.PropertyType
                )

            let result = ResizeArray<'T>(rowCount)
            let args = Array.zeroCreate<obj> columnReaders.Length
            
            for i in 0 .. rowCount - 1 do
                for c in 0 .. columnReaders.Length - 1 do
                    args.[c] <- columnReaders.[c] i
                
                let record = FSharpValue.MakeRecord(typeof<'T>, args) :?> 'T
                result.Add(record)
            
            Seq.toList result