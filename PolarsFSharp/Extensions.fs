namespace PolarsFSharp

open System
open FSharp.Reflection
open Apache.Arrow
open Apache.Arrow.Types
open Polars.Native

[<AutoOpen>]
module Serialization =

    // ==========================================
    // 1. Helpers
    // ==========================================

    // 创建 Option.Some / Option.None 的构造器 (用于读)
    let private createOptionWrapper (t: Type) : (obj -> obj) * obj =
        if t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<option<_>> then
            let cases = FSharpType.GetUnionCases(t)
            let noneCase = cases |> Array.find (fun c -> c.Name = "None")
            let someCase = cases |> Array.find (fun c -> c.Name = "Some")
            
            let mkSome (v: obj) = FSharpValue.MakeUnion(someCase, [| v |])
            let noneValue = FSharpValue.MakeUnion(noneCase, [||])
            
            (mkSome, noneValue)
        else
            // 非 Option 类型，不做包装，None 值就是 null
            ((fun x -> x), null)

    // 创建 Option 解包器 (用于写: Some x -> x, None -> null)
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
    // 2. Reading (Arrow -> Record)
    // ==========================================

    // 工厂：为某一列创建一个专用的读取闭包 (int -> obj)
    let private createColumnReader (col: IArrowArray) (targetType: Type) : (int -> obj) =
        
        let isOption = targetType.IsGenericType && targetType.GetGenericTypeDefinition() = typedefof<option<_>>
        let coreType = if isOption then targetType.GetGenericArguments().[0] else targetType

        let (wrapSome, valueNone) = createOptionWrapper targetType

        // 内部读取器：假设非空，读取原始值
        let getRawValue : int -> obj =
            match col with
            // 数值类型：使用 GetValueOrDefault 配合 box
            | :? Int64Array as arr -> fun i -> box (arr.GetValue(i).GetValueOrDefault())
            | :? Int32Array as arr -> fun i -> box (arr.GetValue(i).GetValueOrDefault())
            | :? Int16Array as arr -> fun i -> box (arr.GetValue(i).GetValueOrDefault())
            | :? Int8Array  as arr -> fun i -> box (arr.GetValue(i).GetValueOrDefault())
            | :? UInt64Array as arr -> fun i -> box (arr.GetValue(i).GetValueOrDefault())
            | :? UInt32Array as arr -> fun i -> box (arr.GetValue(i).GetValueOrDefault())
            | :? UInt16Array as arr -> fun i -> box (arr.GetValue(i).GetValueOrDefault())
            | :? UInt8Array  as arr -> fun i -> box (arr.GetValue(i).GetValueOrDefault())
            | :? DoubleArray as arr -> fun i -> box (arr.GetValue(i).GetValueOrDefault())
            | :? FloatArray  as arr -> fun i -> box (arr.GetValue(i).GetValueOrDefault())

            // 字符串：Arrow String 可能返回 null
            | :? StringArray as arr -> 
                fun i -> 
                    let s = arr.GetString(i)
                    if isNull s then box "" else box s
            | :? StringViewArray as arr -> 
                fun i -> 
                    let s = arr.GetString(i)
                    if isNull s then box "" else box s
            
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
                    // Polars 默认 Microseconds
                    try box (DateTime.UnixEpoch.AddTicks(v * 10L)) 
                    with _ -> box DateTime.MinValue

            | _ -> failwithf "Unsupported Arrow Type for reading: %s" (col.GetType().Name)

        // 返回闭包：处理 Null 和 类型转换
        fun (rowIndex: int) ->
            if col.IsNull(rowIndex) then
                if isOption then valueNone
                else if not coreType.IsValueType then null // 引用类型允许 null
                else failwithf "Column '%s' has null at row %d but record field '%s' is not Option" (col.GetType().Name) rowIndex (targetType.Name)
            else
                let raw = getRawValue rowIndex
                // 类型转换 (例如 Arrow Int64 -> Record Int32)
                let converted = 
                    if isNull raw then null
                    elif raw.GetType() = coreType then raw
                    else Convert.ChangeType(raw, coreType)
                
                if isOption then wrapSome converted else converted

    // ==========================================
    // 3. Writing (Record -> Arrow)
    // ==========================================

    // 工厂：为某个属性创建写入闭包
    // 返回: (AppendFunc, FieldCreator, ArrayBuilder)
    let private createFieldWriter (prop: Reflection.PropertyInfo) 
        : (obj -> unit) * (unit -> Field) * (unit -> IArrowArray) =
        
        let t = prop.PropertyType
        let name = prop.Name
        
        let isOption = t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<option<_>>
        let coreType = if isOption then t.GetGenericArguments().[0] else t
        
        // 获取解包器
        let unwrap = createOptionUnwrapper t

        // 通用 Append 逻辑
        let appendWithNullCheck (f: obj -> unit) (builderNull: unit -> unit) (v: obj) =
            let realVal = unwrap v
            if isNull realVal then builderNull()
            else f realVal

        if coreType = typeof<int> then
            let b = new Int32Array.Builder()
            let append v = appendWithNullCheck (fun x -> b.Append(unbox<int> x) |> ignore) (fun () -> b.AppendNull() |> ignore) v
            let field () = new Field(name, Int32Type.Default, true)
            let build () = b.Build() :> IArrowArray
            (append, field, build)

        else if coreType = typeof<int64> then
            let b = new Int64Array.Builder()
            let append v = appendWithNullCheck (fun x -> b.Append(unbox<int64> x) |> ignore) (fun () -> b.AppendNull() |> ignore) v
            let field () = new Field(name, Int64Type.Default, true)
            let build () = b.Build() :> IArrowArray
            (append, field, build)

        else if coreType = typeof<double> then
            let b = new DoubleArray.Builder()
            let append v = appendWithNullCheck (fun x -> b.Append(unbox<double> x) |> ignore) (fun () -> b.AppendNull() |> ignore) v
            let field () = new Field(name, DoubleType.Default, true)
            let build () = b.Build() :> IArrowArray
            (append, field, build)
            
        else if coreType = typeof<string> then
            let b = new StringArray.Builder() // 写的时候用 StringArray 兼容性最好
            let append v = appendWithNullCheck (fun x -> b.Append(unbox<string> x) |> ignore) (fun () -> b.AppendNull() |> ignore) v
            let field () = new Field(name, StringType.Default, true)
            let build () = b.Build() :> IArrowArray
            (append, field, build)

        else if coreType = typeof<bool> then
            let b = new BooleanArray.Builder()
            let append v = appendWithNullCheck (fun x -> b.Append(unbox<bool> x) |> ignore) (fun () -> b.AppendNull() |> ignore) v
            let field () = new Field(name, BooleanType.Default, true)
            let build () = b.Build() :> IArrowArray
            (append, field, build)

        else if coreType = typeof<DateTime> then
                    // Arrow 默认使用 Microsecond，这也是 Polars 的默认值
                    let tsType = new TimestampType(TimeUnit.Microsecond, (null: string))
                    let b = new TimestampArray.Builder(tsType)
                    
                    let writeValue (x: obj) = 
                        let dt = unbox<DateTime> x
                        let dtUtc = DateTime(dt.Ticks, DateTimeKind.Utc)
                        // Builder.Append 需要 DateTimeOffset，不要传 long
                        // Arrow 会自动根据 TimeUnit 将其转换为对应的微秒整数
                        let dto = DateTimeOffset dtUtc
                        b.Append dto |> ignore

                    let writeNull () = 
                        b.AppendNull() |> ignore

                    let append v = appendWithNullCheck writeValue writeNull v
                    
                    // [修复 2] 显式将 null 转换为 string，消除构造函数重载歧义
                    // (TimeUnit.Microsecond, timezone=null)
                    let field () = new Field(name, new TimestampType(TimeUnit.Microsecond, (null:string)), true)
                    
                    let build () = b.Build() :> IArrowArray
                    
                    append, field, build

        else
            failwithf "Unsupported type for DataFrame.ofRecords: %s" coreType.Name

    // ==========================================
    // 4. Extensions
    // ==========================================
    
    type DataFrame with
        
        /// <summary>
        /// [ToRecords] 将 DataFrame 转换为 F# Record 列表
        /// </summary>
        member this.ToRecords<'T>() : 'T list =
            if not (FSharpType.IsRecord(typeof<'T>)) then
                failwithf "Type '%s' is not an F# Record" (typeof<'T>.Name)
            
            let props = FSharpType.GetRecordFields(typeof<'T>)
            
            // 全量转 Arrow (适合中小数据量实体化)
            use batch = this.ToArrow()
            let rowCount = batch.Length
            
            // 预构建读取器
            let columnReaders = 
                props 
                |> Array.map (fun prop -> 
                    let col = batch.Column(prop.Name)
                    if isNull col then failwithf "Column '%s' not found in DataFrame" prop.Name
                    
                    createColumnReader col prop.PropertyType
                )

            let result = ResizeArray<'T>(rowCount)
            let args = Array.zeroCreate<obj> columnReaders.Length
            
            for i in 0 .. rowCount - 1 do
                for c in 0 .. columnReaders.Length - 1 do
                    args.[c] <- columnReaders.[c] i
                
                let record = FSharpValue.MakeRecord(typeof<'T>, args) :?> 'T
                result.Add(record)
            
            Seq.toList result

        /// <summary>
        /// [ofRecords] 从 F# Record 序列创建 DataFrame
        /// </summary>
        static member ofRecords<'T> (data: seq<'T>) : DataFrame =
            if not (FSharpType.IsRecord(typeof<'T>)) then
                failwithf "Type '%s' is not an F# Record" (typeof<'T>.Name)

            let props = FSharpType.GetRecordFields(typeof<'T>)
            
            // 必须先物化 seq 以获取长度 (RecordBatch 需要)
            let items = Seq.toArray data
            let rowCount = items.Length
            
            // 预构建写入器
            let handlers = props |> Array.map createFieldWriter
            
            // 填充数据
            for item in items do
                let values = FSharpValue.GetRecordFields(item)
                for i in 0 .. handlers.Length - 1 do
                    let (append, _, _) = handlers.[i]
                    append values.[i]

            // 构建 Arrow Batch
            let fields = ResizeArray<Field>()
            let arrays = ResizeArray<IArrowArray>()
            
            for (_, createField, buildArray) in handlers do
                fields.Add(createField())
                arrays.Add(buildArray())

            let schema = new Schema(fields, null)
            
            // 这里的 RecordBatch 内存由 C# 分配
            use batch = new RecordBatch(schema, arrays, rowCount)
            
            // 零拷贝传入 Polars (PolarsWrapper.FromArrow 内部会处理导出)
            new DataFrame(PolarsWrapper.FromArrow(batch))