namespace Polars.FSharp

open Polars.NET.Core

type TimeUnit = 
    | Nanoseconds
    | Microseconds
    | Milliseconds

// 定义字段 (用于 Struct)
type Field = { Name: string; DataType: DataType }

/// <summary>
/// Polars data types for casting and schema definitions.
/// </summary>
and DataType =
    | Boolean
    | Int8 | Int16 | Int32 | Int64
    | UInt8 | UInt16 | UInt32 | UInt64
    | Float32 | Float64
    | String
    | Date | Datetime of TimeUnit * string option | Time
    | Duration of TimeUnit
    | Binary
    | Categorical
    | Decimal of precision: int option * scale: int option
    | Unknown | SameAsInput | Null | List of DataType
    | Struct of Field list

    // 转换 helper
    static member FromHandle (handle: DataTypeHandle) : DataType =
        // 1. 获取类型枚举值 (Kind)
        let kind = PolarsWrapper.GetDataTypeKind handle

    // 2. 匹配 Kind (假设 Rust 端的枚举顺序如下，请根据实际情况校对)
        match kind with
        | 1 -> Boolean
        | 2 -> Int8
        | 3 -> Int16
        | 4 -> Int32
        | 5 -> Int64
        | 6 -> UInt8
        | 7 -> UInt16
        | 8 -> UInt32
        | 9 -> UInt64
        | 10 -> Float32
        | 11 -> Float64
        | 12 -> String // Utf8
        | 13 -> Date
        
        // --- 复杂类型处理 ---
        
        // Datetime
        | 14 -> 
            let unitCode = PolarsWrapper.GetTimeUnit handle
            let unit = 
                match unitCode with 
                | 0 -> Nanoseconds 
                | 1 -> Microseconds 
                | 2 -> Milliseconds 
                | _ -> Microseconds
            
            let tz = Option.ofObj (PolarsWrapper.GetTimeZone handle)
            Datetime(unit, tz)

        | 15 -> Time
        
        // Duration
        | 16 -> 
            let unitCode = PolarsWrapper.GetTimeUnit handle
            let unit = 
                match unitCode with 
                | 0 -> Nanoseconds 
                | 1 -> Microseconds 
                | 2 -> Milliseconds 
                | _ -> Microseconds
            Duration unit

        | 17 -> Binary
        | 18 -> Null // 同时也用于 List<Null>
        
        // Struct (递归)
        | 19 -> 
            let len = PolarsWrapper.GetStructLen handle
            let fields = 
                [ for i in 0UL .. len - 1UL do
                    let mutable name = Unchecked.defaultof<string>
                    let mutable fieldHandle = Unchecked.defaultof<DataTypeHandle>
                    
                    // 调用 C# Wrapper (out 参数在 F# 中用 & 引用)
                    PolarsWrapper.GetStructField(handle, i, &name, &fieldHandle)
                    
                    // [关键] 必须 Dispose 临时 Handle
                    // 使用 use 确保它在当前迭代结束时释放
                    use h = fieldHandle 
                    yield { Name = name; DataType = DataType.FromHandle h }
                ]
            Struct fields

        // List (递归)
        | 20 -> 
            // 获取内部类型的 Handle
            use innerHandle = PolarsWrapper.GetListInnerType handle
            let innerType = DataType.FromHandle innerHandle
            List innerType

        | 21 -> Categorical

        // Decimal
        | 22 -> // 假设 22 是 Decimal
            let mutable prec = 0
            let mutable scale = 0
            PolarsWrapper.GetDecimalInfo(handle, &prec, &scale)
            Decimal(Some prec,Some scale)

        | _ -> Unknown

    member this.IsNumeric =
        match this with
        | UInt8 | UInt16 | UInt32 | UInt64
        | Int8 | Int16 | Int32 | Int64
        | Float32 | Float64 
        | Decimal _ -> true
        | _ -> false

    /// <summary>
    /// Creates a native Polars DataTypeHandle from this F# DataType.
    /// Recursive structures (List, Struct) are handled automatically.
    /// </summary>
    member internal this.CreateHandle() : DataTypeHandle =
        
        // 辅助：将 TimeUnit 转为 int code (0=ns, 1=us, 2=ms)
        let toUnitCode tu = 
            match tu with 
            | Nanoseconds -> 0 
            | Microseconds -> 1 
            | Milliseconds -> 2

        match this with
        | SameAsInput -> PolarsWrapper.NewPrimitiveType 0
        | Null -> PolarsWrapper.NewPrimitiveType 18
        | Boolean -> PolarsWrapper.NewPrimitiveType 1
        | Int8 -> PolarsWrapper.NewPrimitiveType 2
        | Int16 -> PolarsWrapper.NewPrimitiveType 3
        | Int32 -> PolarsWrapper.NewPrimitiveType 4
        | Int64 -> PolarsWrapper.NewPrimitiveType 5
        | UInt8 -> PolarsWrapper.NewPrimitiveType 6
        | UInt16 -> PolarsWrapper.NewPrimitiveType 7
        | UInt32 -> PolarsWrapper.NewPrimitiveType 8
        | UInt64 -> PolarsWrapper.NewPrimitiveType 9
        | Float32 -> PolarsWrapper.NewPrimitiveType 10
        | Float64 -> PolarsWrapper.NewPrimitiveType 11
        | String -> PolarsWrapper.NewPrimitiveType 12
        | Binary -> PolarsWrapper.NewPrimitiveType 17
        | Date -> PolarsWrapper.NewPrimitiveType 13
        | Time -> PolarsWrapper.NewPrimitiveType 15
        
        // --- 复杂类型 ---

        // [升级] Datetime: 传递单位和时区
        | Datetime(unit, tz) ->
            let code = toUnitCode unit
            let tzStr = Option.toObj tz // None -> null
            PolarsWrapper.NewDateTimeType(code, tzStr)

        // [升级] Duration: 传递单位
        | Duration unit ->
            let code = toUnitCode unit
            PolarsWrapper.NewDurationType code

        // [升级] Categorical
        | Categorical -> 
            PolarsWrapper.NewCategoricalType()

        // [升级] Decimal: 传递精度 (p, s)
        | Decimal(p, s) ->
            // Rust 端通常用 0 或特定的值表示 None，这里假设 0 为自动/默认
            let prec = defaultArg p 0
            let scale = defaultArg s 0 
            PolarsWrapper.NewDecimalType(prec, scale)

        // [升级] List: 递归创建
        | List innerType ->
            // 1. 递归创建内部类型的 Handle
            // 使用 use 确保这个临时 Handle 在传给 NewListType 后被释放
            use innerHandle = innerType.CreateHandle()
            
            // 2. 传递给 Wrapper (Rust 会 Clone 这个类型定义)
            PolarsWrapper.NewListType innerHandle

        // [升级] Struct: 递归创建字段
        | Struct fields ->
            let names = fields |> List.map (fun f -> f.Name) |> List.toArray
            
            // 1. 递归创建所有字段类型的 Handle
            let typeHandles = fields |> List.map (fun f -> f.DataType.CreateHandle()) |> List.toArray
            
            try
                // 2. 传递给 Wrapper
                PolarsWrapper.NewStructType(names, typeHandles)
            finally
                // 3. [关键] 清理所有临时的子 Handle
                // 因为 NewStructType 内部将这些类型转成了 C 数组传给 Rust
                // Rust 那边复制完数据后，这边的 Handle 就没用了
                for h in typeHandles do h.Dispose()

        | Unknown -> PolarsWrapper.NewPrimitiveType 0
        
/// <summary>
/// Represents the type of join operation to perform.
/// </summary>
type JoinType =
    | Inner
    | Left
    | Outer
    | Cross
    | Semi
    | Anti
    
    // 内部转换 helper
    member internal this.ToNative() =
        match this with
        | Inner -> PlJoinType.Inner
        | Left -> PlJoinType.Left
        | Outer -> PlJoinType.Outer
        | Cross -> PlJoinType.Cross
        | Semi -> PlJoinType.Semi
        | Anti -> PlJoinType.Anti

/// <summary>
/// Specifies the aggregation function for pivot operations.
/// </summary>
type PivotAgg =
    | First | Sum | Min | Max | Mean | Median | Count | Last
    
    member internal this.ToNative() =
        match this with
        | First -> PlPivotAgg.First
        | Sum -> PlPivotAgg.Sum
        | Min -> PlPivotAgg.Min
        | Max -> PlPivotAgg.Max
        | Mean -> PlPivotAgg.Mean
        | Median -> PlPivotAgg.Median
        | Count -> PlPivotAgg.Count
        | Last -> PlPivotAgg.Last

/// <summary>
/// Specifies the type of concat operations.
/// </summary>
type ConcatType =
    | Vertical
    | Horizonal
    | Diagonal
    
    // 内部转换 helper
    member internal this.ToNative() =
        match this with
        | Vertical -> PlConcatType.Vertical
        | Horizonal -> PlConcatType.Horizontal
        | Diagonal -> PlConcatType.Diagonal
