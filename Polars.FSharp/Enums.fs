namespace Polars.FSharp

open Polars.NET.Core

/// <summary>
/// Polars data types for casting and schema definitions.
/// </summary>
type DataType =
    | Boolean
    | Int8 | Int16 | Int32 | Int64
    | UInt8 | UInt16 | UInt32 | UInt64
    | Float16 | Float32 | Float64
    | String
    | Date | Datetime | Time
    | Duration
    | Binary
    | Categorical
    | Struct
    | Decimal of precision: int option * scale: int
    | Unknown | SameAsInput | Null

    // 转换 helper
    member internal this.CreateHandle() =
        match this with
        | SameAsInput -> PolarsWrapper.NewPrimitiveType 0
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
        | Date -> PolarsWrapper.NewPrimitiveType 13
        | Datetime -> PolarsWrapper.NewPrimitiveType 14
        | Time -> PolarsWrapper.NewPrimitiveType 15
        | Duration -> PolarsWrapper.NewPrimitiveType 16
        | Binary -> PolarsWrapper.NewPrimitiveType 17
        | Null -> PolarsWrapper.NewPrimitiveType 18
        | Struct -> PolarsWrapper.NewPrimitiveType 19
        | Float16 -> PolarsWrapper.NewPrimitiveType 20
        | Unknown -> PolarsWrapper.NewPrimitiveType 0
        | Categorical -> PolarsWrapper.NewCategoricalType()
        | Decimal (p, s) -> 
            let prec = defaultArg p 0 // 0 means None in Rust shim
            PolarsWrapper.NewDecimalType(prec, s)
    static member Parse(str: string) =
        match str with
        | "bool" -> Boolean
        | "i8" -> Int8
        | "i16" -> Int16
        | "i32" -> Int32
        | "i64" -> Int64
        | "u8" -> UInt8
        | "u16" -> UInt16
        | "u32" -> UInt32
        | "u64" -> UInt64
        | "f16" -> Float16
        | "f32" -> Float32
        | "f64" -> Float64
        | "str" | "String" -> String // 兼容一下旧版
        | "date" -> Date
        | "time" -> Time
        | "null" -> Null
        | "struct" -> Struct
        | s when s.StartsWith "datetime" -> Datetime // 处理 "datetime[μs]" 这种带参数的
        | s when s.StartsWith "duration" -> Duration
        | s when s.StartsWith "decimal" -> 
            // 简单处理 decimal，暂不解析具体精度
            Decimal(None, 0)
        | "cat" -> Categorical
        | _ -> Unknown
    member this.IsNumeric =
        match this with
        | UInt8 | UInt16 | UInt32 | UInt64
        | Int8 | Int16 | Int32 | Int64
        | Float16 | Float32 | Float64 
        | Decimal _ -> true
        | _ -> false

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