using System;
using Polars.Native;

namespace Polars.CSharp;

/// <summary>
/// Represents a Polars data type. 
/// Wraps the underlying Rust DataType.
/// </summary>
/// <summary>
    /// Represents a Polars data type. 
    /// Wraps the underlying Rust DataType Handle and provides high-level metadata.
    /// </summary>
public class DataType : IDisposable
{
    internal DataTypeHandle Handle { get; }
    
    /// <summary>
    /// Gets the high-level kind of this data type.
    /// </summary>
    public DataTypeKind Kind { get; }

    // 修改构造函数：强制要求传入 Handle 和 Kind
    internal DataType(DataTypeHandle handle, DataTypeKind kind)
    {
        Handle = handle;
        Kind = kind;
    }

    /// <summary>
    /// Dispose the underlying DataTypeHandle.
    /// </summary>
    public void Dispose()
    {
        Handle.Dispose();
    }
    /// <summary>
    /// Output DataTypeKind to string
    /// </summary>
    /// <returns></returns>
    public override string ToString() => $"{Kind}";

    // ==========================================
    // Helper Properties
    // ==========================================

    /// <summary>
    /// Returns true if the type is a numeric type.
    /// </summary>
    public bool IsNumeric => Kind switch
    {
        DataTypeKind.Int8 or DataTypeKind.Int16 or DataTypeKind.Int32 or DataTypeKind.Int64 or
        DataTypeKind.UInt8 or DataTypeKind.UInt16 or DataTypeKind.UInt32 or DataTypeKind.UInt64 or
        DataTypeKind.Float32 or DataTypeKind.Float64 or DataTypeKind.Decimal => true,
        _ => false
    };

    // ==========================================
    // Schema Parsing Logic (C# 侧解析)
    // ==========================================
    /// <summary>
    /// 解析形如 "list[i64]" 或 "list[list[str]]" 的字符串
    /// </summary>
    private static DataType ParseListType(string typeStr)
    {
        // 格式通常是 "list[<inner_type>]"
        // 如果只是 "list" (极少见)，默认给个 List<Null> 或 List<Unknown>
        int openBracket = typeStr.IndexOf('[');
        int closeBracket = typeStr.LastIndexOf(']');

        if (openBracket > -1 && closeBracket > openBracket)
        {
            // 1. 提取内部类型字符串，例如 "i64" 或 "list[str]"
            string innerStr = typeStr.Substring(openBracket + 1, closeBracket - openBracket - 1).Trim();

            // 2. [关键递归] 再次调用 Parse 解析内部类型
            // 这一步会创建一个临时的 DataType 对象 (比如 Int64)
            using var innerType = DataType.Parse(innerStr);

            // 3. 使用内部类型的 Handle 创建 List Handle
            // 注意：PolarsWrapper.NewListType 应该接受 innerType.Handle
            // 并且 Rust 端通常会 Clone 这个 Handle 指向的类型定义，所以我们 dispose innerType 是安全的
            return new DataType(PolarsWrapper.NewListType(innerType.Handle), DataTypeKind.List);
        }

        // 如果解析失败或者只是纯 "list" 字符串，返回一个默认的 List<Null>
        // 这样至少 Kind 是对的，代码不会崩
        using var nullType = new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.Null), DataTypeKind.Null);
        return new DataType(PolarsWrapper.NewListType(nullType.Handle), DataTypeKind.List);
    }
    /// <summary>
    /// Parse a schema type string (e.g., "i64", "date") into a DataType object.
    /// </summary>
    public static DataType Parse(string str)
    {
        if (string.IsNullOrEmpty(str))
            return new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.Unknown), DataTypeKind.Unknown);

        // 简单类型直接匹配
        // 注意：这里调用下面的静态属性，会创建一个新的 Handle 并带上正确的 Kind
        return str switch
        {
            "bool" => Boolean,
            "i8" => Int8,
            "i16" => Int16,
            "i32" => Int32,
            "i64" => Int64,
            "u8" => UInt8,
            "u16" => UInt16,
            "u32" => UInt32,
            "u64" => UInt64,
            "f32" => Float32,
            "f64" => Float64,
            "str" or "String" or "Utf8" => String,
            "date" => Date,
            "time" => Time,
            "cat" => Categorical,
            "binary" => Binary,
            var s when s.Contains("struct") => Struct,
            
            // 复杂类型处理 (前缀匹配)
            // 这里我们尽可能返回正确的 Handle，如果需要参数化（如 Datetime 时区），
            // 暂时返回默认版本，或者需要更复杂的解析逻辑。
            // 既然主要目的是为了 schema 检查，返回默认 Datetime 是安全的。
            var s when s.StartsWith("datetime") => Datetime,
            var s when s.StartsWith("duration") => Duration,
            var s when s.StartsWith("decimal") => Decimal(38, 9), // 默认精度，用于 schema 占位
            var s when s.StartsWith("list") => ParseListType(s),
            _ => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.Unknown), DataTypeKind.Unknown)
        };
    }

    // ==========================================
    // Primitive Factories (Static Properties)
    // ==========================================
    
    // 每次调用都会创建一个新的 Handle，由 SafeHandle 负责释放
    // 关键修改：在创建时传入对应的 DataTypeKind
    /// <summary>
    /// Boolean
    /// </summary>
    public static DataType Boolean => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.Boolean), DataTypeKind.Boolean);
    /// <summary>
    /// Int8
    /// </summary>
    public static DataType Int8    => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.Int8), DataTypeKind.Int8);
    /// <summary>
    /// Int16
    /// </summary>
    public static DataType Int16   => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.Int16), DataTypeKind.Int16);
    /// <summary>
    /// Int32
    /// </summary>
    public static DataType Int32   => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.Int32), DataTypeKind.Int32);
    /// <summary>
    /// Int64
    /// </summary>
    public static DataType Int64   => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.Int64), DataTypeKind.Int64);
    /// <summary>
    /// UInt8
    /// </summary>
    public static DataType UInt8   => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.UInt8), DataTypeKind.UInt8);
    /// <summary>
    /// UInt16
    /// </summary>
    public static DataType UInt16  => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.UInt16), DataTypeKind.UInt16);
    /// <summary>
    /// UInt32
    /// </summary>
    public static DataType UInt32  => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.UInt32), DataTypeKind.UInt32);
    /// <summary>
    /// UInt64
    /// </summary>
    public static DataType UInt64  => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.UInt64), DataTypeKind.UInt64);
    /// <summary>
    /// Float32
    /// </summary>
    public static DataType Float32 => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.Float32), DataTypeKind.Float32);
    /// <summary>
    /// Float64
    /// </summary>
    public static DataType Float64 => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.Float64), DataTypeKind.Float64);
    /// <summary>
    /// String
    /// </summary>
    public static DataType String  => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.String), DataTypeKind.String);
    /// <summary>
    /// DateOnly
    /// </summary>
    public static DataType Date    => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.Date), DataTypeKind.Date);
    /// <summary>
    /// DateTime
    /// </summary>
    public static DataType Datetime=> new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.Datetime), DataTypeKind.Datetime);
    /// <summary>
    /// TimeOnly
    /// </summary>
    public static DataType Time    => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.Time), DataTypeKind.Time);
    /// <summary>
    /// Duration
    /// </summary>
    public static DataType Duration=> new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.Duration), DataTypeKind.Duration);
    /// <summary>
    /// Binary
    /// </summary>
    public static DataType Binary  => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.Binary), DataTypeKind.Binary);
    /// <summary>
    /// Null
    /// </summary>
    public static DataType Null  => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.Null), DataTypeKind.Null);
    /// <summary>
    /// Struct
    /// </summary>
    public static DataType Struct  => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.Struct), DataTypeKind.Struct);
    /// <summary>
    /// SameAsInput
    /// </summary>
    public static DataType SameAsInput => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.SameAsInput), DataTypeKind.Unknown);

    // ==========================================
    // Complex Factories (Methods)
    // ==========================================
    /// <summary>
    /// Decimal
    /// </summary>
    /// <param name="precision"></param>
    /// <param name="scale"></param>
    /// <returns></returns>
    public static DataType Decimal(int precision, int scale) 
        => new DataType(PolarsWrapper.NewDecimalType(precision, scale), DataTypeKind.Decimal);
    /// <summary>
    /// Categorical
    /// </summary>
    public static DataType Categorical 
        => new DataType(PolarsWrapper.NewCategoricalType(), DataTypeKind.Categorical);
}
/// <summary>
/// Enum of DataTypeKind
/// </summary>
public enum DataTypeKind
    {
        /// <summary>
        /// Boolean
        /// </summary>
        Boolean,
        /// <summary>
        /// Int8
        /// </summary>
        Int8,
        /// <summary>
        /// Int16
        /// </summary>
        Int16,
        /// <summary>
        /// Int32
        /// </summary>
        Int32,
        /// <summary>
        /// Int64
        /// </summary>
        Int64,
        /// <summary>
        /// UInt8
        /// </summary>
        UInt8,
        /// <summary>
        /// UInt16
        /// </summary>
        UInt16,
        /// <summary>
        /// UInt 
        /// </summary>
        UInt32,
        /// <summary>
        /// UInt64
        /// </summary>
        UInt64,
        /// <summary>
        /// Float32
        /// </summary>
        Float32,
        /// <summary>
        /// Float64
        /// </summary>
        Float64,
        /// <summary>
        /// String
        /// </summary>
        String,
        /// <summary>
        /// DateOnly
        /// </summary>
        Date,
        /// <summary>
        /// DateTime
        /// </summary>
        Datetime,
        /// <summary>
        /// Duration
        /// </summary>
        Duration,
        /// <summary>
        /// Time
        /// </summary>
        Time,
        /// <summary>
        /// List
        /// </summary>
        List,
        /// <summary>
        /// Struct
        /// </summary>
        Struct,
        /// <summary>
        /// Object
        /// </summary>
        Object,
        /// <summary>
        /// Categorical
        /// </summary>
        Categorical,
        /// <summary>
        /// Decimal
        /// </summary>
        Decimal,
        /// <summary>
        /// Binary
        /// </summary>
        Binary,
        /// <summary>
        /// Null
        /// </summary>
        Null,
        /// <summary>
        /// Unknown
        /// </summary>
        Unknown
    }