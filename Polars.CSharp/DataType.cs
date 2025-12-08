using Polars.Native;

namespace Polars.CSharp;

/// <summary>
/// Data types supported by Polars.
/// </summary>
/// <summary>
/// Data types supported by Polars.
/// </summary>
public enum DataType
{
    /// <summary>
    /// Use the same data type as the input expression (Commonly used for UDFs).
    /// </summary>
    SameAsInput,

    /// <summary>
    /// Boolean Type
    /// </summary>
    Boolean,
    
    /// <summary>
    /// Signed 8-bit Integer Type
    /// </summary>
    Int8,
    
    /// <summary>
    /// Signed 16-bit Integer Type
    /// </summary>
    Int16, 
    
    /// <summary>
    /// Signed 32-bit Integer Type
    /// </summary>
    Int32, 
    
    /// <summary>
    /// Signed 64-bit Integer Type
    /// </summary>
    Int64,
    
    /// <summary>
    /// Unsigned 8-bit Integer Type
    /// </summary>
    UInt8, 
    
    /// <summary>
    /// Unsigned 16-bit Integer Type
    /// </summary>
    UInt16, 
    
    /// <summary>
    /// Unsigned 32-bit Integer Type
    /// </summary>
    UInt32, 
    
    /// <summary>
    /// Unsigned 64-bit Integer Type
    /// </summary>
    UInt64,
    
    /// <summary>
    /// 32-bit Floating Point Type
    /// </summary>
    Float32, 
    
    /// <summary>
    /// 64-bit Floating Point Type
    /// </summary>
    Float64,
    
    /// <summary>
    /// String Types (UTF-8)
    /// </summary>
    String,
    
    /// <summary>
    /// Date Type (Days since epoch)
    /// </summary>
    Date, 
    
    /// <summary>
    /// DateTime Type (Microseconds since epoch)
    /// </summary>
    Datetime, 
    
    /// <summary>
    /// Time Type (Nanoseconds since midnight)
    /// </summary>
    Time, 
    
    /// <summary>
    /// Duration Type
    /// </summary>
    Duration,
    
    /// <summary>
    /// Binary Type (Bytes)
    /// </summary>
    Binary,
    
    /// <summary>
    /// Unknown or Unsupported Type
    /// </summary>
    Unknown
}
internal static class DataTypeExtensions
{
    public static PlDataType ToNative(this DataType dt) => dt switch
    {
        DataType.SameAsInput => PlDataType.SameAsInput, // [新增映射]
        DataType.Boolean => PlDataType.Boolean,
        DataType.Int8 => PlDataType.Int8,
        DataType.Int16 => PlDataType.Int16,
        DataType.Int32 => PlDataType.Int32,
        DataType.Int64 => PlDataType.Int64,
        DataType.UInt8 => PlDataType.UInt8,
        DataType.UInt16 => PlDataType.UInt16,
        DataType.UInt32 => PlDataType.UInt32,
        DataType.UInt64 => PlDataType.UInt64,
        DataType.Float32 => PlDataType.Float32,
        DataType.Float64 => PlDataType.Float64,
        DataType.String => PlDataType.String,
        DataType.Date => PlDataType.Date,
        DataType.Datetime => PlDataType.Datetime,
        DataType.Time => PlDataType.Time,
        DataType.Duration => PlDataType.Duration,
        DataType.Binary => PlDataType.Binary,
        _ => PlDataType.Unknown
    };
}