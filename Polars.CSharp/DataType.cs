#pragma warning disable CS1591 // 缺少对公共可见类型或成员的 XML 注释
using Polars.NET.Core;

namespace Polars.CSharp;

/// <summary>
/// Represents a Polars data type. 
/// Wraps the underlying Rust DataType Handle and provides high-level metadata.
/// </summary>
public class DataType : IDisposable
{
    internal DataTypeHandle Handle { get; }
    private DataTypeHandle CloneHandle() => PolarsWrapper.CloneHandle(Handle);
    
    /// <summary>
    /// Gets the high-level kind of this data type.
    /// </summary>
    public DataTypeKind Kind { get; }
    private string? _displayString;
    public int? Precision { get; private set; }
    public int? Scale { get; private set; }

    public TimeUnit? Unit { get; private set; }
    public string? TimeZone { get; private set; }

    // 修改构造函数：强制要求传入 Handle 和 Kind
    internal DataType(DataTypeHandle handle, DataTypeKind kind = DataTypeKind.Unknown)
    {
        Handle = handle;

        // 1. 确定 Kind
        if (kind == DataTypeKind.Unknown)
        {
            // 调用 Wrapper 的 Borrow 方法
            Kind = (DataTypeKind)PolarsWrapper.GetDataTypeKind(Handle);
        }
        else
        {
            Kind = kind;
        }

        // 2. 提取元数据 (也是通过 Wrapper Borrow)
        switch (Kind)
        {
            case DataTypeKind.Datetime:
                Unit = MapIntToTimeUnit(PolarsWrapper.GetTimeUnit(Handle));
                TimeZone = PolarsWrapper.GetTimeZone(Handle);
                break;

            case DataTypeKind.Duration:
                Unit = MapIntToTimeUnit(PolarsWrapper.GetTimeUnit(Handle));
                break;

            case DataTypeKind.Decimal:
                PolarsWrapper.GetDecimalInfo(Handle, out int p, out int s);
                Precision = p;
                Scale = s;
                break;
        }
    }
    private static TimeUnit? MapIntToTimeUnit(int val) => val switch
    {
        0 => TimeUnit.Nanoseconds, 1 => TimeUnit.Microseconds, 2 => TimeUnit.Milliseconds, _ => null
    };
    
    /// <summary>
    /// Dispose the underlying DataTypeHandle.
    /// </summary>
    public void Dispose()
    {
        Handle.Dispose();
    }
    /// <summary>
    /// Output DataType string (e.g., "datetime[ms, Asia/Shanghai]")
    /// </summary>
    public override string ToString()
    {
        if (_displayString == null)
        {
            // ToString requires ownership transfer in our binding design
            using var clone = PolarsWrapper.CloneHandle(Handle);
            _displayString = PolarsWrapper.GetDataTypeString(clone);
        }
        return _displayString;
    }
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
    // Primitive Factories (Static Properties)
    // ==========================================
    
    // 每次调用都会创建一个新的 Handle，由 SafeHandle 负责释放
    // 关键修改：在创建时传入对应的 DataTypeKind
    public static DataType Unknown => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.Unknown), DataTypeKind.Unknown);
    public static DataType Boolean => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.Boolean), DataTypeKind.Boolean);
    public static DataType Int8    => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.Int8), DataTypeKind.Int8);
    public static DataType Int16   => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.Int16), DataTypeKind.Int16);
    public static DataType Int32   => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.Int32), DataTypeKind.Int32);    
    public static DataType Int64   => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.Int64), DataTypeKind.Int64);
    public static DataType UInt8   => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.UInt8), DataTypeKind.UInt8);
    public static DataType UInt16  => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.UInt16), DataTypeKind.UInt16);
    public static DataType UInt32  => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.UInt32), DataTypeKind.UInt32);
    public static DataType UInt64  => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.UInt64), DataTypeKind.UInt64);
    public static DataType Float32 => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.Float32), DataTypeKind.Float32);
    public static DataType Float64 => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.Float64), DataTypeKind.Float64);
    public static DataType String  => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.String), DataTypeKind.String);
    public static DataType Date    => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.Date), DataTypeKind.Date);
    public static DataType Time    => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.Time), DataTypeKind.Time);
    public static DataType Null  => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.Null), DataTypeKind.Null);
    public static DataType SameAsInput => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.SameAsInput), DataTypeKind.SameAsInput);

    // ==========================================
    // Complex Factories (Methods)
    // ==========================================
    /// <summary>
    /// Decimal
    /// </summary>
    /// <param name="precision"></param>
    /// <param name="scale"></param>
    /// <returns></returns>
    public static DataType Decimal(int precision=38, int scale=9) 
        => new(PolarsWrapper.NewDecimalType(precision, scale), DataTypeKind.Decimal);
    /// <summary>
    /// Categorical
    /// </summary>
    public static DataType Categorical 
        => new(PolarsWrapper.NewCategoricalType(), DataTypeKind.Categorical);
    /// <summary>
    /// 创建一个带具体精度和时区的 Datetime 类型。
    /// </summary>
    /// <param name="unit">精度 (ns, us, ms)</param>
    /// <param name="timeZone">时区字符串 (e.g. "Asia/Shanghai")，传 null 表示无时区 (Naive)</param>
    public static DataType Datetime(TimeUnit unit, string? timeZone = null)
    {
        // 调用你刚加的 Wrapper
        var handle = PolarsWrapper.NewDateTimeType((int)unit, timeZone);
        return new DataType(handle,DataTypeKind.Datetime);
    }
    /// <summary>
    /// Creates a Duration type. Default is Microseconds.
    /// Usage: DataType.Duration(TimeUnit.Nanoseconds)
    /// </summary>
    public static DataType Duration(TimeUnit unit = TimeUnit.Microseconds)
    {
        return new DataType(PolarsWrapper.NewDurationType((int)unit), DataTypeKind.Duration);
    }
    /// <summary>
    /// Creates a List type.
    /// Usage: DataType.List(DataType.Int32)
    /// </summary>
    public static DataType List(DataType innerType)
    {
        // Wrapper 负责调用 Rust 创建 List<inner>
        return new DataType(PolarsWrapper.NewListType(innerType.Handle), DataTypeKind.List);
    }
    public static DataType Struct(string[] names, DataType[] types)
    {
        // 提取 Handles
        var handles = Array.ConvertAll(types, t => t.Handle);
        
        // 调用新 Wrapper
        var h = PolarsWrapper.NewStructType(names, handles);
        
        return new DataType(h, DataTypeKind.Struct);
    }
}
