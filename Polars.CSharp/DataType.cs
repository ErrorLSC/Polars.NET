#pragma warning disable CS1591 // 缺少对公共可见类型或成员的 XML 注释
using System.Text.RegularExpressions;
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

    public int? Precision { get; private set; }
    public int? Scale { get; private set; }

    public TimeUnit? Unit { get; private set; }
    public string? TimeZone { get; private set; }

    // 修改构造函数：强制要求传入 Handle 和 Kind
    internal DataType(DataTypeHandle handle, DataTypeKind kind)
    {
        Handle = handle;
        
        // 1. 获取 Rust 端最权威的类型字符串
        //    例如: "datetime[ms, Asia/Shanghai]" 或 "duration[us]"
        _displayString = PolarsWrapper.GetDataTypeString(CloneHandle());

        // 2. 如果传入的 kind 是 Unknown (或者我们需要解析参数)，则重新解析
        if (kind == DataTypeKind.Unknown || 
            kind == DataTypeKind.Datetime || 
            kind == DataTypeKind.Duration ||
            kind == DataTypeKind.Decimal) // [新增]
        {
            var info = ExtractInfo(_displayString);
            
            Kind = (kind == DataTypeKind.Unknown) ? info.Kind : kind;

            // 填充属性
            if (info.Unit.HasValue) Unit = info.Unit;
            if (info.TimeZone != null) TimeZone = info.TimeZone;
            
            // [新增] 填充 Decimal 属性
            if (info.Precision.HasValue) Precision = info.Precision;
            if (info.Scale.HasValue) Scale = info.Scale;
        }
        else
        {
            Kind = kind;
        }
    }
    private string? _displayString;
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
    public override string ToString()
    {
        return _displayString ?? Kind.ToString();
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
    // Schema Parsing Logic
    // ==========================================
    /// <summary>
    /// Parse "list[i64]" or "list[list[str]]" like string
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
            using var innerType = Parse(innerStr);

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
    /// 解析 Struct 字符串，例如: "struct[a: i64, b: list[str], c: decimal[10, 2]]"
    /// </summary>
    private static DataType ParseStructType(string typeStr)
    {
        int openBracket = typeStr.IndexOf('[');
        int closeBracket = typeStr.LastIndexOf(']');

        // 1. 处理空 Struct
        if (openBracket == -1 || closeBracket <= openBracket)
        {
             return new DataType(
                 PolarsWrapper.NewStructType(Array.Empty<string>(), Array.Empty<DataTypeHandle>()), 
                 DataTypeKind.Struct
             );
        }

        // 2. 提取内容
        string content = typeStr.Substring(openBracket + 1, closeBracket - openBracket - 1);

        var names = new List<string>();
        
        // 使用 List<DataType> 而不是 List<DataTypeHandle>
        // 原因：Parse() 返回的是 DataType 对象。我们需要保持这个对象存活，直到我们将 Handle 移交给 Rust。
        // 如果我们只存 Handle 而丢弃 DataType 对象，在 Debug 模式下可能没事，
        // 但在 Release 模式下，GC 可能会在 Native 调用前回收 DataType 对象，触发 Finalizer 释放 Handle，导致 Crash。
        var tempTypes = new List<DataType>(); 

        // 3. 智能分割字段 (Bracket Aware Split)
        int depth = 0;
        int start = 0;
        for (int i = 0; i < content.Length; i++)
        {
            char c = content[i];
            
            if (c == '[') depth++;
            else if (c == ']') depth--;
            else if (c == ',' && depth == 0)
            {
                ParseStructField(content.Substring(start, i - start), names, tempTypes);
                start = i + 1;
            }
        }
        if (start < content.Length)
        {
            ParseStructField(content.Substring(start), names, tempTypes);
        }

        // 4. [关键] 提取 Handle 数组并调用
        // 这里的 Select 操作是轻量的
        var handles = tempTypes.Select(t => t.Handle).ToArray();

        // 调用新版 Wrapper (Rust 会接管这些 Handle 的所有权)
        var structHandle = PolarsWrapper.NewStructType(names.ToArray(), handles);
        
        // 5. 返回结果
        // tempTypes 里的 DataType 对象现在包含了无效的 Handle (因为 TransferOwnership 了)，
        // 它们随后会被 GC，这是安全的。
        return new DataType(structHandle, DataTypeKind.Struct);
    }

    // 辅助方法签名也要微调，接收 List<DataType>
    private static void ParseStructField(string segment, List<string> names, List<DataType> types)
    {
        segment = segment.Trim();
        if (string.IsNullOrEmpty(segment)) return;

        int colonIndex = segment.IndexOf(':');
        if (colonIndex == -1) 
        {
            names.Add("unknown");
            types.Add(Unknown);
            return;
        }

        string name = segment.Substring(0, colonIndex).Trim();
        string typeStr = segment.Substring(colonIndex + 1).Trim();

        // 递归解析
        types.Add(Parse(typeStr));
        names.Add(name);
    }

    private readonly record struct DataTypeInfo(
    DataTypeKind Kind, 
    TimeUnit? Unit = null, 
    string? TimeZone = null,
    int? Precision = null, // 新增
    int? Scale = null      // 新增
    );

    private static readonly Regex RegexDatetime = new Regex(@"^datetime(?:\[(ms|us|ns)(?:,\s*(.+))?\])?$", RegexOptions.Compiled);
    private static readonly Regex RegexDuration = new Regex(@"^duration(?:\[(ms|us|ns)\])?$", RegexOptions.Compiled);
    private static readonly Regex RegexDecimal  = new Regex(@"^decimal(?:\[(\d+),\s*(\d+)\])?$", RegexOptions.Compiled);

    /// <summary>
    /// Parse a schema type string (e.g., "i64", "date") into a DataType object.
    /// </summary>
    private static DataTypeInfo ExtractInfo(string str)
    {
        if (string.IsNullOrEmpty(str))
            return new DataTypeInfo(DataTypeKind.Unknown);
        str = str.Trim();

        // 简单类型直接匹配
        // 注意：这里调用下面的静态属性，会创建一个新的 Handle 并带上正确的 Kind
        switch(str)
        {
            case "bool": 
            case "boolean"  :return new DataTypeInfo(DataTypeKind.Boolean);
            case "i8": return new DataTypeInfo(DataTypeKind.Int8);
            case "i16": return new DataTypeInfo(DataTypeKind.Int16);
            case "i32": return new DataTypeInfo(DataTypeKind.Int32);
            case "i64": return new DataTypeInfo(DataTypeKind.Int64);
            case "u8": return new DataTypeInfo(DataTypeKind.UInt8);
            case "u16": return new DataTypeInfo(DataTypeKind.UInt16);
            case "u32": return new DataTypeInfo(DataTypeKind.UInt32);
            case "u64": return new DataTypeInfo(DataTypeKind.UInt64);
            case "f32": return new DataTypeInfo(DataTypeKind.Float32);
            case "f64": return new DataTypeInfo(DataTypeKind.Float64);
            case "str": case "String": case "Utf8": return new DataTypeInfo(DataTypeKind.String);
            case "date": return new DataTypeInfo(DataTypeKind.Date);
            case "time": return new DataTypeInfo(DataTypeKind.Time);
            case "cat": return new DataTypeInfo(DataTypeKind.Categorical);
            case "binary": return new DataTypeInfo(DataTypeKind.Binary);
            case "null": return new DataTypeInfo(DataTypeKind.Null);
        }
        // --- Datetime ---
        // 匹配: "datetime", "datetime[ms]", "datetime[us, Asia/Tokyo]"
        var dtMatch = RegexDatetime.Match(str);
        if (dtMatch.Success)
        {
            var unitStr = dtMatch.Groups[1].Value; // ms, us, ns
            var tz = dtMatch.Groups[2].Value;      // Timezone string or empty
            
            // 默认 us
            var unit = ParseTimeUnit(unitStr); 
            
            // 处理 Timezone: 如果为空字符串，传 null
            string? timeZone = string.IsNullOrWhiteSpace(tz) ? null : tz;

            return new DataTypeInfo(DataTypeKind.Datetime, unit, string.IsNullOrWhiteSpace(tz) ? null : tz);
        }

        // --- Duration ---
        // 匹配: "duration", "duration[ns]"
        var durMatch = RegexDuration.Match(str);
        if (durMatch.Success)
        {
            var unit = ParseTimeUnit(durMatch.Groups[1].Value);
            return new DataTypeInfo(DataTypeKind.Duration, unit);
        }

        // --- Decimal ---
        // 匹配: "decimal", "decimal[38, 9]"
        var decMatch = RegexDecimal.Match(str);
        if (decMatch.Success)
        {
            // 情况 1: 只有 "decimal" (无参数)
            // Polars 默认 decimal 是 (38, 9)，我们在解析阶段就定好，
            // 这样构造函数里拿到 info 就能直接赋值属性，不用再猜。
            if (!decMatch.Groups[1].Success) 
            {
                return new DataTypeInfo(DataTypeKind.Decimal, Precision: 38, Scale: 9);
            }

            // 情况 2: "decimal[10, 2]"
            int precision = int.Parse(decMatch.Groups[1].Value);
            int scale = int.Parse(decMatch.Groups[2].Value);
            
            // 使用命名参数初始化，代码更易读
            return new DataTypeInfo(DataTypeKind.Decimal, Precision: precision, Scale: scale);
        }
        // --- List ---
        if (str.StartsWith("list"))
        {
            return new DataTypeInfo(DataTypeKind.List);
        }
        if (str.StartsWith("struct"))
        {
            return new DataTypeInfo(DataTypeKind.Struct);
        }
        if (str.StartsWith("datetime")) return new DataTypeInfo(DataTypeKind.Datetime, Unit: TimeUnit.Microseconds);
        if (str.StartsWith("duration")) return new DataTypeInfo(DataTypeKind.Duration, Unit: TimeUnit.Microseconds);
        if (str.StartsWith("decimal")) return new DataTypeInfo(DataTypeKind.Decimal, Precision: 38, Scale: 9);

        return new DataTypeInfo(DataTypeKind.Unknown);
    }
    private static TimeUnit ParseTimeUnit(string s)
    {
        return s switch
        {
            "ns" => TimeUnit.Nanoseconds,
            "ms" => TimeUnit.Milliseconds,
            "us" => TimeUnit.Microseconds,
            "μs" => TimeUnit.Microseconds,
            _ => TimeUnit.Microseconds // 默认
        };
    }
    public static DataType Parse(string str)
    {
        // 1. 先提取信息
        var info = ExtractInfo(str);

        // 2. 根据信息调用对应的 Native 工厂 (创建 Handle)
        return info.Kind switch
        {
            DataTypeKind.Datetime => Datetime(info.Unit ?? TimeUnit.Microseconds, info.TimeZone),
            DataTypeKind.Duration => Duration(info.Unit ?? TimeUnit.Microseconds),
            
            // List 和 Struct 比较特殊，ExtractInfo 可能只返回了 Kind，
            // 需要用原来的递归逻辑去创建
            DataTypeKind.List => ParseListType(str),
            DataTypeKind.Struct => ParseStructType(str),
            DataTypeKind.Decimal => Decimal(
            info.Precision ?? 38, // 兜底，虽然 ExtractInfo 应该已经处理了
            info.Scale ?? 9),

            // 基础类型
            _ => new DataType(PolarsWrapper.NewPrimitiveType((int)MapKindToPlType(info.Kind)), info.Kind)
        };
    }

    // 辅助：Kind -> PlDataType 枚举转换 (用于基础类型)
    private static PlDataType MapKindToPlType(DataTypeKind kind)
    {
        // ... 简单的 switch case ...
        // 比如 DataTypeKind.Int32 => PlDataType.Int32
        return (PlDataType)(int)kind; // 假设枚举值是对齐的，不然手写 switch
    }

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
    public static DataType SameAsInput => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.SameAsInput), DataTypeKind.Unknown);

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
