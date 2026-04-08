#pragma warning disable CS1591
using Apache.Arrow;
using Apache.Arrow.Types;
using Polars.NET.Core;
using Polars.NET.Core.Arrow;

namespace Polars.CSharp;

/// <summary>
/// Represents a Polars data type. 
/// Wraps the underlying Rust DataType Handle and provides high-level metadata.
/// </summary>
public class DataType : IDisposable, IEquatable<DataType>,IPolarsDataType
{
    internal DataTypeHandle Handle { get; }
    
    /// <summary>
    /// Gets the high-level kind of this data type.
    /// </summary>
    public DataTypeKind Kind { get; }
    private string? _displayString;
    public int? Precision { get; private set; }
    public int? Scale { get; private set; }

    public TimeUnit? Unit { get; private set; }
    public string? TimeZone { get; private set; }

    private IReadOnlyList<(string Name, DataType Type)>? _structFields;

    /// <summary>
    /// If this is an Array type, returns the fixed width.
    /// Returns 0 if not an Array type.
    /// </summary>
    public long ArrayWidth => (long)PolarsWrapper.DataTypeGetArrayWidth(Handle);

    internal DataType(DataTypeHandle handle, DataTypeKind kind = DataTypeKind.Unknown)
    {
        Handle = handle;

        if (kind == DataTypeKind.Unknown)
        {
            Kind = (DataTypeKind)PolarsWrapper.GetDataTypeKind(Handle);
        }
        else
        {
            Kind = kind;
        }

        switch (Kind)
        {
            case DataTypeKind.Datetime:
                Unit = (TimeUnit?)PolarsWrapper.GetTimeUnit(Handle);
                TimeZone = PolarsWrapper.GetTimeZone(Handle);
                break;

            case DataTypeKind.Duration:
                Unit = (TimeUnit?)PolarsWrapper.GetTimeUnit(Handle);
                break;

            case DataTypeKind.Decimal:
                PolarsWrapper.GetDecimalInfo(Handle, out int p, out int s);
                Precision = p;
                Scale = s;
                break;
        }
    }
    
    /// <summary>
    /// Dispose the underlying DataTypeHandle.
    /// </summary>
    public void Dispose()
    {
        Handle?.Dispose();
        GC.SuppressFinalize(this); 
    }

    /// <summary>
    /// Output DataType string (e.g., "datetime[ms, Asia/Shanghai]")
    /// </summary>
    public override string ToString()
    {
        _displayString ??= PolarsWrapper.GetDataTypeString(Handle);
        return _displayString;
    }
    /// <summary>
    /// Return the inner type of list/array. Non-List/Array input will return null.
    /// </summary>
    public DataType? InnerType 
    {
        get 
        {
            if (Kind != DataTypeKind.List && Kind != DataTypeKind.Array) return null;

            var innerHandle = PolarsWrapper.GetInnerType(Handle);
            
            if (innerHandle.IsInvalid) return null;
            
            return new DataType(innerHandle); 
        }
    }

    /// <summary>
    /// Gets the fields of a Struct type. 
    /// Returns null if this DataType is not a Struct.
    /// </summary>
    public IReadOnlyList<(string Name, DataType Type)>? StructFields
    {
        get
        {
            if (Kind != DataTypeKind.Struct) return null;

            if (_structFields == null)
            {
                ulong len = PolarsWrapper.GetStructLen(Handle);
                var fields = new List<(string Name, DataType Type)>((int)len);

                for (ulong i = 0; i < len; i++)
                {
                    PolarsWrapper.GetStructField(Handle, i, out string name, out DataTypeHandle typeHandle);
                    
                    fields.Add((name, new DataType(typeHandle)));
                }

                _structFields = fields.AsReadOnly();
            }

            return _structFields;
        }
    }

    // =========================================================================
    // Value Equality Implementation
    // =========================================================================

    public override bool Equals(object? obj)
    {
        return Equals(obj as DataType);
    }

    public bool Equals(DataType? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;

        if (Kind != other.Kind) return false;

        switch (Kind)
        {
            case DataTypeKind.Datetime:
                return Unit == other.Unit && TimeZone == other.TimeZone;

            case DataTypeKind.Duration:
                return Unit == other.Unit;

            case DataTypeKind.Decimal:
                return Precision == other.Precision && Scale == other.Scale;

            case DataTypeKind.Array:
                if (ArrayWidth != other.ArrayWidth) return false;
                goto case DataTypeKind.List;

            case DataTypeKind.List:
                using (var myInner = InnerType)
                using (var otherInner = other.InnerType)
                {
                    if (myInner == null && otherInner == null) return true;
                    if (myInner == null || otherInner == null) return false;
                    return myInner.Equals(otherInner);
                }
            case DataTypeKind.Struct:
                var myFields = StructFields;
                var otherFields = other.StructFields;

                if (myFields == null && otherFields == null) return true;
                if (myFields == null || otherFields == null) return false;
                if (myFields.Count != otherFields.Count) return false;

                for (int i = 0; i < myFields.Count; i++)
                {
                    if (myFields[i].Name != otherFields[i].Name) return false;
                    
                    if (!myFields[i].Type.Equals(otherFields[i].Type)) return false;
                }
                return true;

            default:
                return true;
        }
    }

    public override int GetHashCode()
        => ToString().GetHashCode();

    public static bool operator ==(DataType? left, DataType? right)
    {
        if (left is null) return right is null;
        return left.Equals(right);
    }

    public static bool operator !=(DataType? left, DataType? right)
    {
        return !(left == right);
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
        DataTypeKind.Float32 or DataTypeKind.Float64 or DataTypeKind.Decimal or 
        DataTypeKind.Int128 or DataTypeKind.UInt128 or DataTypeKind.Float16=> true,
        _ => false
    };
    public bool IsInteger => Kind switch
    {
        DataTypeKind.Int8 or DataTypeKind.Int16 or DataTypeKind.Int32 or DataTypeKind.Int64 or
        DataTypeKind.UInt8 or DataTypeKind.UInt16 or DataTypeKind.UInt32 or DataTypeKind.UInt64 or
        DataTypeKind.Int128 or DataTypeKind.UInt128=> true,
        _ => false
    };

    // ==========================================
    // Primitive Factories (Static Properties)
    // ==========================================
    
    public static DataType Unknown => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.Unknown), DataTypeKind.Unknown);
    public static DataType Boolean => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.Boolean), DataTypeKind.Boolean);
    public static DataType Int8    => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.Int8), DataTypeKind.Int8);
    public static DataType Int16   => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.Int16), DataTypeKind.Int16);
    public static DataType Int32   => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.Int32), DataTypeKind.Int32);    
    public static DataType Int64   => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.Int64), DataTypeKind.Int64);
    public static DataType Int128   => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.Int128), DataTypeKind.Int128);
    public static DataType UInt8   => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.UInt8), DataTypeKind.UInt8);
    public static DataType UInt16  => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.UInt16), DataTypeKind.UInt16);
    public static DataType UInt32  => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.UInt32), DataTypeKind.UInt32);
    public static DataType UInt64  => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.UInt64), DataTypeKind.UInt64);
    public static DataType UInt128   => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.UInt128), DataTypeKind.UInt128);
    public static DataType Float16 => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.Float16), DataTypeKind.Float16);
    public static DataType Float32 => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.Float32), DataTypeKind.Float32);
    public static DataType Float64 => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.Float64), DataTypeKind.Float64);
    public static DataType String  => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.String), DataTypeKind.String);
    public static DataType Date    => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.Date), DataTypeKind.Date);
    public static DataType Time    => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.Time), DataTypeKind.Time);
    public static DataType Null  => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.Null), DataTypeKind.Null);
    public static DataType Binary  => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.Binary), DataTypeKind.Null);
    public static DataType SameAsInput => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.SameAsInput), DataTypeKind.SameAsInput);

    // ==========================================
    // Complex Factories (Methods)
    // ==========================================
    /// <summary>
    /// Create a Decimal type
    /// </summary>
    /// <param name="precision"></param>
    /// <param name="scale"></param>
    /// <returns></returns>
    public static DataType Decimal(int precision=38, int scale=9) 
        => new(PolarsWrapper.NewDecimalType(precision, scale), DataTypeKind.Decimal);
    /// <summary>
    /// Create a Categorical type
    /// </summary>
    public static DataType Categorical 
        => new(PolarsWrapper.NewCategoricalType(), DataTypeKind.Categorical);
    /// <summary>
    /// Create a datetime type with unit and timezone
    /// </summary>
    /// <param name="unit">precision (ns, us, ms)</param>
    /// <param name="timeZone">timezone string (e.g. "Asia/Shanghai")， null for no timezone (Naive)</param>
    public static DataType Datetime(TimeUnit unit, string? timeZone = null)
    {
        var handle = PolarsWrapper.NewDateTimeType((byte)unit, timeZone);
        return new DataType(handle,DataTypeKind.Datetime);
    }
    /// <summary>
    /// Creates a Duration type. Default is Microseconds.
    /// Usage: DataType.Duration(TimeUnit.Nanoseconds)
    /// </summary>
    public static DataType Duration(TimeUnit unit = TimeUnit.Microseconds)
        => new(PolarsWrapper.NewDurationType((byte)unit), DataTypeKind.Duration);
    /// <summary>
    /// Creates a List type.
    /// Usage: DataType.List(DataType.Int32)
    /// </summary>
    public static DataType List(DataType innerType)
        => new(PolarsWrapper.NewListType(innerType.Handle), DataTypeKind.List);
    /// <summary>
    /// Create a Fixed-Size List (Array) data type.
    /// <para>Example: DataType.Array(DataType.Int32, 3),DataType.Array(typeof(int), 3) </para>
    /// </summary>
    /// <param name="inner">The data type of the elements.</param>
    /// <param name="width">The fixed length of the array.</param>
    public static DataType Array(DataType inner, int width)
    {
        var h = PolarsWrapper.NewArrayType(inner.Handle, (ulong)width);
        return new DataType(h,DataTypeKind.Array);
    }
    public static DataType Struct(string[] names, DataType[] types)
    {
        var handles = System.Array.ConvertAll(types, t => t.Handle);
        
        var h = PolarsWrapper.NewStructType(names, handles);
        
        return new DataType(h, DataTypeKind.Struct);
    }
    /// <summary>
    /// Create a Struct DataType from a collection of named fields.
    /// Preserves the exact order of the fields, which is strictly required by Polars.
    /// </summary>
    public static DataType Struct(IEnumerable<(string Name, DataType Type)> fields)
    {
        var names = new List<string>();
        var handles = new List<DataTypeHandle>();

        foreach (var field in fields)
        {
            names.Add(field.Name);
            handles.Add(field.Type.Handle);
        }

        var h = PolarsWrapper.NewStructType([.. names], [.. handles]);
        return new DataType(h, DataTypeKind.Struct);
    }
    /// <summary>
    /// Create a Struct DataType explicitly using Tuples.
    /// <para>Example: <c>DataType.Struct(("Id", DataType.Int32), ("Name", DataType.String))</c></para>
    /// </summary>
    public static DataType Struct(params (string Name, DataType Type)[] fields)
        => Struct((IEnumerable<(string Name, DataType Type)>)fields);
    /// <summary>
    /// Convert this DataType to a DataTypeExpr literal.
    /// Equivalent to Python's polars.DataType.to_dtype_expr()
    /// </summary>
    public DataTypeExpr ToDataTypeExpr()
    {
        var h = PolarsWrapper.DataTypeExprFromDataType(this.Handle);
        return new DataTypeExpr(h);
    }
    public Type GetNetType() => ArrowTypeResolver.GetNetTypeFromArrowType(GetArrowType());
    public static DataType FromNetType<T>() => FromArrowType(ArrowTypeResolver.GetArrowTypeFromNetType(typeof(T)));
    /// <summary>
    /// Implicitly convert System.Type to Polars DataType.
    /// Example: DataType dt = typeof(int); // dt is now DataType.Int32
    /// </summary>
    public static implicit operator DataType(Type type) 
        => FromArrowType(ArrowTypeResolver.GetArrowTypeFromNetType(type));
    /// <summary>
    /// Get Apache Arrow Type
    /// </summary>
    public IArrowType GetArrowType() => ArrowFfiBridge.ImportDataType(Handle);
    /// <summary>
    /// Generate DataType from ArrowType
    /// </summary>
    /// <param name="arrowType"></param>
    /// <returns></returns>
    /// <exception cref="NotSupportedException"></exception>
    public static DataType FromArrowType(IArrowType arrowType)
    {
        return arrowType switch
        {
            Int8Type => Int8,
            Int16Type => Int16,
            Int32Type => Int32,
            Int64Type => Int64,
            UInt8Type => UInt8,
            UInt16Type => UInt16,
            UInt32Type => UInt32,
            UInt64Type => UInt64,
            HalfFloatType => Float16,
            FloatType => Float32,
            DoubleType => Float64,
            BooleanType => Boolean,

            Decimal128Type d => Decimal(d.Precision, d.Scale),
            Decimal256Type d => Decimal(d.Precision, d.Scale),

            StringType or StringViewType or LargeStringType => String,
            BinaryType or BinaryViewType or LargeBinaryType => Binary,

            Date32Type => Date,
            Time64Type => Time,
            TimestampType t => Datetime(
                t.Unit switch
                {
                    Apache.Arrow.Types.TimeUnit.Microsecond => TimeUnit.Microseconds,
                    Apache.Arrow.Types.TimeUnit.Millisecond => TimeUnit.Milliseconds, 
                    Apache.Arrow.Types.TimeUnit.Nanosecond => TimeUnit.Nanoseconds, 
                    _ => TimeUnit.Microseconds
                }, 
                t.Timezone
            ),
            DurationType d => Duration(
                d.Unit switch { 
                    Apache.Arrow.Types.TimeUnit.Microsecond => TimeUnit.Microseconds,
                    Apache.Arrow.Types.TimeUnit.Millisecond => TimeUnit.Milliseconds, 
                    Apache.Arrow.Types.TimeUnit.Nanosecond => TimeUnit.Nanoseconds, 
                    _ => TimeUnit.Microseconds
                }
            ),

            ListType l => List(FromArrowType(l.ValueDataType)),
            LargeListType l => List(FromArrowType(l.ValueDataType)),
            FixedSizeListType l => Array(FromArrowType(l.ValueDataType), l.ListSize),
            
            StructType s => Struct(
                [.. s.Fields.Select(f => f.Name)],
                [.. s.Fields.Select(f => FromArrowType(f.DataType))]
            ),

            DictionaryType dict => Categorical,

            MapType map => List(Struct(
                ["key", "value"], 
                [FromArrowType(map.KeyField.DataType), FromArrowType(map.ValueField.DataType)]
            )),

            _ => throw new NotSupportedException($"ArrowType {arrowType.GetType().Name} is not supported yet.")
        };
    }
}
