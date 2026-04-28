#pragma warning disable CS1591
using System.Collections.Concurrent;
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
    internal static DataType CreateFromHandle(DataTypeHandle handle)
    {
        var kind = (DataTypeKind)PolarsWrapper.GetDataTypeKind(handle);

        if (kind != DataTypeKind.Extension)
        {
            return new DataType(handle, kind);
        }

        string extName = PolarsWrapper.DataTypeGetExtensionName(handle);
        string? metadata = PolarsWrapper.DataTypeGetExtensionMetadata(handle);
        DataTypeHandle storageHandle = PolarsWrapper.DataTypeGetExtensionStorage(handle);
        
        DataType storage = CreateFromHandle(storageHandle);

        if (ExtensionRegistry.TryGetResolution(extName, out var factory, out bool asStorage))
        {
            if (asStorage)
            {
                handle.Dispose(); 
                return storage;
            }

            if (factory is not null)
            {
                BaseExtension.AmbientHandle.Value = handle;
                try
                {
                    return factory(storage, metadata);
                }
                finally
                {
                    BaseExtension.AmbientHandle.Value = null;
                }
            }
        }

        BaseExtension.AmbientHandle.Value = handle;
        try
        {
            return new UnknownExtension(extName, storage, metadata);
        }
        finally
        {
            BaseExtension.AmbientHandle.Value = null;
        }
    }
    /// <summary>
    /// Return underlying Categories for categorical type, for other types returns null
    /// </summary>
    public Categories? Categories
    {
        get
        {
            if (Kind != DataTypeKind.Categorical) 
                return null;

            var handle = PolarsWrapper.GetCategories(Handle);
            
            if (handle == null || handle.IsInvalid) 
                return null;

            return new Categories(handle);
        }
    }

    /// <summary>
    /// Return underlying FrozenCategories for enum type, for other types returns null
    /// </summary>
    public FrozenCategories? EnumCategories
    {
        get
        {
            if (Kind != DataTypeKind.Enum) 
                return null;

            var handle = PolarsWrapper.GetEnumCategories(Handle);
            
            if (handle == null || handle.IsInvalid) 
                return null;

            return new FrozenCategories(handle);
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

    public override bool Equals(object? obj) => Equals(obj as DataType);

    public bool Equals(DataType? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;

        return PolarsWrapper.DataTypeEq(this.Handle,other.Handle);
    }

    public override int GetHashCode()
        => ToString().GetHashCode();

    public static bool operator ==(DataType? left, DataType? right)
    {
        if (left is null) return right is null;
        return left.Equals(right);
    }

    public static bool operator !=(DataType? left, DataType? right) => !(left == right);

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
    public bool IsFloat => Kind switch
    {
        DataTypeKind.Float16 or DataTypeKind.Float32 or DataTypeKind.Float64 => true,
        _ => false
    };
    public bool IsDecimal => Kind switch
    {
        DataTypeKind.Decimal => true,
        _ => false
    };
    public bool IsExtension => Kind switch
    {
        DataTypeKind.Extension => true,
        _ => false
    };
    public bool IsNested => Kind switch
    {
        DataTypeKind.List or DataTypeKind.Array or DataTypeKind.Struct => true,
        _ => false
    };
    public bool IsTemporal => Kind switch
    {
        DataTypeKind.Duration or DataTypeKind.Date or DataTypeKind.Datetime or
        DataTypeKind.Time => true,
        _ => false
    };
    public bool IsSignedInteger => Kind switch
    {
        DataTypeKind.Int8 or DataTypeKind.Int16 or DataTypeKind.Int32 or DataTypeKind.Int64
        or DataTypeKind.Int128 => true,
        _ => false
    };
    public bool IsUnsignedInteger => Kind switch
    {
        DataTypeKind.UInt8 or DataTypeKind.UInt16 or DataTypeKind.UInt32 or DataTypeKind.UInt64
        or DataTypeKind.UInt128 => true,
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
    public static DataType Binary  => new(PolarsWrapper.NewPrimitiveType((int)PlDataType.Binary), DataTypeKind.Binary);
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
    public static DataType Categorical(Categories? categories=null)
    {
        Categories realCate = categories ?? Categories.Global();
        return new(PolarsWrapper.NewCategoricalType(realCate.Handle), DataTypeKind.Categorical);
    }
    public static DataType Categorical(string categories)
    {
        Categories realCate = new(name:categories);
        return new(PolarsWrapper.NewCategoricalType(realCate.Handle), DataTypeKind.Categorical);
    }
    /// <summary>
    /// Create a Enum type
    /// </summary>
    /// <param name="categories"></param>
    /// <returns></returns>
    public static DataType Enum(FrozenCategories categories) => new(PolarsWrapper.NewEnumType(categories.Handle),DataTypeKind.Enum);
    public static DataType Enum(Series categories)
    {
        FrozenCategories cate = new(categories.ToArray<string>());
        return Enum(cate);
    }
    public static DataType Enum(Categories categories) => Enum(categories.Freeze());
    public static DataType Enum<T>() where T : struct, Enum
    {
        string[] names = System.Enum.GetNames(typeof(T));
        FrozenCategories cate = new(names);
        return Enum(cate);
    }
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
    /// Create an Extension data type
    /// </summary>
    /// <param name="name">The registered name of the extension type (e.g. "geoarrow.wkb")</param>
    /// <param name="inner">The physical storage data type</param>
    /// <param name="metadata">Optional metadata string</param>
    public static DataType Extension(string name, DataType inner, string? metadata = null)
    {
        var h = PolarsWrapper.NewExtensionType(name, inner.Handle, metadata);
        return new DataType(h, DataTypeKind.Extension); 
    }
    /// <summary>
    /// Convert this DataType to a DataTypeExpr literal.
    /// Equivalent to Python's polars.DataType.to_dtype_expr()
    /// </summary>
    public DataTypeExpr ToDataTypeExpr()
        => new(PolarsWrapper.DataTypeExprFromDataType(this.Handle));
    
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

            DictionaryType dict => Categorical(Categories.Random()),

            MapType map => List(Struct(
                ["key", "value"], 
                [FromArrowType(map.KeyField.DataType), FromArrowType(map.ValueField.DataType)]
            )),

            _ => throw new NotSupportedException($"ArrowType {arrowType.GetType().Name} is not supported yet.")
        };
    }
}

public class Categories : IDisposable,IEquatable<Categories>
{
    internal CategoriesHandle Handle { get; }
    private bool _disposed;

    public Categories(string? name=null, string? nameSpace= "",CategoricalPhysical physical = CategoricalPhysical.U32)
    {
        Handle = PolarsWrapper.CategoriesNew(name,nameSpace,physical.ToNative());
    }

    internal Categories(CategoriesHandle handle)
    {
        Handle = handle;
    }

    public static Categories Random(string nameSpace = "", CategoricalPhysical physical = CategoricalPhysical.U32)
    {
        var handle = PolarsWrapper.CategoriesRandom(nameSpace,physical.ToNative());
        return new Categories(handle);
    }
    public string Name() => PolarsWrapper.CategoriesGetName(Handle);
    public string NameSpace() => PolarsWrapper.CategoriesGetNameSpace(Handle);
    public bool IsGlobal() => PolarsWrapper.CategoriesIsGlobal(Handle);
    public CategoricalPhysical Physical() => (CategoricalPhysical)PolarsWrapper.CategoriesPhysical(Handle);
    public static Categories Global() => new(PolarsWrapper.CategoriesGlobal());

    public FrozenCategories Freeze() => new(PolarsWrapper.CategoriesFreeze(Handle));

    public ulong Hash
    {
        get
        {
            return PolarsWrapper.CategoriesHash(Handle);
        }
    }
    public void Dispose()
    {
        if (!_disposed)
        {
            Handle?.Dispose();
            _disposed = true;
        }
        GC.SuppressFinalize(this);
    }
    public bool Equals(Categories? other)
    {
        if (other is null) return false;
        
        if (ReferenceEquals(this, other)) return true;
        
        return this.Hash == other.Hash;
    }

    public override bool Equals(object? obj) => Equals(obj as Categories);

    public override int GetHashCode() => Hash.GetHashCode();

    public static bool operator ==(Categories? left, Categories? right)
    {
        if (ReferenceEquals(left, right)) return true;
        if (left is null) return false;
        return left.Equals(right);
    }

    public static bool operator !=(Categories? left, Categories? right)
    {
        return !(left == right);
    }
}

public class FrozenCategories : IDisposable,IEquatable<FrozenCategories>
{
    internal FrozenCategoriesHandle Handle { get; }
    private bool _disposed;

    public FrozenCategories(string[] categories)
    {
        Handle = PolarsWrapper.FrozenCategoriesNew(categories);
    }

    internal FrozenCategories(FrozenCategoriesHandle handle)
    {
        Handle = handle;
    }

    public string[] GetCategories() => new Series(PolarsWrapper.FrozenCategoriesGetCategories(Handle)).ToArray<string>();
    public CategoricalPhysical Physical() => (CategoricalPhysical)PolarsWrapper.FrozenCategoriesPhysical(Handle);

    public ulong Hash
    {
        get
        {
            return PolarsWrapper.FrozenCategoriesHash(Handle);
        }
    }
    public void Dispose()
    {
        if (!_disposed)
        {
            Handle?.Dispose();
            _disposed = true;
        }
        GC.SuppressFinalize(this);
    }
    public bool Equals(FrozenCategories? other)
    {
        if (other is null) return false;
        
        if (ReferenceEquals(this, other)) return true;
        
        return this.Hash == other.Hash;
    }

    public override bool Equals(object? obj) => Equals(obj as FrozenCategories);

    public override int GetHashCode() => Hash.GetHashCode();

    public static bool operator ==(FrozenCategories? left, FrozenCategories? right)
    {
        if (ReferenceEquals(left, right)) return true;
        if (left is null) return false;
        return left.Equals(right);
    }

    public static bool operator !=(FrozenCategories? left, FrozenCategories? right)
    {
        return !(left == right);
    }
}

public abstract class BaseExtension(string name, DataType storage, string? metadata = null) : DataType(ResolveHandle(name, storage, metadata), DataTypeKind.Extension)
{
    internal static readonly AsyncLocal<DataTypeHandle?> AmbientHandle = new();

    public string ExtensionName { get; } = name;
    public DataType Storage { get; } = storage;
    public string? Metadata { get; } = metadata;

    private static DataTypeHandle ResolveHandle(string name, DataType storage, string? metadata)
    {
        if (AmbientHandle.Value is { } existingHandle)
        {
            AmbientHandle.Value = null;
            return existingHandle;
        }

        return PolarsWrapper.NewExtensionType(name, storage.Handle, metadata);
    }
}

public sealed class UnknownExtension : BaseExtension
{
    internal UnknownExtension(string name, DataType storage, string? metadata) 
        : base(name, storage, metadata)
    {
    }
}

public delegate BaseExtension ExtensionFactory(DataType storage, string? metadata);

public static class ExtensionRegistry
{
    private static readonly ConcurrentDictionary<string, ExtensionFactory?> _registry = new();

    public static void RegisterExtensionType<T>(string extName, ExtensionFactory factory) where T : BaseExtension
    {
        if (!_registry.TryAdd(extName, factory))
        {
            throw new ArgumentException($"Extension type '{extName}' is already registered.");
        }
    }

    public static void RegisterExtensionTypeAsStorage(string extName)
    {
        if (!_registry.TryAdd(extName, null))
        {
            throw new ArgumentException($"Extension type '{extName}' is already registered.");
        }
    }

    public static void UnregisterExtensionType(string extName)
    {
        _registry.TryRemove(extName, out _);
    }

    internal static bool TryGetResolution(string extName, out ExtensionFactory? factory, out bool asStorage)
    {
        if (_registry.TryGetValue(extName, out factory))
        {
            asStorage = factory is null;
            return true;
        }

        asStorage = false;
        return false;
    }
}