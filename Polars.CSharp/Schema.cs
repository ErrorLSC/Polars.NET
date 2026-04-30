#pragma warning disable CS1591
using System.Collections.Frozen;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Polars.NET.Core;

namespace Polars.CSharp;

/// <summary>
/// Represents a Polars Schema (Name -> DataType mapping).
/// </summary>
public class PolarsSchema : IDisposable,IPolarsSchema, IEquatable<PolarsSchema>
{
    internal SchemaHandle Handle { get; private set; }
    private bool _disposed;

    /// <summary>
    /// Internal constructor: Wrap an existing handle (e.g. from Rust return).
    /// </summary>
    internal PolarsSchema(SchemaHandle handle)
    {
        Handle = handle;
    }

    /// <summary>
    /// Create a new empty Schema.
    /// </summary>
    public PolarsSchema()
    {
        Handle = PolarsWrapper.SchemaCreate();
    }

    /// <summary>
    /// Create a Schema from a Dictionary.
    /// </summary>
    public static PolarsSchema From(IReadOnlyDictionary<string, DataType> fields)
    {
        var schema = new PolarsSchema();
        foreach (var kvp in fields)
        {
            schema.Add(kvp.Key, kvp.Value);
        }
        return schema;
    }

    /// <summary>
    /// Create a schema from a collection of named fields.
    /// </summary>
    public static PolarsSchema From(IEnumerable<(string Name, DataType Type)> fields)
    {
        var schema = new PolarsSchema();
        if (fields != null)
        {
            foreach (var field in fields)
            {
                schema.Add(field.Name, field.Type);
            }
        }
        return schema;
    }

    /// <summary>
    /// Create a Schema directly from a .NET type.
    /// </summary>
    public static PolarsSchema From<T>() => From(typeof(T));

    /// <summary>
    /// Create a Schema directly from a System.Type.
    /// </summary>
    public static PolarsSchema From(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);
        SchemaHandle handle = PolarsWrapper.NewSchemaFromType(type);
        return new PolarsSchema(handle);
    }

    /// <summary>
    /// Add a field to the schema.
    /// </summary>
    /// <param name="name">Column name.</param>
    /// <param name="dtype">Column data type.</param>
    /// <returns>The schema instance (Fluent API).</returns>
    public PolarsSchema Add(string name, DataType dtype)
    {
        ArgumentNullException.ThrowIfNull(dtype);
        
        PolarsWrapper.SchemaAddField(Handle, name, dtype.Handle);
        return this;
    }

    /// <summary>
    /// Converts the Schema to a standard read-only dictionary.
    /// Fast creation, standard read performance.
    /// </summary>
    public IReadOnlyDictionary<string, DataType> ToDictionary()
    {
        if (Handle.IsInvalid) return new Dictionary<string, DataType>();

        ulong len = PolarsWrapper.GetSchemaLen(Handle);
        var result = new Dictionary<string, DataType>((int)len);

        for (ulong i = 0; i < len; i++)
        {
            PolarsWrapper.GetSchemaFieldAt(Handle, i, out string name, out DataTypeHandle dtHandle);
            result[name] = DataType.CreateFromHandle(dtHandle);
        }

        return result;
    }
    /// <summary>
    /// Converts the Schema to a FrozenDictionary.
    /// Slower creation time, but blazing fast read performance. Ideal for caching.
    /// </summary>
    public FrozenDictionary<string, DataType> ToFrozenDictionary()
    {
        if (Handle.IsInvalid) return FrozenDictionary<string, DataType>.Empty;

        ulong len = PolarsWrapper.GetSchemaLen(Handle);
        
        var pairs = new KeyValuePair<string, DataType>[len];

        for (ulong i = 0; i < len; i++)
        {
            PolarsWrapper.GetSchemaFieldAt(Handle, i, out string name, out DataTypeHandle dtHandle);
            pairs[i] = new KeyValuePair<string, DataType>(name, DataType.CreateFromHandle(dtHandle));
        }

        return pairs.ToFrozenDictionary(); 
    }
    IReadOnlyDictionary<string, IPolarsDataType> IPolarsSchema.ToDictionary()
    {
        return ToDictionary()
                    .ToDictionary(kvp => kvp.Key, kvp => (IPolarsDataType)kvp.Value);
    }
    /// <summary>
    /// Returns the schema as an ordered list of fields.
    /// </summary>
    public List<(string Name, DataType Type)> ToList()
    {
        if (Handle.IsInvalid) return [];

        ulong len = PolarsWrapper.GetSchemaLen(Handle);
        var result = new List<(string Name, DataType Type)>((int)len);

        for (ulong i = 0; i < len; i++)
        {
            PolarsWrapper.GetSchemaFieldAt(Handle, i, out string name, out DataTypeHandle dtHandle);
            result.Add((name, DataType.CreateFromHandle(dtHandle)));
        }

        return result;
    }

    public DataType this[string name]
    {
        get
        {
            ulong len = PolarsWrapper.GetSchemaLen(Handle);
            for (ulong i = 0; i < len; i++)
            {
                PolarsWrapper.GetSchemaFieldAt(Handle, i, out string fName, out DataTypeHandle dtHandle);
                if (fName == name)
                {
                    return DataType.CreateFromHandle(dtHandle);
                }
            }
            throw new KeyNotFoundException($"Column '{name}' not found in Schema.");
        }
    }
        
    // ==========================================
    // ColumnNames / Length / dtype
    // ==========================================
    public int Length => (int)PolarsWrapper.GetSchemaLen(Handle);

    public List<string> ColumnNames
    {
        get
        {
            ulong len = PolarsWrapper.GetSchemaLen(Handle);
            var list = new List<string>((int)len);
            for (ulong i = 0; i < len; i++)
            {
                PolarsWrapper.GetSchemaFieldAt(Handle, i, out string name, out _);
                list.Add(name);
            }
            return list;
        }
    }

    public List<string> Names => ColumnNames;
    /// <summary>
    /// Gets the list of data types for the columns in the schema.
    /// </summary>
    public List<DataType> DataTypes
    {
        get
        {
            // Get the total number of fields in the schema
            ulong len = PolarsWrapper.GetSchemaLen(Handle);
            var list = new List<DataType>((int)len);
            
            for (ulong i = 0; i < len; i++)
            {
                // Discard the column name, keep the DataTypeHandle
                PolarsWrapper.GetSchemaFieldAt(Handle, i, out _, out DataTypeHandle dtypeHandle);
                
                // Wrap the native handle in the high-level DataType API class
                list.Add(DataType.CreateFromHandle(dtypeHandle));
            }
            return list;
        }
    }

    IPolarsDataType IPolarsSchema.this[string name] 
        => this[name];
    
    /// <summary>
    /// Create an empty (n=0) or n-row null-filled (n>0) copy of the DataFrame.
    /// Returns a n-row null-filled DataFrame with an identical schema. n can be greater than the current number of rows in the DataFrame.
    /// </summary>
    /// <returns>An empty DataFrame with columns and types matching the schema.</returns>
    public DataFrame ToDataFrame(long length=0)
        => new(PolarsWrapper.DataFrameFromSchema(Handle, (uint)length));
    /// <summary>
    /// Create an empty copy of the current LazyFrame, with zero to ‘n’ rows.
    /// Returns a copy with an identical schema but no data.
    /// </summary>
    /// <returns>An empty DataFrame with columns and types matching the schema.</returns>
    public LazyFrame ToLazyFrame(long length=0)
    {
        using var df = new DataFrame(PolarsWrapper.DataFrameFromSchema(Handle, (uint)length));
        return df.Lazy();
    }

    // ==========================================
    // ToString
    // ==========================================
    public override string ToString()
    {
        if (Handle.IsInvalid) return "Schema: {}";

        var sb = new StringBuilder();
        sb.Append("Schema: {");
        
        ulong len = PolarsWrapper.GetSchemaLen(Handle);
        for (ulong i = 0; i < len; i++)
        {
            PolarsWrapper.GetSchemaFieldAt(Handle, i, out string name, out DataTypeHandle dtHandle);

            using var dt = DataType.CreateFromHandle(dtHandle); 
            sb.Append($"{name}: {dt.Kind}");

            if (i < len - 1) sb.Append(", ");
        }
        
        sb.Append('}');
        return sb.ToString();
    }
    /// <summary>
    /// Convert a Polars Schema to an Apache.Arrow.Schema.
    /// </summary>
    public Apache.Arrow.Schema ToArrowSchema()
    {
        var builder = new Apache.Arrow.Schema.Builder();

        foreach (var (columnName, polarsType) in ToDictionary())
        {
            IArrowType arrowType = polarsType.GetArrowType();
            
            Dictionary<string, string>? metadata = null;

            if (polarsType is BaseExtension ext)
            {
                metadata = new Dictionary<string, string>
                {
                    { "ARROW:extension:name", ext.ExtensionName }
                };

                if (ext.Metadata != null)
                {
                    metadata["ARROW:extension:metadata"] = ext.Metadata;
                }
            }

            var field = new Apache.Arrow.Field(columnName, arrowType, nullable: true, metadata);
            
            builder.Field(field);
        }

        return builder.Build();
    }
    /// <summary>
    /// Creates a Polars Schema from an Apache.Arrow.Schema.
    /// </summary>
    public static PolarsSchema FromArrowSchema(Apache.Arrow.Schema arrowSchema)
    {
        var polarsSchema = new PolarsSchema();

        foreach (var field in arrowSchema.FieldsList)
        {
            DataType polarsType;

            if (field.Metadata != null && field.Metadata.TryGetValue("ARROW:extension:name", out var extName))
            {
                field.Metadata.TryGetValue("ARROW:extension:metadata", out var extMetadata);
                
                var storageType = DataType.FromArrowType(field.DataType);

                if (ExtensionRegistry.TryGetResolution(extName, out var factory, out var asStorage))
                {
                    if (asStorage)
                    {
                        polarsType = storageType;
                    }
                    else
                    {
                        polarsType = factory!(storageType, extMetadata);
                    }
                }
                else
                {
                    polarsType = new UnknownExtension(extName, storageType, extMetadata);
                }
            }
            else
            {
                polarsType = DataType.FromArrowType(field.DataType);
            }

            polarsSchema.Add(field.Name, polarsType); 
        }

        return polarsSchema;
    }

    // ==========================================
    // Equality Members
    // ==========================================
    
    public override bool Equals(object? obj)
        => Equals(obj as PolarsSchema);

    public bool Equals(PolarsSchema? other)
    {
        if (ReferenceEquals(this, other)) return true;
        
        if (other is null || Handle.IsInvalid || other.Handle.IsInvalid) return false;

        if (Length != other.Length) return false;

        return ToList().SequenceEqual(other.ToList());
    }

    public override int GetHashCode()
    {
        if (Handle.IsInvalid) return 0;

        var hash = new HashCode();
        foreach (var (name, type) in ToList())
        {
            hash.Add(name);
            hash.Add(type);
        }
        return hash.ToHashCode();
    }

    public static bool operator ==(PolarsSchema? left, PolarsSchema? right)
    {
        if (left is null) return right is null;
        return left.Equals(right);
    }

    public static bool operator !=(PolarsSchema? left, PolarsSchema? right)
    {
        return !(left == right);
    }
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            Handle?.Dispose();
            _disposed = true;
        }
    }
}