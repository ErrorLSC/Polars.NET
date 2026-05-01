#pragma warning disable CS1591
using System.Collections;
using System.Collections.Frozen;
using System.Text;
using Apache.Arrow.Types;
using Polars.NET.Core;

namespace Polars.CSharp;

/// <summary>
/// Represents a Polars Schema (Name -> DataType mapping).
/// </summary>
public class PolarsSchema : IDisposable,IPolarsSchema, IEquatable<PolarsSchema>,IReadOnlyDictionary<string, DataType>, IEnumerable<Field>
{
    internal SchemaHandle Handle { get; private set; }
    private bool _disposed;
    private IReadOnlyList<Field>? _fields;

    private IReadOnlyList<Field> GetFields()
    {
        if (_fields != null) return _fields;

        if (Handle.IsInvalid)
        {
            _fields = [];
            return _fields;
        }

        ulong len = PolarsWrapper.GetSchemaLen(Handle);
        var fields = new Field[len];
        for (ulong i = 0; i < len; i++)
        {
            PolarsWrapper.GetSchemaFieldAt(Handle, i, out string name, out DataTypeHandle dtHandle);
            fields[i] = new Field(name, DataType.CreateFromHandle(dtHandle));
        }
        _fields = fields;
        return _fields;
    }

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
        PolarsWrapper.SchemaAddField(Handle, name, dtype.Handle);
        _fields = null; 
        return this;
    }

    // =========================
    // IReadOnlyDictionary<string, DataType>
    // =========================
    public IEnumerable<string> Keys => GetFields().Select(f => f.Name);
    public IEnumerable<DataType> Values => GetFields().Select(f => f.DataType);
    public int Count => (int)PolarsWrapper.GetSchemaLen(Handle);

    public bool ContainsKey(string key) => GetFields().Any(f => f.Name == key);

    public bool TryGetValue(string key, out DataType value)
    {
        var field = GetFields().FirstOrDefault(f => f.Name == key);
        if (field.Name != null)
        {
            value = field.DataType;
            return true;
        }
        value = default!;
        return false;
    }
    /// <summary>
    /// Converts the Schema to a FrozenDictionary.
    /// Slower creation time, but blazing fast read performance. Ideal for caching.
    /// </summary>
    public FrozenDictionary<string, DataType> ToFrozenDictionary()
        => GetFields().ToFrozenDictionary(f => f.Name, f => f.DataType);
    /// <summary>
    /// Returns the schema as an ordered list of fields.
    /// </summary>
    public IReadOnlyList<(string Name, DataType Type)> ToList()
        => [.. GetFields().Select(f => (f.Name, f.DataType))];

    public DataType this[string name]
    {
        get
        {
            if (TryGetValue(name, out var value))
                return value;
            throw new KeyNotFoundException($"Column '{name}' not found in Schema.");
        }
    }

    IEnumerator<KeyValuePair<string, DataType>> IEnumerable<KeyValuePair<string, DataType>>.GetEnumerator()
    {
        foreach (var f in GetFields())
            yield return new KeyValuePair<string, DataType>(f.Name, f.DataType);
    }

    // =========================
    // IEnumerable<Field>
    // =========================
    public IEnumerator<Field> GetEnumerator() => GetFields().GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
      
    // ==========================================
    // ColumnNames / Length / dtype
    // ==========================================
    public int Len() => Count;
    public IReadOnlyList<string> ColumnNames => [.. Keys];

    public IReadOnlyList<string> Names => ColumnNames;
    /// <summary>
    /// Gets the list of data types for the columns in the schema.
    /// </summary>
    public IReadOnlyList<DataType> DataTypes => [.. Values];

    IPolarsDataType IReadOnlyDictionary<string, IPolarsDataType>.this[string key]
    {
        get => this[key]; 
    }
    IEnumerable<IPolarsDataType> IReadOnlyDictionary<string, IPolarsDataType>.Values
        => Values.Select(dt => (IPolarsDataType)dt);
    bool IReadOnlyDictionary<string, IPolarsDataType>.TryGetValue(string key, out IPolarsDataType value)
    {
        bool found = TryGetValue(key, out DataType dt);
        value = found ? dt : default!;
        return found;
    }

    bool IReadOnlyDictionary<string, IPolarsDataType>.ContainsKey(string key)
        => ContainsKey(key);

    int IReadOnlyCollection<KeyValuePair<string, IPolarsDataType>>.Count => Count;

    IEnumerator<KeyValuePair<string, IPolarsDataType>> IEnumerable<KeyValuePair<string, IPolarsDataType>>.GetEnumerator()
    {
        foreach (var field in GetFields())
            yield return new KeyValuePair<string, IPolarsDataType>(field.Name, field.DataType);
    }
    
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

        foreach (var (columnName, polarsType) in this)
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
        if (other is null || Count != other.Count) return false;
        return GetFields().SequenceEqual(other.GetFields());
    }

    public override int GetHashCode()
    {
        var hash = new HashCode();
        foreach (var f in GetFields())
        {
            hash.Add(f.Name);
            hash.Add(f.DataType);
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