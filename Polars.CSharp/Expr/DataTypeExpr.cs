#pragma warning disable CS1591 
using Polars.NET.Core;

namespace Polars.CSharp;

/// <summary>
/// A lazily instantiated DataType that can be used in an Expr.
/// This expression is made to represent a DataType that can be used to reference a datatype in a lazy context.
/// </summary>
public partial class DataTypeExpr : IDisposable
{
    internal DataTypeExprHandle Handle { get; }

    internal DataTypeExpr(DataTypeExprHandle handle)
    {
        Handle = handle;
    }

    internal DataTypeExprHandle CloneHandle() => PolarsWrapper.DataTypeExprClone(Handle);
    /// <summary>
    /// Clone DataTypeExpr
    /// </summary>
    /// <returns></returns>
    public DataTypeExpr Clone() => new(CloneHandle());
    /// <summary>
    /// Materialize the DataTypeExpr in a specific context.
    /// </summary>
    /// <param name="schema"></param>
    /// <returns></returns>
    public DataType CollectDataType(PolarsSchema schema)
    {
        if (schema.Length == 0)
        {
            throw new ArgumentException("Polars engine requires a non-empty schema to resolve expressions.", nameof(schema));
        }
        return new(PolarsWrapper.DataTypeExprIntoDataType(CloneHandle(),schema.Handle));
    }
    /// <summary>
    /// Materialize the DataTypeExpr using a DataFrame's schema as context.
    /// </summary>
    public DataType CollectDataType(DataFrame df)
    {
        using var schema = df.Schema; 
        return CollectDataType(schema);
    }

    /// <summary>
    /// Materialize the DataTypeExpr using a LazyFrame's schema as context.
    /// </summary>
    public DataType CollectDataType(LazyFrame lf)
    {
        using var schema = lf.Schema; 
        return CollectDataType(schema);
    }
    
    /// <summary>
    /// Convert DataTypeExpr into DataType Literal.
    /// Returns null if the expression is not a literal (e.g. SelfDtype).
    /// </summary>
    /// <returns>A DataType instance or null.</returns>
    public DataType? ToLiteral()
    {
        var handle = PolarsWrapper.DataTypeExprIntoLiteral(this.CloneHandle());
        
        if (handle.IsInvalid)
        {
            handle.Dispose();
            return null;
        }
        return new DataType(handle);
    }
    /// <summary>
    /// Get a default value of a specific type.
    /// <para>
    /// Integers and floats are their zero value as default, unless otherwise specified</para>
    /// <para>Temporals are a physical zero as default</para>
    /// <para>Decimal is zero as default</para>
    /// <para>String and Binary are an empty string</para>
    /// <para>List is an empty list, unless otherwise specified</para>
    /// <para>Array is the inner default value repeated over the shape</para>
    /// <para>Struct is the inner default value for all fields</para>
    /// <para>Enum is the first category if it exists</para>
    /// <para>Null, Object and Categorical are null.</para>
    /// </summary>
    /// <param name="n">Number of types you want the value</param>
    /// <param name="numericToOne">Use 1 instead of 0 as the default value for numeric types</param>
    /// <param name="numListValues">The amount of values a list contains</param>
    public Expr DefaultValue(int n = 1, bool numericToOne = false, int numListValues = 0)
        => new(PolarsWrapper.DataTypeExprDefaultValue(CloneHandle(), n, numericToOne, numListValues));

    /// <summary>
    /// Get a formatted version of the output DataType.
    /// </summary>
    public Expr Display() => new(PolarsWrapper.DataTypeExprDisplay(CloneHandle()));

    /// <summary>
    /// Get the inner DataType of a List or Array.
    /// </summary>
    public DataTypeExpr InnerDataType() => new(PolarsWrapper.DataTypeExprInnerDtype(CloneHandle()));
    /// <summary>
    /// Get whether the output DataType is matches a certain selector.
    /// </summary>
    /// <param name="selector"></param>
    /// <returns></returns>
    public Expr Matches(Selector selector) =>new(PolarsWrapper.DataTypeExprMatches(CloneHandle(), selector.CloneHandle()));

    /// <summary>
    /// Get the DataType wrapped in a list.
    /// </summary>
    /// <returns></returns>
    public DataTypeExpr WrapInList() => new(PolarsWrapper.DataTypeExprWrapInList(CloneHandle()));

    /// <summary>
    /// Get the DataType wrapped in an array.
    /// </summary>
    /// <param name="width">Array Width</param>
    /// <returns></returns>
    public DataTypeExpr WrapInArray(int width) => new(PolarsWrapper.DataTypeExprWrapInArray(CloneHandle(), width));
    /// <summary>
    /// Get the signed integer version of the same bitsize.
    /// </summary>
    /// <returns></returns>
    public DataTypeExpr ToSignedInteger() => new(PolarsWrapper.DataTypeExprIntToSigned(CloneHandle()));
    /// <summary>
    /// Get the unsigned integer version of the same bitsize.
    /// </summary>
    /// <returns></returns>
    public DataTypeExpr ToUnsignedInteger() => new(PolarsWrapper.DataTypeExprIntToUnsigned(CloneHandle()));
    // ==========================================
    // Namespaces 
    // ==========================================
    public ListNameSpace List => new(this);
    public ArrayNameSpace Array => new(this);
    public StructNameSpace Struct => new(this);

    // --- List Namespace ---
    public readonly struct ListNameSpace
    {
        private readonly DataTypeExpr _parent;
        internal ListNameSpace(DataTypeExpr parent) => _parent = parent;
        /// <summary>
        /// Get the inner DataType of list.
        /// </summary>
        /// <returns></returns>
        public DataTypeExpr InnerDataType() => new(PolarsWrapper.DataTypeExprListInnerDtype(_parent.CloneHandle()));
    }

    // --- Array Namespace ---
    public readonly struct ArrayNameSpace
    {
        private readonly DataTypeExpr _parent;
        internal ArrayNameSpace(DataTypeExpr parent) => _parent = parent;
        /// <summary>
        /// Get the inner DataType of array.
        /// </summary>
        /// <returns></returns>
        public DataTypeExpr InnerDataType() => new(PolarsWrapper.DataTypeExprArrayInnerDtype(_parent.CloneHandle()));
        /// <summary>
        /// Get the array width.
        /// </summary>
        /// <returns></returns>
        public Expr Width() => new(PolarsWrapper.DataTypeExprArrayWidth(_parent.CloneHandle()));
        /// <summary>
        /// Get the array shape.
        /// </summary>
        /// <returns></returns>
        public Expr Shape() => new(PolarsWrapper.DataTypeExprArrayShape(_parent.CloneHandle()));
    }

    // --- Struct Namespace ---
    public readonly struct StructNameSpace
    {
        private readonly DataTypeExpr _parent;
        internal StructNameSpace(DataTypeExpr parent) => _parent = parent;
        /// <summary>
        /// Get the field names in a struct as a list.
        /// </summary>
        /// <returns></returns>
        public Expr FieldNames() => new(PolarsWrapper.DataTypeExprStructFieldNames(_parent.CloneHandle()));
        /// <summary>
        /// Get the DataType of field with a specific field name.
        /// </summary>
        /// <returns></returns>
        public DataTypeExpr FieldDataType(string name) => new(PolarsWrapper.DataTypeExprStructFieldDtypeByName(_parent.CloneHandle(), name));
        /// <summary>
        /// Get the DataType of field with a specific field index.
        /// </summary>
        /// <returns></returns>
        public DataTypeExpr FieldDataType(long index) => new(PolarsWrapper.DataTypeExprStructFieldDtypeByIndex(_parent.CloneHandle(), index));
        /// <summary>
        /// Get the DataType of field with a specific field index.
        /// </summary>
        public DataTypeExpr this[long index] => FieldDataType(index);
        /// <inheritdoc cref="FieldDataType(long)"/>
        public DataTypeExpr this[int index] => FieldDataType(index);
        /// <summary>
        /// Get the DataType of field by name.
        /// </summary>
        public DataTypeExpr this[string name] => FieldDataType(name);
    }
    /// <summary>
    /// Dispose a handle.
    /// </summary>
    public void Dispose()
    {
        Handle?.Dispose();
        GC.SuppressFinalize(this); 
    }
}