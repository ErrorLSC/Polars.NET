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
    /// Convert DataTypeExpr into DataType
    /// </summary>
    /// <param name="schema"></param>
    /// <returns></returns>
    public DataType CollectDtype(PolarsSchema schema) => new(PolarsWrapper.DataTypeExprIntoDataType(CloneHandle(),schema.Handle));
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
    // 对应 python: default_value

    public Expr DefaultValue(int n = 1, bool numericToOne = false, int numListValues = 0)
        => new(PolarsWrapper.DataTypeExprDefaultValue(CloneHandle(), n, numericToOne, numListValues));

    // 对应 python: display
    public Expr Display() => new(PolarsWrapper.DataTypeExprDisplay(CloneHandle()));

    // 对应 python: inner_dtype (全局的)
    public DataTypeExpr InnerDtype() => new(PolarsWrapper.DataTypeExprInnerDtype(CloneHandle()));

    // 对应 python: matches
    public Expr Matches(Selector selector)
        =>new(PolarsWrapper.DataTypeExprMatches(CloneHandle(), selector.CloneHandle()));

    // 对应 python: wrap_in_list
    public DataTypeExpr WrapInList() => new(PolarsWrapper.DataTypeExprWrapInList(CloneHandle()));

    // 对应 python: wrap_in_array
    public DataTypeExpr WrapInArray(int width) => new(PolarsWrapper.DataTypeExprWrapInArray(CloneHandle(), width));

    public DataTypeExpr ToSignedInteger() => new(PolarsWrapper.DataTypeExprIntToSigned(CloneHandle()));
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

        public DataTypeExpr InnerDtype() => new(PolarsWrapper.DataTypeExprListInnerDtype(_parent.CloneHandle()));
    }

    // --- Array Namespace ---
    public readonly struct ArrayNameSpace
    {
        private readonly DataTypeExpr _parent;
        internal ArrayNameSpace(DataTypeExpr parent) => _parent = parent;

        public DataTypeExpr InnerDtype() => new(PolarsWrapper.DataTypeExprArrayInnerDtype(_parent.CloneHandle()));
        
        public Expr Width() => new(PolarsWrapper.DataTypeExprArrayWidth(_parent.CloneHandle()));
        public Expr Shape() => new(PolarsWrapper.DataTypeExprArrayShape(_parent.CloneHandle()));
    }

    // --- Struct Namespace ---
    public readonly struct StructNameSpace
    {
        private readonly DataTypeExpr _parent;
        internal StructNameSpace(DataTypeExpr parent) => _parent = parent;

        public Expr FieldNames() => new(PolarsWrapper.DataTypeExprStructFieldNames(_parent.CloneHandle()));

        public DataTypeExpr FieldDtype(string name) => new(PolarsWrapper.DataTypeExprStructFieldDtypeByName(_parent.CloneHandle(), name));

        public DataTypeExpr FieldDtype(long index) => new(PolarsWrapper.DataTypeExprStructFieldDtypeByIndex(_parent.CloneHandle(), index));
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