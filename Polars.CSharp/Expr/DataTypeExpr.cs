using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;

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
    // ==========================================
    // Clean Up
    // ==========================================
    /// <summary>
    /// Dispose a handle.
    /// </summary>
    public void Dispose()
    {
        Handle?.Dispose();
        GC.SuppressFinalize(this); 
    }
}