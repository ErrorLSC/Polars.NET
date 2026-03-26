#pragma warning disable CS1591
using Polars.NET.Core;

namespace Polars.CSharp;

// ==========================================
// ArrayOps Helper Class
// ==========================================
/// <summary>
/// Offers methods for accessing fields within array columns.
/// </summary>
public readonly struct ArrayOps
{
    private readonly Expr _expr;
    internal ArrayOps(Expr expr) { _expr = expr; }

    private Expr Wrap(Func<ExprHandle, ExprHandle> op)
        => new(op(_expr.CloneHandle()));
    // --- Aggregations ---
    public Expr Max() => Wrap(PolarsWrapper.ArrayMax);
    public Expr Min() => Wrap(PolarsWrapper.ArrayMin);
    public Expr Sum() => Wrap(PolarsWrapper.ArraySum);
    public Expr Mean() => Wrap(PolarsWrapper.ArrayMean); 
    public Expr Median() => Wrap(PolarsWrapper.ArrayMedian); 
    public Expr Std(byte ddof = 1) => new(PolarsWrapper.ArrayStd(PolarsWrapper.CloneExpr(_expr.Handle), ddof)); 
    public Expr Var(byte ddof = 1) => new(PolarsWrapper.ArrayVar(PolarsWrapper.CloneExpr(_expr.Handle), ddof)); 

    // --- Boolean ---
    public Expr Any() => Wrap(PolarsWrapper.ArrayAny); 
    public Expr All() => Wrap(PolarsWrapper.ArrayAll); 

    // --- Sort & Search ---
    public Expr Sort(bool descending = false, bool nullsLast = false, bool maintainOrder = false)
        => new(PolarsWrapper.ArraySort(_expr.CloneHandle(), descending, nullsLast, maintainOrder));

    public Expr Reverse() => Wrap(PolarsWrapper.ArrayReverse); 
    public Expr ArgMin() => Wrap(PolarsWrapper.ArrayArgMin); 
    public Expr ArgMax() => Wrap(PolarsWrapper.ArrayArgMax); 

    // --- Structure ---
    public Expr Get(int index, bool nullOnOob = true) => Get(Polars.Lit(index), nullOnOob);
    public Expr Get(Expr index, bool nullOnOob = true)
        => new(PolarsWrapper.ArrayGet(_expr.CloneHandle(), index.CloneHandle(), nullOnOob));

    public Expr Join(string separator, bool ignoreNulls = true)
        => new(PolarsWrapper.ArrayJoin(_expr.CloneHandle(), separator, ignoreNulls));
    
    public Expr Explode(bool emptyAsNull=true,bool keepNulls = true)
        => new(PolarsWrapper.ArrayExplode(_expr.CloneHandle(),emptyAsNull,keepNulls)); 
    
    /// <summary>
    /// Convert array to struct. Fields will be named field_0, field_1, etc.
    /// </summary>
    public Expr ToStruct() => Wrap(PolarsWrapper.ArrayToStruct); 

    public Expr ToList() => Wrap(PolarsWrapper.ArrayToList);

    // [Update] Updated Contains signature
    public Expr Contains(Expr item, bool nullsEqual = false)
        => new(PolarsWrapper.ArrayContains(_expr.CloneHandle(),item.CloneHandle(), nullsEqual));
    
    public Expr Contains(int item, bool nullsEqual = false) => Contains(Polars.Lit(item), nullsEqual);
    public Expr Contains(double item, bool nullsEqual = false) => Contains(Polars.Lit(item), nullsEqual);

    // Unique
    public Expr Unique(bool stable = false)
        => new(PolarsWrapper.ArrayUnique(_expr.CloneHandle(), stable));
    /// <summary>
    /// Concat this Array expression with other Array expressions.
    /// </summary>
    /// <param name="others">Other list expressions to append.</param>
    public Expr Concat(params Expr[] others)
    {
        var allExprs = new ExprHandle[others.Length + 1];

        allExprs[0] = _expr.CloneHandle();

        for (int i = 0; i < others.Length; i++)
        {
            allExprs[i + 1] = others[i].CloneHandle();
        }

        return new Expr(PolarsWrapper.ConcatArray(allExprs));
    }
}