#pragma warning disable CS1591
using Polars.NET.Core;
using Polars.NET.Core.Helpers;

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
    public Expr Len() => Wrap(PolarsWrapper.ArrayLen);
    public Expr Max() => Wrap(PolarsWrapper.ArrayMax);
    public Expr Min() => Wrap(PolarsWrapper.ArrayMin);
    public Expr Sum() => Wrap(PolarsWrapper.ArraySum);
    public Expr Mean() => Wrap(PolarsWrapper.ArrayMean); 
    public Expr Median() => Wrap(PolarsWrapper.ArrayMedian); 
    public Expr NUnique() => Wrap(PolarsWrapper.ArrayNUnique); 
    public Expr Std(byte ddof = 1) => new(PolarsWrapper.ArrayStd(PolarsWrapper.CloneExpr(_expr.Handle), ddof)); 
    public Expr Var(byte ddof = 1) => new(PolarsWrapper.ArrayVar(PolarsWrapper.CloneExpr(_expr.Handle), ddof)); 
    public Expr CountMatches(Expr element) => new(PolarsWrapper.ArrayCountMatches(_expr.CloneHandle(),element.CloneHandle()));
    public Expr Agg(Expr expr) => new(PolarsWrapper.ArrayAgg(_expr.CloneHandle(),expr.CloneHandle()));
    public Expr Eval(Expr expr,bool asList=false) => new(PolarsWrapper.ArrayEval(_expr.CloneHandle(),expr.CloneHandle(),asList));
    public Expr First() => Get(0,true);
    public Expr Last() => Get(-1,true);
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
    internal Expr Get(Expr index, bool nullOnOob = true)
        => new(PolarsWrapper.ArrayGet(_expr.CloneHandle(), index.CloneHandle(), nullOnOob));
    public Expr Get(long index,bool nullOnOob = true) => Get((Expr)index,nullOnOob);

    public Expr Join(string separator, bool ignoreNulls = true)
        => new(PolarsWrapper.ArrayJoin(_expr.CloneHandle(), separator, ignoreNulls));
    
    public Expr Explode(bool emptyAsNull=true,bool keepNulls = true)
        => new(PolarsWrapper.ArrayExplode(_expr.CloneHandle(),emptyAsNull,keepNulls)); 
    
    /// <summary>
    /// Convert array to struct.
    /// </summary>
    /// <param name="fields">Desired field names</param>
    public Expr ToStruct(params string[] fields)
        => new(PolarsWrapper.ArrayToStruct(_expr.CloneHandle(),fields)); 
    /// <summary>
    /// Convert array to a struct using a function to generate field names dynamically.
    /// </summary>
    /// <param name="nameGenerator">A function that takes an index and returns a string.</param>
    /// <param name="fieldCount">The expected number of fields to generate.</param>
    public Expr ToStruct(Func<int, string> nameGenerator, int fieldCount)
    {
        if (fieldCount <= 0) 
            return ToStruct();

        string[] fields = new string[fieldCount];
        for (int i = 0; i < fieldCount; i++)
        {
            fields[i] = nameGenerator(i);
        }
        return ToStruct(fields);
    }
    public Expr ToList() => Wrap(PolarsWrapper.ArrayToList);

    // Updated Contains signature
    public Expr Contains(Expr item, bool nullsEqual = false)
        => new(PolarsWrapper.ArrayContains(_expr.CloneHandle(),item.CloneHandle(), nullsEqual));

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