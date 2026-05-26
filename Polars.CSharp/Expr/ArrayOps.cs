using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;

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
    /// <inheritdoc cref="SeriesArrayOps.Len"/>
    public Expr Len() => Wrap(PolarsWrapper.ArrayLen);
    /// <inheritdoc cref="SeriesArrayOps.Max"/>
    public Expr Max() => Wrap(PolarsWrapper.ArrayMax);
    /// <inheritdoc cref="SeriesArrayOps.Min"/>
    public Expr Min() => Wrap(PolarsWrapper.ArrayMin);
    /// <inheritdoc cref="SeriesArrayOps.Sum"/>
    public Expr Sum() => Wrap(PolarsWrapper.ArraySum);
    /// <inheritdoc cref="SeriesArrayOps.Mean"/>
    public Expr Mean() => Wrap(PolarsWrapper.ArrayMean); 
    /// <inheritdoc cref="SeriesArrayOps.Median"/>
    public Expr Median() => Wrap(PolarsWrapper.ArrayMedian); 
    /// <inheritdoc cref="SeriesArrayOps.NUnique"/>
    public Expr NUnique() => Wrap(PolarsWrapper.ArrayNUnique); 
    /// <inheritdoc cref="SeriesArrayOps.Std"/>
    public Expr Std(byte ddof = 1) => new(PolarsWrapper.ArrayStd(PolarsWrapper.CloneExpr(_expr.Handle), ddof)); 
    /// <inheritdoc cref="SeriesArrayOps.Var"/>
    public Expr Var(byte ddof = 1) => new(PolarsWrapper.ArrayVar(PolarsWrapper.CloneExpr(_expr.Handle), ddof)); 
    /// <inheritdoc cref="SeriesArrayOps.CountMatches"/>
    public Expr CountMatches(Expr element) => new(PolarsWrapper.ArrayCountMatches(_expr.CloneHandle(),element.CloneHandle()));
    /// <inheritdoc cref="SeriesArrayOps.Agg"/>
    public Expr Agg(Expr expr) => new(PolarsWrapper.ArrayAgg(_expr.CloneHandle(),expr.CloneHandle()));
    /// <inheritdoc cref="SeriesArrayOps.Eval"/>
    public Expr Eval(Expr expr,bool asList=false) => new(PolarsWrapper.ArrayEval(_expr.CloneHandle(),expr.CloneHandle(),asList));
    /// <inheritdoc cref="SeriesArrayOps.First"/>
    public Expr First() => Get(0,true);
    /// <inheritdoc cref="SeriesArrayOps.Last"/>
    public Expr Last() => Get(-1,true);
    // --- Boolean ---
    /// <inheritdoc cref="SeriesArrayOps.Any"/>
    public Expr Any() => Wrap(PolarsWrapper.ArrayAny); 
    /// <inheritdoc cref="SeriesArrayOps.All"/>
    public Expr All() => Wrap(PolarsWrapper.ArrayAll); 
    /// <inheritdoc cref="SeriesArrayOps.Sort"/>
    public Expr Sort(bool descending = false, bool nullsLast = false, bool maintainOrder = false)
        => new(PolarsWrapper.ArraySort(_expr.CloneHandle(), descending, nullsLast, maintainOrder));
    /// <inheritdoc cref="SeriesArrayOps.Reverse"/>
    public Expr Reverse() => Wrap(PolarsWrapper.ArrayReverse); 
    /// <inheritdoc cref="SeriesArrayOps.ArgMin"/>
    public Expr ArgMin() => Wrap(PolarsWrapper.ArrayArgMin); 
    /// <inheritdoc cref="SeriesArrayOps.ArgMax"/>
    public Expr ArgMax() => Wrap(PolarsWrapper.ArrayArgMax); 
    internal Expr Get(Expr index, bool nullOnOob = true)
        => new(PolarsWrapper.ArrayGet(_expr.CloneHandle(), index.CloneHandle(), nullOnOob));
    /// <inheritdoc cref="SeriesArrayOps.Get"/>
    public Expr Get(long index,bool nullOnOob = true) => Get(Pl.Lit(index),nullOnOob);
    /// <inheritdoc cref="SeriesArrayOps.Join"/>
    public Expr Join(string separator, bool ignoreNulls = true)
        => new(PolarsWrapper.ArrayJoin(_expr.CloneHandle(), separator, ignoreNulls));
    /// <inheritdoc cref="SeriesArrayOps.Explode"/>
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
    /// <inheritdoc cref="SeriesArrayOps.ToList"/>
    public Expr ToList() => Wrap(PolarsWrapper.ArrayToList);
    /// <inheritdoc cref="SeriesArrayOps.Contains"/>
    public Expr Contains(Expr item, bool nullsEqual = false)
        => new(PolarsWrapper.ArrayContains(_expr.CloneHandle(),item.CloneHandle(), nullsEqual));
    /// <inheritdoc cref="SeriesArrayOps.Unique"/>
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