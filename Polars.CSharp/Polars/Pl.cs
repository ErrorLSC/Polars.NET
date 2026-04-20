#pragma warning disable 1591
#pragma warning disable 0618
using Polars.NET.Core;
using Polars.NET.Core.Helpers;
using Cs = Polars.CSharp.Polars.Selectors;
namespace Polars.CSharp;

/// <summary>
/// Polars Static Helpers
/// </summary>
public readonly partial struct Polars
{
    /// <summary>
    /// Column Expr (name: string)
    /// </summary>
    /// <param name="name"></param>
    /// <returns></returns>
    public static Expr Col(string name)
        => new(PolarsWrapper.Col(name));
    /// <summary>
    /// Column Exprs (name: string)
    /// </summary>
    /// <param name="names"></param>
    /// <returns></returns>
    public static Expr Col(params string[] names) 
    { 
        using Selector sel = Cs.ByName(names);
        return sel.ToExpr();
    }
    /// <summary>
    /// Alias for an element being evaluated in an eval or filter expression.
    /// </summary>
    public static Expr Element() => Col("");
    /// <summary>
    /// Return the lines count of current context.
    /// </summary>
    public static Expr Len() => new(PolarsWrapper.Len());
    // --- Literals ---
    public static Expr Lit(string? value)
    {
        if (value is null)
        {
            return new Expr(PolarsWrapper.LitNull());
        }
        return new Expr(PolarsWrapper.Lit(value));
    }
    public static Expr Lit(sbyte value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(byte value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(short value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(ushort value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(int value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(uint value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(long value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(ulong value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(Int128 value) => new(PolarsWrapper.Lit(value));
    // public static Expr Lit(UInt128 value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(double value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(DateTime value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(DateTimeOffset value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(DateOnly value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(TimeOnly value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(TimeSpan value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(bool value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(Half value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(float value) => new(PolarsWrapper.Lit(value));
    public static Expr Lit(decimal value) => new(PolarsWrapper.Lit(value));
    public static Expr LitNull() => new(PolarsWrapper.LitNull());
    /// <summary>
    /// Convert Series into Literal Expr.
    /// <para>The Series is cloned implicitly, so the original Series remains valid.</para>
    /// </summary>
    public static Expr Lit(Series series)
    {
        var clonedHandle = PolarsWrapper.CloneSeries(series.Handle);
        return new Expr(PolarsWrapper.Lit(clonedHandle));
    }
    // -------------------------------------------------------------------------
    // Struct Literals
    // -------------------------------------------------------------------------

    /// <summary>
    /// Create a Struct Expression from a single anonymous object or class instance.
    /// <para>Example: <c>LitStruct(new { A = 1, B = "hi" })</c></para>
    /// </summary>
    public static Expr LitStruct<T>(T value) where T : class
        => LitStruct([value]);

    /// <summary>
    /// Create a Struct Expression from an array of objects.
    /// <para>The properties of the objects become the fields of the struct.</para>
    /// </summary>
    public static Expr LitStruct<T>(T[] values)
    {
        SeriesHandle sHandle = StructPacker.Pack("literal", values);
        ExprHandle eHandle = PolarsWrapper.Lit(sHandle);
        return new Expr(eHandle);
    }
    // =========================================================================
    // The "Magic" Lit
    // =========================================================================

    public static Expr Lit<T>(T[] values)
    {
        using var s = CSharp.Series.From("", values);
        
        return Lit(s);
    }
    
    public static Expr Lit<T>(IEnumerable<T> values)
    {
        if (values is T[] arr) return Lit(arr);
        return Lit(values.ToArray());
    }

    public static Expr Lit<T>(ReadOnlySpan<T> values)
    {
        using var s = CSharp.Series.FromSpan("", values);
        return Lit(s);
    }

    /// <summary>
    /// Creates a scalar Binary Literal Expression from a byte array.
    /// </summary>
    public static Expr LitBinary(byte[] value)
        => Lit(value).Implode().Cast(DataType.Binary);
    
    // ==========================================
    // Control Flow
    // ==========================================

    /// <summary>
    /// If-Else control flow: if predicate evaluates to true, return trueExpr, otherwise return falseExpr.
    /// Similar to SQL's CASE WHEN ... THEN ... ELSE ... END.
    /// </summary>
    public static Expr IfElse(Expr predicate, Expr trueExpr, Expr falseExpr)
        => new(PolarsWrapper.IfElse(predicate.CloneHandle(), trueExpr.CloneHandle(), falseExpr.CloneHandle()));

    // ==========================================
    // List Operations
    // ==========================================
    /// <summary>
    /// Concat multiple list expressions into a single list expression.
    /// </summary>
    public static Expr ConcatList(params Expr[] exprs)
    {
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        return new Expr(PolarsWrapper.ConcatList(handles));
    }
    // ==========================================
    // Array Operations
    // ==========================================
    /// <summary>
    /// Concat multiple array expressions into a single array expression.
    /// </summary>
    public static Expr ConcatArray(params Expr[] exprs)
    {
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        return new Expr(PolarsWrapper.ConcatArray(handles));
    }
    // ==========================================
    // String Operations
    // ==========================================
    /// <summary>
    /// Concat multiple string expressions into a single string expression.
    /// </summary>
    public static Expr ConcatString(string separator=",",bool ignoreNulls=false,params Expr[] exprs)
    {
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        return new Expr(PolarsWrapper.ConcatString(handles,separator,ignoreNulls));
    }
    /// <summary>
    /// Format multiple string expressions into a single formated string expression.
    /// </summary>
    public static Expr FormatString(string format,params Expr[] exprs)
    {
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        return new Expr(PolarsWrapper.FormatString(format,handles));
    }
    // ==========================================
    // Concat Exprs
    // ==========================================
    /// <summary>
    /// Concat multiple expressions into a single expression.
    /// </summary>
    public static Expr ConcatExpr(bool rechunk=false,params Expr[] exprs)
    {
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        return new Expr(PolarsWrapper.ConcatExprs(handles,rechunk));
    }
    /// <inheritdoc cref="ConcatExpr(bool,Expr[])"/>
    public static Expr ConcatExpr(params Expr[] exprs) => ConcatExpr(false,exprs);
    // ==========================================
    // Struct Operations
    // ==========================================

    /// <summary>
    /// Combine multiple expressions into a Struct expression.
    /// </summary>
    [Obsolete("Renamed to Struct to align with pypolars API name")]
    public static Expr AsStruct(params IntoColumnExpr[] exprs)
    {
        if (exprs == null || exprs.Length == 0)
        {
            throw new ArgumentException("Struct requires at least one expression to be combined.");
        }

        var handles = exprs.Select(e => e.Consume().Handle).ToArray();
        
        return new Expr(PolarsWrapper.AsStruct(handles));
    }
    /// <summary>
    /// Collect several expressions and combine them into a single Struct column.
    /// </summary>
    public static Expr Struct(params IntoColumnExpr[] exprs) => AsStruct(exprs);
    /// <summary>
    /// Collect several expressions and combine them into a single Struct Series
    /// </summary>
    public static Series StructSeries(params IntoColumnExpr[] exprs) => CSharp.Series.FromExpr(Struct(exprs));
    // ==========================================
    // SQL
    // ==========================================
    /// <summary>
    /// Create a new SQL Context.
    /// </summary>
    public static SqlContext Sql() => new();
    /// <summary>
    /// Create a Polars Expr from a SQL string.
    /// </summary>
    /// <param name="sql">The SQL expression string.</param>
    /// <returns>A Polars Expr representing the SQL logic.</returns>
    /// <exception cref="ArgumentException">Thrown when the provided SQL string is null, empty, or consists only of white-space characters.</exception>
    public static Expr SqlExpr(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            throw new ArgumentException("SQL expression can not be null", nameof(sql));

        return new Expr(PolarsWrapper.SqlExpr(sql));
    }
    /// <summary>
    /// Create an array of Polars Exprs from a collection of SQL strings.
    /// </summary>
    /// <param name="sqls">The collection of SQL expression strings.</param>
    /// <returns>An array of Polars Expr objects.</returns>
    public static Expr[] SqlExprs(IEnumerable<string> sqls) 
            => [.. sqls.Select(SqlExpr)];

    // ==========================================
    // Temporal
    // ==========================================

    /// <summary>
    /// Combine a Date expression and a Time expression into a Datetime expression.
    /// <para>
    /// Useful for combining a Date column with a Time column, or combining literal arrays.
    /// </para>
    /// <para>
    /// <b>Note:</b> Only sub-second units (<see cref="TimeUnit.Nanoseconds"/>, <see cref="TimeUnit.Microseconds"/>, <see cref="TimeUnit.Milliseconds"/>) are supported.
    /// </para>
    /// </summary>
    /// <param name="date">Expression for the Date component (can be a Column, Literal, or calculation).</param>
    /// <param name="time">Expression for the Time component.</param>
    /// <param name="tu">The desired TimeUnit for the resulting Datetime (default: Microseconds).</param>
    /// <returns>A new expression evaluating to Datetime.</returns>
    /// <example>
    /// <code>
    /// // 1. Combine Columns
    /// df.Select(Polars.Combine(Col("date_col"), Col("time_col")));
    /// 
    /// // 2. Combine Arrays (Literals)
    /// var dates = new[] { new DateOnly(2024, 1, 1), new DateOnly(2024, 1, 2) };
    /// var times = new[] { new TimeOnly(10, 0), new TimeOnly(11, 0) };
    /// 
    /// df.Select(
    ///     Polars.Combine(Polars.Lit(dates), Polars.Lit(times)).Alias("combined")
    /// );
    /// </code>
    /// </example>
    public static Expr CombineDateAndTime(Expr date, Expr time, TimeUnit tu = TimeUnit.Microseconds)
        => date.Dt.Combine(time, tu);

    // ==========================================
    // Fold & Reduce
    // ==========================================

    /// <summary>
    /// Accumulate over multiple columns horizontally/row-wise with a left fold.
    /// </summary>
    public static Expr Fold(Expr acc, Func<Expr, Expr, Expr> f, IEnumerable<Expr> exprs)
    {
        Expr current = acc;
        foreach (var expr in exprs)
        {
            current = f(current, expr);
        }
        return current;
    }

    /// <summary>
    /// Reduce multiple columns horizontally/row-wise with a left fold.
    /// </summary>
    public static Expr Reduce(Func<Expr, Expr, Expr> f, IEnumerable<Expr> exprs)
    {
        using var enumerator = exprs.GetEnumerator();
        if (!enumerator.MoveNext())
            throw new ArgumentException("exprs cannot be empty for Reduce");

        Expr current = enumerator.Current;
        while (enumerator.MoveNext())
        {
            current = f(current, enumerator.Current);
        }
        return current;
    }

    public static Expr Arange(IntoColumnExpr start, IntoColumnExpr? end = null, long step = 1, IntoDataTypeExpr? datatype = null)
        => IntRange(start,end,step,datatype);

    /// <summary>
    /// Gets the DataType of an expression.
    /// Equivalent to Python's polars.dtype_of()
    /// </summary>
    public static DataTypeExpr DataTypeOf(Expr expr)
    {
        var h = PolarsWrapper.DataTypeExprDtypeOf(expr.CloneHandle());
        return new DataTypeExpr(h);
    }

    /// <summary>
    /// Represents the intrinsic data type of the current element.
    /// Equivalent to Python's polars.self_dtype()
    /// </summary>
    public static DataTypeExpr SelfDataType()
    {
        var h = PolarsWrapper.DataTypeExprSelfDtype();
        return new DataTypeExpr(h);
    }
    /// <summary>
    /// (Lazy) Evaluates the arguments in order and returns the first non-null value.
    /// </summary>
    /// <param name="exprs">Expressions to evaluate. Strings, Literals, Series are automatically converted.</param>
    /// <returns>A new expression.</returns>
    public static Expr Coalesce(params IntoColumnExpr[] exprs)
    {
        if (exprs == null || exprs.Length == 0)
            throw new ArgumentException("At least one expression must be provided.");

        var handles = new ExprHandle[exprs.Length];
        
        for (int i = 0; i < exprs.Length; i++)
        {
            Expr e = exprs[i].Consume();
            handles[i] = e.Handle; 
        }

        return new Expr(PolarsWrapper.Coalesce(handles));
    }

    /// <summary>
    /// (Eager) Evaluates the arguments eagerly and returns the first non-null value as a Series.
    /// </summary>
    /// <param name="exprs">Expressions or Series to evaluate.</param>
    public static Series CoalesceAsSeries(params IntoColumnExpr[] exprs)
    {
        if (exprs == null || exprs.Length == 0)
            throw new ArgumentException("At least one expression must be provided.");

        using var expr = Coalesce(exprs);
        
        return Series(expr);
    }

}

internal static class InterfaceUnwrapperExtensions
{
    /// <summary>
    /// Unwrap IPolarsDataFrame as DataFrame 
    /// </summary>
    internal static DataFrame AsDataFrame(this IPolarsDataFrame idf)
    {
        if (idf is DataFrame df)
            return df;
        
        throw new InvalidCastException("Not Standard Polars DataFrame");
    }
    /// <summary>
    /// Unwrap IPolarsLazyFrame as LazyFrame 
    /// </summary>
    /// <param name="ilf">A IPolarsLazyFrame</param>
    /// <returns></returns>
    /// <exception cref="InvalidCastException"></exception>
    internal static LazyFrame AsLazyFrame(this IPolarsLazyFrame ilf)
    {
        if (ilf is LazyFrame lf)
            return lf;
        
        throw new InvalidCastException("Not Standard Polars LazyFrame");
    }
    /// <summary>
    /// Unwrap IPolarsLazyFrame as LazyFrame 
    /// </summary>
    /// <param name="iS">A IPolarsSeries</param>
    /// <returns></returns>
    /// <exception cref="InvalidCastException"></exception>
    internal static Series AsSeries(this IPolarsSeries iS)
    {
        if (iS is Series s)
            return s;
        
        throw new InvalidCastException("Not Standard Polars Series");
    }
}