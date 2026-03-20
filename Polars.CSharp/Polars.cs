#pragma warning disable CS1591
using Polars.NET.Core;
using Polars.NET.Core.Helpers;
namespace Polars.CSharp;

/// <summary>
/// Polars Static Helpers
/// </summary>
public static partial class Polars
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
        => new(PolarsWrapper.Cols(names));
    /// <summary>
    /// Return the lines count of current context.
    /// </summary>
    public static Expr Len()
        => new(PolarsWrapper.Len());
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
        using var s = Series.From("", values);
        
        return Lit(s);
    }
    
    public static Expr Lit<T>(IEnumerable<T> values)
    {
        if (values is T[] arr) return Lit(arr);
        return Lit(values.ToArray());
    }
    
    // ---------------------------------------------------------
    // Selectors Entry Points
    // ---------------------------------------------------------

    /// <summary>
    /// String matching selectors namespace.
    /// Usage: Polars.Selectors.StartsWith("A")
    /// </summary>
    public static class Selectors
    {
        /// <summary>
        /// Select all columns.
        /// </summary>
        public static Selector All() 
            => new(PolarsWrapper.SelectorAll());

        /// <summary>
        /// Select all numeric columns (Int, Float, etc.).
        /// </summary>
        public static Selector Numeric() 
            => new(PolarsWrapper.SelectorNumeric());

        /// <summary>
        /// Select all string/utf8 columns.
        /// </summary>
        public static Selector String() 
            => new(PolarsWrapper.SelectorByDtype(PlDataType.String));

        /// <summary>
        /// Select all date columns.
        /// </summary>
        public static Selector Date() 
            => new(PolarsWrapper.SelectorByDtype(PlDataType.Date));

        /// <summary>
        /// Select all datetime columns (any precision/timezone).
        /// </summary>
        public static Selector Datetime() 
            => new(PolarsWrapper.SelectorByDtype(PlDataType.Datetime));

        /// <summary>
        /// Select columns by specific DataType.
        /// </summary>
        public static Selector DType(DataType type) 
        {
            var typeKind = type.Kind;
            return new(PolarsWrapper.SelectorByDtype(typeKind.ToNative()));
        }
        /// <summary>
        /// Select column whose name starts with given prefix.
        /// </summary>
        /// <param name="prefix"></param>
        /// <returns></returns>
        public static Selector StartsWith(string prefix) 
            => new(PolarsWrapper.SelectorStartsWith(prefix));
        /// <summary>
        /// Select column whose name ends with given suffix.
        /// </summary>
        /// <param name="suffix"></param>
        /// <returns></returns>
        public static Selector EndsWith(string suffix) 
            => new(PolarsWrapper.SelectorEndsWith(suffix));
        /// <summary>
        /// Select column whose name contains given string.
        /// </summary>
        /// <param name="str"></param>
        /// <returns></returns>
        public static Selector Contains(string str) 
            => new(PolarsWrapper.SelectorContains(str));
        /// <summary>
        /// Select column whose name matches given string.
        /// </summary>
        /// <param name="regex"></param>
        /// <returns></returns>
        public static Selector Matches(string regex) 
            => new(PolarsWrapper.SelectorMatch(regex));
    }
    // ==========================================
    // Control Flow
    // ==========================================

    /// <summary>
    /// If-Else control flow: if predicate evaluates to true, return trueExpr, otherwise return falseExpr.
    /// Similar to SQL's CASE WHEN ... THEN ... ELSE ... END.
    /// </summary>
    public static Expr IfElse(Expr predicate, Expr trueExpr, Expr falseExpr)
    {
        var p = PolarsWrapper.CloneExpr(predicate.Handle);
        var t = PolarsWrapper.CloneExpr(trueExpr.Handle);
        var f = PolarsWrapper.CloneExpr(falseExpr.Handle);
        
        return new Expr(PolarsWrapper.IfElse(p, t, f));
    }
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
    // Struct Operations
    // ==========================================

    /// <summary>
    /// Combine multiple expressions into a Struct expression.
    /// </summary>
    public static Expr AsStruct(params Expr[] exprs)
    {
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        return new Expr(PolarsWrapper.AsStruct(handles));
    }
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
}

public static class InterfaceUnwrapperExtensions
{
    /// <summary>
    /// Unwrap IPolarsDataFrame as DataFrame 
    /// </summary>
    public static DataFrame AsDataFrame(this IPolarsDataFrame idf)
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
    public static LazyFrame AsLazyFrame(this IPolarsLazyFrame ilf)
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
    public static Series AsSeries(this IPolarsSeries iS)
    {
        if (iS is Series s)
            return s;
        
        throw new InvalidCastException("Not Standard Polars Series");
    }
}