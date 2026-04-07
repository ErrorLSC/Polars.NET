#pragma warning disable 1591
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
    /// Evaluate a bitwise AND operation on the specified columns.
    /// This function is syntactic sugar for col(names).all().
    /// </summary>
    /// <param name="names">The names of the columns to aggregate.</param>
    /// <returns>A boolean aggregation expression.</returns>
    public static Expr All(params string[] names)
    {
        if (names is null || names.Length == 0)
        {
            return Col("*");
        }
        
        return Col(names).All(ignoreNulls: true);
    }
    /// <summary>
    /// Evaluate a bitwise AND operation on the specified columns, with explicit null-handling.
    /// This function is syntactic sugar for col(names).all(ignoreNulls).
    /// </summary>
    /// <param name="ignoreNulls">Whether to ignore null values in the aggregation.</param>
    /// <param name="names">The names of the columns to aggregate.</param>
    /// <returns>A boolean aggregation expression.</returns>
    public static Expr All(bool ignoreNulls, params string[] names)
    {
        if (names is null || names.Length == 0)
        {
            return Col("*");
        }
        
        return Col(names).All(ignoreNulls);
    }
    /// <summary>
    /// Compute the logical AND horizontally across columns.
    /// <para>Kleene logic is used to deal with nulls: if the column contains any null values and no True values, the output is null.</para>
    /// </summary>
    /// <param name="exprs">Column(s) to use in the aggregation. Accepts expression input. Strings are parsed as column names, other non-expression inputs are parsed as literals.</param>
    /// <returns></returns>
    /// <exception cref="ArgumentException"></exception>
    public static Expr AllHorizontal(params IntoExpr[] exprs) 
    {
        if (exprs is null || exprs.Length == 0)
            throw new ArgumentException("Cannot create horizontal fold with empty expressions.", nameof(exprs));

        var handles = Array.ConvertAll(exprs, e => e.Consume().Handle);
        
        return new Expr(PolarsWrapper.ExprAllHorizontal(handles));
    }
    /// <summary>
    /// Evaluate a bitwise OR operation.
    /// </summary>
    /// <param name="names">Name(s) of the columns to use in the aggregation.</param>
    /// <param name="ignoreNulls">If set to True (default), null values are ignored. If there are no non-null values, the output is False.
    /// If set to False, Kleene logic is used to deal with nulls: if the column contains any null values and no True values, the output is null.</param>
    /// <returns></returns>
    public static Expr Any(bool ignoreNulls, params string[] names) => Col(names).Any(ignoreNulls);
    /// <inheritdoc cref="Any(bool,string[])"/>
    public static Expr Any(params string[] names) => Col(names).Any(true);
    /// <summary>
    /// Compute the logical OR horizontally across columns.
    /// <para>Kleene logic is used to deal with nulls: if the column contains any null values and no True values, the output is null.</para>
    /// </summary>
    /// <param name="exprs">Column(s) to use in the aggregation. Accepts expression input. Strings are parsed as column names, other non-expression inputs are parsed as literals.</param>
    /// <returns></returns>
    /// <exception cref="ArgumentException"></exception>
    public static Expr AnyHorizontal(params IntoExpr[] exprs) 
    {
        if (exprs is null || exprs.Length == 0)
            throw new ArgumentException("Cannot create horizontal fold with empty expressions.", nameof(exprs));

        var handles = Array.ConvertAll(exprs, e => e.Consume().Handle);
        
        return new Expr(PolarsWrapper.ExprAnyHorizontal(handles));
    }
    /// <summary>
    /// Get the maximum value.
    /// Syntactic sugar for Col(names).Max().
    /// </summary>
    /// <param name="names">Name(s) of the columns to use in the aggregation.</param>
    /// <returns></returns>
    public static Expr Max(params string[] names) => Col(names).Max();
    /// <summary>
    /// Get the maximum value horizontally across columns.
    /// </summary>
    /// <param name="exprs">Column(s) to use in the aggregation. Accepts expression input. Strings are parsed as column names, other non-expression inputs are parsed as literals.</param>
    /// <returns></returns>
    /// <exception cref="ArgumentException"></exception>
    public static Expr MaxHorizontal(params IntoExpr[] exprs) 
    {
        if (exprs is null || exprs.Length == 0)
            throw new ArgumentException("Cannot create horizontal fold with empty expressions.", nameof(exprs));

        var handles = Array.ConvertAll(exprs, e => e.Consume().Handle);
        
        return new Expr(PolarsWrapper.ExprMaxHorizontal(handles));
    }
    /// <summary>
    /// Get the Minimum value.
    /// Syntactic sugar for Col(names).Min().
    /// </summary>
    /// <param name="names">Name(s) of the columns to use in the aggregation.</param>
    /// <returns></returns>
    public static Expr Min(params string[] names) => Col(names).Min();
    /// <summary>
    /// Get the Minimum value horizontally across columns.
    /// </summary>
    /// <param name="exprs">Column(s) to use in the aggregation. Accepts expression input. Strings are parsed as column names, other non-expression inputs are parsed as literals.</param>
    /// <returns></returns>
    /// <exception cref="ArgumentException"></exception>
    public static Expr MinHorizontal(params IntoExpr[] exprs) 
    {
        if (exprs is null || exprs.Length == 0)
            throw new ArgumentException("Cannot create horizontal fold with empty expressions.", nameof(exprs));

        var handles = Array.ConvertAll(exprs, e => e.Consume().Handle);
        
        return new Expr(PolarsWrapper.ExprMinHorizontal(handles));
    }
    /// <summary>
    /// Get the Sum value.
    /// Syntactic sugar for Col(names).Sum().
    /// </summary>
    /// <param name="names">Name(s) of the columns to use in the aggregation.</param>
    /// <returns></returns>
    public static Expr Sum(params string[] names) => Col(names).Sum();
    /// <summary>
    /// Sum all values horizontally across columns.
    /// </summary>
    /// <param name="exprs">An iterable of expressions</param>
    /// <param name="ignoreNulls">Whether to ignore null values (default: true).</param>
    public static Expr SumHorizontal(IEnumerable<Expr> exprs, bool ignoreNulls = true)
    {
        ArgumentNullException.ThrowIfNull(exprs);
        
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        if (handles.Length == 0)
            throw new ArgumentException("Cannot create horizontal fold with empty expressions.", nameof(exprs));

        return new Expr(PolarsWrapper.ExprSumHorizontal(handles, ignoreNulls));
    }
    /// <summary>
    /// Sum all values horizontally across columns.
    /// Supports passing a collection dynamically.
    /// </summary>
    /// <param name="exprs">An iterable of expressions or column names.</param>
    /// <param name="ignoreNulls">Whether to ignore null values (default: true).</param>
    public static Expr SumHorizontal(IEnumerable<IntoExpr> exprs, bool ignoreNulls = true)
    {
        ArgumentNullException.ThrowIfNull(exprs);
        
        var handles = exprs.Select(e => e.Consume().Handle).ToArray();
        if (handles.Length == 0)
            throw new ArgumentException("Cannot create horizontal fold with empty expressions.", nameof(exprs));

        return new Expr(PolarsWrapper.ExprSumHorizontal(handles, ignoreNulls)); 
    }
    /// <summary>
    /// Sum all values horizontally across columns. (ignore_nulls defaults to true)
    /// </summary>
    /// <param name="exprs">Expressions or column names to sum.</param>
    public static Expr SumHorizontal(params IntoExpr[] exprs)
    {
        if (exprs is null || exprs.Length == 0)
            throw new ArgumentException("Cannot create horizontal fold with empty expressions.", nameof(exprs));

        var handles = Array.ConvertAll(exprs, e => e.Consume().Handle);
        return new Expr(PolarsWrapper.ExprSumHorizontal(handles, true));
    }

    /// <summary>
    /// Sum all values horizontally across columns, with explicit null-handling.
    /// </summary>
    /// <param name="ignoreNulls">Whether to ignore null values.</param>
    /// <param name="exprs">Expressions or column names to sum.</param>
    public static Expr SumHorizontal(bool ignoreNulls, params IntoExpr[] exprs)
    {
        if (exprs is null || exprs.Length == 0)
            throw new ArgumentException("Cannot create horizontal fold with empty expressions.", nameof(exprs));

        var handles = Array.ConvertAll(exprs, e => e.Consume().Handle);
        return new Expr(PolarsWrapper.ExprSumHorizontal(handles, ignoreNulls));
    }
    /// <summary>
    /// Get the Mean value.
    /// Syntactic sugar for Col(names).Mean().
    /// </summary>
    /// <param name="names">Name(s) of the columns to use in the aggregation.</param>
    /// <returns></returns>
    public static Expr Mean(params string[] names) => Col(names).Mean();
    /// <inheritdoc cref="MeanHorizontal(bool,IntoExpr[])"/>
    public static Expr MeanHorizontal(IEnumerable<Expr> exprs, bool ignoreNulls = true)
    {
        ArgumentNullException.ThrowIfNull(exprs);
        
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        if (handles.Length == 0)
            throw new ArgumentException("Cannot create horizontal fold with empty expressions.", nameof(exprs));

        return new Expr(PolarsWrapper.ExprMeanHorizontal(handles, ignoreNulls));
    }
    /// <inheritdoc cref="MeanHorizontal(bool,IntoExpr[])"/>
    public static Expr MeanHorizontal(IEnumerable<IntoExpr> exprs, bool ignoreNulls = true)
    {
        ArgumentNullException.ThrowIfNull(exprs);
        
        var handles = exprs.Select(e => e.Consume().Handle).ToArray();
        if (handles.Length == 0)
            throw new ArgumentException("Cannot create horizontal fold with empty expressions.", nameof(exprs));

        return new Expr(PolarsWrapper.ExprMeanHorizontal(handles, ignoreNulls)); 
    }
    /// <inheritdoc cref="MeanHorizontal(bool,IntoExpr[])"/>
    public static Expr MeanHorizontal(params IntoExpr[] exprs)
    {
        if (exprs is null || exprs.Length == 0)
            throw new ArgumentException("Cannot create horizontal fold with empty expressions.", nameof(exprs));

        var handles = Array.ConvertAll(exprs, e => e.Consume().Handle);
        return new Expr(PolarsWrapper.ExprMeanHorizontal(handles, true));
    }
    /// <summary>
    /// Compute the mean of all values horizontally across columns.
    /// </summary>
    /// <param name="exprs">Column(s) to use in the aggregation. Accepts expression input. Strings are parsed as column names, other non-expression inputs are parsed as literals.</param>
    /// <param name="ignoreNulls">Ignore null values (default). If set to False, any null value in the input will lead to a null output.</param>
    /// <returns></returns>
    /// <exception cref="ArgumentException"></exception>
    public static Expr MeanHorizontal(bool ignoreNulls, params IntoExpr[] exprs)
    {
        if (exprs is null || exprs.Length == 0)
            throw new ArgumentException("Cannot create horizontal fold with empty expressions.", nameof(exprs));

        var handles = Array.ConvertAll(exprs, e => e.Consume().Handle);
        return new Expr(PolarsWrapper.ExprMeanHorizontal(handles, ignoreNulls));
    }
    /// <summary>
    /// Return the number of non-null values in the column.
    /// </summary>
    /// <param name="names"></param>
    /// <returns></returns>
    public static Expr Count(params string[] names)
    {
        if (names is null || names.Length == 0)
            throw new ArgumentException("Please at least input one column", nameof(names));
        return Col(names).Count();
    }
}