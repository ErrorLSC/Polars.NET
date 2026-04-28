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
    // Calculation
    // ==========================================
    /// <summary>
    /// Compute two argument arctan in radians.
    /// Returns the angle (in radians) in the plane between the positive x-axis and the ray from the origin to (x,y).
    /// </summary>
    /// <param name="y">Column name or Expression.</param>
    /// <param name="x">Column name or Expression.</param>
    public static Expr ArcTan2(IntoExprColumn y,IntoExprColumn x)
        => new(PolarsWrapper.ArcTan2(y.Consume().Handle,x.Consume().Handle));
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
    public static Expr AsStruct(params IntoExprColumn[] exprs)
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
    public static Expr Struct(params IntoExprColumn[] exprs) => AsStruct(exprs);
    /// <summary>
    /// Collect several expressions and combine them into a single Struct Series
    /// </summary>
    public static Series StructSeries(params IntoExprColumn[] exprs) => CSharp.Series.FromExpr(Struct(exprs));
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
    ///     Pl.Combine(Pl.Lit(dates), Pl.Lit(times)).Alias("combined")
    /// );
    /// </code>
    /// </example>
    public static Expr CombineDateAndTime(Expr date, Expr time, TimeUnit tu = TimeUnit.Microseconds)
        => date.Dt.Combine(time, tu);
    /// <summary>
    /// Count the number of business days between start and end (not including end).
    /// </summary>
    /// <param name="start">Start dates.</param>
    /// <param name="end">End dates.</param>
    /// <param name="weekMask">Which days of the week to count. The default is Monday to Friday. If you wanted to count only Monday to Thursday, you would pass (True, True, True, True, False, False, False).</param>
    /// <param name="holidays">Holidays to exclude from the count.</param>
    public static Expr BusinessDayCount(IntoExprColumn start,IntoExprColumn end,bool[]? weekMask=null,IntoDateSeries? holidays=null)
    {
        bool[] realWeek = weekMask ?? DtOps.DefaultWeekMask;
        int[] holidaysMask = holidays?.ToPhysicalArray() ?? [];
        return new(PolarsWrapper.DtBusinessDayCount(start.Consume().Handle,end.Consume().Handle,realWeek,holidaysMask));
    }
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

    public static Expr Arange(IntoExprColumn start, IntoExprColumn? end = null, long step = 1, IntoDataTypeExpr? datatype = null)
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
    public static Expr Coalesce(params IntoExprColumn[] exprs)
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
    public static Series CoalesceAsSeries(params IntoExprColumn[] exprs)
    {
        if (exprs == null || exprs.Length == 0)
            throw new ArgumentException("At least one expression must be provided.");

        using var expr = Coalesce(exprs);
        
        return Series(expr);
    }
    /// <summary>
    /// Return the row indices that would sort the column(s).
    /// </summary>
    /// <param name="expr">Column(s) to arg sort by. Accepts expression input. Strings are parsed as column names.</param>
    /// <param name="descending">Sort in descending order. When sorting by multiple columns, can be specified per column by passing a sequence of booleans.</param>
    /// <param name="nullsLast">Place null values last.</param>
    /// <param name="multithreaded">Sort using multiple threads.</param>
    /// <param name="maintainOrder">Whether the order should be maintained if elements are equal.</param>
    public static Expr ArgSortBy(
        IntoExprColumn expr, 
        bool descending = false, 
        bool nullsLast = false, 
        bool multithreaded = true, 
        bool maintainOrder = false)
    {
        return ArgSortBy(
            [expr], 
            [descending], 
            [nullsLast], 
            multithreaded, 
            maintainOrder
        );
    }
    /// <inheritdoc cref="ArgSortBy(IntoExprColumn,bool,bool,bool,bool)"/>
    public static Expr ArgSortBy(
        IEnumerable<IntoExprColumn> exprs, 
        bool descending = false, 
        bool nullsLast = false, 
        bool multithreaded = true, 
        bool maintainOrder = false)
    {
        var exprList = exprs.ToArray();
        var n = exprList.Length;
        
        return ArgSortBy(
            exprList,
            [.. Enumerable.Repeat(descending, n)],
            [.. Enumerable.Repeat(nullsLast, n)],
            multithreaded,
            maintainOrder
        );
    }

    /// <inheritdoc cref="ArgSortBy(IntoExprColumn,bool,bool,bool,bool)"/>
    public static Expr ArgSortBy(
        IEnumerable<IntoExprColumn> exprs, 
        IEnumerable<bool> descending, 
        IEnumerable<bool> nullsLast, 
        bool multithreaded = true, 
        bool maintainOrder = false)
    {
        var handles = exprs.Select(e => e.Consume().Handle).ToArray();
        var descArray = descending.ToArray();
        var nullsArray = nullsLast.ToArray();

        var newHandle = PolarsWrapper.ArgSortBy(
            handles,
            descArray,
            nullsArray,
            multithreaded,
            maintainOrder
        );

        return new Expr(newHandle);
    }
    /// <inheritdoc cref="ArgSortBy(IntoExprColumn,bool,bool,bool,bool)"/>
    public static Expr ArgSortBy(params IntoExprColumn[] exprs) => ArgSortBy(exprs, descending: false);
    /// <summary>
    /// Return indices where condition evaluates True.
    /// </summary>
    /// <param name="condition">Boolean expression/Series to evaluate</param>
    public static Expr ArgWhere(IntoExpr condition) => new(PolarsWrapper.ArgWhere(condition.Consume().Handle));
    /// <inheritdoc cref="ArgWhere(IntoExpr)"/>
    public static Series ArgWhereAsSeries(IntoExpr condition) => CSharp.Series.FromExpr(ArgWhere(condition));
    // ==========================================
    // Correlation
    // ==========================================
    /// <summary>
    /// Compute the covariance between two columns/ expressions.
    /// </summary>
    /// <param name="a">Column name or Expression.</param>
    /// <param name="b">Column name or Expression.</param>
    /// <param name="ddof">“Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1.</param>
    /// <returns></returns>
    public static Expr Cov(IntoExprColumn a,IntoExprColumn b, byte ddof =1)
        => new(PolarsWrapper.Cov(a.Consume().Handle,b.Consume().Handle,ddof));
    /// <inheritdoc cref="Cov"/>
    public static Series CovAsSeries(IntoExprColumn a,IntoExprColumn b, byte ddof =1)
        => CSharp.Series.FromExpr(Cov(a,b,ddof));
    /// <summary>
    /// Compute the Pearson’s or Spearman rank correlation between two columns.
    /// </summary>
    /// <param name="a">Column name or Expression.</param>
    /// <param name="b">Column name or Expression.</param>
    /// <param name="method">Correlation method.</param>
    /// <param name="propagateNans">If True any NaN encountered will lead to NaN in the output. Defaults to False where NaN are regarded as larger than any finite number and thus lead to the highest rank.</param>
    /// <returns></returns>
    public static Expr Corr(IntoExprColumn a,IntoExprColumn b,CorrelationMethod method=CorrelationMethod.Pearson,bool propagateNans=false)
    {
        ExprHandle aE = a.Consume().Handle;
        ExprHandle bE = b.Consume().Handle;
        if (method == CorrelationMethod.Pearson)
            return new(PolarsWrapper.PearsonCorr(aE,bE));
        else 
            return new(PolarsWrapper.SpearmanRankCorr(aE,bE,propagateNans));
    }
    /// <inheritdoc cref="Corr"/>
    public static Series CorrAsSeries(IntoExprColumn a,IntoExprColumn b,CorrelationMethod method=CorrelationMethod.Pearson,bool propagateNans=false)
        => CSharp.Series.FromExpr(Corr(a,b,method,propagateNans));
    /// <summary>
    /// Compute the rolling correlation between two columns/ expressions.
    /// The window at a given row includes the row itself and the window_size - 1 elements before it.
    /// </summary>
    /// <param name="a">Column name or Expression.</param>
    /// <param name="b">Column name or Expression.</param>
    /// <param name="windowSize">The length of the window.</param>
    /// <param name="minSamples">The number of values in the window that should be non-null before computing a result. If None, it will be set equal to window size.</param>
    public static Expr RollingCorr(IntoExprColumn a,IntoExprColumn b,uint windowSize,uint? minSamples=null)
    {
        uint minSam = minSamples ?? windowSize;
        return new(PolarsWrapper.RollingCorr(a.Consume().Handle,b.Consume().Handle,windowSize,minSam));
    }
    /// <summary>
    /// Compute the rolling covariance between two columns/ expressions.
    /// The window at a given row includes the row itself and the window_size - 1 elements before it.
    /// </summary>
    /// <param name="a">Column name or Expression.</param>
    /// <param name="b">Column name or Expression.</param>
    /// <param name="windowSize">The length of the window.</param>
    /// <param name="minSamples">The number of values in the window that should be non-null before computing a result. If None, it will be set equal to window size.</param>
    /// <param name="ddof">Delta degrees of freedom. The divisor used in calculations is N - ddof, where N represents the number of elements.</param>
    public static Expr RollingCov(IntoExprColumn a,IntoExprColumn b,uint windowSize,uint? minSamples=null,byte ddof=1)
    {
        uint minSam = minSamples ?? windowSize;
        return new(PolarsWrapper.RollingCov(a.Consume().Handle,b.Consume().Handle,windowSize,minSam,ddof));
    }
    /// <summary>
    /// Return the cumulative count of the non-null values in the column.This function is syntactic sugar for Col(column).CumCount().
    /// </summary>
    /// <param name="column">Name of the columns to use.</param>
    /// <param name="reverse">reverse the operation</param>
    /// <returns></returns>
    public static Expr CumCount(string column,bool reverse=false)
        => Col(column).CumCount(reverse);
    /// <summary>
    /// Cumulatively sum all values.
    /// Syntactic sugar for Col(names).CumSum().
    /// </summary>
    /// <param name="columns">Name(s) of the columns to use in the aggregation.</param>
    public static Expr CumSum(params string[] columns)
        => Col(columns).CumSum();
    /// <summary>
    /// Represent all columns except for the given columns.
    /// Syntactic sugar for Pl.All().Exclude(columns).
    /// </summary>
    /// <param name="columns">The name or datatype of the column(s) to exclude. Accepts regular expression input. Regular expressions should start with ^ and end with $.</param>
    /// <returns></returns>
    public static Expr Exclude(params string[] columns)
        => All().Exclude(columns);
    /// <inheritdoc cref="Exclude(string[])"/>
    public static Expr Exclude(params ReadOnlySpan<DataType> columns)
        => All().Exclude(columns);  
    /// <summary>
    /// Parses an integer column (seconds, milliseconds, etc.) into a Datetime or Date expression.
    /// </summary>
    public static Expr FromEpoch(IntoExprColumn column, TimeUnit timeUnit = TimeUnit.Second)
    {
        var expr = column.Consume();

        return timeUnit switch
        {
            TimeUnit.Day => 
                expr.Cast(DataType.Date),
                
            TimeUnit.Second => 
                // Multiply by Int64 literal to prevent overflow
                (expr * 1_000_000L).Cast(DataType.Datetime(TimeUnit.Microseconds)),
                
            TimeUnit.Milliseconds => 
                (expr * 1_000L).Cast(DataType.Datetime(TimeUnit.Microseconds)),
                
            TimeUnit.Microseconds => 
                expr.Cast(DataType.Datetime(TimeUnit.Microseconds)),
                
            TimeUnit.Nanoseconds => 
                expr.Cast(DataType.Datetime(TimeUnit.Nanoseconds)),
                
            _ => throw new ArgumentException(
                $"`timeUnit` must be one of Nanoseconds, Microseconds, Milliseconds, Second, Day. Got: {timeUnit}")
        };
    }
    /// <summary>
    /// Get the first n rows.
    /// This function is syntactic sugar for Pl.Col(column).Head(n).
    /// </summary>
    /// <param name="column">Column name.</param>
    /// <param name="n">Number of rows to return</param>
    public static Expr Head(string column,int n=10)
        => Col(column).Head(n);
    /// <summary>
    /// Get the Last n rows.
    /// This function is syntactic sugar for Pl.Col(column).Tail(n).
    /// </summary>
    /// <param name="column">Column name.</param>
    /// <param name="n">Number of rows to return</param>
    public static Expr Tail(string column, int n=10)
        => Col(column).Tail(n);
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