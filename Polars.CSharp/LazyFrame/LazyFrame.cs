#pragma warning disable CS1573

using System.Data;
using Polars.NET.Core;
using Polars.NET.Core.Helpers;
using Cs = Polars.CSharp.Polars.Selectors;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;

/// <summary>
/// Represents a lazily evaluated DataFrame.
/// Until the query is executed, operations are just recorded in a query plan.
/// Once executed, the data is materialized in memory.
/// </summary>
public partial class LazyFrame : IDisposable,IPolarsLazyFrame
{
    internal LazyFrameHandle Handle { get; }

    internal LazyFrame(LazyFrameHandle handle)
    {
        Handle = handle;
    }

    // ==========================================
    // Meta / Inspection
    // ==========================================

    /// <summary>
    /// Gets the Schema of the LazyFrame.
    /// Note: The returned PolarsSchema object is IDisposable. 
    /// Usage in 'using' block is recommended if accessed frequently.
    /// </summary>
    public PolarsSchema Schema
    {
        get
        {
            var handle = PolarsWrapper.GetLazySchema(Handle);
            
            return new PolarsSchema(handle);
        }
    }
    IPolarsSchema IPolarsLazyFrame.Schema => this.Schema;
    /// <summary>
    /// Prints the schema to the console.
    /// </summary>
    public void PrintSchema()
    {
        using var schema = Schema;
        
        Console.WriteLine("root");
        
        foreach (var name in schema.ColumnNames)
        {
            var type = schema[name]; 
            Console.WriteLine($" |-- {name}: {type.Kind}");
        }
    }
    /// <summary>
    /// Return LazyFrame Columns' Name
    /// </summary>
    public string[] Columns => [.. Schema.ColumnNames];

    /// <summary>
    /// Return LazyFrame Columns' Name
    /// </summary>
    public string[] ColumnNames => Columns;

    /// <summary>
    /// Return LazyFrame Columns' DataType
    /// </summary>
    public List<DataType> DataTypes => Schema.DataTypes;

    /// <summary>
    /// Get an explanation of the optimized query plan.
    /// <para>
    /// Returns a string representation of the logical plan after Polars optimizers 
    /// (predicate pushdown, projection pushdown, etc.) have run.
    /// </para>
    /// </summary>
    /// <param name="optimized">If true, show the optimized plan. If false, show the logical plan as built.</param>
    /// <returns>The plan as a string.</returns>
    /// <example>
    /// <code>
    /// var q = df.Lazy()
    ///     .Filter(Col("group") != "C")
    ///     .WithColumns((Col("val") * 2).Alias("val_x_2"))
    ///     .Select("group", "val_x_2");
    /// 
    /// Console.WriteLine(q.Explain());
    /// /* Output (Optimized Plan):
    /// simple π 2/2 ["group", "val_x_2"]
    ///    WITH_COLUMNS:
    ///    [[(col("val")) * (2)].alias("val_x_2")] 
    ///     FILTER [(col("group")) != ("C")]
    ///     FROM
    ///       DF ["group", "val"]; PROJECT["group", "val"] 2/2 COLUMNS
    /// */
    /// </code>
    /// </example>
    public string Explain(bool optimized = true)
    {
        return PolarsWrapper.Explain(Handle, optimized);
    }
    /// <summary>
    /// Clone the LazyFrame, creating a new independent copy.
    /// </summary>
    /// <returns></returns>
    public LazyFrame Clone()
    {
        return new LazyFrame(PolarsWrapper.LazyClone(Handle));
    }
    internal LazyFrameHandle CloneHandle()
    {
        return PolarsWrapper.LazyClone(Handle);
    }

    /// <summary>
    /// Return the number of non-null elements for each column.
    /// </summary>
    /// <returns></returns>
    public LazyFrame Count()
        => Select(Polars.All().Count());
    /// <summary>
    /// Aggregate the columns in the Frame to their sum value.
    /// </summary>
    /// <returns></returns>
    public LazyFrame Sum()
        => Select(Polars.All().Sum());
    /// <summary>
    /// Aggregate the columns in the Frame to their maximum value.
    /// </summary>
    /// <returns></returns>
    public LazyFrame Max()
        => Select(Polars.All().Max());
    /// <summary>
    /// Aggregate the columns in the Frame to their minimum value.
    /// </summary>
    /// <returns></returns>
    public LazyFrame Min()
        => Select(Polars.All().Min());
    /// <summary>
    /// Aggregate the columns in the Frame to their mean value.
    /// </summary>
    /// <returns></returns>
    public LazyFrame Mean()
        => Select(Polars.All().Mean());
    /// <summary>
    /// Aggregate the columns in the Frame to their median value.
    /// </summary>
    /// <returns></returns>
    public LazyFrame Median()
        => Select(Polars.All().Median());
    /// <summary>
    /// Aggregate the columns in the Frame as the sum of their null value count.
    /// </summary>
    /// <returns></returns>
    public LazyFrame NullCount()
        => Select(Polars.All().NullCount());
    /// <summary>
    /// Aggregate the columns in the Frame to their standard deviation value.
    /// </summary>
    /// <param name="ddof">“Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1.</param>
    /// <returns></returns>
    public LazyFrame Std(int ddof=1)
        => Select(Polars.All().Std(ddof));
    /// <summary>
    /// Aggregate the columns in the Frame to their variance value.
    /// </summary>
    /// <param name="ddof">“Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1.</param>
    /// <returns></returns>
    public LazyFrame Var(int ddof=1)
        => Select(Polars.All().Var(ddof));

    /// <summary>
    /// Aggregate the columns in the Frame to their quantile value.
    /// </summary>
    /// <param name="quantile">Quantile between 0.0 and 1.0.</param>
    /// <param name="method">['nearest’, ‘higher’, ‘lower’, ‘midpoint’, ‘linear’] Interpolation method.</param>
    /// <returns></returns>
    public LazyFrame Quantile(double quantile, QuantileMethod method = QuantileMethod.Linear)
        => Select(Polars.All().Quantile(quantile,method));

    // ==========================================
    // Transformations
    // ==========================================
    /// <summary>
    /// Select specific columns or expressions.
    /// </summary>
    /// <example>
    /// <code>
    /// // Select "a" and calculate "b" * 2
    /// lf.Select(Col("a"), (Col("b") * 2).Alias("b_double"));
    /// </code>
    /// </example>
    public LazyFrame Select(params Expr[] exprs)
    {
        var lfClone = CloneHandle();
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        return new LazyFrame(PolarsWrapper.LazySelect(lfClone, handles));
    }
    /// <summary>
    /// Select columns by name.
    /// <para>Syntactic sugar for <c>Select(Expr.Col(name))</c>.</para>
    /// </summary>
    public LazyFrame Select(params string[] columns)
    {
        var exprs = columns.Select(Polars.Col).ToArray();
        return Select(exprs);
    }
    /// <summary>
    /// Filter rows based on a boolean expression.
    /// <para>
    /// In a LazyFrame, this operation is added to the logical plan and is optimized before execution.
    /// Polars will attempt to push this filter down as close to the data source as possible (Predicate Pushdown).
    /// </para>
    /// </summary>
    /// <param name="expr">A boolean expression.</param>
    /// <returns>A new LazyFrame with the filter applied.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     group = new[] { "A", "A", "B", "B", "C" },
    ///     val = new[] { 1, 2, 3, 4, 5 }
    /// });
    /// 
    /// // Build a lazy query:
    /// // 1. Filter out group 'C'
    /// // 2. Multiply 'val' by 2
    /// // 3. Select specific columns
    /// var q = df.Lazy()
    ///     .Filter(Col("group") != "C")
    ///     .WithColumns((Col("val") * 2).Alias("val_x_2"))
    ///     .Select("group", "val_x_2");
    /// 
    /// // Execute
    /// q.Collect().Show();
    /// /* Output:
    /// shape: (4, 2)
    /// ┌───────┬─────────┐
    /// │ group ┆ val_x_2 │
    /// │ ---   ┆ ---     │
    /// │ str   ┆ i32     │
    /// ╞═══════╪═════════╡
    /// │ A     ┆ 2       │
    /// │ A     ┆ 4       │
    /// │ B     ┆ 6       │
    /// │ B     ┆ 8       │
    /// └───────┴─────────┘
    /// */
    /// </code>
    /// </example>
    public LazyFrame Filter(Expr expr)
        => new(PolarsWrapper.LazyFilter(CloneHandle(), expr.CloneHandle()));
    /// <summary>
    ///  Filter rows based on a boolean series.
    /// </summary>
    public LazyFrame Filter(Series series)
    {
        if (series.DataType != DataType.Boolean)
        {
            throw new InvalidExpressionException("Can not Filter by non-boolean series.");
        }
        
        using var expr = Polars.Lit(series); 
        
        return Filter(expr); 
    }

    /// <summary>
    /// Add or modify columns based on expressions.
    /// </summary>
    /// <example>
    /// <code>
    /// // Add a new column "c" while keeping "a" and "b"
    /// lf.WithColumns((Col("a") + Col("b")).Alias("c"));
    /// </code>
    /// </example>
    public LazyFrame WithColumns(params Expr[] exprs)
    {
        var lfClone = CloneHandle();
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        return new LazyFrame(PolarsWrapper.LazyWithColumns(lfClone, handles));
    }
    /// <summary>
    /// Slice the LazyFrame.
    /// <para>This operation is lazy; it only affects the query plan.</para>
    /// </summary>
    /// <param name="offset">Start index. Negative values count from the end.</param>
    /// <param name="length">Number of rows to return.</param>
    public LazyFrame Slice(long offset, uint length)
    {
        var handle = PolarsWrapper.LazySlice(CloneHandle(), offset, length);
        return new LazyFrame(handle);
    }
    /// <summary>
    /// Renames columns in the <see cref="LazyFrame"/>.
    /// </summary>
    /// <param name="existing">An array of existing column names to be renamed.</param>
    /// <param name="newNames">An array of new column names, corresponding by index to the names in <paramref name="existing"/>.</param>
    /// <param name="strict">
    /// If <c>true</c>, an error is raised if any column in <paramref name="existing"/> is not found in the schema. 
    /// If <c>false</c>, columns that are not found are silently ignored. Default is <c>true</c>.
    /// </param>
    /// <returns>A new <see cref="LazyFrame"/> with the rename operation applied.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="existing"/> or <paramref name="newNames"/> is null.</exception>
    /// <exception cref="ArgumentException">Thrown when the length of <paramref name="existing"/> does not match the length of <paramref name="newNames"/>.</exception>
    public LazyFrame Rename(string[] existing, string[] newNames, bool strict = true)
    {
        ArgumentNullException.ThrowIfNull(existing);
        ArgumentNullException.ThrowIfNull(newNames);

        var newHandle = PolarsWrapper.LazyRename(Handle, existing, newNames, strict);
        
        return new LazyFrame(newHandle);
    }
    /// <summary>
    /// Slice the LazyFrame (Convenience overload).
    /// </summary>
    public LazyFrame Slice(long offset, int length)
    {
        if (length < 0) throw new ArgumentOutOfRangeException(nameof(length), "Length must be non-negative.");
        return Slice(offset, (uint)length);
    }
    /// <summary>
    /// Cast LazyFrame column(s) to the specified dtype(s) using a dictionary mapping.
    /// </summary>
    public LazyFrame Cast(IDictionary<string, DataType> dtypes, bool strict = true)
    {
        ArgumentNullException.ThrowIfNull(dtypes);

        var castExprs = dtypes.Select(kvp =>
            Pl.Col(kvp.Key).Cast(kvp.Value, strict)
        );

        return WithColumns([.. castExprs]);
    }

    /// <summary>
    /// Cast all columns in the LazyFrame to the specified dtype.
    /// </summary>
    public LazyFrame Cast(DataType dtype, bool strict = true)
        => Select(Pl.All().Cast(dtype, strict));

    /// <summary>
    /// Cast columns matching a specific Expression or Selector to a DataType.
    /// Example: lf.Cast(Cs.EndsWith("Cm").ToExpr(), DataType.Float32)
    /// </summary>
    public LazyFrame Cast(Expr expr, DataType dtype, bool strict = true)
    {
        ArgumentNullException.ThrowIfNull(expr);
        
        return WithColumns(expr.Cast(dtype, strict));
    }

    /// <summary>
    /// Cast multiple expressions/selectors to their target DataTypes using tuples.
    /// </summary>
    public LazyFrame Cast(IEnumerable<(Expr Expr, DataType Dtype)> dtypes, bool strict = true)
    {
        ArgumentNullException.ThrowIfNull(dtypes);

        var castExprs = dtypes.Select(t => t.Expr.Cast(t.Dtype, strict)).ToArray();
        return WithColumns(castExprs);
    }

    /// <summary>
    /// lf.Cast((Cs.Numeric().ToExpr(), DataType.Float32), (Polars.Col("Id"), DataType.Int32))
    /// </summary>
    public LazyFrame Cast(params (Expr Expr, DataType Dtype)[] dtypes)
        =>Cast((IEnumerable<(Expr, DataType)>)dtypes, strict: true);
    /// <summary>
    /// Sort the LazyFrame by a single column.
    /// </summary>
    public LazyFrame Sort(
        string column, 
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false)
        => Sort([Pl.Col(column)], [descending], [nullsLast], maintainOrder);
    /// <summary>
    /// Sort using a single expression.
    /// </summary>
    public LazyFrame Sort(
        Expr expr, 
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false)
    {
        return Sort([expr], [descending], [nullsLast], maintainOrder);
    }
    /// <summary>
    /// Sort using multiple exprs and single option.
    /// </summary>
    /// <param name="exprs"></param>
    /// <param name="descending"></param>
    /// <param name="nullsLast"></param>
    /// <param name="maintainOrder"></param>
    /// <returns></returns>
    public LazyFrame Sort(
        Expr[] exprs, 
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false)
    {
        return Sort(exprs, [descending], [nullsLast], maintainOrder);
    }
    /// <summary>
    /// Sort the LazyFrame by multiple columns (all ascending or all descending).
    /// </summary>
    public LazyFrame Sort(
        string[] columns, 
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false)
    {
        var exprs = columns.Select(Polars.Col).ToArray();
        return Sort(exprs, [descending], [nullsLast], maintainOrder);
    }

    /// <summary>
    /// Lazily sort the DataFrame by multiple columns.
    /// <para>
    /// This operation is added to the logical plan. 
    /// Use <see cref="TopK(int, string, bool)"/> if you only need the top/bottom N rows, as it is more efficient.
    /// </para>
    /// </summary>
    /// <param name="columns">Names of the columns to sort by.</param>
    /// <param name="descending">Sort order for each column.</param>
    /// <param name="nullsLast">Whether nulls go last for each column.</param>
    /// <param name="maintainOrder">Whether to maintain the relative order of rows with equal keys.</param>
    /// <seealso cref="DataFrame.Sort(string[], bool[], bool[], bool)"/>
    /// <example>
    /// <code>
    /// df.Lazy()
    ///   .Sort(
    ///       columns: new[] { "group", "val" }, 
    ///       descending: new[] { false, true }, 
    ///       nullsLast: new[] { false, false }
    ///   )
    ///   .Collect();
    /// /* Output:
    /// shape: (5, 2)
    /// ┌───────┬─────┐
    /// │ group ┆ val │
    /// │ ---   ┆ --- │
    /// │ str   ┆ i32 │
    /// ╞═══════╪═════╡
    /// │ A     ┆ 10  │
    /// │ A     ┆ 8   │
    /// │ ...   ┆ ... │
    /// └───────┴─────┘
    /// */
    /// </code>
    /// </example>
    public LazyFrame Sort(
        string[] columns, 
        bool[] descending, 
        bool[] nullsLast, 
        bool maintainOrder = false)
    {
        var exprs = columns.Select(Polars.Col).ToArray();
        return Sort(exprs, descending, nullsLast, maintainOrder);
    }

    /// <summary>
    /// Sort the LazyFrame by multiple exprs.
    /// </summary>
    public LazyFrame Sort(
        Expr[] exprs, 
        bool[] descending, 
        bool[] nullsLast, 
        bool maintainOrder = false)
    {
        var clonedHandles = new ExprHandle[exprs.Length];
        for (int i = 0; i < exprs.Length; i++)
        {
            clonedHandles[i] = PolarsWrapper.CloneExpr(exprs[i].Handle);
        }

        var h = PolarsWrapper.LazyFrameSort(
            Handle, 
            clonedHandles, 
            descending, 
            nullsLast, 
            maintainOrder
        );
        
        return new LazyFrame(h);
    }
    /// <summary>
    /// Get the top k rows according to the given expressions.
    /// <para>This selects the largest values.</para>
    /// </summary>
    /// <param name="k">Number of rows to return.</param>
    /// <param name="by">Expressions to sort by.</param>
    /// <param name="reverse">
    /// If true, select the smallest values (reverse the sort order) for that column.
    /// </param>
    public LazyFrame TopK(int k, Expr[] by, bool[] reverse)
    {
        if (by.Length != reverse.Length)
            throw new ArgumentException("Length of 'by' and 'reverse' must match.");

        var lfHandle = CloneHandle(); // Consume self
        var clonedHandles = new ExprHandle[by.Length];
        for (int i = 0; i < by.Length; i++)
        {
            clonedHandles[i] = PolarsWrapper.CloneExpr(by[i].Handle);
        }

        var h = PolarsWrapper.LazyFrameTopK(lfHandle, (uint)k, clonedHandles, reverse);
        return new LazyFrame(h);
    }

    /// <summary>
    /// Get the top k rows according to a single expression.
    /// </summary>
    public LazyFrame TopK(int k, Expr by, bool reverse = false)
    {
        return TopK(k, [by], [reverse]);
    }
    
    /// <summary>
    /// Get the top k rows according to a single column name.
    /// </summary>
    /// <param name="k"></param>
    /// <param name="colName"></param>
    /// <param name="reverse"></param>
    /// <returns></returns>
    public LazyFrame TopK(int k, string colName, bool reverse = false)
    {
        return TopK(k, Polars.Col(colName), reverse);
    }

    /// <summary>
    /// Get the bottom k rows according to the given expressions.
    /// <para>This selects the smallest values.</para>
    /// </summary>
    public LazyFrame BottomK(int k, Expr[] by, bool[] reverse)
    {
        if (by.Length != reverse.Length)
            throw new ArgumentException("Length of 'by' and 'reverse' must match.");

        var lfHandle = CloneHandle();
        var clonedHandles = new ExprHandle[by.Length];
        for (int i = 0; i < by.Length; i++)
        {
            clonedHandles[i] = PolarsWrapper.CloneExpr(by[i].Handle);
        }

        var h = PolarsWrapper.LazyFrameBottomK(lfHandle, (uint)k, clonedHandles, reverse);
        return new LazyFrame(h);
    }

    /// <summary>
    /// Get the bottom k rows according to a single expression.
    /// </summary>
    public LazyFrame BottomK(int k, Expr by, bool reverse = false)
    {
        return BottomK(k, [by], [reverse]);
    }
    /// <summary>
    /// Get the bottom k rows according to a single column name.
    /// </summary>
    /// <param name="k"></param>
    /// <param name="colName"></param>
    /// <param name="reverse"></param>
    /// <returns></returns>
    public LazyFrame BottomK(int k, string colName, bool reverse = false)
    {
        return BottomK(k, Polars.Col(colName), reverse);
    }
    /// <summary>
    /// Limit the number of rows in the LazyFrame.
    /// </summary>
    /// <param name="n"></param>
    /// <returns></returns>
    public LazyFrame Limit(uint n)
    {
        var lfClone = CloneHandle();
        return new LazyFrame(PolarsWrapper.LazyLimit(lfClone, n));
    }
    /// <summary>
    /// Lazily unnest struct columns.
    /// <para>
    /// Currently uses a Selector to perform the unnesting.
    /// </para>
    /// </summary>
    /// <seealso cref="DataFrame.Unnest(string[], string?)"/>
    /// <example>
    /// <code>
    /// df.Lazy()
    ///   .Unnest("User")
    ///   .Collect();
    /// </code>
    /// </example>
    public LazyFrame Unnest(Selector selector,string? separator = null)
    {
        var lfClone = CloneHandle();
        var sClone = selector.CloneHandle();
        var h = PolarsWrapper.LazyFrameUnnest(lfClone, sClone,separator);
        return new LazyFrame(h);
    }
    /// <summary>
    /// Unnest specific struct columns by name.
    /// (Syntactic sugar for Unnest(Selector.Cols(...)))
    /// </summary>
    public LazyFrame Unnest(params string[] columns)
    {
        using var sel = Selector.Cols(columns);
        return Unnest(sel,null);
    }
    /// <summary>
    /// Drop selected columns by selector.
    /// </summary>
    /// <param name="selector"></param>
    /// <returns></returns>
    public LazyFrame Drop(Selector selector)
    {
        var lfClone = CloneHandle();
        var sClone = selector.CloneHandle();
        var h = PolarsWrapper.LazyFrameDrop(lfClone, sClone);
        return new LazyFrame(h);
    }
    /// <summary>
    /// Drop selected columns by column names.
    /// </summary>
    /// <param name="columns"></param>
    /// <returns></returns>
    public LazyFrame Drop(params string[] columns)
    {
        using var sel = Selector.Cols(columns);
        return Drop(sel);
    }
    /// <summary>
    /// Keep unique rows (stable) based on a subset of columns defined by a Selector.
    /// </summary>
    /// <param name="subset">Selector defining the subset of columns. If null, uses all columns.</param>
    /// <param name="keep">Strategy to keep duplicates (First, Last, Any, None).</param>
    public LazyFrame Unique(Selector? subset = null, UniqueKeepStrategy keep = UniqueKeepStrategy.First, bool maintainOrder=false)
        => new (PolarsWrapper.LazyUnique(CloneHandle(), subset?.CloneHandle()!, keep.ToNative(),maintainOrder));
    
    /// <summary>
    /// Keep unique rows based on specific column names.
    /// </summary>
    /// <param name="columns">A collection of column names to group by.</param>
    /// <param name="keep">Strategy to keep duplicates (First, Last, Any, None).</param>
    /// <param name="maintainOrder">Whether to maintain the original order of the rows (stable).</param>
    public LazyFrame Unique(
        IEnumerable<string> columns, 
        UniqueKeepStrategy keep = UniqueKeepStrategy.First, 
        bool maintainOrder = false)
    {
        var columnsArray = columns as string[] ?? [.. columns];
        
        if (columnsArray.Length == 0)
        {
            return Unique(subset: null, keep, maintainOrder);
        }

        using var selector = Selector.Cols(columnsArray);
        
        return Unique(selector, keep, maintainOrder);
    }
    
    /// <summary>
    /// Explode list-like columns into multiple rows.
    /// </summary>
    /// <param name="selector"></param>
    /// <param name="emptyAsNull">
    /// If <c>true</c>, empty lists are exploded into a single <c>null</c> value. 
    /// If <c>false</c>, rows with empty lists are removed from the result.
    /// </param>
    /// <param name="keepNulls">
    /// If <c>true</c>, <c>null</c> values in the column are preserved as <c>null</c> in the result. 
    /// If <c>false</c>, rows with <c>null</c> values are removed.
    /// </param>
    /// <returns></returns>
    public LazyFrame Explode(Selector selector,bool emptyAsNull=true,bool keepNulls=true)
    {
        var lfClone = CloneHandle();
        var sClone = selector.CloneHandle();
        var h = PolarsWrapper.LazyExplode(lfClone, sClone,emptyAsNull,keepNulls);
        return new LazyFrame(h);
    }
    /// <summary>
    /// Explode list-like columns into multiple rows.
    /// </summary>
    /// <param name="columns"></param>
    /// <returns></returns>
    public LazyFrame Explode(params string[] columns)
    {
        using var sel = Selector.Cols(columns);
        return Explode(sel);
    }

    // ==========================================
    // Reshaping
    // ==========================================
    /// <summary>
    /// Pivot the LazyFrame.
    /// <para>
    /// <b>Important:</b> Lazy pivot requires an eager <paramref name="onColumns"/> DataFrame 
    /// to determine the output schema (column names) during the planning phase.
    /// </para>
    /// </summary>
    /// <param name="index">Selector for the index column(s) (the rows).</param>
    /// <param name="columns">Selector for the column(s) to pivot (the new column headers).</param>
    /// <param name="values">Selector for the value column(s) to populate the cells.</param>
    /// <param name="onColumns">
    /// An <b>Eager DataFrame</b> containing the unique values of the <paramref name="columns"/>.
    /// <br/>This is strictly used for schema inference.
    /// </param>
    /// <param name="aggregateExpr">Optional expression to aggregate the values. If null, uses <paramref name="aggregateFunction"/>.</param>
    /// <param name="aggregateFunction">Aggregation function to use if <paramref name="aggregateExpr"/> is null. Default is First.</param>
    /// <param name="maintainOrder">Sort the result by the index column.</param>
    /// <param name="separator">Separator used to combine column names when multiple value columns are selected.</param>
    /// <returns>A new LazyFrame with the pivot operation applied.</returns>
    public LazyFrame Pivot(
        Selector index,
        Selector columns,
        Selector values,
        DataFrame onColumns,
        Expr? aggregateExpr = null,
        PivotAgg aggregateFunction = PivotAgg.First,
        bool maintainOrder = true,
        string? separator = null)
    {
        using var indexH = index.CloneHandle();
        using var columnsH = columns.CloneHandle(); 
        using var valuesH = values.CloneHandle();
        using var aggExprH = aggregateExpr?.CloneHandle();

        var h = PolarsWrapper.LazyPivot(
            Handle,
            columnsH,   // on
            onColumns.Handle,  // onColumns (Eager DF Handle)
            indexH,     // index
            valuesH,    // values
            aggExprH,          // aggExpr (Wrapper handles null internally)
            aggregateFunction.ToNative(),
            maintainOrder,
            separator
        );

        return new LazyFrame(h);
    }

    // -------------------------------------------------------------------------
    // Overload: String Array (Syntax Sugar)
    // -------------------------------------------------------------------------

    /// <summary>
    /// Pivot the LazyFrame using column names.
    /// </summary>
    /// <param name="index">Column names to use as the index.</param>
    /// <param name="columns">Column names to use for the new column headers.</param>
    /// <param name="values">Column names to use for the values.</param>
    /// <param name="onColumns">
    /// An <b>Eager DataFrame</b> containing the unique values of the <paramref name="columns"/>.
    /// </param>
    /// <param name="aggregateFunction">Aggregation function. Default is First.</param>
    /// <param name="maintainOrder">Sort the result by the index column.</param>
    /// <param name="separator">Separator for generated column names.</param>
    public LazyFrame Pivot(
        string[] index,
        string[] columns,
        string[] values,
        DataFrame onColumns,
        PivotAgg aggregateFunction = PivotAgg.First,
        bool maintainOrder = true,
        string? separator = null)
    {
        using var sIndex = Selector.Cols(index);
        using var sColumns = Selector.Cols(columns);
        using var sValues = Selector.Cols(values);

        return Pivot(
            sIndex,
            sColumns,
            sValues,
            onColumns, 
            aggregateExpr: null,
            aggregateFunction: aggregateFunction,
            maintainOrder: maintainOrder,
            separator: separator
        );
    }

    /// <summary>
    /// Pivot the LazyFrame using column names and a custom aggregation expression.
    /// </summary>
    public LazyFrame Pivot(
        string[] index,
        string[] columns,
        string[] values,
        DataFrame onColumns,
        Expr aggregateExpr,
        bool maintainOrder = true,
        string? separator = null)
    {
        using var sIndex = Selector.Cols(index);
        using var sColumns = Selector.Cols(columns);
        using var sValues = Selector.Cols(values);

        return Pivot(
            sIndex,
            sColumns,
            sValues,
            onColumns,
            aggregateExpr: aggregateExpr,
            aggregateFunction: PivotAgg.First, // Ignored
            maintainOrder: maintainOrder,
            separator: separator
        );
    }
    /// <summary>
    /// Unpivot (Melt) the LazyFrame from wide to long format.
    /// </summary>
    /// <param name="index"></param>
    /// <param name="on"></param>
    /// <param name="variableName"></param>
    /// <param name="valueName"></param>
    /// <returns></returns>
    public LazyFrame Unpivot(Selector index, Selector? on, string variableName = "variable", string valueName = "value")
    {
        var lfClone = CloneHandle();
        var indexClone = index.CloneHandle();
        var onClone = on?.CloneHandle();
        return new LazyFrame(PolarsWrapper.LazyUnpivot(lfClone, indexClone, onClone, variableName, valueName));
    }
    /// <summary>
    /// Unpivot using column names (String Array overload).
    /// Wraps strings into Selectors automatically.
    /// </summary>
    public LazyFrame Unpivot(string[] index, string[]? on, string variableName = "variable", string valueName = "value")
    {
        using var sIndex = Selector.Cols(index);
        using var sOn = on is not null ? Selector.Cols(on) : null;

        return Unpivot(sIndex, sOn, variableName, valueName);
    }
    /// <summary>
    /// Unpivot using single column names (Convenience overload).
    /// </summary>
    public LazyFrame Unpivot(string index, string? on, string variableName = "variable", string valueName = "value")
    {
        string[]? onCols = on is null ? null : [on];
        return Unpivot([index], onCols, variableName, valueName);
    }
    /// <summary>
    /// Melt the DataFrame from wide to long format.
    /// </summary>
    /// <param name="index"></param>
    /// <param name="on"></param>
    /// <param name="variableName"></param>
    /// <param name="valueName"></param>
    /// <returns></returns>
    public LazyFrame Melt(Selector index, Selector? on, string variableName = "variable", string valueName = "value") 
        => Unpivot(index, on, variableName, valueName);
    /// <summary>
    /// Lazily concatenate multiple LazyFrames.
    /// <para>
    /// This adds a concat node to the query plan. 
    /// For vertical concatenation, schemas must align (or be capable of supertype unification).
    /// </para>
    /// </summary>
    /// <example>
    /// <code>
    /// LazyFrame.Concat(new[] { lf1, lf2 }, ConcatType.Vertical)
    ///          .Collect();
    /// </code>
    /// </example>
    public static LazyFrame Concat(
        IEnumerable<LazyFrame> lfs, 
        ConcatType how = ConcatType.Vertical, 
        bool rechunk = false, 
        bool parallel = true)
    {
        var lfClones = lfs.Select(l => l.CloneHandle()).ToArray();
        var handles = lfClones.Select(l => l).ToArray();
        return new LazyFrame(PolarsWrapper.LazyConcat(handles, how.ToNative(), rechunk, parallel));
    }

    // ==========================================
    // Join
    // ==========================================
    /// <summary>
    /// Lazily join with another LazyFrame.
    /// <para>
    /// Polars will optimize the join execution order. 
    /// Note: Both frames must be LazyFrames.
    /// </para>
    /// </summary>
    /// <seealso cref="DataFrame.Join(DataFrame, Expr[], Expr[], JoinType,string?,JoinValidation,JoinCoalesce,JoinMaintainOrder,JoinSide,bool,long?,ulong)"/>
    /// <example>
    /// <code>
    /// var lf1 = df1.Lazy();
    /// var lf2 = df2.Lazy();
    /// 
    /// // Lazy Left Join
    /// var joined = lf1.Join(lf2, Col("id"), Col("id"), JoinType.Left)
    ///                 .Collect();
    ///                 
    /// /* Output:
    /// shape: (3, 3)
    /// ┌─────┬─────────┬───────┐
    /// │ id  ┆ name    ┆ score │
    /// │ --- ┆ ---     ┆ ---   │
    /// │ i32 ┆ str     ┆ i32   │
    /// ╞═════╪═════════╪═══════╡
    /// │ 1   ┆ Alice   ┆ 90    │
    /// │ 2   ┆ Bob     ┆ 80    │
    /// │ 3   ┆ Charlie ┆ null  │
    /// └─────┴─────────┴───────┘
    /// */
    /// </code>
    /// </example>
    public LazyFrame Join(LazyFrame other,        
        Expr[] leftOn, 
        Expr[] rightOn, 
        JoinType how = JoinType.Inner,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        JoinSide joinSide = JoinSide.LetPolarsDecide,
        bool nullsEqual = false,
        long? sliceOffset = null,
        ulong sliceLen = 0)
    {
        var lOn = leftOn.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        var rOn = rightOn.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        var lfClone = CloneHandle();
        var otherClone = other.CloneHandle();
        return new LazyFrame(PolarsWrapper.Join(
            lfClone, 
            otherClone, 
            lOn, 
            rOn, 
            how.ToNative(),
            suffix,
            validation.ToNative(),
            coalesce.ToNative(),
            maintainOrder.ToNative(),
            joinSide.ToNative(),
            nullsEqual,
            sliceOffset,
            sliceLen
        ));
    }
    /// <summary>
    /// Join with another LazyFrame using column names.
    /// </summary>
    public LazyFrame Join(LazyFrame other,         
        string[] leftOn, 
        string[] rightOn, 
        JoinType how = JoinType.Inner,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        JoinSide joinSide = JoinSide.LetPolarsDecide,
        bool nullsEqual = false,
        long? sliceOffset = null,
        ulong sliceLen = 0)
    {
        var lExprs = leftOn.Select(Polars.Col).ToArray();
        var rExprs = rightOn.Select(Polars.Col).ToArray();
        return Join(
            other, 
            lExprs, 
            rExprs, 
            how, 
            suffix, 
            validation, 
            coalesce, 
            maintainOrder, 
            joinSide,
            nullsEqual, 
            sliceOffset, 
            sliceLen
        );
    }

    /// <summary>
    /// Join with another LazyFrame using a single column pair.
    /// </summary>
    public LazyFrame Join(LazyFrame other,
        string leftOn, 
        string rightOn, 
        JoinType how = JoinType.Inner,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        JoinSide joinSide = JoinSide.LetPolarsDecide,
        bool nullsEqual = false,
        long? sliceOffset = null,
        ulong sliceLen = 0)
    {
        return Join(
            other, 
            [leftOn], 
            [rightOn], 
            how, 
            suffix, 
            validation, 
            coalesce, 
            maintainOrder, 
            joinSide,
            nullsEqual, 
            sliceOffset, 
            sliceLen
        );
    }

    /// <summary>
    /// Perform an As-of join (also known as a time-series join).
    /// <para>
    /// This is similar to a left join except that we match on nearest key rather than equal keys.
    /// The join keys must be sorted.
    /// </para>
    /// </summary>
    /// <param name="other">The right LazyFrame to join with.</param>
    /// <param name="leftOn">Join key of the left LazyFrame. Must be sorted.</param>
    /// <param name="rightOn">Join key of the right LazyFrame. Must be sorted.</param>
    /// <param name="toleranceStr">
    /// Tolerance as a time duration string (e.g., "2h", "10s", "1d"). 
    /// Matches that are further away than this duration are discarded.
    /// </param>
    /// <param name="toleranceInt">
    /// Tolerance as a numeric integer (e.g., for integer-based timestamps or simple counters).
    /// </param>
    /// <param name="toleranceFloat">
    /// Tolerance as a floating point number.
    /// </param>
    /// <param name="strategy">
    /// The strategy to determine which value is "nearest" (Backward, Forward, or Nearest).
    /// Defaults to <see cref="AsofStrategy.Backward"/>.
    /// </param>
    /// <param name="leftBy">
    /// Columns to match exactly (equivalence join) before performing the as-of join. 
    /// Useful for joining separate time-series per group (e.g., by "Symbol").
    /// </param>
    /// <param name="rightBy">
    /// Columns to match exactly in the right DataFrame.
    /// </param>
    /// <param name="allowEq">
    /// If true, allow exact matches to be included in the result. 
    /// If false, a match must be strictly unequal (e.g. less than for Backward strategy) to the key.
    /// </param>
    /// <param name="checkSorted">
    /// Check if the join keys are sorted. 
    /// If false, the user must ensure keys are sorted; otherwise results are undefined (but execution is faster).
    /// </param>
    /// <param name="suffix">Suffix to append to columns with name conflicts. Defaults to "_right".</param>
    /// <param name="validation">Check if join keys are unique (mostly relevant for the 'by' columns).</param>
    /// <param name="coalesce">How to coalesce the join keys.</param>
    /// <param name="maintainOrder">How to maintain the order of the join.</param>
    /// <param name="joinSide">pecifies the strategy for the hash join build side.</param>
    /// <param name="nullsEqual">Consider nulls as equal.</param>
    /// <param name="sliceOffset">Slice the result starting at this offset (optimization).</param>
    /// <param name="sliceLen">Length of the slice to keep.</param>
    /// <example>
    /// <code>
    /// // Trades: Events happening at specific times
    /// var trades = DataFrame.FromColumns(new
    /// {
    ///     time = new[] { 10, 20, 30 },
    ///     stock = new[] { "A", "A", "A" }
    /// }).Lazy();
    /// 
    /// // Quotes: Price updates (irregular intervals)
    /// // 9->100, 15->101, 25->102, 40->103
    /// var quotes = DataFrame.FromColumns(new
    /// {
    ///     time = new[] { 9, 15, 25, 40 },
    ///     bid = new[] { 100, 101, 102, 103 }
    /// }).Lazy();
    /// 
    /// // Find the latest quote BEFORE or AT the trade time
    /// var asof = trades.JoinAsOf(
    ///     quotes, 
    ///     leftOn: Col("time"), 
    ///     rightOn: Col("time"),
    ///     strategy: AsofStrategy.Backward
    /// );
    /// 
    /// var df = asof.Collect();
    /// df.Show();
    /// /* Output:
    /// shape: (3, 3)
    /// ┌──────┬───────┬─────┐
    /// │ time ┆ stock ┆ bid │
    /// │ ---  ┆ ---   ┆ --- │
    /// │ i32  ┆ str   ┆ i32 │
    /// ╞══════╪═══════╪═════╡
    /// │ 10   ┆ A     ┆ 100 │ // Matches time 9
    /// │ 20   ┆ A     ┆ 101 │ // Matches time 15
    /// │ 30   ┆ A     ┆ 102 │ // Matches time 25
    /// └──────┴───────┴─────┘
    /// */
    /// </code>
    /// </example>
    internal LazyFrame JoinAsOf(
        LazyFrame other, 
        Expr leftOn, Expr rightOn, 
        string? toleranceStr = null,
        long? toleranceInt = null,
        double? toleranceFloat = null,
        AsofStrategy strategy = AsofStrategy.Backward,
        Expr[]? leftBy = null,
        Expr[]? rightBy = null,
        bool allowEq = true,
        bool checkSorted = true,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        JoinSide joinSide = JoinSide.LetPolarsDecide,
        bool nullsEqual = false,
        long? sliceOffset = null,
        ulong sliceLen = 0)
    {
        var lfClone = CloneHandle();
        var otherClone = other.CloneHandle();
        
        var lOn = PolarsWrapper.CloneExpr(leftOn.Handle);
        var rOn = PolarsWrapper.CloneExpr(rightOn.Handle);
        
        var lBy = leftBy?.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        var rBy = rightBy?.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();

        return new LazyFrame(PolarsWrapper.JoinAsOf(
            lfClone, otherClone,
            [lOn], [rOn], // Wrap single Expr into array
            lBy, rBy,
            strategy.ToNative(),
            toleranceStr,
            toleranceInt,
            toleranceFloat,
            allowEq,
            checkSorted,
            suffix,
            validation.ToNative(),
            coalesce.ToNative(),
            maintainOrder.ToNative(),
            joinSide.ToNative(),
            nullsEqual,
            sliceOffset,
            sliceLen
        ));
    }
    // 1. String Tolerance
    /// <inheritdoc cref="JoinAsOf(LazyFrame, Expr, Expr, string?, long?, double?, AsofStrategy, Expr[], Expr[], bool, bool, string?, JoinValidation, JoinCoalesce, JoinMaintainOrder,JoinSide ,bool, long?, ulong)"/>
    /// <param name="tolerance">
    /// Tolerance as a time duration string (e.g., "2h", "10s", "1d"). 
    /// Matches that are further away than this duration are discarded.
    /// </param>
    public LazyFrame JoinAsOf(LazyFrame other, Expr leftOn, Expr rightOn, string tolerance, AsofStrategy strategy = AsofStrategy.Backward, Expr[]? leftBy = null, Expr[]? rightBy = null)
        => JoinAsOf(other, leftOn, rightOn, toleranceStr: tolerance, strategy: strategy, leftBy: leftBy, rightBy: rightBy);

    // 2. TimeSpan Tolerance
    /// <inheritdoc cref="JoinAsOf(LazyFrame, Expr, Expr, string?, long?, double?, AsofStrategy, Expr[], Expr[], bool, bool, string?, JoinValidation, JoinCoalesce, JoinMaintainOrder, JoinSide,bool, long?, ulong)"/>
    /// <param name="tolerance">
    /// Tolerance as a <see cref="TimeSpan"/>. 
    /// Matches that are further away than this duration are discarded.
    /// </param>
    public LazyFrame JoinAsOf(LazyFrame other, Expr leftOn, Expr rightOn, TimeSpan tolerance, AsofStrategy strategy = AsofStrategy.Backward, Expr[]? leftBy = null, Expr[]? rightBy = null)
        => JoinAsOf(other, leftOn, rightOn, toleranceStr: DurationFormatter.ToPolarsString(tolerance), strategy: strategy, leftBy: leftBy, rightBy: rightBy);

    // 3. Int Tolerance (e.g. integer timestamps)
    /// <inheritdoc cref="JoinAsOf(LazyFrame, Expr, Expr, string?, long?, double?, AsofStrategy, Expr[], Expr[], bool, bool, string?, JoinValidation, JoinCoalesce, JoinMaintainOrder,JoinSide, bool, long?, ulong)"/>
    /// <param name="tolerance">
    /// Tolerance as a numeric integer (e.g., for integer-based timestamps or simple counters).
    /// </param>
    public LazyFrame JoinAsOf(LazyFrame other, Expr leftOn, Expr rightOn, long tolerance, AsofStrategy strategy = AsofStrategy.Backward, Expr[]? leftBy = null, Expr[]? rightBy = null)
        => JoinAsOf(other, leftOn, rightOn, toleranceInt: tolerance, strategy: strategy, leftBy: leftBy, rightBy: rightBy);

    // 4. Double Tolerance (e.g. float keys)
    /// <inheritdoc cref="JoinAsOf(LazyFrame, Expr, Expr, string?, long?, double?, AsofStrategy, Expr[], Expr[], bool, bool, string?, JoinValidation, JoinCoalesce, JoinMaintainOrder,JoinSide, bool, long?, ulong)"/>
    /// <param name="tolerance">
    /// Tolerance as a floating point number.
    /// </param>
    public LazyFrame JoinAsOf(LazyFrame other, Expr leftOn, Expr rightOn, double tolerance, AsofStrategy strategy = AsofStrategy.Backward, Expr[]? leftBy = null, Expr[]? rightBy = null)
        => JoinAsOf(other, leftOn, rightOn, toleranceFloat: tolerance, strategy: strategy, leftBy: leftBy, rightBy: rightBy);
    // ==========================================
    // GroupBy
    // ==========================================
    /// <summary>
    /// Start a lazy GroupBy operation.
    /// </summary>
    /// <remarks>
    /// Unlike <see cref="DataFrame.GroupBy(Expr[])"/> which returns a <see cref="GroupByBuilder"/>,
    /// this returns a <see cref="LazyGroupBy"/> object which allows constructing the aggregation plan.
    /// </remarks>
    /// <example>
    /// <code>
    /// df.Lazy()
    ///   .GroupBy("group")
    ///   .Agg(Col("val").Sum().Alias("sum_val"))
    ///   .Collect();
    ///   
    /// /* Output:
    /// shape: (2, 2)
    /// ┌───────┬─────────┐
    /// │ group ┆ sum_val │
    /// │ ---   ┆ ---     │
    /// │ str   ┆ i32     │
    /// ╞═══════╪═════════╡
    /// │ A     ┆ 3       │
    /// │ B     ┆ 7       │
    /// └───────┴─────────┘
    /// */
    /// </code>
    /// </example>
    public LazyGroupBy GroupBy(params Expr[] keys)
    {
        var lfClone = CloneHandle();
        
        return new LazyGroupBy(lfClone, keys);
    }
    /// <summary>
    /// Group by a single column name.
    /// <para>
    /// Explicit overload to ensure the string is treated as a Column, not a Literal.
    /// </para>
    /// </summary>
    public LazyGroupBy GroupBy(string name)
        => GroupBy([Polars.Col(name)]);

    /// <summary>
    /// Group by multiple column names.
    /// <para>
    /// Explicit overload to ensure strings are treated as Columns, not Literals.
    /// </para>
    /// </summary>
    public LazyGroupBy GroupBy(params string[] names)
    {
        var exprs = names.Select(n => Polars.Col(n)).ToArray();
        return GroupBy(exprs);
    }
    /// <summary>
    /// Lazily group based on a time index using dynamic windows.
    /// <para>
    /// This defines a dynamic groupby in the query plan.
    /// </para>
    /// </summary>
    /// <seealso cref="DataFrame.GroupByDynamic"/>
    /// <example>
    /// <code>
    /// df.Lazy()
    ///   .GroupByDynamic("time", every: TimeSpan.FromHours(1))
    ///   .Agg(Col("val").Sum().Alias("total"))
    ///   .Collect();
    /// </code>
    /// </example>
    public LazyDynamicGroupBy GroupByDynamic(
        string indexColumn,
        TimeSpan every,
        TimeSpan? period = null,
        TimeSpan? offset = null,
        Expr[]? by = null,
        Label label = Label.Left,
        bool includeBoundaries = false,
        ClosedWindow closedWindow = ClosedWindow.Left,
        StartBy startBy = StartBy.WindowBound
    )
    {
        string everyStr = DurationFormatter.ToPolarsString(every);
        string periodStr = DurationFormatter.ToPolarsString(period) ?? everyStr;
        string offsetStr = DurationFormatter.ToPolarsString(offset) ?? "0s";

        var keys = by ?? [];
        return new LazyDynamicGroupBy(
            CloneHandle(),
            indexColumn,
            everyStr,
            periodStr,
            offsetStr,
            keys,
            label, 
            includeBoundaries,
            closedWindow,
            startBy
        );
    }
    /// <summary>
    /// Generate a summary statistics DataFrame (count, mean, std, min, 25%, 50%, 75%, max).
    /// Similar to pandas/polars describe().
    /// Notice: This will collect LazyFrame once, but LazyFrame won't be consumed.
    /// </summary>
    public DataFrame Describe()
        => Clone().Collect().Describe();

    /// <summary>
    /// Returns the string representation of the LazyFrame (ASCII table).
    /// This allows Console.WriteLine(df) to print the table directly.
    /// </summary>
    public override string ToString()
    {
        if (Handle.IsInvalid) return "LazyFrame (Disposed)";
        return Clone().Collect().ToString();
    }

    /// <summary>
    /// Print the LazyFrame to Console.
    /// </summary>
    public void Show() => Console.WriteLine(ToString());

    // ==========================================
    // Execution (Collect)
    // ==========================================

    /// <summary>
    /// Execute the query plan and return a DataFrame.
    /// </summary>
    public DataFrame Collect(bool useStreaming=false)
        => new(PolarsWrapper.LazyCollect(Handle,useStreaming));

    IPolarsDataFrame IPolarsLazyFrame.Collect(bool useStreaming)
    {
        return this.Collect(useStreaming);
    }

    /// <summary>
    /// Execute the query plan using the streaming engine.
    /// </summary>
    public DataFrame CollectStreaming()
        => new(PolarsWrapper.CollectStreaming(Handle));
    /// <summary>
    /// Execute the query plan asynchronously and return a DataFrame.
    /// </summary>
    public async Task<DataFrame> CollectAsync(bool useStreaming = false, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var dfHandle = await PolarsWrapper.LazyCollectAsync(Handle, useStreaming, cancellationToken)
                                          .ConfigureAwait(false);

        return new DataFrame(dfHandle);
    }

    async Task<IPolarsDataFrame> IPolarsLazyFrame.CollectAsync(bool useStreaming, CancellationToken cancellationToken)
        => await CollectAsync(useStreaming, cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Dispose the LazyFrame and release native resources.
    /// </summary>
    public void Dispose()
    {
        Handle?.Dispose();
        GC.SuppressFinalize(this); 
    }
}