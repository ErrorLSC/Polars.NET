using Polars.NET.Core;

namespace Polars.CSharp;
public partial class LazyFrame : IDisposable, IPolarsLazyFrame
{
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
        using var sIndex = Cs.ByName(index);
        using var sColumns = Cs.ByName(columns);
        using var sValues = Cs.ByName(values);

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
        using var sIndex = Cs.ByName(index);
        using var sColumns = Cs.ByName(columns);
        using var sValues = Cs.ByName(values);

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
        using var sIndex = Cs.ByName(index);
        using var sOn = on is not null ? Cs.ByName(on) : null;

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
 
}

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Pivot the DataFrame using Selectors for column selection. 
    /// This is the most flexible pivot method.
    /// </summary>
    /// <param name="index">Selector for the index column(s).</param>
    /// <param name="columns">Selector for the column(s) to pivot.</param>
    /// <param name="values">Selector for the value column(s).</param>
    /// <param name="aggregateExpr">Optional expression to aggregate the values. If null, uses <paramref name="aggregateFunction"/>.</param>
    /// <param name="aggregateFunction">Aggregation function to use if <paramref name="aggregateExpr"/> is null. Default is First.</param>
    /// <param name="sortColumns">Sort the transposed columns by name.</param>
    /// <param name="maintainOrder">Keep the original order of the rows (index).</param>
    /// <param name="separator">Separator used to combine column names when multiple value columns are selected.</param>
    public DataFrame Pivot(
        Selector index, 
        Selector columns, 
        Selector values, 
        Expr? aggregateExpr = null, 
        PivotAgg aggregateFunction = PivotAgg.First,
        bool sortColumns = false, 
        bool maintainOrder = true,
        string? separator = null)
    {
        using var indexH = index.CloneHandle();
        using var columnsH = columns.CloneHandle();
        using var valuesH = values.CloneHandle();
        using var aggExprH = aggregateExpr?.CloneHandle(); 

        var h = PolarsWrapper.Pivot(
            Handle,
            indexH,
            columnsH,
            valuesH,
            aggExprH, 
            aggregateFunction.ToNative(),
            sortColumns,
            maintainOrder,
            separator
        );

        return new DataFrame(h);
    }
    /// <summary>
    /// Pivot the DataFrame from long to wide format.
    /// <para>
    /// This creates a new column for each unique value in the <paramref name="columns"/> argument.
    /// The values in the new columns come from the <paramref name="values"/> column.
    /// </para>
    /// </summary>
    /// <param name="index">Column names to use as the index (the rows).</param>
    /// <param name="columns">Column names to use for the new column headers.</param>
    /// <param name="values">Column names to use for the values in the cells.</param>
    /// <param name="aggregateFunction">Aggregation function to use if multiple values exist for an index/column pair. Default is First.</param>
    /// <param name="sortColumns">Sort the transposed columns by name. Default is by order of discovery.</param>
    /// <param name="maintainOrder">Keep the original order of the rows.</param>
    /// <param name="separator">Used as separator/delimiter in generated column names in case of multiple values columns.</param>
    /// <returns>A wide-format DataFrame.</returns>
    /// <example>
    /// <code>
    /// var dfLong = DataFrame.FromColumns(new
    /// {
    ///     date = new[] { "2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02" },
    ///     country = new[] { "US", "CN", "US", "CN" },
    ///     sales = new[] { 100, 200, 110, 220 }
    /// });
    /// 
    /// // Pivot: rows=date, cols=country, values=sales
    /// var pivoted = dfLong.Pivot(
    ///     index: new[] { "date" }, 
    ///     columns: new[] { "country" }, 
    ///     values: new[] { "sales" }
    /// );
    /// 
    /// pivoted.Show();
    /// /* Output:
    /// shape: (2, 3)
    /// ┌────────────┬─────┬─────┐
    /// │ date       ┆ US  ┆ CN  │
    /// │ ---        ┆ --- ┆ --- │
    /// │ str        ┆ i32 ┆ i32 │
    /// ╞════════════╪═════╪═════╡
    /// │ 2024-01-01 ┆ 100 ┆ 200 │
    /// │ 2024-01-02 ┆ 110 ┆ 220 │
    /// └────────────┴─────┴─────┘
    /// */
    /// </code>
    /// </example>
    public DataFrame Pivot(string[] index, string[] columns, string[] values, PivotAgg aggregateFunction = PivotAgg.First,bool sortColumns =false,bool maintainOrder = true,string? separator=null)
    {
        using var sIndex = Cs.ByName(index);
        using var sColumns = Cs.ByName(columns);
        using var sValues = Cs.ByName(values);

        return Pivot(
            sIndex, 
            sColumns, 
            sValues, 
            aggregateExpr: null, 
            aggregateFunction: aggregateFunction, 
            sortColumns: sortColumns, 
            maintainOrder: maintainOrder, 
            separator: separator
        );
    }
    /// <summary>
    /// Pivot a DataFrame with a custom aggregation expression.
    /// </summary>
    /// <param name="index">Column names to use as index.</param>
    /// <param name="columns">Column names to use as columns.</param>
    /// <param name="values">Column names to use as values.</param>
    /// <param name="aggregateExpr">
    /// A custom expression to aggregate the values (e.g., <c>pl.ByName("val").Sum() * 100</c>).
    /// </param>
    /// <param name="sortColumns">
    /// Sort the transposed columns.
    /// </param>
    /// <param name="separator">
    /// Separator used to combine column names when multiple value columns are selected.
    /// </param>
    /// <param name="maintainOrder">Keep the original order of the rows.</param>
    /// <returns>Pivoted DataFrame.</returns>
    public DataFrame Pivot(
        string[] index,
        string[] columns,
        string[] values,
        Expr aggregateExpr,
        bool sortColumns = false,
        bool maintainOrder = true,
        string? separator = null)
    {
        using var sIndex = Cs.ByName(index);
        using var sColumns = Cs.ByName(columns);
        using var sValues = Cs.ByName(values);

        return Pivot(
            sIndex, 
            sColumns, 
            sValues, 
            aggregateExpr: aggregateExpr, 
            sortColumns: sortColumns, 
            maintainOrder: maintainOrder, 
            separator: separator
        );
    }
    /// <summary>
    /// Unpivot (Melt) the DataFrame from wide to long format.
    /// <para>
    /// This is the reverse of <see cref="Pivot(string[], string[], string[], PivotAgg, bool,bool, string?)"/>. It collapses multiple columns into key-value pairs.
    /// </para>
    /// </summary>
    /// <param name="index">Column names to keep as identifiers (id_vars).</param>
    /// <param name="on">Column names to unpivot/melt (value_vars).</param>
    /// <param name="variableName">Name for the new variable column (default "variable").</param>
    /// <param name="valueName">Name for the new value column (default "value").</param>
    /// <returns>A long-format DataFrame.</returns>
    /// <example>
    /// <code>
    /// // Using the 'pivoted' DataFrame from the Pivot example
    /// // shape: (2, 3) [date, US, CN]
    /// 
    /// // Unpivot back to long format
    /// var melted = pivoted.Unpivot(
    ///     index: new[] { "date" },
    ///     on: new[] { "CN", "US" },
    ///     variableName: "country",
    ///     valueName: "sales"
    /// );
    /// 
    /// melted.Sort(new[] { "date", "country" }).Show();
    /// /* Output:
    /// shape: (4, 3)
    /// ┌────────────┬─────────┬───────┐
    /// │ date       ┆ country ┆ sales │
    /// │ ---        ┆ ---     ┆ ---   │
    /// │ str        ┆ str     ┆ i32   │
    /// ╞════════════╪═════════╪═══════╡
    /// │ 2024-01-01 ┆ CN      ┆ 200   │
    /// │ 2024-01-01 ┆ US      ┆ 100   │
    /// │ 2024-01-02 ┆ CN      ┆ 220   │
    /// │ 2024-01-02 ┆ US      ┆ 110   │
    /// └────────────┴─────────┴───────┘
    /// */
    /// </code>
    /// </example>
    public DataFrame Unpivot(string[] index, string[]? on, string variableName = "variable", string valueName = "value")
    {
        using var sIndex = Cs.ByName(index);
        using var sOn = on is not null ? Cs.ByName(on) : null;

        return Unpivot(sIndex, sOn, variableName, valueName);
    }
    /// <summary>
    /// Unpivot (Melt) the DataFrame from wide to long format.
    /// </summary>
    /// <param name="index"></param>
    /// <param name="on"></param>
    /// <param name="variableName"></param>
    /// <param name="valueName"></param>
    /// <returns></returns>
    public DataFrame Unpivot(Selector index, Selector? on, string variableName = "variable", string valueName = "value")
    {
        var lf = Lazy().Unpivot(index,on,variableName,valueName);
        return lf.Collect();
    }
    /// <summary>
    /// Alias for <see cref="Unpivot(string[], string[], string, string)"/>. Melts the DataFrame from wide to long format.
    /// </summary>
    /// <seealso cref="Unpivot(string[], string[], string, string)"/>
    public DataFrame Melt(string[] index, string[]? on, string variableName = "variable", string valueName = "value") 
        => Unpivot(index, on, variableName, valueName);

}