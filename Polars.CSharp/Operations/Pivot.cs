#pragma warning disable CS1591
using Polars.NET.Core;
using Cs = Polars.CSharp.Polars.Selectors;

namespace Polars.CSharp;

public readonly struct IntoPivotHint
{
    private readonly DataFrame _df;
    private readonly bool _ownsDf;

    public static implicit operator IntoPivotHint(DataFrame df) => new(df, ownsDf: false);

    public static implicit operator IntoPivotHint(Series series) => new(series.ToFrame(), ownsDf: true);

    public static implicit operator IntoPivotHint(string[] expectedColumns)
    {
        var series = Series.From("on_columns", expectedColumns);
        return new(new DataFrame(series), ownsDf: true);
    }

    private IntoPivotHint(DataFrame df, bool ownsDf)
    {
        ArgumentNullException.ThrowIfNull(df);
        _df = df;
        _ownsDf = ownsDf;
    }

    public DataFrame Consume()
    {
        if (_ownsDf) return _df;
        
        return new DataFrame(PolarsWrapper.CloneDataFrame(_df.Handle)); 
    }
}

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
    /// <param name="on">Selector for the column(s) to pivot (the new column headers).</param>
    /// <param name="values">Selector for the value column(s) to populate the cells.</param>
    /// <param name="onColumns">
    /// A DataFrame / Series / string array containing the unique values of the <paramref name="onColumns"/>.
    /// <br/>This is strictly used for schema inference.
    /// </param>
    /// <param name="aggregateExpr">Optional expression to aggregate the values. If null, uses <paramref name="aggregateFunction"/>.</param>
    /// <param name="aggregateFunction">Aggregation function to use if <paramref name="aggregateExpr"/> is null. Default is First.</param>
    /// <param name="maintainOrder">Sort the result by the index column.</param>
    /// <param name="separator">Separator used to combine column names when multiple value columns are selected.</param>
    /// <param name="columnNaming">How resulting column names will be constructed.</param>
    /// <returns>A new LazyFrame with the pivot operation applied.</returns>
    public LazyFrame Pivot(
        IntoSelector on,                       
        IntoPivotHint onColumns,            
        IntoSelector? index = null,          
        IntoSelector? values = null,           
        Expr? aggregateExpr = null,            
        PivotAgg aggregateFunction = PivotAgg.First, 
        bool maintainOrder = false,        
        string separator = "_",
        PivotColumnNaming columnNaming = PivotColumnNaming.Auto)               
    {
        using var safeOn = on.Consume();
        using var safeIndex = index?.Consume();
        using var safeValues = values?.Consume();
        using var aggExprH = aggregateExpr?.CloneHandle();

        using var hintDf = onColumns.Consume();

        var hIndex = safeIndex?.Handle ?? new SelectorHandle(); 
        var hValues = safeValues?.Handle ?? new SelectorHandle();

        var h = PolarsWrapper.LazyPivot(
            Handle,
            safeOn.CloneHandle(),    
            hintDf.Handle,           
            hIndex,                  
            hValues,                 
            aggExprH,                
            aggregateFunction.ToNative(),
            maintainOrder,
            separator,
            columnNaming.ToNative()
        );

        return new LazyFrame(h);
    }
    /// <summary>
    /// Pivot the LazyFrame using column names.
    /// </summary>
    /// <param name="index">Column names to use as the index.</param>
    /// <param name="on">Column names to use for the new column headers.</param>
    /// <param name="values">Column names to use for the values.</param>
    /// <param name="onColumns">
    /// A DataFrame / Series / string array containing the unique values of the <paramref name="onColumns"/>.
    /// <br/>This is strictly used for schema inference.
    /// </param>
    /// <param name="aggregateExpr">Optional expression to aggregate the values. If null, uses <paramref name="aggregateFunction"/>.</param>
    /// <param name="aggregateFunction">Aggregation function. Default is First.</param>
    /// <param name="maintainOrder">Sort the result by the index column.</param>
    /// <param name="separator">Separator for generated column names.</param>
    /// <param name="columnNaming">How resulting column names will be constructed.</param>
    public LazyFrame Pivot(
        IEnumerable<string> on,
        IntoPivotHint onColumns,
        IEnumerable<string>? index = null,
        IEnumerable<string>? values = null,
        Expr? aggregateExpr = null,
        PivotAgg aggregateFunction = PivotAgg.First,
        bool maintainOrder = false,
        string separator = "_",
        PivotColumnNaming columnNaming = PivotColumnNaming.Auto)
    {
        var onArr = on as string[] ?? [.. on];
        if (onArr.Length == 0) 
            throw new ArgumentException("The 'on' parameter cannot be empty.", nameof(on));
        
        using var onSel = Cs.ByName(onArr);

        var idxArr = index as string[] ?? index?.ToArray();
        using var idxSel = (idxArr != null && idxArr.Length > 0) ? Cs.ByName(idxArr) : null;
        var valArr = values as string[] ?? values?.ToArray();
        using var valSel = (valArr != null && valArr.Length > 0) ? Cs.ByName(valArr) : null;

        return Pivot(
            on: onSel, 
            onColumns: onColumns, 
            index: idxSel is not null ? (IntoSelector?)idxSel : null, 
            values: valSel is not null ? (IntoSelector?)valSel : null, 
            aggregateExpr: aggregateExpr, 
            aggregateFunction: aggregateFunction, 
            maintainOrder: maintainOrder, 
            separator: separator,
            columnNaming:columnNaming
        );
    }
    /// <summary>
    /// Unpivot (Melt) the LazyFrame from wide to long format.
    /// Supports mixing Selectors, Expressions, Types, or single Strings.
    /// Usage: lf.Unpivot("Date", Cs.Numeric()) or lf.Unpivot(Cs.StartsWith("id"), "Status")
    /// </summary>
    public LazyFrame Unpivot(
        IntoSelector index, 
        IntoSelector? on = null, 
        string variableName = "variable", 
        string valueName = "value")
    {
        using var safeIndex = index.Consume();
        using var safeOn = on?.Consume();

        var h = PolarsWrapper.LazyUnpivot(
            CloneHandle(), 
            safeIndex.CloneHandle(), 
            safeOn?.CloneHandle(), 
            variableName, 
            valueName
        );

        return new LazyFrame(h);
    }

    /// <summary>
    /// Bridge overload to support C# 12 collection expressions.
    /// Usage: lf.Unpivot(["Date", "Region"], ["Q1", "Q2", "Q3"])
    /// </summary>
    public LazyFrame Unpivot(
        IEnumerable<string> index, 
        IEnumerable<string>? on = null, 
        string variableName = "variable", 
        string valueName = "value")
    {
        var idxArr = index as string[] ?? [.. index];
        if (idxArr.Length == 0)
            throw new ArgumentException("The 'index' parameter cannot be empty.", nameof(index));

        using var idxSel = Cs.ByName(idxArr);

        var onArr = on as string[] ?? on?.ToArray();
        using var onSel = (onArr != null && onArr.Length > 0) ? Cs.ByName(onArr) : null;

        return Unpivot(
            index: idxSel, 
            on: onSel is not null ? (IntoSelector?)onSel : null, 
            variableName: variableName, 
            valueName: valueName
        );
    }
    
    /// <inheritdoc cref="Unpivot(IntoSelector, IntoSelector?, string, string)"/>
    public LazyFrame Melt(IntoSelector index, IntoSelector? on = null, string variableName = "variable", string valueName = "value") 
        => Unpivot(index, on, variableName, valueName);

    /// <inheritdoc cref="Unpivot(IEnumerable{string}, IEnumerable{string}?, string, string)"/>
    public LazyFrame Melt(IEnumerable<string> index, IEnumerable<string>? on = null, string variableName = "variable", string valueName = "value") 
        => Unpivot(index, on, variableName, valueName);
 
}

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Pivot the DataFrame using Selectors for column selection. 
    /// This is the most flexible pivot method.
    /// </summary>
    /// <param name="index">Selector for the index column(s).</param>
    /// <param name="on">Selector for the column(s) to pivot.</param>
    /// <param name="values">Selector for the value column(s).</param>
    /// <param name="aggregateExpr">Optional expression to aggregate the values. If null, uses <paramref name="aggregateFunction"/>.</param>
    /// <param name="aggregateFunction">Aggregation function to use if <paramref name="aggregateExpr"/> is null. Default is First.</param>
    /// <param name="maintainOrder">Keep the original order of the rows (index).</param>
    /// <param name="separator">Separator used to combine column names when multiple value columns are selected.</param>
    /// <param name="columnNaming">How resulting column names will be constructed.</param>
    public DataFrame Pivot(
        IntoSelector on,
        IntoSelector? index = null,
        IntoSelector? values = null,
        Expr? aggregateExpr = null,
        PivotAgg aggregateFunction = PivotAgg.First,
        bool maintainOrder = false,
        string separator = "_",
        PivotColumnNaming columnNaming = PivotColumnNaming.Auto)
    {
        using var safeOn = on.Consume();
        
        string[] onCols = Cs.ExpandSelector(this, safeOn);
        
        if (onCols.Length == 0)
            throw new ArgumentException("The 'on' selector did not match any columns.");

        using var hintDf = this.Select(onCols).Unique();
        using var onColsSelector = Cs.ByName(onCols);
        using var lf = Lazy().Pivot(
            on: onColsSelector,              
            onColumns: hintDf,       
            index: index,
            values: values,
            aggregateExpr: aggregateExpr,
            aggregateFunction: aggregateFunction,
            maintainOrder: maintainOrder,
            separator: separator,
            columnNaming:columnNaming
        );

        return lf.Collect();
    }
    /// <summary>
    /// Pivot the DataFrame from long to wide format.
    /// <para>
    /// This creates a new column for each unique value in the <paramref name="on"/> argument.
    /// The values in the new columns come from the <paramref name="values"/> column.
    /// </para>
    /// </summary>
    /// <param name="index">Column names to use as the index (the rows).</param>
    /// <param name="on">Column names to use for the new column headers.</param>
    /// <param name="values">Column names to use for the values in the cells.</param>
    /// <param name="aggregateExpr">
    /// A custom expression to aggregate the values (e.g., <c>pl.ByName("val").Sum() * 100</c>).
    /// </param>
    /// <param name="aggregateFunction">Aggregation function to use if multiple values exist for an index/column pair. Default is First.</param>
    /// <param name="maintainOrder">Keep the original order of the rows.</param>
    /// <param name="separator">Used as separator/delimiter in generated column names in case of multiple values columns.</param>
    /// <param name="columnNaming">How resulting column names will be constructed.</param>
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
    public DataFrame Pivot(
        IEnumerable<string> on,
        IEnumerable<string>? index = null,
        IEnumerable<string>? values = null,
        Expr? aggregateExpr = null,
        PivotAgg aggregateFunction = PivotAgg.First,
        bool maintainOrder = false,
        string separator = "_",
        PivotColumnNaming columnNaming = PivotColumnNaming.Auto)
    {
        var onArr = on as string[] ?? [.. on];
        if (onArr.Length == 0) 
            throw new ArgumentException("The 'on' parameter cannot be empty.", nameof(on));
        
        using var onSel = Cs.ByName(onArr);

        var idxArr = index as string[] ?? index?.ToArray();
        using var idxSel = (idxArr != null && idxArr.Length > 0) ? Cs.ByName(idxArr) : null;

        var valArr = values as string[] ?? values?.ToArray();
        using var valSel = (valArr != null && valArr.Length > 0) ? Cs.ByName(valArr) : null;

        return Pivot(
            on: onSel, 
            index: idxSel is not null ? (IntoSelector?)idxSel : null, 
            values: valSel is not null ? (IntoSelector?)valSel : null, 
            aggregateExpr: aggregateExpr, 
            aggregateFunction: aggregateFunction, 
            maintainOrder: maintainOrder, 
            separator: separator,
            columnNaming:columnNaming
        );
    }
    /// <summary>
    /// Unpivot (Melt) the DataFrame from wide to long format.
    /// <para>
    /// This is the reverse of <see cref="Pivot(IntoSelector, IntoSelector?, IntoSelector?, Expr?, PivotAgg, bool, string,PivotColumnNaming)"/>. It collapses multiple columns into key-value pairs.
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
    public DataFrame Unpivot(        
        IEnumerable<string> index, 
        IEnumerable<string>? on = null, 
        string variableName = "variable", 
        string valueName = "value")
    => Lazy().Unpivot(index,on,variableName,valueName).Collect();
    /// <summary>
    /// Unpivot (Melt) the DataFrame from wide to long format.
    /// </summary>
    /// <param name="index"></param>
    /// <param name="on"></param>
    /// <param name="variableName"></param>
    /// <param name="valueName"></param>
    /// <returns></returns>
    public DataFrame Unpivot(        
        IntoSelector index, 
        IntoSelector? on = null, 
        string variableName = "variable", 
        string valueName = "value")
        => Lazy().Unpivot(index,on,variableName,valueName).Collect();
    
    /// <inheritdoc cref="Unpivot(IntoSelector, IntoSelector?, string, string)"/>
    public DataFrame Melt(IntoSelector index, IntoSelector? on = null, string variableName = "variable", string valueName = "value") 
        => Unpivot(index, on, variableName, valueName);

    /// <inheritdoc cref="Unpivot(IEnumerable{string}, IEnumerable{string}?, string, string)"/>
    public DataFrame Melt(IEnumerable<string> index, IEnumerable<string>? on = null, string variableName = "variable", string valueName = "value") 
        => Unpivot(index, on, variableName, valueName);

}