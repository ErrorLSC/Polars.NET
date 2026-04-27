using Polars.NET.Core;
using Polars.NET.Core.Helpers;
using Cs = Polars.CSharp.Polars.Selectors;

namespace Polars.CSharp;
public partial class LazyFrame : IDisposable, IPolarsLazyFrame
{
    /// <summary>
    /// Group by multiple expressions, strings, or literals.
    /// Default behavior: maintains group order.
    /// </summary>
    /// <remarks>
    /// Unlike <see cref="DataFrame.GroupBy(IntoExprColumn,bool)"/> which returns a <see cref="GroupByBuilder"/>,
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
    public LazyGroupBy GroupBy(IEnumerable<IntoExprColumn> keys, bool maintainOrder = true)
    {
        var exprs = keys.Select(k => k.Consume()).ToArray();
        
        return new LazyGroupBy(CloneHandle(), exprs, maintainOrder);
    }
    /// <summary>
    /// Group by a single key with explicit control over maintainOrder.
    /// </summary>
    public LazyGroupBy GroupBy(IntoExprColumn key, bool maintainOrder = true)
        => GroupBy([key], maintainOrder);
    /// <summary>
    /// Lazily group based on a time index using dynamic windows.
    /// <para>
    /// This defines a dynamic groupby in the query plan.
    /// </para>
    /// </summary>
    /// <example>
    /// <code>
    /// df.Lazy()
    ///   .GroupByDynamic("time", every: TimeSpan.FromHours(1))
    ///   .Agg(Col("val").Sum().Alias("total"))
    ///   .Collect();
    /// </code>
    /// </example>
    public LazyGroupBy GroupByDynamic(
        IntoSelector indexColumn, 
        IntoDuration every,
        IntoDuration? period = null,
        IntoDuration? offset = null,
        IEnumerable<IntoExprColumn>? groupBy = null, 
        Label label = Label.Left,
        bool includeBoundaries = false,
        ClosedInterval ClosedInterval = ClosedInterval.Left,
        StartBy startBy = StartBy.WindowBound
    )
    {
        using var idxSelector = indexColumn.Consume();
        var expandedCols = Cs.ExpandSelector(this, idxSelector);
        
        if (expandedCols.Length != 1)
        {
            throw new ArgumentException(
                $"The dynamic indexColumn must resolve to exactly ONE column. " +
                $"But your expression/selector resolved to {expandedCols.Length} column(s): " +
                $"[{(expandedCols.Length > 0 ? string.Join(", ", expandedCols) : "none")}]"
            );
        }
        
        string actualIndexCol = expandedCols[0];
        
        string everyStr = every.Value;
        string periodStr = period?.Value ?? everyStr;
        string offsetStr = offset?.Value ?? "0s";

        var keys = groupBy?.Select(k => k.Consume()).ToArray() ?? [];

        return new LazyGroupBy(
            CloneHandle(),
            actualIndexCol,
            everyStr,
            periodStr,
            offsetStr,
            keys,
            label, 
            includeBoundaries,
            ClosedInterval,
            startBy
        );
    }
    /// <summary>
    /// Lazily group based on a time index using rolling windows.
    /// </summary>
    public LazyGroupBy Rolling(
        IntoSelector indexColumn, 
        IntoDuration period,
        IntoDuration? offset = null,
        IEnumerable<IntoExprColumn>? groupBy = null,
        ClosedInterval ClosedInterval = ClosedInterval.Right)
    {
        using var idxSelector = indexColumn.Consume();
        var expandedCols = Cs.ExpandSelector(this, idxSelector);
        
        if (expandedCols.Length != 1)
        {
            throw new ArgumentException(
                $"The rolling indexColumn must resolve to exactly ONE column. " +
                $"But your selector resolved to {expandedCols.Length} column(s)."
            );
        }
        
        string actualIndexCol = expandedCols[0];
        
        string periodStr = period.Value;
        string actualOffset = offset?.Value ?? $"-{periodStr}";
        var keys = groupBy?.Select(k => k.Consume()).ToArray() ?? [];

        return new LazyGroupBy(
            CloneHandle(),
            actualIndexCol, 
            periodStr,
            actualOffset,
            keys,
            ClosedInterval
        );
    }
}

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Start a GroupBy operation to perform aggregations on groups of data.
    /// <para>
    /// This returns a <see cref="GroupByBuilder"/> which allows you to specify the aggregation functions 
    /// (like Sum, Min, Max, Count) using the <c>.Agg()</c> method.
    /// </para>
    /// </summary>
    /// <returns>A builder object to define aggregations.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     group = new[] { "A", "A", "B", "B", "B" },
    ///     val1 = new[] { 1, 2, 3, 4, 5 },
    ///     val2 = new[] { 10, 20, 10, 20, 30 }
    /// });
    /// 
    /// // Group by "group" and calculate:
    /// // 1. Sum of val1
    /// // 2. Max of val2 (aliased to "max_val2")
    /// // 3. Count of elements in the group
    /// var grouped = df.GroupBy(Col("group")).Agg(
    ///     Col("val1").Sum(),
    ///     Col("val2").Max().Alias("max_val2"),
    ///     Col("val1").Count().Alias("count")
    /// );
    /// 
    /// grouped.Sort("group").Show();
    /// /* Output:
    /// shape: (2, 4)
    /// ┌───────┬──────┬──────────┬───────┐
    /// │ group ┆ val1 ┆ max_val2 ┆ count │
    /// │ ---   ┆ ---  ┆ ---      ┆ ---   │
    /// │ str   ┆ i32  ┆ i32      ┆ u32   │
    /// ╞═══════╪══════╪══════════╪═══════╡
    /// │ A     ┆ 3    ┆ 20       ┆ 2     │
    /// │ B     ┆ 12   ┆ 30       ┆ 3     │
    /// └───────┴──────┴──────────┴───────┘
    /// */
    /// </code>
    /// </example>


    /// <summary>
    /// Group by a single key with explicit control over maintainOrder.
    /// </summary>
    public GroupByBuilder GroupBy(IntoExprColumn key, bool maintainOrder = true)
        => GroupBy([key], maintainOrder);
    /// <summary>
    /// The core GroupBy implementation. 
    /// All other overloads route here.
    /// </summary>
    public GroupByBuilder GroupBy(IEnumerable<IntoExprColumn> keys, bool maintainOrder = true)
    {
        var exprs = keys.Select(k => k.Consume()).ToArray();
        
        return new GroupByBuilder(this,Lazy().GroupBy(keys,maintainOrder));
    }
    /// <summary>
    /// Group based on a time index using dynamic windows (Rolling/Resampling).
    /// <para>
    /// This is essential for time-series analysis, allowing you to downsample data (e.g., 1-minute data to 1-hour bars).
    /// </para>
    /// </summary>
    /// <param name="indexColumn">The column containing time/date data (must be sorted).</param>
    /// <param name="every">The interval at which to start a new window (e.g., "1h", "1d"). Also known as the step size.</param>
    /// <param name="period">The duration of each window. If null, defaults to <paramref name="every"/>.</param>
    /// <param name="offset">Offset to shift the window start times.</param>
    /// <param name="groupBy">Optional extra columns to group by (e.g., group by "stock_symbol" AND time window).</param>
    /// <param name="label">Which label to use for the window (Left boundary, Right boundary, or Datapoint).</param>
    /// <param name="includeBoundaries">Whether to include the window boundaries in the output.</param>
    /// <param name="ClosedInterval">Which side of the window interval is closed (inclusive).</param>
    /// <param name="startBy">Strategy to determine the start of the first window.</param>
    /// <returns>A <see cref="GroupByBuilder"/> object to define aggregations.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     time = new[] 
    ///     { 
    ///         new DateTime(2024, 1, 1, 10, 0, 0),
    ///         new DateTime(2024, 1, 1, 10, 30, 0), // Inside 10:00-11:00 window
    ///         new DateTime(2024, 1, 1, 11, 0, 0), // Start of next window
    ///         new DateTime(2024, 1, 1, 11, 15, 0),
    ///         new DateTime(2024, 1, 2, 09, 0, 0)
    ///     },
    ///     val = new[] { 1, 2, 3, 4, 5 }
    /// });
    /// 
    /// // Group into 1-hour windows based on "time"
    /// var dynamicGrouped = df.GroupByDynamic(
    ///     indexColumn: "time", 
    ///     every: TimeSpan.FromHours(1)
    /// ).Agg(
    ///     Col("val").Sum().Alias("total_val"),
    ///     Col("val").Count().Alias("count")
    /// );
    /// 
    /// dynamicGrouped.Show();
    /// /* Output:
    /// shape: (3, 3)
    /// ┌─────────────────────┬───────────┬───────┐
    /// │ time                ┆ total_val ┆ count │
    /// │ ---                 ┆ ---       ┆ ---   │
    /// │ datetime[μs]        ┆ i32       ┆ u32   │
    /// ╞═════════════════════╪═══════════╪═══════╡
    /// │ 2024-01-01 10:00:00 ┆ 3         ┆ 2     │ // 10:00 + 10:30
    /// │ 2024-01-01 11:00:00 ┆ 7         ┆ 2     │ // 11:00 + 11:15
    /// │ 2024-01-02 09:00:00 ┆ 5         ┆ 1     │
    /// └─────────────────────┴───────────┴───────┘
    /// */
    /// </code>
    /// </example>
    public GroupByBuilder GroupByDynamic(
        IntoSelector indexColumn,
        IntoDuration every,
        IntoDuration? period = null,
        IntoDuration? offset = null,
        IEnumerable<IntoExprColumn>? groupBy = null, 
        Label label = Label.Left,
        bool includeBoundaries = false,
        ClosedInterval ClosedInterval = ClosedInterval.Left,
        StartBy startBy = StartBy.WindowBound
    )
        => new(this,Lazy().GroupByDynamic(indexColumn,every,period,offset,groupBy,label,includeBoundaries,ClosedInterval,startBy));
    /// <summary>
    /// Group based on a time index using rolling windows.
    /// </summary>
    public GroupByBuilder Rolling(
        IntoSelector indexColumn,
        IntoDuration period,
        IntoDuration? offset = null,
        IEnumerable<IntoExprColumn>? groupBy = null,
        ClosedInterval ClosedInterval = ClosedInterval.Left)
    {
        return new GroupByBuilder(
            this,Lazy().Rolling(
                indexColumn, 
                period, 
                offset, 
                groupBy, 
                ClosedInterval
            )
        );
    }
}