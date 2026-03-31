using Polars.NET.Core;
using Polars.NET.Core.Helpers;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;
public partial class LazyFrame : IDisposable, IPolarsLazyFrame
{
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
        => new(CloneHandle(), keys);

    /// <summary>
    /// Group by a single column name.
    /// <para>
    /// Explicit overload to ensure the string is treated as a Column, not a Literal.
    /// </para>
    /// </summary>
    public LazyGroupBy GroupBy(string name)
        => GroupBy([Pl.Col(name)]);

    /// <summary>
    /// Group by multiple column names.
    /// <para>
    /// Explicit overload to ensure strings are treated as Columns, not Literals.
    /// </para>
    /// </summary>
    public LazyGroupBy GroupBy(params string[] names)
        => GroupBy(names.Select(Pl.Col).ToArray());
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
    /// <param name="by">Expressions defining the columns to group by.</param>
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
    public GroupByBuilder GroupBy(params Expr[] by) => new (this, by);
    /// <summary>
    /// Start a GroupBy operation using column names (convenience overload).
    /// </summary>
    /// <param name="columns">Names of the columns to group by.</param>
    /// <returns>A builder object to define aggregations.</returns>
    /// <remarks>
    /// For more complex grouping logic (expressions), use <see cref="GroupBy(Expr[])"/>.
    /// </remarks>
    /// <seealso cref="GroupBy(Expr[])"/>
    public GroupByBuilder GroupBy(params string[] columns)
        => GroupBy(columns.Select(Pl.Col).ToArray());
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
    /// <param name="by">Optional extra columns to group by (e.g., group by "stock_symbol" AND time window).</param>
    /// <param name="label">Which label to use for the window (Left boundary, Right boundary, or Datapoint).</param>
    /// <param name="includeBoundaries">Whether to include the window boundaries in the output.</param>
    /// <param name="closedWindow">Which side of the window interval is closed (inclusive).</param>
    /// <param name="startBy">Strategy to determine the start of the first window.</param>
    /// <returns>A <see cref="DynamicGroupBy"/> object to define aggregations.</returns>
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
    public DynamicGroupBy GroupByDynamic(
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
        return new DynamicGroupBy(
            this,
            indexColumn,
            every,
            period,
            offset,
            by,
            label,
            includeBoundaries,
            closedWindow,
            startBy
        );
    }
}