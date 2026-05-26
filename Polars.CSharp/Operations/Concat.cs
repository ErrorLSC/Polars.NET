using Polars.NET.Core;

namespace Polars.CSharp;
public partial class LazyFrame : IDisposable, IPolarsLazyFrame
{
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
}

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    private static DataFrame ConcatInternal(
        IEnumerable<DataFrame> dfs, 
        PlConcatType how, 
        bool checkDuplicates,
        bool strict = true,
        bool unitLengthAsScalar = false)
    {
        var dfList = dfs.ToList();
        if (dfList.Count == 0) return new DataFrame();

        var handles = dfList.Select(df => df.Clone().Handle).ToArray();

        var h = PolarsWrapper.Concat(handles, how, checkDuplicates,strict,unitLengthAsScalar);
        return new DataFrame(h);
    }
    /// <summary>
    /// Concatenate multiple DataFrames either vertically (union) or horizontally.
    /// </summary>
    /// <param name="dfs">A collection of DataFrames to concatenate.</param>
    /// <returns>A new combined DataFrame.</returns>
    /// <example>
    /// <code>
    /// var df1 = DataFrame.FromColumns(new
    /// {
    ///     a = new[] { 1, 2 },
    ///     b = new[] { 10, 20 }
    /// });
    /// 
    /// var df2 = DataFrame.FromColumns(new
    /// {
    ///     a = new[] { 3, 4 },
    ///     b = new[] { 30, 40 }
    /// });
    /// 
    /// // Vertical Concat (Union rows)
    /// var catVertical = DataFrame.ConcatVertical(new[] { df1, df2 });
    /// catVertical.Show();
    /// /* Output:
    /// shape: (4, 2)
    /// ┌─────┬─────┐
    /// │ a   ┆ b   │
    /// │ --- ┆ --- │
    /// │ i32 ┆ i32 │
    /// ╞═════╪═════╡
    /// │ 1   ┆ 10  │
    /// │ 2   ┆ 20  │
    /// │ 3   ┆ 30  │
    /// │ 4   ┆ 40  │
    /// └─────┴─────┘
    /// */
    /// 
    /// // Horizontal Concat (Append columns)
    /// var df3 = DataFrame.FromColumns(new { c = new[] { "x", "y" } });
    /// var catHorizontal = DataFrame.ConcatHorizontal(new[] { df1, df3 });
    /// catHorizontal.Show();
    /// /* Output:
    /// shape: (2, 3)
    /// ┌─────┬─────┬─────┐
    /// │ a   ┆ b   ┆ c   │
    /// │ --- ┆ --- ┆ --- │
    /// │ i32 ┆ i32 ┆ str │
    /// ╞═════╪═════╪═════╡
    /// │ 1   ┆ 10  ┆ x   │
    /// │ 2   ┆ 20  ┆ y   │
    /// └─────┴─────┴─────┘
    /// */
    /// </code>
    /// </example>
    public static DataFrame Concat(IEnumerable<DataFrame> dfs)
        =>ConcatInternal(dfs, PlConcatType.Vertical, true);
    /// <summary>
    /// Horizontal concatenation of DataFrames.
    /// </summary>
    /// <param name="dfs">DataFrames to concat.</param>
    /// <param name="checkDuplicates">
    /// If true, check that the column names are unique. 
    /// If multiple columns have the same name, they will be dropped.
    /// </param>
    /// <param name="strict">For Horizontal: if true, error on height mismatch.</param>
    /// <param name="unitLengthAsScalar">For Horizontal: if true, broadcast length-1 DataFrames to match height.</param>
    public static DataFrame ConcatHorizontal(
        IEnumerable<DataFrame> dfs,
        bool checkDuplicates = true,
        bool strict = true,
        bool unitLengthAsScalar = false)
            => ConcatInternal(dfs, PlConcatType.Horizontal, checkDuplicates, strict,unitLengthAsScalar);
    /// <summary>
    /// Diagonal concatenation of DataFrames.
    /// </summary>
    public static DataFrame ConcatDiagonal(IEnumerable<DataFrame> dfs)
        => ConcatInternal(dfs, PlConcatType.Diagonal, true);
}