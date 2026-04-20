using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;
public partial class LazyFrame : IDisposable, IPolarsLazyFrame
{
    /// <summary>
    /// Return the number of non-null elements for each column.
    /// </summary>
    /// <returns></returns>
    public LazyFrame Count()
        => Select(Pl.All().Count());
    /// <summary>
    /// Aggregate the columns in the Frame to their sum value.
    /// </summary>
    /// <returns></returns>
    public LazyFrame Sum()
        => Select(Pl.All().Sum());
    /// <summary>
    /// Aggregate the columns in the Frame to their maximum value.
    /// </summary>
    /// <returns></returns>
    public LazyFrame Max()
        => Select(Pl.All().Max());
    /// <summary>
    /// Aggregate the columns in the Frame to their minimum value.
    /// </summary>
    /// <returns></returns>
    public LazyFrame Min()
        => Select(Pl.All().Min());
    /// <summary>
    /// Aggregate the columns in the Frame to their mean value.
    /// </summary>
    /// <returns></returns>
    public LazyFrame Mean()
        => Select(Pl.All().Mean());
    /// <summary>
    /// Aggregate the columns in the Frame to their median value.
    /// </summary>
    /// <returns></returns>
    public LazyFrame Median()
        => Select(Pl.All().Median());
    /// <summary>
    /// Aggregate the columns in the Frame as the sum of their null value count.
    /// </summary>
    /// <returns></returns>
    public LazyFrame NullCount()
        => Select(Pl.All().NullCount());
    /// <summary>
    /// Aggregate the columns in the Frame to their standard deviation value.
    /// </summary>
    /// <param name="ddof">“Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1.</param>
    /// <returns></returns>
    public LazyFrame Std(int ddof=1)
        => Select(Pl.All().Std(ddof));
    /// <summary>
    /// Aggregate the columns in the Frame to their variance value.
    /// </summary>
    /// <param name="ddof">“Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1.</param>
    /// <returns></returns>
    public LazyFrame Var(int ddof=1)
        => Select(Pl.All().Var(ddof));

    /// <summary>
    /// Aggregate the columns in the Frame to their quantile value.
    /// </summary>
    /// <param name="quantile">Quantile between 0.0 and 1.0.</param>
    /// <param name="method">['nearest’, ‘higher’, ‘lower’, ‘midpoint’, ‘linear’] Interpolation method.</param>
    /// <returns></returns>
    public LazyFrame Quantile(double quantile, QuantileMethod method = QuantileMethod.Linear)
        => Select(Pl.All().Quantile(quantile,method));
}

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <inheritdoc cref="LazyFrame.Count"/>
    public DataFrame Count()
        => Lazy().Count().Collect();

    /// <inheritdoc cref="LazyFrame.Sum"/>
    public DataFrame Sum()
        => Lazy().Sum().Collect();
    /// <inheritdoc cref="Pl.SumHorizontal(IntoExprColumn[])"/>
    public Series SumHorizontal(bool ignoreNulls=true) => Select(Pl.SumHorizontal(ignoreNulls,Pl.All()))[0].Rename("sum");
    /// <inheritdoc cref="Pl.MaxHorizontal(IntoExprColumn[])"/>
    public Series MaxHorizontal() => Select(Pl.MaxHorizontal(Pl.All()))[0].Rename("max");

    /// <inheritdoc cref="LazyFrame.Max"/>
    public DataFrame Max()
        => Lazy().Max().Collect();

    /// <inheritdoc cref="LazyFrame.Min"/>
    public DataFrame Min()
        => Lazy().Min().Collect();


    /// <inheritdoc cref="Pl.MinHorizontal(IntoExprColumn[])"/>
    public Series MinHorizontal() => Select(Pl.MinHorizontal(Pl.All()))[0].Rename("min");

    /// <inheritdoc cref="LazyFrame.Mean"/>
    public DataFrame Mean()
        => Lazy().Mean().Collect();
    /// <inheritdoc cref="Pl.MeanHorizontal(IntoExprColumn[])"/>
    public Series MeanHorizontal(bool ignoreNulls=true) => Select(Pl.MeanHorizontal(ignoreNulls,Pl.All()))[0].Rename("mean");

    /// <inheritdoc cref="LazyFrame.Median"/>
    public DataFrame Median()
        => Lazy().Median().Collect();

    /// <inheritdoc cref="LazyFrame.NullCount"/>
    public DataFrame NullCount()
        => Lazy().NullCount().Collect();
    
    /// <inheritdoc cref="LazyFrame.Std"/>
    public DataFrame Std(int ddof=1)
        => Lazy().Std(ddof).Collect();

    /// <inheritdoc cref="LazyFrame.Var"/>
    public DataFrame Var(int ddof=1)
        => Lazy().Var(ddof).Collect();

    /// <inheritdoc cref="LazyFrame.Quantile"/>
    public DataFrame Quantile(double quantile,QuantileMethod method = QuantileMethod.Linear)
        => Lazy().Quantile(quantile,method).Collect();
}