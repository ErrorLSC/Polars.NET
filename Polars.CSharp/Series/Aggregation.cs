#pragma warning disable CS1591 
using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;

public partial class Series : IDisposable,IPolarsSeries
{
    internal T? ExtractScalar<T>(Expr expr) where T : struct
    {
        using var tempSeries = ApplyExpr(expr);
        return ExtractScalarFromSeries<T>(tempSeries);
    }

    internal T? ExtractScalar<T>(Func<Series> seriesProvider) where T : struct
    {
        using var tempSeries = seriesProvider();
        return ExtractScalarFromSeries<T>(tempSeries);
    }

    private T? ExtractScalarFromSeries<T>(Series tempSeries) where T : struct
    {
        if (tempSeries is null || tempSeries.Len() == 0)
        {
            return null; 
        }

        if (tempSeries.IsNullAt(0)) 
        {
            return null;
        }

        return tempSeries.GetValue<T>(0);
    }
    
    /// <summary>
    /// <inheritdoc cref="Expr.First" path="/summary"/>
    /// </summary>
    /// <returns>A new <see cref="Series"/> containing the first value (length 1).</returns>
    public Series First() => ApplyExpr(Pl.Col(Name).First());
    /// <summary>
    /// First series into scalar
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public T? First<T>() where T : struct => ExtractScalar<T>(Pl.Col(Name).First());
    /// <summary>
    /// <inheritdoc cref="Expr.Last" path="/summary"/>
    /// </summary>
    /// <returns>A new <see cref="Series"/> containing the last value (length 1).</returns>
    public Series Last() => ApplyExpr(Pl.Col(Name).Last());
    /// <summary>
    /// Last series into scalar
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public T? Last<T>() where T : struct => ExtractScalar<T>(Pl.Col(Name).Last());
    /// <summary>
    /// Sum series into 1 length series(Scalar)
    /// </summary>
    /// <returns></returns>
    public Series Sum() => new(PolarsWrapper.SeriesSum(Handle));
    /// <summary>
    /// Sum series into scalar
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public T? Sum<T>() where T : struct => ExtractScalar<T>(Pl.Col(Name).Sum());
    /// <summary>
    /// Mean series into 1 length series(Scalar)
    /// </summary>
    /// <returns></returns>
    public Series Mean() => new(PolarsWrapper.SeriesMean(Handle));
    /// <summary>
    /// Mean series into scalar
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public T? Mean<T>() where T : struct => ExtractScalar<T>(Pl.Col(Name).Mean());
    /// <summary>
    /// Min series into 1 length series(Scalar)
    /// </summary>
    /// <returns></returns>
    public Series Min() => new(PolarsWrapper.SeriesMin(Handle));
    /// <summary>
    /// Min series into scalar
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public T? Min<T>() where T : struct => ExtractScalar<T>(Pl.Col(Name).Min());
    /// <summary>
    /// Max series into 1 length series(Scalar)
    /// </summary>
    /// <returns></returns>
    public Series Max() => new(PolarsWrapper.SeriesMax(Handle));
    /// <summary>
    /// Max series into scalar
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public T? Max<T>() where T : struct => ExtractScalar<T>(Pl.Col(Name).Max());
    /// <summary>
    /// Get the maximum value in this Series, ordered by an expression.
    /// </summary>
    /// <param name="by">Column used to determine the largest element. Accepts expression input.</param>
    public Series MaxBy(IntoExpr by) => ApplyBinaryExpr(by, (left, right) => left.MaxBy(right));
    /// <summary>
    /// Get the maximum value in this Series, ordered by an expression.
    /// </summary>
    /// <param name="by">Column used to determine the largest element. Accepts expression input.</param>
    public T? MaxBy<T>(IntoExpr by) where T : struct => ExtractScalar<T>(() => ApplyBinaryExpr(by, (left, right) => left.MaxBy(right)));
    /// <summary>
    /// Get the minimum value in this Series, ordered by an expression.
    /// </summary>
    /// <param name="by">Column used to determine the largest element. Accepts expression input.</param>
    public Series MinBy(IntoExpr by) => ApplyBinaryExpr(by, (left, right) => left.MinBy(right));
    /// <summary>
    /// Get the minimum value in this Series, ordered by an expression.
    /// </summary>
    /// <param name="by">Column used to determine the smallest element. Accepts expression input.</param>
    public T? MinBy<T>(IntoExpr by) where T : struct => ExtractScalar<T>(() => ApplyBinaryExpr(by, (left, right) => left.MinBy(right)));
    /// <summary>
    /// Product series into 1 length series(Scalar)
    /// </summary>
    /// <returns></returns>
    public Series Product() => ApplyExpr(Pl.Col(Name).Product());
    /// <summary>
    /// Product series into scalar
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public T? Product<T>() where T : struct => ExtractScalar<T>(Pl.Col(Name).Product());
    /// <summary>
    /// Get the index of the maximum value.
    /// Returns null if the Series is empty or contains only null values.
    /// </summary>
    public long? ArgMax() => ExtractScalar<long>(Pl.Col(Name).ArgMax());

    /// <summary>
    /// Get the index of the minimum value.
    /// Returns null if the Series is empty or contains only null values.
    /// </summary>
    public long? ArgMin() => ExtractScalar<long>(Pl.Col(Name).ArgMin());
    /// <summary>
    /// Compute the most occurring value(s).Can return multiple Values.
    /// </summary>
    /// <param name="maintainOrder">Maintain order of data. This requires more work.</param>
    /// <returns></returns>
    public Series Mode(bool maintainOrder=false) => new(PolarsWrapper.SeriesMode(Handle,maintainOrder));
    /// <summary>
    /// <inheritdoc cref="Expr.Count()" path="/summary"/>
    /// </summary>
    /// <returns>A new <see cref="Series"/> containing the count of non-null values.</returns>
    public long Count() => ExtractScalar<long>(Pl.Col(Name).Count()) ?? 0L;
    /// <summary>
    /// Get the standard deviation of this Series.
    /// </summary>
    /// <param name="ddof">Delta Degrees of Freedom. The divisor used in calculations is N - ddof.</param>
    public T? Std<T>(int ddof = 1) where T : struct => ExtractScalar<T>(Pl.Col(Name).Std(ddof));
    /// <summary>
    /// Get the variance of this Series.
    /// </summary>
    /// <param name="ddof">Delta Degrees of Freedom. The divisor used in calculations is N - ddof.</param>
    public T? Var<T>(int ddof = 1) where T : struct => ExtractScalar<T>(Pl.Col(Name).Var(ddof));
    /// <summary>
    /// <inheritdoc cref="Expr.Median()" path="/summary"/>
    /// </summary>
    public T? Median<T>() where T : struct => ExtractScalar<T>(Pl.Col(Name).Median());
    /// <summary>
    /// Aggregate values into a list.
    /// Result is a Series with 1 row containing a List of all values.
    /// </summary>
    public Series Implode() => new(PolarsWrapper.SeriesImplode(Handle));
}