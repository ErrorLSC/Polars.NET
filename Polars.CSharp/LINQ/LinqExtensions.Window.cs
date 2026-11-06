namespace Polars.CSharp.Linq;

public static partial class LinqExtensions
{
    /// <summary>
    /// Shifts the values in a column by a specified offset, introducing nulls for the shifted positions.
    /// </summary>
    /// <typeparam name="T">The type of the column.</typeparam>
    /// <param name="column">The column to shift.</param>
    /// <param name="offset">The offset by which to shift the values.</param>
    /// <returns>The shifted column.</returns>
    public static T Shift<T>(this T column, long offset)
        => throw MarkerExceptionHelper.Throw();
    /// <inheritdoc cref="Shift{T}(T,long)"/>
    public static T Shift<T, TOffset>(this T column, TOffset offsetColumn)
        => throw MarkerExceptionHelper.Throw();
    /// <inheritdoc cref="Diff{T}(T,long,NullBehavior)"/>
    public static T Diff<T>(this T column)
        => throw MarkerExceptionHelper.Throw();
    /// <inheritdoc cref="Diff{T}(T,long,NullBehavior)"/>
    public static T Diff<T>(this T column, long n)
        => throw MarkerExceptionHelper.Throw();
    /// <summary>
    /// Computes the difference between the current value and the value n periods ago, with a specified null behavior.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="column">The column to compute differences for.</param>
    /// <param name="n">The number of periods to look back.</param>
    /// <param name="nullBehavior">The behavior for handling null values.</param>
    /// <returns>The column with differences computed.</returns>
    public static T Diff<T>(this T column, long n, NullBehavior nullBehavior)
        => throw MarkerExceptionHelper.Throw();
    /// <inheritdoc cref="Over{T, TPartition, TOrder}(T, TPartition, TOrder, bool)"/>
    public static T Over<T, TPartition>(this T expr, TPartition partitionBy)
        => throw MarkerExceptionHelper.Throw();
    /// <inheritdoc cref="Over{T, TPartition, TOrder}(T, TPartition, TOrder, bool)"/>
    public static T Over<T>(this T expr, params object[] partitionBy)
        => throw MarkerExceptionHelper.Throw();
    /// <summary>
    /// Computes a windowed expression over a specified partition and order, with an optional descending order.
    /// </summary>
    /// <typeparam name="T">The type of the expression.</typeparam>
    /// <typeparam name="TPartition">The type of the partitioning column(s).</typeparam>
    /// <typeparam name="TOrder">The type of the ordering column(s).</typeparam>
    /// <param name="expr">The expression to compute the windowed operation for.</param>
    /// <param name="partitionBy">The column(s) to partition by.</param>
    /// <param name="orderBy">The column(s) to order by.</param>
    /// <param name="descending">Indicates whether to sort in descending order.</param>
    /// <returns>The column with the windowed expression computed.</returns>
    public static T Over<T, TPartition, TOrder>(
        this T expr,
        TPartition partitionBy,
        TOrder orderBy,
        bool descending = false)
        => throw MarkerExceptionHelper.Throw();
        
    /// <summary>
    /// Computes the rank of values using the default Average method.
    /// </summary>
    public static double Rank<T>(this T column)
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Computes the rank of values with a specified sorting order.
    /// </summary>
    public static double Rank<T>(this T column, bool descending)
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Computes the rank of values using user-facing RankMethod.
    /// </summary>
    public static double Rank<T>(this T column, RankMethod method, bool descending = false)
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Computes the rank of values using user-facing RankMethod with seed.
    /// </summary>
    public static double Rank<T>(this T column, RankMethod method, bool descending, ulong? seed)
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Computes the percentage change between current and previous values.
    /// </summary>
    public static double? PctChange<T>(this T column, long n = 1)
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Computes the mean of the column.
    /// </summary>
    public static double Mean<T>(this T column)
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Computes the sum of the column.
    /// </summary>
    public static T Sum<T>(this T column) where T : struct
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Computes the sum of the nullable column.
    /// </summary>
    public static T? Sum<T>(this T? column) where T : struct
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Computes the mean of the nullable column.
    /// </summary>
    public static double Mean<T>(this T? column) where T : struct
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Computes the average of the column.
    /// </summary>
    public static double Average<T>(this T column) where T : struct
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Computes the average of the nullable column.
    /// </summary>
    public static double Average<T>(this T? column) where T : struct
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Counts the number of non-null values in the column.
    /// </summary>
    public static int Count<T>(this T column) where T : struct
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Counts the number of non-null values in the nullable column.
    /// </summary>
    public static int Count<T>(this T? column) where T : struct
        => throw MarkerExceptionHelper.Throw();
    /// <summary>
    /// Counts the number of non-null values in the column.
    /// </summary>
    public static int Count(this string? column)
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Finds the minimum value in the column.
    /// </summary>
    public static T Min<T>(this T column) where T : struct
        => throw MarkerExceptionHelper.Throw();
    /// <summary>
    /// Finds the minimum value in the nullable column.
    /// </summary>
    public static T? Min<T>(this T? column) where T : struct
        => throw MarkerExceptionHelper.Throw();
    /// <summary>
    /// Finds the minimum value in the string column.
    /// </summary>
    public static string? Min(this string? column)
        => throw MarkerExceptionHelper.Throw();
    /// <summary>
    /// Finds the maximum value in the column.
    /// </summary>
    public static T Max<T>(this T column) where T : struct
        => throw MarkerExceptionHelper.Throw();
    /// <summary>
    /// Finds the maximum value in the nullable column.
    /// </summary>
    public static T? Max<T>(this T? column) where T : struct
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Finds the maximum value in the string column.
    /// </summary>
    public static string? Max(this string? column)
        => throw MarkerExceptionHelper.Throw();
    /// <summary>
    /// Computes the sample standard deviation (ddof = 1 by default).
    /// Null values are skipped; the result is null when there are no more than ddof non-null values.
    /// </summary>
    public static double? Std<T>(this T column, byte ddof = 1)
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Computes the sample variance (ddof = 1 by default).
    /// Null values are skipped; the result is null when there are no more than ddof non-null values.
    /// </summary>
    public static double? Var<T>(this T column, byte ddof = 1)
        => throw MarkerExceptionHelper.Throw();
    /// <summary>
    /// Computes the cumulative sum of the column.
    /// </summary>
    public static T CumSum<T>(this T column, bool reverse = false)
        => throw MarkerExceptionHelper.Throw();
    /// <summary>
    /// Computes the cumulative maximum of the column.
    /// </summary>
    public static T CumMax<T>(this T column, bool reverse = false)
        => throw MarkerExceptionHelper.Throw();
    /// <summary>
    /// Computes the cumulative minimum of the column.
    /// </summary>
    public static T CumMin<T>(this T column, bool reverse = false)
        => throw MarkerExceptionHelper.Throw();
    /// <summary>
    /// Computes the cumulative product of the column.
    /// </summary>
    public static T CumProd<T>(this T column, bool reverse = false)
        => throw MarkerExceptionHelper.Throw();
    /// <summary>
    /// Computes the cumulative count of the column.
    /// </summary>
    public static long CumCount<T>(this T column, bool reverse = false)
        => throw MarkerExceptionHelper.Throw();
    /// <summary>
    /// Interpolates null values using the specified user-facing InterpolationMethod (defaults to Linear).
    /// </summary>
    public static T Interpolate<T>(this T column, InterpolationMethod method = InterpolationMethod.Linear)
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Interpolates null values based on the values of another column (e.g., timestamps or indices).
    /// </summary>
    public static T InterpolateBy<T, TBy>(this T column, TBy byColumn)
        => throw MarkerExceptionHelper.Throw();
    /// <summary>
    /// Computes the median value of the column.
    /// </summary>
    public static double? Median<T>(this T column)
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Computes the quantile of the column with the specified probability and method.
    /// </summary>
    public static double? Quantile<T>(
        this T column,
        double quantile,
        QuantileMethod method = QuantileMethod.Nearest)
        => throw MarkerExceptionHelper.Throw();
}
