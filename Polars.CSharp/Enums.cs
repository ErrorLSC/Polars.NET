using Polars.NET.Core;

namespace Polars.CSharp;
/// <summary>
/// Enums of JoinTypes
/// </summary>
public enum JoinType
{
    /// <summary>
    /// Inner Join
    /// </summary>
    Inner,
    /// <summary>
    /// Left Join
    /// </summary>
    Left,
    /// <summary>
    /// Full Join
    /// </summary>
    Outer,
    /// <summary>
    /// Cross Join
    /// </summary>
    Cross,
    /// <summary>
    /// Semi Join
    /// </summary>
    Semi,
    /// <summary>
    /// Anti Join
    /// </summary>
    Anti
}
/// <summary>
/// Specifies the aggregation function for pivot operations.
/// </summary>
public enum PivotAgg
{
    /// <summary>
    /// Selects the first value encountered in the group.
    /// </summary>
    First,

    /// <summary>
    /// Computes the sum of the values in the group.
    /// </summary>
    Sum, 

    /// <summary>
    /// Finds the minimum value in the group.
    /// </summary>
    Min, 

    /// <summary>
    /// Finds the maximum value in the group.
    /// </summary>
    Max, 

    /// <summary>
    /// Computes the arithmetic mean (average) of the values in the group.
    /// </summary>
    Mean, 

    /// <summary>
    /// Computes the median of the values in the group.
    /// </summary>
    Median, 

    /// <summary>
    /// Counts the number of non-null values in the group.
    /// </summary>
    Count, 

    /// <summary>
    /// Computes the length of the group (number of rows).
    /// </summary>
    Len, 

    /// <summary>
    /// Selects the last value encountered in the group.
    /// </summary>
    Last
}
/// <summary>
/// TimeUnit Enums
/// </summary>
public enum TimeUnit
{
    /// <summary>
    /// Nanoseconds
    /// </summary>
    Nanoseconds = 0,
    /// <summary>
    /// Microseconds
    /// </summary>
    Microseconds = 1,
    /// <summary>
    /// Milliseconds
    /// </summary>
    Milliseconds = 2,
    /// <summary>
    /// Seconds
    /// </summary>
    Second = 3,
    /// <summary>
    /// Minutes
    /// </summary>
    Minute = 4,
    /// <summary>
    /// Hours
    /// </summary>
    Hour=5,
    /// <summary>
    /// Days
    /// </summary>
    Day=6,
    /// <summary>
    /// Months
    /// </summary>
    Month=7,
    /// <summary>
    /// Years
    /// </summary>
    Year=8
}
/// <summary>
/// Concat Type Enum
/// </summary>
public enum ConcatType
{
    /// <summary>
    /// Vertical Concatenation
    /// </summary>
    Vertical,
    /// <summary>
    /// Horizontal Concatenation
    /// </summary>
    Horizontal,
    /// <summary>
    /// Diagonal Concatenation
    /// </summary>
    Diagonal
}
internal static class EnumExtensions
{
    public static PlTimeUnit ToNative(this TimeUnit unit) => unit switch
    {
        TimeUnit.Nanoseconds => PlTimeUnit.Nanoseconds,
        TimeUnit.Microseconds => PlTimeUnit.Microseconds,
        TimeUnit.Milliseconds => PlTimeUnit.Milliseconds,
        TimeUnit.Second => PlTimeUnit.Second,
        TimeUnit.Minute => PlTimeUnit.Minute,
        TimeUnit.Hour => PlTimeUnit.Hour,
        TimeUnit.Day => PlTimeUnit.Day,
        TimeUnit.Month => PlTimeUnit.Month,
        TimeUnit.Year => PlTimeUnit.Year,
        _ => PlTimeUnit.Nanoseconds
    };
    public static PlJoinType ToNative(this JoinType type) => type switch
    {
        JoinType.Inner => PlJoinType.Inner,
        JoinType.Left => PlJoinType.Left,
        JoinType.Outer => PlJoinType.Outer,
        JoinType.Cross => PlJoinType.Cross,
        JoinType.Semi => PlJoinType.Semi,
        JoinType.Anti => PlJoinType.Anti,
        _ => PlJoinType.Inner
    };

    //
    public static PlPivotAgg ToNative(this PivotAgg agg) => agg switch
    {
        PivotAgg.First => PlPivotAgg.First,
        PivotAgg.Sum => PlPivotAgg.Sum,
        PivotAgg.Min => PlPivotAgg.Min,
        PivotAgg.Max => PlPivotAgg.Max,
        PivotAgg.Mean => PlPivotAgg.Mean,
        PivotAgg.Median => PlPivotAgg.Median,
        PivotAgg.Count => PlPivotAgg.Count,
        PivotAgg.Len => PlPivotAgg.Len,
        PivotAgg.Last => PlPivotAgg.Last,
        _ => PlPivotAgg.First
    };
    
    public static PlConcatType ToNative(this ConcatType type) => type switch
    {
        ConcatType.Vertical => PlConcatType.Vertical,
        ConcatType.Horizontal => PlConcatType.Horizontal,
        ConcatType.Diagonal => PlConcatType.Diagonal,
        _ => PlConcatType.Vertical
    };
}