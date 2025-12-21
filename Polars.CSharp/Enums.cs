#pragma warning disable CS1591 // 缺少对公共可见类型或成员的 XML 注释
using Polars.NET.Core;

namespace Polars.CSharp;
/// <summary>
/// Enums of JoinTypes
/// </summary>
public enum JoinType
{
    Inner,Left, Outer,Cross,Semi,Anti
}
/// <summary>
/// Specifies the aggregation function for pivot operations.
/// </summary>
public enum PivotAgg
{
    First,Sum,Min,Max, Mean,Median,Count,Len,Last
}
/// <summary>
/// TimeUnit Enums
/// </summary>
public enum TimeUnit
{
    Nanoseconds = 0,
    Microseconds = 1,
    Milliseconds = 2,
    Second = 3,
    Minute = 4,
    Hour = 5,
    Day = 6,
    Month = 7,
    Year = 8
}
/// <summary>
/// Concat Type Enum
/// </summary>
public enum ConcatType
{
    Vertical,Horizontal,Diagonal
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