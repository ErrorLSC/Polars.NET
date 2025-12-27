#pragma warning disable CS1591 // 缺少对公共可见类型或成员的 XML 注释
using CoreEnums = Polars.NET.Core;

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

/// <summary>
/// Enum of DataTypeKind
/// </summary>
public enum DataTypeKind
{
    Boolean = 1 ,
    Int8 =2,
    Int16=3,
    Int32=4,
    Int64=5,
    UInt8=6,
    UInt16=7,
    UInt32=8,UInt64=9,
    Float32=10,Float64=11,
    String=12,Date=13,Datetime=14,Time=15,Duration=16,
    Binary=17,
    Null=18,
    Struct=19,List=20,Categorical=21,
    Decimal=22,
    Unknown = 0,
    SameAsInput=0
}

/// <summary>
/// Defines which boundary of the window to use for the label.
/// </summary>
public enum Label
{
    Left,
    Right,
    DataPoint
}

/// <summary>
/// Defines the strategy for determining the start of the window.
/// </summary>
public enum StartBy
{
    WindowBound,
    DataPoint,
    Monday,
    Tuesday,
    Wednesday,
    Thursday,
    Friday,
    Saturday,
    Sunday
}

/// <summary>
/// Defines which window boundaries are closed (inclusive).
/// </summary>
public enum ClosedWindow
{
    Left,
    Right,
    Both,
    None
}

internal static class EnumExtensions
{
    public static CoreEnums.PlTimeUnit ToNative(this TimeUnit unit) => unit switch
    {
        TimeUnit.Nanoseconds => CoreEnums.PlTimeUnit.Nanoseconds,
        TimeUnit.Microseconds => CoreEnums.PlTimeUnit.Microseconds,
        TimeUnit.Milliseconds => CoreEnums.PlTimeUnit.Milliseconds,
        TimeUnit.Second => CoreEnums.PlTimeUnit.Second,
        TimeUnit.Minute => CoreEnums.PlTimeUnit.Minute,
        TimeUnit.Hour => CoreEnums.PlTimeUnit.Hour,
        TimeUnit.Day => CoreEnums.PlTimeUnit.Day,
        TimeUnit.Month => CoreEnums.PlTimeUnit.Month,
        TimeUnit.Year => CoreEnums.PlTimeUnit.Year,
        _ => CoreEnums.PlTimeUnit.Nanoseconds
    };
    public static CoreEnums.PlJoinType ToNative(this JoinType type) => type switch
    {
        JoinType.Inner => CoreEnums.PlJoinType.Inner,
        JoinType.Left => CoreEnums.PlJoinType.Left,
        JoinType.Outer => CoreEnums.PlJoinType.Outer,
        JoinType.Cross => CoreEnums.PlJoinType.Cross,
        JoinType.Semi => CoreEnums.PlJoinType.Semi,
        JoinType.Anti => CoreEnums.PlJoinType.Anti,
        _ => CoreEnums.PlJoinType.Inner
    };

    //
    public static CoreEnums.PlPivotAgg ToNative(this PivotAgg agg) => agg switch
    {
        PivotAgg.First => CoreEnums.PlPivotAgg.First,
        PivotAgg.Sum => CoreEnums.PlPivotAgg.Sum,
        PivotAgg.Min => CoreEnums.PlPivotAgg.Min,
        PivotAgg.Max => CoreEnums.PlPivotAgg.Max,
        PivotAgg.Mean => CoreEnums.PlPivotAgg.Mean,
        PivotAgg.Median => CoreEnums.PlPivotAgg.Median,
        PivotAgg.Count => CoreEnums.PlPivotAgg.Count,
        PivotAgg.Len => CoreEnums.PlPivotAgg.Len,
        PivotAgg.Last => CoreEnums.PlPivotAgg.Last,
        _ => CoreEnums.PlPivotAgg.First
    };
    
    public static CoreEnums.PlConcatType ToNative(this ConcatType type) => type switch
    {
        ConcatType.Vertical => CoreEnums.PlConcatType.Vertical,
        ConcatType.Horizontal => CoreEnums.PlConcatType.Horizontal,
        ConcatType.Diagonal => CoreEnums.PlConcatType.Diagonal,
        _ => CoreEnums.PlConcatType.Vertical
    };
    internal static CoreEnums.PlLabel ToNative(this Label label) => label switch
    {
        Label.Left => CoreEnums.PlLabel.Left,
        Label.Right => CoreEnums.PlLabel.Right,
        Label.DataPoint => CoreEnums.PlLabel.DataPoint,
        _ => throw new ArgumentOutOfRangeException(nameof(label), label, null)
    };

    internal static CoreEnums.PlStartBy ToNative(this StartBy startBy) => startBy switch
    {
        StartBy.WindowBound => CoreEnums.PlStartBy.WindowBound,
        StartBy.DataPoint => CoreEnums.PlStartBy.DataPoint,
        StartBy.Monday => CoreEnums.PlStartBy.Monday,
        StartBy.Tuesday => CoreEnums.PlStartBy.Tuesday,
        StartBy.Wednesday => CoreEnums.PlStartBy.Wednesday,
        StartBy.Thursday => CoreEnums.PlStartBy.Thursday,
        StartBy.Friday => CoreEnums.PlStartBy.Friday,
        StartBy.Saturday => CoreEnums.PlStartBy.Saturday,
        StartBy.Sunday => CoreEnums.PlStartBy.Sunday,
        _ => throw new ArgumentOutOfRangeException(nameof(startBy), startBy, null)
    };

    internal static CoreEnums.PlClosedWindow ToNative(this ClosedWindow closed) => closed switch
    {
        ClosedWindow.Left => CoreEnums.PlClosedWindow.Left,
        ClosedWindow.Right => CoreEnums.PlClosedWindow.Right,
        ClosedWindow.Both => CoreEnums.PlClosedWindow.Both,
        ClosedWindow.None => CoreEnums.PlClosedWindow.None,
        _ => throw new ArgumentOutOfRangeException(nameof(closed), closed, null)
    };
}

