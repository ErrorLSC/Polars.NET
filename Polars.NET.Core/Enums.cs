namespace Polars.NET.Core;

public enum PlDataType : int
{
    Unknown = 0,
    SameAsInput = 0,
    Boolean = 1,
    Int8 = 2,
    Int16 = 3,
    Int32 = 4,
    Int64 = 5,
    UInt8 = 6,
    UInt16 = 7,
    UInt32 = 8,
    UInt64 = 9,
    Float32 = 10,
    Float64 = 11,
    String = 12,
    Date = 13,
    Datetime = 14,
    Time = 15,
    Duration = 16,
    Binary = 17,
    Null = 18,
    Struct = 19,
    List = 20
}

public enum PlJoinType
{
    Inner = 0,
    Left = 1,
    Outer = 2, // Polars 0.50 叫 Full
    Cross = 3,
    Semi = 4,
    Anti = 5
}

// 对应 Pivot 的聚合方式
public enum PlPivotAgg
{
    First = 0,
    Sum = 1,
    Min = 2,
    Max = 3,
    Mean = 4,
    Median = 5,
    Count = 6,
    Len = 7,
    Last = 8
}

// 对应时间单位 (Cast 用)
public enum PlTimeUnit
{
    Nanoseconds = 0,
    Microseconds = 1,
    Milliseconds = 2,
    Second = 3,
    Minute = 4,
    Hour = 5,
    Day = 6,
    Month =7,
    Year =8
}
/// <summary>
/// Concat Type Enum
/// </summary>
public enum PlConcatType
{
    Vertical = 0,
    Horizontal = 1,
    Diagonal = 2
}

public enum Label
{
    Left = 0,
    Right = 1,
    DataPoint = 2
}

public enum StartBy
{
    WindowBound = 0,
    DataPoint = 1,
    Monday = 2,
    Tuesday = 3,
    Wednesday = 4,
    Thursday = 5,
    Friday = 6,
    Saturday = 7,
    Sunday = 8
}

public enum ClosedWindow
{
    Left = 0,
    Right = 1,
    Both = 2,
    None = 3
}