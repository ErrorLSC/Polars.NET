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
    List = 20,
    Categorical=21,
    Decimal=22,
    Array = 23,
    Int128= 24,
    UInt128 =25,
    Float16=26
}

public enum PlJoinType : byte
{
    Inner = 0,
    Left = 1,
    Outer = 2, 
    Cross = 3,
    Semi = 4,
    Anti = 5,
    IEJoin = 6
}

public enum PlPivotAgg: byte
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

public enum PlTimeUnit: byte
{
    Nanoseconds = 0,
    Microseconds = 1,
    Milliseconds = 2,
    Second = 3,
    Minute = 4,
    Hour = 5,
    Day = 6,
    Month =7,
    Year =8,
    All = 100
}
/// <summary>
/// Concat Type Enum
/// </summary>
public enum PlConcatType : byte
{
    Vertical = 0,
    Horizontal = 1,
    Diagonal = 2
}

public enum PlLabel
{
    Left = 0,
    Right = 1,
    DataPoint = 2
}

public enum PlEngine : byte
{
    Auto = 0,
    InMemory = 1,
    Streaming = 2,
    Gpu = 3
}

public enum PlStartBy : byte
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

public enum PlClosedWindow: byte
{
    Left = 0,
    Right = 1,
    Both = 2,
    None = 3
}

public enum PlNullBehavior: byte
{
    Ignore = 0,
    Drop = 1,
}

public enum PlSetOperation: byte
{
    Union = 0,
    Difference = 1,
    Intersection =2,
    SymmetricDifference=3
}

/// <summary>
/// Strategy to handle dates that land on non-business days (weekends or holidays).
/// </summary>
public enum PlRoll
{
    /// <summary>
    /// Raise an error if the result is not a business day.
    /// </summary>
    Raise = 0,
    
    /// <summary>
    /// Roll forward to the next business day.
    /// </summary>
    Forward = 1,
    
    /// <summary>
    /// Roll backward to the previous business day.
    /// </summary>
    Backward = 2
}

public enum PlRankMethod: byte
{
    Average = 0, // Default
    Min = 1,
    Max = 2,
    Dense = 3,
    Ordinal = 4,
    Random = 5,
}
public enum PlRollingRankMethod: byte
{
    Average = 0, // Default
    Min = 1,
    Max = 2,
    Dense = 3,
    Random = 4,
}
public enum PlQuantileMethod : byte
{
    Nearest = 0,
    Higher = 1,
    Lower = 2,
    Midpoint = 3,
    Linear = 4 // Default
}

public enum PlUniqueKeepStrategy : byte
{
    First = 0,
    Last = 1,
    Any = 2,
    None = 3
}

public enum PlJoinValidation: byte
{
    // Default
    ManyToMany = 0,
    ManyToOne = 1,
    OneToMany = 2,
    OneToOne = 3
}

public enum PlJoinCoalesce: byte
{
    // Default
    JoinSpecific = 0,
    CoalesceColumns = 1,
    KeepColumns = 2,
}

public enum PlJoinMaintainOrder: byte
{
    // Default
    None = 0,
    Left = 1,
    Right =2,
    LeftRight =3,
    RightLeft =4
}

public enum PlJoinSide : byte
{
    /// <summary>
    /// Let Polars decide the best join strategy (Optimizer's choice).
    /// </summary>
    None = 0,
    
    /// <summary>
    /// Prefer using the left side as the build side (hash table).
    /// Optimizer may override this if the right side is significantly smaller.
    /// </summary>
    PreferLeft = 1,
    
    /// <summary>
    /// Force using the left side as the build side.
    /// </summary>
    ForceLeft = 2,
    
    /// <summary>
    /// Prefer using the right side as the build side (hash table).
    /// Optimizer may override this if the left side is significantly smaller.
    /// </summary>
    PreferRight = 3,
    
    /// <summary>
    /// Force using the right side as the build side.
    /// </summary>
    ForceRight = 4
}

public enum PlAsofStrategy: byte  
{
    // Default
    Backward =0,
    Forward =1,
    Nearest =2
}

public enum PlParallelStrategy: byte
{
    // Default
    Auto=0,
    Columns=1,
    RowGroups=2,
    None=3
}

public enum PlCsvEncoding: byte
{
    UTF8=0,
    LossyUTF8=1    
}

public enum PlJsonFormat: byte
{
    Json =0,
    JsonLines=1
}

public enum PlIpcCompression : byte
{
    None = 0,
    LZ4 = 1,
    ZSTD = 2
}

public enum PlSyncOnClose : byte
{
    None = 0,
    Data = 1,
    All = 2
}

public enum PlParquetCompression : byte
{
    Uncompressed = 0,
    Snappy = 1,
    Gzip = 2,
    Brotli = 3,
    ZSTD = 4,
    Lz4Raw = 5
}

public enum PlQuoteStyle : byte
{
    Necessary = 0,
    Always = 1,
    NonNumeric = 2,
    Never = 3
}

public enum PlInterpolationMethod
{
    Linear = 0,
    Nearest = 1
}

public enum PlCloudProvider : byte
{
    None = 0,
    Aws = 1,
    Azure = 2,
    Gcp = 3,
    Http = 4,
    HuggingFace = 5
}

public enum PlDeltaSaveMode : byte
{
    Append = 0,
    Overwrite = 1,
    ErrorIfExists = 2,
    Ignore = 3
}

public enum PlExternalCompression : byte
{
    Uncompressed = 0,
    Gzip = 1,
    ZSTD = 2
}

public enum PlAvroCompression : byte
{
    Uncompressed = 0,
    Deflate = 1,
    Snappy = 2
}

/// <summary>
/// Defines the type of action to perform during a Delta Merge operation.
/// </summary>
public enum PlMergeActionType : byte
{
    MatchedUpdate = 0,
    MatchedDelete = 1,
    NotMatchedInsert = 2,
    NotMatchedBySourceDelete = 3
}

public enum PlCatalogTableType : byte
{
    Managed = 0,
    External = 1
}

public enum PlSearchSortedSide : byte { Any = 0, Left = 1, Right = 2 }

public enum PlClosedInterval: byte
{
    Left = 0,
    Right = 1,
    Both =2,
    None =3
}
public enum PlMissingColumnsPolicyType : byte { Insert = 0, Raise = 1, InsertWith = 2 }
public enum PlMissingColumnsPolicy : byte { Raise = 0, Insert = 1 }
public enum PlUpcastOrForbid : byte { Forbid = 0, Upcast = 1 }
public enum PlExtraColumnsPolicy : byte { Raise = 0, Ignore = 1 }

[Flags]
public enum PlSortStateFlags : byte
{
    NotSorted  = 0,
    IsSorted   = 1 << 0, // 001 (1)
    Descending = 1 << 1, // 010 (2)
    NullsLast  = 1 << 2  // 100 (4)
}