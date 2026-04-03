#pragma warning disable CS1591
using CoreEnums = Polars.NET.Core;

namespace Polars.CSharp;
/// <summary>
/// Enums of JoinTypes
/// </summary>
public enum JoinType: byte
{
    Inner,Left, Outer,Cross,Semi,Anti,IEJoin
}
/// <summary>
/// Specifies the aggregation function for pivot operations.
/// </summary>
public enum PivotAgg : byte
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
    Array=23,
    Int128 = 24,
    UInt128=25,
    Float16=26,
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

/// <summary>
/// Strategy to handle dates that land on non-business days (weekends or holidays).
/// </summary>
public enum Roll
{
    /// <summary>
    /// Raise an error if the result is not a business day.
    /// </summary>
    Raise,
    
    /// <summary>
    /// Roll forward to the next business day.
    /// </summary>
    Forward,
    
    /// <summary>
    /// Roll backward to the previous business day.
    /// </summary>
    Backward
}

public enum QuantileMethod : byte
{
    Nearest = 0,
    Higher = 1,
    Lower = 2,
    Midpoint = 3,
    Linear = 4 // Default
}

public enum RankMethod : byte
{
    /// <summary>
    ///  The average of the ranks that would have been assigned to all the tied values is assigned to each value.
    /// </summary>
    Average = 0, // Default
    /// <summary>
    /// The minimum of the ranks that would have been assigned to all the tied values is assigned to each value. (This is also referred to as “competition” ranking.)
    /// </summary>
    Min = 1,
    /// <summary>
    /// The maximum of the ranks that would have been assigned to all the tied values is assigned to each value.
    /// </summary>
    Max = 2,
    /// <summary>
    /// Like ‘min’, but the rank of the next highest element is assigned the rank immediately after those assigned to the tied elements.
    /// </summary>
    Dense = 3,
    /// <summary>
    /// All values are given a distinct rank, corresponding to the order that the values occur in the Series.
    /// </summary>
    Ordinal = 4,
    /// <summary>
    /// Like ‘ordinal’, but the rank for ties is not dependent on the order that the values occur in the Series.
    /// </summary>
    Random = 5
}

public enum RollingRankMethod : byte
{
    /// <summary>
    ///  The average of the ranks that would have been assigned to all the tied values is assigned to each value.
    /// </summary>
    Average = 0, // Default
    /// <summary>
    /// The minimum of the ranks that would have been assigned to all the tied values is assigned to each value. (This is also referred to as “competition” ranking.)
    /// </summary>
    Min = 1,
    /// <summary>
    /// The maximum of the ranks that would have been assigned to all the tied values is assigned to each value.
    /// </summary>
    Max = 2,
    /// <summary>
    /// Like ‘min’, but the rank of the next highest element is assigned the rank immediately after those assigned to the tied elements.
    /// </summary>
    Dense = 3,
    /// <summary>
    /// All values are given a distinct rank, corresponding to the order that the values occur in the Series.
    /// </summary>
    Random = 4
}

public enum UniqueKeepStrategy : byte
{
    First = 0,
    Last = 1,
    Any = 2,
    None = 3
}

public enum JoinValidation: byte
{
    // Default
    ManyToMany = 0,
    ManyToOne = 1,
    OneToMany = 2,
    OneToOne = 3
}

public enum JoinCoalesce: byte
{
    // Default
    JoinSpecific = 0,
    CoalesceColumns = 1,
    KeepColumns = 2,
}

public enum JoinMaintainOrder: byte
{
    // Default
    None = 0,
    Left = 1,
    Right =2,
    LeftRight =3,
    RightLeft =4
}
/// <summary>
/// Specifies the strategy for the hash join build side.
/// <para>
/// In a hash join, one dataframe is loaded into memory (the "build side") to construct a hash table, 
/// while the other is streamed (the "probe side") to find matches.
/// </para>
/// <para>
/// Generally, the smaller dataframe should be the build side for optimal performance and memory usage.
/// </para>
/// </summary>
public enum JoinSide : byte
{
    /// <summary>
    /// Let Polars decide the best join strategy (Optimizer's choice).
    /// </summary>
    LetPolarsDecide = 0,
    
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

public enum AsofStrategy: byte
{
    Backward = 0,
    Forward = 1,
    Nearest = 2
}

public enum ParallelStrategy: byte
{
    Auto = 0,
    Columns = 1,
    RowGroups = 2,
    None =3
}

public enum CsvEncoding: byte
{
    UTF8 = 0,
    LossyUTF8 = 1,
}

public enum JsonFormat: byte
{
    Json = 0,
    JsonLines = 1,
}

public enum IpcCompression: byte
{
    None = 0,
    LZ4 = 1,
    ZSTD = 2
}

/// <summary>
/// Controls the file synchronization behavior when closing the file.
/// </summary>
public enum SyncOnClose : byte
{
    /// <summary>
    /// Don't call sync on close. (Default, fastest)
    /// </summary>
    None = 0,
    
    /// <summary>
    /// Sync only the file contents.
    /// </summary>
    Data = 1,
    
    /// <summary>
    /// Sync the file contents and the metadata. (Slowest, safest)
    /// </summary>
    All = 2
}

public enum ParquetCompression : byte
{
    None = 0,
    Snappy = 1,
    Gzip = 2,
    Brotli = 3,
    ZSTD = 4,
    Lz4Raw = 5
}

public enum QuoteStyle : byte
{
    /// <summary>
    /// Quote fields only when necessary (e.g. containing delimiter/quote). Default.
    /// </summary>
    Necessary = 0,
    
    /// <summary>
    /// Quote every field.
    /// </summary>
    Always = 1,
    
    /// <summary>
    /// Quote non-numeric fields.
    /// </summary>
    NonNumeric = 2,
    
    /// <summary>
    /// Never quote fields.
    /// </summary>
    Never = 3
}

public enum InterpolationMethod
{
    Linear = 0,
    Nearest = 1
}

/// <summary>
/// Supported cloud providers for IO operations.
/// </summary>
public enum CloudProvider : byte
{
    /// <summary>
    /// No cloud provider / Local file.
    /// </summary>
    None = 0,

    /// <summary>
    /// Amazon S3 or compatible services (MinIO, etc).
    /// </summary>
    Aws = 1,

    /// <summary>
    /// Azure Blob Storage / Data Lake Gen2.
    /// </summary>
    Azure = 2,

    /// <summary>
    /// Google Cloud Storage.
    /// </summary>
    Gcp = 3,

    /// <summary>
    /// Generic HTTP/HTTPS.
    /// </summary>
    Http = 4,

    /// <summary>
    /// Hugging Face datasets.
    /// </summary>
    HuggingFace = 5
}

/// <summary>
/// mode for saving delta lake table
/// </summary>
public enum DeltaSaveMode
{
    /// <summary>
    /// Append to the table.
    /// </summary>
    Append = 0,

    /// <summary>
    /// Overwrite the table.
    /// </summary>
    Overwrite = 1,

    /// <summary>
    /// Throw an error if the table already exists.
    /// </summary>
    ErrorIfExists = 2,

    /// <summary>
    /// Ignore the operation if the table already exists.
    /// </summary>
    Ignore = 3
}

public enum ExternalCompression: byte
{
    Uncompressed = 0,
    Gzip=1,
    ZSTD=2
}

public enum AvroCompression: byte
{
    Uncompressed = 0,
    Deflate=1,
    Snappy=2
}

/// <summary>
/// Defines the type of action to perform during a Delta Merge operation.
/// </summary>
public enum MergeActionType : byte
{
    MatchedUpdate = 0,
    MatchedDelete = 1,
    NotMatchedInsert = 2,
    NotMatchedBySourceDelete = 3
}

/// <summary>
/// Specifies the behavior when bulk ingesting a DataFrame into an ADBC database table.
/// </summary>
public enum AdbcIngestMode
{
    /// <summary>
    /// Creates a new table and inserts the data. 
    /// Fails if the target table already exists. (Default behavior)
    /// </summary>
    Create,

    /// <summary>
    /// Appends the data to an existing table. 
    /// Fails if the target table does not exist, or if the DataFrame schema doesn't match.
    /// </summary>
    Append,

    /// <summary>
    /// Drops the target table if it already exists, creates a new one, and inserts the data.
    /// Extremely useful for overriding temporary/staging tables.
    /// </summary>
    Replace
}

public enum CatalogTableType : byte
{
    Managed = 0,
    External = 1
}

public enum SearchSortedSide : byte { Any = 0, Left = 1, Right = 2 }

public enum SizeUnit
{
    Bytes,
    Kilobytes,
    Megabytes,
    Gigabytes,
    Terabytes
}

public enum UnstackDirection
{
    Vertical,
    Horizontal
}

public enum ClosedInterval
{
    Left,Right,Both,None
}

internal static class EnumExtensions
{
    internal static CoreEnums.PlDataType ToNative(this DataTypeKind kind) => kind switch
    {
        DataTypeKind.SameAsInput => CoreEnums.PlDataType.SameAsInput,
        DataTypeKind.Int8 => CoreEnums.PlDataType.Int8,
        DataTypeKind.UInt8 => CoreEnums.PlDataType.UInt8,
        DataTypeKind.Int16 => CoreEnums.PlDataType.Int16,
        DataTypeKind.UInt16 => CoreEnums.PlDataType.UInt16,
        DataTypeKind.Int32=> CoreEnums.PlDataType.Int32,
        DataTypeKind.UInt64 => CoreEnums.PlDataType.UInt64,
        DataTypeKind.Int128 => CoreEnums.PlDataType.Int128,
        DataTypeKind.UInt128 => CoreEnums.PlDataType.UInt128,
        DataTypeKind.Float16 => CoreEnums.PlDataType.Float16,
        DataTypeKind.Float32 => CoreEnums.PlDataType.Float32,
        DataTypeKind.Float64 => CoreEnums.PlDataType.Float64,
        DataTypeKind.Datetime => CoreEnums.PlDataType.Datetime,
        DataTypeKind.Date => CoreEnums.PlDataType.Date,
        DataTypeKind.Time => CoreEnums.PlDataType.Time,
        DataTypeKind.Duration => CoreEnums.PlDataType.Duration,
        DataTypeKind.List=> CoreEnums.PlDataType.List,
        DataTypeKind.Array => CoreEnums.PlDataType.Array,
        DataTypeKind.Struct => CoreEnums.PlDataType.Struct,
        DataTypeKind.Binary => CoreEnums.PlDataType.Binary,
        DataTypeKind.Decimal => CoreEnums.PlDataType.Decimal,
        DataTypeKind.Boolean => CoreEnums.PlDataType.Boolean,
        DataTypeKind.Categorical => CoreEnums.PlDataType.Categorical,
        DataTypeKind.Null => CoreEnums.PlDataType.Null,
        DataTypeKind.String => CoreEnums.PlDataType.String, 
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null)
    };
    internal static CoreEnums.PlTimeUnit ToNative(this TimeUnit unit) => unit switch
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
    internal static CoreEnums.PlJoinType ToNative(this JoinType type) => type switch
    {
        JoinType.Inner => CoreEnums.PlJoinType.Inner,
        JoinType.Left => CoreEnums.PlJoinType.Left,
        JoinType.Outer => CoreEnums.PlJoinType.Outer,
        JoinType.Cross => CoreEnums.PlJoinType.Cross,
        JoinType.Semi => CoreEnums.PlJoinType.Semi,
        JoinType.Anti => CoreEnums.PlJoinType.Anti,
        JoinType.IEJoin => CoreEnums.PlJoinType.IEJoin,
        _ => CoreEnums.PlJoinType.Inner
    };

    internal static CoreEnums.PlPivotAgg ToNative(this PivotAgg agg) => agg switch
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
    
    internal static CoreEnums.PlConcatType ToNative(this ConcatType type) => type switch
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

    internal static CoreEnums.PlRoll ToNative(this Roll roll) => roll switch
    {
        Roll.Backward => CoreEnums.PlRoll.Backward,
        Roll.Forward => CoreEnums.PlRoll.Forward,
        Roll.Raise => CoreEnums.PlRoll.Raise,
        _ => throw new ArgumentOutOfRangeException(nameof(roll), roll, null)
    };
    internal static CoreEnums.PlQuantileMethod ToNative(this QuantileMethod interpol) => interpol switch
    {
        QuantileMethod.Nearest => CoreEnums.PlQuantileMethod.Nearest,
        QuantileMethod.Higher => CoreEnums.PlQuantileMethod.Higher,
        QuantileMethod.Lower => CoreEnums.PlQuantileMethod.Lower,
        QuantileMethod.Midpoint => CoreEnums.PlQuantileMethod.Midpoint,
        QuantileMethod.Linear => CoreEnums.PlQuantileMethod.Linear,  
        _ => throw new ArgumentOutOfRangeException(nameof(interpol), interpol, null)
    };
    internal static CoreEnums.PlRankMethod ToNative(this RankMethod method) => method switch
    {
        RankMethod.Average => CoreEnums.PlRankMethod.Average,
        RankMethod.Min => CoreEnums.PlRankMethod.Min,
        RankMethod.Max => CoreEnums.PlRankMethod.Max,
        RankMethod.Dense => CoreEnums.PlRankMethod.Dense,
        RankMethod.Ordinal => CoreEnums.PlRankMethod.Ordinal,
        RankMethod.Random => CoreEnums.PlRankMethod.Random,
        _ => throw new ArgumentOutOfRangeException(nameof(method), method, null)
    };
    internal static CoreEnums.PlRollingRankMethod ToNative(this RollingRankMethod method) => method switch
    {
        RollingRankMethod.Average => CoreEnums.PlRollingRankMethod.Average,
        RollingRankMethod.Min => CoreEnums.PlRollingRankMethod.Min,
        RollingRankMethod.Max => CoreEnums.PlRollingRankMethod.Max,
        RollingRankMethod.Dense => CoreEnums.PlRollingRankMethod.Dense,
        RollingRankMethod.Random => CoreEnums.PlRollingRankMethod.Random,
        _ => throw new ArgumentOutOfRangeException(nameof(method), method, null)
    };
    internal static CoreEnums.PlUniqueKeepStrategy ToNative(this UniqueKeepStrategy strategy) => strategy switch
    {
        UniqueKeepStrategy.First => CoreEnums.PlUniqueKeepStrategy.First,
        UniqueKeepStrategy.Last => CoreEnums.PlUniqueKeepStrategy.Last,
        UniqueKeepStrategy.Any => CoreEnums.PlUniqueKeepStrategy.Any,
        UniqueKeepStrategy.None => CoreEnums.PlUniqueKeepStrategy.None,
        _ => throw new ArgumentOutOfRangeException(nameof(strategy), strategy, null)
    };
    internal static CoreEnums.PlJoinValidation ToNative(this JoinValidation validation) => validation switch
    {
        JoinValidation.ManyToMany => CoreEnums.PlJoinValidation.ManyToMany,
        JoinValidation.ManyToOne => CoreEnums.PlJoinValidation.ManyToOne,
        JoinValidation.OneToMany => CoreEnums.PlJoinValidation.OneToMany,
        JoinValidation.OneToOne => CoreEnums.PlJoinValidation.OneToOne,
        _ => throw new ArgumentOutOfRangeException(nameof(validation), validation, null)
    };
    internal static CoreEnums.PlJoinCoalesce ToNative(this JoinCoalesce coalesce) => coalesce switch
    {
        JoinCoalesce.JoinSpecific => CoreEnums.PlJoinCoalesce.JoinSpecific,
        JoinCoalesce.CoalesceColumns => CoreEnums.PlJoinCoalesce.CoalesceColumns,
        JoinCoalesce.KeepColumns => CoreEnums.PlJoinCoalesce.KeepColumns,
        _ => throw new ArgumentOutOfRangeException(nameof(coalesce), coalesce, null)
    };
    internal static CoreEnums.PlJoinMaintainOrder ToNative(this JoinMaintainOrder maintainOrder) => maintainOrder switch
    {
        JoinMaintainOrder.None => CoreEnums.PlJoinMaintainOrder.None,
        JoinMaintainOrder.Left => CoreEnums.PlJoinMaintainOrder.Left,
        JoinMaintainOrder.Right => CoreEnums.PlJoinMaintainOrder.Right,
        JoinMaintainOrder.LeftRight => CoreEnums.PlJoinMaintainOrder.LeftRight,
        JoinMaintainOrder.RightLeft => CoreEnums.PlJoinMaintainOrder.RightLeft,
        _ => throw new ArgumentOutOfRangeException(nameof(maintainOrder), maintainOrder, null)
    };
    internal static CoreEnums.PlJoinSide ToNative(this JoinSide joinSide) => joinSide switch
    {
        JoinSide.LetPolarsDecide => CoreEnums.PlJoinSide.None,
        JoinSide.PreferLeft => CoreEnums.PlJoinSide.PreferLeft,
        JoinSide.ForceLeft => CoreEnums.PlJoinSide.ForceLeft,
        JoinSide.PreferRight => CoreEnums.PlJoinSide.PreferRight,
        JoinSide.ForceRight => CoreEnums.PlJoinSide.ForceRight,
        _ => throw new ArgumentOutOfRangeException(nameof(joinSide), joinSide, null)
    };
    internal static CoreEnums.PlAsofStrategy ToNative(this AsofStrategy strategy) => strategy switch
    {
        AsofStrategy.Backward => CoreEnums.PlAsofStrategy.Backward,
        AsofStrategy.Forward => CoreEnums.PlAsofStrategy.Forward,
        AsofStrategy.Nearest => CoreEnums.PlAsofStrategy.Nearest,
        _ => throw new ArgumentOutOfRangeException(nameof(strategy), strategy, null)
    };
    internal static CoreEnums.PlParallelStrategy ToNative(this ParallelStrategy strategy) => strategy switch
    {
        ParallelStrategy.Auto => CoreEnums.PlParallelStrategy.Auto,
        ParallelStrategy.Columns => CoreEnums.PlParallelStrategy.Columns,
        ParallelStrategy.RowGroups => CoreEnums.PlParallelStrategy.RowGroups,
        ParallelStrategy.None => CoreEnums.PlParallelStrategy.None,
        _ => throw new ArgumentOutOfRangeException(nameof(strategy), strategy, null)
    };
    internal static CoreEnums.PlCsvEncoding ToNative(this CsvEncoding encoding) => encoding switch
    {
        CsvEncoding.UTF8 => CoreEnums.PlCsvEncoding.UTF8,
        CsvEncoding.LossyUTF8 => CoreEnums.PlCsvEncoding.LossyUTF8,
        _ => throw new ArgumentOutOfRangeException(nameof(encoding), encoding, null)
    };
    internal static CoreEnums.PlJsonFormat ToNative(this JsonFormat jsonFormat) => jsonFormat switch
    {
        JsonFormat.Json => CoreEnums.PlJsonFormat.Json,
        JsonFormat.JsonLines => CoreEnums.PlJsonFormat.JsonLines,
        _ => throw new ArgumentOutOfRangeException(nameof(jsonFormat), jsonFormat, null)
    };
    internal static CoreEnums.PlIpcCompression ToNative(this IpcCompression compression) => compression switch
    {
        IpcCompression.None => CoreEnums.PlIpcCompression.None,
        IpcCompression.LZ4 => CoreEnums.PlIpcCompression.LZ4,
        IpcCompression.ZSTD => CoreEnums.PlIpcCompression.ZSTD,
        _ => throw new ArgumentOutOfRangeException(nameof(compression), compression, null)
    };
    internal static CoreEnums.PlExternalCompression ToNative(this ExternalCompression compression) => compression switch
    {
        ExternalCompression.Uncompressed => CoreEnums.PlExternalCompression.Uncompressed,
        ExternalCompression.Gzip => CoreEnums.PlExternalCompression.Gzip,
        ExternalCompression.ZSTD => CoreEnums.PlExternalCompression.ZSTD,
        _ => throw new ArgumentOutOfRangeException(nameof(compression), compression, null)
    };
    internal static CoreEnums.PlSyncOnClose ToNative(this SyncOnClose syncOnClose) => syncOnClose switch
    {
        SyncOnClose.None => CoreEnums.PlSyncOnClose.None,
        SyncOnClose.Data => CoreEnums.PlSyncOnClose.Data,
        SyncOnClose.All => CoreEnums.PlSyncOnClose.All,
        _ => throw new ArgumentOutOfRangeException(nameof(syncOnClose), syncOnClose, null)
    };
    internal static CoreEnums.PlParquetCompression ToNative(this ParquetCompression compression) => compression switch
    {
        ParquetCompression.None => CoreEnums.PlParquetCompression.Uncompressed,
        ParquetCompression.Snappy => CoreEnums.PlParquetCompression.Snappy,
        ParquetCompression.Gzip => CoreEnums.PlParquetCompression.Gzip,
        ParquetCompression.Brotli => CoreEnums.PlParquetCompression.Brotli,
        ParquetCompression.ZSTD => CoreEnums.PlParquetCompression.ZSTD,
        ParquetCompression.Lz4Raw => CoreEnums.PlParquetCompression.Lz4Raw,
        _ => throw new ArgumentOutOfRangeException(nameof(compression), compression, null)
    };
    internal static CoreEnums.PlQuoteStyle ToNative(this QuoteStyle style) => style switch
    {
        QuoteStyle.Necessary => CoreEnums.PlQuoteStyle.Necessary,
        QuoteStyle.Always => CoreEnums.PlQuoteStyle.Always,
        QuoteStyle.NonNumeric => CoreEnums.PlQuoteStyle.NonNumeric,
        QuoteStyle.Never => CoreEnums.PlQuoteStyle.Never,
        _ => throw new ArgumentOutOfRangeException(nameof(style), style, null)
    };
    internal static CoreEnums.PlInterpolationMethod ToNative(this InterpolationMethod method) => method switch
    {
        InterpolationMethod.Nearest => CoreEnums.PlInterpolationMethod.Nearest,
        InterpolationMethod.Linear => CoreEnums.PlInterpolationMethod.Linear,
        _ => throw new ArgumentOutOfRangeException(nameof(method), method, null)
    };
    internal static CoreEnums.PlCloudProvider ToNative(this CloudProvider cloud) => cloud switch
    {
        CloudProvider.None => CoreEnums.PlCloudProvider.None,
        CloudProvider.Aws => CoreEnums.PlCloudProvider.Aws,
        CloudProvider.Azure => CoreEnums.PlCloudProvider.Azure,
        CloudProvider.Gcp => CoreEnums.PlCloudProvider.Gcp,
        CloudProvider.Http => CoreEnums.PlCloudProvider.Http,
        CloudProvider.HuggingFace => CoreEnums.PlCloudProvider.HuggingFace,
        _ => throw new ArgumentOutOfRangeException(nameof(cloud), cloud, null)
    };
    internal static CoreEnums.PlDeltaSaveMode ToNative(this DeltaSaveMode mode) => mode switch
    {
        DeltaSaveMode.Append => CoreEnums.PlDeltaSaveMode.Append,
        DeltaSaveMode.Overwrite => CoreEnums.PlDeltaSaveMode.Overwrite,
        DeltaSaveMode.ErrorIfExists => CoreEnums.PlDeltaSaveMode.ErrorIfExists,
        DeltaSaveMode.Ignore => CoreEnums.PlDeltaSaveMode.Ignore,
        _ => CoreEnums.PlDeltaSaveMode.Append
    };
    internal static CoreEnums.PlAvroCompression ToNative(this AvroCompression compression) => compression switch
    {
        AvroCompression.Uncompressed => CoreEnums.PlAvroCompression.Uncompressed,
        AvroCompression.Snappy => CoreEnums.PlAvroCompression.Snappy,
        AvroCompression.Deflate => CoreEnums.PlAvroCompression.Deflate,
        _ => throw new ArgumentOutOfRangeException(nameof(compression), compression, null)
    };
    internal static CoreEnums.PlMergeActionType ToNative(this MergeActionType condition) => condition switch
    {
        MergeActionType.MatchedUpdate => CoreEnums.PlMergeActionType.MatchedUpdate,
        MergeActionType.MatchedDelete => CoreEnums.PlMergeActionType.MatchedDelete,
        MergeActionType.NotMatchedInsert => CoreEnums.PlMergeActionType.NotMatchedInsert,
        MergeActionType.NotMatchedBySourceDelete => CoreEnums.PlMergeActionType.NotMatchedBySourceDelete,
        _ => throw new ArgumentOutOfRangeException(nameof(condition), condition, null)
    };
    internal static CoreEnums.PlCatalogTableType ToNative(this CatalogTableType tableType) => tableType switch
    {
        CatalogTableType.External => CoreEnums.PlCatalogTableType.External,
        CatalogTableType.Managed => CoreEnums.PlCatalogTableType.Managed,
        _ => throw new ArgumentOutOfRangeException(nameof(tableType), tableType, null)
    };
    internal static CoreEnums.PlSearchSortedSide ToNative(this SearchSortedSide side) => side switch
    {
        SearchSortedSide.Any => CoreEnums.PlSearchSortedSide.Any,
        SearchSortedSide.Left => CoreEnums.PlSearchSortedSide.Left,
        SearchSortedSide.Right => CoreEnums.PlSearchSortedSide.Right,
        _ => throw new ArgumentOutOfRangeException(nameof(side), side, null)
    };
    internal static CoreEnums.PlClosedInterval ToNative(this ClosedInterval closed) => closed switch
    {
        ClosedInterval.Both => CoreEnums.PlClosedInterval.Both,
        ClosedInterval.Left => CoreEnums.PlClosedInterval.Left,
        ClosedInterval.Right => CoreEnums.PlClosedInterval.Right,
        ClosedInterval.None => CoreEnums.PlClosedInterval.None,
        _ => throw new ArgumentOutOfRangeException(nameof(closed), closed, null)
    };
}

