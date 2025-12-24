namespace Polars.FSharp

open System
open Polars.NET.Core
open Apache.Arrow
open System.Collections.Generic
open Polars.NET.Core.Arrow
open Polars.NET.Core.Data
open System.Data
open System.Threading.Tasks
open System.Collections.Concurrent
/// --- Series ---
/// <summary>
/// An eager Series holding a single column of data.
/// </summary>
type Series(handle: SeriesHandle) =

    interface IDisposable with member _.Dispose() = handle.Dispose()
    member _.Handle = handle

    member _.Name = PolarsWrapper.SeriesName handle
    member _.Length = PolarsWrapper.SeriesLen handle
    
    member this.Rename(name: string) = 
        PolarsWrapper.SeriesRename(handle, name)
        this
    member this.Slice(offset: int64, length: int64) =
        new Series(PolarsWrapper.SeriesSlice(handle, offset, length))
    /// <summary>
    /// Get the string representation of the Series Data Type (e.g., "Int64", "String").
    /// </summary>
    member _.DtypeStr = PolarsWrapper.GetSeriesDtypeString handle
    member this.DataType : DataType =
        // 1. 获取 Series 的类型 Handle
        // Wrapper 会调用 pl_series_get_dtype 返回一个新的 Handle
        use typeHandle = PolarsWrapper.GetSeriesDataType handle
        
        // 2. 递归构建 F# 类型
        DataType.FromHandle typeHandle
    /// <summary>
    /// Returns a boolean Series indicating which values are null.
    /// </summary>
    member this.IsNull() : Series = 
        new Series(PolarsWrapper.SeriesIsNull handle)

    /// <summary>
    /// Returns a boolean Series indicating which values are not null.
    /// </summary>
    member this.IsNotNull() : Series = 
        new Series(PolarsWrapper.SeriesIsNotNull handle)
    /// <summary>
    /// Check if the value at the specified index is null.
    /// This is faster than retrieving the value and checking for Option.None.
    /// </summary>
    member _.IsNullAt(index: int) : bool =
        PolarsWrapper.SeriesIsNullAt(handle, int64 index)
    /// <summary>
    /// Get the number of null values in the Series.
    /// This is an O(1) operation (metadata access).
    /// </summary>
    member _.NullCount : int64 = 
        PolarsWrapper.SeriesNullCount handle
    /// <summary> Check if floating point values are NaN. </summary>
    member this.IsNan() = new Series(PolarsWrapper.SeriesIsNan handle)

    /// <summary> Check if floating point values are not NaN. </summary>
    member this.IsNotNan() = new Series(PolarsWrapper.SeriesIsNotNan handle)

    /// <summary> Check if floating point values are finite (not NaN and not Inf). </summary>
    member this.IsFinite() = new Series(PolarsWrapper.SeriesIsFinite handle)

    /// <summary> Check if floating point values are infinite. </summary>
    member this.IsInfinite() = new Series(PolarsWrapper.SeriesIsInfinite handle)
    // ==========================================
    // Static Constructors
    // ==========================================
    
    // --- Int32 ---
    static member create(name: string, data: int seq) = 
        new Series(PolarsWrapper.SeriesNew(name, Seq.toArray data, null))

    static member create(name: string, data: int option seq) = 
        let arr = Seq.toArray data
        let vals = Array.zeroCreate<int> arr.Length
        let valid = Array.zeroCreate<bool> arr.Length
        for i in 0 .. arr.Length - 1 do
            match arr.[i] with
            | Some v -> vals.[i] <- v; valid.[i] <- true
            | None -> vals.[i] <- 0; valid.[i] <- false
        new Series(PolarsWrapper.SeriesNew(name, vals, valid))

    // --- Int64 ---
    static member create(name: string, data: int64 seq) = 
        new Series(PolarsWrapper.SeriesNew(name, Seq.toArray data, null))

    static member create(name: string, data: int64 option seq) = 
        let arr = Seq.toArray data
        let vals = Array.zeroCreate<int64> arr.Length
        let valid = Array.zeroCreate<bool> arr.Length
        for i in 0 .. arr.Length - 1 do
            match arr.[i] with
            | Some v -> vals.[i] <- v; valid.[i] <- true
            | None -> vals.[i] <- 0L; valid.[i] <- false
        new Series(PolarsWrapper.SeriesNew(name, vals, valid))
        
    // --- Float64 ---
    static member create(name: string, data: double seq) = 
        new Series(PolarsWrapper.SeriesNew(name, Seq.toArray data, null))

    static member create(name: string, data: double option seq) = 
        let arr = Seq.toArray data
        let vals = Array.zeroCreate<double> arr.Length
        let valid = Array.zeroCreate<bool> arr.Length
        for i in 0 .. arr.Length - 1 do
            match arr.[i] with
            | Some v -> vals.[i] <- v; valid.[i] <- true
            | None -> vals.[i] <- Double.NaN; valid.[i] <- false
        new Series(PolarsWrapper.SeriesNew(name, vals, valid))

    // --- Boolean ---
    static member create(name: string, data: bool seq) = 
        new Series(PolarsWrapper.SeriesNew(name, Seq.toArray data, null))

    static member create(name: string, data: bool option seq) = 
        let arr = Seq.toArray data
        let vals = Array.zeroCreate<bool> arr.Length
        let valid = Array.zeroCreate<bool> arr.Length
        for i in 0 .. arr.Length - 1 do
            match arr.[i] with
            | Some v -> vals.[i] <- v; valid.[i] <- true
            | None -> vals.[i] <- false; valid.[i] <- false
        new Series(PolarsWrapper.SeriesNew(name, vals, valid))

    // --- String ---
    static member create(name: string, data: string seq) = 
        // 这里的 string seq 本身可能包含 null (如果源是 C#), 或者 F# string (不可空)
        // 为了安全，我们转为 string[] 即可
        new Series(PolarsWrapper.SeriesNew(name, Seq.toArray data))

    static member create(name: string, data: string option seq) = 
        let arr = Seq.toArray data
        // 将 Option 转换为 string array (None -> null)
        let vals = arr |> Array.map (fun opt -> match opt with Some s -> s | None -> null)
        new Series(PolarsWrapper.SeriesNew(name, vals))
    // --- DateTime ---
    static member create(name: string, data: DateTime seq) = 
        let arr = Seq.toArray data
        let longs = Array.zeroCreate<int64> arr.Length
        let epoch = 621355968000000000L
        
        for i in 0 .. arr.Length - 1 do
            // 转换为 Unix Microseconds
            longs.[i] <- (arr.[i].Ticks - epoch) / 10L

        // 1. 创建 Int64 Series
        let s = Series.create(name, longs)
        // 2. 转换为 Datetime 类型 (Microseconds, No Timezone)
        s.Cast(Datetime(Microseconds, None))

    static member create(name: string, data: DateTime option seq) = 
        let arr = Seq.toArray data
        let longs = Array.zeroCreate<int64> arr.Length
        let valid = Array.zeroCreate<bool> arr.Length
        let epoch = 621355968000000000L
        
        for i in 0 .. arr.Length - 1 do
            match arr.[i] with
            | Some dt -> 
                longs.[i] <- (dt.Ticks - epoch) / 10L
                valid.[i] <- true
            | None -> 
                longs.[i] <- 0L
                valid.[i] <- false

        // 直接调用底层 Wrapper 创建带 Validity 的 Int64 Series
        let s = new Series(PolarsWrapper.SeriesNew(name, longs, valid))
        s.Cast(Datetime(Microseconds, None))

    // --- Decimal ---
    /// <summary>
    /// Create a Decimal Series.
    /// scale: The number of decimal places (e.g., 2 for currency).
    /// </summary>
    static member create(name: string, data: decimal seq, scale: int) = 
        new Series(PolarsWrapper.SeriesNewDecimal(name, Seq.toArray data, null, scale))

    static member create(name: string, data: decimal option seq, scale: int) = 
        let arr = Seq.toArray data // decimal option[]
        // 转换逻辑稍复杂，我们在 Wrapper 里处理了 nullable 数组转换
        // 这里我们需要把 seq<decimal option> 转为 decimal?[] (Nullable<decimal>[]) 传给 C#
        let nullableArr = 
            arr |> Array.map (function Some v -> Nullable(v) | None -> Nullable())
            
        new Series(PolarsWrapper.SeriesNewDecimal(name, nullableArr, scale))
    // ==========================================
    // Temporal Types Creation
    // ==========================================

    // --- DateOnly (Polars Date: i32 days) ---
    static member create(name: string, data: DateOnly seq) =
        let arr = Seq.toArray data
        let days = Array.zeroCreate<int> arr.Length
        let epochOffset = 719162 // 0001-01-01 to 1970-01-01
        
        for i in 0 .. arr.Length - 1 do
            days.[i] <- arr.[i].DayNumber - epochOffset
            
        let s = Series.create(name, days)
        s.Cast Date

    static member create(name: string, data: DateOnly option seq) =
        let arr = Seq.toArray data
        let days = Array.zeroCreate<int> arr.Length
        let valid = Array.zeroCreate<bool> arr.Length
        let epochOffset = 719162
        
        for i in 0 .. arr.Length - 1 do
            match arr.[i] with
            | Some d -> 
                days.[i] <- d.DayNumber - epochOffset
                valid.[i] <- true
            | None -> 
                days.[i] <- 0
                valid.[i] <- false
                
        // 调用底层 int32 (SeriesNew)
        let s = new Series(PolarsWrapper.SeriesNew(name, days, valid))
        s.Cast DataType.Date

    // --- TimeOnly (Polars Time: i64 nanoseconds) ---
    static member create(name: string, data: TimeOnly seq) =
        let arr = Seq.toArray data
        let nanos = Array.zeroCreate<int64> arr.Length
        
        for i in 0 .. arr.Length - 1 do
            // Ticks = 100ns -> * 100 = ns
            nanos.[i] <- arr.[i].Ticks * 100L
            
        let s = Series.create(name, nanos)
        s.Cast DataType.Time

    static member create(name: string, data: TimeOnly option seq) =
        let arr = Seq.toArray data
        let nanos = Array.zeroCreate<int64> arr.Length
        let valid = Array.zeroCreate<bool> arr.Length
        
        for i in 0 .. arr.Length - 1 do
            match arr.[i] with
            | Some t -> 
                nanos.[i] <- t.Ticks * 100L
                valid.[i] <- true
            | None -> 
                nanos.[i] <- 0L
                valid.[i] <- false
                
        let s = new Series(PolarsWrapper.SeriesNew(name, nanos, valid))
        s.Cast DataType.Time

    // --- TimeSpan (Polars Duration: i64 microseconds) ---
    // 为了和 Datetime(us) 兼容，Duration 也默认用 us
    static member create(name: string, data: TimeSpan seq) =
        let arr = Seq.toArray data
        let micros = Array.zeroCreate<int64> arr.Length
        
        for i in 0 .. arr.Length - 1 do
            // Ticks = 100ns -> / 10 = us
            micros.[i] <- arr.[i].Ticks / 10L
            
        let s = Series.create(name, micros)
        s.Cast(Duration Microseconds)

    static member create(name: string, data: TimeSpan option seq) =
        let arr = Seq.toArray data
        let micros = Array.zeroCreate<int64> arr.Length
        let valid = Array.zeroCreate<bool> arr.Length
        
        for i in 0 .. arr.Length - 1 do
            match arr.[i] with
            | Some t -> 
                micros.[i] <- t.Ticks / 10L
                valid.[i] <- true
            | None -> 
                micros.[i] <- 0L
                valid.[i] <- false
                
        let s = new Series(PolarsWrapper.SeriesNew(name, micros, valid))
        s.Cast(Duration Microseconds)
    /// <summary>
    /// Smart Constructor:
    /// 1. Handles primitive types (int, double...).
    /// 2. Handles Option types (int option...) by forwarding to ofOptionSeq.
    /// 3. Handles Decimal types (decimal, decimal option) by inferring scale.
    /// </summary>
    static member ofOptionSeq<'T>(name: string, data: seq<'T option>) : Series =
        let t = typeof<'T>
        if t = typeof<int> then Series.create(name, data |> Seq.cast<int option>)
        else if t = typeof<int64> then Series.create(name, data |> Seq.cast<int64 option>)
        else if t = typeof<double> then Series.create(name, data |> Seq.cast<double option>)
        else if t = typeof<bool> then Series.create(name, data |> Seq.cast<bool option>)
        else if t = typeof<string> then Series.create(name, data |> Seq.cast<string option>)
        else if t = typeof<DateTime> then Series.create(name, data |> Seq.cast<DateTime option>)
        else if t = typeof<DateOnly> then Series.create(name, data |> Seq.cast<DateOnly option>)
        else if t = typeof<TimeOnly> then Series.create(name, data |> Seq.cast<TimeOnly option>)
        else if t = typeof<TimeSpan> then Series.create(name, data |> Seq.cast<TimeSpan option>)
        else failwithf "Unsupported type for Series.ofOptionSeq: %A" t

    // --- Scalar Access ---
    
    /// <summary> Get value as Int64 Option. Handles Int32/Int64 etc. </summary>
    member _.Int(index: int) : int64 option = 
        PolarsWrapper.SeriesGetInt(handle, int64 index) |> Option.ofNullable

    /// <summary> Get value as Double Option. Handles Float32/Float64. </summary>
    member _.Float(index: int) : float option = 
        PolarsWrapper.SeriesGetDouble(handle, int64 index) |> Option.ofNullable

    /// <summary> Get value as String Option. </summary>
    member _.String(index: int) : string option = 
        PolarsWrapper.SeriesGetString(handle, int64 index) |> Option.ofObj

    /// <summary> Get value as Boolean Option. </summary>
    member _.Bool(index: int) : bool option = 
        PolarsWrapper.SeriesGetBool(handle, int64 index) |> Option.ofNullable

    /// <summary> Get value as Decimal Option. </summary>
    member _.Decimal(index: int) : decimal option = 
        PolarsWrapper.SeriesGetDecimal(handle, int64 index) |> Option.ofNullable

    // 时间类型
    member _.Date(index: int) : DateOnly option = 
        PolarsWrapper.SeriesGetDate(handle, int64 index) |> Option.ofNullable

    member _.Time(index: int) : TimeOnly option = 
        PolarsWrapper.SeriesGetTime(handle, int64 index) |> Option.ofNullable

    member _.Datetime(index: int) : DateTime option = 
        PolarsWrapper.SeriesGetDatetime(handle, int64 index) |> Option.ofNullable

    member _.Duration(index: int) : TimeSpan option = 
        PolarsWrapper.SeriesGetDuration(handle, int64 index) |> Option.ofNullable
    // --- Aggregations (Returning Series of len 1) ---
    // 返回 Series 而不是 scalar，是为了支持链式计算 (s.Sum() / s.Count())
    // 最终要取值时，用户可以调 s.Int(0) 或 s.Float(0)
    
    member this.Sum() = new Series(PolarsWrapper.SeriesSum handle)
    member this.Mean() = new Series(PolarsWrapper.SeriesMean handle)
    member this.Min() = new Series(PolarsWrapper.SeriesMin handle)
    member this.Max() = new Series(PolarsWrapper.SeriesMax handle)

    // --- Operators (Arithmetic) ---

    static member (+) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesAdd(lhs.Handle, rhs.Handle))
    static member (-) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesSub(lhs.Handle, rhs.Handle))
    static member (*) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesMul(lhs.Handle, rhs.Handle))
    static member (/) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesDiv(lhs.Handle, rhs.Handle))

    // --- Operators (Comparison) ---

    static member (.=) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesEq(lhs.Handle, rhs.Handle))
    static member (!=) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesNeq(lhs.Handle, rhs.Handle))
    static member (.>) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesGt(lhs.Handle, rhs.Handle))
    static member (.<) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesLt(lhs.Handle, rhs.Handle))

    static member (.>=) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesGtEq(lhs.Handle, rhs.Handle))
    static member (.<=) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesLtEq(lhs.Handle, rhs.Handle))

    // --- Broadcasting Helpers (Scalar Ops) ---
    // 允许 s + 1, s * 2.5 等操作
    // 我们创建一个临时的长度为1的 Series 传给 Rust，Rust 会处理广播

    static member (+) (lhs: Series, rhs: int) = lhs + Series.create("lit", [rhs])
    static member (+) (lhs: Series, rhs: double) = lhs + Series.create("lit", [rhs])
    static member (-) (lhs: Series, rhs: int) = lhs - Series.create("lit", [rhs])
    static member (-) (lhs: Series, rhs: double) = lhs - Series.create("lit", [rhs])
    
    static member (*) (lhs: Series, rhs: int) = lhs * Series.create("lit", [rhs])
    static member (*) (lhs: Series, rhs: double) = lhs * Series.create("lit", [rhs])
    
    static member (/) (lhs: Series, rhs: int) = lhs / Series.create("lit", [rhs])
    static member (/) (lhs: Series, rhs: double) = lhs / Series.create("lit", [rhs])

    // Comparison with Scalar
    static member (.>) (lhs: Series, rhs: int) = lhs .> Series.create("lit", [rhs])
    static member (.>) (lhs: Series, rhs: double) = lhs .> Series.create("lit", [rhs])
    static member (.<) (lhs: Series, rhs: int) = lhs .< Series.create("lit", [rhs])
    static member (.<) (lhs: Series, rhs: double) = lhs .< Series.create("lit", [rhs])
    static member (.>=) (lhs: Series, rhs: int) = lhs .>= Series.create("lit", [rhs])
    static member (.<=) (lhs: Series, rhs: double) = lhs .<= Series.create("lit", [rhs])
    
    static member (.=) (lhs: Series, rhs: int) = lhs .= Series.create("lit", [rhs])
    static member (.=) (lhs: Series, rhs: double) = lhs .= Series.create("lit", [rhs])
    static member (.=) (lhs: Series, rhs: string) = lhs .= Series.create("lit", [rhs])
    static member (.!=) (lhs: Series, rhs: int) = lhs != Series.create("lit", [rhs])
    static member (.!=) (lhs: Series, rhs: string) = lhs != Series.create("lit", [rhs])
    // ==========================================
    // Interop with DataFrame
    // ==========================================
    member this.ToFrame() : DataFrame =
        let h = PolarsWrapper.SeriesToFrame handle
        new DataFrame(h)

    member this.Cast(dtype: DataType) : Series =
        use typeHandle = dtype.CreateHandle()
        let newHandle = PolarsWrapper.SeriesCast(handle, typeHandle)
        new Series(newHandle)
    member this.ToArrow() : IArrowArray =
        PolarsWrapper.SeriesToArrow handle

// --- Frames ---

/// <summary>
/// An eager DataFrame holding data in memory.
/// </summary>
and DataFrame(handle: DataFrameHandle) =
    interface IDisposable with
        member _.Dispose() = handle.Dispose()
    member this.Clone() = new DataFrame(PolarsWrapper.CloneDataFrame handle)
    member internal this.CloneHandle() = PolarsWrapper.CloneDataFrame handle
    member _.Handle = handle
    static member ReadCsv (path: string, 
                               ?schema: Map<string, DataType>, // 注意逗号
                               ?hasHeader: bool,               // 注意逗号
                               ?separator: char,
                               ?skipRows: int,
                               ?tryParseDates: bool) : DataFrame =
        
        let header = defaultArg hasHeader true
        let sep = defaultArg separator ','
        let skip = defaultArg skipRows 0
        let dates = defaultArg tryParseDates true
        
        let schemaDict = 
            match schema with
            | Some m -> 
                let d = Dictionary<string, DataTypeHandle>()
                m |> Map.iter (fun k v -> d.Add(k, v.CreateHandle()))
                d
            | None -> null

        let h = PolarsWrapper.ReadCsv(path, schemaDict, header, sep, uint64 skip, dates)
        new DataFrame(h)
    /// <summary> Asynchronously read a CSV file into a DataFrame. </summary>
    static member ReadCsvAsync(path: string, 
                               ?schema: Map<string, DataType>,
                               ?hasHeader: bool,
                               ?separator: char,
                               ?skipRows: int,
                               ?tryParseDates: bool) : Async<DataFrame> =
        
        // 1. 在主线程准备参数 (参数解析是非常快的)
        let header = defaultArg hasHeader true
        let sep = defaultArg separator ','
        let skip = defaultArg skipRows 0
        let dates = defaultArg tryParseDates true
        
        // 2. 转换 Schema (Map -> Dictionary)
        let schemaDict = 
            match schema with
            | Some m -> 
                let d = Dictionary<string, DataTypeHandle>()
                // 记得使用 CreateHandle()
                m |> Map.iter (fun k v -> d.Add(k, v.CreateHandle()))
                d
            | None -> null

        // 3. 进入 Async 工作流
        async {
            // 调用 C# Wrapper 的 Async 方法 (返回 Task<DataFrameHandle>)
            // 使用 Async.AwaitTask 等待 C# Task 完成
            let! handle = 
                PolarsWrapper.ReadCsvAsync(
                    path, 
                    schemaDict, 
                    header, 
                    sep, 
                    uint64 skip, 
                    dates
                ) |> Async.AwaitTask

            return new DataFrame(handle)
        }

    /// <summary>
    /// [Eager] Create a DataFrame from an IDataReader (e.g. SqlDataReader).
    /// Uses high-performance streaming ingestion.
    /// </summary>
    /// <param name="reader">The open DataReader.</param>
    /// <param name="batchSize">Rows per batch (default 50,000).</param>
    static member ReadDb(reader: Data.IDataReader, ?batchSize: int) : DataFrame =
        let size = defaultArg batchSize 50_000
        
        // 1. 将 DataReader 转为 Arrow Batch 流
        // 这是一个 C# 扩展方法，在 F# 中作为静态方法调用
        let batchStream = reader.ToArrowBatches size
        
        // 2. 直接导入
        let handle = Polars.NET.Core.Arrow.ArrowStreamInterop.ImportEager batchStream
        
        if handle.IsInvalid then
            DataFrame.create []
        else
            new DataFrame(handle)

    /// <summary> Read a parquet file into a DataFrame (Eager). </summary>
    static member ReadParquet (path: string) = new DataFrame(PolarsWrapper.ReadParquet path)
    /// <summary> Read a JSON file into a DataFrame (Eager). </summary>
    static member ReadJson (path: string) : DataFrame =
        new DataFrame(PolarsWrapper.ReadJson path)
    /// <summary> Read an IPC file into a DataFrame (Eager). </summary>
    static member ReadIpc (path: string) = new DataFrame(PolarsWrapper.ReadIpc path)

    static member ofSeqStream<'T>(data: seq<'T>, ?batchSize: int) : DataFrame =
        let size = defaultArg batchSize 100_000

        // 1. 构建惰性流 (Lazy Stream)
        // 只有当底层 Rust 开始拉取数据时，这里才会真正执行 chunk 和 BuildRecordBatch
        let batchStream = 
            data
            |> Seq.chunkBySize size
            |> Seq.map Polars.NET.Core.Arrow.ArrowFfiBridge.BuildRecordBatch

        // 2. 一键导入
        // C# 的 ImportEager 会自动处理 peek schema 和缝合逻辑
        let handle = Polars.NET.Core.Arrow.ArrowStreamInterop.ImportEager batchStream

        // 3. 处理空流情况 (ImportEager 返回 InvalidHandle 时)
        if handle.IsInvalid then
            DataFrame.create []
        else
            new DataFrame(handle)
    static member FromArrow (batch: Apache.Arrow.RecordBatch) : DataFrame =
        new DataFrame(PolarsWrapper.FromArrow batch)
    /// <summary> Write DataFrame to CSV. </summary>
    member this.WriteCsv (path: string) = 
        PolarsWrapper.WriteCsv(this.Handle, path)
        this 
    /// <summary> Write DataFrame to Parquet. </summary>
    member this.WriteParquet (path: string) = 
        PolarsWrapper.WriteParquet(this.Handle, path)
        this
    /// <summary>
    /// Write DataFrame to an Arrow IPC (Feather) file.
    /// This is a fast, zero-copy binary format.
    /// </summary>
    member this.WriteIpc(path: string)=
        PolarsWrapper.WriteIpc(this.Handle, path)
        this
    /// <summary>
    /// Write DataFrame to a JSON file (standard array format).
    /// </summary>
    member this.WriteJson(path: string) =
        PolarsWrapper.WriteJson(this.Handle, path)
        this 
    /// <summary>
    /// Export the DataFrame as a stream of Arrow RecordBatches (Zero-Copy).
    /// Calls 'onBatch' for each chunk in the DataFrame.
    /// Useful for custom eager sinks (e.g. WriteDatabase).
    /// </summary>
    member this.ExportBatches(onBatch: Action<RecordBatch>) : unit =
        // Eager 操作通常是只读的，不需要 TransferOwnership
        // PolarsWrapper.ExportBatches 负责遍历内部 Chunks 并回调 C#
        PolarsWrapper.ExportBatches(this.Handle, onBatch)

    /// <summary>
    /// Stream the DataFrame directly to a database or other IDataReader consumer.
    /// Uses a producer-consumer pattern with bounded capacity.
    /// </summary>
    /// <param name="writerAction">Callback to consume the IDataReader (e.g. using SqlBulkCopy).</param>
    /// <param name="bufferSize">Max number of batches to buffer in memory (default: 5).</param>
    /// <param name="typeOverrides">Force specific C# types for columns (Target Schema).</param>
    member this.WriteTo(writerAction: Action<IDataReader>, ?bufferSize: int, ?typeOverrides: IDictionary<string, Type>) : unit =
        let capacity = defaultArg bufferSize 5
        
        // 1. 生产者-消费者缓冲区
        // use 确保 Collection 释放
        use buffer = new BlockingCollection<Apache.Arrow.RecordBatch>(capacity)

        // 2. 启动消费者任务 (Consumer: DB Writer)
        // 在后台线程运行，避免阻塞主线程（虽然 Eager WriteTo 本身通常是阻塞调用）
        let consumerTask = Task.Run(fun () ->
            // 获取消费流
            let stream = buffer.GetConsumingEnumerable()
            
            // 处理类型覆盖
            let overrides = 
                match typeOverrides with 
                | Some d -> new Dictionary<string, Type>(d) 
                | None -> null
            
            // 构造伪装的 DataReader
            use reader = new ArrowToDbStream(stream, overrides)
            
            // 执行用户回调 (如 SqlBulkCopy.WriteToServer)
            writerAction.Invoke reader
        )

        // 3. 启动生产者 (Producer: DataFrame Iterator)
        // 当前线程执行，遍历 DataFrame 的内存块
        try
            try
                // 将 DataFrame 的 Chunks 推入 Buffer
                // 如果 Buffer 满了，这里会阻塞
                this.ExportBatches(fun batch -> buffer.Add batch)
            finally
                // 4. 通知消费者：没有更多数据了
                buffer.CompleteAdding()
        with
        | _ -> 
            // 确保异常抛出
            reraise()

        // 5. 等待消费者完成
        try
            consumerTask.Wait()
        with
        | :? AggregateException as aggEx ->
            // 解包 Task 异常，抛出真实的 SqlException 等
            raise (aggEx.Flatten().InnerException)
    
    /// <summary>
    /// Get the schema as Map<ColumnName, DataType>.
    /// </summary>
    member this.Schema : Map<string, DataType> =
        // 1. 获取所有列名 (string[])
        // 假设 this.Columns 属性已经实现了 (调用 PolarsWrapper.GetColumnNames)
        let names = this.ColumnNames
        
        // 2. 遍历列名，获取每一列的 Series 和 DataType
        names
        |> Array.map (fun (name: string) ->
            // [关键] 这是一个非常轻量的操作
            // 我们只是获取了一个指向现有 Series 的 Handle，没有数据拷贝
            let s = this.Column name
            
            // s.DataType 现在调用的是我们刚写的 DataType.FromHandle (递归 Native 读取)
            name, s.DataType
        )
        |> Map.ofArray
    member this.Lazy() : LazyFrame =
        let lfHandle = PolarsWrapper.DataFrameToLazy handle
        new LazyFrame(lfHandle)
    /// <summary>
    /// Print schema in a readable format.
    /// </summary>
    member this.PrintSchema() =
        printfn "--- DataFrame Schema ---"
        this.Schema |> Map.iter (fun name dtype -> 
            // 简单的反射打印 DU 名称
            printfn "%-15s | %A" name dtype
        )
        printfn "------------------------"
    // ==========================================
    // Eager Ops
    // ==========================================
    member this.WithColumns (exprs:Expr list) : DataFrame =
        let handles = exprs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let h = PolarsWrapper.WithColumns(this.Handle,handles)
        new DataFrame(h)
    member this.WithColumn (expr: Expr) : DataFrame =
        let handle = expr.CloneHandle()
        let h = PolarsWrapper.WithColumns(this.Handle,[| handle |])
        new DataFrame(h)
    member this.Select (exprs:Expr list) : DataFrame =
         let handles = exprs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
         let h = PolarsWrapper.Select(this.Handle,handles)
         new DataFrame(h)
    member this.Filter (expr: Expr) : DataFrame = 
        let h = PolarsWrapper.Filter(this.Handle,expr.CloneHandle())
        new DataFrame(h)
    member this.Sort (expr: Expr) (desc :bool) : DataFrame =
        let h = PolarsWrapper.Sort(this.Handle,expr.CloneHandle(),desc)
        new DataFrame(h)
    member this.Orderby (expr: Expr) (desc :bool) : DataFrame =
        this.Sort expr desc
    member this.GroupBy (keys: Expr list) (aggs: Expr list) : DataFrame =
        let kHandles = keys |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let aHandles = aggs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let h = PolarsWrapper.GroupByAgg(this.Handle, kHandles, aHandles)
        new DataFrame(h)
    member this.Join (other: DataFrame) (leftOn: Expr list) (rightOn: Expr list) (how: JoinType) : DataFrame =
        let lHandles = leftOn |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let rHandles = rightOn |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let h = PolarsWrapper.Join(this.Handle, other.Handle, lHandles, rHandles, how.ToNative())
        new DataFrame(h)
    static member Concat (dfs: DataFrame list) (how: ConcatType): DataFrame =
        let handles = dfs |> List.map (fun df -> df.CloneHandle()) |> List.toArray
        new DataFrame(PolarsWrapper.Concat (handles,how.ToNative()))
    member this.Head (?rows: int) : DataFrame  =
        let n = defaultArg rows 5
        use h = PolarsWrapper.Head(this.Handle, uint n) 
        new DataFrame(h)
    member this.Tail (?n: int) : DataFrame =
        let rows = defaultArg n 5
        let h = PolarsWrapper.Tail(this.Handle, uint rows) 
        new DataFrame(h)
    member this.Explode (exprs: Expr list) : DataFrame =
        let handles = exprs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let h = PolarsWrapper.Explode(this.Handle, handles)
        new DataFrame(h)
    member this.UnnestColumn(column: string) : DataFrame =
        let cols = [| column |]
        let newHandle = PolarsWrapper.Unnest(this.Handle, cols)
        new DataFrame(newHandle)
    member this.UnnestColumns(columns: string list) : DataFrame =
        let cArr = List.toArray columns
        let newHandle = PolarsWrapper.Unnest(this.Handle, cArr)
        new DataFrame(newHandle)
       /// <summary> Pivot the DataFrame from long to wide format. </summary>
    member this.Pivot (index: string list) (columns: string list) (values: string list) (aggFn: PivotAgg) : DataFrame =
        let iArr = List.toArray index
        let cArr = List.toArray columns
        let vArr = List.toArray values
        new DataFrame(PolarsWrapper.Pivot(this.Handle, iArr, cArr, vArr, aggFn.ToNative()))

    /// <summary> Unpivot (Melt) the DataFrame from wide to long format. </summary>
    member this.Unpivot (index: string list) (on: string list) (variableName: string option) (valueName: string option) : DataFrame =
        let iArr = List.toArray index
        let oArr = List.toArray on
        let varN = Option.toObj variableName 
        let valN = Option.toObj valueName 
        new DataFrame(PolarsWrapper.Unpivot(this.Handle, iArr, oArr, varN, valN))
    member this.Melt = this.Unpivot
    // ==========================================
    // Printing / String Representation
    // ==========================================

    /// <summary>
    /// Returns the native Polars string representation of the DataFrame.
    /// Includes shape, header, and truncated data.
    /// </summary>
    override this.ToString() =
        PolarsWrapper.DataFrameToString handle

    /// <summary>
    /// Print the DataFrame to Console (Stdout).
    /// </summary>
    member this.Show() =
        printfn "%s" (this.ToString())
    static member create(series: Series list) : DataFrame =
        let handles = 
            series 
            |> List.map (fun s -> s.Handle) 
            |> List.toArray
            
        let h = PolarsWrapper.DataFrameNew handles
        new DataFrame(h)
    
    // 重载：允许变长参数 (df = DataFrame.create(s1, s2, s3))
    static member create([<System.ParamArray>] series: Series[]) : DataFrame =
        let handles = series |> Array.map (fun s -> s.Handle)
        let h = PolarsWrapper.DataFrameNew handles
        new DataFrame(h)
    /// <summary>
    /// Remove a column by name. Returns a new DataFrame.
    /// </summary>
    member this.Drop(name: string) : DataFrame =
        new DataFrame(PolarsWrapper.Drop(handle, name))

    /// <summary>
    /// Rename a column. Returns a new DataFrame.
    /// </summary>
    member this.Rename(oldName: string, newName: string) : DataFrame =
        new DataFrame(PolarsWrapper.Rename(handle, oldName, newName))

    /// <summary>
    /// Drop rows containing any null values.
    /// subset: Optional list of column names to consider.
    /// </summary>
    member this.DropNulls(?subset: string list) : DataFrame =
        let s = subset |> Option.map List.toArray |> Option.toObj
        new DataFrame(PolarsWrapper.DropNulls(handle, s))

    /// <summary>
    /// Sample n rows from the DataFrame.
    /// </summary>
    member this.Sample(n: int, ?withReplacement: bool, ?shuffle: bool, ?seed: uint64) : DataFrame =
        let replace = defaultArg withReplacement false
        let shuff = defaultArg shuffle true
        let s = Option.toNullable seed
        
        // n 必须 >= 0
        new DataFrame(PolarsWrapper.SampleN(handle, uint64 n, replace, shuff, s))

    /// <summary>
    /// Sample a fraction of rows from the DataFrame.
    /// </summary>
    member this.Sample(frac: double, ?withReplacement: bool, ?shuffle: bool, ?seed: uint64) : DataFrame =
        let replace = defaultArg withReplacement false
        let shuff = defaultArg shuffle true
        let s = Option.toNullable seed
        
        new DataFrame(PolarsWrapper.SampleFrac(handle, frac, replace, shuff, s))
    // Interop
    member this.ToArrow() = ArrowFfiBridge.ExportDataFrame handle
    member _.Rows = PolarsWrapper.DataFrameHeight handle
    member _.Len = PolarsWrapper.DataFrameHeight handle
    member _.Width = PolarsWrapper.DataFrameWidth handle
    member _.ColumnNames = PolarsWrapper.GetColumnNames handle
    member this.Item 
        with get(colName: string, rowIndex: int) =
            PolarsWrapper.GetDouble(handle, colName, int64 rowIndex)
    member this.Int(colName: string, rowIndex: int) : int64 option = 
        let nullableVal = PolarsWrapper.GetInt(handle, colName, int64 rowIndex)
        if nullableVal.HasValue then Some nullableVal.Value else None
    member this.Float(colName: string, rowIndex: int) : float option = 
        let nullableVal = PolarsWrapper.GetDouble(handle, colName, int64 rowIndex)
        if nullableVal.HasValue then Some nullableVal.Value else None
    member this.String(colName: string, rowIndex: int) = PolarsWrapper.GetString(handle, colName, int64 rowIndex) |> Option.ofObj
    member this.StringList(colName: string, rowIndex: int) : string list option =
        use colHandle = PolarsWrapper.Select(handle, [| PolarsWrapper.Col colName |])
        use tempDf = new DataFrame(colHandle)
        use arrowBatch = tempDf.ToArrow()
        
        let col = arrowBatch.Column colName
        
        let extractStrings (valuesArr: IArrowArray) (startIdx: int) (endIdx: int) =
            match valuesArr with
            | :? StringArray as sa ->
                [ for i in startIdx .. endIdx - 1 -> sa.GetString i ]
            | :? StringViewArray as sva ->
                [ for i in startIdx .. endIdx - 1 -> sva.GetString i ]
            | _ -> [] 

        match col with
        // Case A: Arrow.ListArray 
        | :? Apache.Arrow.ListArray as listArr ->
            if listArr.IsNull rowIndex then None
            else
                let start = listArr.ValueOffsets.[rowIndex]
                let end_ = listArr.ValueOffsets.[rowIndex + 1]
                Some (extractStrings listArr.Values start end_)

        // Case B: Large List (64-bit offsets) 
        | :? Apache.Arrow.LargeListArray as listArr ->
            if listArr.IsNull rowIndex then None
            else
                // Offset 是 long，强转 int (单行 List 长度通常不会超过 20 亿)
                let start = int listArr.ValueOffsets.[rowIndex]
                let end_ = int listArr.ValueOffsets.[rowIndex + 1]
                Some (extractStrings listArr.Values start end_)

        | _ -> 
            // System.Console.WriteLine($"[Debug] Mismatched Array Type: {col.GetType().Name}")
            None
    member this.Decimal(col: string, row: int) : decimal option =
        use s = this.Column col
        s.Decimal row
    // 1. Boolean
    member this.Bool(col: string, row: int) : bool option =
        use s = this.Column col
        s.Bool row
    // 2. Date (DateOnly)
    member this.Date(col: string, row: int) : DateOnly option =
        use s = this.Column col
        s.Date row

    // 3. Time (TimeOnly)
    member this.Time(col: string, row: int) : TimeOnly option =
        use s = this.Column col
        s.Time row

    // 4. Datetime (DateTime)
    member this.Datetime(col: string, row: int) : DateTime option =
        use s = this.Column col
        s.Datetime row

    // 5. Duration (TimeSpan)
    member this.Duration(col: string, row: int) : TimeSpan option =
        use s = this.Column col
        s.Duration row
    member this.Column(name: string) : Series =
    // 我们假设 Rust 端有 pl_dataframe_get_column
        let h = PolarsWrapper.DataFrameGetColumn(this.Handle, name)
        new Series(h)
    member this.Column(index: int) : Series =
        let h = PolarsWrapper.DataFrameGetColumnAt(this.Handle, index)
        new Series(h)
        
    member this.Item 
        with get(name: string) = this.Column name
    
    member this.Item 
        with get(index: int) = this.Column index

    member this.GetSeries() : Series list =
        [ for i in 0 .. int this.Width - 1 -> this.Column i ]
    /// <summary>
    /// Check if the value at the specified column and row is null.
    /// </summary>
    member this.IsNullAt(col: string, row: int) : bool =
        use s = this.Column col
        s.IsNullAt row
    /// <summary>
    /// Get the number of null values in a specific column.
    /// </summary>
    member this.NullCount(colName: string) : int64 =
        use s = this.Column colName
        s.NullCount
    member this.IsNan(col: string) =
        use s = this.Column col
        s.IsNan()
    member this.IsNotNan (col:string) =
        use s = this.Column col
        s.IsNotNan()
    member this.IsFinite (col:string) =
        use s = this.Column col
        s.IsFinite()
    member this.IsInfinite (col:string) =
        use s = this.Column col
        s.IsInfinite()
/// <summary>
/// A LazyFrame represents a logical plan of operations that will be optimized and executed only when collected.
/// </summary>
and LazyFrame(handle: LazyFrameHandle) =
    member _.Handle = handle
    member internal this.CloneHandle() = PolarsWrapper.LazyClone handle
    /// <summary> Execute the plan and return a DataFrame. </summary>
    member this.Collect() = 
        let dfHandle = PolarsWrapper.LazyCollect handle
        new DataFrame(dfHandle)
    member this.CollectStreaming() =
        let dfHandle = PolarsWrapper.CollectStreaming handle
        new DataFrame(dfHandle)
    /// <summary> Get the schema string of the LazyFrame without executing it. </summary>
    member _.SchemaRaw = PolarsWrapper.GetSchemaString handle

    /// <summary>
    /// Get the schema of the LazyFrame without executing it.
    /// Uses Zero-Copy native introspection.
    /// </summary>
    member _.Schema : Map<string, DataType> =
        // 1. 获取 Native Schema Handle
        // 使用 use 确保 Handle 用完即焚
        use schemaHandle = PolarsWrapper.GetLazySchema handle

        // 2. 获取字段数量
        let len = PolarsWrapper.GetSchemaLen schemaHandle

        // 3. 遍历并构建 Map
        if len = 0UL then 
            Map.empty
        else
            [| for i in 0UL .. len - 1UL do
                let mutable name = Unchecked.defaultof<string>
                let mutable typeHandle = Unchecked.defaultof<DataTypeHandle>
                
                // 调用 C# Wrapper 获取第 i 个字段的信息
                PolarsWrapper.GetSchemaFieldAt(schemaHandle, i, &name, &typeHandle)
                
                // [关键] typeHandle 是新创建的，必须 Dispose
                use h = typeHandle
                
                // 递归构建 F# DataType (复用我们之前的成果)
                let dtype = DataType.FromHandle(h)
                
                yield name, dtype
            |]
            |> Map.ofArray

    /// <summary> Print the query plan. </summary>
    member this.Explain(?optimized: bool) = 
        let opt = defaultArg optimized true
        PolarsWrapper.Explain(handle, opt)
    /// <summary>
    /// Lazily scan a CSV file into a LazyFrame.
    /// </summary>
    static member ScanCsv(path: string,
                          ?schema: Map<string, DataType>,
                          ?hasHeader: bool,
                          ?separator: char,
                          ?skipRows: int,
                          ?tryParseDates: bool) : LazyFrame =
        
        let header = defaultArg hasHeader true
        let sep = defaultArg separator ','
        let skip = defaultArg skipRows 0
        let dates = defaultArg tryParseDates true
        
        let schemaDict = 
            match schema with
            | Some m -> 
                let d = Dictionary<string, DataTypeHandle>()
                m |> Map.iter (fun k v -> d.Add(k, v.CreateHandle()))
                d
            | None -> null

        let h = PolarsWrapper.ScanCsv(path, schemaDict, header, sep, uint64 skip, dates)
        new LazyFrame(h)
    /// <summary> Scan a parquet file into a LazyFrame. </summary>
    static member ScanParquet (path: string) = new LazyFrame(PolarsWrapper.ScanParquet path)
    /// <summary> Scan a JSON file into a LazyFrame. </summary>
    static member ScanNdjson (path: string) : LazyFrame =
        new LazyFrame(PolarsWrapper.ScanNdjson path)
    /// <summary> Scan an IPC file into a LazyFrame. </summary>
    static member ScanIpc (path: string) = new LazyFrame(PolarsWrapper.ScanIpc path)
    
    // ==========================================
    // Streaming Scan (Lazy)
    // ==========================================

    /// <summary>
    /// Lazily scan a sequence of objects using Apache Arrow Stream Interface.
    /// This supports predicate pushdown and streaming execution.
    /// Data is pulled from the sequence only when needed.
    /// </summary>
    /// <param name="data">The data source sequence.</param>
    /// <param name="batchSize">Rows per Arrow batch (default: 100,000).</param>
    static member scanSeq<'T>(data: seq<'T>, ?batchSize: int) : LazyFrame =
        let size = defaultArg batchSize 100_000

        // 1. 静态推断 Schema (无需触碰数据流)
        // 相比 C# 的 ProbeEnumerator (预读首帧)，F# 这里利用 BuildRecordBatch 的反射能力，
        // 传入空序列即可得到正确的 Schema。这样更安全，不会消耗流的任何元素。
        let dummyBatch = ArrowFfiBridge.BuildRecordBatch(Seq.empty<'T>)
        let schema = dummyBatch.Schema

        // 2. 定义流工厂 (The Stream Factory)
        // 每次 Polars 引擎需要扫描数据时，都会调用这个工厂。
        let streamFactory = Func<IEnumerator<RecordBatch>>(fun () ->
            data
            |> Seq.chunkBySize size
            |> Seq.map ArrowFfiBridge.BuildRecordBatch
            // [修复] Seq 模块没有 getEnumerator，直接调用接口方法
            |> fun s -> s.GetEnumerator()
        )

        // 3. 调用 Core 层封装
        // ArrowStreamInterop.ScanStream 封装了 CreateDirectScanContext, ExportSchema, CallWrapper, FreeSchema 等所有逻辑
        let handle = ArrowStreamInterop.ScanStream(streamFactory, schema)
        
        new LazyFrame(handle)

    /// <summary>
    /// [Lazy] Scan a database query lazily.
    /// Requires a factory function to create new IDataReaders for potential multi-pass scans.
    /// </summary>
    static member scanDb(readerFactory: unit -> IDataReader, ?batchSize: int) : LazyFrame =
        let size = defaultArg batchSize 50_000
        
        // 1. 预读 Schema (Open -> GetSchema -> Close)
        let schema = 
            use tempReader = readerFactory()
            tempReader.GetArrowSchema()

        // 2. 定义流工厂
        let streamFactory = Func<IEnumerator<RecordBatch>>(fun () ->
            // 注意：这里我们创建一个 seq，并让它负责 reader 的生命周期
            let batchSeq = seq {
                use reader = readerFactory()
                // yield! 展开枚举器
                yield! reader.ToArrowBatches size
            }
            batchSeq.GetEnumerator()
        )

        // 3. 调用 Core
        let handle = ArrowStreamInterop.ScanStream(streamFactory, schema)
        new LazyFrame(handle)

    /// <summary> Write LazyFrame execution result to Parquet (Streaming). </summary>
    member this.SinkParquet (path: string) : unit =
        PolarsWrapper.SinkParquet(this.CloneHandle(), path)
    /// <summary> Write LazyFrame execution result to IPC (Streaming). </summary>
    member this.SinkIpc (path: string) = 
        PolarsWrapper.SinkIpc(this.CloneHandle(), path)
    // ==========================================
    // Streaming Sink (Lazy)
    // ==========================================
    /// <summary>
    /// Stream the query result in batches.
    /// This executes the query and calls 'onBatch' for each RecordBatch produced.
    /// </summary>
    member this.SinkBatches(onBatch: Action<RecordBatch>) : unit =
        // 这里假设 Wrapper 返回 void，或者是阻塞的
        // 如果 Wrapper 返回 Handle (如 C# 示例)，我们需要确保它被执行
        let newHandle = PolarsWrapper.SinkBatches(this.CloneHandle(), onBatch)
        
        // C# 示例里为了触发执行，手动调了 CollectStreaming。
        // 这说明 SinkBatches 只是构建了 Plan，还没跑。
        let lfRes = new LazyFrame(newHandle)
        use _ = lfRes.CollectStreaming()
        () 
    /// <summary>
    /// Stream query results directly to a database or other IDataReader consumer.
    /// Uses a producer-consumer pattern with bounded capacity for memory efficiency.
    /// </summary>
    /// <param name="writerAction">Callback to consume the IDataReader (e.g., using SqlBulkCopy).</param>
    /// <param name="bufferSize">Max number of batches to buffer in memory (default: 5).</param>
    /// <param name="typeOverrides">Force specific C# types for columns (e.g. map Date32 to DateTime).</param>
    member this.SinkTo(writerAction: Action<IDataReader>, ?bufferSize: int, ?typeOverrides: IDictionary<string, Type>) : unit =
        let capacity = defaultArg bufferSize 5
        
        // 1. 生产者-消费者缓冲区
        // 使用 use 确保 Collection 在结束后 Dispose (虽然它主要是清理句柄)
        use buffer = new BlockingCollection<RecordBatch>(boundedCapacity = capacity)

        // 2. 启动消费者任务 (Consumer: DB Writer)
        // Task.Run 启动后台线程
        let consumerTask = Task.Run(fun () ->
            // 获取消费枚举器 (GetConsumingEnumerable 会阻塞等待直到 CompleteAdding)
            let stream = buffer.GetConsumingEnumerable()
            
            // 将 override 字典传给 ArrowToDbStream (如果提供了的话)
            // C# 的 ArrowToDbStream 构造函数应该接受 IEnumerable 和 Dictionary
            let overrides = 
                    match typeOverrides with 
                    | Some d -> new Dictionary<string, Type>(d) 
                    | None -> null
            
            // 构造伪装的 DataReader
            use reader = new ArrowToDbStream(stream, overrides)
            
            // 执行用户逻辑 (如 SqlBulkCopy)
            writerAction.Invoke reader
        )

        // 3. 启动生产者 (Producer: Polars Engine)
        // 在当前线程阻塞执行
        try
            try
                // 将 Polars 生产的 Batch 推入 Buffer
                // 如果 Buffer 满了，Buffer.Add 会阻塞，从而反压 Rust 引擎
                this.SinkBatches(fun batch -> buffer.Add batch)
            finally
                // 4. 通知消费者：没有更多数据了
                buffer.CompleteAdding()
        with
        | _ -> 
            // 如果生产者崩溃，确保 Task 也能收到异常或取消，防止死锁
            // (CompleteAdding 已经在 finally 里了，消费者会读完剩余数据后退出)
            reraise()

        // 4. 等待消费者完成并抛出可能的聚合异常
        try
            consumerTask.Wait()
        with
        | :? AggregateException as aggEx ->
            // 拆包 AggregateException，抛出真正的首个错误，让调用方好处理
            raise (aggEx.Flatten().InnerException)
    
    /// <summary>
    /// Join with another LazyFrame.
    /// </summary>
    member this.Join(other: LazyFrame, leftOn: Expr seq, rightOn: Expr seq, how: JoinType) : LazyFrame =
        // 1. 准备 Left On 表达式数组 (Clone Handle, Move 语义)
        let lOnArr = leftOn |> Seq.map (fun e -> e.CloneHandle()) |> Seq.toArray
        
        // 2. 准备 Right On 表达式数组
        let rOnArr = rightOn |> Seq.map (fun e -> e.CloneHandle()) |> Seq.toArray
        
        // 3. 准备 LazyFrame Handles
        // Join 算子会消耗左右两个 LazyFrame，所以我们需要传入 Clone 的 Handle
        let lHandle = this.CloneHandle()
        let rHandle = other.CloneHandle()

        // 4. 调用 Wrapper (假设 Wrapper 接受 int 枚举作为 JoinType)
        // 注意：Wrapper 签名通常是 (Left, Right, LeftExprs, RightExprs, JoinType)
        let newHandle = PolarsWrapper.Join(lHandle, rHandle, lOnArr, rOnArr, how.ToNative())
        
        new LazyFrame(newHandle)
    
    member this.Filter (expr: Expr) : LazyFrame =
        let lfClone = this.CloneHandle()
        let exprClone = expr.CloneHandle()
        
        let h = PolarsWrapper.LazyFilter(lfClone, exprClone)
        new LazyFrame(h)
    
    member this.Select (exprs: Expr list) : LazyFrame =
        let lfClone = this.CloneHandle()
        let handles = exprs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        
        let h = PolarsWrapper.LazySelect(lfClone, handles)
        new LazyFrame(h)
    member this.Sort (expr: Expr) (desc: bool) : LazyFrame =
        let lfClone = this.CloneHandle()
        let exprClone = expr.CloneHandle()
        let h = PolarsWrapper.LazySort(lfClone, exprClone, desc)
        new LazyFrame(h)
    member this.OrderBy = this.Sort
    member this.Limit (n: uint) : LazyFrame =
        let lfClone = this.CloneHandle()
        let h = PolarsWrapper.LazyLimit(lfClone, n)
        new LazyFrame(h)
    member this.WithColumn (expr: Expr) : LazyFrame =
        let lfClone = this.CloneHandle()
        let exprClone = expr.CloneHandle()
        let handles = [| exprClone |] // 使用克隆的 handle
        let h = PolarsWrapper.LazyWithColumns(lfClone, handles)
        new LazyFrame(h)
    member this.WithColumns (exprs: Expr list) : LazyFrame =
        let lfClone = this.CloneHandle()
        let handles = exprs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let h = PolarsWrapper.LazyWithColumns(lfClone, handles)
        new LazyFrame(h)
    member this.GroupBy (keys: Expr list) (aggs: Expr list) : LazyFrame =
        let lfClone = this.CloneHandle()
        let kHandles = keys |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let aHandles = aggs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let h = PolarsWrapper.LazyGroupByAgg(lfClone, kHandles, aHandles)
        new LazyFrame(h)
    member this.Unpivot (index: string list) (on: string list) (variableName: string option) (valueName: string option) : LazyFrame =
        let lfClone = this.CloneHandle() // 必须 Clone
        let iArr = List.toArray index
        let oArr = List.toArray on
        let varN = Option.toObj variableName
        let valN = Option.toObj valueName 
        new LazyFrame(PolarsWrapper.LazyUnpivot(lfClone, iArr, oArr, varN, valN))
    member this.Melt = this.Unpivot
/// <summary>
    /// JoinAsOf with string tolerance (e.g., "2d", "1h").
    /// </summary>
    member this.JoinAsOf(other: LazyFrame, 
                         leftOn: Expr, 
                         rightOn: Expr, 
                         byLeft: Expr list, 
                         byRight: Expr list, 
                         strategy: string option, 
                         tolerance: string option) : LazyFrame =
        
        let lClone = this.CloneHandle()
        let rClone = other.CloneHandle()
        
        let lOn = leftOn.CloneHandle()
        let rOn = rightOn.CloneHandle()
        
        // 处理分组列
        let lByArr = byLeft |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let rByArr = byRight |> List.map (fun e -> e.CloneHandle()) |> List.toArray

        // 处理可选参数
        let strat = defaultArg strategy "backward"
        let tol = Option.toObj tolerance 

        let h = PolarsWrapper.JoinAsOf(
            lClone, rClone, 
            lOn, rOn, 
            lByArr, rByArr,
            strat, tol
        )
        new LazyFrame(h)

    /// <summary>
    /// JoinAsOf with TimeSpan tolerance.
    /// </summary>
    member this.JoinAsOf(other: LazyFrame, 
                         leftOn: Expr, 
                         rightOn: Expr, 
                         byLeft: Expr list, 
                         byRight: Expr list, 
                         strategy: string option, 
                         tolerance: TimeSpan option) : LazyFrame =
        
        // 将 TimeSpan 转换为 Polars 字符串格式 (e.g. "1h30m")
        let tolStr = 
            tolerance 
            |> Option.map DurationFormatter.ToPolarsString

        // 调用上面的主重载
        this.JoinAsOf(other, leftOn, rightOn, byLeft, byRight, strategy, tolStr)
    static member Concat  (lfs: LazyFrame list) (how: ConcatType) : LazyFrame =
        let handles = lfs |> List.map (fun lf -> lf.CloneHandle()) |> List.toArray
        // 默认 rechunk=false, parallel=true (Lazy 的常见默认值)
        new LazyFrame(PolarsWrapper.LazyConcat(handles, how.ToNative(), false, true))

/// <summary>
/// SQL Context for executing SQL queries on registered LazyFrames.
/// </summary>
type SqlContext() =
    let handle = PolarsWrapper.SqlContextNew()
    
    interface IDisposable with
        member _.Dispose() = handle.Dispose()

    /// <summary> Register a LazyFrame as a table for SQL querying. </summary>
    member _.Register(name: string, lf: LazyFrame) =
        PolarsWrapper.SqlRegister(handle, name, lf.CloneHandle())

    /// <summary> Execute a SQL query and return a LazyFrame. </summary>
    member _.Execute(query: string) =
        new LazyFrame(PolarsWrapper.SqlExecute(handle, query))