using System.Collections.Concurrent;
using System.Data;
using Apache.Arrow;
using Apache.Arrow.C;
using Polars.NET.Core;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Data;

namespace Polars.CSharp;

/// <summary>
/// Represents a lazily evaluated DataFrame.
/// Until the query is executed, operations are just recorded in a query plan.
/// Once executed, the data is materialized in memory.
/// </summary>
public class LazyFrame : IDisposable
{
    internal LazyFrameHandle Handle { get; }

    internal LazyFrame(LazyFrameHandle handle)
    {
        Handle = handle;
    }

    // ==========================================
    // 工厂方法 (Scan IO)
    // ==========================================
    /// <summary>
    /// Scans a CSV file lazily.
    /// </summary>
    public static LazyFrame ScanCsv(
        string path,
        Dictionary<string, DataType>? schema = null,
        bool hasHeader = true,
        char separator = ',',
        ulong skipRows = 0,
        bool tryParseDates = true) // [新增参数]
    {
        var schemaHandles = schema?.ToDictionary(
            kv => kv.Key, 
            kv => kv.Value.Handle
        );

        var handle = PolarsWrapper.ScanCsv(
            path, 
            schemaHandles, 
            hasHeader, 
            separator, 
            skipRows,
            tryParseDates // 传递给 Wrapper
        );

        return new LazyFrame(handle);
    }
    /// <summary>
    /// Read a Parquet file as a LazyFrame.
    /// </summary>
    /// <param name="path"></param>
    /// <returns></returns>
    public static LazyFrame ScanParquet(string path)
    {
        //
        return new LazyFrame(PolarsWrapper.ScanParquet(path));
    }
    /// <summary>
    /// Read an IPC (Feather) file as a LazyFrame.
    /// </summary>
    /// <param name="path"></param>
    /// <returns></returns>
    public static LazyFrame ScanIpc(string path)
    {
        //
        return new LazyFrame(PolarsWrapper.ScanIpc(path));
    }
    /// <summary>
    /// Read a NDJSON file as a LazyFrame.
    /// </summary>
    /// <param name="path"></param>
    /// <returns></returns>
    public static LazyFrame ScanNdjson(string path)
    {
        //
        return new LazyFrame(PolarsWrapper.ScanNdjson(path));
    }
    /// <summary>
    /// Scan Arrow Stream As LazyFrame
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="data"></param>
    /// <param name="batchSize"></param>
    /// <returns></returns>
    public static LazyFrame ScanArrowStream<T>(IEnumerable<T> data, int batchSize = 100_000)
    {
        // 1. 定义流生成器
        // 注意：这里只是定义，还没开始读
        IEnumerable<RecordBatch> StreamGenerator() => data.ToArrowBatches(batchSize);

        // 2. 预读 Schema (不可避免的开销)
        // 我们必须先拿出一个枚举器来看看第一帧，从而确定 Schema
        using var probeEnumerator = StreamGenerator().GetEnumerator();
        
        if (!probeEnumerator.MoveNext()) 
        {
            // 空流兜底：利用 T 反射生成空 DataFrame
            return DataFrame.From(Enumerable.Empty<T>()).Lazy();
        }
        
        var schema = probeEnumerator.Current.Schema;
        // 探测完毕，关闭这个探测用的枚举器
        // (假设 data 是可重放的 IEnumerable，如果不是，需要由用户显式传入 Schema 的重载)
        
        // 3. 调用 Core 层
        // 传入一个工厂 lambda，每次 Rust 需要扫描时，都会从头创建一个新的枚举器
        var handle = ArrowStreamInterop.ScanStream(
            () => StreamGenerator().GetEnumerator(), 
            schema
        );
        
        return new LazyFrame(handle);
    }

    /// <summary>
    /// Scan Arrow Stream As LazyFrame with schema input
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="data"></param>
    /// <param name="schema"></param>
    /// <param name="batchSize"></param>
    /// <returns></returns>
    public static LazyFrame ScanArrowStream<T>(IEnumerable<T> data, Schema schema, int batchSize = 100_000)
    {
        var handle = ArrowStreamInterop.ScanStream(
            () => data.ToArrowBatches(batchSize).GetEnumerator(),
            schema
        );
        return new LazyFrame(handle);
    }

    /// <summary>
    /// 底层入口：直接扫描 RecordBatch 流。
    /// 如果提供了 schema，则不会尝试读取第一行来探测（避免副作用）。
    /// </summary>
    public static LazyFrame ScanRecordBatches(IEnumerable<RecordBatch> stream, Schema schema = null!)
    {
        // 1. 确定 Schema (防止空流 Peek)
        if (schema == null)
        {
            // 这里必须短暂 Peek 一下流来获取 Schema
            // 注意：这假设 stream 是可重放的 (IEnumerable)，
            // 如果是只读一次的网络流，用户必须显式传递 schema，否则第一帧数据会丢失
            using var enumerator = stream.GetEnumerator();
            
            if (!enumerator.MoveNext())
                throw new InvalidOperationException("Cannot scan empty stream without schema. Please provide a schema explicitly.");
            
            schema = enumerator.Current.Schema;
        }

        // 2. 委托给 Core 层处理所有脏活
        // 我们只需要提供一个工厂方法，让 Rust 可以在需要时获取新的迭代器
        var handle = ArrowStreamInterop.ScanStream(
            stream.GetEnumerator, 
            schema
        );

        return new LazyFrame(handle);
    }
    /// <summary>
    /// 
    /// </summary>
    /// <param name="reader"></param>
    /// <param name="batchSize"></param>
    /// <returns></returns>
    public static LazyFrame ScanDatabase(IDataReader reader, int batchSize = 50_000)
    {
        // 1. 显式获取 Schema (为了传给 ScanRecordBatches，防止它去 Peek)
        var schema = reader.GetArrowSchema();
        
        // 2. 获取流
        var stream = reader.ToArrowBatches(batchSize);

        // 3. 调用底层，传入 Schema
        return ScanRecordBatches(stream, schema);
    }
    /// <summary>
    /// Lazy scan from a database using a factory.
    /// Recommended for scenarios where the query might be executed multiple times.
    /// </summary>
    /// <param name="readerFactory">A function that creates a NEW IDataReader instance each time.</param>
    /// <param name="batchSize">Define the size of the batch</param>
    public static LazyFrame ScanDatabase(Func<IDataReader> readerFactory, int batchSize = 50_000)
    {
        // 1. 预读 Schema (Probe)
        // 因为我们需要先构建 Logical Plan，所以必须先看一眼元数据
        // 我们创建一个临时的 Reader，看完 Schema 立刻销毁
        Schema schema;
        using (var probeReader = readerFactory())
        {
            schema = probeReader.GetArrowSchema();
        }

        // 2. 定义可重放的流 (Replayable Stream)
        // 这是一个本地函数，利用 C# 的迭代器状态机
        IEnumerable<RecordBatch> ReplayableStream()
        {
            // 每次枚举开始时，调用工厂创建一个全新的 Reader
            using var reader = readerFactory();
            
            // 转换为 Arrow 流并透传
            foreach (var batch in reader.ToArrowBatches(batchSize))
            {
                yield return batch;
            }
            
            // 循环结束，reader 自动 Dispose
        }

        // 3. 调用底层 Scan
        // 我们显式传入 schema，避免底层再次探测
        return ScanRecordBatches(ReplayableStream(), schema);
    }
    // ==========================================
    // Meta / Inspection
    // ==========================================

    /// <summary>
    /// Fetch the schema as a dictionary of column names and their data types.
    /// </summary>
    public Dictionary<string, string> Schema => PolarsWrapper.GetSchema(Handle);

    /// <summary>
    /// Fetch the schema as a JSON string.
    /// </summary>
    public string SchemaString => PolarsWrapper.GetSchemaString(Handle);

    /// <summary>
    /// Get an explanation of the query plan.
    /// </summary>
    public string Explain(bool optimized = true)
    {
        return PolarsWrapper.Explain(Handle, optimized);
    }
    /// <summary>
    /// Clone the LazyFrame, creating a new independent copy.
    /// </summary>
    /// <returns></returns>
    public LazyFrame Clone()
    {
        //
        return new LazyFrame(PolarsWrapper.LazyClone(Handle));
    }
    internal LazyFrameHandle CloneHandle()
    {
        return PolarsWrapper.LazyClone(Handle);
    }
    // ==========================================
    // Transformations
    // ==========================================
    /// <summary>
    /// Select specific columns or expressions.
    /// </summary>
    /// <param name="exprs"></param>
    /// <returns></returns>
    public LazyFrame Select(params Expr[] exprs)
    {
        var lfClone = this.CloneHandle();
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        // LazySelect 会消耗当前的 Handle
        return new LazyFrame(PolarsWrapper.LazySelect(lfClone, handles));
    }
    /// <summary>
    /// Filter rows based on a boolean expression.
    /// </summary>
    /// <param name="expr"></param>
    /// <returns></returns>
    public LazyFrame Filter(Expr expr)
    {
        var lfClone = this.CloneHandle();
        var h = PolarsWrapper.CloneExpr(expr.Handle);
        //
        return new LazyFrame(PolarsWrapper.LazyFilter(lfClone, h));
    }
    /// <summary>
    /// Add or modify columns based on expressions.
    /// </summary>
    /// <param name="exprs"></param>
    /// <returns></returns>
    public LazyFrame WithColumns(params Expr[] exprs)
    {
        var lfClone = this.CloneHandle();
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        //
        return new LazyFrame(PolarsWrapper.LazyWithColumns(lfClone, handles));
    }
    /// <summary>
    /// Sort the DataFrame by an expression.
    /// </summary>
    /// <param name="by"></param>
    /// <param name="descending"></param>
    /// <returns></returns>
    public LazyFrame Sort(Expr by, bool descending = false)
    {
        var lfClone = this.CloneHandle();
        var h = PolarsWrapper.CloneExpr(by.Handle);
        //
        return new LazyFrame(PolarsWrapper.LazySort(lfClone, h, descending));
    }
    /// <summary>
    /// Limit the number of rows in the LazyFrame.
    /// </summary>
    /// <param name="n"></param>
    /// <returns></returns>
    public LazyFrame Limit(uint n)
    {
        var lfClone = this.CloneHandle();
        return new LazyFrame(PolarsWrapper.LazyLimit(lfClone, n));
    }
    /// <summary>
    /// Explode list-like columns into multiple rows.
    /// </summary>
    /// <param name="exprs"></param>
    /// <returns></returns>
    public LazyFrame Explode(params Expr[] exprs)
    {
        var lfClone = this.CloneHandle();
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        //
        return new LazyFrame(PolarsWrapper.LazyExplode(lfClone, handles));
    }

    // ==========================================
    // Reshaping
    // ==========================================
    /// <summary>
    /// Unpivot (Melt) the DataFrame from wide to long format.
    /// </summary>
    /// <param name="index"></param>
    /// <param name="on"></param>
    /// <param name="variableName"></param>
    /// <param name="valueName"></param>
    /// <returns></returns>
    public LazyFrame Unpivot(string[] index, string[] on, string variableName = "variable", string valueName = "value")
    {
        var lfClone = this.CloneHandle();
        return new LazyFrame(PolarsWrapper.LazyUnpivot(lfClone, index, on, variableName, valueName));
    }
    /// <summary>
    /// Melt the DataFrame from wide to long format.
    /// </summary>
    /// <param name="index"></param>
    /// <param name="on"></param>
    /// <param name="variableName"></param>
    /// <param name="valueName"></param>
    /// <returns></returns>
    public LazyFrame Melt(string[] index, string[] on, string variableName = "variable", string valueName = "value") 
        => Unpivot(index, on, variableName, valueName);
    /// <summary>
    /// Concatenate multiple LazyFrames into one.
    /// </summary>
    /// <param name="how"></param>
    /// <param name="lfs"></param>
    /// <param name="rechunk"></param>
    /// <param name="parallel"></param>
    /// <returns></returns>
    public static LazyFrame Concat(
        IEnumerable<LazyFrame> lfs, 
        ConcatType how = ConcatType.Vertical, 
        bool rechunk = false, 
        bool parallel = true)
    {
        var lfClones = lfs.Select(l => l.CloneHandle()).ToArray();
        var handles = lfClones.Select(l => l).ToArray();
        return new LazyFrame(PolarsWrapper.LazyConcat(handles, how.ToNative(), rechunk, parallel));
    }

    // ==========================================
    // Join
    // ==========================================
    /// <summary>
    /// Join with another LazyFrame on specified columns.
    /// </summary>
    /// <param name="other"></param>
    /// <param name="leftOn"></param>
    /// <param name="rightOn"></param>
    /// <param name="how"></param>
    /// <returns></returns>
    public LazyFrame Join(LazyFrame other, Expr[] leftOn, Expr[] rightOn, JoinType how = JoinType.Inner)
    {
        var lOn = leftOn.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        var rOn = rightOn.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        var lfClone = this.CloneHandle();
        var otherClone = other.CloneHandle();
        // Join 消耗 left(this) 和 right(other)
        return new LazyFrame(PolarsWrapper.Join(
            lfClone, 
            otherClone, 
            lOn, 
            rOn, 
            how.ToNative()
        ));
    }
    /// <summary>
    /// Join with another LazyFrame on a single column.
    /// </summary>
    /// <param name="other"></param>
    /// <param name="leftOn"></param>
    /// <param name="rightOn"></param>
    /// <param name="how"></param>
    /// <returns></returns>
    public LazyFrame Join(LazyFrame other, Expr leftOn, Expr rightOn, JoinType how = JoinType.Inner)
    {
        return Join(other, [leftOn], [rightOn], how);
    }

    /// <summary>
    /// Perform an As-Of Join (time-series join).
    /// </summary>
    public LazyFrame JoinAsOf(
        LazyFrame other, 
        Expr leftOn, Expr rightOn, 
        string? tolerance = null,
        string strategy = "backward",
        Expr[]? leftBy = null,
        Expr[]? rightBy = null)
    {
        var lfClone = CloneHandle();
        var otherClone = other.CloneHandle();
        var lOn = PolarsWrapper.CloneExpr(leftOn.Handle);
        var rOn = PolarsWrapper.CloneExpr(rightOn.Handle);
        
        var lBy = leftBy?.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        var rBy = rightBy?.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();

        return new LazyFrame(PolarsWrapper.JoinAsOf(
            lfClone, otherClone,
            lOn, rOn,
            lBy, rBy,
            strategy, tolerance
        ));
    }
    /// <summary>
    /// Perform an As-Of Join with tolerance as timespan (time-series join)
    /// </summary>
    /// <param name="other"></param>
    /// <param name="leftOn"></param>
    /// <param name="rightOn"></param>
    /// <param name="tolerance"></param>
    /// <param name="strategy"></param>
    /// <param name="leftBy"></param>
    /// <param name="rightBy"></param>
    /// <returns></returns>
    public LazyFrame JoinAsOf(
    LazyFrame other, 
    Expr leftOn, Expr rightOn, 
    TimeSpan tolerance,
    string strategy = "backward",
    Expr[]? leftBy = null,
    Expr[]? rightBy = null)
    {
        return JoinAsOf(other,leftOn,rightOn,DurationFormatter.ToPolarsString(tolerance),strategy,leftBy,rightBy);
    }
    // ==========================================
    // GroupBy
    // ==========================================
    /// <summary>
    /// Start a GroupBy operation on specified keys.
    /// </summary>
    /// <param name="keys"></param>
    /// <returns></returns>
    public LazyGroupBy GroupBy(params Expr[] keys)
    {
        var lfClone = this.CloneHandle();
        
        return new LazyGroupBy(lfClone, keys);
    }
    /// <summary>
    /// Group by dynamic windows based on a time index.
    /// </summary>
    public LazyDynamicGroupBy GroupByDynamic(
        string indexColumn,
        TimeSpan every,
        TimeSpan? period = null,
        TimeSpan? offset = null,
        Expr[]? by = null,
        Label label = Label.Left, // [修改] 默认 Left
        bool includeBoundaries = false,
        ClosedWindow closedWindow = ClosedWindow.Left,
        StartBy startBy = StartBy.WindowBound
    )
    {
        string everyStr = DurationFormatter.ToPolarsString(every);
        string periodStr = DurationFormatter.ToPolarsString(period) ?? everyStr;
        string offsetStr = DurationFormatter.ToPolarsString(offset) ?? "0s";

        var keys = by ?? [];
        return new LazyDynamicGroupBy(
            this.CloneHandle(),
            indexColumn,
            everyStr,
            periodStr,
            offsetStr,
            keys,
            label, // [修改]
            includeBoundaries,
            closedWindow,
            startBy
        );
    }
    // ==========================================
    // Execution (Collect)
    // ==========================================

    /// <summary>
    /// Execute the query plan and return a DataFrame.
    /// </summary>
    public DataFrame Collect()
    {
        //
        return new DataFrame(PolarsWrapper.LazyCollect(Handle));
    }

    /// <summary>
    /// Execute the query plan using the streaming engine.
    /// </summary>
    public DataFrame CollectStreaming()
    {
        //
        return new DataFrame(PolarsWrapper.CollectStreaming(Handle));
    }
    /// <summary>
    /// Execute the query plan asynchronously and return a DataFrame.
    /// </summary>
    public async Task<DataFrame> CollectAsync()
    {
        var dfHandle = await PolarsWrapper.LazyCollectAsync(Handle);
        return new DataFrame(dfHandle);
    }
    // ==========================================
    // Output Sink (IO)
    // ==========================================
    /// <summary>
    /// Sink the LazyFrame to a Parquet file.
    /// </summary>
    /// <param name="path"></param>
    public void SinkParquet(string path)
    {
        //
        PolarsWrapper.SinkParquet(Handle, path);
    }
    /// <summary>
    /// Sink the LazyFrame to a CSV file.
    /// </summary>
    /// <param name="path"></param>
    public void SinkIpc(string path)
    {
        //
        PolarsWrapper.SinkIpc(Handle, path);
    }
    /// <summary>
    /// Sink the LazyFrame to JSON file.
    /// </summary>
    /// <param name="path"></param>
    public void SinkJson(string path)
    {
        //
        PolarsWrapper.SinkJson(Handle, path);
    }
    /// <summary>
    /// Sink the LazyFrame to CSV file.
    /// </summary>
    /// <param name="path"></param>
    public void SinkCsv(string path)
    {
        //
        PolarsWrapper.SinkCsv(Handle, path);
    }
    /// <summary>
    /// 通用流式 Sink：每计算出一批数据，就触发一次回调。
    /// 这是实现自定义 Sink（如数据库、网络流、消息队列）的基础。
    /// </summary>
    public void SinkBatches(Action<Apache.Arrow.RecordBatch> onBatchReceived)
    {
        // CloneHandle() 增加引用计数，确保 this 不受影响，
        // 而 Clone 出来的 handle 会在 Wrapper 里被 TransferOwnership 给 Rust 消耗掉
        using var newLfHandle = PolarsWrapper.SinkBatches(this.CloneHandle(), onBatchReceived);

        // 驱动流式执行
        using var lfRes = new LazyFrame(newLfHandle);
        using var _ = lfRes.CollectStreaming(); 
    }
    /// <summary>
    /// 通用流式 Sink 接口：将 LazyFrame 计算结果流式转换为 IDataReader 并交给 writerAction 处理。
    /// 全程内存占用极低 (O(1))。
    /// 用户可以在 writerAction 里使用 SqlBulkCopy, NpgsqlBinaryImporter 等工具。
    /// </summary>
    /// <param name="writerAction">接收 IDataReader 的回调 (在独立线程执行)</param>
    /// <param name="bufferSize">缓冲区大小 (Batch 数量)</param>
    /// <param name="typeOverrides">Target Schema</param>
    public void SinkTo(Action<IDataReader> writerAction, int bufferSize = 5,Dictionary<string, Type>? typeOverrides = null)
    {
        // 1. 生产者-消费者缓冲区
        using var buffer = new BlockingCollection<RecordBatch>(boundedCapacity: bufferSize);

        // 2. 启动消费者 (DB Writer)
        var consumerTask = Task.Run(() => 
        {
            // ArrowToDbStream 负责把 Buffer 伪装成 DataReader
            // 它会自动处理 Dispose，所以 writerAction 读完后 Batch 就会被释放
            using var reader = new ArrowToDbStream(buffer.GetConsumingEnumerable(),typeOverrides);
            
            // [核心] 将 reader 移交给用户逻辑
            // 用户在这里调用 bulk.WriteToServer(reader)
            writerAction(reader);
        });

        // 3. 启动生产者 (Polars Engine - 当前线程阻塞执行)
        try
        {
            // 将 Rust 生产的数据推入 Buffer
            // 如果 Buffer 满了，这里会阻塞，从而自动反压 Rust 引擎
            SinkBatches(buffer.Add);
        }
        finally
        {
            // 4. 通知消费者：没有更多数据了
            buffer.CompleteAdding();
        }

        // 5. 等待消费者写入完成，并抛出可能的异常
        consumerTask.Wait();
    }
    /// <summary>
    /// Dispose the LazyFrame and release native resources.
    /// </summary>
    public void Dispose()
    {
        Handle?.Dispose();
    }
}