using Polars.Native;

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
    /// Read a CSV file as a LazyFrame.
    /// </summary>
    /// <param name="path"></param>
    /// <param name="tryParseDates"></param>
    /// <returns></returns>
    public static LazyFrame ScanCsv(string path, bool tryParseDates = true)
    {
        //
        return new LazyFrame(PolarsWrapper.ScanCsv(path, tryParseDates));
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
        var lfClone = this.CloneHandle();
        var otherClone = other.CloneHandle();
        var lOn = PolarsWrapper.CloneExpr(leftOn.Handle);
        var rOn = PolarsWrapper.CloneExpr(rightOn.Handle);
        
        var lBy = leftBy?.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        var rBy = rightBy?.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();

        //
        return new LazyFrame(PolarsWrapper.JoinAsOf(
            lfClone, otherClone,
            lOn, rOn,
            lBy, rBy,
            strategy, tolerance
        ));
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
    /// Dispose the LazyFrame and release native resources.
    /// </summary>
    public void Dispose()
    {
        Handle?.Dispose();
    }
}