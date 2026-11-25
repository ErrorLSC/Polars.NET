using Kaminari.Samidare;
using Kaminari.Samidare.Config;
using Kaminari.Samidare.IR;
using Microsoft.FSharp.Core;
using Polars.NET.Core.Delta;
using Polars.NET.Core.Helpers;
using Polars.NET.Core.Wrappers;

namespace Polars.NET.Core.Linq.Backend;

/// <summary>Physical Polars LazyFrame implementation of ILogicalTableSource.</summary>
internal sealed class PolarsTableSource(LazyFrameHandle handle, LogicalSchema schema)
    : ILogicalTableSource, ISourceStatistics, ISourceIdentity, IColumnStatisticsSource
{
    private readonly Lazy<long?> _estimatedRows = new(() => PolarsWrapper.LazyEstimatedRows(handle));

    private readonly Lazy<string?> _identity = new(() =>
    {
        string plan = PolarsWrapper.Explain(handle, false);
        return plan.Contains("SCAN [") ? plan : null;
    });

    public LazyFrameHandle Handle => handle;

    public string Name => "PolarsLazyFrame";

    public LogicalSchema Schema => schema;

    public FSharpOption<long>? EstimatedRows => _estimatedRows.Value.ToOption();

    public FSharpOption<string>? SourceIdentity => _identity.Value.ToOption();

    public FSharpOption<ColumnStatistics>? TryGetColumnStatistics(string column) =>
        SamidareConfig.CollectColumnStatistics ? PolarsColumnStatistics.TryGet(handle, schema, _estimatedRows.Value, column).ToOption() : null;

    public FSharpOption<Tuple<LiteralValue, LiteralValue>>? TryGetColumnRange(string column) =>
        SamidareConfig.CollectColumnStatistics && PolarsColumnStatistics.TryGetRange(handle, schema, column) is { } range
            ? IrOption.Some(Tuple.Create(range.Low, range.High))
            : null;
}

/// <summary>Frames as table sources.</summary>
internal static class PolarsTableSources
{
    /// <summary>The schema, with column types, of a physical LazyFrameHandle.</summary>
    public static LogicalSchema SchemaOf(LazyFrameHandle lf)
    {
        static Tuple<string, LogicalType> FieldOf((string Name, DataTypeHandle Type) field)
        {
            using var dataType = field.Type;
            return Tuple.Create(field.Name, PolarsVocabulary.OfDataTypeHandle(dataType));
        }
        Tuple<string, LogicalType>[] fields = [.. PolarsSchemaExtractor.GetFields(lf).Select(FieldOf)];
        return LogicalSchema.OfFields(fields);
    }

    /// <summary>Wraps a physical LazyFrameHandle as an opaque ILogicalTableSource (schema, with column types, read from the frame).</summary>
    public static ILogicalTableSource OfLazyFrame(LazyFrameHandle lf) => new PolarsTableSource(lf, SchemaOf(lf));
}

/// <summary>
/// A Delta table scanned by the managed kernel, as a query source: a frame <c>LazyFrame.ScanDelta</c> (or a catalog
/// table scan) returned, whose origin says how to scan it again. The filter a query puts right on top of it arrives as a
/// hint (IFilterAwareSource) and leaves out the files whose partition values and statistics rule every row out (the
/// kernel's file skipping, from the IR as it is); the query still filters the rows. Statistics and identity are the
/// frame's.
/// </summary>
internal sealed class DeltaKernelSource(DeltaScanOrigin origin, PolarsTableSource frame, ScalarExpr? filter, Action<int>? scannedFiles)
    : ILogicalTableSource, ISourceStatistics, ISourceIdentity, IColumnStatisticsSource, IFilterAwareSource
{
    /// <summary>
    /// A source over a frame a Delta scan made, with the origin it carries. <paramref name="scannedFiles"/>, when not
    /// null, is told how many data files each lowering of a query scans.
    /// </summary>
    public DeltaKernelSource(DeltaScanOrigin origin, LazyFrameHandle frame, Action<int>? scannedFiles)
        : this(origin, new PolarsTableSource(frame, PolarsTableSources.SchemaOf(frame)), null, scannedFiles)
    {
    }

    /// <summary>A source over the table at <paramref name="path"/> at <paramref name="version"/> (null: the latest when the source is made).</summary>
    public static DeltaKernelSource Of(string path, long? version, Action<int>? scannedFiles)
    {
        var frame = DeltaKernelScan.ScanPath(path, version, null, null, null, null, null, null, out long read, out _);
        var origin = new DeltaScanOrigin(read, (f, scanned) => DeltaKernelScan.ScanPath(path, read, null, null, null, scanned, null, f));
        return new DeltaKernelSource(origin, frame, scannedFiles);
    }

    /// <summary>The row filter the source scans with; null when it reads every file.</summary>
    public ScalarExpr? Filter => filter;

    /// <summary>
    /// The table as a lazy frame, without the files the filter rules out: scanned again with the filter, or the frame
    /// itself when there is none. When the new scan fails (the catalog client released, the version's log cleaned up)
    /// the frame is read whole: the same rows, without skipping.
    /// </summary>
    public LazyFrameHandle Open()
    {
        if (filter is null) return PolarsWrapper.LazyClone(frame.Handle);
        try
        {
            return origin.Scan(filter, scannedFiles);
        }
        catch (Exception e)
        {
            System.Diagnostics.Trace.TraceWarning($"[Polars.NET] Delta file skipping not applied, the table is read whole: {e.Message}");
            return PolarsWrapper.LazyClone(frame.Handle);
        }
    }

    public string Name => "DeltaKernel";

    public LogicalSchema Schema => frame.Schema;

    public FSharpOption<long>? EstimatedRows => frame.EstimatedRows;

    public FSharpOption<string>? SourceIdentity => frame.SourceIdentity;

    public FSharpOption<ColumnStatistics>? TryGetColumnStatistics(string column) => frame.TryGetColumnStatistics(column);

    public FSharpOption<Tuple<LiteralValue, LiteralValue>>? TryGetColumnRange(string column) => frame.TryGetColumnRange(column);

    public ILogicalTableSource WithFilterHint(ScalarExpr predicate) =>
        new DeltaKernelSource(origin, frame, filter is null ? predicate : ScalarExpr.NewBinary(BinaryOp.And, filter, predicate), scannedFiles);
}

/// <summary>Binding frames to query sources: a frame a Delta scan returned as it is becomes a DeltaKernelSource.</summary>
internal static class DeltaKernelSources
{
    /// <summary>
    /// The query source of <paramref name="frame"/> (<paramref name="original"/>, or a clone of it): a Delta scan's,
    /// with file skipping, when <paramref name="original"/> carries the scan's origin; else the frame itself.
    /// </summary>
    public static ILogicalTableSource OfLazyFrame(LazyFrameHandle original, LazyFrameHandle frame) =>
        original.DeltaOrigin is { } origin ? new DeltaKernelSource(origin, frame, null) : PolarsTableSources.OfLazyFrame(frame);
}
