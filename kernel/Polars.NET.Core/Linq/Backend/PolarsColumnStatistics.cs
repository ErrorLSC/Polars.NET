using System.Collections.Concurrent;
using Apache.Arrow.Types;
using Kaminari.Samidare;
using Kaminari.Samidare.IR;
using Microsoft.FSharp.Collections;
using Microsoft.FSharp.Core;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Wrappers;

namespace Polars.NET.Core.Linq.Backend;

/// <summary>
/// Column statistics read from the file behind a LazyFrame while a query is translated.
/// Only a small file table is read, and only a column that a filter tests, once per process for each scan of a given
/// plan: what the statistics change is the choice of plan, so a wrong or stale one costs speed, never correctness. The
/// tables that filters most often reduce are dimensions, which is what makes reading them whole affordable. In-memory
/// frames have no stable identity to remember the result by, and are small enough for fixed selectivities.
/// </summary>
internal static partial class PolarsColumnStatistics
{
    private const long MaxTableRows = 500_000L;
    private const int MaxListedValues = 32;
    /// <summary>Above this many distinct values the frequent values are not worth reading back.</summary>
    private const long MaxValuesRead = 4096L;
    private const int MaxRemembered = 20_000;
    private const int HistogramBuckets = 32;

    private static readonly ConcurrentDictionary<(string, string), Lazy<ColumnStatistics?>> Cache = new();
    private static readonly ConcurrentDictionary<(string, string), Lazy<(LiteralValue Low, LiteralValue High)?>> Ranges = new();

    private static bool IsNumberLike(LogicalType t) =>
        t is LogicalType.Arrow
        {
            arrowType: Int8Type or Int16Type or Int32Type or Int64Type or UInt8Type or UInt16Type or UInt32Type or UInt64Type
                or FloatType or DoubleType or Decimal128Type or Date32Type,
        };

    private static bool IsText(LogicalType t) => t is LogicalType.Arrow { arrowType: StringType or StringViewType or LargeStringType };

    private static ColumnStatistics Read(LazyFrameHandle handle, string column, bool numeric, long tableRows)
    {
        var key = numeric
            ? PolarsScalarLowerer.Lower(ScalarExpr.NewCast(ScalarExpr.NewColumn(column), LogicalTypes.Float64, false))
            : PolarsWrapper.Col(column);
        var nonNull = PolarsWrapper.LazyFilter(
            PolarsWrapper.LazySelect(PolarsWrapper.LazyClone(handle), [PolarsWrapper.Alias(key, "value")]),
            PolarsScalarLowerer.Lower(ScalarExpr.NewUnary(UnaryOp.IsNotNull, ScalarExpr.NewColumn("value"))));
        var counted = PolarsWrapper.LazySelect(
            PolarsWrapper.LazyGroupByAgg(nonNull, [PolarsWrapper.Col("value")], [PolarsWrapper.Alias(PolarsWrapper.Len(), "n")], null!, false),
            [
                PolarsWrapper.Col("value"),
                PolarsScalarLowerer.Lower(ScalarExpr.NewAlias(ScalarExpr.NewCast(ScalarExpr.NewColumn("n"), LogicalTypes.Float64, false), "n")),
            ]);
        using var groups = PolarsWrapper.LazyCollect(counted, PlEngine.Auto);
        long distinct = PolarsWrapper.DataFrameHeight(groups);
        using var values = PolarsWrapper.DataFrameGetColumn(groups, "value");
        using var counts = PolarsWrapper.DataFrameGetColumn(groups, "n");
        double CountOf(long i) => PolarsWrapper.SeriesGetDoubleFast(counts, i);
        double nonNullRows = 0;
        for (long i = 0; i < distinct; i++) nonNullRows += CountOf(i);
        double Share(double rows) => rows / Math.Max(1L, tableRows);
        LiteralValue ValueOf(long i) =>
            numeric ? LiteralValue.NewFloat64(PolarsWrapper.SeriesGetDoubleFast(values, i)) : LiteralValue.NewString(PolarsWrapper.SeriesGetStringFast(values, i));
        FSharpList<Tuple<LiteralValue, double>> frequent = distinct > MaxValuesRead
            ? FSharpList<Tuple<LiteralValue, double>>.Empty
            : ListModule.OfSeq(
                Enumerable.Range(0, (int)distinct)
                    .Select(i => (long)i)
                    .OrderByDescending(CountOf)
                    .Take(MaxListedValues)
                    .Select(i => Tuple.Create(ValueOf(i), Share(CountOf(i)))));
        (double Value, double Count)[] sortedCounts = numeric && distinct > 0
            ? [.. Enumerable.Range(0, (int)distinct).Select(i => (PolarsWrapper.SeriesGetDoubleFast(values, i), CountOf(i))).OrderBy(p => p.Item1)]
            : [];
        FSharpOption<LiteralValue>? min = null, max = null;
        FSharpOption<double[]>? histogram = null;
        if (sortedCounts.Length > 0)
        {
            min = LiteralValue.NewFloat64(sortedCounts[0].Value);
            max = LiteralValue.NewFloat64(sortedCounts[^1].Value);
            var boundaries = new double[HistogramBuckets + 1];
            System.Array.Fill(boundaries, sortedCounts[0].Value);
            double seen = 0.0;
            int next = 1;
            foreach (var (value, count) in sortedCounts)
            {
                seen += count;
                while (next < HistogramBuckets && seen >= nonNullRows * next / HistogramBuckets)
                {
                    boundaries[next] = value;
                    next++;
                }
            }
            boundaries[HistogramBuckets] = sortedCounts[^1].Value;
            histogram = boundaries;
        }
        return new ColumnStatistics(
            Math.Max(0.0, 1.0 - Share(nonNullRows)),
            (double)distinct,
            min,
            max,
            frequent,
            histogram,
            StatisticsOrigin.Exact);
    }

    private static (LiteralValue, LiteralValue)? ReadRange(LazyFrameHandle handle, string column)
    {
        ExprHandle Bound(string name, Func<ExprHandle, ExprHandle> aggregate) =>
            PolarsWrapper.Alias(aggregate(PolarsScalarLowerer.Lower(ScalarExpr.NewCast(ScalarExpr.NewColumn(column), LogicalTypes.Float64, false))), name);
        var present = PolarsScalarLowerer.Lower(ScalarExpr.NewAlias(
            ScalarExpr.NewCast(ScalarExpr.NewAggregation(AggKind.Count, ScalarExpr.NewColumn(column), false), LogicalTypes.Float64, false),
            "present"));
        var frame = PolarsWrapper.LazySelect(PolarsWrapper.LazyClone(handle), [Bound("low", PolarsWrapper.Min), Bound("high", PolarsWrapper.Max), present]);
        using var bounds = PolarsWrapper.LazyCollect(frame, PlEngine.Auto);
        using var low = PolarsWrapper.DataFrameGetColumn(bounds, "low");
        using var high = PolarsWrapper.DataFrameGetColumn(bounds, "high");
        using var count = PolarsWrapper.DataFrameGetColumn(bounds, "present");
        double lowest = PolarsWrapper.SeriesGetDoubleFast(low, 0L), highest = PolarsWrapper.SeriesGetDoubleFast(high, 0L);
        // The native getter reads a null as zero, so a column without values must be recognized by its count.
        if (PolarsWrapper.SeriesGetDoubleFast(count, 0L) == 0.0 || double.IsNaN(lowest) || double.IsNaN(highest)) return null;
        return (LiteralValue.NewFloat64(lowest), LiteralValue.NewFloat64(highest));
    }

    /// <summary>
    /// The range from a Parquet footer, for the types whose stored minimum and maximum are their values: a decimal's
    /// are unscaled, and a 64-bit unsigned value does not fit the signed statistics.
    /// </summary>
    private static (LiteralValue, LiteralValue)? TryFooterRange(LazyFrameHandle handle, LogicalType dataType, string column)
    {
        if (dataType is not LogicalType.Arrow { arrowType: Int8Type or Int16Type or Int32Type or Int64Type or UInt8Type or UInt16Type or UInt32Type or Date32Type })
            return null;
        return PolarsWrapper.LazyParquetIntRange(handle, column) is { } range
            ? (LiteralValue.NewFloat64(range.Min), LiteralValue.NewFloat64(range.Max))
            : null;
    }

    private static LogicalField? FieldOf(LogicalSchema schema, string column) => schema.Fields.FirstOrDefault(f => f.Name == column);

    /// <summary>
    /// The lowest and highest values of a number or date column of a file table: from its footer when the file is a
    /// Parquet file with statistics, otherwise by reading that column once.
    /// </summary>
    public static (LiteralValue Low, LiteralValue High)? TryGetRange(LazyFrameHandle handle, LogicalSchema schema, string column)
    {
        if (FieldOf(schema, column) is not { } field || !IsNumberLike(field.DataType)) return null;
        if (FileTableIdentity(handle) is not { } identity) return null;
        if (Ranges.Count >= MaxRemembered) Ranges.Clear();
        var entry = Ranges.GetOrAdd((identity, column), _ => new Lazy<(LiteralValue, LiteralValue)?>(() =>
        {
            try
            {
                return TryFooterRange(handle, field.DataType, column) ?? ReadRange(handle, column);
            }
            catch
            {
                return null;
            }
        }));
        return entry.Value;
    }

    /// <summary>The statistics of a filtered column of a small file table, or null when reading it is not worthwhile.</summary>
    public static ColumnStatistics? TryGet(LazyFrameHandle handle, LogicalSchema schema, long? rows, string column)
    {
        if (rows is not { } tableRows || tableRows > MaxTableRows) return null;
        if (FieldOf(schema, column) is not { } field || !(IsNumberLike(field.DataType) || IsText(field.DataType))) return null;
        if (FileTableIdentity(handle) is not { } identity) return null;
        if (Cache.Count >= MaxRemembered) Cache.Clear();
        var entry = Cache.GetOrAdd((identity, column), _ => new Lazy<ColumnStatistics?>(() =>
        {
            try
            {
                return Read(handle, column, IsNumberLike(field.DataType), tableRows);
            }
            catch
            {
                return null;
            }
        }));
        return entry.Value;
    }

    /// <summary>
    /// The plan of a file table (what its statistics are remembered by), or null for another plan: a file scan of the
    /// engine, or a managed scan of files (a resolver of the seseragi plugin, its resolution count left out: it grows
    /// as queries run). A managed scan of memory has no identity to remember it by.
    /// </summary>
    private static string? FileTableIdentity(LazyFrameHandle handle)
    {
        string plan = PolarsWrapper.Explain(handle, false);
        if (plan.Contains("RESOLVER[name: seseragi: ", StringComparison.Ordinal))
            return plan.Contains("[memory]", StringComparison.Ordinal) ? null : CachedResolves().Replace(plan, "");
        return plan.Contains("SCAN [", StringComparison.Ordinal) ? plan : null;
    }

    [System.Text.RegularExpressions.GeneratedRegex(@", cached_resolves: \d+")]
    private static partial System.Text.RegularExpressions.Regex CachedResolves();
}
