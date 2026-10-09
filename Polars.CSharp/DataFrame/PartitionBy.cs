using Polars.NET.Core;
using Polars.NET.Core.Helpers;
using Cs = Polars.CSharp.Polars.Selectors;

namespace Polars.CSharp;

/// <summary>
/// DataFrame represents a 2-dimensional labeled data structure similar to a table or spreadsheet.
/// </summary>
public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Split into multiple DataFrames partitioned by groups.
    /// </summary>
    public DataFrame[] PartitionBy(params IntoSelector[] by)
        => PartitionBy(by, maintainOrder: true, includeKey: true);

    /// <summary>
    /// Convenience overload for Params usage.
    /// </summary>
    public DataFrame[] PartitionBy(IntoSelector by, bool maintainOrder = true, bool includeKey = true)
        => PartitionBy([by], maintainOrder, includeKey);
    /// <summary>
    /// Group by the given columns and return the groups as separate dataframes.
    /// </summary>
    /// <param name="by">Column name(s) or selector(s) to group by.</param>
    /// <param name="maintainOrder">Ensure that the order of the groups is consistent with the input data. This is slower than a default partition by operation.</param>
    /// <param name="includeKey">Include the columns used to partition the DataFrame in the output.</param>
    /// <returns></returns>
    public DataFrame[] PartitionBy(IEnumerable<IntoSelector> by, bool maintainOrder = true, bool includeKey = true)
    {
        var resolvedCols = by.SelectMany(s => Cs.ExpandSelector(this, s.Consume()))
                             .Distinct()
                             .ToArray();

        return PartitionByInternal(resolvedCols, maintainOrder, includeKey);
    }
    /// <inheritdoc cref="PartitionByAsDict(IEnumerable{IntoSelector},bool,bool)"/>
    public Dictionary<object?[], DataFrame> PartitionByAsDict(params IntoSelector[] by)
        => PartitionByAsDict(by, maintainOrder: true, includeKey: true);
    /// <inheritdoc cref="PartitionByAsDict(IEnumerable{IntoSelector},bool,bool)"/>
    public Dictionary<object?[], DataFrame> PartitionByAsDict(IntoSelector by, bool maintainOrder = true, bool includeKey = true)
        => PartitionByAsDict([by], maintainOrder, includeKey);
    /// <summary>
    /// Split into multiple DataFrames, returning a dictionary mapping the group keys to the DataFrames.
    /// </summary>
    /// <param name="by">Column name(s) or selector(s) to group by.</param>
    /// <param name="maintainOrder">Ensure that the order of the groups is consistent with the input data. This is slower than a default partition by operation.</param>
    /// <param name="includeKey">Include the columns used to partition the DataFrame in the output.</param>
    public Dictionary<object?[], DataFrame> PartitionByAsDict(IEnumerable<IntoSelector> by, bool maintainOrder = true, bool includeKey = true)
    {
        var resolvedCols = by.SelectMany(s => Cs.ExpandSelector(this, s.Consume()))
                             .Distinct()
                             .ToArray();

        if (!includeKey && !maintainOrder)
        {
            throw new ArgumentException("Cannot use `PartitionByAsDict` with `maintainOrder=false` and `includeKey=false`. Group keys cannot be matched to partitions.");
        }

        var partitions = PartitionByInternal(resolvedCols, maintainOrder, includeKey);
        
        var dict = new Dictionary<object?[], DataFrame>(ObjectArrayComparer.Instance);

        if (includeKey)
        {
            foreach (var p in partitions)
            {
                var row = new object?[resolvedCols.Length];
                for (int i = 0; i < resolvedCols.Length; i++)
                {
                    using var col = p[resolvedCols[i]];
                    row[i] = col.GetValue<object>(0L, uncheck: true);
                }
                dict.Add(row, p);
            }
        }
        else
        {
            using var uniqueKeysDf = this.Select(resolvedCols).Unique(maintainOrder: true);
            for (int i = 0; i < uniqueKeysDf.Height; i++)
            {
                dict.Add(uniqueKeysDf.Row(i), partitions[i]);
            }
        }

        return dict;
    }
    /// <summary>
    /// Partitions the DataFrame by specified columns into a strongly typed dictionary.
    /// Supports domain model keys (record struct / record class), scalar identifiers (string, int), and tuples.
    /// </summary>
    /// <typeparam name="TKey">The strongly typed domain key type.</typeparam>
    /// <param name="by">Column selectors to partition by.</param>
    /// <param name="maintainOrder">Ensure partition order matches input data sequence. Default is true.</param>
    /// <param name="includeKey">Whether to retain key columns in each partition DataFrame. Default is true.</param>
    /// <param name="comparer">Optional equality comparer for custom domain key hashing.</param>
    /// <returns>A dictionary mapping strongly typed domain keys to their respective partition DataFrames.</returns>
    public Dictionary<TKey, DataFrame> PartitionByAsDict<TKey>(
        IEnumerable<IntoSelector> by,
        bool maintainOrder = true,
        bool includeKey = true,
        IEqualityComparer<TKey>? comparer = null)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(by);

        var resolvedCols = by.SelectMany(s => Cs.ExpandSelector(this, s.Consume()))
                             .Distinct()
                             .ToArray();

        if (resolvedCols.Length == 0)
        {
            throw new ArgumentException("At least one partition column must be specified.", nameof(by));
        }

        if (!includeKey && !maintainOrder)
        {
            throw new ArgumentException("Cannot use PartitionByAsDict with maintainOrder=false and includeKey=false. Group keys cannot be matched to partitions.");
        }

        var partitions = PartitionByInternal(resolvedCols, maintainOrder, includeKey);
        var dict = new Dictionary<TKey, DataFrame>(partitions.Length, comparer ?? EqualityComparer<TKey>.Default);

        if (partitions.Length == 0)
        {
            return dict;
        }

        bool isSingleScalar = resolvedCols.Length == 1 && PolarsTypeHelper.IsScalarType(typeof(TKey));

        if (includeKey)
        {
            if (isSingleScalar)
            {
                // Fast-path: Single scalar domain key extracted directly from Row 0 of the resolved key column
                var keyColName = resolvedCols[0];
                foreach (var p in partitions)
                {
                    if (p.Height == 0L) continue;
                    using var col = p[keyColName];
                    var key = col.GetValue<TKey>(0L, uncheck: true);
                    dict.Add(key!, p);
                }
            }
            else
            {
                // Composite domain key: Compile RowMapper once against schema of partition 0
                var keyMapper = RowMapper<TKey>.GetOrCreate(partitions[0]);
                foreach (var p in partitions)
                {
                    if (p.Height == 0L) continue;
                    var key = keyMapper(p, 0L);
                    dict.Add(key, p);
                }
            }
        }
        else
        {
            // Fallback when partition DataFrames strip key columns: hydrate keys from unique distinct key slice
            using var uniqueKeysDf = this.Select(resolvedCols).Unique(maintainOrder: true);
            long keyHeight = uniqueKeysDf.Height;

            if (isSingleScalar)
            {
                var keyColName = resolvedCols[0];
                using var keyCol = uniqueKeysDf[keyColName];
                for (long i = 0L; i < keyHeight; i++)
                {
                    var key = keyCol.GetValue<TKey>(i, uncheck: true);
                    dict.Add(key!, partitions[i]);
                }
            }
            else
            {
                var keyMapper = RowMapper<TKey>.GetOrCreate(uniqueKeysDf);
                for (long i = 0L; i < keyHeight; i++)
                {
                    var key = keyMapper(uniqueKeysDf, i);
                    dict.Add(key, partitions[i]);
                }
            }
        }

        return dict;
    }

    // --- Private Helpers ---

    private DataFrame[] PartitionByInternal(string[] byCols, bool maintainOrder, bool includeKey)
    {
        var handles = PolarsWrapper.PartitionBy(Handle, byCols, maintainOrder, includeKey);
        
        var result = new DataFrame[handles.Length];
        for (int i = 0; i < handles.Length; i++)
        {
            result[i] = new DataFrame(handles[i]);
        }
        return result;
    }
    /// <summary>
    /// Helper class to compare object arrays by their content, enabling their use as Dictionary keys.
    /// </summary>
    private sealed class ObjectArrayComparer : IEqualityComparer<object?[]>
    {
        public static readonly ObjectArrayComparer Instance = new();

        public bool Equals(object?[]? x, object?[]? y)
        {
            if (ReferenceEquals(x, y)) return true;
            if (x is null || y is null) return false;
            if (x.Length != y.Length) return false;
            
            for (int i = 0; i < x.Length; i++)
            {
                if (!Equals(x[i], y[i])) return false;
            }
            return true;
        }

        public int GetHashCode(object?[] obj)
        {
            if (obj is null) return 0;
            
            var hash = new HashCode();
            foreach (var item in obj)
            {
                hash.Add(item);
            }
            return hash.ToHashCode();
        }
    }
}