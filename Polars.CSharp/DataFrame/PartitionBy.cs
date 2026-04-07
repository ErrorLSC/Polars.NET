using Polars.NET.Core;
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
                using var keySlice = p.Select(resolvedCols);
                dict.Add(keySlice.Row(0), p);
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