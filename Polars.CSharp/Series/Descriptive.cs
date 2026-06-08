using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;
namespace Polars.CSharp;

public partial class Series : IDisposable,IPolarsSeries
{
    /// <summary>
    /// Get the length of each individual chunk.
    /// </summary>
    public long[] ChunkLengths()
    {
        nuint[] nativeLengths = PolarsWrapper.SeriesChunkLengths(Handle);
        
        long[] lengths = new long[nativeLengths.Length];
        for (int i = 0; i < nativeLengths.Length; i++)
        {
            lengths[i] = (long)nativeLengths[i]; 
        }

        return lengths;
    }
    /// <inheritdoc cref="DataFrame.Describe"/>
    public DataFrame Describe()
    {
        using var df = ToFrame();
        return df.Describe();
    }
    /// <inheritdoc cref="DataFrame.EstimatedSize"/> 
    public double EstimatedSize(SizeUnit unit = SizeUnit.Bytes)
    {
        long bytes = PolarsWrapper.SeriesEstimatedSize(Handle);

        return unit switch
        {
            SizeUnit.Bytes     => bytes, 
            SizeUnit.Kilobytes => bytes / 1024.0,
            SizeUnit.Megabytes => bytes / Math.Pow(1024, 2),
            SizeUnit.Gigabytes => bytes / Math.Pow(1024, 3),
            SizeUnit.Terabytes => bytes / Math.Pow(1024, 4),
            _ => throw new ArgumentOutOfRangeException(nameof(unit), $"Unsupported size unit: {unit}")
        };
    }
    /// <summary>
    /// Check whether the Series contains one or more null values.
    /// </summary>
    public bool HasNulls() => PolarsWrapper.SeriesHasNulls(Handle);
    /// <summary>
    /// True if the Series is empty.
    /// </summary>
    public bool IsEmpty(bool ignoreNulls = false)
        => ignoreNulls 
            ? Length == NullCount
            : Length == 0;
    /// <summary>
    /// Gets the number of underlying Arrow memory chunks.
    /// </summary>
    public long NChunks => (long)PolarsWrapper.SeriesChunkCounts(Handle);

    /// <summary>
    /// Determines if the Series memory is physically contiguous (i.e., consists of a single chunk).
    /// </summary>
    public bool IsContiguous => NChunks == 1L;
    /// <summary>
    /// Check whether this series is finite
    /// </summary>
    public Series IsFinite() => new(PolarsWrapper.SeriesIsFinite(Handle));
    /// <summary>
    /// Check whether this series is infinite
    /// </summary>
    public Series IsInfinite() => new(PolarsWrapper.SeriesIsInfinite(Handle));
    /// <summary>
    /// Return a boolean mask indicating the first occurrence of each distinct value.
    /// </summary>
    public Series IsFirstDistinct() => new(PolarsWrapper.SeriesIsFirstDistinct(Handle));
    /// <summary>
    /// Return a boolean mask indicating the last occurrence of each distinct value.
    /// </summary>
    public Series IsLastDistinct() => new(PolarsWrapper.SeriesIsLastDistinct(Handle));
    /// <summary>
    /// Get a boolean mask indicating which values are unique.
    /// <para>Implemented via DataFrame expression composition.</para>
    /// </summary>
    public Series IsUnique() => new(PolarsWrapper.SeriesIsUnique(Handle));
    /// <summary>
    /// Get a boolean mask indicating which values are duplicated.
    /// <para>Implemented via DataFrame expression composition.</para>
    /// </summary>
    public Series IsDuplicated() => new(PolarsWrapper.SeriesIsDuplicated(Handle));
    /// <summary>
    /// Check if elements of this Series are in the other Series.
    /// </summary>
    public Series IsIn(Series other, bool nullsEqual = false)
    {
        DataType dtype = other.DataType;

        bool isNested = dtype.Kind == DataTypeKind.List || dtype.Kind == DataTypeKind.Array;

        if (isNested)
        {
            return new(PolarsWrapper.SeriesIsIn(Handle, other.Handle, nullsEqual));
        }
        else
        {
            using var implodedOther = other.Implode();
            return new(PolarsWrapper.SeriesIsIn(Handle, implodedOther.Handle, nullsEqual));
        }
    }
    /// <summary>
    /// Check if elements of this Series are in the collections.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="collection"></param>
    /// <param name="nullsEqual"></param>
    public Series IsIn<T>(IEnumerable<T?> collection,bool nullsEqual=false)
    {
        using var other = From("__TEMP_FOR_ISIN",collection);
        return IsIn(other,nullsEqual);
    }
    /// <summary>
    /// Return a Boolean series, where null value will be masked as true.
    /// </summary>
    public Series IsNull() => new(PolarsWrapper.SeriesIsNull(Handle));
    /// <summary>
    /// Return a Boolean series, where null value will be masked as false.
    /// </summary>
    public Series IsNotNull() => new(PolarsWrapper.SeriesIsNotNull(Handle));
    /// <summary>
    /// Check whether this series is NaN
    /// </summary>
    public Series IsNan() => new(PolarsWrapper.SeriesIsNan(Handle));
    /// <summary>
    /// Check whether this series is not NaN
    /// </summary>
    public Series IsNotNan() => new(PolarsWrapper.SeriesIsNotNan(Handle));
    /// <summary>
    /// Count the number of unique values in this Series.
    /// </summary>
    public long NUnique() => PolarsWrapper.SeriesNUnique(Handle);
    /// <summary>
    /// Return the lower bound of this Series’ dtype as a unit Series.
    /// </summary>
    public Series LowerBound() => ApplyExpr(Pl.Col(Name).LowerBound());
    /// <summary>
    /// Return the upper bound of this Series’ dtype as a unit Series.
    /// </summary>
    public Series UpperBound() => ApplyExpr(Pl.Col(Name).UpperBound());
    /// <summary>
    /// Return a count of the unique values in the order of appearance.
    /// </summary>
    public Series UniqueCounts() => new(PolarsWrapper.SeriesUniqueCounts(Handle));
    /// <summary>
    /// Count the occurrences of unique values.
    /// <para>
    /// Similar to SQL <c>GROUP BY val COUNT(*)</c>.
    /// </para>
    /// </summary>
    /// <param name="sort">Sort the output by count in descending order. Default is true.</param>
    /// <param name="parallel">Execute in parallel. Default is true.</param>
    /// <param name="name">Give the resulting count column a specific name; if normalize is True this defaults to “proportion”, otherwise defaults to “count”..</param>
    /// <param name="normalize">If true, the count column will contain probabilities (fractions) instead of absolute counts. Default is false.</param>
    /// <returns>A DataFrame with the series values and their counts.</returns>
    /// <example>
    /// <code>
    /// var s = Series.From("fruit", ["apple", "apple", "banana"]);
    /// 
    /// // Default: sorted, absolute counts
    /// s.ValueCounts().Show();
    /// 
    /// // Normalized (percentage)
    /// s.ValueCounts(normalize: true, name: "prob").Show();
    /// // Result
    /// ┌────────┬───────┐
    /// │ fruit  ┆ prob  │
    /// │ ---    ┆ ---   │
    /// │ str    ┆ u32   │
    /// ╞════════╪═══════╡
    /// │ apple  ┆ 3     │
    /// │ orange ┆ 2     │
    /// │ banana ┆ 1     │
    /// └────────┴───────┘
    /// </code>
    /// </example>
    public DataFrame ValueCounts(bool sort = true, bool parallel = true, string? name = null, bool normalize = false)
        => new(PolarsWrapper.SeriesValueCounts(Handle, sort, parallel, name, normalize));
}