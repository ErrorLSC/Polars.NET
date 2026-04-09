using Polars.NET.Core;

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
    public bool IsEmpty => Length == 0;
    /// <summary>
    /// Gets the number of underlying Arrow memory chunks.
    /// </summary>
    public long NChunks => (long)PolarsWrapper.SeriesChunkCounts(Handle);

    /// <summary>
    /// Determines if the Series memory is physically contiguous (i.e., consists of a single chunk).
    /// </summary>
    public bool IsContiguous => NChunks == 1L;
    /// <summary>
    /// Check whether this series is NaN
    /// </summary>
    /// <returns></returns>
    public Series IsNan() => new(PolarsWrapper.SeriesIsNan(Handle));
    /// <summary>
    /// Check whether this series is not NaN
    /// </summary>
    /// <returns></returns>
    public Series IsNotNan() => new(PolarsWrapper.SeriesIsNotNan(Handle));
    /// <summary>
    /// Check whether this series is finite
    /// </summary>
    /// <returns></returns>
    public Series IsFinite() => new(PolarsWrapper.SeriesIsFinite(Handle));
    /// <summary>
    /// Check whether this series is infinite
    /// </summary>
    /// <returns></returns>
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
}