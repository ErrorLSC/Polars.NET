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
}