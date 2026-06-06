#pragma warning disable CS1591
using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;

/// <summary>
/// A unified parameter wrapper that channels various .NET index types into a Polars LazyFrame 
/// for the <c>Gather</c> operation without triggering eager evaluation.
/// </summary>
public readonly struct IntoIndexLazyFrame
{
    private readonly LazyFrame _lf;

    public static implicit operator IntoIndexLazyFrame(LazyFrame lf) => new(lf ?? throw new ArgumentNullException(nameof(lf)));

    public static implicit operator IntoIndexLazyFrame(int n) => FromSeries(Pl.CreateSeries(string.Empty, [n]));
    public static implicit operator IntoIndexLazyFrame(long n) => FromSeries(Pl.CreateSeries(string.Empty, [n]));
    public static implicit operator IntoIndexLazyFrame(ReadOnlySpan<int> n) => FromSeries(Pl.CreateSeries(string.Empty, n));
    public static implicit operator IntoIndexLazyFrame(ReadOnlySpan<long> n) => FromSeries(Pl.CreateSeries(string.Empty, n));
    public static implicit operator IntoIndexLazyFrame(Series s) => FromSeries(s);

    private static IntoIndexLazyFrame FromSeries(Series series)
    {
        ArgumentNullException.ThrowIfNull(series);

        if (!series.DataType.IsInteger)
        {
            throw new ArgumentException(
                $"The gather index Series must be of an integer data type (e.g., Int32, Int64). " +
                $"The provided Series '{series.Name}' has an invalid data type of '{series.DataType}'.", 
                nameof(series));
        }
        
        // Series -> DataFrame -> LazyFrame
        return new IntoIndexLazyFrame(series.ToFrame().Lazy());
    }

    private IntoIndexLazyFrame(LazyFrame lf)
    {
        _lf = lf;
    }

    /// <summary>
    /// Consumes and returns the underlying LazyFrame instance.
    /// </summary>
    public LazyFrame Consume() => _lf.Clone();
    
}


public partial class LazyFrame : IDisposable, IPolarsLazyFrame
{
    /// <summary>
    /// Selects rows from this LazyFrame at the given indices.
    /// </summary>
    /// <param name="index">The indices of the rows to select.</param>
    /// <param name="nullOnOob">If true when an index is out-of-bounds a null row will be generated instead of raising an error.</param>
    public LazyFrame Gather(IntoIndexLazyFrame index,bool nullOnOob = false)
        => new (PolarsWrapper.LazyFrameGather(CloneHandle(),index.Consume().Handle,nullOnOob));
    /// <summary>
    /// Take every nth row in the Frame and return as a new Frame.
    /// </summary>
    /// <param name="n">Gather every n-th row.</param>
    /// <param name="offset">Starting Index</param>
    /// <returns></returns>
    public LazyFrame GatherEvery(int n, int offset = 0)
        => Select(Pl.All().GatherEvery((ulong)n, (ulong)offset));
}

public partial class DataFrame
{
    /// <summary>
    /// Selects rows from this DataFrame at the given indices.
    /// </summary>
    /// <param name="index">The indices of the rows to select.</param>
    /// <param name="nullOnOob">If true when an index is out-of-bounds a null row will be generated instead of raising an error.</param>
    public DataFrame Gather(IntoIndexLazyFrame index,bool nullOnOob = false)
        => Lazy().Gather(index,nullOnOob).Collect();
    /// <inheritdoc cref="LazyFrame.GatherEvery(int, int)"/>
    public DataFrame GatherEvery(int n, int offset = 0)
        => Select(Pl.All().GatherEvery((ulong)n, (ulong)offset));
    /// <summary>
    /// Alias for gather
    /// </summary>
    public DataFrame Take(IntoIndexLazyFrame index,bool nullOnOob = false)
        => Gather(index,nullOnOob);   
}