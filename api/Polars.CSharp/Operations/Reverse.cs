using Polars.NET.Core.Wrappers;

namespace Polars.CSharp;

public partial class LazyFrame : IDisposable
{
    /// <summary>
    /// Reverse the LazyFrame.
    /// </summary>
    public LazyFrame Reverse()
        => new(PolarsWrapper.LazyFrameReverse(CloneHandle()));
}

public partial class DataFrame
{
    /// <summary>
    /// Reverse the DataFrame.
    /// </summary>
    public DataFrame Reverse() => Lazy().Reverse().Collect();
}