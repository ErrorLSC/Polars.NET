using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;
public partial class LazyFrame : IDisposable, IPolarsLazyFrame
{   
    /// <summary>
    /// Reverse the LazyFrame.
    /// </summary>
    public LazyFrame Reverse() => Select(Pl.All().Reverse());
}

public partial class DataFrame
{   
    /// <summary>
    /// Reverse the DataFrame.
    /// </summary>
    public DataFrame Reverse() => Select(Pl.All().Reverse());
}