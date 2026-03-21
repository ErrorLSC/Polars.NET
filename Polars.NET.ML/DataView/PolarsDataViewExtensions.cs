using Microsoft.ML;
using Polars.NET.Core;

namespace Polars.NET.ML.DataView; 

/// <summary>
/// Extension Methods for DataFrame interOps with IDataView
/// </summary>
public static class PolarsDataViewExtensions
{
    /// <summary>
    /// Convert Polars DataFrame to IDataView for ML.NET
    /// </summary>
    /// <param name="df">Polars DataFrame</param>
    /// <param name="enableMacroShuffle">
    /// If true, performs zero-copy Chunk-level shuffling in Rust. 
    /// Warning: This disables ML.NET's row-level shuffling. Best for pre-randomized data.
    /// </param>
    /// <returns>IDataView for ML.NET</returns>
    public static IDataView AsDataView(this IPolarsDataFrame df, bool enableMacroShuffle = false)
        => new PolarsDataView(df, enableMacroShuffle);
}