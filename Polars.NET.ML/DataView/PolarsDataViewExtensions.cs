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
    /// <returns>IDataView for ML.NET</returns>
    public static IDataView AsDataView(this IPolarsDataFrame df, bool enableMacroShuffle = false)
        => new PolarsDataView(df, enableMacroShuffle);
}