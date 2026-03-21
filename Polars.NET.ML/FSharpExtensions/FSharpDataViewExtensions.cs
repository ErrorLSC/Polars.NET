using Microsoft.ML;
using Polars.FSharp;
using Polars.NET.ML.DataView;

namespace Polars.NET.ML.FSharpExtensions;

/// <summary>
/// Extension Methods for DataFrame interOps with IDataView
/// </summary>
public static class PolarsFSharpDataViewExtensions
{
    /// <summary>
    /// Conver IDataView to Polars DataFrame
    /// </summary>
    public static DataFrame ToDataFrame(this IDataView dataview)
    {
        var handle = DataViewToPolarsExtensions.ToPolarsDataFrameHandle(dataview);
        return new DataFrame(handle);
    }
}