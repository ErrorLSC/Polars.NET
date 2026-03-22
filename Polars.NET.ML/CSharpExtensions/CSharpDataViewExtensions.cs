using Microsoft.ML;
using Polars.CSharp;
using Polars.NET.ML.DataView;

namespace Polars.NET.ML.CSharpExtensions;

/// <summary>
/// Extension Methods for DataFrame interOps with IDataView
/// </summary>
public static class PolarsCSharpDataViewExtensions
{
    /// <summary>
    /// Conver IDataView to Polars DataFrame
    /// </summary>
    public static DataFrame ToDataFrame(this IDataView dataview,int batchSize = 64000)
    {
        var handle = DataViewToPolarsExtensions.ToPolarsDataFrameHandle(dataview, batchSize);
        return new DataFrame(handle);
    }
}