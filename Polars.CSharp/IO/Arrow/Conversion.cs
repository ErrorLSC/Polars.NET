using Apache.Arrow;
using Polars.NET.Core;
using Polars.NET.Core.Arrow;

namespace Polars.CSharp;
public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Create DataFrame from Apache Arrow RecordBatch.
    /// </summary>
    public static DataFrame FromArrow(RecordBatch batch)
        => new(ArrowFfiBridge.ImportDataFrame(batch));
    /// <summary>
    /// Transfer a DataFrame to Arrow
    /// </summary>
    /// <returns></returns>
    public RecordBatch ToArrow() => ArrowFfiBridge.ExportDataFrame(Handle);
}

public partial class Series : IDisposable,IPolarsSeries
{
    /// <summary>
    /// Zero-copy convert to Apache Arrow Array.
    /// </summary>
    public IArrowArray ToArrow() => PolarsWrapper.SeriesToArrow(Handle);
    /// <summary>
    /// Low-level entry point: Create Series from existing Arrow Array.
    /// </summary>
    public static Series FromArrow(string name, IArrowArray arrowArray) => new(ArrowFfiBridge.ImportSeries(name, arrowArray));
}