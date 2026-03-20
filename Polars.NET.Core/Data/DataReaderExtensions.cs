using System.Data;
using Apache.Arrow;
using Polars.NET.Core.Arrow;

namespace Polars.NET.Core.Data;

public static class DataReaderExtensions
{
    /// <summary>
    /// From SchemaTable of IDataReader infer Arrow Schema.
    /// </summary>
    public static Schema GetArrowSchema(this IDataReader reader)
        => ArrowTypeResolver.GetSchemaFromDataReader(reader);

    /// <summary>
    /// Convert IDataReader to Arrow RecordBatch Stream
    /// </summary>
    public static IEnumerable<RecordBatch> ToArrowBatches(this IDataReader reader, int batchSize = 50_000)
        => DbToArrowStream.ToArrowBatches(reader,batchSize);
}
