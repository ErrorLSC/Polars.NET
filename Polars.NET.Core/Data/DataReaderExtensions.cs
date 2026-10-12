using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;
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
    /// <summary>
    /// Asynchronously streams database cursor records in chunks of Apache Arrow RecordBatches.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IAsyncEnumerable<RecordBatch> ToArrowBatchesAsync(
        this DbDataReader reader,
        int batchSize = 50_000,
        CancellationToken cancellationToken = default)
        => DbToArrowStream.ToArrowBatchesAsync(reader, batchSize, cancellationToken);
    
}
