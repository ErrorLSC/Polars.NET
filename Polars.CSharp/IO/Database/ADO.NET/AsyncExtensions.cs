using System.Data.Common;
using System.Runtime.CompilerServices;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Data;

namespace Polars.CSharp;

/// <summary>
/// Async Polars Extensions for DbDataReader
/// </summary>
public static partial class DatabaseExtensions
{
    /// <summary>
    /// Asynchronously streams database cursor records into chunks of native Polars DataFrames.
    /// </summary>
    /// <param name="reader">The active asynchronous database data reader.</param>
    /// <param name="batchSize">Maximum number of rows per Arrow/DataFrame batch.</param>
    /// <param name="cancellationToken">Optional cooperative cancellation token.</param>
    /// <returns>An asynchronous stream of materialized DataFrame chunks.</returns>
    public static async IAsyncEnumerable<DataFrame> ReadDatabaseBatchesAsync(
        this DbDataReader reader,
        int batchSize = 50_000,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);
        cancellationToken.ThrowIfCancellationRequested();

        // Directly consume the async Arrow RecordBatch stream pumped by DbToArrowStream
        await foreach (var batch in DbToArrowStream.ToArrowBatchesAsync(reader, batchSize, cancellationToken).ConfigureAwait(false))
        {
            using (batch)
            {
                if (batch.Length == 0)
                {
                    continue;
                }

                // Bridge Arrow RecordBatch directly into a native DataFrame handle via C Data Interface
                var handle = ArrowFfiBridge.ImportDataFrame(batch);
                yield return new DataFrame(handle);
            }
        }
    }

    /// <summary>
    /// Streams strongly typed rows asynchronously from DbDataReader.
    /// </summary>
    public static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(
        this DbDataReader reader,
        int batchSize = 50_000,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);
        cancellationToken.ThrowIfCancellationRequested();

        Func<DataFrame, long, T>? mapper = null;

        await foreach (var chunkDf in reader.ReadDatabaseBatchesAsync(batchSize, cancellationToken).ConfigureAwait(false))
        {
            using (chunkDf)
            {
                if (chunkDf.Height == 0)
                {
                    continue;
                }

                mapper ??= RowMapper<T>.GetOrCreate(chunkDf);
                long height = chunkDf.Height;

                for (long i = 0L; i < height; i++)
                {
                    yield return mapper(chunkDf, i);
                }
            }
        }
    }
}