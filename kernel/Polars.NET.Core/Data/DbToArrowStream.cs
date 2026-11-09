using System.Diagnostics.CodeAnalysis;
using System.Data;
using System.Data.Common;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using Polars.NET.Core.Arrow;
using Kaminari.Sazanami.Arrow;
using Kaminari.Sazanami.Data;

namespace Polars.NET.Core.Data;
public static class DbToArrowStream
{
    /// <summary>
    /// High-Performance: IDataReader -> Arrow RecordBatch Stream
    /// Zero-Boxing on Hot Paths.
    /// </summary>
    public static IEnumerable<RecordBatch> ToArrowBatches(IDataReader reader, int batchSize = 50_000)
    {
        // 1. Resolve Schema
        var schema = ArrowTypeResolver.GetSchemaFromDataReader(reader);
        int fieldCount = reader.FieldCount;

        if (schema.FieldsList.Count != fieldCount)
        {
            throw new InvalidOperationException(
                $"Schema mismatch! Reader has {fieldCount} fields, but Schema has {schema.FieldsList.Count} fields.");
        }

        // 2. Initialize Builders
        bool isDbReader = reader is DbDataReader;

        var builders = new ColumnBuilder[fieldCount];
        for (int i = 0; i < fieldCount; i++)
        {
            var field = schema.FieldsList[i];
            var netType = reader.GetFieldType(i);

            builders[i] = ColumnBuilderFactory.Create(field, netType, isDbReader, batchSize);
        }

        // 3. Pump Loop
        int rowCount = 0;
        while (reader.Read())
        {
            for (int i = 0; i < fieldCount; i++)
            {
                builders[i].Add(reader, i);
            }

            rowCount++;

            if (rowCount >= batchSize)
            {
                yield return BuildBatch(schema, builders, rowCount);
                rowCount = 0;
            }
        }

        if (rowCount > 0)
        {
            yield return BuildBatch(schema, builders, rowCount);
        }
    }
    /// <summary>
    /// High-Performance Async: DbDataReader -> Arrow RecordBatch Stream.
    /// Fully reuses the ColumnBuilder system; zero boxing on hot paths with strictly bounded O(batch) memory.
    /// </summary>
    public static async IAsyncEnumerable<RecordBatch> ToArrowBatchesAsync(
        DbDataReader reader,
        int batchSize = 50_000,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);
        cancellationToken.ThrowIfCancellationRequested();

        // 1. Resolve Schema using existing ArrowTypeResolver
        var schema = ArrowTypeResolver.GetSchemaFromDataReader(reader);
        int fieldCount = reader.FieldCount;

        if (schema.FieldsList.Count != fieldCount)
        {
            throw new InvalidOperationException(
                $"Schema mismatch! Reader has {fieldCount} fields, but Schema has {schema.FieldsList.Count} fields.");
        }

        // 2. Initialize Builders once (Builders automatically clear internal buffers on Build())
        var builders = new ColumnBuilder[fieldCount];
        for (int i = 0; i < fieldCount; i++)
        {
            var field = schema.FieldsList[i];
            var netType = reader.GetFieldType(i);
            builders[i] = ColumnBuilderFactory.Create(field, netType, isDbReader: true, batchSize);
        }

        // 3. Async Pump Loop
        int rowCount = 0;
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            for (int i = 0; i < fieldCount; i++)
            {
                builders[i].Add(reader, i);
            }

            rowCount++;

            if (rowCount >= batchSize)
            {
                yield return BuildBatch(schema, builders, rowCount);
                rowCount = 0;
            }
        }

        if (rowCount > 0)
        {
            yield return BuildBatch(schema, builders, rowCount);
        }
    }

    private static RecordBatch BuildBatch(Schema schema, ColumnBuilder[] builders, int length)
    {
        var arrays = new IArrowArray[builders.Length];
        for (int i = 0; i < builders.Length; i++)
        {
            arrays[i] = builders[i].Build();
        }
        return new RecordBatch(schema, arrays, length);
    }
}
