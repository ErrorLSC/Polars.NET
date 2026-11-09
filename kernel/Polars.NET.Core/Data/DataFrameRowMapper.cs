using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Wrappers;
using Kaminari.Sazanami.Data;

namespace Polars.NET.Core.Data;

/// <summary>
/// Reads the rows of a native DataFrame through the Arrow row mapper.
/// </summary>
public static class DataFrameRowMapper<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>
{
    /// <summary>
    /// Exports a DataFrameHandle to an Arrow RecordBatch in a single FFI call and binds a fast row cursor.
    /// </summary>
    public static ArrowBatchCursor<T> CreateCursor(DataFrameHandle dfHandle)
    {
        long height = PolarsWrapper.DataFrameHeight(dfHandle);
        if (height == 0L)
        {
            return ArrowRowMapper<T>.CreateEmptyCursor();
        }

        RecordBatch batch = ArrowFfiBridge.ExportDataFrame(dfHandle);
        return ArrowRowMapper<T>.CreateCursor(batch, ownsBatch: true);
    }
}
