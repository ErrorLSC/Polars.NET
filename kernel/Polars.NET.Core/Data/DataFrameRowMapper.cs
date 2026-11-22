using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Wrappers;
using Kaminari.Sazanami.Data.Mapping;

namespace Polars.NET.Core.Data;

/// <summary>
/// Reads the rows of a native DataFrame through the Arrow row mapper.
/// </summary>
public static class DataFrameRowMapper<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>
{
    /// <summary>
    /// Exports a DataFrameHandle to an Arrow RecordBatch in a single FFI call and binds a fast row cursor. With
    /// <paramref name="foldNulls"/> (query results), an object whose mapped columns are all null is null, as an outer
    /// join leaves a missing side; without it (a frame's rows), it is an object of nulls.
    /// </summary>
    public static ArrowBatchCursor<T> CreateCursor(DataFrameHandle dfHandle, bool foldNulls = false)
    {
        long height = PolarsWrapper.DataFrameHeight(dfHandle);
        if (height == 0L)
        {
            return ArrowRowMapper<T>.CreateEmptyCursor();
        }

        RecordBatch batch = ArrowFfiBridge.ExportDataFrame(dfHandle);
        return ArrowRowMapper<T>.CreateCursor(batch, ownsBatch: true, foldNulls);
    }
}
