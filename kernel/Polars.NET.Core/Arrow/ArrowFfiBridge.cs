// Polars.NET.Core / Arrow / ArrowFfiBridge.cs
using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Apache.Arrow.C;
using Apache.Arrow.Types;
using Polars.NET.Core.Native;
using Polars.NET.Core.Exceptions;
using Polars.NET.Core.Wrappers;
using Kaminari.Sazanami.Arrow.Interop;
using Kaminari.Sazanami.Data.Building;
using Kaminari.Sazanami.Data.Mapping;

namespace Polars.NET.Core.Arrow;

public static class ArrowFfiBridge
{
    // ==========================================
    // DataType Import (ArrowSchema)
    // ==========================================
    /// <summary>
    /// Import an Apache Arrow IArrowType from a Polars DataType handle.
    /// </summary>
    public static unsafe IArrowType ImportDataType(DataTypeHandle dataTypeHandle)
    {
        var cSchema = CArrowSchema.Create();

        try
        {
            bool success = NativeBindings.pl_datatype_export_arrow_schema(dataTypeHandle, cSchema);
            ErrorHelper.CheckBool(success);
            var field = CArrowSchemaImporter.ImportField(cSchema);
            return field.DataType;
        }
        finally
        {
            CArrowSchema.Free(cSchema);
        }
    }
    /// <summary>
    /// Create a Polars Series from an Apache.Arrow array.
    /// This allows zero-copy import of complex types (List, Struct, etc.) constructed in C#.
    /// </summary>
    /// <param name="name">Name of the Series</param>
    /// <param name="arrowArray">The C# Apache.Arrow IArrowArray instance</param>
    /// <returns>A new Series</returns>
    public static SeriesHandle ImportSeries(string name, IArrowArray arrowArray)
    {
        unsafe
        {
            var cArray = CArrowArray.Create();
            var cSchema = CArrowSchema.Create();

            try
            {
                CArrowSchemaExporter.ExportType(arrowArray.Data.DataType, cSchema);
                ArrowCExport.ExportArray(arrowArray, cArray);
                using var wide = WideIntegerFormats.PatchType(arrowArray.Data.DataType, cSchema);
                return PolarsWrapper.SeriesFromArrow(name, cArray, cSchema);
            }
            finally
            {
                // The engine takes the array (leaving it released) and only borrows the schema.
                CArrowArray.Free(cArray);
                CArrowSchema.Free(cSchema);
            }
        }
    }
    // ==========================================
    // DataFrame Import (FromArrow)
    // ==========================================
    public static unsafe DataFrameHandle ImportDataFrame(RecordBatch batch)
    {
        var cArray = CArrowArray.Create();
        var cSchema = CArrowSchema.Create();

        try
        {
            // 1. C# -> C Structs
            CArrowSchemaExporter.ExportSchema(batch.Schema, cSchema);
            ArrowCExport.ExportRecordBatch(batch, cArray);

            // 2. C Structs -> Rust DataFrame
            DataFrameHandle handle;
            using (WideIntegerFormats.PatchSchema(batch.Schema, cSchema))
                handle = NativeBindings.pl_dataframe_from_arrow_record_batch(cArray, cSchema);

            return ErrorHelper.Check(handle);
        }
        finally
        {
            // The engine takes the array (leaving it released, also when it fails) and only borrows the schema;
            // Free releases what is still ours and frees both structs.
            CArrowArray.Free(cArray);
            CArrowSchema.Free(cSchema);
        }
    }

    // ==========================================
    // DataFrame Export (ToArrow)
    // ==========================================
    public static unsafe RecordBatch ExportDataFrame(DataFrameHandle handle)
    {
        var array = CArrowArray.Create();
        var schema = CArrowSchema.Create();
        try
        {
            NativeBindings.pl_to_arrow(handle, array, schema);

            ErrorHelper.CheckVoid();

            var managedSchema = WideIntegerFormats.ImportSchema(schema);
            // The import moves the array out of the struct (no GC memory pressure, unlike Apache.Arrow's importer).
            return ArrowCImport.ImportRecordBatch(array, managedSchema);
        }
        finally
        {
            // Releases what is still ours (nothing once imported) and frees both structs.
            CArrowArray.Free(array);
            CArrowSchema.Free(schema);
        }
    }
    /// <summary>
    /// Import Arrow RecordBatch as Polars Series
    /// </summary>
    public static unsafe SeriesHandle ImportRecordBatchAsSeries(RecordBatch batch)
    {
        var cArray = CArrowArray.Create();
        var cSchema = CArrowSchema.Create();

        try
        {
            CArrowSchemaExporter.ExportSchema(batch.Schema, cSchema);
            ArrowCExport.ExportRecordBatch(batch, cArray);

            return ErrorHelper.Check(NativeBindings.pl_arrow_to_series(
                "data",
                cArray,
                cSchema
            ));
        }
        finally
        {
            // The engine takes the array (leaving it released) and only borrows the schema.
            CArrowArray.Free(cArray);
            CArrowSchema.Free(cSchema);
        }
    }
    public static RecordBatch ToRecordBatch(StructArray structArray)
    {
        var structType = (StructType)structArray.Data.DataType;
        var schema = new Schema(structType.Fields, metadata: null);
        return new RecordBatch(schema, structArray.Fields, structArray.Length);
    }

    public static StructArray ToStructArray(RecordBatch batch)
    {
        var structType = new StructType(batch.Schema.FieldsList);
        return new StructArray(
            structType,
            batch.Length,
            batch.Arrays,
            nullBitmapBuffer: ArrowBuffer.Empty,
            nullCount: 0);
    }
}
public static class ArrowStreamingExtensions
{
    /// <summary>
    /// The items as Arrow batches of <paramref name="batchSize"/> rows (the last one shorter), built in native memory by
    /// Sazanami's <see cref="ArrowRowBuilder{T}"/>; each batch is the caller's to dispose. No batch when there are no items.
    /// </summary>
    /// <param name="source">The items.</param>
    /// <param name="batchSize">The rows of each batch.</param>
    public static IEnumerable<RecordBatch> ToArrowBatches<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(
        this IEnumerable<T> source,
        int batchSize = 100_000)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);
        using var builder = new ArrowRowBuilder<T>();
        foreach (T item in source)
        {
            builder.Append(item);
            if (builder.Count >= batchSize) yield return builder.ExportBatch();
        }
        if (builder.Count > 0) yield return builder.ExportBatch();
    }

    /// <summary>A batch of no rows with <typeparamref name="T"/>'s schema (<see cref="ArrowRowBuilder{T}.Schema"/>).</summary>
    public static RecordBatch EmptyBatch<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>()
    {
        using var builder = new ArrowRowBuilder<T>();
        return builder.ExportBatch();
    }
}
