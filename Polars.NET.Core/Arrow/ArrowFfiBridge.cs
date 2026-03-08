// Polars.NET.Core / Arrow / ArrowFfiBridge.cs
using Apache.Arrow;
using Apache.Arrow.C;
using Apache.Arrow.Types;
using Polars.NET.Core.Native;

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
            NativeBindings.pl_datatype_export_arrow_schema(dataTypeHandle, cSchema);

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
            var cArray = new CArrowArray();
            var cSchema = new CArrowSchema();

            CArrowSchemaExporter.ExportType(arrowArray.Data.DataType, &cSchema);
            CArrowArrayExporter.ExportArray(arrowArray, &cArray);

            return PolarsWrapper.SeriesFromArrow(name, &cArray, &cSchema);
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
            CArrowArrayExporter.ExportRecordBatch(batch, cArray);

            // 2. C Structs -> Rust DataFrame
            var handle = NativeBindings.pl_dataframe_from_arrow_record_batch(cArray, cSchema);
            
            return ErrorHelper.Check(handle);
        }
        catch
        {
            // if (cArray != null) cArray->release = null;
            // if (cSchema != null) cSchema->release = null;
            CArrowArray.Free(cArray);
            CArrowSchema.Free(cSchema);
            throw;
        }
    }

    // ==========================================
    // DataFrame Export (ToArrow)
    // ==========================================
    public static unsafe RecordBatch ExportDataFrame(DataFrameHandle handle)
    {
        var array = CArrowArray.Create();
        var schema = CArrowSchema.Create();

        bool ownershipTransferred = false;

        try
        {
            NativeBindings.pl_to_arrow(handle, array, schema);
            
            ErrorHelper.CheckVoid();

            var managedSchema = CArrowSchemaImporter.ImportSchema(schema);
            var batch = CArrowArrayImporter.ImportRecordBatch(array, managedSchema);
            ownershipTransferred = true;
            return batch;
        }
        finally
        {
            if (!ownershipTransferred)
            {
                CArrowArray.Free(array);
            }
            CArrowSchema.Free(schema);
        }
    }
    /// <summary>
    /// Helper: Convert IEnumerable<T> directly to RecordBatch
    /// This bridges the gap between ArrowConverter (returns Array) and ImportDataFrame (needs Batch).
    /// </summary>
    public static RecordBatch BuildRecordBatch<T>(IEnumerable<T> data)
    {
        // Use ArrowConverter to build StructArray
        var arrowArray = ArrowConverter.Build(data);

        if (arrowArray is not StructArray structArray)
        {
            throw new ArgumentException($"Type {typeof(T).Name} did not result in a StructArray. Is it a primitive type? DataFrame.ofRecords expects objects/records.");
        }

        // Unbox StructArray as RecordBatch
        var structType = (StructType)structArray.Data.DataType;
        
        // Build Schema
        var schema = new Schema(structType.Fields, null); // null for metadata

        // Build RecordBatch
        return new RecordBatch(schema, structArray.Fields, structArray.Length);
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
            CArrowArrayExporter.ExportRecordBatch(batch, cArray);

            return NativeBindings.pl_arrow_to_series(
                "data", 
                cArray, 
                cSchema
            );
        }
        catch
        {
            CArrowArray.Free(cArray);
            CArrowSchema.Free(cSchema);
            throw;
        }
    }
}
public static class ArrowStreamingExtensions
{
    /// <summary>
    /// Divide huge dataflow as multi-RecordBatch to save memory
    /// </summary>
    /// <param name="source">Source DataFlow </param>
    /// <param name="batchSize"> the Size of each Batch </param>
    public static IEnumerable<RecordBatch> ToArrowBatches<T>(
        this IEnumerable<T> source, 
        int batchSize = 100_000)
    {
        // Prepare buffer
        var buffer = new List<T>(batchSize);

        foreach (var item in source)
        {
            buffer.Add(item);

            if (buffer.Count >= batchSize)
            {
                yield return BuildBatchFromBuffer(buffer);
                
                buffer.Clear();
            }
        }

        if (buffer.Count > 0)
        {
            yield return BuildBatchFromBuffer(buffer);
        }
    }

    private static RecordBatch BuildBatchFromBuffer<T>(List<T> buffer)
        => ArrowFfiBridge.BuildRecordBatch(buffer);
}
