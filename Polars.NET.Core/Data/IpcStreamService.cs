using System.Data;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;

namespace Polars.NET.Core.Data;

public static class IpcStreamService
{
    public abstract class TempIpcScope : IDisposable
    {
        public string? FilePath { get; protected set; }

        public void Dispose()
        {
            if (File.Exists(FilePath))
            {
                try { File.Delete(FilePath); } catch { /* Ignore */ }
            }
            GC.SuppressFinalize(this);
        }
    }

    public class TempIpcScope<T> : TempIpcScope
    {
        public TempIpcScope(IEnumerable<T> data, int batchSize)
        {
            FilePath = StartBufferedFileWriter(data, batchSize);
        }
    }

    public class TempIpcScopeReader : TempIpcScope
    {
        public TempIpcScopeReader(IDataReader reader, int batchSize)
        {
            FilePath = StartBufferedFileWriter(reader, batchSize);
        }
    }

    public static string StartBufferedFileWriter<T>(IEnumerable<T> data, int batchSize = 100_000)
    {
        string filePath = Path.GetTempFileName(); 
        WriteDataToFile(filePath, data, batchSize);
        return filePath;
    }

    public static string StartBufferedFileWriter(IDataReader reader, int batchSize)
    {
        var filePath = Path.GetTempFileName();
        try
        {
            var originalSchema = reader.GetArrowSchema();
            var ipcSchema = DowngradeSchemaForIpc(originalSchema);

            using var stream = File.OpenWrite(filePath);
            using var writer = new ArrowFileWriter(stream, ipcSchema);

            long rowsWritten = 0;

            foreach (var batch in reader.ToArrowBatches(batchSize).Prefetch(2))
            {
                using var downgradedBatch = DowngradeBatchForIpc(batch, ipcSchema);
                writer.WriteRecordBatch(downgradedBatch);
                
                rowsWritten += downgradedBatch.Length;
                
                if (rowsWritten % 1_000_000 == 0)
                {
                    GC.Collect(0, GCCollectionMode.Optimized); 
                }
            }
            writer.WriteEnd();
        }
        catch
        {
            if (File.Exists(filePath)) try { File.Delete(filePath); } catch { }
            throw;
        }
        return filePath;
    }

    private static void WriteDataToFile<T>(string filePath, IEnumerable<T> data, int batchSize)
    {
        var originalSchema = Arrow.ArrowConverter.GetSchemaFromType<T>();
        var ipcSchema = DowngradeSchemaForIpc(originalSchema);

        using var stream = File.OpenWrite(filePath);
        using var writer = new ArrowFileWriter(stream, ipcSchema);

        long rowsWritten = 0;
        foreach (var batch in Arrow.ArrowConverter.ToArrowBatches(data, batchSize).Prefetch(2))
        {
            using var downgradedBatch = DowngradeBatchForIpc(batch, ipcSchema);
            writer.WriteRecordBatch(downgradedBatch);
            
            rowsWritten += downgradedBatch.Length;
            if (rowsWritten % 1_000_000 == 0)
            {
                GC.Collect(0, GCCollectionMode.Optimized);
            }
        }
        writer.WriteEnd(); 
    }

    private static Schema DowngradeSchemaForIpc(Schema schema)
    {
        var newFields = new Field[schema.FieldsList.Count];
        bool changed = false;

        for (int i = 0; i < schema.FieldsList.Count; i++)
        {
            var field = schema.FieldsList[i];
            if (field.DataType.TypeId == ArrowTypeId.StringView)
            {
                newFields[i] = new Field(field.Name, StringType.Default, field.IsNullable);
                changed = true;
            }
            else if (field.DataType.TypeId == ArrowTypeId.BinaryView)
            {
                newFields[i] = new Field(field.Name, BinaryType.Default, field.IsNullable);
                changed = true;
            }
            else
            {
                newFields[i] = field;
            }
        }
        return changed ? new Schema(newFields, schema.Metadata) : schema;
    }

    private static RecordBatch DowngradeBatchForIpc(RecordBatch batch, Schema downgradedSchema)
    {
        var newArrays = new IArrowArray[batch.ColumnCount];
        bool changed = false;

        for (int i = 0; i < batch.ColumnCount; i++)
        {
            var array = batch.Column(i);
            
            if (array is StringViewArray strView)
            {
                var builder = new StringArray.Builder();
                for (int j = 0; j < strView.Length; j++)
                {
                    if (strView.IsNull(j)) builder.AppendNull();
                    else builder.Append(strView.GetString(j));
                }
                newArrays[i] = builder.Build();
                changed = true;
            }
            else if (array is BinaryViewArray binView)
            {
                var builder = new BinaryArray.Builder();
                for (int j = 0; j < binView.Length; j++)
                {
                    if (binView.IsNull(j)) builder.AppendNull();
                    else builder.Append(binView.GetBytes(j));
                }
                newArrays[i] = builder.Build();
                changed = true;
            }
            else
            {
                newArrays[i] = array;
            }
        }

        if (!changed) return batch;

        return new RecordBatch(downgradedSchema, newArrays, batch.Length);
    }
}