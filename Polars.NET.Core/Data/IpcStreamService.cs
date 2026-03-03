using System.Data;
using Apache.Arrow.Ipc;

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
    /// <summary>
    /// Start a background task, write data stream to IPC temp file
    /// </summary>
    public static string StartBufferedFileWriter<T>(IEnumerable<T> data, int batchSize = 100_000)
    {
        // Generate Temp File Path
        string filePath = Path.GetTempFileName(); 
        
        WriteDataToFile(filePath, data, batchSize);
        
        return filePath;
    }
    public static string StartBufferedFileWriter(IDataReader reader, int batchSize)
    {
        // Generate Temp File Path
        var filePath = Path.GetTempFileName();

        try
        {
            var schema = reader.GetArrowSchema();

            using var stream = File.OpenWrite(filePath);
            using var writer = new ArrowFileWriter(stream, schema);

            long rowsWritten = 0;

            foreach (var batch in reader.ToArrowBatches(batchSize))
            {
                writer.WriteRecordBatch(batch);
                
                rowsWritten += batch.Length;
                
                batch.Dispose();

                if (rowsWritten % 5_000_000 == 0)
                {
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
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
        // Get Schema
        var schema = Arrow.ArrowConverter.GetSchemaFromType<T>();

        using var stream = File.OpenWrite(filePath);
        
        using var writer = new ArrowFileWriter(stream, schema);

        using var enumerator = Arrow.ArrowConverter.ToArrowBatches(data, batchSize).GetEnumerator();
        long rowsWritten = 0;
        while (enumerator.MoveNext())
        {
            var batch = enumerator.Current;
            writer.WriteRecordBatch(batch);
            rowsWritten += batch.Length;
            batch.Dispose(); 
            if (rowsWritten % 5_000_000 == 0)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }
        }
        
        writer.WriteEnd(); 
    }
}

