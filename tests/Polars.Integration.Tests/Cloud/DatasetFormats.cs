using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Ipc;
using Kaminari.Seseragi.Files.Json;
using Kaminari.Seseragi.Files.Parquet;

namespace Polars.Integration.Tests.Cloud;

/// <summary>
/// The dataset formats the store tests run over (Parquet, Arrow IPC files, Arrow IPC streams, NDJSON): files of
/// <c>id</c> and <c>s</c> written in each, and datasets of them opened and read alike.
/// </summary>
internal static class DatasetFormats
{
    public static TheoryData<string> All => ["parquet", "ipc", "ipcstream", "json"];

    /// <summary>The files' extension.</summary>
    public static string Extension(string format) => format switch { "ipc" => "arrow", "ipcstream" => "arrows", "json" => "jsonl", _ => "parquet" };

    /// <summary>
    /// A file of ids from <paramref name="first"/> and strings: <paramref name="large"/> (uncompressed) beyond the
    /// first read of a file's tail, else (compressed) within it, so that its footer is kept or not.
    /// </summary>
    public static byte[] File(string format, long first, int rows, bool large)
    {
        if (format == "json")
        {
            // NDJSON lines; the small files gzip-compressed (read by their header, whatever their name).
            byte[] text = System.Text.Encoding.UTF8.GetBytes(string.Concat(Enumerable.Range(0, rows).Select(i => $$"""{"id":{{first + i}},"s":"value {{first + i}}"}""" + "\n")));
            if (large) return text;
            using var compressed = new MemoryStream();
            using (var gzip = new System.IO.Compression.GZipStream(compressed, System.IO.Compression.CompressionLevel.Fastest)) gzip.Write(text);
            return compressed.ToArray();
        }
        var schema = new Apache.Arrow.Schema([new Field("id", Int64Type.Default, true), new Field("s", StringType.Default, true)], null);
        var ids = new Int64Array.Builder().AppendRange(Enumerable.Range(0, rows).Select(i => first + i)).Build();
        var s = new StringArray.Builder().AppendRange(Enumerable.Range(0, rows).Select(i => $"value {first + i}")).Build();
        using var stream = new MemoryStream();
        long tail;
        if (format is "ipc" or "ipcstream")
        {
            using IpcWriter writer = IpcWriter.Create(stream, schema, new IpcWriteOptions
            {
                Compression = large ? NoCompression.Instance : new ZstandardCompression(),
                Format = format == "ipcstream" ? IpcFormat.Stream : IpcFormat.File,
            });
            int batchRows = Math.Max(1, rows / 4);
            for (int at = 0; at < rows; at += batchRows)
            {
                int n = Math.Min(batchRows, rows - at);
                // Slices share the arrays' managed buffers: not disposed (as the Parquet batch below).
                writer.Write(new RecordBatch(schema, [ArrowArrayFactory.Slice(ids, at, n), ArrowArrayFactory.Slice(s, at, n)], n));
            }
            writer.Complete();
            tail = 64 * 1024; // IpcFile.TailGuess (internal)
        }
        else
        {
            using var writer = ParquetWriter.Create(stream, schema, new ParquetWriteOptions
            {
                RowGroupRows = Math.Max(1, rows / 4),
                Compression = large ? NoCompression.Instance : new ZstandardCompression(),
            });
            writer.Write(new RecordBatch(schema, [ids, s], rows));
            writer.Complete();
            tail = ParquetMetadata.TailGuess;
        }
        byte[] bytes = stream.ToArray();
        if (large) Assert.True(bytes.Length > tail, $"a large {format} file of {bytes.Length} bytes");
        return bytes;
    }

    /// <summary>A dataset of <paramref name="paths"/> (a glob, a directory or URLs) in <paramref name="format"/>, on <paramref name="store"/> (null: by the paths' schemes).</summary>
    public static Opened Open(string format, IReadOnlyList<string> paths, IFileStore? store = null) => format switch
    {
        "ipc" or "ipcstream" => new Opened(IpcDataset.Open(paths, new IpcDatasetOptions { Store = store })),
        "json" => new Opened(JsonDataset.Open(paths, new JsonDatasetOptions { Store = store })),
        _ => new Opened(ParquetDataset.Open(paths, new ParquetDatasetOptions { Store = store })),
    };

    /// <inheritdoc cref="Open(string, IReadOnlyList{string}, IFileStore?)"/>
    public static Opened Open(string format, string path, IFileStore? store = null) => Open(format, [path], store);

    /// <summary>A dataset of either format.</summary>
    public sealed class Opened(IDisposable dataset) : IDisposable
    {
        public int Files => dataset switch { IpcDataset d => d.Files.Count, ParquetDataset d => d.Files.Count, JsonDataset d => d.Files.Count, _ => 0 };

        /// <summary>Every batch, read at <paramref name="threads"/> threads (the caller disposes them).</summary>
        public IEnumerable<RecordBatch> Read(int threads)
        {
            if (dataset is JsonDataset json)
            {
                using JsonDatasetReader reader = json.OpenReader(new JsonReadOptions { MaxDegreeOfParallelism = threads });
                foreach (RecordBatch batch in reader.ReadBatches()) yield return batch;
            }
            else if (dataset is IpcDataset ipc)
            {
                using IpcDatasetReader reader = ipc.OpenReader(new IpcReadOptions { MaxDegreeOfParallelism = threads });
                foreach (RecordBatch batch in reader.ReadBatches()) yield return batch;
            }
            else
            {
                using ParquetDatasetReader reader = ((ParquetDataset)dataset).OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = threads });
                foreach (RecordBatch batch in reader.ReadBatches()) yield return batch;
            }
        }

        /// <summary>The ids read at <paramref name="threads"/> threads.</summary>
        public List<long> Ids(int threads)
        {
            List<long> ids = [];
            foreach (RecordBatch batch in Read(threads))
            {
                using (batch) ids.AddRange(((Int64Array)batch.Column("id")).Values.ToArray());
            }
            return ids;
        }

        public void Dispose() => dataset.Dispose();
    }
}
