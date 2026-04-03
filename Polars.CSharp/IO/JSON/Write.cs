#pragma warning disable CS1573
using Polars.NET.Core;

namespace Polars.CSharp;
public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Write DataFrame to a JSON file.
    /// </summary>
    /// <param name="path">Output file path.</param>
    /// <param name="format">JSON format (Json Array or JsonLines). Defaults to Json.</param>
    public void WriteJson(string path, JsonFormat format = JsonFormat.Json)
    {
        PolarsWrapper.WriteJson(Handle, path, format.ToNative());
    }
    /// <summary>
    /// Writes the DataFrame to a JSON format in memory.
    /// </summary>
    /// <param name="jsonFormat">The JSON format to use (Json or JsonLines).</param>
    /// <returns>A byte array containing the JSON data.</returns>
    public byte[] WriteJsonMemory(JsonFormat jsonFormat = JsonFormat.Json)
    {
        return PolarsWrapper.WriteJsonMemory(
            Handle,
            jsonFormat.ToNative()
        );
    }
    /// <inheritdoc cref="LazyFrame.SinkJsonPartitioned"/>
    public void WriteJsonPartitioned(
        string path,
        IntoSelector partitionBy,
        bool includeKeys = true,
        bool keysPreGrouped = false,
        int maxRowsPerFile = 0,
        long approxBytesPerFile = 0,
        ExternalCompression compression = ExternalCompression.Uncompressed,
        int compressionLevel = -1,
        bool checkExtension = true,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        CloudOptions? cloudOptions = null)
    {
        var lf = Lazy();
        lf.SinkJsonPartitioned(
            path,partitionBy,includeKeys,keysPreGrouped,maxRowsPerFile,approxBytesPerFile,
            compression,compressionLevel,checkExtension,maintainOrder,syncOnClose,mkdir,cloudOptions
        );
    }

    /// <summary>
    /// Write DataFrame to a Newline Delimited JSON (NDJSON) file.
    /// <para>
    /// This uses the Lazy execution engine internally to support streaming, compression, and cloud storage.
    /// </para>
    /// </summary>
    /// <param name="path">Output file path.</param>
    /// <param name="compression">Compression method (Gzip/Zstd). Defaults to None.</param>
    /// <param name="compressionLevel">Compression level. -1 for default.</param>
    /// <param name="checkExtension">Whether to check if the file extension matches '.json' or '.ndjson'.</param>
    /// <param name="maintainOrder">Maintain the order of data.</param>
    /// <param name="syncOnClose">Sync to disk on close.</param>
    /// <param name="mkdir">Create parent directories.</param>
    /// <param name="cloudOptions">Cloud storage options.</param>
    public void WriteNdJson(
        string path,
        ExternalCompression compression = ExternalCompression.Uncompressed,
        int compressionLevel = -1,
        bool checkExtension = true,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        CloudOptions? cloudOptions = null)
    {
        var lf = Lazy();

        lf.SinkJson(
            path,
            compression,
            compressionLevel,
            checkExtension,
            maintainOrder,
            syncOnClose,
            mkdir,
            cloudOptions
        );
    }
}