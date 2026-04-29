using Polars.NET.Core;

namespace Polars.CSharp;
public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Read a JSON file into a DataFrame.
    /// </summary>
    /// <param name="path">Path to the JSON file.</param>
    /// <param name="columns">Select specific columns to read.</param>
    /// <param name="schema">Manually specify the schema (recommended for stability).</param>
    /// <param name="inferSchemaLen">Number of rows to scan for schema inference (default: scan all).</param>
    /// <param name="batchSize">Batch size for reading (optimization).</param>
    /// <param name="ignoreErrors">Ignore parsing errors (skip malformed lines).</param>
    /// <param name="jsonFormat">Format: Json Array or Json Lines (NDJSON).</param>
    public static DataFrame ReadJson(
        string path,
        string[]? columns = null,
        IntoSchema? schema = null,
        ulong? inferSchemaLen = null,
        ulong? batchSize = null,
        bool ignoreErrors = false,
        JsonFormat jsonFormat = JsonFormat.Json)
    {
        if (!File.Exists(path)) 
            throw new FileNotFoundException($"JSON file not found: {path}");

        var schemaHandle = schema?.Consume().Handle;

        var h = PolarsWrapper.ReadJson(
            path,
            columns,
            schemaHandle,
            inferSchemaLen,
            batchSize,
            ignoreErrors,
            jsonFormat.ToNative()
        );

        return new DataFrame(h);
    }

    // ---------------------------------------------------------
    // Read JSON (Memory / Bytes)
    // ---------------------------------------------------------

    /// <summary>
    /// Read JSON from an in-memory byte array.
    /// </summary>
    public static DataFrame ReadJson(
        byte[] buffer,
        string[]? columns = null,
        IntoSchema? schema = null,
        ulong? inferSchemaLen = null,
        ulong? batchSize = null,
        bool ignoreErrors = false,
        JsonFormat jsonFormat = JsonFormat.Json)
    {
        var schemaHandle = schema?.Consume().Handle;

        var h = PolarsWrapper.ReadJson(
            buffer,
            columns,
            schemaHandle,
            inferSchemaLen,
            batchSize,
            ignoreErrors,
            jsonFormat.ToNative()
        );

        return new DataFrame(h);
    }

    // ---------------------------------------------------------
    // Read JSON (Stream)
    // ---------------------------------------------------------

    /// <summary>
    /// Read JSON from a Stream.
    /// </summary>
    public static DataFrame ReadJson(
        Stream stream,
        string[]? columns = null,
        IntoSchema? schema = null,
        ulong? inferSchemaLen = null,
        ulong? batchSize = null,
        bool ignoreErrors = false,
        JsonFormat jsonFormat = JsonFormat.Json)
    {
        using var ms = new MemoryStream();
        stream.CopyTo(ms);
        
        return ReadJson(
            ms.ToArray(), 
            columns, 
            schema, 
            inferSchemaLen, 
            batchSize, 
            ignoreErrors, 
            jsonFormat
        );
    }
}