using Polars.NET.Core;
using Polars.NET.Core.Json;

namespace Polars.CSharp;

public partial class DataFrame : IDisposable, IEnumerable<Series>
{
    /// <summary>
    /// Read a JSON file into a DataFrame: one document (an array of objects, or one object) or newline-delimited JSON.
    /// The input is decoded on every core.
    /// </summary>
    /// <param name="path">Path to the JSON file.</param>
    /// <param name="columns">Select specific columns to read.</param>
    /// <param name="schema">Every column and its type: nothing is inferred, fields it does not name are passed over, fields a record lacks are nulls.</param>
    /// <param name="inferSchemaLen">Rows to infer the schema from (100 by default). Null or 0 infers it from the whole input; values above 16,777,216 do the same.</param>
    /// <param name="batchSize">Rows per batch read.</param>
    /// <param name="ignoreErrors">A value that does not fit its column's type is a null instead of an error (syntax errors still are errors).</param>
    /// <param name="jsonFormat">Format: Json (a document) or JsonLines (NDJSON).</param>
    /// <param name="schemaOverrides">The types of some inferred columns (with <paramref name="schema"/>: of some of its columns).</param>
    /// <remarks>
    /// String columns take other JSON values as their JSON text; binary columns take base64 strings. Errors are
    /// <see cref="Kaminari.Seseragi.Files.Json.JsonFormatException"/> (not JSON of the layout read) and
    /// <see cref="Kaminari.Seseragi.Files.Json.JsonValueException"/> (a value its column cannot hold), naming the line.
    /// </remarks>
    public static DataFrame ReadJson(
        string path,
        string[]? columns = null,
        IntoSchema? schema = null,
        ulong? inferSchemaLen = 100,
        ulong? batchSize = null,
        bool ignoreErrors = false,
        JsonFormat jsonFormat = JsonFormat.Json,
        IntoSchema? schemaOverrides = null)
        => ReadJsonWith(schema, schemaOverrides, (s, o) => ManagedJson.ReadJson(path, columns, s, o, inferSchemaLen, batchSize, ignoreErrors, jsonFormat.ToNative()));

    // ---------------------------------------------------------
    // Read JSON (Memory / Bytes)
    // ---------------------------------------------------------

    /// <summary>
    /// Read JSON from an in-memory byte array.
    /// </summary>
    /// <remarks>The bytes are read in place during the call.</remarks>
    /// <inheritdoc cref="ReadJson(string, string[], IntoSchema?, ulong?, ulong?, bool, JsonFormat, IntoSchema?)"/>
    public static DataFrame ReadJson(
        byte[] buffer,
        string[]? columns = null,
        IntoSchema? schema = null,
        ulong? inferSchemaLen = 100,
        ulong? batchSize = null,
        bool ignoreErrors = false,
        JsonFormat jsonFormat = JsonFormat.Json,
        IntoSchema? schemaOverrides = null)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        return ReadJsonWith(schema, schemaOverrides, (s, o) => ManagedJson.ReadJson(buffer.AsMemory(), columns, s, o, inferSchemaLen, batchSize, ignoreErrors, jsonFormat.ToNative()));
    }

    /// <summary>
    /// Read JSON from an in-memory byte array.
    /// </summary>
    /// <remarks>The bytes are read in place during the call.</remarks>
    /// <inheritdoc cref="ReadJson(string, string[], IntoSchema?, ulong?, ulong?, bool, JsonFormat, IntoSchema?)"/>
    public static DataFrame ReadJson(
        ReadOnlySpan<byte> buffer,
        string[]? columns = null,
        IntoSchema? schema = null,
        ulong? inferSchemaLen = 100,
        ulong? batchSize = null,
        bool ignoreErrors = false,
        JsonFormat jsonFormat = JsonFormat.Json,
        IntoSchema? schemaOverrides = null)
    {
        try
        {
            return new DataFrame(ManagedJson.ReadJson(buffer, columns, schema?.Consume().Handle, schemaOverrides?.Consume().Handle, inferSchemaLen,
                batchSize, ignoreErrors, jsonFormat.ToNative()));
        }
        finally
        {
            schema?.DisposeTempSchema();
            schemaOverrides?.DisposeTempSchema();
        }
    }

    /// <summary>
    /// Read JSON from an in-memory byte array.
    /// </summary>
    /// <remarks>The memory is read in place during the call.</remarks>
    /// <inheritdoc cref="ReadJson(string, string[], IntoSchema?, ulong?, ulong?, bool, JsonFormat, IntoSchema?)"/>
    public static DataFrame ReadJson(
        ReadOnlyMemory<byte> buffer,
        string[]? columns = null,
        IntoSchema? schema = null,
        ulong? inferSchemaLen = 100,
        ulong? batchSize = null,
        bool ignoreErrors = false,
        JsonFormat jsonFormat = JsonFormat.Json,
        IntoSchema? schemaOverrides = null)
        => ReadJsonWith(schema, schemaOverrides, (s, o) => ManagedJson.ReadJson(buffer, columns, s, o, inferSchemaLen, batchSize, ignoreErrors, jsonFormat.ToNative()));

    // ---------------------------------------------------------
    // Read JSON (Stream)
    // ---------------------------------------------------------

    /// <summary>
    /// Read JSON from a Stream, decoded as it arrives (from its position; the stream is left open).
    /// </summary>
    /// <inheritdoc cref="ReadJson(string, string[], IntoSchema?, ulong?, ulong?, bool, JsonFormat, IntoSchema?)"/>
    public static DataFrame ReadJson(
        Stream stream,
        string[]? columns = null,
        IntoSchema? schema = null,
        ulong? inferSchemaLen = 100,
        ulong? batchSize = null,
        bool ignoreErrors = false,
        JsonFormat jsonFormat = JsonFormat.Json,
        IntoSchema? schemaOverrides = null)
    {
        ArgumentNullException.ThrowIfNull(stream);
        return ReadJsonWith(schema, schemaOverrides, (s, o) => ManagedJson.ReadJson(stream, columns, s, o, inferSchemaLen, batchSize, ignoreErrors, jsonFormat.ToNative()));
    }

    // The schemas given (borrowed by the read; temporary ones disposed after it).
    private static DataFrame ReadJsonWith(IntoSchema? schema, IntoSchema? overrides, Func<SchemaHandle?, SchemaHandle?, DataFrameHandle> read)
    {
        try
        {
            return new DataFrame(read(schema?.Consume().Handle, overrides?.Consume().Handle));
        }
        finally
        {
            schema?.DisposeTempSchema();
            overrides?.DisposeTempSchema();
        }
    }
}
