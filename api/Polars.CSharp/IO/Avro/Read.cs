using Polars.NET.Core;
using Polars.NET.Core.Wrappers;

namespace Polars.CSharp;

public partial class DataFrame : IDisposable, IEnumerable<Series>, IPolarsDataFrame
{
    /// <summary>
    /// Read an Avro file into a DataFrame.
    /// </summary>
    /// <param name="path">The path to the Avro file.</param>
    /// <param name="nRows">Stop reading when `nRows` are read.</param>
    /// <param name="columns">Columns to select/project by name.</param>
    /// <param name="projection">Columns to select/project by index.</param>
    /// <param name="parallel">Decode the file's blocks on several threads (row order is kept).</param>
    /// <returns>A new DataFrame.</returns>
    public static DataFrame ReadAvro(
        string path,
        ulong? nRows = null,
        string[]? columns = null,
        int[]? projection = null,
        bool parallel = true)
        => new(PolarsWrapper.ReadAvro(path, nRows, columns, projection, parallel));

    /// <summary>
    /// Read an Avro memory buffer into a DataFrame.
    /// </summary>
    /// <param name="buffer">The bytes, read in place during the call.</param>
    /// <param name="nRows">Stop reading when `nRows` are read.</param>
    /// <param name="columns">Columns to select/project by name.</param>
    /// <param name="projection">Columns to select/project by index.</param>
    /// <param name="parallel">Decode the file's blocks on several threads (row order is kept).</param>
    /// <returns>A new DataFrame.</returns>
    public static DataFrame ReadAvro(
        byte[] buffer,
        ulong? nRows = null,
        string[]? columns = null,
        int[]? projection = null,
        bool parallel = true)
        => ReadAvro(buffer.AsSpan(), nRows, columns, projection, parallel);

    /// <summary>
    /// Read an Avro memory buffer into a DataFrame.
    /// </summary>
    /// <param name="buffer">The bytes, read in place during the call.</param>
    /// <param name="nRows">Stop reading when `nRows` are read.</param>
    /// <param name="columns">Columns to select/project by name.</param>
    /// <param name="projection">Columns to select/project by index.</param>
    /// <param name="parallel">Decode the file's blocks on several threads (row order is kept).</param>
    /// <returns>A new DataFrame.</returns>
    public static DataFrame ReadAvro(
        ReadOnlySpan<byte> buffer,
        ulong? nRows = null,
        string[]? columns = null,
        int[]? projection = null,
        bool parallel = true)
        => new(PolarsWrapper.ReadAvro(buffer, nRows, columns, projection, parallel));

    /// <summary>
    /// Read an Avro memory buffer into a DataFrame.
    /// </summary>
    /// <param name="buffer">The bytes, read in place during the call.</param>
    /// <param name="nRows">Stop reading when `nRows` are read.</param>
    /// <param name="columns">Columns to select/project by name.</param>
    /// <param name="projection">Columns to select/project by index.</param>
    /// <param name="parallel">Decode the file's blocks on several threads (row order is kept).</param>
    /// <returns>A new DataFrame.</returns>
    public static DataFrame ReadAvro(
        ReadOnlyMemory<byte> buffer,
        ulong? nRows = null,
        string[]? columns = null,
        int[]? projection = null,
        bool parallel = true)
        => ReadAvro(buffer.Span, nRows, columns, projection, parallel);

    /// <summary>
    /// Read Avro data from a stream into a DataFrame. The stream is read block by block (not buffered whole)
    /// and left open.
    /// </summary>
    /// <param name="stream">The stream containing Avro data.</param>
    /// <param name="nRows">Stop reading when `nRows` are read.</param>
    /// <param name="columns">Columns to select/project by name.</param>
    /// <param name="projection">Columns to select/project by index.</param>
    /// <param name="parallel">Decode the file's blocks on several threads (row order is kept).</param>
    /// <returns>A new DataFrame.</returns>
    public static DataFrame ReadAvro(
        Stream stream,
        ulong? nRows = null,
        string[]? columns = null,
        int[]? projection = null,
        bool parallel = true)
        => new(PolarsWrapper.ReadAvro(stream, nRows, columns, projection, parallel));
}

public partial class LazyFrame
{
    /// <summary>
    /// Lazily scan an Avro file. The header is read now for the schema; the data is streamed block by block each
    /// time the query runs.
    /// </summary>
    /// <param name="path">The path to the Avro file.</param>
    /// <param name="nRows">Stop reading when `nRows` are read.</param>
    /// <param name="columns">Columns to select/project by name.</param>
    /// <param name="projection">Columns to select/project by index.</param>
    /// <param name="parallel">Decode the file's blocks on several threads (row order is kept).</param>
    /// <returns>A new LazyFrame.</returns>
    public static LazyFrame ScanAvro(
        string path,
        ulong? nRows = null,
        string[]? columns = null,
        int[]? projection = null,
        bool parallel = true)
        => new(PolarsWrapper.ScanAvro(path, nRows, columns, projection, parallel));
}
