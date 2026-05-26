using Polars.NET.Core;

namespace Polars.CSharp;
public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Read an Avro file into a DataFrame.
    /// </summary>
    /// <param name="path">The path to the Avro file.</param>
    /// <param name="nRows">Stop reading when `nRows` are read.</param>
    /// <param name="columns">Columns to select/project by name.</param>
    /// <param name="projection">Columns to select/project by index.</param>
    /// <returns>A new DataFrame.</returns>
    public static DataFrame ReadAvro(
        string path, 
        ulong? nRows = null, 
        string[]? columns = null, 
        int[]? projection = null)
    {
        var handle = PolarsWrapper.ReadAvro(path, nRows, columns, projection);
        return new DataFrame(handle);
    }
    /// <summary>
    /// Read an Avro memory buffer into a DataFrame.
    /// </summary>
    /// <param name="buffer">The byte array containing Avro data.</param>
    /// <param name="nRows">Stop reading when `nRows` are read.</param>
    /// <param name="columns">Columns to select/project by name.</param>
    /// <param name="projection">Columns to select/project by index.</param>
    /// <returns>A new DataFrame.</returns>
    public static DataFrame ReadAvro(
        byte[] buffer, 
        ulong? nRows = null, 
        string[]? columns = null, 
        int[]? projection = null)
    {
        var handle = PolarsWrapper.ReadAvro(buffer, nRows, columns, projection);
        return new DataFrame(handle);
    }
    /// <summary>
    /// Read an Avro memory Stream into a DataFrame.
    /// </summary>
    /// <param name="stream">The stream containing Avro data.</param>
    /// <param name="nRows">Stop reading when `nRows` are read.</param>
    /// <param name="columns">Columns to select/project by name.</param>
    /// <param name="projection">Columns to select/project by index.</param>
    /// <returns>A new DataFrame.</returns>
    public static DataFrame ReadAvro(
        Stream stream, 
        ulong? nRows = null, 
        string[]? columns = null, 
        int[]? projection = null)
    {
        using var ms = new MemoryStream();
        stream.CopyTo(ms);
        var handle = PolarsWrapper.ReadAvro(ms.ToArray(), nRows, columns, projection);
        return new DataFrame(handle);
    }
}