using Polars.NET.Core;

namespace Polars.CSharp;
public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Write the DataFrame to an Apache Avro file.
    /// </summary>
    /// <param name="path">The file path to write to.</param>
    /// <param name="compression">The compression algorithm to use.</param>
    /// <param name="name">The name of the Avro record.</param>
    public void WriteAvro(
        string path, 
        AvroCompression compression = AvroCompression.Uncompressed, 
        string name = "")
    {
        PolarsWrapper.WriteAvro(this.Handle, path, compression.ToNative(), name);
    }
    /// <summary>
    /// Write the DataFrame to an Apache Avro memory buffer.
    /// </summary>
    /// <param name="compression">The compression algorithm to use.</param>
    /// <param name="name">The name of the Avro record.</param>
    /// <returns>A byte array containing the Avro data.</returns>
    public byte[] WriteAvroMemory(
        AvroCompression compression = AvroCompression.Uncompressed, 
        string name = "")
    {
        return PolarsWrapper.WriteAvroToMemory(Handle, compression.ToNative(), name);
    }
}