namespace Kaminari.Seseragi.Lakehouse.Engine;

/// <summary>
/// Reads Parquet files for the kernel. The kernel itself does not decode Parquet: the engine that runs the queries
/// already can, so it implements this.
/// </summary>
public interface IParquetReader
{
    /// <summary>
    /// Reads every row of a Parquet file and returns them as JSON lines in UTF-8: one JSON object per row, with the
    /// column names as keys. A struct is an object, a list an array, and a map either an object or an array of
    /// <c>{"key": ..., "value": ...}</c> objects. A null struct may be written as null or as an object of nulls.
    /// </summary>
    /// <param name="path">The file path relative to the table root, with <c>/</c> separators.</param>
    byte[] ReadAsJsonLines(string path);
}
