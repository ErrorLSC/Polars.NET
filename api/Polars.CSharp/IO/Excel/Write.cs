using System.IO.Compression;
using Polars.NET.Core.Wrappers;

namespace Polars.CSharp;

public partial class DataFrame : IDisposable, IEnumerable<Series>
{
    /// <summary>
    /// Writes the DataFrame as one sheet of an .xlsx workbook (the managed streaming writer of Kaminari.Seseragi.Files).
    /// <para>
    /// Integers and decimals are numbers when a double holds them exactly (Int64 up to 2^53), text otherwise;
    /// Float32 values keep their shortest single-precision form; NaN and infinities become #NUM! errors; nulls are
    /// empty cells. Dates, datetimes (any unit; a zoned value is written as the wall-clock time in its zone), times
    /// and durations are serial numbers with the given formats. Values Excel cannot hold are refused with
    /// <see cref="NotSupportedException"/>: binary and nested columns, sub-microsecond times, dates before 1900,
    /// strings over 32,767 characters, more than 1,048,576 rows.
    /// </para>
    /// </summary>
    /// <param name="path">The file to write (a failure deletes the partial file).</param>
    /// <param name="sheetName">Name of the worksheet. Defaults to "Sheet1".</param>
    /// <param name="dateFormat">Excel number format of Date columns. Defaults to "yyyy-mm-dd".</param>
    /// <param name="datetimeFormat">Excel number format of Datetime columns. Defaults to "yyyy-mm-dd hh:mm:ss".</param>
    /// <param name="timeFormat">Excel number format of Time columns. Defaults to "hh:mm:ss".</param>
    /// <param name="durationFormat">Excel number format of Duration columns. Defaults to "[h]:mm:ss".</param>
    /// <param name="includeHeader">Write the column names as the first row. Default is true.</param>
    /// <param name="autofit">Set column widths from the header and the first 100 rows.</param>
    /// <param name="freezeHeader">Keep the header row in view when scrolling.</param>
    /// <param name="compressionLevel">Zip compression: <see cref="CompressionLevel.Optimal"/> (default, the smallest
    /// files) or <see cref="CompressionLevel.Fastest"/> (about three times faster, about 20% larger files).</param>
    public void WriteExcel(
        string path,
        string? sheetName = null,
        string? dateFormat = null,
        string? datetimeFormat = null,
        string? timeFormat = null,
        string? durationFormat = null,
        bool includeHeader = true,
        bool autofit = false,
        bool freezeHeader = false,
        CompressionLevel compressionLevel = CompressionLevel.Optimal)
    {
        if (string.IsNullOrWhiteSpace(path))
            throw new ArgumentException("File path cannot be empty.", nameof(path));
        PolarsWrapper.WriteExcel(Handle, path, sheetName, dateFormat, datetimeFormat, timeFormat, durationFormat, includeHeader, autofit, freezeHeader, compressionLevel);
    }

    /// <summary>
    /// Writes the DataFrame as one sheet of an .xlsx workbook into a stream (flushed, left open). See
    /// <see cref="WriteExcel(string, string, string, string, string, string, bool, bool, bool, CompressionLevel)"/>.
    /// </summary>
    public void WriteExcel(
        Stream stream,
        string? sheetName = null,
        string? dateFormat = null,
        string? datetimeFormat = null,
        string? timeFormat = null,
        string? durationFormat = null,
        bool includeHeader = true,
        bool autofit = false,
        bool freezeHeader = false,
        CompressionLevel compressionLevel = CompressionLevel.Optimal) =>
        PolarsWrapper.WriteExcel(Handle, stream, sheetName, dateFormat, datetimeFormat, timeFormat, durationFormat, includeHeader, autofit, freezeHeader, compressionLevel);
}

public partial class LazyFrame
{
    /// <summary>
    /// Run the query on the streaming engine and write the result as one sheet of an .xlsx workbook as batches
    /// arrive. The parameters are those of
    /// <see cref="DataFrame.WriteExcel(string, string, string, string, string, string, bool, bool, bool, CompressionLevel)"/>;
    /// the file is deleted when writing fails.
    /// </summary>
    public void SinkExcel(
        string path,
        string? sheetName = null,
        string? dateFormat = null,
        string? datetimeFormat = null,
        string? timeFormat = null,
        string? durationFormat = null,
        bool includeHeader = true,
        bool autofit = false,
        bool freezeHeader = false,
        CompressionLevel compressionLevel = CompressionLevel.Optimal)
    {
        try
        {
            using var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, 1 << 16);
            SinkExcel(stream, sheetName, dateFormat, datetimeFormat, timeFormat, durationFormat, includeHeader, autofit, freezeHeader, compressionLevel);
        }
        catch
        {
            File.Delete(path);
            throw;
        }
    }

    /// <summary>
    /// Run the query on the streaming engine and write the result as one sheet of an .xlsx workbook into a stream as
    /// batches arrive. The stream is flushed but not disposed.
    /// </summary>
    public void SinkExcel(
        Stream stream,
        string? sheetName = null,
        string? dateFormat = null,
        string? datetimeFormat = null,
        string? timeFormat = null,
        string? durationFormat = null,
        bool includeHeader = true,
        bool autofit = false,
        bool freezeHeader = false,
        CompressionLevel compressionLevel = CompressionLevel.Optimal)
    {
        ArgumentNullException.ThrowIfNull(stream);
        using var sink = new PolarsWrapper.ExcelBatchSink(stream, sheetName, dateFormat, datetimeFormat, timeFormat, durationFormat,
            includeHeader, autofit, freezeHeader, compressionLevel);
        SinkBatches(sink.Write);
        sink.Complete(Handle);
    }
}
