using Polars.NET.Core;
using Polars.NET.Core.Csv;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Wrappers;

namespace Polars.CSharp.Tests;

/// <summary>
/// The engine's own CSV IO (native Polars), reached below the public API now that the public API reads and writes CSV
/// with the managed reader and writer: the reference of the C5 differential, until native CSV IO is deleted.
/// </summary>
internal static class NativeCsv
{
    private static PlCsvEncoding Encoding(CsvScanArgs a) => a.LossyEncoding ? PlCsvEncoding.LossyUTF8 : PlCsvEncoding.UTF8;

    /// <summary>A native scan of a path with the options given (released defaults: every file infers when none is said).</summary>
    public static LazyFrame Scan(string path, CsvScanArgs a) => new(PolarsWrapper.ScanCsv(path, a.Schema, a.DtypeOverride, a.HasHeader, a.Separator, a.QuoteChar,
        a.EolChar, a.IgnoreErrors, a.TryParseDates, lowMemory: false, cache: true, a.Glob, a.Rechunk, a.RaiseIfEmpty, a.SkipRows, a.SkipRowsAfterHeader, a.SkipLines,
        a.NRows, a.InferSchemaLength, a.InferSchemaFiles ?? ulong.MaxValue, a.NThreads, chunkSize: null, a.RowIndexName, a.RowIndexOffset, a.IncludeFilePaths,
        Encoding(a), a.NullValues?.ToArray(), a.MissingIsNull, a.CommentPrefix, a.DecimalComma, a.TruncateRaggedLines, PlCloudProvider.None, 0, 0, 0, 0, 0, null,
        null));

    /// <summary>A native scan of bytes (copied) with the options given.</summary>
    public static LazyFrame Scan(ReadOnlySpan<byte> bytes, CsvScanArgs a) => new(PolarsWrapper.ScanCsv(NativeBytes.Copy(bytes), a.Schema, a.DtypeOverride,
        a.HasHeader, a.Separator, a.QuoteChar, a.EolChar, a.IgnoreErrors, a.TryParseDates, lowMemory: false, cache: true, a.Glob, a.Rechunk, a.RaiseIfEmpty, a.SkipRows,
        a.SkipRowsAfterHeader, a.SkipLines, a.NRows, a.InferSchemaLength, a.InferSchemaFiles ?? ulong.MaxValue, a.NThreads, chunkSize: null, a.RowIndexName,
        a.RowIndexOffset, a.IncludeFilePaths, Encoding(a), a.NullValues?.ToArray(), a.MissingIsNull, a.CommentPrefix, a.DecimalComma, a.TruncateRaggedLines));

    /// <summary>A native read of a path: the scan collected, the columns asked selected (as the released ReadCsv did).</summary>
    public static DataFrame Read(string path, CsvScanArgs a, string[] columns = null) => Collect(Scan(path, a), columns);

    /// <summary>A native read of bytes.</summary>
    public static DataFrame Read(ReadOnlySpan<byte> bytes, CsvScanArgs a, string[] columns = null) => Collect(Scan(bytes, a), columns);

    private static DataFrame Collect(LazyFrame lf, string[] columns)
    {
        using (lf)
        {
            if (columns is not { Length: > 0 }) return lf.Collect();
            using LazyFrame selected = lf.Select(global::Polars.CSharp.Polars.Selectors.ByName(columns));
            return selected.Collect();
        }
    }

    /// <summary>A frame written by the native writer, as bytes.</summary>
    public static byte[] Write(DataFrame df, CsvWriteArgs w)
    {
        using LazyFrame lf = df.Lazy();
        using NativeBuffer buffer = PolarsWrapper.SinkCsvNativeMemory(lf.CloneHandle(), w.IncludeBom, w.IncludeHeader, batchSize: 1024, checkExtension: false,
            w.Compression, w.CompressionLevel, w.DateFormat, w.TimeFormat, w.DatetimeFormat, w.FloatScientific switch { true => 1, false => 0, null => -1 },
            w.FloatPrecision ?? -1, w.DecimalComma, (byte)w.Separator, (byte)w.QuoteChar, w.NullValue, w.LineTerminator, w.QuoteStyle, maintainOrder: true);
        return buffer.ToArray();
    }
}
