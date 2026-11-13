using System.IO.Compression;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Excel;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Exceptions;
using Polars.NET.Core.Native;

namespace Polars.NET.Core.Wrappers;

/// <summary>
/// Excel runs on the managed Seseragi.Files reader and writer: sheets stream through Arrow record batches into and
/// out of the engine. Workbooks the managed reader does not read yet (.xls, .ods) still go through the native reader,
/// from a path only.
/// </summary>
public readonly partial struct PolarsWrapper
{
    private static ExcelReadOptions ExcelOptions(string? sheetName, ulong sheetIndex, SchemaHandle? schema, bool hasHeader,
        ulong inferSchemaLen, bool dropEmptyRows, bool raiseIfEmpty, string[]? columns, bool dropEmptyColumns, string? tableName) => new()
    {
        SheetName = sheetName,
        SheetIndex = sheetIndex <= int.MaxValue ? (int)sheetIndex : throw new ArgumentOutOfRangeException(nameof(sheetIndex)),
        TableName = tableName,
        HasHeader = hasHeader,
        Columns = columns,
        SchemaOverrides = schema is null ? null : ArrowTypesOf(schema),
        InferSchemaLength = inferSchemaLen <= int.MaxValue ? (int)inferSchemaLen : null,
        DropEmptyRows = dropEmptyRows,
        DropEmptyColumns = dropEmptyColumns,
        RaiseIfEmpty = raiseIfEmpty,
        LeaveOpen = true,
    };

    private static Dictionary<string, IArrowType> ArrowTypesOf(SchemaHandle schema)
    {
        Dictionary<string, IArrowType> types = [];
        ulong count = GetSchemaLen(schema);
        for (ulong i = 0; i < count; i++)
        {
            GetSchemaFieldAt(schema, i, out string name, out DataTypeHandle type);
            using (type) types[name] = ArrowFfiBridge.ImportDataType(type);
        }
        return types;
    }

    /// <summary>Whether the file is an xlsx or xlsb workbook (read by the managed reader); the others go to the native one.</summary>
    private static bool IsManagedExcel(string path)
    {
        using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.RandomAccess);
        Span<byte> magic = stackalloc byte[4];
        if (stream.ReadAtLeast(magic, 4, throwOnEndOfStream: false) < 4 || !magic.SequenceEqual("PK\u0003\u0004"u8)) return false;
        stream.Position = 0;
        try
        {
            using var zip = new ZipArchive(stream, ZipArchiveMode.Read, leaveOpen: true);
            return zip.Entries.Any(e => e.FullName.TrimStart('/') is var name
                && (name.Equals("xl/workbook.xml", StringComparison.OrdinalIgnoreCase) || name.Equals("xl/workbook.bin", StringComparison.OrdinalIgnoreCase)));
        }
        catch (InvalidDataException)
        {
            return false;
        }
    }

    // A sheet without columns (empty, read with raiseIfEmpty off) is an empty frame: an Arrow stream needs a field.
    private static DataFrameHandle ImportExcel(ExcelReader reader) => reader.Schema.FieldsList.Count == 0
        ? DataFrameNew([])
        : ArrowStreamInterop.ImportEagerStrict(reader.ReadBatches(), reader.Schema);

    // ---------------------------------------------------------------- read

    public static DataFrameHandle ReadExcel(
        string path,
        string? sheetName,
        ulong sheetIndex,
        SchemaHandle? schema,
        bool hasHeader,
        ulong inferSchemaLen,
        bool dropEmptyRows,
        bool raiseIfEmpty,
        string[]? columns = null,
        bool dropEmptyColumns = true,
        string? tableName = null)
    {
        if (!File.Exists(path)) throw new FileNotFoundException($"Excel file not found: {path}", path);
        if (!IsManagedExcel(path))
        {
            if (columns is not null || tableName is not null)
                throw new NotSupportedException("Column selection and tables are only supported for .xlsx and .xlsb workbooks so far.");
            return ReadExcelNative(path, sheetName, sheetIndex, schema, hasHeader, inferSchemaLen, dropEmptyRows, raiseIfEmpty);
        }
        var options = ExcelOptions(sheetName, sheetIndex, schema, hasHeader, inferSchemaLen, dropEmptyRows, raiseIfEmpty, columns, dropEmptyColumns, tableName);
        using var reader = ExcelReader.Open(path, options);
        return ImportExcel(reader);
    }

    /// <summary>Reads a sheet of an xlsx workbook in a stream (left open).</summary>
    public static DataFrameHandle ReadExcel(
        Stream stream,
        string? sheetName,
        ulong sheetIndex,
        SchemaHandle? schema,
        bool hasHeader,
        ulong inferSchemaLen,
        bool dropEmptyRows,
        bool raiseIfEmpty,
        string[]? columns = null,
        bool dropEmptyColumns = true,
        string? tableName = null)
    {
        ArgumentNullException.ThrowIfNull(stream);
        var options = ExcelOptions(sheetName, sheetIndex, schema, hasHeader, inferSchemaLen, dropEmptyRows, raiseIfEmpty, columns, dropEmptyColumns, tableName);
        using var reader = ExcelReader.Open(stream, options);
        return ImportExcel(reader);
    }

    /// <summary>
    /// A lazy scan of a sheet of an xlsx workbook: the header and the inference window are read now for the schema,
    /// the sheet each time the plan runs, streamed batch by batch.
    /// </summary>
    public static LazyFrameHandle ScanExcel(
        string path,
        string? sheetName,
        ulong sheetIndex,
        SchemaHandle? schema,
        bool hasHeader,
        ulong inferSchemaLen,
        bool dropEmptyRows,
        bool raiseIfEmpty,
        string[]? columns = null,
        bool dropEmptyColumns = true,
        string? tableName = null)
    {
        if (!File.Exists(path)) throw new FileNotFoundException($"Excel file not found: {path}", path);
        var options = ExcelOptions(sheetName, sheetIndex, schema, hasHeader, inferSchemaLen, dropEmptyRows, raiseIfEmpty, columns, dropEmptyColumns, tableName)
            with { LeaveOpen = false };
        Schema arrowSchema;
        using (var probe = ExcelReader.Open(path, options)) arrowSchema = probe.Schema;
        if (arrowSchema.FieldsList.Count == 0) { using var empty = DataFrameNew([]); return DataFrameToLazy(empty); }
        return ArrowStreamInterop.ScanStream(Batches, arrowSchema, strict: true);

        IEnumerable<RecordBatch> Batches()
        {
            using var reader = ExcelReader.Open(path, options);
            foreach (var b in reader.ReadBatches()) yield return b;
        }
    }

    private static DataFrameHandle ReadExcelNative(string path, string? sheetName, ulong sheetIndex, SchemaHandle? schema,
        bool hasHeader, ulong inferSchemaLen, bool dropEmptyRows, bool raiseIfEmpty)
    {
        using var schemaLock = new SafeHandleLock<SchemaHandle>(schema != null ? [schema] : null);
        IntPtr schemaPtr = schema != null ? schemaLock.Pointers[0] : IntPtr.Zero;
        var h = NativeBindings.pl_read_excel(path, sheetName, (UIntPtr)sheetIndex, schemaPtr, hasHeader, (UIntPtr)inferSchemaLen,
            dropEmptyRows, raiseIfEmpty);
        return ErrorHelper.Check(h);
    }

    // ---------------------------------------------------------------- write

    private static ExcelWriteOptions ExcelWriteOptionsFor(string? sheetName, string? dateFormat, string? datetimeFormat,
        string? timeFormat, string? durationFormat, bool includeHeader, bool autofit, bool freezeHeader, CompressionLevel compressionLevel)
    {
        var defaults = new ExcelWriteOptions();
        return defaults with
        {
            SheetName = string.IsNullOrEmpty(sheetName) ? defaults.SheetName : sheetName,
            DateFormat = string.IsNullOrEmpty(dateFormat) ? defaults.DateFormat : dateFormat,
            DatetimeFormat = string.IsNullOrEmpty(datetimeFormat) ? defaults.DatetimeFormat : datetimeFormat,
            TimeFormat = string.IsNullOrEmpty(timeFormat) ? defaults.TimeFormat : timeFormat,
            DurationFormat = string.IsNullOrEmpty(durationFormat) ? defaults.DurationFormat : durationFormat,
            IncludeHeader = includeHeader,
            Autofit = autofit,
            FreezeHeader = freezeHeader,
            CompressionLevel = compressionLevel,
            LeaveOpen = true,
        };
    }

    /// <summary>Writes a frame as one sheet of an xlsx workbook into a stream (flushed, left open).</summary>
    public static void WriteExcel(DataFrameHandle df, Stream stream, string? sheetName, string? dateFormat, string? datetimeFormat,
        string? timeFormat = null, string? durationFormat = null, bool includeHeader = true, bool autofit = false, bool freezeHeader = false,
        CompressionLevel compressionLevel = CompressionLevel.Optimal)
    {
        ArgumentNullException.ThrowIfNull(stream);
        var options = ExcelWriteOptionsFor(sheetName, dateFormat, datetimeFormat, timeFormat, durationFormat, includeHeader, autofit, freezeHeader, compressionLevel);
        using var exported = ArrowStreamInterop.ExportToStream(df);
        using var writer = ExcelWriter.Create(stream, exported.Schema, options);
        while (exported.ReadNextRecordBatchAsync().AsTask().GetAwaiter().GetResult() is { } batch)
        {
            using (batch) writer.Write(batch);
        }
        writer.Complete();
    }

    /// <summary>Writes a file; a failure deletes the partial file.</summary>
    public static void WriteExcel(DataFrameHandle df, string path, string? sheetName, string? dateFormat, string? datetimeFormat,
        string? timeFormat = null, string? durationFormat = null, bool includeHeader = true, bool autofit = false, bool freezeHeader = false,
        CompressionLevel compressionLevel = CompressionLevel.Optimal)
    {
        if (df.IsInvalid) throw new ObjectDisposedException(nameof(df), "DataFrame handle is invalid or closed.");
        try
        {
            using var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, 1 << 16);
            WriteExcel(df, stream, sheetName, dateFormat, datetimeFormat, timeFormat, durationFormat, includeHeader, autofit, freezeHeader, compressionLevel);
        }
        catch
        {
            File.Delete(path);
            throw;
        }
    }

    /// <summary>
    /// Receives the batches of a streaming sink and writes them as one sheet. The writer is created with the first
    /// batch's schema (exactly what the engine exports); an empty result writes a sheet with the planned columns.
    /// </summary>
    public sealed class ExcelBatchSink(Stream stream, string? sheetName, string? dateFormat, string? datetimeFormat,
        string? timeFormat, string? durationFormat, bool includeHeader, bool autofit, bool freezeHeader,
        CompressionLevel compressionLevel = CompressionLevel.Optimal) : IDisposable
    {
        private readonly ExcelWriteOptions _options =
            ExcelWriteOptionsFor(sheetName, dateFormat, datetimeFormat, timeFormat, durationFormat, includeHeader, autofit, freezeHeader, compressionLevel);
        private ExcelWriter? _writer;

        public void Write(RecordBatch batch)
        {
            _writer ??= ExcelWriter.Create(stream, batch.Schema, _options);
            _writer.Write(batch);
        }

        /// <summary>Finishes the workbook. When no batch arrived, <paramref name="query"/> is run with no rows so that
        /// the header gets the columns the engine exports.</summary>
        public void Complete(LazyFrameHandle query)
        {
            if (_writer is null)
            {
                using var empty = LazyCollect(LazySlice(LazyClone(query), 0, 0), PlEngine.Auto);
                _writer = ExcelWriter.Create(stream, ArrowSchemaOf(empty), _options);
            }
            _writer.Complete();
        }

        public void Dispose() => _writer?.Dispose();
    }
}
