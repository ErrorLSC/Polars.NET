using Apache.Arrow;
using Apache.Arrow.C;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Native;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Exceptions;

namespace Polars.NET.Core.Wrappers;

public readonly partial struct PolarsWrapper
{
    /// <summary>
    /// Scans an explicit list of Parquet files as one source, in the given order, without globbing.
    /// </summary>
    /// <param name="ignoreExtraColumns">Columns of a file that <paramref name="schema"/> does not name are left out instead of raising.</param>
    /// <param name="lenientCasts">
    /// A file may hold a narrower type than <paramref name="schema"/>: smaller integers and floats are widened,
    /// integers become floats, nanosecond timestamps are cut to the schema's unit, time zones are converted, and
    /// structs may lack fields (filled with nulls) or carry extra ones (dropped).
    /// </param>
    public static LazyFrameHandle ScanParquetFiles(
        string[] paths,
        ulong? nRows = null,
        PlParallelStrategy parallel = PlParallelStrategy.Auto,
        bool lowMemory = false,
        bool useStatistics = true,
        bool allowMissingColumns = true,
        bool ignoreExtraColumns = false,
        bool lenientCasts = false,
        bool rechunk = false,
        bool cache = true,
        string? rowIndexName = null,
        uint rowIndexOffset = 0,
        string? includePathColumn = null,
        SchemaHandle? schema = null,
        PlCloudProvider cloudProvider = default,
        nuint cloudRetries = 2,
        ulong cloudRetryTimeoutMs = 10_000,
        ulong cloudRetryInitBackoffMs = 100,
        ulong cloudRetryMaxBackoffMs = 15_000,
        ulong cloudCacheTtl = 0,
        string[]? cloudKeys = null,
        string[]? cloudValues = null)
    {
        ArgumentNullException.ThrowIfNull(paths);
        if (paths.Length == 0) throw new ArgumentException("At least one path is required.", nameof(paths));

        unsafe
        {
            ulong nRowsVal = nRows.GetValueOrDefault();
            IntPtr nRowsPtr = nRows.HasValue ? (IntPtr)(&nRowsVal) : IntPtr.Zero;

            using var schemaLock = new SafeHandleLock<SchemaHandle>(schema != null ? [schema] : null);
            IntPtr schemaPtr = schema != null ? schemaLock.Pointers[0] : IntPtr.Zero;

            var h = NativeBindings.pl_scan_parquet_files(
                paths,
                (nuint)paths.Length,
                nRowsPtr,
                parallel,
                lowMemory,
                useStatistics,
                allowMissingColumns,
                ignoreExtraColumns,
                lenientCasts,
                rechunk,
                cache,
                rowIndexName,
                rowIndexOffset,
                includePathColumn,
                schemaPtr,
                cloudProvider,
                cloudRetries,
                cloudRetryTimeoutMs,
                cloudRetryInitBackoffMs,
                cloudRetryMaxBackoffMs,
                cloudCacheTtl,
                cloudKeys,
                cloudValues,
                (nuint)(cloudKeys?.Length ?? 0));

            return ErrorHelper.Check(h);
        }
    }

    public static LazyFrameHandle ScanParquet(
        string path,
        ulong? nRows,
        PlParallelStrategy parallel,
        bool lowMemory,
        bool useStatistics,
        bool glob,
        bool allowMissingColumns,
        bool rechunk,
        bool cache,
        // ----------------
        string? rowIndexName,
        uint rowIndexOffset,
        string? includePathColumn,
        SchemaHandle? schema,
        bool hivePartitioning,
        SchemaHandle? hivePartitionSchema,
        bool tryParseHiveDates,
        PlCloudProvider cloudProvider,
        nuint cloudRetries,
        ulong cloudRetryTimeoutMs,
        ulong cloudRetryInitBackoffMs,
        ulong cloudRetryMaxBackoffMs,
        ulong cloudCacheTtl,
        string[]? cloudKeys,
        string[]? cloudValues,
        ParquetScanExtrasHandle? extras = null
        )
    {
        unsafe
        {
            ulong nRowsVal = nRows.GetValueOrDefault();
            IntPtr nRowsPtr = nRows.HasValue ? (IntPtr)(&nRowsVal) : IntPtr.Zero;

            using var schemaLock = new SafeHandleLock<SchemaHandle>(
                schema != null ? [schema] : null
            );
            IntPtr schemaPtr = schema != null ? schemaLock.Pointers[0] : IntPtr.Zero;

            using var hiveLock = new SafeHandleLock<SchemaHandle>(
                hivePartitionSchema != null ? [hivePartitionSchema] : null
            );
            IntPtr hiveSchemaPtr = hivePartitionSchema != null ? hiveLock.Pointers[0] : IntPtr.Zero;
            nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);
            using var extrasRef = new BorrowedHandle(extras);
            var h = NativeBindings.pl_scan_parquet(
                path,
                nRowsPtr,
                parallel,
                lowMemory,
                useStatistics,
                glob,
                allowMissingColumns,
                rechunk,
                cache,
                rowIndexName,
                rowIndexOffset,
                includePathColumn,
                schemaPtr,
                hivePartitioning,
                hiveSchemaPtr,
                tryParseHiveDates,
                // Cloud Options
                cloudProvider,
                cloudRetries,
                cloudRetryTimeoutMs,
                cloudRetryInitBackoffMs,
                cloudRetryMaxBackoffMs,
                cloudCacheTtl,
                cloudKeys,
                cloudValues,
                cloudLen,
                extrasRef.Pointer
            );

            return ErrorHelper.Check(h);
        }
    }

    public static LazyFrameHandle ScanParquet(
        NativeBytes buffer,
        ulong? nRows,
        PlParallelStrategy parallel,
        bool lowMemory,
        bool useStatistics,
        bool glob,
        bool allowMissingColumns,
        bool rechunk,
        bool cache,
        string? rowIndexName,
        uint rowIndexOffset,
        string? includePathColumn,
        SchemaHandle? schema,
        bool hivePartitioning,
        SchemaHandle? hivePartitionSchema,
        bool tryParseHiveDates,
        ParquetScanExtrasHandle? extras = null)
    {
        if (buffer.Length == 0)
            throw new ArgumentException("Buffer cannot be empty", nameof(buffer));

        unsafe
        {
            {
                ulong nRowsVal = nRows.GetValueOrDefault();
                IntPtr nRowsPtr = nRows.HasValue ? (IntPtr)(&nRowsVal) : IntPtr.Zero;

                using var schemaLock = new SafeHandleLock<SchemaHandle>(
                    schema != null ? [schema] : null
                );
                IntPtr schemaPtr = schema != null ? schemaLock.Pointers[0] : IntPtr.Zero;

                using var hiveLock = new SafeHandleLock<SchemaHandle>(
                    hivePartitionSchema != null ? [hivePartitionSchema] : null
                );
                IntPtr hiveSchemaPtr = hivePartitionSchema != null ? hiveLock.Pointers[0] : IntPtr.Zero;

                fixed (byte* copyPointer = buffer.CopySource)
                {
                    var lease = buffer.Acquire(copyPointer);
                    LazyFrameHandle h;
                    try
                    {
                        using var extrasRef = new BorrowedHandle(extras);
                        h = NativeBindings.pl_scan_parquet_memory(
                            (IntPtr)lease.Pointer, (UIntPtr)lease.Length, lease.Release, lease.UserData,
                            nRowsPtr,
                            parallel,
                            lowMemory,
                            useStatistics,
                            glob,
                            allowMissingColumns,
                            rechunk,
                            cache,
                            rowIndexName,
                            rowIndexOffset,
                            includePathColumn,
                            schemaPtr,
                            hivePartitioning,
                            hiveSchemaPtr,
                            tryParseHiveDates,
                            extrasRef.Pointer
                        );
                    }
                    catch
                    {
                        // The native function was not reached: it did not take the pin.
                        NativeBytes.ReleaseUnused(lease);
                        throw;
                    }

                    return ErrorHelper.Check(h);
                }
            }
        }
    }
    // Sink Parquet
    public static void SinkParquet(
        LazyFrameHandle lf,
        string path,
        PlParquetCompression compression,
        int compressionLevel,
        bool statistics,
        int rowGroupSize,
        int dataPageSize,
        int compatLevel,
        bool maintainOrder,
        PlSyncOnClose syncOnClose,
        bool mkdir,
        PlCloudProvider cloudProvider,
        nuint cloudRetries,
        ulong cloudRetryTimeoutMs,
        ulong cloudRetryInitBackoffMs,
        ulong cloudRetryMaxBackoffMs,
        ulong cloudCacheTtl,
        string[]? cloudKeys,
        string[]? cloudValues,
        ParquetWriteExtrasHandle? extras = null
        )
    {
        nuint rgs = rowGroupSize > 0 ? (nuint)rowGroupSize : 0;
        nuint dps = dataPageSize > 0 ? (nuint)dataPageSize : 0;
        int safeCompatLevel = compatLevel;
        if (safeCompatLevel < -1) safeCompatLevel = -1;
        else if (safeCompatLevel > 1) safeCompatLevel = 1;

        nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);

        using var extrasRef = new BorrowedHandle(extras);
        NativeBindings.pl_lazyframe_sink_parquet(
            lf,
            path,
            compression,
            compressionLevel,
            statistics,
            rgs,
            dps,
            safeCompatLevel,
            maintainOrder,
            syncOnClose,
            mkdir,
            // Cloud Args
            cloudProvider,
            cloudRetries,
            cloudRetryTimeoutMs,
            cloudRetryInitBackoffMs,
            cloudRetryMaxBackoffMs,
            cloudCacheTtl,
            cloudKeys,
            cloudValues,
            cloudLen,
            extrasRef.Pointer
        );

        lf.TransferOwnership();
        ErrorHelper.CheckVoid();
    }
    public static void SinkParquetPartitioned(
        LazyFrameHandle lf,
        string path,

        // --- Partition Params ---
        SelectorHandle partitionBy,
        bool includeKeys,
        bool keysPreGrouped,
        nuint maxRowsPerFile,
        ulong approxBytesPerFile,

        // --- Parquet Options ---
        PlParquetCompression compression,
        int compressionLevel,
        bool statistics,
        int rowGroupSize,
        int dataPageSize,
        int compatLevel,

        // --- Unified Options ---
        bool maintainOrder,
        PlSyncOnClose syncOnClose,
        bool mkdir,

        // --- Cloud Params ---
        PlCloudProvider cloudProvider,
        nuint cloudRetries,
        ulong cloudRetryTimeoutMs,
        ulong cloudRetryInitBackoffMs,
        ulong cloudRetryMaxBackoffMs,
        ulong cloudCacheTtl,
        string[]? cloudKeys,
        string[]? cloudValues,
        ParquetWriteExtrasHandle? extras = null
    )
    {
        nuint rgs = rowGroupSize > 0 ? (nuint)rowGroupSize : 0;
        nuint dps = dataPageSize > 0 ? (nuint)dataPageSize : 0;

        int safeCompatLevel = compatLevel;
        if (safeCompatLevel < -1) safeCompatLevel = -1;
        else if (safeCompatLevel > 1) safeCompatLevel = 1;

        nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);

        // Call Native Binding
        using var extrasRef = new BorrowedHandle(extras);
        NativeBindings.pl_lazyframe_sink_parquet_partitioned(
            lf,
            path,

            // Partition Params
            partitionBy,
            includeKeys,
            keysPreGrouped,
            maxRowsPerFile,
            approxBytesPerFile,

            // Parquet Options
            compression,
            compressionLevel,
            statistics,
            rgs,
            dps,
            safeCompatLevel,

            // Unified Options
            maintainOrder,
            syncOnClose,
            mkdir,

            // Cloud Params
            cloudProvider,
            cloudRetries,
            cloudRetryTimeoutMs,
            cloudRetryInitBackoffMs,
            cloudRetryMaxBackoffMs,
            cloudCacheTtl,
            cloudKeys,
            cloudValues,
            cloudLen,
            extrasRef.Pointer
        );

        lf.TransferOwnership();
        partitionBy.TransferOwnership();

        ErrorHelper.CheckVoid();
    }
    private static FfiBuffer SinkParquetMemoryRaw(
        LazyFrameHandle lf,
        PlParquetCompression compression,
        int compressionLevel,
        bool statistics,
        int rowGroupSize,
        int dataPageSize,
        int compatLevel,
        bool maintainOrder,
        ParquetWriteExtrasHandle? extras = null)
    {
        nuint rowGroupSizeNative = rowGroupSize > 0 ? (nuint)rowGroupSize : 0;
        nuint dataPageSizeNative = dataPageSize > 0 ? (nuint)dataPageSize : 0;

        using var extrasRef = new BorrowedHandle(extras);
        NativeBindings.pl_lazyframe_sink_parquet_memory(
            lf,
            out var ffiBuffer,
            compression,
            compressionLevel,
            statistics,
            rowGroupSizeNative,
            dataPageSizeNative,
            compatLevel,
            maintainOrder,
            extrasRef.Pointer
        );

        lf.TransferOwnership();
        ErrorHelper.CheckVoid();

        return ffiBuffer;
    }
    public static byte[] SinkParquetMemory(
        LazyFrameHandle lf,
        PlParquetCompression compression,
        int compressionLevel,
        bool statistics,
        int rowGroupSize,
        int dataPageSize,
        int compatLevel,
        bool maintainOrder,
        ParquetWriteExtrasHandle? extras = null)
        => NativeBuffer.TakeToArray(SinkParquetMemoryRaw(lf, compression, compressionLevel, statistics, rowGroupSize, dataPageSize, compatLevel, maintainOrder, extras));
    public static NativeBuffer SinkParquetNativeMemory(
        LazyFrameHandle lf,
        PlParquetCompression compression,
        int compressionLevel,
        bool statistics,
        int rowGroupSize,
        int dataPageSize,
        int compatLevel,
        bool maintainOrder,
        ParquetWriteExtrasHandle? extras = null)
        => NativeBuffer.Take(SinkParquetMemoryRaw(lf, compression, compressionLevel, statistics, rowGroupSize, dataPageSize, compatLevel, maintainOrder, extras));
    public static void SinkParquetCallback(
        LazyFrameHandle lf,
        NativeWriteTarget target,
        PlParquetCompression compression,
        int compressionLevel,
        bool statistics,
        int rowGroupSize,
        int dataPageSize,
        int compatLevel,
        bool maintainOrder,
        ParquetWriteExtrasHandle? extras = null)
    {
        nuint rowGroupSizeNative = rowGroupSize > 0 ? (nuint)rowGroupSize : 0;
        nuint dataPageSizeNative = dataPageSize > 0 ? (nuint)dataPageSize : 0;

        using var extrasRef = new BorrowedHandle(extras);
        NativeBindings.pl_lazyframe_sink_parquet_callback(
            lf,
            target.Callback,
            target.UserData,
            target.BufferSize,
            compression,
            compressionLevel,
            statistics,
            rowGroupSizeNative,
            dataPageSizeNative,
            compatLevel,
            maintainOrder,
            extrasRef.Pointer
        );

        lf.TransferOwnership();
        ErrorHelper.CheckVoid();
    }
    // ---------------------------------------------------------
    // Scan IPC (File / Cloud)
    // ---------------------------------------------------------
    public static LazyFrameHandle ScanIpc(
        string path,
        ulong? nRows,
        bool rechunk,
        bool cache,
        bool glob,
        string? rowIndexName,
        uint rowIndexOffset,
        string? includePathColumn,
        SchemaHandle? schema,
        bool hivePartitioning,
        SchemaHandle? hivePartitionSchema,
        bool tryParseHiveDates,
        // --- Cloud Params ---
        PlCloudProvider cloudProvider,
        nuint cloudRetries,
        ulong cloudRetryTimeoutMs,
        ulong cloudRetryInitBackoffMs,
        ulong cloudRetryMaxBackoffMs,
        ulong cloudCacheTtl,
        string[]? cloudKeys,
        string[]? cloudValues)
    {
        unsafe
        {
            ulong rowsVal = nRows.GetValueOrDefault();
            IntPtr rowsPtr = nRows.HasValue ? (IntPtr)(&rowsVal) : IntPtr.Zero;

            using var schemaLock = new SafeHandleLock<SchemaHandle>(
                schema != null ? [schema] : null
            );
            IntPtr schemaPtr = schema != null ? schemaLock.Pointers[0] : IntPtr.Zero;

            using var hiveLock = new SafeHandleLock<SchemaHandle>(
                hivePartitionSchema != null ? [hivePartitionSchema] : null
            );
            IntPtr hiveSchemaPtr = hivePartitionSchema != null ? hiveLock.Pointers[0] : IntPtr.Zero;

            nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);

            var h = NativeBindings.pl_scan_ipc(
                path,
                // --- Unified Args ---
                rowsPtr,
                rechunk,
                cache,
                glob,
                rowIndexName,
                rowIndexOffset,
                includePathColumn,
                // --- Schema & Hive ---
                schemaPtr,
                hivePartitioning,
                hiveSchemaPtr,
                tryParseHiveDates,
                // --- Cloud Params ---
                cloudProvider,
                cloudRetries,
                cloudRetryTimeoutMs,
                cloudRetryInitBackoffMs,
                cloudRetryMaxBackoffMs,
                cloudCacheTtl,
                cloudKeys,
                cloudValues,
                cloudLen
            );

            return ErrorHelper.Check(h);
        }
    }

    // ---------------------------------------------------------
    // Scan IPC (Memory / Bytes)
    // ---------------------------------------------------------
    public static LazyFrameHandle ScanIpc(
        NativeBytes buffer,
        ulong? nRows,
        bool rechunk,
        bool cache,
        string? rowIndexName,
        uint rowIndexOffset,
        string? includePathColumn,
        SchemaHandle? schema,
        bool hivePartitioning,
        SchemaHandle? hivePartitionSchema,
        bool tryParseHiveDates)
    {
        if (buffer.Length == 0)
            throw new ArgumentException("Buffer cannot be empty", nameof(buffer));

        unsafe
        {
            {
                ulong rowsVal = nRows.GetValueOrDefault();
                nint rowsPtr = nRows.HasValue ? (nint)(&rowsVal) : nint.Zero;

                using var schemaLock = new SafeHandleLock<SchemaHandle>(
                    schema != null ? [schema] : null
                );
                nint schemaPtr = schema != null ? schemaLock.Pointers[0] : nint.Zero;

                using var hiveLock = new SafeHandleLock<SchemaHandle>(
                    hivePartitionSchema != null ? [hivePartitionSchema] : null
                );
                nint hiveSchemaPtr = hivePartitionSchema != null ? hiveLock.Pointers[0] : nint.Zero;

                fixed (byte* copyPointer = buffer.CopySource)
                {
                    var lease = buffer.Acquire(copyPointer);
                    LazyFrameHandle h;
                    try
                    {
                        h = NativeBindings.pl_scan_ipc_memory(
                            lease.Pointer, lease.Length, lease.Release, lease.UserData,
                            // --- Unified Args ---
                            rowsPtr,
                            rechunk,
                            cache,
                            rowIndexName,
                            rowIndexOffset,
                            includePathColumn,
                            // --- Schema & Hive ---
                            schemaPtr,
                            hivePartitioning,
                            hiveSchemaPtr,
                            tryParseHiveDates
                        );
                    }
                    catch
                    {
                        // The native function was not reached: it did not take the pin.
                        NativeBytes.ReleaseUnused(lease);
                        throw;
                    }

                    return ErrorHelper.Check(h);
                }
            }
        }
    }
    public static DataFrameHandle ReadIpcStream(
        string path,
        string[]? columns,
        uint[]? projection,
        ulong? nRows,
        string? rowIndexName,
        uint rowIndexOffset,
        bool rechunk)
    {
        if (!File.Exists(path)) throw new FileNotFoundException($"Ipc Stream file not found: {path}");

        unsafe
        {
            ulong rowsVal = nRows.GetValueOrDefault();
            nint rowsPtr = nRows.HasValue ? (nint)(&rowsVal) : nint.Zero;

            nuint columnsLen = (nuint)(columns?.Length ?? 0);
            nuint[]? nativeProjection = null;
            nuint projectionLen = 0;
            if (projection != null)
            {
                nativeProjection = System.Array.ConvertAll(projection, x => (nuint)x);
                projectionLen = (nuint)projection.Length;
            }
            var h = NativeBindings.pl_read_ipc_stream(
                path,
                columns,
                columnsLen,
                nativeProjection,
                projectionLen,
                rowsPtr,
                rowIndexName,
                rowIndexOffset,
                rechunk
            );

            return ErrorHelper.Check(h);
        }
    }
    public static DataFrameHandle ReadIpcStream(
        ReadOnlySpan<byte> buffer,
        string[]? columns,
        uint[]? projection,
        ulong? nRows,
        string? rowIndexName,
        uint rowIndexOffset,
        bool rechunk)
    {
        if (buffer.IsEmpty) throw new ArgumentException("Buffer cannot be empty.", nameof(buffer));

        unsafe
        {
            ulong rowsVal = nRows.GetValueOrDefault();
            nint rowsPtr = nRows.HasValue ? (nint)(&rowsVal) : nint.Zero;

            nuint columnsLen = (nuint)(columns?.Length ?? 0);
            nuint bufferLen = (nuint)buffer.Length;
            nuint[]? nativeProjection = null;
            nuint projectionLen = 0;
            if (projection != null)
            {
                nativeProjection = System.Array.ConvertAll(projection, x => (nuint)x);
                projectionLen = (nuint)projection.Length;
            }
            var h = NativeBindings.pl_read_ipc_stream_memory(
                buffer,
                bufferLen,
                columns,
                columnsLen,
                nativeProjection,
                projectionLen,
                rowsPtr,
                rowIndexName,
                rowIndexOffset,
                rechunk
            );

            return ErrorHelper.Check(h);
        }
    }
    public static SchemaHandle ReadIpcStreamSchema(string path)
    {
        if (!File.Exists(path)) throw new FileNotFoundException($"Ipc Stream file not found: {path}");

        var h = NativeBindings.pl_read_ipc_stream_schema(path);
        return ErrorHelper.Check(h);
    }

    public static SchemaHandle ReadIpcStreamSchema(ReadOnlySpan<byte> buffer)
    {
        if (buffer.IsEmpty) throw new ArgumentException("Buffer cannot be empty.", nameof(buffer));

        nuint bufferLen = (nuint)buffer.Length;
        var h = NativeBindings.pl_read_ipc_stream_schema_memory(buffer, bufferLen);
        return ErrorHelper.Check(h);
    }
    /// <summary>
    /// Sinks the LazyFrame to an IPC file.
    /// Consumes the LazyFrame handle.
    /// </summary>
    public static void SinkIpc(
        LazyFrameHandle lf,
        string path,
        PlIpcCompression compression,
        int compatLevel,
        int recordBatchSize,
        bool recordBatchStatistics,
        bool maintainOrder,
        PlSyncOnClose syncOnClose,
        bool mkdir,
        // --- Cloud Options ---
        PlCloudProvider cloudProvider,
        nuint cloudRetries,
        ulong cloudRetryTimeoutMs,
        ulong cloudRetryInitBackoffMs,
        ulong cloudRetryMaxBackoffMs,
        ulong cloudCacheTtl,
        string[]? cloudKeys,
        string[]? cloudValues
        )
    {
        nuint batchSize = recordBatchSize > 0 ? (nuint)recordBatchSize : 0;
        nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);
        NativeBindings.pl_lazyframe_sink_ipc(
            lf,
            path,
            compression,
            compatLevel,
            batchSize,
            recordBatchStatistics,
            maintainOrder,
            syncOnClose,
            mkdir,
            // Cloud Args
            cloudProvider,
            cloudRetries,
            cloudRetryTimeoutMs,
            cloudRetryInitBackoffMs,
            cloudRetryMaxBackoffMs,
            cloudCacheTtl,
            cloudKeys,
            cloudValues,
            cloudLen
        );

        lf.TransferOwnership();
        ErrorHelper.CheckVoid();
    }
    /// <summary>
    /// Sinks the LazyFrame to partitioned IPC files.
    /// Consumes the LazyFrame handle.
    /// </summary>
    public static void SinkIpcPartitioned(
        LazyFrameHandle lf,
        string path,
        // --- Partition Params ---
        SelectorHandle partitionBy,
        bool includeKeys,
        bool keysPreGrouped,
        nuint maxRowsPerFile,
        ulong approxBytesPerFile,
        PlIpcCompression compression,
        int compatLevel,
        int recordBatchSize,
        bool recordBatchStatistics,
        bool maintainOrder,
        PlSyncOnClose syncOnClose,
        bool mkdir,
        // --- Cloud Options ---
        PlCloudProvider cloudProvider,
        nuint cloudRetries,
        ulong cloudRetryTimeoutMs,
        ulong cloudRetryInitBackoffMs,
        ulong cloudRetryMaxBackoffMs,
        ulong cloudCacheTtl,
        string[]? cloudKeys,
        string[]? cloudValues
        )
    {
        nuint batchSize = recordBatchSize > 0 ? (nuint)recordBatchSize : 0;
        nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);
        NativeBindings.pl_sink_ipc_partitioned(
            lf,
            path,
            // Partition Params
            partitionBy,
            includeKeys,
            keysPreGrouped,
            maxRowsPerFile,
            approxBytesPerFile,
            compression,
            compatLevel,
            batchSize,
            recordBatchStatistics,
            maintainOrder,
            syncOnClose,
            mkdir,
            // Cloud Args
            cloudProvider,
            cloudRetries,
            cloudRetryTimeoutMs,
            cloudRetryInitBackoffMs,
            cloudRetryMaxBackoffMs,
            cloudCacheTtl,
            cloudKeys,
            cloudValues,
            cloudLen
        );
        partitionBy.TransferOwnership();
        lf.TransferOwnership();
        ErrorHelper.CheckVoid();
    }
    private static FfiBuffer SinkIpcMemoryRaw(
        LazyFrameHandle lf,
        PlIpcCompression compression,
        int compatLevel,
        int recordBatchSize,
        bool recordBatchStatistics,
        bool maintainOrder)
    {
        nuint batchSize = recordBatchSize > 0 ? (nuint)recordBatchSize : 0;

        NativeBindings.pl_lazyframe_sink_ipc_memory(
            lf,
            out var ffiBuffer,
            compression,
            compatLevel,
            batchSize,
            recordBatchStatistics,
            maintainOrder
        );

        lf.TransferOwnership();

        ErrorHelper.CheckVoid();

        return ffiBuffer;
    }
    public static byte[] SinkIpcMemory(
        LazyFrameHandle lf,
        PlIpcCompression compression,
        int compatLevel,
        int recordBatchSize,
        bool recordBatchStatistics,
        bool maintainOrder)
        => NativeBuffer.TakeToArray(SinkIpcMemoryRaw(lf, compression, compatLevel, recordBatchSize, recordBatchStatistics, maintainOrder));
    public static NativeBuffer SinkIpcNativeMemory(
        LazyFrameHandle lf,
        PlIpcCompression compression,
        int compatLevel,
        int recordBatchSize,
        bool recordBatchStatistics,
        bool maintainOrder)
        => NativeBuffer.Take(SinkIpcMemoryRaw(lf, compression, compatLevel, recordBatchSize, recordBatchStatistics, maintainOrder));
    public static void SinkIpcCallback(
        LazyFrameHandle lf,
        NativeWriteTarget target,
        PlIpcCompression compression,
        int compatLevel,
        int recordBatchSize,
        bool recordBatchStatistics,
        bool maintainOrder)
    {
        nuint batchSize = recordBatchSize > 0 ? (nuint)recordBatchSize : 0;

        NativeBindings.pl_lazyframe_sink_ipc_callback(
            lf,
            target.Callback,
            target.UserData,
            target.BufferSize,
            compression,
            compatLevel,
            batchSize,
            recordBatchStatistics,
            maintainOrder
        );

        lf.TransferOwnership();
        ErrorHelper.CheckVoid();
    }
    public static void WriteIpcStream(
        DataFrameHandle df,
        string path,
        PlIpcCompression compression,
        int compatLevel)
    {
        NativeBindings.pl_dataframe_write_ipc_stream(
            df,
            path,
            compression,
            compatLevel
        );

        ErrorHelper.CheckVoid();
    }

    private static FfiBuffer WriteIpcStreamMemoryRaw(
        DataFrameHandle df,
        PlIpcCompression compression,
        int compatLevel)
    {
        NativeBindings.pl_dataframe_write_ipc_stream_memory(
            df,
            out var ffiBuffer,
            compression,
            compatLevel
        );

        ErrorHelper.CheckVoid();

        return ffiBuffer;
    }
    public static byte[] WriteIpcStreamMemory(
        DataFrameHandle df,
        PlIpcCompression compression,
        int compatLevel)
        => NativeBuffer.TakeToArray(WriteIpcStreamMemoryRaw(df, compression, compatLevel));
    public static NativeBuffer WriteIpcStreamNativeMemory(
        DataFrameHandle df,
        PlIpcCompression compression,
        int compatLevel)
        => NativeBuffer.Take(WriteIpcStreamMemoryRaw(df, compression, compatLevel));
    public static void WriteIpcStreamCallback(
        DataFrameHandle df,
        NativeWriteTarget target,
        PlIpcCompression compression,
        int compatLevel)
    {
        NativeBindings.pl_dataframe_write_ipc_stream_callback(
            df,
            target.Callback,
            target.UserData,
            target.BufferSize,
            compression,
            compatLevel
        );

        ErrorHelper.CheckVoid();
    }
    public static void ExportBatches(DataFrameHandle dfHandle, Action<RecordBatch> onBatchReceived)
    {
        var (callback, cleanup, userData) = ArrowStreamInterop.PrepareSink(onBatchReceived);

        NativeBindings.pl_dataframe_export_batches(
            dfHandle,
            callback,
            cleanup,
            userData
        );
        ErrorHelper.CheckVoid();
    }

    public static LazyFrameHandle SinkBatches(LazyFrameHandle lf, Action<RecordBatch> onBatchReceived)
    {
        // Prepare Interop Resource (Delegate, GCHandle, Cleanup)
        var (callback, cleanup, userData) = ArrowStreamInterop.PrepareSink(onBatchReceived);

        var handle = NativeBindings.pl_lazy_map_batches(
            lf,
            callback,
            cleanup,
            userData
        );
        lf.TransferOwnership();
        return ErrorHelper.Check(handle);
    }
}
