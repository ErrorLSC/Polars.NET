using Apache.Arrow;
using Apache.Arrow.C;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Native;
namespace Polars.NET.Core;

public readonly partial struct PolarsWrapper
{
    public static LazyFrameHandle ScanCsv(
        string path,
        SchemaHandle? schema,
        SchemaHandle? dtypeOverride,
        bool hasHeader,
        char separator,
        char? quoteChar,       
        char eolChar,         
        bool ignoreErrors,
        bool tryParseDates,
        bool lowMemory,
        bool cache,
        bool glob,
        bool rechunk,
        bool raiseIfEmpty,
        ulong skipRows,
        ulong skipRowsAfterHeader,
        ulong skipLines,
        ulong? nRows,
        ulong? inferSchemaLength,
        ulong? nThreads,
        ulong? chunkSize,
        string? rowIndexName,
        ulong rowIndexOffset,
        string? includeFilePaths,
        PlCsvEncoding encoding,
        string[]? nullValues, 
        bool missingIsNull,   
        string? commentPrefix,
        bool decimalComma,
        bool truncateRaggedLines,
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
            ulong nRowsVal = nRows.GetValueOrDefault();
            IntPtr pNRows = nRows.HasValue ? (IntPtr)(&nRowsVal) : IntPtr.Zero;

            ulong inferVal = inferSchemaLength.GetValueOrDefault();
            IntPtr pInfer = inferSchemaLength.HasValue ? (IntPtr)(&inferVal) : IntPtr.Zero;

            ulong nThreadsVal = nThreads.GetValueOrDefault();
            IntPtr pNThreads = nThreads.HasValue ? (IntPtr)(&nThreadsVal) : IntPtr.Zero;

            nuint csize = chunkSize.HasValue ? (nuint)chunkSize.Value : (nuint)0;
            byte quoteVal = quoteChar.HasValue ? (byte)quoteChar.Value : (byte)0;
            
            SchemaHandle schemaHandle = schema ?? new SchemaHandle();
            SchemaHandle dtypeOverrideHandle = dtypeOverride ?? new SchemaHandle();
            
            nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);

            var h = NativeBindings.pl_scan_csv(
                path,
                
                // --- Core ---
                hasHeader, (byte)separator, quoteVal, (byte)eolChar, 
                ignoreErrors, tryParseDates, lowMemory, cache, glob, rechunk, raiseIfEmpty,
                
                // --- Sizes & Threads ---
                (nuint)skipRows, (nuint)skipRowsAfterHeader, (nuint)skipLines,
                pNRows, pInfer, pNThreads, csize,
                
                // --- Row Index & Path ---
                rowIndexName, (nuint)rowIndexOffset, includeFilePaths,
                
                // --- Schema & Encoding ---
                schemaHandle,dtypeOverrideHandle, encoding,
                
                // --- Advanced ---
                nullValues, (nuint)(nullValues?.Length ?? 0), 
                missingIsNull, commentPrefix, decimalComma, truncateRaggedLines,
                
                // --- Cloud ---
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

    public static LazyFrameHandle ScanCsv(
        byte[] buffer,
        SchemaHandle? schema,
        SchemaHandle? dtypeOverride,
        bool hasHeader,
        char separator,
        char? quoteChar,       
        char eolChar,         
        bool ignoreErrors,
        bool tryParseDates,
        bool lowMemory,
        bool cache,
        bool glob,
        bool rechunk,
        bool raiseIfEmpty,
        ulong skipRows,
        ulong skipRowsAfterHeader,
        ulong skipLines,
        ulong? nRows,
        ulong? inferSchemaLength,
        ulong? nThreads,
        ulong? chunkSize,
        string? rowIndexName,
        ulong rowIndexOffset,
        string? includeFilePaths,
        PlCsvEncoding encoding,
        string[]? nullValues, 
        bool missingIsNull,   
        string? commentPrefix,
        bool decimalComma,
        bool truncateRaggedLines)
    {
        if (buffer == null || buffer.Length == 0)
            throw new ArgumentException("Buffer cannot be empty", nameof(buffer));

        unsafe
        {
            ulong nRowsVal = nRows.GetValueOrDefault();
            IntPtr pNRows = nRows.HasValue ? (IntPtr)(&nRowsVal) : IntPtr.Zero;

            ulong inferVal = inferSchemaLength.GetValueOrDefault();
            IntPtr pInfer = inferSchemaLength.HasValue ? (IntPtr)(&inferVal) : IntPtr.Zero;

            ulong nThreadsVal = nThreads.GetValueOrDefault();
            IntPtr pNThreads = nThreads.HasValue ? (IntPtr)(&nThreadsVal) : IntPtr.Zero;

            nuint csize = chunkSize.HasValue ? (nuint)chunkSize.Value : (nuint)0;
            byte quoteVal = quoteChar.HasValue ? (byte)quoteChar.Value : (byte)0;
            
            SchemaHandle schemaHandle = schema ?? new SchemaHandle();
            SchemaHandle dtypeOverrideHandle = dtypeOverride ?? new SchemaHandle();

            fixed (byte* pBuffer = buffer)
            {
                var h = NativeBindings.pl_scan_csv_mem(
                    pBuffer,
                    (UIntPtr)buffer.Length,
                    
                    // --- Core ---
                    hasHeader, (byte)separator, quoteVal, (byte)eolChar,   
                    ignoreErrors, tryParseDates, lowMemory, cache, glob, rechunk, raiseIfEmpty,
                    
                    // --- Sizes & Threads ---
                    (nuint)skipRows, (nuint)skipRowsAfterHeader, (nuint)skipLines,
                    pNRows, pInfer, pNThreads, csize,
                    
                    // --- Row Index & Path ---
                    rowIndexName, (nuint)rowIndexOffset, includeFilePaths,
                    
                    // --- Schema & Encoding ---
                    schemaHandle,dtypeOverrideHandle, encoding,
                    
                    // --- Advanced ---
                    nullValues, (nuint)(nullValues?.Length ?? 0), 
                    missingIsNull, commentPrefix, decimalComma, truncateRaggedLines
                );

                return ErrorHelper.Check(h);
            }
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
        string[]? cloudValues
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
                cloudLen
            );

            return ErrorHelper.Check(h);
        }
    }
    public static LazyFrameHandle ScanParquet(
        byte[] buffer,
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
        bool tryParseHiveDates)
    {
        unsafe
        {
            fixed (byte* pBuf = buffer)
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

                var h = NativeBindings.pl_scan_parquet_memory(
                    (IntPtr)pBuf, (UIntPtr)buffer.Length,
                    nRowsPtr, 
                    parallel, 
                    lowMemory, 
                    useStatistics, 
                    glob, 
                    allowMissingColumns, 
                    cache,
                    rechunk,
                    rowIndexName, 
                    rowIndexOffset, 
                    includePathColumn,
                    schemaPtr,
                    hivePartitioning,
                    hiveSchemaPtr,
                    tryParseHiveDates
                );

                return ErrorHelper.Check(h);
            }
        }
    }
    public static void SinkCsv(
        LazyFrameHandle lf,
        string path,

        // --- CSV Writer Options ---
        bool hasHeader,
        bool useBom,
        int batchSize,       
        bool checkExtension, 

        // --- Compression ---
        PlExternalCompression compressionCode, 
        int compressionLevel,                  
        // --- Serialize Options ---
        char separator,
        char quoteChar,
        PlQuoteStyle quoteStyle,
        string? nullValue,
        string? lineTerminator,
        string? dateFormat,
        string? timeFormat,
        string? datetimeFormat,
        bool? floatScientific,
        int? floatPrecision,
        bool decimalComma,

        // --- Unified Sink Options ---
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
        nuint bs = batchSize > 0 ? (nuint)batchSize : 0;
        
        // Float Scientific: null->-1, false->0, true->1
        int fScientific = floatScientific switch { null => -1, false => 0, true => 1 };
        
        // Float Precision: null->-1
        int fPrecision = floatPrecision ?? -1;

        // Cloud Length
        nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);

        NativeBindings.pl_lazyframe_sink_csv(
            lf, 
            path,
            
            // CSV Writer Options
            useBom,           // include_bom
            hasHeader,        // include_header
            bs,               // batch_size
            checkExtension,   // check_extension

            // Compression
            compressionCode,
            compressionLevel,

            // SerializeOptions
            dateFormat,
            timeFormat,
            datetimeFormat,
            fScientific,
            fPrecision,
            decimalComma,
            (byte)separator,
            (byte)quoteChar,
            nullValue,
            lineTerminator,
            quoteStyle,

            // UnifiedSinkArgs
            maintainOrder,
            syncOnClose,
            mkdir,

            // Cloud Options
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
    public static void SinkCsvPartitioned(
        LazyFrameHandle lf,
        string path,

        // --- Partition Params ---
        SelectorHandle partitionBy,
        bool includeKeys,
        bool keysPreGrouped,
        nuint maxRowsPerFile,
        ulong approxBytesPerFile,

        // --- CSV Writer Options ---
        bool hasHeader,
        bool useBom,
        int batchSize,       
        bool checkExtension, 

        // --- Compression ---
        PlExternalCompression compressionCode, 
        int compressionLevel,                  
        // --- Serialize Options ---
        char separator,
        char quoteChar,
        PlQuoteStyle quoteStyle,
        string? nullValue,
        string? lineTerminator,
        string? dateFormat,
        string? timeFormat,
        string? datetimeFormat,
        bool? floatScientific,
        int? floatPrecision,
        bool decimalComma,

        // --- Unified Sink Options ---
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
        nuint bs = batchSize > 0 ? (nuint)batchSize : 0;
        
        // Float Scientific: null->-1, false->0, true->1
        int fScientific = floatScientific switch { null => -1, false => 0, true => 1 };
        
        // Float Precision: null->-1
        int fPrecision = floatPrecision ?? -1;

        // Cloud Length
        nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);

        NativeBindings.pl_lazyframe_sink_csv_partitioned(
            lf, 
            path,
            // Partition Params
            partitionBy,
            includeKeys,
            keysPreGrouped,
            maxRowsPerFile,
            approxBytesPerFile,
            // CSV Writer Options
            useBom,           // include_bom
            hasHeader,        // include_header
            bs,               // batch_size
            checkExtension,   // check_extension

            // Compression
            compressionCode,
            compressionLevel,

            // SerializeOptions
            dateFormat,
            timeFormat,
            datetimeFormat,
            fScientific,
            fPrecision,
            decimalComma,
            (byte)separator,
            (byte)quoteChar,
            nullValue,
            lineTerminator,
            quoteStyle,

            // UnifiedSinkArgs
            maintainOrder,
            syncOnClose,
            mkdir,

            // Cloud Options
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
    public static byte[] SinkCsvMemory(
        LazyFrameHandle lf,
        bool includeBom,
        bool includeHeader,
        int batchSize,
        bool checkExtension,
        PlExternalCompression compressionCode,
        int compressionLevel,
        string? dateFormat,
        string? timeFormat,
        string? datetimeFormat,
        int floatScientific,
        int floatPrecision,
        bool decimalComma,
        byte separator,
        byte quoteChar,
        string? nullValue,
        string? lineTerminator,
        PlQuoteStyle quoteStyle,
        bool maintainOrder)
    {
        nuint batchSizeNative = batchSize > 0 ? (nuint)batchSize : 0;

        NativeBindings.pl_lazyframe_sink_csv_memory(
            lf,
            out var ffiBuffer,
            includeBom,
            includeHeader,
            batchSizeNative,
            checkExtension,
            compressionCode,
            compressionLevel,
            dateFormat,
            timeFormat,
            datetimeFormat,
            floatScientific,
            floatPrecision,
            decimalComma,
            separator,
            quoteChar,
            nullValue,
            lineTerminator,
            quoteStyle,
            maintainOrder
        );

        lf.TransferOwnership();
        ErrorHelper.CheckVoid();

        try
        {
            unsafe
            {
                var span = new ReadOnlySpan<byte>(ffiBuffer.Data.ToPointer(), (int)ffiBuffer.Length);
                return span.ToArray();
            }
        }
        finally
        {
            NativeBindings.pl_free_ffi_buffer(ffiBuffer);
        }
    }
    public static void WriteJson(DataFrameHandle df, string path, PlJsonFormat format)
    {
        NativeBindings.pl_dataframe_write_json(df, path, format);
        ErrorHelper.CheckVoid();
    }
    public static byte[] WriteJsonMemory(DataFrameHandle df,PlJsonFormat format)
    {
        NativeBindings.pl_dataframe_write_json_memory(df,out var ffiBuffer,format);
        ErrorHelper.CheckVoid();
        try
        {
            unsafe
            {
                var span = new ReadOnlySpan<byte>(ffiBuffer.Data.ToPointer(), (int)ffiBuffer.Length);
                return span.ToArray();
            }
        }
        finally
        {
            NativeBindings.pl_free_ffi_buffer(ffiBuffer);
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
        string[]? cloudValues
        )
    {
        nuint rgs = rowGroupSize > 0 ? (nuint)rowGroupSize : 0;
        nuint dps = dataPageSize > 0 ? (nuint)dataPageSize : 0;
        int safeCompatLevel = compatLevel;
        if (safeCompatLevel < -1) safeCompatLevel = -1;
        else if (safeCompatLevel > 1) safeCompatLevel = 1;

        nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);

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
            cloudLen
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
        string[]? cloudValues
    )
    {
        nuint rgs = rowGroupSize > 0 ? (nuint)rowGroupSize : 0;
        nuint dps = dataPageSize > 0 ? (nuint)dataPageSize : 0;
        
        int safeCompatLevel = compatLevel;
        if (safeCompatLevel < -1) safeCompatLevel = -1;
        else if (safeCompatLevel > 1) safeCompatLevel = 1;

        nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);

        // Call Native Binding
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
            cloudLen
        );

        lf.TransferOwnership();
        partitionBy.TransferOwnership();
        
        ErrorHelper.CheckVoid();
    }
    public static byte[] SinkParquetMemory(
        LazyFrameHandle lf,
        PlParquetCompression compression,
        int compressionLevel,
        bool statistics,
        int rowGroupSize,
        int dataPageSize,
        int compatLevel,
        bool maintainOrder)
    {
        nuint rowGroupSizeNative = rowGroupSize > 0 ? (nuint)rowGroupSize : 0;
        nuint dataPageSizeNative = dataPageSize > 0 ? (nuint)dataPageSize : 0;

        NativeBindings.pl_lazyframe_sink_parquet_memory(
            lf,
            out var ffiBuffer,
            compression,
            compressionLevel,
            statistics,
            rowGroupSizeNative,
            dataPageSizeNative,
            compatLevel,
            maintainOrder
        );

        lf.TransferOwnership();
        ErrorHelper.CheckVoid();

        try
        {
            unsafe
            {
                var span = new ReadOnlySpan<byte>(ffiBuffer.Data.ToPointer(), (int)ffiBuffer.Length);
                return span.ToArray();
            }
        }
        finally
        {
            NativeBindings.pl_free_ffi_buffer(ffiBuffer);
        }
    }
    // ---------------------------------------------------------
    // Read JSON (File)
    // ---------------------------------------------------------
    public static DataFrameHandle ReadJson(
        string path,
        string[]? columns,
        SchemaHandle? schema,
        ulong? inferSchemaLen, // Rust: Option<usize>
        ulong? batchSize,      // Rust: Option<usize>
        bool ignoreErrors,
        PlJsonFormat jsonFormat)
    {
        unsafe
        {
            ulong inferVal = inferSchemaLen.GetValueOrDefault();
            IntPtr inferPtr = inferSchemaLen.HasValue ? (IntPtr)(&inferVal) : IntPtr.Zero;

            ulong batchVal = batchSize.GetValueOrDefault();
            IntPtr batchPtr = batchSize.HasValue ? (IntPtr)(&batchVal) : IntPtr.Zero;

            using var schemaLock = new SafeHandleLock<SchemaHandle>(
                schema != null ? [schema] : null
            );
            IntPtr schemaPtr = schema != null ? schemaLock.Pointers[0] : IntPtr.Zero;

            var h = NativeBindings.pl_read_json(
                path,
                columns, (UIntPtr)(columns?.Length ?? 0),
                schemaPtr,
                inferPtr,
                batchPtr,
                ignoreErrors,
                jsonFormat 
            );

            return ErrorHelper.Check(h);
        };
    }

    // ---------------------------------------------------------
    // Read JSON (Memory / Bytes)
    // ---------------------------------------------------------
    public static DataFrameHandle ReadJson(
        byte[] buffer,
        string[]? columns,
        SchemaHandle? schema,
        ulong? inferSchemaLen,
        ulong? batchSize,
        bool ignoreErrors,
        PlJsonFormat jsonFormat)
    {
        unsafe
        {
            fixed (byte* pBuf = buffer)
            {
                ulong inferVal = inferSchemaLen.GetValueOrDefault();
                IntPtr inferPtr = inferSchemaLen.HasValue ? (IntPtr)(&inferVal) : IntPtr.Zero;

                ulong batchVal = batchSize.GetValueOrDefault();
                IntPtr batchPtr = batchSize.HasValue ? (IntPtr)(&batchVal) : IntPtr.Zero;

                using var schemaLock = new SafeHandleLock<SchemaHandle>(
                    schema != null ? [schema] : null
                );
                IntPtr schemaPtr = schema != null ? schemaLock.Pointers[0] : IntPtr.Zero;

                var h = NativeBindings.pl_read_json_memory(
                    (IntPtr)pBuf, (UIntPtr)buffer.Length,
                    columns, (UIntPtr)(columns?.Length ?? 0),
                    schemaPtr,
                    inferPtr,
                    batchPtr,
                    ignoreErrors,
                    jsonFormat
                );

                return ErrorHelper.Check(h);
            }
        };
    }

    // ---------------------------------------------------------
    // Scan NDJSON (File)
    // ---------------------------------------------------------
    public static LazyFrameHandle ScanNdjson(
        string path,
        SchemaHandle? schema,
        ulong? batchSize,
        ulong? inferSchemaLen,
        ulong? nRows,
        bool lowMemory,
        bool rechunk,
        bool ignoreErrors,
        string? rowIndexName,
        uint rowIndexOffset,
        string? includePathColumn,
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
        nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);
        unsafe
        {
            ulong batchVal = batchSize.GetValueOrDefault();
            IntPtr batchPtr = batchSize.HasValue ? (IntPtr)(&batchVal) : IntPtr.Zero;

            ulong inferVal = inferSchemaLen.GetValueOrDefault();
            IntPtr inferPtr = inferSchemaLen.HasValue ? (IntPtr)(&inferVal) : IntPtr.Zero;

            ulong nRowsVal = nRows.GetValueOrDefault();
            IntPtr nRowsPtr = nRows.HasValue ? (IntPtr)(&nRowsVal) : IntPtr.Zero;

            using var schemaLock = new SafeHandleLock<SchemaHandle>(
                schema != null ? [schema] : null
            );
            IntPtr schemaPtr = schema != null ? schemaLock.Pointers[0] : IntPtr.Zero;

            // 3. 调用 Native
            var h = NativeBindings.pl_scan_ndjson(
                path,
                batchPtr,
                lowMemory,
                rechunk,
                schemaPtr,
                inferPtr,
                nRowsPtr,
                ignoreErrors,
                rowIndexName,
                rowIndexOffset,
                includePathColumn,
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
    // Scan NDJSON (Memory / Bytes)
    // ---------------------------------------------------------
    public static LazyFrameHandle ScanNdjson(
        byte[] buffer,
        SchemaHandle? schema,
        ulong? batchSize,
        ulong? inferSchemaLen,
        ulong? nRows,
        bool lowMemory,
        bool rechunk,
        bool ignoreErrors,
        string? rowIndexName,
        uint rowIndexOffset,
        string? includePathColumn)
    {
        unsafe
        {
            fixed (byte* pBuf = buffer)
            {
                ulong batchVal = batchSize.GetValueOrDefault();
                IntPtr batchPtr = batchSize.HasValue ? (IntPtr)(&batchVal) : IntPtr.Zero;

                ulong inferVal = inferSchemaLen.GetValueOrDefault();
                IntPtr inferPtr = inferSchemaLen.HasValue ? (IntPtr)(&inferVal) : IntPtr.Zero;

                ulong nRowsVal = nRows.GetValueOrDefault();
                IntPtr nRowsPtr = nRows.HasValue ? (IntPtr)(&nRowsVal) : IntPtr.Zero;

                using var schemaLock = new SafeHandleLock<SchemaHandle>(
                    schema != null ? new[] { schema } : null
                );
                IntPtr schemaPtr = schema != null ? schemaLock.Pointers[0] : IntPtr.Zero;
                
                var h = NativeBindings.pl_scan_ndjson_memory(
                    (IntPtr)pBuf, (UIntPtr)buffer.Length,
                    batchPtr,
                    lowMemory,
                    rechunk,
                    schemaPtr,
                    inferPtr,
                    nRowsPtr,
                    ignoreErrors,
                    rowIndexName,
                    rowIndexOffset,
                    includePathColumn
                );

                return ErrorHelper.Check(h);
            }
        }
    }
    public static void SinkJson(
        LazyFrameHandle lf,
        string path,
        // Removed: PlJsonFormat format (Native supports NDJSON only for sink)
        PlExternalCompression compression,
        int compressionLevel,
        bool checkExtension,
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
        nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);
        NativeBindings.pl_lazyframe_sink_json(
            lf,
            path,
            compression,
            compressionLevel,
            checkExtension,
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
    public static void SinkJsonPartitioned(
        LazyFrameHandle lf,
        string path,
        // --- Partition Params ---
        SelectorHandle partitionBy,
        bool includeKeys,
        bool keysPreGrouped,
        nuint maxRowsPerFile,
        ulong approxBytesPerFile,
        // Removed: PlJsonFormat format (Native supports NDJSON only for sink)
        PlExternalCompression compression,
        int compressionLevel,
        bool checkExtension,
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
        nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);
        NativeBindings.pl_lazyframe_sink_json_partitioned(
            lf,
            path,
            // Partition Params
            partitionBy,
            includeKeys,
            keysPreGrouped,
            maxRowsPerFile,
            approxBytesPerFile,
            compression,
            compressionLevel,
            checkExtension,
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
    public static byte[] SinkJsonMemory(
        LazyFrameHandle lf,
        PlExternalCompression compression,
        int compressionLevel,
        bool checkExtension,
        bool maintainOrder
        )
    {
        NativeBindings.pl_lazyframe_sink_json_memory(
            lf,
            out var ffiBuffer,
            compression,
            compressionLevel,
            checkExtension,
            maintainOrder
        );

        lf.TransferOwnership();
        ErrorHelper.CheckVoid();

        try
        {
            unsafe
            {
                var span = new ReadOnlySpan<byte>(ffiBuffer.Data.ToPointer(), (int)ffiBuffer.Length);
                return span.ToArray();
            }
        }
        finally
        {
            NativeBindings.pl_free_ffi_buffer(ffiBuffer);
        }
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
        byte[] buffer,
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
        if (buffer == null || buffer.Length == 0)
            throw new ArgumentException("Buffer cannot be empty", nameof(buffer));

        unsafe
        {
            fixed (byte* pBuf = buffer)
            {
                ulong rowsVal = nRows.GetValueOrDefault();
                nint rowsPtr = nRows.HasValue ? (nint)(&rowsVal) : nint.Zero;

                using var schemaLock = new SafeHandleLock<SchemaHandle>(
                    schema != null ? new[] { schema } : null
                );
                nint schemaPtr = schema != null ? schemaLock.Pointers[0] : nint.Zero;

                using var hiveLock = new SafeHandleLock<SchemaHandle>(
                    hivePartitionSchema != null ? new[] { hivePartitionSchema } : null
                );
                nint hiveSchemaPtr = hivePartitionSchema != null ? hiveLock.Pointers[0] : nint.Zero;

                var h = NativeBindings.pl_scan_ipc_memory(
                    pBuf, (nuint)buffer.Length,
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

                return ErrorHelper.Check(h);
            }
        }
    }
    public static DataFrameHandle ReadIpcStream(
        string path, 
        string[]? columns,
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
            
            var h = NativeBindings.pl_read_ipc_stream(
                path,
                columns,
                columnsLen,
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

            var h = NativeBindings.pl_read_ipc_stream_memory(
                buffer,
                bufferLen,
                columns,
                columnsLen,
                rowsPtr,
                rowIndexName,
                rowIndexOffset,
                rechunk
            );

            return ErrorHelper.Check(h);
        }
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
        NativeBindings.pl_lazyframe_sink_ipc_partitioned(
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
    public static byte[] SinkIpcMemory(
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

        try
        {
            unsafe
            {
                var span = new ReadOnlySpan<byte>(ffiBuffer.Data.ToPointer(), (int)ffiBuffer.Length);
                
                return span.ToArray();
            }
        }
        finally
        {
            NativeBindings.pl_free_ffi_buffer(ffiBuffer);
        }
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

    public static byte[] WriteIpcStreamMemory(
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

        try
        {
            unsafe
            {
                if (ffiBuffer.Data == 0 || ffiBuffer.Length == 0)
                {
                    return [];
                }

                var span = new ReadOnlySpan<byte>((void*)ffiBuffer.Data, (int)ffiBuffer.Length);
                
                return span.ToArray();
            }
        }
        finally
        {
            NativeBindings.pl_free_ffi_buffer(ffiBuffer);
        }
    }
    public static unsafe LazyFrameHandle LazyFrameScanStream(
        CArrowSchema* schema,
        delegate* unmanaged[Cdecl]<void*, Arrow.CArrowArrayStream*> callback,
        delegate* unmanaged[Cdecl]<void*, void> destroyCallback,
        void* userData)
    {
        var handle = NativeBindings.pl_lazy_frame_scan_stream(schema, callback,destroyCallback, userData);
        return ErrorHelper.Check(handle);
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
    // ---------------------------------------------------------
    // Read Excel
    // ---------------------------------------------------------
    public static DataFrameHandle ReadExcel(
        string path,
        string? sheetName,
        ulong sheetIndex,
        SchemaHandle? schema,
        bool hasHeader,
        ulong inferSchemaLen,
        bool dropEmptyRows,
        bool raiseIfEmpty)
    {
        unsafe
        {
            using var schemaLock = new SafeHandleLock<SchemaHandle>(
                schema != null ? new[] { schema } : null
            );
            IntPtr schemaPtr = schema != null ? schemaLock.Pointers[0] : IntPtr.Zero;

            var h = NativeBindings.pl_read_excel(
                path,
                sheetName,
                (UIntPtr)sheetIndex, // ulong -> usize
                schemaPtr,
                hasHeader,
                (UIntPtr)inferSchemaLen,
                dropEmptyRows,
                raiseIfEmpty
            );

            return ErrorHelper.Check(h);
        }
    }

    // ---------------------------------------------------------
    // Write Excel
    // ---------------------------------------------------------
    public static void WriteExcel(
        DataFrameHandle handle,
        string path,
        string? sheetName,
        string? dateFormat,
        string? datetimeFormat)
    {
        if (handle.IsInvalid)
            throw new ObjectDisposedException(nameof(handle), "DataFrame handle is invalid or closed.");
        NativeBindings.pl_write_excel(handle, path, sheetName, dateFormat, datetimeFormat);
    }
    public static DataFrameHandle ReadAvro(
        string path, 
        ulong? nRows, 
        string[]? columns, 
        int[]? projection)
    {
        if (!File.Exists(path)) throw new FileNotFoundException($"Avro file not found: {path}");

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

            var h = NativeBindings.pl_read_avro(
                path,
                rowsPtr,
                columns,
                columnsLen,
                nativeProjection,
                projectionLen
            );

            return ErrorHelper.Check(h);
        }
    }
    public static DataFrameHandle ReadAvro(
        byte[] buffer, 
        ulong? nRows, 
        string[]? columns, 
        int[]? projection )
    {
        if (buffer == null || buffer.Length == 0) 
            throw new ArgumentException("Buffer cannot be null or empty", nameof(buffer));

        unsafe
        {
            ulong rowsVal = nRows.GetValueOrDefault();
            IntPtr rowsPtr = nRows.HasValue ? (IntPtr)(&rowsVal) : IntPtr.Zero;

            nuint columnsLen = (nuint)(columns?.Length ?? 0);
            
            nuint[]? nativeProjection = null;
            nuint projectionLen = 0;
            if (projection != null)
            {
                nativeProjection = System.Array.ConvertAll(projection, x => (nuint)x);
                projectionLen = (nuint)projection.Length;
            }

            fixed (byte* pBuffer = buffer)
            {
                var h = NativeBindings.pl_read_avro_mem(
                    pBuffer,
                    (nuint)buffer.Length,
                    rowsPtr,
                    columns,
                    columnsLen,
                    nativeProjection,
                    projectionLen
                );

                return ErrorHelper.Check(h);
            }
        }
    }
    public static void WriteAvro(
        DataFrameHandle df, 
        string path, 
        PlAvroCompression compression, 
        string name)
    {
        NativeBindings.pl_write_avro(df, path, compression, name ?? string.Empty);
        ErrorHelper.CheckVoid();
    }
    public static byte[] WriteAvroToMemory(
        DataFrameHandle df, 
        PlAvroCompression compression, 
        string name)
    {
        NativeBindings.pl_write_avro_mem(
            df,
            out var ffiBuffer,
            compression,
            name ?? string.Empty
        );

        ErrorHelper.CheckVoid();
        try
        {
            unsafe
            {
                var span = new ReadOnlySpan<byte>(ffiBuffer.Data.ToPointer(), (int)ffiBuffer.Length);
                
                return span.ToArray();
            }
        }
        finally
        {
            NativeBindings.pl_free_ffi_buffer(ffiBuffer);
        }
    }

}