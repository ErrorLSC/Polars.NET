using Apache.Arrow;
using Apache.Arrow.C;

namespace Polars.NET.Core;

public static partial class PolarsWrapper
{
    private static T WithSchemaArrays<T>(
            Dictionary<string, DataTypeHandle>? schema, 
            Func<IntPtr[]?, IntPtr[]?, UIntPtr, T> action)
        {
            if (schema == null || schema.Count == 0)
            {
                return action(null, null, UIntPtr.Zero);
            }

            var names = schema.Keys.ToArray();
            // DataTypeHandle 是 SafeHandle，DangerousGetHandle() 获取原始指针
            var typeHandles = schema.Values.Select(h => h.DangerousGetHandle()).ToArray();
            
            return UseUtf8StringArray(names, namePtrs => 
            {
                return action(namePtrs, typeHandles, (UIntPtr)names.Length);
            });
        }
    public static DataFrameHandle ReadCsv(
            string path, 
            Dictionary<string, DataTypeHandle>? schema = null,
            bool hasHeader = true,
            char separator = ',',
            ulong skipRows = 0,
            bool tryParseDates = true) // [新增] 默认开启
        {
            return WithSchemaArrays(schema, (namePtrs, typePtrs, len) => 
            {
                return ErrorHelper.Check(NativeBindings.pl_read_csv(
                    path, 
                    namePtrs, 
                    typePtrs, 
                    len, 
                    hasHeader, 
                    (byte)separator, 
                    (UIntPtr)skipRows,
                    tryParseDates
                ));
            });
        }
    public static Task<DataFrameHandle> ReadCsvAsync(
            string path,
            Dictionary<string, DataTypeHandle>? schema = null,
            bool hasHeader = true,
            char separator = ',',
            ulong skipRows = 0, 
            bool tryParseDates = true)
    {
        return Task.Run(() => ReadCsv(path,schema,hasHeader,separator,skipRows, tryParseDates));
    }
    public static LazyFrameHandle ScanCsv(
            string path, 
            Dictionary<string, DataTypeHandle>? schema = null,
            bool hasHeader = true,
            char separator = ',',
            ulong skipRows = 0,
            bool tryParseDates = true) // [新增]
        {
            return WithSchemaArrays(schema, (namePtrs, typePtrs, len) => 
            {
                return ErrorHelper.Check(NativeBindings.pl_scan_csv(
                    path, 
                    namePtrs, 
                    typePtrs, 
                    len, 
                    hasHeader, 
                    (byte)separator, 
                    (UIntPtr)skipRows,
                    tryParseDates
                ));
            });
        }

    public static DataFrameHandle ReadParquet(string path)
    {
         if (!File.Exists(path)) throw new FileNotFoundException($"Parquet not found: {path}");
         return ErrorHelper.Check(NativeBindings.pl_read_parquet(path));
    }
    public static Task<DataFrameHandle> ReadParquetAsync(string path)
    {
        return Task.Run(() => ReadParquet(path));
    }
    public static LazyFrameHandle ScanParquet(string path) {
        if (!File.Exists(path)) throw new FileNotFoundException($"Parquet not found: {path}");
        return ErrorHelper.Check(NativeBindings.pl_scan_parquet(path));
    } 

    public static void WriteCsv(DataFrameHandle df, string path)
    {
        NativeBindings.pl_write_csv(df, path);
        ErrorHelper.CheckVoid();
    }

    public static void WriteParquet(DataFrameHandle df, string path)
    {
        NativeBindings.pl_write_parquet(df, path);
        ErrorHelper.CheckVoid();
    }
    public static void WriteIpc(DataFrameHandle df, string path)
    {
        NativeBindings.pl_dataframe_write_ipc(df, path);
        ErrorHelper.CheckVoid(); 
    }

    public static void WriteJson(DataFrameHandle df, string path)
    {
        NativeBindings.pl_dataframe_write_json(df, path);
        ErrorHelper.CheckVoid();
    }
    // Sink Parquet
    public static void SinkParquet(LazyFrameHandle lf, string path)
    {
        NativeBindings.pl_lazy_sink_parquet(lf, path);
        lf.TransferOwnership();
        ErrorHelper.CheckVoid();
    }
    // JSON Eager
    public static DataFrameHandle ReadJson(string path)
    {
        if (!File.Exists(path)) throw new FileNotFoundException($"JSON file not found: {path}");
        return ErrorHelper.Check(NativeBindings.pl_read_json(path));
    }

    // NDJSON Lazy
    public static LazyFrameHandle ScanNdjson(string path)
    {
        if (!File.Exists(path)) throw new FileNotFoundException($"NDJSON file not found: {path}");
        return ErrorHelper.Check(NativeBindings.pl_scan_ndjson(path));
    }
    public static DataFrameHandle ReadIpc(string path)
    {
        if (!File.Exists(path)) throw new FileNotFoundException($"IPC file not found: {path}");
        return ErrorHelper.Check(NativeBindings.pl_read_ipc(path));
    }

    public static LazyFrameHandle ScanIpc(string path)
    {
        if (!File.Exists(path)) throw new FileNotFoundException($"IPC file not found: {path}");
        return ErrorHelper.Check(NativeBindings.pl_scan_ipc(path));
    }

    public static void SinkIpc(LazyFrameHandle lf, string path)
    {
        NativeBindings.pl_lazy_sink_ipc(lf, path);
        lf.TransferOwnership();
        ErrorHelper.CheckVoid();
    }
    public static unsafe DataFrameHandle FromArrow(RecordBatch batch)
    {
        // 1. 在栈上分配 C 结构体 (避免 GC 压力)
        // 这里的 new 是 C# 的 struct new，分配在栈上
        var cArray = new CArrowArray();
        var cSchema = new CArrowSchema();

        // 2. 分两步导出
        // Step A: 导出数据 (填充 cArray)
        CArrowArrayExporter.ExportRecordBatch(batch, &cArray);

        // Step B: 导出 Schema (填充 cSchema)
        // 注意：RecordBatch 有一个 .Schema 属性
        CArrowSchemaExporter.ExportSchema(batch.Schema, &cSchema);

        // 3. 传给 Rust
        // Rust 会执行 import，从而接管 cArray/cSchema 指向的堆内存
        // 注意：一旦 Rust import 成功，它会把 cArray->release 置空
        var h = NativeBindings.pl_dataframe_from_arrow_record_batch(&cArray, &cSchema);
        
        return ErrorHelper.Check(h);
    }
    public static unsafe LazyFrameHandle LazyFrameScanStream(
        CArrowSchema* schema,
        delegate* unmanaged[Cdecl]<void*, Polars.NET.Core.Arrow.CArrowArrayStream*> callback,
        delegate* unmanaged[Cdecl]<void*, void> destroyCallback,
        void* userData)
    {
        var handle = NativeBindings.pl_lazy_frame_scan_stream(schema, callback,destroyCallback, userData);
        return ErrorHelper.Check(handle);
    }
}