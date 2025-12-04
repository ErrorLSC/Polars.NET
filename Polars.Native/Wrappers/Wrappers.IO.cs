using Apache.Arrow;
using Apache.Arrow.C;

namespace Polars.Native;

public static partial class PolarsWrapper
{
    public static DataFrameHandle ReadCsv(string path, bool tryParseDates)
    {
        if (!File.Exists(path)) throw new FileNotFoundException($"CSV not found: {path}");
        return ErrorHelper.Check(NativeBindings.pl_read_csv(path, tryParseDates));
    }

    public static LazyFrameHandle ScanCsv(string path, bool tryParseDates)
    {
        if (!File.Exists(path)) throw new FileNotFoundException($"CSV not found: {path}");
        return ErrorHelper.Check(NativeBindings.pl_scan_csv(path, tryParseDates));
    }

    public static DataFrameHandle ReadParquet(string path)
    {
         if (!File.Exists(path)) throw new FileNotFoundException($"Parquet not found: {path}");
         return ErrorHelper.Check(NativeBindings.pl_read_parquet(path));
    }

    public static LazyFrameHandle ScanParquet(string path) {
        if (!File.Exists(path)) throw new FileNotFoundException($"Parquet not found: {path}");
        return ErrorHelper.Check(NativeBindings.pl_scan_parquet(path));
    } 

    public static void WriteCsv(DataFrameHandle df, string path)
    {
        // 1. 调用 Rust (借用操作，不消耗 df)
        NativeBindings.pl_write_csv(df, path);
        
        // 2. [修复] 检查 Rust 是否报错 (例如磁盘满、权限拒绝)
        ErrorHelper.CheckVoid();
    }

    public static void WriteParquet(DataFrameHandle df, string path)
    {
        NativeBindings.pl_write_parquet(df, path);
        
        // [修复] 必须检查错误
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
}