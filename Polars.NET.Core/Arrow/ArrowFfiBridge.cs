// Polars.NET.Core / Arrow / ArrowFfiBridge.cs
using Apache.Arrow;
using Apache.Arrow.C;

namespace Polars.NET.Core.Arrow
{
    public static class ArrowFfiBridge
    {
        /// <summary>
        /// Create a Polars Series from an Apache.Arrow array.
        /// This allows zero-copy import of complex types (List, Struct, etc.) constructed in C#.
        /// </summary>
        /// <param name="name">Name of the Series</param>
        /// <param name="arrowArray">The C# Apache.Arrow IArrowArray instance</param>
        /// <returns>A new Series</returns>
        public static SeriesHandle ImportSeries(string name, IArrowArray arrowArray)
        {
            unsafe
            {
                var cArray = new CArrowArray();
                var cSchema = new CArrowSchema();

                // 导出
                CArrowSchemaExporter.ExportType(arrowArray.Data.DataType, &cSchema);
                CArrowArrayExporter.ExportArray(arrowArray, &cArray);

                // 调用 Rust (注意：这里的 PolarsWrapper 也在 Core 层)
                return PolarsWrapper.SeriesFromArrow(name, &cArray, &cSchema);
            }
        }
        // ==========================================
        // DataFrame Import (FromArrow)
        // ==========================================
        public static unsafe DataFrameHandle ImportDataFrame(RecordBatch batch)
        {
            var cArray = CArrowArray.Create();
            var cSchema = CArrowSchema.Create();

            try
            {
                // 1. C# -> C Structs
                // 使用 Exporter 把 C# RecordBatch 导出到 C 结构体
                CArrowSchemaExporter.ExportSchema(batch.Schema, cSchema);
                CArrowArrayExporter.ExportRecordBatch(batch, cArray);

                // 2. C Structs -> Rust DataFrame
                // 调用你提供的绑定
                var handle = NativeBindings.pl_dataframe_from_arrow_record_batch(cArray, cSchema);
                
                return ErrorHelper.Check(handle);
            }
            catch
            {
                // 如果出错，我们需要手动释放，否则内存泄漏
                // 如果成功，Rust 会接管 array/schema 的所有权（通过 release 回调），我们不需要 Free
                // 但 CArrowArray.Create 分配的"壳子"可能需要 Free？
                // 通常 FFI 约定是：如果 Rust 接管了，它会调用 release。
                // 但 Exporter 只是填充内容。
                
                // 保险起见，如果报错才手动 Free。成功的话，所有权交给 Rust 了。
                CArrowArray.Free(cArray);
                CArrowSchema.Free(cSchema);
                throw;
            }
        }

        // ==========================================
        // DataFrame Export (ToArrow)
        // ==========================================
        public static unsafe RecordBatch ExportDataFrame(DataFrameHandle handle)
        {
            // 直接复用你提供的优秀实现！
            // CArrowArray/Schema 是结构体还是类取决于 Arrow 版本，
            // 新版通常是 struct，Create/Free 是静态方法。
            var array = CArrowArray.Create();
            var schema = CArrowSchema.Create();

            bool ownershipTransferred = false;

            try
            {
                // 调用 Native: 让 Rust 把数据填进这两个指针指向的内存
                NativeBindings.pl_to_arrow(handle, array, schema);
                
                // 检查 Rust 是否报错
                ErrorHelper.CheckVoid();

                // 将 C 结构体转回 C# 的 Schema 和 RecordBatch
                var managedSchema = CArrowSchemaImporter.ImportSchema(schema);
                var batch = CArrowArrayImporter.ImportRecordBatch(array, managedSchema);
                ownershipTransferred = true;
                return batch;
            }
            finally
            {
                // [关键修复] 只有在没有成功转移所有权（即出异常）时，才手动释放 array
                // 如果成功了，batch 对象析构时会自动调用 release 回调
                if (!ownershipTransferred)
                {
                    CArrowArray.Free(array);
                }
                
                // Schema 通常是元数据拷贝，ImportSchema 后 C# 端有一份新的。
                // 原来的 C 结构体需要释放吗？
                // Apache Arrow 的 ImportSchema 行为是 Copy。所以 schema 指针指向的 C 结构体依然归我们管。
                // 安全起见，Schema 总是 Free 是没问题的（它不持有大数据 buffer）。
                CArrowSchema.Free(schema);
            }
        }
    }
}