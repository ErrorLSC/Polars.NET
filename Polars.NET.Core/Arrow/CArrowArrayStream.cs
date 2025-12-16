using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.C;

namespace Polars.NET.Core.Arrow
{
    // 对应 C 结构: ArrowArrayStream
    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct CArrowArrayStream
    {
        // 1. 定义委托类型 (解决报错的关键)
        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        public delegate int get_schema_delegate(CArrowArrayStream* stream, CArrowSchema* outSchema);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        public delegate int get_next_delegate(CArrowArrayStream* stream, CArrowArray* outArray);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        public delegate byte* get_last_error_delegate(CArrowArrayStream* stream);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        public delegate void release_delegate(CArrowArrayStream* stream);
        // 获取 Schema 的回调
        public delegate* unmanaged[Cdecl]<CArrowArrayStream*, CArrowSchema*, int> get_schema;
        
        // 获取下一个 Batch 的回调
        public delegate* unmanaged[Cdecl]<CArrowArrayStream*, CArrowArray*, int> get_next;
        
        // 获取最近一次错误信息 (可选)
        public delegate* unmanaged[Cdecl]<CArrowArrayStream*, byte*> get_last_error;
        
        // 释放流的回调
        public delegate* unmanaged[Cdecl]<CArrowArrayStream*, void> release;
        
        // 私有数据指针 (存放我们的 C# Enumerator)
        public void* private_data;
    }
    public unsafe class ArrowStreamExporter(IEnumerator<RecordBatch> enumerator, Schema schema) : IDisposable
    {
        private readonly IEnumerator<RecordBatch> _enumerator = enumerator;
        private readonly Schema _schema = schema;
        private bool _isDisposed;

        // 导出到 C 指针
        public void Export(CArrowArrayStream* outStream)
        {
            outStream->get_schema = &GetSchemaStatic;
            outStream->get_next = &GetNextStatic;
            outStream->get_last_error = &GetLastErrorStatic;
            outStream->release = &ReleaseStatic;
            // 将 "this" (Exporter 实例) 钉在 private_data 里
            // GCHandle 确保 GC 不会移动或回收我们
            outStream->private_data = (void*)GCHandle.ToIntPtr(GCHandle.Alloc(this));
        }

        // --- Static Callbacks ---

        // [关键] 指定调用约定为 Cdecl，这是 Arrow C Stream Interface 要求的
        [UnmanagedCallersOnly(CallConvs = new[] { typeof(CallConvCdecl) })]
        private static int GetSchemaStatic(CArrowArrayStream* stream, CArrowSchema* outSchema)
        {
            try
            {
                var exporter = GetExporter(stream);
                CArrowSchemaExporter.ExportSchema(exporter._schema, outSchema);
                return 0; // Success
            }
            catch (Exception e)
            {
                Console.WriteLine($"[ArrowStream] GetSchema Error: {e}");
                return 5; // EIO (Input/output error)
            }
        }

        [UnmanagedCallersOnly(CallConvs = new[] { typeof(CallConvCdecl) })]
        private static int GetNextStatic(CArrowArrayStream* stream, CArrowArray* outArray)
        {
            try
            {
                var exporter = GetExporter(stream);
                
                if (exporter._enumerator.MoveNext())
                {
                    var batch = exporter._enumerator.Current;
                    // 导出 RecordBatch
                    CArrowArrayExporter.ExportRecordBatch(batch, outArray);
                }
                else
                {
                    // [修改 3] 解决 CArrowArray.release 不可访问的问题
                    // 协议规定：End of stream is signaled by marking the array released.
                    // 将整个结构体设为 default，其内部的 release 函数指针自然就是 NULL (0)
                    *outArray = default; 
                }
                return 0;
            }
            catch (Exception e)
            {
                Console.WriteLine($"[ArrowStream] GetNext Error: {e}");
                return 5; // EIO
            }
        }

        [UnmanagedCallersOnly(CallConvs = new[] { typeof(CallConvCdecl) })]
        private static byte* GetLastErrorStatic(CArrowArrayStream* stream)
        {
             // 简单起见返回 null，意味着没有错误信息
             return null; 
        }

        [UnmanagedCallersOnly(CallConvs = new[] { typeof(CallConvCdecl) })]
        private static void ReleaseStatic(CArrowArrayStream* stream)
        {
            var ptr = (IntPtr)stream->private_data;
            if (ptr != IntPtr.Zero)
            {
                // 恢复 GCHandle 并释放
                var handle = GCHandle.FromIntPtr(ptr);
                if (handle.IsAllocated)
                {
                    var exporter = (ArrowStreamExporter)handle.Target!;
                    exporter.Dispose();
                    handle.Free(); // 释放 GCHandle，允许 Exporter 被 GC
                }
                // stream->private_data = null;
            }
            // 标记 Stream 自身已释放
            Marshal.FreeHGlobal((IntPtr)stream);
        }

        private static ArrowStreamExporter GetExporter(CArrowArrayStream* stream)
        {
            var handle = GCHandle.FromIntPtr((IntPtr)stream->private_data);
            return (ArrowStreamExporter)handle.Target!;
        }

        public void Dispose()
        {
            if (!_isDisposed)
            {
                _enumerator.Dispose();
                _isDisposed = true;
            }
        }
    }
}