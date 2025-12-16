using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;

namespace Polars.NET.Core.Arrow
{
    /// <summary>
    /// 专门负责处理 Arrow Stream 的 FFI 互操作逻辑。
    /// 将 LazyFrame 中的指针操作和静态回调隔离在此。
    /// </summary>
    public static unsafe class ArrowStreamInterop
    {
        // 上下文对象：持有重建流所需的所有信息
        private class ScanContext
        {
            public Func<IEnumerator<Apache.Arrow.RecordBatch>> Factory = default!;
            public Apache.Arrow.Schema Schema = default!;
        }

        /// <summary>
        /// 准备 Lazy Scan 上下文，并返回指向 Context 的 GCHandle 指针。
        /// </summary>
        public static void* CreateScanContext<T>(IEnumerable<T> data, int batchSize, Apache.Arrow.Schema schema)
        {
            var context = new ScanContext
            {
                // 封装工厂：每次调用都会生成一个新的枚举器
                Factory = () => data.ToArrowBatches(batchSize).GetEnumerator(),
                Schema = schema
            };

            var gcHandle = GCHandle.Alloc(context);
            return (void*)GCHandle.ToIntPtr(gcHandle);
        }

        // ---------------------------------------------------------
        // 静态回调函数 (供 Rust 调用)
        // ---------------------------------------------------------

        // 1. 获取创建流的回调函数指针
        public static delegate* unmanaged[Cdecl]<void*, CArrowArrayStream*> GetFactoryCallback()
        {
            return &StreamFactoryCallbackStatic;
        }

        // 2. 获取销毁上下文的回调函数指针
        public static delegate* unmanaged[Cdecl]<void*, void> GetDestroyCallback()
        {
            return &DestroyScanContextStatic;
        }

        // --- 实现细节 ---

        [UnmanagedCallersOnly(CallConvs = new[] { typeof(CallConvCdecl) })]
        private static CArrowArrayStream* StreamFactoryCallbackStatic(void* userData)
        {
            try
            {
                // 1. 恢复上下文
                var handle = GCHandle.FromIntPtr((IntPtr)userData);
                var context = (ScanContext)handle.Target!;
                
                // 2. 创建全新的枚举器 (从头开始)
                // 优化：Lazy 模式下每次都是新流，不需要 PrependEnumerator！
                // Exporter 内部会自己调用 MoveNext()
                var enumerator = context.Factory();
                
                // 3. 在堆上分配 C 结构体 (因为要返回指针给 Rust)
                var ptr = (CArrowArrayStream*)Marshal.AllocHGlobal(sizeof(CArrowArrayStream));
                
                // 4. 初始化 Exporter 并导出
                // 注意：schema 我们直接从 context 里拿，不需要再 peek 了
                var exporter = new ArrowStreamExporter(enumerator, context.Schema);
                exporter.Export(ptr);
                
                return ptr;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Polars.NET Critical] Error in Stream Factory Callback: {ex}");
                return null;
            }
        }

        [UnmanagedCallersOnly(CallConvs = new[] { typeof(CallConvCdecl) })]
        private static void DestroyScanContextStatic(void* userData)
        {
            try
            {
                var ptr = (IntPtr)userData;
                if (ptr != IntPtr.Zero)
                {
                    var handle = GCHandle.FromIntPtr(ptr);
                    if (handle.IsAllocated)
                    {
                        handle.Free(); // 释放 GCHandle，允许 Context 被 GC
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Polars.NET Critical] Error in Destroy Callback: {ex}");
            }
        }
        // ---------------------------------------------------------
        // Eager Mode (立即执行)
        // ---------------------------------------------------------

        /// <summary>
        /// Eager 模式：在当前栈帧分配 C 结构体，并立即调用 Rust 消费。
        /// </summary>
        public static DataFrameHandle ImportEager(IEnumerator<Apache.Arrow.RecordBatch> stream, Apache.Arrow.Schema schema)
        {
            // 1. 在栈上分配结构体 (比 AllocHGlobal 更快，且无需手动 Free)
            // 注意：Rust 端读取时是同步的，所以在此方法返回前，stack memory 都是有效的
            var cStream = new CArrowArrayStream();

            // 2. 初始化 Exporter
            // Exporter 会被 "钉" 在这里，直到 using 结束 (即 Rust 消费完毕)
            using var exporter = new ArrowStreamExporter(stream, schema);
            
            // 3. 挂载
            exporter.Export(&cStream);

            // 4. 调用 Rust
            // Rust 端会立即开始拉取数据，直到流结束
            return PolarsWrapper.DataFrameNewFromStream(&cStream);
        }
    }
}