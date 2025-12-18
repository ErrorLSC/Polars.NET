using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using Apache.Arrow.C;
using Apache.Arrow;

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

        public static void* CreateDirectScanContext(
            Func<IEnumerator<Apache.Arrow.RecordBatch>> factory, 
            Apache.Arrow.Schema schema)
        {
            var context = new ScanContext
            {
                // 直接使用传入的工厂，不需要再包一层 ToArrowBatches
                Factory = factory,
                Schema = schema
            };

            var gcHandle = GCHandle.Alloc(context);
            return (void*)GCHandle.ToIntPtr(gcHandle);
        }

        public static DataFrameHandle ImportEager(IEnumerable<RecordBatch> stream)
        {
            var enumerator = stream.GetEnumerator();

            // 1. 探测首帧
            if (!enumerator.MoveNext())
            {
                // 流是空的
                enumerator.Dispose();
                return new DataFrameHandle(); // 返回无效句柄 (IsInvalid == true)
            }

            // 2. 获取元数据
            var firstBatch = enumerator.Current;
            var schema = firstBatch.Schema;

            // 3. 缝合迭代器 (利用 PrependEnumerator)
            // 注意：firstBatch 的所有权现在归 PrependEnumerator 管理，它负责 Dispose
            var combinedEnumerator = new PrependEnumerator(firstBatch, enumerator);

            // 4. 调用底层实现
            // 这里传入 Schema，因为我们已经拿到它了
            return ImportEager(combinedEnumerator, schema);
        }
        /// <summary>
        /// 封装 Lazy Scan 的底层逻辑：创建上下文 -> 导出 Schema -> 调用 Rust -> 清理 C Schema
        /// </summary>
        public static LazyFrameHandle ScanStream(
            Func<IEnumerator<RecordBatch>> streamFactory, 
            Schema schema)
        {
            // 1. 准备上下文 (UserData)
            // 使用我们之前做好的“直通”方法，不需要 T
            var userData = CreateDirectScanContext(streamFactory, schema);

            // 2. 导出 Schema
            var cSchema = CArrowSchema.Create();
            CArrowSchemaExporter.ExportSchema(schema, cSchema);

            try
            {
                // 3. 调用 Rust
                return PolarsWrapper.LazyFrameScanStream(
                    cSchema,
                    GetFactoryCallback(),
                    GetDestroyCallback(),
                    userData
                );
            }
            finally
            {
                // 4. 清理 C Schema (UserData 由 Rust 的 DestroyCallback 负责释放)
                CArrowSchema.Free(cSchema);
            }
        }
        // ------------------------------------------------------------
        // Sink to DataBase
        // ------------------------------------------------------------
        // ------------------------------------------------------------
        // 1. 委托定义 (Delegates)
        // ------------------------------------------------------------

        // 对应 Rust: fn(*mut ArrowArray, *mut ArrowSchema, *mut char) -> i32
        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        public delegate int SinkCallback(
            CArrowArray* array, 
            CArrowSchema* schema, 
            byte* errorMsg // 接收错误信息的 buffer (1KB)
        );

        // [新增] 对应 Rust: fn(*mut c_void)
        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        public delegate void CleanupCallback(void* userData);

        // ------------------------------------------------------------
        // 2. 上下文管理 (Context)
        // ------------------------------------------------------------

        /// <summary>
        /// 这是一个纯内部类，用来保活 Delegate 和传递 User Action
        /// </summary>
        private class SinkContext
        {
            public Action<RecordBatch> UserAction = null!;
            // [关键] 必须强引用这个 Native Delegate，防止在 Rust 调用期间被 C# GC 回收
            public SinkCallback KeepAliveCallback = null!; 
        }

        // ------------------------------------------------------------
        // 3. 静态辅助方法 (对外暴露的工厂)
        // ------------------------------------------------------------

        /// <summary>
        /// 准备 Sink 所需的所有非托管资源。
        /// 返回: (Callback委托, Cleanup委托, UserData指针)
        /// </summary>
        public static (SinkCallback, CleanupCallback, IntPtr) PrepareSink(Action<RecordBatch> onBatchReceived)
        {
            // 1. 创建上下文
            var ctx = new SinkContext
            {
                UserAction = onBatchReceived
            };

            // 2. 定义核心 Native 回调 (在这里处理指针 -> C# 对象)
            // 这样 LazyFrame 就不需要碰 CArrowArray* 这种脏东西了
            ctx.KeepAliveCallback = (arrPtr, schemaPtr, errPtr) =>
            {
                try
                {
                    // Import 会接管所有权
                    // 1. 先把 C Schema 指针转为 C# Schema 对象
                    var schema = CArrowSchemaImporter.ImportSchema(schemaPtr);
                    // 2. 再导入 RecordBatch
                    // 注意：这里 ImportRecordBatch 会接管 arrPtr 的所有权
                    var batch = CArrowArrayImporter.ImportRecordBatch(arrPtr, schema);
                    ctx.UserAction(batch);
                    return 0; // Success
                }
                catch (Exception ex)
                {
                    // 异常传回 Rust
                    var msgBytes = System.Text.Encoding.UTF8.GetBytes(ex.Message);
                    int len = Math.Min(msgBytes.Length, 1023);
                    Marshal.Copy(msgBytes, 0, (IntPtr)errPtr, len);
                    errPtr[len] = 0;
                    return 1; // Error
                }
            };

            // 3. 打包 UserData (GCHandle)
            var handle = GCHandle.Alloc(ctx);
            IntPtr userDataPtr = GCHandle.ToIntPtr(handle);

            // 4. 定义 Cleanup 回调 (静态闭包即可，不需要保活)
            CleanupCallback cleanup = (ptr) =>
            {
                var h = GCHandle.FromIntPtr((IntPtr)ptr);
                if (h.IsAllocated) h.Free();
            };

            return (ctx.KeepAliveCallback, cleanup, userDataPtr);
        }
    }
}