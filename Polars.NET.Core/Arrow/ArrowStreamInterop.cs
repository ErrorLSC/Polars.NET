using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using Apache.Arrow.C;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using System.Reflection;
using Polars.NET.Core.Native;

namespace Polars.NET.Core.Arrow;
/// <summary>
/// Arrow Stream FFI InterOp Logic
/// </summary>
public static unsafe class ArrowStreamInterop
{
    // Context Object
    private class ScanContext
    {
        public Func<IEnumerable<RecordBatch>> Factory = default!;
        public Schema Schema = default!;
        public List<IntPtr> AllocatedStreams = [];
    }

    /// <summary>
    /// Perpare lazy scan context，then return GCHandle pointed to context.
    /// </summary>
    public static void* CreateScanContext<T>(IEnumerable<T> data, int batchSize, Schema schema)
    {
        var context = new ScanContext
        {
            Factory = () => data.ToArrowBatches(batchSize),
            Schema = schema
        };

        var gcHandle = GCHandle.Alloc(context);
        return (void*)GCHandle.ToIntPtr(gcHandle);
    }

    // ---------------------------------------------------------
    // Static Callback Delegate (for Rust callback)
    // ---------------------------------------------------------

    // Get steramfactory callback function pointer
    public static delegate* unmanaged[Cdecl]<void*, CArrowArrayStream*> GetFactoryCallback()
    {
        return &StreamFactoryCallbackStatic;
    }

    // Get destroy context callback function pointer
    public static delegate* unmanaged[Cdecl]<void*, void> GetDestroyCallback()
    {
        return &DestroyScanContextStatic;
    }

    [UnmanagedCallersOnly(CallConvs = new[] { typeof(CallConvCdecl) })]
    private static CArrowArrayStream* StreamFactoryCallbackStatic(void* userData)
    {
        try
        {
            // Resume Context
            var handle = GCHandle.FromIntPtr((IntPtr)userData);
            var context = (ScanContext)handle.Target!;
            
            // Create
            var enumerable = context.Factory();
            var rawEnumerator = enumerable.GetEnumerator();
            var safeEnumerator = new SafeEnumerator<RecordBatch>(rawEnumerator);
            
            // Alloc C Struct at Heap
            var ptr = (CArrowArrayStream*)Marshal.AllocHGlobal(sizeof(CArrowArrayStream));

            lock(context.AllocatedStreams) 
            {
                context.AllocatedStreams.Add((IntPtr)ptr);
            }
            
            // Init Exporter and export
            var exporter = new ArrowStreamExporter(safeEnumerator, context.Schema);
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
            if (ptr == IntPtr.Zero) return;

            var handle = GCHandle.FromIntPtr(ptr);
            if (handle.IsAllocated)
            {
                var context = (ScanContext)handle.Target!;
                
                if (context.AllocatedStreams != null)
                {
                    lock(context.AllocatedStreams)
                    {
                        foreach (var streamPtr in context.AllocatedStreams)
                        {
                            Marshal.FreeHGlobal(streamPtr);
                        }
                        context.AllocatedStreams.Clear();
                    }
                }

                handle.Free();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Polars.NET Critical] Error in Destroy Callback: {ex}");
        }
    }

    
    // ---------------------------------------------------------
    // Eager Mode
    // ---------------------------------------------------------

    /// <summary>
    /// Eager Mode: Alloc C struct at current stack frame and call Rust to consume
    /// </summary>
    public static DataFrameHandle ImportEager(IEnumerable<RecordBatch> stream, Schema schema)
    {
        // Alloc Struct at Stack
        var cStream = new CArrowArrayStream();

        // Init Exporter
        using var enumerator = stream.GetEnumerator();
        using var exporter = new ArrowStreamExporter(enumerator, schema);
        
        exporter.Export(&cStream);

        // Call Rust
        return PolarsWrapper.DataFrameNewFromStream(&cStream);
    }

    public static void* CreateDirectScanContext(
        Func<IEnumerable<RecordBatch>> factory, 
        Schema schema)
    {
        var context = new ScanContext
        {
            Factory = factory,
            Schema = schema
        };

        var gcHandle = GCHandle.Alloc(context);
        return (void*)GCHandle.ToIntPtr(gcHandle);
    }

    /// <summary>
    /// Logic For Lazy Scan
    /// </summary>
    public static LazyFrameHandle ScanStream(
        Func<IEnumerable<RecordBatch>> streamFactory, 
        Schema schema)
    {
        var userData = CreateDirectScanContext(streamFactory, schema);

        var cSchema = CArrowSchema.Create();
        CArrowSchemaExporter.ExportSchema(schema, cSchema);

        try
        {
            return PolarsWrapper.LazyFrameScanStream(
                cSchema,
                GetFactoryCallback(),
                GetDestroyCallback(),
                userData
            );
        }
        finally
        {
            CArrowSchema.Free(cSchema);
        }
    }

    private static int _streamByteOffset = -1;

    private class RawDataScanner { public byte Data; }

    public static DataFrameHandle ImportForeignStream(IArrowArrayStream stream)
    {
        var type = stream.GetType();

        // ==========================================
        // Step 1: Sniff
        // ==========================================
        if (_streamByteOffset == -1)
        {
            var fieldInfo = type.GetField("_cArrayStream", BindingFlags.NonPublic | BindingFlags.Instance)
                ?? throw new NotSupportedException("No _cArrayStream field found.");

            // Get boxedStruct
            object boxedStruct = fieldInfo.GetValue(stream)!;
            
            // Get self defined struct size
            int structSize = sizeof(CArrowArrayStream); 

            // Get payload first mem address
            ref byte streamPayload = ref Unsafe.As<RawDataScanner>(stream).Data;
            ref byte structPayload = ref Unsafe.As<RawDataScanner>(boxedStruct).Data;

            bool found = false;
            // Scan First 128 bytes
            for (int offset = 0; offset < 128; offset++)
            {
                ref byte candidate = ref Unsafe.Add(ref streamPayload, offset);
                
                // Byte Compare
                bool match = true;
                for (int i = 0; i < structSize; i++)
                {
                    if (Unsafe.Add(ref candidate, i) != Unsafe.Add(ref structPayload, i))
                    {
                        match = false;
                        break;
                    }
                }

                if (match)
                {
                    _streamByteOffset = offset;
                    found = true;
                    // Console.WriteLine($"[Wormhole Hacker] Memory offset sniffed successfully: {_streamByteOffset} bytes.");
                    break;
                }
            }

            if (!found) throw new Exception("Memory offset sniffing failed! The object layout is heavily obfuscated.");
        }

        // ==========================================
        // Step 2: Hijack
        // ==========================================
        var allocatedStream = Apache.Arrow.C.CArrowArrayStream.Create();
        var customCStream = (CArrowArrayStream*)allocatedStream;

        try
        {
            ref byte payloadStart = ref Unsafe.As<RawDataScanner>(stream).Data;
            
            ref byte targetStart = ref Unsafe.Add(ref payloadStart, _streamByteOffset);
            
            ref var myStructRef = ref Unsafe.As<byte, CArrowArrayStream>(ref targetStart);
            ValidateStream(ref myStructRef); // Safe checker

            *customCStream = myStructRef;
            myStructRef = default; 

            var handle = NativeBindings.pl_dataframe_new_from_stream_strict_type(customCStream);
            
            return ErrorHelper.Check(handle); 
        }
        finally
        {
            if (allocatedStream != null) Apache.Arrow.C.CArrowArrayStream.Free(allocatedStream);
            GC.KeepAlive(stream);
        }
    }
    // Safe Code Here:

    // private delegate ref Apache.Arrow.C.CArrowArrayStream GetCArrayStreamDelegate(object instance);
    // private static readonly ConcurrentDictionary<Type, GetCArrayStreamDelegate> _streamAccessorCache = new();
    // private static readonly ConcurrentDictionary<Type, FieldInfo> _aotFieldCache = new();

    /// <param name="ptr"></param>
    /// <returns></returns>
    // /// <summary>
    // /// Get C ptr from ADBC Stream and send to Rust
    // /// </summary>
    // public static DataFrameHandle ImportForeignStream(IArrowArrayStream stream)
    // {
    //     var type = stream.GetType();
    //     Console.WriteLine($"\n[Wormhole Radar] Target Type Name : {type.FullName}");
    //     Console.WriteLine($"[Wormhole Radar] Target Assembly  : {type.Assembly.GetName().Name}");
    //     Console.WriteLine($"[Wormhole Radar] Base Type        : {type.BaseType?.FullName}\n");
        
    //     var allocatedStream = Apache.Arrow.C.CArrowArrayStream.Create();
    //     var customCStream = (CArrowArrayStream*)allocatedStream;

    //     try
    //     {
    //         if (RuntimeFeature.IsDynamicCodeSupported)
    //         {
    //             // ==========================================
    //             // JIT  (IL Emit + Unsafe)
    //             // ==========================================
    //             var accessor = _streamAccessorCache.GetOrAdd(type, t =>
    //             {
    //                 var field = t.GetField("_cArrayStream", BindingFlags.NonPublic | BindingFlags.Instance)
    //                     ?? throw new NotSupportedException($"Type {t.Name} does not have _cArrayStream.");

    //                 var method = new DynamicMethod("GetCArrayStream_Fast",
    //                     typeof(Apache.Arrow.C.CArrowArrayStream).MakeByRefType(),
    //                     [typeof(object)], typeof(ArrowStreamInterop).Module, skipVisibility: true);

    //                 var il = method.GetILGenerator();
    //                 il.Emit(OpCodes.Ldarg_0);
    //                 il.Emit(OpCodes.Castclass, t);
    //                 il.Emit(OpCodes.Ldflda, field);
    //                 il.Emit(OpCodes.Ret);
    //                 return (GetCArrayStreamDelegate)method.CreateDelegate(typeof(GetCArrayStreamDelegate));
    //             });

    //             ref var apacheStructRef = ref accessor(stream);
    //             ref var myStructRef = ref Unsafe.As<Apache.Arrow.C.CArrowArrayStream, CArrowArrayStream>(ref apacheStructRef);

    //             ValidateStream(ref myStructRef); // Safe checker

    //             *customCStream = myStructRef;
    //             myStructRef = default; // Prevent double free
    //         }
    //         else
    //         {
    //             // ==========================================
    //             // Native AOT
    //             // ==========================================

    //             var fieldInfo = _aotFieldCache.GetOrAdd(type, t =>
    //                 t.GetField("_cArrayStream", BindingFlags.NonPublic | BindingFlags.Instance)
    //                 ?? throw new NotSupportedException($"Type {t.Name} does not have _cArrayStream.")
    //             );

    //             // Unbox
    //             object boxedValue = fieldInfo.GetValue(stream)!;
    //             var apacheStruct = (Apache.Arrow.C.CArrowArrayStream)boxedValue;
                
    //             // Reinterpret
    //             ref var myStructRef = ref Unsafe.As<Apache.Arrow.C.CArrowArrayStream, CArrowArrayStream>(ref apacheStruct);
                
    //             // Check
    //             ValidateStream(ref myStructRef);

    //             // Copy
    //             *customCStream = myStructRef;
                
    //             // Reset
    //             fieldInfo.SetValue(stream, default(Apache.Arrow.C.CArrowArrayStream));
    //         }

    //         // Call Rust
    //         var handle = NativeBindings.pl_dataframe_new_from_stream_strict_type(customCStream);

    //         return ErrorHelper.Check(handle);
    //     }
    //     finally
    //     {
    //         if (allocatedStream != null)
    //         {
    //             Apache.Arrow.C.CArrowArrayStream.Free(allocatedStream);
    //         }
    //         GC.KeepAlive(stream);
    //     }
    // }
    // [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_cArrayStream")]

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsInvalidPtr(void* ptr) => (nint)ptr < 0x10000;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateStream(ref CArrowArrayStream streamRef)
    {
        if (IsInvalidPtr(streamRef.release))
            throw new InvalidOperationException("Invalid ArrowArrayStream release function pointer (Null or Zero-Page).");
            
        if (IsInvalidPtr(streamRef.get_next) || IsInvalidPtr(streamRef.get_schema))
            throw new InvalidOperationException("Invalid ArrowArrayStream data/schema callbacks.");
            
        if (IsInvalidPtr(streamRef.private_data))
            throw new InvalidOperationException("Invalid ArrowArrayStream private_data. External DB driver fault.");

        if (IsInvalidPtr(streamRef.get_last_error))
        {
            streamRef.get_last_error = null;
        }
        if ((nint)streamRef.release % IntPtr.Size != 0)
            throw new InvalidOperationException("Misaligned function pointer.");
    }
    /// <summary>
    /// Export Polars DataFrame to C# IArrowArrayStream
    /// </summary>
    public static IArrowArrayStream ExportToStream(
        DataFrameHandle dfHandle, 
        ReadOnlySpan<int> columnIndices = default, 
        ulong? seed = null) 
    {
        var cStream = Apache.Arrow.C.CArrowArrayStream.Create();
        int result;

        if (seed.HasValue)
        {
            ulong seedValue = seed.Value;
            result = NativeBindings.pl_dataframe_export_to_stream(
                dfHandle, 
                (Apache.Arrow.C.CArrowArrayStream*)cStream, 
                columnIndices, 
                (nuint)columnIndices.Length,
                &seedValue); 
        }
        else
        {
            result = NativeBindings.pl_dataframe_export_to_stream(
                dfHandle, 
                (Apache.Arrow.C.CArrowArrayStream*)cStream, 
                columnIndices, 
                (nuint)columnIndices.Length,
                null); // 
        }

        ErrorHelper.CheckStatus(result);

        var managedStream = Apache.Arrow.C.CArrowArrayStreamImporter.ImportArrayStream(cStream);
        
        return new SafePolarsExportStream(managedStream, (IntPtr)cStream);
    }

    /// <summary>
    /// Export Polars DataFrame to Native CArrowArrayStream 
    /// </summary>
    public static IntPtr ExportToNativeCStream(
        DataFrameHandle dfHandle, 
        ReadOnlySpan<int> columnIndices = default,
        ulong? seed = null)
    {
        var cStream = Apache.Arrow.C.CArrowArrayStream.Create();
        int result;
        
        if (seed.HasValue)
        {
            ulong seedValue = seed.Value;
            result = NativeBindings.pl_dataframe_export_to_stream(
                dfHandle, 
                (Apache.Arrow.C.CArrowArrayStream*)cStream, 
                columnIndices, 
                (nuint)columnIndices.Length, 
                &seedValue);
        }
        else
        {
            result = NativeBindings.pl_dataframe_export_to_stream(
                dfHandle, 
                (Apache.Arrow.C.CArrowArrayStream*)cStream, 
                columnIndices, 
                (nuint)columnIndices.Length, 
                null);
        }

        if (result != 0)
        {
            Apache.Arrow.C.CArrowArrayStream.Free((Apache.Arrow.C.CArrowArrayStream*)cStream);
            ErrorHelper.CheckStatus(result);
            throw new Exception("Rust FFI failed to export the stream."); 
        }
        
        return (IntPtr)cStream;
    }

    // ------------------------------------------------------------
    // Sink to DataBase
    // ------------------------------------------------------------
    // ------------------------------------------------------------
    // Delegates
    // ------------------------------------------------------------

    // Rust: fn(*mut ArrowArray, *mut ArrowSchema, *mut char) -> i32
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate int SinkCallback(
        CArrowArray* array, 
        CArrowSchema* schema, 
        byte* errorMsg
    );

    // Rust: fn(*mut c_void)
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void CleanupCallback(void* userData);

    // ------------------------------------------------------------
    // Context
    // ------------------------------------------------------------

    /// <summary>
    /// For Delegate lifecycle and delivery User Action
    /// </summary>
    private class SinkContext
    {
        public Action<RecordBatch> UserAction = null!;
        public SinkCallback KeepAliveCallback = null!; 
    }

    // ------------------------------------------------------------
    // Static Factory
    // ------------------------------------------------------------

    /// <summary>
    /// Prepare sink native resource
    /// </summary>
    public static (SinkCallback, CleanupCallback, IntPtr) PrepareSink(Action<RecordBatch> onBatchReceived)
    {
        // Build Context
        var ctx = new SinkContext
        {
            UserAction = onBatchReceived
        };

        // Define Native Callbacl (Pointer -> C# Object)
        ctx.KeepAliveCallback = (arrPtr, schemaPtr, errPtr) =>
        {
            try
            {
                var schema = CArrowSchemaImporter.ImportSchema(schemaPtr);
                var batch = CArrowArrayImporter.ImportRecordBatch(arrPtr, schema);
                ctx.UserAction(batch);
                return 0; // Success
            }
            catch (Exception ex)
            {
                var msgBytes = System.Text.Encoding.UTF8.GetBytes(ex.Message);
                int len = Math.Min(msgBytes.Length, 1023);
                Marshal.Copy(msgBytes, 0, (IntPtr)errPtr, len);
                errPtr[len] = 0;
                return 1; // Error
            }
        };

        // Pack UserData (GCHandle)
        var handle = GCHandle.Alloc(ctx);
        IntPtr userDataPtr = GCHandle.ToIntPtr(handle);

        // Define Cleanup Callback
        static void cleanup(void* ptr)
        {
            var h = GCHandle.FromIntPtr((IntPtr)ptr);
            if (h.IsAllocated) h.Free();
        }

        return (ctx.KeepAliveCallback, cleanup, userDataPtr);
    }

    private class SafeEnumerator<T>(IEnumerator<T> inner) : IEnumerator<T>
    {
        private readonly IEnumerator<T> _inner = inner;

        public T Current => _inner.Current;
        object System.Collections.IEnumerator.Current => _inner.Current!;

        public void Dispose()
        {
            try { _inner.Dispose(); } catch { /* Ignore dispose errors */ }
        }

        public bool MoveNext()
        {
            try
            {
                return _inner.MoveNext();
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"[CRITICAL INTEROP ERROR] Stream iteration failed: {ex}");
                Console.ResetColor();
                return false; 
            }
        }

        public void Reset() => _inner.Reset();
    }
}

internal sealed class SafePolarsExportStream(IArrowArrayStream nativeStream, IntPtr cStream) : IArrowArrayStream
{
    private int _isDisposed = 0;

    public Schema Schema => nativeStream.Schema;

    public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        => nativeStream.ReadNextRecordBatchAsync(cancellationToken);

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) == 0)
        {
            nativeStream.Dispose();
            if (cStream != IntPtr.Zero)
            {
                unsafe { Apache.Arrow.C.CArrowArrayStream.Free((Apache.Arrow.C.CArrowArrayStream*)cStream); }
            }
        }
    }
}

public sealed class ArrowStreamEnumerable(IArrowArrayStream stream) : IEnumerable<RecordBatch>
{
    public IEnumerator<RecordBatch> GetEnumerator() => new ArrowStreamEnumerator(stream);
    System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();

    private sealed class ArrowStreamEnumerator(IArrowArrayStream stream) : IEnumerator<RecordBatch>
    {
        private RecordBatch? _current;

        public RecordBatch Current => _current!;
        
        object System.Collections.IEnumerator.Current => _current!;

        public bool MoveNext()
        {
            _current?.Dispose();
            
            _current = stream.ReadNextRecordBatchAsync().AsTask().GetAwaiter().GetResult();
            
            return _current != null;
        }

        public void Reset() => throw new NotSupportedException("Arrow streams are forward-only.");

        public void Dispose()
        {
            _current?.Dispose();
            stream.Dispose(); 
        }
    }
}