using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using Apache.Arrow.C;
using Apache.Arrow;
using Polars.NET.Core.Exceptions;
using Apache.Arrow.Ipc;
using System.Reflection;
using Polars.NET.Core.Native;
using Polars.NET.Core.Wrappers;
using Kaminari.Sazanami.Arrow.Compute;
using Kaminari.Sazanami.Arrow.Interop;
using CArrowArrayStream = Kaminari.Sazanami.Arrow.Interop.CArrowArrayStream;

namespace Polars.NET.Core.Arrow;
/// <summary>
/// Arrow Stream FFI InterOp Logic
/// </summary>
public static unsafe class ArrowStreamInterop
{
    // Context Object
    private class ScanContext
    {
        public Func<StreamScanRequest, IEnumerable<RecordBatch>> Factory = default!;
        public Schema Schema = default!;
        public List<IntPtr> AllocatedStreams = [];
        /// <summary>What the scan's streams read from (a copy of the input, pinned memory, a store), released with the scan.</summary>
        public IDisposable? Owner;
    }

    /// <summary>
    /// Perpare lazy scan context，then return GCHandle pointed to context.
    /// </summary>
    public static void* CreateScanContext<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(IEnumerable<T> data, int batchSize, Schema schema)
    {
        var context = new ScanContext
        {
            Factory = _ => data.ToArrowBatches(batchSize),
            Schema = schema
        };

        var gcHandle = GCHandle.Alloc(context);
        return (void*)GCHandle.ToIntPtr(gcHandle);
    }

    // ---------------------------------------------------------
    // Static Callback Delegate (for Rust callback)
    // ---------------------------------------------------------

    // Get steramfactory callback function pointer
    public static delegate* unmanaged[Cdecl]<void*, byte**, nint, long, CArrowArrayStream*> GetFactoryCallback()
    {
        return &StreamFactoryCallbackStatic;
    }

    // Get destroy context callback function pointer
    public static delegate* unmanaged[Cdecl]<void*, void> GetDestroyCallback()
    {
        return &DestroyScanContextStatic;
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static CArrowArrayStream* StreamFactoryCallbackStatic(void* userData, byte** columns, nint columnCount, long rowLimit)
    {
        try
        {
            // Resume Context
            var handle = GCHandle.FromIntPtr((IntPtr)userData);
            var context = (ScanContext)handle.Target!;

            // Create: the engine says which columns and how many leading rows the query needs. The stream has
            // exactly the requested columns, whatever the source gives.
            var request = StreamScanRequest.FromNative(columns, columnCount, rowLimit);
            var schema = request.Columns is { } names ? ArrowProjection.Select(context.Schema, names) : context.Schema;
            var enumerable = context.Factory(request);
            var rawEnumerator = enumerable.GetEnumerator();
            var safeEnumerator = new SafeEnumerator<RecordBatch>(rawEnumerator);

            // Alloc C Struct at Heap
            var ptr = (CArrowArrayStream*)Marshal.AllocHGlobal(sizeof(CArrowArrayStream));

            lock(context.AllocatedStreams)
            {
                context.AllocatedStreams.Add((IntPtr)ptr);
            }

            // Init Exporter and export
            var exporter = new ArrowStreamExporter(safeEnumerator, schema)
            {
                SchemaExported = PatchWideIntegers,
                SelectByName = request.Columns is not null,
            };
            exporter.Export(ptr);

            return ptr;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Polars.NET Critical] Error in Stream Factory Callback: {ex}");
            return null;
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
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
                context.Owner?.Dispose();
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
    /// <summary>The engine's names for 128-bit integer columns in a stream's schema, which the engine takes.</summary>
    private static void PatchWideIntegers(Schema schema, nint exported) => WideIntegerFormats.PatchOwnedSchema(schema, (CArrowSchema*)exported);

    public static DataFrameHandle ImportEager(IEnumerable<RecordBatch> stream, Schema schema)
    {
        // Alloc Struct at Stack
        var cStream = new CArrowArrayStream();

        // Init Exporter
        using var enumerator = stream.GetEnumerator();
        using var exporter = new ArrowStreamExporter(enumerator, schema) { SchemaExported = PatchWideIntegers };

        exporter.Export(&cStream);

        // Call Rust
        return PolarsWrapper.DataFrameNewFromStream(&cStream);
    }

    /// <summary>
    /// Eager import of batches in any standard Arrow layout (not just the engine's own, e.g. Utf8 rather than
    /// Utf8View, Time32 rather than Time64[ns]): every column goes through the engine's type conversion.
    /// </summary>
    public static DataFrameHandle ImportEagerStrict(IEnumerable<RecordBatch> stream, Schema schema)
    {
        var cStream = new CArrowArrayStream();
        using var enumerator = stream.GetEnumerator();
        using var exporter = new ArrowStreamExporter(enumerator, schema) { SchemaExported = PatchWideIntegers };
        exporter.Export(&cStream);
        return ErrorHelper.Check(NativeBindings.pl_dataframe_new_from_stream_strict_type(&cStream));
    }

    public static void* CreateDirectScanContext(
        Func<StreamScanRequest, IEnumerable<RecordBatch>> factory,
        Schema schema,
        IDisposable? owner = null)
    {
        var context = new ScanContext
        {
            Factory = factory,
            Schema = schema,
            Owner = owner,
        };

        var gcHandle = GCHandle.Alloc(context);
        return (void*)GCHandle.ToIntPtr(gcHandle);
    }

    /// <summary>
    /// Logic For Lazy Scan
    /// </summary>
    /// <param name="streamFactory">Opens the stream each time the plan runs.</param>
    /// <param name="schema">The stream's schema.</param>
    /// <param name="strict">True when the batches may use any standard Arrow layout (foreign data): every column is
    /// converted by the engine. False only for batches already in the engine's physical layout (Sazanami's ArrowRowBuilder).</param>
    public static LazyFrameHandle ScanStream(
        Func<IEnumerable<RecordBatch>> streamFactory,
        Schema schema,
        bool strict = false)
        => Scan(_ => streamFactory(), schema, buffered: false, strict);

    /// <summary>
    /// A lazy scan whose stream is opened for what the query needs: the columns it reads and the leading rows it
    /// keeps (<see cref="StreamScanRequest"/>), as the engine's optimizer found them. The factory may return more
    /// (any columns of the schema, in any order, and more rows): the engine keeps only what was asked.
    /// </summary>
    /// <param name="streamFactory">Opens the stream for a request each time the plan runs.</param>
    /// <param name="schema">The stream's full schema.</param>
    /// <param name="strict">As for <see cref="ScanStream(Func{IEnumerable{RecordBatch}}, Schema, bool)"/>.</param>
    public static LazyFrameHandle ScanStream(
        Func<StreamScanRequest, IEnumerable<RecordBatch>> streamFactory,
        Schema schema,
        bool strict = false)
        => Scan(streamFactory, schema, buffered: false, strict);

    /// <summary>
    /// As <see cref="ScanStream(Func{StreamScanRequest, IEnumerable{RecordBatch}}, Schema, bool)"/>, with what the streams
    /// read from: <paramref name="owner"/> is disposed once the engine releases the scan (the last frame reading it, and
    /// any query still running on it, done), or now when the scan cannot be made.
    /// </summary>
    public static LazyFrameHandle ScanStream(
        Func<StreamScanRequest, IEnumerable<RecordBatch>> streamFactory,
        Schema schema,
        bool strict,
        IDisposable owner)
        => Scan(streamFactory, schema, buffered: false, strict, owner);

    /// <summary>
    /// Reads the stream now into an anonymous temporary Arrow IPC file and scans that file. The engine owns the file:
    /// it is removed when the last frame that scans it is released. The whole stream is read: the query is not
    /// known yet.
    /// </summary>
    public static LazyFrameHandle ScanStreamBuffered(
        Func<IEnumerable<RecordBatch>> streamFactory,
        Schema schema,
        bool strict = false)
        => Scan(_ => streamFactory(), schema, buffered: true, strict);

    private static LazyFrameHandle Scan(Func<StreamScanRequest, IEnumerable<RecordBatch>> streamFactory, Schema schema, bool buffered, bool strict,
        IDisposable? owner = null)
    {
        if (schema.FieldsList.Count == 0)
        {
            // No columns (an empty input read as an empty frame): no rows either. Apache.Arrow cannot export a schema
            // without fields, and nothing is left to read.
            owner?.Dispose();
            using DataFrameHandle empty = PolarsWrapper.DataFrameNew([]);
            return PolarsWrapper.DataFrameToLazy(empty);
        }
        var cSchema = CArrowSchema.Create();
        try
        {
            try
            {
                CArrowSchemaExporter.ExportSchema(schema, cSchema);
            }
            catch
            {
                owner?.Dispose();
                throw;
            }
            // The engine owns userData from here on and releases it through the destroy callback.
            var userData = CreateDirectScanContext(streamFactory, schema, owner);
            return buffered
                ? PolarsWrapper.LazyFrameScanStreamBuffered(cSchema, GetFactoryCallback(), GetDestroyCallback(), userData, strict)
                : PolarsWrapper.LazyFrameScanStream(cSchema, GetFactoryCallback(), GetDestroyCallback(), userData, strict);
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
        // A stream from ToArrowStream wraps an imported C stream.
        if (stream is SafePolarsExportStream exported)
        {
            stream = exported.Inner;
        }

        // The engine's own stream goes back to it as it is (moved out of the reader: a second import is refused).
        if (stream is ArrowCStreamReader reader)
        {
            var moved = Apache.Arrow.C.CArrowArrayStream.Create();
            try
            {
                reader.MoveTo((CArrowArrayStream*)moved);
                return ErrorHelper.Check(NativeBindings.pl_dataframe_new_from_stream_strict_type((CArrowArrayStream*)moved));
            }
            finally
            {
                Apache.Arrow.C.CArrowArrayStream.Free(moved);
            }
        }

        // Only an imported C stream holds the struct that can be moved out directly. Any other stream is a managed
        // implementation, and reading its object layout would read unrelated memory.
        var type = stream.GetType();
        if (type.GetField("_cArrayStream", BindingFlags.NonPublic | BindingFlags.Instance) is null)
        {
            return ImportManagedStream(stream);
        }

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

    /// <summary>
    /// Imports a managed stream by exporting it through the Arrow C stream interface.
    /// </summary>
    private static DataFrameHandle ImportManagedStream(IArrowArrayStream stream)
    {
        // The engine reads the stream through the exporter, which disposes it (and each batch) when released.
        var cStream = new CArrowArrayStream();
        using var exporter = new ArrowStreamExporter(Batches(stream), stream.Schema) { SchemaExported = PatchWideIntegers };
        exporter.Export(&cStream);
        return ErrorHelper.Check(NativeBindings.pl_dataframe_new_from_stream_strict_type(&cStream));

        static IEnumerator<RecordBatch> Batches(IArrowArrayStream stream)
        {
            try
            {
                while (stream.ReadNextRecordBatchAsync().AsTask().GetAwaiter().GetResult() is { } batch) yield return batch;
            }
            finally
            {
                stream.Dispose();
            }
        }
    }

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
    /// <param name="dfHandle">The frame.</param>
    /// <param name="columnIndices">The columns exported (all when empty).</param>
    /// <param name="seed">A seed for the engine's export.</param>
    /// <param name="wideIntegers">Whether 128-bit integer columns come as decimal(38, 0) columns marked by field
    /// metadata (<see cref="Kaminari.Sazanami.Data.Building.WideIntegers"/>) instead of failing the import.</param>
    public static IArrowArrayStream ExportToStream(
        DataFrameHandle dfHandle,
        ReadOnlySpan<int> columnIndices = default,
        ulong? seed = null,
        bool wideIntegers = false)
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

        // The reader moves the stream out of the struct; SafePolarsExportStream frees the struct. 128-bit integers come
        // as marked decimals when asked (Apache.Arrow has no type for them).
        var managedStream = new ArrowCStreamReader(cStream)
        {
            SchemaImporter = wideIntegers ? static schema => WideIntegerFormats.ImportSchema((CArrowSchema*)schema) : null,
        };

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
                var schema = WideIntegerFormats.ImportSchema(schemaPtr);
                var batch = ArrowCImport.ImportRecordBatch(arrPtr, schema);
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

        // A source's failure goes to the engine as the stream's error (the exporter reports it), never as its end:
        // ending the stream would return a truncated result.
        public bool MoveNext() => _inner.MoveNext();

        public void Reset() => _inner.Reset();
    }
}

internal sealed class SafePolarsExportStream(IArrowArrayStream nativeStream, IntPtr cStream) : IArrowArrayStream
{
    private int _isDisposed = 0;

    internal IArrowArrayStream Inner => nativeStream;

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