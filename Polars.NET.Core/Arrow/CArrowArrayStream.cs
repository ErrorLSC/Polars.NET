using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.C;

namespace Polars.NET.Core.Arrow;
// C Struct: ArrowArrayStream
[StructLayout(LayoutKind.Sequential)]
public unsafe struct CArrowArrayStream
{
    // Define delegate
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate int get_schema_delegate(CArrowArrayStream* stream, CArrowSchema* outSchema);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate int get_next_delegate(CArrowArrayStream* stream, CArrowArray* outArray);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate byte* get_last_error_delegate(CArrowArrayStream* stream);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void release_delegate(CArrowArrayStream* stream);
    // Callback for getting schema
    public delegate* unmanaged[Cdecl]<CArrowArrayStream*, CArrowSchema*, int> get_schema;
    
    // Callback for next batch
    public delegate* unmanaged[Cdecl]<CArrowArrayStream*, CArrowArray*, int> get_next;
    
    // Callback for last error message
    public delegate* unmanaged[Cdecl]<CArrowArrayStream*, byte*> get_last_error;
    
    // Callback for releasing stream
    public delegate* unmanaged[Cdecl]<CArrowArrayStream*, void> release;
    
    // Private data pointer
    public void* private_data;
}
public unsafe class ArrowStreamExporter : IDisposable
{
    private readonly IEnumerator<RecordBatch> _enumerator;
    private readonly Schema _schema;
    private bool _isDisposed;

    public ArrowStreamExporter(IEnumerator<RecordBatch> enumerator, Schema schema)
    {
        _enumerator = enumerator;
        _schema = schema;
    }

    // Export as C pointer
    public void Export(CArrowArrayStream* outStream)
    {
        outStream->get_schema = &GetSchemaStatic;
        outStream->get_next = &GetNextStatic;
        outStream->get_last_error = &GetLastErrorStatic;
        outStream->release = &ReleaseStatic;
        // GCHandle let GC not move or recycle data
        outStream->private_data = (void*)GCHandle.ToIntPtr(GCHandle.Alloc(this));
    }

    // --- Static Callbacks ---

    // Set callconvs as Cdecl for Arrow C Stream Interface requirement
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
                CArrowArrayExporter.ExportRecordBatch(batch, outArray);
            }
            else
            {
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
        // No error message, need more work
        return null; 
    }

    [UnmanagedCallersOnly(CallConvs = new[] { typeof(CallConvCdecl) })]
    private static void ReleaseStatic(CArrowArrayStream* stream)
    {
        var ptr = (IntPtr)stream->private_data;
        if (ptr != IntPtr.Zero)
        {
            // Restore GCHandle and release
            var handle = GCHandle.FromIntPtr(ptr);
            if (handle.IsAllocated)
            {
                var exporter = (ArrowStreamExporter)handle.Target!;
                exporter.Dispose();
                handle.Free(); // Release GCHandle，allow Exporter GC 
            }
            // stream->private_data = null;
        }
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
        GC.SuppressFinalize(this);
    }
}
