using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.C;

namespace Polars.NET.Core.Arrow;

// C Struct: ArrowArrayStream 
[StructLayout(LayoutKind.Sequential)]
public unsafe struct CArrowArrayStream
{
    public delegate* unmanaged[Cdecl]<CArrowArrayStream*, CArrowSchema*, int> get_schema;
    public delegate* unmanaged[Cdecl]<CArrowArrayStream*, CArrowArray*, int> get_next;
    public delegate* unmanaged[Cdecl]<CArrowArrayStream*, byte*> get_last_error;
    public delegate* unmanaged[Cdecl]<CArrowArrayStream*, void> release;
    public void* private_data;
}

public unsafe class ArrowStreamExporter(IEnumerator<RecordBatch> enumerator, Schema schema) : IDisposable
{
    private readonly IEnumerator<RecordBatch> _enumerator = enumerator;
    private readonly Schema _schema = schema;
    private bool _isDisposed;
    
    internal IntPtr _lastErrorPointer = IntPtr.Zero;

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

    internal void SetLastError(string message)
    {
        if (_lastErrorPointer != IntPtr.Zero)
        {
            Marshal.FreeCoTaskMem(_lastErrorPointer);
        }
        _lastErrorPointer = Marshal.StringToCoTaskMemUTF8(message);
    }

    // --- Static Callbacks ---

    [UnmanagedCallersOnly(CallConvs = new[] { typeof(CallConvCdecl) })]
    private static int GetSchemaStatic(CArrowArrayStream* stream, CArrowSchema* outSchema)
    {
        try
        {
            var exporter = GetExporter(stream);
            CArrowSchemaExporter.ExportSchema(exporter._schema, outSchema);
            return 0; // Success
        }
        catch (Exception ex)
        {
            SafelySetLastError(stream, $"GetSchema Error: {ex.Message}");
            return 5; 
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
                // Indicate End of Stream
                *outArray = default; 
            }
            return 0;
        }
        catch (Exception ex)
        {
            SafelySetLastError(stream, $"GetNext Error: {ex.Message}");
            return 5; 
        }
    }

    [UnmanagedCallersOnly(CallConvs = new[] { typeof(CallConvCdecl) })]
    private static byte* GetLastErrorStatic(CArrowArrayStream* stream)
    {
        if (stream == null || stream->private_data == null) return null;

        try
        {
            var handle = GCHandle.FromIntPtr((IntPtr)stream->private_data);
            
            if (handle.Target is ArrowStreamExporter exporter)
            {
                return (byte*)exporter._lastErrorPointer;
            }
        }
        catch 
        { 
            
        }
        
        return null; 
    }

    [UnmanagedCallersOnly(CallConvs = new[] { typeof(CallConvCdecl) })]
    private static void ReleaseStatic(CArrowArrayStream* stream)
    {
        if (stream == null) return;

        var ptr = (IntPtr)stream->private_data;
        if (ptr != IntPtr.Zero)
        {
            var handle = GCHandle.FromIntPtr(ptr);
            if (handle.IsAllocated)
            {
                var exporter = (ArrowStreamExporter)handle.Target!;
                exporter.Dispose();
                handle.Free(); 
            }
            stream->private_data = null;
        }
        
        stream->release = null; 
    }

    // --- Helper Methods ---

    private static ArrowStreamExporter GetExporter(CArrowArrayStream* stream)
    {
        var handle = GCHandle.FromIntPtr((IntPtr)stream->private_data);
        return (ArrowStreamExporter)handle.Target!;
    }

    private static void SafelySetLastError(CArrowArrayStream* stream, string message)
    {
        if (stream == null || stream->private_data == null) return;
        try
        {
            var handle = GCHandle.FromIntPtr((IntPtr)stream->private_data);
            if (handle.Target is ArrowStreamExporter exp)
            {
                exp.SetLastError(message);
            }
        }
        catch { }
    }

    public void Dispose()
    {
        if (_isDisposed) return;

        if (_lastErrorPointer != IntPtr.Zero)
        {
            Marshal.FreeCoTaskMem(_lastErrorPointer);
            _lastErrorPointer = IntPtr.Zero;
        }
        
        _enumerator.Dispose();
        _isDisposed = true;
        
        GC.SuppressFinalize(this);
    }
}