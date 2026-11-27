using System.Runtime.InteropServices;

namespace Polars.NET.Core.Native;

internal unsafe partial class NativeBindings
{
    [LibraryImport(LibName)]
    public static partial void pl_io_plugin_register(byte* package, nuint packageLength, byte* id, nuint idLength, void* contract);

    [LibraryImport(LibName)]
    public static partial LazyFrameHandle pl_lazy_frame_scan_io_plugin(byte* package, nuint packageLength, void* handle);

    [LibraryImport(LibName)]
    public static partial nint pl_io_plugin_layout();
}
