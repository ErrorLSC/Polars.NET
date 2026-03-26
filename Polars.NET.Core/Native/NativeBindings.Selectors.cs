using System.Runtime.InteropServices;

namespace Polars.NET.Core.Native;

unsafe internal partial class NativeBindings
{
    // =================================================================
    // Selectors
    // =================================================================
    [LibraryImport(LibName)] public static partial void pl_selector_free(IntPtr ptr);
    [LibraryImport(LibName)] 
    public static partial SelectorHandle pl_selector_clone(SelectorHandle sel);
    // Selectors
    [LibraryImport(LibName)] public static partial SelectorHandle pl_selector_all();
    [LibraryImport(LibName)]
    public static partial SelectorHandle pl_selector_cols(
        IntPtr[] names,
        UIntPtr len
    );
    [LibraryImport(LibName)] 
    public static partial SelectorHandle pl_selector_exclude(
        SelectorHandle sel, 
        IntPtr[] names,
        UIntPtr len
    );

    // String Matchers
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)]
    public static partial SelectorHandle pl_selector_starts_with(string pattern);

    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)]
    public static partial SelectorHandle pl_selector_ends_with(string pattern);

    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)]
    public static partial SelectorHandle pl_selector_contains(string pattern);

    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)]
    public static partial SelectorHandle pl_selector_match(string pattern);

    // Type Selectors
    [LibraryImport(LibName)]
    public static partial SelectorHandle pl_selector_by_dtype(int kind);

    [LibraryImport(LibName)]
    public static partial SelectorHandle pl_selector_numeric();

    // Set Operations
    [LibraryImport(LibName)]
    public static partial SelectorHandle pl_selector_and(SelectorHandle left, SelectorHandle right);

    [LibraryImport(LibName)]
    public static partial SelectorHandle pl_selector_or(SelectorHandle left, SelectorHandle right);

    [LibraryImport(LibName)]
    public static partial SelectorHandle pl_selector_not(SelectorHandle sel);
    // Bridges
    [LibraryImport(LibName)] public static partial ExprHandle pl_selector_into_expr(SelectorHandle sel);

}