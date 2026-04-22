using System.Runtime.InteropServices;

namespace Polars.NET.Core.Native;
internal partial class NativeBindings
{
    [LibraryImport(LibName)] public static partial void pl_categories_free(nint categories);
    [LibraryImport(LibName,StringMarshalling =StringMarshalling.Utf8)]
    public static partial CategoriesHandle pl_categories_new(
        string? name,
        string? nameSpace,
        PlCategoricalPhysical code
    );
    [LibraryImport(LibName,StringMarshalling =StringMarshalling.Utf8)]
    public static partial CategoriesHandle pl_categories_random(
        string nameSpace,
        PlCategoricalPhysical code
    );
    [LibraryImport(LibName)]
    public static partial CategoriesHandle pl_categories_global();
    [LibraryImport(LibName)]
    public static partial int pl_categories_get_name(
        CategoriesHandle categories,
        out nint name
    );
    [LibraryImport(LibName)]
    public static partial int pl_categories_is_global(
        CategoriesHandle categories,
        [MarshalAs(UnmanagedType.U1)]out bool isGlobal
    );
    [LibraryImport(LibName)]
    public static partial int pl_categories_get_namespace(
        CategoriesHandle categories,
        out nint name
    );
    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.U1)]
    public static partial bool pl_categories_hash(
        CategoriesHandle categories,
        out ulong hash
    );
    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.U1)]
    public static partial bool pl_categories_physical(
        CategoriesHandle categories,
        out PlCategoricalPhysical physical
    );
    [LibraryImport(LibName)]
    public static partial FrozenCategoriesHandle pl_categories_freeze(CategoriesHandle categories);
    [LibraryImport(LibName)]
    public static partial void pl_frozencategories_free(nint ptr);
    [LibraryImport(LibName,StringMarshalling =StringMarshalling.Utf8)]
    public static partial FrozenCategoriesHandle pl_frozencategories_new(
        string[] enums,
        nuint len
    );
    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.U1)]
    public static partial bool pl_frozencategories_hash(
        FrozenCategoriesHandle categories,
        out ulong hash
    );
    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_frozencategories_get_categories(
        FrozenCategoriesHandle categories
    );
    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.U1)]
    public static partial bool pl_frozencategories_physical(
        FrozenCategoriesHandle categories,
        out PlCategoricalPhysical physical
    );
}