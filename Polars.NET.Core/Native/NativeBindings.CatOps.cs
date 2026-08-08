using System.Runtime.InteropServices;

namespace Polars.NET.Core.Native;

internal partial class NativeBindings
{
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_cat_get_categories(ExprHandle expr);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_cat_len_bytes(ExprHandle expr);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_cat_len_chars(ExprHandle expr);
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_cat_starts_with(ExprHandle expr,string prefix);
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_cat_ends_with(ExprHandle expr,string suffix);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_cat_slice(
        ExprHandle expr,
        long offset,
        [MarshalAs(UnmanagedType.U1)] bool hasLength,
        nuint length);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_cat_physical(ExprHandle expr);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_cat_to(
        ExprHandle expr,
        DataTypeExprHandle dtypeExpr,
        [MarshalAs(UnmanagedType.U1)]bool strict);

}