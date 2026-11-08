using System.Runtime.InteropServices;

namespace Polars.NET.Core.Native;

internal partial class NativeBindings
{
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_bin_size_bytes(ExprHandle expr);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_bin_contains(ExprHandle expr,ExprHandle pat);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_bin_ends_with(ExprHandle expr,ExprHandle sub);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_bin_starts_with(ExprHandle expr,ExprHandle sub);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_bin_head(ExprHandle expr,ExprHandle n);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_bin_tail(ExprHandle expr,ExprHandle n);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_bin_hex_decode(ExprHandle expr,[MarshalAs(UnmanagedType.U1)]bool strict);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_bin_base64_decode(ExprHandle expr,[MarshalAs(UnmanagedType.U1)]bool strict);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_bin_hex_encode(ExprHandle expr);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_bin_base64_encode(ExprHandle expr);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_bin_reinterpret(ExprHandle expr,DataTypeExprHandle dtype,[MarshalAs(UnmanagedType.U1)]bool isLittleEndian);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_bin_get(ExprHandle expr,ExprHandle index,[MarshalAs(UnmanagedType.U1)]bool nullOnOob);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_bin_slice(ExprHandle expr,ExprHandle offset,ExprHandle length);
}