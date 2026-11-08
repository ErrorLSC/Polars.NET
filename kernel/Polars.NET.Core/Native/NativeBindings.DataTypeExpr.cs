using System.Runtime.InteropServices;

namespace Polars.NET.Core.Native;

internal partial class NativeBindings
{
    [LibraryImport(LibName)] public static partial void pl_datatype_expr_free(IntPtr ptr);
    [LibraryImport(LibName)] public static partial DataTypeExprHandle pl_datatype_expr_clone(DataTypeExprHandle dexpr);
    [LibraryImport(LibName)] public static partial DataTypeExprHandle pl_datatype_expr_dtype_of(ExprHandle expr);
    [LibraryImport(LibName)] public static partial DataTypeExprHandle pl_datatype_expr_self_dtype();
    [LibraryImport(LibName)] public static partial DataTypeExprHandle pl_datatype_expr_from_datatype(DataTypeHandle datatype);
    [LibraryImport(LibName)] public static partial DataTypeHandle pl_datatype_expr_into_datatype(DataTypeExprHandle dexpr,SchemaHandle schema);
    [LibraryImport(LibName)] public static partial DataTypeHandle pl_datatype_expr_into_literal(DataTypeExprHandle dexpr);
    [LibraryImport(LibName)] public static partial DataTypeExprHandle pl_datatype_expr_inner_dtype(DataTypeExprHandle dexpr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_datatype_expr_equals(DataTypeExprHandle left,DataTypeExprHandle right);
    [LibraryImport(LibName)] public static partial ExprHandle pl_datatype_expr_display(DataTypeExprHandle dexpr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_datatype_expr_default_value(DataTypeExprHandle dexpr,nuint n,[MarshalAs(UnmanagedType.U1)] bool numericToOne,nuint numListValues);
    [LibraryImport(LibName)] public static partial ExprHandle pl_datatype_expr_matches(DataTypeExprHandle dexpr,SelectorHandle selector);
    [LibraryImport(LibName)] public static partial DataTypeExprHandle pl_datatype_expr_wrap_in_list(DataTypeExprHandle dexpr);
    [LibraryImport(LibName)] public static partial DataTypeExprHandle pl_datatype_expr_wrap_in_array(DataTypeExprHandle dexpr,nuint width);
    [LibraryImport(LibName)] public static partial DataTypeExprHandle pl_datatype_expr_int_to_unsigned(DataTypeExprHandle dexpr);
    [LibraryImport(LibName)] public static partial DataTypeExprHandle pl_datatype_expr_int_to_signed(DataTypeExprHandle dexpr);
    [LibraryImport(LibName)] public static partial DataTypeExprHandle pl_datatype_expr_list_inner_dtype(DataTypeExprHandle dexpr);
    [LibraryImport(LibName)] public static partial DataTypeExprHandle pl_datatype_expr_arr_inner_dtype(DataTypeExprHandle dexpr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_datatype_expr_arr_width(DataTypeExprHandle dexpr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_datatype_expr_arr_shape(DataTypeExprHandle dexpr);
    [LibraryImport(LibName)] public static partial DataTypeExprHandle pl_datatype_expr_struct_field_dtype_by_index(DataTypeExprHandle dexpr,long index);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataTypeExprHandle pl_datatype_expr_struct_field_dtype_by_name(DataTypeExprHandle dexpr,string name);
    [LibraryImport(LibName)] public static partial ExprHandle pl_datatype_expr_struct_field_names(DataTypeExprHandle dexpr);
}