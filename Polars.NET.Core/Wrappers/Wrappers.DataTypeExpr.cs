using System.ComponentModel.DataAnnotations;
using System.Runtime.InteropServices;
using Polars.NET.Core.Native;

namespace Polars.NET.Core;

public readonly partial struct PolarsWrapper
{
    private static DataTypeExprHandle UnaryOpToDataTypeExpr(Func<DataTypeExprHandle, DataTypeExprHandle> op, DataTypeExprHandle dexpr)
    {
        var h = op(dexpr);
        dexpr.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    private static ExprHandle UnaryOpToExpr(Func<DataTypeExprHandle, ExprHandle> op, DataTypeExprHandle dexpr)
    {
        var h = op(dexpr);
        dexpr.TransferOwnership();
        return ErrorHelper.Check(h);
    }

    public static DataTypeExprHandle DataTypeExprClone(DataTypeExprHandle dexpr) => UnaryOpToDataTypeExpr(NativeBindings.pl_datatype_expr_clone,dexpr);
    public static DataTypeHandle DataTypeExprIntoDataType(DataTypeExprHandle dexpr,SchemaHandle schema)
    {
        var h = NativeBindings.pl_datatype_expr_into_datatype(dexpr,schema);
        dexpr.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static DataTypeExprHandle DataTypeExprDtypeOf(ExprHandle expr)
    {
        var h = NativeBindings.pl_datatype_expr_dtype_of(expr);
        expr.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static DataTypeExprHandle DataTypeExprSelfDtype()
    {
        var h = NativeBindings.pl_datatype_expr_self_dtype();
        return ErrorHelper.Check(h);
    }
    public static DataTypeExprHandle DataTypeExprFromDataType(DataTypeHandle datatype)
    {
        var h = NativeBindings.pl_datatype_expr_from_datatype(datatype);
        return ErrorHelper.Check(h);
    }
    public static DataTypeHandle DataTypeExprIntoLiteral(DataTypeExprHandle dexpr) 
    {
        var h = NativeBindings.pl_datatype_expr_into_literal(dexpr);
        
        dexpr.TransferOwnership();
        ErrorHelper.CheckVoid();
        return h;
    }
    public static DataTypeExprHandle DataTypeExprInnerDtype(DataTypeExprHandle dexpr) => UnaryOpToDataTypeExpr(NativeBindings.pl_datatype_expr_inner_dtype,dexpr);
    public static ExprHandle DataTypeExprEquals(DataTypeExprHandle left,DataTypeExprHandle right)
    {
        var h = NativeBindings.pl_datatype_expr_equals(left,right);
        left.TransferOwnership();
        right.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle DataTypeExprDisplay(DataTypeExprHandle dexpr) => UnaryOpToExpr(NativeBindings.pl_datatype_expr_display,dexpr);
    public static ExprHandle DataTypeExprDefaultValue(DataTypeExprHandle dexpr,int n,bool numericToOne,int numListValues)
    {
        var h = NativeBindings.pl_datatype_expr_default_value(dexpr, (nuint)n, numericToOne, (nuint)numListValues);
        dexpr.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle DataTypeExprMatches(DataTypeExprHandle dexpr, SelectorHandle selector)
    {
        var h = NativeBindings.pl_datatype_expr_matches(dexpr,selector);
        dexpr.TransferOwnership();
        selector.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static DataTypeExprHandle DataTypeExprWrapInList(DataTypeExprHandle dexpr) => UnaryOpToDataTypeExpr(NativeBindings.pl_datatype_expr_wrap_in_list,dexpr);
    public static DataTypeExprHandle DataTypeExprWrapInArray(DataTypeExprHandle dexpr,int width)
    {
        var h = NativeBindings.pl_datatype_expr_wrap_in_array(dexpr,(nuint)width);
        dexpr.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static DataTypeExprHandle DataTypeExprIntToUnsigned(DataTypeExprHandle dexpr) => UnaryOpToDataTypeExpr(NativeBindings.pl_datatype_expr_int_to_unsigned,dexpr);
    public static DataTypeExprHandle DataTypeExprIntToSigned(DataTypeExprHandle dexpr) => UnaryOpToDataTypeExpr(NativeBindings.pl_datatype_expr_int_to_signed,dexpr);
    public static DataTypeExprHandle DataTypeExprListInnerDtype(DataTypeExprHandle dexpr) => UnaryOpToDataTypeExpr(NativeBindings.pl_datatype_expr_list_inner_dtype,dexpr);
    public static DataTypeExprHandle DataTypeExprArrayInnerDtype(DataTypeExprHandle dexpr) => UnaryOpToDataTypeExpr(NativeBindings.pl_datatype_expr_arr_inner_dtype,dexpr);
    public static ExprHandle DataTypeExprArrayWidth(DataTypeExprHandle dexpr) => UnaryOpToExpr(NativeBindings.pl_datatype_expr_arr_width,dexpr);
    public static ExprHandle DataTypeExprArrayShape(DataTypeExprHandle dexpr) => UnaryOpToExpr(NativeBindings.pl_datatype_expr_arr_shape,dexpr);  
    public static ExprHandle DataTypeExprStructFieldNames(DataTypeExprHandle dexpr) => UnaryOpToExpr(NativeBindings.pl_datatype_expr_struct_field_names,dexpr);  
    public static DataTypeExprHandle DataTypeExprStructFieldDtypeByName(DataTypeExprHandle dexpr,string name)
    {
        var h = NativeBindings.pl_datatype_expr_struct_field_dtype_by_name(dexpr,name);
        dexpr.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static DataTypeExprHandle DataTypeExprStructFieldDtypeByIndex(DataTypeExprHandle dexpr,long index)
    {
        var h = NativeBindings.pl_datatype_expr_struct_field_dtype_by_index(dexpr,index);
        dexpr.TransferOwnership();
        return ErrorHelper.Check(h);
    }

}