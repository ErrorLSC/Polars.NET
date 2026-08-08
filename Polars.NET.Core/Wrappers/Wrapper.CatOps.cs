using Polars.NET.Core.Native;

namespace Polars.NET.Core;
public readonly partial struct PolarsWrapper
{
    public static ExprHandle CatGetCategories(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_cat_get_categories, e);
    public static ExprHandle CatLenBytes(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_cat_len_bytes, e);
    public static ExprHandle CatLenChars(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_cat_len_chars, e);
    public static ExprHandle CatPhysical(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_cat_physical, e);
    public static ExprHandle CatStartsWith(ExprHandle e, string prefix)
    {
        var h = NativeBindings.pl_expr_cat_starts_with(e,prefix);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle CatEndsWith(ExprHandle e, string suffix)
    {
        var h = NativeBindings.pl_expr_cat_ends_with(e,suffix);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle CatSlice(ExprHandle e, long offset, nuint? length)
    {
        var h = NativeBindings.pl_expr_cat_slice(
            e, 
            offset, 
            length.HasValue, 
            length.GetValueOrDefault()
        );
        
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle CatTo(ExprHandle e, DataTypeExprHandle dtype,bool strict)
    {
        var h = NativeBindings.pl_expr_cat_to(e,dtype,strict);
        e.TransferOwnership();
        dtype.TransferOwnership();
        return ErrorHelper.Check(h);
    }
}