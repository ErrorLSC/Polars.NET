using Polars.NET.Core.Native;

namespace Polars.NET.Core;
public readonly partial struct PolarsWrapper
{
    public static ExprHandle BinSizeBytes(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_bin_size_bytes, e);
    public static ExprHandle BinContains(ExprHandle e,ExprHandle literal) => BinaryOp(NativeBindings.pl_expr_bin_contains,e,literal);
    public static ExprHandle BinEndsWith(ExprHandle e,ExprHandle sub) => BinaryOp(NativeBindings.pl_expr_bin_ends_with,e,sub);
    public static ExprHandle BinStartsWith(ExprHandle e,ExprHandle sub) => BinaryOp(NativeBindings.pl_expr_bin_starts_with,e,sub);
    public static ExprHandle BinHead(ExprHandle e,ExprHandle n) => BinaryOp(NativeBindings.pl_expr_bin_head,e,n);
    public static ExprHandle BinTail(ExprHandle e,ExprHandle n) => BinaryOp(NativeBindings.pl_expr_bin_tail,e,n);
    public static ExprHandle BinHexDecode(ExprHandle e, bool strict)
    {
        var h = NativeBindings.pl_expr_bin_hex_decode(e,strict);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle BinBase64Decode(ExprHandle e, bool strict)
    {
        var h = NativeBindings.pl_expr_bin_base64_decode(e,strict);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle BinHexEncode(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_bin_hex_encode,e);
    public static ExprHandle BinBase64Encode(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_bin_base64_encode,e);
    public static ExprHandle BinReinterpret(ExprHandle e, DataTypeExprHandle dtype,bool isLittleEndian)
    {
        var h = NativeBindings.pl_expr_bin_reinterpret(e,dtype,isLittleEndian);
        e.TransferOwnership();
        dtype.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle BinSlice(ExprHandle e, ExprHandle offset,ExprHandle length)
    {
        var h = NativeBindings.pl_expr_bin_slice(e,offset,length);
        e.TransferOwnership();
        offset.TransferOwnership();
        length.TransferOwnership();
        return ErrorHelper.Check(h);
    }

}