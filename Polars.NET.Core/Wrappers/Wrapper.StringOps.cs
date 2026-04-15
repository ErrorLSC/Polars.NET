using System.Runtime.InteropServices;
using System.Text;
using Polars.NET.Core.Native;

namespace Polars.NET.Core;

public readonly partial struct PolarsWrapper
{
    public static ExprHandle StrContains(ExprHandle e, ExprHandle pat,bool strict) 
    {
        var h = NativeBindings.pl_expr_str_contains(e, pat,strict);
        e.TransferOwnership();
        pat.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StrContainsAny(ExprHandle e, ExprHandle pat,bool asciiCaseInsensitive) 
    {
        var h = NativeBindings.pl_expr_str_contains_any(e, pat,asciiCaseInsensitive);
        e.TransferOwnership();
        pat.TransferOwnership();
        return ErrorHelper.Check(h);
    }

    public static ExprHandle StrToUpper(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_str_to_uppercase, e);
    public static ExprHandle StrToLower(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_str_to_lowercase, e);
    public static ExprHandle StrToTitlecase(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_str_to_titlecase, e);
    public static ExprHandle StrLenBytes(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_str_len_bytes, e);
    public static ExprHandle StrLenChars(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_str_len_chars, e); 
    public static ExprHandle StrEscapeRegex(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_str_escape_regex, e); 
    public static ExprHandle StrReverse(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_str_reverse, e); 
    public static ExprHandle StrSlice(ExprHandle e, ExprHandle offset, ExprHandle length)
    {
        var h = NativeBindings.pl_expr_str_slice(e, offset, length);
        e.TransferOwnership();
        offset.TransferOwnership();
        length.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StrSplit(
        ExprHandle expr, 
        ExprHandle by, 
        bool inclusive, 
        bool literal, 
        bool strict)
    {
        ExprHandle h;

        if (!literal)
        {
            if (inclusive)
            {
                h = NativeBindings.pl_expr_str_split_regex_inclusive(expr, by, strict);
            }
            else
            {
                h = NativeBindings.pl_expr_str_split_regex(expr, by, strict);
            }
        }
        else
        {
            if (inclusive)
            {
                h = NativeBindings.pl_expr_str_split_inclusive(expr, by);
            }
            else
            {
                h = NativeBindings.pl_expr_str_split(expr, by);
            }
        }

        expr.TransferOwnership();
        by.TransferOwnership();

        return ErrorHelper.Check(h);
    }
    public static ExprHandle StrSplitN(ExprHandle e, ExprHandle by, int n)
    {
        var h = NativeBindings.pl_expr_str_splitn(e, by, (nuint)n);
        e.TransferOwnership();
        by.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StrSplitExact(
        ExprHandle expr, 
        ExprHandle by, 
        int n,
        bool inclusive)
    {
        ExprHandle h;
        if (inclusive)
        {
            h = NativeBindings.pl_expr_str_split_exact_inclusive(expr, by,(nuint)n);
        }
        else
        {
            h = NativeBindings.pl_expr_str_split_exact(expr, by,(nuint)n);
        }
        expr.TransferOwnership();
        by.TransferOwnership();

        return ErrorHelper.Check(h);
    }
    public static ExprHandle StrFind(
        ExprHandle expr, 
        ExprHandle pat, 
        bool literal, 
        bool strict)
    {
        ExprHandle h;
        if (literal)
        {
            h = NativeBindings.pl_expr_str_find_literal(expr, pat);
        }
        else
        {
            h = NativeBindings.pl_expr_str_find(expr, pat, strict);
        }
        expr.TransferOwnership();
        pat.TransferOwnership();

        return ErrorHelper.Check(h);
    }
    public static ExprHandle StrFindMany(ExprHandle e, ExprHandle pat, bool asciiCaseInsensitive,bool overlapping,bool leftMost)
    {
        var h = NativeBindings.pl_expr_str_find_many(e, pat, asciiCaseInsensitive,overlapping,leftMost);
        e.TransferOwnership();
        pat.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StrCountMatches(ExprHandle e, ExprHandle pat, bool literal)
    {
        var h = NativeBindings.pl_expr_str_count_matches(e, pat, literal);
        e.TransferOwnership();
        pat.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StrReplace(ExprHandle e, ExprHandle pat, ExprHandle val,bool literal,long n)
    {
        var h = NativeBindings.pl_expr_str_replace_n(e, pat, val,literal,n);
        e.TransferOwnership();
        pat.TransferOwnership();
        val.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StrReplaceAll(ExprHandle e, ExprHandle pat, ExprHandle val,bool literal)
    {
        var h = NativeBindings.pl_expr_str_replace_all(e, pat, val,literal);
        e.TransferOwnership();
        pat.TransferOwnership();
        val.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StrReplaceMany(ExprHandle e, ExprHandle pat, ExprHandle val,bool asciiCaseInsensitive,bool leftMost)
    {
        var h = NativeBindings.pl_expr_str_replace_many(e, pat, val,asciiCaseInsensitive,leftMost);
        e.TransferOwnership();
        pat.TransferOwnership();
        val.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StrExtract(ExprHandle e, ExprHandle pat, int groupIndex)
    {
        var h = NativeBindings.pl_expr_str_extract(e, pat, (nuint)groupIndex);
        e.TransferOwnership();
        pat.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StrExtractMany(
        ExprHandle e,
        ExprHandle pat,
        bool asciiCaseInsensitive,
        bool overlapping,
        bool leftMost)
    {
        var h = NativeBindings.pl_expr_str_extract_many(e, pat, asciiCaseInsensitive,overlapping,leftMost);
        e.TransferOwnership();
        pat.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StrExtractAll(ExprHandle e, ExprHandle pat)
        => BinaryOp(NativeBindings.pl_expr_str_extract_all, e, pat);
    public static ExprHandle StrExtractGroups(ExprHandle e, string separator)
    {
        var h = NativeBindings.pl_expr_str_extract_groups(e,separator);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StrStripChars(ExprHandle e, ExprHandle matches)
        => BinaryOp(NativeBindings.pl_expr_str_strip_chars, e, matches);

    public static ExprHandle StrStripCharsStart(ExprHandle e, ExprHandle matches)
        => BinaryOp(NativeBindings.pl_expr_str_strip_chars_start, e, matches);

    public static ExprHandle StrStripCharsEnd(ExprHandle e, ExprHandle matches)
        => BinaryOp(NativeBindings.pl_expr_str_strip_chars_end, e, matches);
    public static ExprHandle StrStripPrefix(ExprHandle e, ExprHandle prefix)
        => BinaryOp(NativeBindings.pl_expr_str_strip_prefix, e, prefix);

    public static ExprHandle StrStripSuffix(ExprHandle e, ExprHandle suffix)
        => BinaryOp(NativeBindings.pl_expr_str_strip_suffix, e, suffix);
    public static ExprHandle StrStartsWith(ExprHandle e, ExprHandle prefix)
        => BinaryOp(NativeBindings.pl_expr_str_starts_with, e, prefix);

    public static ExprHandle StrEndsWith(ExprHandle e, ExprHandle suffix)
        => BinaryOp(NativeBindings.pl_expr_str_ends_with, e, suffix);
    public static ExprHandle StrHead(ExprHandle e, ExprHandle n)
        => BinaryOp(NativeBindings.pl_expr_str_head, e, n);
    public static ExprHandle StrTail(ExprHandle e, ExprHandle n)
        => BinaryOp(NativeBindings.pl_expr_str_tail, e, n);
    public static ExprHandle StrJsonPathMatch(ExprHandle e, ExprHandle n)
        => BinaryOp(NativeBindings.pl_expr_str_json_path_match, e, n);
    public static ExprHandle StrJsonDecode(ExprHandle e, DataTypeExprHandle dtype)
    {
        var h = NativeBindings.pl_expr_str_json_decode(e,dtype);
        e.TransferOwnership();
        dtype.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StrZfill(ExprHandle e, ExprHandle length)
        => BinaryOp(NativeBindings.pl_expr_str_zfill, e, length);
    public static ExprHandle StrPadStart(ExprHandle e, ExprHandle length,string fillChar)
    {
        var h = NativeBindings.pl_expr_str_pad_start(e,length,fillChar);
        e.TransferOwnership();
        length.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StrPadEnd(ExprHandle e, ExprHandle length,string fillChar)
    {
        var h = NativeBindings.pl_expr_str_pad_end(e,length,fillChar);
        e.TransferOwnership();
        length.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StrJoin(ExprHandle e, string delimiter,bool ignoreNulls)
    {
        var h = NativeBindings.pl_expr_str_join(e,delimiter,ignoreNulls);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StrHexDecode(ExprHandle e, bool strict)
    {
        var h = NativeBindings.pl_expr_str_hex_decode(e,strict);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StrBase64Decode(ExprHandle e, bool strict)
    {
        var h = NativeBindings.pl_expr_str_base64_decode(e,strict);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StrHexEncode(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_str_hex_encode,e);
    public static ExprHandle StrBase64Encode(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_str_base64_encode,e);
    public static ExprHandle StrToDate(        
        ExprHandle e,
        string? format,
        [MarshalAs(UnmanagedType.U1)] bool strict,
        [MarshalAs(UnmanagedType.U1)] bool exact,
        [MarshalAs(UnmanagedType.U1)] bool cache)
    {
        var h = NativeBindings.pl_expr_str_to_date(e,format,strict,exact,cache);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StrToTime(        
        ExprHandle e,
        string? format,
        [MarshalAs(UnmanagedType.U1)] bool strict,
        [MarshalAs(UnmanagedType.U1)] bool exact,
        [MarshalAs(UnmanagedType.U1)] bool cache)
    {
        var h = NativeBindings.pl_expr_str_to_time(e,format,strict,exact,cache);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StrToDatetime(        
        ExprHandle e,
        PlTimeUnit unit,
        string? timeZone,
        string? format,
        [MarshalAs(UnmanagedType.U1)] bool strict,
        [MarshalAs(UnmanagedType.U1)] bool exact,
        [MarshalAs(UnmanagedType.U1)] bool cache,
        ExprHandle ambiguous)
    {
        var h = NativeBindings.pl_expr_str_to_datetime(e,unit,timeZone,format,strict,exact,cache,ambiguous);
        e.TransferOwnership();
        ambiguous.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle Strptime(        
        ExprHandle e,
        DataTypeExprHandle dtype,
        string? format,
        [MarshalAs(UnmanagedType.U1)] bool strict,
        [MarshalAs(UnmanagedType.U1)] bool exact,
        [MarshalAs(UnmanagedType.U1)] bool cache,
        ExprHandle ambiguous)
    {
        var h = NativeBindings.pl_expr_strptime(e,dtype,format,strict,exact,cache,ambiguous);
        e.TransferOwnership();
        dtype.TransferOwnership();
        ambiguous.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StrToDecimal(ExprHandle expr,int scale)
    {
        var h = NativeBindings.pl_expr_str_to_decimal(expr,(nuint)scale);
        expr.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StrToInteger(ExprHandle expr,ExprHandle baseExpr,DataTypeHandle dtype,bool strict)
    {
        var h = NativeBindings.pl_expr_str_to_integer(expr,baseExpr,dtype,strict);
        expr.TransferOwnership();
        baseExpr.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StrNormalize(ExprHandle expr,NormalizationForm form)
    {
        var h = NativeBindings.pl_expr_str_normalize(expr,form);
        expr.TransferOwnership();
        return ErrorHelper.Check(h);
    }

}