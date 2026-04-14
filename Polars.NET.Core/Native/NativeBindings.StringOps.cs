using System.Runtime.InteropServices;
using System.Text;

namespace Polars.NET.Core.Native;

internal partial class NativeBindings
{
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_concat_str(IntPtr[] exprs,nuint exprLen, string separator, [MarshalAs(UnmanagedType.U1)] bool ignoreNulls);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_format_str(string format,IntPtr[] exprs,UIntPtr exprLen);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_str_contains(
        ExprHandle expr, 
        ExprHandle pat,
        [MarshalAs(UnmanagedType.U1)]bool strict
    );
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_str_contains_any(
        ExprHandle expr, 
        ExprHandle pat,
        [MarshalAs(UnmanagedType.U1)]bool asciiCaseInsensitive
    );

    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_str_to_uppercase(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_str_to_lowercase(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_str_to_titlecase(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_str_len_bytes(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_str_len_chars(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_str_escape_regex(ExprHandle expr);
    // String Cleaning
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_str_strip_chars(ExprHandle e, ExprHandle matches);

    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_str_strip_chars_start(ExprHandle e, ExprHandle matches);

    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_str_strip_chars_end(ExprHandle e, ExprHandle matches);

    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_str_strip_prefix(ExprHandle e, ExprHandle prefix);

    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_str_strip_suffix(ExprHandle e, ExprHandle suffix);

    // Anchors
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_str_starts_with(ExprHandle e, ExprHandle prefix);

    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_str_ends_with(ExprHandle e, ExprHandle suffix);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_str_head(ExprHandle e, ExprHandle n);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_str_tail(ExprHandle e, ExprHandle n);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_str_json_path_match(ExprHandle e, ExprHandle pat);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_str_json_decode(ExprHandle e, DataTypeExprHandle dtype);
    // Parsing
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_str_to_date(        
        ExprHandle e,
        string? format,
        [MarshalAs(UnmanagedType.U1)] bool strict,
        [MarshalAs(UnmanagedType.U1)] bool exact,
        [MarshalAs(UnmanagedType.U1)] bool cache
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_str_to_time(        
        ExprHandle e,
        string? format,
        [MarshalAs(UnmanagedType.U1)] bool strict,
        [MarshalAs(UnmanagedType.U1)] bool exact,
        [MarshalAs(UnmanagedType.U1)] bool cache
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_str_to_datetime(
        ExprHandle e,
        PlTimeUnit unit,
        string? timeZone,
        string? format,
        [MarshalAs(UnmanagedType.U1)] bool strict,
        [MarshalAs(UnmanagedType.U1)] bool exact,
        [MarshalAs(UnmanagedType.U1)] bool cache,
        ExprHandle ambiguous
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_strptime(
        ExprHandle e,
        DataTypeExprHandle dtype,
        string? format,
        [MarshalAs(UnmanagedType.U1)] bool strict,
        [MarshalAs(UnmanagedType.U1)] bool exact,
        [MarshalAs(UnmanagedType.U1)] bool cache,
        ExprHandle ambiguous
    );
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_str_to_decimal(ExprHandle expr, nuint scale);
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_str_to_integer(
        ExprHandle expr, 
        ExprHandle baseExpr,
        DataTypeHandle dtype,
        [MarshalAs(UnmanagedType.U1)] bool strict
    );
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_str_normalize(ExprHandle expr, NormalizationForm form);
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_str_slice(ExprHandle expr, ExprHandle offset, ExprHandle length);
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_str_split(ExprHandle expr,  ExprHandle by);
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_str_split_inclusive(ExprHandle expr,  ExprHandle by);
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_str_split_exact(ExprHandle expr,  ExprHandle by, nuint n);
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_str_split_exact_inclusive(ExprHandle expr,  ExprHandle by, nuint n);
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_str_splitn(ExprHandle expr,  ExprHandle by, nuint n);
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_str_split_regex(ExprHandle expr,  ExprHandle by,[MarshalAs(UnmanagedType.U1)] bool strict);
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_str_split_regex_inclusive(ExprHandle expr,  ExprHandle by, [MarshalAs(UnmanagedType.U1)]bool strict);

    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_str_replace_all(
        ExprHandle expr, 
        ExprHandle pat, 
        ExprHandle val,
        [MarshalAs(UnmanagedType.U1)] bool literal
    );
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_str_replace_n(
        ExprHandle expr, 
        ExprHandle pat, 
        ExprHandle val,
        [MarshalAs(UnmanagedType.U1)] bool literal,
        long n
    );
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_str_replace_many(
        ExprHandle expr, 
        ExprHandle pat, 
        ExprHandle val,
        [MarshalAs(UnmanagedType.U1)] bool asciiCaseInsensitive,
        [MarshalAs(UnmanagedType.U1)] bool leftMost
    );
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_str_extract(
        ExprHandle expr, 
        ExprHandle pat, 
        nuint groupIndex
    );
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_str_extract_many(
        ExprHandle e,
        ExprHandle pat,
        [MarshalAs(UnmanagedType.U1)] bool asciiCaseInsensitive,
        [MarshalAs(UnmanagedType.U1)] bool overlapping,
        [MarshalAs(UnmanagedType.U1)] bool leftMost
    );
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_str_extract_all(ExprHandle expr,  ExprHandle pat);
    [LibraryImport(LibName,StringMarshalling =StringMarshalling.Utf8)] 
    public static partial ExprHandle pl_expr_str_extract_groups(ExprHandle expr,string separator);
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_str_find(
        ExprHandle expr, 
        ExprHandle pat, 
        [MarshalAs(UnmanagedType.U1)]bool strict
    );
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_str_find_many(
        ExprHandle e,
        ExprHandle pat,
        [MarshalAs(UnmanagedType.U1)] bool asciiCaseInsensitive,
        [MarshalAs(UnmanagedType.U1)] bool overlapping,
        [MarshalAs(UnmanagedType.U1)] bool leftMost
    );
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_str_find_literal(ExprHandle expr,  ExprHandle pat);
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_str_count_matches(
        ExprHandle expr, 
        ExprHandle pat, 
        [MarshalAs(UnmanagedType.U1)]bool literal
    );

    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_str_zfill(ExprHandle e, ExprHandle length);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_str_pad_start(ExprHandle e, ExprHandle length, string fillChar);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_str_pad_end(ExprHandle e, ExprHandle length, string fillChar);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_str_join(ExprHandle e, string separator,[MarshalAs(UnmanagedType.U1)] bool ignoreNulls);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_str_hex_decode(ExprHandle e,[MarshalAs(UnmanagedType.U1)] bool strict);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_str_base64_decode(ExprHandle e,[MarshalAs(UnmanagedType.U1)] bool strict);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_str_base64_encode(ExprHandle e);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_str_hex_encode(ExprHandle e);

}