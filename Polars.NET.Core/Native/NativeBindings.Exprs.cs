using System.Runtime.InteropServices;
using Apache.Arrow.C;

namespace Polars.NET.Core.Native;

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
public delegate void CleanupCallback(IntPtr userData);

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
public unsafe delegate int UdfCallback(
    CArrowArray* inArray, 
    CArrowSchema* inSchema, 
    CArrowArray* outArray, 
    CArrowSchema* outSchema,
    byte* msgBuf
);

unsafe internal partial class NativeBindings
{
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_set_env_var(
    string key, 
    string value);
    [LibraryImport(LibName)] public static partial void pl_expr_free(IntPtr ptr);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_concat_expr(IntPtr[] exprs,UIntPtr exprLen, [MarshalAs(UnmanagedType.U1)] bool rechunk);
    // String Free
    [LibraryImport(LibName)] public static partial void pl_free_string(IntPtr ptr);
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)] 
    public static partial ExprHandle pl_expr_sql(string query);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)] 
    public static partial ExprHandle pl_expr_col(string name);
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_cols(IntPtr[] names, UIntPtr len);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_lit_i8(sbyte val);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_lit_u8(byte val);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_lit_i16(short val);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_lit_u16(ushort val);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_lit_i32(int val);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_lit_u32(uint val);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_lit_bool([MarshalAs(UnmanagedType.I1)] bool val);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_lit_i64(long val);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_lit_u64(ulong val);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_lit_i128(ulong low,long high);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_lit_decimal(
        ulong low, 
        long high, 
        uint scale
    );
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_lit_f16(Half val);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_lit_f32(float val);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_lit_null();
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_lit_str([MarshalAs(UnmanagedType.LPUTF8Str)] string val);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_lit_f64(double val);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_lit_datetime(long micros);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_lit_date(int days);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_lit_time(long nanoseconds);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_lit_duration(long microseconds);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_lit_series(SeriesHandle seriesHandle);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_mul(ExprHandle left, ExprHandle right);
    // Comparsion
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_eq(ExprHandle left, ExprHandle right);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_neq(ExprHandle l, ExprHandle r);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_gt(ExprHandle left, ExprHandle right);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_gt_eq(ExprHandle l, ExprHandle r);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_lt(ExprHandle l, ExprHandle r);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_lt_eq(ExprHandle l, ExprHandle r);
    // Top-k & Bottom-k
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_top_k(ExprHandle expr, uint k);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_bottom_k(ExprHandle expr, uint k);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_top_k_by(
        ExprHandle expr, 
        uint k, 
        IntPtr[] by_ptrs,   
        UIntPtr by_len,
        bool* descending,  
        UIntPtr desc_len
    );
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_bottom_k_by(
        ExprHandle expr, 
        uint k, 
        IntPtr[] by_ptrs,
        UIntPtr by_len,
        bool* descending, 
        UIntPtr desc_len
    );
    // Reverse
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_reverse(ExprHandle expr);
    // Arithmetic
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_add(ExprHandle l, ExprHandle r);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_sub(ExprHandle l, ExprHandle r);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_div(ExprHandle l, ExprHandle r);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_floor_div(ExprHandle l, ExprHandle r);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_rem(ExprHandle l, ExprHandle r);
    // Bitwise Shift
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_bit_shl(ExprHandle expr, int n);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_bit_shr(ExprHandle expr, int n);
    // Logic
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_and(ExprHandle l, ExprHandle r);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_or(ExprHandle l, ExprHandle r);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_not(ExprHandle e);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_xor(ExprHandle l, ExprHandle r);
    // Aggregation
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_first(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_last(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_all(ExprHandle expr, [MarshalAs(UnmanagedType.U1)] bool ignoreNulls);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_any(ExprHandle expr, [MarshalAs(UnmanagedType.U1)] bool ignoreNulls);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_item(ExprHandle expr, [MarshalAs(UnmanagedType.U1)] bool allowEmpty);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_sum(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_mean(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_max(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_min(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_abs(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_skew(ExprHandle expr,[MarshalAs(UnmanagedType.U1)] bool bias);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_kurtosis(ExprHandle expr,[MarshalAs(UnmanagedType.U1)] bool fisher,[MarshalAs(UnmanagedType.U1)] bool bias);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_product(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_pct_change(ExprHandle expr, long n);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_rank(ExprHandle expr, PlRankMethod method,[MarshalAs(UnmanagedType.U1)] bool descending, ulong* seed);
    // Cumulative Fuctions
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_cum_sum(ExprHandle expr,[MarshalAs(UnmanagedType.U1)] bool reverse);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_cum_max(ExprHandle expr,[MarshalAs(UnmanagedType.U1)] bool reverse);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_cum_min(ExprHandle expr,[MarshalAs(UnmanagedType.U1)] bool reverse);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_cum_prod(ExprHandle expr,[MarshalAs(UnmanagedType.U1)] bool reverse);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_cum_count(ExprHandle expr,[MarshalAs(UnmanagedType.U1)] bool reverse);
    // --- EWM Functions ---
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_ewm_mean(
        ExprHandle expr,
        double alpha,
        [MarshalAs(UnmanagedType.U1)] bool adjust,
        [MarshalAs(UnmanagedType.U1)] bool bias,
        UIntPtr min_periods,
        [MarshalAs(UnmanagedType.U1)] bool ignore_nulls);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_ewm_std(
        ExprHandle expr,
        double alpha,
        [MarshalAs(UnmanagedType.U1)] bool adjust,
        [MarshalAs(UnmanagedType.U1)] bool bias,
        UIntPtr min_periods,
        [MarshalAs(UnmanagedType.U1)] bool ignore_nulls);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_ewm_var(
        ExprHandle expr,
        double alpha,
        [MarshalAs(UnmanagedType.U1)] bool adjust,
        [MarshalAs(UnmanagedType.U1)] bool bias,
        UIntPtr min_periods,
        [MarshalAs(UnmanagedType.U1)] bool ignore_nulls);
    [LibraryImport(LibName,StringMarshalling=StringMarshalling.Utf8)] public static partial ExprHandle pl_expr_ewm_mean_by(
        ExprHandle expr,
        ExprHandle by,
        string half_life
    );
    // null ops
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_fill_null(ExprHandle expr, ExprHandle fillValue);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_fill_nan(ExprHandle expr, ExprHandle fillValue);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_interpolate(ExprHandle expr, PlInterpolationMethod method);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_interpolate_by(ExprHandle expr, ExprHandle by);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_is_null(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_is_not_null(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_drop_nulls(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_drop_nans(ExprHandle expr);
    // Unique ops
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_is_unique(ExprHandle expr);

    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_is_duplicated(ExprHandle expr);

    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_unique(ExprHandle expr);

    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_unique_stable(ExprHandle expr);
    // Math ops
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_pow(ExprHandle baseExpr, ExprHandle exponent);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dot(ExprHandle left, ExprHandle right);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_sqrt(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_cbrt(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_exp(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_log(ExprHandle expr, double baseVal);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_round(ExprHandle expr, uint decimals);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_sin(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_cos(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_tan(ExprHandle expr);
    
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_arcsin(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_arccos(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_arctan(ExprHandle expr);
    
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_sinh(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_cosh(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_tanh(ExprHandle expr);

    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_arcsinh(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_arccosh(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_arctanh(ExprHandle expr);

    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_sign(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_ceil(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_floor(ExprHandle expr);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_is_between(ExprHandle expr, ExprHandle lower, ExprHandle upper);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_filter(ExprHandle expr, ExprHandle predicate);
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_is_in(ExprHandle expr, ExprHandle other, [MarshalAs(UnmanagedType.U1)] bool nulls_equal);

    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_alias(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string name);

    // Temporal
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_year(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_quarter(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_month(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_day(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_ordinal_day(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_weekday(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_hour(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_minute(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_second(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_millisecond(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_microsecond(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_nanosecond(ExprHandle expr);
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_dt_to_string(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string format);
    
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_date(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_time(ExprHandle expr);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_dt_truncate(ExprHandle e, string every);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_dt_round(ExprHandle e, string every);

    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_dt_offset_by(ExprHandle e, ExprHandle by);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_dt_combine(ExprHandle e, ExprHandle time,PlTimeUnit timeUnit);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_dt_timestamp(ExprHandle e, PlTimeUnit unitCode);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_dt_convert_time_zone(ExprHandle e, string timeZone);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_dt_replace_time_zone(
        ExprHandle e, 
        string? timeZone, 
        string? ambiguous, 
        string? nonExistent
    );
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_add_business_days(
        ExprHandle expr,
        ExprHandle n,
        [In, MarshalAs(UnmanagedType.LPArray, SizeConst = 7)] byte[] weekMask,
        [In, MarshalAs(UnmanagedType.LPArray)] int[] holidays,     
        UIntPtr holidaysLen,
        PlRoll rollStrategy
    );

    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_is_business_day(
        ExprHandle expr,
        [In, MarshalAs(UnmanagedType.LPArray, SizeConst = 7)] byte[] weekMask,
        [In, MarshalAs(UnmanagedType.LPArray)] int[] holidays,
        UIntPtr holidaysLen
    );
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_clone(ExprHandle expr);

    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_map(
        ExprHandle expr, 
        UdfCallback callback, 
        DataTypeHandle returnType,
        CleanupCallback cleanup,
        IntPtr userData          
    );
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_cast(ExprHandle expr, DataTypeHandle dtype, [MarshalAs(UnmanagedType.U1)] bool strict);
    // String Ops
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_concat_str(IntPtr[] exprs,nuint exprLen, string separator, [MarshalAs(UnmanagedType.U1)] bool ignoreNulls);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_format_str(string format,IntPtr[] exprs,UIntPtr exprLen);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_str_contains(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string pat);

    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_str_to_uppercase(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_str_to_lowercase(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_str_len_bytes(ExprHandle expr);
    // String Cleaning
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_str_strip_chars(ExprHandle e, string? matches);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_str_strip_chars_start(ExprHandle e, string? matches);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_str_strip_chars_end(ExprHandle e, string? matches);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_str_strip_prefix(ExprHandle e, string prefix);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_str_strip_suffix(ExprHandle e, string suffix);

    // Anchors
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_str_starts_with(ExprHandle e, string prefix);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_str_ends_with(ExprHandle e, string suffix);
    // Parsing
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_str_to_date(ExprHandle e, string format);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_str_to_datetime(ExprHandle e, string format);
    
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_str_slice(ExprHandle expr, long offset, ulong length);
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_str_split(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string pat);
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_str_replace_all(
        ExprHandle expr, 
        [MarshalAs(UnmanagedType.LPUTF8Str)] string pat, 
        [MarshalAs(UnmanagedType.LPUTF8Str)] string val,
        [MarshalAs(UnmanagedType.U1)] bool useRegex
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)] 
    public static partial ExprHandle pl_expr_str_extract(
        ExprHandle expr, 
        string pat, 
        UIntPtr groupIndex
    );

    // List Ops
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_first(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_get(ExprHandle expr, long index);
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_explode(
        ExprHandle expr,
        [MarshalAs(UnmanagedType.U1)] bool emptyAsNull,
        [MarshalAs(UnmanagedType.U1)] bool keepNulls);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_implode(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_join(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string sep);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_len(ExprHandle expr);
    // List Aggs
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_sum(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_min(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_max(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_mean(ExprHandle expr);
    
    // List Other
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_sort(
        ExprHandle expr,
        [MarshalAs(UnmanagedType.U1)] bool descending,
        [MarshalAs(UnmanagedType.U1)] bool nulls_last,
        [MarshalAs(UnmanagedType.U1)] bool maintain_order
    );
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_contains(ExprHandle expr, ExprHandle item, [MarshalAs(UnmanagedType.U1)] bool nulls_equal);
    [LibraryImport(LibName)] public static partial ExprHandle pl_concat_list(IntPtr[] exprs,nuint exprLen);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_list_reverse(ExprHandle expr);
    // Array Ops
    [LibraryImport(LibName)] public static partial ExprHandle pl_concat_array(IntPtr[] exprs,nuint exprLen);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_array_max(ExprHandle expr);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_array_min(ExprHandle expr);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_array_sum(ExprHandle expr);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_array_unique(ExprHandle expr,[MarshalAs(UnmanagedType.U1)] bool stable);
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_array_join(
        ExprHandle expr, 
        [MarshalAs(UnmanagedType.LPUTF8Str)] string sep,
        [MarshalAs(UnmanagedType.U1)] bool ignoreNulls);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_array_contains(
        ExprHandle expr, 
        ExprHandle item,
        [MarshalAs(UnmanagedType.U1)] bool nullsEqual
    );
    // [New] Stats
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_array_mean(ExprHandle expr);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_array_median(ExprHandle expr);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_array_std(ExprHandle expr, byte ddof);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_array_var(ExprHandle expr, byte ddof);

    // [New] Boolean
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_array_any(ExprHandle expr);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_array_all(ExprHandle expr);

    // [New] Sort & Args
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_array_sort(
        ExprHandle expr, 
        [MarshalAs(UnmanagedType.U1)] bool descending, 
        [MarshalAs(UnmanagedType.U1)] bool nullsLast,
        [MarshalAs(UnmanagedType.U1)] bool maintainOrder
    );
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_array_reverse(ExprHandle expr);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_array_arg_min(ExprHandle expr);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_array_arg_max(ExprHandle expr);

    // [New] Structure
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_array_get(
        ExprHandle expr, 
        ExprHandle index, 
        [MarshalAs(UnmanagedType.I1)] bool nullOnOob
    );
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_array_explode(
        ExprHandle expr,
        [MarshalAs(UnmanagedType.U1)] bool emptyAsNull,
        [MarshalAs(UnmanagedType.U1)] bool keepNulls);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_array_to_list(ExprHandle expr);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_array_to_struct(ExprHandle expr);


    // Naming
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_prefix(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string prefix);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_suffix(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string suffix);


    // Expr Len
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_len();
    [LibraryImport(LibName)] public static partial IntPtr pl_get_last_error();
    [LibraryImport(LibName)] public static partial void pl_free_error_msg(IntPtr ptr);

    // Struct
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_as_struct(IntPtr[] exprs, UIntPtr len);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_struct_field_by_name(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string name);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_struct_field_by_index(ExprHandle e, long index);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_struct_rename_fields(
        ExprHandle e, 
        [MarshalAs(UnmanagedType.LPArray, ArraySubType = UnmanagedType.LPStr, SizeParamIndex = 2)] 
        string[] names, 
        UIntPtr len
    );
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_struct_json_encode(ExprHandle e);
    // Window
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_over(
        ExprHandle expr, 
        IntPtr[] partitionBy, 
        UIntPtr len
    );
    // Shift / Diff
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_shift(ExprHandle expr, long n);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_diff(ExprHandle expr, long n);

    // Fill
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_forward_fill(ExprHandle expr, uint limit);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_backward_fill(ExprHandle expr, uint limit);
    // Rolling Window
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_rolling_mean(
        ExprHandle expr,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string windowSize,
        UIntPtr minPeriods,
        [MarshalAs(UnmanagedType.LPArray)] double[]? weights,
        UIntPtr weights_len,
        [MarshalAs(UnmanagedType.U1)]bool center);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_rolling_sum(
        ExprHandle expr,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string windowSize,
        UIntPtr minPeriods,
        [MarshalAs(UnmanagedType.LPArray)] double[]? weights,
        UIntPtr weights_len,
        [MarshalAs(UnmanagedType.U1)]bool center);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_rolling_min(
        ExprHandle expr, 
        [MarshalAs(UnmanagedType.LPUTF8Str)] string windowSize,
        UIntPtr minPeriods,
        [MarshalAs(UnmanagedType.LPArray)] double[]? weights,
        UIntPtr weights_len,
        [MarshalAs(UnmanagedType.U1)]bool center);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_rolling_max(
        ExprHandle expr, 
        [MarshalAs(UnmanagedType.LPUTF8Str)] string windowSize,
        UIntPtr minPeriods,
        [MarshalAs(UnmanagedType.LPArray)] double[]? weights,
        UIntPtr weights_len,
        [MarshalAs(UnmanagedType.U1)]bool center);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_rolling_std(
        ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string windowSize,
        UIntPtr minPeriods,[MarshalAs(UnmanagedType.LPArray)] double[]? weights,
        UIntPtr weights_len,
        [MarshalAs(UnmanagedType.U1)]bool center);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_rolling_var(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string windowSize,UIntPtr minPeriods,        
        [MarshalAs(UnmanagedType.LPArray)] double[]? weights,
        UIntPtr weights_len,
        [MarshalAs(UnmanagedType.U1)]bool center,byte ddof);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_rolling_median(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string windowSize,UIntPtr minPeriods,
        [MarshalAs(UnmanagedType.LPArray)] double[]? weights,
        UIntPtr weights_len,
        [MarshalAs(UnmanagedType.U1)]bool center);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_rolling_skew(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string windowSize,UIntPtr minPeriods,        
        [MarshalAs(UnmanagedType.LPArray)] double[]? weights,
        UIntPtr weights_len,
        [MarshalAs(UnmanagedType.U1)]bool center,
        [MarshalAs(UnmanagedType.U1)]bool bias);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_rolling_kurtosis(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string windowSize,UIntPtr minPeriods, 
        [MarshalAs(UnmanagedType.LPArray)] double[]? weights,
        UIntPtr weights_len,
        [MarshalAs(UnmanagedType.U1)]bool center,
        [MarshalAs(UnmanagedType.U1)]bool fisher,
        [MarshalAs(UnmanagedType.U1)]bool bias);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_rolling_rank(
        ExprHandle expr,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string windowSize,
        UIntPtr minPeriods,
        PlRankMethod method,
        ulong* seed,
        [MarshalAs(UnmanagedType.LPArray)] double[]? weights,
        UIntPtr weights_len,
        [MarshalAs(UnmanagedType.U1)]bool center
        );
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_rolling_quantile(
        ExprHandle expr, 
        double quantile,
        PlQuantileMethod interpolation,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string windowSize,
        UIntPtr minPeriods,
        [MarshalAs(UnmanagedType.LPArray)] double[]? weights,
        UIntPtr weights_len,
        [MarshalAs(UnmanagedType.U1)]bool center
        );

    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)] public static partial ExprHandle pl_expr_rolling_mean_by(ExprHandle expr, string windowSize,UIntPtr minPeriods, ExprHandle by, PlClosedWindow closed);
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)] public static partial ExprHandle pl_expr_rolling_sum_by(ExprHandle expr, string windowSize,UIntPtr minPeriods, ExprHandle by, PlClosedWindow closed);
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)] public static partial ExprHandle pl_expr_rolling_min_by(ExprHandle expr, string windowSize,UIntPtr minPeriods, ExprHandle by, PlClosedWindow closed);
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)] public static partial ExprHandle pl_expr_rolling_max_by(ExprHandle expr, string windowSize,UIntPtr minPeriods, ExprHandle by, PlClosedWindow closed);
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)] public static partial ExprHandle pl_expr_rolling_std_by(ExprHandle expr, string windowSize,UIntPtr minPeriods, ExprHandle by, PlClosedWindow closed);
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)] public static partial ExprHandle pl_expr_rolling_var_by(ExprHandle expr, string windowSize,UIntPtr minPeriods, ExprHandle by, PlClosedWindow closed, byte ddof);
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)] public static partial ExprHandle pl_expr_rolling_median_by(ExprHandle expr, string windowSize,UIntPtr minPeriods, ExprHandle by, PlClosedWindow closed);
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)] public static partial ExprHandle pl_expr_rolling_rank_by(ExprHandle expr, PlRollingRankMethod method,ulong* seed,string windowSize,UIntPtr minPeriods, ExprHandle by, PlClosedWindow closed);
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)] public static partial ExprHandle pl_expr_rolling_quantile_by(
        ExprHandle expr,
        double quantile,
        PlQuantileMethod interpolation,
        string windowSize,
        UIntPtr minPeriods,
        ExprHandle by,
        PlClosedWindow closed
    );
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_if_else(ExprHandle pred, ExprHandle ifTrue, ExprHandle ifFalse);
    // Statistical
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_count(ExprHandle e);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_mode(ExprHandle e);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_std(ExprHandle e, byte ddof);

    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_var(ExprHandle e, byte ddof);

    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_median(ExprHandle e);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_quantile(ExprHandle e, double quantile, PlQuantileMethod interpol);

    [LibraryImport(LibName)]
    public static partial void pl_free_c_string(IntPtr ptr);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_gather(ExprHandle expr, ExprHandle idx);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_gather_every(ExprHandle expr, nuint n, nuint offset);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_get(
        ExprHandle expr, 
        ExprHandle idx, 
        [MarshalAs(UnmanagedType.I1)] bool nullOnOob
    );
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_sort(
        ExprHandle expr,
        [MarshalAs(UnmanagedType.I1)] bool descending,
        [MarshalAs(UnmanagedType.I1)] bool nullsLast,
        [MarshalAs(UnmanagedType.I1)] bool multithreaded,
        [MarshalAs(UnmanagedType.I1)] bool maintainOrder,
        uint* limitPtr
    );
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_arg_unique(ExprHandle expr);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_arg_min(ExprHandle expr);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_arg_max(ExprHandle expr);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_arg_sort(
        ExprHandle expr, 
        [MarshalAs(UnmanagedType.I1)] bool descending,
        [MarshalAs(UnmanagedType.I1)] bool nullsLast
    );
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_index_of(
        ExprHandle expr, 
        ExprHandle element
    );
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_search_sorted(
        ExprHandle expr, 
        ExprHandle element,
        PlSearchSortedSide side,
        [MarshalAs(UnmanagedType.I1)] bool descending
    );
    
}