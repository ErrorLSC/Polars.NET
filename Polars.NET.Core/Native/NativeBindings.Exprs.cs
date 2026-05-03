using System.Runtime.InteropServices;

namespace Polars.NET.Core.Native;

unsafe internal partial class NativeBindings
{
    [LibraryImport(LibName)]
    public static partial int pl_expr_meta_eq(ExprHandle expr, ExprHandle other, [MarshalAs(UnmanagedType.I1)] out bool outVal);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_set_env_var(
    string key, 
    string value);
    [LibraryImport(LibName)] public static partial void pl_expr_free(IntPtr ptr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_rechunk(ExprHandle expr);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_concat_expr(IntPtr[] exprs,UIntPtr exprLen, [MarshalAs(UnmanagedType.U1)] bool rechunk);
    // String Free
    [LibraryImport(LibName)] public static partial void pl_free_string(IntPtr ptr);
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)] 
    public static partial ExprHandle pl_expr_sql(string query);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)] 
    public static partial ExprHandle pl_expr_col(string name);
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
    // [LibraryImport(LibName)] public static partial ExprHandle pl_expr_lit_u128(ulong low,ulong high);
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
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_eq_missing(ExprHandle left, ExprHandle right);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_neq(ExprHandle l, ExprHandle r);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_neq_missing(ExprHandle left, ExprHandle right);
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
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_coalesce(nint[] exprPtrs,nuint len);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_lower_bound(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_upper_bound(ExprHandle expr);
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
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_first_non_null(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_last_non_null(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_last(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_rle(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_rle_id(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_peak_max(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_peak_min(ExprHandle expr);
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_cut(
        ExprHandle expr,
        ref double breaks,
        nuint breaks_len,
        string[]? labels,
        nuint labels_len,
        [MarshalAs(UnmanagedType.I1)] bool left_closed,
        [MarshalAs(UnmanagedType.I1)] bool include_breaks
    );

    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_qcut(
        ExprHandle expr,
        ref double probs,
        nuint probs_len,
        string[]? labels,
        nuint labels_len,
        [MarshalAs(UnmanagedType.I1)] bool left_closed,
        [MarshalAs(UnmanagedType.I1)] bool allow_duplicates,
        [MarshalAs(UnmanagedType.I1)] bool include_breaks
    );

    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_qcut_uniform(
        ExprHandle expr,
        nuint n_bins,
        string[]? labels,
        nuint labels_len,
        [MarshalAs(UnmanagedType.I1)] bool left_closed,
        [MarshalAs(UnmanagedType.I1)] bool allow_duplicates,
        [MarshalAs(UnmanagedType.I1)] bool include_breaks
    );
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_replace(ExprHandle expr, ExprHandle old,ExprHandle newExpr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_replace_strict(ExprHandle expr, ExprHandle old,ExprHandle newExpr,nint defaultExpr, nint dtype);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_head(ExprHandle expr, UIntPtr length);

    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_tail(ExprHandle expr, UIntPtr length);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_all(ExprHandle expr, [MarshalAs(UnmanagedType.U1)] bool ignoreNulls);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_any(ExprHandle expr, [MarshalAs(UnmanagedType.U1)] bool ignoreNulls);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_item(ExprHandle expr, [MarshalAs(UnmanagedType.U1)] bool allowEmpty);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_sum(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_mean(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_max(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_bitwise_and(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_bitwise_or(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_bitwise_xor(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_bitwise_count_ones(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_bitwise_count_zeros(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_bitwise_leading_ones(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_bitwise_leading_zeros(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_bitwise_trailing_ones(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_bitwise_trailing_zeros(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_nan_max(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_max_by(ExprHandle expr, ExprHandle by);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_min(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_nan_min(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_min_by(ExprHandle expr, ExprHandle by);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_abs(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_null_count(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_n_unique(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_unique_counts(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_approx_n_unique(ExprHandle expr);
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
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_cumulative_eval(ExprHandle expr,ExprHandle eval,nuint minSamples);
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
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_fill_null_with_strategy(ExprHandle expr, PlFillNullStrategy strategy,uint limit);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_fill_nan(ExprHandle expr, ExprHandle fillValue);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_clip_min(ExprHandle expr,ExprHandle min);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_clip_max(ExprHandle expr,ExprHandle max);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_clip(ExprHandle expr,ExprHandle min,ExprHandle max);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_interpolate(ExprHandle expr, PlInterpolationMethod method);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_interpolate_by(ExprHandle expr, ExprHandle by);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_is_null(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_is_not_null(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_is_nan(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_is_not_nan(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_is_finite(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_is_infinite(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_is_first_distinct(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_is_last_distinct(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_is_close(
        ExprHandle expr,
        ExprHandle other,
        double abs_tol,
        double rel_tol,
        [MarshalAs(UnmanagedType.U1)]bool nans_equal);
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
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_log(ExprHandle expr, ExprHandle baseVal);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_log1p(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_entropy(ExprHandle expr,double baseVal,[MarshalAs(UnmanagedType.U1)]bool normalize);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_round(ExprHandle expr, uint decimals,PlRoundMode mode);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_round_sig_figs(ExprHandle expr, int digits);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_to_physical(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_shuffle(
        ExprHandle expr,
        [MarshalAs(UnmanagedType.U1)] bool hasSeed,
        ulong seed);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_sample_n(
        ExprHandle e, 
        ExprHandle n,
        [MarshalAs(UnmanagedType.U1)] bool withReplacement,
        [MarshalAs(UnmanagedType.U1)] bool shuffle,
        [MarshalAs(UnmanagedType.U1)] bool hasSeed,
        ulong seed);

    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_sample_frac(
        ExprHandle e,
        ExprHandle frac,
        [MarshalAs(UnmanagedType.U1)] bool withReplacement,
        [MarshalAs(UnmanagedType.U1)] bool shuffle,
        [MarshalAs(UnmanagedType.U1)] bool hasSeed,
        ulong seed);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_slice(
        ExprHandle e,
        ExprHandle offset,
        ExprHandle length
    );
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_sin(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_cos(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_tan(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_cot(ExprHandle expr);
    
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_arcsin(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_arccos(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_arctan(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_arctan2(ExprHandle expr,ExprHandle x);
    
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_sinh(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_cosh(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_tanh(ExprHandle expr);

    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_arcsinh(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_arccosh(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_arctanh(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_degrees(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_radians(ExprHandle expr);

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
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_reshape(
        ExprHandle expr,
        ReadOnlySpan<long> dims_ptr, 
        nuint dims_len              
    );
   
  
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_clone(ExprHandle expr);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_cast(
        ExprHandle expr_ptr, 
        DataTypeExprHandle dexpr_ptr, 
        [MarshalAs(UnmanagedType.U1)] bool strict, 
        [MarshalAs(UnmanagedType.U1)] bool wrap_numerical
    );

    // List Ops
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_get(ExprHandle expr, ExprHandle index,[MarshalAs(UnmanagedType.U1)]bool nullsOnOob);
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_explode(
        ExprHandle expr,
        [MarshalAs(UnmanagedType.U1)] bool emptyAsNull,
        [MarshalAs(UnmanagedType.U1)] bool keepNulls);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_implode(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_join(
        ExprHandle expr, 
        [MarshalAs(UnmanagedType.LPUTF8Str)] string sep,
        [MarshalAs(UnmanagedType.U1)]bool ignoreNulls);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_len(ExprHandle expr);
    // List Aggs
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_all(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_any(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_sum(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_min(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_max(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_mean(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_median(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_drop_nulls(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_n_unique(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_arg_max(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_arg_min(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_unique(ExprHandle expr,[MarshalAs(UnmanagedType.U1)]bool maintainOrder);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_gather(
        ExprHandle expr,
        ExprHandle index,
        [MarshalAs(UnmanagedType.U1)]bool nullOnOob
    );
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_gather_every(
        ExprHandle expr,
        ExprHandle n,
        ExprHandle offset
    );    
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_slice(
        ExprHandle expr,
        ExprHandle offset,
        ExprHandle length
    );    
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_head(
        ExprHandle expr,
        ExprHandle n
    );        
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_tail(
        ExprHandle expr,
        ExprHandle n
    );     
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_count_matches(
        ExprHandle expr,
        ExprHandle item
    );       
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_agg(
        ExprHandle expr,
        ExprHandle agg
    );      
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_shift(
        ExprHandle expr,
        ExprHandle shift
    );   
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_std(
        ExprHandle expr,
        byte ddof
    );  
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_var(
        ExprHandle expr,
        byte ddof
    );    
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_to_array(
        ExprHandle expr,
        nuint width
    );   
    [LibraryImport(LibName,StringMarshalling =StringMarshalling.Utf8)] public static partial ExprHandle pl_expr_list_to_struct(
        ExprHandle expr,
        string[]? names,
        nuint len
    );   
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_diff(
        ExprHandle expr,
        long n,
        PlNullBehavior nullBehavior
    );   
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_sample(
        ExprHandle expr,
        ExprHandle nFrac,
        [MarshalAs(UnmanagedType.U1)] bool isFraction,
        [MarshalAs(UnmanagedType.U1)] bool withReplacement,
        [MarshalAs(UnmanagedType.U1)] bool shuffle,
        [MarshalAs(UnmanagedType.U1)] bool hasSeed,
        ulong seed
    );   
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_set_operation(
        ExprHandle expr,
        ExprHandle other,
        PlSetOperation setOperation
    );   
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
    public static partial ExprHandle pl_expr_array_len(ExprHandle expr);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_array_n_unique(ExprHandle expr);
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
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_array_count_matches(ExprHandle expr, ExprHandle item);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_array_agg(ExprHandle expr, ExprHandle agg);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_array_shift(ExprHandle expr, ExprHandle shift);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_array_head(ExprHandle expr, ExprHandle num,[MarshalAs(UnmanagedType.U1)]bool asArray);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_array_tail(ExprHandle expr, ExprHandle num,[MarshalAs(UnmanagedType.U1)]bool asArray);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_array_eval(ExprHandle expr, ExprHandle other,[MarshalAs(UnmanagedType.U1)]bool asList);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_array_slice(ExprHandle expr, ExprHandle offset,ExprHandle length,[MarshalAs(UnmanagedType.U1)]bool asList);

    // Boolean
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
    [LibraryImport(LibName,StringMarshalling=StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_array_to_struct(ExprHandle expr,string[]? fieldNames,nuint len);

    // Naming
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_prefix(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string prefix);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_suffix(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string suffix);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_name_prefix_fields(ExprHandle expr,[MarshalAs(UnmanagedType.LPUTF8Str)] string prefix); 
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_name_suffix_fields(ExprHandle expr,[MarshalAs(UnmanagedType.LPUTF8Str)] string suffix);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_name_to_uppercase(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_name_to_lowercase(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_name_keep(ExprHandle expr);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
     public static partial ExprHandle pl_expr_name_replace(
        ExprHandle expr,
        string pattern,
        string value,
        [MarshalAs(UnmanagedType.U1)]bool literal
    );
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate IntPtr MapStringCallback(IntPtr name);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void FreeStringCallback(IntPtr ptr);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void FreeHandleCallback(IntPtr gcHandlePtr);

    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_name_map(
        ExprHandle expr, MapStringCallback callback, FreeStringCallback freeStringCb, IntPtr gcHandlePtr, FreeHandleCallback freeHandleCb);

    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_name_map_fields(
        ExprHandle expr, MapStringCallback callback, FreeStringCallback freeStringCb, IntPtr gcHandlePtr, FreeHandleCallback freeHandleCb);

    // Expr Len
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_len(ExprHandle e);
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_len();
    [LibraryImport(LibName)] public static partial IntPtr pl_get_last_error();
    [LibraryImport(LibName)] public static partial void pl_free_error_msg(IntPtr ptr);

    // Struct
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_as_struct(IntPtr[] exprs, UIntPtr len);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_struct_field_by_names(
        ExprHandle expr, 
        string[] names, 
        nuint namesLen
    );
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
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_struct_with_fields(
        ExprHandle expr,
        nint[] fields, 
        nuint fieldsLen);
    // Window
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_over_with_options(
        ExprHandle exprPtr,
        nint[] partitionByPtr,
        nuint partitionByLen,
        nint[] orderByPtr,
        nuint orderByLen,
        [MarshalAs(UnmanagedType.I1)] bool descending,
        [MarshalAs(UnmanagedType.I1)] bool nullsLast,
        [MarshalAs(UnmanagedType.I1)] bool multithreaded,
        [MarshalAs(UnmanagedType.I1)] bool maintainOrder,
        PlWindowMapping mappingCode 
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_rolling(
        ExprHandle expr,
        ExprHandle indexColumn,
        string period,
        string offset,
        PlClosedInterval closed
    );
    // Shift / Diff
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_shift(ExprHandle expr, ExprHandle n);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_diff(ExprHandle expr, ExprHandle n, PlNullBehavior nullBehavior);

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

    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)] public static partial ExprHandle pl_expr_rolling_mean_by(ExprHandle expr, string windowSize,UIntPtr minPeriods, ExprHandle by, PlClosedInterval closed);
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)] public static partial ExprHandle pl_expr_rolling_sum_by(ExprHandle expr, string windowSize,UIntPtr minPeriods, ExprHandle by, PlClosedInterval closed);
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)] public static partial ExprHandle pl_expr_rolling_min_by(ExprHandle expr, string windowSize,UIntPtr minPeriods, ExprHandle by, PlClosedInterval closed);
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)] public static partial ExprHandle pl_expr_rolling_max_by(ExprHandle expr, string windowSize,UIntPtr minPeriods, ExprHandle by, PlClosedInterval closed);
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)] public static partial ExprHandle pl_expr_rolling_std_by(ExprHandle expr, string windowSize,UIntPtr minPeriods, ExprHandle by, PlClosedInterval closed);
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)] public static partial ExprHandle pl_expr_rolling_var_by(ExprHandle expr, string windowSize,UIntPtr minPeriods, ExprHandle by, PlClosedInterval closed, byte ddof);
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)] public static partial ExprHandle pl_expr_rolling_median_by(ExprHandle expr, string windowSize,UIntPtr minPeriods, ExprHandle by, PlClosedInterval closed);
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)] public static partial ExprHandle pl_expr_rolling_rank_by(ExprHandle expr, PlRollingRankMethod method,ulong* seed,string windowSize,UIntPtr minPeriods, ExprHandle by, PlClosedInterval closed);
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)] public static partial ExprHandle pl_expr_rolling_quantile_by(
        ExprHandle expr,
        double quantile,
        PlQuantileMethod interpolation,
        string windowSize,
        UIntPtr minPeriods,
        ExprHandle by,
        PlClosedInterval closed
    );
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_ternary(ExprHandle pred, ExprHandle ifTrue, ExprHandle ifFalse);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_extend_constant(ExprHandle e, ExprHandle value, ExprHandle n);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_append(ExprHandle e, ExprHandle other, [MarshalAs(UnmanagedType.U1)]bool upcast);
    // Statistical
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_count(ExprHandle e);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_mode(ExprHandle e);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_std(ExprHandle e, byte ddof);

    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_var(ExprHandle e, byte ddof);

    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_median(ExprHandle e);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_quantile(ExprHandle e, double quantile, PlQuantileMethod interpol);

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
    public static partial ExprHandle pl_expr_arg_sort_by(
        nint[] exprs, 
        nuint len,
        ReadOnlySpan<byte> descending, 
        ReadOnlySpan<byte> nullsLast,  
        [MarshalAs(UnmanagedType.I1)] bool multithreaded,
        [MarshalAs(UnmanagedType.I1)] bool maintainOrder
    );
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_arg_where(ExprHandle condition);

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
    [LibraryImport(LibName)] public static partial SelectorHandle pl_expr_try_into_selector(ExprHandle expr);
    [LibraryImport(LibName)]
    public static partial int pl_expr_get_output_name(
        ExprHandle expr, 
        out IntPtr outStr
    );
    [LibraryImport(LibName)]
    public static partial IntPtr pl_expr_to_string(ExprHandle expr);
    [LibraryImport(LibName)]
    public static partial int pl_expr_meta_into_tree_formatter(
        ExprHandle expr, 
        [MarshalAs(UnmanagedType.I1)] bool displayAsDot, 
        IntPtr schemaPtr,
        out IntPtr outStr);
    [LibraryImport(LibName)]
    public static partial int pl_expr_meta_is_column(ExprHandle expr,[MarshalAs(UnmanagedType.U1)]out bool result);
    [LibraryImport(LibName)]
    public static partial int pl_expr_meta_is_column_selection(ExprHandle expr,[MarshalAs(UnmanagedType.U1)] bool allowAliasing,[MarshalAs(UnmanagedType.U1)]out bool result);
    [LibraryImport(LibName)]
    public static partial int pl_expr_meta_is_literal(ExprHandle expr,[MarshalAs(UnmanagedType.U1)] bool allowAliasing,[MarshalAs(UnmanagedType.U1)]out bool result);
    [LibraryImport(LibName)]
    public static partial int pl_expr_meta_is_regex_projection(ExprHandle expr,[MarshalAs(UnmanagedType.U1)]out bool result);
    [LibraryImport(LibName)]
    public static partial int pl_expr_meta_has_multiple_outputs(ExprHandle expr,[MarshalAs(UnmanagedType.U1)]out bool result);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_meta_undo_aliases(ExprHandle expr);
    [LibraryImport(LibName)]
    public static partial int pl_expr_meta_root_names(ExprHandle expr,out IntPtr rootNames);
    [LibraryImport(LibName)]
    public static partial int pl_expr_meta_pop(IntPtr expr, out IntPtr outPtrs, out nuint outLen);

    [LibraryImport(LibName)]
    public static partial void pl_free_ptr_array(IntPtr ptr, nuint len);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_int_range(
        ExprHandle start,
        ExprHandle end,
        long step,
        DataTypeExprHandle dataTypeExpr
    );
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_int_ranges(
        ExprHandle start,
        ExprHandle end,
        ExprHandle step,
        DataTypeExprHandle datatypeExpr
    );
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_date_range(
        IntPtr start,
        IntPtr end,
        string? interval,
        IntPtr numSamples,
        PlClosedInterval closedWindow
    );
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_date_ranges(
        IntPtr start,
        IntPtr end,
        string? interval,
        IntPtr numSamples,
        PlClosedInterval closedWindow
    );
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_datetime_range(
        IntPtr start,
        IntPtr end,
        string? interval,
        IntPtr numSamples,
        PlClosedInterval closedWindow,
        PlTimeUnit unit,
        string? timeZone
    );  
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_datetime_ranges(
        IntPtr start,
        IntPtr end,
        string? interval,
        IntPtr numSamples,
        PlClosedInterval closedWindow,
        PlTimeUnit unit,
        string? timeZone
    ); 
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_time_range(
        ExprHandle start,
        ExprHandle end,
        string? interval,
        PlClosedInterval closedWindow
    );
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_time_ranges(
        ExprHandle start,
        ExprHandle end,
        string? interval,
        PlClosedInterval closedWindow
    );
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_linear_space(
        ExprHandle start,
        ExprHandle end,
        ExprHandle numSamples,
        PlClosedInterval closedWindow
    );
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_linear_spaces(
        ExprHandle start,
        ExprHandle end,
        ExprHandle numSamples,
        PlClosedInterval closedWindow,
        [MarshalAs(UnmanagedType.U1)] bool asArray
    );
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_all_horizontal(nint[] exprs,nuint len);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_any_horizontal(nint[] exprs,nuint len);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_max_horizontal(nint[] exprs,nuint len);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_min_horizontal(nint[] exprs,nuint len);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_sum_horizontal(nint[] exprs,nuint len,[MarshalAs(UnmanagedType.U1)] bool ignoreNulls);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_mean_horizontal(nint[] exprs,nuint len,[MarshalAs(UnmanagedType.U1)] bool ignoreNulls);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_set_sorted_flag(ExprHandle expr,[MarshalAs(UnmanagedType.U1)] bool descending,[MarshalAs(UnmanagedType.U1)]  bool nullsLast);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_ext_to(ExprHandle expr,DataTypeExprHandle dtype);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_ext_storage(ExprHandle expr);
    [LibraryImport(LibName,StringMarshalling =StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_value_counts(
        ExprHandle expr,
        [MarshalAs(UnmanagedType.U1)] bool sort,
        [MarshalAs(UnmanagedType.U1)] bool parallel,
        string name,
        [MarshalAs(UnmanagedType.U1)] bool normalize);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_hash(
        ExprHandle expr,
        ulong k0,ulong k1,ulong k2,ulong k3);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_hist(
        ExprHandle expr,
        nint bins,
        [MarshalAs(UnmanagedType.U1)] bool has_bin_count,
        nuint bin_count,
        [MarshalAs(UnmanagedType.U1)] bool include_category,
        [MarshalAs(UnmanagedType.U1)] bool include_breakpoint
    );
    [LibraryImport(LibName,StringMarshalling =StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_inspect(
        ExprHandle expr,
        string format);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_reinterpret(
        ExprHandle expr,
        [MarshalAs(UnmanagedType.U1)] bool signed);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_repeat(
        ExprHandle value,
        ExprHandle n);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_repeat_by(
        ExprHandle expr,
        ExprHandle by);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_cov(
        ExprHandle a,
        ExprHandle b,
        byte ddof);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_pearson_corr(
        ExprHandle a,
        ExprHandle b
    );
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_spearman_rank_corr(
        ExprHandle a,
        ExprHandle b,
        [MarshalAs(UnmanagedType.U1)] bool propagateNans
    );
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_rolling_corr(
        ExprHandle x,
        ExprHandle y,
        uint windowSize,
        uint minPeriods,
        byte ddof
    );
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_rolling_cov(
        ExprHandle x,
        ExprHandle y,
        uint windowSize,
        uint minPeriods,
        byte ddof
    );

    
    
}