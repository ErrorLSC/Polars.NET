using System.Runtime.InteropServices;
using Apache.Arrow.C;
using Polars.NET.Core.Helpers;

namespace Polars.NET.Core.Native;

unsafe internal partial class NativeBindings
{
    // --- Series Lifecycle ---
    [LibraryImport(LibName)]
    public static partial void pl_series_free(IntPtr ptr);
    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_series_rechunk(SeriesHandle handle);
    [LibraryImport(LibName)]
    public static partial void pl_series_shrink_to_fit(SeriesHandle df);
    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.U1)]
    public static partial bool pl_series_chunk_count(SeriesHandle handle, out uint count);
    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.U1)]
    public static partial bool pl_series_approx_n_unique(
        SeriesHandle series, 
        out uint count
    );
    // --- Series Getters ---
    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.I1)]
    public static partial bool pl_series_get_i64(SeriesHandle s, UIntPtr idx, out long val,[MarshalAs(UnmanagedType.U1)] out bool isNull);
    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.I1)]
    public static partial bool pl_series_get_i128(SeriesHandle s, UIntPtr idx, out Int128 val,[MarshalAs(UnmanagedType.U1)] out bool isNull);
    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.I1)]
    public static partial bool pl_series_get_u128(SeriesHandle series, UIntPtr idx, out UInt128 val,[MarshalAs(UnmanagedType.U1)] out bool isNull);
    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.I1)]
    public static partial bool pl_series_get_f64(SeriesHandle s, UIntPtr idx, out double val,[MarshalAs(UnmanagedType.U1)] out bool isNull);

    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.U1)] 
    public static partial bool pl_series_get_bool(
        SeriesHandle series, 
        nuint idx, 
        [MarshalAs(UnmanagedType.U1)] out bool val,    
        [MarshalAs(UnmanagedType.U1)] out bool isNull  
    );

    [LibraryImport(LibName)]
    public static partial IntPtr pl_series_get_str(SeriesHandle s, UIntPtr idx);

    // Decimal: out Int128, out UIntPtr (scale)
    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.U1)]
    public static partial bool pl_series_get_decimal(
        SeriesHandle series, 
        nuint idx, 
        out Int128 val,         
        out nuint precision, 
        out nuint scale, 
        [MarshalAs(UnmanagedType.U1)] out bool isNull
    );
    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.I1)]
    public static partial bool pl_series_get_date(SeriesHandle s, nuint idx, out int val,[MarshalAs(UnmanagedType.U1)] out bool isNull);

    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.I1)]
    public static partial bool pl_series_get_time(SeriesHandle s, UIntPtr idx, out long val,[MarshalAs(UnmanagedType.U1)] out bool isNull);

    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.U1)]
    public static partial bool pl_series_get_datetime(
        SeriesHandle series, 
        nuint idx, 
        out long val, 
        out PlTimeUnit timeUnit, 
        out IntPtr timezone, 
        [MarshalAs(UnmanagedType.U1)] out bool isNull
    );

    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.I1)]
    public static partial bool pl_series_get_duration(SeriesHandle s, UIntPtr idx, out long val, out PlTimeUnit timeUnit, [MarshalAs(UnmanagedType.U1)] out bool isNull);
    // --- Series Constructors ---
    // DataFrame -> Series (ByName)
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial SeriesHandle pl_dataframe_get_column(DataFrameHandle df, string name);
    // DataFrame -> Series (ByIndex)
    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_dataframe_get_column_at(DataFrameHandle df, UIntPtr index);
    // Series -> DataFrame
    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_series_to_frame(SeriesHandle s);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial SeriesHandle pl_series_new_i8(string name, ref sbyte ptr, ref byte validity, UIntPtr len);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial SeriesHandle pl_series_new_u8(string name, ref byte ptr, ref byte validity, UIntPtr len);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial SeriesHandle pl_series_new_i16(string name, ref short ptr, ref byte validity, UIntPtr len);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial SeriesHandle pl_series_new_u16(string name, ref ushort ptr, ref byte validity, UIntPtr len);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial SeriesHandle pl_series_new_i32(string name, ref int ptr, ref byte validity, UIntPtr len);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8,EntryPoint = "pl_series_new_u32")]
    public static partial SeriesHandle pl_series_new_u32(string name, ref uint ptr, ref byte validity, UIntPtr len);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial SeriesHandle pl_series_new_i64(string name, ref long ptr, ref byte validity, UIntPtr len);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial SeriesHandle pl_series_new_u64(string name, ref ulong ptr, ref byte validity, UIntPtr len);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial SeriesHandle pl_series_new_i128(string name,ref Int128 ptr, ref byte validity, UIntPtr len);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial SeriesHandle pl_series_new_u128(string name, ref UInt128 ptr,ref byte validity, UIntPtr len);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial SeriesHandle pl_series_new_f16(string name, ref Half ptr, ref byte validity, UIntPtr len);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial SeriesHandle pl_series_new_f32(string name, ref float ptr, ref byte validity, UIntPtr len);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial SeriesHandle pl_series_new_f64(string name, ref double ptr, ref byte validity, UIntPtr len);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial SeriesHandle pl_series_new_bool(
        string name, 
        ref byte data,       
        ref byte validity,  
        UIntPtr len
    );

    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)]
    public static partial SeriesHandle pl_series_new_str_simd(
        string name,
        ref byte values_ptr,           
        nuint values_len,           
        ref ArrowStringView views_ptr, 
        ref byte validity_ptr,         
        nuint len                   
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial SeriesHandle pl_series_new_datetime(
        string name,
        ref long ptr,
        ref byte validity,
        UIntPtr len,
        PlTimeUnit unit, // 0=ns, 1=us, 2=ms
        string? zone
    );
    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_series_new_date(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name,
        ref int ptr,        // Int32
        ref byte validity,
        UIntPtr len
    );
    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_series_new_time(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name,
        ref long ptr,       // Int64
        ref byte validity,
        UIntPtr len
    );
    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_series_new_duration(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name,
        ref long ptr,       // Int64
        ref byte validity,
        UIntPtr len,
        PlTimeUnit unit           // 0=ns, 1=us, 2=ms
    );
    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_series_new_decimal(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name,
        ref Int128 ptr,   
        ref byte validity,
        UIntPtr len,
        UIntPtr precision,
        UIntPtr scale
    );
    // =================================================================
    // FixedSizeList (Array) Bindings
    // Rust: impl_fixed_list_ffi!
    // Paras: name, flat_ptr, flat_len, validity, parent_len, width
    // =================================================================

    #region Signed Integers (i8, i16, i32, i64)

    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_series_new_array_i8(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name,
        ref sbyte flat_ptr,
        UIntPtr flat_len,
        ref byte validity,
        UIntPtr parent_len,
        UIntPtr width
    );

    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_series_new_array_i16(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name,
        ref short flat_ptr,
        UIntPtr flat_len,
        ref byte validity,
        UIntPtr parent_len,
        UIntPtr width
    );

    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_series_new_array_i32(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name,
        ref int flat_ptr,
        UIntPtr flat_len,
        ref byte validity,
        UIntPtr parent_len,
        UIntPtr width
    );

    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_series_new_array_i64(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name,
        ref long flat_ptr,
        UIntPtr flat_len,
        ref byte validity,
        UIntPtr parent_len,
        UIntPtr width
    );

    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_series_new_array_i128(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name,
        ref Int128 flat_ptr,
        UIntPtr flat_len,
        ref byte validity,
        UIntPtr parent_len,
        UIntPtr width
    );

    #endregion

    #region Unsigned Integers (u8, u16, u32, u64)

    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_series_new_array_u8(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name,
        ref byte flat_ptr,
        UIntPtr flat_len,
        ref byte validity,
        UIntPtr parent_len,
        UIntPtr width
    );

    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_series_new_array_u16(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name,
        ref ushort flat_ptr,
        UIntPtr flat_len,
        ref byte validity,
        UIntPtr parent_len,
        UIntPtr width
    );

    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_series_new_array_u32(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name,
        ref uint flat_ptr,
        UIntPtr flat_len,
        ref byte validity,
        UIntPtr parent_len,
        UIntPtr width
    );

    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_series_new_array_u64(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name,
        ref ulong flat_ptr,
        UIntPtr flat_len,
        ref byte validity,
        UIntPtr parent_len,
        UIntPtr width
    );
    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_series_new_array_u128(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name,
        ref UInt128 flat_ptr,
        UIntPtr flat_len,
        ref byte validity,
        UIntPtr parent_len,
        UIntPtr width
    );

    #endregion

    #region Floats (f16, f32, f64) and Decimal
    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_series_new_array_f16(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name,
        ref Half flat_ptr,
        UIntPtr flat_len,
        ref byte validity,
        UIntPtr parent_len,
        UIntPtr width
    );

    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_series_new_array_f32(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name,
        ref float flat_ptr,
        UIntPtr flat_len,
        ref byte validity,
        UIntPtr parent_len,
        UIntPtr width
    );

    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_series_new_array_f64(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name,
        ref double flat_ptr,
        UIntPtr flat_len,
        ref byte validity,
        UIntPtr parent_len,
        UIntPtr width
    );
    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_series_new_array_decimal(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name,
        ref Int128 flat_ptr,   // Rust: *const i128
        UIntPtr flat_len,   // Rust: usize
        ref byte validity,    // Rust: *const u8
        UIntPtr parent_len, // Rust: usize
        UIntPtr width,      // Rust: usize
        UIntPtr scale       // Rust: usize (Extra param for Decimal)
    );
    #endregion
    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_series_new_struct(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name, 
        ref IntPtr fields, 
        nuint len
    );
    [LibraryImport(LibName)] 
    public static partial SeriesHandle pl_series_clone(SeriesHandle s);
    // --- Series Properties ---
    [LibraryImport(LibName)]
    public static partial IntPtr pl_series_dtype_str(SeriesHandle s);
    [LibraryImport(LibName)]
    public static partial DataTypeHandle pl_series_get_dtype(SeriesHandle handle);
    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.U1)]
    public static partial bool pl_series_len(SeriesHandle h, out uint len);

    [LibraryImport(LibName)]
    public static partial IntPtr pl_series_name(SeriesHandle h);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    [return: MarshalAs(UnmanagedType.U1)]
    public static partial bool pl_series_rename(
        SeriesHandle series, 
        string name
    );
    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_series_is_null(SeriesHandle s);

    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_series_is_not_null(SeriesHandle s);
    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.I1)]
    public static partial bool pl_series_is_null_at(SeriesHandle s,nuint idx,[MarshalAs(UnmanagedType.I1)] out bool IsNull);
    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_series_drop_nulls(SeriesHandle s);
    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.I1)]
    public static partial bool pl_series_null_count(SeriesHandle s, out uint count);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_is_nan(SeriesHandle s);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_is_not_nan(SeriesHandle s);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_is_finite(SeriesHandle s);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_is_infinite(SeriesHandle s);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_unique(SeriesHandle series);

    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_unique_stable(SeriesHandle series);
    
    [LibraryImport(LibName)] 
    [return: MarshalAs(UnmanagedType.U1)]
    public static partial bool pl_series_n_unique(SeriesHandle series, out uint count);
    // --- Series Ops ---
    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_series_slice(
        SeriesHandle series, 
        long offset, 
        UIntPtr length
    );
    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_series_take(
        SeriesHandle series, 
        SeriesHandle indices
    ); 
    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.I1)] 
    public static partial bool pl_series_append(SeriesHandle s_ptr, SeriesHandle other_ptr);
    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.I1)] 
    public static partial bool pl_series_extend(SeriesHandle s_ptr, SeriesHandle other_ptr);
    // --- Series Cast ---
    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_series_cast(
        SeriesHandle ptr, 
        DataTypeHandle dtype_ptr, 
        [MarshalAs(UnmanagedType.U1)] bool strict, 
        [MarshalAs(UnmanagedType.U1)] bool wrap_numerical
    );
    // Arithmetic
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_add(SeriesHandle s1, SeriesHandle s2);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_sub(SeriesHandle s1, SeriesHandle s2);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_mul(SeriesHandle s1, SeriesHandle s2);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_div(SeriesHandle s1, SeriesHandle s2);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_rem(SeriesHandle s1, SeriesHandle s2);

    // Comparison
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_eq(SeriesHandle s1, SeriesHandle s2);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_eq_missing(SeriesHandle s1, SeriesHandle s2);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_neq(SeriesHandle s1, SeriesHandle s2);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_neq_missing(SeriesHandle s1, SeriesHandle s2);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_gt(SeriesHandle s1, SeriesHandle s2);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_lt(SeriesHandle s1, SeriesHandle s2);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_gt_eq(SeriesHandle s1, SeriesHandle s2);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_lt_eq(SeriesHandle s1, SeriesHandle s2);

    // Aggregation
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_sum(SeriesHandle s);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_mean(SeriesHandle s);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_min(SeriesHandle s);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_max(SeriesHandle s);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_not(SeriesHandle s);
    // --- Arrow Export ---
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)]
    public static partial SeriesHandle pl_arrow_to_series(
        string name,
        CArrowArray* cArray,
        CArrowSchema* cSchema
    );
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_sort(SeriesHandle series,
    [MarshalAs(UnmanagedType.U1)] bool descending,
    [MarshalAs(UnmanagedType.U1)] bool nulls_last,
    [MarshalAs(UnmanagedType.U1)] bool multithreaded,
    [MarshalAs(UnmanagedType.U1)] bool maintain_order);
    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_series_struct_unnest(SeriesHandle series);
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataFrameHandle pl_series_value_counts(SeriesHandle series,
    [MarshalAs(UnmanagedType.U1)] bool sort,
    [MarshalAs(UnmanagedType.U1)] bool parallel,
    string name,
    [MarshalAs(UnmanagedType.U1)] bool normalize);
    [LibraryImport(LibName)]
    public static partial int pl_series_is_sorted(
        SeriesHandle h,
        [MarshalAs(UnmanagedType.U1)] bool descending,
        [MarshalAs(UnmanagedType.U1)] bool nullsLast,
        [MarshalAs(UnmanagedType.U1)] out bool flag);
    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_series_set_sorted_flag(
        SeriesHandle h,
        [MarshalAs(UnmanagedType.U1)] bool descending);
        // [MarshalAs(UnmanagedType.U1)] bool nullsLast);
    [LibraryImport(LibName)]
    public static partial int pl_series_get_sorted_flags(
        SeriesHandle h,
        out PlSortStateFlags flags);
    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_series_set_with_mask(
        SeriesHandle series,
        SeriesHandle mask,
        SeriesHandle value
    );
    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_series_scatter_indices(
        SeriesHandle series,
        SeriesHandle index,
        SeriesHandle value
    );

    // --- Arrow Export ---
    [LibraryImport(LibName)]
    public static partial ArrowArrayContextHandle pl_series_to_arrow(SeriesHandle h);
    [LibraryImport(LibName)] public static partial IntPtr pl_series_to_string(SeriesHandle series);

}