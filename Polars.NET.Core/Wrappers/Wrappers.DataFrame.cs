using System.Runtime.InteropServices;
using Apache.Arrow.C;

namespace Polars.NET.Core;

public static partial class PolarsWrapper
{
    // ==========================================
    // Metadata (元数据)
    // ==========================================
    public static long DataFrameHeight(DataFrameHandle df) => (long)NativeBindings.pl_dataframe_height(df);
    public static long DataFrameWidth(DataFrameHandle df) => (long)NativeBindings.pl_dataframe_width(df);
    public static string[] GetColumnNames(DataFrameHandle df)
    {
        long width = DataFrameWidth(df);
        var names = new string[width];
        
        for (long i = 0; i < width; i++)
        {
            IntPtr ptr = NativeBindings.pl_dataframe_get_column_name(df, (UIntPtr)i);
            if (ptr == IntPtr.Zero)
            {
                names[i] = string.Empty; // Should not happen
            }
            else
            {
                try { names[i] = Marshal.PtrToStringUTF8(ptr)?? string.Empty; }
                finally { NativeBindings.pl_free_string(ptr); }
            }
        }
        return names;
    }
    public static string GetDataFrameSchemaString(DataFrameHandle h)
    {
        var ptr = NativeBindings.pl_dataframe_schema(h);
        return ErrorHelper.CheckString(ptr);
    }
    public static DataFrameHandle CloneDataFrame(DataFrameHandle df)
    {
        return ErrorHelper.Check(NativeBindings.pl_dataframe_clone(df));
    }
    // ==========================================
    // Scalar Access (标量获取 - O(1))
    // ==========================================

    public static long? GetInt(DataFrameHandle df, string colName, long row)
    {
        if (NativeBindings.pl_dataframe_get_i64(df, colName, (UIntPtr)row, out long val))
        {
            return val;
        }
        return null; // 失败或空值返回 null
    }

    public static double? GetDouble(DataFrameHandle df, string colName, long row)
    {
        if (NativeBindings.pl_dataframe_get_f64(df, colName, (UIntPtr)row, out double val))
        {
            return val;
        }
        return null;
    }

    public static string? GetString(DataFrameHandle df, string colName, long row)
    {
        IntPtr ptr = NativeBindings.pl_dataframe_get_string(df, colName, (UIntPtr)row);
        if (ptr == IntPtr.Zero) return null; // Null or Error
        try { return Marshal.PtrToStringUTF8(ptr); }
        finally { NativeBindings.pl_free_string(ptr); }
    }
    // ==========================================
    // Eager Ops (立即执行操作)
    // ==========================================

    public static DataFrameHandle Head(DataFrameHandle df, uint n)
    {
        return ErrorHelper.Check(NativeBindings.pl_head(df, n));
    }

    public static DataFrameHandle Tail(DataFrameHandle df, uint n)
    {
        return ErrorHelper.Check(NativeBindings.pl_tail(df, n));
    }
    public static DataFrameHandle Drop(DataFrameHandle df, string name)
    {
        return ErrorHelper.Check(NativeBindings.pl_dataframe_drop(df, name));
    }

    public static DataFrameHandle Rename(DataFrameHandle df, string oldName, string newName)
    {
        return ErrorHelper.Check(NativeBindings.pl_dataframe_rename(df, oldName, newName));
    }

    public static DataFrameHandle DropNulls(DataFrameHandle df, string[]? subset)
    {
        return UseUtf8StringArray(subset ?? Array.Empty<string>(), ptrs => 
        {
            // 如果 subset 是空，传给 Rust 空数组，Rust 会处理为 drop all nulls
            // 但我们要区分 "subset=null" (all columns) 和 "subset=[]" (no columns?? logic check)
            // 根据 Rust 实现: subset.is_null || len==0 都会导致 drop any row with null in ANY column
            return ErrorHelper.Check(NativeBindings.pl_dataframe_drop_nulls(df, ptrs, (UIntPtr)ptrs.Length));
        });
    }

    public static unsafe DataFrameHandle SampleN(DataFrameHandle df, ulong n, bool replacement, bool shuffle, ulong? seed)
    {
        ulong sVal = seed ?? 0;
        ulong* sPtr = seed.HasValue ? &sVal : null;
        return ErrorHelper.Check(NativeBindings.pl_dataframe_sample_n(df, (UIntPtr)n, replacement, shuffle, sPtr));
    }

    public static unsafe DataFrameHandle SampleFrac(DataFrameHandle df, double frac, bool replacement, bool shuffle, ulong? seed)
    {
        ulong sVal = seed ?? 0;
        ulong* sPtr = seed.HasValue ? &sVal : null;
        return ErrorHelper.Check(NativeBindings.pl_dataframe_sample_frac(df, frac, replacement, shuffle, sPtr));
    }
    public static DataFrameHandle Filter(DataFrameHandle df, ExprHandle expr)
    {
        var h = NativeBindings.pl_filter(df, expr);
        expr.TransferOwnership(); // Expr 被消耗了
        return ErrorHelper.Check(h);
    }
    public static DataFrameHandle WithColumns(DataFrameHandle df, ExprHandle[] exprs)
    {
        var raw = HandlesToPtrs(exprs);
        return ErrorHelper.Check(NativeBindings.pl_with_columns(df, raw, (UIntPtr)raw.Length));
    }
    public static DataFrameHandle Select(DataFrameHandle df, ExprHandle[] exprs)
    {
        var rawExprs = HandlesToPtrs(exprs);
        return ErrorHelper.Check(NativeBindings.pl_select(df, rawExprs, (UIntPtr)rawExprs.Length));
    }

    public static DataFrameHandle Join(DataFrameHandle left, DataFrameHandle right, ExprHandle[] leftOn, ExprHandle[] rightOn, PlJoinType how)
    {
        var lPtrs = HandlesToPtrs(leftOn);
        var rPtrs = HandlesToPtrs(rightOn);
        return ErrorHelper.Check(NativeBindings.pl_join(left, right, lPtrs, (UIntPtr)lPtrs.Length, rPtrs, (UIntPtr)rPtrs.Length, how));
    }
    public static DataFrameHandle Sort(DataFrameHandle df, ExprHandle expr, bool descending)
    {
        var h = NativeBindings.pl_sort(df, expr, descending);
        expr.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static DataFrameHandle Explode(DataFrameHandle df, ExprHandle[] exprs)
    {
        var raw = HandlesToPtrs(exprs);
        return ErrorHelper.Check(NativeBindings.pl_explode(df, raw, (UIntPtr)raw.Length));
    }
    public static DataFrameHandle Unnest(DataFrameHandle df, string[] columns)
    {
        // 记得把 int Length 转为 UIntPtr
        return ErrorHelper.Check(NativeBindings.pl_dataframe_unnest(df, columns, (UIntPtr)columns.Length));
    }
    // GroupBy
    public static DataFrameHandle GroupByAgg(DataFrameHandle df, ExprHandle[] by, ExprHandle[] agg)
    {
        var rawBy = HandlesToPtrs(by);
        var rawAgg = HandlesToPtrs(agg);
        return ErrorHelper.Check(NativeBindings.pl_groupby_agg(
            df, 
            rawBy, (UIntPtr)rawBy.Length,
            rawAgg, (UIntPtr)rawAgg.Length
        ));
    }
    // Pivot (Eager)
    public static DataFrameHandle Pivot(DataFrameHandle df, string[] index, string[] columns, string[] values, PlPivotAgg aggFn)
    {
        // 三层嵌套稍微有点丑，但能复用 UseUtf8StringArray 的安全机制
        return UseUtf8StringArray(index, iPtrs =>
            UseUtf8StringArray(columns, cPtrs =>
                UseUtf8StringArray(values, vPtrs =>
                {
                    return ErrorHelper.Check(NativeBindings.pl_pivot(
                        df,
                        vPtrs, (UIntPtr)vPtrs.Length,
                        iPtrs, (UIntPtr)iPtrs.Length,
                        cPtrs, (UIntPtr)cPtrs.Length,
                        aggFn
                    ));
                })
            )
        );
    }

    // Unpivot (Eager)
    public static DataFrameHandle Unpivot(DataFrameHandle df, string[] index, string[] on, string? variableName, string? valueName)
    {
        return UseUtf8StringArray(index, iPtrs =>
            UseUtf8StringArray(on, oPtrs =>
            {
                return ErrorHelper.Check(NativeBindings.pl_unpivot(
                    df,
                    iPtrs, (UIntPtr)iPtrs.Length,
                    oPtrs, (UIntPtr)oPtrs.Length,
                    variableName,
                    valueName
                ));
            })
        );
    }
    public static DataFrameHandle Concat(DataFrameHandle[] handles, PlConcatType how)
    {
        var ptrs = HandlesToPtrs(handles);
        
        // 这里的 int 转换对应 Rust 的 how
        var h = NativeBindings.pl_concat(ptrs, (UIntPtr)ptrs.Length, how);

        // Rust 接管并消耗了所有 DF
        foreach (var handle in handles)
        {
            handle.TransferOwnership();
        }

        return ErrorHelper.Check(h);
    }
    public static SeriesHandle DataFrameGetColumn(DataFrameHandle h, string name)
    {
        var sh = NativeBindings.pl_dataframe_get_column(h, name);
        if (sh.IsInvalid)
        {
            // 这里抛异常比较好，F# 层可以 try-catch 或者我们提供 TryGetColumn
            throw new ArgumentException($"Column '{name}' not found in DataFrame.");
        }
        return sh;
    }

    // 按索引获取
    public static SeriesHandle DataFrameGetColumnAt(DataFrameHandle h, int index)
    {
        var sh = NativeBindings.pl_dataframe_get_column_at(h, (UIntPtr)index);
        if (sh.IsInvalid)
        {
            throw new IndexOutOfRangeException($"Column index {index} is out of bounds.");
        }
        return sh;
    }
    public static DataFrameHandle DataFrameNew(SeriesHandle[] series)
    {
        if (series == null || series.Length == 0)
        {
            // 传空数组给 Rust，或者直接报错，视需求而定
            // 这里传空数组，Rust 会返回空 DF
            return ErrorHelper.Check(NativeBindings.pl_dataframe_new(Array.Empty<IntPtr>(), UIntPtr.Zero));
        }

        // 提取原始指针 (Borrow, not Consume)
        var ptrs = new IntPtr[series.Length];
        for (int i = 0; i < series.Length; i++)
        {
            // DangerousGetHandle 不会转移所有权，仅仅是读取
            ptrs[i] = series[i].DangerousGetHandle();
        }

        // 调用 Native
        // 注意：ptrs 数组会被 Pin 住传给 Rust
        return ErrorHelper.Check(NativeBindings.pl_dataframe_new(ptrs, (UIntPtr)series.Length));
    }
    /// <summary>
    /// Create a DataFrame from an Arrow C Stream.
    /// </summary>
    public static unsafe DataFrameHandle DataFrameNewFromStream(Arrow.CArrowArrayStream* stream)
    {
        var handle = NativeBindings.pl_dataframe_new_from_stream(stream);
        return ErrorHelper.Check(handle);
    }
    /// <summary>
    /// Convert DataFrame to LazyFrame
    /// </summary>
    /// <param name="df"></param>
    /// <returns></returns>
    public static LazyFrameHandle DataFrameToLazy(DataFrameHandle df)
    {
        // 调用 Rust，返回一个新的 LazyFrameHandle
        // 原来的 df 不会被 Dispose (因为 Rust 端做了 clone)
        return ErrorHelper.Check(NativeBindings.pl_dataframe_lazy(df));
    }
    public static string DataFrameToString(DataFrameHandle handle)
    {
        var ptr = NativeBindings.pl_dataframe_to_string(handle);
        try
        {
            // 将非托管 UTF8 指针转为 C# String
            return Marshal.PtrToStringUTF8(ptr) ?? "";
        }
        finally
        {
            // 释放 Rust 分配的内存
            NativeBindings.pl_free_string(ptr);
        }
    }
}
