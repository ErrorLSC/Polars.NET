using System.Runtime.InteropServices;

namespace Polars.Native;

public static partial class PolarsWrapper
{
    // ==========================================
    // Metadata (元数据)
    // ==========================================

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
        return ErrorHelper.Check(NativeBindings.pl_head(df, (UIntPtr)n));
    }

    public static DataFrameHandle Tail(DataFrameHandle df, uint n)
    {
        return ErrorHelper.Check(NativeBindings.pl_tail(df, (UIntPtr)n));
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
        expr.TransferOwnership();// 消耗 Expr
        return ErrorHelper.Check(h);
    }
    public static DataFrameHandle Explode(DataFrameHandle df, ExprHandle[] exprs)
    {
        var raw = HandlesToPtrs(exprs);
        return ErrorHelper.Check(NativeBindings.pl_explode(df, raw, (UIntPtr)raw.Length));
    }
    // GroupBy 封装
    public static DataFrameHandle GroupByAgg(DataFrameHandle df, ExprHandle[] by, ExprHandle[] agg)
    {
        // 转换两个数组
        var rawBy = HandlesToPtrs(by);
        var rawAgg = HandlesToPtrs(agg);
        return ErrorHelper.Check(NativeBindings.pl_groupby_agg(
            df, 
            rawBy, (UIntPtr)rawBy.Length,
            rawAgg, (UIntPtr)rawAgg.Length
        ));
    }
    // [新增] Pivot (Eager)
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

    // [新增] Unpivot (Eager)
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
    public static DataFrameHandle Concat(DataFrameHandle[] dfs)
    {
        var ptrs = HandlesToPtrs(dfs); // 转移所有权
        var h = NativeBindings.pl_concat_vertical(ptrs, (UIntPtr)ptrs.Length);
        return ErrorHelper.Check(h);
    }
}
