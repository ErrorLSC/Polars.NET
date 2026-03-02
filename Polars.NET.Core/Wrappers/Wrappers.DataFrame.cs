using Polars.NET.Core.Native;

namespace Polars.NET.Core;

public static partial class PolarsWrapper
{
    // ==========================================
    // Metadata
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
            
            names[i] = ErrorHelper.CheckString(ptr);
        }
        return names;
    }
    public static SchemaHandle GetDataFrameSchema(DataFrameHandle handle)
        =>ErrorHelper.Check(NativeBindings.pl_dataframe_get_schema(handle));
    public static DataFrameHandle CloneDataFrame(DataFrameHandle df)
    {
        return ErrorHelper.Check(NativeBindings.pl_dataframe_clone(df));
    }
    // ==========================================
    // Scalar Access
    // ==========================================

    public static long? GetInt(DataFrameHandle df, string colName, long row)
    {
        if (NativeBindings.pl_dataframe_get_i64(df, colName, (UIntPtr)row, out long val))
        {
            return val;
        }
        return null;
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
        return ErrorHelper.CheckString(ptr);
    }
    // ==========================================
    // Eager Ops
    // ==========================================

    public static DataFrameHandle Head(DataFrameHandle df, uint n)
        => ErrorHelper.Check(NativeBindings.pl_head(df, n));

    public static DataFrameHandle Tail(DataFrameHandle df, uint n)
        => ErrorHelper.Check(NativeBindings.pl_tail(df, n));
    public static DataFrameHandle Slice(DataFrameHandle df, long offset,ulong length)
        => ErrorHelper.Check(NativeBindings.pl_dataframe_slice(df,offset,(UIntPtr)length));
    public static DataFrameHandle Drop(DataFrameHandle df, string name)
        => ErrorHelper.Check(NativeBindings.pl_dataframe_drop(df, name));
    public static DataFrameHandle DataFrameUniqueStable(
        DataFrameHandle dfHandle, 
        string[]? subset, 
        PlUniqueKeepStrategy keep,
        (long offset, ulong len)? slice)
    {
        // Slice handling
        byte sliceValid = 0;
        long offset = 0;
        ulong len = 0;

        if (slice.HasValue)
        {
            sliceValid = 1;
            offset = slice.Value.offset;
            len = slice.Value.len;
        }

        UIntPtr subLen = subset == null ? UIntPtr.Zero : (UIntPtr)subset.Length;

        return NativeBindings.pl_df_unique_stable(
            dfHandle,
            subset,
            subLen,
            keep, 
            offset,
            (UIntPtr)len,
            sliceValid
        );
    }

    public static DataFrameHandle Rename(DataFrameHandle df, string oldName, string newName)
        => ErrorHelper.Check(NativeBindings.pl_dataframe_rename(df, oldName, newName));
    public static DataFrameHandle Rename(DataFrameHandle df, string[] oldNames, string[] newNames)
    {
    if (oldNames.Length != newNames.Length)
    {
        throw new ArgumentException("The lengths of oldNames and newNames must be identical.");
    }

    return ErrorHelper.Check(NativeBindings.pl_dataframe_rename_many(df, oldNames, newNames, (nuint)oldNames.Length));
    }
    public static DataFrameHandle DropNulls(DataFrameHandle df, string[]? subset)
    {
        return UseUtf8StringArray(subset ?? [], ptrs => 
        {
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
        expr.TransferOwnership(); 
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

    public static DataFrameHandle Join(
        DataFrameHandle left, 
        DataFrameHandle right, 
        ExprHandle[] leftOn, 
        ExprHandle[] rightOn, 
        PlJoinType how,
        string? suffix,
        PlJoinValidation validation,
        PlJoinCoalesce coalesce,
        PlJoinMaintainOrder maintainOrder,
        PlJoinSide joinSide,
        bool nullsEqual,
        long? sliceOffset,
        ulong sliceLen)
    {
        var lPtrs = HandlesToPtrs(leftOn);
        var rPtrs = HandlesToPtrs(rightOn);

        unsafe 
        {
            long offsetVal = sliceOffset.GetValueOrDefault();
            IntPtr offsetPtr = sliceOffset.HasValue ? (IntPtr)(&offsetVal) : IntPtr.Zero;

            return ErrorHelper.Check(NativeBindings.pl_join(
                left, 
                right, 
                lPtrs, (UIntPtr)lPtrs.Length, 
                rPtrs, (UIntPtr)rPtrs.Length, 
                how,
                suffix,         
                validation,
                coalesce,
                maintainOrder,
                joinSide,
                nullsEqual,
                offsetPtr,      
                (UIntPtr)sliceLen
            ));
        }
    }
    public static DataFrameHandle DataFrameSort(
        DataFrameHandle df, 
        ExprHandle[] exprs, 
        bool[] descending,
        bool[] nullsLast,
        bool maintainOrder)
    {
        if ((descending.Length != 1 && descending.Length != exprs.Length) ||
            (nullsLast.Length != 1 && nullsLast.Length != exprs.Length))
        {
                throw new ArgumentException("Sort options length mismatch.");
        }

        var exprPtrs = HandlesToPtrs(exprs);

        unsafe
        {
            fixed (bool* descPtr = descending)
            fixed (bool* nullsPtr = nullsLast)
            {
                return ErrorHelper.Check(NativeBindings.pl_dataframe_sort(
                    df,
                    exprPtrs,
                    (UIntPtr)exprs.Length,
                    descPtr,
                    (UIntPtr)descending.Length,
                    nullsPtr,
                    (UIntPtr)nullsLast.Length,
                    maintainOrder
                ));
            }
        }
    }
    public static DataFrameHandle Explode(DataFrameHandle df, SelectorHandle selector, bool emptyAsNull,bool keepNulls)
    {
       var h = NativeBindings.pl_dataframe_explode(df,selector, emptyAsNull,keepNulls);
       selector.TransferOwnership();
       return ErrorHelper.Check(h);
    } 
    public static DataFrameHandle Unnest(DataFrameHandle df, string[] columns,string? separator)
        => ErrorHelper.Check(NativeBindings.pl_dataframe_unnest(df, columns, (UIntPtr)columns.Length,separator));
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
    public static DataFrameHandle Pivot(
        DataFrameHandle df, 
        SelectorHandle index, 
        SelectorHandle columns,
        SelectorHandle values,
        ExprHandle? aggExpr, 
        PlPivotAgg aggFn,
        bool sortColumns,
        bool maintainOrder,
        string? separator)
    {
        IntPtr aggExprHandle = aggExpr?.TransferOwnership() ?? IntPtr.Zero;

        var handle = NativeBindings.pl_dataframe_pivot(
            df,
            columns,       
            index,         
            values,        
            aggExprHandle,
            aggFn,
            maintainOrder,
            sortColumns,
            separator
        );

        index.TransferOwnership();
        columns.TransferOwnership();
        values.TransferOwnership();

        return ErrorHelper.Check(handle);
    }

    // Unpivot (Eager)
    public static DataFrameHandle Unpivot(DataFrameHandle df, SelectorHandle index, SelectorHandle? on, string? variableName, string? valueName)
    {
        var h = NativeBindings.pl_unpivot(df,index,on,variableName,valueName);
        index.TransferOwnership();
        on?.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static DataFrameHandle Concat(
        DataFrameHandle[] handles, 
        PlConcatType how, 
        bool checkDuplicates,
        bool strict,
        bool unitLengthAsScalar)
    {
        var ptrs = HandlesToPtrs(handles);
        
        var h = NativeBindings.pl_dataframe_concat(ptrs, (UIntPtr)ptrs.Length, how,checkDuplicates,strict,unitLengthAsScalar);

        foreach (var handle in handles)
        {
            handle.TransferOwnership();
        }

        return ErrorHelper.Check(h);
    }
    // ==========================================
    // Stack Ops
    // ==========================================

    public static DataFrameHandle HStack(DataFrameHandle df, SeriesHandle[] columns)
    {
        if (columns == null || columns.Length == 0)
        {
            // If no columns are provided, effectively return a clone of the original
            return CloneDataFrame(df);
        }

        using var locker = new SafeHandleLock<SeriesHandle>(columns);
        
        return ErrorHelper.Check(NativeBindings.pl_hstack(
            df, 
            locker.Pointers, 
            (UIntPtr)columns.Length
        ));
    }

    public static DataFrameHandle VStack(DataFrameHandle df, DataFrameHandle other)
        => ErrorHelper.Check(NativeBindings.pl_vstack(df, other));


    public static SeriesHandle DataFrameGetColumn(DataFrameHandle h, string name)
    {
        var sh = NativeBindings.pl_dataframe_get_column(h, name);
        if (sh.IsInvalid)
        {
            throw new ArgumentException($"Column '{name}' not found in DataFrame.");
        }
        return sh;
    }

    // Get by Index
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
                return ErrorHelper.Check(NativeBindings.pl_dataframe_new(Array.Empty<IntPtr>(), UIntPtr.Zero));
            }

        using var locker = new SafeHandleLock<SeriesHandle>(series);

        return ErrorHelper.Check(NativeBindings.pl_dataframe_new(locker.Pointers, (UIntPtr)series.Length));
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
        => ErrorHelper.Check(NativeBindings.pl_dataframe_lazy(df));
    public static string DataFrameToString(DataFrameHandle handle)
    {
        var ptr = NativeBindings.pl_dataframe_to_string(handle);
        return ErrorHelper.CheckString(ptr);
    }
}
