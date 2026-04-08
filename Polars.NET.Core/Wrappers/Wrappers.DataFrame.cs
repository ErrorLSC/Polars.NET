using Polars.NET.Core.Native;

namespace Polars.NET.Core;

public readonly partial struct PolarsWrapper
{
    // ==========================================
    // Metadata
    // ==========================================
    public static long DataFrameHeight(DataFrameHandle df)
    {
        bool success = NativeBindings.pl_dataframe_height(df,out uint height);
        
        ErrorHelper.CheckBool(success); 
        
        return height;
    }
    public static long DataFrameWidth(DataFrameHandle df)
    {
        bool success = NativeBindings.pl_dataframe_width(df,out uint width);
        
        ErrorHelper.CheckBool(success); 
        
        return width;
    }
    public static long DataFrameEstimatedSize(DataFrameHandle df)
    {
        bool success = NativeBindings.pl_dataframe_estimated_size(df,out nuint size);
        
        ErrorHelper.CheckBool(success); 
        
        return (long)size;
    }
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
    public static SeriesHandle DataFrameIsDuplicated(DataFrameHandle handle)
        =>ErrorHelper.Check(NativeBindings.pl_dataframe_is_duplicated(handle));
    public static SeriesHandle DataFrameIsUnique(DataFrameHandle handle)
        =>ErrorHelper.Check(NativeBindings.pl_dataframe_is_unique(handle));
    public static DataFrameHandle CloneDataFrame(DataFrameHandle df)
        => ErrorHelper.Check(NativeBindings.pl_dataframe_clone(df));
    // ==========================================
    // Eager Ops
    // ==========================================

    public static DataFrameHandle Head(DataFrameHandle df, uint n)
        => ErrorHelper.Check(NativeBindings.pl_head(df, n));

    public static DataFrameHandle Tail(DataFrameHandle df, uint n)
        => ErrorHelper.Check(NativeBindings.pl_tail(df, n));
    public static DataFrameHandle Slice(DataFrameHandle df, long offset,ulong length)
        => ErrorHelper.Check(NativeBindings.pl_dataframe_slice(df,offset,(UIntPtr)length));
    public static DataFrameHandle Drop(DataFrameHandle df, string[] columns)
        => ErrorHelper.Check(NativeBindings.pl_dataframe_drop_many(df, columns,(nuint)columns.Length));
    public static SeriesHandle DropInPlace(DataFrameHandle df, string name)
        => ErrorHelper.Check(NativeBindings.pl_dataframe_drop_in_place(df, name));
    public static DataFrameHandle DataFrameUnique(
        DataFrameHandle dfHandle, 
        string[]? subset, 
        PlUniqueKeepStrategy keep,
        bool maintainOrder,
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

        return NativeBindings.pl_df_unique(
            dfHandle,
            subset,
            subLen,
            keep, 
            maintainOrder,
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
    public static DataFrameHandle Unnest(DataFrameHandle df, string[] columns,string? separator)
        => ErrorHelper.Check(NativeBindings.pl_dataframe_unnest(df, columns, (UIntPtr)columns.Length,separator));
    public static DataFrameHandle Explode(DataFrameHandle df, string[] columns,bool emptyAsNull,bool keepNulls)
        => ErrorHelper.Check(NativeBindings.pl_dataframe_explode(df, columns, (UIntPtr)columns.Length,emptyAsNull,keepNulls));
    
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
    public static SeriesHandle DataFrameHashRows(DataFrameHandle df, ulong? seed)
        => NativeBindings.pl_dataframe_hash_rows(df, seed ?? 0, seed.HasValue);
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
    public static void DataFrameExtend(DataFrameHandle df, DataFrameHandle other)
    {
        bool success = NativeBindings.pl_dataframe_extend(df, other);

        ErrorHelper.CheckBool(success);
    }
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
    public static DataFrameHandle DataFrameNew(ReadOnlySpan<SeriesHandle> series)
    {
        if (series.Length == 0)
        {
            return ErrorHelper.Check(NativeBindings.pl_dataframe_new([], nuint.Zero));
        }

        Span<nint> pointers = series.Length <= 512 
            ? stackalloc nint[series.Length] 
            : new nint[series.Length];

        Span<bool> locks = series.Length <= 512 
            ? stackalloc bool[series.Length] 
            : new bool[series.Length];

        using var locker = new SafeHandleSpanLock<SeriesHandle>(series, pointers, locks);

        return ErrorHelper.Check(NativeBindings.pl_dataframe_new(pointers, (nuint)series.Length));
    }
    public static unsafe DataFrameHandle DataFrameNewFromStream(Arrow.CArrowArrayStream* stream)
    {
        var handle = NativeBindings.pl_dataframe_new_from_stream(stream);
        return ErrorHelper.Check(handle);
    }
    public static DataFrameHandle DataFrameFromSchema(SchemaHandle schema,uint length)
    {
        var handle = NativeBindings.pl_dataframe_from_schema(schema,length);
        return ErrorHelper.Check(handle);
    }
    public static LazyFrameHandle DataFrameToLazy(DataFrameHandle df) 
        => ErrorHelper.Check(NativeBindings.pl_dataframe_lazy(df));
    public static string DataFrameToString(DataFrameHandle handle)
    {
        var ptr = NativeBindings.pl_dataframe_to_string(handle);
        return ErrorHelper.CheckString(ptr);
    }
    public static DataFrameHandle DataFrameRechunk(DataFrameHandle df) 
        => ErrorHelper.Check(NativeBindings.pl_dataframe_rechunk(df));
    public static void DataFrameShrinkToFit(DataFrameHandle df) 
    {
        NativeBindings.pl_dataframe_shrink_to_fit(df);
        ErrorHelper.CheckVoid();
    }
    public static DataFrameHandle DataFrameAlignChunks(DataFrameHandle df) 
        => ErrorHelper.Check(NativeBindings.pl_dataframe_align_chunks(df));
    public static DataFrameHandle[] PartitionBy(
        DataFrameHandle df, 
        string[] byCols, 
        bool maintainOrder, 
        bool includeKey)
    {
        unsafe
        {
            IntPtr arrayPtr = NativeBindings.pl_dataframe_partition_by(
                df, 
                byCols, 
                (nuint)byCols.Length, 
                maintainOrder, 
                includeKey, 
                out nuint outLen
            );

            ErrorHelper.Check(arrayPtr); 

            int len = (int)outLen;
            var handles = new DataFrameHandle[len];

            IntPtr* rawPointers = (IntPtr*)arrayPtr;
            for (int i = 0; i < len; i++)
            {
                handles[i] = new DataFrameHandle(rawPointers[i]);
            }

            NativeBindings.pl_free_ptr_array(arrayPtr, outLen);

            return handles;
        }
        ;
    }
    public static bool DataFrameEquals(DataFrameHandle df, DataFrameHandle other, bool nullEqual)
    {
        int status = NativeBindings.pl_dataframe_equals(df, other, nullEqual, out bool result);
        
        ErrorHelper.CheckStatus(status);
        
        return result;
    }
    public static void ReplaceColumnAt(DataFrameHandle df, int index, SeriesHandle series)
    {
        bool success = NativeBindings.pl_dataframe_replace_column_at(df, (UIntPtr)index, series);
        ErrorHelper.CheckBool(success);
    }

    public static void Replace(DataFrameHandle df, string name, SeriesHandle series)
    {
        bool success = NativeBindings.pl_dataframe_replace(df, name, series);
        ErrorHelper.CheckBool(success);
    }
    public static DataFrameHandle DataFrameWithRowIndex(DataFrameHandle df, string name, int? offset = null)
    {
        long rustOffset = offset ?? -1L;
        var h = NativeBindings.pl_dataframe_with_row_index(df, name, rustOffset);
        return ErrorHelper.Check(h);
    }
    public static DataFrameHandle DataFrameTranspose(DataFrameHandle df, string? keepNamesAs, string? columnName,string[]? customNames)
    {
        nuint customNamesLen = (nuint)(customNames?.Length ?? 0);

        var h = NativeBindings.pl_dataframe_transpose(
            df, 
            keepNamesAs, 
            columnName, 
            customNames, 
            customNamesLen
        );
        
        return ErrorHelper.Check(h);
    }
    public static DataFrameHandle DataFrameUpsample(DataFrameHandle df, string timeColumn, string? every,string[]? groupBy, bool maintainOrder)
    {
        nuint groupByLen = (nuint)(groupBy?.Length ?? 0);

        var h = NativeBindings.pl_dataframe_upsample(
            df, 
            timeColumn, 
            every, 
            groupBy, 
            groupByLen,
            maintainOrder
        );
        
        return ErrorHelper.Check(h);
    }
    public static DataFrameHandle DataFrameToDummies(DataFrameHandle df, string[]? columns, string? separator, bool dropFirst,bool dropNulls)
    {
        nuint columnsLen = (nuint)(columns?.Length ?? 0);

        var h = NativeBindings.pl_dataframe_to_dummies(
            df, 
            columns, 
            columnsLen, 
            separator, 
            dropFirst,
            dropNulls
        );
        
        return ErrorHelper.Check(h);
    }

}
