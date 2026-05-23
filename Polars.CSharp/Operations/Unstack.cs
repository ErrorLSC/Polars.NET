using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;
using Cs = Polars.CSharp.Polars.Selectors;
namespace Polars.CSharp;

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Unstack a long table to a wide form without doing an aggregation.
    /// This can be much faster than a pivot, because it can skip the grouping phase.
    /// </summary>
    /// <param name="step">Number of rows in the unstacked frame.</param>
    /// <param name="how">Direction of the unstack.</param>
    /// <param name="columns">Column name(s) or selector(s) to include in the operation. If set to None (default), use all columns.</param>
    /// <param name="fillValues">Fill values that don’t fit the new size with this value.</param>
    public DataFrame Unstack(int step, IntoSelector columns, UnstackDirection how = UnstackDirection.Vertical, object?[]? fillValues = null)
    {
        using var safeSelector = columns.Consume();
        string[] resolvedColumns = Cs.ExpandSelector(this, safeSelector);
        
        return UnstackInternal(step, how, resolvedColumns, fillValues);
    }

    /// <summary>
    /// Unstack specific columns by names.
    /// </summary>
    public DataFrame Unstack(int step, IEnumerable<string> columns, UnstackDirection how = UnstackDirection.Vertical, object?[]? fillValues = null)
    {
        var colsArray = columns as string[] ?? [.. columns];
        return UnstackInternal(step, how, colsArray, fillValues);
    }

    /// <summary>
    /// Unstack all columns
    /// </summary>
    public DataFrame Unstack(
        int step, 
        UnstackDirection how = UnstackDirection.Vertical, 
        object?[]? fillValues = null)
        => UnstackInternal(step, how, [], fillValues);
    private DataFrame UnstackInternal(int step, UnstackDirection how, string[] resolvedColumns, object?[]? fillValues)
    {
        // Column Selection
        DataFrame df;
        if (resolvedColumns.Length > 0)
        {
            df = Select(resolvedColumns);
        }
        else
        {
            df = Clone();
        }

        try
        {
            long height = df.Height;
            long nRows, nCols;

            if (how == UnstackDirection.Vertical)
            {
                nRows = step;
                nCols = (long)Math.Ceiling((double)height / nRows);
            }
            else
            {
                nCols = step;
                nRows = (long)Math.Ceiling((double)height / nCols);
            }

            // Padding Logic 
            long nFill = nCols * nRows - height;
            if (nFill > 0)
            {
                var fills = fillValues ?? [];
                if (fills.Length != df.Width)
                {
                    object? singleFill = fills.Length > 0 ? fills[0] : null;
                    fills = [.. Enumerable.Repeat(singleFill, (int)df.Width)];
                }

                var schema = df.Schema; 
                var extendExprs = new Expr[df.Width];

                for (int i = 0; i < df.Width; i++)
                {
                    string colName = df.Columns[i];
                    var colType = schema[colName]; 
                    
                    var fillExpr = Expr.MakeLit(fills[i]!).Cast(colType); 
                    var nFillExpr = Expr.MakeLit(nFill);

                    extendExprs[i] = Pl.Col(colName)
                                           .ExtendConstant(fillExpr, nFillExpr)
                                           .Alias(colName);
                }

                var oldDf = df;
                df = df.Select(extendExprs);
                oldDf.Dispose();
            }

            // Horizontal Sort
            if (how == UnstackDirection.Horizontal)
            {
                var oldDf = df;
                df = df.WithColumns(
                          (Pl.IntRange(0, nCols * nRows) % nCols).Alias("__sort_order")
                       )
                       .Sort("__sort_order")
                       .Drop("__sort_order");
                oldDf.Dispose();
            }

            // Slicing 
            int zfillVal = (int)Math.Floor(Math.Log10(nCols)) + 1;
            var slices = new List<Series>();

            foreach (var s in df.GetColumns())
            {
                for (int sliceNbr = 0; sliceNbr < nCols; sliceNbr++)
                {
                    var slice = s.Slice(sliceNbr * nRows, (ulong?)nRows);
                    slice.Name = $"{s.Name}_{sliceNbr.ToString().PadLeft(zfillVal, '0')}";
                    slices.Add(slice);
                }
                s.Dispose();
            }

            return new DataFrame([.. slices]);
        }
        finally
        {
            df.Dispose();
        }
    }
}