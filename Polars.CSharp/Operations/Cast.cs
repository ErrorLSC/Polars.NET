using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;
using Cs = Polars.CSharp.Polars.Selectors;

namespace Polars.CSharp;
public partial class LazyFrame : IDisposable, IPolarsLazyFrame
{

    /// <summary> Cast all columns to the specified target type. </summary>
    public LazyFrame Cast(IntoDataTypeExpr dtype, bool strict = true,bool wrapNumerical = false)
        => Select(Pl.All().Cast(dtype, strict,wrapNumerical));

    /// <summary> Cast a specific Expression, String, Selector, or Type to a DataType. </summary>
    public LazyFrame Cast(IntoSelector selector, IntoDataTypeExpr dtype, bool strict = true, bool wrapNumerical = false)
    {
        using var sel = selector.Consume();
        return WithColumns(sel.ToExpr().Cast(dtype, strict, wrapNumerical));
    }

    /// <summary> 
    /// Cast multiple targets using tuples. Supports String, Expr, Selector mixed with Type, DataType, or DataTypeExpr!
    /// Usage: lf.Cast( ("Age", typeof(int)), (Cs.Numeric(), DataType.Float32), (DataType.Float64, DataType.Float32) )
    /// </summary>
    public LazyFrame Cast(params (IntoSelector Selector, IntoDataTypeExpr Dtype)[] casts)
    {
        if (casts.Length == 0) return this;
        var castExprs = new IntoExprColumn[casts.Length];
        
        for (int i = 0; i < casts.Length; i++)
        {
            using var sel = casts[i].Selector.Consume();
            castExprs[i] = sel.ToExpr().Cast(casts[i].Dtype, strict: true, wrapNumerical: false); 
        }
            
        return WithColumns(castExprs);
    }
    /// <summary> 
    /// Cast columns to the types specified in a Schema, DataFrame, or LazyFrame.
    /// </summary>
    public LazyFrame Cast(IntoSchema schemaWrapper, bool strict = true, bool wrapNumerical = false)
    {
        var schema = schemaWrapper.Consume();
        
        try
        {
            if (schema == null || schema.Handle.IsInvalid || schema.Count == 0) 
                return this;

            var fields = schema.ToList();
            var castExprs = new IntoExprColumn[fields.Count];

            for (int i = 0; i < fields.Count; i++)
            {
                castExprs[i] = Pl.Col(fields[i].Name).Cast(fields[i].Type, strict, wrapNumerical);
            }

            return WithColumns(castExprs);
        }
        finally
        {
            schemaWrapper.DisposeTempSchema();
        }
    }
}
public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary> Cast all columns to the specified target type. </summary>
    public DataFrame Cast(IntoDataTypeExpr dtype, bool strict = true,bool wrapNumerical = false)
        => Select(Pl.All().Cast(dtype, strict,wrapNumerical));

    /// <summary> Cast a specific Expression, String, Selector, or Type to a DataType. </summary>
    public DataFrame Cast(IntoSelector selector, IntoDataTypeExpr dtype, bool strict = true, bool wrapNumerical = false)
    {
        using var sel = selector.Consume();
        return WithColumns(sel.ToExpr().Cast(dtype, strict, wrapNumerical));
    }

    /// <summary> 
    /// Cast multiple targets using tuples. Supports String, Expr, Selector mixed with Type, DataType, or DataTypeExpr!
    /// Usage: df.Cast( ("Age", typeof(int)), (Cs.Numeric(), DataType.Float32), (DataType.Float64, DataType.Float32) )
    /// </summary>
    public DataFrame Cast(params (IntoSelector Selector, IntoDataTypeExpr Dtype)[] casts)
    {
        if (casts.Length == 0) return this;
        var castExprs = new IntoExprColumn[casts.Length];
        
        for (int i = 0; i < casts.Length; i++)
        {
            using var sel = casts[i].Selector.Consume();
            castExprs[i] = sel.ToExpr().Cast(casts[i].Dtype, strict: true, wrapNumerical: false); 
        }
            
        return WithColumns(castExprs);
    }
    /// <summary> 
    /// Cast columns to the types specified in a Schema, DataFrame, or LazyFrame.
    /// </summary>
    public DataFrame Cast(IntoSchema schemaWrapper, bool strict = true, bool wrapNumerical = false)
    {
        var schema = schemaWrapper.Consume();
        
        try
        {
            if (schema == null || schema.Handle.IsInvalid || schema.Count == 0) 
                return this;

            var fields = schema.ToList();
            var castExprs = new IntoExprColumn[fields.Count];

            for (int i = 0; i < fields.Count; i++)
            {
                castExprs[i] = Pl.Col(fields[i].Name).Cast(fields[i].Type, strict, wrapNumerical);
            }

            return WithColumns(castExprs);
        }
        finally
        {
            schemaWrapper.DisposeTempSchema();
        }
    }
}

