#pragma warning disable CS1591
using Pl = Polars.CSharp.Polars;
using Cs = Polars.CSharp.Polars.Selectors;
using Polars.NET.Core;
using Polars.NET.Core.Helpers;
namespace Polars.CSharp;

/// <summary>
/// A type that can be implicitly converted into a Polars Expression.
/// Matches the 'IntoExpr' concept in Rust and Python Polars.
/// </summary>
public readonly struct IntoExpr
{
    private readonly Expr _expr;
    private readonly bool _ownsExpr;

    public static implicit operator IntoExpr(Expr expr) => new(expr, ownsExpr: false);

    public static implicit operator IntoExpr(Selector selector) => new(selector.ToExpr(), ownsExpr: true);
    public static implicit operator IntoExpr(string name) => new(Pl.Col(name), ownsExpr: true);
    public static implicit operator IntoExpr(Series series) => new(Pl.Lit(series), ownsExpr: true);
    public static implicit operator IntoExpr(DataType dtype) => new(Cs.ByDtype(dtype).ToExpr(), ownsExpr: true);
    public static implicit operator IntoExpr(Type type) => new(Cs.ByDtype(type).ToExpr(), ownsExpr: true);
    public static implicit operator IntoExpr(DateOnly date) => new(Pl.Lit(date), ownsExpr: true);
    public static implicit operator IntoExpr(DateTime dt) => new(Pl.Lit(dt), ownsExpr: true);
    public static implicit operator IntoExpr(TimeOnly time) => new(Pl.Lit(time), ownsExpr: true);
    public static implicit operator IntoExpr(TimeSpan ts) => new(Pl.Lit(ts), ownsExpr: true);
    public static implicit operator IntoExpr(DateTimeOffset dtoffset) => new(Pl.Lit(dtoffset), ownsExpr: true);
    public static implicit operator IntoExpr(int i) => new(Pl.Lit(i), ownsExpr: true);
    public static implicit operator IntoExpr(double d) => new(Pl.Lit(d), ownsExpr: true);
    public static implicit operator IntoExpr(float f) => new(Pl.Lit(f), ownsExpr: true);
    public static implicit operator IntoExpr(long l) => new(Pl.Lit(l), ownsExpr: true);

    private IntoExpr(Expr expr, bool ownsExpr) 
    {
        ArgumentNullException.ThrowIfNull(expr);
        _expr = expr;
        _ownsExpr = ownsExpr;
    }

    /// <summary>
    /// Consume generated Expr
    /// </summary>
    public Expr Consume()
    {
        if (_ownsExpr) return _expr;
        
        return new Expr(PolarsWrapper.CloneExpr(_expr.Handle));
    }
}

/// <summary>
/// A type that can be implicitly converted into a Polars Selector.
/// Ideal for methods like Unique(), Drop(), etc. that expect column subsets.
/// </summary>
public readonly struct IntoSelector
{
    private readonly Selector _selector;
    private readonly bool _ownsSelector;

    public static implicit operator IntoSelector(Selector selector) => new(selector, ownsSelector: false);

    public static implicit operator IntoSelector(string name) => new(Cs.ByName(name), ownsSelector: true);
    public static implicit operator IntoSelector(DataType dtype) => new(Cs.ByDtype(dtype), ownsSelector: true);
    public static implicit operator IntoSelector(Type type) => new(Cs.ByDtype(type), ownsSelector: true);
    public static implicit operator IntoSelector(Expr expr) 
    {
        ArgumentNullException.ThrowIfNull(expr);

        if (!expr.Meta.IsColumnSelection(allowAliasing: true)) 
        {
            throw new ArgumentException(
                "Invalid conversion to Selector. A Selector must strictly be a column selection " +
                "(e.g., Pl.Col(\"name\"), Cs.Numeric(), or regex). " +
                "Mathematical computations, aggregations, or literals cannot be used as Selectors."
            );
        }

        return new(expr.ToSelector(), ownsSelector: true);
    }

    private IntoSelector(Selector selector, bool ownsSelector)
    {
        _selector = selector;
        _ownsSelector = ownsSelector;
        
    }

    public Selector Consume()
    {
        if (_ownsSelector)
        {
            return _selector;
        }
        else
        {
            return new Selector(_selector.CloneHandle());
        }
    }
}

/// <summary>
/// A union type representing a time interval. 
/// Can be implicitly converted from a Polars duration string (e.g., "1mo", "1w") or a .NET TimeSpan.
/// </summary>
public readonly struct IntoDuration
{
    public readonly string Value;

    public static implicit operator IntoDuration(string value) => new(value);
    
    public static implicit operator IntoDuration(TimeSpan timeSpan) => new(timeSpan.ToPolarsDuration());

    private IntoDuration(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new ArgumentException("Duration string cannot be null or empty.", nameof(value));
        
        Value = value;
    }
}

/// <summary>
/// A union type representing a Polars data type or type expression.
/// Can be implicitly converted from a .NET Type, a Polars DataType, or a dynamic DataTypeExpr.
/// </summary>
public readonly struct IntoDataTypeExpr
{
    private readonly DataTypeExpr _expr;
    private readonly bool _ownsExpr;

    public static implicit operator IntoDataTypeExpr(DataTypeExpr expr) => new(expr, ownsExpr: false);

    public static implicit operator IntoDataTypeExpr(DataType dtype) => new(dtype.ToDataTypeExpr(), ownsExpr: true);

    public static implicit operator IntoDataTypeExpr(Type type) 
    {
        DataType dtype = type; 
        
        return new(dtype.ToDataTypeExpr(), ownsExpr: true);
    }

    private IntoDataTypeExpr(DataTypeExpr expr, bool ownsExpr)
    {
        ArgumentNullException.ThrowIfNull(expr);
        _expr = expr;
        _ownsExpr = ownsExpr;
    }

    /// <summary>
    /// Consume generated DataTypeExpr
    /// </summary>
    public DataTypeExpr Consume()
    {
        if (_ownsExpr) 
        {
            return _expr;
        }
        else 
        {
            return _expr.Clone(); 
        }
    }
}

/// <summary>
/// A union type representing a PolarsSchema.
/// Can be implicitly converted from a PolarsSchema, DataFrame, or LazyFrame.
/// </summary>
public readonly struct IntoSchema
{
    private readonly PolarsSchema _schema;
    private readonly bool _ownsSchema;

    public static implicit operator IntoSchema(PolarsSchema schema) => new(schema, ownsSchema: false);

    public static implicit operator IntoSchema(DataFrame df) 
    {
        ArgumentNullException.ThrowIfNull(df);
        return new(df.Schema, ownsSchema: true);
    }

    public static implicit operator IntoSchema(LazyFrame lf) 
    {
        ArgumentNullException.ThrowIfNull(lf);
        return new(lf.Schema, ownsSchema: true); 
    }

    private IntoSchema(PolarsSchema schema, bool ownsSchema)
    {
        ArgumentNullException.ThrowIfNull(schema);
        _schema = schema;
        _ownsSchema = ownsSchema;
    }

    /// <summary>
    /// Consume the schema. 
    /// Note: Caller should NOT dispose the returned schema if ownsSchema is false, 
    /// but for safe iteration to extract fields, returning the reference is fine.
    /// </summary>
    public PolarsSchema Consume()
    {
        return _schema;
    }

    /// <summary>
    /// Automatically clean up if we generated a temporary schema (from DF/LF).
    /// </summary>
    public void DisposeTempSchema()
    {
        if (_ownsSchema)
        {
            _schema.Dispose();
        }
    }
}