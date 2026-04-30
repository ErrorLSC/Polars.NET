#pragma warning disable CS1591
using Pl = Polars.CSharp.Polars;
using Cs = Polars.CSharp.Polars.Selectors;
using Polars.NET.Core.Helpers;
namespace Polars.CSharp;

/// <summary>
/// A type that can be implicitly converted into a Polars Expression.String will be parsed as column name.
/// Matches the 'IntoExprColumn' concept in Rust and Python Polars.
/// </summary>
public readonly struct IntoExprColumn
{
    private readonly Expr _expr;
    private readonly bool _ownsExpr;

    public static implicit operator IntoExprColumn(Expr expr) => new(expr, ownsExpr: false);
    public static implicit operator IntoExprColumn(Selector selector) => new(selector.ToExpr(), ownsExpr: true);
    public static implicit operator IntoExprColumn(string name) => new(Pl.Col(name), ownsExpr: true);
    public static implicit operator IntoExprColumn(Series series) => new(Pl.Lit(series), ownsExpr: true);
    public static implicit operator IntoExprColumn(DataType dtype) => new(Cs.ByDtype(dtype).ToExpr(), ownsExpr: true);
    public static implicit operator IntoExprColumn(Type type) => new(Cs.ByDtype(type).ToExpr(), ownsExpr: true);
    public static implicit operator IntoExprColumn(DateOnly date) => new(Pl.Lit(date), ownsExpr: true);
    public static implicit operator IntoExprColumn(DateTime dt) => new(Pl.Lit(dt), ownsExpr: true);
    public static implicit operator IntoExprColumn(TimeOnly time) => new(Pl.Lit(time), ownsExpr: true);
    public static implicit operator IntoExprColumn(TimeSpan ts) => new(Pl.Lit(ts), ownsExpr: true);
    public static implicit operator IntoExprColumn(DateTimeOffset dtoffset) => new(Pl.Lit(dtoffset), ownsExpr: true);
    public static implicit operator IntoExprColumn(sbyte sb) => new(Pl.Lit(sb), ownsExpr: true);
    public static implicit operator IntoExprColumn(byte b) => new(Pl.Lit(b), ownsExpr: true);
    public static implicit operator IntoExprColumn(short sh) => new(Pl.Lit(sh), ownsExpr: true);
    public static implicit operator IntoExprColumn(ushort ush) => new(Pl.Lit(ush), ownsExpr: true);
    public static implicit operator IntoExprColumn(uint ui) => new(Pl.Lit(ui), ownsExpr: true);
    public static implicit operator IntoExprColumn(int i) => new(Pl.Lit(i), ownsExpr: true);
    public static implicit operator IntoExprColumn(Half hf) => new(Pl.Lit(hf), ownsExpr: true);
    public static implicit operator IntoExprColumn(double d) => new(Pl.Lit(d), ownsExpr: true);
    public static implicit operator IntoExprColumn(float f) => new(Pl.Lit(f), ownsExpr: true);
    public static implicit operator IntoExprColumn(ulong ul) => new(Pl.Lit(ul), ownsExpr: true);
    public static implicit operator IntoExprColumn(long l) => new(Pl.Lit(l), ownsExpr: true);
    public static implicit operator IntoExprColumn(decimal dm) => new(Pl.Lit(dm), ownsExpr: true);
    public static implicit operator IntoExprColumn(Int128 i128) => new(Pl.Lit(i128), ownsExpr: true);
    public static implicit operator IntoExprColumn(bool bo) => new(Pl.Lit(bo), ownsExpr: true);
    public static implicit operator IntoExprColumn(int[] arr) => new(Pl.Lit(arr), ownsExpr: true);
    public static implicit operator IntoExprColumn(long[] arr) => new(Pl.Lit(arr), ownsExpr: true);
    public static implicit operator IntoExprColumn(ReadOnlySpan<int> arr) => new(Pl.Lit(arr), ownsExpr: true);
    public static implicit operator IntoExprColumn(ReadOnlySpan<long> arr) => new(Pl.Lit(arr), ownsExpr: true);
    // public static implicit operator IntoExpr(string[] arr) => new(Pl.Lit(Pl.Series("", arr)), ownsExpr: true);
    // public static implicit operator IntoExpr(UInt128 u128) => new(Pl.Lit(u128), ownsExpr: true);

    private IntoExprColumn(Expr expr, bool ownsExpr) 
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
        
        return new(_expr.CloneHandle());
    }
    public override string ToString() => _expr.ToString();
}

/// <summary>
/// A type that can be implicitly converted into a Polars Expression.String will be parsed as literal value.
/// Matches the 'IntoExpr' concept in Rust and Python Polars.
/// </summary>
public readonly struct IntoExpr
{
    private readonly Expr _expr;
    private readonly bool _ownsExpr;

    public static implicit operator IntoExpr(Expr expr) => new(expr, ownsExpr: false);
    public static implicit operator IntoExpr(string str) => new(Pl.Lit(str), ownsExpr: true);
    public static implicit operator IntoExpr(Series series) => new(Pl.Lit(series), ownsExpr: true);
    public static implicit operator IntoExpr(DateOnly date) => new(Pl.Lit(date), ownsExpr: true);
    public static implicit operator IntoExpr(DateTime dt) => new(Pl.Lit(dt), ownsExpr: true);
    public static implicit operator IntoExpr(TimeOnly time) => new(Pl.Lit(time), ownsExpr: true);
    public static implicit operator IntoExpr(TimeSpan ts) => new(Pl.Lit(ts), ownsExpr: true);
    public static implicit operator IntoExpr(DateTimeOffset dtoffset) => new(Pl.Lit(dtoffset), ownsExpr: true);
    public static implicit operator IntoExpr(sbyte sb) => new(Pl.Lit(sb), ownsExpr: true);
    public static implicit operator IntoExpr(byte b) => new(Pl.Lit(b), ownsExpr: true);
    public static implicit operator IntoExpr(short sh) => new(Pl.Lit(sh), ownsExpr: true);
    public static implicit operator IntoExpr(ushort ush) => new(Pl.Lit(ush), ownsExpr: true);
    public static implicit operator IntoExpr(uint ui) => new(Pl.Lit(ui), ownsExpr: true);
    public static implicit operator IntoExpr(int i) => new(Pl.Lit(i), ownsExpr: true);
    public static implicit operator IntoExpr(Half hf) => new(Pl.Lit(hf), ownsExpr: true);
    public static implicit operator IntoExpr(double d) => new(Pl.Lit(d), ownsExpr: true);
    public static implicit operator IntoExpr(float f) => new(Pl.Lit(f), ownsExpr: true);
    public static implicit operator IntoExpr(ulong ul) => new(Pl.Lit(ul), ownsExpr: true);
    public static implicit operator IntoExpr(long l) => new(Pl.Lit(l), ownsExpr: true);
    public static implicit operator IntoExpr(decimal dm) => new(Pl.Lit(dm), ownsExpr: true);
    public static implicit operator IntoExpr(Int128 i128) => new(Pl.Lit(i128), ownsExpr: true);
    public static implicit operator IntoExpr(bool bo) => new(Pl.Lit(bo), ownsExpr: true);

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
        
        return new(_expr.CloneHandle());
    }
    public override string ToString() => _expr.ToString();
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

public readonly struct IntoDateSeries
{
    public readonly Series DateSeries;

    public static implicit operator IntoDateSeries(ReadOnlySpan<DateOnly> datelist) => new(Pl.Series("__Date__",datelist));
    public static implicit operator IntoDateSeries(Series series) => new(series);
    public static implicit operator IntoDateSeries(Expr dateExpr) => new(Series.FromExpr(dateExpr));

    private IntoDateSeries(Series series)
    {
        if (series.DataType != DataType.Date)
            throw new ArgumentException("This series is not a date series,please try to cast it to dateonly");   
        
        DateSeries = series;
    }

    public ReadOnlySpan<int> ToPhysicalSpan() => DateSeries.DropNulls().ToPhysical().AsReadOnlySpan<int>();
    public int[] ToPhysicalArray() => DateSeries.DropNulls().ToPhysical().ToArray<int>();
}

public readonly struct DurationOrExpr
{
    internal readonly Expr Expression;

    private DurationOrExpr(Expr expr)
    {
        Expression = expr;
    }

    public static implicit operator DurationOrExpr(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new ArgumentException("Duration string cannot be null or empty.", nameof(value));
            
        return new DurationOrExpr(Pl.Lit(value));
    }

    public static implicit operator DurationOrExpr(TimeSpan timeSpan) 
        => new(Pl.Lit(timeSpan.ToPolarsDuration()));

    public static implicit operator DurationOrExpr(Expr expr) 
        => new(expr);
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
/// Schema Dictionary and Tuple are also accepted.
/// </summary>
public readonly struct IntoSchema
{
    private readonly PolarsSchema _schema;
    private readonly bool _ownsSchema;

    private IntoSchema(PolarsSchema schema, bool ownsSchema)
    {
        ArgumentNullException.ThrowIfNull(schema);
        _schema = schema;
        _ownsSchema = ownsSchema;
    }

    // ==========================================
    // 1. PolarsSchema, DataFrame, LazyFrame
    // ==========================================
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

    // ==========================================
    // 2. Dict {name: type} -> Dictionary
    // ==========================================
    public static implicit operator IntoSchema(Dictionary<string, DataType> dict)
    {
        ArgumentNullException.ThrowIfNull(dict);
        return new(PolarsSchema.From(dict), ownsSchema: true); 
    }

    // ==========================================
    // 3. List of (name, type) -> Tuple Arrays / Lists
    // ==========================================
    public static implicit operator IntoSchema((string Name, DataType Type)[] pairs)
    {
        ArgumentNullException.ThrowIfNull(pairs);
        return new(PolarsSchema.From(pairs), ownsSchema: true);
    }

    public static implicit operator IntoSchema(List<(string Name, DataType Type)> pairs)
    {
        ArgumentNullException.ThrowIfNull(pairs);
        return new(PolarsSchema.From(pairs), ownsSchema: true);
    }

    // ==========================================
    // 4. List of names (types auto-inferred) -> String Arrays / Lists
    // ==========================================
    public static implicit operator IntoSchema(string[] columnNames)
    {
        ArgumentNullException.ThrowIfNull(columnNames);
        return new(CreateSchemaFromNames(columnNames), ownsSchema: true);
    }

    public static implicit operator IntoSchema(List<string> columnNames)
    {
        ArgumentNullException.ThrowIfNull(columnNames);
        return new(CreateSchemaFromNames(columnNames), ownsSchema: true);
    }
    // ==========================================
    // 5. Apache Arrow Schema -> PolarsSchema
    // ==========================================
    public static implicit operator IntoSchema(Apache.Arrow.Schema arrowSchema)
    {
        ArgumentNullException.ThrowIfNull(arrowSchema);
        return new(PolarsSchema.FromArrowSchema(arrowSchema), ownsSchema: true);
    }
    private static PolarsSchema CreateSchemaFromNames(IEnumerable<string> names)
    {
        var schema = new PolarsSchema();
        foreach (var name in names)
        {
            schema.Add(name, DataType.Unknown); 
        }
        return schema;
    }
    public static implicit operator IntoSchema(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);
        return new(PolarsSchema.From(type), ownsSchema: true);
    }

    public PolarsSchema Consume() => _schema;

    public void DisposeTempSchema()
    {
        if (_ownsSchema)
        {
            _schema.Dispose();
        }
    }
}

