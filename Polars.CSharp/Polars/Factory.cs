#pragma warning disable CS1591
using System.Runtime.CompilerServices;
namespace Polars.CSharp;

/// <summary>
/// Polars Static Helpers
/// </summary>
public readonly partial struct Polars
{
    /// <summary>
    /// Create a DataFrame from a collection of Series.
    /// Example: Pl.DataFrame(Pl.Series("a", new[] {1, 2}), Pl.Series("b", new[] {3, 4}))
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DataFrame DataFrame(params Series[] series)
        => [.. series];

    /// <summary>
    /// Create a DataFrame from a collection of strongly-typed objects (POCOs).
    /// Example: Pl.DataFrame(studentList)
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DataFrame DataFrame<T>(IEnumerable<T> data)
        => CSharp.DataFrame.FromRows(data);

    /// <summary>
    /// Create a DataFrame from an anonymous object where properties represent columns.
    /// Example: Pl.DataFrame(new { A = new[] { 1, 2 }, B = new[] { "x", "y" } })
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DataFrame DataFrame(object columns)
        => CSharp.DataFrame.FromColumns(columns);

    /// <summary>
    /// Create a DataFrame from explicitly named column tuples.
    /// Example: Pl.DataFrame(("A", new[] { 1, 2 }), ("B", new[] { "x", "y" }))
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DataFrame DataFrame(params (string Name, object Data)[] columns)
        => CSharp.DataFrame.FromColumns(columns);

    /// <summary>
    /// Create a Series from an IEnumerable of objects, primitives, or nested lists.
    /// Example: Pl.Series("Name", list)
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Series Series<T>(string name, IEnumerable<T> data)
        => CSharp.Series.From(name, data);

    /// <summary>
    /// Create a Series directly from an array (Fast Path).
    /// Example: Pl.Series("Age", new int[] { 25, 30 })
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Series Series<T>(string name, T[] data)
        => CSharp.Series.From(name, data);

    /// <summary>
    /// Create a Series from a 2D matrix.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Series Series<T>(string name, T[,] data)
        => CSharp.Series.From(name, data);

    /// <summary>
    /// Create a Series from a ReadOnlySpan (Zero allocation path).
    /// </summary>
    public static Series Series<T>(string name, ReadOnlySpan<T> data)
        => CSharp.Series.FromSpan(name, data);

    /// <summary>
    /// Materialize a single logical Expression into a physical Series.
    /// Example: Pl.Series(Pl.Lit(42).RepeatBy(5))
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Series Series(Expr expr)
        => CSharp.Series.FromExpr(expr);
    /// <summary>
    /// UTF-8 encoded string type.
    /// </summary>
    public static DataType String => DataType.String;
    /// <summary>
    /// alias of String
    /// </summary>
    public static DataType Utf8 => DataType.String;
    /// <summary>
    /// Boolean type.
    /// </summary>
    public static DataType Boolean => DataType.Boolean;
    /// <summary>
    /// 8-bit signed integer type.
    /// </summary>
    public static DataType Int8 => DataType.Int8;
    /// <summary>
    /// 16-bit signed integer type.
    /// </summary>
    public static DataType Int16 => DataType.Int16;
    /// <summary>
    /// 32-bit signed integer type.
    /// </summary>
    public static DataType Int32 => DataType.Int32;
    /// <summary>
    /// 64-bit signed integer type.
    /// </summary>
    public static DataType Int64 => DataType.Int64;
    /// <summary>
    /// 128-bit signed integer type.
    /// </summary>
    public static DataType Int128 => DataType.Int128;
    /// <summary>
    /// 8-bit unsigned integer type.
    /// </summary>
    public static DataType UInt8 => DataType.UInt8;
    /// <summary>
    /// 16-bit unsigned integer type.
    /// </summary>
    public static DataType UInt16 => DataType.UInt16;
    /// <summary>
    /// 32-bit unsigned integer type.
    /// </summary>
    public static DataType UInt32 => DataType.UInt32;
    /// <summary>
    /// 64-bit unsigned integer type.
    /// </summary>
    public static DataType UInt64 => DataType.UInt64;
    // public static DataType UInt128 => DataType.UInt128;
    /// <summary>
    /// Data type representing a calendar date.
    /// <para>The underlying representation of this type is a 32-bit signed integer. 
    /// The integer indicates the number of days since the Unix epoch (1970-01-01). 
    /// The number can be negative to indicate dates before the epoch.</para>
    /// </summary>
    public static DataType Date => DataType.Date;
    /// <summary>
    /// Data type representing a calendar date and time of day.
    /// </summary>
    /// <param name="unit">Unit of time. Defaults to microseconds.</param>
    /// <param name="timeZone">Time zone string. When used to match dtypes, can set this to “*” to check for Datetime columns that have any (non-null) timezone.</param>
    /// <returns></returns>
    public static DataType Datetime(TimeUnit unit=TimeUnit.Microseconds,string? timeZone=null) => DataType.Datetime(unit,timeZone);
    /// <summary>
    /// Data type representing the time of day.
    /// <para>The underlying representation of this type is a 64-bit signed integer. The integer indicates the number of nanoseconds since midnight.</para>
    /// </summary>
    public static DataType Time => DataType.Time;
    /// <summary>
    /// Data type representing a time duration.
    /// <para>The underlying representation of this type is a 64-bit signed integer. 
    /// The integer indicates an amount of time units and can be negative to indicate negative time offsets.</para>
    /// </summary>
    /// <param name="unit">Unit of time. Defaults to microseconds.</param>
    /// <returns></returns>
    public static DataType Duration(TimeUnit unit=TimeUnit.Microseconds) => DataType.Duration(unit);
    /// <summary>
    /// Decimal 128-bit type with an optional precision and non-negative scale.
    /// </summary>
    /// <param name="precision">Maximum number of digits in each number.Max is 38.</param>
    /// <param name="scale">Number of digits to the right of the decimal point in each number.</param>
    /// <returns></returns>
    public static DataType Decimal(int precision=38,int scale =9) => DataType.Decimal(precision,scale);
    /// <summary>
    /// 16-bit floating point type.
    /// </summary>
    public static DataType Float16 => DataType.Float16;
    /// <summary>
    /// 32-bit floating point type.
    /// </summary>
    public static DataType Float32 => DataType.Float32;
    /// <summary>
    /// 64-bit floating point type.
    /// </summary>
    public static DataType Float64 => DataType.Float64;
    /// <summary>
    /// Variable length list type.
    /// </summary>
    /// <param name="inner">The DataType of the values within each list.</param>
    /// <returns></returns>
    public static DataType List(DataType inner) => DataType.List(inner);
    /// <summary>
    /// Fixed length list type.
    /// </summary>
    /// <param name="inner">The DataType of the values within each array.</param>
    /// <param name="shape">The shape of the arrays.</param>
    /// <returns></returns>
    public static DataType Array(DataType inner,params uint[] shape) => DataType.Array(inner,shape);
    /// <summary>
    /// Struct composite type.
    /// </summary>
    /// <param name="fields">The fields that make up the struct. Can be either a sequence of Field objects or a mapping of column names to data types.</param>
    /// <returns></returns>
    public static DataType Struct(params Field[] fields) => DataType.Struct(fields);
    /// <inheritdoc cref="Polars.Struct(CSharp.Field[])"/> 
    public static DataType Struct(IReadOnlyDictionary<string, DataType> fields) => DataType.Struct(fields);
    /// <summary>
    /// Definition of a single field within a Struct DataType.
    /// </summary>
    /// <param name="name">The name of the field within its parent Struct.</param>
    /// <param name="dtype">The DataType of the field’s values.</param>
    /// <returns></returns>
    public static Field Field(string name,DataType dtype) => new(name,dtype);
    /// <summary>
    /// A named collection of categories for Categorical.
    /// <para>Two categories are considered equal (and will use the same physical mapping of categories to strings)
    ///  if they have the same name, namespace and physical backing type, even if they are created in separate calls to Categories.</para>
    /// </summary>
    /// <param name="name">The name of this Categories. If set to None or an empty string, this refers to the global categories.</param>
    /// <param name="nameSpace">An optional namespace for this Categories. Defaults to the empty string.
    ///  If the name is empty or None indicating the global categories, the namespace must also be empty.</param>
    /// <param name="physical">The physical type used to represent the categories. Defaults to UInt32.</param>
    /// <returns></returns>
    public static Categories Categories(string? name = null, string? nameSpace = "", CategoricalPhysical physical = CategoricalPhysical.U32) 
        => new(name,nameSpace,physical);
    /// <inheritdoc cref="Polars.Categories(string?, string?, CategoricalPhysical)"/>
    public static DataType Categorical(Categories? categories=null) => DataType.Categorical(categories); 
    /// <inheritdoc cref="Polars.Categories(string?, string?, CategoricalPhysical)"/>
    public static DataType Categorical(string categories) => DataType.Categorical(categories); 
    /// <summary>
    /// Binary type.
    /// </summary>
    public static DataType Binary => DataType.Binary;
    /// <summary>
    /// A fixed categorical encoding of a unique set of strings.
    /// </summary>
    /// <param name="categories">The categories in the dataset; must be a unique set of strings, or an existing .NET enum type.</param>
    /// <returns></returns>
    public static DataType Enum(Categories categories) => DataType.Enum(categories);
    /// <inheritdoc cref="Polars.Enum(CSharp.Categories)"/>
    public static DataType Enum(Series categories) => DataType.Enum(categories);
    /// <inheritdoc cref="Polars.Enum(CSharp.Categories)"/>
    public static DataType Enum(string[] categories) => DataType.Enum(categories);
    /// <inheritdoc cref="Polars.Enum(CSharp.Categories)"/>
    public static DataType Enum<T>() where T : struct, Enum => DataType.Enum<T>();
    /// <summary>
    /// Data type representing null values.
    /// </summary>
    public static DataType Null => DataType.Null;
    /// <summary>
    /// Type representing DataType values that could not be determined statically.
    /// </summary>
    public static DataType Unknown => DataType.Unknown;
    public static DataType SameAsInput => Unknown;
    /// <summary>
    /// Generic extension data type.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="inner"></param>
    /// <param name="metadata"></param>
    /// <returns></returns>
    public static DataType Extension(string name, DataType inner, string? metadata = null) => DataType.Extension(name,inner,metadata);
    /// <summary>
    /// Ordered mapping of column names to their data type.
    /// </summary>
    /// <param name="schema">The schema definition given by column names and their associated Polars data type. 
    /// Accepts a mapping, or an iterable of tuples, Arrow Schemas,Frames.</param>
    /// <returns></returns>
    public static PolarsSchema Schema(IntoSchema schema) => schema.Consume();
    /// <summary>
    /// Create an empty Polars Schema.
    /// </summary>
    /// <returns></returns>
    public static PolarsSchema Schema() => [];
}