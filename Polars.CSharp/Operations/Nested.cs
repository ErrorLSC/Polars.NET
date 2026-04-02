using Polars.NET.Core;
using Cs = Polars.CSharp.Polars.Selectors;

namespace Polars.CSharp;
public partial class LazyFrame : IDisposable, IPolarsLazyFrame
{
    /// <summary>
    /// Lazily unnest struct columns using a Selector, String, DataType, or Expr.
    /// Usage: lf.Unnest("User") or lf.Unnest("User", separator: "_")
    /// </summary>
    public LazyFrame Unnest(IntoSelector columns, string? separator = null)
    {
        using var selector = columns.Consume();
        var h = PolarsWrapper.LazyFrameUnnest(CloneHandle(), selector.CloneHandle(), separator);
        return new LazyFrame(h);
    }

    /// <summary>
    /// Lazily unnest specific struct columns.
    /// Bridge overload to support C# 12 collection expressions.
    /// Usage: lf.Unnest(["User", "Profile"]) or lf.Unnest(["User", "Profile"], separator: "_")
    /// </summary>
    public LazyFrame Unnest(IEnumerable<string> columns, string? separator = null)
    {
        var colsArray = columns as string[] ?? [.. columns];
        if (colsArray.Length == 0) return this;
        
        using var selector = Cs.ByName(colsArray);
        var h = PolarsWrapper.LazyFrameUnnest(CloneHandle(), selector.CloneHandle(), separator);
        return new LazyFrame(h);
    }
    /// <summary>
    /// Lazily unnest specific struct columns.
    /// Bridge overload to support C# 12 collection expressions.
    /// Usage: lf.Unnest(["User", "Profile"]) or lf.Unnest(["User", "Profile"], separator: "_")
    /// </summary>
    public LazyFrame Unnest(params string[] columns)
    {
        if (columns.Length == 0) return this;
        return Unnest(columns, null);
    }
    /// <summary>
    /// Explode list-like columns into multiple rows.
    /// </summary>
    /// <param name="columns">the columns need to be exploded</param>
    /// <param name="emptyAsNull">
    /// If <c>true</c>, empty lists are exploded into a single <c>null</c> value. 
    /// If <c>false</c>, rows with empty lists are removed from the result.
    /// </param>
    /// <param name="keepNulls">
    /// If <c>true</c>, <c>null</c> values in the column are preserved as <c>null</c> in the result. 
    /// If <c>false</c>, rows with <c>null</c> values are removed.
    /// </param>
    /// <returns></returns>
    public LazyFrame Explode(IntoSelector columns, bool emptyAsNull = true, bool keepNulls = true)
    {
        using var safeSelector = columns.Consume();
        var h = PolarsWrapper.LazyExplode(CloneHandle(), safeSelector.CloneHandle(), emptyAsNull, keepNulls);
        return new LazyFrame(h);
    }
    /// <summary>
    /// Bridge overload to support C# 12 collection expressions.
    /// Usage: lf.Explode(["list1", "list2"]) or lf.Explode(["list1", "list2"], keepNulls: false)
    /// </summary>
    public LazyFrame Explode(IEnumerable<string> columns, bool emptyAsNull = true, bool keepNulls = true)
    {
        var colsArray = columns as string[] ?? [.. columns];
        if (colsArray.Length == 0) return this;

        using var selector = Cs.ByName(colsArray);
        var h = PolarsWrapper.LazyExplode(CloneHandle(), selector.CloneHandle(), emptyAsNull, keepNulls);
        return new LazyFrame(h);
    }
    /// <summary>
    /// Explode list-like columns into multiple rows.
    /// </summary>
    /// <param name="columns"></param>
    /// <returns></returns>
    public LazyFrame Explode(params string[] columns)
        => Explode(Cs.ByName(columns));
}

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Decompose a struct column into multiple columns.
    /// <para>
    /// This is useful for flattening nested JSON-like data.
    /// </para>
    /// </summary>
    /// <param name="columns">The names of the struct columns to unnest.</param>
    /// <param name="separator">
    /// Optional separator to append to the struct field names (e.g., "struct_col.field"). 
    /// If null, the field names replace the struct column name directly.
    /// </param>
    /// <returns>A new DataFrame with the struct columns expanded.</returns>
    /// <example>
    /// <code>
    /// var data = new[] { new { User = new { Name = "Alice", Age = 30 } } };
    /// var df = DataFrame.From(data);
    /// 
    /// // Unnest with a separator to avoid name collisions
    /// // Result columns: "User_Name", "User_Age"
    /// var unnested = df.Unnest(new[] { "User" }, separator: "_");
    /// unnested.Show();
    /// /* Output:
    /// shape: (1, 2)
    /// ┌───────────┬──────────┐
    /// │ User_Name ┆ User_Age │
    /// │ ---       ┆ ---      │
    /// │ str       ┆ i32      │
    /// ╞═══════════╪══════════╡
    /// │ Alice     ┆ 30       │
    /// └───────────┴──────────┘
    /// */
    /// </code>
    /// </example>
    public DataFrame Unnest(IntoSelector columns, string? separator = null)
    {
        using var safeSelector = columns.Consume();
        
        string[] resolvedColumns = Cs.ExpandSelector(this, safeSelector);

        if (resolvedColumns.Length == 0) return this; 

        var newHandle = PolarsWrapper.Unnest(Handle, resolvedColumns, separator);
        return new DataFrame(newHandle);
    }
    /// <summary>
    /// Decompose a struct column into multiple columns (one for each field in the struct).
    /// <para>
    /// This is useful for flattening nested JSON-like data or composite types.
    /// The original struct column is replaced by its fields.
    /// </para>
    /// </summary>
    /// <param name="columns">The names of the struct columns to unnest.</param>
    /// <param name="separator">
    /// Optional separator to append to the struct field names (e.g., "struct_col.field"). 
    /// If null, the field names replace the struct column name directly.
    /// </param>
    /// <returns>A new DataFrame with the struct columns expanded.</returns>
    /// <example>
    /// <code>
    /// // Create a DataFrame with a nested structure (simulating JSON)
    /// var nestedData = new[] 
    /// {
    ///     new { Id = 1, User = new { Name = "Alice", City = "NY" } },
    ///     new { Id = 2, User = new { Name = "Bob",   City = "LA" } }
    /// };
    /// 
    /// var df = DataFrame.From(nestedData);
    /// 
    /// // Unnest the "User" column into "Name" and "City"
    /// var unnested = df.Unnest("User");
    /// 
    /// unnested.Show();
    /// /* Output:
    /// shape: (2, 3)
    /// ┌─────┬───────┬──────┐
    /// │ Id  ┆ Name  ┆ City │
    /// │ --- ┆ ---   ┆ ---  │
    /// │ i32 ┆ str   ┆ str  │
    /// ╞═════╪═══════╪══════╡
    /// │ 1   ┆ Alice ┆ NY   │
    /// │ 2   ┆ Bob   ┆ LA   │
    /// └─────┴───────┴──────┘
    /// */
    /// </code>
    /// </example>
    public DataFrame Unnest(IEnumerable<string> columns, string? separator = null)
    {
        var colsArray = columns as string[] ?? [.. columns];
        if (colsArray.Length == 0) return this;
        
        var newHandle = PolarsWrapper.Unnest(Handle, colsArray, separator);
        return new DataFrame(newHandle);
    }
    /// <summary>
    /// Decompose a struct column into multiple columns (one for each field in the struct).
    /// </summary>
    public DataFrame Unnest(params string[] columns)
    {
        if (columns.Length == 0) return this;
        return Unnest(columns, null);
    }
    /// <summary>
    /// Explode list/array columns into multiple rows.
    /// <para>
    /// This un-nests list columns. If multiple columns are provided, they are exploded in parallel.
    /// Note: The list columns being exploded must have the same length for each row.
    /// </para>
    /// </summary>
    /// <param name="columns">Expressions selecting the columns to explode.</param>
    /// <returns>A new DataFrame where list elements are expanded into rows.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     id = new[] { 1, 2 },
    ///     tags = new[] { new[] { "apple", "orange" }, new[] { "banana" } },
    ///     scores = new[] { new[] { 10, 20 }, new[] { 30 } }
    /// });
    /// 
    /// // Explode "tags" and "scores" columns simultaneously
    /// var exploded = df.Explode(["tags","scores"]);
    /// 
    /// exploded.Show();
    /// /* Output:
    /// shape: (3, 3)
    /// ┌─────┬────────┬────────┐
    /// │ id  ┆ tags   ┆ scores │
    /// │ --- ┆ ---    ┆ ---    │
    /// │ i32 ┆ str    ┆ i32    │
    /// ╞═════╪════════╪════════╡
    /// │ 1   ┆ apple  ┆ 10     │
    /// │ 1   ┆ orange ┆ 20     │
    /// │ 2   ┆ banana ┆ 30     │
    /// └─────┴────────┴────────┘
    /// */
    /// </code>
    /// </example>
    public DataFrame Explode(params string[] columns)
    {
        if (columns.Length == 0) return this;
        return Explode((IEnumerable<string>)columns); 
    }
    /// <summary>
    /// Explode list/array columns into multiple rows using selector.
    /// </summary>
    /// <param name="columns">Columns need to be exploded</param>
    /// <param name="emptyAsNull">
    /// If <c>true</c>, empty lists are exploded into a single <c>null</c> value. 
    /// If <c>false</c>, rows with empty lists are removed from the result.
    /// </param>
    /// <param name="keepNulls">
    /// If <c>true</c>, <c>null</c> values in the column are preserved as <c>null</c> in the result. 
    /// If <c>false</c>, rows with <c>null</c> values are removed.
    /// </param>
    /// <returns></returns>
    public DataFrame Explode(IntoSelector columns, bool emptyAsNull = true, bool keepNulls = true)
    {
        using var safeSelector = columns.Consume();
        
        string[] resolvedColumns = Cs.ExpandSelector(this, safeSelector);
        
        if (resolvedColumns.Length == 0) return this;

        var h = PolarsWrapper.Explode(Handle, resolvedColumns, emptyAsNull, keepNulls);
        return new DataFrame(h);
    }
    /// <summary>
    /// Bridge overload to support C# 12 collection expressions.
    /// Usage: df.Explode(["list1", "list2"])
    /// </summary>
    public DataFrame Explode(IEnumerable<string> columns, bool emptyAsNull = true, bool keepNulls = true)
    {
        var colsArray = columns as string[] ?? columns.ToArray();
        if (colsArray.Length == 0) return this;

        var h = PolarsWrapper.Explode(Handle, colsArray, emptyAsNull, keepNulls);
        return new DataFrame(h);
    }
 
}