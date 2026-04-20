#pragma warning disable CS1591
using Polars.NET.Core;

namespace Polars.CSharp;

// ==========================================
// StructOps Helper Class
// ==========================================
/// <summary>
/// Operations on Struct columns. Access via <see cref="Expr.Struct"/>.
/// </summary>
public readonly struct StructOps
{
    private readonly Expr _expr;
    internal StructOps(Expr expr) { _expr = expr; }

    /// <summary>
    /// Retrieve a field from the struct by name.
    /// </summary>
    /// <param name="name">The name of the field.</param>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     id = new[] { 1, 2 },
    ///     product = new[] 
    ///     { 
    ///         new { Name = "Laptop", Specs = new { Ram = 16, SSD = 512 } },
    ///         new { Name = "Mouse",  Specs = new { Ram = 0,  SSD = 0   } }
    ///     }
    /// });
    /// 
    /// df.Select(
    ///     Col("id"),
    ///     Col("product").Struct.Field("Name").Alias("prod_name"),
    ///     // Nested Access
    ///     Col("product").Struct.Field("Specs").Struct.Field("Ram").Alias("ram_gb")
    /// ).Show();
    /// /* Output:
    /// shape: (2, 3)
    /// ┌─────┬───────────┬────────┐
    /// │ id  ┆ prod_name ┆ ram_gb │
    /// │ --- ┆ ---       ┆ ---    │
    /// │ i32 ┆ str       ┆ i32    │
    /// ╞═════╪═══════════╪════════╡
    /// │ 1   ┆ Laptop    ┆ 16     │
    /// │ 2   ┆ Mouse     ┆ 0      │
    /// └─────┴───────────┴────────┘
    /// */
    /// </code>
    /// </example>
    public Expr Field(params string[] name)
        => new(PolarsWrapper.StructFieldByName(_expr.CloneHandle(), name));
    /// <summary>
    /// Retrieve a field by its index.
    /// </summary>
    public Expr Field(int index)
        => new(PolarsWrapper.StructFieldByIndex(_expr.CloneHandle(), index));
    
    /// <inheritdoc cref="Field(int)"/>
    public Expr this[int index] => Field(index);
    /// <inheritdoc cref="Field(string[])"/>
    public Expr this[string[] name] => Field(name);
    /// <summary>
    /// Rename the fields of the struct.
    /// </summary>
    /// <param name="names">The new names for the fields.</param>
    /// <example>
    /// <code>
    /// df.Select(
    ///     Col("product").Struct.RenameFields(new[] { "NewName", "NewSpecs" })
    /// );
    /// </code>
    /// </example>
    public Expr RenameFields(params string[] names)
        => new(PolarsWrapper.StructRenameFields(_expr.CloneHandle(), names));
    /// <summary>
    /// Convert the struct column into a JSON string column.
    /// Useful for debugging or exporting to systems that support JSON strings.
    /// </summary>
    public Expr JsonEncode() => new(PolarsWrapper.StructJsonEncode(_expr.CloneHandle()));
    /// <summary>
    /// Expand the struct into its individual fields.Alias for Expr.struct.field("*").
    /// </summary>
    /// <returns></returns>
    public Expr Unnest() => Field("*");

    public Expr WithFields(params IntoColumnExpr[] fields)
    {
        if (fields == null || fields.Length == 0)
        {
            return new Expr(PolarsWrapper.StructWithFields(_expr.Handle, []));
        }

        var handles = new ExprHandle[fields.Length];
        for (int i = 0; i < fields.Length; i++)
        {
            handles[i] = fields[i].Consume().Handle; 
        }

        return new Expr(PolarsWrapper.StructWithFields(_expr.Handle, handles));
    }
}