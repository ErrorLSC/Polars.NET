using Polars.NET.Core.Wrappers;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;

/// <summary>
/// Operations on Map columns (<c>map[key, value]</c>). Access via <see cref="Expr.MapOps"/>.
/// <para>
/// A null map stays null through every operation; an empty map (<c>{}</c>) is not null. A map read from Arrow data may
/// hold a key more than once: lookups find its first entry.
/// </para>
/// </summary>
/// <example>
/// <code>
/// using var df = DataFrame.From&lt;Order&gt;([
///     new(1, new() { ["apple"] = 3, ["pear"] = 1 }),
///     new(2, new() { ["kiwi"] = 5 }),
/// ]);
///
/// df.Select(
///     Pl.Col("Id"),
///     Pl.Col("Items").MapOps.Get("apple").Alias("apples"),
///     Pl.Col("Items").MapOps.Len().Alias("kinds")
/// ).Show();
/// /* Output:
/// shape: (2, 3)
/// ┌─────┬────────┬───────┐
/// │ Id  ┆ apples ┆ kinds │
/// │ --- ┆ ---    ┆ ---   │
/// │ i32 ┆ i32    ┆ u32   │
/// ╞═════╪════════╪═══════╡
/// │ 1   ┆ 3      ┆ 2     │
/// │ 2   ┆ null   ┆ 1     │
/// └─────┴────────┴───────┘
/// */
///
/// record Order(int Id, Dictionary&lt;string, int&gt; Items);
/// </code>
/// </example>
public readonly struct MapOps
{
    private readonly Expr _expr;
    internal MapOps(Expr expr) { _expr = expr; }

    /// <summary>
    /// The entries of every map as a list of <c>{key, value}</c> structs, in entry order.
    /// </summary>
    public Expr Entries() => new(PolarsWrapper.MapEntries(_expr.CloneHandle()));

    /// <summary>
    /// The keys of every map as a list, in entry order.
    /// </summary>
    public Expr Keys() => new(PolarsWrapper.MapKeys(_expr.CloneHandle()));

    /// <summary>
    /// The values of every map as a list, in entry order.
    /// </summary>
    public Expr Values() => new(PolarsWrapper.MapValues(_expr.CloneHandle()));

    /// <summary>
    /// The number of entries of every map (<see cref="DataType.UInt32"/>).
    /// </summary>
    public Expr Len() => new(PolarsWrapper.MapLen(_expr.CloneHandle()));

    /// <summary>
    /// Whether every map holds <paramref name="key"/> (an expression: a literal, or a column holding one key per row).
    /// A null key is never found.
    /// </summary>
    public Expr ContainsKey(Expr key) => new(PolarsWrapper.MapContainsKey(_expr.CloneHandle(), key.CloneHandle()));

    /// <summary>
    /// Whether every map holds the text <paramref name="key"/> (a literal key, not a column name).
    /// </summary>
    public Expr ContainsKey(string key) => ContainsKey(Pl.Lit(key));

    /// <summary>
    /// The value of <paramref name="key"/> in every map (an expression: a literal, or a column holding one key per row);
    /// null where the map does not hold the key, and for a null map.
    /// </summary>
    public Expr Get(Expr key) => new(PolarsWrapper.MapGet(_expr.CloneHandle(), key.CloneHandle()));

    /// <summary>
    /// The value of the text <paramref name="key"/> (a literal key, not a column name) in every map; null where the map
    /// does not hold it.
    /// </summary>
    public Expr Get(string key) => Get(Pl.Lit(key));
}
