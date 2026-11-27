namespace Polars.FSharp

open Polars.NET.Core
open Polars.NET.Core.Wrappers

/// <summary>
/// Operations on Map columns (map[key, value]). Access via <c>expr.MapOps</c>.
/// <para>
/// A null map stays null through every operation; an empty map ({}) is not null. A map read from Arrow data may hold a
/// key more than once: lookups find its first entry.
/// </para>
/// </summary>
[<Struct>]
type MapOps(handle: ExprHandle) =
    /// <summary> The entries of every map as a list of {key, value} structs, in entry order. </summary>
    member _.Entries() = new Expr(PolarsWrapper.MapEntries handle)

    /// <summary> The keys of every map as a list, in entry order. </summary>
    member _.Keys() = new Expr(PolarsWrapper.MapKeys handle)

    /// <summary> The values of every map as a list, in entry order. </summary>
    member _.Values() = new Expr(PolarsWrapper.MapValues handle)

    /// <summary> The number of entries of every map (UInt32). </summary>
    member _.Len() = new Expr(PolarsWrapper.MapLen handle)

    /// <summary>
    /// Whether every map holds the key (an expression: a literal, or a column holding one key per row). A null key is
    /// never found.
    /// </summary>
    member _.ContainsKey(key: Expr) =
        new Expr(PolarsWrapper.MapContainsKey(handle, key.CloneHandle()))

    /// <summary> Whether every map holds the text key (a literal key, not a column name). </summary>
    member _.ContainsKey(key: string) =
        new Expr(PolarsWrapper.MapContainsKey(handle, PolarsWrapper.Lit key))

    /// <summary> Whether every map holds the integer key. </summary>
    member _.ContainsKey(key: int) =
        new Expr(PolarsWrapper.MapContainsKey(handle, PolarsWrapper.Lit key))

    /// <summary> Whether every map holds the integer key. </summary>
    member _.ContainsKey(key: int64) =
        new Expr(PolarsWrapper.MapContainsKey(handle, PolarsWrapper.Lit key))

    /// <summary>
    /// The value of the key in every map (an expression: a literal, or a column holding one key per row); null where
    /// the map does not hold the key, and for a null map.
    /// </summary>
    member _.Get(key: Expr) =
        new Expr(PolarsWrapper.MapGet(handle, key.CloneHandle()))

    /// <summary> The value of the text key (a literal key, not a column name) in every map; null where it is missing. </summary>
    member _.Get(key: string) =
        new Expr(PolarsWrapper.MapGet(handle, PolarsWrapper.Lit key))

    /// <summary> The value of the integer key in every map; null where it is missing. </summary>
    member _.Get(key: int) =
        new Expr(PolarsWrapper.MapGet(handle, PolarsWrapper.Lit key))

    /// <summary> The value of the integer key in every map; null where it is missing. </summary>
    member _.Get(key: int64) =
        new Expr(PolarsWrapper.MapGet(handle, PolarsWrapper.Lit key))
