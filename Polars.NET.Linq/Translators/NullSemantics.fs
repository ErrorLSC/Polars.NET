namespace Polars.NET.Linq

open Polars.NET.Core.Config
open Polars.NET.Linq.IR
open System

/// How comparisons treat null, as EF Core does: C# semantics by default, compensating for the three-valued
/// logic of the engine, or relational (SQL) semantics when CoreConfig.UseRelationalNulls is set.
///
/// C# semantics, which the translations below produce:
/// - `a == b` / `a != b` are two-valued: null equals null and differs from every value (eq_missing / ne_missing).
/// - A lifted ordering comparison (`a &lt; b` over Nullable operands) is false when either operand is null.
///
/// A filter treats null as false, so in a filter's positive positions the compensation is redundant for
/// equality with a non-null literal and for lifted orderings; FilterNullSimplification removes it there, which
/// keeps the plain comparisons statistics-based predicate pushdown recognizes.
module internal NullSemantics =

    /// True when comparisons follow relational (SQL) null semantics instead of C# semantics.
    let useRelationalNulls () = CoreConfig.UseRelationalNulls

    /// Whether a null key matches a null key in the joins that decorrelate a subquery (`Any(y => y.K == x.K)`,
    /// `Contains` over a query, correlated aggregates) and in `Contains` over a list or an in-memory collection:
    /// true under C# semantics, false under relational semantics.
    let nullKeysMatch () = not (useRelationalNulls ())

    /// `item IN collection` for an in-memory collection. Under C# semantics a null item is in a collection
    /// holding null. Under relational semantics it is SQL IN: a null item gives null, and so does an item not
    /// found in a collection holding null (`x IN (1, NULL)` is null for x = 2, which keeps NOT IN false).
    let inCollection (item: ScalarExpr) (values: System.Collections.IEnumerable) (elementType: Type) : ScalarExpr =
        if not (useRelationalNulls ()) then
            ScalarExpr.InList(item, InListSource.Collection(values, elementType), false, true)
        else
            let all = values |> Seq.cast<obj> |> Seq.toArray
            let nonNull = all |> Array.filter (fun v -> not (isNull v))

            if nonNull.Length = all.Length then
                ScalarExpr.InList(item, InListSource.Collection(values, elementType), false, false)
            else
                let typed = Array.CreateInstance(elementType, nonNull.Length)
                nonNull |> Array.iteri (fun i v -> typed.SetValue(v, i))
                ScalarExpr.Binary(
                    BinaryOp.Or,
                    ScalarExpr.InList(item, InListSource.Collection(typed, elementType), false, false),
                    ScalarExpr.Literal(LiteralValue.Null LogicalTypes.Boolean)
                )

    /// True for a CLR type whose values can be null: a reference type or Nullable<T>.
    let isNullableClrType (t: Type) = not t.IsValueType || not (isNull (Nullable.GetUnderlyingType t))

    /// True for a translated constant (a literal, possibly cast or enum-converted).
    let rec isLiteralScalar (s: ScalarExpr) =
        match s with
        | ScalarExpr.Literal _ -> true
        | ScalarExpr.Cast(inner, _, _)
        | ScalarExpr.EnumConvert(_, inner, _) -> isLiteralScalar inner
        | _ -> false

    /// True for a translated operand of CLR type `clrType` that can be null when the query runs.
    let mayBeNull (clrType: Type) (s: ScalarExpr) = isNullableClrType clrType && not (isLiteralScalar s)

    /// Whether two operands, with their CLR types, need null compensation under the current semantics.
    let needsCompensation (operands: (Type * ScalarExpr) list) =
        not (useRelationalNulls ()) && operands |> List.exists (fun (t, s) -> mayBeNull t s)

    /// `l == r` (or `l != r` when `isEqual` is false) under the current null semantics.
    let equality (isEqual: bool) (leftType: Type, l: ScalarExpr) (rightType: Type, r: ScalarExpr) : ScalarExpr =
        let op =
            match isEqual, needsCompensation [ leftType, l; rightType, r ] with
            | true, true -> BinaryOp.EqMissing
            | false, true -> BinaryOp.NotEqMissing
            | true, false -> BinaryOp.Eq
            | false, false -> BinaryOp.NotEq

        ScalarExpr.Binary(op, l, r)

    /// A lifted ordering comparison (`comparison` over the given operands) with a non-nullable bool result:
    /// false, not null, when an operand is null.
    let liftedComparison (resultType: Type) (operands: (Type * ScalarExpr) list) (comparison: ScalarExpr) : ScalarExpr =
        if resultType = typeof<bool> && needsCompensation operands then
            ScalarExpr.Coalesce [ comparison; ScalarExpr.Literal(LiteralValue.Bool false) ]
        else
            comparison
