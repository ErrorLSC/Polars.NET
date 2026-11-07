namespace Polars.NET.Linq

open Polars.NET.Core.Config
open Polars.NET.Linq.IR
open System
open System.Linq.Expressions

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

/// .NET floating-point comparison semantics (double, float, Half), compensating for the engine's total order,
/// in which NaN equals NaN and is greater than every number. In .NET a comparison involving NaN is false (and
/// `!=` true), and ordering (Comparer<T>.Default, OrderBy) places NaN before every number (after null).
module internal NaNSemantics =

    let isFloatingClrType (t: Type) =
        let core = match Nullable.GetUnderlyingType t with null -> t | u -> u
        core = typeof<double> || core = typeof<float32> || core = typeof<Half>

    let rec private isNonNaNLiteral (s: ScalarExpr) =
        match s with
        | ScalarExpr.Literal(LiteralValue.Float64 v) -> not (Double.IsNaN v)
        | ScalarExpr.Literal(LiteralValue.Float32 v) -> not (Single.IsNaN v)
        | ScalarExpr.Literal(LiteralValue.Float16 v) -> not (Half.IsNaN v)
        | ScalarExpr.Literal _ -> true
        | ScalarExpr.Cast(inner, _, _) -> isNonNaNLiteral inner
        | _ -> false

    /// Whether a translated operand of CLR type `t` may be NaN when the query runs.
    let mayBeNaN (t: Type) (s: ScalarExpr) = isFloatingClrType t && not (isNonNaNLiteral s)

    /// True when the value is NaN; false for a number or null.
    let private isNaN (s: ScalarExpr) =
        ScalarExpr.Coalesce [ ScalarExpr.Unary(UnaryOp.IsNan, s); ScalarExpr.Literal(LiteralValue.Bool false) ]

    /// True when the value is not NaN (a number or null).
    let private notNaN (s: ScalarExpr) = ScalarExpr.Unary(UnaryOp.Not, isNaN s)

    /// A comparison `l op r` (already translated, with null compensation) made to follow .NET NaN semantics.
    /// Only operands that may be NaN are guarded:
    /// - `==`: the engine's NaN == NaN must be false;
    /// - `!=`: NaN != NaN must be true;
    /// - `<`, `<=`: false when the right side is NaN (the engine orders NaN last);
    /// - `>`, `>=`: false when the left side is NaN.
    let comparison (op: ExpressionType) (leftType: Type, l: ScalarExpr) (rightType: Type, r: ScalarExpr) (cmp: ScalarExpr) =
        let lNaN, rNaN = mayBeNaN leftType l, mayBeNaN rightType r
        let andAlso guard = ScalarExpr.Binary(BinaryOp.And, cmp, guard)
        match op with
        | ExpressionType.Equal when lNaN && rNaN -> andAlso (notNaN l)
        | ExpressionType.NotEqual when lNaN && rNaN -> ScalarExpr.Binary(BinaryOp.Or, cmp, isNaN l)
        | ExpressionType.LessThan
        | ExpressionType.LessThanOrEqual when rNaN -> andAlso (notNaN r)
        | ExpressionType.GreaterThan
        | ExpressionType.GreaterThanOrEqual when lNaN -> andAlso (notNaN l)
        | _ -> cmp

    /// The sort orders for a key of CLR type `keyType`: a floating key is preceded by `NOT is_nan(key)`, which
    /// puts NaN (false) before the numbers (true) in the key's direction and is null for a null key, so nulls
    /// keep the place the key gives them.
    let sortOrders (keyType: Type) (order: SortOrder) : SortOrder list =
        if isFloatingClrType keyType then
            [ { order with Expr = ScalarExpr.Unary(UnaryOp.Not, ScalarExpr.Unary(UnaryOp.IsNan, order.Expr)) }; order ]
        else
            [ order ]

