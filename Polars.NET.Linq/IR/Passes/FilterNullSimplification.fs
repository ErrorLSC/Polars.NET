namespace Polars.NET.Linq.IR.Passes

open Polars.NET.Linq.IR

/// Removes null compensation that a filter makes redundant.
///
/// A filter keeps only rows whose predicate is true, so null and false are interchangeable at the predicate's
/// positive positions: the predicate itself and the operands of AND / OR beneath it (Kleene AND / OR are
/// monotone, so a false that becomes null can never turn the predicate true), and the branches and test of a
/// conditional there. Under NOT the distinction matters, so the walk stops at any other node.
///
/// At those positions:
/// - `coalesce(p, false)` (a lifted comparison made two-valued) becomes `p`;
/// - `a eq_missing lit` with a non-null literal becomes `a = lit`: both are false or null on a null `a`.
/// The plain forms are what statistics-based predicate pushdown recognizes.
module internal FilterNullSimplification =

    let rec private isNonNullLiteral (s: ScalarExpr) =
        match s with
        | ScalarExpr.Literal(LiteralValue.Null _) -> false
        | ScalarExpr.Literal _ -> true
        | ScalarExpr.Cast(inner, _, _)
        | ScalarExpr.EnumConvert(_, inner, _) -> isNonNullLiteral inner
        | _ -> false

    let rec simplifyPredicate (pred: ScalarExpr) : ScalarExpr =
        match pred with
        | ScalarExpr.Coalesce [ inner; ScalarExpr.Literal(LiteralValue.Bool false) ] -> simplifyPredicate inner
        | ScalarExpr.Binary(BinaryOp.EqMissing, l, r) when isNonNullLiteral l || isNonNullLiteral r ->
            ScalarExpr.Binary(BinaryOp.Eq, l, r)
        | ScalarExpr.Binary((BinaryOp.And | BinaryOp.Or as op), l, r) ->
            ScalarExpr.Binary(op, simplifyPredicate l, simplifyPredicate r)
        | ScalarExpr.Conditional(test, ifTrue, ifFalse) ->
            ScalarExpr.Conditional(simplifyPredicate test, simplifyPredicate ifTrue, simplifyPredicate ifFalse)
        | other -> other

    let rec apply (plan: LogicalPlan) : LogicalPlan =
        match plan with
        | LogicalPlan.Filter(input, pred) -> LogicalPlan.Filter(apply input, simplifyPredicate pred)
        | other -> LogicalPlanOps.mapInputs apply other
