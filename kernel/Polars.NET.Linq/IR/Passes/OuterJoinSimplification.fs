namespace Polars.NET.Linq.IR.Passes

open Polars.NET.Linq.IR

/// Turns a left join into an inner join when an inner join above it rejects the rows it would add.
///
/// `(f LEFT JOIN r ON ...) JOIN d ON r.a = d.a` drops every row of `f` without a match in `r`: its `r.a` is null, and a
/// null key does not match. The left join then keeps only matched rows, in the same order, which an inner join also
/// does. Being an inner join, it can be reordered, so that `d` filters `r` before `f` is probed.
module internal OuterJoinSimplification =

    let private columnsOf (plan: LogicalPlan) = Set.ofList (SchemaInference.inferPlanSchema plan).ColumnNames

    let rec private rewrite (node: LogicalPlan) : LogicalPlan =
        match LogicalPlanOps.mapInputs rewrite node with
        | LogicalPlan.Join(
            (LogicalPlan.Join(left, right, innerLeftOn, innerRightOn, JoinKind.Left, innerSuffix, innerCoalesce, false) as outer),
            other,
            leftOn,
            rightOn,
            JoinKind.Inner,
            suffix,
            coalesce,
            false) as join ->
            let added = Set.difference (columnsOf outer) (columnsOf left)
            let rejectsUnmatched =
                leftOn
                |> List.exists (function
                    | ScalarExpr.Column name -> added.Contains name
                    | _ -> false)
            if rejectsUnmatched then
                let inner = LogicalPlan.Join(left, right, innerLeftOn, innerRightOn, JoinKind.Inner, innerSuffix, innerCoalesce, false)
                LogicalPlan.Join(inner, other, leftOn, rightOn, JoinKind.Inner, suffix, coalesce, false)
            else
                join
        | other -> other

    let apply (plan: LogicalPlan) : LogicalPlan = rewrite plan
