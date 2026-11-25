namespace Kaminari.Samidare.IR

/// Short-circuit evaluation for the raising row nodes (SEMANTICS.md, Guards). A frontend whose language skips the
/// right of AND / OR, the untaken branch of CASE and the later arguments of COALESCE (SQL as Spark evaluates it, C#'s
/// `&&`, `||`, `?:`, `??`) runs this once, right after binding and before the optimizer (which may reorder conjuncts):
/// it conjoins into each `CheckedNarrow` / `CheckedDivide` guard the conditions under which the node is reached.
/// Running it twice repeats conditions, which changes nothing but the plan's size.
module RaisingGuards =

    let private isRaising (fn: ScalarFunction) =
        match fn with
        | ScalarFunction.CheckedNarrow
        | ScalarFunction.CheckedDivide _ -> true
        | _ -> false

    let private isTrue (e: ScalarExpr) =
        match e with
        | ScalarExpr.Literal(LiteralValue.Bool true) -> true
        | _ -> false

    let private bool (v: bool) = ScalarExpr.Literal(LiteralValue.Bool v)

    /// The conjunction of the conditions, literal true for none.
    let private conjoin (conditions: ScalarExpr list) : ScalarExpr =
        match conditions |> List.filter (isTrue >> not) with
        | [] -> ScalarExprOps.unguarded
        | first :: rest -> rest |> List.fold (fun acc c -> ScalarExpr.Binary(BinaryOp.And, acc, c)) first

    /// `reached`: the conditions (outermost first) under which `expr` is evaluated.
    let rec private guard (reached: ScalarExpr list) (expr: ScalarExpr) : ScalarExpr =
        let under extra e = guard (reached @ [ extra ]) e
        match expr with
        | ScalarExpr.Function(fn, args, t) when isRaising fn && not args.IsEmpty ->
            let args = args |> List.map (guard reached)
            let operands, own = List.splitAt (args.Length - 1) args
            ScalarExpr.Function(fn, operands @ [ conjoin (reached @ own) ], t)
        // A null test takes the else branch.
        | ScalarExpr.Conditional(test, ifTrue, ifFalse) ->
            let test = guard reached test
            let taken = ScalarExpr.Coalesce [ test; bool false ]
            ScalarExpr.Conditional(test, under taken ifTrue, under (ScalarExpr.Unary(UnaryOp.Not, taken)) ifFalse)
        // Kleene: the right side decides unless the left is false (AND) or true (OR).
        | ScalarExpr.Binary(BinaryOp.And, left, right) ->
            let left = guard reached left
            ScalarExpr.Binary(BinaryOp.And, left, under (ScalarExpr.Coalesce [ left; bool true ]) right)
        | ScalarExpr.Binary(BinaryOp.Or, left, right) ->
            let left = guard reached left
            ScalarExpr.Binary(BinaryOp.Or, left, under (ScalarExpr.Unary(UnaryOp.Not, ScalarExpr.Coalesce [ left; bool false ])) right)
        | ScalarExpr.Coalesce exprs -> ScalarExpr.Coalesce(guardFallbacks reached exprs)
        | ScalarExpr.Function(ScalarFunction.FillNull, args, t) -> ScalarExpr.Function(ScalarFunction.FillNull, guardFallbacks reached args, t)
        // An aggregation's or a window's argument is evaluated over its own rows: only the conditions inside it apply.
        | ScalarExpr.Aggregation _
        | ScalarExpr.Window _
        | ScalarExpr.SortBy _ -> ScalarExprOps.mapChildren (guard []) expr
        | ScalarExpr.Subquery(scope, subplan, kind) ->
            let kind =
                match kind with
                | SubqueryKind.In(target, nullsEqual) -> SubqueryKind.In(guard reached target, nullsEqual)
                | other -> other
            ScalarExpr.Subquery(scope, ofPlan subplan, kind)
        | _ -> ScalarExprOps.mapChildren (guard reached) expr

    /// Each argument after the first is evaluated only where every earlier one is null.
    and private guardFallbacks (reached: ScalarExpr list) (exprs: ScalarExpr list) : ScalarExpr list =
        let folded, _ =
            exprs
            |> List.fold
                (fun (acc, earlier) e ->
                    let e = guard (reached @ earlier) e
                    e :: acc, earlier @ [ ScalarExpr.Unary(UnaryOp.IsNull, e) ])
                ([], [])
        List.rev folded

    /// Every expression of the plan (subqueries' plans included) with its raising nodes guarded.
    and ofPlan (plan: LogicalPlan) : LogicalPlan =
        plan |> LogicalPlanOps.mapInputs ofPlan |> LogicalPlanOps.mapExpressions (guard [])

    /// The expression with its raising nodes guarded.
    let ofExpr (expr: ScalarExpr) : ScalarExpr = guard [] expr
