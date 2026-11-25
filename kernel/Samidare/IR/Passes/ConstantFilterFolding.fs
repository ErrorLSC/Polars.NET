namespace Kaminari.Samidare.IR.Passes

open Kaminari.Samidare.IR

/// Computes the constant parts of filters, and drops the branches of a UNION ALL that a constant filter empties.
///
/// A filter can hold constants its author never wrote: `(1999 + 2)` from a SQL text, or a comparison of two literals
/// once pushdown has replaced a column by the literal a union branch gives it. A common table expression over a union
/// of channels, each marked by a literal (`'s' AS sale_type`), read as `WHERE sale_type = 's'` leaves `'c' = 's'` in
/// the other channels' branches: each branch still scans and aggregates its fact table to keep no rows.
///
/// Each predicate is folded by `FilterShapes.foldConstants` (the semantics are unchanged; a bound such as
/// `d_year <= 2001` is then a literal that later passes recognize); a filter folded to true goes away. A branch of a
/// UNION ALL is dropped when it is empty whatever its input: a filter folded to false or null (or a limit of no rows),
/// seen through filters,
/// renames and row-local projections that read a column (one row in, one row out). Nothing else carries the
/// emptiness: an aggregate without keys over no rows still yields one row, and a projection of literals alone may
/// yield one. The union's output keeps the left branch's names and types: a right branch left alone is projected
/// onto them as the union would align it, by name when both have the same names, else by position.
module internal ConstantFilterFolding =

    let private isFalseOrNull (predicate: ScalarExpr) =
        match predicate with
        | ScalarExpr.Literal(LiteralValue.Bool false)
        | ScalarExpr.Literal(LiteralValue.Null _) -> true
        | _ -> false

    /// Whether the plan yields no rows whatever its input.
    let rec private isEmpty (plan: LogicalPlan) =
        match plan with
        | LogicalPlan.Filter(_, predicate) when isFalseOrNull predicate -> true
        // Predicate pushdown's form of a false filter.
        | LogicalPlan.Limit(_, _, 0u) -> true
        | LogicalPlan.Filter(input, _)
        | LogicalPlan.Rename(input, _, _) -> isEmpty input
        | LogicalPlan.Project(input, columns) ->
            columns |> List.forall (snd >> ScalarExprOps.dependsOnOtherRows >> not)
            && columns |> List.exists (fun (_, expr) -> not (ScalarExprOps.collectColumns expr).IsEmpty)
            && isEmpty input
        | LogicalPlan.WithColumns(input, columns) -> columns |> List.forall (snd >> ScalarExprOps.dependsOnOtherRows >> not) && isEmpty input
        | LogicalPlan.Union(left, right, true) -> isEmpty left && isEmpty right
        | _ -> false

    /// The right branch of a union with the left branch's names and types, aligned as the union aligns them; None when
    /// the branches do not align.
    let private asLeft (left: LogicalPlan) (right: LogicalPlan) =
        let leftFields = (SchemaInference.inferPlanSchema left).Fields
        let rightFields = (SchemaInference.inferPlanSchema right).Fields
        let names (fields: LogicalField list) = fields |> List.map (fun f -> f.Name)
        let types (fields: LogicalField list) = fields |> List.map (fun f -> f.DataType)
        let byName = Set.ofList (names leftFields) = Set.ofList (names rightFields)
        let source (index: int) (field: LogicalField) =
            if byName then rightFields |> List.find (fun (f: LogicalField) -> f.Name = field.Name) else rightFields[index]
        if names leftFields = names rightFields && types leftFields = types rightFields then
            Some right
        elif not byName && leftFields.Length <> rightFields.Length then
            None
        else
            let columns =
                leftFields
                |> List.mapi (fun index field ->
                    let from = source index field
                    let read = ScalarExpr.Column from.Name
                    field.Name, (if from.DataType = field.DataType then read else ScalarExpr.Cast(read, field.DataType, false)))
            Some(LogicalPlan.Project(right, columns))

    let rec private rewrite (plan: LogicalPlan) : LogicalPlan =
        match LogicalPlanOps.mapInputs rewrite plan with
        | LogicalPlan.Filter(input, predicate) ->
            match FilterShapes.foldConstants predicate with
            | ScalarExpr.Literal(LiteralValue.Bool true) -> input
            | folded -> LogicalPlan.Filter(input, folded)
        | LogicalPlan.Union(left, right, true) as union ->
            match isEmpty left, isEmpty right with
            | false, true -> left
            | true, false -> asLeft left right |> Option.defaultValue union
            | _ -> union
        | other -> other

    let apply (plan: LogicalPlan) : LogicalPlan = rewrite plan
