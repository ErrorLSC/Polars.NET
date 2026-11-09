namespace Kaminari.Samidare.IR

open System
open System.Text

/// Construction helpers, visitors, and graph visualization for LogicalPlan
module internal LogicalPlanOps =

    // =========================================================================
    // 1. Direct Child & Subplan Navigation
    // =========================================================================

    /// Extracts the immediate child inputs of a logical plan node.
    let getInputs (plan: LogicalPlan) : LogicalPlan list =
        match plan with
        | LogicalPlan.Scan _
        | LogicalPlan.Empty _ -> []
        | LogicalPlan.Filter(input, _)
        | LogicalPlan.Project(input, _)
        | LogicalPlan.WithColumns(input, _)
        | LogicalPlan.GroupBy(input, _, _, _, _)
        | LogicalPlan.GroupingSets(input, _)
        | LogicalPlan.Sort(input, _)
        | LogicalPlan.Limit(input, _, _)
        | LogicalPlan.Distinct(input, _, _, _)
        | LogicalPlan.Explode(input, _, _, _)
        | LogicalPlan.Unpivot(input, _, _, _, _)
        | LogicalPlan.WithRowIndex(input, _, _)
        | LogicalPlan.Rename(input, _, _)
        | LogicalPlan.Reverse input
        | LogicalPlan.SkipLast(input, _)
        | LogicalPlan.Shared(_, input) -> [ input ]
        | LogicalPlan.HorizontalConcat inputs -> inputs
        | LogicalPlan.Join(left, right, _, _, _, _, _, _)
        | LogicalPlan.JoinAsOf(left, right, _)
        | LogicalPlan.Union(left, right, _) -> [ left; right ]

    /// Transforms immediate child subplans of a logical plan node. A node whose children all come back
    /// unchanged (the same objects) is returned itself, so passes that change nothing keep the plan's node
    /// identities (and their memoized schemas) and do not reallocate the tree.
    let mapInputs (f: LogicalPlan -> LogicalPlan) (plan: LogicalPlan) : LogicalPlan =
        let inline same (a: LogicalPlan) (b: LogicalPlan) = obj.ReferenceEquals(a, b)
        match plan with
        | LogicalPlan.Scan _
        | LogicalPlan.Empty _ -> plan
        | LogicalPlan.Filter(input, pred) -> let i = f input in if same i input then plan else LogicalPlan.Filter(i, pred)
        | LogicalPlan.Project(input, projs) -> let i = f input in if same i input then plan else LogicalPlan.Project(i, projs)
        | LogicalPlan.WithColumns(input, cols) -> let i = f input in if same i input then plan else LogicalPlan.WithColumns(i, cols)
        | LogicalPlan.Join(left, right, lOn, rOn, how, sfx, coal, nullEq) ->
            let l = f left
            let r = f right
            if same l left && same r right then plan else LogicalPlan.Join(l, r, lOn, rOn, how, sfx, coal, nullEq)
        | LogicalPlan.JoinAsOf(left, right, spec) ->
            let l = f left
            let r = f right
            if same l left && same r right then plan else LogicalPlan.JoinAsOf(l, r, spec)
        | LogicalPlan.GroupBy(input, keys, aggs, having, ord) ->
            let i = f input in if same i input then plan else LogicalPlan.GroupBy(i, keys, aggs, having, ord)
        | LogicalPlan.GroupingSets(input, spec) -> let i = f input in if same i input then plan else LogicalPlan.GroupingSets(i, spec)
        | LogicalPlan.Sort(input, orderings) -> let i = f input in if same i input then plan else LogicalPlan.Sort(i, orderings)
        | LogicalPlan.Limit(input, off, cnt) -> let i = f input in if same i input then plan else LogicalPlan.Limit(i, off, cnt)
        | LogicalPlan.Distinct(input, sub, keep, ord) ->
            let i = f input in if same i input then plan else LogicalPlan.Distinct(i, sub, keep, ord)
        | LogicalPlan.Explode(input, cols, emAsNull, kpNulls) ->
            let i = f input in if same i input then plan else LogicalPlan.Explode(i, cols, emAsNull, kpNulls)
        | LogicalPlan.Unpivot(input, idx, on, varN, valN) ->
            let i = f input in if same i input then plan else LogicalPlan.Unpivot(i, idx, on, varN, valN)
        | LogicalPlan.Union(left, right, isAll) ->
            let l = f left
            let r = f right
            if same l left && same r right then plan else LogicalPlan.Union(l, r, isAll)
        | LogicalPlan.WithRowIndex(input, name, off) -> let i = f input in if same i input then plan else LogicalPlan.WithRowIndex(i, name, off)
        | LogicalPlan.Rename(input, existing, newN) -> let i = f input in if same i input then plan else LogicalPlan.Rename(i, existing, newN)
        | LogicalPlan.Reverse input -> let i = f input in if same i input then plan else LogicalPlan.Reverse i
        | LogicalPlan.SkipLast(input, count) -> let i = f input in if same i input then plan else LogicalPlan.SkipLast(i, count)
        | LogicalPlan.Shared(id, input) -> let i = f input in if same i input then plan else LogicalPlan.Shared(id, i)
        | LogicalPlan.HorizontalConcat inputs ->
            let mapped = inputs |> List.map f
            if List.forall2 same mapped inputs then plan else LogicalPlan.HorizontalConcat mapped

    /// Transforms all scalar expressions embedded in the current logical plan node (non-recursively).
    let mapExpressions (f: ScalarExpr -> ScalarExpr) (plan: LogicalPlan) : LogicalPlan =
        match plan with
        | LogicalPlan.Scan _
        | LogicalPlan.Empty _ -> plan
        | LogicalPlan.Filter(input, pred) -> LogicalPlan.Filter(input, f pred)
        | LogicalPlan.Project(input, projs) ->
            LogicalPlan.Project(input, projs |> List.map (fun (name, e) -> name, f e))
        | LogicalPlan.WithColumns(input, cols) ->
            LogicalPlan.WithColumns(input, cols |> List.map (fun (name, e) -> name, f e))
        | LogicalPlan.Join(left, right, lOn, rOn, how, sfx, coal, nullEq) ->
            LogicalPlan.Join(left, right, lOn |> List.map f, rOn |> List.map f, how, sfx, coal, nullEq)
        | LogicalPlan.JoinAsOf(left, right, spec) ->
            LogicalPlan.JoinAsOf(
                left,
                right,
                { spec with
                    LeftOn = f spec.LeftOn
                    RightOn = f spec.RightOn
                    LeftBy = spec.LeftBy |> List.map f
                    RightBy = spec.RightBy |> List.map f
                }
            )
        | LogicalPlan.GroupBy(input, keys, aggs, having, ord) ->
            LogicalPlan.GroupBy(
                input,
                keys |> List.map f,
                aggs |> List.map (fun (name, e) -> name, f e),
                having |> Option.map f,
                ord
            )
        | LogicalPlan.GroupingSets(input, spec) ->
            LogicalPlan.GroupingSets(
                input,
                { spec with
                    KeyAliases = spec.KeyAliases |> List.map (fun (name, e) -> name, f e)
                    Aggs = spec.Aggs |> List.map (fun (name, e) -> name, f e)
                    Levels = spec.Levels |> List.map (fun (idxs, m) -> idxs, m |> Map.map (fun _ e -> f e))
                }
            )
        | LogicalPlan.Sort(input, orderings) ->
            LogicalPlan.Sort(
                input,
                orderings
                |> List.map (fun o ->
                    { o with
                        Expr = f o.Expr
                    })
            )
        | LogicalPlan.Limit _
        | LogicalPlan.Distinct _
        | LogicalPlan.Explode _
        | LogicalPlan.Unpivot _
        | LogicalPlan.Union _
        | LogicalPlan.WithRowIndex _
        | LogicalPlan.Rename _
        | LogicalPlan.Reverse _
        | LogicalPlan.SkipLast _
        | LogicalPlan.Shared _
        | LogicalPlan.HorizontalConcat _ -> plan

    // =========================================================================
    // 2. Tree Folding & Transformation
    // =========================================================================

    /// Post-order recursive traversal over the entire logical plan tree.
    let rec rewrite (rule: LogicalPlan -> LogicalPlan option) (plan: LogicalPlan) : LogicalPlan =
        let rewrittenChildren = mapInputs (rewrite rule) plan

        match rule rewrittenChildren with
        | Some newPlan -> newPlan
        | None -> rewrittenChildren

    /// Collects all column names required by operators at or above this plan node.
    let rec collectReferencedColumns (plan: LogicalPlan) : Set<string> =
        let currentCols =
            match plan with
            | LogicalPlan.Scan _
            | LogicalPlan.Empty _ -> Set.empty
            | LogicalPlan.Filter(_, pred) -> ScalarExprOps.collectColumns pred
            | LogicalPlan.Project(_, projs) ->
                projs |> List.map (snd >> ScalarExprOps.collectColumns) |> Set.unionMany
            | LogicalPlan.WithColumns(_, cols) ->
                cols |> List.map (snd >> ScalarExprOps.collectColumns) |> Set.unionMany
            | LogicalPlan.Join(_, _, lOn, rOn, _, _, _, _) ->
                let lCols = lOn |> List.map ScalarExprOps.collectColumns |> Set.unionMany
                let rCols = rOn |> List.map ScalarExprOps.collectColumns |> Set.unionMany
                Set.union lCols rCols
            | LogicalPlan.JoinAsOf(_, _, spec) ->
                let lCols = ScalarExprOps.collectColumns spec.LeftOn
                let rCols = ScalarExprOps.collectColumns spec.RightOn
                let lBy = spec.LeftBy |> List.map ScalarExprOps.collectColumns |> Set.unionMany
                let rBy = spec.RightBy |> List.map ScalarExprOps.collectColumns |> Set.unionMany
                Set.unionMany [ lCols; rCols; lBy; rBy ]
            | LogicalPlan.GroupBy(_, keys, aggs, having, _) ->
                let kCols = keys |> List.map ScalarExprOps.collectColumns |> Set.unionMany
                let aCols = aggs |> List.map (snd >> ScalarExprOps.collectColumns) |> Set.unionMany
                let hCols = having |> Option.map ScalarExprOps.collectColumns |> Option.defaultValue Set.empty
                Set.unionMany [ kCols; aCols; hCols ]
            | LogicalPlan.GroupingSets(_, spec) ->
                let kCols = spec.KeyAliases |> List.map (snd >> ScalarExprOps.collectColumns) |> Set.unionMany
                let aCols = spec.Aggs |> List.map (snd >> ScalarExprOps.collectColumns) |> Set.unionMany
                Set.union kCols aCols
            | LogicalPlan.Sort(_, orderings) ->
                orderings |> List.map (fun o -> ScalarExprOps.collectColumns o.Expr) |> Set.unionMany
            | LogicalPlan.Distinct(_, subsetOpt, _, _) ->
                subsetOpt |> Option.map Set.ofList |> Option.defaultValue Set.empty
            | LogicalPlan.Explode(_, cols, _, _) -> Set.ofList cols
            | LogicalPlan.Unpivot(_, idx, onOpt, _, _) ->
                let idxCols = Set.ofList idx
                let onCols = onOpt |> Option.map Set.ofList |> Option.defaultValue Set.empty
                Set.union idxCols onCols
            | LogicalPlan.Rename(_, existing, _) -> Set.ofList existing
            | LogicalPlan.Limit _
            | LogicalPlan.Union _
            | LogicalPlan.WithRowIndex _
            | LogicalPlan.Reverse _
            | LogicalPlan.SkipLast _
            | LogicalPlan.Shared _
            | LogicalPlan.HorizontalConcat _ -> Set.empty

        let childCols =
            getInputs plan |> List.map collectReferencedColumns |> Set.unionMany

        Set.union currentCols childCols

    // =========================================================================
    // 3. Plan Graph Formatter / Pretty Printer
    // =========================================================================

    /// Formats a ScalarExpr purely symbolically into a human-readable algebraic representation.
    let rec formatScalar (expr: ScalarExpr) : string =
        match expr with
        | ScalarExpr.Column name -> sprintf "col(\"%s\")" name
        | ScalarExpr.Literal lit ->
            match lit with
            | LiteralValue.Null dt -> sprintf "null(%A)" dt
            | LiteralValue.Bool b -> sprintf "%b" b
            | LiteralValue.Int8 sb -> sprintf "%dy" sb
            | LiteralValue.Int16 s -> sprintf "%ds" s
            | LiteralValue.Int32 i -> sprintf "%d" i
            | LiteralValue.Int64 l -> sprintf "%dL" l
            | LiteralValue.UInt8 b -> sprintf "%duy" b
            | LiteralValue.UInt16 us -> sprintf "%dus" us
            | LiteralValue.UInt32 ui -> sprintf "%du" ui
            | LiteralValue.UInt64 ul -> sprintf "%dUL" ul
            | LiteralValue.Float16 h -> sprintf "%gh" (float32 h)
            | LiteralValue.Float32 f -> sprintf "%gf" f
            | LiteralValue.Float64 d -> sprintf "%g" d
            | LiteralValue.Decimal m -> sprintf "%sM" (m.ToString())
            | LiteralValue.String s -> sprintf "\"%s\"" s
            | LiteralValue.Date d -> sprintf "date(\"%s\")" (d.ToString "yyyy-MM-dd")
            | LiteralValue.DateTime dt -> sprintf "datetime(\"%s\")" (dt.ToString "o")
            | LiteralValue.DateTimeOffset dto -> sprintf "datetimeoffset(\"%s\")" (dto.ToString "o")
            | LiteralValue.TimeSpan ts -> sprintf "timespan(\"%s\")" (ts.ToString())
            | LiteralValue.TimeOnly to' -> sprintf "time(\"%s\")" (to'.ToString "o")

        | ScalarExpr.Binary(op, left, right) ->
            let opStr =
                match op with
                | BinaryOp.Eq -> "=="
                | BinaryOp.NotEq -> "!="
                | BinaryOp.EqMissing -> "<=>"
                | BinaryOp.NotEqMissing -> "!<=>"
                | BinaryOp.Lt -> "<"
                | BinaryOp.LtEq -> "<="
                | BinaryOp.Gt -> ">"
                | BinaryOp.GtEq -> ">="
                | BinaryOp.And -> "AND"
                | BinaryOp.Or -> "OR"
                | BinaryOp.Xor -> "XOR"
                | BinaryOp.Add -> "+"
                | BinaryOp.Subtract -> "-"
                | BinaryOp.Multiply -> "*"
                | BinaryOp.Divide -> "/"
                | BinaryOp.Modulo -> "%"
                | BinaryOp.TruncatedDivide -> "div"
                | BinaryOp.FloorDivide -> "//"
                | BinaryOp.Power -> "**"
                | BinaryOp.StringConcat -> "+"
                | BinaryOp.Like -> "LIKE"
                | BinaryOp.Contains -> "CONTAINS"
                | BinaryOp.StartsWith -> "STARTS_WITH"
                | BinaryOp.EndsWith -> "ENDS_WITH"
                | BinaryOp.RegexMatch -> "MATCHES"

            sprintf "(%s %s %s)" (formatScalar left) opStr (formatScalar right)

        | ScalarExpr.Unary(op, inner) ->
            match op with
            | UnaryOp.Not -> sprintf "NOT(%s)" (formatScalar inner)
            | UnaryOp.Negate -> sprintf "-(%s)" (formatScalar inner)
            | UnaryOp.IsNull -> sprintf "(%s IS NULL)" (formatScalar inner)
            | UnaryOp.IsNotNull -> sprintf "(%s IS NOT NULL)" (formatScalar inner)
            | UnaryOp.IsNan -> sprintf "(%s IS NAN)" (formatScalar inner)
            | UnaryOp.Abs -> sprintf "ABS(%s)" (formatScalar inner)

        | ScalarExpr.Cast(inner, targetType, isStrict) ->
            sprintf "CAST(%s AS %A%s)" (formatScalar inner) targetType (if isStrict then " STRICT" else "")

        | ScalarExpr.Function(fn, args, _) ->
            let argsStr = args |> List.map formatScalar |> String.concat ", "
            sprintf "%s(%s)" fn.Name argsStr

        | ScalarExpr.Aggregation(kind, exprOpt, isDistinct) ->
            let innerStr =
                match exprOpt with
                | Some e -> (if isDistinct then "DISTINCT " else "") + formatScalar e
                | None -> "*"
            sprintf "%A(%s)" kind innerStr

        | ScalarExpr.Window(inner, partitionBy, orderBy) ->
            let partStr =
                if partitionBy.IsEmpty then ""
                else " PARTITION BY " + (partitionBy |> List.map formatScalar |> String.concat ", ")
            let orderStr =
                if orderBy.IsEmpty then ""
                else " ORDER BY " + (orderBy |> List.map (fun o -> formatScalar o.Expr + (if o.Descending then " DESC" else " ASC")) |> String.concat ", ")
            sprintf "%s OVER (%s%s)" (formatScalar inner) partStr orderStr

        | ScalarExpr.SortBy(inner, orderBy) ->
            let orderStr =
                orderBy
                |> List.map (fun o -> formatScalar o.Expr + (if o.Descending then " DESC" else " ASC"))
                |> String.concat ", "
            sprintf "%s.sort_by([%s])" (formatScalar inner) orderStr

        | ScalarExpr.Conditional(test, ifTrue, ifFalse) ->
            sprintf "CASE WHEN %s THEN %s ELSE %s END" (formatScalar test) (formatScalar ifTrue) (formatScalar ifFalse)

        | ScalarExpr.Coalesce exprs ->
            sprintf "COALESCE(%s)" (exprs |> List.map formatScalar |> String.concat ", ")

        | ScalarExpr.ItemAccess(target, index) ->
            sprintf "%s[%s]" (formatScalar target) (formatScalar index)

        | ScalarExpr.Slice(target, offset, length) ->
            sprintf "%s[%d:%d]" (formatScalar target) offset (offset + int64 length)

        | ScalarExpr.Alias(inner, alias) ->
            sprintf "%s AS %s" (formatScalar inner) alias

        | ScalarExpr.EnumConvert(conversion, inner, _) ->
            sprintf "ENUM_%s(%s)" ((sprintf "%A" conversion).ToUpperInvariant()) (formatScalar inner)

        | ScalarExpr.Subquery(_, kind) ->
            sprintf "SUBQUERY(%A)" kind

        | ScalarExpr.Udf(name, args, _, _) ->
            let argsStr = args |> List.map formatScalar |> String.concat ", "
            sprintf "UDF:%s(%s)" name argsStr

        | ScalarExpr.InList(target, _, negated, _) ->
            let op = if negated then "NOT IN" else "IN"
            sprintf "(%s %s [...])" (formatScalar target) op

    let formatPlan (root: LogicalPlan) : string =
        let sb = StringBuilder()

        let rec formatNode (indent: string) (isLast: bool) (plan: LogicalPlan) =
            let marker = if isLast then "└── " else "├── "
            sb.Append(indent).Append(marker) |> ignore

            let nextIndent = indent + (if isLast then "    " else "│   ")

            match plan with
            | LogicalPlan.Scan(source, schema) ->
                let srcDesc =
                    match source with
                    | LogicalPlanSource.Slot index -> sprintf "Source[%d]" index
                    | LogicalPlanSource.SubqueryRef id -> sprintf "SubqueryRef(%s)" id
                    | LogicalPlanSource.Empty -> "Empty"

                sb.AppendLine(sprintf "Scan: %s columns=[%s]" srcDesc (String.concat ", " schema.ColumnNames))
                |> ignore

            | LogicalPlan.Filter(input, pred) ->
                sb.AppendLine(sprintf "Filter: %s" (formatScalar pred)) |> ignore
                formatNode nextIndent true input

            | LogicalPlan.Project(input, projs) ->
                let projStr =
                    projs |> List.map (fun (n, e) -> sprintf "%s=%s" n (formatScalar e)) |> String.concat ", "

                sb.AppendLine(sprintf "Project: [%s]" projStr) |> ignore
                formatNode nextIndent true input

            | LogicalPlan.WithColumns(input, cols) ->
                let colStr =
                    cols |> List.map (fun (n, e) -> sprintf "%s=%s" n (formatScalar e)) |> String.concat ", "

                sb.AppendLine(sprintf "WithColumns: [%s]" colStr) |> ignore
                formatNode nextIndent true input

            | LogicalPlan.Join(left, right, lOn, rOn, how, sfx, _, _) ->
                let lStr = lOn |> List.map formatScalar |> String.concat ", "
                let rStr = rOn |> List.map formatScalar |> String.concat ", "
                sb.AppendLine(sprintf "Join: how=%A suffix=%A leftOn=[%s] rightOn=[%s]" how sfx lStr rStr) |> ignore
                formatNode nextIndent false left
                formatNode nextIndent true right

            | LogicalPlan.JoinAsOf(left, right, spec) ->
                sb.AppendLine(sprintf "JoinAsOf: strategy=%A leftOn=%s rightOn=%s" spec.Strategy (formatScalar spec.LeftOn) (formatScalar spec.RightOn))
                |> ignore
                formatNode nextIndent false left
                formatNode nextIndent true right

            | LogicalPlan.GroupBy(input, keys, aggs, having, ord) ->
                let keyStr = keys |> List.map formatScalar |> String.concat ", "
                let aggStr =
                    aggs |> List.map (fun (n, e) -> sprintf "%s=%s" n (formatScalar e)) |> String.concat ", "
                let havingStr = having |> Option.map formatScalar |> Option.defaultValue "None"

                sb.AppendLine(sprintf "GroupBy: keys=[%s] aggs=[%s] having=%s maintainOrder=%b" keyStr aggStr havingStr ord)
                |> ignore
                formatNode nextIndent true input

            | LogicalPlan.GroupingSets(input, spec) ->
                let keyStr = spec.KeyAliases |> List.map fst |> String.concat ", "
                let aggStr = spec.Aggs |> List.map (fun (n, e) -> sprintf "%s=%s" n (formatScalar e)) |> String.concat ", "
                sb.AppendLine(sprintf "GroupingSets: keys=[%s] aggs=[%s] levels=%d" keyStr aggStr spec.Levels.Length)
                |> ignore
                formatNode nextIndent true input

            | LogicalPlan.Sort(input, orderings) ->
                let ordStr =
                    orderings
                    |> List.map (fun o -> sprintf "%s %s" (formatScalar o.Expr) (if o.Descending then "DESC" else "ASC"))
                    |> String.concat ", "
                sb.AppendLine(sprintf "Sort: [%s]" ordStr) |> ignore
                formatNode nextIndent true input

            | LogicalPlan.Limit(input, off, cnt) ->
                sb.AppendLine(sprintf "Limit: offset=%d count=%u" off cnt) |> ignore
                formatNode nextIndent true input

            | LogicalPlan.Distinct(input, subOpt, keep, ord) ->
                sb.AppendLine(sprintf "Distinct: subset=%A keep=%A maintainOrder=%b" subOpt keep ord) |> ignore
                formatNode nextIndent true input

            | LogicalPlan.Explode(input, cols, _, _) ->
                sb.AppendLine(sprintf "Explode: columns=%A" cols) |> ignore
                formatNode nextIndent true input

            | LogicalPlan.Unpivot(input, idx, onOpt, varN, valN) ->
                sb.AppendLine(sprintf "Unpivot: index=%A on=%A var=%s val=%s" idx onOpt varN valN) |> ignore
                formatNode nextIndent true input

            | LogicalPlan.Union(left, right, isAll) ->
                sb.AppendLine(sprintf "Union: isAll=%b" isAll) |> ignore
                formatNode nextIndent false left
                formatNode nextIndent true right

            | LogicalPlan.WithRowIndex(input, name, off) ->
                sb.AppendLine(sprintf "WithRowIndex: name=%s offset=%A" name off) |> ignore
                formatNode nextIndent true input

            | LogicalPlan.Rename(input, existing, newN) ->
                sb.AppendLine(sprintf "Rename: existing=%A new=%A" existing newN) |> ignore
                formatNode nextIndent true input

            | LogicalPlan.Reverse input ->
                sb.AppendLine "Reverse" |> ignore
                formatNode nextIndent true input

            | LogicalPlan.SkipLast(input, count) ->
                sb.AppendLine(sprintf "SkipLast: count=%u" count) |> ignore
                formatNode nextIndent true input

            | LogicalPlan.Shared(id, input) ->
                sb.AppendLine(sprintf "Shared: id=%d" id) |> ignore
                formatNode nextIndent true input

            | LogicalPlan.HorizontalConcat inputs ->
                sb.AppendLine(sprintf "HorizontalConcat: inputs=%d" inputs.Length) |> ignore
                inputs |> List.iteri (fun i inp -> formatNode nextIndent (i = inputs.Length - 1) inp)

            | LogicalPlan.Empty schema ->
                sb.AppendLine(sprintf "Empty: columns=[%s]" (String.concat ", " schema.ColumnNames))
                |> ignore

        formatNode "" true root
        sb.ToString()

