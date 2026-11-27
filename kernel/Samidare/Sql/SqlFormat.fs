namespace Kaminari.Samidare.Sql

open System
open System.Text

/// Writes a syntax tree back as SQL with every compound expression parenthesized, so that the text shows how the
/// parser grouped it. Literals and names are written as they were (numbers as written, quoted names quoted).
module internal SqlFormat =

    let private name (part: SqlIdentifier) =
        if part.Quoted then "\"" + part.Text.Replace("\"", "\"\"") + "\"" else part.Text

    let private qualified (parts: SqlName) = parts |> List.map name |> String.concat "."

    let private quote (value: string) = "'" + value.Replace("'", "''") + "'"

    let private literal (value: SqlLiteral) =
        match value with
        | SqlLiteral.Integer text
        | SqlLiteral.Decimal text
        | SqlLiteral.Float text -> text
        | SqlLiteral.Suffixed(number, suffix) -> number + suffix
        | SqlLiteral.String value -> quote value
        | SqlLiteral.Boolean true -> "TRUE"
        | SqlLiteral.Boolean false -> "FALSE"
        | SqlLiteral.Null -> "NULL"
        | SqlLiteral.Typed(typeName, value) -> typeName + " " + quote value
        | SqlLiteral.Interval(value, unit) -> "INTERVAL " + quote value + " " + unit

    let private binaryOp (op: SqlBinaryOp) =
        match op with
        | SqlBinaryOp.Or -> "OR"
        | SqlBinaryOp.And -> "AND"
        | SqlBinaryOp.Eq -> "="
        | SqlBinaryOp.NotEq -> "<>"
        | SqlBinaryOp.Lt -> "<"
        | SqlBinaryOp.LtEq -> "<="
        | SqlBinaryOp.Gt -> ">"
        | SqlBinaryOp.GtEq -> ">="
        | SqlBinaryOp.Add -> "+"
        | SqlBinaryOp.Subtract -> "-"
        | SqlBinaryOp.Multiply -> "*"
        | SqlBinaryOp.Divide -> "/"
        | SqlBinaryOp.Modulo -> "%"
        | SqlBinaryOp.IntegerDivide -> "DIV"
        | SqlBinaryOp.Concat -> "||"

    let rec private typeName (t: SqlTypeName) =
        if not t.Arguments.IsEmpty then t.Name + "(" + (t.Arguments |> List.map typeName |> String.concat ", ") + ")"
        elif t.Parameters.IsEmpty then t.Name
        else t.Name + "(" + (t.Parameters |> List.map string |> String.concat ", ") + ")"

    let private join (separator: string) (items: string list) = String.Join(separator, items)

    let private not' (negated: bool) = if negated then "NOT " else ""

    let rec expression (e: SqlExpr) : string =
        match e.Kind with
        | SqlExprKind.Literal value -> literal value
        | SqlExprKind.Column parts -> qualified parts
        | SqlExprKind.Unary(SqlUnaryOp.Not, operand) -> $"(NOT {expression operand})"
        | SqlExprKind.Unary(SqlUnaryOp.Negate, operand) -> $"(-{expression operand})"
        | SqlExprKind.Unary(SqlUnaryOp.Plus, operand) -> $"(+{expression operand})"
        | SqlExprKind.Binary(op, left, right) -> $"({expression left} {binaryOp op} {expression right})"
        | SqlExprKind.IsNull(operand, negated) -> $"({expression operand} IS {not' negated}NULL)"
        | SqlExprKind.IsDistinctFrom(left, right, negated) -> $"({expression left} IS {not' negated}DISTINCT FROM {expression right})"
        | SqlExprKind.Between(operand, low, high, negated) ->
            $"({expression operand} {not' negated}BETWEEN {expression low} AND {expression high})"
        | SqlExprKind.InList(operand, items, negated) ->
            let list = items |> List.map expression |> join ", "
            $"({expression operand} {not' negated}IN ({list}))"
        | SqlExprKind.InSubquery(operand, q, negated) -> $"({expression operand} {not' negated}IN ({query q}))"
        | SqlExprKind.Quantified(operand, op, all, q) ->
            let quantifier = if all then "ALL" else "ANY"
            $"({expression operand} {binaryOp op} {quantifier} ({query q}))"
        | SqlExprKind.Like(operand, pattern, escape, negated) ->
            let escape = escape |> Option.map (fun e -> " ESCAPE " + expression e) |> Option.defaultValue ""
            $"({expression operand} {not' negated}LIKE {expression pattern}{escape})"
        | SqlExprKind.ILike(operand, pattern, escape, negated) ->
            let escape = escape |> Option.map (fun e -> " ESCAPE " + expression e) |> Option.defaultValue ""
            $"({expression operand} {not' negated}ILIKE {expression pattern}{escape})"
        | SqlExprKind.RLike(operand, pattern, negated) -> $"({expression operand} {not' negated}RLIKE {expression pattern})"
        | SqlExprKind.Exists q -> $"(EXISTS ({query q}))"
        | SqlExprKind.ScalarSubquery q -> $"({query q})"
        | SqlExprKind.Case(operand, whens, otherwise) ->
            let builder = StringBuilder("(CASE")
            operand |> Option.iter (fun o -> builder.Append(' ').Append(expression o) |> ignore)
            for test, value in whens do
                builder.Append(" WHEN ").Append(expression test).Append(" THEN ").Append(expression value) |> ignore
            otherwise |> Option.iter (fun o -> builder.Append(" ELSE ").Append(expression o) |> ignore)
            builder.Append(" END)").ToString()
        | SqlExprKind.Cast(operand, t, isTry) -> $"""{(if isTry then "TRY_CAST" else "CAST")}({expression operand} AS {typeName t})"""
        | SqlExprKind.Extract(field, operand) -> $"EXTRACT({field} FROM {expression operand})"
        | SqlExprKind.Subscript(target, key) -> $"({expression target}[{expression key}])"
        | SqlExprKind.MapLiteral entries ->
            entries |> List.map (fun (k, v) -> $"{expression k}: {expression v}") |> String.concat ", " |> sprintf "MAP {%s}"
        | SqlExprKind.Function call ->
            let arguments =
                if call.Star then "*"
                else
                    (if call.Distinct then "DISTINCT " else "")
                    + (call.Arguments |> List.map expression |> join ", ")
                    + (if call.IgnoreNulls then " IGNORE NULLS" else "")
            let over = call.Over |> Option.map (fun w -> " OVER (" + window w + ")") |> Option.defaultValue ""
            $"{qualified call.Name}({arguments}){over}"

    and private window (w: SqlWindow) =
        [ if not w.PartitionBy.IsEmpty then yield "PARTITION BY " + (w.PartitionBy |> List.map expression |> join ", ")
          if not w.OrderBy.IsEmpty then yield "ORDER BY " + (w.OrderBy |> List.map orderItem |> join ", ")
          match w.Frame with
          | Some frame -> yield frameText frame
          | None -> () ]
        |> join " "

    and private frameText (frame: SqlFrame) =
        let unit =
            match frame.Unit with
            | SqlFrameUnit.Rows -> "ROWS"
            | SqlFrameUnit.Range -> "RANGE"
            | SqlFrameUnit.Groups -> "GROUPS"
        let bound (b: SqlFrameBound) =
            match b with
            | SqlFrameBound.UnboundedPreceding -> "UNBOUNDED PRECEDING"
            | SqlFrameBound.Preceding e -> expression e + " PRECEDING"
            | SqlFrameBound.CurrentRow -> "CURRENT ROW"
            | SqlFrameBound.Following e -> expression e + " FOLLOWING"
            | SqlFrameBound.UnboundedFollowing -> "UNBOUNDED FOLLOWING"
        match frame.End with
        | Some last -> $"{unit} BETWEEN {bound frame.Start} AND {bound last}"
        | None -> $"{unit} {bound frame.Start}"

    and private orderItem (item: SqlOrderItem) =
        let direction =
            match item.Direction with
            | SqlSortDirection.Default -> ""
            | SqlSortDirection.Ascending -> " ASC"
            | SqlSortDirection.Descending -> " DESC"
        let nulls =
            match item.Nulls with
            | SqlNullOrder.Default -> ""
            | SqlNullOrder.First -> " NULLS FIRST"
            | SqlNullOrder.Last -> " NULLS LAST"
        expression item.Expr + direction + nulls

    and private alias (a: SqlTableAlias option) =
        match a with
        | None -> ""
        | Some a when a.Columns.IsEmpty -> " AS " + name a.Name
        | Some a -> " AS " + name a.Name + "(" + (a.Columns |> List.map name |> join ", ") + ")"

    and private tableRef (t: SqlTableRef) =
        match t with
        | SqlTableRef.Table(parts, a, _) -> qualified parts + alias a
        | SqlTableRef.Derived(q, a, _) -> "(" + query q + ")" + alias a
        | SqlTableRef.Join(left, right, kind, condition, _) ->
            let kindText =
                match kind with
                | SqlJoinKind.Inner -> "JOIN"
                | SqlJoinKind.Left -> "LEFT JOIN"
                | SqlJoinKind.Right -> "RIGHT JOIN"
                | SqlJoinKind.Full -> "FULL JOIN"
                | SqlJoinKind.Cross -> "CROSS JOIN"
            let conditionText =
                match condition with
                | SqlJoinCondition.On e -> " ON " + expression e
                | SqlJoinCondition.Using columns -> " USING (" + (columns |> List.map name |> join ", ") + ")"
                | SqlJoinCondition.None -> ""
            $"({tableRef left} {kindText} {tableRef right}{conditionText})"

    and private groupingItem (g: SqlGroupingItem) =
        let items (gs: SqlGroupingItem list) = gs |> List.map groupingItem |> join ", "
        match g with
        | SqlGroupingItem.Expr e -> expression e
        | SqlGroupingItem.Set(exprs, _) -> "(" + (exprs |> List.map expression |> join ", ") + ")"
        | SqlGroupingItem.Rollup(gs, _) -> "ROLLUP (" + items gs + ")"
        | SqlGroupingItem.Cube(gs, _) -> "CUBE (" + items gs + ")"
        | SqlGroupingItem.GroupingSets(gs, _) -> "GROUPING SETS (" + items gs + ")"

    and private select (s: SqlSelect) =
        let item (i: SqlSelectItem) =
            match i with
            | SqlSelectItem.Wildcard(None, _) -> "*"
            | SqlSelectItem.Wildcard(Some q, _) -> qualified q + ".*"
            | SqlSelectItem.Expr(e, None) -> expression e
            | SqlSelectItem.Expr(e, Some a) -> expression e + " AS " + name a
        [ yield "SELECT " + (if s.Distinct then "DISTINCT " else "") + (s.Items |> List.map item |> join ", ")
          if not s.From.IsEmpty then yield "FROM " + (s.From |> List.map tableRef |> join ", ")
          match s.Where with
          | Some w -> yield "WHERE " + expression w
          | None -> ()
          if not s.GroupBy.IsEmpty then yield "GROUP BY " + (s.GroupBy |> List.map groupingItem |> join ", ")
          match s.Having with
          | Some h -> yield "HAVING " + expression h
          | None -> () ]
        |> join " "

    and private body (b: SqlQueryBody) =
        match b with
        | SqlQueryBody.Select s -> select s
        | SqlQueryBody.Nested q -> "(" + query q + ")"
        | SqlQueryBody.Values(rows, _) ->
            "VALUES " + (rows |> List.map (fun row -> "(" + (row |> List.map expression |> join ", ") + ")") |> join ", ")
        | SqlQueryBody.SetOperation(op, all, left, right, _) ->
            let opText =
                match op with
                | SqlSetOp.Union -> "UNION"
                | SqlSetOp.Intersect -> "INTERSECT"
                | SqlSetOp.Except -> "EXCEPT"
            $"""({body left}) {opText}{(if all then " ALL" else "")} ({body right})"""

    and query (q: SqlQuery) : string =
        [ if not q.With.IsEmpty then
              let cte (c: SqlCommonTable) =
                  let columns = if c.Columns.IsEmpty then "" else "(" + (c.Columns |> List.map name |> join ", ") + ")"
                  name c.Name + columns + " AS (" + query c.Query + ")"
              yield "WITH " + (if q.Recursive then "RECURSIVE " else "") + (q.With |> List.map cte |> join ", ")
          yield body q.Body
          if not q.OrderBy.IsEmpty then yield "ORDER BY " + (q.OrderBy |> List.map orderItem |> join ", ")
          match q.Limit with
          | Some l -> yield "LIMIT " + expression l
          | None -> ()
          match q.Offset with
          | Some o -> yield "OFFSET " + expression o
          | None -> () ]
        |> join " "
