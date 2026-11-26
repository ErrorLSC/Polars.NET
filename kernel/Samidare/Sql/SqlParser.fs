namespace Kaminari.Samidare.Sql

open System

/// Reads SQL text into the syntax tree (SqlAst.fs): recursive descent for queries, precedence climbing for
/// expressions. It checks the grammar only; names, types and meaning are the binder's.
///
/// Expression precedence, loosest first: OR; AND; NOT; comparisons, IS, [NOT] IN, BETWEEN, LIKE (which do not
/// chain); `||`; `+ -`; `* / %`; unary `- +`. Set operations: INTERSECT before UNION and EXCEPT, left to right; the
/// query's ORDER BY and limits apply to the whole set operation. In FROM, JOIN binds before the comma.
module internal SqlParser =

    /// Keywords a misspelt word may have meant.
    let private suggestable =
        [ "SELECT"; "FROM"; "WHERE"; "GROUP"; "BY"; "HAVING"; "ORDER"; "LIMIT"; "UNION"; "INTERSECT"; "EXCEPT"; "JOIN"
          "LEFT"; "RIGHT"; "INNER"; "OUTER"; "CROSS"; "ON"; "USING"; "AS"; "AND"; "OR"; "NOT"; "IN"; "BETWEEN"; "LIKE"
          "CASE"; "WHEN"; "THEN"; "ELSE"; "END"; "DISTINCT"; "WITH"; "OVER"; "PARTITION"; "ROLLUP"; "EXISTS"; "NULLS"
          "OFFSET"; "FETCH"; "CAST"; "DESC"; "ASC" ]

    /// The edit distance of two words, ignoring case (insertions, deletions, substitutions, adjacent swaps).
    let private distance (a: string) (b: string) =
        let a, b = a.ToUpperInvariant(), b.ToUpperInvariant()
        let d = Array2D.zeroCreate (a.Length + 1) (b.Length + 1)
        for i in 0 .. a.Length do d.[i, 0] <- i
        for j in 0 .. b.Length do d.[0, j] <- j
        for i in 1 .. a.Length do
            for j in 1 .. b.Length do
                let cost = if a.[i - 1] = b.[j - 1] then 0 else 1
                d.[i, j] <- min (min (d.[i - 1, j] + 1) (d.[i, j - 1] + 1)) (d.[i - 1, j - 1] + cost)
                if i > 1 && j > 1 && a.[i - 1] = b.[j - 2] && a.[i - 2] = b.[j - 1] then
                    d.[i, j] <- min d.[i, j] (d.[i - 2, j - 2] + 1)
        d.[a.Length, b.Length]

    /// The keyword a word is a likely misspelling of.
    let private nearKeyword (word: string) =
        if word.Length < 2 then None
        else
            let limit = if word.Length <= 4 then 1 else 2
            suggestable
            |> List.map (fun k -> k, distance word k)
            |> List.filter (fun (k, d) -> d > 0 && d <= limit && not (String.Equals(k, word, StringComparison.OrdinalIgnoreCase)))
            |> List.sortBy snd
            |> List.tryHead
            |> Option.map fst

    /// Deeper nesting than this is refused (a deeper text is not a query a person wrote).
    let private maxDepth = 200

    type private Parser(dialect: SqlDialect, text: string, tokens: SqlToken array) =
        let mutable pos = 0
        let mutable lastEnd = 0
        let mutable depth = 0
        /// The token position of the last alias taken without AS, to explain a misspelt keyword read as one.
        let mutable lastImplicitAlias = -1
        /// Whether a query starts at a token position, once read (nested `((SELECT` would be read again per level).
        let startsQuery = System.Collections.Generic.Dictionary<int, bool>()

        member _.Peek = tokens.[pos]
        member _.PeekAt(k: int) = tokens.[min (pos + k) (tokens.Length - 1)]
        member _.Position = pos
        member _.Rewind(p: int) =
            pos <- p
            lastEnd <- if p = 0 then 0 else tokens.[p - 1].Span.End

        member _.Advance() =
            let t = tokens.[pos]
            if pos < tokens.Length - 1 then pos <- pos + 1
            lastEnd <- t.Span.End
            t

        /// The span from `start` (a token's start offset) to the end of the last token taken.
        member _.SpanFrom(start: int) : SqlSpan = { Start = start; Length = max 0 (lastEnd - start) }

        member _.Fail<'T>(reason: string, span: SqlSpan, ?suggestion: string) : 'T =
            raise (SqlException(reason, text, span, suggestion))

        member this.Describe(t: SqlToken) =
            match t.Kind with
            | SqlTokenKind.End -> "the end of the text"
            | SqlTokenKind.String -> $"the string '{t.Text}'"
            | SqlTokenKind.QuotedIdentifier -> $"the name \"{t.Text}\""
            | _ -> $"'{t.Text}'"

        /// Fails at the next token: expected `what` `where`, with a keyword the token may misspell.
        member this.Expected<'T>(what: string, where: string) : 'T =
            let t = this.Peek
            let where = if String.IsNullOrEmpty where then "" else " " + where
            let near = if t.Kind = SqlTokenKind.Word then nearKeyword t.Text else None
            let afterAlias =
                // `SELECT a FORM t`: FORM was taken as a's alias, and t is unexpected.
                if lastImplicitAlias >= 0 && lastImplicitAlias = pos - 1 then
                    let alias = tokens.[lastImplicitAlias]
                    nearKeyword alias.Text |> Option.map (fun k -> k, alias)
                else None
            match afterAlias with
            | Some(keyword, alias) ->
                this.Fail(
                    $"Expected {what}{where}, found {this.Describe t}",
                    t.Span,
                    $"Did you mean {keyword}? '{alias.Text}' was read as an alias of what precedes it.")
            | None ->
                match near with
                | Some keyword -> this.Fail($"Expected {what}{where}, found {this.Describe t}", t.Span, $"Did you mean {keyword}?")
                | None -> this.Fail($"Expected {what}{where}, found {this.Describe t}", t.Span)

        member this.IsWord(keyword: string, ?ahead: int) =
            let t = this.PeekAt(defaultArg ahead 0)
            t.Kind = SqlTokenKind.Word && String.Equals(t.Text, keyword, StringComparison.OrdinalIgnoreCase)

        member this.IsSymbol(symbol: string, ?ahead: int) =
            let t = this.PeekAt(defaultArg ahead 0)
            t.Kind = SqlTokenKind.Symbol && t.Text = symbol

        member this.AcceptWord(keyword: string) =
            if this.IsWord keyword then this.Advance() |> ignore; true else false

        member this.AcceptSymbol(symbol: string) =
            if this.IsSymbol symbol then this.Advance() |> ignore; true else false

        member this.ExpectWord(keyword: string, where: string) =
            if not (this.AcceptWord keyword) then this.Expected(keyword, where)

        member this.ExpectSymbol(symbol: string, where: string) =
            if not (this.AcceptSymbol symbol) then this.Expected($"'{symbol}'", where)

        member this.Enter(span: SqlSpan) =
            depth <- depth + 1
            if depth > maxDepth then this.Fail($"The text nests deeper than {maxDepth} levels", span)

        member _.Leave() = depth <- depth - 1

        // ------------------------------------------------------------------------------------------------------
        // Names
        // ------------------------------------------------------------------------------------------------------

        /// Whether the next token can start a name (a word that is not reserved, or a quoted name).
        member this.AtName(?ahead: int) =
            let t = this.PeekAt(defaultArg ahead 0)
            match t.Kind with
            | SqlTokenKind.QuotedIdentifier -> true
            | SqlTokenKind.Word -> not (SqlLexer.isReserved t.Text)
            | _ -> false

        member this.Identifier(where: string) : SqlIdentifier =
            let t = this.Peek
            match t.Kind with
            | SqlTokenKind.QuotedIdentifier ->
                this.Advance() |> ignore
                { Text = t.Text; Quoted = true; Span = t.Span }
            | SqlTokenKind.Word when not (SqlLexer.isReserved t.Text) ->
                this.Advance() |> ignore
                { Text = t.Text; Quoted = false; Span = t.Span }
            | SqlTokenKind.Word ->
                this.Fail($"Expected a name {where}, found the keyword {t.Text.ToUpperInvariant()}", t.Span,
                          $"Quote it to use it as a name: \"{t.Text}\".")
            | _ -> this.Expected("a name", where)

        /// `a`, `a.b`, `a.b.c`.
        member this.Name(where: string) : SqlName =
            let first = this.Identifier where
            let parts = ResizeArray [ first ]
            while this.IsSymbol "." && this.AtName 1 do
                this.Advance() |> ignore
                parts.Add(this.Identifier where)
            List.ofSeq parts

        /// `[AS] alias [(columns)]`; without AS only a non-reserved word or a quoted name.
        member this.Alias(where: string) : SqlTableAlias option =
            let explicitAs = this.AcceptWord "AS"
            if explicitAs || this.AtName() then
                let aliasPosition = this.Position
                let name = this.Identifier where
                if not explicitAs && not name.Quoted then lastImplicitAlias <- aliasPosition
                let columns =
                    if this.IsSymbol "(" then this.Parenthesized(fun () -> this.CommaList(fun () -> this.Identifier "in the column list of an alias"))
                    else []
                Some { Name = name; Columns = columns }
            else None

        member this.Parenthesized<'T>(inner: unit -> 'T) : 'T =
            let opening = this.Peek.Span
            this.ExpectSymbol("(", "")
            let value = inner ()
            if not (this.AcceptSymbol ")") then
                this.Fail($"Expected ')' to close the '(' at line {fst (SqlException.Position(text, opening.Start))}, column {snd (SqlException.Position(text, opening.Start))}, found {this.Describe this.Peek}", this.Peek.Span)
            value

        member this.CommaList<'T>(item: unit -> 'T) : 'T list =
            let items = ResizeArray [ item () ]
            while this.AcceptSymbol "," do
                items.Add(item ())
            List.ofSeq items

        // ------------------------------------------------------------------------------------------------------
        // Queries
        // ------------------------------------------------------------------------------------------------------

        /// Whether the tokens from `ahead` start a query: SELECT or WITH, or opening parentheses around a query that
        /// a set operation, ORDER BY or a limit continues or the enclosing parenthesis closes (`((SELECT ..) + 1)` is
        /// an expression), decided by reading it.
        member this.StartsQuery(ahead: int) =
            // VALUES is a query only before its first row's parenthesis (otherwise a name).
            let opens (k: int) = this.IsWord("SELECT", k) || this.IsWord("WITH", k) || (this.IsWord("VALUES", k) && this.IsSymbol("(", k + 1))
            if opens ahead then true
            elif this.IsSymbol("(", ahead) then
                let mutable k = ahead
                while this.IsSymbol("(", k) do
                    k <- k + 1
                if not (opens k) then false
                else
                    match startsQuery.TryGetValue(this.Position + ahead) with
                    | true, known -> known
                    | _ ->
                        let saved, savedDepth, savedAlias = this.Position, depth, lastImplicitAlias
                        this.Rewind(saved + ahead)
                        let isQuery =
                            try
                                this.Parenthesized(fun () -> this.Query()) |> ignore
                                this.IsSymbol ")" || this.IsWord "UNION" || this.IsWord "INTERSECT" || this.IsWord "EXCEPT"
                                || this.IsWord "ORDER" || this.IsWord "LIMIT" || this.IsWord "OFFSET" || this.IsWord "FETCH"
                            with :? SqlException -> false
                        this.Rewind saved
                        depth <- savedDepth
                        lastImplicitAlias <- savedAlias
                        startsQuery.[saved + ahead] <- isQuery
                        isQuery
            else false

        member this.Query() : SqlQuery =
            let start = this.Peek.Span.Start
            this.Enter this.Peek.Span
            let recursive, ctes =
                if this.AcceptWord "WITH" then
                    let recursive = this.AcceptWord "RECURSIVE"
                    recursive, this.CommaList(fun () -> this.CommonTable())
                else false, []
            let body = this.SetExpression()
            let orderBy =
                if this.AcceptWord "ORDER" then
                    this.ExpectWord("BY", "after ORDER")
                    this.CommaList(fun () -> this.OrderItem())
                else []
            let mutable limit = None
            let mutable offset = None
            if this.AcceptWord "LIMIT" then
                limit <- Some(this.Expression())
                if this.AcceptWord "OFFSET" then offset <- Some(this.Expression())
            else
                if this.AcceptWord "OFFSET" then
                    offset <- Some(this.Expression())
                    (this.AcceptWord "ROWS" || this.AcceptWord "ROW") |> ignore
                if this.AcceptWord "FETCH" then
                    if not (this.AcceptWord "FIRST" || this.AcceptWord "NEXT") then this.Expected("FIRST or NEXT", "after FETCH")
                    limit <- Some(if this.IsWord "ROWS" || this.IsWord "ROW" then { Kind = SqlExprKind.Literal(SqlLiteral.Integer "1"); Span = this.Peek.Span } else this.Expression())
                    if not (this.AcceptWord "ROWS" || this.AcceptWord "ROW") then this.Expected("ROWS", "in FETCH FIRST n ROWS ONLY")
                    this.ExpectWord("ONLY", "in FETCH FIRST n ROWS ONLY")
            this.Leave()
            { With = ctes; Recursive = recursive; Body = body; OrderBy = orderBy; Limit = limit; Offset = offset; Span = this.SpanFrom start }

        member this.CommonTable() : SqlCommonTable =
            let start = this.Peek.Span.Start
            let name = this.Identifier "for a common table expression"
            let columns =
                if this.IsSymbol "(" then this.Parenthesized(fun () -> this.CommaList(fun () -> this.Identifier "in the column list of a common table expression"))
                else []
            this.ExpectWord("AS", $"after the name of the common table expression {name.Text}")
            let query = this.Parenthesized(fun () -> this.Query())
            { Name = name; Columns = columns; Query = query; Span = this.SpanFrom start }

        /// UNION and EXCEPT over INTERSECT terms, left to right.
        member this.SetExpression() : SqlQueryBody =
            let start = this.Peek.Span.Start
            let mutable left = this.IntersectTerm()
            let mutable go = true
            while go do
                let op =
                    if this.AcceptWord "UNION" then Some SqlSetOp.Union
                    elif this.AcceptWord "EXCEPT" then Some SqlSetOp.Except
                    else None
                match op with
                | Some op ->
                    let all = this.SetQuantifier()
                    let right = this.IntersectTerm()
                    left <- SqlQueryBody.SetOperation(op, all, left, right, this.SpanFrom start)
                | None -> go <- false
            left

        member this.IntersectTerm() : SqlQueryBody =
            let start = this.Peek.Span.Start
            let mutable left = this.QueryPrimary()
            while this.AcceptWord "INTERSECT" do
                let all = this.SetQuantifier()
                let right = this.QueryPrimary()
                left <- SqlQueryBody.SetOperation(SqlSetOp.Intersect, all, left, right, this.SpanFrom start)
            left

        member this.SetQuantifier() =
            if this.AcceptWord "ALL" then true
            else
                this.AcceptWord "DISTINCT" |> ignore
                false

        member this.QueryPrimary() : SqlQueryBody =
            if this.IsWord "SELECT" then SqlQueryBody.Select(this.Select())
            elif this.IsWord "VALUES" && this.IsSymbol("(", 1) then
                let start = this.Advance().Span.Start
                let rows = this.CommaList(fun () -> this.Parenthesized(fun () -> this.CommaList(fun () -> this.Expression())))
                SqlQueryBody.Values(rows, this.SpanFrom start)
            elif this.IsSymbol "(" then SqlQueryBody.Nested(this.Parenthesized(fun () -> this.Query()))
            else this.Expected("SELECT", "to start a query")

        member this.Select() : SqlSelect =
            let start = this.Peek.Span.Start
            this.ExpectWord("SELECT", "")
            let distinct =
                if this.AcceptWord "DISTINCT" then true
                else
                    this.AcceptWord "ALL" |> ignore
                    false
            let items = this.CommaList(fun () -> this.SelectItem())
            let from = if this.AcceptWord "FROM" then this.CommaList(fun () -> this.TableReference()) else []
            let where = if this.AcceptWord "WHERE" then Some(this.Expression()) else None
            let groupBy =
                if this.AcceptWord "GROUP" then
                    this.ExpectWord("BY", "after GROUP")
                    this.CommaList(fun () -> this.GroupingItem())
                else []
            let having = if this.AcceptWord "HAVING" then Some(this.Expression()) else None
            if this.IsWord "WINDOW" || this.IsWord "QUALIFY" then
                this.Fail($"{this.Peek.Text.ToUpperInvariant()} clauses are not supported", this.Peek.Span)
            { Distinct = distinct; Items = items; From = from; Where = where; GroupBy = groupBy; Having = having; Span = this.SpanFrom start }

        member this.SelectItem() : SqlSelectItem =
            let start = this.Peek.Span.Start
            if this.IsSymbol "*" then
                this.Advance() |> ignore
                SqlSelectItem.Wildcard(None, this.SpanFrom start)
            else
                // `t.*`, `s.t.*`: names, each followed by a dot, then a star.
                let mutable k = 0
                while this.AtName k && this.IsSymbol(".", k + 1) do
                    k <- k + 2
                if k > 0 && this.IsSymbol("*", k) then
                    let qualifier = this.Name "before .*"
                    this.ExpectSymbol(".", "")
                    this.ExpectSymbol("*", "")
                    SqlSelectItem.Wildcard(Some qualifier, this.SpanFrom start)
                else
                    let expr = this.Expression()
                    let alias =
                        if this.AcceptWord "AS" then Some(this.Identifier "after AS")
                        elif this.AtName() then
                            lastImplicitAlias <- this.Position
                            Some(this.Identifier "")
                        else None
                    SqlSelectItem.Expr(expr, alias)

        member this.GroupingItem() : SqlGroupingItem =
            let start = this.Peek.Span.Start
            this.Enter this.Peek.Span
            let item = this.GroupingItemBody start
            this.Leave()
            item

        member this.GroupingItemBody(start: int) : SqlGroupingItem =
            let groupList () =
                this.Parenthesized(fun () -> if this.IsSymbol ")" then [] else this.CommaList(fun () -> this.GroupingItem()))
            if this.IsWord "ROLLUP" && this.IsSymbol("(", 1) then
                this.Advance() |> ignore
                let items = groupList ()
                SqlGroupingItem.Rollup(items, this.SpanFrom start)
            elif this.IsWord "CUBE" && this.IsSymbol("(", 1) then
                this.Advance() |> ignore
                let items = groupList ()
                SqlGroupingItem.Cube(items, this.SpanFrom start)
            elif this.IsWord "GROUPING" && this.IsWord("SETS", 1) then
                this.Advance() |> ignore
                this.Advance() |> ignore
                let items = groupList ()
                SqlGroupingItem.GroupingSets(items, this.SpanFrom start)
            elif this.IsSymbol "(" && this.IsSymbol(")", 1) then
                this.Advance() |> ignore
                this.Advance() |> ignore
                SqlGroupingItem.Set([], this.SpanFrom start)
            elif this.IsSymbol "(" && not (this.StartsQuery 1) then
                // `(a, b)` is a set of columns; `(a)` and `(a + b) * 2` are expressions.
                let saved = this.Position
                let exprs = this.Parenthesized(fun () -> this.CommaList(fun () -> this.Expression()))
                if exprs.Length > 1 && (this.IsSymbol "," || this.IsSymbol ")" || this.AtGroupByEnd()) then
                    SqlGroupingItem.Set(exprs, this.SpanFrom start)
                else
                    this.Rewind saved
                    SqlGroupingItem.Expr(this.Expression())
            else SqlGroupingItem.Expr(this.Expression())

        /// Whether GROUP BY ends at the next token (a clause, a set operation, the end of a parenthesized query).
        member this.AtGroupByEnd() =
            this.Peek.Kind = SqlTokenKind.End || this.IsSymbol ";"
            || [ "HAVING"; "ORDER"; "LIMIT"; "OFFSET"; "FETCH"; "UNION"; "INTERSECT"; "EXCEPT"; "WINDOW"; "QUALIFY" ] |> List.exists (fun k -> this.IsWord k)

        // ------------------------------------------------------------------------------------------------------
        // FROM
        // ------------------------------------------------------------------------------------------------------

        member this.TableReference() : SqlTableRef =
            let start = this.Peek.Span.Start
            let mutable left = this.TablePrimary()
            let mutable go = true
            while go do
                let kind =
                    if this.AcceptWord "JOIN" then Some SqlJoinKind.Inner
                    elif this.IsWord "INNER" then
                        this.Advance() |> ignore
                        this.ExpectWord("JOIN", "after INNER")
                        Some SqlJoinKind.Inner
                    elif this.IsWord "CROSS" then
                        this.Advance() |> ignore
                        this.ExpectWord("JOIN", "after CROSS")
                        Some SqlJoinKind.Cross
                    elif this.IsWord "LEFT" || this.IsWord "RIGHT" || this.IsWord "FULL" then
                        let side = this.Advance().Text.ToUpperInvariant()
                        this.AcceptWord "OUTER" |> ignore
                        this.ExpectWord("JOIN", $"after {side}")
                        Some(match side with "LEFT" -> SqlJoinKind.Left | "RIGHT" -> SqlJoinKind.Right | _ -> SqlJoinKind.Full)
                    elif this.IsWord "NATURAL" then this.Fail("NATURAL joins are not supported", this.Peek.Span, "Name the columns with USING (...) or ON.")
                    else None
                match kind with
                | Some kind ->
                    let right = this.TablePrimary()
                    let condition =
                        if kind = SqlJoinKind.Cross then SqlJoinCondition.None
                        elif this.AcceptWord "ON" then SqlJoinCondition.On(this.Expression())
                        elif this.AcceptWord "USING" then
                            if not (this.IsSymbol "(") then this.Expected("'(' and the joined columns", "after USING")
                            SqlJoinCondition.Using(this.Parenthesized(fun () -> this.CommaList(fun () -> this.Identifier "in USING")))
                        else this.Expected("ON or USING", "after the joined table")
                    left <- SqlTableRef.Join(left, right, kind, condition, this.SpanFrom start)
                | None -> go <- false
            left

        member this.TablePrimary() : SqlTableRef =
            let start = this.Peek.Span.Start
            if this.IsSymbol "(" then
                if this.StartsQuery 1 then
                    let query = this.Parenthesized(fun () -> this.Query())
                    let alias = this.Alias "for a derived table"
                    SqlTableRef.Derived(query, alias, this.SpanFrom start)
                else
                    this.Enter this.Peek.Span
                    let inner = this.Parenthesized(fun () -> this.TableReference())
                    this.Leave()
                    inner
            elif this.IsWord "LATERAL" then this.Fail("LATERAL is not supported", this.Peek.Span)
            else
                let name = this.Name "for a table"
                if this.IsSymbol "(" then
                    this.Fail("Table functions are not supported", this.Peek.Span)
                let alias = this.Alias "for a table"
                SqlTableRef.Table(name, alias, this.SpanFrom start)

        // ------------------------------------------------------------------------------------------------------
        // Expressions
        // ------------------------------------------------------------------------------------------------------

        member this.Expression() : SqlExpr = this.Binary 0

        member this.Node(kind: SqlExprKind, start: int) : SqlExpr = { Kind = kind; Span = this.SpanFrom start }

        /// Whether the token `ahead` is a pattern-matching word: LIKE, and Spark's ILIKE, RLIKE and REGEXP.
        member this.AtPatternWord(ahead: int) =
            this.IsWord("LIKE", ahead)
            || (dialect.IsSpark && (this.IsWord("ILIKE", ahead) || this.IsWord("RLIKE", ahead) || this.IsWord("REGEXP", ahead)))

        /// Whether the next tokens are NOT followed by IN, BETWEEN or a pattern-matching word.
        member this.AtNegatedPredicate() =
            this.IsWord "NOT" && (this.IsWord("IN", 1) || this.IsWord("BETWEEN", 1) || this.AtPatternWord 1)

        /// Whether the next token is a comparison-level operator.
        member this.AtComparison() =
            let t = this.Peek
            (t.Kind = SqlTokenKind.Symbol
             && (t.Text = "=" || t.Text = "<>" || t.Text = "!=" || t.Text = "<" || t.Text = "<=" || t.Text = ">" || t.Text = ">="
                 || t.Text = "==" || t.Text = "<=>"))
            || this.IsWord "IS" || this.IsWord "IN" || this.IsWord "BETWEEN" || this.AtPatternWord 0 || this.AtNegatedPredicate()

        member this.Binary(minPrecedence: int) : SqlExpr =
            let start = this.Peek.Span.Start
            this.Enter this.Peek.Span
            let mutable left = this.Prefix()
            let mutable go = true
            while go do
                if minPrecedence <= 1 && this.IsWord "OR" then
                    this.Advance() |> ignore
                    let right = this.Binary 2
                    left <- this.Node(SqlExprKind.Binary(SqlBinaryOp.Or, left, right), start)
                elif minPrecedence <= 2 && this.IsWord "AND" then
                    this.Advance() |> ignore
                    let right = this.Binary 3
                    left <- this.Node(SqlExprKind.Binary(SqlBinaryOp.And, left, right), start)
                elif minPrecedence <= 4 && this.AtComparison() then
                    left <- this.Comparison(left, start)
                    if this.AtComparison() then
                        this.Fail(
                            $"Comparisons do not chain: {this.Describe this.Peek} follows a comparison",
                            this.Peek.Span,
                            "Add parentheses to say which comparison comes first.")
                elif minPrecedence <= 5 && this.IsSymbol "||" then
                    this.Advance() |> ignore
                    let right = this.Binary 6
                    left <- this.Node(SqlExprKind.Binary(SqlBinaryOp.Concat, left, right), start)
                elif minPrecedence <= 6 && (this.IsSymbol "+" || this.IsSymbol "-") then
                    let op = if this.Advance().Text = "+" then SqlBinaryOp.Add else SqlBinaryOp.Subtract
                    let right = this.Binary 7
                    left <- this.Node(SqlExprKind.Binary(op, left, right), start)
                elif minPrecedence <= 7 && (this.IsSymbol "*" || this.IsSymbol "/" || this.IsSymbol "%" || (dialect.IsSpark && this.IsWord "DIV")) then
                    let op =
                        match this.Advance().Text with
                        | "*" -> SqlBinaryOp.Multiply
                        | "/" -> SqlBinaryOp.Divide
                        | "%" -> SqlBinaryOp.Modulo
                        | _ -> SqlBinaryOp.IntegerDivide
                    let right = this.Binary 8
                    left <- this.Node(SqlExprKind.Binary(op, left, right), start)
                else go <- false
            this.Leave()
            left

        /// A comparison-level operator after `left`; operands bind tighter than comparisons.
        member this.Comparison(left: SqlExpr, start: int) : SqlExpr =
            let operand () = this.Binary 5
            if this.IsWord "IS" then
                this.Advance() |> ignore
                let negated = this.AcceptWord "NOT"
                if this.AcceptWord "NULL" || this.AcceptWord "UNKNOWN" then this.Node(SqlExprKind.IsNull(left, negated), start)
                elif this.AcceptWord "DISTINCT" then
                    this.ExpectWord("FROM", "in IS DISTINCT FROM")
                    let right = operand ()
                    this.Node(SqlExprKind.IsDistinctFrom(left, right, negated), start)
                elif this.IsWord "TRUE" || this.IsWord "FALSE" then
                    // `x IS TRUE` is `x IS NOT DISTINCT FROM TRUE`: two-valued, false for null.
                    let truth = this.Peek
                    this.Advance() |> ignore
                    let value = this.Node(SqlExprKind.Literal(SqlLiteral.Boolean(truth.Text.Equals("TRUE", StringComparison.OrdinalIgnoreCase))), truth.Span.Start)
                    this.Node(SqlExprKind.IsDistinctFrom(left, value, not negated), start)
                else this.Expected("NULL, TRUE, FALSE, UNKNOWN or DISTINCT FROM", if negated then "after IS NOT" else "after IS")
            else
                let negated = this.AcceptWord "NOT"
                if this.AcceptWord "IN" then
                    if this.IsSymbol "(" && this.StartsQuery 1 then
                        let query = this.Parenthesized(fun () -> this.Query())
                        this.Node(SqlExprKind.InSubquery(left, query, negated), start)
                    elif this.IsSymbol "(" then
                        let items = this.Parenthesized(fun () -> this.CommaList(fun () -> this.Expression()))
                        this.Node(SqlExprKind.InList(left, items, negated), start)
                    else this.Expected("'(' with a list or a query", "after IN")
                elif this.AcceptWord "BETWEEN" then
                    let low = operand ()
                    this.ExpectWord("AND", "between the bounds of BETWEEN")
                    let high = operand ()
                    this.Node(SqlExprKind.Between(left, low, high, negated), start)
                elif this.IsWord "LIKE" || this.IsWord "ILIKE" then
                    let insensitive = this.Advance().Text.Length = 5
                    let pattern = operand ()
                    let escape = if this.AcceptWord "ESCAPE" then Some(operand ()) else None
                    if insensitive then this.Node(SqlExprKind.ILike(left, pattern, escape, negated), start)
                    else this.Node(SqlExprKind.Like(left, pattern, escape, negated), start)
                elif this.AcceptWord "RLIKE" || this.AcceptWord "REGEXP" then
                    let pattern = operand ()
                    this.Node(SqlExprKind.RLike(left, pattern, negated), start)
                elif this.AcceptSymbol "<=>" then
                    let right = operand ()
                    this.Node(SqlExprKind.IsDistinctFrom(left, right, true), start)
                else
                    let op =
                        match this.Advance().Text with
                        | "="
                        | "==" -> SqlBinaryOp.Eq
                        | "<>"
                        | "!=" -> SqlBinaryOp.NotEq
                        | "<" -> SqlBinaryOp.Lt
                        | "<=" -> SqlBinaryOp.LtEq
                        | ">" -> SqlBinaryOp.Gt
                        | _ -> SqlBinaryOp.GtEq
                    if (this.IsWord "ANY" || this.IsWord "SOME" || this.IsWord "ALL") && this.IsSymbol("(", 1) && this.StartsQuery 2 then
                        let all = this.Advance().Text.Equals("ALL", StringComparison.OrdinalIgnoreCase)
                        let query = this.Parenthesized(fun () -> this.Query())
                        this.Node(SqlExprKind.Quantified(left, op, all, query), start)
                    else
                        let right = operand ()
                        this.Node(SqlExprKind.Binary(op, left, right), start)

        member this.Prefix() : SqlExpr =
            let start = this.Peek.Span.Start
            if this.IsWord "NOT" || this.IsSymbol "!" then
                this.Advance() |> ignore
                let operand = this.Binary 4
                this.Node(SqlExprKind.Unary(SqlUnaryOp.Not, operand), start)
            elif this.IsSymbol "-" || this.IsSymbol "+" then
                let op = if this.Advance().Text = "-" then SqlUnaryOp.Negate else SqlUnaryOp.Plus
                let operand = this.Binary 8
                this.Node(SqlExprKind.Unary(op, operand), start)
            else this.Primary()

        member this.Primary() : SqlExpr =
            let t = this.Peek
            let start = t.Span.Start
            let literal value =
                this.Advance() |> ignore
                this.Node(SqlExprKind.Literal value, start)
            match t.Kind with
            | SqlTokenKind.Integer -> literal (SqlLiteral.Integer t.Text)
            | SqlTokenKind.Decimal -> literal (SqlLiteral.Decimal t.Text)
            | SqlTokenKind.Float -> literal (SqlLiteral.Float t.Text)
            | SqlTokenKind.SuffixedNumber ->
                let text = t.Text
                let suffixLength = if text.EndsWith("BD", StringComparison.OrdinalIgnoreCase) then 2 else 1
                literal (SqlLiteral.Suffixed(text.Substring(0, text.Length - suffixLength), text.Substring(text.Length - suffixLength).ToUpperInvariant()))
            // Spark runs adjacent strings together: 'a' 'b' is 'ab'.
            | SqlTokenKind.String when dialect.IsSpark && this.PeekAt(1).Kind = SqlTokenKind.String ->
                let builder = Text.StringBuilder()
                while this.Peek.Kind = SqlTokenKind.String do
                    builder.Append(this.Advance().Text) |> ignore
                this.Node(SqlExprKind.Literal(SqlLiteral.String(builder.ToString())), start)
            | SqlTokenKind.String -> literal (SqlLiteral.String t.Text)
            | SqlTokenKind.Symbol when t.Text = "(" ->
                if this.StartsQuery 1 then
                    let query = this.Parenthesized(fun () -> this.Query())
                    this.Node(SqlExprKind.ScalarSubquery query, start)
                else
                    let inner =
                        this.Parenthesized(fun () ->
                            let inner = this.Expression()
                            if this.IsSymbol "," then this.Fail("Row values are not supported", this.Peek.Span)
                            inner)
                    // The parentheses are kept in the span only.
                    { inner with Span = this.SpanFrom start }
            | SqlTokenKind.Word when this.IsWord "NULL" -> literal SqlLiteral.Null
            | SqlTokenKind.Word when this.IsWord "TRUE" -> literal (SqlLiteral.Boolean true)
            | SqlTokenKind.Word when this.IsWord "FALSE" -> literal (SqlLiteral.Boolean false)
            | SqlTokenKind.Word when (this.IsWord "DATE" || this.IsWord "TIME" || this.IsWord "TIMESTAMP"
                                      || (dialect.IsSpark && (this.IsWord "TIMESTAMP_NTZ" || this.IsWord "TIMESTAMP_LTZ")))
                                     && this.PeekAt(1).Kind = SqlTokenKind.String ->
                let typeName = this.Advance().Text.ToUpperInvariant()
                let value = this.Advance().Text
                this.Node(SqlExprKind.Literal(SqlLiteral.Typed(typeName, value)), start)
            | SqlTokenKind.Word when this.IsWord "INTERVAL" && (this.PeekAt(1).Kind = SqlTokenKind.String || this.PeekAt(1).Kind = SqlTokenKind.Integer) ->
                // `INTERVAL '5' DAY`, or as DuckDB writes it, `INTERVAL 5 DAY`.
                this.Advance() |> ignore
                let value = this.Advance().Text
                if this.Peek.Kind <> SqlTokenKind.Word then this.Expected("a unit (DAY, MONTH, ...)", "after the INTERVAL value")
                let unit = this.Advance().Text.ToUpperInvariant()
                this.Node(SqlExprKind.Literal(SqlLiteral.Interval(value, unit)), start)
            | SqlTokenKind.Word when this.IsWord "CASE" -> this.Case()
            | SqlTokenKind.Word when (this.IsWord "CAST" || this.IsWord "TRY_CAST") && this.IsSymbol("(", 1) ->
                let isTry = this.Advance().Text.Length > 4
                let expr, typeName =
                    this.Parenthesized(fun () ->
                        let expr = this.Expression()
                        this.ExpectWord("AS", "in CAST(value AS type)")
                        expr, this.TypeName())
                this.Node(SqlExprKind.Cast(expr, typeName, isTry), start)
            | SqlTokenKind.Word when this.IsWord "EXISTS" ->
                this.Advance() |> ignore
                if not (this.IsSymbol "(" && this.StartsQuery 1) then this.Expected("'(' and a query", "after EXISTS")
                let query = this.Parenthesized(fun () -> this.Query())
                this.Node(SqlExprKind.Exists query, start)
            // LEFT and RIGHT are joins' words, and functions before a parenthesis.
            | SqlTokenKind.Word when (this.IsWord "LEFT" || this.IsWord "RIGHT") && this.IsSymbol("(", 1) ->
                let word = this.Advance()
                this.FunctionCall([ { Text = word.Text; Quoted = false; Span = word.Span } ], start)
            // SUBSTRING(x FROM start [FOR length]) as well as SUBSTRING(x, start[, length]).
            | SqlTokenKind.Word when this.IsWord "SUBSTRING" && this.IsSymbol("(", 1) ->
                let word = this.Advance()
                let arguments =
                    this.Parenthesized(fun () ->
                        let subject = this.Expression()
                        if this.AcceptWord "FROM" then
                            let from = this.Expression()
                            if this.AcceptWord "FOR" then [ subject; from; this.Expression() ] else [ subject; from ]
                        elif this.IsSymbol "," then
                            this.Advance() |> ignore
                            subject :: this.CommaList(fun () -> this.Expression())
                        else [ subject ])
                let name = [ { Text = word.Text; Quoted = false; Span = word.Span } ]
                this.Node(SqlExprKind.Function { Name = name; Distinct = false; Star = false; Arguments = arguments; IgnoreNulls = false; Over = None }, start)
            | SqlTokenKind.Word when this.IsWord "EXTRACT" && this.IsSymbol("(", 1) ->
                this.Advance() |> ignore
                let field, expr =
                    this.Parenthesized(fun () ->
                        if this.Peek.Kind <> SqlTokenKind.Word then this.Expected("a field (YEAR, MONTH, ...)", "in EXTRACT")
                        let field = this.Advance().Text.ToUpperInvariant()
                        this.ExpectWord("FROM", "in EXTRACT(field FROM value)")
                        field, this.Expression())
                this.Node(SqlExprKind.Extract(field, expr), start)
            | SqlTokenKind.Word
            | SqlTokenKind.QuotedIdentifier when this.AtName() ->
                let name = this.Name "in an expression"
                if this.IsSymbol "(" then this.FunctionCall(name, start)
                else this.Node(SqlExprKind.Column name, start)
            | _ -> this.Expected("an expression", "")

        /// `IGNORE NULLS` (Some true) or `RESPECT NULLS` (Some false) when it comes next.
        member this.NullTreatment() : bool option =
            if (this.IsWord "IGNORE" || this.IsWord "RESPECT") && this.IsWord("NULLS", 1) then
                let ignoreNulls = this.Advance().Text.Equals("IGNORE", StringComparison.OrdinalIgnoreCase)
                this.Advance() |> ignore
                Some ignoreNulls
            else None

        member this.FunctionCall(name: SqlName, start: int) : SqlExpr =
            let distinct, star, arguments, inside =
                this.Parenthesized(fun () ->
                    if this.IsSymbol "*" then
                        this.Advance() |> ignore
                        false, true, [], None
                    elif this.IsSymbol ")" then false, false, [], None
                    else
                        let distinct =
                            if this.AcceptWord "DISTINCT" then true
                            else
                                this.AcceptWord "ALL" |> ignore
                                false
                        let arguments = this.CommaList(fun () -> this.Expression())
                        distinct, false, arguments, this.NullTreatment())
            // Both spellings: inside the parentheses (DuckDB, BigQuery) and after them (ISO, Oracle, SQL Server, Spark).
            let treatmentStart = this.Peek.Span
            let ignoreNulls =
                match inside, this.NullTreatment() with
                | Some _, Some _ -> this.Fail("IGNORE NULLS or RESPECT NULLS is written once", treatmentStart, "Keep the one inside the parentheses or the one after them.")
                | Some i, None
                | None, Some i -> i
                | None, None -> false
            if this.IsWord "FILTER" && this.IsSymbol("(", 1) then
                this.Fail("FILTER clauses are not supported", this.Peek.Span, "Write the condition in a CASE inside the aggregate.")
            let over =
                if this.AcceptWord "OVER" then
                    if not (this.IsSymbol "(") then
                        this.Fail("Named windows are not supported", this.Peek.Span, "Write the window in parentheses after OVER.")
                    Some(this.Window())
                else None
            this.Node(SqlExprKind.Function { Name = name; Distinct = distinct; Star = star; Arguments = arguments; IgnoreNulls = ignoreNulls; Over = over }, start)

        member this.Window() : SqlWindow =
            let start = this.Peek.Span.Start
            let partitionBy, orderBy, frame =
                this.Parenthesized(fun () ->
                    let partitionBy =
                        if this.AcceptWord "PARTITION" then
                            this.ExpectWord("BY", "after PARTITION")
                            this.CommaList(fun () -> this.Expression())
                        else []
                    let orderBy =
                        if this.AcceptWord "ORDER" then
                            this.ExpectWord("BY", "after ORDER")
                            this.CommaList(fun () -> this.OrderItem())
                        else []
                    let frame =
                        if this.IsWord "ROWS" || this.IsWord "RANGE" || this.IsWord "GROUPS" then Some(this.Frame()) else None
                    partitionBy, orderBy, frame)
            { PartitionBy = partitionBy; OrderBy = orderBy; Frame = frame; Span = this.SpanFrom start }

        member this.Frame() : SqlFrame =
            let start = this.Peek.Span.Start
            let unit =
                match this.Advance().Text.ToUpperInvariant() with
                | "ROWS" -> SqlFrameUnit.Rows
                | "RANGE" -> SqlFrameUnit.Range
                | _ -> SqlFrameUnit.Groups
            let bound () =
                if this.AcceptWord "UNBOUNDED" then
                    if this.AcceptWord "PRECEDING" then SqlFrameBound.UnboundedPreceding
                    elif this.AcceptWord "FOLLOWING" then SqlFrameBound.UnboundedFollowing
                    else this.Expected("PRECEDING or FOLLOWING", "after UNBOUNDED")
                elif this.AcceptWord "CURRENT" then
                    this.ExpectWord("ROW", "after CURRENT")
                    SqlFrameBound.CurrentRow
                else
                    let offset = this.Binary 5
                    if this.AcceptWord "PRECEDING" then SqlFrameBound.Preceding offset
                    elif this.AcceptWord "FOLLOWING" then SqlFrameBound.Following offset
                    else this.Expected("PRECEDING or FOLLOWING", "after a frame offset")
            if this.AcceptWord "BETWEEN" then
                let first = bound ()
                this.ExpectWord("AND", "between the bounds of the frame")
                let last = bound ()
                { Unit = unit; Start = first; End = Some last; Span = this.SpanFrom start }
            else
                let first = bound ()
                { Unit = unit; Start = first; End = None; Span = this.SpanFrom start }

        member this.OrderItem() : SqlOrderItem =
            let start = this.Peek.Span.Start
            let expr = this.Expression()
            let direction =
                if this.AcceptWord "ASC" then SqlSortDirection.Ascending
                elif this.AcceptWord "DESC" then SqlSortDirection.Descending
                else SqlSortDirection.Default
            let nulls =
                if this.AcceptWord "NULLS" then
                    if this.AcceptWord "FIRST" then SqlNullOrder.First
                    elif this.AcceptWord "LAST" then SqlNullOrder.Last
                    else this.Expected("FIRST or LAST", "after NULLS")
                else SqlNullOrder.Default
            { Expr = expr; Direction = direction; Nulls = nulls; Span = this.SpanFrom start }

        member this.Case() : SqlExpr =
            let start = this.Peek.Span.Start
            this.ExpectWord("CASE", "")
            let operand = if this.IsWord "WHEN" then None else Some(this.Expression())
            let whens = ResizeArray()
            while this.AcceptWord "WHEN" do
                let test = this.Expression()
                this.ExpectWord("THEN", "after the condition of WHEN")
                whens.Add(test, this.Expression())
            if whens.Count = 0 then this.Expected("WHEN", "in CASE")
            let otherwise = if this.AcceptWord "ELSE" then Some(this.Expression()) else None
            this.ExpectWord("END", "to close CASE")
            this.Node(SqlExprKind.Case(operand, List.ofSeq whens, otherwise), start)

        /// A type in CAST: words (`DOUBLE PRECISION`, `TIMESTAMP WITH TIME ZONE`) and integer parameters.
        member this.TypeName() : SqlTypeName =
            let start = this.Peek.Span.Start
            if this.Peek.Kind <> SqlTokenKind.Word then this.Expected("a type", "after AS")
            let words = ResizeArray [ this.Advance().Text.ToUpperInvariant() ]
            let continuation = [ "PRECISION"; "VARYING"; "WITH"; "WITHOUT"; "TIME"; "ZONE" ]
            while continuation |> List.exists (fun w -> this.IsWord w) do
                words.Add(this.Advance().Text.ToUpperInvariant())
            let parameters =
                if this.IsSymbol "(" then
                    this.Parenthesized(fun () ->
                        this.CommaList(fun () ->
                            let t = this.Peek
                            if t.Kind <> SqlTokenKind.Integer then this.Expected("an integer", "as a type parameter")
                            this.Advance() |> ignore
                            match Int32.TryParse t.Text with
                            | true, v -> v
                            | _ -> this.Fail($"The type parameter {t.Text} is too large", t.Span)))
                else []
            { Name = String.Join(" ", words); Parameters = parameters; Span = this.SpanFrom start }

        // ------------------------------------------------------------------------------------------------------
        // Statements
        // ------------------------------------------------------------------------------------------------------

        member this.Finish(what: string) =
            this.AcceptSymbol ";" |> ignore
            if this.Peek.Kind <> SqlTokenKind.End then this.Expected("the end of the text", $"after the {what}")

    let private parser dialect (text: string) =
        if isNull text then nullArg "text"
        Parser(dialect, text, SqlLexer.tokenize dialect text)

    /// Reads a query (one SELECT statement, possibly with WITH, set operations, ORDER BY and LIMIT). Fails with
    /// SqlException where the text is not one.
    let parseQuery (dialect: SqlDialect) (text: string) : SqlQuery =
        let p = parser dialect text
        let query = p.Query()
        p.Finish "query"
        query

    /// Reads one scalar expression (a Delta CHECK constraint, a generated column's expression).
    let parseExpression (dialect: SqlDialect) (text: string) : SqlExpr =
        let p = parser dialect text
        let expr = p.Expression()
        p.Finish "expression"
        expr
