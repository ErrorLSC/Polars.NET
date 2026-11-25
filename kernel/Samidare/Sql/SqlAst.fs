namespace Kaminari.Samidare.Sql

/// The SQL a text is read as. `Standard` is ISO SQL with the documented extensions; `SparkExpression` reads the
/// scalar expressions Delta stores in its log (CHECK constraints, generated columns, defaults), which may quote
/// identifiers with backticks.
[<RequireQualifiedAccess>]
type internal SqlDialect =
    | Standard
    | SparkExpression

    member this.BacktickIdentifiers =
        match this with
        | Standard -> false
        | SparkExpression -> true

/// A name as written: its text without quotes, and whether it was quoted (a quoted name matches exactly, an unquoted
/// one ignores case).
type SqlIdentifier = { Text: string; Quoted: bool; Span: SqlSpan }

/// A possibly qualified name (`t.a`, `s.t`), its parts in order.
type internal SqlName = SqlIdentifier list

/// A literal as written. Numbers keep their text: `1.000` is a decimal of scale 3, never a double.
[<RequireQualifiedAccess>]
type internal SqlLiteral =
    /// Digits only: `42`.
    | Integer of text: string
    /// Digits with a point and no exponent: `1.000`, `.5`.
    | Decimal of text: string
    /// A number with an exponent: `1e3`, `2.5E-2`.
    | Float of text: string
    /// The string's value (its doubled quotes made single).
    | String of value: string
    | Boolean of bool
    | Null
    /// A typed literal: `DATE '2000-01-01'`, `TIMESTAMP '...'`, `TIME '...'` (the type's name upper case).
    | Typed of typeName: string * value: string
    /// `INTERVAL '5' DAY`: the quoted value and the unit (upper case).
    | Interval of value: string * unit: string

/// A type as written in CAST: its words upper case (`DOUBLE PRECISION`) and its parameters (`DECIMAL(7, 2)`).
type internal SqlTypeName = { Name: string; Parameters: int list; Span: SqlSpan }

[<RequireQualifiedAccess>]
type internal SqlUnaryOp =
    | Negate
    | Plus
    | Not

[<RequireQualifiedAccess>]
type internal SqlBinaryOp =
    | Or
    | And
    | Eq
    | NotEq
    | Lt
    | LtEq
    | Gt
    | GtEq
    | Add
    | Subtract
    | Multiply
    | Divide
    | Modulo
    | Concat

/// An ORDER BY direction: written or left to the default.
[<RequireQualifiedAccess>]
type internal SqlSortDirection =
    | Default
    | Ascending
    | Descending

/// A NULLS FIRST / LAST clause, or none (the default order).
[<RequireQualifiedAccess>]
type internal SqlNullOrder =
    | Default
    | First
    | Last

[<RequireQualifiedAccess>]
type internal SqlFrameUnit =
    | Rows
    | Range
    | Groups

type internal SqlExpr =
    { Kind: SqlExprKind
      Span: SqlSpan }

and [<RequireQualifiedAccess>] internal SqlExprKind =
    | Literal of SqlLiteral
    | Column of SqlName
    | Unary of SqlUnaryOp * SqlExpr
    | Binary of SqlBinaryOp * SqlExpr * SqlExpr
    /// `x IS [NOT] NULL`.
    | IsNull of SqlExpr * negated: bool
    /// `x IS [NOT] DISTINCT FROM y`.
    | IsDistinctFrom of SqlExpr * SqlExpr * negated: bool
    | Between of SqlExpr * low: SqlExpr * high: SqlExpr * negated: bool
    | InList of SqlExpr * SqlExpr list * negated: bool
    | InSubquery of SqlExpr * SqlQuery * negated: bool
    /// `x op ANY (query)`, `x op SOME (query)` (`all` false) or `x op ALL (query)`, op a comparison.
    | Quantified of SqlExpr * SqlBinaryOp * all: bool * SqlQuery
    | Like of SqlExpr * pattern: SqlExpr * escape: SqlExpr option * negated: bool
    /// `EXISTS (query)`; `NOT EXISTS` is `Unary Not` around it.
    | Exists of SqlQuery
    | ScalarSubquery of SqlQuery
    /// `CASE [operand] WHEN .. THEN .. [ELSE ..] END`.
    | Case of operand: SqlExpr option * whens: (SqlExpr * SqlExpr) list * otherwise: SqlExpr option
    /// `CAST(x AS t)`, or `TRY_CAST(x AS t)` (`isTry`: null where the value does not convert).
    | Cast of SqlExpr * SqlTypeName * isTry: bool
    /// `EXTRACT(field FROM x)`, the field upper case.
    | Extract of field: string * SqlExpr
    | Function of SqlFunctionCall

/// A function call: `f(a, b)`, `count(DISTINCT x)`, `count(*)`, with OVER for a window function.
and internal SqlFunctionCall =
    { Name: SqlName
      Distinct: bool
      /// `f(*)`: no arguments, every row.
      Star: bool
      Arguments: SqlExpr list
      /// `IGNORE NULLS`, written inside the parentheses (`lag(x IGNORE NULLS)`) or after them (`lag(x) IGNORE NULLS`);
      /// false for `RESPECT NULLS` and when not written.
      IgnoreNulls: bool
      Over: SqlWindow option }

and internal SqlWindow =
    { PartitionBy: SqlExpr list
      OrderBy: SqlOrderItem list
      Frame: SqlFrame option
      Span: SqlSpan }

and internal SqlFrame =
    { Unit: SqlFrameUnit
      Start: SqlFrameBound
      /// None for the one-bound form (`ROWS UNBOUNDED PRECEDING`).
      End: SqlFrameBound option
      Span: SqlSpan }

and [<RequireQualifiedAccess>] internal SqlFrameBound =
    | UnboundedPreceding
    | Preceding of SqlExpr
    | CurrentRow
    | Following of SqlExpr
    | UnboundedFollowing

and internal SqlOrderItem =
    { Expr: SqlExpr
      Direction: SqlSortDirection
      Nulls: SqlNullOrder
      Span: SqlSpan }

/// A query: its common table expressions, body, ORDER BY and row limits.
and internal SqlQuery =
    { With: SqlCommonTable list
      Recursive: bool
      Body: SqlQueryBody
      OrderBy: SqlOrderItem list
      /// `LIMIT n` or `FETCH FIRST n ROWS ONLY`.
      Limit: SqlExpr option
      Offset: SqlExpr option
      Span: SqlSpan }

and internal SqlCommonTable =
    { Name: SqlIdentifier
      Columns: SqlIdentifier list
      Query: SqlQuery
      Span: SqlSpan }

and [<RequireQualifiedAccess>] internal SqlSetOp =
    | Union
    | Intersect
    | Except

and [<RequireQualifiedAccess>] internal SqlQueryBody =
    | Select of SqlSelect
    /// `left op [ALL] right`: `all` keeps duplicates (UNION ALL), otherwise DISTINCT, written or not.
    | SetOperation of op: SqlSetOp * all: bool * left: SqlQueryBody * right: SqlQueryBody * span: SqlSpan
    /// A parenthesized query, with its own WITH, ORDER BY and limits.
    | Nested of SqlQuery
    /// `VALUES (a, b), (c, d)`: rows of expressions, as many in each.
    | Values of rows: SqlExpr list list * span: SqlSpan

and internal SqlSelect =
    { Distinct: bool
      Items: SqlSelectItem list
      /// The FROM list: comma-separated items, each a table, a derived table or joins.
      From: SqlTableRef list
      Where: SqlExpr option
      GroupBy: SqlGroupingItem list
      Having: SqlExpr option
      Span: SqlSpan }

and [<RequireQualifiedAccess>] internal SqlSelectItem =
    | Expr of SqlExpr * alias: SqlIdentifier option
    /// `*` (no qualifier) or `t.*`.
    | Wildcard of qualifier: SqlName option * span: SqlSpan

/// A GROUP BY element.
and [<RequireQualifiedAccess>] internal SqlGroupingItem =
    | Expr of SqlExpr
    /// A parenthesized list of expressions, possibly empty: `(a, b)`, `()`.
    | Set of SqlExpr list * span: SqlSpan
    | Rollup of SqlGroupingItem list * span: SqlSpan
    | Cube of SqlGroupingItem list * span: SqlSpan
    | GroupingSets of SqlGroupingItem list * span: SqlSpan

and internal SqlTableAlias =
    { Name: SqlIdentifier
      Columns: SqlIdentifier list }

and [<RequireQualifiedAccess>] internal SqlJoinKind =
    | Inner
    | Left
    | Right
    | Full
    | Cross

and [<RequireQualifiedAccess>] internal SqlJoinCondition =
    | On of SqlExpr
    | Using of SqlIdentifier list
    /// CROSS JOIN.
    | None

and [<RequireQualifiedAccess>] internal SqlTableRef =
    | Table of name: SqlName * alias: SqlTableAlias option * span: SqlSpan
    /// `(query) [AS] alias`; the alias may be left out (an extension).
    | Derived of SqlQuery * alias: SqlTableAlias option * span: SqlSpan
    | Join of left: SqlTableRef * right: SqlTableRef * kind: SqlJoinKind * condition: SqlJoinCondition * span: SqlSpan

    member this.Span =
        match this with
        | Table(_, _, span)
        | Derived(_, _, span)
        | Join(_, _, _, _, span) -> span
