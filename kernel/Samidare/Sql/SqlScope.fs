namespace Kaminari.Samidare.Sql

open System
open Kaminari.Samidare.IR

/// Where a query's tables come from.
type ISqlCatalog =
    /// The tables a one-part name may refer to: one, none, or several when only case tells registered names apart
    /// (an unquoted part ignores case, a quoted one matches exactly).
    abstract FindTables: name: SqlIdentifier -> (string * ILogicalTableSource) list
    /// The names tables are registered under (for a suggestion when a name finds none).
    abstract TableNames: string list

/// Tables registered by name.
type SqlCatalog() =
    let tables = ResizeArray<string * ILogicalTableSource>()

    /// Registers a table under a name; a name registered again (exactly) is replaced.
    member _.Register(name: string, source: ILogicalTableSource) =
        if String.IsNullOrEmpty name then invalidArg (nameof name) "A table needs a name."
        match tables.FindIndex(fun (n, _) -> n = name) with
        | -1 -> tables.Add((name, source))
        | i -> tables.[i] <- (name, source)

    interface ISqlCatalog with
        member _.TableNames = tables |> Seq.map fst |> List.ofSeq

        member _.FindTables(name: SqlIdentifier) =
            [ for registered, source in tables do
                  if (if name.Quoted then registered = name.Text else String.Equals(registered, name.Text, StringComparison.OrdinalIgnoreCase)) then
                      registered, source ]

/// A column a name can reach: its name as defined, the IR column holding it, its type.
type internal SqlColumn =
    { Name: string
      Internal: string
      Type: LogicalType
      /// Whether an unqualified name reaches it (false for a USING key's second occurrence).
      Unqualified: bool }

/// A FROM item as names see it: its name (the alias, or the table's name; None for a derived table without an alias)
/// and its columns.
type internal SqlRelation =
    { Name: SqlIdentifier option
      Columns: SqlColumn list }

/// The relations a name is resolved in, and the scope of the query around (a subquery's correlation): with the IR scope
/// number the subquery's OuterColumns carry.
type internal SqlScope =
    { Relations: SqlRelation list
      Outer: (SqlScope * int) option }

    static member Empty = { Relations = []; Outer = None }

/// Where a column name led.
[<RequireQualifiedAccess>]
type internal SqlResolution =
    /// The column; `outer` = the subquery scopes it was reached through, innermost first (empty: this query's own).
    | Found of SqlColumn * outer: int list
    | Ambiguous of candidates: string list
    /// No such column: the names that were in reach (for a suggestion), or no such table qualifier.
    | NotFound of inReach: string list
    | NoRelation of qualifier: string

module internal SqlNames =

    /// Whether a name as written matches a defined name: exactly when quoted, ignoring case otherwise.
    let matches (written: SqlIdentifier) (defined: string) =
        if written.Quoted then written.Text = defined else String.Equals(written.Text, defined, StringComparison.OrdinalIgnoreCase)

    /// A relation's and column's name as messages show them.
    let qualified (relation: SqlRelation) (column: SqlColumn) =
        match relation.Name with
        | Some name -> $"{name.Text}.{column.Name}"
        | None -> column.Name

    let rec private resolveIn (scope: SqlScope) (name: SqlName) (outer: int list) : SqlResolution =
        let local =
            match name with
            | [ column ] ->
                let hits =
                    [ for relation in scope.Relations do
                          for c in relation.Columns do
                              if c.Unqualified && matches column c.Name then relation, c ]
                match hits with
                | [ _, c ] -> SqlResolution.Found(c, outer)
                | [] -> SqlResolution.NotFound [ for r in scope.Relations do for c in r.Columns do if c.Unqualified then c.Name ]
                | many -> SqlResolution.Ambiguous(many |> List.map (fun (r, c) -> qualified r c))
            | [ qualifier; column ] ->
                let relations = scope.Relations |> List.filter (fun r -> r.Name |> Option.exists (fun n -> matches qualifier n.Text))
                match relations with
                | [] -> SqlResolution.NoRelation qualifier.Text
                | [ relation ] ->
                    match relation.Columns |> List.filter (fun c -> matches column c.Name) with
                    | [ c ] -> SqlResolution.Found(c, outer)
                    | [] -> SqlResolution.NotFound(relation.Columns |> List.map (fun c -> c.Name))
                    | many -> SqlResolution.Ambiguous(many |> List.map (qualified relation))
                | many -> SqlResolution.Ambiguous(many |> List.map (fun r -> r.Name.Value.Text + "." + column.Text))
            | _ -> SqlResolution.NotFound []
        match local, scope.Outer with
        | (SqlResolution.NotFound _ | SqlResolution.NoRelation _), Some(parent, subquery) ->
            match resolveIn parent name (outer @ [ subquery ]) with
            | SqlResolution.Found _ as found -> found
            | SqlResolution.Ambiguous _ as ambiguous -> ambiguous
            | _ -> local
        | _ -> local

    /// Resolves a column name in a scope, then in the scopes around it.
    let resolve (scope: SqlScope) (name: SqlName) = resolveIn scope name []

    /// The edit distance of two names, ignoring case.
    let distance (a: string) (b: string) =
        let a, b = a.ToUpperInvariant(), b.ToUpperInvariant()
        let d = Array2D.zeroCreate (a.Length + 1) (b.Length + 1)
        for i in 0 .. a.Length do d.[i, 0] <- i
        for j in 0 .. b.Length do d.[0, j] <- j
        for i in 1 .. a.Length do
            for j in 1 .. b.Length do
                let cost = if a.[i - 1] = b.[j - 1] then 0 else 1
                d.[i, j] <- min (min (d.[i - 1, j] + 1) (d.[i, j - 1] + 1)) (d.[i - 1, j - 1] + cost)
        d.[a.Length, b.Length]

    /// The name in reach closest to a misspelt one, if one is close.
    let suggest (written: string) (inReach: string list) =
        let limit = max 1 (written.Length / 3)
        inReach
        |> List.distinct
        |> List.map (fun n -> n, distance written n)
        |> List.filter (fun (_, d) -> d <= limit)
        |> List.sortBy snd
        |> List.tryHead
        |> Option.map fst
