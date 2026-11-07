namespace Polars.NET.Linq.IR

/// Which entity of a row a scope designates.
[<RequireQualifiedAccess>]
type RowScope =
    /// An entity bound to a name: a range variable or a join selector's parameter (`a` in `select new { a, b }`),
    /// or the member a transparent selector or `let` binds it to.
    | Named of name: string
    /// The entity at a member path of a transparent row: ["t"; "x"] for row.t.x, ["Item1"] for row.Item1, [] for
    /// the row itself. Paths identify an entity exactly where member names repeat (F# rows are Item1..ItemN at
    /// every nesting level).
    | Path of path: string list
    /// The row's element itself, for a member no scope qualifies (a grouping element's own members).
    | Unqualified

/// Where the members of a row's entities live: (scope, member) -> physical column. It describes rows whose
/// entities are still flat columns after joins (transparent identifiers), so that `t.a.Id` resolves to the
/// column holding it (`Id`, or `Id_right` when the right side's name clashed). An empty shape means the row is
/// one entity over its own columns.
type RowShape =
    private
        { Columns: Map<RowScope * string, string> }

    static member Empty: RowShape = { Columns = Map.empty }

    static member OfEntries(entries: ((RowScope * string) * string) seq) : RowShape = { Columns = Map.ofSeq entries }

    member this.IsEmpty = this.Columns.IsEmpty

    /// Every (scope, member) -> column entry.
    member this.Entries: ((RowScope * string) * string) list = Map.toList this.Columns

    /// The column holding `memberName` of the entity `scope` designates.
    member this.TryResolve(scope: RowScope, memberName: string) : string option = this.Columns.TryFind((scope, memberName))

    member this.Add(scope: RowScope, memberName: string, column: string) : RowShape =
        { Columns = this.Columns.Add((scope, memberName), column) }

    /// Whether the shape holds members of `scope`.
    member this.HasScope(scope: RowScope) : bool = this.Columns |> Map.exists (fun (s, _) _ -> s = scope)

    /// The (member, column) pairs of `scope`, in member order.
    member this.Members(scope: RowScope) : (string * string) list =
        this.Columns |> Map.toList |> List.choose (fun ((s, m), column) -> if s = scope then Some(m, column) else None)

    /// The names bound as scopes.
    member this.ScopeNames: Set<string> =
        this.Columns
        |> Map.toSeq
        |> Seq.choose (fun ((s, _), _) ->
            match s with
            | RowScope.Named name -> Some name
            | _ -> None)
        |> Set.ofSeq

    /// Whether the row is a transparent row whose entities are designated by path.
    member this.HasPathScopes: bool =
        this.Columns
        |> Map.exists (fun (s, _) _ ->
            match s with
            | RowScope.Path _ -> true
            | _ -> false)

    /// The (path, member) -> column entries of the path scopes.
    member this.PathEntries: ((string list * string) * string) list =
        this.Columns
        |> Map.toList
        |> List.choose (fun ((s, m), column) ->
            match s with
            | RowScope.Path path -> Some((path, m), column)
            | _ -> None)

    /// The shape without its path scopes.
    member this.WithoutPathScopes: RowShape =
        { Columns =
            this.Columns
            |> Map.filter (fun (s, _) _ ->
                match s with
                | RowScope.Path _ -> false
                | _ -> true) }

    /// The shape where `target` also designates every member of `source`.
    member this.Alias(source: RowScope, target: RowScope) : RowShape =
        if source = target then
            this
        else
            this.Members source |> List.fold (fun (acc: RowShape) (m, column) -> acc.Add(target, m, column)) this

    /// The shape with every entry of `other` added (its entries win on a clash).
    member this.Union(other: RowShape) : RowShape =
        { Columns = other.Columns |> Map.fold (fun acc key column -> Map.add key column acc) this.Columns }

    override this.ToString() =
        this.Columns
        |> Map.toList
        |> List.map (fun ((s, m), c) -> sprintf "%A.%s=%s" s m c)
        |> String.concat ", "
