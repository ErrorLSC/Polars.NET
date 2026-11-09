namespace Kaminari.Samidare.IR

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

/// The key of a row shape entry: (scope, member). Ordered as the (RowScope * string) tuple it replaces
/// (Named < Path < Unqualified; names ordinally, paths element by element with a prefix first; then the
/// member), but compared directly: generic structural comparison of the tuples dominated compiling a query
/// joining many tables, whose every join re-keys the entities it moves under a new member.
[<Sealed; AllowNullLiteral>]
type internal RowShapeKey(scope: RowScope, memberName: string) =
    member _.Scope = scope
    member _.Member = memberName

    static member private Tag(s: RowScope) =
        match s with
        | RowScope.Named _ -> 0
        | RowScope.Path _ -> 1
        | RowScope.Unqualified -> 2

    static member private ComparePaths(a: string list, b: string list) : int =
        match a, b with
        | [], [] -> 0
        | [], _ -> -1
        | _, [] -> 1
        | x :: xs, y :: ys ->
            let c = System.String.CompareOrdinal(x, y)
            if c <> 0 then c else RowShapeKey.ComparePaths(xs, ys)

    static member CompareScopes(a: RowScope, b: RowScope) : int =
        match a, b with
        | RowScope.Named x, RowScope.Named y -> System.String.CompareOrdinal(x, y)
        | RowScope.Path x, RowScope.Path y -> RowShapeKey.ComparePaths(x, y)
        | RowScope.Unqualified, RowScope.Unqualified -> 0
        | _ -> compare (RowShapeKey.Tag a) (RowShapeKey.Tag b)

    member this.CompareTo(other: RowShapeKey) : int =
        let c = RowShapeKey.CompareScopes(scope, other.Scope)
        if c <> 0 then c else System.String.CompareOrdinal(memberName, other.Member)

    interface System.IComparable<RowShapeKey> with
        member this.CompareTo(other: RowShapeKey) = this.CompareTo other

    interface System.IComparable with
        member this.CompareTo(other: obj) = this.CompareTo(other :?> RowShapeKey)

    override this.Equals(other: obj) =
        match other with
        | :? RowShapeKey as o -> this.CompareTo o = 0
        | _ -> false

    override _.GetHashCode() = System.HashCode.Combine(scope, memberName)

/// Where the members of a row's entities live: (scope, member) -> physical column. It describes rows whose
/// entities are still flat columns after joins (transparent identifiers), so that `t.a.Id` resolves to the
/// column holding it (`Id`, or `Id_right` when the right side's name clashed). An empty shape means the row is
/// one entity over its own columns.
type RowShape =
    private
        { Columns: Map<RowShapeKey, string> }

    static member Empty: RowShape = { Columns = Map.empty }

    static member OfEntries(entries: ((RowScope * string) * string) seq) : RowShape =
        { Columns = entries |> Seq.map (fun ((s, m), c) -> RowShapeKey(s, m), c) |> Map.ofSeq }

    member this.IsEmpty = this.Columns.IsEmpty

    /// Every (scope, member) -> column entry.
    member this.Entries: ((RowScope * string) * string) list =
        this.Columns |> Map.toList |> List.map (fun (k, c) -> (k.Scope, k.Member), c)

    /// The column holding `memberName` of the entity `scope` designates.
    member this.TryResolve(scope: RowScope, memberName: string) : string option = this.Columns.TryFind(RowShapeKey(scope, memberName))

    member this.Add(scope: RowScope, memberName: string, column: string) : RowShape =
        { Columns = this.Columns.Add(RowShapeKey(scope, memberName), column) }

    /// Whether the shape holds members of `scope`.
    member this.HasScope(scope: RowScope) : bool = this.Columns |> Map.exists (fun k _ -> k.Scope = scope)

    /// The (member, column) pairs of `scope`, in member order.
    member this.Members(scope: RowScope) : (string * string) list =
        this.Columns |> Map.toList |> List.choose (fun (k, column) -> if k.Scope = scope then Some(k.Member, column) else None)

    /// The names bound as scopes.
    member this.ScopeNames: Set<string> =
        this.Columns
        |> Map.toSeq
        |> Seq.choose (fun (k, _) ->
            match k.Scope with
            | RowScope.Named name -> Some name
            | _ -> None)
        |> Set.ofSeq

    /// Whether the row is a transparent row whose entities are designated by path.
    member this.HasPathScopes: bool =
        this.Columns
        |> Map.exists (fun k _ ->
            match k.Scope with
            | RowScope.Path _ -> true
            | _ -> false)

    /// The (path, member) -> column entries of the path scopes.
    member this.PathEntries: ((string list * string) * string) list =
        this.Columns
        |> Map.toList
        |> List.choose (fun (k, column) ->
            match k.Scope with
            | RowScope.Path path -> Some((path, k.Member), column)
            | _ -> None)

    /// The shape without its path scopes.
    member this.WithoutPathScopes: RowShape =
        { Columns =
            this.Columns
            |> Map.filter (fun k _ ->
                match k.Scope with
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
        |> List.map (fun (k, c) -> sprintf "%A.%s=%s" k.Scope k.Member c)
        |> String.concat ", "
