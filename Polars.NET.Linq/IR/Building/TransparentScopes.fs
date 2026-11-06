namespace Polars.NET.Linq.IR

open System
open System.Linq.Expressions
open Polars.NET.Linq

/// Scope rules for transparent identifiers: which join result selectors keep the joined entities as
/// flat columns, and how their members and the rows of `let` projections map to join scopes.
module internal TransparentScopes =

    /// Checks whether a two-parameter Join result selector merely packages parameters into a transparent identifier/tuple.
    let private isTransparentJoinResSelector (resSel: LambdaExpression) : bool =
        match tryExtractMemberBindings resSel.Body with
        | Some bindings when resSel.Parameters.Count >= 2 ->
            let p1, p2 = resSel.Parameters.[0].Name, resSel.Parameters.[1].Name

            let rec isTransparentArg (e: Expression) =
                match ExpressionPatterns.unwrap e with
                | :? ParameterExpression as p -> p.Name = p1 || p.Name = p2
                | MemberAccess(target, m) when isNavigationMember m -> isTransparentArg target
                | :? NewExpression as ne when isTupleOrAnonType ne.Type ->
                    ne.Arguments |> Seq.forall isTransparentArg
                | _ -> false

            bindings |> List.forall (fun (_, expr) -> isTransparentArg expr)
        | _ -> false

    /// Active pattern categorizing an optional Join/Unpivot result selector into transparent (or absent) vs concrete.
    let (|TransparentOrNoResSelector|ConcreteResSelector|) (resSelOpt: LambdaExpression option) =
        match resSelOpt with
        | Some resSel when not (isTransparentJoinResSelector resSel) -> ConcreteResSelector resSel
        | _ -> TransparentOrNoResSelector

    /// Registers the names a two-parameter result selector gives the joined sides as scopes: its parameters,
    /// which may differ from the key selectors', and the members a transparent selector binds them to
    /// (`new { E = x, D = y }`), so that `t.D.Name` resolves to the right side's column. `leftScope` is None
    /// when the left side is itself a transparent identifier rather than one entity.
    let aliasResultSelectorScopes
        (leftScope: string option)
        (rightScope: string)
        (resSelOpt: LambdaExpression option)
        (scopeMap: Map<string * string, string>)
        : Map<string * string, string> =
        let copyScope (fromScope: string) (toScope: string) (m: Map<string * string, string>) =
            if fromScope = toScope then
                m
            else
                m |> Map.fold (fun acc (s, c) phys -> if s = fromScope then Map.add (toScope, c) phys acc else acc) m

        match resSelOpt with
        | Some resSel when resSel.Parameters.Count >= 2 ->
            let pLeft, pRight = resSel.Parameters.[0].Name, resSel.Parameters.[1].Name
            let withParams =
                match leftScope with
                | Some l -> scopeMap |> copyScope l pLeft |> copyScope rightScope pRight
                | None -> scopeMap |> copyScope rightScope pRight

            match tryExtractMemberBindings resSel.Body with
            | Some bindings ->
                bindings
                |> List.fold (fun acc (memberName, expr) ->
                    match ExpressionPatterns.unwrap expr with
                    | :? ParameterExpression as p when p.Name = pRight -> copyScope pRight memberName acc
                    | :? ParameterExpression as p when p.Name = pLeft && leftScope.IsSome -> copyScope pLeft memberName acc
                    | _ -> acc) withParams
            | None -> withParams
        | _ -> scopeMap

    /// Scopes of a transparent `let` projection (`x => new { x, f = ... }`), which keeps the row's columns and
    /// appends the computed ones: a member bound to the row parameter becomes a scope over the row's columns,
    /// and a member bound to an existing scope under another name aliases it.
    let registerLetScopes
        (scopeMap: Map<string * string, string>)
        (plan: LogicalPlan)
        (lam: LambdaExpression)
        : Map<string * string, string> =
        match tryExtractMemberBindings lam.Body with
        | Some bindings when EntityPacking.isAnonymousType lam.ReturnType ->
            let rowParam = lam.Parameters.[0]
            let rowCols = (SchemaInference.inferPlanSchema plan).ColumnNames

            bindings
            |> List.fold (fun acc (memberName, expr) ->
                match ExpressionPatterns.unwrap expr with
                | :? ParameterExpression as p when p.Name = rowParam.Name && scopeMap.IsEmpty && not (isTupleOrAnonType p.Type) ->
                    rowCols |> List.fold (fun m c -> Map.add (memberName, c) c m) acc
                | e when not scopeMap.IsEmpty ->
                    match ColumnResolver.tryExtractScopeName e with
                    | Some scope when scope <> memberName ->
                        scopeMap |> Map.fold (fun m (s, c) phys -> if s = scope then Map.add (memberName, c) phys m else m) acc
                    | _ -> acc
                | _ -> acc) scopeMap
        | _ -> scopeMap

    /// The scope map of a join's result row. A result selector binding the sides to members (a transparent
    /// identifier) places each side's entity, or the left side's previous row, under its member's path;
    /// without a selector the row is the left side's and keeps its scopes. `updated` is the map after the
    /// join registered both sides (`leftScope` only when the left side was one entity, i.e. `preJoin` held
    /// no row path scopes).
    let joinResultScopes
        (preJoin: Map<string * string, string>)
        (updated: Map<string * string, string>)
        (leftScope: string)
        (rightScope: string)
        (resSelOpt: LambdaExpression option)
        : Map<string * string, string> =
        match resSelOpt with
        | Some resSel when resSel.Parameters.Count >= 2 ->
            let entries scope =
                updated |> Map.toList |> List.choose (fun ((s, m), phys) -> if s = scope then Some(m, phys) else None)
            let leftSide = if EntityPacking.hasRowPathScopes preJoin then RowSide.Row else RowSide.Entity(entries leftScope)
            let sides = [ resSel.Parameters.[0].Name, leftSide; resSel.Parameters.[1].Name, RowSide.Entity(entries rightScope) ]

            tryExtractMemberBindings resSel.Body
            |> Option.bind (EntityPacking.tryReshapeRow updated sides)
            |> Option.defaultWith (fun () -> updated |> Map.filter (fun (s, _) _ -> not (isRowPathScope s)))
        | _ -> updated

    /// Rows a client predicate cannot receive from the native plan: System tuples, which the materializer
    /// reads by position and suffix conventions. Anonymous types and F# AnonymousObject rows, transparent
    /// identifiers included, materialize once their scope entities are packed (EntityPacking.packRow).
    let isUnmaterializableClientRow (t: Type) =
        isTupleOrAnonType t && not (EntityPacking.isTransparentRowType t)
