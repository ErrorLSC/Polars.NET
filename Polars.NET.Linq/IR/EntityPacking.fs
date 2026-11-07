namespace Polars.NET.Linq.IR

open System
open System.Collections
open System.Linq.Expressions
open System.Runtime.CompilerServices
open Polars.NET.Core.Helpers
open Polars.NET.Linq

/// How the rows of a plan must be reshaped before they are materialized as a given element type.
[<RequireQualifiedAccess>]
type internal RowPacking =
    /// The plan's columns are the element's members.
    | NotNeeded
    /// The plan with the element's entity members packed into struct columns.
    | Packed of LogicalPlan
    /// An entity member has no columns in the plan: the rows cannot be materialized natively.
    | Unresolvable

/// How a parameter of a projection or join result selector stands for the current row's columns.
[<RequireQualifiedAccess>]
type internal RowSide =
    /// The previous transparent row, whose entities are the path scopes of the row shape.
    | Row
    /// One entity, as (member, physical column) pairs.
    | Entity of (string * string) list

/// Materializes whole entities held in flat columns as struct columns.
///
/// A transparent join (`(x, y) => new { x, y }`) keeps both entities as flat columns and records, in the
/// row shape (RowShape), which physical column holds each (scope, member). Reading a whole entity, either as a member of
/// a projection or as a member of the materialized element, needs its columns packed into one struct column
/// named after the member; the row materializer turns that struct back into the entity. The same applies to
/// the row parameter of a single-table projection (`x => new { x, x.Name }`).
module internal EntityPacking =

    /// Physical columns of `plan` that an outer join fills with nulls for unmatched rows.
    /// Conservative: below an operator it does not follow, no column is reported.
    let rec nullExtendedColumns (plan: LogicalPlan) : Set<string> =
        let joined (left: LogicalPlan) (right: LogicalPlan) (suffix: string) =
            let leftNames = (SchemaInference.inferPlanSchema left).ColumnNames |> Set.ofList
            let rename (c: string) = if leftNames.Contains c then c + suffix else c
            let rightNames = (SchemaInference.inferPlanSchema right).ColumnNames |> List.map rename |> Set.ofList
            leftNames, rightNames, nullExtendedColumns left, nullExtendedColumns right |> Set.map rename

        match plan with
        | LogicalPlan.Join(left, right, _, _, how, suffixOpt, _, _) ->
            let leftNames, rightNames, leftExt, rightExt =
                joined left right (defaultArg suffixOpt LinqConstants.DefaultRightSuffix)

            match how with
            | JoinKind.Semi
            | JoinKind.Anti -> leftExt
            | JoinKind.Left -> Set.union leftExt rightNames
            | JoinKind.Full -> Set.union leftNames rightNames
            | JoinKind.Inner
            | JoinKind.Cross -> Set.union leftExt rightExt
        | LogicalPlan.JoinAsOf(left, right, spec) ->
            let _, rightNames, leftExt, _ =
                joined left right (defaultArg spec.Suffix LinqConstants.DefaultRightSuffix)
            Set.union leftExt rightNames
        | LogicalPlan.Filter(input, _)
        | LogicalPlan.Sort(input, _)
        | LogicalPlan.Limit(input, _, _)
        | LogicalPlan.Distinct(input, _, _, _)
        | LogicalPlan.Reverse input
        | LogicalPlan.SkipLast(input, _)
        | LogicalPlan.WithRowIndex(input, _, _) -> nullExtendedColumns input
        | LogicalPlan.WithColumns(input, cols) ->
            Set.difference (nullExtendedColumns input) (cols |> List.map fst |> Set.ofList)
        | LogicalPlan.Project(input, projs) ->
            let inner = nullExtendedColumns input
            projs
            |> List.choose (fun (name, e) ->
                match e with
                | ScalarExpr.Column c when inner.Contains c -> Some name
                | _ -> None)
            |> Set.ofList
        | LogicalPlan.Rename(input, existing, newNames) ->
            let renames = List.zip existing newNames |> Map.ofList
            nullExtendedColumns input |> Set.map (fun c -> renames.TryFind c |> Option.defaultValue c)
        | _ -> Set.empty

    /// A struct of the given (field, physical column) pairs. When every column comes from the null-extended
    /// side of an outer join, an unmatched row has no entity there, so the struct is null rather than an
    /// instance whose members are all null.
    let private packColumns (nullExtended: Lazy<Set<string>>) (fields: (string * string) list) : ScalarExpr =
        let structExpr =
            ScalarExpr.Function(
                ScalarFunction.AsStruct,
                fields |> List.map (fun (name, phys) -> ScalarExpr.Alias(ScalarExpr.Column phys, name)),
                LogicalType.Unknown
            )

        let physCols = fields |> List.map snd

        if physCols |> List.forall nullExtended.Value.Contains then
            let allNull =
                physCols
                |> List.map (fun c -> ScalarExpr.Unary(UnaryOp.IsNull, ScalarExpr.Column c))
                |> List.reduce (fun a b -> ScalarExpr.Binary(BinaryOp.And, a, b))
            ScalarExpr.Conditional(allNull, ScalarExpr.Literal(LiteralValue.Null LogicalTypes.Null), structExpr)
        else
            structExpr

    let private isCollectionType (t: Type) =
        t <> typeof<string> && typeof<IEnumerable>.IsAssignableFrom t

    /// A type materialized from several columns (or one struct column): not a scalar, collection or tuple.
    let private isEntityType (t: Type) =
        not (PolarsTypeHelper.IsScalarType t) && not (isCollectionType t) && not (isTupleOrAnonType t)

    /// A C# anonymous type (including query-syntax transparent identifiers) or an F# anonymous record.
    let isAnonymousType (t: Type) =
        not (isNull t.Name)
        && t.Name.Contains "AnonymousType"
        && t.IsDefined(typeof<CompilerGeneratedAttribute>, false)

    /// The row type F# queries use for tuples, anonymous records and `let`: members Item1..ItemN, which the
    /// materializer reads by position.
    let private isFSharpAnonymousObject (t: Type) =
        not (isNull t.FullName)
        && t.FullName.StartsWith("Microsoft.FSharp.Linq.RuntimeHelpers.AnonymousObject", StringComparison.Ordinal)

    /// System.Tuple / System.ValueTuple of up to seven elements (eight and more nest the rest in `Rest`).
    let private isSystemTuple (t: Type) =
        t.IsGenericType
        && not (isNull t.FullName)
        && (t.FullName.StartsWith("System.Tuple`", StringComparison.Ordinal)
            || t.FullName.StartsWith("System.ValueTuple`", StringComparison.Ordinal))
        && t.GetGenericArguments().Length < 8

    /// A row type whose members may hold whole entities: anonymous types, F# AnonymousObject and System tuples.
    /// Their entity members are packed into struct columns named after the member (Item1..ItemN for tuples).
    let isTransparentRowType (t: Type) = isAnonymousType t || isFSharpAnonymousObject t || isSystemTuple t

    /// The members of a row: Item1..ItemN for a System tuple (fields of a ValueTuple), otherwise the model
    /// properties.
    let private rowMembers (t: Type) : string array =
        if isSystemTuple t then
            Array.init (t.GetGenericArguments().Length) (fun i -> sprintf "Item%d" (i + 1))
        else
            PolarsTypeHelper.GetModelProperties t |> Array.map (fun p -> p.Name)

    let private isRowElementType (t: Type) = isEntityType t || isTransparentRowType t

    let rec private isRootedInScope (ctx: TranslationContext) (e: Expression) =
        match ExpressionPatterns.unwrap e with
        | :? ParameterExpression as p -> isSyntheticScopeParam ctx.ContainsParam p
        | MemberAccess(target, _) -> isRootedInScope ctx target
        | _ -> false

    /// Struct of a group element's columns, used by aggregations over whole rows (g.ToList(), g.Distinct(), ...).
    /// Built only when every property of the element type is a column of the input plan: a projection's
    /// anonymous type qualifies, a join's pair of entities (transparent identifier) does not.
    let tryWholeRowStruct (elemType: Type) (cols: string list) : ScalarExpr option =
        let props = PolarsTypeHelper.GetModelProperties elemType
        let available = set cols

        if props.Length > 0 && props |> Array.forall (fun p -> available.Contains p.Name) then
            let colExprs = props |> Array.map (fun p -> ScalarExpr.Column p.Name) |> Array.toList
            Some(ScalarExpr.Function(ScalarFunction.AsStruct, colExprs, LogicalType.Unknown))
        else
            None

    /// The (member, physical column) pairs of a join scope, in the entity's property order.
    let private scopeFields (rowShape: RowShape) (scope: RowScope) (entityType: Type) =
        let entries = rowShape.Members scope

        if entries.IsEmpty then
            None
        else
            let props = PolarsTypeHelper.GetModelProperties entityType
            let rank (m: string) =
                props
                |> Array.tryFindIndex (fun p -> p.Name = m || PolarsTypeHelper.GetSerializedName p = m)
                |> Option.defaultValue Int32.MaxValue
            Some(entries |> List.sortBy (fun (m, _) -> rank m, m))

    /// Packs an expression that denotes one whole entity: a join scope or a single-table row parameter.
    let private tryPackEntity
        (ctx: TranslationContext)
        (cols: Lazy<string list>)
        (nullExtended: Lazy<Set<string>>)
        (e: Expression)
        : ScalarExpr option =
        if not (isEntityType e.Type) then
            None
        else
            match ctx, e with
            | TranslationContext.ScopedEntity(rowParam :: _, rowShape, _), _ when isRootedInScope ctx e ->
                // The entity's path in the row identifies it exactly; its name only when names are unique.
                tryMemberPath rowParam e
                |> Option.filter (fun path -> not path.IsEmpty)
                |> Option.bind (fun path -> scopeFields rowShape (RowScope.Path path) e.Type)
                |> Option.orElseWith (fun () ->
                    ColumnResolver.tryExtractScopeName e
                    |> Option.bind (fun scope -> scopeFields rowShape (RowScope.Named scope) e.Type))
                |> Option.map (packColumns nullExtended)
            | (TranslationContext.Entity p | TranslationContext.EntityWithCols(p, _)), (:? ParameterExpression as pe) when pe.Name = p ->
                let available = set cols.Value
                let props = PolarsTypeHelper.GetModelProperties e.Type

                if props.Length > 0 && props |> Array.forall (fun prop -> available.Contains prop.Name) then
                    Some(packColumns nullExtended [ for prop in props -> prop.Name, prop.Name ])
                else
                    None
            | _ -> None

    /// Translates a member value to (expr, packed), where `packed` tells whether the value is, or contains,
    /// a whole entity packed into a struct; None when it cannot be translated.
    let rec private translate
        (ctx: TranslationContext)
        (cols: Lazy<string list>)
        (nullExtended: Lazy<Set<string>>)
        (expr: Expression)
        : (ScalarExpr * bool) option =
        // Unwrapped only to recognize an entity; an ordinary value keeps its conversions.
        let e = ExpressionPatterns.unwrap expr

        match tryPackEntity ctx cols nullExtended e with
        | Some packed -> Some(packed, true)
        | None ->
            match tryPackComposite ctx cols nullExtended e with
            | Some composite -> composite
            | None -> ExprTranslator.tryTranslateToScalar ctx expr |> Option.map (fun s -> s, false)

    /// A transparent identifier nested in the row (`t` in `new { t, z }` after `new { x, y }`): a struct of its
    /// members when at least one of them is a scope entity. Some None when such a member cannot be translated;
    /// None when it holds no entity and is translated as an ordinary value.
    and private tryPackComposite
        (ctx: TranslationContext)
        (cols: Lazy<string list>)
        (nullExtended: Lazy<Set<string>>)
        (e: Expression)
        : (ScalarExpr * bool) option option =
        let isScoped =
            match ctx with
            | TranslationContext.ScopedEntity(_, rowShape, _) -> not rowShape.IsEmpty
            | _ -> false

        if not (isScoped && isTransparentRowType e.Type && isRootedInScope ctx e) then
            None
        else
            let parts =
                rowMembers e.Type
                |> Array.map (fun name -> name, translate ctx cols nullExtended (Expression.PropertyOrField(e, name)))

            if not (parts |> Array.exists (fun (_, r) -> match r with Some(_, true) -> true | _ -> false)) then
                None
            elif parts |> Array.forall (snd >> Option.isSome) then
                let fields = parts |> Array.map (fun (n, r) -> ScalarExpr.Alias(fst r.Value, n)) |> Array.toList
                Some(Some(ScalarExpr.Function(ScalarFunction.AsStruct, fields, LogicalType.Unknown), true))
            else
                Some None

    /// Whether a member value is a whole entity or a transparent row, rather than a column value.
    let holdsEntities (expr: Expression) =
        let t = (ExpressionPatterns.unwrap expr).Type
        isEntityType t || isTransparentRowType t

    let hasRowPathScopes (rowShape: RowShape) = rowShape.HasPathScopes

    /// How the parameter of a projection over the current row stands for its columns: the previous
    /// transparent row when the row has path scopes, otherwise one entity over all the row's columns.
    let rowSideOf (rowShape: RowShape) (rowCols: string list) : RowSide =
        if hasRowPathScopes rowShape then RowSide.Row else RowSide.Entity [ for c in rowCols -> c, c ]

    /// The shape of the row a transparent projection or join result selector builds from `bindings`:
    /// a member bound to one entity holds that entity, a member bound to the previous row (or a part of it)
    /// holds that subtree, and the previous row's path scopes are replaced. Members of other types are
    /// columns. None when a member bound to an entity or a row is not one of `sides`' entities or subtrees.
    let tryReshapeRow
        (rowShape: RowShape)
        (sides: (string * RowSide) list)
        (bindings: (string * Expression) list)
        : RowShape option =
        let previous = rowShape.PathEntries
        let named = rowShape.WithoutPathScopes

        let entriesFor (memberName: string) (expr: Expression) =
            sides
            |> List.tryPick (fun (param, side) -> tryMemberPath param expr |> Option.map (fun path -> side, path))
            |> Option.bind (fun (side, path) ->
                match side, path with
                | RowSide.Entity cols, [] -> Some [ for m, phys in cols -> (RowScope.Path [ memberName ], m), phys ]
                | RowSide.Row, _ ->
                    let moved =
                        previous
                        |> List.choose (fun ((p, m), phys) ->
                            if p.Length >= path.Length && List.take path.Length p = path then
                                Some((RowScope.Path(memberName :: List.skip path.Length p), m), phys)
                            else
                                None)
                    if moved.IsEmpty then None else Some moved
                | _ -> None)

        let rec collect acc remaining =
            match remaining with
            | [] -> Some acc
            | (memberName, expr) :: rest when holdsEntities expr ->
                match entriesFor memberName expr with
                | Some entries -> collect (entries @ acc) rest
                | None -> None
            | _ :: rest -> collect acc rest

        collect [] bindings
        |> Option.map (List.fold (fun (m: RowShape) ((scope, memberName), column) -> m.Add(scope, memberName, column)) named)

    /// Translates a projected member, packing whole entities (a join scope, a transparent identifier holding
    /// scopes, or the row parameter itself) into struct columns.
    let tryTranslateMember (ctx: TranslationContext) (plan: LogicalPlan) (expr: Expression) : ScalarExpr option =
        let cols = lazy (SchemaInference.inferPlanSchema plan).ColumnNames
        let nullExtended = lazy (nullExtendedColumns plan)
        translate ctx cols nullExtended expr |> Option.map fst

    /// Reshapes the rows of `plan`, whose transparent join scopes are `rowShape`, so they materialize as
    /// `rowType`: each member of the element that is (or holds) a scope entity becomes a struct column.
    let packRow (rowShape: RowShape) (plan: LogicalPlan) (rowType: Type) : RowPacking =
        if rowShape.IsEmpty || not (isRowElementType rowType) then
            RowPacking.NotNeeded
        else
            let row = Expression.Parameter(rowType, "<>row")
            let cols = (SchemaInference.inferPlanSchema plan).ColumnNames
            let ctx = TranslationContext.ScopedEntity([ row.Name ], rowShape, cols)
            let nullExtended = lazy (nullExtendedColumns plan)

            let members =
                rowMembers rowType
                |> Array.map (fun name -> name, translate ctx (Lazy<string list>.CreateFromValue cols) nullExtended (Expression.PropertyOrField(row, name)))

            // Scope resolution falls back to the member's own name, which need not be a column of the plan.
            let available = set cols
            let isResolved (r: (ScalarExpr * bool) option) =
                match r with
                | Some(expr, _) -> Set.isSubset (ScalarExprOps.collectColumns expr) available
                | None -> false

            if not (members |> Array.exists (fun (_, r) -> match r with Some(_, true) -> true | _ -> false)) then
                RowPacking.NotNeeded
            elif members |> Array.forall (snd >> isResolved) then
                RowPacking.Packed(LogicalPlan.Project(plan, members |> Array.map (fun (n, r) -> n, fst r.Value) |> Array.toList))
            else
                RowPacking.Unresolvable
