namespace Kaminari.Samidare

open System
open System.Collections
open System.Linq
open System.Linq.Expressions
open Kaminari.Samidare.IR
open Kaminari.Sazanami.Helpers

/// Pure symbolic resolution of sequence operands (Join/Concat/Union/Zip inputs, subquery roots, Append/Prepend
/// elements) into LogicalPlan subtrees. Every source is registered in the caller's SourceRegistry and scanned
/// by slot: backend-native objects are bound through ISourceBinder, in-memory collections become deferred
/// SourceBinding.InMemory snapshots. No native resources are allocated here.
module internal SourceResolver =

    /// Finds the root queryable of a query-operator chain and returns it with the chain re-rooted on a
    /// constant of it. A root reached through a closure (a subquery inside a lambda reads `closure.q`, which
    /// partial evaluation leaves alone because it is IQueryable-typed) is evaluated and inlined, so the chain
    /// can be re-created on the root's provider and grafted instead of being bound opaquely.
    let rec private tryRootChain (e: Expression) : (IQueryable * Expression) option =
        match e with
        | :? ConstantExpression as c when not (isNull c.Value) ->
            match c.Value with
            | :? IQueryable as q -> Some(q, e)
            | _ -> None
        | :? MethodCallExpression as mc ->
            mc.Arguments
            |> Seq.indexed
            |> Seq.tryPick (fun (i, arg) ->
                tryRootChain arg
                |> Option.map (fun (q, rerooted) ->
                    let args = mc.Arguments |> Seq.mapi (fun j a -> if j = i then rerooted else a)
                    q, (mc.Update(mc.Object, args) :> Expression)))
        | :? MemberExpression when typeof<IQueryable>.IsAssignableFrom e.Type ->
            match PartialEvaluator.tryEvaluate e with
            | Some(:? IQueryable as q) -> Some(q, Expression.Constant(q, e.Type) :> Expression)
            | _ -> None
        | _ -> None

    /// Locates a Polars queryable for the expression: a constant queryable, a subtree re-rooted on its
    /// root provider, or an evaluated value.
    let private tryResolvePolarsQueryable (expr: Expression) : IPolarsInternalExplainable option =
        match expr with
        | :? ConstantExpression as c when not (isNull c.Value) ->
            match c.Value with
            | :? IPolarsInternalExplainable as q -> Some q
            | _ -> None
        | _ ->
            match tryRootChain expr with
            | Some(rootQuery, rerooted) ->
                match rerooted with
                | :? ConstantExpression as c ->
                    match c.Value with
                    | :? IPolarsInternalExplainable as q -> Some q
                    | _ -> None
                | _ ->
                    try
                        match rootQuery.Provider.CreateQuery rerooted with
                        | :? IPolarsInternalExplainable as q -> Some q
                        | _ -> None
                    with _ ->
                        None
            | None ->
                match PartialEvaluator.tryEvaluate expr with
                | Some(:? IPolarsInternalExplainable as q) -> Some q
                | _ -> None

    /// Evaluates an arbitrary sequence expression to its runtime value.
    let private tryEvaluateValue (expr: Expression) : obj option =
        match ExpressionPatterns.unwrap expr with
        | :? ConstantExpression as ce -> Option.ofObj ce.Value
        | other ->
            match PartialEvaluator.tryEvaluate other with
            | Some v -> Option.ofObj v
            | None ->
                try
                    let thunk = Expression.Lambda<Func<obj>>(Expression.Convert(other, typeof<obj>)).Compile()
                    Option.ofObj (thunk.Invoke())
                with _ ->
                    None

    /// Schema of the table the backend produces when transposing rows of elemType:
    /// a single "value" column for simple types, otherwise one column per model property.
    /// Enum columns carry their type (their literals and conversions depend on it); other columns are Unknown.
    let inMemorySchema (elemType: Type) : LogicalSchema =
        let columnType (t: Type) =
            LogicalTypes.tryEnumOf (ClrTypeHelper.UnwrapCoreType t)
            |> Option.defaultValue LogicalType.Unknown

        if ClrTypeHelper.IsSupportedSimpleType elemType then
            LogicalSchema.OfFields [ ("value", columnType elemType) ]
        else
            ClrTypeHelper.GetModelProperties elemType
            |> Array.map (fun p -> p.Name, columnType p.PropertyType)
            |> LogicalSchema.OfFields

    /// Registers an in-memory typed row array and builds a Scan over it.
    let inMemoryPlan (sources: SourceRegistry) (rows: Array) (elemType: Type) : LogicalPlan =
        sources.Scan(SourceBinding.InMemory(rows, elemType), inMemorySchema elemType)

    /// Registers a backend table source and builds a Scan over it.
    let tablePlan (sources: SourceRegistry) (table: ILogicalTableSource) : LogicalPlan =
        sources.Scan(SourceBinding.Table table, table.Schema)

    /// Snapshots an in-memory collection into a fresh typed array (enumerated exactly once, at IR construction).
    let private snapshot (values: IEnumerable) (elemType: Type) : Array =
        let buffer = ResizeArray<obj>()
        for v in values do
            buffer.Add v
        let rows = Array.CreateInstance(elemType, buffer.Count)
        for i in 0 .. buffer.Count - 1 do
            rows.SetValue(buffer.[i], i)
        rows

    /// Binds a non-string IEnumerable<T> value as a deferred InMemory scan.
    let tryBindInMemory (sources: SourceRegistry) (value: obj) : LogicalPlan option =
        match value with
        | null
        | :? string -> None
        | :? IEnumerable as values ->
            match ClrTypeHelper.TryGetEnumerableElementType(value.GetType()) with
            | null -> None
            | elemType -> Some(inMemoryPlan sources (snapshot values elemType) elemType)
        | _ -> None

    /// Builds a 1-row InMemory scan from a single element (Append / Prepend).
    let singleRowPlan (sources: SourceRegistry) (elem: obj) (elemType: Type) : LogicalPlan option =
        if isNull elem then
            None
        else
            let rows = Array.CreateInstance(elemType, 1)
            rows.SetValue(elem, 0)
            Some(inMemoryPlan sources rows elemType)

    /// Resolves a sequence expression into a LogicalPlan:
    /// 1. Polars queryables with a pure plan are grafted as IR subtrees (their sources join the registry).
    /// 2. Other Polars queryables and backend-native objects are bound as opaque table sources by the backend.
    /// 3. In-memory collections become deferred InMemory snapshots.
    let resolveSequencePlan (binder: ISourceBinder) (sources: SourceRegistry) (expr: Expression) : LogicalPlan option =
        let queryableOpt = tryResolvePolarsQueryable expr

        let purePlanOpt =
            queryableOpt
            |> Option.bind (fun q -> q.TryGetPureLogicalPlan())
            |> Option.map sources.Graft

        purePlanOpt
        |> Option.orElseWith (fun () ->
            let valueOpt =
                match queryableOpt with
                | Some q -> Some(box q)
                | None -> tryEvaluateValue expr

            valueOpt
            |> Option.bind (fun value ->
                match binder.TryBindExternalSource value with
                | Some table -> Some(tablePlan sources table)
                | None -> tryBindInMemory sources value))
