namespace Polars.NET.Linq

open System
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open Kaminari.Samidare.IR
open Polars.NET.Linq.Backend.Polars
open Kaminari.Sazanami.Helpers
open Kaminari.Samidare

/// Executing a pipeline that needs the full LINQ-to-Objects fallback: the longest natively compilable prefix
/// runs in Polars, and the rest of the pipeline runs via EnumerableQuery over its materialized rows.
module internal InMemoryExecution =

    /// For a pipeline that needs full LINQ-to-Objects fallback, finds the longest prefix of its
    /// method-call chain that compiles without it. Returns the call at which LINQ-to-Objects takes over
    /// and the prefix expression that call consumes, or None when no non-trivial prefix runs natively.
    /// `effectiveExpr` must be the tree the caller later rewrites, since the prefix is identified by reference.
    let tryFindNativePrefix (provider: IQueryProvider) (effectiveExpr: Expression) : (MethodCallExpression * Expression) option =
        let isQueryable (t: Type) = typeof<IQueryable>.IsAssignableFrom t

        // Method calls along the source spine, outermost first.
        let rec spine (e: Expression) =
            match e with
            | :? MethodCallExpression as call when
                isNull call.Object && call.Arguments.Count > 0 && isQueryable call.Arguments.[0].Type
                ->
                call :: spine call.Arguments.[0]
            | _ -> []

        let compilesNatively (prefix: Expression) =
            try
                match provider.CreateQuery prefix with
                | :? IPolarsInternalExplainable as q -> q.IsNativelyExecutable()
                | _ -> false
            with _ ->
                false

        effectiveExpr
        |> spine
        |> List.tryPick (fun consumer ->
            let prefix = consumer.Arguments.[0]
            // ThenBy needs the ordered sequence its OrderBy produced, so the two are never split.
            let splitsOrdering = consumer.Method.Name = "ThenBy" || consumer.Method.Name = "ThenByDescending"
            match prefix with
            | :? MethodCallExpression when not splitsOrdering && compilesNatively prefix -> Some(consumer, prefix)
            | _ -> None)

    /// Executes the full LINQ expression pipeline in-memory via .NET EnumerableQuery LINQ-to-Objects engine
    let executeRootInMemory<'T> (effectiveExpr: Expression) : IEnumerable<'T> =
        match tryFindRootQueryable effectiveExpr with
        | Some(rootExpr, rootType) ->
            let rootConst = rootExpr :?> ConstantExpression
            let rootQueryable = rootConst.Value :?> IQueryable

            let rootElemType =
                match ClrTypeHelper.TryGetEnumerableElementType rootType with
                | null -> rootQueryable.ElementType
                | t -> t

            let toArrayMethod =
                LinqReflectionCache.GetEnumerableMethod("ToArray", 1, [| rootElemType |])

            let castMethod =
                LinqReflectionCache.GetEnumerableMethod("Cast", 1, [| rootElemType |])

            let casted = castMethod.Invoke(null, [| box rootQueryable |])
            let inMemoryArray = toArrayMethod.Invoke(null, [| casted |])

            let asQueryableMethod =
                LinqReflectionCache.GetQueryableMethod("AsQueryable", 1, [| rootElemType |])

            let inMemoryQueryable =
                asQueryableMethod.Invoke(null, [| inMemoryArray |]) :?> IQueryable

            let targetQueryType = typedefof<IQueryable<_>>.MakeGenericType(rootElemType)
            let replacement = Expression.Constant(inMemoryQueryable, targetQueryType)

            let replacer =
                { new ExpressionVisitor() with
                    override _.VisitConstant(node: ConstantExpression) =
                        if
                            not (isNull node.Value)
                            && (obj.ReferenceEquals(node.Value, rootConst.Value)
                                || typeof<IPolarsPlanSource>.IsAssignableFrom(node.Type))
                        then
                            replacement
                        else
                            base.VisitConstant(node)
                }

            let inMemoryExpr = replacer.Visit(effectiveExpr)
            let resultQueryable = inMemoryQueryable.Provider.CreateQuery<'T>(inMemoryExpr)
            resultQueryable :> IEnumerable<'T>

        | None -> failwithf "Failed to locate root queryable for in-memory execution: %A" effectiveExpr

    /// Executes a pipeline that needs LINQ-to-Objects: the longest natively compilable prefix runs in Polars,
    /// and the rest of the pipeline runs via EnumerableQuery over the materialized prefix rows.
    /// Falls back to replaying the whole pipeline over the in-memory root when no prefix runs natively.
    let execute<'T> (provider: IQueryProvider) (effectiveExpr: Expression) : IEnumerable<'T> =
        let split =
            tryFindNativePrefix provider effectiveExpr
            |> Option.bind (fun (_, prefix) ->
                match ClrTypeHelper.TryGetEnumerableElementType prefix.Type with
                | null -> None
                | rowType -> Some(prefix, rowType))

        match split with
        | Some(prefix, rowType) ->
            let prefixQuery = provider.CreateQuery prefix
            let toArrayMethod = LinqReflectionCache.GetEnumerableMethod("ToArray", 1, [| rowType |])
            let rows = toArrayMethod.Invoke(null, [| box prefixQuery |])

            let asQueryableMethod = LinqReflectionCache.GetQueryableMethod("AsQueryable", 1, [| rowType |])
            let localRows = asQueryableMethod.Invoke(null, [| rows |]) :?> IQueryable

            if prefix.Type.IsAssignableFrom(localRows.GetType()) then
                let replacement = Expression.Constant(localRows, prefix.Type)

                let replacer =
                    { new ExpressionVisitor() with
                        override _.Visit(node: Expression) =
                            if obj.ReferenceEquals(node, prefix) then replacement :> Expression
                            else base.Visit(node)
                    }

                let suffixExpr = replacer.Visit effectiveExpr
                localRows.Provider.CreateQuery<'T>(suffixExpr) :> IEnumerable<'T>
            else
                executeRootInMemory<'T> effectiveExpr
        | None -> executeRootInMemory<'T> effectiveExpr
