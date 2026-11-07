namespace Polars.NET.Linq

open Polars.NET.Linq.IR
open System
open System.Linq.Expressions

type internal TranslationContext =
    /// Empty context with no parameter bindings (e.g. constants, unbound reductions, or outer Having predicates).
    | Empty

    /// Standard single-entity row context (e.g. e => e.Age), where member accesses resolve to DataFrame columns.
    | Entity of paramName: string

    /// Entity row context carrying known column names for precise tuple key and carrier member resolution.
    | EntityWithCols of paramName: string * cols: string list

    /// Scalar sequence element context (e.g. x => x.Length or x => x > 10).
    /// The parameter node itself resolves to physicalCol, while member accesses on it represent scalar operations.
    | Scalar of paramName: string * physicalCol: string

    /// Binary parameter context for Joins and Merges (e.g. (outer, inner) => new { outer.Id, inner.Name }).
    | BinaryJoin of leftParam: string * rightParam: string * leftCols: Set<string> * rightSuffix: string

    /// Merge context: (target, source) => predicate
    /// Target columns resolve to "Col", source columns resolve to "Col" + sourceSuffix.
    | Merge of targetParam: string * sourceParam: string * sourceSuffix: string

    /// Binary Tuple Join context for .NET 11 tuple joins returning (TOuter Outer, TInner Inner).
    /// Resolves pair.Outer (Item1) and pair.Inner (Item2) against physical columns in the joined LazyFrame.
    | TupleJoin of pairParam: string * leftCols: Set<string> * rightSuffix: string

    /// Binary correlated subquery join context (outerParams, innerParam, leftCols, rightSuffix, rowShape)
    | CorrelatedJoin of outerParams: string list * innerParam: string * leftCols: Set<string> * rightSuffix: string * rowShape: RowShape

    /// Scoped entity context carrying the row shape: where each scope's members live (e.g. sn.n_name -> "n_name_right")
    /// across cascaded multi-table joins.
    | ScopedEntity of paramNames: string list * rowShape: RowShape * fallbackCols: string list

    /// List element placeholder context for row-level ListEval (pl.element()).
    | ListElement of paramName: string

    /// Determines whether the given parameter name matches any parameter active in the current scope.
    member this.ContainsParam(name: string) : bool =
        match this with
        | TranslationContext.Empty -> false
        | TranslationContext.Entity p -> p = name
        | TranslationContext.EntityWithCols(p, _) -> p = name
        | TranslationContext.ScopedEntity(paramsList, rowShape, _) ->
            List.contains name paramsList || rowShape.HasScope(RowScope.Named name)
        | TranslationContext.ListElement p -> p = name
        | TranslationContext.Scalar(p, _) -> p = name
        | TranslationContext.BinaryJoin(left, right, _, _) -> name = left || name = right
        | TranslationContext.Merge(target, source, _) -> name = target || name = source
        | TranslationContext.TupleJoin(pairParam, _, _) -> name = pairParam
        | TranslationContext.CorrelatedJoin(outerParams, inner, _, _, rowShape) ->
            List.contains name outerParams || name = inner || rowShape.HasScope(RowScope.Named name)

    /// Attempts to resolve the parameter expression node itself to a pure symbolic ScalarExpr.
    member this.TryResolveParameter(name: string) : ScalarExpr option =
        match this with
        | TranslationContext.Scalar(p, physicalCol) when p = name && not (String.IsNullOrEmpty physicalCol) ->
            Some(ScalarExpr.Column physicalCol)
        | TranslationContext.ListElement p when p = name ->
            Some(ScalarExpr.Column "")
        | _ -> None

    /// Indicates whether member/property accesses are eligible to be resolved as DataFrame columns.
    member this.AllowsColumnMemberLookup: bool =
        match this with
        | TranslationContext.Scalar _ -> false
        | _ -> true

/// Signature for higher-order recursive scalar expression evaluation across translator modules.
type internal ScalarExprEvaluator = TranslationContext -> Expression -> ScalarExpr option
