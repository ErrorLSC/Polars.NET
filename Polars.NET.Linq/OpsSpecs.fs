namespace Polars.NET.Linq

open Polars.NET.Core
open Polars.NET.Core.Enums
open System.Linq.Expressions
open System.Reflection
open System

/// Canonical constants used across Polars LINQ engine
[<RequireQualifiedAccess>]
module internal LinqConstants =
    [<Literal>]
    let DefaultRightSuffix = "_right"
    [<Literal>]
    let DefaultLeftSuffix = "_left"

/// Diagnostic tracker for managed C# UDF callbacks pushed down to Polars native engine
type internal UdfTracker =
    [<ThreadStatic; DefaultValue>]
    static val mutable private udfs: string list

    static member Reset() =
        UdfTracker.udfs <- []

    static member Record(udfDesc: string) =
        let current =
            if isNull (box UdfTracker.udfs) then [] else UdfTracker.udfs
        if not (List.contains udfDesc current) then
            UdfTracker.udfs <- udfDesc :: current

    static member DetectedUdfs =
        if isNull (box UdfTracker.udfs) then [] else List.rev UdfTracker.udfs

/// Optimization context for coordinating max split indices across multiple bindings in a projection
type internal SplitOptContext =
    [<ThreadStatic; DefaultValue>]
    static val mutable private maxMap: Map<string * string, int> option
    static member Current
        with get () = SplitOptContext.maxMap
        and set (v) = SplitOptContext.maxMap <- v

/// Uniform contract to expose internal LazyFrameHandle without leaking generic parameters
type internal IPolarsPlanSource =
    /// Compiles the queryable pipeline stages and returns the final optimized LazyFrameHandle
    abstract member GetCompiledLazyFrameHandle: unit -> LazyFrameHandle

/// Linear representation of query expressions before optimization/fusion
[<RequireQualifiedAccess>]
type internal LinqStage =
    | Filter of LambdaExpression
    | Sort of LambdaExpression * isDescending: bool * nullsLast: bool
    | SortDefault of isDescending: bool * nullsLast: bool // For .NET 7 Order() / OrderDescending()
    | Index // For .NET 9 Index()
    | TakeWhile of LambdaExpression
    | SkipWhile of LambdaExpression
    | Take of uint32
    | Skip of uint32
    | TakeLast of uint32
    | SkipLast of uint32
    | Chunk of size: int
    | CountBy of keySelector: LambdaExpression
    | AggregateBy of
        keySelector: LambdaExpression *
        seedExpr: Expression *
        seedFactoryOpt: LambdaExpression option *
        func: LambdaExpression
    | Distinct
    | DistinctBy of LambdaExpression
    | Concat of Expression
    | Append of Expression
    | Prepend of Expression
    | Union of Expression * keyLambdaOpt: LambdaExpression option
    | Join of MethodInfo * Expression * LambdaExpression * LambdaExpression * LambdaExpression option
    | JoinAsOf of
        methodInfo: MethodInfo *
        inner: Expression *
        outerKey: LambdaExpression *
        innerKey: LambdaExpression *
        outerByOpt: LambdaExpression option *
        innerByOpt: LambdaExpression option *
        resSelOpt: LambdaExpression option *
        toleranceStr: string option *
        toleranceInt: int64 option *
        toleranceFloat: double option *
        strategy: PlAsofStrategy
    | Explode of collectionLambda: LambdaExpression * resultLambdaOpt: LambdaExpression option
    | Unpivot of collectionLambda: LambdaExpression * resultLambdaOpt: LambdaExpression option
    | CrossJoin of innerExpr: Expression * resultLambdaOpt: LambdaExpression option
    | SetOp of MethodInfo * Expression * LambdaExpression option * LambdaExpression option
    | GroupByKey of LambdaExpression
    | GroupPartitionSlice of
        keySelector: LambdaExpression *
        innerStages: LinqStage list *
        resSelectorOpt: LambdaExpression option
    | GroupByWithResult of LambdaExpression * LambdaExpression
    | GroupByWithElementAndResult of
        keySelector: LambdaExpression *
        elemSelector: LambdaExpression *
        resSelector: LambdaExpression
    | GroupByRollup of keySelector: LambdaExpression * resSelector: LambdaExpression
    | GroupByCube of keySelector: LambdaExpression * resSelector: LambdaExpression
    | GroupByGroupingSets of keySelector: LambdaExpression * setsExpr: Expression * resSelector: LambdaExpression
    | GroupJoin of
        methodInfo: MethodInfo *
        inner: Expression *
        outerKey: LambdaExpression *
        innerKey: LambdaExpression *
        resSelOpt: LambdaExpression option
    | Zip of secondExpr: Expression * resultLambdaOpt: LambdaExpression option
    | Zip3 of secondExpr: Expression * thirdExpr: Expression
    | Reverse
    | Shuffle
    | Cast of targetType: Type
    | OfType of sourceType: Type * targetType: Type
    | DefaultIfEmpty of defaultValExprOpt: Expression option
    | Project of LambdaExpression
