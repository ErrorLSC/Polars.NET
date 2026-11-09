namespace Kaminari.Samidare

open Kaminari.Samidare.IR
open System.Linq.Expressions
open System.Reflection
open System

/// Canonical constants used across the LINQ engine
[<RequireQualifiedAccess>]
module internal LinqConstants =
    [<Literal>]
    let DefaultRightSuffix = "_right"
    [<Literal>]
    let DefaultLeftSuffix = "_left"

/// Linear representation of query expressions before optimization/fusion
[<RequireQualifiedAccess>]
type internal LinqStage =
    /// A query operator the pipeline does not translate (an overload with a comparer, an unknown operator):
    /// the query runs as LINQ to Objects, under the client evaluation policy, and never skips it.
    | Unsupported of operatorCall: string
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
        tolerance: AsofTolerance option *
        strategy: AsofStrategy
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

/// Execution traits of pipeline stages that several components must agree on.
[<RequireQualifiedAccess>]
module internal LinqStageTraits =

    /// Stages applied to the collected frame or during materialization rather than in the native plan
    /// (Shuffle after collect; Chunk, AggregateBy, DefaultIfEmpty while materializing). A pipeline containing
    /// one does not end in a plain native frame: its plan cannot be grafted into another query, and its rows
    /// cannot be streamed batch by batch.
    let isPostCollect (stage: LinqStage) : bool =
        match stage with
        | LinqStage.Shuffle
        | LinqStage.Chunk _
        | LinqStage.AggregateBy _
        | LinqStage.DefaultIfEmpty _ -> true
        | _ -> false

    /// Stages whose materialization is not a flat row stream. Client row operations (a client Where / Select)
    /// stream over flat native rows, so once one is present these stages are only correct under LINQ-to-Objects.
    /// Shuffle is absent: it permutes the collected frame before the row operations run. OfType is present:
    /// it filters materialized objects by runtime type.
    let breaksRowStream (stage: LinqStage) : bool =
        match stage with
        | LinqStage.Chunk _
        | LinqStage.AggregateBy _
        | LinqStage.DefaultIfEmpty _
        | LinqStage.OfType _ -> true
        | _ -> false
