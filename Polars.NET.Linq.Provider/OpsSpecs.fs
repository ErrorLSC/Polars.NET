namespace Polars.NET.Linq.Provider

open Polars.NET.Core
open System.Linq.Expressions
open System.Reflection
open System

/// Specification for multi-column sorting pushdown
type internal SortSpec = {
    Expr: ExprHandle
    Descending: bool
    NullsLast: bool
}

/// Specification for exploding list/array columns
type internal ExplodeSpec = {
    ColumnNames: string array
    EmptyAsNull: bool
    KeepNulls: bool
}

/// Specification for column renaming pushdown
type internal RenameSpec = {
    ExistingNames: string array
    NewNames: string array
}

/// Specification for native join pushdown
type internal JoinSpec = {
    RightLf: LazyFrameHandle
    LeftOn: ExprHandle array
    RightOn: ExprHandle array
    How: PlJoinType
    Suffix: string option
    InvertSides: bool
}

/// Specification for native GroupBy aggregation pushdown
type internal GroupBySpec = {
    Keys: ExprHandle array
    Aggs: ExprHandle array
    Having: ExprHandle option
    MaintainOrder: bool
}

type internal UniqueSpec = {
    SubsetCols: string array option
    Keep: PlUniqueKeepStrategy
    MaintainOrder: bool
}

/// Specification for vertical concatenation pushdown
type internal ConcatSpec = {
    OtherLf: LazyFrameHandle
    Prepend: bool // false: current @ other (Append), true: other @ current (Prepend)
}

/// Uniform contract to expose internal LazyFrameHandle without leaking generic parameters
type internal IPolarsPlanSource =
    abstract member GetRawLazyFrameHandle: unit -> LazyFrameHandle

/// Abstract representation of pushdown operations in the LINQ execution pipeline
[<RequireQualifiedAccess>]
type internal QueryOp =
    | Filter of ExprHandle
    | Slice of offset: int64 * length: uint32
    | SkipLast of count: uint32
    | Sort of SortSpec list
    | Join of JoinSpec
    | GroupBy of GroupBySpec
    | Unique of UniqueSpec
    | Concat of ConcatSpec
    | HorizontalConcat of otherLfs: LazyFrameHandle array
    | Explode of ExplodeSpec
    | Rename of RenameSpec
    | Select of ExprHandle array
    | WithColumns of ExprHandle array
    | Reverse
    | SelectPassthrough

/// Linear representation of query expressions before optimization/fusion
[<RequireQualifiedAccess>]
type internal LinqStage =
    | Filter of LambdaExpression
    | Sort of LambdaExpression * isDescending: bool
    | TakeWhile of LambdaExpression
    | SkipWhile of LambdaExpression
    | Take of uint32
    | Skip of uint32
    | TakeLast of uint32
    | SkipLast of uint32
    | Chunk of size: int
    | CountBy of keySelector: LambdaExpression
    | Distinct
    | DistinctBy of LambdaExpression
    | Concat of Expression
    | Append of Expression
    | Prepend of Expression
    | Union of Expression * keyLambdaOpt: LambdaExpression option
    | Join of MethodInfo * Expression * LambdaExpression * LambdaExpression * LambdaExpression
    | Explode of collectionLambda: LambdaExpression * resultLambdaOpt: LambdaExpression option
    | CrossJoin of innerExpr: Expression * resultLambdaOpt: LambdaExpression option
    | SetOp of MethodInfo * Expression * LambdaExpression option * LambdaExpression option
    | GroupByKey of LambdaExpression
    | GroupByWithResult of LambdaExpression * LambdaExpression
    | GroupJoin of MethodInfo * Expression * LambdaExpression * LambdaExpression * LambdaExpression
    | Zip of secondExpr: Expression * resultLambdaOpt: LambdaExpression option
    | Zip3 of secondExpr: Expression * thirdExpr: Expression
    | Reverse
    | Shuffle
    | Cast of targetType: Type
    | OfType of sourceType: Type * targetType: Type
    | DefaultIfEmpty of defaultValExprOpt: Expression option
    | Project of LambdaExpression

