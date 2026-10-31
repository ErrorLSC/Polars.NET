namespace Polars.NET.Linq

open Polars.NET.Core
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

/// Specification for multi-column sorting pushdown
type internal SortSpec =
    {
        Expr: ExprHandle
        Descending: bool
        NullsLast: bool
    }

/// Specification for exploding list/array columns
type internal ExplodeSpec =
    {
        ColumnNames: string array
        EmptyAsNull: bool
        KeepNulls: bool
    }

/// Specification for unpivoting (melting) wide columns into rows
type internal UnpivotSpec =
    {
        Index: string array
        On: string array option
        VariableName: string
        ValueName: string
    }

/// Optimization context for coordinating max split indices across multiple bindings in a projection
type internal SplitOptContext =
    [<ThreadStatic; DefaultValue>]
    static val mutable private maxMap: Map<string * string, int> option
    static member Current
        with get () = SplitOptContext.maxMap
        and set (v) = SplitOptContext.maxMap <- v

/// Specification for column renaming pushdown
type internal RenameSpec =
    {
        ExistingNames: string array
        NewNames: string array
    }

/// Specification for native join pushdown
type internal JoinSpec =
    {
        RightLf: LazyFrameHandle
        LeftOn: ExprHandle array
        RightOn: ExprHandle array
        How: PlJoinType
        Suffix: string option
        InvertSides: bool
        Coalesce: PlJoinCoalesce
        NullsEqual: bool
    }

/// Specification for native AsOf join pushdown
type internal AsOfJoinSpec =
    {
        RightLf: LazyFrameHandle
        LeftOn: ExprHandle array
        RightOn: ExprHandle array
        LeftBy: ExprHandle array option
        RightBy: ExprHandle array option
        Strategy: PlAsofStrategy
        ToleranceStr: string option
        ToleranceInt: int64 option
        ToleranceFloat: double option
        AllowEq: bool
        CheckSorted: bool
        Suffix: string option
        Validation: PlJoinValidation
        Coalesce: PlJoinCoalesce
        MaintainOrder: PlJoinMaintainOrder
        JoinSide: PlJoinSide
        NullsEqual: bool
        SliceOffset: int64 option
        SliceLen: uint64
    }

/// Specification for native GroupBy aggregation pushdown
type internal GroupBySpec =
    {
        Keys: ExprHandle array
        Aggs: ExprHandle array
        Having: ExprHandle option
        MaintainOrder: bool
    }

type internal UniqueSpec =
    {
        SubsetCols: string array option
        Keep: PlUniqueKeepStrategy
        MaintainOrder: bool
    }

/// Specification for vertical concatenation pushdown
type internal ConcatSpec =
    {
        OtherLf: LazyFrameHandle
        Prepend: bool // false: current @ other (Append), true: other @ current (Prepend)
    }

/// Uniform contract to expose internal LazyFrameHandle without leaking generic parameters
type internal IPolarsPlanSource =
    /// Compiles the queryable pipeline stages and returns the final optimized LazyFrameHandle
    abstract member GetCompiledLazyFrameHandle: unit -> LazyFrameHandle

/// Abstract representation of pushdown operations in the LINQ execution pipeline
[<RequireQualifiedAccess>]
type internal QueryOp =
    | Filter of ExprHandle
    | Slice of offset: int64 * length: uint32
    | SkipLast of count: uint32
    | Sort of SortSpec list
    | Join of JoinSpec
    | JoinAsOf of AsOfJoinSpec
    | GroupBy of GroupBySpec
    | Unique of UniqueSpec
    | Concat of ConcatSpec
    | HorizontalConcat of otherLfs: LazyFrameHandle array
    | Explode of ExplodeSpec
    | Unpivot of UnpivotSpec
    | Rename of RenameSpec
    | Select of ExprHandle array
    | WithColumns of ExprHandle array
    | WithRowIndex of name: string * offset: uint32 option
    | Reverse
    | SelectPassthrough

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
