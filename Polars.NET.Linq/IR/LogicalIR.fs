namespace Polars.NET.Linq.IR

open System
open Polars.NET.Core

/// Strongly-typed literal representation eliminating heap boxing and type ambiguity
[<RequireQualifiedAccess>]
type LiteralValue =
    | Null of PlDataType
    | Bool of bool
    | Int8 of sbyte
    | Int16 of int16
    | Int32 of int
    | Int64 of int64
    | UInt8 of byte
    | UInt16 of uint16
    | UInt32 of uint32
    | UInt64 of uint64
    | Float32 of float32
    | Float64 of double
    | Decimal of decimal
    | String of string
    | Date of DateOnly
    | DateTime of DateTime
    | DateTimeOffset of DateTimeOffset
    | TimeSpan of TimeSpan
    | TimeOnly of TimeOnly
    with
        /// Returns the exact Polars data type for this literal value.
        member this.DataType: PlDataType =
            match this with
            | Null dt -> dt
            | Bool _ -> PlDataType.Boolean
            | Int8 _ -> PlDataType.Int8
            | Int16 _ -> PlDataType.Int16
            | Int32 _ -> PlDataType.Int32
            | Int64 _ -> PlDataType.Int64
            | UInt8 _ -> PlDataType.UInt8
            | UInt16 _ -> PlDataType.UInt16
            | UInt32 _ -> PlDataType.UInt32
            | UInt64 _ -> PlDataType.UInt64
            | Float32 _ -> PlDataType.Float32
            | Float64 _ -> PlDataType.Float64
            | Decimal _ -> PlDataType.Decimal
            | String _ -> PlDataType.String
            | Date _ -> PlDataType.Date
            | DateTime _ -> PlDataType.Datetime
            | DateTimeOffset _ -> PlDataType.Datetime
            | TimeSpan _ -> PlDataType.Duration
            | TimeOnly _ -> PlDataType.Time

        /// Extracts the boxed object representation on demand for FFI boundaries.
        member this.BoxedValue: obj =
            match this with
            | Null _ -> null
            | Bool b -> box b
            | Int8 i -> box i
            | Int16 i -> box i
            | Int32 i -> box i
            | Int64 i -> box i
            | UInt8 u -> box u
            | UInt16 u -> box u
            | UInt32 u -> box u
            | UInt64 u -> box u
            | Float32 f -> box f
            | Float64 f -> box f
            | Decimal m -> box m
            | String s -> box s
            | Date d -> box d
            | DateTime dt -> box dt
            | DateTimeOffset dto -> box dto
            | TimeSpan ts -> box ts
            | TimeOnly to' -> box to'

        /// Converts a dynamic runtime object into a strongly-typed LiteralValue.
        static member OfObject(raw: obj) : LiteralValue =
            match raw with
            | null -> LiteralValue.Null PlDataType.Null
            | :? bool as b -> LiteralValue.Bool b
            | :? int as i -> LiteralValue.Int32 i
            | :? int64 as i -> LiteralValue.Int64 i
            | :? double as d -> LiteralValue.Float64 d
            | :? float32 as f -> LiteralValue.Float32 f
            | :? string as s -> LiteralValue.String s
            | :? decimal as m -> LiteralValue.Decimal m
            | :? sbyte as b -> LiteralValue.Int8 b
            | :? int16 as s -> LiteralValue.Int16 s
            | :? byte as b -> LiteralValue.UInt8 b
            | :? uint16 as u -> LiteralValue.UInt16 u
            | :? uint32 as u -> LiteralValue.UInt32 u
            | :? uint64 as u -> LiteralValue.UInt64 u
            | :? DateOnly as d -> LiteralValue.Date d
            | :? DateTime as dt -> LiteralValue.DateTime dt
            | :? DateTimeOffset as dto -> LiteralValue.DateTimeOffset dto
            | :? TimeSpan as ts -> LiteralValue.TimeSpan ts
            | :? TimeOnly as to' -> LiteralValue.TimeOnly to'
            | other -> failwithf "Unsupported literal value type: %s" (other.GetType().FullName)

/// Binary operations supported in ScalarExpr
[<RequireQualifiedAccess>]
type BinaryOp =
    // Comparison
    | Eq
    | NotEq
    | Lt
    | LtEq
    | Gt
    | GtEq
    // Logical
    | And
    | Or
    | Xor
    // Arithmetic
    | Add
    | Subtract
    | Multiply
    | Divide
    | Modulo
    | FloorDivide
    | Power
    // String & Pattern
    | StringConcat
    | Like
    | Contains
    | StartsWith
    | EndsWith
    | RegexMatch

/// Unary operations supported in ScalarExpr
[<RequireQualifiedAccess>]
type UnaryOp =
    | Not
    | Negate
    | IsNull
    | IsNotNull
    | Abs

/// Aggregation kinds supported in ScalarExpr and GroupBy/Window nodes
[<RequireQualifiedAccess>]
type AggKind =
    | Sum
    | Mean
    | Min
    | Max
    | Count
    | First
    | Last
    | Median
    | Std
    | Var
    | Quantile of prob: double
    | List
    | Len

/// Single field representation within a LogicalSchema
type LogicalField =
    {
        Name: string
        DataType: PlDataType
        Nullable: bool
    }

/// Relational schema description for logical plan nodes
type LogicalSchema =
    {
        Fields: LogicalField list
    }
    with
        static member Empty = { Fields = [] }

        static member OfNames(names: string seq) =
            {
                Fields =
                    names
                    |> Seq.map (fun n ->
                        {
                            Name = n
                            DataType = PlDataType.Unknown
                            Nullable = true
                        })
                    |> Seq.toList
            }

        static member OfFields(fields: (string * PlDataType) seq) =
            {
                Fields =
                    fields
                    |> Seq.map (fun (n, dt) ->
                        {
                            Name = n
                            DataType = dt
                            Nullable = true
                        })
                    |> Seq.toList
            }

        member this.ColumnNames: string list = this.Fields |> List.map (fun f -> f.Name)

        member this.FieldCount: int = this.Fields.Length

        member this.TryFindField(name: string) : LogicalField option =
            this.Fields |> List.tryFind (fun f -> f.Name = name)

        member this.Contains(name: string) : bool =
            this.Fields |> List.exists (fun f -> f.Name = name)

        member this.AddField(field: LogicalField) : LogicalSchema =
            {
                Fields = this.Fields @ [ field ]
            }

        member this.Merge(other: LogicalSchema) : LogicalSchema =
            let existing = this.ColumnNames |> Set.ofList

            let uniqueOther =
                other.Fields |> List.filter (fun f -> not (existing.Contains f.Name))

            {
                Fields = this.Fields @ uniqueOther
            }

/// Pure symbolic scalar and column expression tree
type [<RequireQualifiedAccess>] ScalarExpr =
    | Column of name: string
    | Literal of value: LiteralValue
    | Binary of op: BinaryOp * left: ScalarExpr * right: ScalarExpr
    | Unary of op: UnaryOp * expr: ScalarExpr
    | Cast of expr: ScalarExpr * targetType: PlDataType * isStrict: bool
    | Function of name: string * args: ScalarExpr list * returnType: PlDataType
    | Aggregation of kind: AggKind * expr: ScalarExpr option * isDistinct: bool
    | Window of expr: ScalarExpr * partitionBy: ScalarExpr list * orderBy: SortOrder list
    | Conditional of test: ScalarExpr * ifTrue: ScalarExpr * ifFalse: ScalarExpr
    | Coalesce of exprs: ScalarExpr list
    | ItemAccess of target: ScalarExpr * index: ScalarExpr
    | Slice of target: ScalarExpr * offset: int64 * length: uint32
    | Subquery of plan: LogicalPlan * kind: SubqueryKind
    | Alias of expr: ScalarExpr * alias: string
    | Opaque of handle: ExprHandle * referencedCols: string list * description: string

/// Subquery semantics when a scalar expression embeds a logical plan
and [<RequireQualifiedAccess>] SubqueryKind =
    | ScalarScalar
    | Exists of negated: bool
    | InSubquery of target: ScalarExpr * negated: bool

/// Ordering specification for sorting and window operations
and SortOrder =
    {
        Expr: ScalarExpr
        Descending: bool
        NullsLast: bool
    }

/// Input source for Leaf Scan nodes
and [<RequireQualifiedAccess>] LogicalPlanSource =
    | LazyFrame of LazyFrameHandle
    | SubqueryRef of subqueryId: string
    | InMemoryTable of name: string * rows: obj list
    | Empty

/// Specification for Logical AsOf join operations
and LogicalAsOfJoinSpec =
    {
        LeftOn: ScalarExpr
        RightOn: ScalarExpr
        LeftBy: ScalarExpr list
        RightBy: ScalarExpr list
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
    }

/// Pure relational algebra logical plan tree
and [<RequireQualifiedAccess>] LogicalPlan =
    | Scan of source: LogicalPlanSource * schema: LogicalSchema
    | Filter of input: LogicalPlan * predicate: ScalarExpr
    | Project of input: LogicalPlan * projections: (string * ScalarExpr) list
    | WithColumns of input: LogicalPlan * columns: (string * ScalarExpr) list
    | Join of
        left: LogicalPlan *
        right: LogicalPlan *
        leftOn: ScalarExpr list *
        rightOn: ScalarExpr list *
        how: PlJoinType *
        suffix: string option *
        coalesce: PlJoinCoalesce *
        nullsEqual: bool
    | JoinAsOf of left: LogicalPlan * right: LogicalPlan * spec: LogicalAsOfJoinSpec
    | GroupBy of
        input: LogicalPlan *
        keys: ScalarExpr list *
        aggs: (string * ScalarExpr) list *
        having: ScalarExpr option *
        maintainOrder: bool
    | Sort of input: LogicalPlan * orderings: SortOrder list
    | Limit of input: LogicalPlan * offset: int64 * count: uint32
    | Distinct of input: LogicalPlan * subset: string list option * keep: PlUniqueKeepStrategy
    | Explode of input: LogicalPlan * columns: string list * emptyAsNull: bool * keepNulls: bool
    | Unpivot of
        input: LogicalPlan *
        index: string list *
        on: string list option *
        variableName: string *
        valueName: string
    | Union of left: LogicalPlan * right: LogicalPlan * isAll: bool
    | WithRowIndex of input: LogicalPlan * name: string * offset: uint32 option
    | Rename of input: LogicalPlan * existingNames: string list * newNames: string list
    | Reverse of input: LogicalPlan
    | SkipLast of input: LogicalPlan * count: uint32
    | HorizontalConcat of inputs: LogicalPlan list
    | Empty of schema: LogicalSchema
