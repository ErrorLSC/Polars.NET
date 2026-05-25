namespace Polars.FSharp
open Polars.NET.Core

[<RequireQualifiedAccess>]
type Tolerance =
    | String of string
    | TimeSpan of System.TimeSpan
    | Integer of int64
    | Float of float
[<AutoOpen>]
module JoinOps = 
    open Polars.NET.Core.Helpers
    type LazyFrame with
        /// <summary>
        /// Join with another LazyFrame using column names.
        /// </summary>
        /// <param name="other">The right DataFrame to join with.</param>
        /// <param name="leftOn">Column names in the left DataFrame to join on.</param>
        /// <param name="rightOn">Column names in the right DataFrame to join on.</param>
        /// <param name="how">Type of join (Inner, Left, Outer, Cross, etc.). Default is Inner.</param>
        /// <param name="suffix">Suffix to append to columns with same name in right DataFrame. Default "_right".</param>
        /// <param name="validation">Check if join keys are unique.</param>
        /// <param name="coalesce">How to coalesce the join keys.</param>
        /// <param name="maintainOrder">How to maintain the order of the join.</param>
        /// <param name="joinSide">Specifies the strategy for the hash join build side.</param>
        /// <param name="nullsEqual">Consider nulls as equal.</param>
        /// <param name="sliceOffset">Slice the result starting at this offset.</param>
        /// <param name="sliceLen">Length of the slice.</param>
        member this.Join(other: LazyFrame, 
                        leftOn: Expr seq, 
                        rightOn: Expr seq, 
                        how: JoinType,
                        // --- New Optional Parameters ---
                        ?suffix: string,
                        ?validation: JoinValidation,
                        ?coalesce: JoinCoalesce,
                        ?maintainOrder: JoinMaintainOrder,
                        ?joinSide: JoinSide,
                        ?nullsEqual: bool,
                        ?sliceOffset: int64,
                        ?sliceLen: uint64) : LazyFrame =

            let lOnArr = leftOn |> Seq.map (fun e -> e.CloneHandle()) |> Seq.toArray
            let rOnArr = rightOn |> Seq.map (fun e -> e.CloneHandle()) |> Seq.toArray
            
            let lHandle = this.CloneHandle()
            let rHandle = other.CloneHandle()

            // Handle Defaults
            let suff = defaultArg suffix null
            let valid = defaultArg validation JoinValidation.ManyToMany
            let coal = defaultArg coalesce JoinCoalesce.JoinSpecific
            let mo = defaultArg maintainOrder JoinMaintainOrder.NotMaintainOrder
            let ne = defaultArg nullsEqual false
            let js = defaultArg joinSide JoinSide.LetPolarsDecide
            
            let so = Option.toNullable sliceOffset
            let sl = defaultArg sliceLen 0UL

            let newHandle = PolarsWrapper.Join(
                lHandle, 
                rHandle, 
                lOnArr, 
                rOnArr, 
                how.ToNative(),
                suff,
                valid.ToNative(),
                coal.ToNative(),
                mo.ToNative(),
                js.ToNative(),
                ne,
                so,
                sl
            )
            
            new LazyFrame(newHandle)
        member this.Join(other, 
                        on: Expr seq, 
                        how: JoinType,
                        // --- New Optional Parameters ---
                        ?suffix: string,
                        ?validation: JoinValidation,
                        ?coalesce: JoinCoalesce,
                        ?maintainOrder: JoinMaintainOrder,
                        ?joinSide: JoinSide,
                        ?nullsEqual: bool,
                        ?sliceOffset: int64,
                        ?sliceLen: uint64) =
            this.Join(other,leftOn=on,rightOn=on,how = how,
                ?suffix=suffix,?validation=validation,?coalesce=coalesce,?maintainOrder=maintainOrder,
                ?joinSide=joinSide,?nullsEqual=nullsEqual,?sliceOffset=sliceOffset,?sliceLen=sliceLen)
        /// <summary>
        /// JoinAsOf with string tolerance (e.g., "2d", "1h").
        /// </summary>
        member internal this.JoinAsOfInternal(other: LazyFrame, 
                            leftOn: Expr, 
                            rightOn: Expr, 
                            // --- Optional Parameters ---
                            ?byLeft: Expr seq, 
                            ?byRight: Expr seq, 
                            ?strategy: AsofStrategy, 
                            ?tolerance: string,      // String (e.g. "2h")
                            ?toleranceInt: int64,    // Int (e.g. timestamp)
                            ?toleranceFloat: float,  // Float
                            ?allowEq: bool,
                            ?checkSorted: bool,
                            ?suffix: string,
                            ?validation: JoinValidation,
                            ?coalesce: JoinCoalesce,
                            ?maintainOrder: JoinMaintainOrder,
                            ?joinSide: JoinSide,
                            ?nullsEqual: bool,
                            ?sliceOffset: int64,
                            ?sliceLen: uint64) : LazyFrame =
            
            // 1. Clone Handles (Mandatory)
            let lClone = this.CloneHandle()
            let rClone = other.CloneHandle()
            let lOn = leftOn.CloneHandle()
            let rOn = rightOn.CloneHandle()
            
            // 2. Handle 'By' keys (Optional List -> Handle Array)
            let toHandleArr (exprs: Expr seq option) =
                match exprs with
                | Some es -> es |> Seq.map (fun e -> e.CloneHandle()) |> Seq.toArray
                | None -> [||]

            let lByArr = toHandleArr byLeft
            let rByArr = toHandleArr byRight

            // 3. Handle Enums & Defaults
            let strat = defaultArg strategy AsofStrategy.Backward
            let valid = defaultArg validation JoinValidation.ManyToMany
            let coal = defaultArg coalesce JoinCoalesce.JoinSpecific
            let mo = defaultArg maintainOrder JoinMaintainOrder.NotMaintainOrder
            let js = defaultArg joinSide JoinSide.LetPolarsDecide
            
            // 4. Handle Bools & Strings
            let ae = defaultArg allowEq true
            let cs = defaultArg checkSorted true
            let ne = defaultArg nullsEqual false
            let suff = defaultArg suffix null // Rust default is "_right"
            
            // 5. Handle Nullables (Tolerances & Slice)
            // Option.toObj converts string option -> string (null if None)
            let tolStr = Option.toObj tolerance 
            // Option.toNullable converts int option -> Nullable<int>
            let tolInt = Option.toNullable toleranceInt
            let tolFloat = Option.toNullable toleranceFloat
            let sOff = Option.toNullable sliceOffset
            let sLen = defaultArg sliceLen 0UL

            // 6. Call Wrapper
            let h = PolarsWrapper.JoinAsOf(
                lClone, rClone, 
                [| lOn |], [| rOn |],
                lByArr, rByArr,
                strat.ToNative(),     // Enum -> PlAsofStrategy
                tolStr,
                tolInt,
                tolFloat,
                ae,
                cs,
                suff,
                valid.ToNative(),
                coal.ToNative(),
                mo.ToNative(),
                js.ToNative(),
                ne,
                sOff,
                sLen
            )
            
            new LazyFrame(h)
        /// <summary>
        /// Perform an As-of join (also known as a time-series join).
        /// <para>
        /// This is similar to a left join except that we match on nearest key rather than equal keys.
        /// The join keys must be sorted.
        /// </para>
        /// </summary>
        /// <param name="other">The right LazyFrame to join with.</param>
        /// <param name="leftOn">Join key of the left LazyFrame. Must be sorted.</param>
        /// <param name="rightOn">Join key of the right LazyFrame. Must be sorted.</param>
        /// <param name="tolerance">
        /// Tolerance as a time duration string (e.g., "2h", "10s", "1d"), int or double or TimeSpan. 
        /// Matches that are further away than this duration are discarded.
        /// </param>
        /// <param name="strategy">
        /// The strategy to determine which value is "nearest" (Backward, Forward, or Nearest).
        /// Defaults to <see cref="AsofStrategy.Backward"/>.
        /// </param>
        /// <param name="leftBy">
        /// Columns to match exactly (equivalence join) before performing the as-of join. 
        /// Useful for joining separate time-series per group (e.g., by "Symbol").
        /// </param>
        /// <param name="rightBy">
        /// Columns to match exactly in the right DataFrame.
        /// </param>
        /// <param name="allowEq">
        /// If true, allow exact matches to be included in the result. 
        /// If false, a match must be strictly unequal (e.g. less than for Backward strategy) to the key.
        /// </param>
        /// <param name="checkSorted">
        /// Check if the join keys are sorted. 
        /// If false, the user must ensure keys are sorted; otherwise results are undefined (but execution is faster).
        /// </param>
        /// <param name="suffix">Suffix to append to columns with name conflicts. Defaults to "_right".</param>
        /// <param name="validation">Check if join keys are unique (mostly relevant for the 'by' columns).</param>
        /// <param name="coalesce">How to coalesce the join keys.</param>
        /// <param name="maintainOrder">How to maintain the order of the join.</param>
        /// <param name="joinSide">pecifies the strategy for the hash join build side.</param>
        /// <param name="nullsEqual">Consider nulls as equal.</param>
        /// <param name="sliceOffset">Slice the result starting at this offset (optimization).</param>
        /// <param name="sliceLen">Length of the slice to keep.</param>
        member this.JoinAsOf(other: LazyFrame, leftOn: Expr, rightOn: Expr, tolerance: Tolerance, 
                            ?strategy: AsofStrategy, ?byLeft: Expr seq, ?byRight: Expr seq,
                            ?allowEq: bool,?checkSorted: bool,?suffix: string,?validation: JoinValidation,
                            ?coalesce: JoinCoalesce,?maintainOrder: JoinMaintainOrder,
                            ?joinSide: JoinSide,?nullsEqual: bool,
                            ?sliceOffset: int64,?sliceLen: uint64) =
            match tolerance with
            | Tolerance.String s ->
                this.JoinAsOfInternal(
                    other, leftOn, rightOn, 
                    tolerance = s , // String
                    ?strategy = strategy, ?byLeft = byLeft, ?byRight = byRight,
                    ?allowEq = allowEq , ?checkSorted = checkSorted, ?suffix=suffix,?validation=validation,
                    ?coalesce = coalesce, ?maintainOrder=maintainOrder,?joinSide=joinSide,
                    ?nullsEqual = nullsEqual, ?sliceOffset = sliceOffset , ?sliceLen = sliceLen 
                )
            | Tolerance.TimeSpan ts ->
                let tolStr = DurationFormatter.ToPolarsString ts
                this.JoinAsOfInternal(
                    other, leftOn, rightOn, 
                    tolerance = tolStr, // Converted String
                    ?strategy = strategy, ?byLeft = byLeft, ?byRight = byRight,
                    ?allowEq = allowEq , ?checkSorted = checkSorted, ?suffix=suffix,?validation=validation,
                    ?coalesce = coalesce, ?maintainOrder=maintainOrder,?joinSide=joinSide,
                    ?nullsEqual = nullsEqual, ?sliceOffset = sliceOffset , ?sliceLen = sliceLen 
                )
            | Tolerance.Integer it ->
                this.JoinAsOfInternal(
                    other, leftOn, rightOn, 
                    toleranceInt = it, // Int64
                    ?strategy = strategy, ?byLeft = byLeft, ?byRight = byRight,
                    ?allowEq = allowEq , ?checkSorted = checkSorted, ?suffix=suffix,?validation=validation,
                    ?coalesce = coalesce, ?maintainOrder=maintainOrder,?joinSide=joinSide,
                    ?nullsEqual = nullsEqual, ?sliceOffset = sliceOffset , ?sliceLen = sliceLen 
                ) 
            | Tolerance.Float fl -> 
                this.JoinAsOfInternal(
                    other, leftOn, rightOn, 
                    toleranceFloat = fl, // Float
                    ?strategy = strategy, ?byLeft = byLeft, ?byRight = byRight,
                    ?allowEq = allowEq , ?checkSorted = checkSorted, ?suffix=suffix,?validation=validation,
                    ?coalesce = coalesce, ?maintainOrder=maintainOrder,?joinSide=joinSide,
                    ?nullsEqual = nullsEqual, ?sliceOffset = sliceOffset , ?sliceLen = sliceLen 
                )
    type DataFrame with
        /// <summary> Join with another DataFrame. </summary>
        member this.Join (other: DataFrame,
                        leftOn: seq<Expr>,
                        rightOn: seq<Expr>,
                        how: JoinType,
                        // --- New Optional Parameters ---
                        ?suffix: string,
                        ?validation: JoinValidation,
                        ?coalesce: JoinCoalesce,
                        ?maintainOrder: JoinMaintainOrder,
                        ?joinSide: JoinSide,
                        ?nullsEqual: bool,
                        ?sliceOffset: int64,
                        ?sliceLen: uint64) : DataFrame =
            
            // Handle Defaults
            let suff = defaultArg suffix null // Pass null to let Rust use default ("_right")
            let valid = defaultArg validation JoinValidation.ManyToMany
            let coal = defaultArg coalesce JoinCoalesce.JoinSpecific
            let mo = defaultArg maintainOrder JoinMaintainOrder.NotMaintainOrder
            let js = defaultArg joinSide JoinSide.LetPolarsDecide
            let ne = defaultArg nullsEqual false
            
            // Slice logic
            let so = Option.toNullable sliceOffset
            let sl = defaultArg sliceLen 0UL

            let lf = this.Lazy().Join(
                other.Lazy(), 
                leftOn, 
                rightOn, 
                how,
                suff,
                valid,
                coal,
                mo,
                js,
                ne,
                ?sliceOffset=sliceOffset,
                ?sliceLen=sliceLen
            )
            lf.Collect()
        member this.Join(other, 
                        on: Expr seq, 
                        how: JoinType,
                        // --- New Optional Parameters ---
                        ?suffix: string,
                        ?validation: JoinValidation,
                        ?coalesce: JoinCoalesce,
                        ?maintainOrder: JoinMaintainOrder,
                        ?joinSide: JoinSide,
                        ?nullsEqual: bool,
                        ?sliceOffset: int64,
                        ?sliceLen: uint64) =
            this.Join(other,leftOn=on,rightOn=on,how = how,
                ?suffix=suffix,?validation=validation,?coalesce=coalesce,?maintainOrder=maintainOrder,
                ?joinSide=joinSide,?nullsEqual=nullsEqual,?sliceOffset=sliceOffset,?sliceLen=sliceLen)
        member this.JoinAsOf(other: DataFrame, 
                            leftOn: Expr, 
                            rightOn: Expr, 
                            tolerance: Tolerance,
                            // --- Optional Parameters ---
                            ?byLeft: Expr seq, 
                            ?byRight: Expr seq, 
                            ?strategy: AsofStrategy, 
                            ?allowEq: bool,
                            ?checkSorted: bool,
                            ?suffix: string,
                            ?validation: JoinValidation,
                            ?coalesce: JoinCoalesce,
                            ?maintainOrder: JoinMaintainOrder,
                            ?joinSide: JoinSide,
                            ?nullsEqual: bool,
                            ?sliceOffset: int64,
                            ?sliceLen: uint64) : DataFrame =
            
            let lfSelf = this.Lazy()
            let lfOther = other.Lazy()
            lfSelf.JoinAsOf(lfOther,leftOn,rightOn,tolerance,
                    ?strategy = strategy, ?byLeft = byLeft, ?byRight = byRight,
                    ?allowEq = allowEq , ?checkSorted = checkSorted, ?suffix=suffix,?validation=validation,
                    ?coalesce = coalesce, ?maintainOrder=maintainOrder,?joinSide=joinSide,
                    ?nullsEqual = nullsEqual, ?sliceOffset = sliceOffset , ?sliceLen = sliceLen 
                ).Collect()

