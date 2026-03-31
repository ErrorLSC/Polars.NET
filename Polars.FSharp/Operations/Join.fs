namespace Polars.FSharp
open Polars.NET.Core

[<AutoOpen>]
module JoinOps = 
    open Polars.NET.Core.Helpers
    type LazyFrame with
        /// <summary>
        /// Join with another LazyFrame.
        /// </summary>
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
       /// <summary>
        /// JoinAsOf with string tolerance (e.g., "2d", "1h").
        /// </summary>
        member internal this.JoinAsOfInternal(other: LazyFrame, 
                            leftOn: Expr, 
                            rightOn: Expr, 
                            // --- Optional Parameters ---
                            ?byLeft: Expr list, 
                            ?byRight: Expr list, 
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
            let toHandleArr (exprs: Expr list option) =
                match exprs with
                | Some es -> es |> List.map (fun e -> e.CloneHandle()) |> List.toArray
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
                [| lOn |], [| rOn |], // Wrapper expects arrays
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
        /// Join with tolerance as string (e.g. "2h", "10s").
        /// </summary>
        member this.JoinAsOf(other: LazyFrame, leftOn: Expr, rightOn: Expr, tolerance: string, 
                            ?strategy: AsofStrategy, ?byLeft: Expr list, ?byRight: Expr list) =
            this.JoinAsOfInternal(
                other, leftOn, rightOn, 
                tolerance = tolerance, // String
                ?strategy = strategy, ?byLeft = byLeft, ?byRight = byRight
            )

        /// <summary>
        /// Join with tolerance as TimeSpan.
        /// </summary>
        member this.JoinAsOf(other: LazyFrame, leftOn: Expr, rightOn: Expr, tolerance: System.TimeSpan, 
                            ?strategy: AsofStrategy, ?byLeft: Expr list, ?byRight: Expr list) =
            let tolStr = DurationFormatter.ToPolarsString tolerance
            this.JoinAsOfInternal(
                other, leftOn, rightOn, 
                tolerance = tolStr, // Converted String
                ?strategy = strategy, ?byLeft = byLeft, ?byRight = byRight
            )

        /// <summary>
        /// Join with tolerance as integer (e.g. timestamp or simple counter).
        /// </summary>
        member this.JoinAsOf(other: LazyFrame, leftOn: Expr, rightOn: Expr, tolerance: int64, 
                            ?strategy: AsofStrategy, ?byLeft: Expr list, ?byRight: Expr list) =
            this.JoinAsOfInternal(
                other, leftOn, rightOn, 
                toleranceInt = tolerance, // Int64
                ?strategy = strategy, ?byLeft = byLeft, ?byRight = byRight
            )

        /// <summary>
        /// Join with tolerance as float.
        /// </summary>
        member this.JoinAsOf(other: LazyFrame, leftOn: Expr, rightOn: Expr, tolerance: float, 
                            ?strategy: AsofStrategy, ?byLeft: Expr list, ?byRight: Expr list) =
            this.JoinAsOfInternal(
                other, leftOn, rightOn, 
                toleranceFloat = tolerance, // Float
                ?strategy = strategy, ?byLeft = byLeft, ?byRight = byRight
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
        member internal this.JoinAsOfInternal(other: DataFrame, 
                            leftOn: Expr, 
                            rightOn: Expr, 
                            // --- Optional Parameters ---
                            ?byLeft: Expr list, 
                            ?byRight: Expr list, 
                            ?strategy: AsofStrategy, 
                            ?tolerance: string,      // String
                            ?toleranceInt: int64,    // Int
                            ?toleranceFloat: float,  // Float
                            ?allowEq: bool,
                            ?checkSorted: bool,
                            ?suffix: string,
                            ?validation: JoinValidation,
                            ?coalesce: JoinCoalesce,
                            ?maintainOrder: JoinMaintainOrder,
                            ?nullsEqual: bool,
                            ?sliceOffset: int64,
                            ?sliceLen: uint64) : DataFrame =
            
            // 1. Convert to Lazy
            let lfSelf = this.Lazy()
            let lfOther = other.Lazy()

            // 2. Delegate to LazyFrame.JoinAsOfInternal
            // F# allows passing optional arguments directly via ?arg=val
            let resLf = lfSelf.JoinAsOfInternal(
                lfOther, leftOn, rightOn,
                ?byLeft = byLeft, 
                ?byRight = byRight, 
                ?strategy = strategy, 
                ?tolerance = tolerance, 
                ?toleranceInt = toleranceInt, 
                ?toleranceFloat = toleranceFloat, 
                ?allowEq = allowEq, 
                ?checkSorted = checkSorted, 
                ?suffix = suffix, 
                ?validation = validation, 
                ?coalesce = coalesce, 
                ?maintainOrder = maintainOrder, 
                ?nullsEqual = nullsEqual, 
                ?sliceOffset = sliceOffset, 
                ?sliceLen = sliceLen
            )

            // 3. Collect back to DataFrame
            resLf.Collect()
        // 1. String Tolerance
        /// <summary>
        /// Join with tolerance as string (e.g. "2h", "10s").
        /// </summary>
        member this.JoinAsOf(other: DataFrame, leftOn: Expr, rightOn: Expr, tolerance: string, 
                            ?strategy: AsofStrategy, ?byLeft: Expr list, ?byRight: Expr list) =
            this.JoinAsOfInternal(
                other, leftOn, rightOn, 
                tolerance = tolerance, 
                ?strategy = strategy, ?byLeft = byLeft, ?byRight = byRight
            )

        // 2. TimeSpan Tolerance
        /// <summary>
        /// Join with tolerance as TimeSpan.
        /// </summary>
        member this.JoinAsOf(other: DataFrame, leftOn: Expr, rightOn: Expr, tolerance: System.TimeSpan, 
                            ?strategy: AsofStrategy, ?byLeft: Expr list, ?byRight: Expr list) =
            let tolStr = DurationFormatter.ToPolarsString tolerance
            this.JoinAsOfInternal(
                other, leftOn, rightOn, 
                tolerance = tolStr, 
                ?strategy = strategy, ?byLeft = byLeft, ?byRight = byRight
            )

        // 3. Int64 Tolerance
        /// <summary>
        /// Join with tolerance as integer (e.g. timestamp or simple counter).
        /// </summary>
        member this.JoinAsOf(other: DataFrame, leftOn: Expr, rightOn: Expr, tolerance: int64, 
                            ?strategy: AsofStrategy, ?byLeft: Expr list, ?byRight: Expr list) =
            this.JoinAsOfInternal(
                other, leftOn, rightOn, 
                toleranceInt = tolerance, 
                ?strategy = strategy, ?byLeft = byLeft, ?byRight = byRight
            )

        // 4. Float Tolerance
        /// <summary>
        /// Join with tolerance as float.
        /// </summary>
        member this.JoinAsOf(other: DataFrame, leftOn: Expr, rightOn: Expr, tolerance: float, 
                            ?strategy: AsofStrategy, ?byLeft: Expr list, ?byRight: Expr list) =
            this.JoinAsOfInternal(
                other, leftOn, rightOn, 
                toleranceFloat = tolerance, 
                ?strategy = strategy, ?byLeft = byLeft, ?byRight = byRight
            )