namespace Polars.FSharp

open System
open Polars.NET.Core

[<AutoOpen>]
module ManipulateOps = 
    /// ========================
    /// WithColumns
    /// ========================
    type LazyFrame with
        /// <summary>
        /// Add or replace a single column in the LazyFrame.
        /// </summary>
        /// <param name="expr">The expression defining the new column.</param>
        member this.WithColumns (expr: Expr) : LazyFrame =
            let lfClone = this.CloneHandle()
            let exprClone = expr.CloneHandle()
            let handles = [| exprClone |] 
            let h = PolarsWrapper.LazyWithColumns(lfClone, handles)
            new LazyFrame(h)
        /// <summary>
        /// Add or replace multiple columns in the LazyFrame.
        /// </summary>
        /// <param name="exprs">List of expressions defining the new columns.</param>
        member this.WithColumns (exprs: seq<Expr>) : LazyFrame =
            let lfClone = this.CloneHandle()
            let handles = exprs |> Seq.map (fun e -> e.CloneHandle()) |> Seq.toArray
            let h = PolarsWrapper.LazyWithColumns(lfClone, handles)
            new LazyFrame(h)
        /// <summary>
        /// Add or replace columns using generic column expressions (Expr or Selectors).
        /// </summary>
        member this.WithColumns (columns:seq<#IColumnExpr>) =
            let exprs = 
                columns 
                |> Seq.collect (fun x -> x.ToExprs()) 
                |> Seq.toList
            
            this.WithColumns exprs

    type DataFrame with
        /// <summary> Add or replace columns using expressions. </summary>
        member this.WithColumns (exprs:seq<Expr>) : DataFrame =
            this.Lazy().WithColumns(exprs).Collect()
        /// <summary> Add or replace columns using generic column expressions (Expr or Selectors). </summary>
        member this.WithColumns (columns:seq<#IColumnExpr>) =
            let exprs = 
                columns 
                |> Seq.collect (fun x -> x.ToExprs()) 
                |> Seq.toList
            
            this.WithColumns exprs
        member this.WithColumns (expr: Expr) : DataFrame =
            this.WithColumns [expr]
    /// ========================
    /// Cast
    /// ========================
    type LazyFrame with
        /// <summary>
        /// Cast LazyFrame column(s) to the specified dtype(s) using a dictionary mapping.
        /// </summary>
        member this.Cast(dtypes: seq<string * DataType>, ?strict: bool) =
            let strictArg = defaultArg strict true

            let castExprs =
                dtypes
                |> Seq.map (fun (colName, dtype) -> 
                    Expr.Col(colName).Cast(dtype, strictArg)
                )

            this.WithColumns castExprs

        /// <summary>
        /// Cast all columns in the LazyFrame to the specified dtype.
        /// </summary>
        member this.Cast(dtype: DataType, ?strict: bool) =
            let strictArg = defaultArg strict true
            
            let castAllExpr = Expr.Col("*").Cast(dtype, strictArg)
            
            this.Select castAllExpr
        /// <summary>
        /// Cast columns matching an Expr/Selector to a specific DataType.
        /// </summary>
        member this.Cast(expr: Expr, dtype: DataType, ?strict: bool) =
            let strictArg = defaultArg strict true
            this.WithColumns [|expr.Cast(dtype, strictArg)|]

        /// <summary>
        /// Cast multiple expressions/selectors to their target DataTypes.
        /// Example: lf.Cast([ (pl.cs.Numeric(), DataType.Float32); (col("Id"), DataType.Int32) ])
        /// </summary>
        member this.Cast(dtypes: seq<Expr * DataType>, ?strict: bool) =
            let strictArg = defaultArg strict true
            
            let castExprs = 
                dtypes 
                |> Seq.map (fun (expr, dt) -> expr.Cast(dt, strictArg))
                
            this.WithColumns castExprs

    /// ========================
    /// Concat
    /// ========================
    type LazyFrame with
        static member Concat  (lfs: seq<LazyFrame>,?how: ConcatType,?rechunk,?useParallel) : LazyFrame =
            let handles = lfs |> Seq.map (fun lf -> lf.CloneHandle()) |> Seq.toArray
            let ho = defaultArg how ConcatType.Vertical 
            let re = defaultArg rechunk false
            let pa = defaultArg useParallel true
            new LazyFrame(PolarsWrapper.LazyConcat(handles, ho.ToNative(), re, pa))
    type DataFrame with
        /// <summary>
        /// General Concat method.
        /// checkDuplicates is only used when how = ConcatType.Horizontal.
        /// </summary>
        static member internal Concat (dfs: seq<DataFrame>, how: ConcatType, ?checkDuplicates: bool,?strict: bool, ?unitLengthAsScalar: bool) : DataFrame =
            let handles = dfs |> Seq.map (fun df -> df.CloneHandle()) |> Seq.toArray
            
            let check = defaultArg checkDuplicates true
            let st = defaultArg strict true
            let uni = defaultArg unitLengthAsScalar false
            let h = PolarsWrapper.Concat(handles, how.ToNative(), check, st, uni)
            new DataFrame(h)

        /// <summary>
        /// Horizontal concatenation (Index alignment).
        /// </summary>
        /// <param name="strict">For Horizontal: if true, error on height mismatch.</param>
        /// <param name="unitLengthAsScalar">For Horizontal: if true, broadcast length-1 DataFrames to match height.</param>
        static member ConcatHorizontal (dfs: seq<DataFrame>, ?checkDuplicates: bool,?strict: bool, ?unitLengthAsScalar: bool) : DataFrame =
            DataFrame.Concat(dfs, ConcatType.Horizontal, ?checkDuplicates = checkDuplicates, ?strict=strict,?unitLengthAsScalar=unitLengthAsScalar)

        /// <summary>
        /// Vertical concatenation (Column alignment).
        /// </summary>
        static member ConcatVertical (dfs: seq<DataFrame>) : DataFrame =
            DataFrame.Concat(dfs, ConcatType.Vertical)

        /// <summary>
        /// Diagonal concatenation.
        /// </summary>
        static member ConcatDiagonal (dfs: seq<DataFrame>) : DataFrame =
            DataFrame.Concat(dfs, ConcatType.Diagonal)
    /// ========================
    /// Explode & Unnest
    /// ========================
    type LazyFrame with
        member this.Explode(selector: Selector,?emptyAsNull:bool,?keepNulls:bool) : LazyFrame =
            let lfClone = this.CloneHandle()
            let sh = selector.CloneHandle()
            let ean = defaultArg emptyAsNull true
            let kn = defaultArg keepNulls true
            new LazyFrame(PolarsWrapper.LazyExplode(lfClone, sh, ean, kn))

        member this.Explode(columns: seq<string>) =
            let names = Seq.toArray columns
            let h = PolarsWrapper.SelectorCols names
            let sel = new Selector(h)
            this.Explode sel

        member this.Explode(column: string) = 
            this.Explode [column]
           /// <summary>
        /// Decompose a struct column into multiple columns.
        /// </summary>
        member this.Unnest(selector: Selector,?separator: string) =
            let lfHandle = this.CloneHandle()
            
            let selHandle = selector.CloneHandle()

            let sep = defaultArg separator null

            let h = PolarsWrapper.LazyFrameUnnest(lfHandle, selHandle, sep)
            new LazyFrame(h)

        /// <summary>
        /// Helper: Unnest columns by name.
        /// </summary>
        member this.Unnest(columns: seq<string>,?separator: string) =
            let columnsArray = columns|> Seq.toArray
            let handle = PolarsWrapper.SelectorCols columnsArray
            let sel = new Selector(handle)
            this.Unnest (sel,?separator=separator)

        /// <summary>
        /// Helper: Unnest a single column by name.
        /// </summary>
        member this.Unnest(column: string, ?separator: string) =
            this.Unnest ([column], ?separator=separator)
    type DataFrame with
        /// <summary> 
        /// Explode list columns to rows using a Selector.
        /// </summary>
        member this.Explode(selector: Selector,?emptyAsNull:bool,?keepNulls:bool) : DataFrame =
            this.Lazy().Explode(selector,?emptyAsNull=emptyAsNull,?keepNulls=keepNulls).Collect()

        /// <summary> 
        /// Explode list columns to rows using column names.
        /// </summary>
        member this.Explode(columns: seq<string>,?emptyAsNull:bool,?keepNulls:bool) =
            let names = Seq.toArray columns
            let h = PolarsWrapper.SelectorCols names
            let sel = new Selector(h)
            this.Explode(sel,?emptyAsNull=emptyAsNull,?keepNulls=keepNulls) 

        /// <summary>Explode a single column by name. </summary>
        member this.Explode(column: string,?emptyAsNull:bool,?keepNulls:bool) =
            this.Explode([column],?emptyAsNull=emptyAsNull,?keepNulls=keepNulls)                          
        /// <summary> Decompose a struct column into multiple columns. </summary>
        member this.UnnestColumn(column: string, ?separator: string) : DataFrame =
            let cols = [| column |]
            let sep = defaultArg separator null
            let newHandle = PolarsWrapper.Unnest(this.Handle, cols, sep)
            new DataFrame(newHandle)
        /// <summary> Decompose multiple struct columns. </summary>
        member this.UnnestColumns(columns: seq<string>, ?separator: string) : DataFrame =
            let cArr = Seq.toArray columns
            let sep = defaultArg separator null
            let newHandle = PolarsWrapper.Unnest(this.Handle, cArr, sep)
            new DataFrame(newHandle) 
    /// ========================
    /// Slice
    /// ========================
    type LazyFrame with
        /// <summary>
        /// Slice the LazyFrame along the rows.
        /// If length is omitted, it slices to the end of the LazyFrame.
        /// </summary>
        member this.Slice(offset: int64, ?length: uint32) =
            let realLength = defaultArg length UInt32.MaxValue
            new LazyFrame(PolarsWrapper.LazySlice(this.CloneHandle(), offset, realLength))
        /// <summary>
        /// Get the first n rows.
        /// </summary>
        /// <param name="n">Number of rows to return.</param>
        member this.Limit(?n) =
            let n0 = defaultArg n 5u
            this.Slice(0,n0)
        member this.Tail(?n) =
            let n0 = defaultArg n 5u
            this.Slice(-(int64 n0),n0)
        /// <summary>
        /// Get the last row.
        /// </summary>
        member this.Last() = this.Tail(1u)
        member this.Head(?n) = this.Limit(?n=n)
        member this.First() = this.Head(1u)
    type DataFrame with
        /// <summary>
        /// Slice the DataFrame along the rows.
        /// Negative offsets count from the end of the DataFrame.
        /// If length is omitted, it slices to the end of the DataFrame.
        /// </summary>
        member this.Slice(offset: int64, ?length: uint64) =
            let absoluteOffset = 
                if offset < 0L then this.Height + offset
                else offset

            if absoluteOffset < 0L || absoluteOffset >= this.Height then
                new DataFrame(PolarsWrapper.Slice(this.Handle, absoluteOffset, 0UL))
            else
                let realLength = defaultArg length (uint64 (this.Height - absoluteOffset))
                new DataFrame(PolarsWrapper.Slice(this.Handle, absoluteOffset, realLength))
        /// <summary>
        /// Slice the DataFrame using F# slicing syntax (e.g., df.[start..finish]).
        /// Note: F# slicing bounds are inclusive at the end.
        /// </summary>
        member this.GetSlice(start: int option, finish: int option) =
            let height = this.Height 

            let s = 
                match start with
                | Some v when v < 0 -> max 0L (height + int64 v)
                | Some v -> min height (int64 v)
                | None -> 0L
                
            let f = 
                match finish with
                | Some v when v < 0 -> max 0L (height + int64 v + 1L)
                | Some v -> min height (int64 v + 1L) 
                | None -> height
                
            let length = max 0L (f - s)
            
            if length <= 0L then
                this.Slice(0L, 0UL)
            else
                this.Slice(s, uint64 length)
        /// <summary>
        /// Returns an iterator over slices of the DataFrame.
        /// </summary>
        /// <param name="nRows">The number of rows per slice. Defaults to 10,000.</param>
        member this.IterSlices(?nRows: int32) =
            let rowsPerSlice = defaultArg nRows 10_000
            
            if rowsPerSlice <= 0 then
                raise (ArgumentOutOfRangeException("nRows", "Number of rows per slice must be greater than zero."))
                
            let totalRows = this.Height
            let step = int64 rowsPerSlice

            Seq.unfold (fun offset ->
                if offset < totalRows then
                    let currentLength = uint64 (min step (totalRows - offset))
                    let slice = this.Slice(offset, currentLength)
                    Some(slice, offset + step)
                else
                    None
            ) 0L
    /// ========================
    /// Fill
    /// ========================            
    type LazyFrame with
        /// <summary>
        /// Fill null values in all columns with a specified Expression.
        /// </summary>
        member this.FillNull(fillValue:Expr) = 
            this.WithColumns(Expr.All().FillNull(fillValue))
        /// <summary>
        /// Fill NaN values in floating point columns with a specified Expression.
        /// </summary>
        member this.FillNan(fillValue:Expr) = 
            this.WithColumns(Selector.Float().ToExpr().FillNan(fillValue))
    type DataFrame with
        member this.FillNull(fillValue) = this.Lazy().FillNull(fillValue).Collect()
        member this.FillNan(fillValue) = this.Lazy().FillNan(fillValue).Collect()
    /// ========================
    /// Drop
    /// ========================
    type LazyFrame with
        /// <summary>
        /// Drop selected columns by selector.
        /// </summary>
        member this.Drop(selector: Selector) =
            let h = PolarsWrapper.LazyFrameDrop(this.CloneHandle(), selector.CloneHandle())
            new LazyFrame(h)

        /// <summary>
        /// Drop selected columns by column names.
        /// </summary>
        member this.Drop([<ParamArray>]columns: string array) =
            if isNull columns then nullArg (nameof columns)
            
            this.Drop(Selector.ByName columns)

        /// <summary>
        /// Drop columns by specific Expressions.
        /// </summary>
        member this.Drop(exprs: seq<Expr>) =
            if isNull exprs then nullArg (nameof exprs)
            
            exprs |> Seq.fold (fun (lf: LazyFrame) expr -> lf.Drop(expr.ToSelector())) this
        member this.Drop([<ParamArray>]exprs: Expr array) =
            this.Drop(exprs :> seq<Expr>)

        /// <summary>
        /// Drop rows containing one or more Null values.
        /// </summary>
        member this.DropNulls(?subset: Selector) =
            let subsetHandle = subset |> Option.map (fun s -> s.CloneHandle()) |> Option.toObj
                
            let h = PolarsWrapper.LazyFrameDropNulls(this.CloneHandle(), subsetHandle)
            new LazyFrame(h)

        /// <summary>
        /// Drop rows with Nulls in specific columns.
        /// </summary>
        member this.DropNulls([<ParamArray>] subset: string array) =
            if isNull subset || subset.Length = 0 then 
                this.DropNulls()
            else 
                this.DropNulls(Expr.Col(subset))

        /// <summary>
        /// Drop rows with Nulls by specific Expressions.
        /// </summary>
        member this.DropNulls([<ParamArray>] exprs: Expr array) =
            if isNull exprs then nullArg (nameof exprs)
            exprs |> Seq.fold (fun (lf: LazyFrame) expr -> lf.DropNulls(expr.ToSelector())) this

        /// <summary>
        /// Drop rows containing one or more NaN values.
        /// </summary>
        member this.DropNans(?subset: Selector) =
            let subsetHandle = subset |> Option.map (fun s -> s.CloneHandle()) |> Option.toObj
                
            let h = PolarsWrapper.LazyFrameDropNans(this.CloneHandle(), subsetHandle)
            new LazyFrame(h)

        /// <summary>
        /// Drop rows with NaN in specific columns.
        /// </summary>
        member this.DropNans([<ParamArray>] subset: string array) =
            if isNull subset || subset.Length = 0 then 
                this.DropNans()
            else 
                this.DropNans(Expr.Col subset)

        /// <summary>
        /// Drop rows with NaN by specific Expressions.
        /// </summary>
        member this.DropNans([<ParamArray>] exprs: Expr array) =
            if isNull exprs then nullArg (nameof exprs)
            exprs |> Seq.fold (fun (lf: LazyFrame) expr -> lf.DropNans(expr.ToSelector())) this

    type DataFrame with
        /// <summary>
        /// Drop one or more columns from the DataFrame.
        /// Returns a new DataFrame.
        member this.Drop([<ParamArray>]columns: string array) =
            if isNull columns || columns.Length = 0 then
                new DataFrame(PolarsWrapper.CloneDataFrame this.Handle)
            else
                let newHandle = PolarsWrapper.Drop(this.Handle, columns)
                new DataFrame(newHandle)

        /// <summary>
        /// Drop columns using Polars Selectors or Expressions.
        /// </summary>
        member this.Drop(exprs: seq<Expr>) =
            if isNull exprs then nullArg (nameof exprs)

            use lf = this.Lazy()
            use droppedLf:LazyFrame = lf.Drop exprs
            
            droppedLf.Collect()
        member this.Drop([<ParamArray>]exprs: Expr array) =
            this.Drop(exprs :> seq<Expr>)
        /// <summary>
        /// Drop a column in-place and return it as a Series.
        /// Note: This mutates the original DataFrame.
        /// </summary>
        member this.DropInPlace(name: string) =
            if String.IsNullOrEmpty name then nullArg (nameof name)
            
            let seriesHandle = PolarsWrapper.DropInPlace(this.Handle, name)
            new Series(seriesHandle)
        
        /// <summary>
        /// Drop rows containing one or more Null values.
        /// </summary>
        member this.DropNulls(?subset: Selector) =
            use lf = this.Lazy()
            let droppedLf: LazyFrame = lf.DropNulls(?subset = subset)
            droppedLf.Collect()

        /// <summary>
        /// Drop rows with Nulls in specific columns.
        /// </summary>
        member this.DropNulls([<ParamArray>]subset: string array) =
            use lf = this.Lazy()
            let droppedLf: LazyFrame = lf.DropNulls(subset)
            droppedLf.Collect()

        /// <summary>
        /// Drop rows with Nulls by specific Expressions.
        /// </summary>
        member this.DropNulls([<ParamArray>]exprs: Expr array) =
            use lf = this.Lazy()
            let droppedLf: LazyFrame = lf.DropNulls(exprs)
            droppedLf.Collect()

        /// <summary>
        /// Drop rows containing one or more NaN values.
        /// </summary>
        member this.DropNans(?subset: Selector) =
            use lf = this.Lazy()
            let droppedLf: LazyFrame= lf.DropNans(?subset = subset)
            droppedLf.Collect()

        /// <summary>
        /// Drop rows with NaN in specific columns.
        /// </summary>
        member this.DropNans([<ParamArray>]subset: string array) =
            use lf = this.Lazy()
            let droppedLf: LazyFrame = lf.DropNans subset
            droppedLf.Collect()

        /// <summary>
        /// Drop rows with NaN by specific Expressions.
        /// </summary>
        member this.DropNans([<ParamArray>]exprs: Expr array) =
            use lf = this.Lazy()
            let droppedLf: LazyFrame = lf.DropNans exprs
            droppedLf.Collect()
    /// ========================
    /// Unique
    /// ========================
    type LazyFrame with
       /// <summary>
        /// Keep unique rows (stable) based on a subset of columns defined by a Selector.
        /// </summary>
        member this.Unique
            (
                ?subset: Selector, 
                ?keep: UniqueKeepStrategy, 
                ?maintainOrder: bool
            ) =
            let keepArg = defaultArg keep UniqueKeepStrategy.First
            let maintainArg = defaultArg maintainOrder false

            let subsetHandle =
                match subset with
                | Some s -> s.CloneHandle()
                | None -> Unchecked.defaultof<SelectorHandle>

            let newHandle = PolarsWrapper.LazyUnique(
                this.CloneHandle(), 
                subsetHandle, 
                keepArg.ToNative(), 
                maintainArg
            )

            new LazyFrame(newHandle)
        /// <summary>
        /// Keep unique rows based on specific column names.
        /// </summary>
        member this.Unique
            (
                columns: seq<string>, 
                ?keep: UniqueKeepStrategy, 
                ?maintainOrder: bool
            ) =
            let columnsArray =
                match columns with
                | :? (string array) as arr -> arr
                | _ -> columns |> Seq.toArray

            if columnsArray.Length = 0 then
                this.Unique(?subset = None, ?keep = keep, ?maintainOrder = maintainOrder)
            else
                use selector = Selector.ByName columnsArray
                this.Unique(subset = selector, ?keep = keep, ?maintainOrder = maintainOrder)
    type DataFrame with
        /// <summary>
        /// Return the number of unique rows, or the number of unique row-subsets.
        /// </summary>
        member this.NUnique(?subset: seq<string>) = 
            use df: DataFrame = this.Unique(?subset = subset)
            df.Height

        /// <summary>
        /// Return the number of unique rows, or the number of unique row-subsets.
        /// </summary>
        member this.NUnique([<ParamArray>]subset: Expr array) =
            use df = this.Unique subset
            df.Height

        /// <summary>
        /// Return the number of unique rows, or the number of unique row-subsets.
        /// </summary> 
        member this.Unique
            (
                ?subset: seq<string>,
                ?keep: UniqueKeepStrategy,
                ?maintainOrder: bool,
                ?offset: int64,
                ?len: int64
            ) =
            let keepArgs = defaultArg keep UniqueKeepStrategy.First
            let maintainArgs = defaultArg maintainOrder false

            let subsetArray =
                match subset with
                | Some s -> Array.ofSeq s
                | None -> null

            let slice =
                match offset, len with
                | Some o, Some l ->
                    let safeLen = uint64 (Math.Max(0L, l))
                    Nullable<struct (int64 * uint64)> struct (o, safeLen) 
                | _ ->
                    Nullable<struct (int64 * uint64)>()

            let h = PolarsWrapper.DataFrameUnique(
                this.Handle,
                subsetArray,
                keepArgs.ToNative(),
                maintainArgs,
                slice
            )

            new DataFrame(h)

        member this.Unique
            (
                subset: seq<Expr>,
                ?keep: UniqueKeepStrategy,
                ?maintainOrder: bool,
                ?offset: int64,
                ?len: int64
            ) :DataFrame =
            let resolvedColumnNames = ResizeArray<string>()

            for expr in subset do
                let name = expr.Meta.OutputName()
                
                if not (String.IsNullOrEmpty name) then
                    resolvedColumnNames.Add name
                else
                    try
                        let expandedNames = (this.Head(0).Select(expr) : DataFrame).Columns
                        resolvedColumnNames.AddRange(expandedNames)
                    with ex ->
                        let msg = sprintf "Cannot parse this expression to column names: %s" ex.Message
                        raise (ArgumentException(msg, ex))

            let finalSubset = 
                resolvedColumnNames 
                |> Seq.distinct 
                |> Seq.toArray

            if finalSubset.Length = 0 then
                raise (ArgumentException "No Columns Selected")

            this.Unique(
                subset = finalSubset,
                ?keep = keep,
                ?maintainOrder = maintainOrder,
                ?offset = offset,
                ?len = len
            )
    /// ========================
    /// TopK & BottomK
    /// ========================
    type LazyFrame with
        /// <summary>
        /// Get the top k rows based on the given columns.
        /// This is often faster than a full sort followed by a head.
        /// </summary>
        /// <param name="k">Number of rows to return.</param>
        /// <param name="by">Columns to sort by.</param>
        /// <param name="reverse">Sort direction per column. Default is false (no reverse).</param>
        member this.TopK(k: int, by: seq<#IColumnExpr>, ?reverse: seq<bool>) =
            let exprHandles = 
                by 
                |> Seq.collect (fun x -> x.ToExprs()) 
                |> Seq.map (fun e -> e.CloneHandle()) 
                |> Seq.toArray
            
            let descArr = 
                match reverse with
                | Some d -> d |> Seq.toArray
                | None -> [| false |] 

            let lfHandle = this.CloneHandle()

            let h = PolarsWrapper.LazyFrameTopK(lfHandle, uint k, exprHandles, descArr)
            new LazyFrame(h)

        /// <summary>
        /// Get the bottom k rows based on the given columns.
        /// </summary>
        member this.BottomK(k: int, by: seq<#IColumnExpr>, ?reverse: seq<bool>) =
            let exprHandles = 
                by 
                |> Seq.collect (fun x -> x.ToExprs()) 
                |> Seq.map (fun e -> e.CloneHandle()) 
                |> Seq.toArray
            
            let descArr = 
                match reverse with
                | Some d -> d |> Seq.toArray
                | None -> [| false |]

            let lfHandle = this.CloneHandle()
            let h = PolarsWrapper.LazyFrameBottomK(lfHandle, uint k, exprHandles, descArr)
            new LazyFrame(h)

        // [Overload] Sugar for single boolean reversing
        member this.TopK(k: int, by: seq<#IColumnExpr>, reverse: bool) =
            this.TopK(k, by, [| reverse |])
        
        member this.BottomK(k: int, by: seq<#IColumnExpr>, reverse: bool) =
            this.BottomK(k, by, [| reverse |])
    /// ========================
    /// Reverse
    /// ========================
    type LazyFrame with
        /// <summary>
        /// Reverse the LazyFrame.
        /// </summary>
        member this.Reverse() = this.Select(Expr.All().Reverse())
    type DataFrame with
        /// <summary>
        /// Reverse the DataFrame.
        /// </summary>
        member this.Reverse() = this.Select(Expr.All().Reverse())
    /// ========================
    /// SetSorted
    /// ========================
    type LazyFrame with
        /// <summary>
        /// Mark one or multiple columns as sorted. 
        /// This is an optimizer hint and will not actually execute a sorting operation.
        /// </summary>
        /// <param name="columns">The columns to mark as sorted. Accepts strings, arrays, or Selectors (e.g. Cs.Temporal()).</param>
        /// <param name="descending">Whether the columns are sorted in descending order.</param>
        /// <param name="nullsLast">Whether null values appear last.</param>
        /// <returns>A new LazyFrame with the sorted hints applied to the query plan.</returns>
        member this.SetSorted(columns:Selector,?descending,?nullsLast) =
            let names = this.Select(columns).Schema.Names
            if names.Length = 0 then
                this.Clone()
            else 
                let de = defaultArg descending false
                let nul = defaultArg nullsLast false
                let exprs = names |> List.map (fun e -> Expr.Col(e).SetSorted(de,nul))
                this.WithColumns exprs
    type DataFrame with
        member this.SetSorted(columns:Selector,?descending,?nullsLast) =
            this.Lazy().SetSorted(columns,?descending=descending,?nullsLast=nullsLast).Collect()

    /// ========================
    /// Clear
    /// ========================
    type LazyFrame with
        /// <summary>
        /// Create an empty (n=0) or n-row null-filled (n>0) copy of the LazyFrame.
        /// Returns a n-row null-filled LazyFrame with an identical schema.
        /// </summary>
        /// <param name="n">Number of (null-filled) rows to return in the cleared frame.</param>
        /// <returns>A new LazyFrame.</returns>
        member this.Clear(?n:int64) = 
            use schema = this.Schema
            schema.ToLazyFrame(?length=n)
    type DataFrame with
        /// <summary>
        /// Create an empty (n=0) or n-row null-filled (n>0) copy of the DataFrame.
        /// Returns a n-row null-filled DataFrame with an identical schema.
        /// </summary>
        /// <param name="n">Number of (null-filled) rows to return in the cleared frame.</param>
        /// <returns>A new DataFrame.</returns>
        member this.Clear(?n: int64) = 
            use schema = this.Schema
            schema.ToDataFrame(?length=n)
    /// ========================
    /// Filter
    /// ========================
    type LazyFrame with
        /// <summary>
        /// Filter rows based on a boolean expression.
        /// <para>
        /// In a LazyFrame, this operation is added to the logical plan and is optimized before execution.
        /// Polars will attempt to push this filter down as close to the data source as possible (Predicate Pushdown).
        /// </para>
        /// </summary>
        member this.Filter (expr: Expr) : LazyFrame =
            let lfClone = this.CloneHandle()
            let exprClone = expr.CloneHandle()
            
            let h = PolarsWrapper.LazyFilter(lfClone, exprClone)
            new LazyFrame(h)

        member this.Filter (series: Series) : LazyFrame =
            if series.DataType <> DataType.Boolean then
                invalidArg "series" "Series DataType should be Boolean"
                
            let sh = PolarsWrapper.Lit(series.CloneHandle())
            use expr = new Expr(sh) 
            this.Filter expr
        member this.Filter(mask:seq<bool>) = 
            let ma = Series.create("___mask",mask)
            this.Filter(ma)
        /// <summary>
        /// Remove rows that match the predicate.
        /// This is the exact opposite of Filter. Rows where the predicate evaluates to False or Null are kept.
        /// </summary>
        member this.Remove(predicate:Expr) = 
            use invertedExpr = predicate.NeqMissing(new Expr(PolarsWrapper.Lit true))
            this.Filter(invertedExpr)
        member this.Remove(predicate:Series) = 
            if predicate.DataType <> DataType.Boolean then
                invalidArg "predicate" "Masking Series DataType should be Boolean"
            let sh = PolarsWrapper.Lit(predicate.CloneHandle())
            use expr = new Expr(sh) 
            this.Remove(expr)
        member this.Remove(mask:seq<bool>) =
            let ma = Series.create("___mask",mask)
            this.Remove(ma)

    type DataFrame with
        /// <summary> Filter rows based on a boolean expression (predicate). </summary>
        member this.Filter (expr: Expr) : DataFrame = 
            this.Lazy().Filter(expr).Collect()
        /// <summary> Filter rows based on a boolean Series. </summary>
        member this.Filter (series: Series) : DataFrame = 
            this.Lazy().Filter(series).Collect()
        member this.Filter(mask:seq<bool>) = 
            let ma = Series.create("___mask",mask)
            this.Filter(ma)
        member this.Remove(predicate:Expr) =
            this.Lazy().Remove(predicate)
        member this.Remove(predicate:Series) = 
            this.Lazy().Remove predicate
        member this.Remove(mask:seq<bool>) = 
            this.Lazy().Remove mask

    /// ========================
    /// Sample
    /// ========================
        /// <summary>
        /// Sample n rows from the DataFrame.
        /// </summary>
        member this.Sample(n: int, ?withReplacement: bool, ?shuffle: bool, ?seed: uint64) : DataFrame =
            let replace = defaultArg withReplacement false
            let shuff = defaultArg shuffle true
            let s = Option.toNullable seed
            
            new DataFrame(PolarsWrapper.SampleNLiteral(this.Handle, uint64 n, replace, shuff, s))

        /// <summary>
        /// Sample a fraction of rows from the DataFrame.
        /// </summary>
        member this.Sample(frac: double, ?withReplacement: bool, ?shuffle: bool, ?seed: uint64) : DataFrame =
            let replace = defaultArg withReplacement false
            let sf = Series.create("",[frac]).Handle
            let shuff = defaultArg shuffle true
            let s = Option.toNullable seed
            
            new DataFrame(PolarsWrapper.SampleFrac(this.Handle, sf, replace, shuff, s))
    /// ========================
    /// Shrink
    /// ========================
        /// <summary>
        /// Shrink DataFrame memory usage.This won't return a new DataFrame
        /// </summary>
        member this.ShrinkToFitInplace() = PolarsWrapper.DataFrameShrinkToFit(this.Handle)
        /// <summary>
        /// Shrink DataFrame memory usage.
        /// </summary>
        /// <returns>A new DataFrame</returns>
        member this.ShrinkToFit() = 
            let newDf = this.Clone()
            PolarsWrapper.DataFrameShrinkToFit(newDf.Handle)
            newDf
    /// ========================
    /// Take
    /// ========================
        /// <summary>
        /// Take rows by physical integer indices.
        /// Note: Negative indices are not supported. All values must be >= 0.
        /// </summary>
        /// <param name="indices">A Series containing integer indices (e.g., UInt32, Int32).</param>
        /// <exception cref="ArgumentException">Thrown when the series is not an integer type.</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when indices are out of bounds or negative.</exception>
        member this.Take(indices: Series) =
            if not indices.DataType.IsInteger then
                raise (ArgumentException("Take requires an integer Series, but got {indices.DataType.Kind}.", nameof(indices)))

            try
                new DataFrame(PolarsWrapper.DataFrameTake(this.Handle, indices.Handle))
            with
            | ex when ex.Message.Contains("OutOfBounds") || ex.Message.Contains("out of bounds") ->
                raise (ArgumentOutOfRangeException(
                    nameof(indices), 
                    "Index out of bounds. This may be caused by index values exceeding the DataFrame's height, or by using negative indices which are not supported in Take."
                ))

    /// ========================
    /// Gather Every
    /// ========================
    type LazyFrame with
        /// <summary>
        /// Take every nth row in the Frame and return as a new Frame.
        /// </summary>
        /// <param name="n">Gather every n-th row.</param>
        /// <param name="offset">Starting Index</param>
        /// <returns></returns>
        member this.GatherEvery(n: uint64, ?offset: uint64) =
            let offsetD = defaultArg offset 0UL
            this.Select(Expr.All().GatherEvery(n,offsetD))
    type DataFrame with
        /// <summary>
        /// Take every nth row in the Frame and return as a new Frame.
        /// </summary>
        /// <param name="n">Gather every n-th row.</param>
        /// <param name="offset">Starting Index</param>
        /// <returns></returns>
        member this.GatherEvery(n: uint64, ?offset: uint64) =
            let offsetD = defaultArg offset 0UL
            this.Select(Expr.All().GatherEvery(n,offsetD))
    /// ========================
    /// Interpolate
    /// ========================
    type LazyFrame with
        /// <summary>
        /// Interpolate intermediate values. The interpolation method is linear.
        /// Nulls at the beginning and end of the series remain null.
        /// </summary>
        member this.Interpolate() =
            this.Select(Expr.All().Interpolate(method=InterpolationMethod.Linear))
    type DataFrame with
        /// <summary>
        /// Interpolate intermediate values. The interpolation method is linear.
        /// Nulls at the beginning and end of the series remain null.
        /// </summary>
        member this.Interpolate() =
            this.Select(Expr.All().Interpolate(method=InterpolationMethod.Linear))
    /// ========================
    /// Column Manipulation
    /// ========================
        /// <summary>
        /// Insert a column into the DataFrame at a specified index.
        /// Accepts a specific index and a Series to be inserted.
        /// </summary>
        /// <param name="index">The index at which to insert the column. Negative indices count from the end.</param>
        /// <param name="column">The Series to insert as a column.</param>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when the index is out of bounds.</exception>
        member this.InsertColumn(index: int, column: Series) =
            let originalIndex = index
            let width = int this.Width

            let targetIndex = 
                if index < 0 then width + index
                else index

            if targetIndex < 0 || targetIndex > width then
                let msg = String.Format("Column index {0} is out of range (frame has {1} columns)", originalIndex, width)
                raise (ArgumentOutOfRangeException("index", msg))

            // Convert existing column names to a list of Expr.Col
            let currentCols = 
                this.Columns 
                |> Array.map Expr.Col 
                |> Array.toList

            // Convert the incoming Series to a literal Expr[cite: 1]
            let h = PolarsWrapper.CloneSeries column.Handle 
            let exprToInsert = new Expr(PolarsWrapper.Lit h)

            // Imperative mutation is avoided.
            // We split the list at targetIndex, insert the new expression, and recombine.
            let left, right = List.splitAt targetIndex currentCols
            let newCols = left @ [ exprToInsert ] @ right

            this.Select(List.toArray newCols)
        /// <summary>
        /// Replace a column by its index in-place.
        /// Supports negative indexing (e.g., -1 replaces the last column).
        /// </summary>
        /// <param name="index">The index of the column to replace. Negative values count from the end.</param>
        /// <param name="newColumn">The new Series to insert.</param>
        /// <param name="keepName">If true, keeps the original column name. If false, uses the new Series's name. Default is false.</param>
        /// <returns>The current DataFrame instance to support method chaining.</returns>
        member this.ReplaceColumn(index: int, newColumn: Series, ?keepName: bool) =
            if box newColumn = null then
                raise (ArgumentNullException(nameof(newColumn)))

            let shouldKeepName = defaultArg keepName false
            let width = int this.Width
            
            let targetIndex = 
                if index < 0 then width + index
                else index

            if targetIndex < 0 || targetIndex >= width then
                let msg = String.Format("Column index {0} is out of bounds. DataFrame width is {1}.", index, width)
                raise (ArgumentOutOfRangeException(nameof(index), msg))

            if shouldKeepName then
                let originalName = this.Columns.[targetIndex]
                PolarsWrapper.Replace(this.Handle, originalName, newColumn.Handle)
            else
                PolarsWrapper.ReplaceColumnAt(this.Handle, targetIndex, newColumn.Handle)
                
            this
        /// <summary>
        /// Replace a column by its name in-place.
        /// </summary>
        /// <param name="columnName">The name of the column to replace.</param>
        /// <param name="newColumn">The new Series to insert.</param>
        /// <param name="keepName">If true, keeps the original column name. If false, uses the new Series's name. Default is true.</param>
        /// <returns>The current DataFrame instance to support method chaining.</returns>
        member this.ReplaceColumn(columnName: string, newColumn: Series, ?keepName: bool) =
            if box newColumn = null then
                raise (ArgumentNullException(nameof(newColumn)))

            let shouldKeepName = defaultArg keepName true

            if shouldKeepName then
                PolarsWrapper.Replace(this.Handle, columnName, newColumn.Handle)
            else
                let index = Array.IndexOf(this.Columns, columnName)
                if index = -1 then
                    let msg = String.Format("Column '{0}' does not exist in the DataFrame.", columnName)
                    raise (ArgumentException(msg))
                PolarsWrapper.ReplaceColumnAt(this.Handle, index, newColumn.Handle)

            this
    /// ========================
    /// Upsample and dummies
    /// ========================
        /// <summary>
        /// Upsample a DataFrame at a regular frequency.
        /// </summary>
        /// <param name="timeColumn">The column used for the time/datetime index. Must resolve to exactly one column.</param>
        /// <param name="every">The interval to upsample to (e.g., Duration.String "1d", or Duration.TimeSpan ts).</param>
        /// <param name="groupBy">Optional. Group by this selector before upsampling.</param>
        /// <param name="maintainOrder">If true, maintains the original order of the groups. Default is false.</param>
        /// <returns>A new DataFrame with missing time steps filled with nulls.</returns>
        member this.Upsample(timeColumn: Selector, every: Dur, ?groupBy: Selector, ?maintainOrder: bool) =
                let shouldMaintainOrder = defaultArg maintainOrder false

                // 1. Inline ExpandSelector logic for timeColumn
                // F# uses 'use' keyword for deterministic disposal (equivalent to C# 'using var')
                use emptyDfForTime = this.Clear()
                use resolvedTimeDf = emptyDfForTime.Select(timeColumn.ToExpr())
                let expandedTimeCols = resolvedTimeDf.Columns

                if expandedTimeCols.Length <> 1 then
                    let colsStr = if expandedTimeCols.Length > 0 then String.Join(", ", expandedTimeCols) else "None"
                    let msg = String.Format("The timeColumn selector must resolve to exactly one column, but it resolved to {0} columns: {1}", expandedTimeCols.Length, colsStr)
                    raise (ArgumentException(msg, nameof(timeColumn)))
                
                let resolvedTimeColumn = expandedTimeCols.[0]

                // 2. Consume Duration DU to FFI string representation
                let durationStr = Dur.consume every

                // 3. Inline ExpandSelector logic for optional groupBy
                let groupByCols = 
                    match groupBy with
                    | Some selector ->
                        use emptyDfForGroup = this.Clear()
                        use resolvedGroupDf = emptyDfForGroup.Select(selector.ToExpr())
                        let cols = resolvedGroupDf.Columns
                        if cols.Length = 0 then null else cols
                    | None -> null

                // 4. Call Core Layer (LibraryImport Wrapper)
                let newHandle = PolarsWrapper.DataFrameUpsample(this.Handle, resolvedTimeColumn, durationStr, groupByCols, shouldMaintainOrder)
                new DataFrame(newHandle)
        /// <summary>
        /// Convert categorical columns to dummy/one-hot encoded variables.
        /// </summary>
        /// <param name="columns">Optional. Specific column names to convert. If omitted, all string and categorical columns are converted.</param>
        /// <param name="separator">The separator between the original column name and the dummy category. Default is "_".</param>
        /// <param name="dropFirst">If true, drops the first dummy column to avoid multicollinearity. Default is false.</param>
        /// <param name="dropNulls">If true, drops dummy columns representing null values. Default is false.</param>
        /// <returns>A new DataFrame with dummy variables.</returns>
        /// <exception cref="ArgumentException">Thrown when the provided columns array is empty.</exception>
        member this.ToDummies(?columns: seq<string>, ?separator: string, ?dropFirst: bool, ?dropNulls: bool) =
            let sep = defaultArg separator "_"
            let shouldDropFirst = defaultArg dropFirst false
            let shouldDropNulls = defaultArg dropNulls false

            let columnsArray = 
                match columns with
                | Some cols ->
                    let arr = Seq.toArray cols
                    if arr.Length = 0 then
                        let msg = "The provided columns sequence cannot be empty. Pass None to convert all default categorical columns."
                        raise (ArgumentException(msg, nameof(columns)))
                    arr
                | None ->
                    use emptyDf = this.Clear()
                    
                    let stringCols = 
                        use sel = Selector.ByDtype DataType.String
                        use res = emptyDf.Select(sel.ToExpr())
                        res.Columns
                        
                    let catCols = 
                        use sel = Selector.ByDtype(DataType.Categorical())
                        use res = emptyDf.Select(sel.ToExpr())
                        res.Columns
                        
                    let enumCols = 
                        use sel =  new Selector(PolarsWrapper.SelectorEnum())
                        use res = emptyDf.Select(sel.ToExpr())
                        res.Columns

                    // Merge all propped columns distinctly
                    let mergedCols = 
                        Array.concat [ stringCols; catCols; enumCols ] 
                        |> Array.distinct
                    
                    if mergedCols.Length = 0 then
                        let msg = "The DataFrame contains no string, categorical, or enum columns to convert."
                        raise (ArgumentException(msg))
                    mergedCols

            let newHandle = PolarsWrapper.DataFrameToDummies(this.Handle, columnsArray, sep, shouldDropFirst, shouldDropNulls)
            new DataFrame(newHandle)
    /// ========================
    /// WithRowIndex
    /// ========================
    type LazyFrame with
        /// <summary>
        /// Add a column at index 0 that counts the rows.
        /// <para>
        /// This can be useful to generate a unique index or to maintain order in operations 
        /// that would otherwise discard it.
        /// </para>
        /// </summary>
        /// <param name="name">The name of the new row index column. Defaults to "index".</param>
        /// <param name="offset">The starting value of the row index. Defaults to 0.</param>
        /// <returns>A new LazyFrame with the row index column added.</returns>
        member this.WithRowIndex(?name:string,?offset:int) =
            let na = defaultArg name "index"
            let off = 
                match offset with
                | Some o -> 
                    if o < 0 then
                        raise (ArgumentException "`offset` input for `WithRowIndex` cannot be negative")
                    else o
                | None -> 0
            new LazyFrame(PolarsWrapper.LazyFrameWithRowIndex(this.CloneHandle(), na,off))
    type DataFrame with
        /// <summary>
        /// Add a column at index 0 that counts the rows.
        /// <para>
        /// This can be useful to generate a unique index or to maintain order in operations 
        /// that would otherwise discard it.
        /// </para>
        /// </summary>
        /// <param name="name">The name of the new row index column. Defaults to "index".</param>
        /// <param name="offset">The starting value of the row index. Defaults to 0.</param>
        /// <returns>A new DataFrame with the row index column added.</returns>
        member this.WithRowIndex(?name:string,?offset:int) =
            let na = defaultArg name "index"
            let off = 
                match offset with
                | Some o -> 
                    if o < 0 then
                        raise (ArgumentException "`offset` input for `WithRowIndex` cannot be negative")
                    else o
                | None -> 0
            new DataFrame(PolarsWrapper.DataFrameWithRowIndex(this.Handle, na,off))
     