namespace Polars.FSharp

open Polars.NET.Core
open System

type [<Struct>] ListOps(handle: ExprHandle) =

    /// <summary> Get element at index. </summary>
    member _.Get(index: int,?nullOnOob:bool) = 
        let nob = defaultArg nullOnOob false
        new Expr(PolarsWrapper.ListGet(handle, PolarsWrapper.Lit index,nob))
    /// <summary> Get the first element of the list. </summary>
    member this.First() = this.Get(0,true)
    /// <summary> Get the Last element of the list. </summary>
    member this.Last() = this.Get(-1,true)
    /// <summary> Join list elements with separator. </summary>
    member _.Join(separator: string,?ignoreNulls:bool) = 
        let ign = defaultArg ignoreNulls true
        new Expr(PolarsWrapper.ListJoin(handle, separator,ign))
    /// <summary>
    /// Take sublists by multiple indices.The indices may be defined in a single column, or by sublists in another column of dtype List.
    /// </summary>
    /// <param name="indices">Indices to return per sublist</param>
    /// <param name="nullOnOob">Behavior if an index is out of bounds: 
    /// True -> set as null 
    /// False -> raise an error Note that defaulting to raising an error is much cheaper</param>
    member _.Gather(indices:Expr,?nullOnOob:bool) = 
        let nul = defaultArg nullOnOob false
        new Expr(PolarsWrapper.ListGather(handle,indices.CloneHandle(),nul))
    /// <summary>
    /// Take every n-th value start from offset in sublists.
    /// </summary>
    /// <param name="n">Gather every n-th element.</param>
    /// <param name="offset">Starting Index</param>
    /// <returns></returns>
    member _.GatherEvery(n:Expr,?offset:Expr) =
        let off = 
            match offset with
            | Some offset -> offset.CloneHandle()
            | None -> PolarsWrapper.Lit 0
        new Expr(PolarsWrapper.ListGatherEvery(handle,n.CloneHandle(),off))
    /// <summary>
    /// Slice every sublist.
    /// </summary>
    /// <param name="offset">Start index. Negative indexing is supported.</param>
    /// <param name="length">Length of the slice. If null, the slice is taken to the end of the list.</param>
    member _.Slice(offset:Expr,?length:Expr) = 
        let len = 
            match length with
            | Some length -> length.CloneHandle()
            | None -> PolarsWrapper.LitNull()
        new Expr(PolarsWrapper.ListSlice(handle,offset.CloneHandle(),len))
    /// <summary>
    /// Slice the first n values of every sublist.
    /// </summary>
    /// <param name="n">Number of values to return for each sublist.</param>
    /// <returns></returns>
    member this.Head(?n:Expr) = 
        let num = 
            match n with
            | Some n -> n
            | None -> new Expr(PolarsWrapper.Lit 5)
        this.Slice(new Expr(PolarsWrapper.Lit 0),num)
    /// <summary>
    /// Slice the last n values of every sublist.
    /// </summary>
    /// <param name="n">Number of values to return for each sublist.</param>
    /// <returns></returns>
    member _.Tail(?n:Expr) =
        let num = 
            match n with
            | Some n -> n.CloneHandle()
            | None -> PolarsWrapper.Lit 5
        new Expr(PolarsWrapper.ListTail(handle,num))
    /// <summary>
    /// Run any polars aggregation expression against the list’ elements.
    /// </summary>
    /// <param name="expr">Expression to run. Note that you can select an element with Pl.Element().</param>
    member _.Agg(expr:Expr) =
        new Expr(PolarsWrapper.ListAgg(handle,expr.CloneHandle()))
    /// <summary>
    /// Run any polars expression against the lists’ elements.
    /// </summary>
    /// <param name="expr">Expression to run. Note that you can select an element with Pl.Element().</param>
    member _.Eval(expr:Expr) =
        new Expr(PolarsWrapper.ListEval(handle,expr.CloneHandle()))
    /// <summary>
    /// Shift list values by the given number of indices.
    /// </summary>
    /// <param name="n">Number of indices to shift forward. 
    /// If a negative value is passed, values are shifted in the opposite direction instead.</param>
    member _.Shift(?n:Expr) =
        let num = 
            match n with
            | Some n -> n.CloneHandle()
            | None -> PolarsWrapper.Lit 1
        new Expr(PolarsWrapper.ListShift(handle,num))
    /// <summary>
    /// Calculate the first discrete difference between shifted items of every sublist.
    /// </summary>
    /// <param name="n">Number of slots to shift.</param>
    /// <param name="nullBehavior">How to handle null values.</param>
    member _.Diff(?n:int64,?nullBehavior:NullBehavior) =
        let num = defaultArg n 1L
        let nb = defaultArg nullBehavior NullBehavior.Ignore
        new Expr(PolarsWrapper.ListDiff(handle,num,nb.ToNative()))
    /// <summary>
    /// Sample from this list.
    /// </summary>
    /// <param name="n">Number of items to return.Defaults to 1</param>
    /// <param name="withReplacement">Allow values to be sampled more than once.</param>
    /// <param name="shuffle">Shuffle the order of sampled data points.</param>
    /// <param name="seed">Seed for the random number generator. 
    /// If set to None (default), a random seed is generated for each sample operation.</param>
    member _.SampleN(?n:Expr,?withReplacement:bool,?shuffle:bool,?seed:uint64) =
        let num = 
            match n with
            | Some n -> n.CloneHandle()
            | None -> PolarsWrapper.Lit 1
        let wr = defaultArg withReplacement false
        let sh = Option.toNullable shuffle
        let sd = seed |> Option.toNullable
        new Expr(PolarsWrapper.ListSampleN(handle,num,wr,sh,sd))
    /// <summary>
    /// Sample from this list.
    /// </summary>
    /// <param name="fraction">Fraction of items to return. </param>
    /// <param name="withReplacement">Allow values to be sampled more than once.</param>
    /// <param name="shuffle">Shuffle the order of sampled data points.</param>
    /// <param name="seed">Seed for the random number generator. 
    /// If set to None (default), a random seed is generated for each sample operation.</param>
    member _.SampleFrac(fraction:Expr,?withReplacement:bool,?shuffle:bool,?seed:uint64) =
        let wr = defaultArg withReplacement false
        let sh = Option.toNullable shuffle
        let sd = seed |> Option.toNullable
        new Expr(PolarsWrapper.ListSampleFraction(handle,fraction.CloneHandle(),wr,sh,sd))
    /// <summary>
    /// Compute the SET UNION between the elements in this list and the elements of other.
    /// </summary>
    /// <param name="other">Right hand side of the set operation.</param>
    member _.SetUnion(other:Expr) = 
        new Expr(PolarsWrapper.ListSetUnion(handle,other.CloneHandle()))
    /// <summary>
    /// Compute the SET DIFFERENCE between the elements in this list and the elements of other.
    /// </summary>
    /// <param name="other">Right hand side of the set operation.</param>
    member _.SetDifference(other:Expr) =
        new Expr(PolarsWrapper.ListSetDifference(handle,other.CloneHandle()))
    /// <summary>
    /// Compute the SET INTERSECTION between the elements in this list and the elements of other.
    /// </summary>
    /// <param name="other">Right hand side of the set operation.</param>
    member _.SetIntersection(other:Expr) =
        new Expr(PolarsWrapper.ListSetIntersection(handle,other.CloneHandle()))
    /// <summary>
    /// Compute the SET SYMMETRIC DIFFERENCE between the elements in this list and the elements of other.
    /// </summary>
    /// <param name="other">Right hand side of the set operation.</param>
    member _.SetSymmetricDifference(other:Expr) = 
        new Expr(PolarsWrapper.ListSetSymmetricDifference(handle,other.CloneHandle()))
    
    /// <summary>
    /// Return the number of elements in each list.
    /// </summary>
    member _.Len() = new Expr(PolarsWrapper.ListLen handle)
    /// <summary> Reverse the list. </summary>
    member this.Reverse() = this.Eval(Expr.Col("").Reverse())
    /// <summary>
    /// Calculate the sum of the values in the list (row-wise).
    /// </summary>
    member _.Sum() = new Expr(PolarsWrapper.ListSum handle)
    /// <summary>
    /// Calculate the min of the list elements.
    /// </summary>
    member _.Min() = new Expr(PolarsWrapper.ListMin handle)
    /// <summary>
    /// Calculate the max of the list elements.
    /// </summary>
    member _.Max() = new Expr(PolarsWrapper.ListMax handle)
    /// <summary>
    /// Calculate the mean of the list elements.
    /// </summary>
    member _.Mean() = new Expr(PolarsWrapper.ListMean handle)
    /// <summary>
    /// Calculate the median of the list elements.
    /// </summary>
    member _.Median() = new Expr(PolarsWrapper.ListMedian handle)
    /// <summary>
    /// Evaluate whether all boolean values in a list are true.
    /// </summary>
    member this.All(?ignoreNulls) = this.Agg(Expr.Col("").All(?ignoreNulls=ignoreNulls))
    /// <summary>
    /// Evaluate whether any boolean value in a list is true.
    /// </summary>
    member this.Any(?ignoreNulls) = this.Agg(Expr.Col("").Any(?ignoreNulls=ignoreNulls))
    /// <summary>
    /// Drop all null values in the list.
    /// The original order of the remaining elements is preserved.
    /// </summary>
    member _.DropNulls() = new Expr(PolarsWrapper.ListDropNulls handle)
    /// <summary>
    /// Count the number of unique values in every sub-lists.
    /// </summary>
    member this.NUnique() = this.Agg(Expr.Col("").NUnique())
    /// <summary>
    /// Retrieve the index of the maximum value in every sublist.
    /// </summary>
    member _.ArgMax() = new Expr(PolarsWrapper.ListArgMax handle)
    /// <summary>
    /// Retrieve the index of the minimum value in every sublist.
    /// </summary>
    member _.ArgMin() = new Expr(PolarsWrapper.ListArgMin handle)
    /// <summary>
    /// Get the standard deviation.
    /// </summary>
    /// <param name="ddof">“Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. 
    /// By default ddof is 1.</param>
    member _.Std(?ddof:uint8) = 
        let dd = defaultArg ddof 1uy
        new Expr(PolarsWrapper.ListStd(handle,dd))
    /// <summary>
    /// Get the variance.
    /// </summary>
    /// <param name="ddof">“Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. 
    /// By default ddof is 1.</param>
    member _.Var(?ddof:uint8) = 
        let dd = defaultArg ddof 1uy
        new Expr(PolarsWrapper.ListVar(handle,dd))
    /// <summary>
    /// Get the unique/distinct values in the list.
    /// </summary>
    /// <param name="maintainOrder">Maintain order of data. This requires more work.</param>
    member this.Unique(?maintainOrder:bool) = this.Eval(Expr.Col("").Unique(?maintainOrder=maintainOrder))
    /// <summary>
    /// Sort the lists in this column.
    /// </summary>
    /// <param name="descending">Sort in descending order.</param>
    /// <param name="nullsLast">Place null values last.</param>
    /// <param name="maintainOrder"></param>
    member _.Sort(?descending: bool, ?nullsLast:bool,?maintainOrder: bool) =
        let desc = defaultArg descending false
        let nullsLastOption = defaultArg nullsLast false 
        let maintainOrderOption = defaultArg maintainOrder false 
        new Expr(PolarsWrapper.ListSort(handle, desc,nullsLastOption,maintainOrderOption))
    /// <summary>
    /// Combine the current expression with other expressions into a List.
    /// Result: [parent_val, other_val_1, other_val_2, ...]
    /// Equivalent to: pl.concatList([parent, others...])
    /// </summary>
    member _.Concat(others: seq<#IColumnExpr>) =
        let currentHandle = handle
        let handles = 
            seq {
                yield currentHandle
                
                yield! others 
                       |> Seq.collect (fun x -> x.ToExprs()) 
                       |> Seq.map (fun e -> e.CloneHandle())
            }
            |> Seq.toArray

        new Expr(PolarsWrapper.ConcatList handles)

    /// <summary>
    /// Overload: Concat a single expression/column.
    /// </summary>
    member this.Concat(other: #IColumnExpr) =
        this.Concat [other]
    /// <summary>
    /// Check if the list contains the given item.
    /// </summary>
    member _.Contains(item: Expr,?nullsEqual: bool) : Expr = 
        let nE = defaultArg nullsEqual false
        new Expr(PolarsWrapper.ListContains(handle, item.CloneHandle(),nE))
    member _.Contains(item: int,?nullsEqual: bool) = 
        let itemHandle = PolarsWrapper.Lit item
        let nE = defaultArg nullsEqual false
        new Expr(PolarsWrapper.ListContains(PolarsWrapper.CloneExpr handle, itemHandle,nE))
    member _.Contains(item: string,?nullsEqual:bool) =
        let nE = defaultArg nullsEqual false 
        let itemHandle = PolarsWrapper.Lit item
        new Expr(PolarsWrapper.ListContains(PolarsWrapper.CloneExpr handle, itemHandle, nE))
    /// <summary>
    /// Explode a list expression.
    /// <para>
    /// This turns a list column into a long column (flattening).
    /// </para>
    /// <para>
    /// <b>Warning:</b> When used in <see cref="DataFrame.Select"/> with other columns, 
    /// it may cause a length mismatch error if the other columns are not broadcasted. 
    /// Use <see cref="DataFrame.Explode"/> for safely exploding columns while repeating others.
    /// </para>
    /// </summary>
    member _.Explode(?emptyAsNull:bool,?keepNulls:bool) =
        let emp = defaultArg emptyAsNull true
        let kep = defaultArg keepNulls true
        new Expr(PolarsWrapper.Explode(handle,emp,kep))
    /// <summary>
    /// Convert a List column into an Array column with the same inner data type.
    /// </summary>
    /// <param name="width">Width of the resulting Array column.</param>
    member _.ToArray(width:int64) = new Expr(PolarsWrapper.ListToArray(handle,width))
    /// <summary>
    /// Convert the list to a struct type with explicitly named fields.
    /// </summary>
    /// <param name="fields">A sequence of names for the struct fields.</param>
    /// <returns>A new Expr representing the struct conversion.</returns>
    member this.ToStruct(fields: seq<string>) =
        if box fields = null then
            raise (ArgumentNullException(nameof(fields)))
        
        let fieldsArray = Seq.toArray fields
        // Call the underlying PolarsWrapper FFI layer via Core layer 
        // Assuming this.Handle contains the internal expression handle
        let newHandle = PolarsWrapper.ListToStruct(handle, fieldsArray)
        new Expr(newHandle)

    /// <summary>
    /// Convert the list to a struct type using a function to generate field names.
    /// </summary>
    /// <param name="nameGenerator">A function that takes a field index and returns the field name.</param>
    /// <param name="fieldCount">The number of struct fields to create.</param>
    /// <returns>A new Expr representing the struct conversion.</returns>
    member this.ToStruct(nameGenerator: int -> string, fieldCount: int) =
        if box nameGenerator = null then
            raise (ArgumentNullException(nameof(nameGenerator)))
            
        if fieldCount <= 0 then
            // Fallback to ToStruct with no explicitly named fields (array null/empty)
            let newHandle = PolarsWrapper.ListToStruct(handle, null)
            new Expr(newHandle)
        else
            let fields = Array.init fieldCount nameGenerator
            this.ToStruct(fields)

    /// <summary>
    /// Convert the list to a struct type up to a maximum number of fields, using default naming ("field_0", "field_1", ...).
    /// </summary>
    /// <param name="upperBound">The maximum number of struct fields to create. Used for compile-time schema inference.</param>
    /// <returns>A new Expr representing the struct conversion.</returns>
    member this.ToStruct(upperBound: int) =
        if upperBound <= 0 then
            let newHandle = PolarsWrapper.ListToStruct(handle, null)
            new Expr(newHandle)
        else
            this.ToStruct((fun i -> System.String.Format("field_{0}", i)), upperBound)