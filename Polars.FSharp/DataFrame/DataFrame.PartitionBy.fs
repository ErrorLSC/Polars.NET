namespace Polars.FSharp

open System
open System.Collections
open Polars.NET.Core

/// <summary>
/// Helper function to safely compare two boxed objects at runtime without static comparison constraints.
/// Handles nulls, DBNull, IStructuralComparable (arrays), and standard IComparable instances.
/// </summary>
[<AutoOpen>]
module private PartitionKeyComparison =
    let compareObj (a: obj) (b: obj) : int =
        if obj.ReferenceEquals(a, b) then 0
        elif isNull a || a = box DBNull.Value then
            if isNull b || b = box DBNull.Value then 0 else -1
        elif isNull b || b = box DBNull.Value then 1
        else
            try
                StructuralComparisons.StructuralComparer.Compare(a, b)
            with _ ->
                // Fallback for non-comparable objects to guarantee total ordering in red-black tree
                let h1 = a.GetHashCode()
                let h2 = b.GetHashCode()
                if h1 <> h2 then compare h1 h2
                else String.CompareOrdinal(string a, string b)

/// <summary>
/// Represents a composite partition key consisting of multiple boxed column values.
/// Defined as a single-case discriminated union to support natural constructor syntax
/// and satisfy F# Map comparison constraints with zero compiler conflicts.
/// </summary>
[<CustomEquality; CustomComparison>]
[<StructuredFormatDisplay("{AsString}")>]
type PartitionKey =
    | PartitionKey of obj array

    /// <summary>
    /// Unwraps the underlying boxed column values array.
    /// </summary>
    member this.Values: obj array =
        let (PartitionKey v) = this
        v

    member this.Item(index: int) : obj = this.Values.[index]
    member this.Length: int = this.Values.Length

    member private this.AsString =
        "[" + String.Join(", ", (this.Values |> Array.map (fun v -> if isNull v || v = box DBNull.Value then "null" else string v))) + "]"

    override this.ToString() = this.AsString

    override this.Equals(other: obj) =
        match other with
        | :? PartitionKey as o ->
            let v1 = this.Values
            let v2 = o.Values
            if obj.ReferenceEquals(v1, v2) then true
            elif isNull v1 || isNull v2 then false
            elif v1.Length <> v2.Length then false
            else
                let mutable eq = true
                let mutable i = 0
                while eq && i < v1.Length do
                    let a = v1.[i]
                    let b = v2.[i]
                    eq <-
                        if obj.ReferenceEquals(a, b) then true
                        elif (isNull a || a = box DBNull.Value) && (isNull b || b = box DBNull.Value) then true
                        elif isNull a || isNull b then false
                        else StructuralComparisons.StructuralEqualityComparer.Equals(a, b)
                    i <- i + 1
                eq
        | _ -> false

    override this.GetHashCode() =
        let v = this.Values
        if isNull v then 0
        else
            let mutable hash = 17
            for item in v do
                let itemHash =
                    if isNull item || item = box DBNull.Value then 0
                    else StructuralComparisons.StructuralEqualityComparer.GetHashCode(item)
                hash <- hash * 31 + itemHash
            hash

    interface IComparable<PartitionKey> with
        member this.CompareTo(o: PartitionKey) =
            let v1 = this.Values
            let v2 = o.Values
            if obj.ReferenceEquals(v1, v2) then 0
            elif isNull v1 then -1
            elif isNull v2 then 1
            else
                let lenCmp = compare v1.Length v2.Length
                if lenCmp <> 0 then lenCmp
                else
                    let mutable cmp = 0
                    let mutable i = 0
                    while cmp = 0 && i < v1.Length do
                        cmp <- compareObj v1.[i] v2.[i]
                        i <- i + 1
                    cmp

    interface IComparable with
        member this.CompareTo(other: obj) =
            match other with
            | null -> 1
            | :? PartitionKey as o -> (this :> IComparable<PartitionKey>).CompareTo(o)
            | _ -> invalidArg (nameof other) "Cannot compare PartitionKey with other types."

[<AutoOpen>]
module DataFramePartitionBy =

    type DataFrame with
        /// <summary>
        /// Group by the given columns and return the groups as separate dataframes.
        /// </summary>
        /// <param name="byCols">Column names to group by.</param>
        /// <param name="maintainOrder">Ensure that the order of the groups is consistent with input data. Default is true.</param>
        /// <param name="includeKey">Include partition key columns in output dataframes. Default is true.</param>
        member this.PartitionBy(byCols: seq<string>, ?maintainOrder: bool, ?includeKey: bool) : DataFrame[] =
            let ma = defaultArg maintainOrder true
            let inc = defaultArg includeKey true
            let byArr = byCols |> Seq.toArray
            let handles = PolarsWrapper.PartitionBy(this.Handle, byArr, ma, inc)
            handles |> Array.map (fun h -> new DataFrame(h))

        /// <summary>
        /// Group by a single column and return the groups as separate dataframes.
        /// </summary>
        /// <param name="byCol">Column name to group by.</param>
        /// <param name="maintainOrder">Ensure consistent group ordering. Default is true.</param>
        /// <param name="includeKey">Include partition key column in output dataframes. Default is true.</param>
        member this.PartitionBy(byCol: string, ?maintainOrder: bool, ?includeKey: bool) : DataFrame[] =
            this.PartitionBy([ byCol ], ?maintainOrder = maintainOrder, ?includeKey = includeKey)

        /// <summary>
        /// Partitions the DataFrame by a single column and associates each partition with its strongly-typed key in an immutable F# Map.
        /// </summary>
        /// <typeparam name="'Key">The target scalar or enum type of the partition key.</typeparam>
        /// <param name="byCol">Column name to group by.</param>
        /// <param name="maintainOrder">Ensure consistent group ordering. Default is true.</param>
        member this.PartitionMap<'Key when 'Key: comparison>(byCol: string, ?maintainOrder: bool) : Map<'Key, DataFrame> =
            let partitions = this.PartitionBy([ byCol ], ?maintainOrder = maintainOrder, includeKey = true)
            let targetType = typeof<'Key>
            let isEnum = targetType.IsEnum
            let underlyingType = if isEnum then Enum.GetUnderlyingType(targetType) else targetType

            partitions
            |> Array.choose (fun partDf ->
                if partDf.Height = 0L then
                    None
                else
                    use keyCol = partDf.[byCol]
                    if keyCol.IsNullAt(0L, uncheck = true) then
                        None
                    else
                        let rawVal = 
                            if isEnum then
                                let getter = typeof<Series>.GetMethod("GetValue", [| typeof<int64>; typeof<bool> |]).MakeGenericMethod([| underlyingType |])
                                let num = getter.Invoke(keyCol, [| box 0L; box true |])
                                Enum.ToObject(targetType, num) :?> 'Key
                            else
                                keyCol.GetValue<'Key>(0L, uncheck = true)
                        Some (rawVal, partDf)
            )
            |> Map.ofArray

        /// <summary>
        /// Partitions the DataFrame by two columns and associates each partition with a strongly-typed tuple key in an immutable F# Map.
        /// </summary>
        /// <typeparam name="'K1">The type of the first partition key column.</typeparam>
        /// <typeparam name="'K2">The type of the second partition key column.</typeparam>
        /// <param name="col1">First column name to group by.</param>
        /// <param name="col2">Second column name to group by.</param>
        /// <param name="maintainOrder">Ensure consistent group ordering. Default is true.</param>
        member this.PartitionMap<'K1, 'K2 when 'K1: comparison and 'K2: comparison>(col1: string, col2: string, ?maintainOrder: bool) : Map<'K1 * 'K2, DataFrame> =
            let partitions = this.PartitionBy([ col1; col2 ], ?maintainOrder = maintainOrder, includeKey = true)
            partitions
            |> Array.choose (fun partDf ->
                if partDf.Height = 0L then
                    None
                else
                    use c1 = partDf.[col1]
                    use c2 = partDf.[col2]
                    if c1.IsNullAt(0L, uncheck = true) || c2.IsNullAt(0L, uncheck = true) then
                        None
                    else
                        let v1 = c1.GetValue<'K1>(0L, uncheck = true)
                        let v2 = c2.GetValue<'K2>(0L, uncheck = true)
                        Some ((v1, v2), partDf)
            )
            |> Map.ofArray

        /// <summary>
        /// Partitions the DataFrame by arbitrary multiple columns and associates each partition with a PartitionKey in an immutable F# Map.
        /// </summary>
        /// <param name="byCols">Column names to group by.</param>
        /// <param name="maintainOrder">Ensure consistent group ordering. Default is true.</param>
        member this.PartitionMap(byCols: seq<string>, ?maintainOrder: bool) : Map<PartitionKey, DataFrame> =
            let colNames = byCols |> Seq.toArray
            let partitions = this.PartitionBy(colNames, ?maintainOrder = maintainOrder, includeKey = true)

            partitions
            |> Array.choose (fun partDf ->
                if partDf.Height = 0L then
                    None
                else
                    let compositeKey =
                        colNames
                        |> Array.map (fun name ->
                            use c = partDf.[name]
                            c.GetValue<obj>(0L, uncheck = true)
                        )
                    Some (PartitionKey compositeKey, partDf)
            )
            |> Map.ofArray