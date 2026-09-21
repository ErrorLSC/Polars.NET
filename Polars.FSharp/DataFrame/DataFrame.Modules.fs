namespace Polars.FSharp

open System
open Microsoft.FSharp.Reflection
open Apache.Arrow
open System.Reflection

[<RequireQualifiedAccess>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module DataFrame =

    /// <summary> Filter rows based on a boolean expression. </summary>
    let filter (expr: Expr) (df: DataFrame) : DataFrame =
        df.Filter expr
    /// <summary> Filter frame rows by combining multiple predicates with logical AND. </summary>
    let filterAll (predicates: seq<Expr>) (df: DataFrame) : DataFrame =
        match Seq.toList predicates with
        | [] -> df
        | [ p ] -> df.Filter p
        | head :: tail ->
            let combined = tail |> List.fold (fun acc e -> acc .&& e) head
            df.Filter combined
    /// <summary> Select columns from the DataFrame. </summary>
    let select (exprs: seq<#IColumnExpr>) (df: DataFrame) : DataFrame =
        df.Select exprs
    /// <summary> Add or replace a single column in the DataFrame. </summary>
    let withColumn (expr: Expr) (df: DataFrame) : DataFrame =
        df.WithColumns expr
    /// <summary> Add or replace multiple columns in the DataFrame. </summary>
    let withColumns (exprs: seq<Expr>) (df: DataFrame) : DataFrame =
        df.WithColumns exprs
    /// <summary> Sort (Order By) the DataFrame in ascending order. </summary>
    let sortAscending (columns:seq<IColumnExpr>)(df: DataFrame) : DataFrame =
        df.Sort(columns,descending=false)
    /// <summary> Sort (Order By) the DataFrame in descending order. </summary>
    let sortDescending (columns:seq<IColumnExpr>)(df: DataFrame) : DataFrame =
        df.Sort(columns,descending=true)
    /// <summary> Order By the DataFrame in ascending order. </summary>
    let orderByAscending (columns: seq<IColumnExpr>) (df: DataFrame) = sortAscending columns df
    /// <summary> Order By the DataFrame in descending order. </summary>
    let orderByDescending (columns: seq<IColumnExpr>) (df: DataFrame) = sortDescending columns df
    /// <summary> Group by keys and apply aggregations. </summary>
    let groupBy (keys: seq<Expr>)(df: DataFrame) : GroupBy =
        df.GroupBy(keys)
    /// <summary> Having clause for GroupBy. </summary>
    let having(predicate: Expr) (builder: GroupBy) = builder.Having(predicate)
    /// <summary> Aggregation clause for GroupBy. </summary>
    let agg(aggs:seq<Expr>)(builder: GroupBy) = builder.Agg(aggs)
    /// <summary> Perform a join between two DataFrames. </summary>
    let join (other: DataFrame) (leftOn: seq<Expr>) (rightOn:seq<Expr>) (how: JoinType) (left: DataFrame) : DataFrame =
        left.Join (other, leftOn, rightOn, how)
    /// <summary> Perform a join between two DataFrames on a common column. </summary>
    let joinOn(other: DataFrame) (on:seq<Expr>) (how: JoinType) (left: DataFrame) : DataFrame =
        left.Join(other,on,how)
    /// <summary> Get the first n rows of the DataFrame. </summary>
    let head (n: int) (df: DataFrame) : DataFrame =
        df.Head n
    /// <summary> Get the last n rows of the DataFrame. </summary>
    let tail (n: int) (df: DataFrame) : DataFrame =
        df.Tail n
    /// <summary> Slice rows starting at offset with length. </summary>
    let slice (offset: int64) (length: uint64) (df: DataFrame) : DataFrame =
        df.Slice(offset, length)
    /// <summary> Explode list-like columns into multiple rows. </summary>
    let explode (columns: #IColumnExpr) (df: DataFrame) : DataFrame =
        df.Explode(columns.ToSelector())
    /// <summary> Decompose multiple struct columns. </summary>
    let unnestColumns(columns: seq<string>) (df:DataFrame) : DataFrame =
        df.UnnestColumns columns
    /// <summary> Drop columns from the DataFrame. </summary>
    let drop(columns:#IColumnExpr) (df:DataFrame):DataFrame =
        df.Drop columns
    /// <summary> Drop null values from the DataFrame. </summary>
    let dropNulls(df: DataFrame) : DataFrame =
        df.DropNulls()
    /// <summary> Drop null values from the DataFrame with specific subset. </summary>
    let dropNullsOn (subset: #IColumnExpr) (df: DataFrame) : DataFrame =
        df.DropNulls subset
    /// <summary> Drop NaN values from the DataFrame. </summary>
    let dropNans(df: DataFrame) : DataFrame =
        df.DropNans()
    /// <summary> Drop NaN values from the DataFrame with a condition. </summary>
    let dropNansOn (subset: #IColumnExpr) (df: DataFrame) : DataFrame =
        df.DropNans subset
    /// <summary> Return unique rows from the DataFrame. </summary>
    let unique (df: DataFrame) : DataFrame =
        df.Unique()
    /// <summary>
    /// Drop duplicate rows based on specific subset expression(s), keeping the first occurrence.
    /// </summary>
    let uniqueOn (subset: seq<Expr>) (df: DataFrame) : DataFrame =
        df.Unique(subset)
    /// <summary>
    /// Drop duplicate rows based on subset with a specified keep strategy (First, Last, None, Any).
    /// </summary>
    let uniqueWith (subset: seq<Expr>) (keep: UniqueKeepStrategy) (df: DataFrame) : DataFrame =
        df.Unique(subset, keep = keep)
    /// <summary> Rename columns based on a mapping dictionary or key-value pairs. </summary>
    let rename (mapping: seq<string * string>) (df: DataFrame) : DataFrame =
        df.Rename mapping
    /// <summary>
    /// Horizontally stack columns to the DataFrame.
    /// </summary>
    let hstack (columns: seq<Series> ) (df: DataFrame) : DataFrame =
        df.HStack columns
    /// <summary>
    /// Group by the given columns and return the groups as separate dataframes.
    /// </summary>
    let partitionBy (columns: seq<string>) (df: DataFrame) : DataFrame[] =
        df.PartitionBy columns
    /// <summary>
    /// Vertically stack another DataFrame to this one.
    /// </summary>
    let vstack (other: DataFrame) (df: DataFrame) : DataFrame =
        df.VStack other
    /// <summary>
    /// Convert the DataFrame to a sequence of records.
    /// </summary>
    let toRecords<'T> (df: DataFrame) : seq<'T> =
        df.ToRecords<'T>()
    /// <summary>
    /// Convert the DataFrame to a sequence of structs.
    /// </summary>
    let toStruct<'T> (name: string) (df: DataFrame) : Series =
        df.ToStruct(name)
    /// <summary>
    /// Convert the DataFrame to a map of column name to Series.
    /// </summary>
    let toMap (df: DataFrame) : Map<string, Series> =
        df.ToMap()
    /// <summary>
    /// Convert the DataFrame to an Arrow RecordBatch.
    /// </summary>
    let toArrow (df: DataFrame) : RecordBatch =
        df.ToArrow()
    /// <summary>
    /// Convert the DataFrame to a LazyFrame.
    /// </summary>
    let asLazy (df: DataFrame) : LazyFrame =
        df.Lazy()
    /// <summary>
    /// Print the DataFrame to Console (Table format).
    /// </summary>
    let show (df: DataFrame) : DataFrame =
        df.Show()
        df
    /// <summary> Get the schema of the DataFrame. </summary>
    let collectSchema (df: DataFrame) : PolarsSchema =
        df.Schema
    /// <summary> Get the column names of the DataFrame. </summary>
    let columnNames (df: DataFrame) : string[] =
        df.Columns
    /// <summary> Reverse the order of the DataFrame. </summary>
    let reverse (df: DataFrame) : DataFrame =
        df.Reverse()
    /// <summary> Fill null values in all columns with a specified value. </summary>
    let fillNull (fillValue: Expr)(df: DataFrame)  : DataFrame =
        df.FillNull fillValue
    /// <summary> Fill NaN values in all columns with a specified value. </summary>
    let fillNan (fillValue: Expr)(df: DataFrame)  : DataFrame =
        df.FillNan fillValue
    /// <summary>
    /// Unpivot (Melt) the DataFrame from wide to long format using Selectors.
    /// Default column names ("variable", "value") will be used.
    /// </summary>
    let unpivot (index: Selector) (on: Selector) (df: DataFrame) : DataFrame =
        df.Unpivot(index, on)
    /// <summary>
    /// Alias for DataFrame.unpivot.
    /// </summary>
    let melt (index: Selector) (on: Selector) (df: DataFrame) : DataFrame =
        df.Unpivot(index, on)
    /// <summary>
    /// Unpivot (Melt) the DataFrame using Selectors with custom variable and value column names.
    /// </summary>
    let unpivotNamed (index: Selector) (on: Selector) (variableName: string) (valueName: string) (df: DataFrame) : DataFrame =
        df.Unpivot(index, on, variableName = variableName, valueName = valueName)
    /// <summary>
    /// Alias for DataFrame.unpivotNamed.
    /// </summary>
    let meltNamed (index: Selector) (on: Selector) (variableName: string) (valueName: string) (df: DataFrame) : DataFrame =
        unpivotNamed index on variableName valueName df
    /// <summary>
    /// Vertically concat DataFrames (Standard concat).
    /// </summary>
    let concatVertical (dfs: seq<DataFrame>) : DataFrame =
        DataFrame.ConcatVertical dfs
    /// <summary>
    /// Horizontally concat DataFrames.
    /// </summary>
    let concatHorizontal (dfs: seq<DataFrame>) : DataFrame =
        DataFrame.ConcatHorizontal(dfs, checkDuplicates=true)
    /// <summary>
    /// Horizontally concat DataFrames (Allow duplicates).
    /// </summary>
    let concatHorizontalNoCheck (dfs: seq<DataFrame>) : DataFrame =
        DataFrame.ConcatHorizontal(dfs, checkDuplicates=false)
    /// <summary>
    /// Diagonally concat DataFrames
    /// </summary>
    let concatDiagonal (dfs: seq<DataFrame>) : DataFrame =
        DataFrame.ConcatDiagonal dfs
    /// <summary>
    /// Repeatedly applies an update function to a DataFrame for n steps,
    /// returning the final materialized state.
    /// </summary>
    let iterate (steps: int) (stepFn: int -> DataFrame -> DataFrame) (initial: DataFrame) : DataFrame =
        let mutable current = initial
        for step = 1 to steps do
            current <- stepFn step current
        current
    /// <summary>
    /// Pipeline combinator for DataFrame.IterRows: ('T -> unit) -> DataFrame -> unit.
    /// </summary>
    let inline iterRows<'T> (action: 'T -> unit) (df: DataFrame) : unit =
        df.IterRows<'T> action

    /// <summary>
    /// Pipeline combinator for DataFrame.IteriRows: (int64 -> 'T -> unit) -> DataFrame -> unit.
    /// </summary>
    let inline iteriRows<'T> (action: int64 -> 'T -> unit) (df: DataFrame) : unit =
        df.IteriRows<'T> action
    /// <summary>
    /// Pipeline combinator for DataFrame.Iter2: ('T1 -> 'T2 -> unit) -> DataFrame -> DataFrame -> unit.
    /// </summary>
    let inline iter2<'T1, 'T2> (action: 'T1 -> 'T2 -> unit) (df1: DataFrame) (df2: DataFrame) : unit =
        df1.Iter2<'T1, 'T2>(df2, action)

    /// <summary>
    /// Pipeline combinator for DataFrame.Iteri2: (int64 -> 'T1 -> 'T2 -> unit) -> DataFrame -> DataFrame -> unit.
    /// </summary>
    let inline iteri2<'T1, 'T2> (action: int64 -> 'T1 -> 'T2 -> unit) (df1: DataFrame) (df2: DataFrame) : unit =
        df1.Iteri2<'T1, 'T2>(df2, action)

    /// <summary>
    /// Applies a folding function across all columns of the DataFrame from left to right,
    /// using the first column as the seed.
    /// </summary>
    let inline fold (folder: Series -> Series -> Series) (df: DataFrame) : Series =
        df.Fold folder

    /// <summary>
    /// Applies a folding function across all columns of the DataFrame from left to right,
    /// threading an accumulator state.
    /// </summary>
    let inline foldState (folder: 'State -> Series -> 'State) (state: 'State) (df: DataFrame) : 'State =
        df.Fold(state, folder)

    /// <summary>
    /// Transforms each column in the DataFrame using a column-wise mapping function (Series -> Series).
    /// </summary>
    /// <param name="mapping">The function applied to each column Series.</param>
    /// <returns>A new DataFrame composed of the transformed Series.</returns>
    let inline map(mapping: Series -> Series) (df:DataFrame) : DataFrame =
        df.Map mapping

    /// <summary>
    /// Transforms DataFrame rows into a new DataFrame using a strongly-typed F# Record mapping function ('TIn -> 'TOut).
    /// </summary>
    /// <typeparam name="'TIn">Input F# Record type representing the source row.</typeparam>
    /// <typeparam name="'TOut">Output F# Record type representing the transformed row.</typeparam>
    /// <param name="mapping">The pure transformation function applied to each row.</param>
    /// <returns>A new DataFrame materialized from the mapped output records.</returns>
    let inline mapRows<'TIn, 'TOut>(mapping: 'TIn -> 'TOut)(df:DataFrame) : DataFrame =
        df.MapRows<'TIn, 'TOut> mapping

    /// <summary>
    /// Returns a row enumerator for the DataFrame, allowing iteration over rows of type 'T.
    /// </summary>
    /// <typeparam name="'T">The type of the row to enumerate.</typeparam>
    /// <param name="df">The DataFrame to enumerate.</param>
    /// <returns>A row enumerator for the DataFrame.</returns>
    let inline rows<'T>(df:DataFrame): RowEnumerator<'T> =
        df.Rows<'T>()
