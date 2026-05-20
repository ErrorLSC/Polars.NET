namespace Polars.FSharp

[<AutoOpen>]
module DataFrameTranspose =
    open Polars.NET.Core
    open System
    type DataFrame with
        /// <summary>Transpose a DataFrame over the diagonal.</summary>
        /// <param name="includeHeader">If true, the column names will be added as the first column.</param>
        /// <param name="headerName">Name of the header column if includeHeader is true (default "column").</param>
        member this.Transpose(?includeHeader: bool, ?headerName: string) =
            let includeHeader = defaultArg includeHeader false
            let headerName = defaultArg headerName "column"
            let keepNamesAs = if includeHeader then headerName else null
            let newHandle = PolarsWrapper.DataFrameTranspose(this.Handle, keepNamesAs, null, null)
            new DataFrame(newHandle)

        /// <summary>Transpose a DataFrame over the diagonal, using an existing column for column names.</summary>
        /// <param name="columnName">Existing column name to use for naming transposed columns.</param>
        /// <inheritdoc cref="Transpose(bool, string)"/>
        member this.Transpose(columnName: string, ?includeHeader: bool, ?headerName: string) =
            if System.String.IsNullOrWhiteSpace columnName then
                raise (ArgumentException("Column name cannot be null or empty.", nameof(columnName)))
            let includeHeader = defaultArg includeHeader false
            let headerName = defaultArg headerName "column"
            let keepNamesAs = if includeHeader then headerName else null
            let newHandle = PolarsWrapper.DataFrameTranspose(this.Handle, keepNamesAs, columnName, null)
            new DataFrame(newHandle)

        /// <summary>Transpose a DataFrame over the diagonal, using custom names for transposed columns.</summary>
        /// <param name="customNames">Sequence of names for the value columns in the transposed data.</param>
        /// <inheritdoc cref="Transpose(bool, string)"/>
        member this.Transpose(customNames: seq<string>, ?includeHeader: bool, ?headerName: string) =
            if isNull (box customNames) then nullArg (nameof(customNames))
            let includeHeader = defaultArg includeHeader false
            let headerName = defaultArg headerName "column"
            let keepNamesAs = if includeHeader then headerName else null
            let namesArray = customNames |> Seq.toArray
            let newHandle = PolarsWrapper.DataFrameTranspose(this.Handle, keepNamesAs, null, namesArray)
            new DataFrame(newHandle)