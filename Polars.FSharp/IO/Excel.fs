namespace Polars.FSharp

open Polars.NET.Core

[<AutoOpen>]
module Excel =
    type DataFrame with
        /// <summary>
        /// Read an Excel file (.xlsx) into a DataFrame using the high-performance Rust 'calamine' engine.
        /// </summary>
        /// <param name="path">Path to the .xlsx file.</param>
        /// <param name="sheetName">Name of the sheet to read. If specified, takes precedence over sheetIndex.</param>
        /// <param name="sheetIndex">Index of the sheet to read (0-based). Default is 0.</param>
        /// <param name="schema">Optional schema overrides to enforce specific column types.</param>
        /// <param name="hasHeader">Indicates if the first row contains header names. Default is true.</param>
        /// <param name="inferSchemaLen">Number of rows to use for schema inference. Default is 100.</param>
        /// <param name="dropEmptyRows">If true, drop rows where all cells are empty/null. Default is true.</param>
        /// <param name="raiseIfEmpty">If true, raises an error if the sheet is empty. Default is true.</param>
        static member ReadExcel(path: string,
                                ?sheetName: string,
                                ?sheetIndex: uint64,
                                ?schema: PolarsSchema,
                                ?hasHeader: bool,
                                ?inferSchemaLen: uint64,
                                ?dropEmptyRows: bool,
                                ?raiseIfEmpty: bool) : DataFrame =
            
            let sName = Option.toObj sheetName
            let sIdx = defaultArg sheetIndex 0UL
            
            let sHandle = 
                match schema with 
                | Some s -> s.Handle 
                | None -> null

            let header = defaultArg hasHeader true
            let infer = defaultArg inferSchemaLen 100UL
            let dropEmpty = defaultArg dropEmptyRows true
            let raiseEmpty = defaultArg raiseIfEmpty true

            let h = PolarsWrapper.ReadExcel(
                path, 
                sName, 
                sIdx, 
                sHandle, 
                header, 
                infer, 
                dropEmpty, 
                raiseEmpty
            )
            
            new DataFrame(h)
        /// <summary>
        /// Write the DataFrame to an Excel file (.xlsx) using the native high-performance engine.
        /// <para>UInt64, Int128, UInt128 are automatically written as Text to preserve precision.</para>
        /// </summary>
        /// <param name="path">Destination file path.</param>
        /// <param name="sheetName">Optional sheet name (default: "Sheet1").</param>
        /// <param name="dateFormat">Optional Excel format string for Date columns (e.g. "yyyy-mm-dd").</param>
        /// <param name="datetimeFormat">Optional Excel format string for Datetime columns (e.g. "yyyy-mm-dd hh:mm:ss").</param>
        member this.WriteExcel(path: string, 
                            ?sheetName: string, 
                            ?dateFormat: string, 
                            ?datetimeFormat: string) =
            
            // F# Option -> C# null handling
            let sName = Option.toObj sheetName
            let dFmt = Option.toObj dateFormat
            let dtFmt = Option.toObj datetimeFormat
            
            PolarsWrapper.WriteExcel(this.Handle, path, sName, dFmt, dtFmt)