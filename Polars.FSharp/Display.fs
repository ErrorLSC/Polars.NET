namespace Polars.FSharp

open System
open System.IO
open System.Text
open Microsoft.DotNet.Interactive.Formatting
open Polars.NET.Core.Arrow
open Polars.NET.Core
open Apache.Arrow.Types
/// <summary>
/// Display utilities for DataFrame and LazyFrame in interactive environments.
/// </summary>
[<AutoOpen>]
module Display =

    /// <summary>
    /// Display DataFrame as Html Table.
    /// Respects global Polars configurations from CoreConfig.
    /// </summary>
    let toHtml (df: DataFrame) =
        let configRows = CoreConfig.TableMaxRows
        let rowsToShow = 
            if configRows.HasValue && configRows.Value > 0 then configRows.Value else 10 
            
        let totalRows = df.Height
        let n = Math.Min(int64 rowsToShow, totalRows)
        
        use pSchema = df.Schema
        let colNames = pSchema.Names 
        let rawColCount = colNames.Length

        let configCols = CoreConfig.TableMaxCols
        let maxColsToShow = 
            if configCols.HasValue && configCols.Value > 0 then configCols.Value else 8

        let isColTruncated = rawColCount > maxColsToShow
        let colCount = if isColTruncated then maxColsToShow else rawColCount

        use previewDf = df.Head(int n)
        use batch = ArrowFfiBridge.ExportDataFrame previewDf.Handle
        let arrowSchema = batch.Schema 
        let rowCount = batch.Length

        let cachedCols = 
            Array.init colCount (fun j -> 
                batch.Column j, arrowSchema.GetFieldByIndex j)

        let htmlLines = [
            // --- CSS ---
            yield """<style>
                .pl-frame { font-family: "Consolas", "Monaco", monospace; font-size: 13px; border-collapse: collapse; border: 1px solid rgba(128, 128, 128, 0.2); }
                .pl-frame th { font-weight: bold; text-align: left; padding: 6px 12px; border-bottom: 2px solid rgba(128, 128, 128, 0.3); }
                .pl-frame td { padding: 6px 12px; border-bottom: 1px solid rgba(128, 128, 128, 0.2); white-space: pre; }
                .pl-frame tr:nth-child(even) { background-color: rgba(128, 128, 128, 0.05); }
                .pl-frame tr:hover { background-color: rgba(128, 128, 128, 0.1); }
                .pl-dim { font-family: sans-serif; font-size: 12px; opacity: 0.8; margin-bottom: 8px; }
                .pl-type { font-size: 10px; color: rgba(128, 128, 128, 0.8); display: block; margin-top: 2px; font-weight: normal; }
                .pl-null { color: rgba(128, 128, 128, 0.5); font-style: italic; }
            </style>"""

            // --- Dimension Info ---
            yield $"<div class='pl-dim'>Polars DataFrame: <b>({totalRows} rows, {rawColCount} columns)</b></div>"
            yield "<div style='overflow-x:auto'><table class='pl-frame'>"
            
            // --- TableHead ---
            yield "<thead><tr>"
            for c in 0 .. colCount - 1 do
                let name = colNames.[c]
                let dtype = pSchema.[name] 
                yield $"<th>{System.Net.WebUtility.HtmlEncode name}<span class='pl-type'>{dtype}</span></th>"
            if isColTruncated then yield "<th>...</th>"
            yield "</tr></thead>"

            // --- TableBody ---
            yield "<tbody>"
            for i in 0 .. rowCount - 1 do
                yield "<tr>"
                for j in 0 .. colCount - 1 do
                    let colArray, arrowField = cachedCols.[j]
                    let rawStr = colArray.FormatValue i
                    
                    let valStr = 
                        if rawStr = "null" then "null"
                        else
                            match arrowField.DataType.TypeId with
                            | ArrowTypeId.Double -> 
                                match Double.TryParse rawStr with
                                | true, v -> v.ToString "G10"
                                | _ -> rawStr
                            | ArrowTypeId.Float ->
                                match Single.TryParse rawStr with
                                | true, v -> v.ToString "G7"
                                | _ -> rawStr
                            | _ -> rawStr

                    if valStr = "null" then
                        yield "<td class='pl-null'>null</td>"
                    else
                        let finalStr = if valStr.Length > 100 then valStr.Substring(0, 97) + "..." else valStr
                        yield $"<td>{System.Net.WebUtility.HtmlEncode finalStr}</td>"
                        
                if isColTruncated then
                    yield "<td style='font-style:italic; opacity: 0.5; text-align:center'>...</td>"
                yield "</tr>"

            // --- Footer ---
            let finalDisplayColSpan = if isColTruncated then colCount + 1 else colCount
            if totalRows > int64 rowsToShow then
                let remaining = totalRows - int64 rowsToShow
                yield $"<tr><td colspan='{finalDisplayColSpan}' style='text-align:center; font-style:italic; opacity: 0.6; padding: 10px'>... {remaining} more rows ...</td></tr>"

            yield "</tbody></table></div>"
        ]

        let sb = StringBuilder()
        htmlLines |> List.iter (fun line -> sb.AppendLine line |> ignore)
        sb.ToString()

    /// <summary>
    /// Init notebook support
    /// </summary>
    let init () =
        Formatter.Register<DataFrame>(
            Action<DataFrame, TextWriter>(fun df writer -> 
                writer.Write(toHtml df)
            ),
            "text/html"
        )
        
        Formatter.Register<LazyFrame>(
            Action<LazyFrame, TextWriter>(fun lf writer -> 
                let plan = lf.Explain true 
                
                use schema = lf.Schema
                let schemaStr = schema.ToString()
                
                let html = $"""
                <div style="font-family: monospace;">
                    <div style="background: rgba(128, 128, 128, 0.1); padding:5px; border-bottom:1px solid rgba(128, 128, 128, 0.2); font-weight:bold">Polars LazyFrame</div>
                    <div style="padding:10px">
                        <div><strong>Schema:</strong> {System.Net.WebUtility.HtmlEncode schemaStr}</div>
                        <br/>
                        <strong>Optimized Plan:</strong>
                        <pre style="background: rgba(128, 128, 128, 0.05); padding:10px; border:1px solid rgba(128, 128, 128, 0.2);">{System.Net.WebUtility.HtmlEncode plan}</pre>
                    </div>
                </div>
                """
                writer.Write html
            ),
            "text/html"
        )