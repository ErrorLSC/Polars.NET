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
            if configRows.HasValue && configRows.Value > 0 then configRows.Value
            else 10 
            
        let totalRows = df.Height
        let n = Math.Min(int64 rowsToShow, totalRows)
        
        use pSchema = df.Schema
        let colNames = pSchema.Names 
        let rawColCount = colNames.Length

        let configCols = CoreConfig.TableMaxCols
        let maxColsToShow = 
            if configCols.HasValue && configCols.Value > 0 then configCols.Value
            else 8

        let isColTruncated = rawColCount > maxColsToShow
        let colCount = if isColTruncated then maxColsToShow else rawColCount

        use previewDf = df.Head(int n)
        use batch = ArrowFfiBridge.ExportDataFrame previewDf.Handle
        let arrowSchema = batch.Schema 

        let sb = StringBuilder()
        
        // CSS Style
        sb.Append("""<style>
            .pl-frame { font-family: "Consolas", "Monaco", monospace; font-size: 13px; border-collapse: collapse; border: 1px solid rgba(128, 128, 128, 0.2); }
            .pl-frame th { font-weight: bold; text-align: left; padding: 6px 12px; border-bottom: 2px solid rgba(128, 128, 128, 0.3); }
            .pl-frame td { padding: 6px 12px; border-bottom: 1px solid rgba(128, 128, 128, 0.2); white-space: pre; }
            .pl-frame tr:nth-child(even) { background-color: rgba(128, 128, 128, 0.05); }
            .pl-frame tr:hover { background-color: rgba(128, 128, 128, 0.1); }
            .pl-dim { font-family: sans-serif; font-size: 12px; opacity: 0.8; margin-bottom: 8px; }
            .pl-type { font-size: 10px; color: rgba(128, 128, 128, 0.8); display: block; margin-top: 2px; font-weight: normal; }
            .pl-null { color: rgba(128, 128, 128, 0.5); font-style: italic; }
        </style>""") |> ignore

        // Dimension Info
        sb.AppendFormat("<div class='pl-dim'>Polars DataFrame: <b>({0} rows, {1} columns)</b></div>", totalRows, rawColCount) |> ignore
        
        // Build Table
        sb.Append "<div style='overflow-x:auto'><table class='pl-frame'>" |> ignore
        
        // --- Table Head  ---
        sb.Append "<thead><tr>" |> ignore
        for c in 0 .. colCount - 1 do
            let name = colNames.[c]
            let dtype = pSchema.[name] 
            sb.AppendFormat("<th>{0}<span class='pl-type'>{1}</span></th>", 
                System.Net.WebUtility.HtmlEncode name, 
                dtype.ToString()) |> ignore 
        if isColTruncated then
            sb.Append "<th>...</th>" |> ignore
        sb.Append "</tr></thead>" |> ignore

        // --- Table Body  ---
        sb.Append "<tbody>" |> ignore
        let rowCount = batch.Length

        for i in 0 .. rowCount - 1 do
            sb.Append "<tr>" |> ignore
            for j in 0 .. colCount - 1 do
                let colArray = batch.Column j
                let arrowField = arrowSchema.GetFieldByIndex j
                
                // Get raw string
                let rawStr = colArray.FormatValue i
                
                // Modify float/double value formatting
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
                    sb.Append "<td class='pl-null'>null</td>" |> ignore
                else
                    // Truncate long strings
                    let finalStr = if valStr.Length > 100 then valStr.Substring(0, 97) + "..." else valStr
                    sb.AppendFormat("<td>{0}</td>", System.Net.WebUtility.HtmlEncode finalStr) |> ignore
            if isColTruncated then
                sb.Append "<td style='font-style:italic; opacity: 0.5; text-align:center'>...</td>" |> ignore
            sb.Append "</tr>" |> ignore

        // Footer for remaining rows
        let finalDisplayColSpan = if isColTruncated then colCount + 1 else colCount
        if totalRows > int64 rowsToShow then
             let remaining = totalRows - int64 rowsToShow
             sb.AppendFormat("<tr><td colspan='{0}' style='text-align:center; font-style:italic; opacity: 0.6; padding: 10px'>... {1} more rows ...</td></tr>", finalDisplayColSpan, remaining) |> ignore

        sb.Append "</tbody></table></div>" |> ignore
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