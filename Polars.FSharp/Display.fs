namespace Polars.FSharp

open System
open System.IO
open System.Text
open Microsoft.DotNet.Interactive.Formatting
open Polars.NET.Core.Arrow
open Apache.Arrow.Types
/// <summary>
/// Display utilities for DataFrame and LazyFrame in interactive environments.
/// </summary>
[<AutoOpen>]
module Display =

    /// <summary>
    /// Display DataFrame as Html Table
    /// </summary>
    let toHtml (df: DataFrame) =
        let rowsToShow = 10
        let totalRows = df.Height
        let n = Math.Min(int64 rowsToShow, totalRows)
        

        use pSchema = df.Schema
        let colNames = pSchema.Names 
        let colCount = colNames.Length

        use previewDf = df.Head(int n)
        use batch = ArrowFfiBridge.ExportDataFrame previewDf.Handle
        let arrowSchema = batch.Schema 

        let sb = StringBuilder()
        
        // CSS Style 
        sb.Append("""<style>
            .pl-frame { font-family: "Consolas", "Monaco", monospace; font-size: 13px; border-collapse: collapse; border: 1px solid #e0e0e0; }
            .pl-frame th { background-color: #f0f0f0; font-weight: bold; text-align: left; padding: 6px 12px; border-bottom: 2px solid #ccc; }
            .pl-frame td { padding: 6px 12px; border-bottom: 1px solid #f0f0f0; white-space: pre; }
            .pl-frame tr:nth-child(even) { background-color: #f9f9f9; }
            .pl-frame tr:hover { background-color: #f1f1f1; }
            .pl-dim { font-family: sans-serif; font-size: 12px; color: #666; margin-bottom: 8px; }
            .pl-type { font-size: 10px; color: #999; display: block; margin-top: 2px; font-weight: normal; }
            .pl-null { color: #d0d0d0; font-style: italic; }
        </style>""") |> ignore

        // 4. Dimension Info
        sb.AppendFormat("<div class='pl-dim'>Polars DataFrame: <b>({0} rows, {1} columns)</b></div>", totalRows, colCount) |> ignore
        
        // 5. Build Table
        sb.Append "<div style='overflow-x:auto'><table class='pl-frame'>" |> ignore
        
        // --- Table Head  ---
        sb.Append "<thead><tr>" |> ignore
        for name in colNames do
            let dtype = pSchema.[name] 
            sb.AppendFormat("<th>{0}<span class='pl-type'>{1}</span></th>", 
                System.Net.WebUtility.HtmlEncode name, 
                dtype.ToString()) |> ignore 
        sb.Append "</tr></thead>" |> ignore

        // --- Table Body  ---
        sb.Append "<tbody>" |> ignore
        let rowCount = batch.Length
        // assert batch.ColumnCount == colCount

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
            sb.Append "</tr>" |> ignore

        // Footer
        if totalRows > int64 rowsToShow then
             let remaining = totalRows - int64 rowsToShow
             sb.AppendFormat("<tr><td colspan='{0}' style='text-align:center; font-style:italic; color:#999; padding: 10px'>... {1} more rows ...</td></tr>", colCount, remaining) |> ignore

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
                        // --------------
                        
                        let html = $"""
                        <div style="font-family: monospace;">
                            <div style="background:#f4f4f4; padding:5px; border-bottom:1px solid #ddd; font-weight:bold">Polars LazyFrame</div>
                            <div style="padding:10px">
                                <div><strong>Schema:</strong> {System.Net.WebUtility.HtmlEncode schemaStr}</div>
                                <br/>
                                <strong>Optimized Plan:</strong>
                                <pre style="background:#f9f9f9; padding:10px; border:1px solid #eee;">{System.Net.WebUtility.HtmlEncode plan}</pre>
                            </div>
                        </div>
                        """
                        writer.Write html
                    ),
                    "text/html"
                )
    