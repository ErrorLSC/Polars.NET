namespace Polars.FSharp

open System
open System.Text
open Microsoft.DotNet.Interactive.Formatting
open Polars.Native
/// <summary>
/// Display utilities for DataFrame and LazyFrame in interactive environments.
/// </summary>
[<AutoOpen>]
module Display =
    
    let private formatValueHtml (col: Apache.Arrow.IArrowArray) (index: int) : string =
        let raw = Polars.formatValue col index 
        System.Web.HttpUtility.HtmlEncode(raw)

    let toHtml (df: DataFrame) =
        let rowsToShow = 10
        let totalRows = df.Rows
        let n = Math.Min(int64 rowsToShow, totalRows)
        
        // 切片预览
        let previewDf = df |> Polars.head (int n)
        use batch = previewDf.ToArrow()
        let fields = batch.Schema.FieldsList

        let sb = StringBuilder()
        
        // CSS 风格 (仿 Pandas/Polars)
        sb.Append("""<style>
            .pl-frame { font-family: sans-serif; font-size: 13px; border-collapse: collapse; border: none; }
            .pl-frame th { background-color: #e0e0e0; font-weight: bold; text-align: left; padding: 8px; border: none; }
            .pl-frame td { padding: 8px; border-top: 1px solid #f0f0f0; border-bottom: 1px solid #f0f0f0; }
            .pl-frame tr:nth-child(even) { background-color: #f9f9f9; }
            .pl-frame tr:hover { background-color: #f1f1f1; }
            .pl-dim { font-size: 11px; color: #666; margin-bottom: 5px; }
            .pl-type { font-size: 10px; color: #888; font-weight: normal; display: block; }
        </style>""") |> ignore

        // 维度提示
        sb.AppendFormat("<div class='pl-dim'>Polars DataFrame: ({0} rows, {1} columns)</div>", totalRows, df.Columns) |> ignore
        sb.Append "<table class='pl-frame'>" |> ignore
        
        // 表头
        sb.Append "<thead><tr>" |> ignore
        // 索引列头 (可选)
        // sb.Append("<th>#</th>") |> ignore
        for field in fields do
            let typeName = field.DataType.Name
            sb.AppendFormat("<th>{0}<span class='pl-type'>{1}</span></th>", field.Name, typeName) |> ignore
        sb.Append "</tr></thead>" |> ignore

        // 表体
        sb.Append "<tbody>" |> ignore
        let limit = batch.Length
        for i in 0 .. limit - 1 do
            sb.Append "<tr>" |> ignore
            // sb.AppendFormat("<td style='color:#999'>{0}</td>", i) |> ignore // 行号
            for field in fields do
                let col = batch.Column field.Name
                // 调用格式化
                sb.AppendFormat("<td>{0}</td>", formatValueHtml col i) |> ignore
            sb.Append "</tr>" |> ignore
        
        if totalRows > int64 rowsToShow then
             let colspan = fields.Count
             sb.AppendFormat("<tr><td colspan='{0}' style='text-align:center; font-style:italic; color:#999'>... {1} more rows ...</td></tr>", colspan, totalRows - int64 rowsToShow) |> ignore

        sb.Append "</tbody></table>" |> ignore
        sb.ToString()

    // --- 自动注册到 Notebook ---
    // 这是一个静态构造函数技巧，或者让用户手动调用 Register
    // 但最稳妥的是提供一个 Initialize 方法
    let init () =
        // [修复] 显式构造 Action<DataFrame, TextWriter> 消除歧义
        Formatter.Register<DataFrame>(
            Action<DataFrame, IO.TextWriter>(fun df writer -> 
                writer.Write(toHtml df)
            ),
            "text/html"
        )
        
        // [修复] 同理，LazyFrame 也显式构造 Action
        Formatter.Register<LazyFrame>(
            Action<LazyFrame, IO.TextWriter>(fun lf writer -> 
                let plan = System.Web.HttpUtility.HtmlEncode(lf.Explain false)
                let schema = System.Web.HttpUtility.HtmlEncode lf.SchemaRaw
                let html = $"<div style='font-family:monospace'><strong>Execution Plan:</strong><pre>{plan}</pre><strong>Schema:</strong><pre>{schema}</pre></div>"
                writer.Write(html)
            ),
            "text/html"
        )