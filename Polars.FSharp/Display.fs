namespace Polars.FSharp

open System
open System.IO
open System.Text
open Microsoft.DotNet.Interactive.Formatting
open Polars.NET.Core.Arrow
/// <summary>
/// Display utilities for DataFrame and LazyFrame in interactive environments.
/// </summary>
[<AutoOpen>]
module Display =

    /// <summary>
    /// 将 DataFrame 渲染为漂亮的 HTML 表格 (用于 Notebook)。
    /// </summary>
    let toHtml (df: DataFrame) =
        let rowsToShow = 10
        let totalRows = df.Rows
        let n = Math.Min(int64 rowsToShow, totalRows)
        
        // 1. 获取预览数据 (Zero-Copy Export)
        // Head 会返回一个新的 DataFrame，ToArrow 导出为 RecordBatch
        // 注意：ToArrow 返回的是单个 Batch，适合小数据预览
        use previewDf = df.Head(int n)
        use batch = ArrowFfiBridge.ExportDataFrame previewDf.Handle
        let schema = batch.Schema

        let sb = StringBuilder()
        
        // 2. CSS 样式 (仿 Polars 官方风格)
        sb.Append("""<style>
            .pl-frame { font-family: "Consolas", "Monaco", monospace; font-size: 13px; border-collapse: collapse; border: 1px solid #e0e0e0; }
            .pl-frame th { background-color: #f0f0f0; font-weight: bold; text-align: left; padding: 6px 12px; border-bottom: 2px solid #ccc; }
            .pl-frame td { padding: 6px 12px; border-bottom: 1px solid #f0f0f0; white-space: pre; } /* white-space: pre 保留格式 */
            .pl-frame tr:nth-child(even) { background-color: #f9f9f9; }
            .pl-frame tr:hover { background-color: #f1f1f1; }
            .pl-dim { font-family: sans-serif; font-size: 12px; color: #666; margin-bottom: 8px; }
            .pl-type { font-size: 10px; color: #999; display: block; margin-top: 2px; font-weight: normal; }
            .pl-null { color: #d0d0d0; font-style: italic; }
        </style>""") |> ignore

        // 3. 维度信息
        sb.AppendFormat("<div class='pl-dim'>Polars DataFrame: <b>({0} rows, {1} columns)</b></div>", totalRows, df.ColumnNames) |> ignore
        
        // 4. 构建表格
        sb.Append "<div style='overflow-x:auto'><table class='pl-frame'>" |> ignore
        
        // --- 表头 ---
        sb.Append "<thead><tr>" |> ignore
        for field in schema.FieldsList do
            // Arrow DataType Name (e.g., Timestamp, Int64)
            // Polars.NET.Core 没有直接暴露简单的 Type Name 字符串，这里直接用 Arrow 的 Name 即可
            let typeName = field.DataType.Name 
            sb.AppendFormat("<th>{0}<span class='pl-type'>{1}</span></th>", 
                System.Net.WebUtility.HtmlEncode field.Name, 
                typeName) |> ignore
        sb.Append "</tr></thead>" |> ignore

        // --- 表体 ---
        sb.Append "<tbody>" |> ignore
        let rowCount = batch.Length
        let colCount = batch.ColumnCount

        for i in 0 .. rowCount - 1 do
            sb.Append "<tr>" |> ignore
            for j in 0 .. colCount - 1 do
                let colArray = batch.Column j
                
                // [核心] 调用 ArrowExtensions.FormatValue
                // 这就是我们之前写的那个强大的格式化器，支持 Struct/List/Date 等
                let valStr = colArray.FormatValue i
                
                if valStr = "null" then
                    sb.Append "<td class='pl-null'>null</td>" |> ignore
                else
                    // HtmlEncode 防止 XSS 或格式破坏
                    sb.AppendFormat("<td>{0}</td>", System.Net.WebUtility.HtmlEncode(valStr)) |> ignore
            sb.Append "</tr>" |> ignore
        
        // --- 截断提示 ---
        if totalRows > int64 rowsToShow then
             let remaining = totalRows - int64 rowsToShow
             sb.AppendFormat("<tr><td colspan='{0}' style='text-align:center; font-style:italic; color:#999; padding: 10px'>... {1} more rows ...</td></tr>", colCount, remaining) |> ignore

        sb.Append "</tbody></table></div>" |> ignore
        sb.ToString()

    /// <summary>
    /// 初始化 Notebook 支持 (Polyglot Notebooks / Jupyter)。
    /// 注册 DataFrame 和 LazyFrame 的 HTML 格式化器。
    /// </summary>
    let init () =
        // 1. 注册 DataFrame (HTML Table)
        Formatter.Register<DataFrame>(
            Action<DataFrame, TextWriter>(fun df writer -> 
                writer.Write(toHtml df)
            ),
            "text/html"
        )
        
        // 2. 注册 LazyFrame (Execution Plan)
        // LazyFrame 最重要的是看它的执行计划 (Explain)
        // Polars 的 Explain 输出已经是漂亮的 ASCII 树，我们直接包在 <pre> 里即可
        Formatter.Register<LazyFrame>(
            Action<LazyFrame, TextWriter>(fun lf writer -> 
                // Explain(true) = Optimized Plan, Explain(false) = Logical Plan
                // 通常看 Optimized Plan 更用
                let plan = lf.Explain true 
                
                // 获取 Schema (Metadata)
                let schemaStr = lf.SchemaRaw // 假设 LazyFrame 有个获取 Schema 字符串的方法，或者手动构建
                
                let html = $"""
                <div style="font-family: monospace;">
                    <div style="background:#f4f4f4; padding:5px; border-bottom:1px solid #ddd; font-weight:bold">Polars LazyFrame</div>
                    <div style="padding:10px">
                        <strong>Optimized Plan:</strong>
                        <pre style="background:#f9f9f9; padding:10px; border:1px solid #eee;">{System.Net.WebUtility.HtmlEncode(plan)}</pre>
                    </div>
                </div>
                """
                writer.Write html
            ),
            "text/html"
        )
    