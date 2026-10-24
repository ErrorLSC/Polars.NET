open System
open System.IO
open System.Text
open System.Text.RegularExpressions

let repoRoot = __SOURCE_DIRECTORY__ + "/.."
let docsDir = Path.Combine(repoRoot, "docs")
let articlesDir = Path.Combine(docsDir, "articles")
let publicDir = Path.Combine(docsDir, "public")

if not (Directory.Exists publicDir) then
    Directory.CreateDirectory publicDir |> ignore

let baseUrl = "https://errorlsc.github.io/Polars.NET"

printfn "=== Generating llms.txt & llms-full.txt ==="

// -------------------------------------------------------------
// 1. Generate docs/public/llms.txt (Standard Lightweight Index)
// -------------------------------------------------------------

let llmsTxtContent = 
    $"""# Polars.NET

> Polars.NET is a high-performance DataFrame library for the .NET ecosystem (C# and F#), powered by the Polars Rust core and Apache Arrow columnar memory format with multi-threaded SIMD execution.

Polars.NET provides idiomatic, zero-copy bindings across multiple .NET programming paradigms:
- **Polars.CSharp**: Object-oriented Fluent API, C# 12 collection expressions, and disposable lifetime management.
- **Polars.FSharp**: Functional pipeline combinators (`pl.*`), sequence modules, and zero-allocation struct enumerators (`series.As<'T>()`, `df.Rows<'T>()`).
- **Polars.NET.Linq**: Native LINQ Query Provider compiling C# LINQ syntax and F# `query {{ }}` computation expressions directly into native Rust Polars `LazyPlan` nodes.
- **Polars.NET.Core**: Low-level FFI bridges, Arrow C Data Interface interop, and native Lakehouse storage engines.

## Core Concepts & Getting Started

- [Quick Start Guide]({baseUrl}/articles/quickstart): 5-minute introduction covering installation, ingestion, filtering, grouping, aggregations, and execution.
- [Introduction]({baseUrl}/articles/intro): Architecture overview, Arrow memory model, streaming engine, and .NET ecosystem positioning.
- [Expressions & Computations]({baseUrl}/articles/expressions): Column selection, arithmetic, Boolean logic, type casting, string transformations, temporal parsing, and list operations.
- [LazyFrame & Query Optimization]({baseUrl}/articles/lazy): Execution plans (`Explain()`), logical optimization, predicate & projection pushdown, common subplan elimination, and streaming out-of-core execution.
- [Time Series & Temporal Analytics]({baseUrl}/articles/timeseries): Temporal types (`Date`, `Datetime`, `Duration`), `Expr.Dt` namespaces, timezone conversions, rolling window aggregations, and dynamic group-by.
- [Delta Lake & ACID Lakehouse]({baseUrl}/articles/delta): Native `delta-rs` ACID transactions, Time Travel, metadata history, file vacuuming/compaction, CDC feed, and SQL MERGE.

## Language Paradigms & Idioms

- [Idiomatic C# Guide]({baseUrl}/articles/csharp-idioms): Modern C# patterns, collection expressions (`[ Col("a"), Col("b") ]`), `using static Polars.CSharp.Polars`, pattern matching, and record ingestion.
- [Idiomatic F# Guide]({baseUrl}/articles/fsharp-idioms): Functional pipelines (`|>`), `pl.*` expression combinators, zero-allocation struct enumerators (`SeriesEnumerator<'T>`, `RowEnumerator<'T>`), and higher-order mapping.
- [LINQ & Query Computation Expressions]({baseUrl}/articles/linq): Standard C# LINQ and F# `query {{ }}` CE compiling into 100%% native Rust LazyPlans without managed loop overhead.
- [Ecosystem & Native Interop]({baseUrl}/articles/interop): Zero-copy Apache Arrow C Data Interface, Arrow Flight, ADBC, ADO.NET DbDataReader ingestion, ML.NET `IDataView`, and TorchSharp / System.Numerics Tensor bridges.

## Complete Context & LLM Ingestion

- [Full Consolidated Reference (llms-full.txt)]({baseUrl}/llms-full.txt): Complete, concatenated documentation containing all chapters, anti-hallucination guardrails, and API cheat sheets in a single file for zero-shot LLM ingestion.
"""

let llmsTxtPath = Path.Combine(publicDir, "llms.txt")
File.WriteAllText(llmsTxtPath, llmsTxtContent.Replace("\r\n", "\n"))
printfn "Created: %s" llmsTxtPath

// -------------------------------------------------------------
// 2. Helper to clean VitePress markdown for LLM consumption
// -------------------------------------------------------------

let cleanMarkdownContent (content: string) =
    let mutable text = content.Replace("\r\n", "\n")
    
    // Strip YAML frontmatter (--- ... ---)
    if text.StartsWith("---") then
        let secondIndex = text.IndexOf("\n---", 3)
        if secondIndex >= 0 then
            text <- text.Substring(secondIndex + 4).TrimStart()
            
    // Remove VitePress container tags like ::: code-group, ::: tip, :::
    text <- Regex.Replace(text, @"^:::[^\n]*\n", "", RegexOptions.Multiline)
    text <- Regex.Replace(text, @"^:::\s*$", "", RegexOptions.Multiline)
    
    // Remove HTML include comments like <!--@include: ...-->
    text <- Regex.Replace(text, @"<!--@include:[^>]+-->", "")
    
    // Remove standalone <script> or <style> tags if any
    text <- Regex.Replace(text, @"<style[^>]*>[\s\S]*?</style>", "")
    text <- Regex.Replace(text, @"<script[^>]*>[\s\S]*?</script>", "")
    
    text.Trim()

// -------------------------------------------------------------
// 3. Generate docs/public/llms-full.txt (Full Concatenated Context)
// -------------------------------------------------------------

let orderedArticles = [
    "intro.md", "Introduction & Architecture"
    "quickstart.md", "Quick Start Guide"
    "expressions.md", "Expressions & Column Computations"
    "lazy.md", "LazyFrame, Optimization & Streaming"
    "timeseries.md", "Time Series & Temporal Operations"
    "delta.md", "Delta Lake & ACID Lakehouse Storage"
    "csharp-idioms.md", "Idiomatic C# Patterns & Practices"
    "fsharp-idioms.md", "Idiomatic F# Patterns & Zero-Allocation Struct Enumerators"
    "linq.md", "Native LINQ & F# Query Computation Expressions"
    "interop.md", "Ecosystem Interoperability & Zero-Copy Bridges"
]

let sb = StringBuilder()

// Header
sb.AppendLine("# Polars.NET Complete LLM Reference & Context Bundle") |> ignore
sb.AppendLine() |> ignore
sb.AppendLine("> Machine-readable reference document designed for Large Language Models (LLMs) and AI Coding Agents.") |> ignore
sb.AppendLine("> Contains complete architectural guidelines, anti-hallucination guardrails, idioms, and full chapter documentation.") |> ignore
sb.AppendLine() |> ignore

// Anti-Hallucination Guardrails
sb.AppendLine("## Anti-Hallucination Guardrails & Critical Rules") |> ignore
sb.AppendLine() |> ignore
sb.AppendLine("When generating or reviewing code for Polars.NET, always adhere to these verified rules:") |> ignore
sb.AppendLine() |> ignore
sb.AppendLine("1. **Native Runtime Dependencies**:") |> ignore
sb.AppendLine("   - Polars.NET is NOT a pure managed library; it wraps the high-performance Rust Polars engine.") |> ignore
sb.AppendLine("   - Applications must reference the appropriate native binary package (e.g. `Polars.NET.Native.linux-x64`, `Polars.NET.Native.win-x64`, `Polars.NET.Native.osx-arm64`) alongside `Polars.CSharp` or `Polars.FSharp`.") |> ignore
sb.AppendLine() |> ignore
sb.AppendLine("2. **DataFrame Ingestion & Factory Methods**:") |> ignore
sb.AppendLine("   - **C#**: Use `DataFrame.FromRows(list)` (the most intuitive .NET pattern for `List<T>`, `IEnumerable<T>`, arrays, and records) or `DataFrame.FromColumns(new { ColA = new[] { 1, 2 }, ColB = new[] { \"x\", \"y\" } })`.") |> ignore
sb.AppendLine("   - **F#**: Use `DataFrame.ofRecords records` or `DataFrame.ofColumns [ \"ColA\", Series.ofValues [...] ]`.") |> ignore
sb.AppendLine("   - File IO: `DataFrame.ReadCsv`, `DataFrame.ReadParquet`, `DataFrame.ReadJson`, `DataFrame.ReadIpc`.") |> ignore
sb.AppendLine() |> ignore
sb.AppendLine("3. **Expression Syntax & Imports**:") |> ignore
sb.AppendLine("   - In C#: `using Polars.CSharp; using static Polars.CSharp.Polars;` allows writing `Col(\"x\") > 10` and `Lit(5)`.") |> ignore
sb.AppendLine("   - In F#: `open Polars.FSharp` provides the `pl` module: `pl.col(\"x\") .> pl.lit(10)`.") |> ignore
sb.AppendLine("   - Column operations: `.Alias(\"new_name\")`, `.Cast(DataType.Int64)`, `.FillNull(0)`.") |> ignore
sb.AppendLine() |> ignore
sb.AppendLine("4. **Zero Hallucination on ADO.NET / ADBC**:") |> ignore
sb.AppendLine("   - **NEVER generate `ToDataReader()` on DataFrame or LazyFrame** — this method does NOT exist.") |> ignore
sb.AppendLine("   - To ingest from an ADO.NET `DbDataReader`: use `DataFrame.ReadDatabase(reader, batchSize)` or `DataFrame.FromDataReader(reader)`.") |> ignore
sb.AppendLine("   - To stream an ADO.NET reader directly into a Polars LINQ pipeline: use `reader.AsPolarsQueryable<T>(batchSize: 50_000)` from `using Polars.CSharp.Linq;`.") |> ignore
sb.AppendLine("   - For Apache Arrow Database Connectivity (ADBC): use `DataFrame.ReadAdbc(query, connection)` (or in F#: `DataFrame.readAdbc query connection`).") |> ignore
sb.AppendLine() |> ignore
sb.AppendLine("5. **F# Zero-Allocation Struct Enumerators & High-Order Functions**:") |> ignore
sb.AppendLine("   - For fast row traversal without heap allocation: `use rows = df.Rows<'T>()` (returns `RowEnumerator<'T>` struct).") |> ignore
sb.AppendLine("   - For typed series traversal: `use s = series.As<'T>()` (returns `SeriesEnumerator<'T>` struct).") |> ignore
sb.AppendLine("   - Higher order functions: `Rows.fold`, `Rows.reduce`, `Rows.scan`, `Rows.mapToArray`, `df.MapRows`, `Series.map`, `Series.iter`, `Series.exists`, `Series.forall`.") |> ignore
sb.AppendLine() |> ignore
sb.AppendLine("6. **LINQ & Query Computation Expressions**:") |> ignore
sb.AppendLine("   - Direct entrypoints:") |> ignore
sb.AppendLine("     * From DataFrame/LazyFrame: `df.AsQueryable<TRecord>()`.") |> ignore
sb.AppendLine("     * From In-Memory Collections: `collection.AsPolarsQueryable<T>()` transposes `List<T>` / `IEnumerable<T>` straight into Polars LINQ.") |> ignore
sb.AppendLine("     * From Database Streams: `reader.AsPolarsQueryable<T>(batchSize = 50000)` streams ADO.NET readers straight into Polars LINQ.") |> ignore
sb.AppendLine("   - Standard LINQ clauses (`where`, `select`, `group by`, `orderby`, `join`) compile directly to Rust `LazyPlan`.") |> ignore
sb.AppendLine("   - F# Query CE: `query { for r in df.AsQueryable<T>() do ... } |> Query.toDataFrame`.") |> ignore
sb.AppendLine("   - To retrieve the native Polars plan: `.Explain()`.") |> ignore
sb.AppendLine("   - To extract back to DataFrame: `.ToDataFrame()` or `.ToLazyFrame()`.") |> ignore
sb.AppendLine() |> ignore
sb.AppendLine("7. **Delta Lake Storage**:") |> ignore
sb.AppendLine("   - Read: `DataFrame.ReadDelta(path, version = 2L)` or `DataFrame.ScanDelta(path)`.") |> ignore
sb.AppendLine("   - Write: `df.WriteDelta(path, mode = SaveMode.Append)`.") |> ignore
sb.AppendLine("   - Maintenance: `Delta.History(path)`, `Delta.Optimize(path)`, `Delta.Vacuum(path, retentionHours = 168.0)`.") |> ignore
sb.AppendLine() |> ignore
sb.AppendLine("---") |> ignore
sb.AppendLine() |> ignore

// Concatenate all documentation articles
for fileName, title in orderedArticles do
    let filePath = Path.Combine(articlesDir, fileName)
    if File.Exists filePath then
        printfn "Processing: %s (%s)" fileName title
        let rawContent = File.ReadAllText filePath
        let cleaned = cleanMarkdownContent rawContent
        
        sb.AppendLine($"# Chapter: {title}") |> ignore
        sb.AppendLine($"*Source: articles/{fileName}*") |> ignore
        sb.AppendLine() |> ignore
        sb.AppendLine(cleaned) |> ignore
        sb.AppendLine() |> ignore
        sb.AppendLine("---") |> ignore
        sb.AppendLine() |> ignore
    else
        printfn "Warning: %s does not exist, skipping." filePath

let llmsFullPath = Path.Combine(publicDir, "llms-full.txt")
File.WriteAllText(llmsFullPath, sb.ToString().Replace("\r\n", "\n"))
printfn "Created: %s (Size: %.2f KB)" llmsFullPath (float (FileInfo(llmsFullPath).Length) / 1024.0)
printfn "=== llms.txt generation complete! ==="
