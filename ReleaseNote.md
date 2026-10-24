# Polars.NET 1.0.0 Release Note

![icon](assets/icon_lite.png)

Welcome to **Polars.NET 1.0.0** — a landmark milestone bringing production-ready stability, a brand-new zero-dependency native LINQ provider, unified F# module architecture, and an overhaul of documentation and interactive tooling to the .NET data ecosystem.

For full architectural guides, API references, and benchmarks, visit our official documentation site:
🌐 **[Polars.NET Documentation Site](https://errorlsc.github.io/polars.net)**

---

## ⚠️ Breaking Changes

### 1. F# API Architecture: Module Functions Migration
The F# API has transitioned to an idiomatic module-first architecture:
* Operations on `DataFrame` and `LazyFrame` previously residing in the `pl.*` namespace (e.g., `pl.select`, `pl.selectLazy`, `pl.filter`, `pl.filterLazy`, `pl.groupBy`, etc.) have been moved to their dedicated module functions:
  * Use **`DataFrame.*`** (e.g., `DataFrame.select`, `DataFrame.filter`, `DataFrame.groupBy`, `DataFrame.withColumn`, `DataFrame.sort`)
  * Use **`LazyFrame.*`** (e.g., `LazyFrame.select`, `LazyFrame.filter`, `LazyFrame.groupBy`, `LazyFrame.collect`, `LazyFrame.sort`)
* The **`pl.*`** namespace is now strictly focused on Expression combinators and Series/Literal constructors (`pl.col`, `pl.lit`, `pl.series`, `pl.when'`, `pl.concat`, etc.), matching standard Polars conventions and idiomatic F# pipeline workflows (`|>`).

### 2. Delta Lake: Deprecated Merge API Removed
* The legacy Delta Merge APIs previously marked as `[Obsolete]` in `Polars.CSharp.IO.Delta` and `Polars.FSharp.IO.Delta` have been completely removed.
* Please migrate to the unified, type-safe SQL MERGE builder API via `df.DeltaMerge(...)` / `LazyFrame.DeltaMerge(...)` or `MergeContext`.

### 3. Native Zero-Dependency LINQ Provider (`Polars.NET.Linq`)
* The prototype LINQ provider based on LINQ2DB has been completely replaced by an in-house, zero-dependency native LINQ engine written in F#.
* **`Polars.NET.Linq`** is now a standard, direct dependency bundled with both `Polars.NET` (C#) and `Polars.FSharp`.
* **Breaking**: All previous extension methods, configurations, and annotations relying on `linq2db` have been removed. Any code importing LINQ2DB-specific extensions must transition to standard C# LINQ (`System.Linq`) or F# `query { ... }` computation expressions.

---

## 🚀 Key Highlights & New Features

### 1. Zero-Overhead Native LINQ & F# Query Computation Expressions
Compile C# LINQ syntax and F# `query { ... }` directly into native Rust Polars `LazyPlan` execution graphs:
* **Multiple Data Entrypoints**:
  * In-memory collections (`orders.AsPolarsQueryable()`)
  * DataFrames and LazyFrames (`df.AsQueryable<T>()`)
  * ADO.NET Streaming (`reader.AsPolarsQueryable<T>(batchSize: 50_000)`)
* **Comprehensive Relational Operators**: Full pushdown support for `Where`, `Select`, `Join` (inner, left), `GroupBy` with multi-metric aggregations (`Sum`, `Average`, `Count`, `Min`, `Max`), `OrderBy`, `Take`, `Skip`, `Distinct`, `Union`, `Concat`, and subqueries.
* **TPC-H Verified**: Validated against the full industry-standard TPC-H query suite (Q1–Q22).
* **Transparent Execution Inspection**: Inspect native query plans via `.Explain()` and inspect client fallback warnings.

### 2. Expression & LazyFrame Reusability
* Expressions (`Expr`) and `LazyFrame` instances are now fully reusable and reentrant.
* Finalizing and transformation methods no longer prematurely consume underlying native Rust handles, enabling complex multi-branch DAG execution without manual cloning.

### 3. High-Performance Zero-Allocation Row Enumeration
* Introduced zero-allocation struct enumerators:
  * `series.As<'T>()` / `SeriesEnumerator<'T>`
  * `df.Rows<'T>()` / `RowEnumerator<'T>`
* Pre-compiled expression delegates and scalar accessors fast-paths for `Guid`, `Categorical`, `Enum`, `Decimal`, `DateTimeOffset`, and high-precision temporal values.

### 4. Interactive LINQPad 7 & 8 Integration
* Packaged first-class interactive `.linq` samples directly inside NuGet packages under `linqpad-samples/`.
* Included NuGet `linqpad-samples` tag to unlock **100% free search, restore, and interactive execution in LINQPad Free Edition**.
* Comprehensive samples covering C# DataFrame QuickStart, LINQ comprehension syntax, joins, expressions, window functions, and F# pipelines / `query { ... }`.

### 5. Interactive HTML Rendering (`.ToHtml()`)
* Added rich CSS-styled `.ToHtml()` table previews across `DataFrame`, `LazyFrame`, `Series`, and `PolarsSchema`.
* Out-of-the-box rich rendering in LINQPad (`Util.RawHtml(df.ToHtml()).Dump()`), Jupyter Notebooks, Polyglot Notebooks, and web embeds.

### 6. Completely Revamped VitePress Documentation Site
* Migrated documentation to a blazingly fast VitePress static site with local search, live code preview cards, and comprehensive guides for C#, F#, LINQ, Delta Lake, Time Series, and Interop.
* Added `llms.txt` and `llms-full.txt` support for automated LLM context ingestion.

---

## 📦 Upgrades & Dependencies

* Updated Rust Polars core engine
* Native runtime packages available for `win-x64`, `linux-x64`, `linux-arm64`, `linux-musl-x64`, `linux-musl-arm64`, and `osx-arm64`
* .NET 8.0 LTS target with modern C# 12 & F# 8 features

---

## 📖 Documentation & Migration Guides

For in-depth guides, tutorials, and full API references:
* **[Quick Start Guide](https://errorlsc.github.io/polars.net/articles/quickstart)**
* **[C# Idiomatic Guide](https://errorlsc.github.io/polars.net/articles/csharp-idioms)**
* **[F# Idiomatic Guide](https://errorlsc.github.io/polars.net/articles/fsharp-idioms)**
* **[LINQ & Query Computation Expressions](https://errorlsc.github.io/polars.net/articles/linq)**
* **[Delta Lake & ACID Lakehouse Guide](https://errorlsc.github.io/polars.net/articles/delta)**
* **[API Reference](https://errorlsc.github.io/polars.net/api/)**
