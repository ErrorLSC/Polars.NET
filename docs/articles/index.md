# Documentation & Guides

Welcome to the Polars.NET documentation and user guides. Here you will find quickstarts, idiom patterns, LINQ guides, and ecosystem interop references.

## Getting Started
- [Introduction](/articles/intro) - Core architecture, Lazy vs Eager evaluation, and streaming engine.
- [Quick Start](/articles/quickstart) - Installing packages and writing your first DataFrame queries.

## Core Concepts
- [Expressions & Computations](/articles/expressions) - Vectorized SIMD model, arithmetic & logic, column selectors, when/then branching, window functions (`Over`), and AST introspection.
- [LazyFrame & Query Optimization](/articles/lazy) - Deferred execution graph, Predicate & Projection pushdown, streaming execution, and async streaming batches.
- [Time Series & Advanced Analytics](/articles/timeseries) - Temporal data types, `Expr.Dt` calendar operations, timezone conversions, dynamic rolling windows, and null interpolation.

## Storage & Lakehouse
- [Delta Lake & ACID Storage](/articles/delta) - ACID transactions, Time Travel, metadata audit logs, Z-Order file compaction, CDC streaming, and ordered SQL MERGE.

## Language Guides & Idioms
- [C# Idioms & Best Practices](/articles/csharp-idioms) - Fluent method chaining, collection expressions, schema mapping, and memory safety.
- [F# Idioms & Best Practices](/articles/fsharp-idioms) - Pipeline combinators, anonymous record queries, and typed projections.
- [LINQ & Query Expressions](/articles/linq) - 3-tier fallback strategy, LINQ to Polars, and F# `query { ... }` computation expressions.

## Interoperability & Platform
- [Ecosystem & Native Interop](/articles/interop) - Native runtime package distribution, Arrow C Data Interface, ADBC, ADO.NET, ML.NET DataView, and Tensors.

