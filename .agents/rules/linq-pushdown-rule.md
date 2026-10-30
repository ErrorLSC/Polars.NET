---
trigger: always_on
---

# LINQ Native Pushdown & Architecture Rules

1. **Zero-Compromise LINQ Syntax**:
   - Write standard, idiomatic C# LINQ / F# Query expressions.
   - Do NOT compromise LINQ queries with manual client pre-calculations (e.g. computing thresholds via `.ToList()` in C# memory, extracting subqueries into procedural variables) to work around pushdown limitations. Enhance `Polars.NET.Linq` to natively push them down instead.

2. **No Compiler Warning Suppression**:
   - Never suppress compiler warnings (such as CS0472) using `#pragma warning disable` to mask engine or expression limitations.
   - In Left Outer Joins / Anti-Joins, write standard `where r == null` (or `g.Sum(x => x == null ? 1L : 0L)`) instead of casting non-nullable value-type keys to nullable (e.g. `where (long?)r.id == null`). The engine natively translates entity null checks.

3. **Architectural Separation of Concerns**:
   - `Polars.CSharp` must NEVER contain AST / expression tree parsing logic. It is strictly a typed Fluent / DSL facade.
   - All expression tree manipulation, transparent identifier desugaring, subquery decorrelation, and pushdown translation belong exclusively in `Polars.NET.Linq`.

