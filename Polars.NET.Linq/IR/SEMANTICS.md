# LINQ IR semantics

The LINQ IR (`LogicalPlan`, `ScalarExpr`) is the contract between the LINQ frontend and every execution
backend: the Polars backend today, and any later one, such as a managed engine. The frontend translates
LINQ into IR that means what the query means. A backend must evaluate each node exactly as this document
says. The optimizer passes may rely only on these rules.

The rules are executable. `Polars.NET.Linq.Conformance.Tests` builds plans by hand and runs each one on
every engine in `Engines.all`, both as written and after `LogicalOptimizer.optimize`. A new backend joins
the suite by implementing `IIrEngine`. When a rule changes, change this file, the doc comments in
`LogicalIR.fs` and the conformance tests together.

## Values and logic

**Typed nulls.** Every type has a null. `LiteralValue.Null` carries its type.

**Three-valued (Kleene) logic.** `And`, `Or` and `Not` follow Kleene rules: `false AND null` is false, and
`true OR null` is true. A comparison (`Eq`, `NotEq`, `Lt`, ...) with a null operand is null. So is
arithmetic with a null operand.

**Two-valued equality.** `EqMissing` and `NotEqMissing` treat null as equal to null and as different from
every value. The frontend uses them for C# `==` / `!=` on nullable operands.

**Null tests in other nodes.**
- `Conditional` takes its else branch when the test is null.
- `Coalesce` returns its first non-null argument.

**C# null semantics are compiled, not built in.** A lifted comparison that is false on null in C# becomes
`Coalesce [cmp; false]`. A filter treats null as false, so `FilterNullSimplification` removes that wrapper
where the predicate is positive: at the root, under `And` / `Or`, and in conditional branches. With
`CoreConfig.UseRelationalNulls`, the frontend emits plain SQL comparisons instead.

## Strings and membership

**Null as the empty string.** `StringConcat`, `ConcatString`, `FormatString` and `ListJoin` follow .NET
`String.Concat`, `String.Format` and `String.Join`. A null operand, argument or element contributes `""`.
The result is never null on account of a null operand.

**The `nullsEqual` flag.** `InList`, `ListContains` and `IsIn` carry `nullsEqual`.
- When true, a null matches a null element, as in .NET `Contains`.
- When false, a null value gives null, as in SQL `IN`.

The flag is always explicit. A backend never chooses it.

## Exceptional cases

**Null propagation.** Where .NET would throw, the IR propagates null, as EF Core does. Examples:
- a member or method called on a null string;
- an index out of range (`ListGet`, `SeriesGet`);
- a failed parse (`StrToDate`, `StrToInteger`).

Strict materialization then reports a null that reaches a non-nullable member.

**`AssertCount`.** The only node that raises is `ScalarFunction.AssertCount` (`[count; value]`). It yields
the value when the count lies in range. Otherwise the query fails with the check's `QueryError`, which the
backend surfaces as the matching .NET exception. It has an effect, so it must be evaluated wherever its
value is used, and never constant-folded. A backend that cannot raise answers
`IBackendCapabilities.SupportsAssertions = false`. The frontend then evaluates such subqueries at compile
time and marks the compilation so its plan is not cached.

## Rows and order

**Order is kept unless a node says otherwise.**
- `Filter`, `Project`, `WithColumns`, `Rename`, `WithRowIndex` and `Explode` keep input order.
- `Sort` is stable. `NullsLast` places nulls.
- `Union` lists the left rows, then the right.
- `Distinct` and `GroupBy` keep first-appearance order only with `maintainOrder`. `GroupOrderRelaxation`
  clears that flag where nothing observes the order.
- Joins:
  - `Inner` and `Left` keep left order, then right order among a row's matches.
  - `Semi` and `Anti` keep left order.
  - `Cross` has no order.

  Constant-key left joins rely on this to attach one-row plans without reordering.
- `Limit` takes `count` rows after `offset`.

**Expressions that read other rows.** Window, rank, cumulative and other frame functions
(`ScalarExprOps.dependsOnOtherRows`) are evaluated over the whole input of the `Project` / `WithColumns`
that computes them. A `Filter` above that node never changes their values, so predicate pushdown must not
move a filter below it.

**Order-reading aggregates.** `First`, `Last` and `List` read their input's order, so a pass must keep every order
guarantee beneath them. `GroupOrderRelaxation` frees order only under order-free aggregates.

**Aggregates skip nulls.**
- `Sum` over no values is 0.
- `Mean`, `Min`, `Max`, `First`, `Last` and `Median` over no values are null.
- `Count` counts non-null values. `Len` counts rows.
- `Std` / `Var` with ddof d are null with at most d values.
- A `Project` whose projections all aggregate yields exactly one row, even over no input rows.

**GroupBy `having`.** `having` is an aggregate predicate over a group's rows. It keeps the groups for
which it is true, before anything above the GroupBy runs.

## Reshaping

- **`Explode`** produces one row per list element, rows in input order and elements in list order.
  - An empty list gives one null row with `emptyAsNull`, otherwise no row.
  - A null list gives one null row with `keepNulls`, otherwise no row.
- **`Unpivot`** outputs the columns `index..., variable, value`. It produces one block of rows per `on` column,
  in `on` order, and each block lists rows in input order.
- **`JoinAsOf`** keeps every left row once, in left order. Each left row gets the right row whose `on` key is
  nearest by the strategy, among the right rows with equal `by` keys:
  - Backward: the greatest key <= the left key;
  - Forward: the least key >=;
  - Nearest: the closest key.

  Without `AllowEq`, `<=` / `>=` become `<` / `>`. When no row qualifies, the right columns are null. Both
  inputs are sorted by their `on` keys.
- **`GroupingSets`** produces one block per level, in `Levels` order.
  - A level groups by its subset of keys. Groups appear in first-key order, and an empty subset gives one
    grand-total row.
  - Keys outside the subset are null.
  - A level's literals set their columns. A level that holds every key takes `Level0Literals` instead.

## Joins and keys

A null join key matches a null key exactly when the join's `nullsEqual` is true.
- Explicit LINQ joins use false (`Enumerable.Join`).
- Decorrelated subqueries use true, or false under `UseRelationalNulls`.
