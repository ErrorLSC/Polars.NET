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

## Floating-point values

**Engine total order.** IR comparisons over floating values (`Float16`, `Float32`, `Float64`) follow the
engine's total order. NaN equals NaN and is greater than every number. `Unary IsNan` is true for NaN, false
for a number and null for null.

**.NET semantics are compiled, not built in (`NaNSemantics`).** For `double`, `float` and `Half` operands that
may be NaN, the frontend guards comparisons:
- `==` adds `AND NOT isnan(left)`;
- `!=` adds `OR isnan(left)`;
- `<` and `<=` add `AND right <= +inf`;
- `>` and `>=` add `AND left <= +inf`.

`x <= +inf` is false only for NaN. It is one plain comparison, which predicate pushdown and statistics
understand. So a comparison involving NaN is false, and `!=` is true.

A floating sort key sorts by `ScalarFunction.FloatOrderKey`, an Int64 whose order is .NET's order:
- NaN comes before every number;
- -0.0 equals 0.0;
- null stays null, so nulls keep the place the order gives them.

This is a single key, as `Comparer<T>.Default` orders.

**Aggregates.** `Min` is .NET `Min`: NaN as soon as one value is NaN. `Max` skips NaN unless every value is
NaN. Grouping and `Distinct` treat NaN as one value (.NET `Equals`).

**Literals.** `LiteralValue.Float16` holds a `Half`.

## Arithmetic

**Truncating division and remainder.** `Modulo` is the .NET remainder for every numeric type: it truncates, so
the result carries the dividend's sign (-7 % 2 = -1, -7.5 % 2 = -1.5). `TruncatedDivide` is .NET integer
division, which truncates toward zero (-7 / 2 = -3). `Divide` over integers is the engine's own division, so the
frontend emits `TruncatedDivide` for integer `/`.

The Polars backend computes both in one pass, with shim kernels.

**Decimals.** Decimals are fixed-point. The frontend widens a decimal dividend to scale 18, so a quotient keeps
18 fractional digits (.NET keeps up to 28 significant digits). A decimal value has its column's scale, so its
string form carries that scale (2.500, not 2.5), while its value is the same.

**Division by zero and overflow.** Integer division or remainder by zero gives null (.NET raises). A decimal
division by zero fails the query. Integer overflow wraps, as unchecked .NET arithmetic does.

**Date and time.** A `DateTimeOffset` is stored as a UTC instant, so its members read the UTC value: `Offset`
is zero, and `Hour` and `DateTime` are the UTC hour and time.

## Strings and membership

**Null as the empty string.** `StringConcat`, `ConcatString`, `FormatString` and `ListJoin` follow .NET
`String.Concat`, `String.Format` and `String.Join`. A null operand, argument or element contributes `""`.
The result is never null on account of a null operand.

**Ordinal string order.** Strings compare and sort ordinally, by code point (the UTF-8 byte order), not by
culture: `"Banana" < "apple" < "äpfel"`. This holds for `Sort`, comparisons and the frontend's `CompareTo`
(-1 / 0 / 1). .NET defaults to the current culture, so results can differ from LINQ to Objects. A query that
passes a comparer (`StringComparer.CurrentCulture`, for instance) runs as LINQ to Objects under the client
evaluation policy (see Untranslated operators). Code points above U+FFFF sort after U+E000-U+FFFF here, while
.NET's ordinal comparison orders UTF-16 code units.

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

**No short-circuit.** Expressions are evaluated whole: both sides of `And` / `Or` and both branches of a
`Conditional`, for every row. An `AssertCount` is therefore checked once whenever its plan runs, even where
the logic around it (`false AND ...`, an empty outer relation) would never read its value. This is stricter
than LINQ to Objects, never laxer: whenever LINQ raises, so does the query.

**Guarded UDFs.** The frontend restores C# short-circuit evaluation for managed UDFs. A UDF on the right of
`&&` / `||` / `??`, or in a `?:` branch, receives the guard (the left side, its negation, the left's null test,
the test or its negation) as one more input. It is invoked only on rows where the guard is true, and yields
null elsewhere, which the guarding operator discards. A guard that aggregates or reads a window is not
applied.

**Untranslated operators.** A query operator the frontend does not translate (an overload with a comparer, an
unknown operator) sends the whole query to LINQ to Objects under the client evaluation policy. It is never
skipped.

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
- A `Project` whose projections all aggregate yields exactly one row, even over no input rows. A `GroupBy`
  over no rows yields no rows, and a `GroupingSets` level with an empty key subset still yields its one
  grand-total row.

**GroupBy `having`.** `having` is an aggregate predicate over a group's rows. It keeps the groups for
which it is true, before anything above the GroupBy runs.

## Reshaping

- **`Explode`** produces one row per list element, rows in input order and elements in list order. Several
  columns explode together and pair their elements, so each row's lists must have equal lengths, or the
  query fails. The frontend explodes one column at a time.
  - An empty list gives one null row with `emptyAsNull`, otherwise no row.
  - A null list gives one null row with `keepNulls`, otherwise no row.
- **`Unpivot`** outputs the columns `index..., variable, value`. It produces one block of rows per `on` column,
  in `on` order, and each block lists rows in input order.
- **`JoinAsOf`** keeps every left row once, in left order. Each left row gets the right row whose `on` key is
  nearest by the strategy, among the right rows with equal `by` keys:
  - Backward: the greatest key <= the left key;
  - Forward: the least key >=;
  - Nearest: the closest key.

  Without `AllowEq`, `<=` / `>=` become `<` / `>`. When no row qualifies, the right columns are null. A null
  `on` or `by` key never matches, on either side. Both inputs are sorted by their `on` keys.
- **`GroupingSets`** produces one block per level, in `Levels` order.
  - A level groups by its subset of keys. Groups appear in first-key order, and an empty subset gives one
    grand-total row.
  - Keys outside the subset are null.
  - Each level's own literals set their columns.

## Joins and keys

A null join key matches a null key exactly when the join's `nullsEqual` is true.
- Explicit LINQ joins use false (`Enumerable.Join`).
- Decorrelated subqueries use true, or false under `UseRelationalNulls`.
