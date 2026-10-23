# Polars.CSharp Idioms & Best Practices

## 1. Namespace & Static Imports

Two standard import idioms are used across `Polars.CSharp`:

```csharp
// Style 1: Explicit Alias (Python-like: Pl.Col, Pl.Lit, Cs.Numeric)
using Polars.CSharp;
using Pl = Polars.CSharp.Polars;
using Cs = Polars.CSharp.Polars.Selectors;

// Style 2: Static Import (Rust-like: Col, Lit, When, Numeric)
using Polars.CSharp;
using static Polars.CSharp.Polars;
using static Polars.CSharp.Polars.Selectors;
```

---

## 2. Creating DataFrames & Series

```csharp
// 1. From anonymous object (quickest for inline tests & prototypes)
using var df1 = DataFrame.FromColumns(new
{
    id = new[] { 1, 2, 3 },
    name = new[] { "Alice", "Bob", "Charlie" },
    score = new double?[] { 95.5, null, 88.0 }
});

// 2. From Series (supports spans, arrays, nullable arrays, tensors)
using var df2 = new DataFrame(
    Series.From("id", [1, 2, 3]),
    Series.From("val", [10.0, 20.0, 30.0])
);

// 3. From strongly-typed POCOs / records / structs
using var df3 = DataFrame.From(employees);
// Convert back to strongly-typed records/classes:
List<Employee> list = df3.ToRecords<Employee>().ToList();

// 4. Zero-copy Apache Arrow Interop
using var dfArrow = DataFrame.FromArrow(recordBatch);
using RecordBatch batch = dfArrow.ToArrow();
```

---

## 3. Expressive 2D Indexers (`df[...]`)

`DataFrame` supports Python/NumPy-style 1D and 2D indexing with `Range`, `Index` (`^1`), boolean mask `Series`, index `Series`, column names, and `IntoSelector` (`Cs.*`, `typeof(...)`):

```csharp
Series col = df["Age"];                                  // Single column by name
Series col0 = df[0];                                     // Single column by index
object? cell = df[^1, "Age"];                            // Single cell at last row
DataFrame subCols = df[["Name", "Score"]];               // Column subset
DataFrame numCols = df[Cs.Numeric()];                    // Selector subset
DataFrame floatCols = df[typeof(double)];                // By .NET Type selector
DataFrame sliced = df[1..10, Cs.Numeric()];              // Row range + Selector
DataFrame filtered = df[df["Age"] >= 18, ["Name", "Age"]]; // Boolean Series mask + columns
```

---

## 4. Expressions, Operators & Sub-namespaces

### Operators on `Expr`
- **Arithmetic**: `+`, `-`, `*`, `/`, `%` (automatically broadcasts scalars: `Pl.Col("x") * 2`)
- **Comparisons**: `==`, `!=`, `>`, `>=`, `<`, `<=`
  - **Note**: `Pl.Col("a") == "foo"` compares column `"a"` to literal `"foo"`. To compare two columns, use `Pl.Col("a") == Pl.Col("b")`.
- **Logical / Bitwise**: Use single `&`, `|`, `^`, `!`, `~` on `Expr` (C# `&&` and `||` cannot be overloaded to return `Expr` directly):
  ```csharp
  df.Filter((Pl.Col("age") >= 18) & (Pl.Col("dept") == "IT"))
  ```

### Conditionals (`When / Then / Otherwise`)
```csharp
Pl.When(Pl.Col("score") >= 90).Then("A")
  .When(Pl.Col("score") >= 60).Then("B")
  .Otherwise("F")
  .Alias("grade")
```

### Sub-namespaces on `Expr` and `Series`
- `.Str` ([Expr.Str.md](../../../docs/api/csharp_generated/Expr.Str.md)): `Contains`, `ContainsAny`, `ReplaceAll`, `Extract`, `Split`, `StripChars`, `ToDatetime`, `JsonPathMatch`, etc.
- `.Dt` ([Expr.Dt.md](../../../docs/api/csharp_generated/Expr.Dt.md)): `Year`, `Month`, `Day`, `Truncate`, `OffsetBy`, `ConvertTimeZone`, `TotalMilliseconds`, `AddBusinessDays`, etc.
- `.List` ([Expr.List.md](../../../docs/api/csharp_generated/Expr.List.md)): `Len`, `Get`, `Slice`, `Eval`, `Explode`, `Join`, `Contains`, `Unique`, `Sum`, `Mean`, etc.
- `.Array` ([Expr.Array.md](../../../docs/api/csharp_generated/Expr.Array.md)): fixed-size array operations (`Width`, `Sum`, `Max`, `ToList`, `Get`, etc.).
- `.Struct` ([Expr.Struct.md](../../../docs/api/csharp_generated/Expr.Struct.md)): `Field`, `RenameFields`, `JsonEncode`, `Unnest`.
- `.Name` ([Expr.Name.md](../../../docs/api/csharp_generated/Expr.Name.md)): `Prefix`, `Suffix`, `ToLowercase`, `ToUppercase`, `Keep`.
- `.Meta` ([Expr.Meta.md](../../../docs/api/csharp_generated/Expr.Meta.md)): `FormatTree`, `OutputName`, `RootNames`, `Eq`.

---

## 5. Selectors (`Polars.Selectors` / `Cs`)

Selectors allow selecting columns by type, pattern, or set algebra (`|`, `&`, `-`, `~`):
```csharp
using Cs = Polars.CSharp.Polars.Selectors;

// Select all numeric columns except "id", and fill nulls with 0
df.WithColumns((Cs.Numeric() - Cs.ByName("id")).FillNull(0));

// Select strings or columns matching regex
df.Select(Cs.String() | Cs.Matches("^meta_.*"));
```

---

## 6. GroupBy, Having & Sugar Aggregations

`GroupByBuilder` supports both full `.Agg(...)` and direct sugar methods (`.Len()`, `.Sum()`, `.Mean()`, `.Min()`, `.Max()`, `.First()`, `.Last()`, `.Head()`, `.Tail()`), plus SQL-style `.Having(...)`:
```csharp
using var res = df
    .GroupBy("dept")
    .Having(Pl.Col("salary").Sum() > 100_000)
    .Agg(
        Pl.Col("salary").Sum().Alias("total_salary"),
        Pl.Col("salary").Mean().Alias("avg_salary"),
        Pl.Len().Alias("headcount")
    )
    .Sort("total_salary", descending: true);
```

---

## 7. LazyFrame, Streaming & Reuse

Prefer `LazyFrame` (`ScanParquet`, `ScanCsv`, `ScanNdjson`, `ScanDelta`, or `df.Lazy()`) for query optimization, predicate pushdown, and streaming execution:
```csharp
using var lf = LazyFrame.ScanParquet("s3://bucket/data/*.parquet", cloudOptions: CloudOptions.Aws("us-east-1"));
using var df = lf
    .Filter(Pl.Col("ts") >= new DateOnly(2025, 1, 1))
    .GroupBy("region")
    .Agg(Pl.Col("revenue").Sum())
    .Collect(engine: Engine.Streaming); // Or SinkParquet("output.parquet")
```

> [!TIP]
> **`LazyFrame` is Reusable & Non-Consuming**:
> In Polars.NET, calling `.Collect()`, `.CollectAsync()`, or any `.Sink*()` method does **not** consume or invalidate the `LazyFrame`. You can freely branch from the same `LazyFrame` or call `.Collect()` multiple times (e.g. in interactive Notebooks). The underlying native handle is automatically released when the `using var lf` scope exits.

