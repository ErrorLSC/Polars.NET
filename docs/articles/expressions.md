# Expressions & Computations

Expressions (`Expr`) are the computational heart of Polars.NET. Rather than manipulating row values inside procedural loops, you construct declarative expression trees that the Polars Rust engine compiles into multithreaded, SIMD-accelerated execution kernels.

---

## 1. The Mental Model: Why No Procedural Loops?

In traditional .NET data processing, developers often iterate rows using `foreach`:

```csharp
// Anti-pattern: Untyped row iteration causes boxing, GC pressure, and zero SIMD parallelism
foreach (var row in df.Rows()) 
{
    row["Score"] = (double)row["Score"] * 1.1; 
}

// If row-level traversal is genuinely required (e.g., streaming to an external API or legacy service):
// Prefer the strongly-typed `df.Rows<T>()`. It returns a stack-allocated `ref struct DataFrameRowEnumerator<T>`
// with pre-compiled fast-path accessors and zero GC heap allocations:
foreach (ScoreRecord record in df.Rows<ScoreRecord>())
{
    SendToExternalService(record);
}

// Idiomatic Polars: Vectorized columnar compute, parallel, zero managed heap allocation
df.WithColumns((Col("Score") * 1.1).Alias("Adjusted_Score"));
```

> [!TIP] Expressions are Immutable and Reusable
> Every operation on an `Expr` (such as `.Alias()`, `.Clip()`, `.Filter()`, or operator overloads) clones handles natively via `CloneHandle()`. You can freely store an expression in a variable and reuse it across multiple DataFrames or branches without consumption side-effects.

---

## 2. Arithmetic, Comparison & Literals

Expressions overload standard arithmetic and logical operators.

::: code-group

```csharp [C# (Fluent API)]
using Polars;
using Polars.CSharp;
using static Polars.CSharp.Polars;

// Column arithmetic and scalar literals
Expr bonus = Col("BaseSalary") * 0.15;
Expr total = Col("BaseSalary") + bonus;

// Bitwise / Logical operators: & (AND), | (OR), ~ (NOT)
Expr filter = (Col("Age") >= 25) & (Col("Dept") == "Engineering");
```

```fsharp [F# (Pipeline API)]
open Polars.FSharp

// Overloaded arithmetic operators: +, -, *, /, %
let bonus = pl.col "BaseSalary" * pl.lit 0.15
let total = pl.col "BaseSalary" + bonus

// F# comparison operators: .==, .!=, .>, .>=, .<, .<=
// F# logical combinators: .&&, .||, !~
let filter = (pl.col "Age" .>= pl.lit 25) .&& (pl.col "Dept" .== pl.lit "Engineering")
```

```csharp [C# (LINQ)]
// Typed query expression with LINQ to Polars
public record Employee(string Name, int Age, string Dept, double BaseSalary);

var query = from r in df.AsQueryable<Employee>()
            where r.Age >= 25 && r.Dept == "Engineering"
            select new {
                r.Name,
                TotalSalary = r.BaseSalary * 1.15
            };
```

:::

> [!NOTE] Column References vs. Literals (`IntoExprColumn` vs `IntoExpr`)
> * When passing strings into selection methods like `df.Select("a", "b")`, `"a"` represents `Col("a")`.
> * In conditional values like `expr.FillNull("unknown")`, `"unknown"` represents literal `Lit("unknown")`.
> * In binary comparison `Col("a") == "foo"`, `"foo"` is treated as a scalar literal value. To compare two columns, write `Col("a") == Col("b")`.

---

## 3. Column Selectors & Set Algebra

Instead of hardcoding column name lists, Polars.NET provides type-aware **Column Selectors** (`Polars.Selectors` / `Cs` / `pl.cs`) with intuitive set algebra operators:

* **Union (`|` / `+`)**: Combine selections.
* **Difference (`-`)**: Exclude columns from a selection.
* **Intersection (`&`)**: Keep columns matching both selectors.
* **Inversion (`~`)**: Select everything except this pattern.

::: code-group

```csharp [C# (Fluent API)]
using Polars.CSharp;
using Polars.CSharp.Selectors;

// Select all numeric columns except the "Id" column
var numericCols = df.Select(Cs.Numeric() - Cs.ByName("Id"));
```

```fsharp [F# (Pipeline API)]
open Polars.FSharp

// Select all numeric columns except "Id" using F# selector subtraction
let numericCols = df.Select(pl.cs.numeric() - pl.cs.byName [ "Id" ])
```

```csharp [C# (Indexers)]
// Direct indexer selection with Selectors
var subset = df[Cs.Numeric() - Cs.ByName("Id")];
```

:::

<!--@include: ../snippets/expr_selectors.html-->

---

## 4. Conditional Logic: When / Then / Otherwise

Polars.NET provides a declarative ternary expression builder (`When-Then-Otherwise`) capable of compiling complex `if-else if-else` logic directly into native execution branches.

::: code-group

```csharp [C# (Fluent API)]
using static Polars.CSharp.Polars;

var gradedDf = df.WithColumns(
    When(Col("Price") >= 500.0).Then("Premium")
    .When(Col("Price") >= 100.0).Then("Mid-Range")
    .Otherwise("Budget")
    .Alias("Price_Tier")
);
```

```fsharp [F# (Pipeline API)]
open Polars.FSharp

let gradedDf =
    df
    |> DataFrame.withColumn (
        (pl.when' (pl.col "Price" .>= pl.lit 500.0)
            .Then(pl.lit "Premium")
            .When(pl.col "Price" .>= pl.lit 100.0)
            .Then(pl.lit "Mid-Range")
            .Otherwise(pl.lit "Budget"))
            .Alias "Price_Tier"
    )
```

```csharp [C# (LINQ)]
// Standard C# ternary operators translated into native Polars ternary kernels
public record ProductItem(int Id, string Product, double Price);

var gradedDf = df.AsQueryable<ProductItem>()
    .Select(r => new {
        r.Id,
        r.Product,
        r.Price,
        Price_Tier = r.Price >= 500.0 ? "Premium" :
                     r.Price >= 100.0 ? "Mid-Range" : "Budget"
    })
    .ToDataFrame();
```

:::

<!--@include: ../snippets/expr_when_then.html-->

---

## 5. Domain Sub-Namespaces (`.Str`, `.Dt`, `.List`, etc.)

Polars organizes domain-specific operators into dedicated namespaces hanging off `Expr` and `Series`:

| Sub-namespace | Focus | Key Methods |
| :--- | :--- | :--- |
| **`.Str`** | String processing | `Contains`, `ReplaceAll`, `Extract`, `Split`, `StripChars`, `ToDatetime`, `JsonPathMatch` |
| **`.Dt`** | Temporal & Calendar | `Year`, `Month`, `Day`, `Truncate`, `OffsetBy`, `ConvertTimeZone`, `TotalMilliseconds` |
| **`.List`** | Dynamic variable-length lists | `Len`, `Get`, `Slice`, `Eval`, `Explode`, `Join`, `Contains`, `Unique`, `Sum`, `Mean` |
| **`.Array`** | Fixed-size homogeneous arrays | `Width`, `Sum`, `Max`, `ToList`, `Get`, `Min` |
| **`.Struct`** | Multi-field composite records | `Field`, `RenameFields`, `JsonEncode`, `Unnest` |
| **`.Name`** | Dynamic column renaming | `Prefix`, `Suffix`, `ToLowercase`, `ToUppercase`, `Keep` |
| **`.Meta`** | AST reflection & introspection | `FormatTree`, `OutputName`, `RootNames`, `Eq` |

::: code-group

```csharp [C# (Fluent API)]
df.Select(
    // 1. String manipulation
    Col("Product").Str.ToUppercase().Alias("PRODUCT_UPPER"),
    Col("Product").Str.Contains("phone").Alias("IsPhone"),

    // 2. Temporal parsing & offset
    Col("Created").Dt.Year().Alias("Year"),
    Col("Created").Dt.OffsetBy("1mo").Alias("NextMonth"),

    // 3. Batch column prefixing
    Col("Score").Name.Prefix("Metric_")
);
```

```fsharp [F# (Pipeline API)]
df
|> DataFrame.select [
    // 1. String manipulation
    (pl.col("Product").Str.ToUppercase()).Alias "PRODUCT_UPPER"
    (pl.col("Product").Str.Contains "phone").Alias "IsPhone"

    // 2. Temporal parsing & offset
    (pl.col("Created").Dt.Year()).Alias "Year"
    (pl.col("Created").Dt.OffsetBy(Dur.String "1mo")).Alias "NextMonth"

    // 3. Batch column prefixing
    (pl.col("Score").Name.Prefix "Metric_")
]
```

:::

---

## 6. Window Functions (`Over`)

Window functions perform calculations within partitions without collapsing rows. This makes it straightforward to compute cumulative sums, group ranks, moving statistics, and deviations from group averages.

::: code-group

```csharp [C# (Fluent API)]
var windowDf = df.Select(
    Col("Employee"),
    Col("Dept"),
    Col("Salary"),
    // 1. Group average retained on each row
    Col("Salary").Mean().Over("Dept").Alias("Dept_Avg"),
    
    // 2. Difference from department average
    (Col("Salary") - Col("Salary").Mean().Over("Dept")).Alias("Diff_From_Dept_Avg"),
    
    // 3. Dense ranking within each department
    Col("Salary").Rank().Over("Dept").Alias("Dept_Salary_Rank")
);
```

```fsharp [F# (Pipeline API)]
let windowDf =
    df
    |> DataFrame.select [
        pl.col "Employee"
        pl.col "Dept"
        pl.col "Salary"
        (pl.col("Salary").Mean().Over(pl.col "Dept")).Alias "Dept_Avg"
        (pl.col "Salary" - pl.col("Salary").Mean().Over(pl.col "Dept")).Alias "Diff_From_Dept_Avg"
        (pl.col("Salary").Rank().Over(pl.col "Dept")).Alias "Dept_Salary_Rank"
    ]
```

```csharp [C# (LINQ)]
public record EmployeeSalary(string Employee, string Dept, double Salary);

var windowDf = df.AsQueryable<EmployeeSalary>()
    .Select(r => new {
        r.Employee,
        r.Dept,
        r.Salary,
        Dept_Avg = r.Salary.Mean().Over(r.Dept),
        Diff_From_Dept_Avg = r.Salary - r.Salary.Mean().Over(r.Dept)
    })
    .ToDataFrame();
```

:::

<!--@include: ../snippets/expr_window_over.html-->

---

## 7. Engine Introspection: AST Visualization

Every Polars expression builds a recursive Syntax Tree that is optimized before execution. You can inspect this tree anytime by calling `.ToHtml(schema)` or `.Meta.FormatTree()`:

::: code-group

```csharp [C# (Console Inspection)]
Expr complexExpr = (Col("Price") * (1.0 - Col("Discount"))).Round(2)
                 - Col("Price").Mean().Over("Category");

// Print Unicode ASCII tree to stdout
Console.WriteLine(complexExpr.Meta.FormatTree(schema: df.Schema));
```

```fsharp [F# (FSI Inspection)]
let complexExpr =
    ((pl.col("Price") * (pl.lit 1.0 - pl.col("Discount"))).Round(2u)
     - pl.col("Price").Mean().Over(pl.col "Category"))

printfn "%s" (complexExpr.Meta.FormatTree(schema = df.Schema))
```

:::

<!--@include: ../snippets/expr_ast_complex.html-->

---

## Next Steps

* [LazyFrame & Streaming Execution](/articles/quickstart#4-the-lazy-engine-execution-planning) - Discover query graph optimization and out-of-core streaming.
* [LINQ Provider Architecture](/articles/linq) - Learn the 3-tier fallback execution strategy from LINQ to native kernels.
* [Ecosystem & Native Interop](/articles/interop) - Connect Polars expressions to Arrow, ADBC, ADO.NET, and ML.NET.
