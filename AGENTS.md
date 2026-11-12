# Agent Rules

## 1. We write a modern .NET project: no legacy idioms, ever

This is the project's highest rule. It applies to new code, to code you touch, and to code samples in the docs.

### Basic syntax

- Use collection expressions `[...]` and the spread operator `[..a, ..b]`. Never write `new List<T>()`, `new[] { ... }`, `new T[] { ... }`, `Array.Empty<T>()` or `Concat(...).ToArray()`.
- Prefer primary constructors for classes, including those that take injected dependencies.
- Write multi-line text, JSON and SQL as raw string literals (`"""`).
- Write constant UTF-8 bytes as `"text"u8`; never `Encoding.UTF8.GetBytes(...)` for a constant.
- Use local functions for helpers that serve one method, instead of lambdas stored in `Func<>` / `Action<>` variables or one-caller private methods; mark them `static` when they capture nothing.
- Map values with switch expressions (`x switch { ... }`, with patterns), not `switch` statements or `if` / `else if` chains that only pick a value.
- Keep lambdas minimal: a method group instead of a lambda that only forwards (`Select(Parse)`, not `Select(x => Parse(x))`), expression bodies instead of `{ return ...; }`, and no explicit parameter types the compiler infers.

### Memory and performance

- Avoid needless managed allocations: take parameters, slices and read-only views as `ReadOnlySpan<T>`; `stackalloc` is fine for small buffers.
- Static lookup dictionaries and sets queried often must be `System.Collections.Frozen` (`FrozenDictionary`, `FrozenSet`).
- Search for characters or patterns with `SearchValues<T>` (SIMD-accelerated).
- P/Invoke always uses `[LibraryImport]` on a `partial` method; never `[DllImport]`.
- Regular expressions always use the `[GeneratedRegex]` source generator; never `new Regex(...)` at run time.
- Lock with `System.Threading.Lock` (.NET 9+); never `lock (new object())`-style lock objects. Exception: the libraries target net8.0, where `Lock` does not exist, so they lock on a `private readonly object _lock = new();` field; projects targeting .NET 9+ (tests, benchmarks) use `Lock`.

### Code samples in the docs

- Build schemas and frames from records: `PolarsSchema.From<Row>()`, `DataFrame.From<Row>([new(...), new(...)])`, never chains of `.Add(...)`. Declare the records at the end of the sample, after the top-level statements, so it compiles as a `Program.cs`.
- Anonymous types (`DataFrame.FromColumns(new { ... })`) cannot take collection expressions in their members; they may stay where they already are, but new samples use records.
