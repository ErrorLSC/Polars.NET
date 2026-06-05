
# Polars.NET 0.6.0 Release Note

![icon](assets/icon_lite.png)

## Upgrades

- Bumped LINQ2DB up to 6.3.0

- Bumped rust polars up to 0.54.4

- Bumped delta-rs up to 0.32.3

## API

- DataFrame pivot pivotcolumnnaming enum added(F#)

- LazyFrame merge sorted add maintainOrder bool(need test,F#)

- Implode add maintainOrder bool

- setsorted nulls last bool added(need test)

- expr truncate added(F#)

- expr reinterpret option dtype added(F#)

- expr isempty, has nulls added(F#)

- lazyframe/dataframe gather added(need add,F#)

- Config module added(need add from rust)

## Behavior Change

- Expr Cut now return Enum type rather than Categorical type

- Expr all, any ignoreNulls default value will true

## BugFix

- FSharp DataFrame aggregation return lazyframe bug fixed.

- Delta delete partition pruning bug fixed.