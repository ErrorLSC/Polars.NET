
# Polars.NET 0.6.0 Release Note

![icon](assets/icon_lite.png)

## Upgrades

- Bumped up to LINQ2DB 6.3.0

- Bumped rust polars to 0.54.4

## API

- DataFrame pivot pivotcolumnnaming enum added

- LazyFrame merge sorted add maintainOrder bool

- Implode add maintainOrder bool

- setsorted nulls last bool added

- expr truncate added

- expr reinterpret option dtype added

- expr isempty, has nulls added

- lazyframe/dataframe gather added

- Config module added

## Behavior Change

- Expr Cut now return Enum type rather than Categorical type

- Expr all, any ignoreNulls default value will true

## BugFix

- FSharp DataFrame aggregation return lazyframe bug fixed.

- Delta delete partition pruning bug fixed.