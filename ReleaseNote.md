
# Polars.NET 0.6.0 Release Note

![icon](assets/icon_lite.png)

## Upgrades

- Bumped LINQ2DB up to 6.3.0

- Bumped rust polars up to 0.54.4

- Bumped delta-rs up to 0.32.3

## API

- DataFrame pivot pivotcolumnnaming enum added

- LazyFrame merge sorted add maintainOrder bool

- Implode add maintainOrder bool

- setsorted nulls last bool added

- expr,series truncate added

- expr,series reinterpret option dtype added

- expr,series isempty, has nulls added

- lazyframe/dataframe gather added

- Config module added(F#)

- DataFrame IPC Stream reader/writer along with schema reader added

## Behavior Change

- Expr Cut now return Enum type rather than Categorical type

- Expr all, any ignoreNulls default value become true

- Series isempty now is a method.

## BugFix

- FSharp DataFrame aggregation return lazyframe bug fixed.

- Delta delete partition pruning bug fixed.

## Others

- Linux X64-glibc now is built in manylinux_2_28_x86_64 to allow lower glibc version linux environments.
