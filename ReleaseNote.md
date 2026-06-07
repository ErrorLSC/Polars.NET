
# Polars.NET 0.6.0 Release Note

![icon](assets/icon_lite.png)

## Upgrades

- Bumped LINQ2DB up to 6.3.0

- Bumped rust polars up to 0.54.4

- Bumped delta-rs up to 0.32.3

## API

- DataFrame pivot pivotcolumnnaming enum added(F#)

- LazyFrame merge sorted add maintainOrder bool(F#, joinwhere)

- Implode add maintainOrder bool

- setsorted nulls last bool added

- expr truncate added(F#)

- expr reinterpret option dtype added(F#)

- expr isempty, has nulls added(F#)

- lazyframe/dataframe gather added(F#)

- Config module added(F#)

- DataFrame IPC Stream reader/writer added(Rust done)

## Behavior Change

- Expr Cut now return Enum type rather than Categorical type

- Expr all, any ignoreNulls default value will true

- Series isempty now is a method.

## BugFix

- FSharp DataFrame aggregation return lazyframe bug fixed.

- Delta delete partition pruning bug fixed.

## Others

- Linux X64-glibc now is built in manylinux_2_28_x86_64 to allow lower glibc version linux environments.
