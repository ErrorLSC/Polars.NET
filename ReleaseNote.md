
# Polars.NET 0.7.0 Release Note

![icon](assets/icon_lite.png)

## Dependency Update

- Bumped polars Rust to 0.55.2
- Bumped Apache ADBC to 0.24
- Bumped calamine to 0.36.1
- Bumped delta-rs to 0.32.4

## Behavior Change

- `LazyFrame.ScanCsv/DataFrame.ReadCsv` now will scan all file to decide schema if inferSchemaLength is not defined.

## API

- `Struct.Drop()` added.
- Add `inferSchemaFiles` parameter to `LazyFrame.ScanCsv/DataFrame.ReadCsv`
- Add `Expr.EwmSum` and `Expr.EwmSumBy`
- Allow `MergeSorted` with multikeys
- Add `Expr.Cat.To` and `Expr.Cat.Physical`
- Add `Expr.IsSorted` and `DataFrame.IsSorted`
