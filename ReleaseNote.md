# Polars.NET 1.0.1 Release Note

![icon](assets/icon_lite.png)

Polars.NET 1.0.1

For full architectural guides, API references, and benchmarks, please visit documentation site:
**[Polars.NET Documentation Site](https://errorlsc.github.io/Polars.NET)**

---

## Upgrades & Dependencies

* Bumped rust xlsxwriter to 0.101.0
* Bumped calamine to 0.37.0

## API

* Added DataFrame, LazyFrame dropMany API

## BugFix

* C# DataFrame.ToHtml() will generate every cell as 1-length Series fixed .
* LINQPad examples fixed.

