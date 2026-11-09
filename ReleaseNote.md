# Polars.NET 1.1.0 Release Note

![icon](assets/icon_lite.png)

Polars.NET 1.1.0

For full architectural guides, API references, and benchmarks, please visit documentation site:
**[Polars.NET Documentation Site](https://errorlsc.github.io/Polars.NET)**

---

## New API

* All IO method(except Excel) now have an option to write to native memory.

## LINQ Provider

* All TPCDS benchmark passed.

## Bug Fix

* CSV writer default quote mark parameter enums fixed.
* Negative polars tolerance strhing now will be handled correctly.
* Old object hydrator is too slow, so it is replaced by a much faster one.
