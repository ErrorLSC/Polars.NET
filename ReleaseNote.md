
# Polars.NET 0.3.2 Release Note

![icon](assets/icon_lite.png)

## API

- PolarsConfig class add. It is intended to inject env var into Rust core.
- 

## Features

- ADBC Read&Write added.

## BugFix

- DataFrame.Show() if strings contains /0 it will crash. Now fixed.
