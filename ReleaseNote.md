
# Polars.NET 0.4.0 Release Note

![icon](assets/icon_lite.png)

## API

- PolarsConfig class added. It is intended to inject env var into Rust core.

```csharp
// Example
PolarsConfig.SetEnvVar("POLARS_DELTA_MAX_RETRIES", "30");
```

- PolarSchema now can be created from record class or dictionary

```CSharp
// Example
public record StaffRecord(string name, int age, int salary);
using var schema = PolarsSchema.From<StaffRecord>();

using (var tableSchema = PolarsSchema.From(new Dictionary<string, DataType>
{
    { "Id", DataType.Int32 },
    { "Msg", DataType.String }
}))
```

- Arg Min/Max/Unique, IndexOf, SearchSorted Expr have been added.

```csharp
// Example
using var res = df.Select(
    Col("val").ArgMin().Alias("min_idx"),
    Col("val").ArgMax().Alias("max_idx"),
    Col("val").ArgUnique().Alias("unique_idx")
);

using var res = df.Select(
    // --- IndexOf  ---
    Col("val").IndexOf(Lit(20)).Alias("idx_of_20"),
    Col("val").IndexOf(Lit(99)).Alias("idx_of_99"), 
    
    // --- SearchSorted ---
    Col("val").SearchSorted(Lit(25)).Alias("search_25"),
    
    Col("val").SearchSorted(Lit(20), side: SearchSortedSide.Left).Alias("search_20_left"),

    Col("val").SearchSorted(Lit(20), side: SearchSortedSide.Right).Alias("search_20_right")
);
```

- AsDataReader() added for DataFrame.

```Csharp
// Example: This is a MSSQL fixture
using var bulkReader = df.AsDataReader(bufferSize: 100, typeOverrides: overrides);

using var bulk = new SqlBulkCopy(_fixture.ConnectionString);
bulk.DestinationTableName = tableName;
bulk.EnableStreaming = true; 
bulk.BatchSize = 2000;

bulk.ColumnMappings.Add("OrderId", "OrderId");
bulk.ColumnMappings.Add("Region", "Region");
bulk.ColumnMappings.Add("Amount", "Amount");
bulk.ColumnMappings.Add("OrderDate", "OrderDate");

await bulk.WriteToServerAsync(bulkReader);
```

- DataFrame.ReadCsv, LazyFrame.ScanCsv now accept full PolarsSchema(with all column names and datatypes strictly) as parameter schema,  
    or partial PolarsSchema as dtypeOverride(column name won't be changed).

## New Features

- UnityCatalog Features added.
- ADBC Read&Write added.
- Polars.NET.Linq Extension package released. Enjoy LINQ powered with Polars Query Engine!

C# LINQ:

```CSharp
using Polars.NET.Linq.CSharpExtensions;
using Polars.NET.Linq;
// DataFrame
var empQuery = dfEmps.AsQueryable<EmployeeSalary>();

var query = from e in empQuery
            group e by e.DeptId into g
            where g.Sum(x => x.Salary) > 5000.0 
            orderby g.Key
            select new DeptStatsDto
            {
                DeptId = g.Key,
                TotalSalary = g.Sum(x => x.Salary),
                EmployeeCount = g.Count()
            };

var results = query.ToList();
// ToDataFrame(), ToLazyFrame() also available

// Series
using var series = Series.From("my_numbers", Enumerable.Range(1, 100).ToArray());

var query = series.AsQueryable<int>().Where(x => x > 90)
                    .OrderByDescending(x => x)
                    .Take(5)
                    .Skip(1);

var results = query.ToList();
// ToSeries() available
```

F# Computation Expression:

```F#
open Polars.NET.Linq.FSharpExtensions
open Polars.NET.Linq

let empQuery = dfEmps.AsQueryable<EmployeeSalary>()

let queryResult = 
    query {
        for e in empQuery do
        groupBy e.DeptId into g
        
        where (g.Sum(fun x -> x.Salary) > 5000.0)
        
        sortBy g.Key
        
        select {
            DeptId = g.Key
            TotalSalary = g.Sum(fun x -> x.Salary)
            EmployeeCount = g.Count()
        }
    } |> Seq.toList 
// ToDataFrame(), ToLazyFrame() also available

// Series
let numbers = [| 1 .. 100 |]
use series = Series.create("my_numbers", numbers)

let queryable = series.AsQueryable<int>()

let seriesQuery = 
    query {
        for x in queryable do
        where (x > 90)
        sortByDescending x
        select x
    }
    
let result = seriesQuery.ToSeries()
```

With ADBC, you can play them together:

```CSharp
var options = new DataOptions().UseConnectionString(ProviderName.PostgreSQL15, "Server=Dummy;");

var records = new[]
{
    new { id = 101, name = "Data", language = "C" },
    new { id = 102, name = "Frame", language = "C++" },
    new { id = 103, name = "Engine", language = "Rust" }
};
using var df = DataFrame.FromEnumerable(records);
df.WriteToAdbc(_connection, "stage1_table");

using var duckDbTranslator = new DataConnection(options); 

using var pushdownDf = duckDbTranslator.GetTable<AdbcE2ERecord>()
    .TableName("stage1_table")
    .Where(x => x.Id > 101) 
    .Select(x => new 
    {
        x.Id,
        x.Name,
        UpperLang = Sql.Upper(x.Language)
    })
    .ToDataFrameAdbc(_connection);
    
// shape: (2, 3)
// ┌─────┬────────┬───────────┐
// │ Id  ┆ Name   ┆ UpperLang │
// │ --- ┆ ---    ┆ ---       │
// │ i32 ┆ str    ┆ str       │
// ╞═════╪════════╪═══════════╡
// │ 102 ┆ Frame  ┆ C++       │
// │ 103 ┆ Engine ┆ RUST      │
// └─────┴────────┴───────────┘

using var finalPolarsDf = pushdownDf.AsQueryable<PushdownRecord>()
    .Select(x => new 
    {
        FinalId = x.Id + 1000,                            
        SuperName = x.Name + " Pro Max",                  
        LangStatus = x.UpperLang == "RUST" ? "Genshin" : "Impact" 
    })
    .ToDataFrame(); 

// shape: (2, 3)
// ┌─────────┬────────────────┬────────────┐
// │ FinalId ┆ SuperName      ┆ LangStatus │
// │ ---     ┆ ---            ┆ ---        │
// │ i32     ┆ str            ┆ str        │
// ╞═════════╪════════════════╪════════════╡
// │ 1102    ┆ Frame Pro Max  ┆ Impact     │
// │ 1103    ┆ Engine Pro Max ┆ Genshin    │
// └─────────┴────────────────┴────────────┘

finalPolarsDf.WriteToAdbc(_connection, "final_destination_table");

using var verifyFinalDf = DataFrame.ReadAdbc(_connection, "SELECT * FROM final_destination_table ORDER BY FinalId");

// shape: (2, 3)
// ┌─────────┬────────────────┬────────────┐
// │ FinalId ┆ SuperName      ┆ LangStatus │
// │ ---     ┆ ---            ┆ ---        │
// │ i32     ┆ str            ┆ str        │
// ╞═════════╪════════════════╪════════════╡
// │ 1102    ┆ Frame Pro Max  ┆ Impact     │
// │ 1103    ┆ Engine Pro Max ┆ Genshin    │
// └─────────┴────────────────┴────────────┘
```

Or with Delta Lake:

```CSharp
var tableName = $"delta_merge_composite_ordered_{Guid.NewGuid():N}";
var rootUrl = $"s3://{minio.BucketName}/{tableName}";

var options = CloudOptions.Aws(
    region: minio.Region,
    accessKey: minio.AccessKey,
    secretKey: minio.SecretKey,
    endpoint: $"http://{minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/')}"
);
options.Credentials!["AWS_ALLOW_HTTP"] = "true";
options.Credentials!["aws_s3_force_path_style"] = "true";
options.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";


using (var df = DataFrame.FromColumns(new { 
    Region = new[]  { "North", "North", "South", "South", "East" },
    StoreId = new[] { 101,     102,     101,     999,     555 },
    Stock = new[]   { 10,      20,      5,       0,       50 },
    Status = new[]  { "Active","Active","Recall","Obsolete","Active" }
}))
{
    df.WriteDelta(
        rootUrl, 
        partitionBy: "Region", 
        mode: DeltaSaveMode.Append, 
        cloudOptions: options
    );
}

using var targetReadDf = DataFrame.ReadDelta(rootUrl, cloudOptions: options);

// LINQ to Polars
var sourceQuery = targetReadDf.AsQueryable<StoreRecord>()
    .Where(x => x.Region == "North" || (x.Region == "South" && x.StoreId == 101))
    .Select(x => new StoreRecord(
        x.Region,
        x.StoreId,
        (x.Region == "North" && x.StoreId == 101) ? 100 : 
        (x.Region == "North" && x.StoreId == 102) ? 15 : 0, 
        (x.Region == "South" && x.StoreId == 101) ? "DeleteMe" : x.Status
    ))
    .Concat([
        new StoreRecord("West", 888, 60, "New"),
        new StoreRecord("West", 999, 0, "Bad")
    ]);
using var sourceDf = sourceQuery.ToDataFrame();

// shape: (5, 4)
// ┌────────┬─────────┬───────┬──────────┐
// │ Region ┆ StoreId ┆ Stock ┆ Status   │
// │ ---    ┆ ---     ┆ ---   ┆ ---      │
// │ str    ┆ i32     ┆ i32   ┆ str      │
// ╞════════╪═════════╪═══════╪══════════╡
// │ North  ┆ 101     ┆ 100   ┆ Active   │
// │ North  ┆ 102     ┆ 15    ┆ Active   │
// │ South  ┆ 101     ┆ 0     ┆ DeleteMe │
// │ West   ┆ 888     ┆ 60    ┆ New      │
// │ West   ┆ 999     ┆ 0     ┆ Bad      │
// └────────┴─────────┴───────┴──────────┘

var updateCond = Delta.Source("Stock") > Delta.Target("Stock");
var matchDeleteCond = Delta.Source("Status") == "DeleteMe";
var insertCond = Delta.Source("Stock") > 0;
var srcDeleteCond = Delta.Target("Status") == "Obsolete";

// ==========================================
// Ordered Full Merge
// ==========================================

sourceDf.MergeDeltaOrdered(
        rootUrl,
        mergeKeys: ["Region", "StoreId"], 
        cloudOptions: options
    )
    .WhenMatchedDelete(matchDeleteCond)           
    .WhenMatchedUpdate(updateCond)                
    .WhenNotMatchedInsert(insertCond)             
    .WhenNotMatchedBySourceDelete(srcDeleteCond)  
    .Execute();                                   


using var dfRes = LazyFrame.ScanDelta(rootUrl, cloudOptions: options)
    .Collect()
    .Sort(["Region", "StoreId"]);

// shape: (4, 4)
// ┌────────┬─────────┬───────┬────────┐
// │ Region ┆ StoreId ┆ Stock ┆ Status │
// │ ---    ┆ ---     ┆ ---   ┆ ---    │
// │ str    ┆ i32     ┆ i32   ┆ str    │
// ╞════════╪═════════╪═══════╪════════╡
// │ East   ┆ 555     ┆ 50    ┆ Active │
// │ North  ┆ 101     ┆ 100   ┆ Active │
// │ North  ┆ 102     ┆ 20    ┆ Active │
// │ West   ┆ 888     ┆ 60    ┆ New    │
// └────────┴─────────┴───────┴────────┘
```

## BugFix

- DataFrame.Show() if strings contains /0, it will crash. Now \0 will be replaced as ␀.

```rust
if s.contains('\0') {
    s = s.replace('\0', "␀"); 
}
```

- Delta Write/Sink/Merge/Optimize Concurrent Safety Enhanced(#15)
- C# API Expr use afte free fixed(#16)
