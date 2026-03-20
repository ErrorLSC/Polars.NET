using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.C;
using LinqToDB;
using LinqToDB.Data;
using LinqToDB.Mapping;
using Polars.CSharp;
using Polars.NET.Linq.CSharpExtensions;

namespace Polars.Integration.Tests;

public class AdbcLocalTests : IDisposable
{
    private readonly AdbcDriver _driver;
    private readonly AdbcDatabase _database;
    private readonly AdbcConnection _connection;

    /// <summary>
    /// Load DuckDB C++ Driver
    /// </summary>
    /// <exception cref="FileNotFoundException"></exception>
    public AdbcLocalTests()
    {
        string soPath = Path.GetFullPath("libduckdb.so"); 

        if (!File.Exists(soPath))
        {
            throw new FileNotFoundException($"Cannot find driver file: {soPath} Please download DuckDB driver");
        }

        _driver = CAdbcDriverImporter.Load(soPath, entryPoint: "duckdb_adbc_init");

        _database = _driver.Open(new Dictionary<string, string> 
        { 
            { "path", ":memory:" } 
        });
        
        _connection = _database.Connect(new Dictionary<string, string>());
    }
    /// <summary>
    /// Load SQLite C++ Driver
    /// </summary>
    // public AdbcLocalTests()
    // {
    //     string soPath = Path.GetFullPath("libadbc_driver_sqlite.so"); 

    //     if (!File.Exists(soPath))
    //     {
    //         throw new FileNotFoundException($"annot find driver file: {soPath}。Please download SQLite driver");
    //     }

    //     _driver = CAdbcDriverImporter.Load(soPath, entryPoint: "AdbcDriverInit");

    //     _database = _driver.Open(new Dictionary<string, string> 
    //     { 
    //         { "uri", ":memory:" } 
    //     });
        
    //     _connection = _database.Connect(new Dictionary<string, string>());
    // }
    [Fact]
    [Trait("ADBC","DuckDBRead")]
    public void Test_DuckDb_Adbc_To_Polars_E2E()
    {
        using var statementInsert = _connection.CreateStatement();
        statementInsert.SqlQuery = @"
            CREATE TABLE developers (
                id INTEGER, 
                name VARCHAR, 
                language VARCHAR
            );
            INSERT INTO developers VALUES 
            (1, 'Alice', 'C#'),
            (2, 'Bob', 'Rust'),
            (3, 'Charlie', 'F#'),
            (4, null, null);";
        statementInsert.ExecuteUpdate();

        string query = "SELECT * FROM developers ORDER BY id;";
        
        var df = DataFrame.ReadAdbc(_connection,query);

        // shape: (4, 3)
        // ┌─────┬─────────┬──────────┐
        // │ id  ┆ name    ┆ language │
        // │ --- ┆ ---     ┆ ---      │
        // │ i32 ┆ str     ┆ str      │
        // ╞═════╪═════════╪══════════╡
        // │ 1   ┆ Alice   ┆ C#       │
        // │ 2   ┆ Bob     ┆ Rust     │
        // │ 3   ┆ Charlie ┆ F#       │
        // │ 4   ┆ null    ┆ null     │
        // └─────┴─────────┴──────────┘

        Assert.NotNull(df);
        Assert.Equal(4, df.Height);
        Assert.Equal(3, df.Width);

        var columns = df.ColumnNames;
        Assert.Contains("id", columns);
        Assert.Contains("name", columns);
        Assert.Contains("language", columns);

        Assert.Equal("Alice",df["name"][0]);
    }
    [Fact]
    [Trait("ADBC", "DuckDBE2E")]
    public void Test_Polars_To_Adbc_DuckDb_E2E()
    {
        var records = new[]
        {
            new { id = 101, name = "Data", language = "C" },
            new { id = 102, name = "Frame", language = "C++" },
            new { id = 103, name = "Engine", language = "Rust" }
        };
        var df = DataFrame.FromEnumerable(records);

        var updateResult = df.WriteToAdbc(_connection, "polars_e2e_test");
        Console.WriteLine($"[Write] -> Success! Rows: {updateResult.AffectedRows}");

        DataFrame verifyDf=DataFrame.ReadAdbc(_connection,"SELECT * FROM polars_e2e_test ORDER BY id");;

        // Console.WriteLine("====== Read back from DuckDB ======");
        // shape: (3, 3)
        // ┌─────┬────────┬──────────┐
        // │ id  ┆ name   ┆ language │
        // │ --- ┆ ---    ┆ ---      │
        // │ i32 ┆ str    ┆ str      │
        // ╞═════╪════════╪══════════╡
        // │ 101 ┆ Data   ┆ C        │
        // │ 102 ┆ Frame  ┆ C++      │
        // │ 103 ┆ Engine ┆ Rust     │
        // └─────┴────────┴──────────┘     

        Assert.NotNull(verifyDf);
        Assert.Equal(3, verifyDf.Height);
        Assert.Equal(3, verifyDf.Width);
    }
    [Fact]
    [Trait("ADBC", "IsolateRustStream")]
    public async Task Test_Manual_Read_Rust_Stream()
    {
        var records = new[]
        {
            new { id = 101, code = 1, type = 2 },
            new { id = 102, code = 3, type = 4 }
        };
        var df = DataFrame.FromEnumerable(records);

        using var arrowStream = df.ToArrowStream();

        var schema = arrowStream.Schema;

        int chunkCount = 0;
        while (true)
        {
            
            var batch = await arrowStream.ReadNextRecordBatchAsync();
            
            if (batch == null)
            {
                break;
            }

            batch.Dispose(); 
            chunkCount++;
        }
    }
    [Fact]
    [Trait("ADBC", "DuckDBBehavior")]
    public void Test_DuckDb_RowsAffected_Behavior()
    {
        var records = new[]
        {
            new { id = 101, name = "Data", language = "C" },
            new { id = 102, name = "Frame", language = "C++" },
            new { id = 103, name = "Engine", language = "Rust" }
        };
        var df = DataFrame.FromEnumerable(records);

        var ingestResult = df.WriteToAdbc(_connection, "polars_update_test");
        Console.WriteLine($"[Bulk Ingest] Rows affected: {ingestResult.AffectedRows} (Expected 0 or -1 due to Bulk Load)");

        using var updateStmt = _connection.CreateStatement();
        
        updateStmt.SqlQuery = "UPDATE polars_update_test SET language = 'C#' WHERE id = 101;";
        
        var updateResult = updateStmt.ExecuteUpdate();
        Console.WriteLine($"[SQL Update] Rows affected: {updateResult.AffectedRows} (Expected 0)");

        var newUpdates = new[] { new { id = 102, new_lang = "C# .NET 10" } };
        var updateDf = DataFrame.FromEnumerable(newUpdates);
        
        updateDf.WriteToAdbc(_connection, "temp_updates");

        using var eltStmt = _connection.CreateStatement();
        eltStmt.SqlQuery = @"
            UPDATE polars_update_test 
            SET language = temp_updates.new_lang 
            FROM temp_updates 
            WHERE polars_update_test.id = temp_updates.id;";
            
        var eltResult = eltStmt.ExecuteUpdate();
        Console.WriteLine($"[ELT Update from DF] Rows affected: {eltResult.AffectedRows} (Expected 0)");
        
        var finalDf = DataFrame.ReadAdbc(_connection, "SELECT * FROM polars_update_test ORDER BY id");
        // shape: (3, 3)
        // ┌─────┬────────┬────────────┐
        // │ id  ┆ name   ┆ language   │
        // │ --- ┆ ---    ┆ ---        │
        // │ i32 ┆ str    ┆ str        │
        // ╞═════╪════════╪════════════╡
        // │ 101 ┆ Data   ┆ C#         │
        // │ 102 ┆ Frame  ┆ C# .NET 10 │
        // │ 103 ┆ Engine ┆ Rust       │
        // └─────┴────────┴────────────┘
    }
    [Fact]
    [Trait("ADBC", "DuckDBE2EToxicPayload")]
    public void Test_Polars_To_Adbc_DuckDb_E2E_Toxic_Payload()
    {
        var records = new[]
        {
            new { 
                id = 101, 
                name = "Data 🚀",                 
                is_active = true,                 
                score = 99.5d,                    
                created_at = new DateTime(2023, 1, 1, 12, 0, 0, DateTimeKind.Utc), 
                optional_code = (int?)42          
            },
            new { 
                id = 102, 
                name = "Frame \0 Null",           
                is_active = false,
                score = double.NaN,             
                created_at = new DateTime(2023, 12, 31, 23, 59, 59, DateTimeKind.Utc),
                optional_code = (int?)null        
            },
            new { 
                id = 103, 
                name = string.Empty,              
                is_active = true,
                score = double.PositiveInfinity,  
                created_at = DateTime.UtcNow,
                optional_code = (int?)999
            }
        };
        var df = DataFrame.FromEnumerable(records);

        // df.Show();
        // shape: (3, 6)
        // ┌─────┬──────────────┬───────────┬───────┬────────────────────────────┬───────────────┐
        // │ id  ┆ name         ┆ is_active ┆ score ┆ created_at                 ┆ optional_code │
        // │ --- ┆ ---          ┆ ---       ┆ ---   ┆ ---                        ┆ ---           │
        // │ i32 ┆ str          ┆ bool      ┆ f64   ┆ datetime[μs]               ┆ i32           │
        // ╞═════╪══════════════╪═══════════╪═══════╪════════════════════════════╪═══════════════╡
        // │ 101 ┆ Data 🚀      ┆ true      ┆ 99.5  ┆ 2023-01-01 12:00:00        ┆ 42            │
        // │ 102 ┆ Frame ␀ Null ┆ false     ┆ NaN   ┆ 2023-12-31 23:59:59        ┆ null          │
        // │ 103 ┆              ┆ true      ┆ inf   ┆ 2026-03-20 04:40:46.531433 ┆ 999           │
        // └─────┴──────────────┴───────────┴───────┴────────────────────────────┴───────────────┘
        df.WriteToAdbc(_connection, "polars_toxic_test");

        var verifyDf = DataFrame.ReadAdbc(_connection, "SELECT * FROM polars_toxic_test ORDER BY id;");

        // Console.WriteLine("====== Read From DuckDB ======");
        // verifyDf.Show();
        // shape: (3, 6)
        // ┌─────┬──────────────┬───────────┬───────┬────────────────────────────┬───────────────┐
        // │ id  ┆ name         ┆ is_active ┆ score ┆ created_at                 ┆ optional_code │
        // │ --- ┆ ---          ┆ ---       ┆ ---   ┆ ---                        ┆ ---           │
        // │ i32 ┆ str          ┆ bool      ┆ f64   ┆ datetime[μs]               ┆ i32           │
        // ╞═════╪══════════════╪═══════════╪═══════╪════════════════════════════╪═══════════════╡
        // │ 101 ┆ Data 🚀      ┆ true      ┆ 99.5  ┆ 2023-01-01 12:00:00        ┆ 42            │
        // │ 102 ┆ Frame ␀ Null ┆ false     ┆ NaN   ┆ 2023-12-31 23:59:59        ┆ null          │
        // │ 103 ┆              ┆ true      ┆ inf   ┆ 2026-03-20 04:40:46.531433 ┆ 999           │
        // └─────┴──────────────┴───────────┴───────┴────────────────────────────┴───────────────┘

        Assert.NotNull(verifyDf);
        Assert.Equal(3, verifyDf.Height);
        Assert.Equal(6, verifyDf.Width); 
    }
    [Fact]
    [Trait("ADBC", "DotNetSpecific")]
    public void Test_DotNet_Specific_Poison()
    {
        PolarsConfig.SetEnvVar("POLARS_IMPORT_INTERVAL_AS_STRUCT", "1");

        var records = new[]
        {
            new { 
                id = 1, 
                money = 12345.6789m,                    
                uuid = Guid.Parse("12345678-1234-1234-1234-1234567890ab"), 
                birthday = new DateOnly(1990, 1, 1),    
                alarm = new TimeOnly(8, 30, 0),        
                duration = TimeSpan.FromDays(11456)    
            },
            new { 
                id = 2, 
                money = -999.99m, 
                uuid = Guid.NewGuid(), 
                birthday = new DateOnly(2026, 3, 10), 
                alarm = new TimeOnly(23, 59, 59), 
                duration = TimeSpan.FromMilliseconds(12345) 
            }
        };

        var df = DataFrame.FromEnumerable(records);

        // shape: (2, 6)
        // ┌─────┬──────────────────────────┬──────────────────────────┬────────────┬──────────┬──────────────┐
        // │ id  ┆ money                    ┆ uuid                     ┆ birthday   ┆ alarm    ┆ duration     │
        // │ --- ┆ ---                      ┆ ---                      ┆ ---        ┆ ---      ┆ ---          │
        // │ i32 ┆ decimal[38,18]           ┆ binary                   ┆ date       ┆ time     ┆ duration[μs] │
        // ╞═════╪══════════════════════════╪══════════════════════════╪════════════╪══════════╪══════════════╡
        // │ 1   ┆ 12345.678900000000000000 ┆ b"xV4\x124\x124\x12\x124 ┆ 1990-01-01 ┆ 08:30:00 ┆ 11456d       │
        // │     ┆                          ┆ \x124V…                  ┆            ┆          ┆              │
        // │ 2   ┆ -999.990000000000000000  ┆ b"\xf1\x9f\x82\x04\xbc!\ ┆ 2026-03-10 ┆ 23:59:59 ┆ 12s 345ms    │
        // │     ┆                          ┆ x02B\x…                  ┆            ┆          ┆              │
        // └─────┴──────────────────────────┴──────────────────────────┴────────────┴──────────┴──────────────┘


        df.WriteToAdbc(_connection, "polars_dotnet_poison",ingestMode:AdbcIngestMode.Create);
        var verifyDf = DataFrame.ReadAdbc(_connection, "SELECT * FROM polars_dotnet_poison ORDER BY id;");
        // verifyDf.Show();
        // shape: (2, 6)
        // ┌─────┬─────────────────────────┬────────────────────────┬────────────┬──────────┬─────────────────┐
        // │ id  ┆ money                   ┆ uuid                   ┆ birthday   ┆ alarm    ┆ duration        │
        // │ --- ┆ ---                     ┆ ---                    ┆ ---        ┆ ---      ┆ ---             │
        // │ i32 ┆ decimal[38,18]          ┆ binary                 ┆ date       ┆ time     ┆ struct[3]       │
        // ╞═════╪═════════════════════════╪════════════════════════╪════════════╪══════════╪═════════════════╡
        // │ 1   ┆ 12345.67890000000000000 ┆ b"xV4\x124\x124\x12\x1 ┆ 1990-01-01 ┆ 08:30:00 ┆ {0,0,11456d}    │
        // │     ┆ 0                       ┆ 24\x124V…              ┆            ┆          ┆                 │
        // │ 2   ┆ -999.990000000000000000 ┆ b"\xf1\x9f\x82\x04\xbc ┆ 2026-03-10 ┆ 23:59:59 ┆ {0,0,12s 345ms} │
        // │     ┆                         ┆ !\x02B\x…              ┆            ┆          ┆                 │
        // └─────┴─────────────────────────┴────────────────────────┴────────────┴──────────┴─────────────────┘
    }
    [Table("polars_e2e_test")]
    public class AdbcE2ERecord
    {
        [Column("id")] public int Id { get; set; }
        [Column("name")] public string? Name { get; set; }
        [Column("language")] public string? Language { get; set; }
    }
    [Table("stage2_pushdown_table")]
    public class PushdownRecord
    {
        [Column("id")] public int Id { get; set; }
        [Column("name")] public string? Name { get; set; }
        [Column("upper_lang")] public string? UpperLang { get; set; }
    }
    [Fact]
    [Trait("ADBC", "DuckDBE2ELINQ")]
    public void Test_Polars_And_DuckDb_Ultimate_PingPong()
    {
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
                LangStatus = x.UpperLang == "RUST" ? "God" : "Mortal" 
            })
            .ToDataFrame(); 

        // shape: (2, 3)
        // ┌─────────┬────────────────┬────────────┐
        // │ FinalId ┆ SuperName      ┆ LangStatus │
        // │ ---     ┆ ---            ┆ ---        │
        // │ i32     ┆ str            ┆ str        │
        // ╞═════════╪════════════════╪════════════╡
        // │ 1102    ┆ Frame Pro Max  ┆ Mortal     │
        // │ 1103    ┆ Engine Pro Max ┆ God        │
        // └─────────┴────────────────┴────────────┘

        finalPolarsDf.WriteToAdbc(_connection, "final_destination_table");

        using var verifyFinalDf = DataFrame.ReadAdbc(_connection, "SELECT * FROM final_destination_table ORDER BY FinalId");

        // shape: (2, 3)
        // ┌─────────┬────────────────┬────────────┐
        // │ FinalId ┆ SuperName      ┆ LangStatus │
        // │ ---     ┆ ---            ┆ ---        │
        // │ i32     ┆ str            ┆ str        │
        // ╞═════════╪════════════════╪════════════╡
        // │ 1102    ┆ Frame Pro Max  ┆ Mortal     │
        // │ 1103    ┆ Engine Pro Max ┆ God        │
        // └─────────┴────────────────┴────────────┘

        Assert.Equal(2, verifyFinalDf.Height);
        Assert.Equal(3, verifyFinalDf.Width);
    }
    
    public void Dispose()
    {
        _connection?.Dispose();
        _database?.Dispose();
        _driver?.Dispose();

    }
}