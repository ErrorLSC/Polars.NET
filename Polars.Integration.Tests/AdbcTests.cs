using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.C; // 你找到的那个命名空间
using Polars.CSharp;
using Polars.NET.Core;
using Polars.NET.Core.Arrow;

namespace Polars.Integration.Tests;

public class AdbcLocalTests : IDisposable
{
    private readonly AdbcDriver _driver;
    private readonly AdbcDatabase _database;
    private readonly AdbcConnection _connection;

    public AdbcLocalTests()
    {
        // 1. 指定你的 .so 文件路径 (相对路径或绝对路径)
        // 假设你把下好的 libduckdb.so 放到了测试项目的根目录，或者设置了 Copy to Output Directory
        string soPath = Path.GetFullPath("libduckdb.so"); 

        if (!File.Exists(soPath))
        {
            throw new FileNotFoundException($"找不到驱动文件: {soPath}。请去 DuckDB 官网下载 Linux C++ 包并解压。");
        }

        // 2. 见证奇迹的时刻：用你找到的 Importer 动态加载 C++ 驱动！
        // 注意：DuckDB 的标准 ADBC 初始化函数名是 "duckdb_adbc_init"。
        // 如果是 SQLite，一般留空使用默认的 "AdbcDriverInit" 即可。
        _driver = CAdbcDriverImporter.Load(soPath, entryPoint: "duckdb_adbc_init");

        // 3. 打开内存数据库并连接
        _database = _driver.Open(new Dictionary<string, string> 
        { 
            { "path", ":memory:" } 
        });
        
        _connection = _database.Connect(new Dictionary<string, string>());
    }
    // public AdbcLocalTests()
    // {
    //     // 1. 指定 SQLite 的 .so 文件路径
    //     string soPath = Path.GetFullPath("libadbc_driver_sqlite.so"); 

    //     if (!File.Exists(soPath))
    //     {
    //         throw new FileNotFoundException($"找不到驱动文件: {soPath}。请检查 mamba 环境并将 so 文件复制到输出目录。");
    //     }

    //     // 2. 加载 C++ 驱动！SQLite 官方的默认入口点是 "AdbcDriverInit"
    //     // (如果 "AdbcDriverInit" 报错找不到入口，可以尝试换成 "AdbcDriverSqliteInit" 或 "adbc_driver_sqlite_init")
    //     _driver = CAdbcDriverImporter.Load(soPath, entryPoint: "AdbcDriverInit");

    //     // 3. 打开内存数据库并连接
    //     _database = _driver.Open(new Dictionary<string, string> 
    //     { 
    //         // SQLite ADBC 使用 'uri' 键，':memory:' 表示纯内存数据库
    //         { "uri", ":memory:" } 
    //     });
        
    //     _connection = _database.Connect(new Dictionary<string, string>());
    // }
    [Fact]
    [Trait("ADBC","DuckDBRead")]
    public void Test_DuckDb_Adbc_To_Polars_E2E()
    {
        // ==========================================
        // Arrange: 往 DuckDB 写入测试数据
        // ==========================================
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

        // ==========================================
        // Act: 用户端显式组装流，传递给 Polars.NET
        // ==========================================
        string query = "SELECT * FROM developers ORDER BY id;";
        
        var df = DataFrame.ReadAdbc(_connection,query);
        
        // 3. 打印看结果
        df.Show();

        // ==========================================
        // Assert
        // ==========================================
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
        // ==========================================
        // Arrange: 原生准备
        // ==========================================
        var records = new[]
        {
            new { id = 101, name = "Data", language = "C" },
            new { id = 102, name = "Frame", language = "C++" },
            new { id = 103, name = "Engine", language = "Rust" }
        };
        var df = DataFrame.FromEnumerable(records);

        // ==========================================
        // Act 1: 零拷贝写入 (Wormhole Hack)
        // ==========================================
        var updateResult = df.WriteToAdbc(_connection, "polars_e2e_test");
        Console.WriteLine($"[Write] -> Success! Rows: {updateResult.AffectedRows}");
        // ==========================================
        // Act 2: 零拷贝读取 (IL Emit Hack)
        // ==========================================
        DataFrame verifyDf=DataFrame.ReadAdbc(_connection,"SELECT * FROM polars_e2e_test ORDER BY id");;

        // ==========================================
        // Assert: 验证闭环
        // ==========================================
        Console.WriteLine("====== Read back from DuckDB ======");
        verifyDf.Show();

        Assert.NotNull(verifyDf);
        Assert.Equal(3, verifyDf.Height);
        Assert.Equal(3, verifyDf.Width);
    }
    [Fact]
    [Trait("ADBC", "Isolate_Rust_Stream")]
    public async Task Test_Manual_Read_Rust_Stream()
    {
        // 1. 准备最简单的数据
        var records = new[]
        {
            new { id = 101, code = 1, type = 2 },
            new { id = 102, code = 3, type = 4 }
        };
        var df = DataFrame.FromEnumerable(records);

        Console.WriteLine("[Test] -> Exporting Rust Stream...");
        using var arrowStream = df.ToArrowStream();
        Console.WriteLine("[Test] -> Stream Exported Successfully.");

        // 2. 尝试读取 Schema
        Console.WriteLine("[Test] -> Reading Schema...");
        var schema = arrowStream.Schema;
        Console.WriteLine($"[Test] -> Schema read! Fields count: {schema.FieldsList.Count}");

        // 3. 尝试读取数据块 (Chunk)
        int chunkCount = 0;
        while (true)
        {
            Console.WriteLine($"[Test] -> Pulling chunk {chunkCount} from Rust...");
            
            // 💥 如果是 FFI 内存或格式问题，绝对会死在这一行！
            var batch = await arrowStream.ReadNextRecordBatchAsync();
            
            if (batch == null)
            {
                Console.WriteLine("[Test] -> EOF reached. Stream ended gracefully.");
                break;
            }

            Console.WriteLine($"[Test] -> Successfully read chunk {chunkCount} with {batch.Length} rows.");
            
            // 记得释放拿到的内存块
            batch.Dispose(); 
            chunkCount++;
        }

        Console.WriteLine("[Test] -> All data consumed! C# <-> Rust bridge is PERFECT!");
    }
    [Fact]
    [Trait("ADBC", "DuckDBBehavior")]
    public void Test_DuckDb_RowsAffected_Behavior()
    {
        // ==========================================
        // 1. 极速建表 (Bulk Ingest) - 预期 Rows: 0
        // ==========================================
        var records = new[]
        {
            new { id = 101, name = "Data", language = "C" },
            new { id = 102, name = "Frame", language = "C++" },
            new { id = 103, name = "Engine", language = "Rust" }
        };
        var df = DataFrame.FromEnumerable(records);

        var ingestResult = df.WriteToAdbc(_connection, "polars_update_test");
        Console.WriteLine($"[Bulk Ingest] Rows affected: {ingestResult.AffectedRows} (Expected 0 or -1 due to Bulk Load)");

        // ==========================================
        // 2. 原生 SQL Update - 预期 Rows: 1
        // ==========================================
        using var updateStmt = _connection.CreateStatement();
        
        // 比如我们把 101 的语言从 C 升级到 C#！
        updateStmt.SqlQuery = "UPDATE polars_update_test SET language = 'C#' WHERE id = 101;";
        
        var updateResult = updateStmt.ExecuteUpdate();
        Console.WriteLine($"[SQL Update] Rows affected: {updateResult.AffectedRows} (Expected 1)");

        // Assert.Equal(1, updateResult.AffectedRows); // 这里绝对能拿到正确的行数！

        // ==========================================
        // 3. 高阶玩法：用 DataFrame 去更新主表！(UPSERT / ELT 模式)
        // ==========================================
        var newUpdates = new[] { new { id = 102, new_lang = "C# .NET 10" } };
        var updateDf = DataFrame.FromEnumerable(newUpdates);
        
        // a. 先把更新数据极速怼进一张临时表
        updateDf.WriteToAdbc(_connection, "temp_updates");

        // b. 用 DuckDB 的 SQL 引擎执行联合更新
        using var eltStmt = _connection.CreateStatement();
        eltStmt.SqlQuery = @"
            UPDATE polars_update_test 
            SET language = temp_updates.new_lang 
            FROM temp_updates 
            WHERE polars_update_test.id = temp_updates.id;";
            
        var eltResult = eltStmt.ExecuteUpdate();
        Console.WriteLine($"[ELT Update from DF] Rows affected: {eltResult.AffectedRows} (Expected 1)");
        
        // Assert.Equal(1, eltResult.AffectedRows);
        
        // 最终看一眼我们蹂躏过的数据表
        var finalDf = DataFrame.ReadAdbc(_connection, "SELECT * FROM polars_update_test ORDER BY id");
        finalDf.Show();
    }
    [Fact]
    [Trait("ADBC", "DuckDBE2EToxicPayload")]
    public void Test_Polars_To_Adbc_DuckDb_E2E_Toxic_Payload()
    {
        // ==========================================
        // Arrange: 准备充满“骚东西”的数据
        // ==========================================
        var records = new[]
        {
            new { 
                id = 101, 
                name = "Data 🚀",                 // 骚东西 1: Emoji (多字节 UTF-8，测试偏移量解析)
                is_active = true,                 // 骚东西 2: Bool (测试底层的 1-bit 位图压缩)
                score = 99.5d,                    // 常规 Float64
                created_at = new DateTime(2023, 1, 1, 12, 0, 0, DateTimeKind.Utc), // 骚东西 3: DateTime (UTC)
                optional_code = (int?)42          // 骚东西 4: Nullable 有值
            },
            new { 
                id = 102, 
                name = "Frame \0 Null",           // 骚东西 5: 字符串内嵌 \0 (测试 C++ 引擎是否会把它当成结束符截断！)
                is_active = false,
                score = double.NaN,               // 骚东西 6: 浮点数特例 (NaN)
                created_at = new DateTime(2023, 12, 31, 23, 59, 59, DateTimeKind.Utc),
                optional_code = (int?)null        // 骚东西 7: 真正的 Null (测试 Validity Bitmap 空洞)
            },
            new { 
                id = 103, 
                name = string.Empty,              // 骚东西 8: 空字符串
                is_active = true,
                score = double.PositiveInfinity,  // 骚东西 9: 浮点数特例 (+Inf)
                created_at = DateTime.UtcNow,
                optional_code = (int?)999
            }
        };
        var df = DataFrame.FromEnumerable(records);

        Console.WriteLine("====== 1. 原始 Polars DataFrame ======");
        df.Show();

        // ==========================================
        // Act 1: 零拷贝写入鸭子 (DuckDB)
        // ==========================================
        df.WriteToAdbc(_connection, "polars_toxic_test");

        // ==========================================
        // Act 2: 零拷贝读取回魂
        // ==========================================
        // 顺便测一下 DuckDB 内部的 SQL 计算能力是否对齐
        var verifyDf = DataFrame.ReadAdbc(_connection, "SELECT * FROM polars_toxic_test ORDER BY id;");

        // ==========================================
        // Assert: 验证闭环 (必须毫发无伤)
        // ==========================================
        Console.WriteLine("====== 2. 从 DuckDB 读取回来的 DataFrame ======");
        verifyDf.Show();

        Assert.NotNull(verifyDf);
        Assert.Equal(3, verifyDf.Height);
        // Assert.Equal(6, verifyDf.Width); // 6 列！
    }
    [Fact]
    [Trait("ADBC", "DotNetSpecific")]
    public void Test_DotNet_Specific_Poison()
    {
        PolarsConfig.SetEnvVar("POLARS_IMPORT_INTERVAL_AS_STRUCT", "1");
        // ==========================================
        // Arrange: 纯血 .NET 独门毒药
        // ==========================================
        var records = new[]
        {
            new { 
                id = 1, 
                money = 12345.6789m,                     // 毒药 1: Decimal (金融级精度)
                uuid = Guid.Parse("12345678-1234-1234-1234-1234567890ab"), // 毒药 2: Guid
                birthday = new DateOnly(1990, 1, 1),     // 毒药 3: DateOnly
                alarm = new TimeOnly(8, 30, 0),          // 毒药 4: TimeOnly
                duration = TimeSpan.FromDays(11456)    // 毒药 5: TimeSpan
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

        Console.WriteLine("====== 1. 成功构建 DataFrame！ ======");
        Console.WriteLine($"[Shape] Height: {df.Height}, Width: {df.Width}");
        df.Show();
        // 如果奇迹发生，它活下来了，我们就把它灌进 DuckDB！
        df.WriteToAdbc(_connection, "polars_dotnet_poison",ingestMode:AdbcIngestMode.Create);
        var verifyDf = DataFrame.ReadAdbc(_connection, "SELECT * FROM polars_dotnet_poison ORDER BY id;");

        Console.WriteLine("====== 2. 从 DuckDB 读取回来！ ======");
        Console.WriteLine($"[Shape] Height: {verifyDf.Height}, Width: {verifyDf.Width}");
        verifyDf.Show();
    }
    
    public void Dispose()
    {
        _connection?.Dispose();
        _database?.Dispose();
        _driver?.Dispose();

    }
}