using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.C; // 你找到的那个命名空间
using Polars.CSharp;

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

    [Fact]
    [Trait("ADBC","DuckDBRead")]
    public void Test_DuckDb_Adbc_To_Polars_E2E()
    {
        // ==========================================
        // Arrange: 往 DuckDB 写入测试数据
        // ==========================================
        using var statement = _connection.CreateStatement();
        statement.SqlQuery = @"
            CREATE TABLE developers (
                id INTEGER, 
                name VARCHAR, 
                language VARCHAR
            );
            INSERT INTO developers VALUES 
            (1, 'Alice', 'C#'),
            (2, 'Bob', 'Rust'),
            (3, 'Charlie', 'F#');";
        statement.ExecuteUpdate();

        // ==========================================
        // Act: 零拷贝读取！
        // ==========================================
        string query = "SELECT * FROM developers ORDER BY id;";
        
        // 调用我们之前写好的 API
        var df = DataFrame.ReadAdbc(query, _connection);
        // var df2 = df.Head(1);
        df.Show();

        // ==========================================
        // Assert
        // ==========================================
        Assert.NotNull(df);
        Assert.Equal(3, df.Height);
        Assert.Equal(3, df.Width);

        var columns = df.ColumnNames;
        Assert.Contains("id", columns);
        Assert.Contains("name", columns);
        Assert.Contains("language", columns);
    }

    public void Dispose()
    {
        _connection?.Dispose();
        _database?.Dispose();
        _driver?.Dispose();
    }
}