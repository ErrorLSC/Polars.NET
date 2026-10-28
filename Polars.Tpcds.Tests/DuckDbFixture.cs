using DuckDB.NET.Data;

namespace Polars.Tpcds.Tests;

public static class DuckDbFixture
{
    public static List<T> Query<T>(string sql, Func<System.Data.Common.DbDataReader, T> map)
    {
        using var connection = new DuckDBConnection("Data Source=:memory:");
        connection.Open();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var results = new List<T>();
        while (reader.Read())
        {
            results.Add(map(reader));
        }
        return results;
    }
}
