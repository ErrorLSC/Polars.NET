using DuckDB.NET.Data;

namespace Polars.Tpcds.Tests;

public static class DuckDbFixture
{
    public static List<T> Query<T>(string sql, Func<System.Data.Common.DbDataReader, T> map)
    {
        using var connection = new DuckDBConnection("Data Source=:memory:");
        connection.Open();
        // TPC-DS leaves NULL ordering to the implementation. Use LINQ's ordering (NULL is the smallest
        // value): NULLS FIRST ascending, NULLS LAST descending. An explicit NULLS clause still wins.
        using (var setup = connection.CreateCommand())
        {
            setup.CommandText = "SET default_null_order = 'nulls_first_on_asc_last_on_desc'";
            setup.ExecuteNonQuery();
        }
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
