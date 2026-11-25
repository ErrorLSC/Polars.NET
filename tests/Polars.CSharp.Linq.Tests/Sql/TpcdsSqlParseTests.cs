using System.Text.Json.Nodes;
using DuckDB.NET.Data;
using Kaminari.Samidare.Sql;

namespace Polars.CSharp.Linq.Tests.Sql;

/// <summary>
/// The SQL parser reads DuckDB's 99 TPC-DS texts as DuckDB does: each query, written back fully parenthesized, is
/// the same syntax tree for DuckDB (json_serialize_sql, source locations left out) as the original text. A wrong
/// precedence or a misread clause changes the tree.
/// </summary>
public class TpcdsSqlParseTests
{
    public static TheoryData<int> Queries => [.. Enumerable.Range(1, 99)];

    internal static string Text(int n) => File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Sql", "Tpcds", $"q{n:00}.sql"));

    [Theory]
    [MemberData(nameof(Queries))]
    public void Query_groups_as_DuckDB_reads_it(int n)
    {
        var text = Text(n);
        var formatted = SqlFormat.query(SqlParser.parseQuery(SqlDialect.Standard, text));
        var expected = DuckDbTree(text);
        var actual = DuckDbTree(formatted);
        Assert.True(JsonNode.DeepEquals(expected, actual), $"Q{n} reads differently:\n{formatted}");
    }

    static JsonNode DuckDbTree(string sql)
    {
        using var connection = new DuckDBConnection("Data Source=:memory:");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT json_serialize_sql(CAST($sql AS VARCHAR))";
        command.Parameters.Add(new DuckDBParameter("sql", sql));
        var json = JsonNode.Parse((string)command.ExecuteScalar()!)!;
        Assert.False((bool)json["error"]!, $"DuckDB cannot read the text: {json}\n{sql}");
        return WithoutLocations(json);
    }

    static JsonNode WithoutLocations(JsonNode node)
    {
        switch (node)
        {
            case JsonObject o:
                o.Remove("query_location");
                foreach (var (_, value) in o.ToList())
                    if (value is not null) WithoutLocations(value);
                break;
            case JsonArray a:
                foreach (var value in a)
                    if (value is not null) WithoutLocations(value);
                break;
        }
        return node;
    }
}
