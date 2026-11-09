using DuckDB.NET.Data;

namespace Polars.Tpcds;

/// <summary>Runs the DuckDB reference SQL and compares its result with a Polars result, column by column.</summary>
public static class TpcdsVerification
{
    /// <summary>
    /// Opens an in-memory DuckDB connection with LINQ's NULL ordering (NULL is the smallest value). TPC-DS leaves it
    /// to the implementation; an explicit NULLS clause still wins.
    /// </summary>
    public static DuckDBConnection OpenDuckDb()
    {
        var connection = new DuckDBConnection("Data Source=:memory:");
        connection.Open();
        using var setup = connection.CreateCommand();
        setup.CommandText = "SET default_null_order = 'nulls_first_on_asc_last_on_desc'";
        setup.ExecuteNonQuery();
        return connection;
    }

    /// <summary>Every row of the query as raw column values, NULL as null.</summary>
    public static List<object?[]> Rows(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        using var reader = command.ExecuteReader();
        var rows = new List<object?[]>();
        while (reader.Read())
        {
            var row = new object?[reader.FieldCount];
            for (int i = 0; i < reader.FieldCount; i++)
                row[i] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            rows.Add(row);
        }

        return rows;
    }

    /// <summary>
    /// The first difference between DuckDB rows and Polars rows, or null if there is none: the i-th DuckDB column
    /// against the i-th property of the row (a scalar row is its only column). Integers and decimals must be equal,
    /// floating values within a relative 1e-9, and NULL only matches NULL; NaN and infinity never match a number.
    /// </summary>
    public static string? FindDifference(IReadOnlyList<object?[]> expected, IReadOnlyList<object?> actual)
    {
        if (expected.Count != actual.Count)
            return $"{expected.Count} rows expected, {actual.Count} found";

        for (int i = 0; i < expected.Count; i++)
        {
            var (names, values) = Columns(actual[i]);
            if (names.Length != expected[i].Length)
                return $"row {i}: {expected[i].Length} columns expected, {names.Length} found";

            for (int c = 0; c < names.Length; c++)
            {
                var (e, a) = (expected[i][c], values[c]);
                if (!Same(e, a))
                    return $"row {i}, column {names[c]}: expected {e ?? "NULL"}, found {a ?? "NULL"}";
            }
        }

        return null;
    }

    private static bool Same(object? e, object? a)
    {
        if (e is null || a is null)
            return e is null && a is null;
        if (e is double or float || a is double or float)
            return Close(Convert.ToDouble(e), Convert.ToDouble(a));
        if (IsExactNumber(e) && IsExactNumber(a))
            return ToDecimal(e) == ToDecimal(a);
        return Equals(Normalize(e), Normalize(a));
    }

    private static bool Close(double e, double a) =>
        !double.IsNaN(a) && !double.IsInfinity(a)
        && Math.Abs(e - a) <= 1e-9 * Math.Max(1.0, Math.Max(Math.Abs(e), Math.Abs(a)));

    private static (string[] Names, object?[] Values) Columns(object? row)
    {
        if (row is null or string or decimal or DateOnly or DateTime || row.GetType().IsPrimitive)
            return (["value"], [row]);
        var props = row.GetType().GetProperties();
        return (props.Select(p => p.Name).ToArray(), props.Select(p => p.GetValue(row)).ToArray());
    }

    private static bool IsExactNumber(object v) =>
        v is decimal or long or int or short or sbyte or ulong or uint or ushort or byte or BigInteger;

    private static decimal ToDecimal(object v) => v is BigInteger b ? (decimal)b : Convert.ToDecimal(v);

    private static object Normalize(object v) => v is DateTime dt ? DateOnly.FromDateTime(dt) : v;
}
