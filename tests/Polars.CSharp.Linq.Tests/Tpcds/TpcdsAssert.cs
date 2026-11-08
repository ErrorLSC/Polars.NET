#nullable enable
namespace Polars.Tpcds.Tests;

/// <summary>
/// Value comparisons for cross-validating Polars results against DuckDB.
/// NULL must match NULL exactly; NaN, infinity and 0 are never accepted in place of NULL.
/// </summary>
public static class TpcdsAssert
{
    /// <summary>
    /// Asserts two floating values are equal within a relative tolerance.
    /// A relative bound (not "N decimal places") avoids spurious failures when both sides
    /// land on opposite sides of a rounding boundary, while still catching real differences.
    /// </summary>
    public static void Close(double? expected, double? actual, double relativeTolerance = 1e-9)
    {
        if (expected is null || actual is null)
        {
            Assert.Equal(expected, actual);
            return;
        }

        var e = expected.Value;
        var a = actual.Value;
        Assert.False(double.IsNaN(a) || double.IsInfinity(a), $"Expected {e}, got {a}");
        var scale = Math.Max(1.0, Math.Max(Math.Abs(e), Math.Abs(a)));
        Assert.True(Math.Abs(e - a) <= relativeTolerance * scale, $"Expected {e}, got {a} (relative tolerance {relativeTolerance})");
    }

    public static void Close(decimal? expected, decimal? actual, double relativeTolerance = 1e-9) =>
        Close((double?)expected, (double?)actual, relativeTolerance);

    /// <summary>Runs a DuckDB query and returns every row as raw column values (NULL as null).</summary>
    public static List<object?[]> DuckDbRows(string sql) =>
        DuckDbFixture.Query(sql, r =>
        {
            var row = new object?[r.FieldCount];
            for (int i = 0; i < r.FieldCount; i++)
            {
                row[i] = r.IsDBNull(i) ? null : r.GetValue(i);
            }
            return row;
        });

    /// <summary>
    /// Compares DuckDB rows with Polars rows column by column: the i-th DuckDB column against the i-th
    /// property of the result type. Integers and decimals must be equal, floating values close.
    /// </summary>
    public static void RowsEqual<T>(IReadOnlyList<object?[]> expected, IReadOnlyList<T> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        var props = typeof(T).GetProperties();
        for (int i = 0; i < expected.Count; i++)
        {
            Assert.Equal(props.Length, expected[i].Length);
            for (int c = 0; c < props.Length; c++)
            {
                var e = expected[i][c];
                var a = props[c].GetValue(actual[i]);
                var where = $"row {i}, column {props[c].Name}";
                if (e is null || a is null)
                {
                    Assert.True(e is null && a is null, $"{where}: expected {e ?? "NULL"}, got {a ?? "NULL"}");
                }
                else if (e is double or float || a is double or float)
                {
                    Close(Convert.ToDouble(e), Convert.ToDouble(a));
                }
                else if (IsExactNumber(e) && IsExactNumber(a))
                {
                    Assert.True(ToDecimal(e) == ToDecimal(a), $"{where}: expected {e}, got {a}");
                }
                else
                {
                    Assert.True(Equals(Normalize(e), Normalize(a)), $"{where}: expected {e}, got {a}");
                }
            }
        }
    }

    private static bool IsExactNumber(object v) =>
        v is decimal or long or int or short or sbyte or ulong or uint or ushort or byte or System.Numerics.BigInteger;

    private static decimal ToDecimal(object v) => v is System.Numerics.BigInteger b ? (decimal)b : Convert.ToDecimal(v);

    private static object Normalize(object v) => v switch
    {
        DateTime dt => DateOnly.FromDateTime(dt),
        _ => v
    };
}
