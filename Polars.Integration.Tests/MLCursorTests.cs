using Microsoft.ML;
using Microsoft.ML.Data;
using Polars.CSharp;
using Polars.NET.ML.DataView;

namespace Polars.Integration.Tests;

/// <summary>
/// Value-level checks for PolarsRowCursor: nulls, temporal types, variable-length vectors and sliced
/// (non-zero offset) Arrow arrays.
/// </summary>
public class PolarsRowCursorTests
{
    private static List<T> ReadColumn<T>(IDataView dv, string name)
    {
        var col = dv.Schema[name];
        using var cursor = dv.GetRowCursor([col]);
        var getter = cursor.GetGetter<T>(col);
        var result = new List<T>();
        T value = default!;
        while (cursor.MoveNext())
        {
            getter(ref value);
            result.Add(value);
        }
        return result;
    }

    private static List<T[]> ReadVectors<T>(IDataView dv, string name)
    {
        var col = dv.Schema[name];
        using var cursor = dv.GetRowCursor([col]);
        var getter = cursor.GetGetter<VBuffer<T>>(col);
        var result = new List<T[]>();
        VBuffer<T> value = default;
        while (cursor.MoveNext())
        {
            getter(ref value);
            result.Add(value.DenseValues().ToArray());
        }
        return result;
    }

    [Fact]
    [Trait("ML", "DataView")]
    public void Scalars_WithNulls_MapToMlNetMissingValues()
    {
        using var df = DataFrame.FromSeries(
            Series.From("I", new int?[] { 1, null, 3 }),
            Series.From("L", new long?[] { null, -5L, 7L }),
            Series.From("F", new float?[] { 1.5f, null, -2f }),
            Series.From("D", new double?[] { null, 2.25, 3.5 }),
            Series.From("B", new bool?[] { true, null, false }),
            Series.From("S", new string?[] { "a", null, "长字符串-long-enough-to-not-be-inlined" })
        );
        var dv = df.AsDataView();

        Assert.Equal([1, 0, 3], ReadColumn<int>(dv, "I"));
        Assert.Equal([0L, -5L, 7L], ReadColumn<long>(dv, "L"));

        var f = ReadColumn<float>(dv, "F");
        Assert.Equal(1.5f, f[0]);
        Assert.True(float.IsNaN(f[1]));
        Assert.Equal(-2f, f[2]);

        var d = ReadColumn<double>(dv, "D");
        Assert.True(double.IsNaN(d[0]));
        Assert.Equal([2.25, 3.5], d.Skip(1));

        Assert.Equal([true, false, false], ReadColumn<bool>(dv, "B"));

        var s = ReadColumn<ReadOnlyMemory<char>>(dv, "S");
        Assert.Equal("a", s[0].ToString());
        Assert.True(s[1].IsEmpty);
        Assert.Equal("长字符串-long-enough-to-not-be-inlined", s[2].ToString());
    }

    [Fact]
    [Trait("ML", "DataView")]
    public void Temporal_Columns_ReadAsDateTimeAndTimeSpan()
    {
        DateTime[] times = [new(2024, 2, 29, 12, 34, 56, 789), new(1969, 7, 20, 20, 17, 0), new(2000, 1, 1)];
        TimeSpan[] spans = [TimeSpan.FromMilliseconds(1234.5), TimeSpan.FromDays(-2), TimeSpan.Zero];

        using var df = DataFrame.FromSeries(Series.From("T", times), Series.From("Dur", spans));
        var dv = df.AsDataView();

        Assert.Equal(times, ReadColumn<DateTime>(dv, "T"));
        Assert.Equal(spans, ReadColumn<TimeSpan>(dv, "Dur"));
    }

    [Fact]
    [Trait("ML", "Tensor")]
    public void VariableLengthVectors_ReadAllElements()
    {
        float[][] floats = [[1f, 2f, 3f], [], [4.5f], [6f, 7f]];
        int[][] ints = [[10], [20, 30, 40], [], [50, 60]];

        using var df = DataFrame.FromSeries(Series.From("Fv", floats), Series.From("Iv", ints));
        var dv = df.AsDataView();

        var fvType = Assert.IsType<VectorDataViewType>(dv.Schema["Fv"].Type);
        Assert.Equal(0, fvType.Size);

        var fv = ReadVectors<float>(dv, "Fv");
        var iv = ReadVectors<int>(dv, "Iv");
        Assert.Equal(floats.Length, fv.Count);
        for (int i = 0; i < floats.Length; i++)
        {
            Assert.Equal(floats[i], fv[i]);
            Assert.Equal(ints[i], iv[i]);
        }
    }

    [Fact]
    [Trait("ML", "Tensor")]
    public void SlicedFrame_ReadsCorrectRows_ForScalarsAndVectors()
    {
        const int n = 50;
        var ids = Enumerable.Range(0, n).ToArray();
        var flags = ids.Select(i => i % 3 == 0).ToArray();
        var names = ids.Select(i => i % 4 == 0 ? $"name-{i}-with-a-long-enough-suffix" : $"n{i}").ToArray();
        var fixedVecs = new float[n, 3];
        for (int i = 0; i < n; i++) for (int j = 0; j < 3; j++) fixedVecs[i, j] = i * 10 + j;
        var varVecs = ids.Select(i => Enumerable.Range(0, i % 4).Select(j => i * 100 + j).ToArray()).ToArray();

        using var full = DataFrame.FromSeries(
            Series.From("Id", ids),
            Series.From("Flag", flags),
            Series.From("Name", names),
            Series.From("Fixed", fixedVecs),
            Series.From("Var", varVecs));

        // Offsets that are not byte-aligned exercise the bit-packed (Boolean / validity) paths.
        const int offset = 13, length = 29;
        using var sliced = full.Slice(offset, length);
        var dv = sliced.AsDataView();

        var expectedIds = ids.Skip(offset).Take(length).ToArray();
        Assert.Equal(expectedIds, ReadColumn<int>(dv, "Id"));
        Assert.Equal(flags.Skip(offset).Take(length), ReadColumn<bool>(dv, "Flag"));
        Assert.Equal(names.Skip(offset).Take(length), ReadColumn<ReadOnlyMemory<char>>(dv, "Name").Select(m => m.ToString()));

        var fixedRead = ReadVectors<float>(dv, "Fixed");
        var varRead = ReadVectors<int>(dv, "Var");
        for (int r = 0; r < length; r++)
        {
            int i = expectedIds[r];
            Assert.Equal([i * 10f, i * 10f + 1, i * 10f + 2], fixedRead[r]);
            Assert.Equal(varVecs[i], varRead[r]);
        }
    }

    [Fact]
    [Trait("ML", "ParallelCursor")]
    public void CursorSet_Vectors_MatchSourceRows()
    {
        const int n = 101;
        var ids = Enumerable.Range(0, n).ToArray();
        var fixedVecs = new float[n, 2];
        for (int i = 0; i < n; i++) { fixedVecs[i, 0] = i; fixedVecs[i, 1] = -i; }
        var varVecs = ids.Select(i => Enumerable.Repeat((float)i, i % 5).ToArray()).ToArray();

        using var df = DataFrame.FromSeries(
            Series.From("Id", ids), Series.From("Fixed", fixedVecs), Series.From("Var", varVecs));
        var dv = df.AsDataView();

        int seen = 0;
        foreach (var cursor in dv.GetRowCursorSet(dv.Schema, 3))
        {
            using (cursor)
            {
                var idG = cursor.GetGetter<int>(dv.Schema["Id"]);
                var fixedG = cursor.GetGetter<VBuffer<float>>(dv.Schema["Fixed"]);
                var varG = cursor.GetGetter<VBuffer<float>>(dv.Schema["Var"]);
                int id = 0;
                VBuffer<float> fv = default, vv = default;
                while (cursor.MoveNext())
                {
                    idG(ref id);
                    fixedG(ref fv);
                    varG(ref vv);
                    Assert.Equal([(float)id, -id], fv.DenseValues().ToArray());
                    Assert.Equal(varVecs[id], vv.DenseValues().ToArray());
                    seen++;
                }
            }
        }
        Assert.Equal(n, seen);
    }

    [Fact]
    [Trait("ML", "DataView")]
    public void GetGetter_RejectsMismatchedTypeAndInactiveColumn()
    {
        using var df = DataFrame.FromSeries(Series.From("A", [1, 2]), Series.From("B", [1.0, 2.0]));
        var dv = df.AsDataView();

        using var cursor = dv.GetRowCursor([dv.Schema["A"]]);
        Assert.Throws<InvalidOperationException>(() => { cursor.GetGetter<long>(dv.Schema["A"]); });
        Assert.Throws<InvalidOperationException>(() => { cursor.GetGetter<double>(dv.Schema["B"]); });
    }
}
