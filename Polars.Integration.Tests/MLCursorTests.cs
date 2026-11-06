using Microsoft.ML;
using Microsoft.ML.Data;
using Polars.CSharp;
using Polars.NET.ML.CSharpExtensions;
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

    // ==========================================
    // Dictionary (Categorical / Enum) -> KeyDataViewType
    // ==========================================

    private static string[] KeyValues(IDataView dv, string name)
    {
        VBuffer<ReadOnlyMemory<char>> kv = default;
        dv.Schema[name].Annotations.GetValue("KeyValues", ref kv);
        return [.. kv.DenseValues().Select(m => m.ToString())];
    }

    /// <summary>Decodes keys back to text through the KeyValues annotation (0 -> null).</summary>
    private static List<string?> DecodeKeys(IDataView dv, string name)
    {
        string[] categories = KeyValues(dv, name);
        return [.. ReadColumn<uint>(dv, name).Select(k => k == 0 ? null : categories[k - 1])];
    }

    [Fact]
    [Trait("ML", "Key")]
    public void Enum_ReadsAsOneBasedKeys_InCategoryOrder()
    {
        using var df = DataFrame.FromSeries(
            Series.From("E", new string?[] { "lo", "hi", "mid", null, "hi" }).Cast(DataType.Enum(["lo", "mid", "hi"])));
        var dv = df.AsDataView();

        var keyType = Assert.IsType<KeyDataViewType>(dv.Schema["E"].Type);
        Assert.Equal(typeof(uint), keyType.RawType);
        Assert.Equal(3UL, keyType.Count);
        Assert.Equal(["lo", "mid", "hi"], KeyValues(dv, "E"));
        Assert.Equal([1u, 3u, 2u, 0u, 3u], ReadColumn<uint>(dv, "E"));
    }

    [Fact]
    [Trait("ML", "Key")]
    public void Categorical_MultiChunk_KeysAreGlobalAcrossBatches()
    {
        // Polars exports a per-batch, re-compacted dictionary: "z" in chunk 2 gets the same local index as "x"
        // in chunk 1. Keys must still be global.
        using var a = DataFrame.FromSeries(Series.From("C", new string?[] { "x", "y", null, "x" }).Cast(DataType.Categorical()));
        using var b = DataFrame.FromSeries(Series.From("C", new string?[] { "z", "y" }).Cast(DataType.Categorical()));
        using var df = a.VStack(b);
        var dv = df.AsDataView();

        Assert.Equal(3UL, ((KeyDataViewType)dv.Schema["C"].Type).Count);
        Assert.Equal(["x", "y", null, "x", "z", "y"], DecodeKeys(dv, "C"));

        var keys = ReadColumn<uint>(dv, "C");
        Assert.Equal(keys[0], keys[3]);
        Assert.Equal(keys[1], keys[5]);
        Assert.NotEqual(keys[0], keys[4]);
    }

    [Fact]
    [Trait("ML", "Key")]
    public void Categorical_SlicedAndCursorSet_DecodeToSourceValues()
    {
        const int n = 60;
        string?[] values = [.. Enumerable.Range(0, n).Select(i => i % 7 == 3 ? null : $"cat{i % 5}")];
        using var a = DataFrame.FromSeries(
            Series.From("Id", Enumerable.Range(0, n / 2).ToArray()),
            Series.From("C", values[..(n / 2)]).Cast(DataType.Categorical()));
        using var b = DataFrame.FromSeries(
            Series.From("Id", Enumerable.Range(n / 2, n / 2).ToArray()),
            Series.From("C", values[(n / 2)..]).Cast(DataType.Categorical()));
        using var df = a.VStack(b);

        using var sliced = df.Slice(11, 37);
        var slicedDv = sliced.AsDataView();
        Assert.Equal(values.Skip(11).Take(37), DecodeKeys(slicedDv, "C"));

        var dv = df.AsDataView();
        string[] categories = KeyValues(dv, "C");
        int seen = 0;
        foreach (var cursor in dv.GetRowCursorSet(dv.Schema, 4))
        {
            using (cursor)
            {
                var idG = cursor.GetGetter<int>(dv.Schema["Id"]);
                var keyG = cursor.GetGetter<uint>(dv.Schema["C"]);
                int id = 0;
                uint key = 0;
                while (cursor.MoveNext())
                {
                    idG(ref id);
                    keyG(ref key);
                    Assert.Equal(values[id], key == 0 ? null : categories[key - 1]);
                    seen++;
                }
            }
        }
        Assert.Equal(n, seen);
    }

    [Fact]
    [Trait("ML", "Key")]
    public void Categorical_AllNull_HasSingleSlotKeyAndReadsMissing()
    {
        using var df = DataFrame.FromSeries(Series.From("C", new string?[] { null, null }).Cast(DataType.Categorical()));
        var dv = df.AsDataView();

        Assert.Equal(1UL, ((KeyDataViewType)dv.Schema["C"].Type).Count);
        Assert.Equal([0u, 0u], ReadColumn<uint>(dv, "C"));
    }

    [Fact]
    [Trait("ML", "Key")]
    public void Key_MlNetMapKeyToValue_AndRoundTripToPolars()
    {
        string?[] values = ["red", "green", null, "blue", "green"];
        using var df = DataFrame.FromSeries(Series.From("Color", values).Cast(DataType.Categorical()));
        var dv = df.AsDataView();

        // ML.NET consumes the KeyValues annotation.
        var ml = new MLContext(seed: 0);
        var mapped = ml.Transforms.Conversion.MapKeyToValue("ColorText", "Color").Fit(dv).Transform(dv);
        var text = ReadColumn<ReadOnlyMemory<char>>(mapped, "ColorText").Select(m => m.ToString());
        Assert.Equal(values.Select(v => v ?? ""), text);

        // Key column back to Polars comes out as text.
        using var back = dv.ToDataFrame();
        Assert.Equal(values, back["Color"].Cast(DataType.String).ToArray<string?>());
    }

    [Fact]
    [Trait("ML", "ParallelCursor")]
    public void MlNetTransform_PreservesSourceRowOrder()
    {
        // ML.NET transforms read through GetRowCursorSet and merge the set by cursor Batch;
        // every cursor reporting Batch = 0 scrambled the row order.
        const int n = 10_000;
        using var df = DataFrame.FromSeries(Series.From("Id", Enumerable.Range(0, n).ToArray()));
        var dv = df.AsDataView();

        var ml = new MLContext(seed: 0);
        var transformed = ml.Transforms.CopyColumns("Id2", "Id").Fit(dv).Transform(dv);

        Assert.Equal(Enumerable.Range(0, n), ReadColumn<int>(transformed, "Id2"));
    }
}
