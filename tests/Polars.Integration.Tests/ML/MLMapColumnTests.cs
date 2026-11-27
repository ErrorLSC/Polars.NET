using Microsoft.ML;
using Microsoft.ML.Data;
using Polars.CSharp;
using Polars.NET.ML.DataView;

namespace Polars.Integration.Tests;

/// <summary>
/// Map columns in the ML.NET DataView: a map of numbers as a sparse vector over its key vocabulary (scanned or
/// given, slot names the keys), a map of text or booleans as two variable-length vectors, other maps refused.
/// </summary>
public class MLMapColumnTests
{
    private static List<VBuffer<T>> Read<T>(IDataView dv, string name)
    {
        var col = dv.Schema[name];
        using var cursor = dv.GetRowCursor([col]);
        var getter = cursor.GetGetter<VBuffer<T>>(col);
        var result = new List<VBuffer<T>>();
        VBuffer<T> value = default;
        while (cursor.MoveNext())
        {
            getter(ref value);
            VBuffer<T> copy = default;
            value.CopyTo(ref copy);
            result.Add(copy);
        }
        return result;
    }

    private static string[] SlotNames(IDataView dv, string name)
    {
        VBuffer<ReadOnlyMemory<char>> names = default;
        dv.Schema[name].GetSlotNames(ref names);
        return [.. names.DenseValues().Select(static n => n.ToString())];
    }

    [Fact]
    [Trait("ML", "DataView")]
    public void A_Map_Of_Numbers_Is_A_Sparse_Vector_Over_Its_Sorted_Keys()
    {
        using var df = DataFrame.FromRows(
        [
            new Scores(1, new() { ["math"] = 90, ["art"] = 70 }),
            new Scores(2, new() { ["music"] = 80 }),
            new Scores(3, null),
            new Scores(4, new() { ["art"] = null, ["math"] = 50, ["music"] = 60 }),
        ]);
        IDataView dv = df.AsDataView();

        var type = Assert.IsType<VectorDataViewType>(dv.Schema["Marks"].Type);
        Assert.Equal(NumberDataViewType.Double, type.ItemType);
        Assert.Equal(3, type.Size);
        Assert.Equal(["art", "math", "music"], SlotNames(dv, "Marks"));

        var rows = Read<double>(dv, "Marks");
        Assert.Equal([70, 90, 0], rows[0].DenseValues());
        Assert.False(rows[1].IsDense);
        Assert.Equal([0, 0, 80], rows[1].DenseValues());
        Assert.Equal([0, 0, 0], rows[2].DenseValues()); // a null map: all zero
        Assert.Equal([double.NaN, 50, 60], rows[3].DenseValues()); // a null value: missing
        Assert.Equal(4, Read<double>(dv, "Marks").Count);
    }

    [Fact]
    [Trait("ML", "DataView")]
    public void A_Given_Vocabulary_Fixes_The_Slots()
    {
        // As at prediction time: the training data's keys, whatever this frame holds (an unknown key is left out).
        using var df = DataFrame.FromRows([new Counts(new() { ["b"] = 2, ["z"] = 9 }), new Counts(new() { ["a"] = 1 })]);
        IDataView dv = df.AsDataView(new Dictionary<string, IReadOnlyList<string>> { ["N"] = ["a", "b", "c"] });

        Assert.Equal(3, ((VectorDataViewType)dv.Schema["N"].Type).Size);
        Assert.Equal(["a", "b", "c"], SlotNames(dv, "N"));
        var rows = Read<long>(dv, "N");
        Assert.Equal([0L, 2, 0], rows[0].DenseValues());
        Assert.Equal([1L, 0, 0], rows[1].DenseValues());
    }

    [Fact]
    [Trait("ML", "DataView")]
    public void A_Repeated_Key_Keeps_Its_First_Value()
    {
        // [x: 1, y: 2, x: 5], built through Arrow so the duplicate survives (List.ToMap would merge it).
        using var df = DataFrame.FromArrow(Duplicated());
        var rows = Read<long>(df.AsDataView(), "m");
        Assert.Equal([1L, 2], rows[0].DenseValues());
    }

    [Fact]
    [Trait("ML", "DataView")]
    public void A_Map_Of_Text_Is_Its_Keys_And_Its_Values()
    {
        using var df = DataFrame.FromRows([new Tags(1, new() { ["color"] = "red", ["size"] = null }), new Tags(2, null), new Tags(3, [])]);
        IDataView dv = df.AsDataView();

        Assert.Equal(["Id", "T.Keys", "T.Values"], dv.Schema.Select(static c => c.Name));
        Assert.Equal(new VectorDataViewType(TextDataViewType.Instance), dv.Schema["T.Keys"].Type);
        var keys = Read<ReadOnlyMemory<char>>(dv, "T.Keys");
        var values = Read<ReadOnlyMemory<char>>(dv, "T.Values");
        Assert.Equal(["color", "size"], keys[0].DenseValues().Select(static k => k.ToString()));
        Assert.Equal(["red", ""], values[0].DenseValues().Select(static k => k.ToString()));
        Assert.Equal(0, keys[1].Length); // a null map: empty vectors
        Assert.Equal(0, values[2].Length);

        // The columns around keep reading their own frame columns.
        var id = dv.Schema["Id"];
        using var cursor = dv.GetRowCursor(dv.Schema);
        var getter = cursor.GetGetter<int>(id);
        List<int> ids = [];
        int value = 0;
        while (cursor.MoveNext())
        {
            getter(ref value);
            ids.Add(value);
        }
        Assert.Equal([1, 2, 3], ids);
    }

    [Fact]
    [Trait("ML", "DataView")]
    public void Columns_After_A_Split_Map_Read_Their_Own_Data()
    {
        // The map becomes two DataView columns: the categorical after it still finds its categories and its rows.
        using var df = DataFrame.FromRows([new Tags(1, new() { ["k"] = "v" }), new Tags(2, null)])
            .WithColumns(Polars.CSharp.Polars.Lit(Series.From("C", ["x", "y"])).Cast(DataType.Categorical()).Alias("C"));
        IDataView dv = df.AsDataView();

        Assert.Equal(["Id", "T.Keys", "T.Values", "C"], dv.Schema.Select(static c => c.Name));
        VBuffer<ReadOnlyMemory<char>> categories = default;
        dv.Schema["C"].GetKeyValues(ref categories);
        Assert.Equal(["x", "y"], categories.DenseValues().Select(static c => c.ToString()));
        var col = dv.Schema["C"];
        using var cursor = dv.GetRowCursor([col, dv.Schema["T.Values"]]);
        var getter = cursor.GetGetter<uint>(col);
        List<uint> keys = [];
        uint key = 0;
        while (cursor.MoveNext())
        {
            getter(ref key);
            keys.Add(key);
        }
        Assert.Equal([1u, 2u], keys);
    }

    [Fact]
    [Trait("ML", "DataView")]
    public void Maps_Of_Other_Values_Are_Refused()
    {
        using var df = DataFrame.FromRows([new Dated(new() { ["a"] = new DateOnly(2024, 1, 2) })]);
        var error = Assert.Throws<NotSupportedException>(() => df.AsDataView());
        Assert.Contains("'D'", error.Message);
        Assert.Contains("MapOps.Get(key)", error.Message);
    }

    [Fact]
    [Trait("ML", "DataView")]
    public void A_Trainer_Takes_The_Sparse_Map_Vector()
    {
        Scores[] rows = [.. Enumerable.Range(0, 200).Select(static i => new Scores(i, new() { ["a"] = i % 7, ["b"] = i % 3 == 0 ? null : i % 5 }))];
        using var df = DataFrame.FromRows(rows).WithColumns(Polars.CSharp.Polars.Col("Id").Cast(DataType.Float32).Alias("Label"));
        var ml = new MLContext(seed: 1);
        var pipeline = ml.Transforms.Conversion.ConvertType("Features", "Marks", DataKind.Single)
            .Append(ml.Regression.Trainers.Sdca(labelColumnName: "Label", featureColumnName: "Features", maximumNumberOfIterations: 3));
        var model = pipeline.Fit(df.AsDataView());
        Assert.NotNull(model);
    }

    private static Apache.Arrow.RecordBatch Duplicated()
    {
        var type = new Apache.Arrow.Types.MapType(new Apache.Arrow.Field("key", Apache.Arrow.Types.StringType.Default, false),
            new Apache.Arrow.Field("value", Apache.Arrow.Types.Int64Type.Default, true));
        var m = new Apache.Arrow.MapArray.Builder(type);
        m.Append();
        ((Apache.Arrow.StringArray.Builder)m.KeyBuilder).Append("x").Append("y").Append("x");
        ((Apache.Arrow.Int64Array.Builder)m.ValueBuilder).Append(1).Append(2).Append(5);
        return new Apache.Arrow.RecordBatch(new Apache.Arrow.Schema([new Apache.Arrow.Field("m", type, true)], []), [m.Build()], 1);
    }

    public sealed record Scores(int Id, Dictionary<string, double?>? Marks);
    public sealed record Counts(Dictionary<string, long> N);
    public sealed record Tags(int Id, Dictionary<string, string?>? T);
    public sealed record Dated(Dictionary<string, DateOnly> D);
}
