#nullable enable
using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

/// <summary>
/// Map columns through the file formats and the other ways out: round trips where the format holds maps (Parquet,
/// IPC, Avro, JSON with string keys), friendly refusals where it does not (CSV, Excel, JSON with other keys, tensors).
/// </summary>
public sealed class MapIoTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("map-io").FullName;
    private readonly DataFrame _df = DataFrame.FromRows([new Row(1, new() { ["a"] = 1, ["b"] = 2 }, new() { [1] = "x" }), new Row(2, null, [])]);

    public void Dispose()
    {
        _df.Dispose();
        Directory.Delete(_dir, recursive: true);
    }

    private string At(string name) => Path.Combine(_dir, name);

    [Fact]
    public void Formats_With_Maps_Round_Trip()
    {
        _df.WriteParquet(At("m.parquet"));
        _df.WriteIpc(At("m.arrow"));
        _df.WriteAvro(At("m.avro"));
        using var parquet = DataFrame.ReadParquet(At("m.parquet"));
        using var ipc = DataFrame.ReadIpc(At("m.arrow"));
        using var avro = DataFrame.ReadAvro(At("m.avro"));

        foreach (var back in (DataFrame[])[parquet, ipc, avro])
        {
            Assert.Equal(DataType.Map(DataType.String, DataType.Int64), back.Schema["M"]);
            Assert.Equal(DataType.Map(DataType.Int32, DataType.String), back.Schema["N"]);
            Assert.True(back.Equals(_df), back.ToString());
        }
    }

    [Fact]
    public void Json_Writes_String_Keyed_Maps_As_Objects_And_Reads_Them_With_A_Schema()
    {
        using var strings = _df.Select(Col("Id"), Col("M"));
        strings.WriteJson(At("m.json"));
        Assert.Equal("""[{"Id":1,"M":{"a":1,"b":2}},{"Id":2,"M":null}]""", File.ReadAllText(At("m.json")).Trim());

        using var back = DataFrame.ReadJson(At("m.json"), schema: strings.Schema);
        Assert.True(back.Equals(strings));
        // Without a schema an object is a struct (as Polars infers it).
        using var inferred = DataFrame.ReadJson(At("m.json"));
        Assert.Equal(DataTypeKind.Struct, inferred.Schema["M"].Kind);
    }

    [Fact]
    public void Json_Refuses_Other_Keys_And_Says_How_To_Write_Them()
    {
        var error = Assert.Throws<NotSupportedException>(() => _df.WriteJson(At("n.json")));
        Assert.Contains("'N'", error.Message);
        Assert.Contains("MapOps.Entries()", error.Message);

        // Both ways the message suggests.
        using (var entries = _df.Select(Col("N").MapOps.Entries()))
            entries.WriteJson(At("entries.json"));
        Assert.Equal("""[{"N":[{"key":1,"value":"x"}]},{"N":[]}]""", File.ReadAllText(At("entries.json")).Trim());
        using (var text = _df.Select(Col("N").MapOps.Entries()
                   .Cast(DataType.List(DataType.Struct(("key", DataType.String), ("value", DataType.String)))).List.ToMap()))
            text.WriteJson(At("text.json"));
        Assert.Equal("""[{"N":{"1":"x"}},{"N":{}}]""", File.ReadAllText(At("text.json")).Trim());
    }

    [Fact]
    public void Csv_Excel_And_Tensors_Refuse_Maps_Friendly()
    {
        Assert.Contains("'M'", Assert.Throws<NotSupportedException>(() => _df.WriteCsv(At("m.csv"))).Message);
        Assert.Contains("'M'", Assert.Throws<NotSupportedException>(() => _df.WriteExcel(At("m.xlsx"))).Message);
        var tensor = Assert.Throws<NotSupportedException>(() => _df["M"].AsTensor<long>());
        Assert.Contains("MapOps.Values()", tensor.Message);
    }

    [Fact]
    public void A_Data_Reader_Gives_Maps_As_Dictionaries()
    {
        using var reader = _df.AsDataReader();
        Assert.Equal(typeof(Dictionary<string, long>), reader.GetFieldType(1));
        Assert.True(reader.Read());
        Assert.Equal(new Dictionary<string, long> { ["a"] = 1, ["b"] = 2 }, reader.GetValue(1));
    }

    public sealed record Row(int Id, Dictionary<string, long>? M, Dictionary<int, string>? N);
}
