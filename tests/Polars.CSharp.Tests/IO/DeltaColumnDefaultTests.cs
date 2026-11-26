#nullable enable
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Lakehouse.Engine;
using Polars.NET.Core.Delta;
using Polars.NET.Core.Exceptions;
using Polars.NET.Core.Wrappers;

namespace Polars.CSharp.Tests;

/// <summary>
/// Column defaults on Delta tables (feature allowColumnDefaults), as Spark writes them: a column the rows lack takes
/// its default, converted to the column's type (an append, a merge's inserted rows); a value given, null included, is
/// kept; current_timestamp() is one instant for all the rows of a write.
/// </summary>
public class DeltaColumnDefaultTests
{
    private sealed record Full(int Id, long? B, string? S, DateTimeOffset? Ts, DateOnly? D, long? G);
    private sealed record Source(int Id);
    private sealed record Partial(int Id, long? B);

    private static void InTable(string name, Action<string> body)
    {
        string path = Path.Combine(Path.GetTempPath(), $"delta_default_{name}_{Guid.NewGuid():N}");
        try
        {
            body(path);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, true);
        }
    }

    private static void Write<T>(string path, IEnumerable<T> rows)
    {
        using DataFrame df = DataFrame.From(rows);
        df.WriteDelta(path, mode: DeltaSaveMode.Append, mkdir: true);
    }

    /// <summary>Sets field metadata, as Spark's DEFAULT and GENERATED ALWAYS AS declare it (writer version 7, with the
    /// features the older versions imply).</summary>
    private static void Declare(string path, string feature, string key, params (string Column, string Text)[] fields)
    {
        var fs = new LocalFileSystem(path);
        Snapshot snapshot = Snapshot.Load(fs);
        StructType schema = StructType.Parse(snapshot.Metadata.SchemaString);
        StructField[] declared =
        [
            .. schema.Fields.Select(f => fields.FirstOrDefault(d => d.Column == f.Name) is not { Column: not null } d ? f : new StructField
            {
                Name = f.Name, Type = f.Type, Nullable = f.Nullable,
                Metadata = new Dictionary<string, string>(f.Metadata) { [key] = d.Text },
                JsonMetadataKeys = f.JsonMetadataKeys,
            }),
        ];
        ProtocolAction p = snapshot.Protocol;
        IEnumerable<string> features = p.MinWriterVersion >= 7 ? p.WriterFeatures ?? [] : TableFeatures.WriterFeaturesOf(p);
        MetadataAction m = snapshot.Metadata;
        Transaction.Start(fs, snapshot, "CHANGE COLUMN")
            .SetProtocol(new ProtocolAction
            {
                MinReaderVersion = p.MinReaderVersion, MinWriterVersion = 7, ReaderFeatures = p.ReaderFeatures,
                WriterFeatures = [.. features.Append(feature).Distinct()],
            })
            .SetMetadata(new MetadataAction
            {
                Id = m.Id, Name = m.Name, Description = m.Description, FormatProvider = m.FormatProvider, FormatOptions = m.FormatOptions,
                SchemaString = new StructType(declared).ToJson(), PartitionColumns = m.PartitionColumns, Configuration = m.Configuration,
                CreatedTime = m.CreatedTime,
            })
            .Commit();
    }

    private static void Defaults(string path, params (string Column, string Text)[] defaults) =>
        Declare(path, TableFeatures.AllowColumnDefaults, Constraints.CurrentDefaultKey, defaults);

    private static List<Full> Rows(string path)
    {
        using DataFrame read = DataFrame.ReadDelta(path).Sort("Id");
        return [.. read.AsEnumerable<Full>()];
    }

    private static readonly Full First = new(0, 1, "a", DateTimeOffset.UnixEpoch, new DateOnly(2000, 1, 1), 2);

    /// <summary>A table whose B defaults to 42, S to 'xy', Ts to the write's time and D to 2020-02-02.</summary>
    private static void DefaultsTable(string path)
    {
        Write(path, [First]);
        Defaults(path, ("B", "42"), ("S", "'x' || \"y\""), ("Ts", "current_timestamp()"), ("D", "DATE '2020-02-02'"));
    }

    [Fact]
    public void Columns_the_rows_lack_take_their_defaults_one_instant_for_the_whole_write() => InTable("filled", path =>
    {
        DefaultsTable(path);
        DateTimeOffset before = DateTimeOffset.UtcNow;
        Write(path, [new Source(1), new Source(2), new Source(3)]);
        DateTimeOffset after = DateTimeOffset.UtcNow;
        List<Full> rows = Rows(path);
        Assert.All(rows.Skip(1), r => Assert.Equal((42L, "xy", new DateOnly(2020, 2, 2)), (r.B!.Value, r.S, r.D!.Value)));
        DateTimeOffset written = rows[1].Ts!.Value;
        Assert.All(rows.Skip(1), r => Assert.Equal(written, r.Ts));
        Assert.InRange(written, before.AddMilliseconds(-1), after);
    });

    [Fact]
    public void A_value_given_null_included_is_kept() => InTable("given", path =>
    {
        DefaultsTable(path);
        Write(path, [new Partial(1, null), new Partial(2, 7)]);
        Assert.Equal([null, 7L], Rows(path).Skip(1).Select(r => r.B));
        Assert.All(Rows(path).Skip(1), r => Assert.Equal("xy", r.S));
    });

    [Fact]
    public void A_merge_gives_its_inserted_rows_the_defaults_and_keeps_an_updated_rows_values() => InTable("merge", path =>
    {
        DefaultsTable(path);
        using DataFrame source = DataFrame.From([new Partial(0, 5), new Partial(9, 6)]);
        DeltaKernelMerge.Merge(path, PolarsWrapper.LazyClone(source.Lazy().Handle), ["Id"], []);
        List<Full> rows = Rows(path);
        Assert.Equal((5L, "a", new DateOnly(2000, 1, 1)), (rows[0].B!.Value, rows[0].S, rows[0].D!.Value));
        Assert.Equal((6L, "xy", new DateOnly(2020, 2, 2)), (rows[1].B!.Value, rows[1].S, rows[1].D!.Value));
    });

    [Fact]
    public void A_generated_column_is_computed_from_a_columns_default() => InTable("generated", path =>
    {
        DefaultsTable(path);
        Declare(path, TableFeatures.GeneratedColumns, Constraints.GenerationExpressionKey, ("G", "B * 2"));
        Write(path, [new Source(1)]);
        Assert.Equal(84L, Rows(path)[1].G);
    });

    [Theory]
    [InlineData("rand()", "rand() is not allowed in a column default: it is not deterministic")]
    [InlineData("Id + 1", "A column default cannot read a column (Id)")]
    [InlineData("'x'", "The string 'x' is not a valid BIGINT")]
    public void A_default_the_kernel_cannot_compute_refuses_the_write(string text, string why) => InTable("refused", path =>
    {
        Write(path, [First]);
        Defaults(path, ("B", text));
        var e = Assert.IsType<DeltaProtocolException>(Assert.Throws<PolarsException>(() => Write(path, [new Source(1)])).InnerException);
        Assert.StartsWith($"The default of the column B ({text}) cannot be computed by this kernel: {why}", e.Message);
        Assert.Single(Rows(path));
    });
}
