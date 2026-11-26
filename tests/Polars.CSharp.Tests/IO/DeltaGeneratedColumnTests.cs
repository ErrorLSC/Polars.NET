#nullable enable
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Lakehouse.Engine;
using Polars.NET.Core.Exceptions;
using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

/// <summary>
/// Generated columns on Delta tables, as Spark writes them: a column the rows lack is computed, a value given for it
/// is checked (null-safe: a null given where the expression is not null violates); an update computes the ones it
/// does not set again on the rows it changes; an optimize's rewrite checks them, a delete's does not.
/// </summary>
public class DeltaGeneratedColumnTests
{
    private sealed record Full(int Id, string? S, string? U, int? N);
    private sealed record Source(int Id, string? S);
    private sealed record Flagged(int Id, string? S, bool? K);
    private sealed record Timed(int Id, DateTimeOffset Ts, DateOnly D);
    private sealed record TimedSource(int Id, DateTimeOffset Ts);

    private static void InTable(string name, Action<string> body)
    {
        string path = Path.Combine(Path.GetTempPath(), $"delta_generated_{name}_{Guid.NewGuid():N}");
        try
        {
            body(path);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, true);
        }
    }

    private static void Write<T>(string path, IEnumerable<T> rows, string? partitionBy = null)
    {
        using DataFrame df = DataFrame.From(rows);
        if (partitionBy is null) df.WriteDelta(path, mode: DeltaSaveMode.Append, mkdir: true);
        else df.WriteDelta(path, partitionBy: partitionBy, mode: DeltaSaveMode.Append, mkdir: true);
    }

    private static Snapshot Latest(string path) => Snapshot.Load(new LocalFileSystem(path));

    /// <summary>Makes columns generated, as Spark's GENERATED ALWAYS AS declares them (writer version 4 at least).</summary>
    private static void Generate(string path, params (string Column, string Expression)[] generated)
    {
        var fs = new LocalFileSystem(path);
        Snapshot snapshot = Snapshot.Load(fs);
        StructType schema = StructType.Parse(snapshot.Metadata.SchemaString);
        StructField[] fields =
        [
            .. schema.Fields.Select(f => generated.FirstOrDefault(g => g.Column == f.Name) is not { Column: not null } g ? f : new StructField
            {
                Name = f.Name, Type = f.Type, Nullable = f.Nullable,
                Metadata = new Dictionary<string, string>(f.Metadata) { [Constraints.GenerationExpressionKey] = g.Expression },
                JsonMetadataKeys = f.JsonMetadataKeys,
            }),
        ];
        ProtocolAction p = snapshot.Protocol;
        MetadataAction m = snapshot.Metadata;
        Transaction.Start(fs, snapshot, "CHANGE COLUMN")
            .SetProtocol(new ProtocolAction
            {
                MinReaderVersion = p.MinReaderVersion,
                MinWriterVersion = p.MinWriterVersion >= 7 ? 7 : Math.Max(p.MinWriterVersion, 4),
                ReaderFeatures = p.ReaderFeatures,
                WriterFeatures = p.MinWriterVersion >= 7 ? [.. p.WriterFeatures ?? [], "generatedColumns"] : null,
            })
            .SetMetadata(new MetadataAction
            {
                Id = m.Id, Name = m.Name, Description = m.Description, FormatProvider = m.FormatProvider, FormatOptions = m.FormatOptions,
                SchemaString = new StructType(fields).ToJson(), PartitionColumns = m.PartitionColumns, Configuration = m.Configuration,
                CreatedTime = m.CreatedTime,
            })
            .Commit();
    }

    private static DeltaConstraintViolationException Violation(Action write) =>
        Assert.IsType<DeltaConstraintViolationException>(Assert.Throws<PolarsException>(write).InnerException);

    private static List<Full> Rows(string path)
    {
        using DataFrame read = DataFrame.ReadDelta(path).Sort("Id");
        return [.. read.AsEnumerable<Full>()];
    }

    /// <summary>A table whose U is upper(S) and N is length(S) * 2.</summary>
    private static void UpperTable(string path)
    {
        Write(path, [new Full(1, "ab", "AB", 4)]);
        Generate(path, ("U", "upper(S)"), ("N", "length(S) * 2"));
    }

    [Fact]
    public void Generated_columns_the_rows_lack_are_computed() => InTable("computed", path =>
    {
        UpperTable(path);
        Write(path, [new Source(2, "cd"), new Source(3, null)]);
        Assert.Equal([new Full(1, "ab", "AB", 4), new Full(2, "cd", "CD", 4), new Full(3, null, null, null)], Rows(path));
    });

    [Fact]
    public void A_value_given_for_a_generated_column_is_checked() => InTable("checked", path =>
    {
        UpperTable(path);
        Write(path, [new Full(2, "x", "X", 2)]);
        var e = Violation(() => Write(path, [new Full(3, "x", "Y", 2)]));
        Assert.Equal("[DELTA_VIOLATE_CONSTRAINT_WITH_VALUES] CHECK constraint Generated Column (U <=> upper(S)) violated by row with values:\n - S : x\n - U : Y", e.Message);
        // A null given where the expression is not null violates too.
        Assert.Equal([KeyValuePair.Create("S", "x"), KeyValuePair.Create("U", "null")], Violation(() => Write(path, [new Full(4, "x", null, 2)])).Values);
        Assert.Equal([1, 2], Rows(path).Select(r => r.Id));
    });

    [Fact]
    public void A_generated_partition_column_is_computed_before_the_rows_are_partitioned() => InTable("partition", path =>
    {
        var then = new DateTimeOffset(2022, 3, 4, 5, 6, 7, TimeSpan.Zero);
        Write(path, [new Timed(1, then, new DateOnly(2022, 3, 4))], partitionBy: "D");
        Generate(path, ("D", "CAST(Ts AS DATE)"));
        Write(path, [new TimedSource(2, then.AddDays(1))]);
        Assert.True(Directory.Exists(Path.Combine(path, "D=2022-03-05")));
        using DataFrame read = DataFrame.ReadDelta(path).Sort("Id");
        Assert.Equal([new DateOnly(2022, 3, 4), new DateOnly(2022, 3, 5)], read.Column("D").ToArray<DateOnly>());
    });

    [Fact]
    public void An_update_computes_the_generated_columns_it_does_not_set_and_checks_the_ones_it_does() => InTable("update", path =>
    {
        // One file: the updates rewrite it, the rows they do not change copied as they are.
        Write(path, [new Full(1, "ab", "AB", 4), new Full(2, "cd", "CD", 4)]);
        Generate(path, ("U", "upper(S)"), ("N", "length(S) * 2"));
        Delta.Update(path, Col("Id") == 1, new Dictionary<string, Expr> { ["S"] = Lit("qqq") });
        Assert.Equal([new Full(1, "qqq", "QQQ", 6), new Full(2, "cd", "CD", 4)], Rows(path));

        Violation(() => Delta.Update(path, Col("Id") == 2, new Dictionary<string, Expr> { ["U"] = Lit("nope") }));
        Delta.Update(path, Col("Id") == 2, new Dictionary<string, Expr> { ["U"] = Lit("CD"), ["Id"] = Lit(20) });
        Assert.Equal([new Full(1, "qqq", "QQQ", 6), new Full(20, "cd", "CD", 4)], Rows(path));
    });

    [Fact]
    public void An_update_computes_only_the_rows_it_changes() => InTable("copied", path =>
    {
        // Row 2 breaks the expression and shares row 1's file: the update copies it as it is, and the check fails (Spark's).
        Write(path, [new Full(1, "ab", "AB", 4), new Full(2, "cd", "wrong", 4)]);
        Generate(path, ("U", "upper(S)"));
        Assert.Equal([KeyValuePair.Create("S", "cd"), KeyValuePair.Create("U", "wrong")],
            Violation(() => Delta.Update(path, Col("Id") == 1, new Dictionary<string, Expr> { ["S"] = Lit("x") })).Values);
    });

    [Fact]
    public void A_generation_expression_of_any_precedence_is_checked_whole() => InTable("precedence", path =>
    {
        Write(path, [new Flagged(1, "ab", true)]);
        Generate(path, ("K", "S = 'ab' OR S = 'x'"));
        Write(path, [new Flagged(2, "x", true), new Flagged(3, "q", false)]);
        Assert.Equal("Generated Column", Violation(() => Write(path, [new Flagged(4, "q", true)])).Constraint);
    });

    [Fact]
    public void An_optimize_checks_the_rows_it_rewrites_a_delete_does_not() => InTable("rewrite", path =>
    {
        // Rows that break the expression (written before the column was generated).
        Write(path, [new Full(1, "ab", "wrong", 4), new Full(2, "cd", "CD", 4)]);
        Write(path, [new Full(3, "ef", "EF", 4)]);
        Generate(path, ("U", "upper(S)"));
        Delta.Delete(path, Col("Id") == 2);
        long version = Latest(path).Version;
        Assert.Equal("Generated Column", Violation(() => Delta.Optimize(path)).Constraint);
        Assert.Equal(version, Latest(path).Version);
    });

    [Theory]
    [InlineData("U", "upper(S)", "N", "length(U)", "it reads the generated column U.")]
    [InlineData("U", "length(S)", null, null, "its type INT is not the column's, STRING.")]
    [InlineData("U", "uuid()", null, null, "uuid() is not allowed in a generated column: it is not deterministic")]
    public void A_generation_expression_the_kernel_cannot_compute_refuses_the_write(string column, string expression, string? other, string? otherExpression, string why) =>
        InTable("refused", path =>
        {
            Write(path, [new Full(1, "ab", "AB", 4)]);
            Generate(path, other is null ? [(column, expression)] : [(column, expression), (other, otherExpression!)]);
            var e = Assert.IsType<DeltaProtocolException>(Assert.Throws<PolarsException>(() => Write(path, [new Source(2, "x")])).InnerException);
            Assert.Contains(why, e.Message);
            Assert.Single(Rows(path));
        });
}
