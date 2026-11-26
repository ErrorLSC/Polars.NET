#nullable enable
using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Engine;
using Polars.NET.Core.Exceptions;
using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

/// <summary>
/// Declaring a table's SQL expressions through the public API, as Spark declares them: CHECK constraints added and
/// dropped (ADD / DROP CONSTRAINT, the table's rows checked first), column defaults set and dropped (ALTER COLUMN
/// SET / DROP DEFAULT), generated columns and defaults declared at CREATE TABLE. Each declaration is checked as Spark
/// checks it, so that a table is never left with an expression its writes would refuse.
/// </summary>
public class DeltaDeclarationTests
{
    private sealed record Row(int Id, int? A, string? S);
    private sealed record IdOnly(int Id);
    private sealed record Typed(int Id, sbyte? T, int? I, string? S, decimal? Dec, DateOnly? D, bool? Bo);
    private sealed record Generated(int Id, int? A, int? G);
    private sealed record WithIdentity(long Key, int A, long G);
    private sealed record IdA(int Id, int A);
    private sealed record AOnly(int A);

    private static void InTable(string name, Action<string> body)
    {
        string path = Path.Combine(Path.GetTempPath(), $"delta_declare_{name}_{Guid.NewGuid():N}");
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

    private static List<T> Rows<T>(string path)
    {
        using DataFrame read = DataFrame.ReadDelta(path).Sort("Id");
        return [.. read.AsEnumerable<T>()];
    }

    private static Snapshot Latest(string path) => Snapshot.Load(new LocalFileSystem(path));

    /// <summary>The commitInfo of a version, as written in the log.</summary>
    private static JsonElement CommitInfo(string path, long version)
    {
        foreach (string line in File.ReadLines(Path.Combine(path, "_delta_log", $"{version:D20}.json")))
        {
            using JsonDocument doc = JsonDocument.Parse(line);
            if (doc.RootElement.TryGetProperty("commitInfo", out JsonElement info)) return info.Clone();
        }
        throw new InvalidOperationException($"Version {version} has no commitInfo.");
    }

    private static string Parameter(JsonElement info, string name) => info.GetProperty("operationParameters").GetProperty(name).GetString()!;

    private static string Refusal(Action action) => Assert.Throws<PolarsException>(action).Message;

    // ---- ADD / DROP CONSTRAINT ----

    [Fact]
    public void An_added_constraint_is_kept_as_written_under_its_lower_cased_name_and_checks_later_writes() => InTable("add", path =>
    {
        Write(path, [new Row(1, 5, "a")]);
        Delta.AddConstraint(path, "Positive A", "  a > -5 AND s <> 'it''s'  ");

        Snapshot table = Latest(path);
        Assert.Equal(1, table.Version);
        Assert.Equal("a > -5 AND s <> 'it''s'", table.Metadata.Configuration["delta.constraints.positive a"]);
        // A legacy protocol goes to writer 3, as Spark takes it.
        Assert.Equal((1, 3), (table.Protocol.MinReaderVersion, table.Protocol.MinWriterVersion));
        JsonElement info = CommitInfo(path, 1);
        Assert.Equal("ADD CONSTRAINT", info.GetProperty("operation").GetString());
        Assert.Equal(("Positive A", "a > -5 AND s <> 'it''s'"), (Parameter(info, "name"), Parameter(info, "expr")));

        Write(path, [new Row(2, 0, "b")]);
        var violation = Assert.IsType<DeltaConstraintViolationException>(
            Assert.Throws<PolarsException>(() => Write(path, [new Row(3, 1, "it's")])).InnerException);
        Assert.Equal("positive a", violation.Constraint);
        Assert.Equal(2, Latest(path).Version);
    });

    [Fact]
    public void A_table_on_table_features_gets_the_checkConstraints_feature() => InTable("features", path =>
    {
        Delta.CreateTable(path, PolarsSchema.From<Row>(), properties: new() { [DeltaTableProperties.EnableRowTracking] = "true" });
        Delta.AddConstraint(path, "c", "a IS NOT NULL");
        ProtocolAction protocol = Latest(path).Protocol;
        Assert.Equal(7, protocol.MinWriterVersion);
        Assert.Contains(TableFeatures.CheckConstraints, protocol.WriterFeatures!);
    });

    [Fact]
    public void Rows_that_violate_a_new_constraint_refuse_it_a_null_result_too() => InTable("violated", path =>
    {
        Write(path, [new Row(1, 5, "a"), new Row(2, -1, "b"), new Row(3, null, "c")]);
        Write(path, [new Row(4, 7, "d")]);
        string message = Refusal(() => Delta.AddConstraint(path, "c1", "a > 0"));
        Assert.Equal($"2 rows in {path} violate the new CHECK constraint (a > 0).", message);
        Assert.Equal(1, Latest(path).Version);
        Assert.DoesNotContain(Latest(path).Metadata.Configuration.Keys, k => k.StartsWith("delta.constraints.", StringComparison.Ordinal));
    });

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Deleted_rows_do_not_refuse_a_new_constraint(bool deletionVectors) => InTable("deleted", path =>
    {
        Delta.CreateTable(path, PolarsSchema.From<Row>(),
            properties: deletionVectors ? new() { ["delta.enableDeletionVectors"] = "true" } : null);
        Write(path, [new Row(1, 5, "a"), new Row(2, -1, "b"), new Row(3, 6, "c")]);
        Delta.Delete(path, Col("A") < 0);
        // Under deletion vectors the deleted row stays in its file, marked in the vector.
        Assert.Equal(deletionVectors, Latest(path).Files.Any(f => f.DeletionVector is not null));
        Delta.AddConstraint(path, "c1", "a > 0");
        Assert.Equal("a > 0", Latest(path).Metadata.Configuration["delta.constraints.c1"]);
    });

    [Fact]
    public void An_empty_table_takes_any_constraint() => InTable("empty", path =>
    {
        Delta.CreateTable(path, PolarsSchema.From<Row>());
        Delta.AddConstraint(path, "c1", "a > 100");
        Assert.Equal(1, Latest(path).Version);
    });

    [Theory]
    [InlineData("C1", "a > 1", "Constraint 'c1' already exists. Please delete the old constraint first.")]
    [InlineData("  ", "a > 1", "A CHECK constraint needs a name.")]
    [InlineData("c2", "a + 1", "The CHECK constraint 'c2' (a + 1) cannot be added: A CHECK constraint must be a BOOLEAN, not INT")]
    [InlineData("c2", "zz > 0", "The CHECK constraint 'c2' (zz > 0) cannot be added:")]
    [InlineData("c2", "rand() < 2", "The CHECK constraint 'c2' (rand() < 2) cannot be added: rand() is not allowed in a CHECK constraint")]
    [InlineData("c2", "a IN (SELECT 1)", "The CHECK constraint 'c2' (a IN (SELECT 1)) cannot be added:")]
    public void A_constraint_the_table_cannot_take_is_refused_before_any_commit(string name, string expression, string refusal) => InTable("refused", path =>
    {
        Write(path, [new Row(1, 5, "a")]);
        Delta.AddConstraint(path, "c1", "a > 0");
        Assert.StartsWith(refusal, Refusal(() => Delta.AddConstraint(path, name, expression)));
        Assert.Equal(1, Latest(path).Version);
    });

    [Fact]
    public void A_dropped_constraint_no_longer_checks_writes() => InTable("drop", path =>
    {
        Write(path, [new Row(1, 5, "a")]);
        Delta.AddConstraint(path, "Pos", "a > 0");
        Delta.DropConstraint(path, "POS");
        Snapshot table = Latest(path);
        Assert.Equal(2, table.Version);
        Assert.DoesNotContain("delta.constraints.pos", table.Metadata.Configuration.Keys);
        Assert.Equal(3, table.Protocol.MinWriterVersion); // the feature stays, as in Spark
        JsonElement info = CommitInfo(path, 2);
        Assert.Equal("DROP CONSTRAINT", info.GetProperty("operation").GetString());
        Assert.Equal(("POS", "a > 0", "true"), (Parameter(info, "name"), Parameter(info, "expr"), Parameter(info, "existed")));
        Write(path, [new Row(2, -1, "b")]);
    });

    [Fact]
    public void Dropping_a_missing_constraint_fails_unless_ifExists_which_commits_nothing() => InTable("dropmissing", path =>
    {
        Write(path, [new Row(1, 5, "a")]);
        Assert.Equal(
            "Cannot drop nonexistent constraint nope from the table. To skip a constraint the table does not have, pass ifExists.",
            Refusal(() => Delta.DropConstraint(path, "nope")));
        Delta.DropConstraint(path, "nope", ifExists: true);
        Assert.Equal(0, Latest(path).Version);
    });

    [Fact]
    public void A_constraint_is_added_with_AddConstraint_not_as_a_property() => InTable("property", path =>
    {
        const string Refused = "The table property delta.constraints.c1 cannot be set directly: add CHECK constraints with Delta.AddConstraint, which checks them.";
        Assert.Equal(Refused, Refusal(() => Delta.CreateTable(path, PolarsSchema.From<Row>(), properties: new() { ["delta.constraints.c1"] = "a > 0" })));
        Assert.False(Directory.Exists(Path.Combine(path, "_delta_log")) && Directory.EnumerateFiles(Path.Combine(path, "_delta_log")).Any());
        Write(path, [new Row(1, 5, "a")]);
        Assert.Equal(Refused, Refusal(() => Delta.SetTableProperties(path, new() { ["delta.constraints.c1"] = "a > 0" })));
    });

    // ---- SET / DROP DEFAULT ----

    private static readonly IReadOnlyList<(string, DataType)> TypedColumns =
    [
        ("Id", DataType.Int32), ("T", DataType.Int8), ("I", DataType.Int32), ("S", DataType.String),
        ("Dec", DataType.Decimal(5, 2)), ("D", DataType.Date), ("Bo", DataType.Boolean),
    ];

    [Fact]
    public void A_set_default_fills_the_column_on_later_writes_converted_as_Spark_stores_it() => InTable("setdefault", path =>
    {
        Delta.CreateTable(path, PolarsSchema.From(TypedColumns));
        Delta.SetColumnDefault(path, "t", " 100 ");
        Delta.SetColumnDefault(path, "I", "'1'");
        Delta.SetColumnDefault(path, "S", "1");
        Delta.SetColumnDefault(path, "Dec", "1.555");
        Delta.SetColumnDefault(path, "D", "TIMESTAMP '2020-01-01 10:00:00'");
        Delta.SetColumnDefault(path, "Bo", "NULL");

        Snapshot table = Latest(path);
        Assert.Equal("100", table.Schema.Field("T")!.Metadata[Constraints.CurrentDefaultKey]);
        Assert.Contains(TableFeatures.AllowColumnDefaults, table.Protocol.WriterFeatures!);
        JsonElement info = CommitInfo(path, 1);
        Assert.Equal("CHANGE COLUMN", info.GetProperty("operation").GetString());
        Assert.Contains("\"CURRENT_DEFAULT\":\"100\"", Parameter(info, "column"), StringComparison.Ordinal);

        Write(path, [new IdOnly(1)]);
        Assert.Equal(new Typed(1, 100, 1, "1", 1.56m, new DateOnly(2020, 1, 1), null), Rows<Typed>(path).Single());
    });

    [Theory]
    [InlineData("Bo", "1", "a value of type INT cannot be stored in a column of type BOOLEAN.")]
    [InlineData("D", "1", "a value of type INT cannot be stored in a column of type DATE.")]
    [InlineData("T", "1000", "its value cannot be stored in the column:")]
    [InlineData("Dec", "1234.5", "its value cannot be stored in the column:")]
    [InlineData("D", "'nope'", "The string 'nope' is not a valid DATE")]
    [InlineData("I", "1 / 0", "its value cannot be stored in the column:")]
    [InlineData("I", "2147483647 + 1", "its value cannot be stored in the column:")]
    [InlineData("I", "Id", "A column default cannot read a column (Id)")]
    [InlineData("I", "rand()", "rand() is not allowed in a column default")]
    public void A_default_Spark_would_refuse_is_refused(string column, string expression, string why) => InTable("baddefault", path =>
    {
        Delta.CreateTable(path, PolarsSchema.From(TypedColumns));
        string message = Refusal(() => Delta.SetColumnDefault(path, column, expression));
        Assert.StartsWith($"The default {expression} of the column {column} is not valid: {why}", message);
        Assert.Equal(0, Latest(path).Version);
    });

    [Fact]
    public void Generated_identity_nested_and_unknown_columns_take_no_default() => InTable("nodefault", path =>
    {
        Delta.CreateTable(path, PolarsSchema.From<WithIdentity>(),
            identityColumns: [new DeltaIdentityColumn("Key")], generatedColumns: [new DeltaGeneratedColumn("G", "Key * 10")]);
        Assert.Equal("The column G is a generated column, which cannot also have a default value.", Refusal(() => Delta.SetColumnDefault(path, "G", "1")));
        Assert.Equal("The column Key is an identity column, which cannot also have a default value.", Refusal(() => Delta.SetColumnDefault(path, "Key", "1")));
        Assert.Equal("The table has no column 'zz'.", Refusal(() => Delta.SetColumnDefault(path, "zz", "1")));
        Assert.Equal("The table has no top-level column 'a.b': only top-level columns have defaults.", Refusal(() => Delta.SetColumnDefault(path, "a.b", "1")));
        Assert.Equal(0, Latest(path).Version);
    });

    [Fact]
    public void A_dropped_default_no_longer_fills_and_dropping_none_commits_nothing() => InTable("dropdefault", path =>
    {
        Delta.CreateTable(path, PolarsSchema.From<Row>(), columnDefaults: [new DeltaColumnDefault("A", "7")]);
        Delta.DropColumnDefault(path, "S");
        Assert.Equal(0, Latest(path).Version);
        Delta.DropColumnDefault(path, "a");
        Assert.Equal(1, Latest(path).Version);
        Assert.False(Latest(path).Schema.Field("A")!.Metadata.ContainsKey(Constraints.CurrentDefaultKey));
        Write(path, [new IdOnly(1)]);
        Assert.Null(Rows<Row>(path).Single().A);
    });

    // ---- CREATE TABLE with generated columns and defaults ----

    [Fact]
    public void A_table_created_with_generated_columns_and_defaults_computes_and_fills_them() => InTable("create", path =>
    {
        Delta.CreateTable(path, PolarsSchema.From<Generated>(), partitionBy: ["G"],
            generatedColumns: [new DeltaGeneratedColumn("G", " A + 1 ")], columnDefaults: [new DeltaColumnDefault("A", "41")]);
        Snapshot table = Latest(path);
        Assert.Equal("A + 1", table.Schema.Field("G")!.Metadata[Constraints.GenerationExpressionKey]);
        Assert.Equal("41", table.Schema.Field("A")!.Metadata[Constraints.CurrentDefaultKey]);
        Assert.Equal(7, table.Protocol.MinWriterVersion);
        Assert.Contains(TableFeatures.GeneratedColumns, table.Protocol.WriterFeatures!);
        Assert.Contains(TableFeatures.AllowColumnDefaults, table.Protocol.WriterFeatures!);

        Write(path, [new IdOnly(1)]);
        Write(path, [new IdA(2, 5)]);
        Assert.Equal([new Generated(1, 41, 42), new Generated(2, 5, 6)], Rows<Generated>(path));
        Assert.IsType<DeltaConstraintViolationException>(
            Assert.Throws<PolarsException>(() => Write(path, [new Generated(3, 1, 9)])).InnerException);
    });

    [Fact]
    public void A_generated_column_may_read_an_identity_column() => InTable("identity", path =>
    {
        Delta.CreateTable(path, PolarsSchema.From<WithIdentity>(),
            identityColumns: [new DeltaIdentityColumn("Key")], generatedColumns: [new DeltaGeneratedColumn("G", "Key * 10")]);
        Write(path, [new AOnly(1), new AOnly(2)]);
        using DataFrame read = DataFrame.ReadDelta(path).Sort("Key");
        Assert.Equal([new WithIdentity(1, 1, 10), new WithIdentity(2, 2, 20)], [.. read.AsEnumerable<WithIdentity>()]);
    });

    public static TheoryData<DeltaGeneratedColumn[], DeltaColumnDefault[], string> BadDeclarations => new()
    {
        { [new("G", "CAST(A AS BIGINT)")], [], "The generated column G (CAST(A AS BIGINT)) is not valid: its type BIGINT is not the column's, INT." },
        { [new("G", "G + 1")], [], "The generated column G (G + 1) is not valid: it reads the column itself." },
        { [new("G", "A + 1"), new("A", "Id + 1")], [], "The generated column G (A + 1) is not valid: it reads the generated column A." },
        { [new("G", "rand()")], [], "The generated column G (rand()) is not valid:" },
        { [new("G", "zz + 1")], [], "The generated column G (zz + 1) is not valid:" },
        { [new("G", "A + 1"), new("G", "A + 2")], [], "The generated column 'G' is declared twice." },
        { [new("G", "A + 1")], [new("G", "1")], "The column 'G' cannot have both a default value and a generation expression." },
        { [new("st.x", "1")], [], "The generated column 'st.x' is not a top-level column of the schema: only top-level columns can be one." },
        { [new("Nope", "1")], [], "The generated column 'Nope' is not in the schema." },
        { [new("G", " ")], [], "The generated column 'G' has no expression." },
        { [], [new("A", "'x'")], "The default 'x' of the column A is not valid:" },
        { [], [new("A", "A + 1")], "The default A + 1 of the column A is not valid: A column default cannot read a column (A)" },
    };

    [Theory]
    [MemberData(nameof(BadDeclarations))]
    public void A_declaration_Spark_would_refuse_creates_no_table(DeltaGeneratedColumn[] generated, DeltaColumnDefault[] defaults, string refusal) => InTable("badcreate", path =>
    {
        string message = Refusal(() => Delta.CreateTable(path, PolarsSchema.From<Generated>(), generatedColumns: generated, columnDefaults: defaults));
        Assert.StartsWith(refusal, message);
        Assert.False(Directory.Exists(Path.Combine(path, "_delta_log")) && Directory.EnumerateFiles(Path.Combine(path, "_delta_log")).Any());
    });

    [Fact]
    public void An_identity_column_is_neither_generated_nor_defaulted() => InTable("identitybad", path =>
    {
        Assert.Equal("The generated column 'Key' is an identity column, which cannot also be one.", Refusal(() =>
            Delta.CreateTable(path, PolarsSchema.From<WithIdentity>(), identityColumns: [new DeltaIdentityColumn("Key")], generatedColumns: [new DeltaGeneratedColumn("Key", "A")])));
        Assert.Equal("The column with a default 'Key' is an identity column, which cannot also be one.", Refusal(() =>
            Delta.CreateTable(path, PolarsSchema.From<WithIdentity>(), identityColumns: [new DeltaIdentityColumn("Key")], columnDefaults: [new DeltaColumnDefault("Key", "1")])));
    });
}
