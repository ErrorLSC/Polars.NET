#nullable enable
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Lakehouse.Engine;
using Polars.NET.Core.Delta;
using Polars.NET.Core.Exceptions;
using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

/// <summary>
/// CHECK constraints and column invariants on Delta tables: every row a write adds (an append, an update, an
/// optimize's rewrite) must make each true, as Spark checks them (a null result violates too); the rows a delete keeps
/// are not checked. A violation fails the write with Spark's message and commits nothing.
/// </summary>
public class DeltaConstraintTests
{
    private sealed record Point(int Id, double F, string? S, DateOnly D, DateTimeOffset Ts, Inner St);
    private sealed record Inner(int X);
    private sealed record Plain(int Id, int? A);

    private static void InTable(string name, Action<string> body)
    {
        string path = Path.Combine(Path.GetTempPath(), $"delta_constraint_{name}_{Guid.NewGuid():N}");
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

    private static Snapshot Latest(string path) => Snapshot.Load(new LocalFileSystem(path));

    /// <summary>The violation a write fails with (the kernel's errors reach the caller as PolarsException).</summary>
    private static DeltaConstraintViolationException Violation(Action write) =>
        Assert.IsType<DeltaConstraintViolationException>(Assert.Throws<PolarsException>(write).InnerException);

    /// <summary>Commits a CHECK constraint as Spark's ALTER TABLE ADD CONSTRAINT does (writer version 3 at least).</summary>
    private static void AddConstraint(string path, string name, string expression)
    {
        var fs = new LocalFileSystem(path);
        Snapshot snapshot = Snapshot.Load(fs);
        var configuration = new Dictionary<string, string>(snapshot.Metadata.Configuration) { [Constraints.ConfigurationPrefix + name] = expression };
        Transaction.Start(fs, snapshot, "ADD CONSTRAINT")
            .SetProtocol(WithFeature(snapshot.Protocol, "checkConstraints", 3))
            .SetMetadata(With(snapshot.Metadata, configuration: configuration))
            .Commit();
    }

    /// <summary>Commits a column invariant on a top-level field, as Delta writes it (writer version 2 at least).</summary>
    private static void AddInvariant(string path, string column, string expression)
    {
        var fs = new LocalFileSystem(path);
        Snapshot snapshot = Snapshot.Load(fs);
        StructType schema = StructType.Parse(snapshot.Metadata.SchemaString);
        string json = $$$"""{"expression": {"expression": "{{{expression}}}"}}""";
        StructField[] fields =
        [
            .. schema.Fields.Select(f => f.Name != column ? f : new StructField
            {
                Name = f.Name, Type = f.Type, Nullable = f.Nullable,
                Metadata = new Dictionary<string, string>(f.Metadata) { [Constraints.InvariantsKey] = json },
                JsonMetadataKeys = f.JsonMetadataKeys,
            }),
        ];
        Transaction.Start(fs, snapshot, "CHANGE COLUMN")
            .SetProtocol(WithFeature(snapshot.Protocol, "invariants", 2))
            .SetMetadata(With(snapshot.Metadata, schema: new StructType(fields).ToJson()))
            .Commit();
    }

    private static ProtocolAction WithFeature(ProtocolAction protocol, string feature, int legacyVersion) => new()
    {
        MinReaderVersion = protocol.MinReaderVersion,
        MinWriterVersion = protocol.MinWriterVersion >= 7 ? 7 : Math.Max(protocol.MinWriterVersion, legacyVersion),
        ReaderFeatures = protocol.ReaderFeatures,
        WriterFeatures = protocol.MinWriterVersion >= 7 ? [.. protocol.WriterFeatures ?? [], feature] : null,
    };

    private static MetadataAction With(MetadataAction m, IReadOnlyDictionary<string, string>? configuration = null, string? schema = null) => new()
    {
        Id = m.Id, Name = m.Name, Description = m.Description, FormatProvider = m.FormatProvider, FormatOptions = m.FormatOptions,
        SchemaString = schema ?? m.SchemaString, PartitionColumns = m.PartitionColumns, Configuration = configuration ?? m.Configuration,
        CreatedTime = m.CreatedTime,
    };

    private static int[] Ids(string path)
    {
        using DataFrame read = DataFrame.ReadDelta(path).Sort("Id");
        return read.Column("Id").ToArray<int>();
    }

    private static readonly Point Good = new(1, 1.5, "ok", new DateOnly(2021, 1, 1), new DateTimeOffset(2021, 1, 1, 0, 0, 0, TimeSpan.Zero), new Inner(1));

    [Fact]
    public void An_append_of_rows_meeting_the_constraints_is_committed_one_violating_them_is_not() => InTable("append", path =>
    {
        Write(path, [Good]);
        AddConstraint(path, "positive", "Id > 0 AND st.x > 0");
        Write(path, [Good with { Id = 2 }]);
        long version = Latest(path).Version;

        var e = Violation(() => Write(path, [Good with { Id = 3 }, Good with { Id = -4 }]));
        Assert.Equal("""
            [DELTA_VIOLATE_CONSTRAINT_WITH_VALUES] CHECK constraint positive (Id > 0 AND st.x > 0) violated by row with values:
             - Id : -4
             - St.X : 1
            """, e.Message);
        Assert.Equal("positive", e.Constraint);
        Assert.Equal(version, Latest(path).Version);
        Assert.Equal([1, 2], Ids(path));
    });

    [Fact]
    public void The_violating_row_is_found_anywhere_in_a_batch() => InTable("batch", path =>
    {
        Write(path, [new Plain(0, 0)]);
        AddConstraint(path, "small", "a < 100");
        var e = Violation(() => Write(path, [.. Enumerable.Range(1, 200).Select(i => new Plain(i, i == 150 ? 1000 : i % 100))]));
        Assert.Equal([KeyValuePair.Create("A", "1000")], e.Values);
    });

    [Fact]
    public void A_null_result_violates_and_values_are_written_as_Spark_writes_them() => InTable("values", path =>
    {
        Write(path, [Good]);
        AddConstraint(path, "c", "F < 100 AND s <> 'bad' AND d > DATE '2000-01-01' AND ts > TIMESTAMP '2000-01-01 00:00:00'");

        DeltaConstraintViolationException Written(Point p) => Violation(() => Write(path, [p]));
        // Dates as days, timestamps as microseconds since the epoch, floating values as Java writes them; columns by
        // their names in the table.
        var values = Written(Good with { F = 1e10, D = new DateOnly(1999, 1, 1), Ts = new DateTimeOffset(1999, 1, 1, 0, 0, 0, 500, TimeSpan.Zero) }).Values;
        Assert.Equal(
            [KeyValuePair.Create("D", "10592"), KeyValuePair.Create("F", "1.0E10"), KeyValuePair.Create("S", "ok"), KeyValuePair.Create("Ts", "915148800500000")],
            [.. values.OrderBy(v => v.Key, StringComparer.OrdinalIgnoreCase)]);
        Assert.Equal("null", Written(Good with { S = null }).Values.Single(v => v.Key == "S").Value);
        Assert.Single(Ids(path));
    });

    [Fact]
    public void A_column_invariant_is_checked_under_Sparks_name_for_it() => InTable("invariant", path =>
    {
        Write(path, [new Plain(1, 5)]);
        AddInvariant(path, "A", "A > 3");
        Write(path, [new Plain(2, 4)]);
        var e = Violation(() => Write(path, [new Plain(3, null)]));
        Assert.Equal("EXPRESSION(('A > 3))", e.Constraint);
        Assert.StartsWith("[DELTA_VIOLATE_CONSTRAINT_WITH_VALUES] CHECK constraint EXPRESSION(('A > 3)) (A > 3) violated by row with values:\n - A : null", e.Message);
        Assert.Equal([1, 2], Ids(path));
    });

    [Fact]
    public void Updates_and_rewrites_are_checked_the_rows_a_delete_keeps_are_not() => InTable("operations", path =>
    {
        // One file: the delete below rewrites it.
        Write(path, [new Plain(1, 1), new Plain(2, 2)]);
        AddConstraint(path, "small", "a < 10");
        Violation(() => Delta.Update(path, Col("Id") == 1, new Dictionary<string, Expr> { ["A"] = Lit(50) }));

        // A constraint the existing rows already break (as one added by hand): a delete keeps them, an optimize's
        // rewrite of them fails.
        AddConstraint(path, "even", "a % 2 = 0");
        Delta.Delete(path, Col("Id") == 2);
        Write(path, [new Plain(3, 4)]);
        long version = Latest(path).Version;
        Violation(() => Delta.Optimize(path));
        Assert.Equal(version, Latest(path).Version);
        Assert.Equal([1, 3], Ids(path));
    });

    [Fact]
    public void A_constraint_the_kernel_cannot_enforce_refuses_the_write_before_any_file() => InTable("unsupported", path =>
    {
        Write(path, [new Plain(1, 1)]);
        AddConstraint(path, "r", "rand() < 2");
        int files = Directory.GetFiles(path, "*.parquet", SearchOption.AllDirectories).Length;
        var e = Assert.IsType<DeltaProtocolException>(Assert.Throws<PolarsException>(() => Write(path, [new Plain(2, 2)])).InnerException);
        Assert.StartsWith("The table's CHECK constraint r (rand() < 2) cannot be enforced by this kernel: rand() is not allowed in a CHECK constraint", e.Message);
        Assert.Equal(files, Directory.GetFiles(path, "*.parquet", SearchOption.AllDirectories).Length);
    });

    [Fact]
    public void A_constraint_fails_where_Spark_fails_it() => InTable("division", path =>
    {
        Write(path, [new Plain(1, 1)]);
        AddConstraint(path, "ratio", "10 / a > 1");
        Assert.Throws<DivideByZeroException>(() => Write(path, [new Plain(2, 0)]));
        Assert.Equal([1], Ids(path));
    });

    [Fact]
    public void A_constraint_short_circuits_as_Spark_evaluates_it() => InTable("guarded", path =>
    {
        // A zero divisor the condition skips is never divided by.
        Write(path, [new Plain(1, 1)]);
        AddConstraint(path, "guarded", "a = 0 OR 10 div a < 100");
        Write(path, [new Plain(2, 0)]);
        Assert.Equal([1, 2], Ids(path));
    });

    [Theory]
    [InlineData(1.5, "1.5")]
    [InlineData(100.0, "100.0")]
    [InlineData(0.001, "0.001")]
    [InlineData(9999999.5, "9999999.5")]
    [InlineData(1e7, "1.0E7")]
    [InlineData(12345678.9, "1.23456789E7")]
    [InlineData(1e10, "1.0E10")]
    [InlineData(0.0001234, "1.234E-4")]
    [InlineData(-2.5e-10, "-2.5E-10")]
    [InlineData(1.7976931348623157e308, "1.7976931348623157E308")]
    [InlineData(0.0, "0.0")]
    [InlineData(-0.0, "-0.0")]
    [InlineData(double.NaN, "NaN")]
    [InlineData(double.NegativeInfinity, "-Infinity")]
    public void Floating_values_are_written_as_Java_writes_them(double value, string text) =>
        Assert.Equal(text, DeltaConstraintCheck.JavaText(value));

    [Fact]
    public void Float_values_take_their_own_shortest_digits() =>
        Assert.Equal(["0.1", "3.4028235E38", "1.0E-5"], [.. new[] { 0.1f, float.MaxValue, 1e-5f }.Select(DeltaConstraintCheck.JavaText)]);
}
