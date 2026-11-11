using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

/// <summary>Creating Delta tables with identity columns, and reading row ids, through the public API.</summary>
public class DeltaIdentityRowTrackingTests
{
    [Fact]
    public void CreateTable_DeclaresIdentity_AndScanDelta_ReturnsStableRowIds()
    {
        string path = Path.Combine(Path.GetTempPath(), $"delta_identity_{Guid.NewGuid():N}");
        try
        {
            // A schema is consumed by the call that takes it.
            static PolarsSchema Schema() => new PolarsSchema().Add("id", DataType.Int64).Add("v", DataType.String);
            Delta.CreateTable(path, Schema(),
                properties: new() { [DeltaTableProperties.EnableRowTracking] = "true" },
                identityColumns: [new DeltaIdentityColumn("id", Start: 100, Step: 10)]);

            using (var rows = DataFrame.FromColumns(new { v = new[] { "a", "b", "c" } }))
                rows.WriteDelta(path, mode: DeltaSaveMode.Append);
            Delta.Delete(path, Col("v") == "b");

            using var read = DataFrame.ReadDelta(path, rowIdColumn: "row_id", rowCommitVersionColumn: "row_version").Sort("v");
            Assert.Equal(new[] { "v", "id", "row_id", "row_version" }.Order(), read.Columns.Order());
            Assert.Equal(100L, read.Column("id").GetValue<long>(0));
            Assert.Equal(120L, read.Column("id").GetValue<long>(1));
            // The delete rewrote the file; the rows kept their ids and versions.
            Assert.Equal(0L, read.Column("row_id").GetValue<long>(0));
            Assert.Equal(2L, read.Column("row_id").GetValue<long>(1));
            Assert.Equal(1L, read.Column("row_version").GetValue<long>(0));

            // GENERATED ALWAYS takes no values; creating again fails; a table without row tracking has no row ids.
            using (var given = DataFrame.FromColumns(new { id = new[] { 1L }, v = new[] { "x" } }))
                Assert.Contains("GENERATED ALWAYS", Assert.ThrowsAny<Exception>(() => given.WriteDelta(path, mode: DeltaSaveMode.Append)).Message);
            Assert.ThrowsAny<Exception>(() => Delta.CreateTable(path, Schema()));
            string plain = Path.Combine(path, "plain");
            Delta.CreateTable(plain, Schema());
            Assert.Contains("row tracking", Assert.ThrowsAny<Exception>(() => LazyFrame.ScanDelta(plain, rowIdColumn: "row_id").Collect()).Message);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, true);
        }
    }
}
