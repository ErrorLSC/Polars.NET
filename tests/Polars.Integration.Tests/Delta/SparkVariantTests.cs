using Kaminari.Seseragi.Files.IO;
using Polars.CSharp;
using Polars.Integration.Tests.Fixtures;
using Polars.NET.Core.Delta;
using Delta = Polars.CSharp.Delta;

namespace Polars.Integration.Tests;

/// <summary>Tables with variant columns (the variantType feature), written by Spark and read by the kernel.</summary>
public class SparkVariantTests
{
    private static void InTable(Action<string> body)
    {
        string root = Path.Combine(Path.GetTempPath(), $"seseragi_variant_{Guid.NewGuid():N}");
        try
        {
            body(Path.Combine(root, "table"));
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }
    }

    /// <summary>A Spark table with variant columns at the top level, in a struct and in an array; row 2 all null.</summary>
    private static void CreateVariantTable(string table)
    {
        LocalSpark.Run("sql", table, "CREATE TABLE {table} (id INT, v VARIANT, s STRUCT<x: VARIANT>, a ARRAY<VARIANT>) USING delta");
        LocalSpark.Run("sql", table, """
            INSERT INTO {table} SELECT 1, parse_json('{"a": 1, "b": [true, null, 2.5], "c": "hi"}'),
                named_struct('x', parse_json('42')), array(parse_json('"s"'), parse_json('null'))
            """);
        LocalSpark.Run("sql", table, "INSERT INTO {table} SELECT 2, NULL, NULL, NULL");
    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void VariantColumns_AreReadAsTheirValueAndMetadataBinaries()
    {
        InTable(table =>
        {
            CreateVariantTable(table);

            using DataFrame read = DataFrame.ReadDelta(table).Sort("id");
            Assert.Equal(["id", "v", "s", "a"], read.Columns);
            string type = read.Schema["v"].ToString();
            Assert.True(type.Contains("value", StringComparison.Ordinal) && type.Contains("metadata", StringComparison.Ordinal), type);
            using DataFrame v = read.Select(Polars.CSharp.Polars.Col("v").Struct.Field("value"), Polars.CSharp.Polars.Col("v").Struct.Field("metadata"));
            byte[]?[] values = v["value"].ToArray<byte[]?>();
            byte[]?[] metadata = v["metadata"].ToArray<byte[]?>();
            // Row 1: an object (basic type 2 in the low bits of the header), metadata version 1; row 2: null.
            Assert.Equal(2, values[0]![0] & 0x3);
            Assert.Equal(1, metadata[0]![0] & 0xF);
            using DataFrame nulls = read.Select(Polars.CSharp.Polars.Col("v").IsNull().Alias("v"), Polars.CSharp.Polars.Col("s").Struct.Field("x").IsNull().Alias("x"));
            Assert.Equal([false, true], nulls["v"].ToArray<bool>());
            Assert.Equal([false, true], nulls["x"].ToArray<bool>());
        });
    }

    /// <summary>Every file of the table, data and log, with its size.</summary>
    private static string[] Files(string table) =>
        [.. Directory.EnumerateFiles(table, "*", SearchOption.AllDirectories)
            .Select(f => $"{Path.GetRelativePath(table, f)}:{new FileInfo(f).Length}").Order(StringComparer.Ordinal)];

    [SparkTheory]
    [InlineData("delete")]
    [InlineData("update")]
    [InlineData("append")]
    [InlineData("optimize")]
    [InlineData("vacuum")]
    [InlineData("restore")]
    [InlineData("properties")]
    [InlineData("dropFeature")]
    [Trait("DeltaKernel", "Spark")]
    public void VariantTables_AreReadOnly(string operation)
    {
        InTable(table =>
        {
            CreateVariantTable(table);
            // A removed file for vacuum to find.
            LocalSpark.Run("sql", table, "DELETE FROM {table} WHERE id = 2");
            string[] before = Files(table);

            Assert.Throws<Polars.NET.Core.Exceptions.PolarsException>(() =>
            {
                switch (operation)
                {
                    case "delete":
                        Delta.Delete(table, Polars.CSharp.Polars.Col("id") == 1);
                        break;
                    case "update":
                        Delta.Update(table, Polars.CSharp.Polars.Col("id") == 1, new Dictionary<string, Expr> { ["id"] = Polars.CSharp.Polars.Lit(5) });
                        break;
                    case "append":
                        using (DataFrame rows = DataFrame.ReadDelta(table))
                            rows.WriteDelta(table, mode: DeltaSaveMode.Append);
                        break;
                    case "optimize":
                        Delta.Optimize(table);
                        break;
                    case "vacuum":
                        Delta.Vacuum(table, retentionHours: 0, enforceRetention: false);
                        break;
                    case "restore":
                        Delta.Restore(table, version: 1);
                        break;
                    case "properties":
                        Delta.SetTableProperties(table, new() { ["delta.appendOnly"] = "true" });
                        break;
                    case "dropFeature":
                        Delta.DropFeature(table, "variantType");
                        break;
                }
            });
            Assert.Equal(before, Files(table));
        });
    }

    /// <summary>Values of every kind a variant holds, one per row, as Spark casts or parses them.</summary>
    private static readonly string[] Values =
    [
        "parse_json('{\"b\": [true, null, 2.5], \"a\": 1, \"c\": \"hi\"}')",
        "parse_json('\"esc: \\\\\" \\\\\\\\ \\\\n \\\\t \\\\u0001 \\\\u001f / ü 😀\"')",
        "parse_json('12345678901234567890123')",
        "parse_json('1e400')",
        "parse_json('[]')",
        "parse_json('{}')",
        "cast(cast(-5 as tinyint) as variant)",
        "cast(cast(300 as smallint) as variant)",
        "cast(-70000 as variant)",
        "cast(5000000000 as variant)",
        "cast(cast(0.1 as float) as variant)",
        "cast(cast(1e10 as float) as variant)",
        "cast(0.1 + 0.2 as variant)",
        "cast(1e-5 as variant)",
        "cast(12345678.9 as variant)",
        "cast(1234567.0 as variant)",
        "cast(cast('NaN' as double) as variant)",
        "cast(cast('-Infinity' as double) as variant)",
        "cast(cast(-0.0 as double) as variant)",
        "cast(4.9e-324 as variant)",
        "cast(1234567.0D as variant)",
        "cast(1.0E7D as variant)",
        "cast(12345678.9D as variant)",
        "cast(0.001D as variant)",
        "cast(1.0E-4D as variant)",
        "cast(1e23D as variant)",
        "cast(100.0D as variant)",
        "cast(0.1D + 0.2D as variant)",
        "cast(cast(3.25 as float) as variant)",
        "cast(cast(1.4E-45 as float) as variant)",
        "cast(cast(1.50 as decimal(5, 2)) as variant)",
        "cast(cast(-0.005 as decimal(10, 3)) as variant)",
        "cast(cast('-12345678901234567890.1' as decimal(38, 1)) as variant)",
        "cast(date'2024-02-29' as variant)",
        "cast(date'1969-12-31' as variant)",
        "cast(timestamp'2024-01-02 03:04:05.5' as variant)",
        "cast(timestamp_ntz'1969-12-31 23:59:59.999999' as variant)",
        "cast(timestamp_ntz'2024-01-02 00:00:00' as variant)",
        "cast(X'010203' as variant)",
        "cast(true as variant)",
        "parse_json('null')",
        "cast(sequence(1, 300) as variant)",
        "parse_json(concat('{', concat_ws(',', transform(sequence(1, 300), i -> concat('\"k', i, '\":', i))), '}'))",
        "to_variant_object(named_struct('z', 1, 'a', array(1, 2), 'm', named_struct('q', 'x')))",
    ];

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void VariantAsJson_RendersEveryValueAsSparksToJson()
    {
        InTable(table =>
        {
            LocalSpark.Run("sql", table,
                "CREATE TABLE {table} (id INT, v VARIANT, s STRUCT<x: VARIANT>, a ARRAY<VARIANT>, m MAP<STRING, VARIANT>) USING delta");
            // A SELECT per value (inline VALUES cannot hold lambdas), then a row of nulls.
            string values = string.Join(" UNION ALL ", Values.Select((value, i) => $"SELECT {i} AS id, {value} AS v"));
            LocalSpark.Run("sql", table, $$"""
                INSERT INTO {table} SELECT id, v, named_struct('x', v), array(v, parse_json('7')), map('k', v) FROM ({{values}})
                UNION ALL SELECT {{Values.Length}}, NULL, NULL, NULL, NULL
                """);

            // Spark's rendering of each value, in each place.
            var spark = LocalSpark.Run("query", table, """
                SELECT id, to_json(v) AS v, to_json(s.x) AS sx, to_json(a[0]) AS a0, to_json(m['k']) AS mk
                FROM {table} ORDER BY id
                """)[0].RootElement.GetProperty("rows").EnumerateArray()
                .Select(r => (Id: r.GetProperty("id").GetInt32(), V: r.GetProperty("v").GetString(), Sx: r.GetProperty("sx").GetString(),
                    A0: r.GetProperty("a0").GetString(), Mk: r.GetProperty("mk").GetString()))
                .ToArray();

            using DataFrame read = DataFrame.ReadDelta(table, variantAsJson: true).Sort("id");
            using DataFrame flat = read.Select(
                Polars.CSharp.Polars.Col("v"),
                Polars.CSharp.Polars.Col("s").Struct.Field("x").Alias("sx"),
                Polars.CSharp.Polars.Col("a").List.Get(0).Alias("a0"),
                Polars.CSharp.Polars.Col("a").List.Get(1).Alias("a1"));
            string?[] v = flat["v"].ToArray<string?>(), sx = flat["sx"].ToArray<string?>(), a0 = flat["a0"].ToArray<string?>();
            Assert.Equal(spark.Length, v.Length);
            for (int i = 0; i < spark.Length; i++)
            {
                Assert.True(spark[i].V == v[i], $"row {i}: Spark {spark[i].V} / kernel {v[i]}");
                Assert.Equal(spark[i].Sx, sx[i]);
                Assert.Equal(spark[i].A0, a0[i]);
            }
            Assert.Equal("7", flat["a1"].ToArray<string?>()[0]);
            // The map's values too.
            using DataFrame map = read.Select(Polars.CSharp.Polars.Col("m").MapOps.Get("k").Alias("mk"));
            Assert.Equal(spark.Select(r => r.Mk), map["mk"].ToArray<string?>());
        });
    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void VariantColumns_OfAMappedPartitionedTableWithDeletionVectors_ReadAsSparkReadsThem()
    {
        InTable(table =>
        {
            LocalSpark.Run("sql", table, """
                CREATE TABLE {table} (id INT, p STRING, v VARIANT) USING delta PARTITIONED BY (p)
                TBLPROPERTIES ('delta.columnMapping.mode'='name', 'delta.enableDeletionVectors'='true')
                """);
            LocalSpark.Run("sql", table, """
                INSERT INTO {table} SELECT /*+ COALESCE(1) */ id, p, parse_json(j) FROM VALUES
                    (1, 'x', '{"k": [1, 2]}'), (2, 'x', '"two"'), (3, 'x', '3.5'), (4, 'y', 'null'), (5, 'y', '{"a": {"b": true}}')
                    AS t(id, p, j)
                """);
            LocalSpark.Run("sql", table, "DELETE FROM {table} WHERE id = 2");
            LocalSpark.Run("sql", table, "ALTER TABLE {table} RENAME COLUMN v TO w");
            Assert.Contains(Kaminari.Seseragi.Lakehouse.Delta.Snapshot.Load(new Kaminari.Seseragi.Lakehouse.Engine.LocalFileSystem(table)).Files,
                f => f.DeletionVector is not null);

            string spark = string.Join(" ", LocalSpark.Run("query", table, "SELECT id, p, to_json(w) AS w FROM {table} ORDER BY id")[0]
                .RootElement.GetProperty("rows").EnumerateArray().Select(r => $"{r.GetProperty("id")}|{r.GetProperty("p").GetString()}|{r.GetProperty("w").GetString()}"));
            using DataFrame json = DataFrame.ReadDelta(table, variantAsJson: true).Sort("id");
            int[] ids = json["id"].ToArray<int>();
            string[] parts = json["p"].ToArray<string>()!;
            string?[] values = json["w"].ToArray<string?>();
            Assert.Equal(spark, string.Join(" ", ids.Select((id, i) => $"{id}|{parts[i]}|{values[i]}")));

            // As stored too, with a row index (the deletion vector's rows dropped by position).
            using DataFrame raw = DataFrame.ReadDelta(table, rowIndexName: "row").Sort("id");
            Assert.Equal([1, 3, 4, 5], raw["id"].ToArray<int>());
            Assert.Contains("row", raw.Columns);
            Assert.Contains("w", raw.Columns);
        });
    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void VariantTables_ReadOnlyTheFootersAndTheColumnsAndRowsAQueryUses()
    {
        InTable(table =>
        {
            // Wide rows, two files, and a widened column: creating the scan reads each file's schema for it.
            LocalSpark.Run("sql", table,
                "CREATE TABLE {table} (id INT, pad STRING, v VARIANT) USING delta TBLPROPERTIES ('delta.enableTypeWidening'='true')");
            LocalSpark.Run("sql", table, """
                INSERT INTO {table} SELECT CAST(id AS INT), repeat(md5(CAST(id AS STRING)), 8), parse_json(concat('{"k":', id, '}'))
                FROM range(100000)
                """);
            LocalSpark.Run("sql", table, "ALTER TABLE {table} ALTER COLUMN id TYPE BIGINT");
            LocalSpark.Run("sql", table, "INSERT INTO {table} SELECT 100000, 'x', parse_json('1')");
            long total = Directory.EnumerateFiles(table, "*.parquet", SearchOption.AllDirectories).Sum(f => new FileInfo(f).Length);

            var counter = new ReadCounter();
            DeltaKernelScan.ManagedStorage.WrapForTests = store => new CountingStore(store, counter, table);
            try
            {
                using var lf = LazyFrame.ScanDelta(table);
                long atCreation = counter.Bytes;
                Assert.InRange(atCreation, 1, total / 10);

                using var head = lf.Select(Polars.CSharp.Polars.Col("id")).Head(5).Collect();
                Assert.Equal(5, head.Height);
                Assert.InRange(counter.Bytes - atCreation, 1, total / 10);

                using var all = lf.Select(Polars.CSharp.Polars.Col("v")).Collect();
                Assert.Equal(100_001, all.Height);
            }
            finally
            {
                DeltaKernelScan.ManagedStorage.WrapForTests = null;
            }
        });
    }

    /// <summary>
    /// Columns of every type next to a variant read as the engine reads them without one: the same rows in a table
    /// with a variant column (managed reader) and one without (engine), on the in-memory and streaming engines and
    /// through a sink.
    /// </summary>
    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void ColumnsNextToAVariant_ReadAsInATableWithout()
    {
        string root = Path.Combine(Path.GetTempPath(), $"seseragi_variant_types_{Guid.NewGuid():N}");
        string with = Path.Combine(root, "with"), without = Path.Combine(root, "without");
        try
        {
            const string columns =
                "id INT, b TINYINT, sh SMALLINT, l BIGINT, f FLOAT, d DOUBLE, bo BOOLEAN, dec DECIMAL(10, 2), dec38 DECIMAL(38, 6), " +
                "bin BINARY, dt DATE, ts TIMESTAMP, tsn TIMESTAMP_NTZ, st STRUCT<t: TIMESTAMP, n: DECIMAL(20, 3)>, " +
                "arr ARRAY<TIMESTAMP>, m MAP<STRING, DECIMAL(10, 2)>";
            const string values = """
                SELECT CAST(id AS INT), CAST(id % 100 AS TINYINT), CAST(id * 7 AS SMALLINT), id * 1000000007, CAST(id / 3.0 AS FLOAT),
                    id / 7.0, id % 2 = 0, CAST(id / 100.0 AS DECIMAL(10, 2)), CAST(id * 12345678901234.123456 AS DECIMAL(38, 6)),
                    CAST(concat('b', id) AS BINARY), date_add(DATE'1969-12-25', CAST(id AS INT)),
                    TIMESTAMP'2024-03-10 01:30:00.123456' + make_interval(0, 0, 0, 0, CAST(id AS INT), 0, 0),
                    TIMESTAMP_NTZ'1900-01-01 00:00:00.5' + make_interval(0, 0, 0, CAST(id AS INT), 0, 0, 0),
                    named_struct('t', TIMESTAMP'2000-01-01 00:00:00', 'n', CAST(id / 1000.0 AS DECIMAL(20, 3))),
                    array(TIMESTAMP'1999-12-31 23:59:59.999999', NULL), map('k', CAST(id AS DECIMAL(10, 2)))
                FROM range(50)
                UNION ALL SELECT 50, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
                """;
            LocalSpark.Run("sql", with, $"CREATE TABLE {{table}} ({columns}, v VARIANT) USING delta");
            LocalSpark.Run("sql", with, $"INSERT INTO {{table}} SELECT *, parse_json('1') FROM ({values})");
            LocalSpark.Run("sql", without, $"CREATE TABLE {{table}} ({columns}) USING delta");
            LocalSpark.Run("sql", without, $"INSERT INTO {{table}} {values}");

            using DataFrame expected = DataFrame.ReadDelta(without).Sort("id");
            string[] names = [.. expected.Columns];
            Polars.CSharp.Expr[] shared = [.. names.Select(n => Polars.CSharp.Polars.Col(n))];
            using LazyFrame scan = LazyFrame.ScanDelta(with);
            using (DataFrame inMemory = scan.Select(shared).Collect().Sort("id")) AssertSame(expected, inMemory);
            using (DataFrame streaming = scan.Select(shared).Collect(Engine.Streaming).Sort("id")) AssertSame(expected, streaming);
            string sunk = Path.Combine(root, "sunk.parquet");
            scan.Select(shared).SinkParquet(sunk);
            using (DataFrame back = DataFrame.ReadParquet(sunk).Sort("id")) AssertSame(expected, back);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }

        static void AssertSame(DataFrame expected, DataFrame actual)
        {
            Assert.Equal(expected.Columns, actual.Columns);
            Assert.Equal(expected.Schema.ToString(), actual.Schema.ToString());
            Assert.Equal(expected.Height, actual.Height);
            Assert.True(expected.Equals(actual), $"expected\n{expected}\nactual\n{actual}");
        }
    }

    /// <summary>
    /// A partitioned table with a variant column is read as one dataset whose partition columns hold the files'
    /// partition values: of every type a partition column may have, null included, the same rows and types as the
    /// engine reads from a table without the variant; a binary partition column, which a dataset cannot have, falls
    /// back to one scan per partition.
    /// </summary>
    [SparkTheory]
    [InlineData("pi INT", "ps STRING", "pd DATE", "pt TIMESTAMP", "pn TIMESTAMP_NTZ", "pb BOOLEAN", "pf DOUBLE", "pc DECIMAL(10, 2)", "pl BIGINT")]
    [InlineData("pi INT", "pbin BINARY")]
    [Trait("DeltaKernel", "Spark")]
    public void PartitionColumns_OfAVariantTable_ReadAsInATableWithout(params string[] columns)
    {
        string root = Path.Combine(Path.GetTempPath(), $"seseragi_variant_parts_{Guid.NewGuid():N}");
        string with = Path.Combine(root, "with"), without = Path.Combine(root, "without");
        try
        {
            string partitions = string.Join(", ", columns);
            string[] names = [.. columns.Select(c => c.Split(' ')[0])];
            string Value(string name, string id) => name switch
            {
                "pi" => $"CAST({id} % 3 AS INT)",
                "ps" => $"concat('a b/c=', {id} % 2)",
                "pd" => $"date_add(DATE'1969-12-30', CAST({id} % 3 AS INT))",
                "pt" => $"TIMESTAMP'2024-03-10 01:30:00.123456' + make_interval(0, 0, 0, 0, CAST({id} % 2 AS INT), 0, 0)",
                "pn" => $"TIMESTAMP_NTZ'1900-01-01 00:00:00' + make_interval(0, 0, 0, CAST({id} % 2 AS INT), 0, 0, 0)",
                "pb" => $"{id} % 2 = 0",
                "pf" => $"{id} / 4.0",
                "pc" => $"CAST({id} / 8.0 AS DECIMAL(10, 2))",
                "pl" => $"{id} * 10000000000",
                "pbin" => $"CAST(concat('b', {id} % 2) AS BINARY)",
                _ => throw new ArgumentException(name),
            };
            string select = $"SELECT CAST(id AS INT) AS id, {string.Join(", ", names.Select(n => $"{Value(n, "id")} AS {n}"))} FROM range(6)"
                + $" UNION ALL SELECT 6, {string.Join(", ", names.Select(_ => "NULL"))}";
            string by = string.Join(", ", names);
            LocalSpark.Run("sql", with, $"CREATE TABLE {{table}} (id INT, {partitions}, v VARIANT) USING delta PARTITIONED BY ({by})");
            LocalSpark.Run("sql", with, $"INSERT INTO {{table}} SELECT *, parse_json(concat('{{\"k\":', id, '}}')) FROM ({select})");
            LocalSpark.Run("sql", without, $"CREATE TABLE {{table}} (id INT, {partitions}) USING delta PARTITIONED BY ({by})");
            LocalSpark.Run("sql", without, $"INSERT INTO {{table}} {select}");

            using DataFrame expected = DataFrame.ReadDelta(without).Sort("id");
            Assert.Equal(7, expected.Height);
            Polars.CSharp.Expr[] shared = [.. expected.Columns.Select(n => Polars.CSharp.Polars.Col(n))];
            using LazyFrame scan = LazyFrame.ScanDelta(with);
            using DataFrame actual = scan.Select(shared).Collect().Sort("id");
            Assert.Equal(expected.Schema.ToString(), actual.Schema.ToString());
            Assert.True(expected.Equals(actual), $"expected\n{expected}\nactual\n{actual}");
            using DataFrame variants = DataFrame.ReadDelta(with, variantAsJson: true).Sort("id");
            Assert.Equal("{\"k\":4}", variants["v"].ToArray<string?>()[4]);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }
    }

    /// <summary>A row of the pruning test's table, as LINQ reads it (the variant column left out).</summary>
    private sealed record PadRow(long id, string pad);

    /// <summary>
    /// A LINQ filter over a variant table reaches the managed reader: on a table without file statistics (no file
    /// can be skipped by the log) the reader leaves out the files whose footers rule every row out, reading less than
    /// half of what the same query without the filter reads.
    /// </summary>
    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void LinqFilters_OverAVariantTable_PruneInTheReader()
    {
        InTable(table =>
        {
            LocalSpark.Run("sql", table,
                "CREATE TABLE {table} (id BIGINT, pad STRING, v VARIANT) USING delta TBLPROPERTIES ('delta.dataSkippingNumIndexedCols'='0')");
            LocalSpark.Run("sql", table, """
                INSERT INTO {table} SELECT /*+ REPARTITION_BY_RANGE(4, id) */ id, repeat(md5(CAST(id AS STRING)), 4), parse_json('1')
                FROM range(40000)
                """);
            Assert.Equal(4, Directory.EnumerateFiles(table, "*.parquet", SearchOption.AllDirectories).Count());

            var counter = new ReadCounter();
            DeltaKernelScan.ManagedStorage.WrapForTests = store => new CountingStore(store, counter, table);
            try
            {
                using var lf = LazyFrame.ScanDelta(table);
                long before = counter.Bytes;
                PadRow[] all = [.. Polars.CSharp.Linq.LinqExtensions.AsQueryable<PadRow>(lf).Where(r => r.id >= 0)];
                long whole = counter.Bytes - before;
                Assert.Equal(40_000, all.Length);

                before = counter.Bytes;
                PadRow[] rows = [.. Polars.CSharp.Linq.LinqExtensions.AsQueryable<PadRow>(lf).Where(r => r.id < 10)];
                Assert.Equal(Enumerable.Range(0, 10).Select(i => (long)i), rows.Select(r => r.id).Order());
                Assert.All(rows, r => Assert.Equal(128, r.pad.Length));
                // One file of four holds the rows (footers read either way).
                Assert.InRange(counter.Bytes - before, 1, whole / 2);
            }
            finally
            {
                DeltaKernelScan.ManagedStorage.WrapForTests = null;
            }
        });
    }

    /// <summary>
    /// The engine's own filters over a variant table reach the managed reader as LINQ's do (through the I/O plugin
    /// bridge): the files ruled out are left unread, and the reader applies the filter exactly (the plan keeps no
    /// filter of the engine's above or in the scan).
    /// </summary>
    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void EngineFilters_OverAVariantTable_PruneInTheReader()
    {
        InTable(table =>
        {
            LocalSpark.Run("sql", table,
                "CREATE TABLE {table} (id BIGINT, pad STRING, v VARIANT) USING delta TBLPROPERTIES ('delta.dataSkippingNumIndexedCols'='0')");
            LocalSpark.Run("sql", table, """
                INSERT INTO {table} SELECT /*+ REPARTITION_BY_RANGE(4, id) */ id, repeat(md5(CAST(id AS STRING)), 4), parse_json('1')
                FROM range(40000)
                """);

            var counter = new ReadCounter();
            DeltaKernelScan.ManagedStorage.WrapForTests = store => new CountingStore(store, counter, table);
            try
            {
                using var lf = LazyFrame.ScanDelta(table);
                long before = counter.Bytes;
                using (var all = lf.Select(Polars.CSharp.Polars.Col("id"), Polars.CSharp.Polars.Col("pad")).Collect()) Assert.Equal(40_000, all.Height);
                long whole = counter.Bytes - before;

                using var q = lf.Filter(Polars.CSharp.Polars.Col("id") < 10).Select(Polars.CSharp.Polars.Col("id"), Polars.CSharp.Polars.Col("pad"));
                string plan = q.Explain();
                Assert.Contains("filters rows by", plan);
                Assert.DoesNotContain("SELECTION:", plan);
                Assert.DoesNotContain("FILTER", plan);
                before = counter.Bytes;
                using var df = q.Collect();
                Assert.Equal(10, df.Height);
                Assert.InRange(counter.Bytes - before, 1, whole / 2);
            }
            finally
            {
                DeltaKernelScan.ManagedStorage.WrapForTests = null;
            }
        });
    }

    /// <summary>The bytes read from files below a table.</summary>
    private sealed class ReadCounter
    {
        private long _bytes;

        public long Bytes => Interlocked.Read(ref _bytes);

        public void Add(long bytes) => Interlocked.Add(ref _bytes, bytes);
    }

    /// <summary>A store counting the bytes read from files below <paramref name="root"/>.</summary>
    private sealed class CountingStore(IFileStore inner, ReadCounter counter, string root) : IFileStore
    {
        public bool CanList => inner.CanList;

        public IAsyncEnumerable<StoredFile> ListAsync(string prefix, bool recursive, CancellationToken cancellationToken = default) =>
            inner.ListAsync(prefix, recursive, cancellationToken);

        public ValueTask<StoredFile?> StatAsync(string path, CancellationToken cancellationToken = default) => inner.StatAsync(path, cancellationToken);

        public async ValueTask<IRandomAccess> OpenAsync(StoredFile file, CancellationToken cancellationToken = default) =>
            Wrap(file.Path, await inner.OpenAsync(file, cancellationToken));

        public async ValueTask<IRandomAccess?> OpenAsync(string path, CancellationToken cancellationToken = default) =>
            await inner.OpenAsync(path, cancellationToken) is { } source ? Wrap(path, source) : null;

        private IRandomAccess Wrap(string path, IRandomAccess source) =>
            path.StartsWith(root, StringComparison.Ordinal) ? new Source(source, counter) : source;

        private sealed class Source(IRandomAccess inner, ReadCounter counter) : IRandomAccess, IDisposable
        {
            public long Length => inner.Length;

            public int ReadAt(long offset, Span<byte> destination)
            {
                int read = inner.ReadAt(offset, destination);
                counter.Add(read);
                return read;
            }

            public async ValueTask<int> ReadAtAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken)
            {
                int read = await inner.ReadAtAsync(offset, destination, cancellationToken);
                counter.Add(read);
                return read;
            }

            public void Dispose() => (inner as IDisposable)?.Dispose();
        }
    }
}

/// <summary>A Spark variant table copied to S3 (MinIO): read there as it reads locally.</summary>
public class SparkVariantCloudTests(MinioFixture minio) : IClassFixture<MinioFixture>
{
    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void VariantTable_OnS3_ReadsAsTheLocalCopy()
    {
        string root = Path.Combine(Path.GetTempPath(), $"seseragi_variant_s3_{Guid.NewGuid():N}");
        string table = Path.Combine(root, "table");
        try
        {
            // Partitioned, with a deletion vector: the grouped scan and the one-file scan both run.
            LocalSpark.Run("sql", table,
                "CREATE TABLE {table} (id INT, p STRING, v VARIANT) USING delta PARTITIONED BY (p) " +
                "TBLPROPERTIES ('delta.enableDeletionVectors'='true')");
            LocalSpark.Run("sql", table, """
                INSERT INTO {table} SELECT CAST(id AS INT), IF(id % 2 = 0, 'even', 'odd'), parse_json(concat('{"k":', id, '}'))
                FROM range(1000)
                """);
            LocalSpark.Run("sql", table, "DELETE FROM {table} WHERE id % 10 = 3");

            var options = CloudOptions.Aws(region: minio.Region, accessKey: minio.AccessKey, secretKey: minio.SecretKey,
                endpoint: $"http://{minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/')}");
            options.Credentials!["aws_allow_http"] = "true";
            options.Credentials!["aws_s3_force_path_style"] = "true";
            string url = $"s3://{minio.BucketName}/variant_{Guid.NewGuid():N}";
            using (var store = new ObjectStoreFileSystem(url, KernelObjectStoreCheck.ToArgs(options)))
            {
                foreach (string file in Directory.EnumerateFiles(table, "*", SearchOption.AllDirectories))
                    store.Overwrite(Path.GetRelativePath(table, file).Replace('\\', '/'), File.ReadAllBytes(file));
            }

            using DataFrame local = DataFrame.ReadDelta(table, variantAsJson: true).Sort("id");
            using DataFrame remote = DataFrame.ReadDelta(url, cloudOptions: options, variantAsJson: true).Sort("id");
            Assert.Equal(900, local.Height);
            Assert.Equal(local["id"].ToArray<int>(), remote["id"].ToArray<int>());
            Assert.Equal(local["p"].ToArray<string>(), remote["p"].ToArray<string>());
            Assert.Equal(local["v"].ToArray<string?>(), remote["v"].ToArray<string?>());
            Assert.Equal("{\"k\":4}", remote["v"].ToArray<string?>()[3]);

            using DataFrame head = LazyFrame.ScanDelta(url, cloudOptions: options).Select(Polars.CSharp.Polars.Col("id")).Head(3).Collect();
            Assert.Equal(3, head.Height);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }
    }
}
