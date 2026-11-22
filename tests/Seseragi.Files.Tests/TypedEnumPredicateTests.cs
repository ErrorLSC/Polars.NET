using System.Linq.Expressions;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Ipc;
using Kaminari.Seseragi.Files.Json;
using Kaminari.Seseragi.Files.Parquet;
using Seseragi.Files.Tests.Parquet;

namespace Seseragi.Files.Tests;

/// <summary>
/// Predicates over enum members are pushed into every typed reader, exactly (the default Strict policy refuses client
/// evaluation): the files hold the members' names (a dictionary of names in Parquet and IPC, strings in JSON and in
/// partition paths), so a comparison of a member's value is evaluated as a comparison of names, orderings as the
/// names whose values satisfy them.
/// </summary>
public sealed class TypedEnumPredicateTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("seseragi-typed-enum-").FullName;

    public void Dispose() => Directory.Delete(_root, recursive: true);

    private static readonly Trade[] Rows = ParquetWriterOfTTests.Trades(3000);

    public static TheoryData<string> Sources => ["parquet", "ipc", "json", "parquet-dataset", "ipc-dataset", "json-dataset"];

    /// <summary>The trades written as <paramref name="source"/>: datasets partitioned by the enum.</summary>
    private string Write(string source)
    {
        string path = Path.Combine(_root, source);
        switch (source)
        {
            case "parquet": ParquetWriter.WriteRows(path, Rows); break;
            case "ipc": IpcWriter.WriteRows(path, Rows); break;
            case "json": JsonWriter.WriteRows(path, Rows); break;
            case "parquet-dataset": ParquetDatasetWriter.WriteRows(path, Rows, new ParquetDatasetWriteOptions { PartitionBy = ["Side"] }); break;
            case "ipc-dataset": IpcDatasetWriter.WriteRows(path, Rows, new IpcDatasetWriteOptions { PartitionBy = ["Side"] }); break;
            default: JsonDatasetWriter.WriteRows(path, Rows, new JsonDatasetWriteOptions { PartitionBy = ["Side"] }); break;
        }
        return path;
    }

    /// <summary>The ids of the rows read with <paramref name="where"/>, and whether the reader filtered them exactly.</summary>
    private static (List<long> Ids, bool Exact) Read(string source, string path, Expression<Func<Trade, bool>> where)
    {
        switch (source)
        {
            case "parquet":
            {
                using var file = ParquetFile.Open(path);
                using var reader = file.OpenReader(where);
                return ([.. reader.ReadRows().Select(t => t.Id)], reader.FiltersExactly);
            }
            case "ipc":
            {
                using var file = IpcFile.Open(path);
                using var reader = file.OpenReader(where);
                return ([.. reader.ReadRows().Select(t => t.Id)], reader.FiltersExactly);
            }
            case "json":
            {
                using var reader = JsonReader<Trade>.Open(path, where);
                return ([.. reader.ReadRows().Select(t => t.Id)], reader.FiltersExactly);
            }
            case "parquet-dataset":
            {
                using var dataset = ParquetDataset.Open(path);
                using var reader = dataset.OpenReader(where);
                return ([.. reader.ReadRows().Select(t => t.Id)], reader.FiltersExactly);
            }
            case "ipc-dataset":
            {
                using var dataset = IpcDataset.Open(path);
                using var reader = dataset.OpenReader(where);
                return ([.. reader.ReadRows().Select(t => t.Id)], reader.FiltersExactly);
            }
            default:
            {
                using var dataset = JsonDataset.Open(path);
                using var reader = dataset.OpenReader(where);
                return ([.. reader.ReadRows().Select(t => t.Id)], reader.FiltersExactly);
            }
        }
    }

    public static IEnumerable<Expression<Func<Trade, bool>>> Predicates()
    {
        Side[] sides = [Side.Buy, Side.Hold];
        Side? none = null;
        yield return t => t.Side == Side.Sell;
        yield return t => t.Side != Side.Sell;
        yield return t => Side.Sell == t.Side;
        yield return t => t.Side > Side.Buy;
        yield return t => t.Side <= Side.Sell;
        yield return t => Side.Sell < t.Side;
        yield return t => t.Side == (Side)7;
        yield return t => t.Side != (Side)7;
        yield return t => t.Side < (Side)7;
        yield return t => t.Side > (Side)(-1);
        yield return t => (Side)7 < t.Side;
        yield return t => (Side)(-1) < t.Side;
        yield return t => (int)t.Side == 2;
        yield return t => sides.Contains(t.Side);
        yield return t => !sides.Contains(t.Side);
        yield return t => t.Hint == Side.Hold;
        yield return t => t.Hint != Side.Hold;
        yield return t => t.Hint == none;
        yield return t => t.Hint != none;
        yield return t => t.Hint > Side.Buy;
        yield return t => !(t.Hint > Side.Buy);
        yield return t => t.Hint == none;
        yield return t => t.Side == Side.Sell && t.Hint != Side.Buy && t.Id < 2000;
        yield return t => t.Side == Side.Buy || t.Hint == Side.Sell;
    }

    [Theory]
    [MemberData(nameof(Sources))]
    public void EnumPredicates_ArePushedExactly(string source)
    {
        string path = Write(source);
        foreach (Expression<Func<Trade, bool>> where in Predicates())
        {
            Func<Trade, bool> compiled = where.Compile();
            (List<long> ids, bool exact) = Read(source, path, where);
            Assert.True(exact, $"{source}: {where} not filtered exactly");
            Assert.True(Rows.Where(compiled).Select(t => t.Id).Order().SequenceEqual(ids.Order()), $"{source}: {where}");
        }
    }

    /// <summary>A predicate on an enum partition column decides the files read: one of the three partitions.</summary>
    [Theory]
    [InlineData("parquet-dataset")]
    [InlineData("ipc-dataset")]
    [InlineData("json-dataset")]
    public void APredicateOnAnEnumPartition_ReadsItsFilesOnly(string source)
    {
        string path = Write(source);
        Expression<Func<Trade, bool>> where = t => t.Side == Side.Hold;
        IReadOnlyList<bool> read;
        List<long> ids;
        switch (source)
        {
            case "parquet-dataset":
            {
                using var dataset = ParquetDataset.Open(path);
                using var reader = dataset.OpenReader(where);
                ids = [.. reader.ReadRows().Select(t => t.Id)];
                read = reader.Inner.FilesRead;
                break;
            }
            case "ipc-dataset":
            {
                using var dataset = IpcDataset.Open(path);
                using var reader = dataset.OpenReader(where);
                ids = [.. reader.ReadRows().Select(t => t.Id)];
                read = reader.Inner.FilesRead;
                break;
            }
            default:
            {
                using var dataset = JsonDataset.Open(path);
                using var reader = dataset.OpenReader(where);
                ids = [.. reader.ReadRows().Select(t => t.Id)];
                read = reader.Inner.FilesRead;
                break;
            }
        }
        Assert.Equal(Rows.Where(t => t.Side == Side.Hold).Select(t => t.Id).Order(), ids.Order());
        Assert.Equal(3, read.Count);
        Assert.Equal(1, read.Count(r => r));
    }

    public enum Grade : byte { Low = 1, Mid = 5, High = 9, Medium = 5 }

    public sealed record Graded(int Id, Grade Grade, Grade? Other);

    /// <summary>
    /// An enum of another underlying type (compared as int by C#) and with an alias (a second name of a value): both
    /// names of the value are its rows, orderings name both.
    /// </summary>
    [Theory]
    [InlineData("parquet")]
    [InlineData("ipc")]
    [InlineData("json")]
    public void ByteEnumsWithAliases_ArePushedExactly(string format)
    {
        Graded[] rows = [.. Enumerable.Range(0, 600).Select(i => new Graded(i, (Grade)(1 + i % 3 * 4), i % 4 == 0 ? null : (Grade)(9 - i % 3 * 4)))];
        string path = Path.Combine(_root, "graded." + format);
        switch (format)
        {
            case "parquet": ParquetWriter.WriteRows(path, rows); break;
            case "ipc": IpcWriter.WriteRows(path, rows); break;
            default: JsonWriter.WriteRows(path, rows); break;
        }
        Expression<Func<Graded, bool>>[] predicates =
        [
            g => g.Grade == Grade.Mid,
            g => g.Grade == Grade.Medium,
            g => g.Grade != Grade.Medium,
            g => g.Grade >= Grade.Medium,
            g => g.Grade < Grade.High,
            g => g.Other == Grade.Medium,
            g => g.Other > Grade.Low,
        ];
        foreach (Expression<Func<Graded, bool>> where in predicates)
        {
            List<int> ids;
            bool exact;
            switch (format)
            {
                case "parquet":
                {
                    using var file = ParquetFile.Open(path);
                    using var reader = file.OpenReader(where);
                    (ids, exact) = ([.. reader.ReadRows().Select(g => g.Id)], reader.FiltersExactly);
                    break;
                }
                case "ipc":
                {
                    using var file = IpcFile.Open(path);
                    using var reader = file.OpenReader(where);
                    (ids, exact) = ([.. reader.ReadRows().Select(g => g.Id)], reader.FiltersExactly);
                    break;
                }
                default:
                {
                    using var reader = JsonReader<Graded>.Open(path, where);
                    (ids, exact) = ([.. reader.ReadRows().Select(g => g.Id)], reader.FiltersExactly);
                    break;
                }
            }
            Assert.True(exact, $"{format}: {where}");
            Assert.True(rows.Where(where.Compile()).Select(g => g.Id).SequenceEqual(ids), $"{format}: {where}");
        }
    }

    /// <summary>
    /// JSON takes an enum's names in any case and its numbers (as .NET's JSON serializers write them); the reader
    /// reads them as the member names, so a pushed comparison of names finds every row that is the member.
    /// </summary>
    [Fact]
    public void JsonEnumsInAnyCaseOrAsNumbers_AreFilteredExactly()
    {
        string path = Path.Combine(_root, "lenient.json");
        File.WriteAllText(path, string.Join("\n",
            "{\"Id\":1,\"Grade\":\"Mid\",\"Other\":null}",
            "{\"Id\":2,\"Grade\":\"mid\",\"Other\":\"HIGH\"}",
            "{\"Id\":3,\"Grade\":5,\"Other\":\"9\"}",
            "{\"Id\":4,\"Grade\":\" medium \",\"Other\":1}",
            "{\"Id\":5,\"Grade\":\"Low\",\"Other\":\"low\"}",
            "{\"Id\":6,\"Grade\":9,\"Other\":null}"));
        using (var reader = JsonReader<Graded>.Open(path, g => g.Grade == Grade.Mid))
        {
            Assert.True(reader.FiltersExactly);
            Assert.Equal([1, 2, 3, 4], reader.ReadRows().Select(g => g.Id));
        }
        using (var reader = JsonReader<Graded>.Open(path, g => g.Other > Grade.Low))
        {
            Assert.True(reader.FiltersExactly);
            Assert.Equal([2, 3], reader.ReadRows().Select(g => g.Id));
        }
        Assert.Equal([Grade.Mid, Grade.Mid, Grade.Mid, Grade.Mid, Grade.Low, Grade.High], JsonReader.ReadList<Graded>(path).Select(g => g.Grade));

        // A value no member has is a mismatch, a number too (an enum column holds members only).
        File.WriteAllText(path, "{\"Id\":1,\"Grade\":7,\"Other\":null}");
        Assert.Contains("is not a member of the enum", Assert.ThrowsAny<Exception>(() => JsonReader.ReadList<Graded>(path)).Message);
        File.WriteAllText(path, "{\"Id\":1,\"Grade\":\"Mild\",\"Other\":null}");
        Assert.Contains("is not a member of the enum", Assert.ThrowsAny<Exception>(() => JsonReader.ReadList<Graded>(path)).Message);
    }
}
