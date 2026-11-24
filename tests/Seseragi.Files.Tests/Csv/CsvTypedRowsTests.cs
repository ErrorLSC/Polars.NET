using System.Text;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Csv;

namespace Seseragi.Files.Tests.Csv;

/// <summary>
/// <see cref="CsvReader{T}"/> and <see cref="CsvWriter{T}"/>: rows written read back (every scalar member kind), the text
/// written (GUIDs, enums, decimals, time spans), headers matched by name (in another case, extra columns passed over, a
/// missing member refused by name), files without a header, nulls, predicates evaluated by the reader, members CSV
/// cannot hold refused.
/// </summary>
public sealed class CsvTypedRowsTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("seseragi-csv-typed").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private string Path(string name) => System.IO.Path.Combine(_dir, name);

    public enum Tint { Red, Green, Blue }

    public sealed record Everything(int Id, long? Big, string? Name, double Score, float Ratio, decimal Price, bool Flag, DateTime When,
        DateTimeOffset At, DateOnly Day, TimeOnly Clock, TimeSpan Span, Guid Key, Tint Color, Tint? Maybe, byte Small, short Mid, char Letter,
        Half Tiny, Int128 Huge, decimal? Discount);

    public sealed record Person(int Id, string? Name, int? Age);

    public sealed record Nested(int Id, List<int> Values);

    public sealed record Blob(int Id, byte[] Bytes);

    private static Everything[] Rows(int count) =>
    [
        .. Enumerable.Range(0, count).Select(i => new Everything(
            i, i % 3 == 0 ? null : i * 1_000_000_007L, i % 5 == 0 ? null : $"name \"{i}\", here", i / 7.0, i / 3f, 12.5m + i, i % 2 == 0,
            new DateTime(2024, 1, 1, 0, 0, 0).AddSeconds(i * 3601.25), new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero).AddMinutes(i),
            new DateOnly(2024, 1, 1).AddDays(i), new TimeOnly(i % 24, i % 60, i % 60), TimeSpan.FromMilliseconds(i * 1500.5) - TimeSpan.FromHours(i % 2),
            new Guid(i, 2, 3, [4, 5, 6, 7, 8, 9, 10, (byte)i]), (Tint)(i % 3), i % 4 == 0 ? null : (Tint)((i + 1) % 3), (byte)i, (short)-i,
            (char)('a' + i % 26), (Half)(i / 4.0), (Int128)i * long.MaxValue, i % 6 == 0 ? null : i / 100m)),
    ];

    private static byte[] Written<T>(IEnumerable<T> rows, CsvWriteOptions? options = null)
    {
        using var stream = new MemoryStream();
        using (CsvWriter<T> writer = CsvWriter<T>.Create(stream, (options ?? new()) with { LeaveOpen = true }))
            writer.WriteRows(rows);
        return stream.ToArray();
    }

    // ---------------------------------------------------------------- round trips

    [Fact]
    public async Task RowsWritten_ReadBack_FromEveryInput()
    {
        Everything[] rows = Rows(3000);
        string path = Path("everything.csv");
        using (CsvWriter<Everything> writer = CsvWriter<Everything>.Create(path)) writer.WriteRows(rows);
        byte[] bytes = await File.ReadAllBytesAsync(path);
        Assert.Equal(rows, CsvReader.ReadList<Everything>(path));
        Assert.Equal(rows, await CsvReader.ReadListAsync<Everything>(path));
        using (CsvReader<Everything> reader = CsvReader<Everything>.Open(bytes.AsMemory())) Assert.Equal(rows, reader.ReadRows());
        using (CsvReader<Everything> reader = CsvReader<Everything>.Open(new MemoryStream(bytes))) Assert.Equal(rows, reader.ReadRows());
        await using (CsvReader<Everything> reader = await CsvReader<Everything>.OpenAsync(new MemoryStream(bytes)))
        {
            List<Everything> read = [];
            await foreach (Everything row in reader.ReadRowsAsync()) read.Add(row);
            Assert.Equal(rows, read);
        }
        // One row at a time and asynchronously write the same bytes.
        using var one = new MemoryStream();
        await using (CsvWriter<Everything> writer = CsvWriter<Everything>.Create(one, new CsvWriteOptions { LeaveOpen = true }))
        {
            foreach (Everything row in rows.Take(10)) writer.WriteRow(row);
            await writer.WriteRowsAsync(rows.Skip(10));
        }
        Assert.Equal(bytes, one.ToArray());
    }

    [Fact]
    public void Values_AreWrittenAsPeopleReadThem()
    {
        Everything row = Rows(2)[1] with { Name = "plain", Price = 1.5m, Discount = 2m, Span = TimeSpan.FromSeconds(-1.5), Color = Tint.Blue, Maybe = null };
        string[] lines = Encoding.UTF8.GetString(Written([row])).Split('\n');
        Assert.Equal("Id,Big,Name,Score,Ratio,Price,Flag,When,At,Day,Clock,Span,Key,Color,Maybe,Small,Mid,Letter,Tiny,Huge,Discount", lines[0]);
        string[] fields = lines[1].Split(',');
        Assert.Equal("1.5", fields[5]);            // decimals: the fewest digits that hold the value
        Assert.Equal("2", fields[^1]);
        Assert.Equal("-PT1.5S", fields[11]);       // time spans: ISO 8601
        Assert.Equal("00000001-0002-0003-0405-060708090a01", fields[12]);
        Assert.Equal("Blue", fields[13]);          // enums: their names
        Assert.Equal("", fields[14]);
    }

    // ---------------------------------------------------------------- headers

    [Fact]
    public void Headers_AreMatchedByName_InAnyCase_ExtraColumnsPassedOver()
    {
        string csv = "AGE,notes,id,NAME\n42,x,1,Ann\n,y,2,\n";
        List<Person> people = CsvReader.ReadList<Person>(Write("people.csv", csv));
        Assert.Equal([new Person(1, "Ann", 42), new Person(2, null, null)], people);
        using CsvReader<Person> reader = CsvReader<Person>.Open(Encoding.UTF8.GetBytes(csv).AsMemory());
        Assert.Equal(["Id", "Name", "Age"], reader.Schema.FieldsList.Select(f => f.Name));
    }

    [Fact]
    public void AMissingMember_IsRefused_ByName()
    {
        var e = Assert.Throws<CsvFormatException>(() => CsvReader.ReadList<Person>(Write("lacking.csv", "id,name\n1,Ann\n")));
        Assert.Contains("'Age'", e.Message);
        Assert.Contains("id, name", e.Message);
        // A column present with every value empty is nulls.
        Assert.Equal([new Person(1, "Ann", null)], CsvReader.ReadList<Person>(Write("empty-age.csv", "id,name,age\n1,Ann,\n")));
    }

    [Fact]
    public void TwoColumnsNamingOneMember_AreRefused()
    {
        var e = Assert.Throws<CsvFormatException>(() => CsvReader.ReadList<Person>(Write("twice.csv", "ID,id2,iD,name,age\n1,2,3,a,4\n")));
        Assert.Contains("'ID'", e.Message);
        Assert.Contains("'iD'", e.Message);
    }

    [Fact]
    public void WithoutAHeader_MembersAreTheColumnsInOrder()
    {
        List<Person> people = CsvReader.ReadList<Person>(Write("plain.csv", "1,Ann,42\n2,Bob,\n"), options: new CsvReadOptions { HasHeader = false });
        Assert.Equal([new Person(1, "Ann", 42), new Person(2, "Bob", null)], people);
    }

    // ---------------------------------------------------------------- values

    [Fact]
    public void Enums_ReadFromNames_InAnyCase_OrNumbers()
    {
        string path = Write("tints.csv", "Id,Color\n1,red\n2,GREEN\n3,2\n");
        Assert.Equal([Tint.Red, Tint.Green, Tint.Blue], CsvReader.ReadList<Painted>(path).Select(p => p.Color));
        var e = Assert.Throws<CsvValueException>(() => CsvReader.ReadList<Painted>(Write("bad-tint.csv", "Id,Color\n1,purple\n")));
        Assert.Contains("purple", e.Message);
    }

    public sealed record Painted(int Id, Tint Color);

    public sealed record Timed(Guid Key, TimeSpan Span);

    [Fact]
    public void GuidsAndTimeSpans_ReadInTheirUsualForms()
    {
        Guid key = Guid.Parse("0f8fad5b-d9cb-469f-a165-70867728950e");
        string path = Write("timed.csv", $"Key,Span\n{key},PT1.5S\n{key:N},1.02:03:04.5\n{{{key}}},-00:00:01\n");
        Assert.Equal([new Timed(key, TimeSpan.FromSeconds(1.5)), new Timed(key, new TimeSpan(1, 2, 3, 4, 500)), new Timed(key, TimeSpan.FromSeconds(-1))],
            CsvReader.ReadList<Timed>(path));
    }

    [Fact]
    public void AnEmptyField_IsRefused_ByAMemberThatTakesNoNull()
    {
        Assert.ThrowsAny<Exception>(() => CsvReader.ReadList<Painted>(Write("no-id.csv", "Id,Color\n,Red\n")));
    }

    // ---------------------------------------------------------------- predicates

    [Fact]
    public void APredicate_IsEvaluatedByTheReader()
    {
        Everything[] rows = Rows(5000);
        byte[] bytes = Written(rows);
        int limit = 4000;
        using CsvReader<Everything> reader = CsvReader<Everything>.Open(bytes.AsMemory(), r => r.Id >= limit && r.Name != null && r.Color == Tint.Green);
        Assert.True(reader.FiltersExactly);
        Assert.Equal(rows.Where(r => r.Id >= limit && r.Name != null && r.Color == Tint.Green), reader.ReadRows());
        // MaxRows counts the rows kept.
        using CsvReader<Everything> first = CsvReader<Everything>.Open(bytes.AsMemory(), r => r.Flag, new CsvReadOptions { MaxRows = 7 });
        Assert.Equal(rows.Where(r => r.Flag).Take(7), first.ReadRows());
    }

    // ---------------------------------------------------------------- refusals

    [Fact]
    public void MembersCsvCannotHold_AreRefused_ByName()
    {
        var nested = Assert.Throws<NotSupportedException>(() => CsvWriter<Nested>.Create(new MemoryStream()));
        Assert.Contains("'Values'", nested.Message);
        Assert.Contains("JSON or Parquet", nested.Message);
        var blob = Assert.Throws<NotSupportedException>(() => CsvReader<Blob>.Open(Encoding.UTF8.GetBytes("Id,Bytes\n1,x\n").AsMemory()));
        Assert.Contains("'Bytes'", blob.Message);
        Assert.Throws<ArgumentException>(() => CsvReader<Person>.Open(Encoding.UTF8.GetBytes("id\n").AsMemory(), options: new CsvReadOptions { RowIndexColumn = "row" }));
    }

    // ---------------------------------------------------------------- datasets

    [Fact]
    public async Task DatasetRows_RoundTrip_PartitionedByAnEnum()
    {
        Everything[] rows = Rows(5000);
        var options = new CsvDatasetWriteOptions { PartitionBy = ["Color"] };
        DatasetWriteResult<DatasetWrittenFile> result = CsvDatasetWriter.WriteRows(Path("t"), rows, options);
        Assert.Equal(3, result.Files.Count);
        Assert.Equal(rows.OrderBy(r => r.Id), CsvDataset.ReadList<Everything>(Path("t")).OrderBy(r => r.Id));
        Assert.Equal(rows.OrderBy(r => r.Id), (await CsvDataset.ReadListAsync<Everything>(Path("t"))).OrderBy(r => r.Id));
        using CsvDataset dataset = CsvDataset.Open(Path("t"));
        // A predicate on the enum partition compares its names (pushed; files of other colors not read).
        using CsvDatasetReader<Everything> few = dataset.OpenReader<Everything>(r => r.Id < 300 && r.Color != Tint.Green && r.Name != null);
        Assert.True(few.FiltersExactly);
        Assert.Equal(rows.Where(r => r.Id < 300 && r.Color != Tint.Green && r.Name != null).Select(r => r.Id).Order(), few.ReadRows().Select(r => r.Id).Order());
        // Written row by row, asynchronously: the same files' rows.
        await using (CsvDatasetWriter<Everything> writer = await CsvDatasetWriter<Everything>.CreateAsync(Path("u"), options))
        {
            foreach (Everything row in rows.Take(10)) writer.WriteRow(row);
            await writer.WriteRowsAsync(rows.Skip(10));
            await writer.CompleteAsync();
        }
        Assert.Equal(rows.OrderBy(r => r.Id), CsvDataset.ReadList<Everything>(Path("u")).OrderBy(r => r.Id));
    }

    [Fact]
    public void DatasetHeaders_AreMatchedPerFile_InAnyCase()
    {
        Directory.CreateDirectory(Path("mixed"));
        Write("mixed/a.csv", "id,name,age,notes\n1,Ann,42,x\n");
        Write("mixed/b.csv", "AGE,NAME,ID\n7,Bob,2\n");
        Write("mixed/c.csv", "Id,Name,Age,extra\n3,,,y\n");
        Assert.Equal([new Person(1, "Ann", 42), new Person(2, "Bob", 7), new Person(3, null, null)],
            CsvDataset.ReadList<Person>(Path("mixed")).OrderBy(p => p.Id));
        // A file lacking a member is refused, naming the file and the member.
        Write("mixed/d.csv", "id,name\n4,Dan\n");
        var e = Assert.ThrowsAny<Exception>(() => CsvDataset.ReadList<Person>(Path("mixed")));
        Assert.Contains("d.csv", e.ToString());
        Assert.Contains("Age", e.ToString());
    }

    private string Write(string name, string text)
    {
        string path = Path(name);
        File.WriteAllText(path, text);
        return path;
    }
}
