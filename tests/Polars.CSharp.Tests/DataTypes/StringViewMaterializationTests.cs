using Polars.CSharp.Linq;

namespace Polars.CSharp.Tests.DataTypes;

/// <summary>
/// Materializing string columns (Arrow Utf8View) into objects, including the accessor's cache of decoded strings
/// by view: repeated values, inlined and referenced strings, look-alike views, and columns of unique values that
/// switch the cache off part-way through.
/// </summary>
public class StringViewMaterializationTests
{
    public class Row
    {
        public long Id { get; set; }
        public string S { get; set; }
    }

    private static void AssertRoundTrip(Row[] rows)
    {
        using var df = DataFrame.From(rows);
        var back = df.AsEnumerable<Row>().ToList();
        Assert.Equal(rows.Length, back.Count);
        for (int i = 0; i < rows.Length; i++)
        {
            Assert.Equal(rows[i].Id, back[i].Id);
            Assert.Equal(rows[i].S, back[i].S);
        }
    }

    [Fact]
    public void Repeated_Inlined_And_Referenced_Strings_Round_Trip()
    {
        string[] values =
        [
            "a", "ab", "abc", "abcd", "abcdefghijk", "abcdefghijkl", // up to 12 bytes: inlined
            "abcdefghijklm", "abcdXXXXXXXXXXXXXXXXX", "abcdYYYYYYYYYYYYYYYYY", // referenced; same prefix, same length
            "", "日本語の文字列", "Ünïcödé longer than twelve bytes", "Electronics", "Books",
        ];
        var rows = Enumerable.Range(0, 5000)
            .Select(i => new Row { Id = i, S = i % 17 == 0 ? null : values[(i * 7) % values.Length] })
            .ToArray();

        AssertRoundTrip(rows);
    }

    [Fact]
    public void Strings_Differing_Only_After_Their_End_Do_Not_Collide()
    {
        // Same length and prefix, different content: the cache must tell them apart.
        var rows = Enumerable.Range(0, 2000)
            .Select(i => new Row { Id = i, S = (i % 4) switch { 0 => "ab", 1 => "ac", 2 => "abcdefghijk", _ => "abcdefghijz" } })
            .ToArray();

        AssertRoundTrip(rows);
    }

    [Fact]
    public void Unique_Values_Then_Repeats_Round_Trip()
    {
        // More unique values than the cache's probation (it switches off), then values repeated again.
        var unique = Enumerable.Range(0, 10_000).Select(i => $"customer-{i:D8}-{(i * 31) % 997}").ToArray();
        var rows = unique
            .Concat(Enumerable.Range(0, 3000).Select(i => unique[i % 50]))
            .Concat(Enumerable.Range(0, 3000).Select(i => i % 2 == 0 ? "x" : "a longer repeated string value"))
            .Select((s, i) => new Row { Id = i, S = s })
            .ToArray();

        AssertRoundTrip(rows);
    }

    [Fact]
    public void Sliced_Frame_Materializes_Its_Own_Rows()
    {
        var rows = Enumerable.Range(0, 3000)
            .Select(i => new Row { Id = i, S = i % 3 == 0 ? $"long string number {i % 10} of the column" : $"s{i % 5}" })
            .ToArray();
        using var df = DataFrame.From(rows);
        using var slice = df.Slice(1234, 1000);

        var back = slice.AsEnumerable<Row>().ToList();
        Assert.Equal(rows.Skip(1234).Take(1000).Select(r => (r.Id, r.S)), back.Select(r => (r.Id, r.S)));
    }

    [Fact]
    public void Join_Repeating_Dimension_Strings_Round_Trips()
    {
        // A dimension's strings repeated by a join share their views.
        var dims = Enumerable.Range(0, 40).Select(i => new Row { Id = i, S = i % 2 == 0 ? $"category {i}" : $"c{i}" }).ToArray();
        var facts = Enumerable.Range(0, 20_000).Select(i => new Row { Id = (i * 13) % 40, S = null }).ToArray();
        using var ddf = DataFrame.From(dims);
        using var fdf = DataFrame.From(facts);

        var joined = (from f in fdf.AsQueryable<Row>()
                      join d in ddf.AsQueryable<Row>() on f.Id equals d.Id
                      select new Row { Id = f.Id, S = d.S }).ToList();
        var expected = (from f in facts join d in dims on f.Id equals d.Id select new Row { Id = f.Id, S = d.S }).ToList();

        Assert.Equal(expected.Select(r => (r.Id, r.S)), joined.Select(r => (r.Id, r.S)));
    }
}
