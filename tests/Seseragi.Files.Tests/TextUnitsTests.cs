using System.Text;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Text;
using Kaminari.Seseragi.Files.IO;

namespace Seseragi.Files.Tests;

/// <summary>
/// CSV records through the shared unit inputs (<see cref="TextUnits.RecordsAsync"/>): blocks that find their own starts,
/// planned starts in memory and in a mapped file, a ranged source read in order, a stream, compressed input — units
/// concatenate to the input and every boundary is a record start of a byte-by-byte reference.
/// </summary>
public class TextUnitsTests
{
    private static readonly int Cases = int.TryParse(Environment.GetEnvironmentVariable("TEXT_UNITS_CASES"), out int n) ? n : 400;

    private static RecordCut Cut(RecordRules rules) => new(rules, static start => new InvalidDataException($"record at {start}"), static e => new InvalidDataException("damaged", e));

    [Fact]
    public async Task RecordUnits_EverySource_BoundariesAreRecordStarts()
    {
        var r = new Random(5);
        string file = Path.Combine(Path.GetTempPath(), $"text-units-{Guid.NewGuid():N}.csv");
        try
        {
            for (int i = 0; i < Cases; i++)
            {
                RecordRules rules = new(
                    r.Next(4) switch { 0 => null, _ => (byte)'"' },
                    r.Next(5) == 0 ? (byte)'|' : null,
                    r.Next(5) == 0 ? "#"u8.ToArray() : null);
                byte[] d = RandomCsv(r, rules);
                SortedSet<int> reference = Reference(d, rules);
                int unit = r.Next(2) == 0 ? r.Next(1, 16) : r.Next(16, 300);
                await File.WriteAllBytesAsync(file, d);
                foreach (string kind in (string[])["memory", "mapped", "ranged", "stream"])
                {
                    using RandomAccessSource? local = kind == "mapped" ? new RandomAccessSource(File.OpenRead(file), false, null, mappable: true) : null;
                    using TextUnits units = kind switch
                    {
                        "memory" => await TextUnits.RecordsAsync(new MemoryRandomAccess(d), false, unit, Cut(rules), 4, true, default),
                        "mapped" => await TextUnits.RecordsAsync(local!, true, unit, Cut(rules), 4, true, default),
                        "ranged" => await TextUnits.RecordsAsync(new Ranged(d), false, unit, Cut(rules), 4, true, default),
                        _ => new StreamInput(new MemoryStream(d), false, unit, Cut(rules)),
                    };
                    await Check(units, d, reference, $"{kind} unit {unit} {rules} {Show(d)}");
                }
            }
        }
        finally { File.Delete(file); }
    }

    /// <summary>Compressed CSV is read in order, whatever the rules.</summary>
    [Fact]
    public async Task CompressedRecords_ReadInOrder()
    {
        byte[] d = Encoding.ASCII.GetBytes(string.Concat(Enumerable.Range(0, 2000).Select(static i => $"{i},\"a\nb\",x\r\n")));
        var packed = new MemoryStream();
        using (var gz = new System.IO.Compression.GZipStream(packed, System.IO.Compression.CompressionLevel.Fastest, leaveOpen: true)) gz.Write(d);
        using TextUnits units = await TextUnits.RecordsAsync(new MemoryRandomAccess(packed.ToArray()), false, 997, Cut(RecordRules.Default), 4, true, default);
        Assert.IsType<StreamInput>(units);
        await Check(units, d, Reference(d, RecordRules.Default), "gzip");
    }

    [Fact]
    public async Task Inputs_ByRules()
    {
        byte[] d = "a,b\n1,2\n"u8.ToArray();
        using TextUnits plain = await TextUnits.RecordsAsync(new MemoryRandomAccess(d), false, 4, Cut(RecordRules.Default with { Quote = null }), 1, false, default);
        Assert.IsType<BlockInput>(plain);
        using TextUnits quoted = await TextUnits.RecordsAsync(new MemoryRandomAccess(d), false, 4, Cut(RecordRules.Default), 1, false, default);
        Assert.IsType<PlannedInput>(quoted);
        using TextUnits ranged = await TextUnits.RecordsAsync(new Ranged(d), false, 4, Cut(RecordRules.Default), 1, false, default);
        Assert.IsType<StreamInput>(ranged);
    }

    /// <summary>A CRLF across a block boundary is one line end (blocks without quotes find their own starts).</summary>
    [Fact]
    public void RecordCut_Next_PairsCrlf()
    {
        RecordCut cut = Cut(RecordRules.Default with { Quote = null });
        Assert.Equal(4, cut.Next("ab\r\ncd"u8, 3, false)); // the CR is the limit's last byte: the LF after it is peeked, CRLF is one end
        Assert.Equal(-2, cut.Next("ab\r"u8, 3, false)); // nothing to peek, more input to come
        Assert.Equal(3, cut.Next("ab\r"u8, 3, true));
        Assert.Equal(4, cut.Next("ab\r\n"u8, 4, true));
        Assert.Equal(-1, cut.Next("abc\n"u8, 3, true)); // the LF lies past the limit
        Assert.Throws<InvalidOperationException>(() => Cut(RecordRules.Default).Next("a\n"u8, 2, true));
    }

    private static async Task Check(TextUnits units, byte[] d, SortedSet<int> reference, string what)
    {
        var all = new List<byte>();
        for (int i = 0; ; i++)
        {
            if (units.Count >= 0 && i >= units.Count) break;
            using TextUnitBytes? bytes = await units.GetAsync(i, true, default);
            if (bytes is null) break;
            int at = all.Count;
            if (bytes.Memory.Length > 0) Assert.True(at == 0 || reference.Contains(at), $"unit {i} starts at {at}, no record start: {what}");
            all.AddRange(bytes.Memory.ToArray());
            units.Release(i);
        }
        Assert.True(d.AsSpan().SequenceEqual([.. all]), $"units do not make the input: {what}");
    }

    /// <summary>A random-access source that is neither memory nor a file: read at offsets.</summary>
    private sealed class Ranged(byte[] bytes) : IRandomAccess
    {
        public long Length => bytes.Length;

        public int ReadAt(long offset, Span<byte> destination)
        {
            int n = (int)Math.Min(destination.Length, bytes.Length - offset);
            bytes.AsSpan((int)offset, n).CopyTo(destination);
            return n;
        }

        public ValueTask<int> ReadAtAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken) => ValueTask.FromResult(ReadAt(offset, destination.Span));
    }

    /// <summary>Every record start, one byte at a time (the rules of QuoteSegments).</summary>
    private static SortedSet<int> Reference(byte[] d, RecordRules rules)
    {
        SortedSet<int> starts = d.Length > 0 ? [0] : [];
        bool inside = false, lineStart = true;
        byte[] prefix = rules.CommentPrefix ?? [];
        for (int i = 0; i < d.Length;)
        {
            if (lineStart && prefix.Length > 0 && d.AsSpan(i).StartsWith(prefix))
            {
                while (i < d.Length && !LineEnd(d[i])) i++;
                if (i == d.Length) break;
                i = After(i);
                continue;
            }
            lineStart = false;
            byte b = d[i];
            if (rules.Quote is { } q && b == q)
            {
                inside = !inside;
                i++;
            }
            else if (!inside && LineEnd(b)) i = After(i);
            else i++;
        }
        return starts;

        bool LineEnd(byte b) => rules.LineEnd is { } e ? b == e : b is (byte)'\n' or (byte)'\r';

        int After(int i)
        {
            byte b = d[i++];
            if (rules.LineEnd is null && b == '\r' && i < d.Length && d[i] == '\n') i++;
            if (i < d.Length) starts.Add(i);
            lineStart = true;
            return i;
        }
    }

    private static byte[] RandomCsv(Random r, RecordRules rules)
    {
        var sb = new StringBuilder();
        string[] ends = rules.LineEnd is { } e ? [((char)e).ToString()] : ["\n", "\r\n", "\r", "\n"];
        for (int line = r.Next(0, 30); line > 0; line--)
        {
            if (rules.CommentPrefix is { } p && r.Next(6) == 0) sb.Append(Encoding.ASCII.GetString(p)).Append("note \" odd");
            else if (r.Next(8) != 0)
                for (int field = r.Next(1, 5); field > 0; field--)
                {
                    if (r.Next(2) == 0 || rules.Quote is null) sb.Append(Pick(r, "abc 1.5"));
                    else sb.Append('"').Append(Pick(r, "a,b\"\"c\n\r\r\n d")).Append('"');
                    if (field > 1) sb.Append(',');
                }
            if (line > 1 || r.Next(2) == 0) sb.Append(ends[r.Next(ends.Length)]);
        }
        return Encoding.ASCII.GetBytes(sb.ToString());

        static string Pick(Random r, string pool) => new([.. Enumerable.Range(0, r.Next(0, 8)).Select(_ => pool[r.Next(pool.Length)])]);
    }

    private static string Show(byte[] d) => Encoding.ASCII.GetString(d).Replace("\r", "\\r").Replace("\n", "\\n");
}
