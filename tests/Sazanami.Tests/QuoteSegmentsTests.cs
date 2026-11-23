using System.Text;
using Kaminari.Sazanami.Text;

namespace Sazanami.Tests;

/// <summary>
/// <see cref="QuoteSegments"/> against a byte-by-byte reference of where records start (every quote toggles, line ends
/// outside quotes end records, CRLF is one line end, comment lines' quotes do not count): the parallel cut at every
/// segment size, the stream cut over random buffer splits, the in-order cut with comment lines.
/// QUOTE_SEGMENTS_CASES raises the random case count (the C1d gate runs 200k).
/// </summary>
public class QuoteSegmentsTests
{
    private static readonly int Cases = int.TryParse(Environment.GetEnvironmentVariable("QUOTE_SEGMENTS_CASES"), out int n) ? n : 3000;

    /// <summary>Every record start (positions before the end) by the rules, one byte at a time: the specification.</summary>
    internal static SortedSet<int> Reference(byte[] d, RecordRules rules)
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

    /// <summary>Random CSV text: plain and quoted fields (separators, line ends and doubled quotes inside), blank lines, mixed line ends, comments, stray quotes.</summary>
    internal static byte[] RandomCsv(Random r, RecordRules rules)
    {
        var sb = new StringBuilder();
        char q = rules.Quote is { } quote ? (char)quote : '"';
        string[] ends = rules.LineEnd is { } e ? [((char)e).ToString()] : ["\n", "\r\n", "\r", "\n", "\r\n"];
        for (int line = r.Next(0, 12); line > 0; line--)
        {
            if (rules.CommentPrefix is { } p && r.Next(6) == 0)
                sb.Append(Encoding.ASCII.GetString(p)).Append(r.Next(2) == 0 ? $"note {q} odd" : $"{q}x{q}{q}");
            else if (r.Next(8) != 0)
                for (int field = r.Next(1, 5); field > 0; field--)
                {
                    int kind = r.Next(10);
                    if (kind < 5) sb.Append(Pick(r, "abc 1.5 x"));
                    else if (kind < 9 && rules.Quote is not null) sb.Append(q).Append(Pick(r, $"a,b{q}{q}c\n\r\r\n d")).Append(q);
                    else sb.Append(Pick(r, "ab")).Append(q).Append(Pick(r, "cd")); // a stray quote: malformed
                    if (field > 1) sb.Append(',');
                }
            if (line > 1 || r.Next(2) == 0) sb.Append(ends[r.Next(ends.Length)]);
        }
        return Encoding.ASCII.GetBytes(sb.ToString());

        static string Pick(Random r, string pool) => new([.. Enumerable.Range(0, r.Next(0, 6)).Select(_ => pool[r.Next(pool.Length)])]);
    }

    private static RecordRules RandomRules(Random r, bool comments) => new(
        r.Next(6) switch { 0 => null, 1 => (byte)'\'', _ => (byte)'"' },
        r.Next(5) == 0 ? (byte)'|' : null,
        comments ? r.Next(2) == 0 ? "#"u8.ToArray() : "//"u8.ToArray() : null);

    [Fact]
    public unsafe void ParallelCut_EverySegmentSize_StartsAreRecordStarts()
    {
        var r = new Random(1);
        var parallel = new ParallelOptions { MaxDegreeOfParallelism = 4 };
        for (int i = 0; i < Cases; i++)
        {
            RecordRules rules = RandomRules(r, false);
            byte[] d = RandomCsv(r, rules);
            SortedSet<int> reference = Reference(d, rules);
            fixed (byte* p = d)
                foreach (int segment in (int[])[1, 2, 3, 5, 8, 13, 64, r.Next(1, 200)])
                {
                    long[] starts = QuoteSegments.Cut(p, d.Length, 0, false, segment, rules, parallel);
                    Assert.Equal(0, starts[0]);
                    Assert.Equal(d.Length, starts[^1]);
                    for (int k = 1; k < starts.Length; k++)
                    {
                        Assert.True(starts[k] >= starts[k - 1], $"decreasing at {k}: {Show(d)}");
                        Assert.True(starts[k] == d.Length || reference.Contains((int)starts[k]), $"segment {segment}: {starts[k]} is no record start in {Show(d)} ({rules})");
                    }
                    // One-byte segments find every record start but one right after byte 0 (segment 0 starts at first).
                    if (segment == 1) Assert.Equal(reference.Where(static s => s > 1), starts.Skip(1).Where(s => s < d.Length).Distinct().Select(static s => (int)s));
                }
        }
    }

    [Fact]
    public void FirstRecordStart_BothStates()
    {
        byte[] d = "a\"b\nc\nd"u8.ToArray();
        Assert.True(QuoteSegments.QuoteParity(d, RecordRules.Default));
        Assert.Equal(-1, QuoteSegments.FirstRecordStart(d, 0, d.Length, false, RecordRules.Default)); // the quote opens: both newlines inside
        Assert.Equal(4, QuoteSegments.FirstRecordStart(d, 0, d.Length, true, RecordRules.Default)); // the quote closes: the newline after b ends it
        Assert.Equal(3, QuoteSegments.FirstRecordStart("x\r\ny"u8, 0, 2, false, RecordRules.Default)); // a CR at the boundary peeks at the LF
        Assert.Equal(-1, QuoteSegments.FirstRecordStart("abc"u8, 0, 3, false, RecordRules.Default));
        Assert.False(QuoteSegments.QuoteParity("a\"b\""u8, RecordRules.Default));
        Assert.False(QuoteSegments.QuoteParity("a\"b"u8, RecordRules.Default with { Quote = null }));
    }

    [Fact]
    public void StreamCut_RandomBuffers_StartsAreRecordStarts()
    {
        var r = new Random(2);
        for (int i = 0; i < Cases; i++)
        {
            RecordRules rules = RandomRules(r, false);
            byte[] d = RandomCsv(r, rules);
            SortedSet<int> reference = Reference(d, rules);
            int at = 0, read = 0;
            while (read < d.Length)
            {
                read = Math.Min(d.Length, read + r.Next(1, 12));
                bool final = read == d.Length;
                int cut = QuoteSegments.LastRecordStart(d.AsSpan(at, read - at), false, final, rules);
                if (cut == 0) continue; // no complete record yet: read on
                Assert.True(at + cut == d.Length || reference.Contains(at + cut), $"{at + cut} is no record start in {Show(d)} ({rules})");
                at += cut;
            }
        }
    }

    [Fact]
    public unsafe void InOrderCut_CommentLines_EveryStart()
    {
        var r = new Random(3);
        for (int i = 0; i < Cases; i++)
        {
            RecordRules rules = RandomRules(r, r.Next(3) != 0);
            byte[] d = RandomCsv(r, rules);
            SortedSet<int> reference = Reference(d, rules);
            fixed (byte* p = d)
            {
                long[] starts = QuoteSegments.InOrder(p, d.Length, 0, 1, rules); // unit size 1: every record start
                Assert.Equal(((long[])[0, .. reference.Where(static s => s > 0).Select(static s => (long)s), d.Length]).Distinct(), starts.Distinct());
            }
            // A stream of random pieces with comment lines: Scan from each cut.
            int at = 0, read = 0;
            while (read < d.Length)
            {
                read = Math.Min(d.Length, read + r.Next(1, 12));
                int last = QuoteSegments.Scan(d.AsSpan(at, read - at), read == d.Length, int.MaxValue, rules, out _);
                if (last == 0) continue;
                Assert.True(at + last == d.Length || reference.Contains(at + last), $"{at + last} is no record start in {Show(d)} ({rules})");
                at += last;
            }
        }
    }

    [Fact]
    public unsafe void Cut_RefusesCommentPrefix()
    {
        byte[] d = "a\n"u8.ToArray();
        Assert.Throws<ArgumentException>(() => { fixed (byte* p = d) QuoteSegments.Cut(p, d.Length, 0, false, 1, RecordRules.Default with { CommentPrefix = "#"u8.ToArray() }, new ParallelOptions()); });
    }

    /// <summary>A quoted field longer than the segments: the segments inside it are empty units.</summary>
    [Fact]
    public unsafe void LongQuotedField_EmptyUnits()
    {
        byte[] d = Encoding.ASCII.GetBytes("a,\"" + string.Join("\n", Enumerable.Repeat("xxxxxxxx", 50)) + "\"\nb\n");
        fixed (byte* p = d)
        {
            long[] starts = QuoteSegments.Cut(p, d.Length, 0, false, 16, RecordRules.Default, new ParallelOptions());
            Assert.Equal([0L, d.Length - 2, d.Length], starts.Distinct());
        }
    }

    private static string Show(byte[] d) => Encoding.ASCII.GetString(d).Replace("\r", "\\r").Replace("\n", "\\n");
}
