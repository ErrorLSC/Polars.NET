namespace Kaminari.Sazanami.Text;

/// <summary>How CSV records are delimited.</summary>
/// <param name="Quote">The quote byte (null: no quoting); every quote byte toggles "inside quotes".</param>
/// <param name="LineEnd">The line end byte, or null for LF, CR or CRLF (one line end).</param>
/// <param name="CommentPrefix">The bytes starting a comment line (at a record start, outside quotes; any length), or null.</param>
internal readonly record struct RecordRules(byte? Quote, byte? LineEnd, byte[]? CommentPrefix)
{
    /// <summary>'"' quotes, LF / CR / CRLF line ends, no comments (<c>new RecordRules()</c> would quote nothing).</summary>
    public static RecordRules Default => new((byte)'"', null, null);
}

/// <summary>
/// Where CSV records start, so an input can be cut into units decoded side by side exactly as a sequential reader reads
/// it. A record starts at the first byte and right after every line end outside quotes (a blank line is a record);
/// every quote byte toggles "inside quotes" — the engine's line splitter's rule (a quote in an unquoted field toggles too,
/// and the tokenizer refuses that field, so a cut stays right up to the first malformed byte). Without comment lines the
/// cut is parallel: each segment's quote parity is counted side by side (the one pass over the input), the states
/// follow by prefix XOR, then each segment scans from its state to its first record start (a short scan). With a comment
/// prefix (a comment line's quotes do not count) the same pass finds where the prefix starts a line: the cut is in order
/// only up to the last of them, parallel after it (<see cref="CommentCut"/>).
/// </summary>
internal static class QuoteSegments
{
    /// <summary>Whether <paramref name="data"/> holds an odd count of quote bytes (the state flips across it).</summary>
    public static bool QuoteParity(ReadOnlySpan<byte> data, RecordRules rules) => rules.Quote is { } q && (data.Count(q) & 1) == 1;

    /// <summary>
    /// The first record start after <paramref name="from"/> for the state <paramref name="inside"/> there, relative to it;
    /// -1 when no line end lies in [from, to). A CR at to - 1 peeks at <c>data[to]</c> for an LF. No comment prefix.
    /// </summary>
    public static int FirstRecordStart(ReadOnlySpan<byte> data, int from, int to, bool inside, RecordRules rules)
    {
        byte q = rules.Quote ?? 0;
        bool quoting = rules.Quote is not null;
        for (int at = from; at < to;)
        {
            // Inside quotes only the closing quote matters (line ends there are data).
            int k = inside ? data[at..to].IndexOf(q) : Find(data[at..to], quoting, q, rules.LineEnd);
            if (k < 0) return -1;
            at += k;
            byte b = data[at++];
            if (quoting && b == q) inside = !inside;
            else return End(data, at, b, rules) - from;
        }
        return -1;
    }

    /// <summary>The next byte that matters: a quote or a line end byte.</summary>
    private static int Find(ReadOnlySpan<byte> span, bool quoting, byte q, byte? lineEnd) => (quoting, lineEnd) switch
    {
        (true, null) => span.IndexOfAny(q, (byte)'\n', (byte)'\r'),
        (true, { } e) => span.IndexOfAny(q, e),
        (false, null) => span.IndexOfAny((byte)'\n', (byte)'\r'),
        (false, { } e) => span.IndexOf(e),
    };

    /// <summary>Where the record after a line end byte <paramref name="b"/> (just before <paramref name="at"/>) starts: after an LF following a CR.</summary>
    private static int End(ReadOnlySpan<byte> data, int at, byte b, RecordRules rules) =>
        rules.LineEnd is null && b == '\r' && at < data.Length && data[at] == '\n' ? at + 1 : at;

    /// <summary>
    /// Unit starts over <c>data[first..length]</c> cut about every <paramref name="segment"/> bytes: starts[0] =
    /// <paramref name="first"/> (a record start, in the state <paramref name="inQuotes"/>: true only when a caller cut
    /// mid-field on purpose), then each segment's first record start, the last = <paramref name="length"/>. Starts never
    /// decrease; equal neighbours make an empty unit (a quoted field longer than a segment). No comment prefix.
    /// </summary>
    public static unsafe long[] Cut(byte* data, long length, long first, bool inQuotes, int segment, RecordRules rules, ParallelOptions parallel)
    {
        if (rules.CommentPrefix is not null) throw new ArgumentException("A comment prefix needs the in-order cut.", nameof(rules));
        return Cut(data, length, first, inQuotes, segment, rules, parallel, null);
    }

    /// <summary>
    /// <see cref="Cut(byte*, long, long, bool, int, RecordRules, ParallelOptions)"/> with comment lines: one parallel
    /// pass counts each segment's quotes and finds the last place the prefix starts a line (right after a line end byte,
    /// or at <paramref name="first"/>). None: the parity cut over those counts. Else the cut is in order (comment lines
    /// as <see cref="Scan"/> reads them) up to the first record start after that line, parallel from there (no comment
    /// line can start later). <paramref name="first"/> is a record start outside quotes.
    /// </summary>
    public static unsafe long[] CommentCut(byte* data, long length, long first, int segment, int unitBytes, RecordRules rules, ParallelOptions parallel)
    {
        byte[] prefix = rules.CommentPrefix ?? throw new ArgumentException("No comment prefix.", nameof(rules));
        RecordRules plain = rules with { CommentPrefix = null };
        int segments = (int)Math.Max(1, (length - first + segment - 1) / segment);
        var odd = new bool[segments];
        var candidates = new long[segments];
        // The prefix after each line end byte (made once: the prefix may be long).
        byte[][] patterns = [.. (rules.LineEnd is { } e ? [e] : "\n\r"u8.ToArray()).Select(end => (byte[])[end, .. prefix])];
        Parallel.For(0, segments, parallel, s =>
        {
            long from = first + (long)s * segment, to = Math.Min(length, from + segment);
            if (rules.Quote is { } q) odd[s] = (new ReadOnlySpan<byte>(data + from, (int)(to - from)).Count(q) & 1) == 1;
            candidates[s] = LastLineStart(data, length, first, from, to, prefix, patterns);
        });
        long last = candidates.Max();
        if (last < 0) return Cut(data, length, first, false, segment, plain, parallel, odd);
        // In order to the first record start after the last candidate's line (quotes in a comment line do not count).
        List<long> starts = [first];
        long resume = InOrderUntil(data, length, first, LineAfter(data, length, last, rules.LineEnd), unitBytes, rules, starts);
        if (resume >= length)
        {
            if (starts[^1] != length) starts.Add(length);
            return [.. starts];
        }
        // The parallel cut from there on pass 1's grid: only the segment the resume point lies in is counted again.
        int s0 = (int)((resume - first) / segment);
        bool[] after = odd[s0..];
        if (rules.Quote is { } quote)
        {
            long to = Math.Min(length, first + (long)(s0 + 1) * segment);
            after[0] = (new ReadOnlySpan<byte>(data + resume, (int)(to - resume)).Count(quote) & 1) == 1;
        }
        long[] rest = Cut(data, length, resume, false, segment, plain, parallel, after, first + (long)s0 * segment);
        if (starts[^1] == resume) starts.RemoveAt(starts.Count - 1);
        return [.. starts, .. rest];
    }

    // The last position in [from, to) where the prefix starts a line (after a line end byte, or at first); -1 when none.
    private static unsafe long LastLineStart(byte* data, long length, long first, long from, long to, ReadOnlySpan<byte> prefix, byte[][] patterns)
    {
        // The byte before the segment and the prefix's bytes past its end take part.
        long lo = from > first ? from - 1 : from, hi = Math.Min(length, to + prefix.Length);
        var span = new ReadOnlySpan<byte>(data + lo, (int)(hi - lo));
        long best = -1;
        foreach (byte[] pattern in patterns)
        {
            int k = span.LastIndexOf(pattern);
            // A match whose prefix starts at or past `to` belongs to the next segment: look before it.
            while (k >= 0 && lo + k + 1 >= to) k = span[..(k + prefix.Length)].LastIndexOf(pattern);
            if (k >= 0) best = Math.Max(best, lo + k + 1);
        }
        if (best < 0 && from == first && span.StartsWith(prefix)) best = first;
        return best;
    }

    // The position after the line end that ends the line at `at` (quotes ignored; CRLF as one), or length.
    private static unsafe long LineAfter(byte* data, long length, long at, byte? lineEnd)
    {
        for (long pos = at; pos < length;)
        {
            var span = new ReadOnlySpan<byte>(data + pos, (int)Math.Min(length - pos, 1 << 30));
            int k = lineEnd is { } e ? span.IndexOf(e) : span.IndexOfAny((byte)'\n', (byte)'\r');
            if (k < 0)
            {
                pos += span.Length;
                continue;
            }
            long end = pos + k + 1;
            if (lineEnd is null && data[pos + k] == '\r' && end < length && data[end] == '\n') end++;
            return end;
        }
        return length;
    }

    // In order from `first` (a record start outside quotes), record by record, to the first record start at or after
    // `until`; unit starts about every unitBytes go to `starts`. Returns that record start (or length).
    private static unsafe long InOrderUntil(byte* data, long length, long first, long until, int unitBytes, RecordRules rules, List<long> starts)
    {
        byte[] prefix = rules.CommentPrefix!;
        RecordRules plain = rules with { CommentPrefix = null };
        long pos = first, previous = first;
        while (pos < until && pos < length)
        {
            var span = new ReadOnlySpan<byte>(data + pos, (int)Math.Min(length - pos, 1 << 30));
            long next;
            if (span.StartsWith(prefix)) next = LineAfter(data, length, pos, rules.LineEnd);
            else
            {
                int k = FirstRecordStart(span, 0, span.Length, false, plain);
                next = k < 0 ? pos + span.Length : pos + k;
            }
            pos = next;
            if (pos - previous >= unitBytes && pos < length)
            {
                starts.Add(pos);
                previous = pos;
            }
        }
        return Math.Min(pos, length);
    }

    // The parallel cut over a grid of segments from `origin` (≤ first, within one segment of it): segment 0 is
    // [first, origin + segment), segment s [origin + s·segment, +segment). Parities counted already may be given.
    private static unsafe long[] Cut(byte* data, long length, long first, bool inQuotes, int segment, RecordRules rules, ParallelOptions parallel,
        bool[]? parities, long origin = -1)
    {
        if (origin < 0) origin = first;
        int segments = (int)Math.Max(1, (length - origin + segment - 1) / segment);
        bool[] odd = parities ?? new bool[segments];
        var starts = new long[segments + 1];
        starts[0] = first;
        starts[segments] = length;
        // One pass over the input: each segment's quote parity (counted already when given).
        if (rules.Quote is { } q && parities is null)
            Parallel.For(0, segments, parallel, s => odd[s] = (Segment(data, length, first, origin, segment, s, out int size)[..size].Count(q) & 1) == 1);
        // The state at each segment by prefix XOR, then from it a short scan to the first record start.
        var inside = new bool[segments];
        bool state = inQuotes;
        for (int s = 0; s < segments; s++)
        {
            inside[s] = state;
            state ^= odd[s];
        }
        Parallel.For(1, segments, parallel, s =>
        {
            ReadOnlySpan<byte> span = Segment(data, length, first, origin, segment, s, out int size);
            int relative = FirstRecordStart(span, 0, size, inside[s], rules);
            starts[s] = relative < 0 ? -1 : origin + (long)s * segment + relative;
        });
        // A segment without a record start takes the next one's: its bytes belong to the unit before.
        for (int s = segments - 1; s > 0; s--)
            if (starts[s] < 0) starts[s] = starts[s + 1];
        return starts;

        // A segment and one byte past it (a CRLF across the boundary).
        static ReadOnlySpan<byte> Segment(byte* data, long length, long first, long origin, int segment, int s, out int size)
        {
            long from = s == 0 ? first : origin + (long)s * segment, to = Math.Min(length, origin + (long)(s + 1) * segment);
            size = (int)(to - from);
            return new ReadOnlySpan<byte>(data + from, size + (to < length ? 1 : 0));
        }
    }

    /// <summary>
    /// The last record start in <paramref name="data"/> (input read as a stream), its start a record start in the state
    /// <paramref name="inQuotes"/>: 0 when no line ends there outside quotes. A CR last is a line end only when
    /// <paramref name="final"/> (an LF may follow). No comment prefix.
    /// </summary>
    public static int LastRecordStart(ReadOnlySpan<byte> data, bool inQuotes, bool final, RecordRules rules)
    {
        if (rules.CommentPrefix is not null) throw new ArgumentException("A comment prefix needs the in-order cut.", nameof(rules));
        byte q = rules.Quote ?? 0;
        bool quoting = rules.Quote is not null;
        bool inside = quoting && (inQuotes ^ (data.Count(q) & 1) == 1); // the state at the end, walked back
        for (int at = data.Length; at > 0;)
        {
            int k = (quoting, rules.LineEnd) switch
            {
                (true, null) => data[..at].LastIndexOfAny(q, (byte)'\n', (byte)'\r'),
                (true, { } e) => data[..at].LastIndexOfAny(q, e),
                (false, null) => data[..at].LastIndexOfAny((byte)'\n', (byte)'\r'),
                (false, { } e) => data[..at].LastIndexOf(e),
            };
            if (k < 0) return 0;
            byte b = data[k];
            if (quoting && b == q) inside = !inside;
            else if (!inside && !(b == '\r' && rules.LineEnd is null && k == data.Length - 1 && !final)) return k + 1;
            at = k;
        }
        return 0;
    }

    /// <summary>
    /// The in-order cut, for any rules (comment lines included): from <paramref name="first"/> (a record start outside
    /// quotes) unit starts about every <paramref name="unitBytes"/> bytes, then <paramref name="length"/>.
    /// </summary>
    public static unsafe long[] InOrder(byte* data, long length, long first, int unitBytes, RecordRules rules)
    {
        List<long> starts = [first];
        long at = first;
        while (at < length)
        {
            // Windows of up to 1 GiB: each starts at a record start, so the scan starts clean.
            int window = (int)Math.Min(length - at, 1 << 30);
            bool final = at + window == length;
            var span = new ReadOnlySpan<byte>(data + at, window);
            int last = Scan(span, final, unitBytes, rules, out List<int> found);
            foreach (int f in found) starts.Add(at + f);
            if (final || last == 0) break; // the end, or one record longer than a window: the rest is one unit
            at += last;
            if (starts[^1] != at) starts.Add(at);
        }
        if (starts[^1] != length) starts.Add(length);
        return [.. starts];
    }

    /// <summary>
    /// Scans <paramref name="data"/> from a clean record start: the record starts at least <paramref name="unitBytes"/>
    /// after the previous one found go to <paramref name="starts"/>; returns the last record start (0 when none), a CR
    /// last counting only when <paramref name="final"/>, a comment prefix cut by the end only when final.
    /// </summary>
    public static int Scan(ReadOnlySpan<byte> data, bool final, int unitBytes, RecordRules rules, out List<int> starts)
    {
        List<int> found = starts = [];
        byte q = rules.Quote ?? 0;
        bool quoting = rules.Quote is not null;
        ReadOnlySpan<byte> prefix = rules.CommentPrefix;
        int at = 0, last = 0, previous = 0;
        bool inside = false;
        while (at < data.Length)
        {
            // At a record start: a comment line runs to its line end, quotes and all.
            if (!inside && prefix.Length > 0 && (at == 0 || at == last))
            {
                if (data.Length - at < prefix.Length && !final && prefix.StartsWith(data[at..])) return last;
                if (data[at..].StartsWith(prefix))
                {
                    int e = rules.LineEnd is { } end ? data[at..].IndexOf(end) : data[at..].IndexOfAny((byte)'\n', (byte)'\r');
                    if (e < 0) return last;
                    at += e;
                    byte lb = data[at++];
                    if (!Ended(data, ref at, lb, final, rules)) return last;
                    Start(at, data.Length, unitBytes, found, ref last, ref previous);
                    continue;
                }
            }
            int k = inside ? data[at..].IndexOf(q) : Find(data[at..], quoting, q, rules.LineEnd);
            if (k < 0) break;
            at += k;
            byte b = data[at++];
            if (quoting && b == q) inside = !inside;
            else
            {
                if (!Ended(data, ref at, b, final, rules)) return last;
                Start(at, data.Length, unitBytes, found, ref last, ref previous);
            }
        }
        return last;

        static void Start(int position, int length, int unitBytes, List<int> found, ref int last, ref int previous)
        {
            last = position;
            if (position - previous >= unitBytes && position < length)
            {
                found.Add(position);
                previous = position;
            }
        }
    }

    /// <summary>After line end byte <paramref name="b"/>: false when a CR is last and more may come (an LF would pair with it).</summary>
    private static bool Ended(ReadOnlySpan<byte> data, ref int at, byte b, bool final, RecordRules rules)
    {
        if (rules.LineEnd is not null || b != '\r') return true;
        if (at == data.Length) return final;
        if (data[at] == '\n') at++;
        return true;
    }
}
