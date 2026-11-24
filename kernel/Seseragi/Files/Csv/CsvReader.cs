using System.Runtime.CompilerServices;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Compute;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Concurrency;
using Kaminari.Sazanami.Filters;
using Kaminari.Sazanami.Text;
using Kaminari.Seseragi.Files.Csv.Reading;
using Kaminari.Seseragi.Files.IO;

namespace Kaminari.Seseragi.Files.Csv;

/// <summary>
/// Reads delimited text (CSV, TSV, …) as Apache Arrow record batches in native memory. The input is cut into chunks of
/// whole records (quotes taken into account), decoded on several threads and handed over in order; a local file is
/// read through a memory mapping, a store's object by ranged reads, a stream in order (gzip, zlib and zstd input is
/// decompressed on the way). Columns are named by the header (or numbered), their types given
/// (<see cref="CsvReadOptions.Schema"/>) or inferred from the first records.
/// </summary>
public sealed class CsvReader : IDisposable, IAsyncDisposable
{
    /// <summary>The records a given schema's temporal columns take their format from (file order).</summary>
    private const int SeedRows = 100;

    /// <summary>Units decoded again in the file's temporal formats (tests check the path is taken).</summary>
    internal static long Redecoded;

    private readonly TextUnits _input;
    private readonly IDisposable? _owned;
    private readonly CsvReadOptions _options;
    private readonly CsvDecodeSettings _settings;
    private readonly int _startUnit, _startOffset;
    private readonly TemporalPattern?[] _groups;
    private volatile TemporalPattern?[] _known;
    private bool _read, _disposed;
    private long _linesBase; // lines passed over before the units (SkipLines): error lines count them

    private CsvReader(TextUnits input, IDisposable? owned, CsvReadOptions options, CsvDecodeSettings settings, int startUnit, int startOffset,
        TemporalPattern?[] groups, bool exact)
    {
        FiltersExactly = exact;
        _input = input;
        _owned = owned;
        _options = options;
        _settings = settings;
        _startUnit = startUnit;
        _startOffset = startOffset;
        _groups = groups;
        _known = groups;
    }

    /// <summary>The columns read (after <see cref="CsvReadOptions.Columns"/>), the row index first when asked.</summary>
    public Schema Schema => _settings.Indexed ?? _settings.Schema;

    /// <summary>
    /// Whether the rows delivered are exactly those <see cref="CsvReadOptions.Filter"/> keeps: true without a filter (or
    /// without <see cref="CsvReadOptions.FilterRows"/>, when every row is delivered), false when a conjunct of it cannot
    /// be evaluated by the reader (the rows then include some the filter drops).
    /// </summary>
    public bool FiltersExactly { get; }

    /// <summary>Opens a file.</summary>
    public static CsvReader Open(string path, CsvReadOptions? options = null) =>
        OpenPathAsync(path, options ?? new CsvReadOptions(), useAsync: false, default).GetAwaiter().GetResult();

    /// <inheritdoc cref="Open(string, CsvReadOptions?)"/>
    public static Task<CsvReader> OpenAsync(string path, CsvReadOptions? options = null, CancellationToken cancellationToken = default) =>
        OpenPathAsync(path, options ?? new CsvReadOptions(), useAsync: true, cancellationToken).AsTask();

    /// <summary>Opens a stream (read from its position; left open with <see cref="CsvReadOptions.LeaveOpen"/>).</summary>
    public static CsvReader Open(Stream stream, CsvReadOptions? options = null) =>
        OpenStreamAsync(stream, options ?? new CsvReadOptions(), useAsync: false, default).GetAwaiter().GetResult();

    /// <inheritdoc cref="Open(Stream, CsvReadOptions?)"/>
    public static Task<CsvReader> OpenAsync(Stream stream, CsvReadOptions? options = null, CancellationToken cancellationToken = default) =>
        OpenStreamAsync(stream, options ?? new CsvReadOptions(), useAsync: true, cancellationToken).AsTask();

    /// <summary>Opens data read at offsets (an object in a store, memory); the caller keeps it open while reading.</summary>
    public static CsvReader Open(IRandomAccess source, CsvReadOptions? options = null) =>
        OpenSourceAsync(source, null, options ?? new CsvReadOptions(), mappable: false, useAsync: false, default).GetAwaiter().GetResult();

    /// <inheritdoc cref="Open(IRandomAccess, CsvReadOptions?)"/>
    public static Task<CsvReader> OpenAsync(IRandomAccess source, CsvReadOptions? options = null, CancellationToken cancellationToken = default) =>
        OpenSourceAsync(source, null, options ?? new CsvReadOptions(), mappable: false, useAsync: true, cancellationToken).AsTask();

    /// <summary>Opens bytes in memory, read in place: the caller keeps them unchanged while reading.</summary>
    public static CsvReader Open(ReadOnlyMemory<byte> bytes, CsvReadOptions? options = null) =>
        OpenSourceAsync(new MemoryRandomAccess(bytes), null, options ?? new CsvReadOptions(), mappable: false, useAsync: false, default).GetAwaiter().GetResult();

    /// <summary>
    /// Opens text (a string): it is encoded to UTF-8 once, into native memory the reader keeps. Unpaired surrogates are
    /// refused (<see cref="CsvFormatException"/>).
    /// </summary>
    public static CsvReader Parse(ReadOnlySpan<char> text, CsvReadOptions? options = null)
    {
        int length = Encoding.UTF8.GetByteCount(text);
        var buffer = new NativeByteBuffer(Math.Max(1, length), pooled: true);
        if (System.Text.Unicode.Utf8.FromUtf16(text, buffer.Span, out int read, out int written, replaceInvalidSequences: false) != System.Buffers.OperationStatus.Done)
        {
            buffer.Release();
            throw new CsvFormatException($"The text holds an unpaired surrogate at character {read}.");
        }
        var source = new MemoryRandomAccess(buffer.Memory[..written]);
        // The text is UTF-8 now, whatever encoding the options name.
        CsvReadOptions utf8 = (options ?? new CsvReadOptions()) with { Encoding = null, InvalidText = CsvInvalidText.Error };
        return OpenSourceAsync(source, new Releaser(buffer), utf8, mappable: false, useAsync: false, default).GetAwaiter().GetResult();
    }

    private static ValueTask<CsvReader> OpenPathAsync(string path, CsvReadOptions options, bool useAsync, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(path);
        var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 1,
            useAsync ? FileOptions.RandomAccess | FileOptions.Asynchronous : FileOptions.RandomAccess);
        RandomAccessSource source;
        try { source = new RandomAccessSource(stream, leaveOpen: false, null, mappable: options.MemoryMap == CsvMemoryMap.Auto); }
        catch
        {
            stream.Dispose();
            throw;
        }
        return OpenSourceAsync(source, source, options, mappable: true, useAsync, ct);
    }

    private static async ValueTask<CsvReader> OpenStreamAsync(Stream stream, CsvReadOptions options, bool useAsync, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(stream);
        if (stream.CanSeek)
        {
            var source = new RandomAccessSource(stream, options.LeaveOpen, null);
            return await OpenSourceAsync(source, source, options, mappable: false, useAsync, ct).ConfigureAwait(false);
        }
        CsvOpened opened;
        try { opened = await CsvInput.OpenAsync(stream, options, Cut(Validate(options)), useAsync, ct).ConfigureAwait(false); }
        catch
        {
            if (!options.LeaveOpen) await stream.DisposeAsync().ConfigureAwait(false);
            throw;
        }
        return await CreateAsync(opened, null, options, useAsync, ct).ConfigureAwait(false);
    }

    private static async ValueTask<CsvReader> OpenSourceAsync(IRandomAccess source, IDisposable? owned, CsvReadOptions options, bool mappable,
        bool useAsync, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(source);
        CsvOpened opened;
        try
        {
            // Units sized by the table's width (the window's budget follows them).
            options = options with { UnitBytes = await CsvInput.UnitBytesAsync(source, options, useAsync, ct).ConfigureAwait(false) };
            opened = await CsvInput.OpenAsync(source, options, Cut(Validate(options)), mappable && options.MemoryMap == CsvMemoryMap.Auto, useAsync, ct)
                .ConfigureAwait(false);
        }
        catch
        {
            owned?.Dispose();
            throw;
        }
        return await CreateAsync(opened, owned, options, useAsync, ct).ConfigureAwait(false);
    }

    // The record rules of the options (checked).
    private static RecordCut Cut(RecordRules rules) => new(rules,
        start => new CsvFormatException($"The record starting at byte {start} is longer than 2 GB (is a quote left open?)."),
        inner => new CsvFormatException($"The compressed input is damaged: {inner.Message}", inner));

    /// <summary>Checks the options (ArgumentException for what they cannot be).</summary>
    internal static void Check(CsvReadOptions options) => Validate(options);

    private static RecordRules Validate(CsvReadOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.BatchSize);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.MaxDegreeOfParallelism);
        if (options.InferSchemaLength is < 0) throw new ArgumentOutOfRangeException(nameof(options), "InferSchemaLength cannot be negative (null infers from the whole input).");
        if (options.MaxRows is < 0) throw new ArgumentOutOfRangeException(nameof(options), "MaxRows cannot be negative.");
        if (options.RowIndexOffset < 0) throw new ArgumentOutOfRangeException(nameof(options), "RowIndexOffset cannot be negative.");
        if (options.Schema is not null && options.SchemaOverrides is not null)
            throw new ArgumentException("Give either a schema or schema overrides, not both.", nameof(options));
        byte separator = Ascii(options.Separator, "Separator");
        byte? quote = options.Quote is { } q ? Ascii(q, "Quote") : null;
        byte? lineEnd = options.LineEnd is { } e ? Ascii(e, "LineEnd") : null;
        bool IsLineEnd(byte b) => lineEnd is { } end ? b == end : b is (byte)'\n' or (byte)'\r';
        if (IsLineEnd(separator)) throw new ArgumentException("The separator cannot be a line end.", nameof(options));
        if (quote is { } qb && (qb == separator || IsLineEnd(qb))) throw new ArgumentException("The quote cannot be the separator or a line end.", nameof(options));
        if (options.ThousandsSeparator is { } t)
        {
            if (t.Length == 0 || t.Any(char.IsAsciiDigit)) throw new ArgumentException("The thousands separator is one or more characters, no digits.", nameof(options));
            if (t == (options.DecimalComma ? "," : ".")) throw new ArgumentException($"The thousands separator '{t}' is the decimal point too.", nameof(options));
        }
        if (!Enum.IsDefined(options.InvalidText)) throw new ArgumentOutOfRangeException(nameof(options), $"Unknown InvalidText {options.InvalidText}.");
        if (options.SkipLines < 0 || options.SkipRows < 0 || options.SkipRowsAfterHeader < 0)
            throw new ArgumentOutOfRangeException(nameof(options), "SkipLines, SkipRows and SkipRowsAfterHeader cannot be negative.");
        if (options.SkipLines > 0 && options.SkipRows > 0)
            throw new ArgumentException("Give either SkipLines (raw lines) or SkipRows (records), not both.", nameof(options));
        byte[]? comment = null;
        if (options.CommentPrefix is { } prefix)
        {
            if (prefix.Length == 0) throw new ArgumentException("The comment prefix cannot be empty (null: no comment lines).", nameof(options));
            if (prefix.Any(c => c is '\n' or '\r' || c == options.LineEnd))
                throw new ArgumentException("The comment prefix cannot hold a line end.", nameof(options));
            comment = Encoding.UTF8.GetBytes(prefix);
        }
        return new RecordRules(quote, lineEnd, comment);

        static byte Ascii(char c, string what) => c < 0x80 ? (byte)c : throw new ArgumentException($"{what} must be an ASCII character.", nameof(options));
    }

    private static async ValueTask<CsvReader> CreateAsync(CsvOpened opened, IDisposable? owned, CsvReadOptions options, bool useAsync, CancellationToken ct)
    {
        TextUnits input = opened.Units;
        try
        {
            CsvValueRules values = Values(options, opened.Charset);
            Prelude prelude = await Prelude.ReadAsync(input, options, values, opened.Mark, opened.SkippedLines, useAsync, ct).ConfigureAwait(false);
            return new CsvReader(input, Both(opened.Owned, owned), options, prelude.Settings, prelude.StartUnit, prelude.StartOffset, prelude.Groups, prelude.Exact)
            {
                _linesBase = opened.SkippedLines,
            };
        }
        catch
        {
            input.Dispose();
            opened.Owned?.Dispose();
            owned?.Dispose();
            throw;
        }

        static IDisposable? Both(IDisposable? first, IDisposable? second) => first is null ? second : second is null ? first : new Pair(first, second);
    }

    // The rules values are read by (the options checked).
    private static CsvValueRules Values(CsvReadOptions options, CsvCharset charset)
    {
        RecordRules rules = Validate(options);
        var tokenizer = new CsvTokenizer((byte)options.Separator, rules.Quote, rules.LineEnd, rules.CommentPrefix, options.TruncateRaggedLines);
        return new CsvValueRules(tokenizer, options.MissingIsNull, options.IgnoreErrors, Bytes(options.NullValues), Bytes(options.TrueValues),
            Bytes(options.FalseValues), options.DecimalComma, options.ThousandsSeparator is { } t ? Encoding.UTF8.GetBytes(t) : null, charset);

        static byte[][] Bytes(IReadOnlyList<string>? values) => values is null ? [] : [.. values.Select(Encoding.UTF8.GetBytes)];
    }

    // ---- a dataset's files ---------------------------------------------------------------------------------------

    /// <summary>A dataset's file opened (its prelude read with <see cref="CsvReadOptions.Dataset"/>); the caller keeps the source open.</summary>
    internal static ValueTask<CsvReader> OpenFileAsync(IRandomAccess source, CsvReadOptions options, bool mappable, bool useAsync, CancellationToken ct) =>
        OpenSourceAsync(source, null, options, mappable, useAsync, ct);

    /// <summary>
    /// A file's column names and the inference of its first records (<see cref="CsvReadOptions.InferSchemaLength"/>),
    /// nothing decoded: a dataset infers its columns from several files' heads.
    /// </summary>
    internal static async ValueTask<CsvHead> HeadAsync(IRandomAccess source, CsvReadOptions options, bool mappable, bool useAsync, CancellationToken ct)
    {
        options = options with { Schema = null, Dataset = null, UnitBytes = await CsvInput.UnitBytesAsync(source, options, useAsync, ct).ConfigureAwait(false) };
        CsvOpened opened = await CsvInput.OpenAsync(source, options, Cut(Validate(options)), mappable && options.MemoryMap == CsvMemoryMap.Auto, useAsync, ct)
            .ConfigureAwait(false);
        try
        {
            Prelude prelude = await Prelude.ReadAsync(opened.Units, options, Values(options, opened.Charset), opened.Mark, opened.SkippedLines, useAsync, ct, head: true)
                .ConfigureAwait(false);
            return prelude.Head!;
        }
        finally
        {
            opened.Units.Dispose();
            opened.Owned?.Dispose();
        }
    }

    /// <summary>The file's units (a dataset's window reads them).</summary>
    internal TextUnits Input => _input;

    /// <summary>Decodes unit <paramref name="index"/> from its bytes (null: past the end), in the formats the file knows so far.</summary>
    internal CsvUnit Decode(TextUnitBytes? bytes, int index, long maxRows) => Decode(bytes, index, maxRows, _known);

    /// <summary>A delivery of the file's units in order (see <see cref="Delivery"/>): at most <paramref name="max"/> rows, numbered from <paramref name="firstRow"/>.</summary>
    internal Delivery Deliver(long max, long firstRow) => new(this, max, firstRow);

    /// <summary>
    /// What the first records give: where the data starts (after blank lines and the header), the columns (named, typed,
    /// those decoded) and each temporal column's format group, read in file order.
    /// </summary>
    private sealed class Prelude
    {
        public CsvDecodeSettings Settings { get; init; } = null!;
        public int StartUnit { get; init; }
        public int StartOffset { get; init; }
        public TemporalPattern?[] Groups { get; init; } = [];
        public bool Exact { get; init; }

        /// <summary>With <c>head</c>: the file's column names and what its first records hold (nothing decoded).</summary>
        public CsvHead? Head { get; init; }

        /// <summary>The prelude of the input (with <paramref name="head"/> only the names and the inference of the first records: a dataset infers its columns).</summary>
        public static async ValueTask<Prelude> ReadAsync(TextUnits input, CsvReadOptions options, CsvValueRules values, int mark, long linesBase,
            bool useAsync, CancellationToken ct, bool head = false)
        {
            CsvTokenizer tokenizer = values.Tokenizer;
            List<CsvField> fields = [];
            string[]? names = null;
            int startUnit = 0, startOffset = 0;
            long lines = linesBase, rows = 0;
            // In file order: blank and comment lines before SkipRows, SkipRows records, the header, SkipRowsAfterHeader
            // records (records found as the cut finds them, nothing in them read), then the data and its first rows.
            // Leading blank (and comment) lines are passed over before SkipRows with a header; without one a blank line is a
            // row, counted by SkipRows (native).
            bool lead = options.SkipRows > 0 && options.HasHeader, started = !options.HasHeader, inData = false, sawRecord = false;
            long skipRows = options.SkipRows, skipAfter = options.SkipRowsAfterHeader;
            // A dataset's file takes the dataset's types (typed, as by a schema): its first rows seed temporal formats only.
            bool typed = options.Schema is not null || options.Dataset is not null;
            long limit = typed ? SeedRows : options.InferSchemaLength is int length ? length : long.MaxValue;
            // Without a header the columns are as many as the widest of the first records (InferSchemaLength, at least one);
            // with a schema given those are checked against it, among the rows read at open.
            long widen = options.HasHeader ? 0 : Math.Max(1, options.InferSchemaLength ?? long.MaxValue);
            if (typed) widen = Math.Min(widen, SeedRows);
            int widest = 0;
            Plan? plan = null;
            try
            {
                for (int u = 0; input.Count < 0 || u < input.Count; u++)
                {
                    using TextUnitBytes? unit = await input.GetAsync(u, useAsync, ct).ConfigureAwait(false);
                    if (unit is null) break;
                    ReadOnlySpan<byte> bytes = unit.Memory.Span;
                    int pos = 0;
                    if (unit.FileStart) pos = Math.Min(mark, bytes.Length);
                    bool clean = values.Charset.Clean(bytes);
                    if (lead)
                    {
                        while (pos < bytes.Length && (tokenizer.AtLineEnd(bytes, pos) || tokenizer.AtComment(bytes, pos)))
                            pos = tokenizer.AtComment(bytes, pos) ? tokenizer.LineAfter(bytes, pos) : tokenizer.SkipLineEnd(bytes, pos);
                        lead = pos == bytes.Length;
                    }
                    while (skipRows > 0 && (pos = tokenizer.SkipComments(bytes, pos)) < bytes.Length)
                    {
                        pos = tokenizer.SkipRecord(bytes, pos);
                        skipRows--;
                    }
                    if (!started && skipRows == 0 && !lead)
                    {
                        // Blank and comment lines before the header are passed over.
                        while (pos < bytes.Length && (tokenizer.AtLineEnd(bytes, pos) || tokenizer.AtComment(bytes, pos)))
                            pos = tokenizer.AtComment(bytes, pos) ? tokenizer.LineAfter(bytes, pos) : tokenizer.SkipLineEnd(bytes, pos);
                        if (pos < bytes.Length)
                        {
                            int start = pos;
                            FieldEnd end = tokenizer.Record(bytes, ref pos, fields, out _, out int failed);
                            long at = lines + tokenizer.CountLines(bytes[..start]) + 1;
                            if (end > FieldEnd.Input) throw new CsvFormatException($"The header (line {at}): {CsvDecode.Malformed(end, failed, fields.Count + 1)}", 0, at);
                            if (values.Charset.FirstInvalid(bytes[start..pos]) >= 0) throw new CsvFormatException($"The header (line {at}): {values.Charset.Invalid}", 0, at);
                            names = Names(bytes, fields, tokenizer, values.Charset);
                            started = sawRecord = true;
                            plan = new Plan(Resolved(options, names), values, names);
                        }
                    }
                    while (started && skipAfter > 0 && (pos = tokenizer.SkipComments(bytes, pos)) < bytes.Length)
                    {
                        pos = tokenizer.SkipRecord(bytes, pos);
                        skipAfter--;
                    }
                    if (started && skipAfter == 0 && skipRows == 0 && !lead && !inData)
                    {
                        // The data starts here (rows, row numbers and MaxRows count from it).
                        inData = true;
                        startUnit = u;
                        startOffset = pos;
                    }
                    while (inData && (rows < limit || plan is null || rows < widen) && (pos = tokenizer.SkipComments(bytes, pos)) < bytes.Length)
                    {
                        int start = pos;
                        FieldEnd end = tokenizer.Record(bytes, ref pos, fields, out bool blank, out int failed);
                        long row = rows + 1, at = lines + tokenizer.CountLines(bytes[..start]) + 1;
                        if (plan is null && !blank)
                        {
                            // Without a header: numbered columns, as many as the schema given or the first record's fields
                            // (more as wider records come among the first ones).
                            int count = options.Schema?.FieldsList.Count ?? fields.Count + (end > FieldEnd.Input ? 1 : 0);
                            names = [.. Enumerable.Range(1, count).Select(i => options.Dataset is { ByPosition: true } d && i <= d.Columns.FieldsList.Count
                                ? d.Columns.FieldsList[i - 1].Name : $"column_{i}")];
                            plan = new Plan(Resolved(options, names), values, names);
                        }
                        int columns = plan?.FileColumns ?? int.MaxValue;
                        if (end > FieldEnd.Input) throw new CsvFormatException($"Row {row} (line {at}): {CsvDecode.Malformed(end, failed, columns)}", row, at);
                        if (!blank && row <= widen) widest = Math.Max(widest, fields.Count);
                        if (fields.Count > columns)
                        {
                            if (row <= widen && !typed) plan!.Widen(fields.Count);
                            else if (!tokenizer.Truncates && !(row <= widen && options.Schema is not null))
                                throw new CsvFormatException($"Row {row} (line {at}): {CsvDecode.Malformed(FieldEnd.TooManyFields, columns, columns)}", row, at);
                        }
                        if (values.Charset.FirstInvalid(bytes[start..pos]) >= 0) throw new CsvFormatException($"Row {row} (line {at}): {values.Charset.Invalid}", row, at);
                        sawRecord = true;
                        rows++;
                        plan?.Observe(bytes, fields, rows, clean);
                    }
                    if (inData && rows >= limit && rows >= widen && plan is not null) break;
                    lines += tokenizer.CountLines(bytes);
                }
            }
            catch
            {
                plan?.Dispose();
                throw;
            }
            // Skipped rows running past the end: no data at all (no unit decodes any).
            if (!inData) startUnit = int.MaxValue;
            if (!options.HasHeader && options.Schema is { } given && widest > 0 && (widest < given.FieldsList.Count || (widest > given.FieldsList.Count && !tokenizer.Truncates)))
            {
                plan?.Dispose();
                throw new CsvFormatException($"The given schema has {given.FieldsList.Count} columns, the records {widest} (without a header the schema gives the columns by position).");
            }
            if (!sawRecord || plan is null)
            {
                plan?.Dispose();
                if (options.RaiseIfEmpty && options.SkipRows > 0)
                    throw new CsvFormatException($"SkipRows ({options.SkipRows}) passes over the whole input, which holds {options.SkipRows - skipRows} record" +
                        $"{(options.SkipRows - skipRows == 1 ? "" : "s")} (a quote left open in a skipped record runs to the end): skip fewer rows, or give RaiseIfEmpty = false to read it as an empty frame.");
                if (options.RaiseIfEmpty)
                    throw new CsvFormatException("The input holds no records (not even a header): give RaiseIfEmpty = false to read it as an empty frame.");
                names = options.Schema is { } s ? [.. s.FieldsList.Select(f => f.Name)] : names ?? [];
                plan = new Plan(Resolved(options, names), values, names);
            }
            using (plan)
            {
                if (head) return new Prelude { Head = plan.TakeHead() };
                (CsvDecodeSettings settings, TemporalPattern?[] groups, bool exact) = plan.Finish();
                return new Prelude { Settings = settings, StartUnit = startUnit, StartOffset = startOffset, Groups = groups, Exact = exact };
            }
        }

        // A dataset's file: the columns it reads and its filter, decided by its names.
        private static CsvReadOptions Resolved(CsvReadOptions options, string[] names)
        {
            if (options.Dataset is not { } dataset) return options;
            (IReadOnlyList<string> columns, Samidare.IR.ScalarExpr? filter) = dataset.Resolve(names);
            return options with { Columns = columns, Filter = filter };
        }

        // The header's names: quotes removed and undoubled, decoded; duplicates renamed name_duplicated_0, _1 … (as the
        // engine names them).
        private static string[] Names(ReadOnlySpan<byte> bytes, List<CsvField> fields, CsvTokenizer tokenizer, CsvCharset charset)
        {
            var names = new string[fields.Count];
            Dictionary<string, int> seen = [];
            Span<byte> stack = stackalloc byte[256];
            for (int c = 0; c < names.Length; c++)
            {
                CsvField field = fields[c];
                scoped ReadOnlySpan<byte> text = bytes.Slice(field.Start, field.Length);
                if (field.IsEscaped)
                {
                    Span<byte> to = text.Length <= stack.Length ? stack : new byte[text.Length];
                    text = to[..tokenizer.Unescape(text, to)];
                }
                if (charset.FieldDecoder is not null && !System.Text.Ascii.IsValid(text))
                {
                    byte[] decoded = new byte[CsvCharset.Room(text.Length)];
                    text = decoded.AsSpan(0, charset.Decode(text, decoded));
                }
                string name = Encoding.UTF8.GetString(text);
                if (seen.TryGetValue(name, out int count))
                {
                    seen[name] = count + 1;
                    name = $"{name}_duplicated_{count}";
                }
                else seen[name] = 0;
                names[c] = name;
            }
            return names;
        }
    }

    /// <summary>
    /// The columns of a file: named by the header (or numbered), typed by the schema given (by name with a header, by
    /// position without), else inferred (types overridden by name); those decoded; the format groups of temporal ones.
    /// </summary>
    private sealed class Plan : IDisposable
    {
        private NativeByteBuffer? _scratch;
        private readonly CsvReadOptions _options;
        private readonly CsvValueRules _values;
        private string[] _names;
        private CsvInference? _inference;
        private Field?[] _given; // per file column: its given field (schema), or null
        private readonly int[]? _order;   // per file column: its place in the given schema's order
        private TemporalColumn[] _dates, _datetimes;
        private bool[] _seeded;

        public Plan(CsvReadOptions options, CsvValueRules values, string[] names)
        {
            _options = options;
            _values = values;
            _names = names;
            int n = names.Length;
            _given = new Field?[n];
            _dates = new TemporalColumn[n];
            _datetimes = new TemporalColumn[n];
            _seeded = new bool[n];
            if (options.Dataset is { } dataset)
            {
                // Each column takes the dataset's type by name; one the dataset lacks is left out, or an error.
                for (int c = 0; c < n; c++)
                {
                    _given[c] = dataset.Columns.GetFieldByName(names[c]) ?? (dataset.IgnoreExtra
                        ? new Field(names[c], StringViewType.Default, true)
                        : throw new CsvFormatException($"The file's column '{names[c]}' is not one of the dataset's ({string.Join(", ", dataset.Columns.FieldsList.Select(f => f.Name))}): " +
                            "give IgnoreExtraColumns = true to leave such columns out, or a schema that has it."));
                }
            }
            else if (options.Schema is { } schema)
            {
                IReadOnlyList<Field> fields = schema.FieldsList;
                if (!options.HasHeader)
                {
                    if (fields.Count != n)
                        throw new CsvFormatException($"The given schema has {fields.Count} columns, the records {n} (without a header the schema gives the columns by position).");
                    for (int c = 0; c < n; c++) _given[c] = fields[c];
                    _order = [.. Enumerable.Range(0, n)];
                }
                else
                {
                    _order = new int[n];
                    for (int c = 0; c < n; c++)
                    {
                        int at = schema.GetFieldIndex(names[c]);
                        if (at < 0)
                            throw new CsvFormatException($"The file's column '{names[c]}' is not in the given schema ({string.Join(", ", fields.Select(f => f.Name))}): " +
                                "the schema names the file's columns (add it), or read without a header for a schema by position.");
                        _given[c] = fields[at];
                        _order[c] = at;
                    }
                    if (fields.FirstOrDefault(f => !names.Contains(f.Name)) is { } missing)
                        throw new CsvFormatException($"The given schema's column '{missing.Name}' is not in the file (its columns: {string.Join(", ", names)}).");
                }
            }
            else
            {
                if (options.SchemaOverrides is { } overrides)
                    for (int c = 0; c < n; c++)
                        if (overrides.TryGetValue(names[c], out IArrowType? type)) _given[c] = new Field(names[c], type, true);
                if (options.InferSchemaLength != 0) _inference = new CsvInference(n, values, options.TryParseDates);
            }
            for (int c = 0; c < n; c++) _seeded[c] = _given[c]?.DataType is not (Date32Type or TimestampType);
        }

        public int FileColumns => _names.Length;

        /// <summary>The names and the inference of the first records, handed over (the plan no longer owns the inference).</summary>
        public CsvHead TakeHead()
        {
            CsvHead head = new(_names, _inference);
            _inference = null;
            return head;
        }

        /// <summary>
        /// Without a header or schema: <paramref name="columns"/> columns from now on (a wider record among the first
        /// ones); the new ones are missing in the records before (nulls).
        /// </summary>
        public void Widen(int columns)
        {
            int n = _names.Length;
            if (columns <= n) return;
            _names = [.. _names, .. Enumerable.Range(n + 1, columns - n).Select(i => $"column_{i}")];
            System.Array.Resize(ref _given, columns);
            System.Array.Resize(ref _dates, columns);
            System.Array.Resize(ref _datetimes, columns);
            System.Array.Resize(ref _seeded, columns);
            for (int c = n; c < columns; c++)
            {
                if (_options.SchemaOverrides is { } overrides && overrides.TryGetValue(_names[c], out IArrowType? type)) _given[c] = new Field(_names[c], type, true);
                _seeded[c] = _given[c]?.DataType is not (Date32Type or TimestampType);
            }
            _inference?.Widen(columns);
        }

        public void Dispose()
        {
            _scratch?.Release();
            _decoded?.Release();
            _inference?.Dispose();
        }

        private NativeByteBuffer? _decoded;

        // A second scratch buffer: decoded text (the first may hold the unescaped text being decoded).
        private Span<byte> Decoded(int bytes)
        {
            _decoded ??= new NativeByteBuffer(Math.Max(bytes, 4096), pooled: true);
            _decoded.EnsureCapacity(bytes);
            return _decoded.Span;
        }

        private Span<byte> Scratch(int bytes)
        {
            _scratch ??= new NativeByteBuffer(Math.Max(bytes, 4096), pooled: true);
            _scratch.EnsureCapacity(bytes);
            return _scratch.Span;
        }

        /// <summary>Takes in a record of the first ones (in file order): inference, and the formats of given temporal columns.</summary>
        public void Observe(ReadOnlySpan<byte> bytes, List<CsvField> fields, long row, bool clean)
        {
            _inference?.Observe(bytes, fields, clean);
            if (row > SeedRows) return;
            Span<byte> stack = stackalloc byte[256];
            for (int c = 0; c < fields.Count && c < _seeded.Length; c++)
            {
                if (_seeded[c] || fields[c].Bits <= 0) continue;
                scoped ReadOnlySpan<byte> text = bytes.Slice(fields[c].Start, fields[c].Length);
                if (fields[c].IsEscaped)
                {
                    Span<byte> to = text.Length <= stack.Length ? stack : Scratch(text.Length);
                    text = to[.._values.Tokenizer.Unescape(text, to)];
                }
                if (!clean && !System.Text.Ascii.IsValid(text))
                {
                    Span<byte> room = Decoded(CsvCharset.Room(text.Length));
                    text = room[.._values.Charset.Decode(text, room)];
                }
                if (text.IsEmpty || (_values.NullValues.Length > 0 && _values.IsNullValue(text))) continue;
                if (_given[c]!.DataType is Date32Type)
                {
                    _dates[c].ReadDate(text, out _);
                    _seeded[c] = _dates[c].Group is not null;
                }
                else
                {
                    _datetimes[c].ReadDatetime(text, TimeUnit.Nanosecond, out _);
                    _seeded[c] = _datetimes[c].Group is not null;
                }
            }
        }

        public (CsvDecodeSettings, TemporalPattern?[], bool Exact) Finish()
        {
            int n = _names.Length;
            Field[] full;
            TemporalPattern?[] groups;
            if (_options.Schema is not null || _options.Dataset is not null || _inference is null)
            {
                full = [.. Enumerable.Range(0, n).Select(c => _given[c] ?? new Field(_names[c], _options.ViewTypes ? StringViewType.Default : StringType.Default, true))];
                groups = new TemporalPattern?[n];
            }
            else (full, groups) = _inference.Finish(_names, _options.ViewTypes);
            for (int c = 0; c < n; c++)
            {
                if (_given[c] is not { } given) continue;
                full[c] = given;
                groups[c] = given.DataType is Date32Type ? _dates[c].Group : given.DataType is TimestampType ? _datetimes[c].Group : null;
            }
            // The columns decoded: those named (all by default), in output order (the schema's, else the file's).
            int[] order = _order ?? [.. Enumerable.Range(0, n)];
            bool[] wanted = new bool[n];
            if (_options.Columns is { } columns)
            {
                // By the columns' names (a schema given without a header names them).
                string[] names = [.. full.Select(f => f.Name)];
                var unknown = columns.Where(name => !names.Contains(name)).ToList();
                if (unknown.Count > 0) throw new ArgumentException($"There are no columns named {string.Join(", ", unknown)}.", nameof(columns));
                foreach (string name in columns) wanted[System.Array.IndexOf(names, name)] = true;
            }
            else System.Array.Fill(wanted, true);
            int[] all = [.. Enumerable.Range(0, n).OrderBy(c => order[c])];
            var selected = new Schema([.. all.Where(c => wanted[c]).Select(c => full[c])], null);
            // A row filter decodes the columns it reads too (in output order); without a program every row is kept.
            RowFilter? filter = null;
            bool exact = true;
            if (_options.FilterRows && _options.Filter is { } expr)
            {
                filter = RowFilter.Create(new Schema([.. all.Select(c => full[c])], null), selected, expr, out exact);
                if (filter.Program is null) filter = null;
            }
            Schema schema = filter?.Decoded ?? selected;
            int[] byDecoded = [.. all.Where(c => schema.GetFieldIndex(full[c].Name) >= 0)];
            int[] slots = new int[n];
            System.Array.Fill(slots, -1);
            for (int k = 0; k < byDecoded.Length; k++) slots[byDecoded[k]] = k;
            // Types inferred from the first rows (not the whole input): values past them may not fit, the errors say so.
            int sample = _options.Dataset?.InferredFrom ?? (_inference is not null && _options.InferSchemaLength is int rows ? rows : 0);
            var settings = new CsvDecodeSettings(schema, n, slots, _values, _options.BatchSize, filter)
            {
                Indexed = Indexed(selected, _options.RowIndexColumn),
                InferredFrom = [.. byDecoded.Select(c => _given[c] is null || _options.Dataset is not null ? sample : 0)],
            };
            // Every column's type has a decoder (NotSupportedException otherwise).
            var none = new TemporalPattern?[schema.FieldsList.Count];
            using (var builder = settings.Builder())
                foreach (CsvColumn column in settings.Columns(builder, none)) column.Dispose();
            using (var builder = settings.TestBuilder())
                foreach (CsvColumn column in settings.TestColumns(builder, none)) column.Dispose();
            return (settings, [.. byDecoded.Select(c => groups[c])], exact);
        }

        // The batches' schema with the row index column first (null without one).
        private static Schema? Indexed(Schema delivered, string? rowIndex)
        {
            if (rowIndex is null) return null;
            if (rowIndex.Length == 0) throw new ArgumentException("The row index column needs a name.", "options");
            if (delivered.GetFieldIndex(rowIndex) >= 0)
                throw new ArgumentException($"The row index column '{rowIndex}' has the name of another column read.", "options");
            return new Schema([new Field(rowIndex, Int64Type.Default, nullable: false), .. delivered.FieldsList], delivered.Metadata);
        }
    }

    /// <summary>The records as record batches (each the caller's to dispose); a stream is read once.</summary>
    public IEnumerable<RecordBatch> ReadBatches()
    {
        Begin();
        return Batches();

        IEnumerable<RecordBatch> Batches()
        {
            var state = new Delivery(this, _options.MaxRows ?? long.MaxValue, _options.RowIndexOffset);
            using OrderedWindow<CsvUnit> window = Window(default);
            for (int i = 0; state.Rows < state.Max && Has(window, i); i++)
            {
                CsvUnit unit = window.Take(i).GetAwaiter().GetResult();
                try
                {
                    unit = state.Check(i, unit, useAsync: false, default).GetAwaiter().GetResult();
                    if (unit.End) yield break;
                    while (state.Next(unit, out RecordBatch? batch)) yield return batch!;
                    state.Done(unit);
                }
                finally
                {
                    unit.Dispose();
                    _input.Release(i);
                }
            }
        }
    }

    /// <inheritdoc cref="ReadBatches"/>
    public IAsyncEnumerable<RecordBatch> ReadBatchesAsync(CancellationToken cancellationToken = default)
    {
        Begin();
        return Batches(cancellationToken);

        async IAsyncEnumerable<RecordBatch> Batches([EnumeratorCancellation] CancellationToken ct)
        {
            var state = new Delivery(this, _options.MaxRows ?? long.MaxValue, _options.RowIndexOffset);
            using OrderedWindow<CsvUnit> window = Window(ct);
            for (int i = 0; state.Rows < state.Max && Has(window, i); i++)
            {
                CsvUnit unit = await window.Take(i).ConfigureAwait(false);
                try
                {
                    unit = await state.Check(i, unit, useAsync: true, ct).ConfigureAwait(false);
                    if (unit.End) yield break;
                    while (state.Next(unit, out RecordBatch? batch)) yield return batch!;
                    state.Done(unit);
                }
                finally
                {
                    unit.Dispose();
                    _input.Release(i);
                }
            }
        }
    }

    /// <summary>
    /// The delivery of units in order: each unit's temporal formats checked against the file's (a unit that read a
    /// column in another format, or refused values before it knew one, is decoded again in the file's), failures placed
    /// at their row and line in the whole input, the rows cut to <see cref="CsvReadOptions.MaxRows"/> and numbered.
    /// </summary>
    internal sealed class Delivery(CsvReader reader, long max, long firstRow)
    {
        private readonly TemporalPattern?[] _groups = [.. reader._groups];
        private long _records, _lines;
        private int _batch;

        public long Max { get; } = max;

        public long Rows { get; private set; }

        /// <summary>The records of the units done (the file's rows before the next unit).</summary>
        public long Records => _records;

        /// <summary>Whether the last unit checked was decoded again (its batches are new).</summary>
        public bool Redone { get; private set; }

        /// <summary>
        /// Checks unit <paramref name="index"/> (decoded again when needed: from <paramref name="bytes"/> when kept, else
        /// read again), learns its formats, throws its failure placed in the file.
        /// </summary>
        public async ValueTask<CsvUnit> Check(int index, CsvUnit unit, bool useAsync, CancellationToken ct, TextUnitBytes? bytes = null)
        {
            _batch = 0;
            Redone = false;
            if (unit.End) return unit;
            if (Redo(unit))
            {
                Interlocked.Increment(ref Redecoded);
                unit.Dispose();
                unit = await Again(index, bytes, useAsync, ct).ConfigureAwait(false);
            }
            else if (unit.Error is { } past && past.Delivered >= Max - Rows)
            {
                // A failure past the rows asked is not the reader's (the unit decoded up to MaxRows of its own, not
                // of the whole input): the unit is decoded again up to them. Batches past them are cut as delivered.
                unit.Dispose();
                unit = await Again(index, bytes, useAsync, ct).ConfigureAwait(false);
            }
            Learn(unit);
            if (unit.Error is { } error)
            {
                long lines = reader._input.ReadOnce ? _lines : await reader.LinesBeforeAsync(index, useAsync, ct).ConfigureAwait(false);
                throw error.At(_records, reader._linesBase + lines);
            }
            return unit;
        }

        // The unit decoded again in the file's formats, up to the rows still wanted.
        private async ValueTask<CsvUnit> Again(int index, TextUnitBytes? bytes, bool useAsync, CancellationToken ct)
        {
            Redone = true;
            return bytes is null ? await reader.DecodeAsync(index, Max - Rows, [.. _groups], useAsync, ct).ConfigureAwait(false)
                : reader.Decode(bytes, index, Max - Rows, [.. _groups]);
        }

        // Whether the unit read a temporal column otherwise than the file does.
        private bool Redo(CsvUnit unit)
        {
            if (unit.Guesses is not { } guesses) return false;
            for (int c = 0; c < guesses.Length; c++)
            {
                if (_groups[c] is not { } known) continue;
                (TemporalPattern? group, bool unsure) = guesses[c];
                if (unsure || (group is not null && group != known)) return true;
            }
            return false;
        }

        // The formats the unit found for columns the file has none for yet.
        private void Learn(CsvUnit unit)
        {
            if (unit.Guesses is not { } guesses) return;
            bool changed = false;
            for (int c = 0; c < guesses.Length; c++)
                if (_groups[c] is null && guesses[c].Group is { } group)
                {
                    _groups[c] = group;
                    changed = true;
                }
            if (changed) reader._known = [.. _groups];
        }

        public bool Next(CsvUnit unit, out RecordBatch? batch)
        {
            batch = null;
            if (_batch >= unit.Batches.Count || Rows >= Max) return false;
            batch = unit.Batches[_batch]!;
            unit.Batches[_batch++] = null;
            if (reader._settings.Indexed is not null) RowNumbers.Shift(batch.Column(0), _records + firstRow);
            if (Rows + batch.Length > Max) batch = RowFilter.First(batch, (int)(Max - Rows));
            Rows += batch.Length;
            return true;
        }

        public void Done(CsvUnit unit)
        {
            _records += unit.Rows;
            if (unit.Lines >= 0) _lines += unit.Lines;
        }
    }

    private void Begin()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_input.ReadOnce && _read) throw new InvalidOperationException("A stream is read once.");
        _read = true;
    }

    // Units decoded ahead of the reader, as many as the threads, within a budget of input bytes.
    private OrderedWindow<CsvUnit> Window(CancellationToken ct)
    {
        int threads = _options.MaxDegreeOfParallelism;
        long max = _options.MaxRows ?? long.MaxValue;
        int unit = _options.UnitBytes;
        return new OrderedWindow<CsvUnit>(Math.Max(0, _input.Count), (i, token) => DecodeAsync(i, max, _known, useAsync: false, token), threads,
            _ => unit, 2L * threads * unit, ct, opened: _options.MaxRows is null);
    }

    // Whether unit i may exist: below the count, or (a stream) found as it is read, the window grown to it.
    private bool Has(OrderedWindow<CsvUnit> window, int i)
    {
        if (_input.Count >= 0) return i < _input.Count;
        window.Grow(i + 2 * _options.MaxDegreeOfParallelism);
        return true;
    }

    private async Task<CsvUnit> DecodeAsync(int index, long maxRows, TemporalPattern?[] groups, bool useAsync, CancellationToken ct)
    {
        using TextUnitBytes? bytes = await _input.GetAsync(index, useAsync, ct).ConfigureAwait(false);
        return Decode(bytes, index, maxRows, groups);
    }

    private CsvUnit Decode(TextUnitBytes? bytes, int index, long maxRows, TemporalPattern?[] groups)
    {
        if (bytes is null) return CsvUnit.EndOfInput;
        bool countLines = _input.ReadOnce;
        if (index < _startUnit)
            return new CsvUnit { Lines = countLines ? _settings.Rules.Tokenizer.CountLines(bytes.Memory.Span) : -1 };
        int from = index == _startUnit ? _startOffset : 0;
        CsvUnit unit = CsvDecode.Decode(bytes.Memory.Span, from, _settings, groups, maxRows, countLines);
        // A filtered unit numbers its kept rows itself.
        if (unit.Error is null && _settings.Indexed is not null && _settings.Filter is null) Numbered(unit, _settings.Indexed);
        return unit;
    }

    // The line ends in the units before unit index (random access: read again on the error path only).
    private async ValueTask<long> LinesBeforeAsync(int index, bool useAsync, CancellationToken ct)
    {
        long lines = 0;
        for (int i = 0; i < index; i++)
        {
            using TextUnitBytes? bytes = await _input.GetAsync(i, useAsync, ct).ConfigureAwait(false);
            if (bytes is not null) lines += _settings.Rules.Tokenizer.CountLines(bytes.Memory.Span);
        }
        return lines;
    }

    // A unit's batches with their rows' numbers first, from the unit's first record (shifted as they are delivered).
    private static void Numbered(CsvUnit unit, Schema indexed)
    {
        long first = 0;
        for (int k = 0; k < unit.Batches.Count; k++)
        {
            RecordBatch batch = unit.Batches[k]!;
            IArrowArray numbers;
            try
            {
                numbers = RowNumbers.Range(first, batch.Length);
            }
            catch
            {
                unit.Dispose();
                throw;
            }
            unit.Batches[k] = new RecordBatch(indexed, [numbers, .. batch.Arrays], batch.Length);
            first += batch.Length;
        }
    }

    // Two things disposed together: what the input owns (decoded text), then the source.
    private sealed class Pair(IDisposable first, IDisposable second) : IDisposable
    {
        public void Dispose()
        {
            first.Dispose();
            second.Dispose();
        }
    }

    /// <summary>Releases the input (and closes a file or stream the reader owns).</summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _input.Dispose();
        _owned?.Dispose();
    }

    /// <inheritdoc cref="Dispose"/>
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }
}
