using System.IO.Hashing;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Kaminari.Samidare.IR;
using Polars.NET.Core.Wrappers;
using static Polars.NET.Core.IO.ManagedFrames;

namespace Polars.NET.Core.IO.Plugins;

/// <summary>What one read of a managed scan asks of its reader.</summary>
/// <param name="Columns">The reader's columns to read (the row index apart), in order; null for all.</param>
/// <param name="Indexed">Whether the reader numbers the rows (the row index asked).</param>
/// <param name="Limit">The rows read at most.</param>
/// <param name="Filter">Rows the reader may skip: those this predicate does not hold for (null: none). The engine
/// applies its filters itself after the read; the reader only drops rows early.</param>
internal readonly record struct ScanRun(string[]? Columns, bool Indexed, long? Limit, ScalarExpr? Filter);

/// <summary>
/// The managed readers' lazy scans (CSV, JSON, …) as engine resolvers: one source (the whole input: the managed reader
/// reads its files, in parallel, pruning by partitions, itself), each resolution baking in the columns, the row limit
/// and the filter the query pushed (as the Samidare IR each pushed filter implies).
/// </summary>
internal static class ManagedScans
{
    private static long _identities;

    /// <summary>
    /// A LazyFrame over a managed scan. <paramref name="owner"/> (the dataset, its store, a copy or pin of the memory read)
    /// is released once the engine releases the scan and every resolution of it, or now when the frame cannot be made.
    /// </summary>
    /// <param name="name">Shown by explain, e.g. <c>csv[data/*.csv]</c> (no secrets).</param>
    /// <param name="fingerprint">Equal only for scans of the same data (<see cref="Fingerprint"/>, <see cref="Identity"/>).</param>
    /// <param name="scan">The scan's columns as the engine takes them, and how batches are delivered.</param>
    /// <param name="read">The reader's raw batches for a run.</param>
    /// <param name="rowIndex">The row index's name (the scan's first column), or null.</param>
    /// <param name="appliesExactly">Whether the reader, given a filter, delivers exactly the rows it holds for (asked
    /// while planning: open a reader, read nothing).</param>
    /// <param name="owner">What the reads read from.</param>
    /// <param name="filters">Whether the reader takes filters (else every filter stays the engine's).</param>
    /// <param name="singlePass">Whether the input can be read only once (a DataReader): the first read (or count)
    /// consumes it, a later one fails with an error saying so.</param>
    /// <param name="engineOnly">Columns the reader takes no filter over (a row index it numbers itself, as an input
    /// column).</param>
    /// <param name="count">The rows a run delivers, counted without reading what need not be read (a footer's row
    /// counts); null counts the rows of a read of one column.</param>
    public static LazyFrameHandle Open(string name, ulong fingerprint, Scan scan, string? rowIndex,
        Func<ScanRun, IEnumerable<RecordBatch>> read, Func<ScalarExpr, bool> appliesExactly, IDisposable owner,
        bool filters = true, bool singlePass = false, IReadOnlyList<string>? engineOnly = null, Func<ScanRun, long>? count = null)
    {
        var shared = new SharedOwner(owner);
        ManagedScanResolver resolver;
        try
        {
            resolver = new ManagedScanResolver(name, fingerprint, scan, rowIndex, read, count, filters ? appliesExactly : null,
                singlePass ? new Consumable(name) : null, shared, engineOnly ?? []);
        }
        catch
        {
            shared.Release();
            throw;
        }
        return IoPluginBridge.Scan(resolver);
    }

    /// <summary>The scan limited to its first rows (the engine pushes the slice down as the read's limit).</summary>
    public static LazyFrameHandle Head(LazyFrameHandle lf, long? rows) =>
        rows is { } n ? PolarsWrapper.LazySlice(lf, 0, (uint)Math.Min(n, uint.MaxValue)) : lf;

    /// <summary>A fingerprint unique to this scan (sources read once, in memory, streams).</summary>
    public static ulong Identity() => new FingerprintBuilder().Add("identity").Add(Interlocked.Increment(ref _identities)).Value;

    /// <summary>A path read's name: the path without what may hold secrets (a URL's query and credentials).</summary>
    public static string Shown(string path)
    {
        if (!Uri.TryCreate(path, UriKind.Absolute, out Uri? uri) || uri.IsFile) return path;
        return $"{uri.Scheme}://{uri.Host}{(uri.IsDefaultPort ? "" : $":{uri.Port}")}{uri.AbsolutePath}";
    }

    /// <summary>
    /// The filter a read applies, and the pushed filters it applies exactly (the engine leaves those out). A pushed
    /// filter is applied exactly when it translates whole, reads the input's columns only (not the row index, which
    /// the engine's filters see numbered before any row is skipped), holds no cast (the document does not say whether
    /// a cast is strict: a strict one that fails must fail the query, not drop the row) and the reader applies it
    /// exactly. The other filters' implied predicates only let the reader skip rows, and are left out when they would
    /// make the filter inexact.
    /// </summary>
    internal static (ScalarExpr? Filter, int[] Applied) ReadFilter(IReadOnlyList<PushedFilter> filters, Schema scan, string? rowIndex,
        Func<ScalarExpr, bool> appliesExactly, IReadOnlyList<string>? engineOnly = null)
    {
        List<PushedFilter> exact = [];
        List<ScalarExpr> skipping = [];
        foreach (PushedFilter filter in filters)
        {
            if (filter.Exact is { } whole && OverInput(whole) && !HasCast(whole) && Exactly(whole)) exact.Add(filter);
            else if (filter.Implied is { } implied && OverInput(implied)) skipping.Add(implied);
        }
        int[] applied = [.. exact.Select(static f => f.Index)];
        ScalarExpr? exactFilter = And([.. exact.Select(static f => f.Exact!)]);
        ScalarExpr? all = And([.. exact.Select(static f => f.Exact!), .. skipping]);
        if (exact.Count == 0 || skipping.Count == 0 || all is null || Exactly(all)) return (all, applied);
        return (exactFilter, applied);

        // A reader that cannot tell (its probe failed) applies nothing exactly: the engine filters.
        bool Exactly(ScalarExpr filter)
        {
            try
            {
                return appliesExactly(filter);
            }
            catch (Exception e) when (e is not OutOfMemoryException)
            {
                return false;
            }
        }

        bool OverInput(ScalarExpr expr) =>
            ScalarExprOps.collectColumns(expr).All(c => c != rowIndex && engineOnly?.Contains(c) != true && scan.GetFieldIndex(c) >= 0);

        static ScalarExpr? And(ScalarExpr[] parts) =>
            parts.Length == 0 ? null : parts.Skip(1).Aggregate(parts[0], static (a, b) => ScalarExpr.NewBinary(BinaryOp.And, a, b));
    }

    /// <summary>Whether the expression holds a cast (or a node the translator does not make, counted as one).</summary>
    private static bool HasCast(ScalarExpr expr) => expr switch
    {
        ScalarExpr.Column or ScalarExpr.Literal => false,
        ScalarExpr.Binary b => HasCast(b.left) || HasCast(b.right),
        ScalarExpr.Unary u => HasCast(u.expr),
        ScalarExpr.InList i => HasCast(i.expr),
        ScalarExpr.Function f => f.args.Any(HasCast),
        _ => true,
    };

    /// <summary>What the scan and its resolutions share; released by the last of them.</summary>
    private sealed class SharedOwner(IDisposable owner)
    {
        private int _references = 1;

        public SharedOwner Acquire()
        {
            if (Interlocked.Increment(ref _references) <= 1) throw new ObjectDisposedException(nameof(SharedOwner));
            return this;
        }

        public void Release()
        {
            if (Interlocked.Decrement(ref _references) == 0) owner.Dispose();
        }
    }

    /// <summary>An input read once: the first read takes it.</summary>
    private sealed class Consumable(string name)
    {
        private int _taken;

        public void Take()
        {
            if (Interlocked.Exchange(ref _taken, 1) != 0)
                throw new InvalidOperationException(
                    $"The {name} scan reads a source that can be read only once (a DataReader), and an earlier run of the query read it: scan a factory that opens a new reader instead (ScanDatabase(Func<IDataReader>)), or collect the frame once and reuse it.");
        }
    }

    private sealed class ManagedScanResolver(string name, ulong fingerprint, Scan scan, string? rowIndex,
        Func<ScanRun, IEnumerable<RecordBatch>> read, Func<ScanRun, long>? count, Func<ScalarExpr, bool>? appliesExactly, Consumable? consumable,
        SharedOwner owner,
        IReadOnlyList<string> engineOnly)
        : ScanResolver(name, fingerprint, scan.Schema)
    {
        public override ScanPlan Resolve(ResolveRequest request)
        {
            Schema schema = request.Projection is { } projection
                ? new Schema([.. projection.Select(c => Schema.GetFieldByName(c) ?? throw new InvalidOperationException($"The scan has no column \"{c}\"."))], Schema.Metadata)
                : Schema;
            var (filter, applied) = appliesExactly is null ? (null, []) : ReadFilter(request.Filters, Schema, rowIndex, appliesExactly, engineOnly);
            ulong print = new FingerprintBuilder()
                .Add(Fingerprint)
                .Add(request.Projection)
                .Add(filter is null ? null : request.Filters.Select(f => f.Document).ToList())
                .Add(request.Limit ?? -1)
                .Value;
            var builder = new ManagedScanBuilder($"{Name} reader", print, schema, scan, rowIndex, read, count, consumable, owner.Acquire(), request.Limit,
                filter);
            return new ScanPlan([Name], builder, applied);
        }

        protected override void OnReleased() => owner.Release();
    }

    private sealed class ManagedScanBuilder(string name, ulong fingerprint, Schema schema, Scan scan, string? rowIndex,
        Func<ScanRun, IEnumerable<RecordBatch>> read, Func<ScanRun, long>? count, Consumable? consumable, SharedOwner owner, long? limit,
        ScalarExpr? filter)
        : ScanReaderBuilder(name, fingerprint, schema)
    {
        public override IReadOnlyList<KeyValuePair<string, string>> Explain =>
        [
            .. limit is { } n ? (KeyValuePair<string, string>[])[new("limit", n.ToString(System.Globalization.CultureInfo.InvariantCulture))] : [],
            .. filter is not null ? (KeyValuePair<string, string>[])[new("filters rows by", filter.ToString())] : [],
        ];

        public override ScanExecution BeginScan() => new Execution(this);

        protected override void OnReleased() => owner.Release();

        private Scan Shape => scan;

        private string? RowIndex => rowIndex;

        private IEnumerable<RecordBatch> Batches(ScanRun run) => read(run);

        private Consumable? Consumable => consumable;

        private Func<ScanRun, long>? Count => count;

        private ScanRun Run(string[]? columns, bool indexed) => new(columns, indexed, limit, filter);

        private sealed class Execution(ManagedScanBuilder builder) : ScanExecution
        {
            public override SourceReader OpenSource(string source) => new Reader(builder);
        }

        private sealed class Reader(ManagedScanBuilder builder) : SourceReader
        {
            public override Schema Schema => builder.Shape.Schema;

            public override long CountRows()
            {
                if (builder.Count is { } count && builder.Consumable is null) return count(builder.Run(null, indexed: false));
                // One column of the input is enough to count the rows a read delivers (a single-pass input is
                // consumed by the count: the engine asks for the count when it reads no column).
                Field? first = Schema.FieldsList.FirstOrDefault(f => f.Name != builder.RowIndex);
                long rows = 0;
                foreach (RecordBatch batch in Read(first is null ? [builder.RowIndex!] : [first.Name], CancellationToken.None))
                {
                    rows += batch.Length;
                    batch.Dispose();
                }
                return rows;
            }

            public override IEnumerable<RecordBatch> Read(IReadOnlyList<string> columns, CancellationToken cancellation)
            {
                builder.Consumable?.Take();
                bool indexed = builder.RowIndex is { } index && columns.Contains(index);
                string[] reading = [.. columns.Where(c => c != builder.RowIndex)];
                return Delivered(builder.Batches(builder.Run(reading, indexed)), indexed, cancellation);
            }

            private IEnumerable<RecordBatch> Delivered(IEnumerable<RecordBatch> batches, bool indexed, CancellationToken cancellation)
            {
                foreach (RecordBatch batch in batches)
                {
                    if (cancellation.IsCancellationRequested)
                    {
                        batch.Dispose();
                        yield break;
                    }
                    yield return builder.Shape.Deliver(batch, indexed);
                }
            }
        }
    }
}

/// <summary>
/// A 64-bit fingerprint of a scan's description (XxHash3 over length-prefixed parts): equal descriptions, equal
/// fingerprints; the parts never run into each other. Stable within a process (plans are never serialized).
/// </summary>
internal sealed class FingerprintBuilder
{
    private readonly XxHash3 _hash = new();

    public ulong Value => _hash.GetCurrentHashAsUInt64();

    public FingerprintBuilder Add(string? text)
    {
        if (text is null) return Add(-1L);
        Add((long)text.Length);
        // The UTF-16 code units as they are: fingerprints are compared within one process only.
        _hash.Append(MemoryMarshal.AsBytes(text.AsSpan()));
        return this;
    }

    public FingerprintBuilder Add(long value)
    {
        Span<byte> bytes = stackalloc byte[sizeof(long)];
        System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(bytes, value);
        _hash.Append(bytes);
        return this;
    }

    public FingerprintBuilder Add(ulong value) => Add(unchecked((long)value));

    public FingerprintBuilder Add(bool value) => Add(value ? 1L : 0L);

    public FingerprintBuilder Add(char value) => Add((long)value);

    public FingerprintBuilder Add(char? value) => Add(value is { } c ? c : -1L);

    public FingerprintBuilder Add(ulong? value) => value is { } v ? Add(1L).Add(v) : Add(-1L);

    /// <summary>A store's settings: its provider and options (an endpoint, a bucket's region …) say which data a URL names.</summary>
    public FingerprintBuilder Add(Polars.NET.Core.Delta.CloudArgs? cloud) =>
        cloud is null ? Add(-1L) : Add((long)cloud.Provider).Add(cloud.Keys).Add(cloud.Values);

    /// <summary>An encoding by its code page and name (null: UTF-8, as the readers default).</summary>
    public FingerprintBuilder Add(System.Text.Encoding? encoding) =>
        encoding is null ? Add(-1L) : Add((long)encoding.CodePage).Add(encoding.WebName);

    public FingerprintBuilder Add(IEnumerable<string>? texts)
    {
        if (texts is null) return Add(-1L);
        IReadOnlyList<string> list = texts as IReadOnlyList<string> ?? [.. texts];
        Add((long)list.Count);
        foreach (string text in list) Add(text);
        return this;
    }

    /// <summary>Every column: its name, its type (structurally, as the IR keys types) and whether it holds nulls.</summary>
    public FingerprintBuilder Add(Schema schema)
    {
        Add((long)schema.FieldsList.Count);
        foreach (Field field in schema.FieldsList)
        {
            Add(field.Name);
            Add(LogicalType.NewArrow(field.DataType).Display);
            Add(field.IsNullable);
        }
        return this;
    }
}
