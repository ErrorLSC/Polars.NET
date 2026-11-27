using System.Runtime.InteropServices;
using Apache.Arrow;
using Kaminari.Samidare.IR;

namespace Polars.NET.Core.IO.Plugins;

/// <summary>
/// What the engine holds a reference to through an I/O plugin handle: a <see cref="ScanResolver"/> (part of a query
/// plan) or a <see cref="ScanReaderBuilder"/> (part of a resolved scan). The engine clones and releases its
/// references from any thread; <see cref="OnReleased"/> runs once the last one is gone.
/// </summary>
/// <param name="name">Shown by explain and in errors (computed now: the engine asks for it from threads that must
/// not block).</param>
/// <param name="fingerprint">Equal for two objects only when they read the same data: the engine merges scans with
/// equal fingerprints (of the same plugin).</param>
public abstract class ScanPluginObject(string name, ulong fingerprint)
{
    private readonly object _lock = new();
    private int _references;
    private bool _released;
    private nint _handle;

    /// <summary>Shown by explain and in errors.</summary>
    public string Name { get; } = name;

    /// <summary>Equal for two objects only when they read the same data.</summary>
    public ulong Fingerprint { get; } = fingerprint;

    /// <summary>
    /// The engine dropped its last reference (also when it never took one: a scan that could not be made). Called on
    /// any thread, also the engine's async ones: release what the object holds without waiting for anything.
    /// </summary>
    protected virtual void OnReleased() { }

    /// <summary>A new engine reference: the handle the engine gets (the same for every reference).</summary>
    internal nint AddReference()
    {
        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_released, this);
            if (_references++ == 0) _handle = GCHandle.ToIntPtr(GCHandle.Alloc(this));
            return _handle;
        }
    }

    /// <summary>Drops an engine reference; the last one frees the handle and releases the object.</summary>
    internal void RemoveReference()
    {
        lock (_lock)
        {
            if (_references <= 0 || --_references > 0) return;
            GCHandle.FromIntPtr(_handle).Free();
            _handle = 0;
            _released = true;
        }
        OnReleased();
    }

    /// <summary>Releases an object the engine never took a reference to (a scan that could not be made).</summary>
    internal void ReleaseUnused()
    {
        lock (_lock)
        {
            if (_references > 0 || _released) return;
            _released = true;
        }
        OnReleased();
    }
}

/// <summary>
/// A LazyFrame source the engine resolves while optimizing a query (contract
/// <c>polars.io_plugin.lazyframe_resolver.v1</c>): it is handed the columns, the filters and the row limit the query
/// pushed down, and answers with the sources to read and the reader builder that reads them. The engine applies on
/// top whatever the scan does not (the filters not reported applied, the slice, the projection).
/// </summary>
/// <param name="name">Shown by explain, e.g. <c>csv[3 files]</c>.</param>
/// <param name="fingerprint">Equal only for resolvers that resolve to the same data.</param>
/// <param name="schema">Every column, decided when the scan is made (the engine asks for it while planning).</param>
public abstract class ScanResolver(string name, ulong fingerprint, Schema schema) : ScanPluginObject(name, fingerprint)
{
    /// <summary>Every column the source has.</summary>
    public Schema Schema { get; } = schema;

    /// <summary>
    /// The scan for what the query needs. Runs on a blocking thread of the engine and may do I/O. Returns null only
    /// when <see cref="ResolveRequest.ExistingVersionKey"/> is set and that resolution is still valid.
    /// </summary>
    public abstract ScanPlan? Resolve(ResolveRequest request);
}

/// <summary>What a query asks of a <see cref="ScanResolver"/>.</summary>
/// <param name="Projection">The columns to produce, in this order; null for all of them.</param>
/// <param name="Limit">The rows needed from the start (the engine also slices on top); never set with filters.</param>
/// <param name="Filters">The conjuncts of the pushed predicate: a row is kept when all of them are true.</param>
/// <param name="FilterColumns">The columns the filters reference.</param>
/// <param name="FilterOnlyColumnsFrom">The columns of <see cref="Projection"/> at and after this index are needed by
/// the filters only.</param>
/// <param name="ExistingVersionKey">The version key of the previous resolution with the same arguments, if any.</param>
/// <param name="TargetSources">How many sources the engine would like (a hint).</param>
/// <param name="Verbose">The engine runs verbose.</param>
public sealed record ResolveRequest(
    IReadOnlyList<string>? Projection,
    long? Limit,
    IReadOnlyList<PushedFilter> Filters,
    IReadOnlyList<string> FilterColumns,
    int? FilterOnlyColumnsFrom,
    string? ExistingVersionKey,
    int? TargetSources,
    bool Verbose);

/// <summary>A conjunct of the predicate a query pushed down to a <see cref="ScanResolver"/>.</summary>
/// <param name="Index">Its index among the request's filters (what <see cref="ScanPlan.AppliedFilters"/> names).</param>
/// <param name="Document">The engine's <c>filter_expr.v1</c> JSON document.</param>
/// <param name="Exact">The whole conjunct as an expression; null when part of it has no translation.</param>
/// <param name="Implied">A predicate the conjunct implies (true wherever the conjunct is true): the conjunct itself,
/// or what remains of it once the untranslatable sides of its <c>and</c>s are dropped; null when nothing remains. Rows
/// it rejects can be skipped; the engine still applies the conjunct unless it is reported applied.</param>
public sealed record PushedFilter(int Index, string Document, ScalarExpr? Exact, ScalarExpr? Implied);

/// <summary>A resolved scan: the sources, the builder of their readers, and what the scan applies itself.</summary>
/// <param name="Sources">What the readers read, one reader each (paths, partition ids, …): passed back verbatim.</param>
/// <param name="Reader">Builds the readers (its <see cref="ScanReaderBuilder.Schema"/> is the scan's); the engine
/// takes a reference to it.</param>
/// <param name="AppliedFilters">The indices of the request's filters the scan applies exactly.</param>
/// <param name="RowEstimate">The estimated row count.</param>
/// <param name="VersionKey">When set, the engine resolves again on every execution (passing it back); when null it
/// may reuse this resolution while the request does not change.</param>
public sealed record ScanPlan(
    IReadOnlyList<string> Sources,
    ScanReaderBuilder Reader,
    IReadOnlyList<int>? AppliedFilters = null,
    long? RowEstimate = null,
    string? VersionKey = null);

/// <summary>
/// Builds the readers of a resolved scan's sources (contract <c>polars.io_plugin.file_reader.v1</c>): immutable,
/// thread-safe, and holding whatever the resolution baked in (columns, filters, limit), which its fingerprint covers.
/// </summary>
/// <param name="name">Shown by explain and in errors.</param>
/// <param name="fingerprint">Equal only for builders that read the same data.</param>
/// <param name="schema">The columns of the scan's rows: the request's projection (every column without one). The
/// engine takes exactly these columns from each source (a source's other columns are not the scan's).</param>
public abstract class ScanReaderBuilder(string name, ulong fingerprint, Schema schema) : ScanPluginObject(name, fingerprint)
{
    /// <summary>The columns of the scan's rows.</summary>
    public Schema Schema { get; } = schema;

    /// <summary>Properties shown by explain.</summary>
    public virtual IReadOnlyList<KeyValuePair<string, string>> Explain => [];

    /// <summary>How the engine may schedule the readers.</summary>
    public virtual ScanCapabilities Capabilities => default;

    /// <summary>One execution of the scan: state its readers share. Must not block (open lazily).</summary>
    public abstract ScanExecution BeginScan();
}

/// <summary>How the engine may schedule a scan's readers.</summary>
/// <param name="Unordered">Sources may be read in any order when the query does not need the row order.</param>
/// <param name="RemoteSources">Sources have remote latency.</param>
/// <param name="MaxConcurrentReaders">At most this many readers run at once.</param>
/// <param name="RemotePreInit">Readers to open ahead when the sources are remote.</param>
public readonly record struct ScanCapabilities(bool Unordered = false, bool RemoteSources = false, int? MaxConcurrentReaders = null,
    int? RemotePreInit = null);

/// <summary>One execution of a scan; disposed when it ends (its readers are disposed before).</summary>
public abstract class ScanExecution : IDisposable
{
    /// <summary>The reader of one source. Must not block (open lazily, in the reader's members).</summary>
    public abstract SourceReader OpenSource(string source);

    public virtual void Dispose() => GC.SuppressFinalize(this);
}

/// <summary>Reads one source. Used by one engine thread at a time; its members may block.</summary>
public abstract class SourceReader : IDisposable
{
    /// <summary>The source's columns: those of the scan (<see cref="ScanReaderBuilder.Schema"/>) with this source's
    /// types, and possibly more (the bridge reports the scan's only).</summary>
    public abstract Schema Schema { get; }

    /// <summary>The source's row count (asked when no column is needed, or to place a slice from the end).</summary>
    public abstract long CountRows();

    /// <summary>
    /// The batches of the given columns (never empty). A batch may have more columns: the bridge exports the asked
    /// ones by name, typed as in <see cref="Schema"/>. The token is cancelled when the engine stops the read.
    /// </summary>
    public abstract IEnumerable<RecordBatch> Read(IReadOnlyList<string> columns, CancellationToken cancellation);

    public virtual void Dispose() => GC.SuppressFinalize(this);
}
