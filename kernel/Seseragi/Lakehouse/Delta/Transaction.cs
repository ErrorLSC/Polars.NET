using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Log;
using Kaminari.Seseragi.Lakehouse.Engine;

namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>
/// Another writer committed first, in a way this transaction cannot be moved past: what it committed conflicts with
/// what this transaction read or changes (or a table was created where one now exists, or the retries ran out).
/// The transaction did not happen.
/// </summary>
public sealed class DeltaCommitConflictException(long version, string reason)
    : DeltaException($"The commit conflicts with version {version}, written by another writer: {reason}.")
{
    /// <summary>The version of the other writer's commit.</summary>
    public long Version { get; } = version;

    /// <summary>What conflicts.</summary>
    public string Reason { get; } = reason;
}

/// <summary>Which concurrent commits a transaction tolerates (the isolation levels of Delta).</summary>
public enum DeltaIsolationLevel
{
    /// <summary>
    /// The default: files another writer appended blindly (without reading the table) may be committed alongside
    /// this transaction even where this transaction read; everything else as <see cref="Serializable"/>.
    /// </summary>
    WriteSerializable,

    /// <summary>Any file another writer added where this transaction read is a conflict.</summary>
    Serializable,
}

/// <summary>
/// A set of actions committed to a table as one new version, or not at all. The commit is a put-if-absent of the
/// next commit file, so of two writers starting from the same version exactly one succeeds.
/// </summary>
/// <remarks>
/// The transaction checks what the protocol asks of every writer and the table's writer features. It does not
/// write data files: the actions point at files the engine has written. Writing the change data files an update or
/// delete needs on a table with the change data feed is up to the caller, as is keeping the data files in the
/// table's column mapping.
/// </remarks>
public sealed class Transaction
{
    /// <summary>The name this kernel gives in <c>commitInfo.engineInfo</c>.</summary>
    public const string EngineInfo = "Kaminari.Seseragi.Lakehouse";

    private readonly IWritableFileSystem _fileSystem;
    private readonly Snapshot? _read;
    private readonly List<DeltaAction> _actions = [];
    private readonly Dictionary<string, string> _operationParameters = new(StringComparer.Ordinal);
    private readonly Dictionary<string, long> _operationMetrics = new(StringComparer.Ordinal);
    private ProtocolAction? _protocol;
    private MetadataAction? _metadata;
    private readonly List<Kaminari.Seseragi.Lakehouse.Skipping.Predicate> _readPredicates = [];
    private bool _readWholeTable;
    private readonly HashSet<(string Path, string? Dv)> _readFiles = [];
    /// <summary>The row id high water mark of the newest commit seen (the read state, then concurrent commits).</summary>
    private long? _rowIdHighWaterMark;

    private Transaction(IWritableFileSystem fileSystem, Snapshot? read, string operation)
    {
        _fileSystem = fileSystem;
        _read = read;
        Operation = operation;
    }

    /// <summary>Starts a transaction that creates a table: it commits version 0 with this protocol and metadata.</summary>
    public static Transaction Create(IWritableFileSystem fileSystem, ProtocolAction protocol, MetadataAction metadata, string operation = "CREATE TABLE")
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(protocol);
        ArgumentNullException.ThrowIfNull(metadata);
        var transaction = new Transaction(fileSystem, null, operation);
        transaction.SetProtocol(protocol);
        transaction.SetMetadata(metadata);
        return transaction;
    }

    /// <summary>Starts a transaction on the table state it read; it commits the version after that state.</summary>
    public static Transaction Start(IWritableFileSystem fileSystem, Snapshot read, string operation)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(read);
        TableFeatures.EnsureWritable(read.Protocol, read.Metadata);
        return new Transaction(fileSystem, read, operation);
    }

    /// <summary>How strictly concurrent commits are checked; WriteSerializable by default.</summary>
    public DeltaIsolationLevel IsolationLevel { get; set; } = DeltaIsolationLevel.WriteSerializable;

    /// <summary>
    /// Records that the transaction read the rows matching <paramref name="predicate"/> (null: the whole table), so a
    /// concurrent commit adding such rows is a conflict. A transaction that records no read is a blind append.
    /// </summary>
    public Transaction ReadPredicate(Kaminari.Seseragi.Lakehouse.Skipping.Predicate? predicate)
    {
        if (predicate is null) _readWholeTable = true;
        else _readPredicates.Add(predicate);
        return this;
    }

    /// <summary>Records data files the transaction read, so a concurrent commit removing one of them is a conflict.</summary>
    public Transaction ReadFiles(IEnumerable<AddAction> files)
    {
        ArgumentNullException.ThrowIfNull(files);
        foreach (AddAction file in files) _readFiles.Add((file.Path, file.DeletionVector?.UniqueId));
        return this;
    }

    private bool ReadAnything => _readWholeTable || _readPredicates.Count > 0 || _readFiles.Count > 0;

    /// <summary>The operation name recorded in <c>commitInfo</c>, such as WRITE or DELETE.</summary>
    public string Operation { get; set; }

    /// <summary>The version this transaction read; -1 when it creates the table.</summary>
    public long ReadVersion => _read?.Version ?? -1;

    /// <summary>The actions added so far, in order.</summary>
    public IReadOnlyList<DeltaAction> Actions => _actions;

    /// <summary>Records an operation parameter in <c>commitInfo.operationParameters</c>.</summary>
    public Transaction WithParameter(string name, string value)
    {
        _operationParameters[name] = value;
        return this;
    }

    /// <summary>Records an operation metric in <c>commitInfo.operationMetrics</c>, such as <c>numDeletedRows</c>.</summary>
    public Transaction WithMetric(string name, long value)
    {
        _operationMetrics[name] = value;
        return this;
    }

    /// <summary>Adds a data file, change file, file removal, application transaction or domain metadata.</summary>
    public Transaction Add(DeltaAction action)
    {
        switch (action)
        {
            case ProtocolAction protocol:
                return SetProtocol(protocol);
            case MetadataAction metadata:
                return SetMetadata(metadata);
            case CommitInfoAction:
                throw new ArgumentException("The transaction writes its own commitInfo.", nameof(action));
            case DomainMetadataAction { Domain: RowTracking.Domain }:
                throw new ArgumentException("The transaction records the row id high water mark itself.", nameof(action));
            case null:
                throw new ArgumentNullException(nameof(action));
            default:
                _actions.Add(action);
                return this;
        }
    }

    /// <summary>Changes the protocol in this commit.</summary>
    public Transaction SetProtocol(ProtocolAction protocol)
    {
        if (_protocol is not null) throw new InvalidOperationException("A commit holds at most one protocol action.");
        _protocol = protocol ?? throw new ArgumentNullException(nameof(protocol));
        return this;
    }

    /// <summary>Changes the metadata (schema, partitioning, properties) in this commit.</summary>
    public Transaction SetMetadata(MetadataAction metadata)
    {
        if (_metadata is not null) throw new InvalidOperationException("A commit holds at most one metaData action.");
        _metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
        return this;
    }

    /// <summary>
    /// Writes the commit. Returns the version it became. When another writer committed the next version first, the
    /// commits in between are checked against this transaction; if none conflicts, the transaction is committed
    /// after them.
    /// </summary>
    /// <param name="now">The writer's clock; the current time when null.</param>
    /// <param name="maxRetries">How often to move past concurrent commits before giving up.</param>
    /// <exception cref="DeltaCommitConflictException">A concurrent commit conflicts, or the retries ran out.</exception>
    /// <exception cref="DeltaProtocolException">The commit would break a rule of the protocol or of the table.</exception>
    public long Commit(DateTimeOffset? now = null, int maxRetries = 20)
    {
        ProtocolAction protocol = _protocol ?? _read?.Protocol ?? throw new DeltaProtocolException("A new table needs a protocol action.");
        MetadataAction metadata = _metadata ?? _read?.Metadata ?? throw new DeltaProtocolException("A new table needs a metaData action.");
        TableFeatures.EnsureWritable(protocol, metadata);
        Validate(metadata);
        long timestamp = (now ?? DateTimeOffset.UtcNow).ToUnixTimeMilliseconds();

        // Suspension is read from the metadata this commit leaves (Spark reads the snapshot's): one ALTER can then
        // lift it and turn tracking on, with the backfilled files getting their ids in that same commit.
        bool rowTracking = !RowTracking.IsSuspended(metadata) && RowTracking.IsSupported(protocol);
        _rowIdHighWaterMark = _read is null ? null : RowTracking.HighWaterMark(_read.DomainMetadata);

        long version = ReadVersion + 1;
        for (int attempt = 0; ; attempt++)
        {
            long? inCommitTimestamp = InCommitTimestamp(metadata, timestamp, version - 1);
            var actions = new List<DeltaAction>(_actions.Count + 4)
            {
                // The commitInfo goes first: with in-commit timestamps the protocol requires it there.
                CommitInfo(timestamp, inCommitTimestamp),
            };
            if (_protocol is not null) actions.Add(_protocol);
            if (_metadata is not null) actions.Add(_metadata);
            if (rowTracking)
            {
                // Row ids and default commit versions depend on the version this attempt commits and on the ids
                // concurrent commits assigned: given afresh each attempt.
                long? mark = _rowIdHighWaterMark;
                var (tracked, assigned) = RowTracking.AssignFresh(_actions, ref mark, version);
                actions.AddRange(tracked);
                if (assigned && mark is long newMark) actions.Add(RowTracking.DomainFor(newMark));
            }
            else
            {
                actions.AddRange(_actions);
            }

            if (_fileSystem.TryCreate(LogFile.CommitPath(version), ActionWriter.WriteCommit(actions)))
                return version;

            if (_read is null) throw new DeltaCommitConflictException(version, "the table was created concurrently");
            if (attempt >= maxRetries) throw new DeltaCommitConflictException(version, $"still behind after {maxRetries} retries");

            // Check every commit made since the last attempt, then try after the newest of them.
            while (_fileSystem.ReadAll(LogFile.CommitPath(version)) is byte[] winner)
            {
                CheckConflicts(version, ActionParser.ParseCommit(winner));
                version++;
            }
        }
    }

    /// <summary>Throws when a concurrent commit conflicts with what this transaction read or changes.</summary>
    private void CheckConflicts(long version, List<DeltaAction> winner)
    {
        var info = winner.OfType<CommitInfoAction>().FirstOrDefault();
        bool winnerBlindAppend = info is not null && IsBlindAppend(info);

        var ourRemoves = _actions.OfType<RemoveAction>().Select(r => (r.Path, r.DeletionVector?.UniqueId)).ToHashSet();
        var ourApps = _actions.OfType<TxnAction>().Select(t => t.AppId).ToHashSet(StringComparer.Ordinal);
        var ourDomains = _actions.OfType<DomainMetadataAction>().Select(d => d.Domain).ToHashSet(StringComparer.Ordinal);
        Skipping.FileFilter[] filters = [.. _readPredicates.Select(p => new Skipping.FileFilter(_read!.Schema, _read.PartitionColumns, p))];

        foreach (DeltaAction action in winner)
        {
            switch (action)
            {
                case DomainMetadataAction { Domain: RowTracking.Domain } rowIds:
                    // Not a conflict: this transaction takes ids after the ones the other writer assigned.
                    if (RowTracking.HighWaterMark(rowIds) is long mark && mark > (_rowIdHighWaterMark ?? -1)) _rowIdHighWaterMark = mark;
                    break;
                case ProtocolAction:
                    throw new DeltaCommitConflictException(version, "it changed the protocol");
                case MetadataAction:
                    throw new DeltaCommitConflictException(version, "it changed the table metadata");
                case TxnAction txn when ourApps.Contains(txn.AppId):
                    throw new DeltaCommitConflictException(version, $"it also committed for application '{txn.AppId}'");
                case DomainMetadataAction domain when ourDomains.Contains(domain.Domain):
                    throw new DeltaCommitConflictException(version, $"it also changed the domain '{domain.Domain}'");
                case RemoveAction remove when _readFiles.Contains((remove.Path, remove.DeletionVector?.UniqueId)):
                    throw new DeltaCommitConflictException(version, $"it removed the file {remove.Path}, which this transaction read");
                case RemoveAction remove when ourRemoves.Contains((remove.Path, remove.DeletionVector?.UniqueId)):
                    throw new DeltaCommitConflictException(version, $"it removed the file {remove.Path}, which this transaction removes too");
                case AddAction { DataChange: true } add when !(winnerBlindAppend && IsolationLevel == DeltaIsolationLevel.WriteSerializable):
                    if (_readWholeTable || filters.Any(f => f.MightMatch(add)))
                        throw new DeltaCommitConflictException(version, $"it added the file {add.Path}, where this transaction read");
                    break;
            }
        }

        // A transaction that changes the protocol or metadata does not move past any concurrent commit.
        if ((_protocol is not null || _metadata is not null) && winner.Any(a => a is not CommitInfoAction))
            throw new DeltaCommitConflictException(version, "this transaction changes the protocol or metadata");
    }

    private static bool IsBlindAppend(CommitInfoAction info)
    {
        using JsonDocument doc = JsonDocument.Parse(info.Json);
        return doc.RootElement.TryGetProperty("isBlindAppend", out JsonElement v) && v.ValueKind == JsonValueKind.True;
    }

    private void Validate(MetadataAction metadata)
    {
        bool appendOnly = metadata.Configuration.TryGetValue(TableFeatures.AppendOnlyKey, out string? flag)
            && string.Equals(flag, "true", StringComparison.OrdinalIgnoreCase);
        var added = new HashSet<(string, string?)>();
        foreach (DeltaAction action in _actions)
        {
            switch (action)
            {
                case RemoveAction { DataChange: true } when appendOnly:
                    throw new DeltaProtocolException("The table is append-only: its data cannot be removed.");
                case AddAction add when !added.Add((add.Path, add.DeletionVector?.UniqueId)):
                    throw new DeltaProtocolException($"The file {add.Path} is added twice in one commit.");
                case AddAction { DeletionVector: not null } when !SupportsDeletionVectors(_protocol ?? _read!.Protocol):
                    throw new DeltaProtocolException("The table does not support deletion vectors.");
            }
        }
    }

    private static bool SupportsDeletionVectors(ProtocolAction protocol) =>
        protocol.MinWriterVersion >= 7 && (protocol.WriterFeatures ?? []).Contains(TableFeatures.DeletionVectors);

    /// <summary>The in-commit timestamp to write, or null when the table does not use them.</summary>
    private long? InCommitTimestamp(MetadataAction metadata, long now, long previousVersion)
    {
        if (!IsOn(metadata)) return null;
        if (_read is null) return now; // on from the first version

        if (!IsOn(_read.Metadata))
        {
            // Turned on by this commit: the metadata names this version and its in-commit timestamp (as Spark does).
            if (metadata.Configuration.TryGetValue(CommitTimeline.EnablementVersionKey, out string? v) && v == (previousVersion + 1).ToString(System.Globalization.CultureInfo.InvariantCulture)
                && metadata.Configuration.TryGetValue(CommitTimeline.EnablementTimestampKey, out string? t) && long.TryParse(t, out long enabledAt))
                return enabledAt;
            throw new DeltaProtocolException(
                "Turning in-commit timestamps on for an existing table needs the enablement version and timestamp in the same commit; set the property through the table properties.");
        }

        // Strictly after the previous commit's own in-commit timestamp, whatever the clock says.
        byte[] previous = _fileSystem.ReadAll(LogFile.CommitPath(previousVersion))
            ?? throw new DeltaException($"The commit of version {previousVersion} is missing.");
        long? last = (ActionParser.ParseCommit(previous).OfType<CommitInfoAction>().FirstOrDefault()?.InCommitTimestamp) ?? throw new DeltaException($"The commit of version {previousVersion} has no in-commit timestamp.");
        return Math.Max(now, last.Value + 1);

        static bool IsOn(MetadataAction m) =>
            m.Configuration.TryGetValue(CommitTimeline.InCommitTimestampsKey, out string? on)
            && string.Equals(on, "true", StringComparison.OrdinalIgnoreCase);
    }

    private CommitInfoAction CommitInfo(long timestamp, long? inCommitTimestamp)
    {
        bool blindAppend = _read is not null && !ReadAnything && _protocol is null && _metadata is null
            && _actions.All(a => a is AddAction { DataChange: true } or CdcAction or TxnAction);
        using var stream = new MemoryStream();
        using (var json = new Utf8JsonWriter(stream, Kaminari.Seseragi.Lakehouse.Delta.Actions.ActionWriter.JsonOptions))
        {
            json.WriteStartObject();
            if (inCommitTimestamp is long ict) json.WriteNumber("inCommitTimestamp", ict);
            json.WriteNumber("timestamp", timestamp);
            json.WriteString("operation", Operation);
            json.WriteStartObject("operationParameters");
            foreach (var (name, value) in _operationParameters) json.WriteString(name, value);
            json.WriteEndObject();
            if (_operationMetrics.Count > 0)
            {
                // Written as strings, as Spark writes them.
                json.WriteStartObject("operationMetrics");
                foreach (var (name, value) in _operationMetrics) json.WriteString(name, value.ToString(System.Globalization.CultureInfo.InvariantCulture));
                json.WriteEndObject();
            }
            if (_read is not null) json.WriteNumber("readVersion", _read.Version);
            json.WriteBoolean("isBlindAppend", blindAppend);
            json.WriteString("engineInfo", EngineInfo);
            json.WriteEndObject();
        }
        return new CommitInfoAction
        {
            Json = System.Text.Encoding.UTF8.GetString(stream.ToArray()),
            Timestamp = timestamp,
            InCommitTimestamp = inCommitTimestamp,
            Operation = Operation,
        };
    }
}
