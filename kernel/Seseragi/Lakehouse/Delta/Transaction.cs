using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Log;
using Kaminari.Seseragi.Lakehouse.Engine;

namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>
/// Another writer committed the version this transaction meant to write. The transaction did not happen; whether
/// it may be retried on the newer state is for the conflict check to decide.
/// </summary>
public sealed class DeltaCommitConflictException(long version)
    : DeltaException($"Version {version} was committed by another writer first.")
{
    /// <summary>The version that was taken.</summary>
    public long Version { get; } = version;
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
    private ProtocolAction? _protocol;
    private MetadataAction? _metadata;

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

    /// <summary>Writes the commit. Returns the version it became.</summary>
    /// <param name="now">The writer's clock; the current time when null.</param>
    /// <exception cref="DeltaCommitConflictException">Another writer committed that version first.</exception>
    /// <exception cref="DeltaProtocolException">The commit would break a rule of the protocol or of the table.</exception>
    public long Commit(DateTimeOffset? now = null)
    {
        long version = ReadVersion + 1;
        ProtocolAction protocol = _protocol ?? _read?.Protocol ?? throw new DeltaProtocolException("A new table needs a protocol action.");
        MetadataAction metadata = _metadata ?? _read?.Metadata ?? throw new DeltaProtocolException("A new table needs a metaData action.");
        TableFeatures.EnsureWritable(protocol, metadata);
        Validate(metadata);

        long timestamp = (now ?? DateTimeOffset.UtcNow).ToUnixTimeMilliseconds();
        long? inCommitTimestamp = InCommitTimestamp(metadata, timestamp);

        var actions = new List<DeltaAction>(_actions.Count + 3)
        {
            // The commitInfo goes first: with in-commit timestamps the protocol requires it there.
            CommitInfo(timestamp, inCommitTimestamp),
        };
        if (_protocol is not null) actions.Add(_protocol);
        if (_metadata is not null) actions.Add(_metadata);
        actions.AddRange(_actions);

        byte[] content = ActionWriter.WriteCommit(actions);
        if (!_fileSystem.TryCreate(LogFile.CommitPath(version), content))
            throw new DeltaCommitConflictException(version);
        return version;
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
    private long? InCommitTimestamp(MetadataAction metadata, long now)
    {
        if (!IsOn(metadata)) return null;
        if (_read is null) return now; // on from the first version

        if (!IsOn(_read.Metadata))
            throw new DeltaProtocolException("Turning in-commit timestamps on for an existing table is not supported yet.");

        // Strictly after the previous commit's own in-commit timestamp, whatever the clock says.
        byte[] previous = _fileSystem.ReadAll(LogFile.CommitPath(_read.Version))
            ?? throw new DeltaException($"The commit of version {_read.Version} is missing.");
        long? last = ActionParser.ParseCommit(previous).OfType<CommitInfoAction>().FirstOrDefault()?.InCommitTimestamp;
        if (last is null) throw new DeltaException($"The commit of version {_read.Version} has no in-commit timestamp.");
        return Math.Max(now, last.Value + 1);

        static bool IsOn(MetadataAction m) =>
            m.Configuration.TryGetValue(CommitTimeline.InCommitTimestampsKey, out string? on)
            && string.Equals(on, "true", StringComparison.OrdinalIgnoreCase);
    }

    private CommitInfoAction CommitInfo(long timestamp, long? inCommitTimestamp)
    {
        bool blindAppend = _read is not null && _actions.All(a => a is AddAction { DataChange: true } or CdcAction or TxnAction);
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
