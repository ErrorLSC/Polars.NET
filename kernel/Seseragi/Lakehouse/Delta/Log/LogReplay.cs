using Kaminari.Seseragi.Lakehouse.Delta.Actions;

namespace Kaminari.Seseragi.Lakehouse.Delta.Log;

/// <summary>
/// Reconciles the actions of a log into the state of the table. Feed it commits from the newest to the oldest:
/// for each file, application or domain the first action seen is the one that counts.
/// </summary>
internal sealed class LogReplay
{
    // A data file is identified by its path together with its deletion vector.
    private readonly record struct FileKey(string Path, string? DeletionVectorId);

    private readonly HashSet<FileKey> _seen = [];
    private readonly List<AddAction> _active = [];
    private readonly Dictionary<string, TxnAction> _txns = new(StringComparer.Ordinal);
    private readonly Dictionary<string, DomainMetadataAction> _domains = new(StringComparer.Ordinal);

    public ProtocolAction? Protocol { get; private set; }
    public MetadataAction? Metadata { get; private set; }
    public IReadOnlyDictionary<string, TxnAction> AppTransactions => _txns;

    /// <summary>Applies the actions of one commit, which must be older than every commit applied before.</summary>
    public void Apply(IReadOnlyList<DeltaAction> commit)
    {
        // Within one commit a file may be removed and added again under the same key only by mistake; adds and
        // removes of one commit are otherwise disjoint, so the order inside the commit does not matter.
        int firstOfCommit = _active.Count;
        foreach (DeltaAction action in commit)
        {
            switch (action)
            {
                case AddAction add:
                    if (_seen.Add(new FileKey(add.Path, add.DeletionVector?.UniqueId))) _active.Add(add);
                    break;
                case RemoveAction remove:
                    _seen.Add(new FileKey(remove.Path, remove.DeletionVector?.UniqueId));
                    break;
                case ProtocolAction protocol:
                    Protocol ??= protocol;
                    break;
                case MetadataAction metadata:
                    Metadata ??= metadata;
                    break;
                case TxnAction txn:
                    _txns.TryAdd(txn.AppId, txn);
                    break;
                case DomainMetadataAction domain:
                    _domains.TryAdd(domain.Domain, domain);
                    break;
            }
        }
        // Keep the files of one commit in their written order once the whole list is reversed.
        _active.Reverse(firstOfCommit, _active.Count - firstOfCommit);
    }

    /// <summary>The files that are part of the table, oldest commit first.</summary>
    public List<AddAction> ActiveFiles()
    {
        var files = new List<AddAction>(_active);
        files.Reverse();
        return files;
    }

    public Dictionary<string, DomainMetadataAction> LiveDomainMetadata()
    {
        var live = new Dictionary<string, DomainMetadataAction>(StringComparer.Ordinal);
        foreach (var (name, domain) in _domains)
        {
            if (!domain.Removed) live[name] = domain;
        }
        return live;
    }
}
