using Apache.Arrow;
using Kaminari.Sazanami.Arrow;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;

namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>
/// The rows of a classic checkpoint appended action by action straight into native Arrow builders (one row per
/// action, its own column set, the others null), exported as batches of at most <see cref="BatchRows"/> rows: no row
/// dictionaries, no boxed values, no managed buffers. The values are those <c>Checkpoints</c>' reference rows hold.
/// </summary>
internal sealed class CheckpointBatches : IDisposable
{
    /// <summary>The rows of one batch (a batch's buffers are held until the writer has encoded them).</summary>
    public const int BatchRows = 16_384;

    // The top-level columns, in the checkpoint schema's order.
    private const int Txn = 0, Add = 1, Remove = 2, MetaData = 3, Protocol = 4, DomainMetadata = 5;

    private readonly Apache.Arrow.Schema _schema;
    private readonly NativeStructBuilder _root;
    private readonly bool _json;
    private readonly int _stats = -1, _partitionsParsed = -1, _statsParsed = -1; // add's optional children
    private readonly CheckpointStatistics.NativeWriter? _typed;

    /// <param name="schema">The checkpoint's Arrow schema (of its Parquet nodes).</param>
    /// <param name="json">Whether add has the text <c>stats</c> column.</param>
    /// <param name="typed">The typed columns of add, when it has them.</param>
    public CheckpointBatches(Apache.Arrow.Schema schema, bool json, CheckpointStatistics? typed)
    {
        _schema = schema;
        _root = (NativeStructBuilder)NativeColumnBuilder.Create(new Apache.Arrow.Types.StructType([.. schema.FieldsList]));
        _json = json;
        int next = 10; // add's fixed children: path … clusteringProvider
        if (json) _stats = next++;
        if (typed?.PartitionNode() is not null) _partitionsParsed = next++;
        if (typed is not null)
        {
            _statsParsed = next;
            var add = (NativeStructBuilder)_root.Children[Add];
            _typed = new CheckpointStatistics.NativeWriter(typed,
                _partitionsParsed >= 0 ? (NativeStructBuilder)add.Children[_partitionsParsed] : null,
                (NativeStructBuilder)add.Children[_statsParsed]);
        }
    }

    /// <summary>The rows appended since the last export.</summary>
    public long Rows => _root.Length;

    /// <summary>The rows appended since the last export, as a batch over native memory; the builders start over.</summary>
    public RecordBatch Export() => _root.ExportBatch(_schema);

    public void Dispose()
    {
        _typed?.Dispose();
        _root.Dispose();
    }

    /// <summary>Opens a row of the column: every other top-level column is null in it.</summary>
    private NativeStructBuilder Begin(int column)
    {
        for (int i = 0; i < _root.Children.Length; i++)
        {
            if (i != column) _root.Children[i].AppendNull();
        }
        return (NativeStructBuilder)_root.Children[column];
    }

    private void End(NativeStructBuilder action)
    {
        action.EndStruct();
        _root.EndStruct();
    }

    public void AppendProtocol(ProtocolAction p)
    {
        NativeStructBuilder s = Begin(Protocol);
        I32(s.Children[0], p.MinReaderVersion);
        I32(s.Children[1], p.MinWriterVersion);
        Strings(s.Children[2], p.ReaderFeatures);
        Strings(s.Children[3], p.WriterFeatures);
        End(s);
    }

    public void AppendMetadata(MetadataAction m)
    {
        NativeStructBuilder s = Begin(MetaData);
        Str(s.Children[0], m.Id);
        Str(s.Children[1], m.Name);
        Str(s.Children[2], m.Description);
        var format = (NativeStructBuilder)s.Children[3];
        Str(format.Children[0], m.FormatProvider);
        Map(format.Children[1], m.FormatOptions);
        format.EndStruct();
        Str(s.Children[4], m.SchemaString);
        Strings(s.Children[5], m.PartitionColumns);
        Map(s.Children[6], m.Configuration);
        I64(s.Children[7], m.CreatedTime);
        End(s);
    }

    public void AppendTxn(TxnAction t)
    {
        NativeStructBuilder s = Begin(Txn);
        Str(s.Children[0], t.AppId);
        I64(s.Children[1], t.Version);
        I64(s.Children[2], t.LastUpdated);
        End(s);
    }

    public void AppendDomain(DomainMetadataAction d)
    {
        NativeStructBuilder s = Begin(DomainMetadata);
        Str(s.Children[0], d.Domain);
        Str(s.Children[1], d.Configuration);
        Bool(s.Children[2], d.Removed);
        End(s);
    }

    /// <summary>A file of the table; files in a checkpoint do not change data (dataChange false).</summary>
    public void AppendAdd(AddAction a)
    {
        NativeStructBuilder s = Begin(Add);
        Str(s.Children[0], a.Path);
        Map(s.Children[1], a.PartitionValues);
        I64(s.Children[2], a.Size);
        I64(s.Children[3], a.ModificationTime);
        Bool(s.Children[4], false);
        Map(s.Children[5], a.Tags);
        DeletionVector(s.Children[6], a.DeletionVector);
        I64(s.Children[7], a.BaseRowId);
        I64(s.Children[8], a.DefaultRowCommitVersion);
        Str(s.Children[9], a.ClusteringProvider);
        if (_json) Str(s.Children[_stats], a.Stats);
        if (_typed is not null)
        {
            _typed.AppendPartitionValues(a);
            _typed.AppendStats(a.Stats);
        }
        End(s);
    }

    /// <summary>A tombstone; dataChange false, as for files.</summary>
    public void AppendRemove(RemoveAction r)
    {
        NativeStructBuilder s = Begin(Remove);
        Str(s.Children[0], r.Path);
        I64(s.Children[1], r.DeletionTimestamp);
        Bool(s.Children[2], false);
        Bool(s.Children[3], r.ExtendedFileMetadata);
        Map(s.Children[4], r.PartitionValues);
        I64(s.Children[5], r.Size);
        DeletionVector(s.Children[6], r.DeletionVector);
        I64(s.Children[7], r.BaseRowId);
        I64(s.Children[8], r.DefaultRowCommitVersion);
        End(s);
    }

    private static void DeletionVector(NativeColumnBuilder builder, DeletionVectorDescriptor? dv)
    {
        if (dv is null)
        {
            builder.AppendNull();
            return;
        }
        var s = (NativeStructBuilder)builder;
        Str(s.Children[0], dv.StorageType);
        Str(s.Children[1], dv.PathOrInlineDv);
        I32(s.Children[2], dv.Offset);
        I32(s.Children[3], dv.SizeInBytes);
        I64(s.Children[4], dv.Cardinality);
        s.Children[5].AppendNull(); // maxRowIndex: not tracked
        s.EndStruct();
    }

    private static void Str(NativeColumnBuilder builder, string? value)
    {
        if (value is null) builder.AppendNull();
        else ((NativeBinaryBuilder)builder).AppendUtf8(value);
    }

    private static void I64(NativeColumnBuilder builder, long? value)
    {
        if (value is { } v) ((NativeFixedWidthBuilder<long>)builder).Append(v);
        else builder.AppendNull();
    }

    private static void I32(NativeColumnBuilder builder, int? value)
    {
        if (value is { } v) ((NativeFixedWidthBuilder<int>)builder).Append(v);
        else builder.AppendNull();
    }

    private static void Bool(NativeColumnBuilder builder, bool? value)
    {
        if (value is { } v) ((NativeBooleanBuilder)builder).Append(v);
        else builder.AppendNull();
    }

    private static void Strings(NativeColumnBuilder builder, IEnumerable<string>? values)
    {
        if (values is null)
        {
            builder.AppendNull();
            return;
        }
        var list = (NativeListBuilder)builder;
        var items = (NativeBinaryBuilder)list.Values;
        foreach (string value in values) items.AppendUtf8(value);
        list.EndList();
    }

    /// <summary>A map of strings (a list of key / value structs); null for no map.</summary>
    private static void Map<TValue>(NativeColumnBuilder builder, IEnumerable<KeyValuePair<string, TValue>>? map) where TValue : class?
    {
        if (map is null)
        {
            builder.AppendNull();
            return;
        }
        var list = (NativeListBuilder)builder;
        var entries = (NativeStructBuilder)list.Values;
        var keys = (NativeBinaryBuilder)entries.Children[0];
        // Parsed maps are dictionaries: their struct enumerator allocates nothing (the interface's would, per map).
        if (map is Dictionary<string, TValue> dictionary)
        {
            foreach (var (key, value) in dictionary) Entry(key, value);
        }
        else if (map is not IReadOnlyCollection<KeyValuePair<string, TValue>> { Count: 0 })
        {
            foreach (var (key, value) in map) Entry(key, value);
        }
        list.EndList();

        void Entry(string key, TValue value)
        {
            keys.AppendUtf8(key);
            Str(entries.Children[1], value as string);
            entries.EndStruct();
        }
    }
}
