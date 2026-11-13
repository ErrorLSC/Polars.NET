using System.IO.Hashing;
using Apache.Arrow;

namespace Kaminari.Seseragi.Files.Avro;

/// <summary>
/// Shares what opening an Avro file works out from its schema (the parsed writer schema, the projection, the Arrow
/// schema, the resolution plan) between readers of files with the same writer schema and the same read options.
/// Many small files of one schema (Iceberg manifests) then skip parsing the schema and resolving it again: pass one
/// cache to every reader through <see cref="AvroReadOptions.Cache"/>.
/// </summary>
/// <remarks>
/// <para>
/// Entries are keyed by the exact bytes of the writer schema in the file header, the reader schema's content (its
/// JSON, worked out once per <see cref="AvroSchema"/> instance, so a reader schema rebuilt for each file still hits),
/// the field matching and the projection. Only immutable values are kept: decoders and buffers are created by each
/// reader, and nothing about an individual file (metadata, codec, sync marker) is cached.
/// </para>
/// <para>
/// The cache is safe to share between threads. It holds at most <see cref="Capacity"/> entries and drops the least
/// recently used one when full. It holds no native memory, so it needs no disposal.
/// </para>
/// </remarks>
public sealed class AvroReadCache
{
    private readonly object _lock = new();
    private readonly Dictionary<CacheKey, LinkedListNode<(CacheKey Key, PreparedRead Value)>> _entries = [];
    private readonly LinkedList<(CacheKey Key, PreparedRead Value)> _order = new(); // most recently used first

    /// <summary>A cache holding up to <paramref name="capacity"/> schemas.</summary>
    public AvroReadCache(int capacity = 64)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        Capacity = capacity;
    }

    /// <summary>The most entries the cache keeps.</summary>
    public int Capacity { get; }

    /// <summary>The number of entries.</summary>
    public int Count
    {
        get
        {
            lock (_lock) return _entries.Count;
        }
    }

    /// <summary>Removes every entry.</summary>
    public void Clear()
    {
        lock (_lock)
        {
            _entries.Clear();
            _order.Clear();
        }
    }

    // The entry for a writer schema and options, prepared outside the lock when missing (a failure is not cached).
    internal PreparedRead GetOrAdd(byte[] writerSchema, AvroReadOptions options, Func<PreparedRead> prepare)
    {
        var key = new CacheKey(writerSchema, options);
        lock (_lock)
        {
            if (_entries.TryGetValue(key, out var hit))
            {
                _order.Remove(hit);
                _order.AddFirst(hit);
                return hit.Value.Value;
            }
        }
        PreparedRead prepared = prepare();
        lock (_lock)
        {
            if (_entries.TryGetValue(key, out var raced)) return raced.Value.Value; // another reader prepared it first
            var node = _order.AddFirst((key, prepared));
            _entries[key] = node;
            if (_entries.Count > Capacity)
            {
                var last = _order.Last!;
                _order.RemoveLast();
                _entries.Remove(last.Value.Key);
            }
        }
        return prepared;
    }

    private sealed class CacheKey : IEquatable<CacheKey>
    {
        private readonly byte[] _writerSchema;
        private readonly string? _readerSchema;
        private readonly AvroFieldMatching _matching;
        private readonly string _projection;
        private readonly int _hash;

        public CacheKey(byte[] writerSchema, AvroReadOptions options)
        {
            _writerSchema = writerSchema;
            _readerSchema = options.ReaderSchema?.CanonicalJson;
            _matching = options.ReaderSchema is null ? AvroFieldMatching.ByName : options.FieldMatching;
            // Names and positions are kept apart ('\u0001' cannot occur in either list's text).
            _projection = options.Columns is { } names ? "n\u0001" + string.Join('\u0001', names)
                : options.ColumnIndices is { } indices ? "i\u0001" + string.Join('\u0001', indices)
                : "";
            _hash = HashCode.Combine(XxHash3.HashToUInt64(writerSchema), _readerSchema, _matching, _projection);
        }

        public bool Equals(CacheKey? other) => other is not null && _hash == other._hash && _matching == other._matching
            && _readerSchema == other._readerSchema && _projection == other._projection
            && _writerSchema.AsSpan().SequenceEqual(other._writerSchema);

        public override bool Equals(object? obj) => Equals(obj as CacheKey);

        public override int GetHashCode() => _hash;
    }
}

/// <summary>What opening a file works out from its writer schema and the read options; immutable and shareable.</summary>
internal sealed record PreparedRead(AvroSchema WriterSchema, Schema Schema,
    IReadOnlyList<(string Name, AvroSchema Schema, int? FieldId)> Fields, bool[] Selected, ResolvedNode? Plan);
