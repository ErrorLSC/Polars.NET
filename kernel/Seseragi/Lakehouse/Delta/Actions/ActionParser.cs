using System.Buffers.Text;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Text.Json;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Encodings;
using Kaminari.Sazanami.Json;
using Kaminari.Sazanami.Text;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Lakehouse.Engine;

namespace Kaminari.Seseragi.Lakehouse.Delta.Actions;

/// <summary>Reads the actions of a JSON commit file: one action per line.</summary>
/// <remarks>
/// Lines are read token by token (<see cref="JsonTokenReader"/>) with the rules of a JSON document reader: a line is
/// one valid JSON value (nested at most 64 levels deep); of an action's fields named more than once the last counts; a
/// field of another kind than the protocol's reads as absent, except an integer that is not a whole 64-bit number, a
/// list item that is not a string, which are errors. A line that is not UTF-8 is refused (strings are checked). A large
/// commit is read in parts on several threads.
/// </remarks>
public static class ActionParser
{
    /// <summary>The deepest nesting of a line (as a JSON document reader allows it).</summary>
    private const int MaxDepth = 64;

    /// <summary>Commits larger than this are cut at lines into parts of about this size, read on several threads.</summary>
    private const int PartBytes = 1 << 20;

    /// <summary>
    /// Reads all actions of a commit file of the storage (read into native memory, freed once parsed), or null when it
    /// does not exist.
    /// </summary>
    /// <exception cref="DeltaException">A line is not valid JSON or an action lacks a required field.</exception>
    public static List<DeltaAction>? ParseFile(IFileSystem fileSystem, string path)
    {
        using FileBytes? content = fileSystem.ReadFile(path);
        return content is null ? null : ParseCommit(content.Memory);
    }

    /// <summary>Reads all actions of a commit file. Lines holding an action this kernel does not know are skipped.</summary>
    /// <exception cref="DeltaException">A line is not valid JSON or an action lacks a required field.</exception>
    public static List<DeltaAction> ParseCommit(ReadOnlyMemory<byte> content)
    {
        if (content.Length <= 2 * PartBytes)
        {
            List<DeltaAction> actions = [];
            ParseLines(content.Span, actions);
            return actions;
        }
        // Parts cut after a newline, read side by side; the first failing part (in the file's order) fails the commit.
        List<(int Start, int Length)> parts = [];
        ReadOnlySpan<byte> span = content.Span;
        for (int start = 0; start < span.Length;)
        {
            int end = Math.Min(span.Length, start + PartBytes);
            if (end < span.Length)
            {
                int newline = span[end..].IndexOf((byte)'\n');
                end = newline < 0 ? span.Length : end + newline + 1;
            }
            parts.Add((start, end - start));
            start = end;
        }
        var results = new List<DeltaAction>[parts.Count];
        var failures = new Exception?[parts.Count];
        Parallel.For(0, parts.Count, i =>
        {
            try
            {
                List<DeltaAction> part = [];
                ParseLines(content.Span.Slice(parts[i].Start, parts[i].Length), part);
                results[i] = part;
            }
            catch (DeltaException e)
            {
                failures[i] = e;
            }
        });
        if (failures.FirstOrDefault(static f => f is not null) is { } failure) ExceptionDispatchInfo.Capture(failure).Throw();
        List<DeltaAction> all = new(results.Sum(static r => r.Count));
        foreach (List<DeltaAction> part in results) all.AddRange(part);
        return all;
    }

    /// <summary>Reads one action. Returns null for an action this kernel does not know.</summary>
    /// <exception cref="DeltaException">The line is not one valid JSON object or the action lacks a required field.</exception>
    public static DeltaAction? ParseLine(ReadOnlyMemory<byte> line)
    {
        List<DeltaAction> actions = [];
        int records = ParseLines(line.Span, actions);
        if (records != 1) throw new DeltaException("A log line is not valid JSON: it holds no value, or several.");
        return actions.Count == 0 ? null : actions[0];
    }

    // The actions of whole lines (blank lines skipped): the lines read.
    private static int ParseLines(ReadOnlySpan<byte> lines, List<DeltaAction> actions)
    {
        var reader = JsonTokenReader.Lines(lines);
        // The statistics of these lines take fewer bytes than the lines: a small commit's arena stays small.
        using var shared = new SharedValues(Math.Min(ByteArena.DefaultFirstChunkBytes, lines.Length));
        int records = 0;
        try
        {
            while (reader.NextRecord())
            {
                records++;
                if (ReadLine(ref reader, shared) is { } action) actions.Add(action);
            }
            return records;
        }
        catch (JsonSyntaxException e)
        {
            throw new DeltaException($"A log line is not valid JSON: {e.Message}", e);
        }
        catch (Exception e) when (e is InvalidOperationException or FormatException)
        {
            throw new DeltaException($"A log action has a field of the wrong type: {e.Message}", e);
        }
    }

    // One line (the reader before its first token): its action, the first of its fields that is an object and names an
    // action this kernel knows; the rest of the line read through.
    private static DeltaAction? ReadLine(ref JsonTokenReader reader, SharedValues shared)
    {
        if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
        {
            if (reader.TokenType is JsonTokenType.StartArray) Skip(ref reader);
            reader.Read();
            throw new DeltaException("A log line is not a JSON object.");
        }
        DeltaAction? action = null;
        Span<byte> scratch = stackalloc byte[NameBytes];
        while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
        {
            int kind = action is null ? Find(Name(in reader, scratch), Kinds) : -1;
            reader.Read();
            if (kind >= 0 && reader.TokenType == JsonTokenType.StartObject) action = ReadAction(kind, ref reader, shared);
            else Skip(ref reader);
        }
        reader.Read(); // nothing else on the line
        return action;
    }

    // The action kinds, as the fields of a line name them.
    private static readonly byte[][] Kinds =
        Names("sidecar", "checkpointMetadata", "add", "remove", "metaData", "protocol", "txn", "cdc", "domainMetadata", "commitInfo");

    // The action of kind (an index of Kinds), the reader on its object's start (left on its end).
    private static DeltaAction ReadAction(int kind, ref JsonTokenReader reader, SharedValues shared)
    {
        ReadOnlySpan<byte> source = reader.Source;
        int start = reader.TokenStartIndex;
        switch (kind)
        {
            case 0:
            {
                Span<Slot> f = stackalloc Slot[SidecarFields.Length];
                Collect(ref reader, SidecarFields, f);
                return new SidecarAction
                {
                    Path = Required(source, f[0], "path", "sidecar"),
                    SizeInBytes = Long(source, f[1]) ?? 0,
                    ModificationTime = Long(source, f[2]) ?? 0,
                };
            }
            case 1:
            {
                Span<Slot> f = stackalloc Slot[CheckpointMetadataFields.Length];
                Collect(ref reader, CheckpointMetadataFields, f);
                return new CheckpointMetadataAction { Version = Long(source, f[0]) ?? throw Missing("version", "checkpointMetadata") };
            }
            case 2:
            {
                Span<Slot> f = stackalloc Slot[AddFields.Length];
                Collect(ref reader, AddFields, f);
                return new AddAction
                {
                    Path = Required(source, f[0], "path", "add"),
                    PartitionValues = shared.PartitionValues(source, f[1]) ?? EmptyMap.Nullable,
                    Size = Long(source, f[2]) ?? 0,
                    ModificationTime = Long(source, f[3]) ?? 0,
                    DataChange = Bool(f[4], true),
                    StatsText = f[5].Type == JsonTokenType.String ? Utf8(source, f[5], shared.Texts) : (ReadOnlyMemory<byte>?)null,
                    Tags = NullableMap(source, f[6]),
                    DeletionVector = ReadDeletionVector(source, f[7]),
                    BaseRowId = Long(source, f[8]),
                    DefaultRowCommitVersion = Long(source, f[9]),
                    ClusteringProvider = Str(source, f[10]),
                };
            }
            case 3:
            {
                Span<Slot> f = stackalloc Slot[RemoveFields.Length];
                Collect(ref reader, RemoveFields, f);
                return new RemoveAction
                {
                    Path = Required(source, f[0], "path", "remove"),
                    DeletionTimestamp = Long(source, f[1]),
                    DataChange = Bool(f[2], true),
                    ExtendedFileMetadata = Bool(f[3], false),
                    PartitionValues = NullableMap(source, f[4]),
                    Size = Long(source, f[5]),
                    DeletionVector = ReadDeletionVector(source, f[6]),
                    BaseRowId = Long(source, f[7]),
                    DefaultRowCommitVersion = Long(source, f[8]),
                };
            }
            case 4:
            {
                Span<Slot> f = stackalloc Slot[MetadataFields.Length];
                Collect(ref reader, MetadataFields, f);
                string provider = "parquet";
                IReadOnlyDictionary<string, string> formatOptions = EmptyMap.Instance;
                if (f[3].Type == JsonTokenType.StartObject)
                {
                    Span<Slot> format = stackalloc Slot[FormatFields.Length];
                    CollectIn(source, f[3], FormatFields, format);
                    provider = Str(source, format[0]) ?? provider;
                    formatOptions = Map(source, format[1]);
                }
                return new MetadataAction
                {
                    Id = Required(source, f[0], "id", "metaData"),
                    Name = Str(source, f[1]),
                    Description = Str(source, f[2]),
                    FormatProvider = provider,
                    FormatOptions = formatOptions,
                    SchemaString = Required(source, f[4], "schemaString", "metaData"),
                    PartitionColumns = Strings(source, f[5]) ?? [],
                    Configuration = Map(source, f[6]),
                    CreatedTime = Long(source, f[7]),
                };
            }
            case 5:
            {
                Span<Slot> f = stackalloc Slot[ProtocolFields.Length];
                Collect(ref reader, ProtocolFields, f);
                return new ProtocolAction
                {
                    MinReaderVersion = (int)(Long(source, f[0]) ?? throw Missing("minReaderVersion", "protocol")),
                    MinWriterVersion = (int)(Long(source, f[1]) ?? throw Missing("minWriterVersion", "protocol")),
                    ReaderFeatures = Strings(source, f[2]),
                    WriterFeatures = Strings(source, f[3]),
                };
            }
            case 6:
            {
                Span<Slot> f = stackalloc Slot[TxnFields.Length];
                Collect(ref reader, TxnFields, f);
                return new TxnAction
                {
                    AppId = Required(source, f[0], "appId", "txn"),
                    Version = Long(source, f[1]) ?? throw Missing("version", "txn"),
                    LastUpdated = Long(source, f[2]),
                };
            }
            case 7:
            {
                Span<Slot> f = stackalloc Slot[CdcFields.Length];
                Collect(ref reader, CdcFields, f);
                return new CdcAction
                {
                    Path = Required(source, f[0], "path", "cdc"),
                    PartitionValues = NullableMap(source, f[1]) ?? EmptyMap.Nullable,
                    Size = Long(source, f[2]) ?? 0,
                    Tags = NullableMap(source, f[3]),
                };
            }
            case 8:
            {
                Span<Slot> f = stackalloc Slot[DomainMetadataFields.Length];
                Collect(ref reader, DomainMetadataFields, f);
                return new DomainMetadataAction
                {
                    Domain = Required(source, f[0], "domain", "domainMetadata"),
                    Configuration = Str(source, f[1]) ?? "",
                    Removed = Bool(f[2], false),
                };
            }
            default:
            {
                Span<Slot> f = stackalloc Slot[CommitInfoFields.Length];
                Collect(ref reader, CommitInfoFields, f);
                return new CommitInfoAction
                {
                    Json = Encoding.UTF8.GetString(source[start..reader.BytesConsumed]),
                    Timestamp = Long(source, f[0]),
                    InCommitTimestamp = Long(source, f[1]),
                    Operation = Str(source, f[2]),
                };
            }
        }
    }

    private static readonly byte[][] SidecarFields = Names("path", "sizeInBytes", "modificationTime");
    private static readonly byte[][] CheckpointMetadataFields = Names("version");
    private static readonly byte[][] AddFields = Names("path", "partitionValues", "size", "modificationTime", "dataChange", "stats", "tags",
        "deletionVector", "baseRowId", "defaultRowCommitVersion", "clusteringProvider");
    private static readonly byte[][] RemoveFields = Names("path", "deletionTimestamp", "dataChange", "extendedFileMetadata", "partitionValues",
        "size", "deletionVector", "baseRowId", "defaultRowCommitVersion");
    private static readonly byte[][] CdcFields = Names("path", "partitionValues", "size", "tags");
    private static readonly byte[][] MetadataFields = Names("id", "name", "description", "format", "schemaString", "partitionColumns",
        "configuration", "createdTime");
    private static readonly byte[][] FormatFields = Names("provider", "options");
    private static readonly byte[][] ProtocolFields = Names("minReaderVersion", "minWriterVersion", "readerFeatures", "writerFeatures");
    private static readonly byte[][] TxnFields = Names("appId", "version", "lastUpdated");
    private static readonly byte[][] DomainMetadataFields = Names("domain", "configuration", "removed");
    private static readonly byte[][] CommitInfoFields = Names("timestamp", "inCommitTimestamp", "operation");
    private static readonly byte[][] DeletionVectorFields = Names("storageType", "pathOrInlineDv", "offset", "sizeInBytes", "cardinality");

    private static byte[][] Names(params string[] names) => [.. names.Select(Encoding.UTF8.GetBytes)];

    private static DeletionVectorDescriptor? ReadDeletionVector(ReadOnlySpan<byte> source, Slot slot)
    {
        if (slot.Type != JsonTokenType.StartObject) return null;
        Span<Slot> f = stackalloc Slot[DeletionVectorFields.Length];
        CollectIn(source, slot, DeletionVectorFields, f);
        return new DeletionVectorDescriptor
        {
            StorageType = Required(source, f[0], "storageType", "deletionVector"),
            PathOrInlineDv = Required(source, f[1], "pathOrInlineDv", "deletionVector"),
            Offset = (int?)Long(source, f[2]),
            SizeInBytes = (int)(Long(source, f[3]) ?? throw Missing("sizeInBytes", "deletionVector")),
            Cardinality = Long(source, f[4]) ?? throw Missing("cardinality", "deletionVector"),
        };
    }

    /// <summary>
    /// What the adds of a part of a commit share: their statistics' bytes (slices of shared chunks, no string per file),
    /// and their partition values (one read-only map for each distinct text, as files of a partition repeat it).
    /// </summary>
    private sealed class SharedValues(int firstChunkBytes) : IDisposable
    {
        private const int MostMaps = 4096;
        private ByteArrayDictionary? _mapTexts; // made at the first map
        private readonly List<IReadOnlyDictionary<string, string?>> _maps = [];

        public ByteArena Texts { get; } = new(firstChunkBytes);

        public IReadOnlyDictionary<string, string?>? PartitionValues(ReadOnlySpan<byte> source, Slot slot)
        {
            if (slot.Type != JsonTokenType.StartObject) return null;
            ReadOnlySpan<byte> text = source.Slice(slot.Start, slot.Length);
            _mapTexts ??= new ByteArrayDictionary();
            int index = _mapTexts.IndexOf(text);
            if (index >= 0) return _maps[index];
            Dictionary<string, string?> map = NullableMap(source, slot)!;
            if (_maps.Count < MostMaps)
            {
                _mapTexts.GetOrAdd(text);
                _maps.Add(map);
            }
            return map;
        }

        public void Dispose() => _mapTexts?.Dispose();
    }

    // ------------------------------------------------------------------ a field's last value

    /// <summary>
    /// Where a field's value is in the line: a string's content (escaped as written) or a number's or literal's text, an
    /// object's or array's whole text; None when the field is absent.
    /// </summary>
    private readonly record struct Slot(JsonTokenType Type, int Start, int Length, bool Escaped);

    /// <summary>The longest field name looked up (longer names match none of them, escaped or not).</summary>
    private const int NameBytes = 256;

    // The fields of an object (the reader on its start, left on its end): the last value of each name of names.
    private static void Collect(ref JsonTokenReader reader, byte[][] names, scoped Span<Slot> slots)
    {
        Span<byte> scratch = stackalloc byte[NameBytes];
        int expected = 0;
        while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
        {
            ReadOnlySpan<byte> name = Name(in reader, scratch);
            int index = expected < names.Length && name.SequenceEqual(names[expected]) ? expected : Find(name, names);
            reader.Read();
            int start = reader.TokenStartIndex;
            JsonTokenType type = reader.TokenType;
            if (type is JsonTokenType.StartObject or JsonTokenType.StartArray)
            {
                Skip(ref reader);
                if (index >= 0) slots[index] = new Slot(type, start, reader.BytesConsumed - start, false);
            }
            else if (index >= 0)
                slots[index] = new Slot(type, type == JsonTokenType.String ? start + 1 : start, reader.ValueSpan.Length, reader.ValueIsEscaped);
            if (index >= 0) expected = index + 1;
        }
    }

    // The fields of the object in slot, as Collect reads them (slots of the whole line).
    private static void CollectIn(ReadOnlySpan<byte> source, Slot slot, byte[][] names, scoped Span<Slot> slots)
    {
        var reader = JsonTokenReader.Value(source.Slice(slot.Start, slot.Length));
        reader.Read();
        Collect(ref reader, names, slots);
        foreach (ref Slot s in slots)
            if (s.Type != JsonTokenType.None) s = s with { Start = s.Start + slot.Start };
    }

    // The name the reader stands on, unescaped (in scratch when escaped; empty when too long to be one looked up).
    private static ReadOnlySpan<byte> Name(in JsonTokenReader reader, Span<byte> scratch)
    {
        if (!reader.ValueIsEscaped) return reader.ValueSpan;
        return reader.ValueSpan.Length > scratch.Length ? [] : scratch[..reader.CopyString(scratch)];
    }

    private static int Find(ReadOnlySpan<byte> name, byte[][] names)
    {
        for (int i = 0; i < names.Length; i++)
            if (name.SequenceEqual(names[i])) return i;
        return -1;
    }

    // Past the value the reader stands on (to its last token), refusing nesting deeper than a JSON document allows.
    private static void Skip(ref JsonTokenReader reader)
    {
        if (reader.TokenType is not (JsonTokenType.StartObject or JsonTokenType.StartArray)) return;
        int depth = reader.CurrentDepth; // a field's value: never deep itself
        while (reader.Read())
        {
            switch (reader.TokenType)
            {
                case JsonTokenType.StartObject or JsonTokenType.StartArray:
                    if (reader.CurrentDepth >= MaxDepth) throw TooDeep(reader.TokenStartIndex);
                    break;
                case JsonTokenType.EndObject or JsonTokenType.EndArray when reader.CurrentDepth == depth:
                    return;
            }
        }
    }

    private static JsonSyntaxException TooDeep(int position) => new($"Nesting deeper than {MaxDepth} levels.", position);

    // ------------------------------------------------------------------ values, as a JSON document reader reads them

    private static DeltaException Missing(string field, string action) =>
        new($"A '{action}' action lacks the required field '{field}'.");

    private static string? Str(ReadOnlySpan<byte> source, Slot slot) =>
        slot.Type == JsonTokenType.String ? Text(source, slot) : null;

    private static string Required(ReadOnlySpan<byte> source, Slot slot, string name, string action) => Str(source, slot) ?? throw Missing(name, action);

    // A number that must be a whole 64-bit integer (anything else is an error, as reading it as Int64 is).
    private static long? Long(ReadOnlySpan<byte> source, Slot slot)
    {
        if (slot.Type != JsonTokenType.Number) return null;
        ReadOnlySpan<byte> text = source.Slice(slot.Start, slot.Length);
        if (JsonNumbers.TryInt64(text, out long value)) return value;
        if (Utf8Parser.TryParse(text, out value, out int used) && used == text.Length) return value;
        throw new FormatException($"The number {Encoding.UTF8.GetString(text)} is not a 64-bit integer.");
    }

    private static bool Bool(Slot slot, bool fallback) => slot.Type switch
    {
        JsonTokenType.True => true,
        JsonTokenType.False => false,
        _ => fallback,
    };

    // An array of strings (null items kept); an item of another kind is an error.
    private static List<string>? Strings(ReadOnlySpan<byte> source, Slot slot)
    {
        if (slot.Type != JsonTokenType.StartArray) return null;
        var reader = JsonTokenReader.Value(source.Slice(slot.Start, slot.Length));
        reader.Read();
        List<string> list = [];
        while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
        {
            list.Add(reader.TokenType switch
            {
                JsonTokenType.String => reader.GetString()!,
                JsonTokenType.Null => null!,
                _ => throw new InvalidOperationException($"An array item is a {reader.TokenType}, not a string."),
            });
        }
        return list;
    }

    private static IReadOnlyDictionary<string, string> Map(ReadOnlySpan<byte> source, Slot slot)
    {
        Dictionary<string, string?>? map = NullableMap(source, slot);
        if (map is null || map.Count == 0) return EmptyMap.Instance;
        var result = new Dictionary<string, string>(map.Count, StringComparer.Ordinal);
        foreach (var (key, value) in map)
        {
            if (value is not null) result[key] = value;
        }
        return result;
    }

    // An object's fields as text: strings unescaped, null as null, anything else as written.
    private static Dictionary<string, string?>? NullableMap(ReadOnlySpan<byte> source, Slot slot)
    {
        if (slot.Type != JsonTokenType.StartObject) return null;
        var reader = JsonTokenReader.Value(source.Slice(slot.Start, slot.Length));
        reader.Read();
        var map = new Dictionary<string, string?>(StringComparer.Ordinal);
        while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
        {
            string name = reader.GetString()!;
            reader.Read();
            int start = reader.TokenStartIndex;
            map[name] = reader.TokenType switch
            {
                JsonTokenType.Null => null,
                JsonTokenType.String => reader.GetString(),
                JsonTokenType.StartObject or JsonTokenType.StartArray => Raw(ref reader, start),
                _ => Encoding.UTF8.GetString(reader.ValueSpan),
            };
        }
        return map;

        static string Raw(ref JsonTokenReader reader, int start)
        {
            reader.Skip();
            return Encoding.UTF8.GetString(reader.Source[start..reader.BytesConsumed]);
        }
    }

    // A string's value as UTF-8 in texts: its content unescaped.
    private static ReadOnlyMemory<byte> Utf8(ReadOnlySpan<byte> source, Slot slot, ByteArena texts)
    {
        ReadOnlySpan<byte> content = source.Slice(slot.Start, slot.Length);
        if (!slot.Escaped) return texts.Add(content);
        return texts.Take(JsonStrings.Unescape(content, texts.GetSpan(content.Length), slot.Start));
    }

    // A string's value: its content unescaped.
    private static string Text(ReadOnlySpan<byte> source, Slot slot)
    {
        ReadOnlySpan<byte> content = source.Slice(slot.Start, slot.Length);
        if (!slot.Escaped) return Encoding.UTF8.GetString(content);
        const int StackBytes = 512;
        byte[]? rented = content.Length > StackBytes ? System.Buffers.ArrayPool<byte>.Shared.Rent(content.Length) : null;
        Span<byte> buffer = rented ?? stackalloc byte[StackBytes];
        try
        {
            return Encoding.UTF8.GetString(buffer[..JsonStrings.Unescape(content, buffer, slot.Start)]);
        }
        finally
        {
            if (rented is not null) System.Buffers.ArrayPool<byte>.Shared.Return(rented);
        }
    }
}
