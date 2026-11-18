using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Kaminari.Sazanami.Arrow.Ipc;

/// <summary>
/// The encapsulated message format (Columnar.rst, "Encapsulated message format") and what readers of both IPC formats
/// share: message prefixes (the continuation marker and a length, or before Arrow 0.15 the length alone; a zero length
/// ends the stream), the body ranges a projection needs, the fields a projection reaches, and assembling batches.
/// </summary>
internal static class IpcFraming
{
    public const uint Continuation = 0xFFFFFFFF;

    /// <summary>"ARROW1": the magic at both ends of an IPC file.</summary>
    public static ReadOnlySpan<byte> FileMagic => "ARROW1"u8;

    /// <summary>
    /// Reads a message prefix from its first four bytes (and, after a continuation marker, the next four).
    /// </summary>
    /// <returns>The metadata length (0: the end of the stream), and through <paramref name="prefixLength"/> how many bytes the prefix takes (4 or 8).</returns>
    /// <exception cref="InvalidDataException">The length is negative.</exception>
    public static int MetadataLength(ReadOnlySpan<byte> first4, ReadOnlySpan<byte> next4, out int prefixLength)
    {
        uint word = BinaryPrimitives.ReadUInt32LittleEndian(first4);
        int length;
        if (word == Continuation)
        {
            prefixLength = 8;
            length = BinaryPrimitives.ReadInt32LittleEndian(next4);
        }
        else
        {
            prefixLength = 4; // before 0.15: the length alone
            length = (int)word;
        }
        if (length < 0) throw new InvalidDataException($"Arrow IPC data is damaged: a message metadata length of {length}.");
        return length;
    }

    /// <summary>Whether <paramref name="first4"/> starts a message with the continuation marker.</summary>
    public static bool HasContinuation(ReadOnlySpan<byte> first4) => BinaryPrimitives.ReadUInt32LittleEndian(first4) == Continuation;

    /// <summary>
    /// The body ranges holding the <paramref name="needed"/> buffers, in order, joined where less than
    /// <paramref name="gap"/> bytes lie between them.
    /// </summary>
    public static List<(long Start, long Length)> Ranges(IpcBuffer[] buffers, bool[] needed, long gap)
    {
        List<(long Start, long End)> spans = [];
        for (int i = 0; i < buffers.Length; i++)
        {
            if (needed[i] && buffers[i].Length > 0) spans.Add((buffers[i].Offset, buffers[i].Offset + buffers[i].Length));
        }
        spans.Sort();
        List<(long Start, long Length)> ranges = [];
        long start = -1, end = -1;
        foreach ((long s, long e) in spans)
        {
            if (start >= 0 && s - end <= gap)
            {
                end = Math.Max(end, e);
                continue;
            }
            if (start >= 0) ranges.Add((start, end - start));
            (start, end) = (s, e);
        }
        if (start >= 0) ranges.Add((start, end - start));
        return ranges;
    }

    /// <summary>
    /// The dictionary ids the <paramref name="selected"/> fields reach (null: all fields), through dictionary values that
    /// are themselves dictionary-encoded.
    /// </summary>
    public static HashSet<long> DictionaryIds(IpcField[] fields, bool[]? selected)
    {
        HashSet<long> ids = [];
        for (int i = 0; i < fields.Length; i++)
        {
            if (selected is null || selected[i]) Collect(fields[i]);
        }
        return ids;

        void Collect(IpcField field)
        {
            if (field.DictionaryId is long id) ids.Add(id);
            foreach (IpcField child in field.Children) Collect(child);
        }
    }

    /// <summary>The schema of the <paramref name="selected"/> fields (null: all), with the schema's metadata.</summary>
    public static Schema Project(IpcSchema schema, bool[]? selected) => selected is null
        ? schema.Schema
        : new Schema(schema.Fields.Where((_, i) => selected[i]).Select(f => f.Field), schema.Schema.Metadata);

    /// <summary>The selected top-level fields by name (null: all); every name must exist.</summary>
    /// <exception cref="ArgumentException">A name is not a field of the schema.</exception>
    public static bool[]? Select(IpcSchema schema, IReadOnlyList<string>? columns)
    {
        if (columns is null) return null;
        var selected = new bool[schema.Fields.Length];
        foreach (string name in columns)
        {
            bool found = false;
            for (int i = 0; i < selected.Length; i++)
            {
                if (schema.Fields[i].Field.Name != name) continue;
                selected[i] = true;
                found = true;
            }
            if (!found) throw new ArgumentException($"The Arrow IPC data has no column '{name}'.", nameof(columns));
        }
        return selected;
    }

    /// <summary>
    /// A record batch of the decoded <paramref name="arrays"/> (the selected ones, in schema order), cut to its first
    /// <paramref name="rows"/> rows.
    /// </summary>
    public static RecordBatch Assemble(Schema schema, IpcArray?[] arrays, int rows)
    {
        List<IArrowArray> columns = new(schema.FieldsList.Count);
        foreach (IpcArray? array in arrays)
        {
            if (array is not null) columns.Add(ArrowArrayFactory.BuildArray(IpcBatchDecoder.Prefix(array, rows).ToArrayData()));
        }
        return new RecordBatch(schema, columns, rows);
    }
}
