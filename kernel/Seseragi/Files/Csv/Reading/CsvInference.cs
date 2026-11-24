using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Data.Building;
using Kaminari.Sazanami.Text;

namespace Kaminari.Seseragi.Files.Csv.Reading;

/// <summary>
/// The columns' types inferred from records in file order, as the engine's CSV reader infers them: each field's kind
/// (<see cref="ValueInference"/>, on the text inside the quotes, not unescaped; nulls, empty and missing fields left
/// out), the kinds of a column merged (Int64 with Float64 is Float64, with Int128 Int128, dates with naive datetimes
/// of the same day-month-year order datetimes, other mixes String; none String); a datetime needing nanoseconds makes its column datetime[ns]. The true and false values given are booleans,
/// numbers grouped by the thousands separator given are numbers. Every temporal column's pattern group is the group of
/// its first value that reads (file order), so the decoders of every unit read it alike.
/// </summary>
internal sealed class CsvInference(int columns, CsvValueRules rules, bool parseDates) : IDisposable
{
    private NativeByteBuffer? _scratch;

    /// <summary>A native scratch buffer of at least <paramref name="bytes"/> (long fields; short ones use the stack).</summary>
    private Span<byte> Scratch(int bytes)
    {
        _scratch ??= new NativeByteBuffer(Math.Max(bytes, 4096), pooled: true);
        _scratch.EnsureCapacity(bytes);
        return _scratch.Span;
    }

    public void Dispose()
    {
        _scratch?.Release();
        _decoded?.Release();
    }

    private NativeByteBuffer? _decoded;

    // A second scratch buffer: decoded fields (the first holds unescaped and rewritten text).
    private Span<byte> Decoded(int bytes)
    {
        _decoded ??= new NativeByteBuffer(Math.Max(bytes, 4096), pooled: true);
        _decoded.EnsureCapacity(bytes);
        return _decoded.Span;
    }

    private readonly List<Column> _columns = [.. Enumerable.Range(0, columns).Select(_ => new Column())];

    /// <summary>More columns from now on (a wider record without a header): the records before lack them.</summary>
    public void Widen(int count)
    {
        while (_columns.Count < count) _columns.Add(new Column());
    }

    private sealed class Column
    {
        public readonly HashSet<TextKind> Kinds = [];
        public bool Nanoseconds;
        public TemporalColumn Date, Datetime, Zoned;
    }

    /// <summary>The records taken in so far.</summary>
    public long Records { get; private set; }

    /// <summary>The columns observed.</summary>
    public int Columns => _columns.Count;

    /// <summary>
    /// Takes in column <paramref name="from"/> of a later file's inference as column <paramref name="column"/> of this one
    /// (a dataset's files united by name, in file order): the kinds seen are united, the formats stay the first file's
    /// that saw one — as if the files were one.
    /// </summary>
    public void Absorb(int column, CsvInference other, int from)
    {
        Column into = _columns[column], seen = other._columns[from];
        into.Kinds.UnionWith(seen.Kinds);
        into.Nanoseconds |= seen.Nanoseconds;
        into.Date.Group ??= seen.Date.Group;
        into.Datetime.Group ??= seen.Datetime.Group;
        into.Zoned.Group ??= seen.Zoned.Group;
    }

    /// <summary>Counts the records of a later file taken in (<see cref="Absorb"/>).</summary>
    public void AbsorbRecords(CsvInference other) => Records += other.Records;

    /// <summary>Takes in one record's fields (in file order; a blank line has none).</summary>
    public void Observe(ReadOnlySpan<byte> unit, IReadOnlyList<CsvField> fields, bool clean)
    {
        Records++;
        Span<byte> stack = stackalloc byte[256];
        for (int c = 0; c < fields.Count && c < _columns.Count; c++)
        {
            CsvField field = fields[c];
            if (field.Bits <= 0) continue;
            ReadOnlySpan<byte> raw = unit.Slice(field.Start, field.Length);
            if (raw.IsEmpty) continue;
            // Text in another encoding (or lossy UTF-8) is decoded first: inference sees what a UTF-8 file would hold.
            if (!clean && !System.Text.Ascii.IsValid(raw))
            {
                Span<byte> room = Decoded(CsvCharset.Room(raw.Length));
                raw = room[..rules.Charset.Decode(raw, room)];
            }
            scoped ReadOnlySpan<byte> text = raw;
            if (field.IsEscaped)
            {
                Span<byte> to = raw.Length <= stack.Length ? stack : Scratch(raw.Length);
                text = to[..rules.Tokenizer.Unescape(raw, to)];
            }
            if (rules.NullValues.Length > 0 && rules.IsNullValue(text)) continue;
            Column column = _columns[c];
            if ((rules.TrueValues.Length > 0 && CsvValueRules.In(rules.TrueValues, text)) || (rules.FalseValues.Length > 0 && CsvValueRules.In(rules.FalseValues, text)))
            {
                column.Kinds.Add(TextKind.Boolean);
                continue;
            }
            bool ns = false;
            TextKind kind = rules.Thousands is { } sep && raw.IndexOf(sep) >= 0 ? Grouped(raw) : ValueInference.Infer(raw, parseDates, rules.DecimalComma, out ns);
            if (kind is TextKind.Datetime or TextKind.DatetimeUtc) column.Nanoseconds |= ns;
            column.Kinds.Add(kind);
            // The first value that reads fixes the column's group (in file order).
            switch (kind)
            {
                case TextKind.Date when column.Date.Group is null:
                    column.Date.ReadDate(raw, out _);
                    break;
                case TextKind.Datetime when column.Datetime.Group is null:
                    column.Datetime.ReadDatetime(raw, TimeUnit.Nanosecond, out _);
                    break;
                case TextKind.DatetimeUtc when column.Zoned.Group is null:
                    column.Zoned.ReadDatetime(raw, TimeUnit.Nanosecond, out _);
                    break;
            }
        }
    }

    // A number grouped by thousands: its kind once the groups are removed (numbers only), else a string.
    private TextKind Grouped(ReadOnlySpan<byte> raw)
    {
        Span<byte> buffer = raw.Length <= 256 ? stackalloc byte[256] : Scratch(raw.Length);
        if (!rules.Rewrite(raw, buffer, out int written)) return TextKind.String;
        // The rewritten number has a decimal point (a decimal comma became one).
        TextKind kind = ValueInference.Infer(buffer[..written], false, false, out _);
        return kind is TextKind.Int64 or TextKind.Int128 or TextKind.Float64 ? kind : TextKind.String;
    }

    /// <summary>
    /// The inferred fields named <paramref name="names"/> (strings as Utf8View with <paramref name="viewTypes"/>) and
    /// each temporal column's group.
    /// </summary>
    public (Field[] Fields, TemporalPattern?[] Groups) Finish(IReadOnlyList<string> names, bool viewTypes)
    {
        var fields = new Field[_columns.Count];
        var groups = new TemporalPattern?[_columns.Count];
        for (int c = 0; c < _columns.Count; c++)
        {
            Column column = _columns[c];
            TextKind kind = Merged(column);
            TimeUnit unit = column.Nanoseconds ? TimeUnit.Nanosecond : TimeUnit.Microsecond;
            (IArrowType type, TemporalPattern? group) = kind switch
            {
                TextKind.Boolean => ((IArrowType)BooleanType.Default, (TemporalPattern?)null),
                TextKind.Int64 => (Int64Type.Default, null),
                TextKind.Int128 => (WideIntegers.Type, null),
                TextKind.Float64 => (DoubleType.Default, null),
                TextKind.Date => (Date32Type.Default, column.Date.Group),
                TextKind.Datetime => (new TimestampType(unit, (string?)null), column.Datetime.Group),
                TextKind.DatetimeUtc => (new TimestampType(unit, "UTC"), column.Zoned.Group),
                TextKind.Time => (new Time64Type(TimeUnit.Nanosecond), null),
                _ => (viewTypes ? StringViewType.Default : StringType.Default, null),
            };
            fields[c] = kind == TextKind.Int128
                ? new Field(names[c], type, true, [new(WideIntegers.MetadataKey, WideIntegers.Signed)])
                : new Field(names[c], type, true);
            groups[c] = group;
        }
        return (fields, groups);
    }

    // The column's kind: the kinds merged (TextValues.Finish), but dates with naive datetimes in the same order of day,
    // month and year are datetimes (a date read as its midnight: no value changes).
    private static TextKind Merged(Column column) =>
        column.Kinds.Count == 2 && column.Kinds.Contains(TextKind.Date) && column.Kinds.Contains(TextKind.Datetime)
            && (column.Date.Group, column.Datetime.Group) is (TemporalPattern.DateYmd, TemporalPattern.DatetimeYmd) or (TemporalPattern.DateDmy, TemporalPattern.DatetimeDmy)
            ? TextKind.Datetime
            : TextValues.Finish(column.Kinds) ?? TextKind.String;
}
