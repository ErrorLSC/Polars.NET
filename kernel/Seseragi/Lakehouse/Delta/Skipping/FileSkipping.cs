using System.Buffers.Binary;
using System.Buffers.Text;
using System.Collections.Concurrent;
using System.Globalization;
using System.Numerics;
using System.Text;
using System.Text.Json;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Json;
using Kaminari.Sazanami.Text;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Pruning;
using Kaminari.Seseragi.Files.Parquet.Schema;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using DecimalType = Kaminari.Seseragi.Lakehouse.Delta.Schema.DecimalType;
using StructType = Kaminari.Seseragi.Lakehouse.Delta.Schema.StructType;
using ParquetLogicalType = Kaminari.Seseragi.Files.Parquet.Metadata.LogicalType;

namespace Kaminari.Seseragi.Lakehouse.Delta.Skipping;

/// <summary>
/// Decides, from a file's partition values and statistics alone, whether a data file may hold a row a filter keeps.
/// The filter is the query compiler's IR (<see cref="ScalarExpr"/>, columns by logical name, struct fields through
/// <c>StructFieldByName</c>): it is compiled by the Parquet reader's statistics pruning, with each data file as one
/// unit whose statistics are the log's. The table's columns are laid out as Spark writes them in Parquet (the
/// statistics' types), arrays, maps and variants left out (nothing is known of their values).
/// </summary>
/// <remarks>
/// The log's statistics are read by Delta's rules, all of which only widen what a file may hold: timestamps are cut
/// to milliseconds (the maximum may be 999 µs later); a string minimum may be cut (still a lower bound; a cut
/// maximum would be no upper bound, so writers leave it out); integers and floats written as text are not trusted; the statistics of files from before a float was
/// widened to double are float values (their bounds are widened by a float step); a file without a column's
/// statistics, or without statistics, may hold anything. A partition column holds the file's partition value in
/// every row.
/// </remarks>
internal sealed class FileSkipping
{
    private const long MicrosPerMilli = 1000;

    private readonly PruneNode _root;
    private readonly Leaf[] _leaves;
    private readonly StatsPaths _paths;

    private FileSkipping(PruneNode root, Leaf[] leaves, StatsPaths paths) => (_root, _leaves, _paths) = (root, leaves, paths);

    /// <summary>The filter compiled for the table; null when the statistics can rule no file out.</summary>
    /// <param name="schema">The table's schema.</param>
    /// <param name="partitionColumns">The partition columns, by logical name.</param>
    /// <param name="filter">The rows a query keeps.</param>
    public static FileSkipping? Create(StructType schema, IReadOnlyList<string> partitionColumns, ScalarExpr filter)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(partitionColumns);
        ArgumentNullException.ThrowIfNull(filter);
        var leaves = new List<Leaf>();
        var elements = new List<SchemaElement> { new() { Name = "schema", NumChildren = 0 } };
        int top = 0;
        foreach (StructField field in schema.Fields)
        {
            bool partition = partitionColumns.Contains(field.Name);
            if (Add(field, [], partition, elements, leaves)) top++;
        }
        elements[0] = new SchemaElement { Name = "schema", NumChildren = top };
        if (leaves.Count == 0) return null;

        SchemaElement[] all = [.. elements];
        var meta = new FileMetaData
        {
            Version = 1,
            Schema = all,
            NumRows = 0,
            RowGroups = [],
            ColumnOrders = [.. leaves.Select(_ => ColumnOrder.TypeDefinedOrder)],
        };
        var compiler = new FilterCompiler(ParquetSchemaTree.Build(all), meta, new ParquetReadOptions());
        // The canonical shapes the compiler reads (a StartsWith call as the operator, constants folded), as a reader's filter.
        if (compiler.Compile(FilterShapes.normalize(filter)) is not { } root) return null;
        // Only the leaves the filter tests are looked for in a file's statistics (partition values are elsewhere).
        int[] tested = [.. compiler.Leaves.Select(static d => d.Leaf).Distinct().Where(l => !leaves[l].IsPartition).Order()];
        Leaf[] tableLeaves = [.. leaves];
        return new FileSkipping(root, tableLeaves, new StatsPaths(tableLeaves, tested));
    }

    /// <summary>Whether <paramref name="file"/> may hold a row the filter keeps.</summary>
    public bool MightMatch(AddAction file)
    {
        using var statistics = new FileStatistics(this);
        return statistics.MightMatch(file);
    }

    /// <summary>The files that may hold a row the filter keeps, in their order (judged on several threads when many).</summary>
    public List<AddAction> Matching(IReadOnlyList<AddAction> files)
    {
        const int Chunk = 4096;
        var keep = new bool[files.Count];
        if (files.Count <= Chunk)
        {
            using var statistics = new FileStatistics(this);
            for (int i = 0; i < files.Count; i++) keep[i] = statistics.MightMatch(files[i]);
        }
        else
        {
            Parallel.ForEach(Partitioner.Create(0, files.Count, Chunk), () => new FileStatistics(this), (range, _, statistics) =>
            {
                for (int i = range.Item1; i < range.Item2; i++) keep[i] = statistics.MightMatch(files[i]);
                return statistics;
            }, static statistics => statistics.Dispose());
        }
        List<AddAction> matching = [];
        for (int i = 0; i < files.Count; i++)
            if (keep[i]) matching.Add(files[i]);
        return matching;
    }

    // ------------------------------------------------------------------ the table as Parquet leaves

    /// <summary>How a leaf's statistics are read.</summary>
    private enum Kind { Integer, Float, Double, Decimal, String, Boolean, Date, Timestamp }

    /// <param name="Physical">The path of physical names (the keys of the statistics and partition values).</param>
    /// <param name="Kind">How the leaf's statistics are read.</param>
    /// <param name="IsPartition">A partition column: its value is in the file's partition values.</param>
    /// <param name="Float32Stats">A double column widened from float: older files' statistics are float values.</param>
    /// <param name="Scale">The scale of a decimal.</param>
    /// <param name="Storage">The stored width of an integer or decimal: 4, 8, or the bytes of a fixed-length decimal.</param>
    private sealed record Leaf(string[] Physical, Kind Kind, bool IsPartition, bool Float32Stats, int Scale, int Storage);

    /// <summary>Adds the field's elements and leaves; false when the field has no testable leaf (and adds nothing).</summary>
    private static bool Add(StructField field, string[] parent, bool partition, List<SchemaElement> elements, List<Leaf> leaves)
    {
        string[] physical = [.. parent, field.PhysicalName];
        if (field.Type is StructType nested)
        {
            int at = elements.Count;
            elements.Add(null!); // the group, once its children are known
            int children = 0;
            foreach (StructField child in nested.Fields)
            {
                if (Add(child, physical, partition: false, elements, leaves)) children++;
            }
            if (children == 0)
            {
                elements.RemoveAt(at);
                return false;
            }
            elements[at] = new SchemaElement { Name = field.Name, Repetition = FieldRepetition.Optional, NumChildren = children };
            return true;
        }
        if (Element(field, physical, partition) is not var (element, leaf)) return false;
        elements.Add(element);
        leaves.Add(leaf);
        return true;
    }

    /// <summary>The Parquet element of a primitive field, as Spark writes it, and how its statistics are read.</summary>
    private static (SchemaElement, Leaf)? Element(StructField field, string[] physical, bool partition)
    {
        SchemaElement Make(ParquetType type, ParquetLogicalType? logical = null, ConvertedType? converted = null, int? length = null,
            int? scale = null, int? precision = null) => new()
        {
            Name = field.Name, Type = type, Repetition = FieldRepetition.Optional, LogicalType = logical, ConvertedType = converted,
            TypeLength = length, Scale = scale, Precision = precision,
        };
        Leaf L(Kind kind, int storage = 0, int scale = 0, bool float32 = false) => new(physical, kind, partition, float32, scale, storage);

        switch (field.Type)
        {
            case DecimalType d:
            {
                var logical = new ParquetLogicalType(LogicalTypeKind.Decimal) { Precision = d.Precision, Scale = d.Scale };
                if (d.Precision <= 9) return (Make(ParquetType.Int32, logical, ConvertedType.Decimal, scale: d.Scale, precision: d.Precision), L(Kind.Decimal, 4, d.Scale));
                if (d.Precision <= 18) return (Make(ParquetType.Int64, logical, ConvertedType.Decimal, scale: d.Scale, precision: d.Precision), L(Kind.Decimal, 8, d.Scale));
                return (Make(ParquetType.FixedLenByteArray, logical, ConvertedType.Decimal, length: 16, scale: d.Scale, precision: d.Precision), L(Kind.Decimal, 16, d.Scale));
            }
            case PrimitiveType p:
                return p.Name switch
                {
                    PrimitiveType.Byte => (Make(ParquetType.Int32, Integer(8)), L(Kind.Integer, 4)),
                    PrimitiveType.Short => (Make(ParquetType.Int32, Integer(16)), L(Kind.Integer, 4)),
                    PrimitiveType.Integer => (Make(ParquetType.Int32), L(Kind.Integer, 4)),
                    PrimitiveType.Long => (Make(ParquetType.Int64), L(Kind.Integer, 8)),
                    PrimitiveType.Float => (Make(ParquetType.Float), L(Kind.Float)),
                    PrimitiveType.Double => (Make(ParquetType.Double), L(Kind.Double, float32: WidenedFrom(field, PrimitiveType.Float))),
                    PrimitiveType.String => (Make(ParquetType.ByteArray, new ParquetLogicalType(LogicalTypeKind.String), ConvertedType.Utf8), L(Kind.String)),
                    PrimitiveType.Boolean => (Make(ParquetType.Boolean), L(Kind.Boolean)),
                    PrimitiveType.Date => (Make(ParquetType.Int32, new ParquetLogicalType(LogicalTypeKind.Date), ConvertedType.Date), L(Kind.Date, 4)),
                    PrimitiveType.Timestamp => (Make(ParquetType.Int64, Timestamp(utc: true)), L(Kind.Timestamp, 8)),
                    PrimitiveType.TimestampNtz => (Make(ParquetType.Int64, Timestamp(utc: false)), L(Kind.Timestamp, 8)),
                    _ => null, // binary, variant: no statistics of their values
                };
            default:
                return null; // arrays and maps: many values per row
        }

        static ParquetLogicalType Integer(int bits) => new(LogicalTypeKind.Integer) { BitWidth = (sbyte)bits, IsSigned = true };

        static ParquetLogicalType Timestamp(bool utc) =>
            new(LogicalTypeKind.Timestamp) { IsAdjustedToUtc = utc, Unit = ParquetTimeUnit.Micros, UnitMember = 2 };
    }

    /// <summary>
    /// Whether the field itself (not an element or entry inside it) once had the type <paramref name="fromType"/>,
    /// as its type widening history (<c>delta.typeChanges</c>) records.
    /// </summary>
    private static bool WidenedFrom(StructField field, string fromType)
    {
        if (!field.Metadata.TryGetValue(SchemaEvolution.TypeChangesKey, out string? json)) return false;
        try
        {
            using JsonDocument doc = JsonDocument.Parse(json);
            if (doc.RootElement.ValueKind != JsonValueKind.Array) return true; // unreadable history: assume the worst
            foreach (JsonElement change in doc.RootElement.EnumerateArray())
            {
                if (change.ValueKind != JsonValueKind.Object) return true;
                if (change.TryGetProperty("fieldPath", out _)) continue; // a change inside an array or map
                if (change.TryGetProperty("fromType", out JsonElement from) && from.ValueKind == JsonValueKind.String && from.GetString() == fromType)
                    return true;
            }
            return false;
        }
        catch (JsonException)
        {
            return true;
        }
    }

    // ------------------------------------------------------------------ a file's statistics

    /// <summary>
    /// The paths of the leaves a filter tests, as a tree of the statistics' field names (physical names, UTF-8): where a
    /// file's <c>minValues</c>, <c>maxValues</c> and <c>nullCount</c> hold their values. Each tested leaf has a number
    /// (its place among them).
    /// </summary>
    private sealed class StatsPaths
    {
        public StatsPaths(Leaf[] leaves, int[] tested)
        {
            Root = new Node([]);
            Numbers = new int[leaves.Length];
            Array.Fill(Numbers, -1);
            Count = tested.Length;
            for (int number = 0; number < tested.Length; number++)
            {
                int leaf = tested[number];
                Numbers[leaf] = number;
                Node node = Root;
                foreach (string name in leaves[leaf].Physical)
                {
                    node.Below.Add(number);
                    byte[] bytes = Encoding.UTF8.GetBytes(name);
                    Node? child = node.Children.FirstOrDefault(c => c.Name.AsSpan().SequenceEqual(bytes));
                    if (child is null) node.Children.Add(child = new Node(bytes));
                    node = child;
                }
                node.Below.Add(number);
                node.Number = number;
            }
        }

        public Node Root { get; }

        /// <summary>The number of each leaf of the table among the tested ones; -1 for the others.</summary>
        public int[] Numbers { get; }

        public int Count { get; }

        public sealed class Node(byte[] name)
        {
            public byte[] Name { get; } = name;

            public List<Node> Children { get; } = [];

            /// <summary>The numbers of the tested leaves at or under this node.</summary>
            public List<int> Below { get; } = [];

            /// <summary>The leaf's number when the node is a tested leaf; -1 when it is a struct.</summary>
            public int Number { get; set; } = -1;

            public Node? Find(ReadOnlySpan<byte> name)
            {
                foreach (Node child in Children)
                    if (name.SequenceEqual(child.Name)) return child;
                return null;
            }
        }
    }

    /// <summary>
    /// Data files as units, one at a time: their statistics by leaf, in the stored representation of Parquet
    /// statistics. A file's statistics text is read (once, token by token, UTF-8) only when the filter asks for a leaf
    /// that is not a partition column, and only the tested leaves' values are kept (where they are in the text).
    /// </summary>
    /// <remarks>
    /// The text is read as a JSON document reader reads it: it must be one valid JSON object nested at most 64 levels
    /// deep (else the file has no statistics); of fields named more than once the last counts, a struct's whole object
    /// included; a null value is no value.
    /// </remarks>
    private sealed class FileStatistics(FileSkipping skipping) : IUnitStatistics, IDisposable
    {
        private const int Min = 0, Max = 1, Nulls = 2, MaxDepth = 64;

        private readonly Leaf[] _leaves = skipping._leaves;
        private readonly StatsPaths _paths = skipping._paths;
        private readonly Value[] _values = new Value[3 * skipping._paths.Count];
        private NativeArray<byte>? _transcoded;    // a string's statistics as UTF-8
        private AddAction? _file;
        private ReadOnlyMemory<byte> _text;         // the statistics read from the log, as UTF-8
        private int _transcodedLength = -1;         // or, when not negative, the transcoded ones
        private bool _read, _readable;
        private long? _records;

        /// <summary>Where a value is in the text: a string's content, a number's text, or another kind (no usable value).</summary>
        private readonly record struct Value(JsonTokenType Type, int Start, int Length, bool Escaped);

        public bool MightMatch(AddAction file)
        {
            _file = file;
            _read = false;
            return (skipping._root.Evaluate(this) & Outcomes.True) != 0;
        }

        public bool TryGet(int leaf, LeafDomain domain, out LeafStatistics statistics)
        {
            statistics = default;
            if ((uint)leaf >= (uint)_leaves.Length) return false;
            Leaf column = _leaves[leaf];
            AddAction file = _file!;

            if (column.IsPartition)
            {
                // Every row holds the partition value; a missing entry tells nothing. The row count is any positive
                // number: with no nulls, or all rows null, what the filter decides does not depend on it, so the
                // statistics are not read for it.
                if (!file.PartitionValues.TryGetValue(column.Physical[0], out string? partition)) return false;
                if (partition is null)
                {
                    statistics = new LeafStatistics(null, null, 1, 1, 0);
                    return true;
                }
                if (FromText(partition, column) is not { } value) return false;
                statistics = new LeafStatistics(value, value, 0, 1, 0);
                return true;
            }

            if (!_read) Read(file);
            int number = _paths.Numbers[leaf];
            if (!_readable || number < 0 || _records is not long count || count < 0) return false;
            ReadOnlySpan<byte> text = Text;
            long? nulls = Integer(text, _values[3 * number + Nulls]) is long k && k >= 0 && k <= count ? k : null;
            byte[]? min = FromStat(text, _values[3 * number + Min], column);
            byte[]? max = FromStat(text, _values[3 * number + Max], column);
            switch (column.Kind)
            {
                case Kind.Timestamp when max is not null:
                    // Cut to milliseconds: the true maximum may be up to 999 microseconds later.
                    max = Int64Bytes(BinaryPrimitives.ReadInt64LittleEndian(max) + MicrosPerMilli - 1);
                    break;
                case Kind.Double when column.Float32Stats:
                    // Statistics of a file from before the widening are float values, as their decimal text.
                    if (min is not null) min = DoubleBytes(MathF.BitDecrement((float)BitConverter.ToDouble(min)));
                    if (max is not null) max = DoubleBytes(MathF.BitIncrement((float)BitConverter.ToDouble(max)));
                    break;
            }
            statistics = new LeafStatistics(min, max, nulls, count, null); // NaN counts are not written
            return true;
        }

        // The file's statistics text (UTF-8 as read from the log, or its string's UTF-8), and the tested leaves' values.
        private void Read(AddAction file)
        {
            _read = true;
            _readable = false;
            _records = null;
            Array.Clear(_values);
            _transcodedLength = -1;
            if (file.StatsText is { } utf8) _text = utf8;
            else if (file.Stats is { Length: > 0 } stats)
            {
                _transcoded ??= new NativeArray<byte>();
                _transcoded.Reserve(Encoding.UTF8.GetMaxByteCount(stats.Length));
                _transcodedLength = Encoding.UTF8.GetBytes(stats, _transcoded.Span);
            }
            else return;
            if (Text.IsEmpty) return;
            try
            {
                _readable = Walk(Text);
            }
            catch (JsonSyntaxException)
            {
                _readable = false; // unreadable statistics are the same as none
            }
        }

        private ReadOnlySpan<byte> Text => _transcodedLength >= 0 ? _transcoded!.AsSpan(0, _transcodedLength) : _text.Span;

        private bool Walk(ReadOnlySpan<byte> text)
        {
            var reader = JsonTokenReader.Value(text);
            if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
            {
                Skip(ref reader);
                return false;
            }
            Span<byte> scratch = stackalloc byte[256];
            while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
            {
                ReadOnlySpan<byte> name = Name(in reader, scratch);
                int section = name.SequenceEqual("minValues"u8) ? Min : name.SequenceEqual("maxValues"u8) ? Max : name.SequenceEqual("nullCount"u8) ? Nulls : -1;
                bool records = section < 0 && name.SequenceEqual("numRecords"u8);
                reader.Read();
                if (records) _records = reader.TokenType == JsonTokenType.Number && reader.TryGetInt64(out long n) ? n : null;
                if (section >= 0)
                {
                    // The last object of a section counts as a whole.
                    Reset(_paths.Root, section);
                    if (reader.TokenType == JsonTokenType.StartObject)
                    {
                        Within(ref reader, _paths.Root, section, scratch);
                        continue;
                    }
                }
                Skip(ref reader);
            }
            // One value, nothing but white space after it.
            return text[reader.BytesConsumed..].IndexOfAnyExcept(" \t\r\n"u8) < 0;
        }

        // The fields of an object of a section (the reader on its start, left on its end) at node.
        private void Within(ref JsonTokenReader reader, StatsPaths.Node node, int section, scoped Span<byte> scratch)
        {
            if (reader.CurrentDepth >= MaxDepth) throw TooDeep(reader.TokenStartIndex);
            while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
            {
                StatsPaths.Node? child = node.Find(Name(in reader, scratch));
                reader.Read();
                if (child is null)
                {
                    Skip(ref reader);
                    continue;
                }
                if (child.Number >= 0)
                {
                    int start = reader.TokenType == JsonTokenType.String ? reader.TokenStartIndex + 1 : reader.TokenStartIndex;
                    _values[3 * child.Number + section] = new Value(reader.TokenType, start, reader.ValueSpan.Length, reader.ValueIsEscaped);
                    Skip(ref reader);
                    continue;
                }
                // A struct: its last object counts as a whole; anything else holds none of its leaves.
                Reset(child, section);
                if (reader.TokenType == JsonTokenType.StartObject) Within(ref reader, child, section, scratch);
                else Skip(ref reader);
            }
        }

        private void Reset(StatsPaths.Node node, int section)
        {
            foreach (int number in node.Below) _values[3 * number + section] = default;
        }

        public void Dispose() => _transcoded?.Dispose();

        // The name the reader stands on, unescaped (empty when too long to be a column's).
        private static ReadOnlySpan<byte> Name(in JsonTokenReader reader, Span<byte> scratch) =>
            !reader.ValueIsEscaped ? reader.ValueSpan
            : reader.ValueSpan.Length > scratch.Length ? [] : scratch[..reader.CopyString(scratch)];

        // Past the value the reader stands on, refusing nesting deeper than a JSON document allows.
        private static void Skip(ref JsonTokenReader reader)
        {
            if (reader.TokenType is not (JsonTokenType.StartObject or JsonTokenType.StartArray)) return;
            if (reader.CurrentDepth >= MaxDepth) throw TooDeep(reader.TokenStartIndex);
            int depth = reader.CurrentDepth;
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

        // A whole 64-bit integer; null for anything else.
        private static long? Integer(ReadOnlySpan<byte> text, Value value) =>
            value.Type == JsonTokenType.Number && TryInt64(text.Slice(value.Start, value.Length), out long n) ? n : null;

        // A JSON number as a JSON document reader reads it as Int64 / as Double.
        private static bool TryInt64(ReadOnlySpan<byte> raw, out long value) =>
            JsonNumbers.TryInt64(raw, out value) || (Utf8Parser.TryParse(raw, out value, out int used) && used == raw.Length);

        private static bool TryDouble(ReadOnlySpan<byte> raw, out double value) =>
            JsonNumbers.TryExactDouble(raw, out value) || (Utf8Parser.TryParse(raw, out value, out int used) && used == raw.Length);

        /// <summary>A statistics value (a JSON number, or text for dates, timestamps, strings and decimals) as stored bytes; null when unusable.</summary>
        private static byte[]? FromStat(ReadOnlySpan<byte> text, Value value, Leaf column)
        {
            ReadOnlySpan<byte> raw = text.Slice(value.Start, value.Length);
            switch (value.Type)
            {
                case JsonTokenType.Number:
                    return column.Kind switch
                    {
                        Kind.Integer when TryInt64(raw, out long l) => IntegerBytes(l, column.Storage),
                        Kind.Float when TryDouble(raw, out double d) && Fits((float)d, d) => FloatBytes((float)d),
                        Kind.Double when TryDouble(raw, out double d) && double.IsFinite(d) => DoubleBytes(d),
                        Kind.Decimal => DecimalBytes(Encoding.UTF8.GetString(raw), column),
                        _ => null,
                    };
                // Integers and floats as text are not trusted; booleans have no statistics.
                case JsonTokenType.String when column.Kind is Kind.String or Kind.Date or Kind.Timestamp or Kind.Decimal:
                    if (!value.Escaped) return column.Kind == Kind.String ? raw.ToArray() : FromText(Encoding.UTF8.GetString(raw), column);
                    byte[] unescaped = new byte[raw.Length];
                    int length = JsonStrings.Unescape(raw, unescaped, value.Start);
                    return column.Kind == Kind.String ? unescaped[..length] : FromText(Encoding.UTF8.GetString(unescaped, 0, length), column);
                default:
                    return null;
            }
        }
    }

    /// <summary>A value as the log writes partition values (text) as stored bytes; null when unusable.</summary>
    private static byte[]? FromText(string text, Leaf column)
    {
        switch (column.Kind)
        {
            case Kind.Integer when long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out long l):
                return IntegerBytes(l, column.Storage);
            case Kind.Float when float.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out float f) && float.IsFinite(f):
                return FloatBytes(f);
            case Kind.Double when double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out double d) && double.IsFinite(d):
                return DoubleBytes(d);
            case Kind.Decimal:
                return DecimalBytes(text, column);
            case Kind.String:
                return Encoding.UTF8.GetBytes(text);
            case Kind.Boolean when bool.TryParse(text, out bool b):
                return [b ? (byte)1 : (byte)0];
            case Kind.Date when DateOnly.TryParseExact(text, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateOnly date):
                return IntegerBytes(date.DayNumber - DateOnly.FromDateTime(DateTime.UnixEpoch).DayNumber, 4);
            case Kind.Timestamp when DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out DateTimeOffset ts):
                return Int64Bytes((ts.UtcTicks - DateTimeOffset.UnixEpoch.UtcTicks) / 10);
            default:
                return null;
        }
    }

    private static bool Fits(float f, double d) => float.IsFinite(f) && double.IsFinite(d);

    private static byte[]? IntegerBytes(long value, int width)
    {
        if (width == 4)
        {
            if (value is < int.MinValue or > int.MaxValue) return null;
            byte[] four = new byte[4];
            BinaryPrimitives.WriteInt32LittleEndian(four, (int)value);
            return four;
        }
        return Int64Bytes(value);
    }

    private static byte[] Int64Bytes(long value)
    {
        byte[] bytes = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, value);
        return bytes;
    }

    private static byte[] FloatBytes(float value) => BitConverter.GetBytes(value);

    private static byte[] DoubleBytes(double value) => BitConverter.GetBytes(value);

    /// <summary>A decimal's text (plain, as the log writes it) as its unscaled value in the column's storage; null when it does not fit exactly.</summary>
    private static byte[]? DecimalBytes(string text, Leaf column)
    {
        if (!TryUnscaled(text, column.Scale, out BigInteger unscaled)) return null;
        switch (column.Storage)
        {
            case 4 or 8:
                return unscaled >= long.MinValue && unscaled <= long.MaxValue ? IntegerBytes((long)unscaled, column.Storage) : null;
            default:
            {
                // Big-endian two's complement, sign-extended to the fixed length.
                byte[] bytes = unscaled.ToByteArray(isUnsigned: false, isBigEndian: true);
                if (bytes.Length > column.Storage) return null;
                byte[] fixedLength = new byte[column.Storage];
                if (unscaled.Sign < 0) fixedLength.AsSpan(0, column.Storage - bytes.Length).Fill(0xFF);
                bytes.CopyTo(fixedLength, column.Storage - bytes.Length);
                return fixedLength;
            }
        }
    }

    /// <summary>The value of a plain decimal text (sign, digits, optional fraction) times 10^scale; false when inexact or not plain.</summary>
    private static bool TryUnscaled(string text, int scale, out BigInteger unscaled)
    {
        unscaled = default;
        ReadOnlySpan<char> s = text.AsSpan().Trim();
        bool negative = s.StartsWith("-");
        if (negative || s.StartsWith("+")) s = s[1..];
        int dot = s.IndexOf('.');
        ReadOnlySpan<char> whole = dot < 0 ? s : s[..dot], fraction = dot < 0 ? [] : s[(dot + 1)..];
        fraction = fraction.TrimEnd('0');
        if (whole.IsEmpty && fraction.IsEmpty || fraction.Length > scale) return false;
        foreach (char c in whole) if (!char.IsAsciiDigit(c)) return false;
        foreach (char c in fraction) if (!char.IsAsciiDigit(c)) return false;
        string digits = string.Concat(whole, fraction, new string('0', scale - fraction.Length));
        if (!BigInteger.TryParse(digits.Length == 0 ? "0" : digits, NumberStyles.None, CultureInfo.InvariantCulture, out unscaled)) return false;
        if (negative) unscaled = -unscaled;
        return true;
    }
}
