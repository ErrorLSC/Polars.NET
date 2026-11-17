using System.Globalization;
using System.Text;
using System.Text.Json;
using Kaminari.Sazanami.Arrow;
using Kaminari.Sazanami.Helpers;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;

namespace Kaminari.Seseragi.Lakehouse.Delta;

internal sealed partial class CheckpointStatistics
{
    /// <summary>How a typed column's values are appended (the Arrow builder its node gives).</summary>
    private enum ValueKind { Int8, Int16, Int32, Int64, Float, Double, Date, Timestamp, Boolean, String, Binary, Decimal }

    private static ValueKind KindOf(DeltaType type) => type switch
    {
        DecimalType => ValueKind.Decimal,
        PrimitiveType p => p.Name switch
        {
            PrimitiveType.Byte => ValueKind.Int8,
            PrimitiveType.Short => ValueKind.Int16,
            PrimitiveType.Integer => ValueKind.Int32,
            PrimitiveType.Long => ValueKind.Int64,
            PrimitiveType.Float => ValueKind.Float,
            PrimitiveType.Double => ValueKind.Double,
            PrimitiveType.Date => ValueKind.Date,
            PrimitiveType.Timestamp or PrimitiveType.TimestampNtz => ValueKind.Timestamp,
            PrimitiveType.Boolean => ValueKind.Boolean,
            PrimitiveType.Binary => ValueKind.Binary,
            _ => ValueKind.String,
        },
        _ => throw new DeltaProtocolException($"The type {type} has no typed statistics."),
    };

    /// <summary>
    /// Appends the typed columns of add actions straight into native Arrow builders (the <c>partitionValues_parsed</c>
    /// and <c>stats_parsed</c> builders of a checkpoint batch): partition values typed, and each file's JSON statistics
    /// read into the struct as Spark's <c>from_json</c> reads them (a value missing, or not readable as the column's
    /// type, is null; a struct the JSON lacks is null as a whole). The statistics are read with a <see cref="Utf8JsonReader"/>, text is parsed as
    /// spans, nothing is boxed.
    /// </summary>
    /// <param name="statistics">The table's typed columns.</param>
    /// <param name="partitions">The <c>partitionValues_parsed</c> builder (null when the table has no partition columns).</param>
    /// <param name="stats">The <c>stats_parsed</c> builder.</param>
    public sealed class NativeWriter(CheckpointStatistics statistics, NativeStructBuilder? partitions, NativeStructBuilder stats) : IDisposable
    {
        /// <summary>A struct of the statistics: its children by JSON name, each a nested struct or a leaf.</summary>
        private sealed class Node(byte[] name, Node[] children, ValueKind kind, bool nullCount, DecimalType? decimalType)
        {
            public byte[] Name { get; } = name;
            public Node[] Children { get; } = children;
            public ValueKind Kind { get; } = kind;
            public bool NullCount { get; } = nullCount;
            public DecimalType? Decimal { get; } = decimalType;
            public bool[] Seen { get; } = new bool[children.Length]; // per struct, reused row after row
            public bool IsLeaf => Children.Length == 0;
        }

        private static ReadOnlySpan<byte> NumRecords => "numRecords"u8;
        private static ReadOnlySpan<byte> MinValues => "minValues"u8;
        private static ReadOnlySpan<byte> MaxValues => "maxValues"u8;
        private static ReadOnlySpan<byte> NullCountName => "nullCount"u8;
        private static ReadOnlySpan<byte> TightBounds => "tightBounds"u8;

        // The positions of the sections among stats_parsed's children (-1: absent), as StatsNode orders them.
        private readonly int _min = statistics._bounds.Length > 0 ? 1 : -1;
        private readonly int _max = statistics._bounds.Length > 0 ? 2 : -1;
        private readonly int _nullCount = statistics._nullCounts.Length > 0 ? (statistics._bounds.Length > 0 ? 3 : 1) : -1;
        private readonly int _tight = statistics._tightBounds
            ? 1 + (statistics._bounds.Length > 0 ? 2 : 0) + (statistics._nullCounts.Length > 0 ? 1 : 0) : -1;

        // minValues and maxValues share one tree: they are read one after the other.
        private readonly Node? _bounds = statistics._bounds.Length > 0
            ? Tree([.. statistics._bounds.Select(l => (l.Path, KindOf(l.Type), false, l.Type as DecimalType))]) : null;
        private readonly Node? _nullCounts = statistics._nullCounts.Length > 0
            ? Tree([.. statistics._nullCounts.Select(p => (p, ValueKind.Int64, true, (DecimalType?)null))]) : null;
        private readonly bool[] _rootSeen = new bool[stats.Children.Length];

        private NativeByteBuffer? _utf8;      // the statistics text as UTF-8
        private NativeByteBuffer? _unescaped; // a string value unescaped

        /// <summary>Frees the scratch buffers.</summary>
        public void Dispose()
        {
            _utf8?.Release();
            _unescaped?.Release();
        }

        /// <summary>The struct nodes of leaves at their paths, nested in first-seen order (as <see cref="StatsNode"/> nests them).</summary>
        private static Node Tree((string[] Path, ValueKind Kind, bool NullCount, DecimalType? Decimal)[] leaves)
        {
            List<Node> children = [];
            foreach (var group in leaves.GroupBy(l => l.Path[0]))
            {
                var items = group.ToArray();
                byte[] name = Encoding.UTF8.GetBytes(group.Key);
                children.Add(items is [{ Path.Length: 1 } single]
                    ? new Node(name, [], single.Kind, single.NullCount, single.Decimal)
                    : new Node(name, Tree([.. items.Select(i => (i.Path[1..], i.Kind, i.NullCount, i.Decimal))]).Children, default, false, null));
            }
            return new Node([], [.. children], default, false, null);
        }

        /// <summary>Appends the file's <c>partitionValues_parsed</c> (one struct slot).</summary>
        public void AppendPartitionValues(AddAction add)
        {
            if (partitions is null) return;
            Leaf[] leaves = statistics._partitions;
            for (int i = 0; i < leaves.Length; i++)
            {
                Leaf leaf = leaves[i];
                string? text = add.PartitionValues is { } values && values.TryGetValue(leaf.Path[0], out string? v) ? v : null;
                NativeColumnBuilder builder = partitions.Children[i];
                if (string.IsNullOrEmpty(text) || !AppendPartitionText(builder, text, leaf)) builder.AppendNull();
            }
            partitions.EndStruct();
        }

        /// <summary>A partition value typed (as the log's partition text writes it); false when it does not read as the column's type.</summary>
        private static bool AppendPartitionText(NativeColumnBuilder builder, string text, Leaf leaf)
        {
            ValueKind kind = KindOf(leaf.Type);
            switch (kind)
            {
                case ValueKind.Boolean:
                    if (!bool.TryParse(text, out bool b)) return false;
                    ((NativeBooleanBuilder)builder).Append(b);
                    return true;
                case ValueKind.Int8 or ValueKind.Int16 or ValueKind.Int32:
                    if (!int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out int i)) return false;
                    AppendInt(builder, kind, i);
                    return true;
                case ValueKind.Int64:
                    if (!long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out long l)) return false;
                    ((NativeFixedWidthBuilder<long>)builder).Append(l);
                    return true;
                case ValueKind.Float:
                    if (!float.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out float f)) return false;
                    ((NativeFixedWidthBuilder<float>)builder).Append(f);
                    return true;
                case ValueKind.Double:
                    if (!double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out double d)) return false;
                    ((NativeFixedWidthBuilder<double>)builder).Append(d);
                    return true;
                case ValueKind.Binary:
                    return false; // the protocol's escaped byte text; readers take the partitionValues map
                default:
                    return AppendText(builder, text, kind, leaf.Type as DecimalType, partition: true);
            }
        }

        /// <summary>Text as the column's value (as the log and partition paths write it): strings, decimals, dates, timestamps; false when it does not read.</summary>
        private static bool AppendText(NativeColumnBuilder builder, ReadOnlySpan<char> text, ValueKind kind, DecimalType? decimalType, bool partition)
        {
            switch (kind)
            {
                case ValueKind.Decimal:
                    if (Unscaled(text, decimalType!) is not { } unscaled) return false;
                    ((NativeFixedWidthBuilder<Int128>)builder).Append(unscaled);
                    return true;
                case ValueKind.String:
                    ((NativeBinaryBuilder)builder).AppendUtf8(text);
                    return true;
                case ValueKind.Date:
                    if (!DateOnly.TryParseExact(text, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateOnly date)) return false;
                    ((NativeFixedWidthBuilder<int>)builder).Append(date.DayNumber - EpochDay);
                    return true;
                case ValueKind.Timestamp:
                    if (!DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                            out DateTimeOffset ts)) return false;
                    ((NativeFixedWidthBuilder<long>)builder).Append((ts.UtcTicks - DateTimeOffset.UnixEpoch.UtcTicks) / TimeSpan.TicksPerMicrosecond);
                    return true;
                case ValueKind.Double when !partition:
                    // A float column widened to double keeps its old statistics as text in some writers.
                    if (!double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out double d)) return false;
                    ((NativeFixedWidthBuilder<double>)builder).Append(d);
                    return true;
                default:
                    return false;
            }
        }

        private static void AppendInt(NativeColumnBuilder builder, ValueKind kind, int value)
        {
            switch (kind)
            {
                case ValueKind.Int8: ((NativeFixedWidthBuilder<sbyte>)builder).Append(checked((sbyte)value)); break;
                case ValueKind.Int16: ((NativeFixedWidthBuilder<short>)builder).Append(checked((short)value)); break;
                default: ((NativeFixedWidthBuilder<int>)builder).Append(value); break;
            }
        }

        /// <summary>Appends the file's <c>stats_parsed</c> (one struct slot), read from its JSON statistics; null when it has none.</summary>
        public void AppendStats(string? text)
        {
            if (string.IsNullOrEmpty(text))
            {
                stats.AppendNull();
                return;
            }
            _utf8 ??= new NativeByteBuffer(4096, pooled: true);
            _utf8.EnsureCapacity(Encoding.UTF8.GetMaxByteCount(text.Length));
            int length = Encoding.UTF8.GetBytes(text, _utf8.Span);
            switch (Check(_utf8.Span[..length]))
            {
                case Shape.Clean:
                    AppendParsed(_utf8.Span[..length]);
                    break;
                case Shape.Repeats:
                    // A name given twice takes its last value (as a parse into a document, and Spark's from_json, read
                    // it); the reader keeps the first, so such text (rare) is rewritten without repeats, and read as is.
                    if (Deduplicated(_utf8.Memory[..length]) is { } unique) AppendParsed(unique.WrittenSpan);
                    else stats.AppendNull();
                    break;
                default:
                    stats.AppendNull();
                    break;
            }
        }

        /// <summary>What the statistics text is: not valid JSON or not an object; clean; or naming a column it reads twice.</summary>
        private enum Shape { Invalid, Clean, Repeats }

        /// <summary>
        /// Checks the whole text (valid JSON, an object at the root, nothing after it, as a parse does) and, against the
        /// columns read, whether an object names one twice (names compared unescaped; names not read never matter).
        /// </summary>
        private Shape Check(ReadOnlySpan<byte> json)
        {
            try
            {
                var reader = new Utf8JsonReader(json);
                if (!reader.Read()) return Shape.Invalid;
                bool isObject = reader.TokenType == JsonTokenType.StartObject;
                bool repeats = isObject ? RootRepeats(ref reader) : SkipAll(ref reader);
                while (reader.Read())
                {
                    // Anything after the root makes the reader throw.
                }
                return !isObject ? Shape.Invalid : repeats ? Shape.Repeats : Shape.Clean;
            }
            catch (JsonException)
            {
                return Shape.Invalid;
            }

            static bool SkipAll(ref Utf8JsonReader reader)
            {
                reader.Skip();
                return false;
            }
        }

        /// <summary>The root's properties: whether a column read is named twice, at any depth (every value read through).</summary>
        private bool RootRepeats(ref Utf8JsonReader reader)
        {
            bool repeats = false;
            Array.Clear(_rootSeen);
            while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
            {
                int child = RootChild(ref reader);
                reader.Read();
                if (child >= 0 && _rootSeen[child]) repeats = true;
                if (child >= 0) _rootSeen[child] = true;
                if (child >= 0 && child != 0 && child != _tight && reader.TokenType == JsonTokenType.StartObject)
                    repeats |= SectionRepeats(ref reader, child == _nullCount ? _nullCounts! : _bounds!);
                else reader.Skip();
            }
            return repeats;
        }

        private bool SectionRepeats(ref Utf8JsonReader reader, Node node)
        {
            bool repeats = false;
            bool[] seen = node.Seen;
            Array.Clear(seen);
            while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
            {
                int child = ChildOf(ref reader, node);
                reader.Read();
                if (child >= 0 && seen[child]) repeats = true;
                if (child >= 0) seen[child] = true;
                if (child >= 0 && !node.Children[child].IsLeaf && reader.TokenType == JsonTokenType.StartObject)
                    repeats |= SectionRepeats(ref reader, node.Children[child]);
                else reader.Skip();
            }
            return repeats;
        }

        /// <summary>The stats_parsed child a root property names (-1: none read).</summary>
        private int RootChild(ref Utf8JsonReader reader) =>
            reader.ValueTextEquals(NumRecords) ? 0
            : _min >= 0 && reader.ValueTextEquals(MinValues) ? _min
            : _max >= 0 && reader.ValueTextEquals(MaxValues) ? _max
            : _nullCount >= 0 && reader.ValueTextEquals(NullCountName) ? _nullCount
            : _tight >= 0 && reader.ValueTextEquals(TightBounds) ? _tight
            : -1;

        /// <summary>The child of a struct node a property names (-1: none read).</summary>
        private static int ChildOf(ref Utf8JsonReader reader, Node node)
        {
            for (int i = 0; i < node.Children.Length; i++)
            {
                if (reader.ValueTextEquals(node.Children[i].Name)) return i;
            }
            return -1;
        }

        /// <summary>Appends checked statistics text (an object, each column read named once at most).</summary>
        private void AppendParsed(ReadOnlySpan<byte> json)
        {
            var reader = new Utf8JsonReader(json);
            reader.Read(); // {
            Array.Clear(_rootSeen);
            while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
            {
                int child = RootChild(ref reader);
                reader.Read();
                if (child < 0 || _rootSeen[child])
                {
                    reader.Skip();
                    continue;
                }
                _rootSeen[child] = true;
                NativeColumnBuilder builder = stats.Children[child];
                if (child == 0)
                {
                    if (reader.TokenType == JsonTokenType.Number && reader.TryGetInt64(out long rows)) ((NativeFixedWidthBuilder<long>)builder).Append(rows);
                    else
                    {
                        builder.AppendNull();
                        reader.Skip();
                    }
                }
                else if (child == _tight)
                {
                    if (reader.TokenType is JsonTokenType.True or JsonTokenType.False) ((NativeBooleanBuilder)builder).Append(reader.GetBoolean());
                    else
                    {
                        builder.AppendNull();
                        reader.Skip();
                    }
                }
                else
                {
                    Node section = child == _nullCount ? _nullCounts! : _bounds!;
                    AppendSection(ref reader, (NativeStructBuilder)builder, section);
                }
            }
            for (int i = 0; i < _rootSeen.Length; i++)
            {
                if (!_rootSeen[i]) stats.Children[i].AppendNull();
            }
            stats.EndStruct();
        }

        /// <summary>The JSON with every object's repeated names dropped but the last; null when it cannot be rewritten.</summary>
        private static System.Buffers.ArrayBufferWriter<byte>? Deduplicated(ReadOnlyMemory<byte> json)
        {
            try
            {
                using JsonDocument document = JsonDocument.Parse(json);
                var output = new System.Buffers.ArrayBufferWriter<byte>(json.Length);
                using (var writer = new Utf8JsonWriter(output, new JsonWriterOptions { Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping }))
                    Write(document.RootElement, writer);
                return output; // a rare row: managed is fine
            }
            catch (InvalidOperationException)
            {
                return null; // a name with an escaped lone surrogate
            }

            static void Write(JsonElement element, Utf8JsonWriter writer)
            {
                if (element.ValueKind != JsonValueKind.Object)
                {
                    element.WriteTo(writer);
                    return;
                }
                var last = new Dictionary<string, JsonElement>(StringComparer.Ordinal);
                foreach (JsonProperty property in element.EnumerateObject()) last[property.Name] = property.Value;
                writer.WriteStartObject();
                foreach (var (name, value) in last)
                {
                    writer.WritePropertyName(name);
                    Write(value, writer);
                }
                writer.WriteEndObject();
            }
        }

        /// <summary>A section (or nested struct) of the statistics: the struct slot when the JSON holds an object, else null.</summary>
        private void AppendSection(ref Utf8JsonReader reader, NativeStructBuilder builder, Node node)
        {
            if (reader.TokenType != JsonTokenType.StartObject)
            {
                // A struct the JSON lacks, or holds as something else, is null as a whole (Spark's from_json).
                builder.AppendNull();
                reader.Skip();
                return;
            }
            bool[] seen = node.Seen;
            Array.Clear(seen);
            while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
            {
                int child = ChildOf(ref reader, node);
                reader.Read();
                if (child < 0 || seen[child])
                {
                    reader.Skip();
                    continue;
                }
                seen[child] = true;
                Node target = node.Children[child];
                NativeColumnBuilder childBuilder = builder.Children[child];
                if (reader.TokenType == JsonTokenType.Null)
                {
                    childBuilder.AppendNull();
                }
                else if (!target.IsLeaf)
                {
                    AppendSection(ref reader, (NativeStructBuilder)childBuilder, target); // the child has its own Seen
                }
                else if (!AppendStat(ref reader, childBuilder, target))
                {
                    childBuilder.AppendNull();
                    reader.Skip();
                }
            }
            for (int i = 0; i < seen.Length; i++)
            {
                if (!seen[i]) builder.Children[i].AppendNull();
            }
            builder.EndStruct();
        }

        /// <summary>A statistic as the column's value (a number, or text for decimals, dates, timestamps, strings); false (nothing appended) when it is not one.</summary>
        private bool AppendStat(ref Utf8JsonReader reader, NativeColumnBuilder builder, Node leaf)
        {
            if (leaf.NullCount)
            {
                if (reader.TokenType != JsonTokenType.Number || !reader.TryGetInt64(out long count)) return false;
                ((NativeFixedWidthBuilder<long>)builder).Append(count);
                return true;
            }
            if (reader.TokenType == JsonTokenType.Number)
            {
                switch (leaf.Kind)
                {
                    case ValueKind.Decimal:
                    {
                        ReadOnlySpan<byte> raw = reader.ValueSpan;
                        if (raw.Length > 256) return false;
                        Span<char> chars = stackalloc char[raw.Length];
                        for (int i = 0; i < raw.Length; i++) chars[i] = (char)raw[i]; // JSON numbers are ASCII
                        if (Unscaled(chars, leaf.Decimal!) is not { } unscaled) return false;
                        ((NativeFixedWidthBuilder<Int128>)builder).Append(unscaled);
                        return true;
                    }
                    case ValueKind.Int8 or ValueKind.Int16 or ValueKind.Int32:
                        if (!reader.TryGetInt32(out int i32)) return false;
                        AppendInt(builder, leaf.Kind, i32);
                        return true;
                    case ValueKind.Int64:
                        if (!reader.TryGetInt64(out long i64)) return false;
                        ((NativeFixedWidthBuilder<long>)builder).Append(i64);
                        return true;
                    // Negative zero becomes zero, as Spark's typed statistics hold it (the two compare equal).
                    case ValueKind.Float:
                        if (!reader.TryGetSingle(out float f)) return false;
                        ((NativeFixedWidthBuilder<float>)builder).Append(f + 0f);
                        return true;
                    case ValueKind.Double:
                        if (!reader.TryGetDouble(out double d)) return false;
                        ((NativeFixedWidthBuilder<double>)builder).Append(d + 0d);
                        return true;
                    default:
                        return false;
                }
            }
            if (reader.TokenType != JsonTokenType.String) return false;

            // The string unescaped, as UTF-8.
            ReadOnlySpan<byte> utf8 = reader.ValueSpan;
            if (reader.ValueIsEscaped)
            {
                _unescaped ??= new NativeByteBuffer(256, pooled: true);
                _unescaped.EnsureCapacity(utf8.Length);
                int written;
                try
                {
                    written = reader.CopyString(_unescaped.Span);
                }
                catch (InvalidOperationException)
                {
                    return false; // an escaped lone surrogate: not text
                }
                utf8 = _unescaped.Span[..written];
            }
            if (leaf.Kind == ValueKind.String)
            {
                ((NativeBinaryBuilder)builder).Append(utf8);
                return true;
            }
            if (leaf.Kind is not (ValueKind.Decimal or ValueKind.Date or ValueKind.Timestamp or ValueKind.Double)) return false;
            if (utf8.Length > 256) return false; // longer than any date, timestamp or 38-digit decimal
            Span<char> text = stackalloc char[256];
            int length = Encoding.UTF8.GetChars(utf8, text);
            return AppendText(builder, text[..length], leaf.Kind, leaf.Decimal, partition: false);
        }

        /// <summary>
        /// The unscaled value of decimal text (<c>-12.5</c> at scale 2 is -1250); null when it has
        /// more fraction digits than the scale, more digits than the precision, or is not a number.
        /// </summary>
        private static Int128? Unscaled(ReadOnlySpan<char> text, DecimalType type)
        {
            ReadOnlySpan<char> trimmed = text.Trim();
            bool negative = trimmed.Length > 0 && trimmed[0] == '-';
            scoped ReadOnlySpan<char> digits = trimmed.TrimStart("-+");
            Span<char> plainDigits = stackalloc char[64];
            if (digits.ContainsAny('e', 'E'))
            {
                if (!decimal.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out decimal plain)
                    || !Math.Abs(plain).TryFormat(plainDigits, out int written, default, CultureInfo.InvariantCulture))
                    return null;
                digits = plainDigits[..written];
            }
            int dot = digits.IndexOf('.');
            ReadOnlySpan<char> whole = (dot < 0 ? digits : digits[..dot]).TrimStart('0');
            ReadOnlySpan<char> fraction = dot < 0 ? [] : digits[(dot + 1)..].TrimEnd('0');
            if (fraction.Length > type.Scale || whole.ContainsAnyExceptInRange('0', '9') || fraction.ContainsAnyExceptInRange('0', '9')) return null;
            if (whole.Length + type.Scale > type.Precision) return null; // at least 10^precision
            Int128 unscaled = 0;
            foreach (char c in whole) unscaled = unscaled * 10 + (c - '0');
            foreach (char c in fraction) unscaled = unscaled * 10 + (c - '0');
            for (int i = fraction.Length; i < type.Scale; i++) unscaled *= 10;
            return negative ? -unscaled : unscaled;
        }
    }
}
