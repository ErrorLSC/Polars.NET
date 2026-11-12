using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Kaminari.Seseragi.Files.Avro;

/// <summary>
/// Maps an Arrow schema to an Avro writer schema and the encoders for it. Rules: a nullable field is
/// <c>["null", T]</c>; strings, binaries, lists, structs, dates, naive and zoned timestamps and decimals as
/// <c>bytes</c> follow the reference dataframe writer; the rest follows spark-avro (narrow integers widen to int,
/// nested records are named after their field inside the parent's namespace, day-time and year-month intervals
/// carry <c>spark.sql.catalyst.type</c>, map keys must be strings). Where neither has a rule and a value could
/// change, the value is refused at write time instead (unsigned 64-bit beyond long, sub-microsecond times,
/// negative or sub-millisecond Avro durations).
/// </summary>
internal static class AvroWriteSchema
{
    public static (AvroRecordSchema Schema, RowEncoder Encoder) Build(Schema arrow, string recordName, string? recordNamespace)
    {
        if (!AvroSchemaParser.IsValidName(recordName)) throw new ArgumentException($"'{recordName}' is not a valid Avro record name.", nameof(recordName));
        var context = new Context();
        var root = new AvroRecordSchema(recordName, string.IsNullOrEmpty(recordNamespace) ? null : recordNamespace);
        context.Claim(root.FullName);
        List<AvroField> fields = [];
        List<AvroEncoder> encoders = [];
        foreach (var f in arrow.FieldsList)
        {
            var (schema, encoder) = context.Map(f, root.FullName, f.Name);
            fields.Add(new AvroField(f.Name, schema, fields.Count));
            encoders.Add(encoder);
        }
        root.Fields = fields;
        return (root, new RowEncoder([.. encoders]));
    }

    /// <summary>Whether a batch column of type <paramref name="actual"/> can be written by encoders compiled for
    /// <paramref name="expected"/>: same physical layout and the same parameters that change the encoding.</summary>
    public static bool SameEncoding(IArrowType expected, IArrowType actual) => (expected, actual) switch
    {
        _ when ReferenceEquals(expected, actual) => true,
        _ when expected.TypeId != actual.TypeId => false,
        (TimestampType e, TimestampType a) => e.Unit == a.Unit && (e.Timezone ?? "") == (a.Timezone ?? ""),
        (TimeType e, TimeType a) => e.Unit == a.Unit,
        (DurationType e, DurationType a) => e.Unit == a.Unit,
        (IntervalType e, IntervalType a) => e.Unit == a.Unit,
        (Decimal32Type e, Decimal32Type a) => (e.Precision, e.Scale) == (a.Precision, a.Scale),
        (Decimal64Type e, Decimal64Type a) => (e.Precision, e.Scale) == (a.Precision, a.Scale),
        (Decimal128Type e, Decimal128Type a) => (e.Precision, e.Scale) == (a.Precision, a.Scale),
        (Decimal256Type e, Decimal256Type a) => (e.Precision, e.Scale) == (a.Precision, a.Scale),
        (FixedSizeBinaryType e, FixedSizeBinaryType a) => e.ByteWidth == a.ByteWidth,
        (DictionaryType e, DictionaryType a) => SameEncoding(e.IndexType, a.IndexType) && SameEncoding(e.ValueType, a.ValueType),
        (FixedSizeListType e, FixedSizeListType a) => e.ListSize == a.ListSize && SameFields(e.Fields, a.Fields),
        (NestedType e, NestedType a) => SameFields(e.Fields, a.Fields), // list, large list, map, struct
        _ => true,
    };

    private static bool SameFields(IReadOnlyList<Field> e, IReadOnlyList<Field> a)
    {
        if (e.Count != a.Count) return false;
        for (int i = 0; i < e.Count; i++)
            if (!SameEncoding(e[i].DataType, a[i].DataType)) return false;
        return true;
    }

    private sealed class Context
    {
        private readonly HashSet<string> _names = [];

        public void Claim(string fullName) => _names.Add(fullName);

        /// <summary>A unique named-type full name <c>{parent}.{name}</c>, made a valid Avro name.</summary>
        private (string Name, string Namespace) NameFor(string parentFullName, string fieldName)
        {
            string name = Sanitize(fieldName);
            string candidate = name;
            for (int i = 1; !_names.Add($"{parentFullName}.{candidate}"); i++) candidate = $"{name}_{i}";
            return (candidate, parentFullName);
        }

        private static string Sanitize(string name)
        {
            var sb = new StringBuilder(name.Length + 1);
            foreach (char c in name) sb.Append(char.IsAsciiLetterOrDigit(c) || c == '_' ? c : '_');
            if (sb.Length == 0 || char.IsAsciiDigit(sb[0])) sb.Insert(0, '_');
            return sb.ToString();
        }

        /// <summary>Maps a field (wrapping nullable ones in a <c>["null", T]</c> union). A named type the field produces
        /// is called <paramref name="typeName"/> (default: the field name); list items and map values pass their
        /// container's field name, as spark-avro does.</summary>
        public (AvroSchema, AvroEncoder) Map(Field field, string parentFullName, string path, string? typeName = null)
        {
            var (schema, encoder) = MapType(field.DataType, parentFullName, typeName ?? field.Name, path);
            return (field.IsNullable, schema.Type) switch
            {
                (_, AvroType.Null) => (schema, encoder),
                (true, _) => (new AvroUnionSchema([new AvroPrimitiveSchema(AvroType.Null), schema]), new NullableEncoder(encoder)),
                (false, _) => (schema, new RequiredEncoder(encoder, $"column '{path}'")),
            };
        }

        private static AvroPrimitiveSchema P(AvroType t, AvroLogicalKind logical = AvroLogicalKind.None) => new(t) { Logical = logical };

        private static (AvroSchema, AvroEncoder) Int<T>(AvroType t, AvroLogicalKind logical, string what, long multiply = 1, long divide = 1)
            where T : unmanaged, System.Numerics.IBinaryInteger<T> =>
            (P(t, logical), new IntegerEncoder<T>(multiply, divide, t == AvroType.Int, what));

        private (AvroSchema, AvroEncoder) MapType(IArrowType type, string parentFullName, string fieldName, string path)
        {
            string what = $"column '{path}' ({type.Name})";
            return type switch
            {
                NullType => (P(AvroType.Null), new NullEncoder()),
                BooleanType => (P(AvroType.Boolean), new BooleanEncoder()),
                Int8Type => Int<sbyte>(AvroType.Int, AvroLogicalKind.None, what),
                Int16Type => Int<short>(AvroType.Int, AvroLogicalKind.None, what),
                Int32Type => Int<int>(AvroType.Int, AvroLogicalKind.None, what),
                Int64Type => Int<long>(AvroType.Long, AvroLogicalKind.None, what),
                UInt8Type => Int<byte>(AvroType.Int, AvroLogicalKind.None, what),
                UInt16Type => Int<ushort>(AvroType.Int, AvroLogicalKind.None, what),
                UInt32Type => Int<uint>(AvroType.Long, AvroLogicalKind.None, what),
                UInt64Type => Int<ulong>(AvroType.Long, AvroLogicalKind.None, what),
                HalfFloatType => (P(AvroType.Float), new HalfEncoder()),
                FloatType => (P(AvroType.Float), new FloatEncoder()),
                DoubleType => (P(AvroType.Double), new DoubleEncoder()),
                StringType => (P(AvroType.String), new BinaryEncoder()),
                LargeStringType => (P(AvroType.String), new LargeBinaryEncoder()),
                StringViewType => (P(AvroType.String), new BinaryViewEncoder()),
                BinaryType => (P(AvroType.Bytes), new BinaryEncoder()),
                LargeBinaryType => (P(AvroType.Bytes), new LargeBinaryEncoder()),
                BinaryViewType => (P(AvroType.Bytes), new BinaryViewEncoder()),
                Decimal32Type d => Decimal(d.Precision, d.Scale, 4),
                Decimal64Type d => Decimal(d.Precision, d.Scale, 8),
                Decimal128Type d => Decimal(d.Precision, d.Scale, 16),
                Decimal256Type d => Decimal(d.Precision, d.Scale, 32),
                FixedSizeBinaryType f => Fixed(f.ByteWidth), // after the decimals, which derive from it
                Date32Type => Int<int>(AvroType.Int, AvroLogicalKind.Date, what),
                Date64Type => Int<long>(AvroType.Int, AvroLogicalKind.Date, what, divide: 86_400_000),
                Time32Type { Unit: TimeUnit.Second } => Int<int>(AvroType.Int, AvroLogicalKind.TimeMillis, what, multiply: 1000),
                Time32Type => Int<int>(AvroType.Int, AvroLogicalKind.TimeMillis, what),
                Time64Type { Unit: TimeUnit.Nanosecond } => Int<long>(AvroType.Long, AvroLogicalKind.TimeMicros, what, divide: 1000),
                Time64Type => Int<long>(AvroType.Long, AvroLogicalKind.TimeMicros, what),
                TimestampType t => Timestamp(t),
                DurationType { Unit: TimeUnit.Second } => SparkDayTime(1_000_000, 1),
                DurationType { Unit: TimeUnit.Millisecond } => SparkDayTime(1000, 1),
                DurationType { Unit: TimeUnit.Microsecond } => SparkDayTime(1, 1),
                DurationType => SparkDayTime(1, 1000),
                IntervalType { Unit: IntervalUnit.YearMonth } => Int<int>(AvroType.Int, AvroLogicalKind.SparkYearMonthInterval, what),
                IntervalType { Unit: IntervalUnit.DayTime } => Duration(new DayTimeDurationEncoder()),
                IntervalType => Duration(new MonthDayNanoDurationEncoder()),
                DictionaryType d => Dictionary(d),
                MapType m => Map(m),
                ListType l => List(l.ValueField, ListLayout.Int32Offsets, 0, parentFullName, fieldName, path),
                LargeListType l => List(l.ValueField, ListLayout.Int64Offsets, 0, parentFullName, fieldName, path),
                FixedSizeListType l => List(l.ValueField, ListLayout.FixedSize, l.ListSize, parentFullName, fieldName, path),
                StructType s => Record(s),
                _ => throw Unsupported(what),
            };

            static (AvroSchema, AvroEncoder) Decimal(int precision, int scale, int width) =>
                (new AvroPrimitiveSchema(AvroType.Bytes) { Logical = AvroLogicalKind.Decimal, Precision = precision, Scale = scale },
                 new DecimalEncoder(width));

            (AvroSchema, AvroEncoder) SparkDayTime(long multiply, long divide) =>
                Int<long>(AvroType.Long, AvroLogicalKind.SparkDayTimeInterval, what, multiply, divide);

            (AvroSchema, AvroEncoder) Fixed(int size)
            {
                var (name, ns) = NameFor(parentFullName, fieldName);
                return (new AvroFixedSchema(name, ns, size), new FixedEncoder(size));
            }

            (AvroSchema, AvroEncoder) Duration(AvroEncoder encoder)
            {
                var (name, ns) = NameFor(parentFullName, fieldName);
                return (new AvroFixedSchema(name, ns, 12) { Logical = AvroLogicalKind.Duration }, encoder);
            }

            (AvroSchema, AvroEncoder) Timestamp(TimestampType t)
            {
                bool zoned = !string.IsNullOrEmpty(t.Timezone);
                var (logical, multiply) = (t.Unit, zoned) switch
                {
                    (TimeUnit.Second, true) => (AvroLogicalKind.TimestampMillis, 1000L),
                    (TimeUnit.Millisecond, true) => (AvroLogicalKind.TimestampMillis, 1L),
                    (TimeUnit.Microsecond, true) => (AvroLogicalKind.TimestampMicros, 1L),
                    (TimeUnit.Nanosecond, true) => (AvroLogicalKind.TimestampNanos, 1L),
                    (TimeUnit.Second, false) => (AvroLogicalKind.LocalTimestampMillis, 1000L),
                    (TimeUnit.Millisecond, false) => (AvroLogicalKind.LocalTimestampMillis, 1L),
                    (TimeUnit.Microsecond, false) => (AvroLogicalKind.LocalTimestampMicros, 1L),
                    _ => (AvroLogicalKind.LocalTimestampNanos, 1L),
                };
                // The zone survives in a schema attribute; UTC is what any reader assumes, so it is left implicit.
                string? zone = zoned && t.Timezone != "UTC" ? t.Timezone : null;
                return (new AvroPrimitiveSchema(AvroType.Long) { Logical = logical, Timezone = zone },
                        new IntegerEncoder<long>(multiply, 1, false, what));
            }

            (AvroSchema, AvroEncoder) Dictionary(DictionaryType d)
            {
                var (valueSchema, valueEncoder) = MapType(d.ValueType, parentFullName, fieldName, path);
                AvroEncoder encoder = d.IndexType switch
                {
                    Int8Type => new DictionaryEncoder<sbyte>(valueEncoder),
                    Int16Type => new DictionaryEncoder<short>(valueEncoder),
                    Int32Type => new DictionaryEncoder<int>(valueEncoder),
                    Int64Type => new DictionaryEncoder<long>(valueEncoder),
                    UInt8Type => new DictionaryEncoder<byte>(valueEncoder),
                    UInt16Type => new DictionaryEncoder<ushort>(valueEncoder),
                    UInt32Type => new DictionaryEncoder<uint>(valueEncoder),
                    UInt64Type => new DictionaryEncoder<ulong>(valueEncoder),
                    _ => throw Unsupported(what),
                };
                return (valueSchema, encoder);
            }

            (AvroSchema, AvroEncoder) Map(MapType m)
            {
                if (m.KeyField.DataType is not (StringType or LargeStringType or StringViewType
                    or DictionaryType { ValueType: StringType or LargeStringType or StringViewType }))
                    throw new NotSupportedException($"Cannot write {what} to Avro: Avro map keys must be strings.");
                var (_, keyEncoder) = MapType(m.KeyField.DataType, parentFullName, fieldName, path + ".key");
                var (valueSchema, valueEncoder) = this.Map(m.ValueField, parentFullName, path + ".value", fieldName);
                return (new AvroMapSchema(valueSchema), new MapEncoder(keyEncoder, valueEncoder));
            }

            (AvroSchema, AvroEncoder) Record(StructType s)
            {
                var (name, ns) = NameFor(parentFullName, fieldName);
                var record = new AvroRecordSchema(name, ns);
                List<AvroField> fields = [];
                List<AvroEncoder> encoders = [];
                foreach (var f in s.Fields)
                {
                    var (schema, encoder) = this.Map(f, record.FullName, $"{path}.{f.Name}");
                    fields.Add(new AvroField(f.Name, schema, fields.Count));
                    encoders.Add(encoder);
                }
                record.Fields = fields;
                return (record, new StructEncoder([.. encoders]));
            }
        }

        private (AvroSchema, AvroEncoder) List(Field item, ListLayout layout, int size, string parentFullName, string fieldName, string path)
        {
            var (itemSchema, itemEncoder) = Map(item, parentFullName, path + ".item", fieldName);
            return (new AvroArraySchema(itemSchema), new ListEncoder(layout, size, itemEncoder));
        }

        private static NotSupportedException Unsupported(string what) => new($"Cannot write {what} to Avro: the type has no Avro mapping.");
    }
}
