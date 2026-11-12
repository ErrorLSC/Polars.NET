using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow;

namespace Kaminari.Seseragi.Files.Avro;

/// <summary>Decodes one Avro value and appends it to a native Arrow builder.</summary>
internal abstract class AvroDecoder
{
    public abstract void Decode(ref AvroSpanReader r);

    /// <summary>
    /// Compiles a decoder for <paramref name="schema"/> writing into <paramref name="builder"/>, which must have
    /// been created from the Arrow type <see cref="AvroReadSchema"/> maps the schema to.
    /// </summary>
    public static AvroDecoder Compile(AvroSchema schema, NativeColumnBuilder builder) => schema switch
    {
        AvroUnionSchema u => CompileUnion(u, builder),
        AvroRecordSchema rec => new RecordDecoder((NativeStructBuilder)builder,
            [.. rec.Fields.Select((f, i) => Compile(f.Schema, ((NativeStructBuilder)builder).Children[i]))]),
        AvroEnumSchema e => new EnumDecoder((NativeDictionaryBuilder)builder, e.Symbols),
        AvroFixedSchema f when f.Logical == AvroLogicalKind.Duration => new DurationDecoder(Fixed<MonthDayNanos>(builder)),
        AvroFixedSchema f when AvroReadSchema.IsArrowDecimal(f) => CompileDecimal(builder, f.Size),
        AvroFixedSchema f => new FixedDecoder((NativeFixedSizeBinaryBuilder)builder, f.Size),
        AvroArraySchema a => CompileArray(a, (NativeListBuilder)builder),
        AvroMapSchema m => CompileMap(m, (NativeListBuilder)builder),
        _ => CompilePrimitive(schema, builder),
    };

    /// <summary>The fewest bytes a value of the schema can take; bounds array block counts on corrupt input.</summary>
    internal static int MinEncodedBytes(AvroSchema s, HashSet<AvroRecordSchema> visiting) => s switch
    {
        AvroRecordSchema rec => visiting.Add(rec) ? rec.Fields.Sum(f => MinEncodedBytes(f.Schema, visiting)) : 0,
        AvroFixedSchema f => f.Size,
        _ => s.Type switch
        {
            AvroType.Null => 0,
            AvroType.Float => 4,
            AvroType.Double => 8,
            _ => 1,
        },
    };

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void CheckBlockCount(long count, int minItemBytes, ref AvroSpanReader r)
    {
        if (minItemBytes > 0 ? count > r.Remaining / minItemBytes : count > int.MaxValue)
            throw new AvroFormatException($"Avro block count {count} exceeds the remaining data.");
    }

    private static NativeFixedWidthBuilder<T> Fixed<T>(NativeColumnBuilder b) where T : unmanaged =>
        (NativeFixedWidthBuilder<T>)b;

    private static AvroDecoder CompilePrimitive(AvroSchema s, NativeColumnBuilder b) => s.Type switch
    {
        AvroType.Null => new NullDecoder(b),
        AvroType.Boolean => new BooleanDecoder((NativeBooleanBuilder)b),
        AvroType.Int => new IntDecoder(Fixed<int>(b)),
        AvroType.Long => new LongDecoder(Fixed<long>(b)),
        AvroType.Float => new FloatDecoder(Fixed<float>(b)),
        AvroType.Double => new DoubleDecoder(Fixed<double>(b)),
        AvroType.Bytes when AvroReadSchema.IsArrowDecimal(s) => CompileDecimal(b, -1),
        AvroType.Bytes => new BinaryDecoder((NativeBinaryBuilder)b),
        AvroType.String => new StringDecoder((NativeBinaryBuilder)b),
        _ => throw new InvalidOperationException($"Unexpected Avro type {s.Type}."),
    };

    private static AvroDecoder CompileDecimal(NativeColumnBuilder b, int fixedSize) => b.Type switch
    {
        Decimal128Type => new Decimal128Decoder(Fixed<Int128>(b), fixedSize),
        _ => new Decimal256Decoder(Fixed<Int256Bits>(b), fixedSize),
    };

    /// <summary>Arrays of plain numbers get loops without a virtual call per item (and a bulk copy for
    /// float/double, whose Avro and Arrow encodings are both little-endian IEEE 754).</summary>
    private static AvroDecoder CompileArray(AvroArraySchema a, NativeListBuilder list) => (a.Items, list.Values) switch
    {
        (AvroUnionSchema or AvroNamedSchema, var items) => new ArrayDecoder(list, Compile(a.Items, items), MinEncodedBytes(a.Items, [])),
        ({ Type: AvroType.Long }, NativeFixedWidthBuilder<long> items) => new LongArrayDecoder(list, items),
        ({ Type: AvroType.Int }, NativeFixedWidthBuilder<int> items) => new IntArrayDecoder(list, items),
        ({ Type: AvroType.Double }, NativeFixedWidthBuilder<double> items) => new RawArrayDecoder<double>(list, items),
        ({ Type: AvroType.Float }, NativeFixedWidthBuilder<float> items) => new RawArrayDecoder<float>(list, items),
        (_, var items) => new ArrayDecoder(list, Compile(a.Items, items), MinEncodedBytes(a.Items, [])),
    };

    private static AvroDecoder CompileMap(AvroMapSchema m, NativeListBuilder list)
    {
        var entries = (NativeStructBuilder)list.Values;
        return new MapDecoder(list, entries, (NativeBinaryBuilder)entries.Children[0], Compile(m.Values, entries.Children[1]));
    }

    private static AvroDecoder CompileUnion(AvroUnionSchema u, NativeColumnBuilder builder)
    {
        var shape = AvroReadSchema.ShapeOf(u, out var nonNull);
        var branches = new AvroDecoder?[u.Branches.Count];
        return shape switch
        {
            AvroReadSchema.UnionShape.NullOnly => new UnionDecoder(builder, branches),
            AvroReadSchema.UnionShape.Single => Single(),
            AvroReadSchema.UnionShape.PromoteToLong or AvroReadSchema.UnionShape.PromoteToDouble => Promoted(),
            _ => Members((NativeStructBuilder)builder),
        };

        UnionDecoder Single()
        {
            branches[nonNull[0]] = Compile(u.Branches[nonNull[0]], builder);
            return new UnionDecoder(builder, branches);
        }

        UnionDecoder Promoted()
        {
            foreach (int i in nonNull)
            {
                branches[i] = u.Branches[i].Type switch
                {
                    AvroType.Int => new IntAsLongDecoder(Fixed<long>(builder)),
                    AvroType.Long => new LongDecoder(Fixed<long>(builder)),
                    AvroType.Float => new FloatAsDoubleDecoder(Fixed<double>(builder)),
                    _ => new DoubleDecoder(Fixed<double>(builder)),
                };
            }
            return new UnionDecoder(builder, branches);
        }

        MemberStructUnionDecoder Members(NativeStructBuilder members)
        {
            var memberOf = new int[u.Branches.Count];
            Array.Fill(memberOf, -1);
            for (int m = 0; m < nonNull.Count; m++)
            {
                branches[nonNull[m]] = Compile(u.Branches[nonNull[m]], members.Children[m]);
                memberOf[nonNull[m]] = m;
            }
            return new MemberStructUnionDecoder(members, branches, memberOf);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected static int ReadBranch(ref AvroSpanReader r, int count)
    {
        long index = r.ReadLong();
        if ((ulong)index >= (ulong)count) throw new AvroFormatException($"Avro union branch index {index} is out of range (the union has {count} branches).");
        return (int)index;
    }
}

internal sealed class NullDecoder(NativeColumnBuilder b) : AvroDecoder
{
    public override void Decode(ref AvroSpanReader r) => b.AppendNull();
}

internal sealed class BooleanDecoder(NativeBooleanBuilder b) : AvroDecoder
{
    public override void Decode(ref AvroSpanReader r) => b.Append(r.ReadBoolean());
}

internal sealed class IntDecoder(NativeFixedWidthBuilder<int> b) : AvroDecoder
{
    public override void Decode(ref AvroSpanReader r) => b.Append(r.ReadInt());
}

internal sealed class LongDecoder(NativeFixedWidthBuilder<long> b) : AvroDecoder
{
    public override void Decode(ref AvroSpanReader r) => b.Append(r.ReadLong());
}

internal sealed class IntAsLongDecoder(NativeFixedWidthBuilder<long> b) : AvroDecoder
{
    public override void Decode(ref AvroSpanReader r) => b.Append(r.ReadInt());
}

internal sealed class FloatDecoder(NativeFixedWidthBuilder<float> b) : AvroDecoder
{
    public override void Decode(ref AvroSpanReader r) => b.Append(r.ReadFloat());
}

internal sealed class FloatAsDoubleDecoder(NativeFixedWidthBuilder<double> b) : AvroDecoder
{
    public override void Decode(ref AvroSpanReader r) => b.Append(r.ReadFloat());
}

internal sealed class DoubleDecoder(NativeFixedWidthBuilder<double> b) : AvroDecoder
{
    public override void Decode(ref AvroSpanReader r) => b.Append(r.ReadDouble());
}

/// <summary>Avro <c>string</c>: UTF-8 is validated, since Arrow Utf8 consumers rely on it.</summary>
internal sealed class StringDecoder(NativeBinaryBuilder b) : AvroDecoder
{
    public override void Decode(ref AvroSpanReader r) => b.Append(r.ReadUtf8());
}

internal sealed class BinaryDecoder(NativeBinaryBuilder b) : AvroDecoder
{
    public override void Decode(ref AvroSpanReader r) => b.Append(r.ReadBytes());
}

internal sealed class FixedDecoder(NativeFixedSizeBinaryBuilder b, int size) : AvroDecoder
{
    public override void Decode(ref AvroSpanReader r) => b.Append(r.ReadFixed(size));
}

internal sealed class EnumDecoder : AvroDecoder
{
    private readonly NativeDictionaryBuilder _b;
    private readonly int _count;

    public EnumDecoder(NativeDictionaryBuilder b, IReadOnlyList<string> symbols)
    {
        _b = b;
        _count = symbols.Count;
        b.Symbols = symbols;
    }

    public override void Decode(ref AvroSpanReader r)
    {
        long index = r.ReadLong();
        if ((ulong)index >= (ulong)_count) throw new AvroFormatException($"Avro enum index {index} is out of range ({_count} symbols).");
        _b.Append((int)index);
    }
}

/// <summary>Avro <c>duration</c>: three little-endian unsigned 32-bit ints (months, days, milliseconds).</summary>
internal sealed class DurationDecoder(NativeFixedWidthBuilder<MonthDayNanos> b) : AvroDecoder
{
    public override void Decode(ref AvroSpanReader r)
    {
        var bytes = r.ReadFixed(12);
        uint months = BinaryPrimitives.ReadUInt32LittleEndian(bytes);
        uint days = BinaryPrimitives.ReadUInt32LittleEndian(bytes[4..]);
        uint millis = BinaryPrimitives.ReadUInt32LittleEndian(bytes[8..]);
        if (months > int.MaxValue || days > int.MaxValue)
            throw new AvroFormatException($"Avro duration ({months} months, {days} days) exceeds the Arrow interval range.");
        b.Append(new MonthDayNanos((int)months, (int)days, millis * 1_000_000L));
    }
}

/// <summary>Decimal from big-endian two's-complement bytes (<c>bytes</c>, or <c>fixed</c> when size ≥ 0).</summary>
internal sealed class Decimal128Decoder(NativeFixedWidthBuilder<Int128> b, int fixedSize) : AvroDecoder
{
    public override void Decode(ref AvroSpanReader r)
    {
        var bytes = fixedSize >= 0 ? r.ReadFixed(fixedSize) : r.ReadBytes();
        Span<byte> le = stackalloc byte[16];
        DecimalBytes.SignExtendLittleEndian(bytes, le);
        b.Append(BinaryPrimitives.ReadInt128LittleEndian(le));
    }
}

internal sealed class Decimal256Decoder(NativeFixedWidthBuilder<Int256Bits> b, int fixedSize) : AvroDecoder
{
    public override void Decode(ref AvroSpanReader r)
    {
        var bytes = fixedSize >= 0 ? r.ReadFixed(fixedSize) : r.ReadBytes();
        Int256Bits value = default;
        DecimalBytes.SignExtendLittleEndian(bytes, System.Runtime.InteropServices.MemoryMarshal.AsBytes(new Span<Int256Bits>(ref value)));
        b.Append(value);
    }
}

internal static class DecimalBytes
{
    /// <summary>Converts big-endian two's complement to sign-extended little endian of <c>dest.Length</c> bytes.</summary>
    public static void SignExtendLittleEndian(ReadOnlySpan<byte> bigEndian, Span<byte> dest)
    {
        byte sign = bigEndian.Length > 0 && (bigEndian[0] & 0x80) != 0 ? (byte)0xFF : (byte)0;
        if (bigEndian.Length > dest.Length)
        {
            // Wider than the target: the extra leading bytes must be pure sign extension.
            int extra = bigEndian.Length - dest.Length;
            if (bigEndian[..extra].ContainsAnyExcept(sign) || ((bigEndian[extra] ^ sign) & 0x80) != 0)
                throw new AvroFormatException($"An Avro decimal of {bigEndian.Length} bytes does not fit its declared precision.");
            bigEndian = bigEndian[extra..];
        }
        dest.Fill(sign);
        for (int i = 0; i < bigEndian.Length; i++) dest[i] = bigEndian[bigEndian.Length - 1 - i];
    }
}

internal sealed class RecordDecoder(NativeStructBuilder b, AvroDecoder[] fields) : AvroDecoder
{
    public override void Decode(ref AvroSpanReader r)
    {
        foreach (var f in fields) f.Decode(ref r);
        b.EndStruct();
    }
}

internal sealed class ArrayDecoder(NativeListBuilder b, AvroDecoder items, int minItemBytes) : AvroDecoder
{
    public override void Decode(ref AvroSpanReader r)
    {
        long count;
        while ((count = r.ReadBlockCount()) != 0)
        {
            CheckBlockCount(count, minItemBytes, ref r);
            for (long i = 0; i < count; i++) items.Decode(ref r);
        }
        b.EndList();
    }
}

internal sealed class LongArrayDecoder(NativeListBuilder list, NativeFixedWidthBuilder<long> items) : AvroDecoder
{
    public override void Decode(ref AvroSpanReader r)
    {
        long count;
        while ((count = r.ReadBlockCount()) != 0)
        {
            CheckBlockCount(count, 1, ref r);
            for (long i = 0; i < count; i++) items.Append(r.ReadLong());
        }
        list.EndList();
    }
}

internal sealed class IntArrayDecoder(NativeListBuilder list, NativeFixedWidthBuilder<int> items) : AvroDecoder
{
    public override void Decode(ref AvroSpanReader r)
    {
        long count;
        while ((count = r.ReadBlockCount()) != 0)
        {
            CheckBlockCount(count, 1, ref r);
            for (long i = 0; i < count; i++) items.Append(r.ReadInt());
        }
        list.EndList();
    }
}

/// <summary>Array of float or double: each block's items are copied in one go.</summary>
internal sealed unsafe class RawArrayDecoder<T>(NativeListBuilder list, NativeFixedWidthBuilder<T> items) : AvroDecoder where T : unmanaged
{
    public override void Decode(ref AvroSpanReader r)
    {
        long count;
        while ((count = r.ReadBlockCount()) != 0)
        {
            CheckBlockCount(count, sizeof(T), ref r);
            items.AppendRaw(r.ReadFixed(checked((int)count * sizeof(T))));
        }
        list.EndList();
    }
}

internal sealed class MapDecoder(NativeListBuilder list, NativeStructBuilder entries, NativeBinaryBuilder keys, AvroDecoder values)
    : AvroDecoder
{
    public override void Decode(ref AvroSpanReader r)
    {
        long count;
        while ((count = r.ReadBlockCount()) != 0)
        {
            CheckBlockCount(count, 1, ref r);
            for (long i = 0; i < count; i++)
            {
                keys.Append(r.ReadUtf8());
                values.Decode(ref r);
                entries.EndStruct();
            }
        }
        list.EndList();
    }
}

/// <summary>A union read as one column: a null branch appends null, other branches decode into the builder.</summary>
internal sealed class UnionDecoder(NativeColumnBuilder b, AvroDecoder?[] branches) : AvroDecoder
{
    public override void Decode(ref AvroSpanReader r)
    {
        var branch = branches[ReadBranch(ref r, branches.Length)];
        if (branch is null) b.AppendNull();
        else branch.Decode(ref r);
    }
}

/// <summary>A general union read as a struct of nullable members; the struct is null for the null branch.</summary>
internal sealed class MemberStructUnionDecoder(NativeStructBuilder b, AvroDecoder?[] branches, int[] memberOf) : AvroDecoder
{
    public override void Decode(ref AvroSpanReader r)
    {
        int index = ReadBranch(ref r, branches.Length);
        int member = memberOf[index];
        if (member < 0)
        {
            b.AppendNull();
            return;
        }
        var children = b.Children;
        for (int m = 0; m < children.Length; m++)
        {
            if (m == member) branches[index]!.Decode(ref r);
            else children[m].AppendNull();
        }
        b.EndStruct();
    }
}

/// <summary>The top-level row decoder: decodes projected fields, skips the others.</summary>
internal sealed class RowDecoder(NativeStructBuilder root, AvroDecoder?[] decoders, AvroSkipper?[] skippers) : AvroDecoder
{
    public override void Decode(ref AvroSpanReader r)
    {
        for (int i = 0; i < decoders.Length; i++)
        {
            var d = decoders[i];
            if (d is not null) d.Decode(ref r);
            else skippers[i]!.Skip(ref r);
        }
        root.EndStruct();
    }
}

/// <summary>Skips one Avro value without decoding it (for projected-away columns).</summary>
internal abstract class AvroSkipper
{
    public abstract void Skip(ref AvroSpanReader r);

    public static AvroSkipper Compile(AvroSchema schema) => Compile(schema, []);

    private static AvroSkipper Compile(AvroSchema schema, Dictionary<AvroRecordSchema, RecordSkipper> records) => schema switch
    {
        AvroRecordSchema rec => CompileRecord(rec, records),
        AvroUnionSchema u => new UnionSkipper([.. u.Branches.Select(b => Compile(b, records))]),
        AvroArraySchema a => new BlockSkipper(Compile(a.Items, records), null, AvroDecoder.MinEncodedBytes(a.Items, [])),
        AvroMapSchema m => new BlockSkipper(Compile(m.Values, records), BytesSkipper.Instance, 1),
        AvroFixedSchema f => new FixedSkipper(f.Size),
        AvroEnumSchema => VarIntSkipper.Instance,
        _ => schema.Type switch
        {
            AvroType.Null => new FixedSkipper(0),
            AvroType.Boolean => new FixedSkipper(1),
            AvroType.Int or AvroType.Long => VarIntSkipper.Instance,
            AvroType.Float => new FixedSkipper(4),
            AvroType.Double => new FixedSkipper(8),
            _ => BytesSkipper.Instance,
        },
    };

    private static RecordSkipper CompileRecord(AvroRecordSchema rec, Dictionary<AvroRecordSchema, RecordSkipper> records)
    {
        if (records.TryGetValue(rec, out var existing)) return existing; // recursive reference
        var skipper = new RecordSkipper();
        records[rec] = skipper;
        skipper.Fields = [.. rec.Fields.Select(f => Compile(f.Schema, records))];
        return skipper;
    }

    private sealed class FixedSkipper(int size) : AvroSkipper
    {
        public override void Skip(ref AvroSpanReader r) => r.Skip(size);
    }

    private sealed class VarIntSkipper : AvroSkipper
    {
        public static readonly VarIntSkipper Instance = new();
        public override void Skip(ref AvroSpanReader r) => r.ReadLong();
    }

    private sealed class BytesSkipper : AvroSkipper
    {
        public static readonly BytesSkipper Instance = new();
        public override void Skip(ref AvroSpanReader r) => r.SkipBytes();
    }

    private sealed class RecordSkipper : AvroSkipper
    {
        public AvroSkipper[] Fields { get; set; } = [];

        public override void Skip(ref AvroSpanReader r)
        {
            foreach (var f in Fields) f.Skip(ref r);
        }
    }

    private sealed class UnionSkipper(AvroSkipper[] branches) : AvroSkipper
    {
        public override void Skip(ref AvroSpanReader r)
        {
            long index = r.ReadLong();
            if ((ulong)index >= (ulong)branches.Length) throw new AvroFormatException($"Avro union branch index {index} is out of range.");
            branches[index].Skip(ref r);
        }
    }

    private sealed class BlockSkipper(AvroSkipper items, AvroSkipper? keys, int minItemBytes) : AvroSkipper
    {
        public override void Skip(ref AvroSpanReader r)
        {
            long count;
            while (true)
            {
                count = r.ReadBlockCountForSkip(out bool skipped);
                if (skipped) continue;
                if (count == 0) return;
                if (minItemBytes > 0 ? count > r.Remaining / minItemBytes : count > int.MaxValue)
                    throw new AvroFormatException($"Avro block count {count} exceeds the remaining data.");
                for (long i = 0; i < count; i++)
                {
                    keys?.Skip(ref r);
                    items.Skip(ref r);
                }
            }
        }
    }
}
