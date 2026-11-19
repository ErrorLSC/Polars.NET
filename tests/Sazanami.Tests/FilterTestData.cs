using System.Buffers.Binary;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Builders;
using Kaminari.Sazanami.Expressions;
using Kaminari.Sazanami.Filters;

namespace Sazanami.Tests;

/// <summary>
/// A column kind for filter tests: its Arrow type, a pool of values (model values: <see cref="Int128"/> for exact
/// kinds and booleans, <see cref="double"/> for floating kinds, <c>byte[]</c> for bytes) and how to lay them out.
/// </summary>
internal sealed record ColumnKind(string Name, IArrowType Type, object[] Pool, int Width = 0)
{
    public bool IsFloating => Type is HalfFloatType or FloatType or DoubleType;
    public bool IsBytes => Type is StringType or BinaryType or StringViewType or BinaryViewType or FixedSizeBinaryType and not Decimal128Type;
    public bool IsBoolean => Type is BooleanType;

    /// <summary>Whether the kind is tested as dictionary values too (booleans and fixed-size binaries are not).</summary>
    public bool Dictionaries => Type is not BooleanType && (Type is not FixedSizeBinaryType || Type is Decimal128Type);

    public StoredScalar Scalar(object value) => value switch
    {
        Int128 i => StoredScalar.Integer(i),
        double d => StoredScalar.Float(d),
        byte[] b => StoredScalar.Bytes(b),
        _ => throw new ArgumentException($"{value}"),
    };

    public override string ToString() => Name;
}

internal static class FilterTestData
{
    private static object[] Ints(params long[] values) => [.. values.Select(v => (object)(Int128)v)];

    private static readonly object[] Floats =
        [double.NaN, double.NegativeInfinity, -2.5, -1.0, -0.0, 0.0, 1.0, 2.5, 3.0, double.PositiveInfinity];

    private static readonly object[] Strings =
    [
        ""u8.ToArray(), "a"u8.ToArray(), "ab"u8.ToArray(), "abc"u8.ToArray(), "abcd"u8.ToArray(), "abcde"u8.ToArray(), "Banana"u8.ToArray(), "apple"u8.ToArray(), "äpfel"u8.ToArray(), "abcdefghijkl"u8.ToArray(),
        "abcdefghijklm"u8.ToArray(), "abcdefghijklX"u8.ToArray(), "abcdXXXXXXXXXXXXXXXXXXXX"u8.ToArray(), "abcdYYYYYYYYYYYYYYYYYYYY"u8.ToArray(), "abc\0"u8.ToArray(),
        (byte[])[0xFF, 0x00, 0x01], (byte[])[0x7F], "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"u8.ToArray(),
    ];

    public static readonly ColumnKind[] Kinds =
    [
        new("i8", Int8Type.Default, Ints(sbyte.MinValue, -3, -1, 0, 1, 2, 5, sbyte.MaxValue), 1),
        new("u8", UInt8Type.Default, Ints(0, 1, 2, 5, 200, byte.MaxValue), 1),
        new("i16", Int16Type.Default, Ints(short.MinValue, -3, 0, 1, 2, 300, short.MaxValue), 2),
        new("u16", UInt16Type.Default, Ints(0, 1, 2, 40000, ushort.MaxValue), 2),
        new("i32", Int32Type.Default, Ints(int.MinValue, -3, -1, 0, 1, 2, 70000, int.MaxValue), 4),
        new("u32", UInt32Type.Default, Ints(0, 1, 2, 3_000_000_000, uint.MaxValue), 4),
        new("i64", Int64Type.Default, Ints(long.MinValue, -3, -1, 0, 1, 2, 1L << 40, long.MaxValue), 8),
        new("u64", UInt64Type.Default, [(Int128)0, (Int128)1, (Int128)2, (Int128)(1UL << 63), (Int128)ulong.MaxValue], 8),
        new("date32", Date32Type.Default, Ints(-1, 0, 1, 19000, 19001), 4),
        new("ts", new TimestampType(TimeUnit.Microsecond, (string?)null), Ints(long.MinValue, -1, 0, 1, 1_700_000_000_000_000, long.MaxValue), 8),
        new("tsutc", new TimestampType(TimeUnit.Nanosecond, "UTC"), Ints(long.MinValue, -1, 0, 1, 1_700_000_000_000_000_000, long.MaxValue), 8),
        new("date64", Date64Type.Default, Ints(-86_400_000, 0, 86_400_000, 1_700_000_000_000), 8),
        new("time32", new Time32Type(TimeUnit.Millisecond), Ints(0, 1, 1000, 86_399_999), 4),
        new("time64", new Time64Type(TimeUnit.Nanosecond), Ints(0, 1, 1_000_000_000, 86_399_999_999_999), 8),
        new("duration", DurationType.Microsecond, Ints(long.MinValue, -5, 0, 5, long.MaxValue), 8),
        new("dec", new Decimal128Type(38, 2), [(Int128)(-1), (Int128)0, (Int128)1, Int128.Parse("99999999999999999999999999999999999999"),
            -Int128.Parse("99999999999999999999999999999999999999"), (Int128)12345], 16),
        new("f16", HalfFloatType.Default, Floats, 2),
        new("f32", FloatType.Default, Floats, 4),
        new("f64", DoubleType.Default, Floats, 8),
        new("bool", BooleanType.Default, Ints(0, 1)),
        new("utf8", StringType.Default, Strings),
        new("binary", BinaryType.Default, Strings),
        new("view", StringViewType.Default, Strings),
        new("bview", BinaryViewType.Default, Strings),
        new("fsb", new FixedSizeBinaryType(4), ["abcd"u8.ToArray(), "abce"u8.ToArray(), "Abcd"u8.ToArray(), (byte[])[0, 0, 0, 0], (byte[])[0xFF, 0, 0, 1]], 4),
    ];

    /// <summary>Random values of a kind (null with the given chance).</summary>
    public static object?[] Values(Random random, ColumnKind kind, int length, double nulls) =>
        [.. Enumerable.Range(0, length).Select(_ => random.NextDouble() < nulls ? null : kind.Pool[random.Next(kind.Pool.Length)])];

    /// <summary>
    /// An array of the values in managed memory (as other libraries hand them over): null slots hold garbage, views of
    /// long values spread over two data buffers.
    /// </summary>
    public static IArrowArray Build(Random random, ColumnKind kind, object?[] values)
    {
        int n = values.Length, nullCount = values.Count(v => v is null);
        var validity = new byte[(n + 7) / 8 + 8];
        for (int i = 0; i < n; i++)
        {
            if (values[i] is not null) validity[i >> 3] |= (byte)(1 << (i & 7));
        }
        ArrowBuffer valid = nullCount == 0 && random.Next(2) == 0 ? ArrowBuffer.Empty : new ArrowBuffer(validity);
        switch (kind.Type)
        {
            case BooleanType:
            {
                var bits = new byte[(n + 7) / 8 + 8];
                random.NextBytes(bits); // null slots: garbage
                for (int i = 0; i < n; i++)
                {
                    if (values[i] is not Int128 b) continue;
                    if (b == 1) bits[i >> 3] |= (byte)(1 << (i & 7));
                    else bits[i >> 3] &= (byte)~(1 << (i & 7));
                }
                return ArrowArrayFactory.BuildArray(new ArrayData(kind.Type, n, nullCount, 0, [valid, new ArrowBuffer(bits)]));
            }
            case StringType or BinaryType:
            {
                var offsets = new int[n + 1];
                var data = new List<byte>();
                for (int i = 0; i < n; i++)
                {
                    if (values[i] is byte[] b) data.AddRange(b);
                    offsets[i + 1] = data.Count;
                }
                return ArrowArrayFactory.BuildArray(new ArrayData(kind.Type, n, nullCount, 0,
                    [valid, new ArrowBuffer(System.Runtime.InteropServices.MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray()), new ArrowBuffer(data.ToArray())]));
            }
            case StringViewType or BinaryViewType:
            {
                var views = new byte[n * 16];
                List<byte>[] buffers = [[], []];
                for (int i = 0; i < n; i++)
                {
                    Span<byte> view = views.AsSpan(i * 16, 16);
                    if (values[i] is not byte[] b)
                    {
                        random.NextBytes(view); // garbage, maybe pointing nowhere
                        continue;
                    }
                    BinaryPrimitives.WriteInt32LittleEndian(view, b.Length);
                    if (b.Length <= 12) b.CopyTo(view[4..]);
                    else
                    {
                        int buffer = random.Next(2);
                        b.AsSpan(0, 4).CopyTo(view[4..]);
                        BinaryPrimitives.WriteInt32LittleEndian(view[8..], buffer);
                        BinaryPrimitives.WriteInt32LittleEndian(view[12..], buffers[buffer].Count);
                        buffers[buffer].AddRange(b);
                    }
                }
                return ArrowArrayFactory.BuildArray(new ArrayData(kind.Type, n, nullCount, 0,
                    [valid, new ArrowBuffer(views), new ArrowBuffer(buffers[0].ToArray()), new ArrowBuffer(buffers[1].ToArray())]));
            }
            case FixedSizeBinaryType f when f is not Decimal128Type:
            {
                var bytes = new byte[n * f.ByteWidth];
                random.NextBytes(bytes);
                for (int i = 0; i < n; i++)
                {
                    if (values[i] is byte[] b) b.CopyTo(bytes, i * f.ByteWidth);
                }
                return ArrowArrayFactory.BuildArray(new ArrayData(kind.Type, n, nullCount, 0, [valid, new ArrowBuffer(bytes)]));
            }
            default:
            {
                var bytes = new byte[n * kind.Width];
                random.NextBytes(bytes);
                for (int i = 0; i < n; i++)
                {
                    if (values[i] is { } v) Encode(kind, v, bytes.AsSpan(i * kind.Width, kind.Width));
                }
                return ArrowArrayFactory.BuildArray(new ArrayData(kind.Type, n, nullCount, 0, [valid, new ArrowBuffer(bytes)]));
            }
        }
    }

    /// <summary>The values as a dictionary array: distinct entries in random order, a null entry, indices of random width.</summary>
    public static IArrowArray BuildDictionary(Random random, ColumnKind kind, object?[] values)
    {
        List<object?> entries = [null];
        foreach (object v in kind.Pool.OrderBy(_ => random.Next())) entries.Add(v);
        int Entry(object? v) => v is null ? (random.Next(2) == 0 ? 0 : -1) : entries.FindIndex(e => e is not null && Compare(e, v) == 0 && Same(e, v));
        int[] indices = [.. values.Select(Entry)];
        IArrowArray dictionary = Build(random, kind, [.. entries]);
        var validity = new byte[(values.Length + 7) / 8 + 1];
        for (int i = 0; i < indices.Length; i++)
        {
            if (indices[i] >= 0) validity[i >> 3] |= (byte)(1 << (i & 7));
        }
        int nulls = indices.Count(i => i < 0);
        IArrowType indexType = random.Next(2) == 0 ? Int32Type.Default : Int8Type.Default;
        byte[] raw = indexType is Int8Type
            ? [.. indices.Select(i => (byte)(sbyte)(i < 0 ? -7 : i))] // a null's index is garbage
            : [.. indices.SelectMany(i => BitConverter.GetBytes(i < 0 ? 12345 : i))];
        var data = new ArrayData(new DictionaryType(indexType, kind.Type, false), values.Length, nulls, 0,
            [nulls == 0 ? ArrowBuffer.Empty : new ArrowBuffer(validity), new ArrowBuffer(raw)], [], dictionary.Data);
        return ArrowArrayFactory.BuildArray(data);

        // -0 and 0 compare equal but are different entries: keep each value its own.
        static bool Same(object a, object b) => a is not double x || b is not double y || BitConverter.DoubleToInt64Bits(x) == BitConverter.DoubleToInt64Bits(y)
            || double.IsNaN(x) && double.IsNaN(y);
    }

    /// <summary>The values as an array in native memory of the reader's builders.</summary>
    public static IArrowArray BuildNative(ColumnKind kind, object?[] values)
    {
        using NativeColumnBuilder builder = NativeColumnBuilder.Create(kind.Type);
        Span<byte> scratch = stackalloc byte[16];
        foreach (object? value in values)
        {
            if (value is null)
            {
                builder.AppendNull();
                continue;
            }
            switch (builder)
            {
                case NativeBooleanBuilder b: b.Append((Int128)value == 1); break;
                case NativeBinaryBuilder b: b.Append((byte[])value); break;
                case NativeBinaryViewBuilder b: b.Append((byte[])value); break;
                case NativeFixedSizeBinaryBuilder b: b.Append((byte[])value); break;
                default:
                    Encode(kind, value, scratch[..kind.Width]);
                    AppendRaw(builder, scratch[..kind.Width]);
                    break;
            }
        }
        return builder.ExportArray();

        static void AppendRaw(NativeColumnBuilder builder, ReadOnlySpan<byte> bytes)
        {
            switch (builder)
            {
                case NativeFixedWidthBuilder<sbyte> b: b.AppendRaw(bytes); break;
                case NativeFixedWidthBuilder<byte> b: b.AppendRaw(bytes); break;
                case NativeFixedWidthBuilder<short> b: b.AppendRaw(bytes); break;
                case NativeFixedWidthBuilder<ushort> b: b.AppendRaw(bytes); break;
                case NativeFixedWidthBuilder<int> b: b.AppendRaw(bytes); break;
                case NativeFixedWidthBuilder<uint> b: b.AppendRaw(bytes); break;
                case NativeFixedWidthBuilder<long> b: b.AppendRaw(bytes); break;
                case NativeFixedWidthBuilder<ulong> b: b.AppendRaw(bytes); break;
                case NativeFixedWidthBuilder<Half> b: b.AppendRaw(bytes); break;
                case NativeFixedWidthBuilder<float> b: b.AppendRaw(bytes); break;
                case NativeFixedWidthBuilder<double> b: b.AppendRaw(bytes); break;
                case NativeFixedWidthBuilder<Int128> b: b.AppendRaw(bytes); break;
                default: throw new NotSupportedException(builder.GetType().Name);
            }
        }
    }

    private static void Encode(ColumnKind kind, object value, Span<byte> target)
    {
        switch (kind.Type)
        {
            case HalfFloatType: BinaryPrimitives.WriteHalfLittleEndian(target, (Half)(double)value); break;
            case FloatType: BinaryPrimitives.WriteSingleLittleEndian(target, (float)(double)value); break;
            case DoubleType: BinaryPrimitives.WriteDoubleLittleEndian(target, (double)value); break;
            default:
            {
                Int128 v = (Int128)value;
                Span<byte> all = stackalloc byte[16];
                BinaryPrimitives.WriteInt128LittleEndian(all, v);
                all[..target.Length].CopyTo(target);
                break;
            }
        }
    }

    /// <summary>The model value of row <paramref name="i"/> (dictionaries by their entry), read without the kernels.</summary>
    public static object? Read(ColumnKind kind, IArrowArray array, int i)
    {
        if (array is DictionaryArray dictionary)
        {
            if (dictionary.IsNull(i)) return null;
            long index = dictionary.Indices switch
            {
                Int8Array a => a.GetValue(i)!.Value,
                Int32Array a => a.GetValue(i)!.Value,
                UInt16Array a => a.GetValue(i)!.Value,
                Int64Array a => a.GetValue(i)!.Value,
                _ => throw new NotSupportedException(),
            };
            return Read(kind, dictionary.Dictionary, (int)index);
        }
        if (array.IsNull(i)) return null;
        ArrayData data = array.Data;
        int at = data.Offset + i;
        return array switch
        {
            BooleanArray b => (Int128)(b.GetValue(i)!.Value ? 1 : 0),
            BinaryViewArray v => v.GetBytes(i).ToArray(),
            BinaryArray b => b.GetBytes(i).ToArray(),
            _ when kind.Type is FixedSizeBinaryType f and not Decimal128Type => data.Buffers[1].Span.Slice(at * f.ByteWidth, f.ByteWidth).ToArray(),
            _ => Decode(kind, data.Buffers[1].Span.Slice(at * kind.Width, kind.Width)),
        };
    }

    private static object Decode(ColumnKind kind, ReadOnlySpan<byte> bytes) => kind.Type switch
    {
        HalfFloatType => (double)BinaryPrimitives.ReadHalfLittleEndian(bytes),
        FloatType => (double)BinaryPrimitives.ReadSingleLittleEndian(bytes),
        DoubleType => BinaryPrimitives.ReadDoubleLittleEndian(bytes),
        Int8Type => (Int128)(sbyte)bytes[0],
        UInt8Type => (Int128)bytes[0],
        Int16Type => (Int128)BinaryPrimitives.ReadInt16LittleEndian(bytes),
        UInt16Type => (Int128)BinaryPrimitives.ReadUInt16LittleEndian(bytes),
        Int32Type or Date32Type or Time32Type => (Int128)BinaryPrimitives.ReadInt32LittleEndian(bytes),
        UInt32Type => (Int128)BinaryPrimitives.ReadUInt32LittleEndian(bytes),
        Int64Type or TimestampType or Date64Type or Time64Type or DurationType => (Int128)BinaryPrimitives.ReadInt64LittleEndian(bytes),
        UInt64Type => (Int128)BinaryPrimitives.ReadUInt64LittleEndian(bytes),
        Decimal128Type => BinaryPrimitives.ReadInt128LittleEndian(bytes),
        _ => throw new NotSupportedException(kind.Name),
    };

    /// <summary>The IR's order of two model values of a kind (NaN equal to NaN and above every number, -0 equal to 0).</summary>
    public static int Compare(object a, object b) => (a, b) switch
    {
        (Int128 x, Int128 y) => x.CompareTo(y),
        (double x, double y) => double.IsNaN(x) ? (double.IsNaN(y) ? 0 : 1) : double.IsNaN(y) ? -1 : x < y ? -1 : x > y ? 1 : 0,
        (byte[] x, byte[] y) => Math.Sign(x.AsSpan().SequenceCompareTo(y)),
        _ => throw new ArgumentException($"{a} vs {b}"),
    };
}

/// <summary>
/// Value expressions (<see cref="ValueNode"/>) on model values, independently of the kernel: integers as
/// <see cref="Int128"/> wrapped into their type, floating values as doubles (Float32 computed in Float32), the IR's
/// arithmetic (division and remainder by zero null, MinValue / -1 wrapping, remainder truncated).
/// </summary>
internal static class ValueModel
{
    public static bool IsArithmetic(IArrowType type) =>
        type is Int8Type or UInt8Type or Int16Type or UInt16Type or Int32Type or UInt32Type or Int64Type or UInt64Type or FloatType or DoubleType;

    private static bool IsSigned(IArrowType type) => type is Int8Type or Int16Type or Int32Type or Int64Type or FloatType or DoubleType;

    /// <summary>A model value of a node (null for null).</summary>
    public static object? Value(ValueNode node, Func<int, object?> column, IReadOnlyList<IArrowType> types, IArrowType type) => node switch
    {
        ValueNode.Column c => column(c.Index),
        ValueNode.Scalar s => s.Value.Kind == StoredScalarKind.Integer ? s.Value.IntegerValue : s.Value.FloatValue,
        ValueNode.Negate n => Value(n.Operand, column, types, type) switch
        {
            null => null,
            Int128 x => Wrap(-x, type),
            double d => type is FloatType ? (double)-(float)d : -d,
            var v => throw new ArgumentException($"-{v}"),
        },
        ValueNode.Cast c => Cast(Value(c.Operand, column, types, ValueCompiler.TypeOf(c.Operand, types)!), ValueCompiler.TypeOf(c.Operand, types)!, c.Type),
        ValueNode.Arithmetic a => Arithmetic(a.Op, Value(a.Left, column, types, type), Value(a.Right, column, types, type), type),
        _ => throw new ArgumentException($"{node}"),
    };

    public static Int128 Wrap(Int128 value, IArrowType type)
    {
        ulong low = (ulong)(value & ulong.MaxValue);
        return type switch
        {
            Int8Type => (sbyte)low,
            UInt8Type => (byte)low,
            Int16Type => (short)low,
            UInt16Type => (ushort)low,
            Int32Type => (int)low,
            UInt32Type => (uint)low,
            Int64Type => (long)low,
            _ => low,
        };
    }

    private static object? Arithmetic(ArithmeticOp op, object? l, object? r, IArrowType type)
    {
        if (l is null || r is null) return null;
        if (l is Int128 x && r is Int128 y)
        {
            bool signed = IsSigned(type);
            return op switch
            {
                ArithmeticOp.Add => Wrap(x + y, type),
                ArithmeticOp.Subtract => Wrap(x - y, type),
                ArithmeticOp.Multiply => Wrap(x * y, type),
                _ when y == 0 => null,
                ArithmeticOp.Divide => signed && y == -1 ? Wrap(-x, type) : Wrap(x / y, type),
                _ => signed && y == -1 ? Int128.Zero : x % y,
            };
        }
        double a = (double)l, b = (double)r;
        if (type is FloatType)
        {
            float fa = (float)a, fb = (float)b;
            return (double)(op switch
            {
                ArithmeticOp.Add => fa + fb,
                ArithmeticOp.Subtract => fa - fb,
                ArithmeticOp.Multiply => fa * fb,
                ArithmeticOp.Divide => fa / fb,
                _ => fa % fb,
            });
        }
        return op switch
        {
            ArithmeticOp.Add => a + b,
            ArithmeticOp.Subtract => a - b,
            ArithmeticOp.Multiply => a * b,
            ArithmeticOp.Divide => a / b,
            _ => a % b,
        };
    }

    private static object? Cast(object? value, IArrowType from, IArrowType to) => value switch
    {
        null => null,
        Int128 x when to is DoubleType => from is UInt64Type ? (double)(ulong)x : (double)(long)x,
        Int128 x when to is FloatType => (double)(from is UInt64Type ? (float)(ulong)x : (float)(long)x),
        Int128 x => Wrap(x, to),
        double d => to is FloatType ? (double)(float)d : d,
        _ => throw new ArgumentException($"cast {value}"),
    };

    /// <summary>A value of the pool of a kind of <paramref name="type"/>, as a scalar.</summary>
    public static StoredScalar Scalar(Random random, IArrowType type)
    {
        ColumnKind kind = FilterTestData.Kinds.First(k => k.Type.TypeId == type.TypeId);
        object v = random.Next(6) switch
        {
            0 when kind.IsFloating => 0.0,
            0 => (Int128)0,
            1 when kind.IsFloating => -1.0,
            1 when IsSigned(type) => (Int128)(-1),
            _ => kind.Pool[random.Next(kind.Pool.Length)],
        };
        return kind.Scalar(v);
    }

    /// <summary>
    /// A random arithmetic expression starting from column <paramref name="c"/> (an arithmetic kind): operations with
    /// scalars or columns of its type, negations, casts; its type out.
    /// </summary>
    public static ValueNode RandomExpression(Random random, IReadOnlyList<IArrowType> types, int c, out IArrowType type)
    {
        type = ValueTypes.ValueType(types[c]);
        ValueNode node = new ValueNode.Column(c);
        ArithmeticOp[] ops = [ArithmeticOp.Add, ArithmeticOp.Subtract, ArithmeticOp.Multiply, ArithmeticOp.Divide, ArithmeticOp.Remainder];
        for (int step = random.Next(1, 4); step > 0; step--)
        {
            IArrowType current = type;
            int[] partners = [.. Enumerable.Range(0, types.Count).Where(i => ValueTypes.ValueType(types[i]).TypeId == current.TypeId)];
            switch (random.Next(6))
            {
                case 0 or 1:
                {
                    ValueNode scalar = new ValueNode.Scalar(Scalar(random, type));
                    ArithmeticOp op = ops[random.Next(ops.Length)];
                    node = random.Next(3) == 0 ? new ValueNode.Arithmetic(op, scalar, node) : new ValueNode.Arithmetic(op, node, scalar);
                    break;
                }
                case 2 when partners.Length > 0:
                    node = new ValueNode.Arithmetic(ops[random.Next(ops.Length)], node, new ValueNode.Column(partners[random.Next(partners.Length)]));
                    break;
                case 3 when IsSigned(type):
                    node = new ValueNode.Negate(node);
                    break;
                case 4:
                {
                    IArrowType[] targets = type is FloatType or DoubleType
                        ? [FloatType.Default, DoubleType.Default]
                        : [Int8Type.Default, UInt8Type.Default, Int16Type.Default, UInt16Type.Default, Int32Type.Default, UInt32Type.Default, Int64Type.Default, UInt64Type.Default, FloatType.Default, DoubleType.Default];
                    IArrowType target = targets[random.Next(targets.Length)];
                    node = new ValueNode.Cast(node, target);
                    type = target;
                    break;
                }
            }
        }
        return node;
    }
}
