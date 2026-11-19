using System.Numerics;
using System.Numerics.Tensors;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Filters;

namespace Kaminari.Sazanami.Expressions;

/// <summary>
/// The native scratch memory of one evaluation (computed values, validity masks): blocks of
/// <see cref="NativeMemoryPool"/>, freed together on <see cref="Dispose"/> unless taken out (<see cref="Detach"/>) to
/// become the buffers of a result.
/// </summary>
internal sealed unsafe class ValueScratch : IDisposable
{
    private readonly List<nint> _blocks = [];

    /// <summary>A block of <paramref name="count"/> values, contents undefined.</summary>
    public Span<T> Rent<T>(int count) where T : unmanaged
    {
        byte* block = NativeMemoryPool.Alloc(Math.Max(1L, (long)count * sizeof(T)));
        _blocks.Add((nint)block);
        return new Span<T>(block, count);
    }

    /// <summary>A mask of <paramref name="length"/> rows, contents undefined.</summary>
    public Span<ulong> RentMask(int length) => Rent<ulong>(Masks.Words(length));

    /// <summary>Takes the block starting at <paramref name="first"/> out of the scratch (its memory is then the caller's to free); false when no block starts there.</summary>
    public bool Detach(void* first)
    {
        int at = _blocks.IndexOf((nint)first);
        if (at < 0) return false;
        _blocks.RemoveAt(at);
        return true;
    }

    public void Dispose()
    {
        foreach (nint block in _blocks) NativeMemoryPool.Free((void*)block);
        _blocks.Clear();
    }
}

/// <summary>The batch an evaluation reads: its columns (arrays of <see cref="Length"/> rows), and the scratch it computes into.</summary>
internal readonly ref struct ValueContext
{
    private readonly ReadOnlySpan<IArrowArray> _columns;
    private readonly ArrayData? _single;

    public ValueContext(ReadOnlySpan<IArrowArray> columns, int length, ValueScratch scratch)
    {
        _columns = columns;
        Length = length;
        Scratch = scratch;
    }

    /// <summary>A context whose every column is <paramref name="single"/>: an expression of one column, evaluated on its values (a dictionary's entries among them).</summary>
    public ValueContext(ArrayData single, int length, ValueScratch scratch)
    {
        _single = single;
        Length = length;
        Scratch = scratch;
    }

    public int Length { get; }

    public ValueScratch Scratch { get; }

    public ArrayData Data(int column) => _single ?? _columns[column].Data;
}

/// <summary>A compiled value expression (see <see cref="ValueNode"/>): its type, and the columns it reads.</summary>
internal abstract class ValueStep(IArrowType type)
{
    /// <summary>The type of the values (never a dictionary).</summary>
    public IArrowType Type { get; } = type;

    public abstract void CollectColumns(ISet<int> columns);

    /// <summary>The values of the context's rows as the data of an Arrow array of <see cref="Type"/>, its buffers native memory of its own.</summary>
    public abstract ArrayData EvaluateArray(in ValueContext context);
}

/// <summary>A compiled value expression whose values are stored as <typeparamref name="T"/>.</summary>
internal abstract class ValueStep<T>(IArrowType type) : ValueStep(type) where T : unmanaged, INumber<T>
{
    /// <summary>
    /// Computes the values of the context's rows and returns them (a column's own values, or scratch). Either sets
    /// <paramref name="allValid"/> (every row valid; <paramref name="valid"/> is left as it was), or writes the valid
    /// rows into <paramref name="valid"/> (the words of <see cref="ValueContext.Length"/> rows, the bits past the last
    /// row clear). A null row holds some value.
    /// </summary>
    public abstract ReadOnlySpan<T> Evaluate(in ValueContext context, Span<ulong> valid, out bool allValid);

    public sealed override ArrayData EvaluateArray(in ValueContext context)
    {
        int length = context.Length;
        Span<ulong> valid = context.Scratch.RentMask(length);
        ReadOnlySpan<T> values = Evaluate(context, valid, out bool allValid);
        ArrowBuffer data = ValueProgram.Own(context.Scratch, MemoryMarshal.AsBytes(values));
        if (allValid) return new ArrayData(Type, length, 0, 0, [ArrowBuffer.Empty, data]);
        int nulls = length - Masks.Count(valid);
        return new ArrayData(Type, length, nulls, 0, [ValueProgram.Own(context.Scratch, MemoryMarshal.AsBytes((ReadOnlySpan<ulong>)valid)), data]);
    }
}

/// <summary>The values of a column; a dictionary's are looked up per row.</summary>
internal sealed class ColumnStep<T>(int column, IArrowType type) : ValueStep<T>(type) where T : unmanaged, INumber<T>
{
    public int Column { get; } = column;

    public override void CollectColumns(ISet<int> columns) => columns.Add(Column);

    public override ReadOnlySpan<T> Evaluate(in ValueContext context, Span<ulong> valid, out bool allValid)
    {
        ArrayData data = context.Data(Column);
        int length = context.Length;
        if (data.DataType is NullType)
        {
            // Null on every row (an Arrow null array has no buffers at all).
            Masks.Fill(valid, length, false);
            allValid = false;
            Span<T> zeros = context.Scratch.Rent<T>(length);
            zeros.Clear();
            return zeros;
        }
        if (data.Dictionary is not { } dictionary)
        {
            allValid = !ValueTypes.HasNulls(data);
            if (!allValid) Masks.Load(data.Buffers[0].Span, data.Offset, length, valid);
            return ValueTypes.Values<T>(data, length);
        }
        Span<T> values = context.Scratch.Rent<T>(length);
        if (ValueTypes.HasNulls(data)) Masks.Load(data.Buffers[0].Span, data.Offset, length, valid);
        else Masks.Fill(valid, length, true);
        ReadOnlySpan<T> entries = ValueTypes.Values<T>(dictionary, dictionary.Length);
        Span<ulong> entryValid = default;
        if (ValueTypes.HasNulls(dictionary))
        {
            entryValid = context.Scratch.RentMask(dictionary.Length);
            Masks.Load(dictionary.Buffers[0].Span, dictionary.Offset, dictionary.Length, entryValid);
        }
        switch (((DictionaryType)data.DataType).IndexType)
        {
            case Int8Type: Gather(ValueTypes.Values<sbyte>(data, length), entries, entryValid, values, valid); break;
            case UInt8Type: Gather(ValueTypes.Values<byte>(data, length), entries, entryValid, values, valid); break;
            case Int16Type: Gather(ValueTypes.Values<short>(data, length), entries, entryValid, values, valid); break;
            case UInt16Type: Gather(ValueTypes.Values<ushort>(data, length), entries, entryValid, values, valid); break;
            case Int32Type: Gather(ValueTypes.Values<int>(data, length), entries, entryValid, values, valid); break;
            case UInt32Type: Gather(ValueTypes.Values<uint>(data, length), entries, entryValid, values, valid); break;
            case Int64Type: Gather(ValueTypes.Values<long>(data, length), entries, entryValid, values, valid); break;
            case UInt64Type: Gather(ValueTypes.Values<ulong>(data, length), entries, entryValid, values, valid); break;
            default: throw new ArgumentException($"Dictionary indices of type {((DictionaryType)data.DataType).IndexType.Name} are not supported.");
        }
        allValid = Masks.IsFull(valid, length);
        return values;

        // The entry of each valid row (a null entry makes the row null); null rows get zero.
        static void Gather<TIndex>(ReadOnlySpan<TIndex> indices, ReadOnlySpan<T> entries, ReadOnlySpan<ulong> entryValid, Span<T> values, Span<ulong> valid)
            where TIndex : unmanaged, IBinaryInteger<TIndex>
        {
            int length = indices.Length;
            for (int w = 0, words = Masks.Words(length); w < words; w++)
            {
                int first = w << 6, n = Math.Min(64, length - first);
                ulong word = valid[w];
                for (int k = 0; k < n; k++)
                {
                    if ((word >> k & 1) == 0)
                    {
                        values[first + k] = T.Zero;
                        continue;
                    }
                    ulong entry = ulong.CreateTruncating(indices[first + k]);
                    if (entry >= (ulong)entries.Length) throw new InvalidDataException($"Dictionary index {indices[first + k]} is past its {entries.Length} entries.");
                    values[first + k] = entries[(int)entry];
                    if (!entryValid.IsEmpty && (entryValid[(int)(entry >> 6)] >> (int)(entry & 63) & 1) == 0) word &= ~(1UL << k);
                }
                valid[w] = word;
            }
        }
    }
}

/// <summary>A value on every row.</summary>
internal sealed class ScalarStep<T>(T value, IArrowType type) : ValueStep<T>(type) where T : unmanaged, INumber<T>
{
    public T Value { get; } = value;

    public override void CollectColumns(ISet<int> columns)
    {
    }

    public override ReadOnlySpan<T> Evaluate(in ValueContext context, Span<ulong> valid, out bool allValid)
    {
        Span<T> values = context.Scratch.Rent<T>(context.Length);
        values.Fill(Value);
        allValid = true;
        return values;
    }
}

/// <summary>Arithmetic between two values (see <see cref="ArithmeticOp"/>); null where either is.</summary>
internal sealed class ArithmeticStep<T>(ArithmeticOp op, ValueStep<T> left, ValueStep<T> right, IArrowType type) : ValueStep<T>(type)
    where T : unmanaged, INumber<T>
{
    private static readonly T s_minusOne = T.Zero - T.One;
    private static readonly bool s_signed = T.IsNegative(s_minusOne);
    private static readonly bool s_floating = typeof(T) == typeof(double) || typeof(T) == typeof(float) || typeof(T) == typeof(Half);

    public override void CollectColumns(ISet<int> columns)
    {
        left.CollectColumns(columns);
        right.CollectColumns(columns);
    }

    public override ReadOnlySpan<T> Evaluate(in ValueContext context, Span<ulong> valid, out bool allValid)
    {
        int length = context.Length;
        Span<T> result = context.Scratch.Rent<T>(length);
        if (right is ScalarStep<T> divisor)
        {
            ReadOnlySpan<T> x = left.Evaluate(context, valid, out allValid);
            WithScalar(x, divisor.Value, result, valid, ref allValid);
            return result;
        }
        if (left is ScalarStep<T> first && op is ArithmeticOp.Add or ArithmeticOp.Multiply)
        {
            ReadOnlySpan<T> y = right.Evaluate(context, valid, out allValid);
            WithScalar(y, first.Value, result, valid, ref allValid); // commutative
            return result;
        }
        ReadOnlySpan<T> a = left.Evaluate(context, valid, out bool aValid);
        Span<ulong> other = context.Scratch.RentMask(length);
        ReadOnlySpan<T> b = right.Evaluate(context, other, out bool bValid);
        allValid = aValid && bValid;
        if (aValid && !bValid) other.CopyTo(valid);
        else if (!aValid && !bValid) Masks.And(valid[..other.Length], other);
        switch (op)
        {
            case ArithmeticOp.Add:
                TensorPrimitives.Add(a, b, result);
                break;
            case ArithmeticOp.Subtract:
                TensorPrimitives.Subtract(a, b, result);
                break;
            case ArithmeticOp.Multiply:
                TensorPrimitives.Multiply(a, b, result);
                break;
            case ArithmeticOp.Divide when s_floating:
                TensorPrimitives.Divide(a, b, result);
                break;
            case ArithmeticOp.Remainder when s_floating:
                for (int i = 0; i < length; i++) result[i] = a[i] % b[i];
                break;
            default:
                for (int i = 0; i < length; i++)
                {
                    T d = b[i];
                    if (d == T.Zero)
                    {
                        result[i] = T.Zero;
                        SetNull(valid, i, length, ref allValid);
                    }
                    else result[i] = Integer(a[i], d);
                }
                break;
        }
        return result;
    }

    /// <summary><c>x op scalar</c> (or <c>scalar op x</c> for Add and Multiply).</summary>
    private void WithScalar(ReadOnlySpan<T> x, T scalar, Span<T> result, Span<ulong> valid, ref bool allValid)
    {
        int length = x.Length;
        switch (op)
        {
            case ArithmeticOp.Add:
                TensorPrimitives.Add(x, scalar, result);
                return;
            case ArithmeticOp.Subtract:
                TensorPrimitives.Subtract(x, scalar, result);
                return;
            case ArithmeticOp.Multiply:
                TensorPrimitives.Multiply(x, scalar, result);
                return;
            case ArithmeticOp.Divide when s_floating:
                TensorPrimitives.Divide(x, scalar, result);
                return;
            case ArithmeticOp.Remainder when s_floating:
                for (int i = 0; i < length; i++) result[i] = x[i] % scalar;
                return;
        }
        if (scalar == T.Zero)
        {
            // Null on every row.
            result.Clear();
            Masks.Fill(valid, length, false);
            allValid = false;
        }
        else if (s_signed && scalar == s_minusOne)
        {
            // x / -1 = -x (MinValue wraps), x % -1 = 0.
            if (op == ArithmeticOp.Divide) TensorPrimitives.Multiply(x, s_minusOne, result);
            else result.Clear();
        }
        else if (op == ArithmeticOp.Divide)
        {
            for (int i = 0; i < length; i++) result[i] = x[i] / scalar;
        }
        else
        {
            for (int i = 0; i < length; i++) result[i] = x[i] % scalar;
        }
    }

    /// <summary>Integer <c>a / d</c> or <c>a % d</c> for a divisor that is not zero, <c>MinValue / -1</c> wrapping.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private T Integer(T a, T d)
    {
        if (s_signed && d == s_minusOne) return op == ArithmeticOp.Divide ? T.Zero - a : T.Zero;
        return op == ArithmeticOp.Divide ? a / d : a % d;
    }

    private static void SetNull(Span<ulong> valid, int row, int length, ref bool allValid)
    {
        if (allValid)
        {
            Masks.Fill(valid, length, true);
            allValid = false;
        }
        valid[row >> 6] &= ~(1UL << (row & 63));
    }
}

/// <summary><c>-x</c> of a signed integer (wrapping) or floating value (the sign flipped: -0 from 0).</summary>
internal sealed class NegateStep<T>(ValueStep<T> operand, IArrowType type) : ValueStep<T>(type) where T : unmanaged, INumber<T>
{
    public override void CollectColumns(ISet<int> columns) => operand.CollectColumns(columns);

    public override ReadOnlySpan<T> Evaluate(in ValueContext context, Span<ulong> valid, out bool allValid)
    {
        ReadOnlySpan<T> x = operand.Evaluate(context, valid, out allValid);
        Span<T> result = context.Scratch.Rent<T>(context.Length);
        // A product with -1 is exact: integers wrap (MinValue stays), floating values flip their sign (0 to -0).
        TensorPrimitives.Multiply(x, T.Zero - T.One, result);
        return result;
    }
}

/// <summary>A value converted as .NET's unchecked conversion does (<see cref="INumberBase{TSelf}.CreateTruncating{TOther}"/>).</summary>
internal sealed class CastStep<TFrom, TTo>(ValueStep<TFrom> operand, IArrowType type) : ValueStep<TTo>(type)
    where TFrom : unmanaged, INumber<TFrom>
    where TTo : unmanaged, INumber<TTo>
{
    public override void CollectColumns(ISet<int> columns) => operand.CollectColumns(columns);

    public override ReadOnlySpan<TTo> Evaluate(in ValueContext context, Span<ulong> valid, out bool allValid)
    {
        ReadOnlySpan<TFrom> x = operand.Evaluate(context, valid, out allValid);
        Span<TTo> result = context.Scratch.Rent<TTo>(context.Length);
        TensorPrimitives.ConvertTruncating(x, result);
        return result;
    }
}

/// <summary>Compiles <see cref="ValueNode"/>s over columns of given types into <see cref="ValueStep"/>s, checking their types.</summary>
internal static class ValueCompiler
{
    /// <summary>The type of a value (a column's value type, a cast's target, arithmetic's operands'), or null for a scalar, which takes the type it meets.</summary>
    /// <exception cref="ArgumentException">A column index is out of range, or arithmetic mixes types.</exception>
    public static IArrowType? TypeOf(ValueNode node, IReadOnlyList<IArrowType> columns) => node switch
    {
        ValueNode.Column c => (uint)c.Index < (uint)columns.Count
            ? ValueTypes.ValueType(columns[c.Index])
            : throw new ArgumentException($"Column {c.Index} is not one of the {columns.Count} columns."),
        ValueNode.Scalar => null,
        ValueNode.Arithmetic a => (TypeOf(a.Left, columns), TypeOf(a.Right, columns)) switch
        {
            (null, null) => throw new ArgumentException("Arithmetic between two scalars is to be computed before (folded)."),
            ({ } l, { } r) when !ValueTypes.Same(l, r) => throw new ArgumentException($"Arithmetic between {l.Name} and {r.Name} values: the operands must be of one type."),
            var (l, r) => l ?? r,
        },
        ValueNode.Negate n => TypeOf(n.Operand, columns) ?? throw new ArgumentException("A negated scalar is to be computed before (folded)."),
        ValueNode.Cast c => c.Type ?? throw new ArgumentException("A cast needs a type."),
        _ => throw new ArgumentException($"Unknown value {node}."),
    };

    /// <summary>Compiles a value of type <paramref name="type"/> (its <see cref="TypeOf"/>, or the type a scalar meets).</summary>
    /// <exception cref="ArgumentException">The value is not well typed, or applies an operation to a type that does not take it.</exception>
    public static ValueStep Compile(ValueNode node, IReadOnlyList<IArrowType> columns, IArrowType type)
    {
        ArgumentNullException.ThrowIfNull(node);
        if (TypeOf(node, columns) is { } own && !ValueTypes.Same(own, type))
            throw new ArgumentException($"A {own.Name} value where a {type.Name} value is expected.");
        if (!ValueTypes.IsNumeric(type)) throw new ArgumentException($"{type.Name} values are not computed with.");
        switch (node)
        {
            case ValueNode.Column c:
                return ValueTypes.Visit<ColumnFactory, ValueStep>(type, new(c.Index, type));
            case ValueNode.Scalar s:
                return ValueTypes.Visit<ScalarFactory, ValueStep>(type, new(s.Value, type));
            case ValueNode.Arithmetic a:
            {
                if (!ValueTypes.IsArithmetic(type)) throw new ArgumentException($"{type.Name} values take no arithmetic.");
                if (!Enum.IsDefined(a.Op)) throw new ArgumentException($"Unknown arithmetic {a.Op}.");
                ValueStep left = Compile(a.Left, columns, type), right = Compile(a.Right, columns, type);
                return ValueTypes.Visit<ArithmeticFactory, ValueStep>(type, new(a.Op, left, right, type));
            }
            case ValueNode.Negate n:
            {
                if (!ValueTypes.IsSigned(type)) throw new ArgumentException($"{type.Name} values are not negated.");
                return ValueTypes.Visit<NegateFactory, ValueStep>(type, new(Compile(n.Operand, columns, type), type));
            }
            case ValueNode.Cast c:
            {
                IArrowType source = TypeOf(c.Operand, columns) ?? throw new ArgumentException("A cast scalar is to be computed before (folded).");
                ValueStep operand = Compile(c.Operand, columns, source);
                if (ValueTypes.Same(source, c.Type)) return operand;
                if (!ValueTypes.IsArithmetic(source) || !ValueTypes.IsArithmetic(c.Type) || ValueTypes.IsFloating(source) && !ValueTypes.IsFloating(c.Type))
                    throw new ArgumentException($"No cast from {source.Name} to {c.Type.Name} values.");
                return ValueTypes.Visit<CastSourceFactory, ValueStep>(source, new(operand, c.Type));
            }
            default:
                throw new ArgumentException($"Unknown value {node}.");
        }
    }

    /// <summary>The columns a step reads, ascending.</summary>
    public static int[] Columns(params ReadOnlySpan<ValueStep> steps)
    {
        SortedSet<int> columns = [];
        foreach (ValueStep step in steps) step.CollectColumns(columns);
        return [.. columns];
    }

    private readonly struct ColumnFactory(int column, IArrowType type) : INumericVisitor<ValueStep>
    {
        public ValueStep Visit<T>(bool floating) where T : unmanaged, INumber<T> => new ColumnStep<T>(column, type);
    }

    private readonly struct ScalarFactory(StoredScalar value, IArrowType type) : INumericVisitor<ValueStep>
    {
        public ValueStep Visit<T>(bool floating) where T : unmanaged, INumber<T> =>
            new ScalarStep<T>(ValueTypes.Exact<T>(value, floating, $"a {type.Name} value"), type);
    }

    private readonly struct ArithmeticFactory(ArithmeticOp op, ValueStep left, ValueStep right, IArrowType type) : INumericVisitor<ValueStep>
    {
        public ValueStep Visit<T>(bool floating) where T : unmanaged, INumber<T>
        {
            if (left is ScalarStep<T> && right is ScalarStep<T>) throw new ArgumentException("Arithmetic between two scalars is to be computed before (folded).");
            return new ArithmeticStep<T>(op, (ValueStep<T>)left, (ValueStep<T>)right, type);
        }
    }

    private readonly struct NegateFactory(ValueStep operand, IArrowType type) : INumericVisitor<ValueStep>
    {
        public ValueStep Visit<T>(bool floating) where T : unmanaged, INumber<T> => new NegateStep<T>((ValueStep<T>)operand, type);
    }

    private readonly struct CastSourceFactory(ValueStep operand, IArrowType target) : INumericVisitor<ValueStep>
    {
        public ValueStep Visit<T>(bool floating) where T : unmanaged, INumber<T> =>
            ValueTypes.Visit<CastTargetFactory<T>, ValueStep>(target, new((ValueStep<T>)operand, target));
    }

    private readonly struct CastTargetFactory<TFrom>(ValueStep<TFrom> operand, IArrowType target) : INumericVisitor<ValueStep>
        where TFrom : unmanaged, INumber<TFrom>
    {
        public ValueStep Visit<T>(bool floating) where T : unmanaged, INumber<T> => new CastStep<TFrom, T>(operand, target);
    }
}
