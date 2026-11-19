using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Builders;
using Kaminari.Sazanami.Arrow.Interop;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Filters;

namespace Kaminari.Sazanami.Expressions;

/// <summary>
/// A value expression (<see cref="ValueNode"/>) over Arrow columns, checked and compiled once by <see cref="Create"/>,
/// then computed batch by batch into Arrow arrays (<see cref="Evaluate"/>): the expression kernel the filters'
/// value comparisons run on too. Semantics are the query compiler's IR's (Samidare's IR/SEMANTICS.md, Arithmetic):
/// integers wrap in their type, an integer division or remainder by zero is null, <c>MinValue / -1</c> wraps, a null
/// operand gives null. Immutable and safe to evaluate from several threads at once.
/// </summary>
internal sealed class ValueProgram
{
    private readonly IArrowType[] _columns;

    private ValueProgram(IArrowType[] columns, ValueStep root)
    {
        _columns = columns;
        Root = root;
        Reads = ValueCompiler.Columns(root);
    }

    /// <summary>The type of the values computed (never a dictionary).</summary>
    public IArrowType Type => Root.Type;

    /// <summary>The columns the expression reads (indices into the columns it was created over), ascending.</summary>
    public IReadOnlyList<int> Reads { get; }

    internal ValueStep Root { get; }

    /// <summary>Checks an expression over columns of the given types (dictionaries of them allowed) and compiles it.</summary>
    /// <exception cref="ArgumentException">The expression is not well typed (a scalar alone among them: it has no type).</exception>
    public static ValueProgram Create(IReadOnlyList<IArrowType> columns, ValueNode root)
    {
        ArgumentNullException.ThrowIfNull(columns);
        IArrowType[] all = [.. columns];
        IArrowType type = ValueCompiler.TypeOf(root, all) ?? throw new ArgumentException("A scalar alone has no type to compute it in.");
        return new(all, ValueCompiler.Compile(root, all, type));
    }

    /// <summary>
    /// Computes the expression on a batch of <paramref name="length"/> rows (one array per column, each of that length)
    /// into an array of <see cref="Type"/>, whose buffers are native memory it owns (dispose it).
    /// </summary>
    /// <exception cref="ArgumentException">The arrays do not match the columns.</exception>
    public IArrowArray Evaluate(ReadOnlySpan<IArrowArray> columns, int length)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        if (columns.Length != _columns.Length) throw new ArgumentException($"The expression is over {_columns.Length} columns; {columns.Length} were given.", nameof(columns));
        foreach (int c in Reads)
        {
            if (columns[c].Length != length) throw new ArgumentException($"Column {c} holds {columns[c].Length} rows, not {length}.", nameof(columns));
            if (columns[c].Data.DataType is not NullType && !ValueTypes.Same(_columns[c], columns[c].Data.DataType))
                throw new ArgumentException($"Column {c} is {_columns[c].Name}; a {columns[c].Data.DataType.Name} array was given.", nameof(columns));
        }
        using var scratch = new ValueScratch();
        return ArrowArrayFactory.BuildArray(Root.EvaluateArray(new ValueContext(columns, length, scratch)));
    }

    /// <summary>A buffer over <paramref name="bytes"/>: their scratch block taken over, or a copy (a column's own values).</summary>
    internal static unsafe ArrowBuffer Own(ValueScratch scratch, ReadOnlySpan<byte> bytes)
    {
        byte* first = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(bytes));
        if (!scratch.Detach(first))
        {
            byte* copy = NativeMemoryPool.Alloc(Math.Max(1, bytes.Length));
            bytes.CopyTo(new Span<byte>(copy, bytes.Length));
            first = copy;
        }
        return NativeBatchExport.NewBuffer(PooledArrowAllocator.Adopt(first, bytes.Length));
    }
}
