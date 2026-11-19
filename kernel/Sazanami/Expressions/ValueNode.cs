using System.Globalization;
using Apache.Arrow.Types;

namespace Kaminari.Sazanami.Expressions;

/// <summary>How two values compare: in the IR's order (see the Filters and Expressions kernels).</summary>
internal enum CompareOp
{
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

/// <summary>The domain of a <see cref="StoredScalar"/>.</summary>
internal enum StoredScalarKind
{
    /// <summary>An exact integer: integers of every width, booleans (0 / 1), dates, times, timestamps and durations in the column's unit, decimals unscaled at the column's scale.</summary>
    Integer,

    /// <summary>A floating value (NaN allowed) of a Float16, Float32 or Float64 column, exactly representable in its width.</summary>
    Float,

    /// <summary>The bytes of a string (UTF-8) or binary value.</summary>
    Bytes,
}

/// <summary>
/// A value in a column's stored representation. Programs (filter programs, value programs) never convert values:
/// whoever builds one (the query compiler's lowering) maps literals onto the column first, and the programs refuse
/// values the column cannot hold exactly.
/// </summary>
internal readonly struct StoredScalar
{
    private StoredScalar(StoredScalarKind kind, Int128 integer, double floating, byte[]? bytes)
    {
        Kind = kind;
        IntegerValue = integer;
        FloatValue = floating;
        BytesValue = bytes;
    }

    public StoredScalarKind Kind { get; }
    public Int128 IntegerValue { get; }
    public double FloatValue { get; }
    public byte[]? BytesValue { get; }

    public static StoredScalar Integer(Int128 value) => new(StoredScalarKind.Integer, value, 0, null);

    public static StoredScalar Float(double value) => new(StoredScalarKind.Float, 0, value, null);

    public static StoredScalar Bytes(byte[] value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return new(StoredScalarKind.Bytes, 0, 0, value);
    }

    public override string ToString() => Kind switch
    {
        StoredScalarKind.Integer => IntegerValue.ToString(CultureInfo.InvariantCulture),
        StoredScalarKind.Float => FloatValue.ToString("R", CultureInfo.InvariantCulture),
        _ => "0x" + Convert.ToHexString(BytesValue!),
    };
}

/// <summary>An arithmetic operation of a <see cref="ValueNode.Arithmetic"/>, with .NET's unchecked semantics in the operands' type.</summary>
internal enum ArithmeticOp
{
    /// <summary><c>a + b</c> (integers wrap).</summary>
    Add,

    /// <summary><c>a - b</c> (integers wrap).</summary>
    Subtract,

    /// <summary><c>a * b</c> (integers wrap).</summary>
    Multiply,

    /// <summary>
    /// <c>a / b</c>: integers truncate toward zero, a zero divisor gives null and <c>MinValue / -1</c> wraps to
    /// <c>MinValue</c>; floating values divide as IEEE does.
    /// </summary>
    Divide,

    /// <summary>
    /// <c>a % b</c>, .NET's remainder (the dividend's sign): a zero integer divisor gives null, <c>MinValue % -1</c>
    /// is 0.
    /// </summary>
    Remainder,
}

/// <summary>
/// A value expression computed on each row of a batch (<see cref="ValueProgram"/>, and the filters' value comparisons):
/// a column, a scalar, or arithmetic over them. Every value has one type: a column its own (a dictionary's value type), a
/// cast its target, arithmetic its operands' (which must agree), a scalar the type of the value it meets. Arithmetic
/// takes integers (8 to 64 bits) and Float32 / Float64; a null operand gives null.
/// </summary>
internal abstract record ValueNode
{
    private ValueNode() { }

    /// <summary>The values of a column (by its index in the program's columns).</summary>
    public sealed record Column(int Index) : ValueNode;

    /// <summary>A value on every row, of the type of the value it meets (it must hold it exactly).</summary>
    public sealed record Scalar(StoredScalar Value) : ValueNode;

    /// <summary><c>left op right</c>; at most one operand is a scalar.</summary>
    public sealed record Arithmetic(ArithmeticOp Op, ValueNode Left, ValueNode Right) : ValueNode;

    /// <summary><c>-operand</c> of a signed integer (wrapping) or floating value.</summary>
    public sealed record Negate(ValueNode Operand) : ValueNode;

    /// <summary>
    /// The operand converted to <paramref name="Type"/> as .NET's unchecked conversion does: integers to integers
    /// truncated, integers to floating values rounded to nearest, Float64 to Float32 rounded, Float32 to Float64
    /// exactly. Floating values to integers are not casts here.
    /// </summary>
    public sealed record Cast(ValueNode Operand, IArrowType Type) : ValueNode;
}
