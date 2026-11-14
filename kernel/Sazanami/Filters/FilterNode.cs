using System.Globalization;
using Apache.Arrow.Types;

namespace Kaminari.Sazanami.Filters;

/// <summary>How a <see cref="FilterNode.Compare"/> compares a column with a value.</summary>
internal enum CompareOp
{
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

/// <summary>The domain of a <see cref="FilterScalar"/>.</summary>
internal enum FilterScalarKind
{
    /// <summary>An exact integer: integers of every width, booleans (0 / 1), dates, times, timestamps and durations in the column's unit, decimals unscaled at the column's scale.</summary>
    Integer,

    /// <summary>A floating value (NaN allowed) of a Float16, Float32 or Float64 column, exactly representable in its width.</summary>
    Float,

    /// <summary>The bytes of a string (UTF-8) or binary value.</summary>
    Bytes,
}

/// <summary>
/// A value in a column's stored representation. Programs never convert values: whoever builds one (the query
/// compiler's lowering) maps literals onto the column first, and <see cref="FilterProgram.Create"/> refuses values the
/// column cannot hold exactly.
/// </summary>
internal readonly struct FilterScalar
{
    private FilterScalar(FilterScalarKind kind, Int128 integer, double floating, byte[]? bytes)
    {
        Kind = kind;
        IntegerValue = integer;
        FloatValue = floating;
        BytesValue = bytes;
    }

    public FilterScalarKind Kind { get; }
    public Int128 IntegerValue { get; }
    public double FloatValue { get; }
    public byte[]? BytesValue { get; }

    public static FilterScalar Integer(Int128 value) => new(FilterScalarKind.Integer, value, 0, null);

    public static FilterScalar Float(double value) => new(FilterScalarKind.Float, 0, value, null);

    public static FilterScalar Bytes(byte[] value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return new(FilterScalarKind.Bytes, 0, 0, value);
    }

    public override string ToString() => Kind switch
    {
        FilterScalarKind.Integer => IntegerValue.ToString(CultureInfo.InvariantCulture),
        FilterScalarKind.Float => FloatValue.ToString("R", CultureInfo.InvariantCulture),
        _ => "0x" + Convert.ToHexString(BytesValue!),
    };
}

/// <summary>A column a <see cref="FilterProgram"/> reads: its path of field names from a top-level column, and its Arrow type.</summary>
internal sealed record FilterColumn(string[] Path, IArrowType Type)
{
    public override string ToString() => string.Join('.', Path);
}

/// <summary>
/// A node of a physical row predicate (<see cref="FilterProgram"/>), in three-valued (Kleene) logic: every node is
/// true, false or null on a row. Leaves test one column (by its index in the program's columns); a leaf on a null
/// value is null, except <see cref="IsNull"/>. Floating values follow the total order of the query compiler's IR: NaN
/// equals NaN and is greater than every number, and -0 equals 0. Bytes compare unsigned, byte by byte (code point
/// order for UTF-8).
/// </summary>
internal abstract record FilterNode
{
    private FilterNode() { }

    /// <summary><c>column op value</c>.</summary>
    public sealed record Compare(int Column, CompareOp Op, FilterScalar Value) : FilterNode;

    /// <summary>Whether the column equals one of the values (null on a null value; the list holds no null).</summary>
    public sealed record InList(int Column, FilterScalar[] Values) : FilterNode;

    /// <summary>Whether the column's bytes start with the prefix.</summary>
    public sealed record StartsWith(int Column, byte[] Prefix) : FilterNode;

    /// <summary>Whether the column is null (never null itself).</summary>
    public sealed record IsNull(int Column) : FilterNode;

    /// <summary>Whether a floating column is NaN.</summary>
    public sealed record IsNan(int Column) : FilterNode;

    /// <summary>A boolean column taken as the predicate.</summary>
    public sealed record BooleanColumn(int Column) : FilterNode;

    /// <summary>True, false or (no value) null on every row.</summary>
    public sealed record Constant(bool? Value) : FilterNode;

    public sealed record And(FilterNode[] Operands) : FilterNode;

    public sealed record Or(FilterNode[] Operands) : FilterNode;

    public sealed record Not(FilterNode Operand) : FilterNode;

    /// <summary>The first operand that is not null on the row (null when all are).</summary>
    public sealed record Coalesce(FilterNode[] Operands) : FilterNode;
}
