using Apache.Arrow.Types;
using Kaminari.Sazanami.Expressions;

namespace Kaminari.Sazanami.Filters;

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
    public sealed record Compare(int Column, CompareOp Op, StoredScalar Value) : FilterNode;

    /// <summary>Whether the column equals one of the values (null on a null value; the list holds no null).</summary>
    public sealed record InList(int Column, StoredScalar[] Values) : FilterNode;

    /// <summary>Whether the column's bytes start with the prefix.</summary>
    public sealed record StartsWith(int Column, byte[] Prefix) : FilterNode;

    /// <summary>Whether the column's bytes end with the suffix.</summary>
    public sealed record EndsWith(int Column, byte[] Suffix) : FilterNode;

    /// <summary>Whether the column's bytes hold the pattern somewhere (an empty pattern: every value).</summary>
    public sealed record Contains(int Column, byte[] Pattern) : FilterNode;

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

    /// <summary>
    /// <c>left op right</c> over two computed values of one type (numbers, booleans, or strings / binaries by their
    /// bytes; at most one side a scalar): null when either is null, unless <paramref name="NullsEqual"/> (with
    /// <see cref="CompareOp.Eq"/> only), where two nulls are equal and a null differs from every value.
    /// </summary>
    public sealed record CompareValues(ValueNode Left, CompareOp Op, ValueNode Right, bool NullsEqual = false) : FilterNode;

    /// <summary>Whether a computed number equals one of the values (null on a null value; the list holds no null).</summary>
    public sealed record ValueInList(ValueNode Value, StoredScalar[] Values) : FilterNode;

    /// <summary>Whether a computed number is null (an operand null, or an integer division by zero); never null itself.</summary>
    public sealed record ValueIsNull(ValueNode Value) : FilterNode;
}
