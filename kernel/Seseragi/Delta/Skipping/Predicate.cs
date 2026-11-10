namespace Kaminari.Seseragi.Delta.Skipping;

/// <summary>The comparisons a predicate can make between a column and a value.</summary>
public enum CompareOp
{
#pragma warning disable CS1591
    Eq, Ne, Lt, Le, Gt, Ge,
#pragma warning restore CS1591
}

/// <summary>
/// A condition on the rows of a table, used to leave out files that cannot hold a matching row. It follows SQL:
/// a comparison with a null value matches nothing.
/// </summary>
public abstract class Predicate
{
    private protected Predicate() { }

    /// <summary>Compares a column with a value.</summary>
    /// <param name="column">The logical column name; for a nested column, the path of names.</param>
    /// <param name="op">The comparison.</param>
    /// <param name="value">
    /// An integer, floating-point number, decimal, string, boolean, <see cref="DateOnly"/>,
    /// <see cref="DateTime"/> or <see cref="DateTimeOffset"/>, matching the column type.
    /// </param>
    public static Predicate Compare(ColumnPath column, CompareOp op, object value) =>
        new ComparePredicate(column, op, value ?? throw new ArgumentNullException(nameof(value)));

#pragma warning disable CS1591
    public static Predicate Eq(ColumnPath column, object value) => Compare(column, CompareOp.Eq, value);
    public static Predicate Ne(ColumnPath column, object value) => Compare(column, CompareOp.Ne, value);
    public static Predicate Lt(ColumnPath column, object value) => Compare(column, CompareOp.Lt, value);
    public static Predicate Le(ColumnPath column, object value) => Compare(column, CompareOp.Le, value);
    public static Predicate Gt(ColumnPath column, object value) => Compare(column, CompareOp.Gt, value);
    public static Predicate Ge(ColumnPath column, object value) => Compare(column, CompareOp.Ge, value);
#pragma warning restore CS1591

    /// <summary>The column is null.</summary>
    public static Predicate IsNull(ColumnPath column) => new NullPredicate(column, isNull: true);

    /// <summary>The column is not null.</summary>
    public static Predicate IsNotNull(ColumnPath column) => new NullPredicate(column, isNull: false);

    /// <summary>All conditions hold.</summary>
    public static Predicate And(params Predicate[] children) => new JunctionPredicate(isAnd: true, children);

    /// <summary>At least one condition holds.</summary>
    public static Predicate Or(params Predicate[] children) => new JunctionPredicate(isAnd: false, children);

    /// <summary>The condition does not hold.</summary>
    public static Predicate Not(Predicate child) => new NotPredicate(child ?? throw new ArgumentNullException(nameof(child)));
}

/// <summary>The path of logical names to a column: one name for a top-level column.</summary>
public readonly struct ColumnPath
{
    /// <summary>The names from the top-level column down.</summary>
    public string[] Segments { get; }

    /// <summary>Creates a path from its names.</summary>
    public ColumnPath(params string[] segments)
    {
        if (segments is null || segments.Length == 0) throw new ArgumentException("A column path needs at least one name.", nameof(segments));
        Segments = segments;
    }

    /// <summary>A top-level column.</summary>
    public static implicit operator ColumnPath(string name) => new(name);

    /// <inheritdoc/>
    public override string ToString() => string.Join(".", Segments ?? []);
}

internal sealed class ComparePredicate(ColumnPath column, CompareOp op, object value) : Predicate
{
    public ColumnPath Column { get; } = column;
    public CompareOp Op { get; } = op;
    public object Value { get; } = value;
}

internal sealed class NullPredicate(ColumnPath column, bool isNull) : Predicate
{
    public ColumnPath Column { get; } = column;
    public bool WantNull { get; } = isNull;
}

internal sealed class JunctionPredicate(bool isAnd, Predicate[] children) : Predicate
{
    public bool IsAnd { get; } = isAnd;
    public Predicate[] Children { get; } = children ?? throw new ArgumentNullException(nameof(children));
}

internal sealed class NotPredicate(Predicate child) : Predicate
{
    public Predicate Child { get; } = child;
}
