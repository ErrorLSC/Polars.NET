namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>A Delta table could not be read or its log is not valid.</summary>
public class DeltaException : Exception
{
    /// <summary>Creates an exception with a message.</summary>
    public DeltaException(string message) : base(message) { }

    /// <summary>Creates an exception that wraps the failure underneath it.</summary>
    public DeltaException(string message, Exception? inner) : base(message, inner) { }
}

/// <summary>There is no Delta table at the location: the log directory holds no commit.</summary>
public sealed class DeltaTableNotFoundException(string message) : DeltaException(message);

/// <summary>The table needs a protocol version or table feature this kernel does not implement.</summary>
public sealed class DeltaProtocolException(string message) : DeltaException(message);

/// <summary>
/// A row a write would add violates a CHECK constraint or a column invariant of the table (its result is false or
/// null): nothing was committed. The message is Spark's: <c>[DELTA_VIOLATE_CONSTRAINT_WITH_VALUES] CHECK constraint c1
/// (a &gt; 0) violated by row with values:</c>, then a line per column the expression reads.
/// </summary>
public sealed class DeltaConstraintViolationException(string constraint, string expression, IReadOnlyList<KeyValuePair<string, string>> values)
    : DeltaException(Format(constraint, expression, values))
{
    /// <summary>The constraint's name (for a column invariant, <c>EXPRESSION(('a &gt; 3))</c>, as Spark names it).</summary>
    public string Constraint { get; } = constraint;

    /// <summary>The expression as the table stores it.</summary>
    public string Expression { get; } = expression;

    /// <summary>The values of the row's columns the expression reads, by name (<c>st.x</c> for a struct's field), as
    /// Spark writes them: dates as days and timestamps as microseconds since the epoch, <c>null</c> for null.</summary>
    public IReadOnlyList<KeyValuePair<string, string>> Values { get; } = values;

    private static string Format(string constraint, string expression, IReadOnlyList<KeyValuePair<string, string>> values) =>
        $"[DELTA_VIOLATE_CONSTRAINT_WITH_VALUES] CHECK constraint {constraint} ({expression}) violated by row with values:"
        + string.Concat(values.Select(v => $"\n - {v.Key} : {v.Value}"));
}
