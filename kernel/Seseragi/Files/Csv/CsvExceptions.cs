namespace Kaminari.Seseragi.Files.Csv;

/// <summary>
/// The input is not delimited text the reader can read: malformed quoting, a record with more fields than columns,
/// text its encoding does not define, no records at all.
/// </summary>
public sealed class CsvFormatException : FormatException
{
    /// <summary>An input error with its message (and cause).</summary>
    public CsvFormatException(string message, Exception? inner = null) : base(message, inner) { }

    internal CsvFormatException(string message, long row, long line, Exception? inner = null) : base(message, inner)
    {
        Row = row;
        Line = line;
    }

    /// <summary>The record (1 = the first data record, 0 = the header), or -1 when the failure is not a record's.</summary>
    public long Row { get; } = -1;

    /// <summary>The line the record starts on (1 = the first), or -1.</summary>
    public long Line { get; } = -1;
}

/// <summary>A value its column's type cannot hold (an integer too large, a date in no format the column reads …).</summary>
public sealed class CsvValueException : Exception
{
    internal CsvValueException(string message, string column, long row, long line, string value) : base(message)
    {
        Column = column;
        Row = row;
        Line = line;
        Value = value;
    }

    /// <summary>The column's name.</summary>
    public string Column { get; }

    /// <summary>The record (1 = the first data record).</summary>
    public long Row { get; }

    /// <summary>The line the record starts on (1 = the first).</summary>
    public long Line { get; }

    /// <summary>The field's text (shortened when long).</summary>
    public string Value { get; }
}

/// <summary>A value the writer cannot write (a string that is not UTF-8 …): the writer cannot be completed then.</summary>
public sealed class CsvWriteException : Exception
{
    internal CsvWriteException(string message, string column, long row) : base(message)
    {
        Column = column;
        Row = row;
    }

    /// <summary>The column's name.</summary>
    public string Column { get; }

    /// <summary>The row among all written (0 = the first).</summary>
    public long Row { get; }
}
