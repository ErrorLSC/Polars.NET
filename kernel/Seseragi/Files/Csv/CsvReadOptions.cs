using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Seseragi.Files.IO;

namespace Kaminari.Seseragi.Files.Csv;

/// <summary>How <see cref="CsvReader"/> reads delimited text.</summary>
public sealed record CsvReadOptions
{
    /// <summary>The default rows per record batch.</summary>
    public const int DefaultBatchSize = 65_536;

    /// <summary>The byte between fields (ASCII; not the quote, nor a line end).</summary>
    public char Separator { get; init; } = ',';

    /// <summary>
    /// The quote around fields holding separators, line ends or quotes (a quote inside is doubled); null: fields are
    /// never quoted (quotes are data).
    /// </summary>
    public char? Quote { get; init; } = '"';

    /// <summary>The byte ending a record; null (the default) for LF, CRLF or a CR alone.</summary>
    public char? LineEnd { get; init; }

    /// <summary>Whether the first record names the columns (blank lines before it are passed over).</summary>
    public bool HasHeader { get; init; } = true;

    /// <summary>A comment line's first one or two characters (outside quotes, at a record start); null: none.</summary>
    public string? CommentPrefix { get; init; }

    /// <summary>Lines passed over at the start, counted as raw line ends (a quoted field's line ends count).</summary>
    public long SkipLines { get; init; }

    /// <summary>Records passed over at the start (after <see cref="SkipLines"/>), before the header.</summary>
    public long SkipRows { get; init; }

    /// <summary>Records passed over after the header.</summary>
    public long SkipRowsAfterHeader { get; init; }

    /// <summary>Field texts read as nulls in every column (compared after quotes are removed).</summary>
    public IReadOnlyList<string>? NullValues { get; init; }

    /// <summary>Whether a field a record lacks (fewer fields than columns) is a null (true) or an empty string.</summary>
    public bool MissingIsNull { get; init; } = true;

    /// <summary>Whether records with more fields than columns drop the extra ones (false: they are errors).</summary>
    public bool TruncateRaggedLines { get; init; }

    /// <summary>Whether floats use ',' as the decimal point (the separator is then not ',' unless fields are quoted).</summary>
    public bool DecimalComma { get; init; }

    /// <summary>
    /// The character grouping a number's integer digits by three ("1.234,56" with '.' and <see cref="DecimalComma"/>,
    /// "1 234,56" with a space or a no-break space, "1'234.5", "1,234.5" quoted); null (the default): numbers have no
    /// groups. Groups are checked (one to three digits, then groups of three), so "1.5" is no number with '.'.
    /// Inference takes grouped numbers as numbers when it is set.
    /// </summary>
    public string? ThousandsSeparator { get; init; }

    /// <summary>Field texts read as true besides "true" (any case), e.g. "VERDADEIRO"; inference takes them as booleans.</summary>
    public IReadOnlyList<string>? TrueValues { get; init; }

    /// <summary>Field texts read as false besides "false" (any case), e.g. "FALSO"; inference takes them as booleans.</summary>
    public IReadOnlyList<string>? FalseValues { get; init; }

    /// <summary>Whether inference takes dates, times and datetimes (false: they are strings).</summary>
    public bool TryParseDates { get; init; } = true;

    /// <summary>The input's encoding; null is UTF-8 (a byte order mark tells UTF-8 or UTF-16 apart).</summary>
    public System.Text.Encoding? Encoding { get; init; }

    /// <summary>What a byte sequence the encoding does not define is: an error (the default) or U+FFFD.</summary>
    public CsvInvalidText InvalidText { get; init; } = CsvInvalidText.Error;

    /// <summary>
    /// The columns and their types: nothing is inferred. With a header the columns are matched by name (the output in
    /// the schema's order; every column of the file must be in it and every column of it in the file); without one, by
    /// position (as many columns as the records hold). Cannot be combined with <see cref="SchemaOverrides"/>.
    /// </summary>
    public Schema? Schema { get; init; }

    /// <summary>Types of some columns, by name, the others inferred (names the input lacks are passed over).</summary>
    public IReadOnlyDictionary<string, IArrowType>? SchemaOverrides { get; init; }

    /// <summary>The records the types are inferred from (100 by default); null reads the whole input first; 0 makes every column a string.</summary>
    public int? InferSchemaLength { get; init; } = 100;

    /// <summary>
    /// The columns to read, by name (the others are passed over without being decoded), in the output's order: the given
    /// schema's with a header, else the file's.
    /// </summary>
    public IReadOnlyList<string>? Columns { get; init; }

    /// <summary>At most this many rows (the first ones).</summary>
    public long? MaxRows { get; init; }

    /// <summary>
    /// The name of an Int64 column put first in the batches, numbering each record from <see cref="RowIndexOffset"/> in
    /// the order of the input; null adds none.
    /// </summary>
    public string? RowIndexColumn { get; init; }

    /// <summary>The first row's number in <see cref="RowIndexColumn"/>.</summary>
    public long RowIndexOffset { get; init; }

    /// <summary>A value that does not fit its column's type is a null instead of an error (input errors still are errors).</summary>
    public bool IgnoreErrors { get; init; }

    /// <summary>Whether input without any record (not even a header) is an error (true) or an empty frame.</summary>
    public bool RaiseIfEmpty { get; init; } = true;

    /// <summary>Rows per record batch (a batch may be shorter: batches never span the input's chunks).</summary>
    public int BatchSize { get; init; } = DefaultBatchSize;

    /// <summary>The most chunks decoded at once.</summary>
    public int MaxDegreeOfParallelism { get; init; } = Environment.ProcessorCount;

    /// <summary>Inferred strings as Utf8View (true, the default) or Utf8. A given schema's types are kept as they are.</summary>
    public bool ViewTypes { get; init; } = true;

    /// <summary>A row filter in the query compiler's IR, evaluated with <see cref="FilterRows"/>.</summary>
    public ScalarExpr? Filter { get; init; }

    /// <summary>Whether only the rows where <see cref="Filter"/> is true are delivered.</summary>
    public bool FilterRows { get; init; }

    /// <summary>Whether a local file opened by its path is read through a memory mapping.</summary>
    public CsvMemoryMap MemoryMap { get; init; } = CsvMemoryMap.Auto;

    /// <summary>Leave the stream open when the reader is disposed.</summary>
    public bool LeaveOpen { get; init; }

    /// <summary>Whether text the reader could cut and tokenize on its own bytes is decoded into UTF-8 first anyway (tests).</summary>
    internal bool ForceTranscode { get; init; }

    /// <summary>The bytes of a unit of work (tests cut the input finely with it).</summary>
    internal int UnitBytes { get; init; } = TextUnits.DefaultUnitBytes;
}

/// <summary>What a byte sequence the input's encoding does not define becomes.</summary>
public enum CsvInvalidText
{
    /// <summary>An error naming its row and line.</summary>
    Error,

    /// <summary>U+FFFD (the replacement character).</summary>
    Replace,
}

/// <summary>Whether a local file is read through a memory mapping.</summary>
public enum CsvMemoryMap
{
    /// <summary>Mapped (local files opened by their path).</summary>
    Auto,

    /// <summary>Ordinary reads into native memory.</summary>
    Off,
}
