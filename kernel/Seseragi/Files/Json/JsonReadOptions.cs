using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;

namespace Kaminari.Seseragi.Files.Json;

/// <summary>How <see cref="JsonReader"/> reads JSON: newline-delimited (one object per line) or one document.</summary>
public sealed record JsonReadOptions
{
    /// <summary>The default rows per record batch.</summary>
    public const int DefaultBatchSize = 65_536;

    /// <summary>The input's layout: one object per line (the default) or one JSON document.</summary>
    public JsonFormat Format { get; init; } = JsonFormat.Lines;

    /// <summary>
    /// The columns and their types: nothing is inferred, fields the schema does not name are passed over, fields a
    /// record lacks are nulls. Cannot be combined with <see cref="SchemaOverrides"/>.
    /// </summary>
    public Schema? Schema { get; init; }

    /// <summary>
    /// Types of some columns, by name, the others inferred (a name the inference does not find is an error). Cannot be
    /// combined with <see cref="Schema"/>.
    /// </summary>
    public IReadOnlyDictionary<string, IArrowType>? SchemaOverrides { get; init; }

    /// <summary>
    /// The records the columns and their types are inferred from (100 by default); null reads the whole input first.
    /// Fields first seen after them are passed over (<see cref="JsonFormat.Lines"/>) or errors
    /// (<see cref="JsonFormat.Document"/>, unless <see cref="IgnoreErrors"/>); values that do not fit the inferred type
    /// are errors (or nulls with <see cref="IgnoreErrors"/>).
    /// </summary>
    public int? InferSchemaLength { get; init; } = 100;

    /// <summary>The columns to read, by name, in schema order (the others are passed over without being decoded).</summary>
    public IReadOnlyList<string>? Columns { get; init; }

    /// <summary>At most this many rows (the first ones).</summary>
    public long? MaxRows { get; init; }

    /// <summary>Rows per record batch (a batch may be shorter: batches never span the input's chunks).</summary>
    public int BatchSize { get; init; } = DefaultBatchSize;

    /// <summary>The most chunks decoded at once.</summary>
    public int MaxDegreeOfParallelism { get; init; } = Environment.ProcessorCount;

    /// <summary>Inferred strings as Utf8View (true, the default) or Utf8. A given schema's types are kept as they are.</summary>
    public bool ViewTypes { get; init; } = true;

    /// <summary>
    /// A value that does not fit its column's type is a null instead of an error, and a document's fields the inference
    /// did not see are passed over (syntax errors still are errors).
    /// </summary>
    public bool IgnoreErrors { get; init; }

    /// <summary>
    /// Whether a field matches its column ignoring case when no field has the column's exact name (off by default: names
    /// are matched exactly). Within an object, the first field matching a column gives its value. <see cref="JsonReader{T}"/>
    /// always matches this way, as rows map columns to members.
    /// </summary>
    public bool KeysIgnoreCase { get; init; }

    /// <summary>
    /// A row filter in the query compiler's IR, evaluated with <see cref="FilterRows"/> (JSON has no statistics to skip
    /// input by, so without it the filter does nothing). Columns are the top-level names, struct fields are reached with
    /// <c>StructFieldByName</c>; the semantics are the IR's (Kleene logic, NaN greater than every number).
    /// </summary>
    public ScalarExpr? Filter { get; init; }

    /// <summary>
    /// Whether only the rows where <see cref="Filter"/> is true are delivered (off by default), each chunk filtered once
    /// decoded, in parallel. Comparisons of columns with literals or with each other, integer and floating arithmetic
    /// (operands of one type), casts between numbers and IN lists are evaluated here; conjuncts the reader cannot
    /// evaluate (functions, powers, …) are left out, so the rows delivered may still include some the filter drops:
    /// <see cref="JsonReader.FiltersExactly"/> tells. Columns the filter reads are decoded even when not selected.
    /// <see cref="MaxRows"/> counts the rows delivered.
    /// </summary>
    public bool FilterRows { get; init; }

    /// <summary>Whether a local file opened by its path is read through a memory mapping.</summary>
    public JsonMemoryMap MemoryMap { get; init; } = JsonMemoryMap.Auto;

    /// <summary>Leave the stream open when the reader is disposed.</summary>
    public bool LeaveOpen { get; init; }

    /// <summary>The bytes of a unit of work (tests cut the input finely with it).</summary>
    internal int UnitBytes { get; init; } = Reading.JsonInput.DefaultUnitBytes;
}

/// <summary>The layout of JSON input.</summary>
public enum JsonFormat
{
    /// <summary>
    /// Newline-delimited JSON (NDJSON, JSON Lines): one object per line, blank lines passed over. The input is cut into
    /// chunks of lines anywhere, so chunks are found and read in parallel.
    /// </summary>
    Lines,

    /// <summary>
    /// One JSON document: an array of objects (a row each), or a single object (one row). The array is cut into chunks
    /// of whole elements (in parallel for memory and local files: a vectorized scan of segments, resolved in order; as
    /// it is read for other sources), decoded in parallel.
    /// </summary>
    Document,
}

/// <summary>Whether a local file is read through a memory mapping.</summary>
public enum JsonMemoryMap
{
    /// <summary>Mapped (local files opened by their path).</summary>
    Auto,

    /// <summary>Ordinary reads into native memory.</summary>
    Off,
}

/// <summary>The input is not JSON of the expected layout (or not UTF-8).</summary>
public sealed class JsonFormatException(string message, Exception? inner = null) : FormatException(message, inner);

/// <summary>A value does not fit its column's type.</summary>
public sealed class JsonValueException(string message) : Exception(message);
