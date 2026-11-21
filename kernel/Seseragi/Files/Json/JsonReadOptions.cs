using Apache.Arrow;
using Apache.Arrow.Types;

namespace Kaminari.Seseragi.Files.Json;

/// <summary>How <see cref="JsonReader"/> reads newline-delimited JSON (one object per line).</summary>
public sealed record JsonReadOptions
{
    /// <summary>The default rows per record batch.</summary>
    public const int DefaultBatchSize = 65_536;

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
    /// Fields first seen after them are passed over; values that do not fit the inferred type are errors (or nulls with
    /// <see cref="IgnoreErrors"/>).
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

    /// <summary>A value that does not fit its column's type is a null instead of an error (syntax errors still are).</summary>
    public bool IgnoreErrors { get; init; }

    /// <summary>Whether a local file opened by its path is read through a memory mapping.</summary>
    public JsonMemoryMap MemoryMap { get; init; } = JsonMemoryMap.Auto;

    /// <summary>Leave the stream open when the reader is disposed.</summary>
    public bool LeaveOpen { get; init; }

    /// <summary>The bytes of a unit of work (tests cut the input finely with it).</summary>
    internal int UnitBytes { get; init; } = Reading.JsonInput.DefaultUnitBytes;
}

/// <summary>Whether a local file is read through a memory mapping.</summary>
public enum JsonMemoryMap
{
    /// <summary>Mapped (local files opened by their path).</summary>
    Auto,

    /// <summary>Ordinary reads into native memory.</summary>
    Off,
}

/// <summary>The input is not newline-delimited JSON (or not UTF-8).</summary>
public sealed class JsonFormatException(string message, Exception? inner = null) : FormatException(message, inner);

/// <summary>A value does not fit its column's type.</summary>
public sealed class JsonValueException(string message) : Exception(message);
