using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Arrow.Ipc;

namespace Kaminari.Seseragi.Files.Ipc;

/// <summary>How <see cref="IpcFile.OpenReader"/> and <see cref="IpcStreamReader"/> read Arrow IPC data.</summary>
public sealed record IpcReadOptions
{
    /// <summary>
    /// The top-level columns to read, by name, returned in schema order (a name several columns share selects them
    /// all); null reads every column. The buffers of the other columns are not read, decompressed or checked.
    /// </summary>
    public IReadOnlyList<string>? Columns { get; init; }

    /// <summary>The most rows to read, in order (with <see cref="FilterRows"/>, the rows delivered after filtering); null reads them all.</summary>
    public long? MaxRows { get; init; }

    /// <summary>
    /// Whether a local file is read through a memory mapping (<see cref="IpcMemoryMap.Auto"/> by default). Only files
    /// opened by path are mapped: other sources (streams, even file streams, memory, stores) ignore it.
    /// </summary>
    public IpcMemoryMap MemoryMap { get; init; } = IpcMemoryMap.Auto;

    /// <summary>
    /// Whether strings (<c>Utf8</c>, <c>LargeUtf8</c>, <c>Utf8View</c>) are checked to be UTF-8 (off by default, as
    /// pyarrow and Polars read them: the format promises it). The structure of every array is always checked.
    /// </summary>
    public bool ValidateUtf8 { get; init; }

    /// <summary>
    /// A row filter in the query compiler's IR, evaluated with <see cref="FilterRows"/> (IPC data has no statistics to
    /// skip batches by, so without it the filter does nothing). Columns are the top-level names, struct fields are
    /// reached with <c>StructFieldByName</c>; the semantics are the IR's (Kleene logic, NaN greater than every number).
    /// </summary>
    public ScalarExpr? Filter { get; init; }

    /// <summary>
    /// Whether only the rows where <see cref="Filter"/> is true are delivered (off by default). Within each batch the
    /// columns the filter reads are decoded first, and the other columns only when it keeps a row; batches where it
    /// keeps none are passed over. Comparisons of columns with literals or with each other, integer and floating
    /// arithmetic (operands of one type), casts between numbers and IN lists are evaluated here; conjuncts the reader
    /// cannot evaluate (functions, powers, …) are left out, so the rows delivered may still include some the filter drops:
    /// <see cref="IpcReader.FiltersExactly"/> tells. Columns the filter reads are read even when not selected, so damage
    /// in them fails the read. List views, unions and run-end encoded columns cannot be selected with it.
    /// </summary>
    public bool FilterRows { get; init; }

    /// <summary>
    /// The name of an Int64 column put first in the batches, numbering each row from 0 in the order of the data (before
    /// any filter: a row keeps its number); null adds none. A dataset numbers rows across its files.
    /// </summary>
    public string? RowIndexColumn { get; init; }

    /// <summary>Added to the numbers in <see cref="RowIndexColumn"/> (a dataset numbers rows across its files).</summary>
    internal long RowIndexOffset { get; init; }

    /// <summary>
    /// How many threads decode at once (all cores by default; 1 reads on the calling thread). Several batches are read
    /// and decoded side by side, ahead of the consumer, and the compressed buffers of a large batch are decompressed
    /// side by side; batches still come in order. An <see cref="IpcStreamReader"/> above 1 reads the stream ahead on a
    /// thread of its own (messages in order) and decodes the batches it reads in parallel.
    /// </summary>
    public int MaxDegreeOfParallelism { get; init; } = Environment.ProcessorCount;

    /// <summary>Batches are started ahead of the consumer while their body sizes stay within this (at least one is).</summary>
    internal long InFlightBytes { get; init; } = 512L << 20;

    /// <summary>The compressed bytes of a batch from which its buffers are decompressed side by side (tests lower it).</summary>
    internal long ParallelDecompressFrom { get; init; } = IpcBody.ParallelFrom;

    internal void Validate()
    {
        if (MaxDegreeOfParallelism <= 0)
            throw new ArgumentOutOfRangeException(nameof(MaxDegreeOfParallelism), MaxDegreeOfParallelism, "The degree of parallelism must be positive.");
        if (MaxRows is < 0) throw new ArgumentOutOfRangeException(nameof(MaxRows), MaxRows, "The row limit is negative.");
        if (!Enum.IsDefined(MemoryMap)) throw new ArgumentOutOfRangeException(nameof(MemoryMap), MemoryMap, "Unknown memory map mode.");
        if (RowIndexColumn is "") throw new ArgumentException("The row index column needs a name.", nameof(RowIndexColumn));
    }
}

/// <summary>Whether an IPC file is read through a memory mapping.</summary>
/// <remarks>
/// Batches read through a mapping point into it (no copy), so the mapping stays until the last of them is disposed.
/// While a file is mapped, it must not be truncated by another process: touching pages that no longer exist ends the
/// process (SIGBUS on Unix, an access violation on Windows), which no exception handler can catch. Use
/// <see cref="Off"/> for files that may change while they are read.
/// </remarks>
public enum IpcMemoryMap
{
    /// <summary>Ordinary reads: each batch's buffers are read into native memory.</summary>
    Off,

    /// <summary>Every batch read from the mapping (compressed ones decompressed out of it).</summary>
    On,

    /// <summary>Uncompressed batches read from the mapping without a copy; compressed ones read and decompressed (the default).</summary>
    Auto,
}
