namespace Kaminari.Seseragi.Files.Ipc;

/// <summary>How <see cref="IpcFile.OpenReader"/> and <see cref="IpcStreamReader"/> read Arrow IPC data.</summary>
public sealed record IpcReadOptions
{
    /// <summary>
    /// The top-level columns to read, by name, returned in schema order (a name several columns share selects them
    /// all); null reads every column. The buffers of the other columns are not read, decompressed or checked.
    /// </summary>
    public IReadOnlyList<string>? Columns { get; init; }

    /// <summary>The most rows to read, in order; null reads them all.</summary>
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

    internal void Validate()
    {
        if (MaxRows is < 0) throw new ArgumentOutOfRangeException(nameof(MaxRows), MaxRows, "The row limit is negative.");
        if (!Enum.IsDefined(MemoryMap)) throw new ArgumentOutOfRangeException(nameof(MemoryMap), MemoryMap, "Unknown memory map mode.");
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
