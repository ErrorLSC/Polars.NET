namespace Kaminari.Seseragi.Files;

/// <summary>
/// Who owns the record batches given to a writer (<see cref="Parquet.ParquetWriter"/>, <see cref="Ipc.IpcWriter"/>):
/// beyond who disposes them, a choice of speed or memory. The writer owning them may work on several at once, holding
/// them meanwhile: faster on several threads, for more memory.
/// </summary>
public enum BatchOwnership
{
    /// <summary>The caller: a batch is encoded before Write returns and may be disposed then. The least memory.</summary>
    Caller,

    /// <summary>
    /// The writer: it takes every batch given to Write (and those of the static Write methods), reads it after Write
    /// returns, and disposes it once it is written (also when Write fails, or the writer is aborted or disposed); the
    /// caller neither uses nor disposes it afterwards. With more than one thread, several batches are then encoded at
    /// once, straight from them (Parquet: row groups; Arrow IPC: record batches compressed side by side): faster, for
    /// the memory of the batches held meanwhile (Parquet: some hundreds of MiB more).
    /// </summary>
    Writer,
}
