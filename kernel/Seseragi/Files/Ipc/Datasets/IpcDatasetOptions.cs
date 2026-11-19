namespace Kaminari.Seseragi.Files.Ipc;

/// <summary>
/// How <see cref="IpcDataset.Open(string, IpcDatasetOptions?)"/> finds and describes a dataset's files (see
/// <see cref="DatasetOptions"/>). Files' strings and binaries in another layout than the dataset's (plain or view) are
/// read in the dataset's: the first file's that has the column, or as a given <see cref="DatasetOptions.Schema"/> says.
/// </summary>
public sealed record IpcDatasetOptions : DatasetOptions;

/// <summary>A file of an Arrow IPC dataset (see <see cref="DatasetFile"/>).</summary>
/// <param name="Path">The path, as the store spells it.</param>
/// <param name="Size">The size in bytes, when known (else the store is asked).</param>
/// <param name="PartitionValues">The file's partition values by column (see <see cref="DatasetFile.PartitionValues"/>).</param>
public sealed record IpcDatasetFile(string Path, long? Size = null, IReadOnlyDictionary<string, object?>? PartitionValues = null)
    : DatasetFile(Path, Size, PartitionValues);
