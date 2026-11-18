namespace Kaminari.Seseragi.Files.Ipc;

/// <summary>The data is not Arrow IPC data, is damaged or invalid, or uses a feature this library does not read.</summary>
public sealed class IpcFormatException(string message, Exception? inner = null) : FormatException(message, inner);
