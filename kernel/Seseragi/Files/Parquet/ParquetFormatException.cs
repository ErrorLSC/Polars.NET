namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>The data is not a Parquet file, is damaged, or uses a feature this library does not read.</summary>
public sealed class ParquetFormatException(string message, Exception? inner = null) : FormatException(message, inner);
