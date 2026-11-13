namespace Kaminari.Seseragi.Files.Parquet.Metadata;

/// <summary>Errors shared by the metadata structs.</summary>
internal static class ThriftModel
{
    public static ParquetFormatException Missing(string field) => new($"The Parquet metadata lacks the required field {field}.");
}
