using System.Text.Json;
using Kaminari.Seseragi.Files.Parquet;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>The Parquet test files and the footers pyarrow reads from them (<c>TestData/parquet/footers.json</c>).</summary>
internal static class ParquetFixtures
{
    public static readonly string Root = Path.Combine(AppContext.BaseDirectory, "TestData", "parquet");

    private static readonly Lazy<JsonDocument> Golden = new(() => JsonDocument.Parse(File.ReadAllBytes(Path.Combine(Root, "footers.json"))));

    /// <summary>Every fixture, by its path relative to <see cref="Root"/>.</summary>
    public static IEnumerable<string> All => Golden.Value.RootElement.GetProperty("files").EnumerateObject().Select(p => p.Name);

    public static TheoryData<string> AllData => [.. All];

    public static JsonElement Expected(string name) => Golden.Value.RootElement.GetProperty("files").GetProperty(name);

    public static string FullPath(string name) => Path.Combine(Root, name);

    public static bool IsEncrypted(string name) => name.EndsWith(".encrypted", StringComparison.Ordinal);

    /// <summary>The footer bytes of a file.</summary>
    public static byte[] FooterBytes(string name)
    {
        byte[] file = File.ReadAllBytes(FullPath(name));
        var (offset, length) = ParquetMetadata.FooterRange(file, file.Length, name);
        return file.AsSpan((int)offset, length).ToArray();
    }
}
