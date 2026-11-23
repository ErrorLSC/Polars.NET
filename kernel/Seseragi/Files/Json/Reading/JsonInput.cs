using Kaminari.Seseragi.Files.IO;

namespace Kaminari.Seseragi.Files.Json.Reading;

/// <summary>JSON lines: units end at newlines; their errors are <see cref="JsonFormatException"/>s.</summary>
internal sealed class JsonLinesCut : NewlineCut
{
    public static JsonLinesCut Instance { get; } = new();

    public override Exception TooLong(long start) => new JsonFormatException($"A line at byte {start} is longer than 2 GB.");

    public override Exception Damaged(Exception inner) => new JsonFormatException($"The compressed input is damaged: {inner.Message}", inner);
}
