using System.Text;
using Kaminari.Seseragi.Files.IO;

namespace Seseragi.Files.Tests;

/// <summary>
/// <see cref="LineSkip"/>: raw lines passed over (LF, CR, CRLF as one, or a chosen byte), whatever the chunks the
/// input comes in (a CRLF split between two), a last line without a line end counting, from memory and from streams.
/// </summary>
public class LineSkipTests
{
    [Theory]
    [InlineData("a\nb\nc\n", 2, null, 4)]
    [InlineData("a\r\nb\r\nc", 2, null, 6)]
    [InlineData("a\rb\rc", 2, null, 4)]
    [InlineData("a\r\n\r\nc", 2, null, 5)]
    [InlineData("\"a\nb\"\nc\n", 1, null, 3)]
    [InlineData("a;b;c", 2, ";", 4)]
    [InlineData("a\nb;c", 1, ";", 4)]
    [InlineData("a\nb", 2, null, 3)]
    [InlineData("a\nb\n", 2, null, 4)]
    [InlineData("a\nb", 3, null, -1)]
    [InlineData("", 1, null, -1)]
    [InlineData("a\n", 0, null, 0)]
    public async Task SkipsRawLines_InAnyChunks(string text, long lines, string? end, long expected)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(text);
        byte? lineEnd = end is null ? null : (byte)end[0];
        // In memory.
        long at = await new LineSkip(lines, lineEnd).SkipAsync(new MemoryRandomAccess(bytes), 0, true, default);
        Assert.Equal(expected, at);
        // Fed in two chunks split anywhere (a CRLF split in two included).
        for (int split = 0; split <= bytes.Length; split++)
        {
            var skip = new LineSkip(lines, lineEnd);
            int found = skip.Feed(bytes.AsSpan(0, split));
            long got = found >= 0 ? found : (found = skip.Feed(bytes.AsSpan(split))) >= 0 ? split + found : skip.Done ? bytes.Length : -1;
            Assert.True(expected == got, $"split {split}: {got}, expected {expected}");
        }
        // From a stream: what is left after the lines.
        using Stream? rest = await new LineSkip(lines, lineEnd).SkipAsync(new MemoryStream(bytes), true, default);
        if (expected < 0) Assert.Null(rest);
        else
        {
            using var left = new MemoryStream();
            rest!.CopyTo(left);
            Assert.Equal(bytes[(int)expected..], left.ToArray());
        }
    }
}
