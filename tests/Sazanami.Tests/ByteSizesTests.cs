using Kaminari.Sazanami.Text;

namespace Sazanami.Tests;

/// <summary>
/// <see cref="ByteSizes.TryParse"/>: binary and decimal units, decimals, spaces and case; what it refuses. Shares of a
/// whole with <see cref="ByteSizes.TryParseSizeOrShare"/>.
/// </summary>
public class ByteSizesTests
{
    [Theory]
    [InlineData("1048576", 1L << 20)]
    [InlineData("512B", 512L)]
    [InlineData("512MiB", 512L << 20)]
    [InlineData(" 2 gib ", 2L << 30)]
    [InlineData("2G", 2L << 30)]
    [InlineData("1.5GiB", 3L << 29)]
    [InlineData("64k", 64L << 10)]
    [InlineData("1TiB", 1L << 40)]
    [InlineData("1GB", 1_000_000_000L)]
    [InlineData("250MB", 250_000_000L)]
    [InlineData("3KB", 3_000L)]
    [InlineData("0", 0L)]
    public void Sizes_AreRead(string text, long bytes)
    {
        Assert.True(ByteSizes.TryParse(text, out long read));
        Assert.Equal(bytes, read);
    }

    [Theory]
    [InlineData("")]
    [InlineData("GiB")]
    [InlineData("-1GiB")]
    [InlineData("2 PiB")]
    [InlineData("two GiB")]
    [InlineData("1e3")]
    [InlineData("9000000TiB")]
    [InlineData("25%")]
    public void OtherText_IsRefused(string text) => Assert.False(ByteSizes.TryParse(text, out _));

    [Theory]
    [InlineData("25%", 250L)]
    [InlineData(" 12.5 % ", 125L)]
    [InlineData("100%", 1000L)]
    [InlineData("0.01%", 1L)]   // at least one byte
    [InlineData("2KiB", 2048L)] // a size is read as one
    [InlineData("0", 0L)]
    public void SizesOrShares_AreRead(string text, long bytes)
    {
        Assert.True(ByteSizes.TryParseSizeOrShare(text, 1000, out long read));
        Assert.Equal(bytes, read);
    }

    [Theory]
    [InlineData("0%")]
    [InlineData("101%")]
    [InlineData("-5%")]
    [InlineData("%")]
    [InlineData("half%")]
    [InlineData("lots")]
    public void OtherSharesAndText_AreRefused(string text) => Assert.False(ByteSizes.TryParseSizeOrShare(text, 1000, out _));
}
