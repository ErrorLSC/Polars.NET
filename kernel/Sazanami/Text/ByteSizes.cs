using System.Globalization;

namespace Kaminari.Sazanami.Text;

/// <summary>Sizes in bytes as people write them in settings: <c>512MiB</c>, <c>2 GiB</c>, <c>1.5GB</c>, <c>1048576</c>.</summary>
internal static class ByteSizes
{
    /// <summary>
    /// Reads a size: a number (decimals allowed) and a unit, case-insensitive, spaces around ignored: none or <c>B</c>
    /// for bytes; <c>K</c>, <c>KiB</c>, <c>M</c>, <c>MiB</c>, <c>G</c>, <c>GiB</c>, <c>T</c>, <c>TiB</c> in powers of
    /// 1024; <c>KB</c>, <c>MB</c>, <c>GB</c>, <c>TB</c> in powers of 1000. False for anything else, a negative size or one
    /// beyond <see cref="long.MaxValue"/>.
    /// </summary>
    public static bool TryParse(ReadOnlySpan<char> text, out long bytes)
    {
        bytes = 0;
        text = text.Trim();
        int unitAt = text.Length;
        while (unitAt > 0 && char.IsAsciiLetter(text[unitAt - 1])) unitAt--;
        double? scale = text[unitAt..].ToString().ToLowerInvariant() switch
        {
            "" or "b" => 1,
            "k" or "kib" => 1L << 10,
            "m" or "mib" => 1L << 20,
            "g" or "gib" => 1L << 30,
            "t" or "tib" => 1L << 40,
            "kb" => 1e3,
            "mb" => 1e6,
            "gb" => 1e9,
            "tb" => 1e12,
            _ => null,
        };
        if (scale is not double factor
            || !double.TryParse(text[..unitAt].Trim(), NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out double number))
            return false;
        double value = Math.Round(number * factor);
        if (value is < 0 or >= long.MaxValue) return false;
        bytes = (long)value;
        return true;
    }

    /// <summary>
    /// Reads a size as <see cref="TryParse"/> does, or a share of <paramref name="whole"/> bytes: a number above 0 and at
    /// most 100 followed by <c>%</c> (<c>25%</c>, <c>12.5 %</c>), at least one byte.
    /// </summary>
    public static bool TryParseSizeOrShare(ReadOnlySpan<char> text, long whole, out long bytes)
    {
        text = text.Trim();
        if (text is not [.., '%']) return TryParse(text, out bytes);
        bytes = 0;
        if (!double.TryParse(text[..^1].Trim(), NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out double percent)
            || percent is not (> 0 and <= 100))
            return false;
        bytes = Math.Max(1, (long)(whole * percent / 100));
        return true;
    }
}
