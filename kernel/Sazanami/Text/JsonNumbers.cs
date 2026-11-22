namespace Kaminari.Sazanami.Text;

/// <summary>
/// Fast paths for numbers in JSON's grammar (<c>-?int(.frac)?([eE][+-]?digits)?</c>, validated by the caller's tokenizer):
/// the common cases parsed directly, every other number left to a full parser (the methods return false). What they
/// return is what a correctly rounded parser returns, bit for bit.
/// </summary>
internal static class JsonNumbers
{
    /// <summary>The powers of ten a double holds exactly.</summary>
    private static ReadOnlySpan<double> ExactPowers =>
    [
        1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11,
        1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22,
    ];

    /// <summary>The largest integer every smaller one of which a double holds exactly (2^53).</summary>
    private const ulong ExactMantissa = 1UL << 53;

    /// <summary>A whole number of at most 18 digits (no fraction, no exponent); false for any other number.</summary>
    public static bool TryInt64(ReadOnlySpan<byte> text, out long value)
    {
        value = 0;
        int i = text.Length > 0 && text[0] == '-' ? 1 : 0;
        int digits = text.Length - i;
        if (digits is < 1 or > 18) return false;
        long result = 0;
        for (; i < text.Length; i++)
        {
            uint d = (uint)(text[i] - '0');
            if (d > 9) return false;
            result = result * 10 + d;
        }
        value = text[0] == '-' ? -result : result;
        return true;
    }

    /// <summary>
    /// A number whose value one correctly rounded operation gives exactly (Clinger's fast path): its significant digits
    /// at most 19 and their value at most 2^53, its power of ten within ±22. False for any other number.
    /// </summary>
    public static bool TryExactDouble(ReadOnlySpan<byte> text, out double value)
    {
        value = 0;
        int i = 0;
        bool negative = text.Length > 0 && text[0] == '-';
        if (negative) i++;
        ulong mantissa = 0;
        int significant = 0, exponent = 0;
        // Integer digits, then fraction digits (each moves the power of ten down by one); leading zeros are not significant.
        for (; i < text.Length && (uint)(text[i] - '0') <= 9; i++) Digit(text[i], ref mantissa, ref significant);
        if (i < text.Length && text[i] == '.')
        {
            for (i++; i < text.Length && (uint)(text[i] - '0') <= 9; i++)
            {
                Digit(text[i], ref mantissa, ref significant);
                exponent--;
            }
        }
        if (significant > 19) return false;
        if (i < text.Length && (text[i] | 0x20) == 'e')
        {
            i++;
            bool down = i < text.Length && text[i] == '-';
            if (i < text.Length && text[i] is (byte)'-' or (byte)'+') i++;
            int power = 0;
            for (; i < text.Length && (uint)(text[i] - '0') <= 9; i++)
            {
                if (power > 1000) return false;
                power = power * 10 + (text[i] - '0');
            }
            exponent += down ? -power : power;
        }
        if (i != text.Length) return false;
        if (mantissa == 0)
        {
            value = negative ? -0.0 : 0.0;
            return true;
        }
        if (mantissa > ExactMantissa || exponent is < -22 or > 22) return false;
        double exact = mantissa;
        exact = exponent >= 0 ? exact * ExactPowers[exponent] : exact / ExactPowers[-exponent];
        value = negative ? -exact : exact;
        return true;

        static void Digit(byte c, ref ulong mantissa, ref int significant)
        {
            if (significant == 0 && c == '0') return;
            if (++significant <= 19) mantissa = mantissa * 10 + (uint)(c - '0');
        }
    }
}
