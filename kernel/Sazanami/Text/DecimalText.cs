using System.Globalization;

namespace Kaminari.Sazanami.Text;

/// <summary>Exact decimal text to a Decimal128 unscaled value.</summary>
internal static class DecimalText
{
    /// <summary>
    /// Parses <c>[-+]digits[.digits][e[-+]digits]</c> as an unscaled integer at <paramref name="scale"/>; false when
    /// the value has more fraction digits than the scale holds (non-zero ones) or more than
    /// <paramref name="precision"/> digits in all.
    /// </summary>
    public static bool TryParse(ReadOnlySpan<byte> text, int precision, int scale, out Int128 unscaled)
    {
        unscaled = 0;
        int i = 0;
        bool negative = false;
        if (i < text.Length && text[i] is (byte)'-' or (byte)'+') negative = text[i++] == (byte)'-';
        int start = i;
        int pointAt = -1;
        int end = i;
        for (; end < text.Length; end++)
        {
            byte c = text[end];
            if (c == (byte)'.' && pointAt < 0) pointAt = end;
            else if ((uint)(c - '0') > 9) break;
        }
        if (end == start || (end == start + 1 && pointAt == start)) return false;
        int exponent = 0;
        if (end < text.Length)
        {
            if (text[end] is not ((byte)'e' or (byte)'E')) return false;
            if (!int.TryParse(text[(end + 1)..], NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out exponent)) return false;
            if (Math.Abs(exponent) > 400) return false;
        }
        // Digits d1..dn with the decimal point after position p: value = digits * 10^(p - n + exponent).
        Int128 digits = 0;
        int count = 0, fractionDigits = 0;
        bool afterPoint = false;
        for (int k = start; k < end; k++)
        {
            if (k == pointAt)
            {
                afterPoint = true;
                continue;
            }
            int d = text[k] - '0';
            if (afterPoint) fractionDigits++;
            if (count == 0 && d == 0)
            {
                continue; // leading zeros
            }
            if (++count > 38) return false;
            digits = digits * 10 + d;
        }
        // Strip trailing zeros of the fraction (they do not need scale).
        int shift = scale - fractionDigits + exponent; // multiply digits by 10^shift
        while (shift < 0 && digits != 0 && digits % 10 == 0)
        {
            digits /= 10;
            shift++;
        }
        if (digits == 0)
        {
            unscaled = 0;
            return true;
        }
        if (shift < 0) return false;
        Int128 limit = Pow10(precision);
        for (int k = 0; k < shift; k++)
        {
            digits *= 10;
            if (digits >= limit) return false;
        }
        if (digits >= limit) return false;
        unscaled = negative ? -digits : digits;
        return true;
    }

    private static Int128 Pow10(int n)
    {
        Int128 r = 1;
        for (int i = 0; i < n; i++) r *= 10;
        return r;
    }
}
