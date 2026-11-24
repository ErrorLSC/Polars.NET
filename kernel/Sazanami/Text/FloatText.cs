using System.Globalization;
using System.Numerics;

namespace Kaminari.Sazanami.Text;

/// <summary>Floating-point numbers as text, as the reference dataframe engine writes them (JSON, CSV).</summary>
internal static class FloatText
{
    /// <summary>The most bytes <see cref="Shortest{T}"/> writes.</summary>
    public const int MaxShortestBytes = 48;

    /// <summary>
    /// A floating-point number (finite): its shortest round-trip digits, in decimal notation when its exponent is from
    /// −5 to 15 for a double, from −6 to 12 for a float (a whole number ends <c>.0</c>), else as <c>d.ddde±x</c>. A half
    /// float is written as the float of its value (its digits are a float's).
    /// </summary>
    public static int Shortest<T>(T value, Span<byte> destination) where T : IFloatingPointIeee754<T>, IUtf8SpanFormattable
    {
        if (typeof(T) == typeof(Half)) return Shortest((float)System.Runtime.CompilerServices.Unsafe.As<T, Half>(ref value), destination);
        Span<byte> digits = stackalloc byte[32];
        bool negative = T.IsNegative(value);
        if (T.IsZero(value))
        {
            ReadOnlySpan<byte> zero = negative ? "-0.0"u8 : "0.0"u8;
            zero.CopyTo(destination);
            return zero.Length;
        }
        int count, exponent;
        if (typeof(T) == typeof(double))
        {
            ShortestDouble.Digits(double.Abs(System.Runtime.CompilerServices.Unsafe.As<T, double>(ref value)), out ulong mantissa, out int e10);
            mantissa.TryFormat(digits, out count, default, CultureInfo.InvariantCulture);
            exponent = e10 + count - 1;
        }
        else (count, exponent) = NetDigits(T.Abs(value), digits);
        (int low, int high) = typeof(T) == typeof(double) ? (-5, 15) : (-6, 12);
        return Layout(destination, negative, digits[..count].TrimEnd((byte)'0'), exponent, low, high);
    }

    // The shortest digits of a positive value as .NET writes them ("1.5E-06", "123", "0.0001"): its significant digits
    // and the exponent of the first.
    private static (int Count, int Exponent) NetDigits<T>(T value, Span<byte> digits) where T : IUtf8SpanFormattable
    {
        Span<byte> text = stackalloc byte[48];
        if (!value.TryFormat(text, out int length, default, CultureInfo.InvariantCulture)) throw new InvalidOperationException("A number's text does not fit.");
        text = text[..length];
        int e = text.IndexOfAny((byte)'E', (byte)'e');
        ReadOnlySpan<byte> mantissa = e < 0 ? text : text[..e];
        int point = mantissa.IndexOf((byte)'.');
        int integerDigits = point < 0 ? mantissa.Length : point, count = 0;
        foreach (byte b in mantissa)
            if (b != '.') digits[count++] = b;
        int lead = 0;
        while (lead < count - 1 && digits[lead] == '0') lead++;
        digits[lead..count].CopyTo(digits);
        int exponent = integerDigits - 1 - lead;
        if (e >= 0) exponent += int.Parse(text[(e + 1)..], NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture);
        return (count - lead, exponent);
    }

    // Significant digits (no trailing zeros) d₀d₁… × 10^exponent laid out.
    private static int Layout(Span<byte> room, bool negative, scoped ReadOnlySpan<byte> significant, int exponent, int low, int high)
    {
        int at = 0;
        if (negative) room[at++] = (byte)'-';
        if (exponent >= low && exponent <= high)
        {
            if (exponent >= 0)
            {
                for (int i = 0; i <= exponent; i++) room[at++] = i < significant.Length ? significant[i] : (byte)'0';
                room[at++] = (byte)'.';
                if (significant.Length > exponent + 1)
                    foreach (byte b in significant[(exponent + 1)..]) room[at++] = b;
                else room[at++] = (byte)'0';
            }
            else
            {
                room[at++] = (byte)'0';
                room[at++] = (byte)'.';
                for (int i = 0; i < -exponent - 1; i++) room[at++] = (byte)'0';
                foreach (byte b in significant) room[at++] = b;
            }
        }
        else
        {
            room[at++] = significant[0];
            if (significant.Length > 1)
            {
                room[at++] = (byte)'.';
                foreach (byte b in significant[1..]) room[at++] = b;
            }
            room[at++] = (byte)'e';
            room[at++] = exponent < 0 ? (byte)'-' : (byte)'+';
            Math.Abs(exponent).TryFormat(room[at..], out int written, default, CultureInfo.InvariantCulture);
            at += written;
        }
        return at;
    }
}
