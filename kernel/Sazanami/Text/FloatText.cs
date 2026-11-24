using System.Globalization;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace Kaminari.Sazanami.Text;

/// <summary>
/// Floating-point numbers as text, as the reference dataframe engine writes them (JSON, CSV): the shortest round-trip
/// digits laid out by the value's width, in scientific or decimal notation on request, or rounded to a number of
/// decimals (the exact binary value rounded half to even). A half float is written as the float of its value. When two
/// shortest digit strings are equally near (the value exactly halfway), the default layout takes the even one and the
/// scientific and decimal notations the greater (the engine's two formatters differ there: 464749.125f is
/// 464749.12 and 4.6474913e5).
/// </summary>
internal static class FloatText
{
    /// <summary>The most bytes <see cref="Shortest{T}"/> and <see cref="Scientific{T}"/> write.</summary>
    public const int MaxShortestBytes = 48;

    /// <summary>The most bytes <see cref="Positional{T}"/> writes (a double's smallest value in decimal notation).</summary>
    public const int MaxPositionalBytes = 350;

    /// <summary>The most bytes <see cref="Fixed"/> writes with <paramref name="decimals"/> decimals.</summary>
    public static int MaxFixedBytes(int decimals) => 315 + decimals;

    /// <summary>
    /// A floating-point number (finite): its shortest round-trip digits, in decimal notation when its exponent is from
    /// −5 to 15 for a double, from −6 to 12 for a float (a whole number ends <c>.0</c>), else as <c>d.ddde±x</c>.
    /// </summary>
    public static int Shortest<T>(T value, Span<byte> destination) where T : IFloatingPointIeee754<T>, IUtf8SpanFormattable
    {
        if (typeof(T) == typeof(Half)) return Shortest((float)Unsafe.As<T, Half>(ref value), destination);
        if (T.IsZero(value)) return Copy(T.IsNegative(value) ? "-0.0"u8 : "0.0"u8, destination);
        if (typeof(T) == typeof(double)) return ShortestOf(Unsafe.As<T, double>(ref value), destination);
        Span<byte> digits = stackalloc byte[32];
        int count = Digits(value, digits, out int exponent);
        (int low, int high) = typeof(T) == typeof(double) ? (-5, 15) : (-6, 12);
        int at = 0;
        if (T.IsNegative(value)) destination[at++] = (byte)'-';
        if (exponent < low || exponent > high) return at + Exponential(digits[..count], exponent, signed: true, destination[at..]);
        at += Decimal(digits[..count], exponent, destination[at..]);
        if (exponent >= count - 1)
        {
            destination[at++] = (byte)'.';
            destination[at++] = (byte)'0';
        }
        return at;
    }

    // Shortest for a double (finite, not zero): its digits formatted once, one place right of where they start, then
    // laid out around them in place (a few bytes moved by hand: no copy calls, no buffer between).
    private static int ShortestOf(double value, Span<byte> destination)
    {
        int at = 0;
        if (double.IsNegative(value)) destination[at++] = (byte)'-';
        ShortestDouble.Digits(double.Abs(value), out ulong digits, out int e10); // no trailing zeros
        digits.TryFormat(destination[(at + 1)..], out int count, default, CultureInfo.InvariantCulture);
        int exponent = e10 + count - 1;
        if (exponent is < -5 or > 15)
        {
            // d.ddde±x: the first digit back one place, the point after it.
            destination[at] = destination[at + 1];
            if (count > 1)
            {
                destination[at + 1] = (byte)'.';
                at += count + 1;
            }
            else at++;
            destination[at++] = (byte)'e';
            destination[at++] = exponent < 0 ? (byte)'-' : (byte)'+';
            Math.Abs(exponent).TryFormat(destination[at..], out int written, default, CultureInfo.InvariantCulture);
            return at + written;
        }
        if (exponent >= count - 1)
        {
            // A whole number: the digits back one place, its zeros, ".0".
            for (int i = 0; i < count; i++) destination[at + i] = destination[at + 1 + i];
            at += count;
            for (int i = count - 1; i < exponent; i++) destination[at++] = (byte)'0';
            destination[at++] = (byte)'.';
            destination[at++] = (byte)'0';
            return at;
        }
        if (exponent >= 0)
        {
            // ddd.fff: the whole digits back one place, the point after them.
            for (int i = 0; i <= exponent; i++) destination[at + i] = destination[at + 1 + i];
            destination[at + exponent + 1] = (byte)'.';
            return at + count + 1;
        }
        // 0.000ddd: the digits moved right past "0." and the zeros (from the last).
        int lead = 1 - exponent;
        for (int i = count - 1; i >= 0; i--) destination[at + lead + i] = destination[at + 1 + i];
        destination[at] = (byte)'0';
        destination[at + 1] = (byte)'.';
        for (int i = 2; i < lead; i++) destination[at + i] = (byte)'0';
        return at + lead + count;
    }

    /// <summary>A finite number's shortest digits in scientific notation: <c>d.ddde-x</c>, no sign on a positive exponent (<c>0e0</c>).</summary>
    public static int Scientific<T>(T value, Span<byte> destination) where T : IFloatingPointIeee754<T>, IUtf8SpanFormattable
    {
        if (typeof(T) == typeof(Half)) return Scientific((float)Unsafe.As<T, Half>(ref value), destination);
        if (T.IsZero(value)) return Copy(T.IsNegative(value) ? "-0e0"u8 : "0e0"u8, destination);
        Span<byte> digits = stackalloc byte[32];
        int count = DigitsTiesUp(value, digits, out int exponent);
        int at = 0;
        if (T.IsNegative(value)) destination[at++] = (byte)'-';
        return at + Exponential(digits[..count], exponent, signed: false, destination[at..]);
    }

    /// <summary>A finite number's shortest digits in decimal notation, every digit written out (a whole number with no point: <c>0</c>).</summary>
    public static int Positional<T>(T value, Span<byte> destination) where T : IFloatingPointIeee754<T>, IUtf8SpanFormattable
    {
        if (typeof(T) == typeof(Half)) return Positional((float)Unsafe.As<T, Half>(ref value), destination);
        if (T.IsZero(value)) return Copy(T.IsNegative(value) ? "-0"u8 : "0"u8, destination);
        Span<byte> digits = stackalloc byte[32];
        int count = DigitsTiesUp(value, digits, out int exponent);
        int at = 0;
        if (T.IsNegative(value)) destination[at++] = (byte)'-';
        return at + Decimal(digits[..count], exponent, destination[at..]);
    }

    /// <summary>
    /// A finite number's exact value rounded half to even to <paramref name="decimals"/> decimals, in decimal notation
    /// (a negative value keeps its sign when it rounds to zero: <c>-0.00</c>).
    /// </summary>
    public static int Fixed(double value, int decimals, Span<byte> destination)
    {
        int at = 0;
        if (double.IsNegative(value)) destination[at++] = (byte)'-';
        // The digits written in place (no buffer between; the room is MaxFixedBytes long), then moved apart for the point.
        Span<byte> digits = destination[at..Math.Min(destination.Length, at + MaxFixedBytes(decimals))];
        int count = Rounded(double.Abs(value), decimals, digits);
        if (count <= decimals)
        {
            // Fewer digits than decimals: zeros before them.
            digits[..count].CopyTo(digits[(decimals + 1 - count)..]);
            digits[..(decimals + 1 - count)].Fill((byte)'0');
            count = decimals + 1;
        }
        if (decimals == 0) return at + count;
        // ddd.ff: the fraction one place right, the point before it.
        int whole = count - decimals;
        digits.Slice(whole, decimals).CopyTo(digits[(whole + 1)..]);
        digits[whole] = (byte)'.';
        return at + count + 1;
    }

    /// <summary>
    /// A finite number's exact value rounded half to even to <paramref name="decimals"/> decimals of its first digit, in
    /// scientific notation (<c>1.23e3</c>, <c>0.00e0</c>).
    /// </summary>
    public static int FixedScientific(double value, int decimals, Span<byte> destination)
    {
        int at = 0;
        if (double.IsNegative(value)) destination[at++] = (byte)'-';
        double abs = double.Abs(value);
        Span<byte> digits = stackalloc byte[decimals + 24];
        int exponent = 0, count;
        if (abs == 0)
        {
            digits[..(decimals + 1)].Fill((byte)'0');
            count = decimals + 1;
        }
        else
        {
            Span<byte> shortest = stackalloc byte[32];
            int shortestCount = Digits(abs, shortest, out exponent);
            // The exact value's first digit: the shortest digits' unless they are a power of ten the value is just under
            // (9.98e-322 written 1e-321).
            if (shortestCount == 1 && shortest[0] == '1' && Compare(abs, 1, exponent) < 0) exponent--;
            count = Rounded(abs, decimals - exponent, digits);
            if (count > decimals + 1) // rounded up to a power of ten (9.995 → 1.00e1)
            {
                exponent++;
                count = decimals + 1;
            }
        }
        destination[at++] = digits[0];
        if (decimals > 0)
        {
            destination[at++] = (byte)'.';
            digits[1..count].CopyTo(destination[at..]);
            at += decimals;
        }
        destination[at++] = (byte)'e';
        exponent.TryFormat(destination[at..], out int written, default, CultureInfo.InvariantCulture);
        return at + written;
    }

    /// <summary>
    /// The shortest round-trip digits of a finite, non-zero value's magnitude (no trailing zeros): their count, and the
    /// exponent of the first (d₀.d₁d₂… × 10^exponent).
    /// </summary>
    public static int Digits<T>(T value, Span<byte> digits, out int exponent) where T : IFloatingPointIeee754<T>, IUtf8SpanFormattable
    {
        int count;
        if (typeof(T) == typeof(double))
        {
            ShortestDouble.Digits(double.Abs(Unsafe.As<T, double>(ref value)), out ulong mantissa, out int e10);
            mantissa.TryFormat(digits, out count, default, CultureInfo.InvariantCulture);
            exponent = e10 + count - 1;
        }
        else (count, exponent) = NetDigits(T.Abs(value), digits);
        return digits[..count].TrimEnd((byte)'0').Length;
    }

    // The shortest digits with an exact halfway value taken up (464749.125f: 46474913, not 46474912).
    private static int DigitsTiesUp<T>(T value, Span<byte> digits, out int exponent) where T : IFloatingPointIeee754<T>, IUtf8SpanFormattable
    {
        int count = Digits(value, digits, out exponent);
        if (count == 0 || digits[count - 1] % 2 != 0) return count; // the even rule took an odd digit: no tie
        // A tie: the value exactly (digits + ½) × 10^(exponent − count + 1), so abs × 2 × 10^(count − 1 − exponent) an odd
        // whole number. With abs = m × 2^e (e < 0), that needs the powers of two of m × 2 × 10^q to cover 2^−e: most
        // values fail this first, without big numbers.
        double abs = double.CreateChecked(T.Abs(value));
        ulong bits = BitConverter.DoubleToUInt64Bits(abs);
        int e = Math.Max((int)(bits >> 52), 1) - 1075, q = count - 1 - exponent;
        ulong m = (bits & ((1UL << 52) - 1)) | (bits >> 52 == 0 ? 0 : 1UL << 52);
        if (e < 0 && q >= 0 && BitOperations.TrailingZeroCount(m) + 1 + q < -e) return count;
        ulong whole = 0;
        for (int i = 0; i < count; i++) whole = whole * 10 + (ulong)(digits[i] - '0');
        if (Compare(abs, (2 * (BigInteger)whole + 1) * 5, exponent - count) != 0) return count;
        // The digits plus one in the last place (a carry may make them a power of ten).
        int k = count - 1;
        while (k >= 0 && digits[k] == '9') digits[k--] = (byte)'0';
        if (k >= 0)
        {
            digits[k]++;
            return digits[..count].TrimEnd((byte)'0').Length;
        }
        digits[0] = (byte)'1';
        exponent++;
        return 1;
    }

    // The sign of abs − n × 10^power, exactly (abs finite, not negative).
    private static int Compare(double abs, BigInteger n, int power)
    {
        ulong bits = BitConverter.DoubleToUInt64Bits(abs);
        int biased = (int)(bits >> 52);
        ulong mantissa = bits & ((1UL << 52) - 1);
        if (biased == 0) biased = 1;
        else mantissa |= 1UL << 52;
        int e = biased - 1075; // abs = mantissa × 2^e
        BigInteger left = mantissa, right = n;
        if (e >= 0) left <<= e;
        else right <<= -e;
        if (power >= 0) right *= BigInteger.Pow(10, power);
        else left *= BigInteger.Pow(10, -power);
        return left.CompareTo(right);
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

    // Significant digits d₀d₁… × 10^exponent in decimal notation, every digit written out (no point after a whole number).
    private static int Decimal(scoped ReadOnlySpan<byte> significant, int exponent, Span<byte> room)
    {
        int at = 0;
        if (exponent >= 0)
        {
            for (int i = 0; i <= exponent; i++) room[at++] = i < significant.Length ? significant[i] : (byte)'0';
            if (significant.Length > exponent + 1)
            {
                room[at++] = (byte)'.';
                significant[(exponent + 1)..].CopyTo(room[at..]);
                at += significant.Length - exponent - 1;
            }
            return at;
        }
        room[at++] = (byte)'0';
        room[at++] = (byte)'.';
        for (int i = 0; i < -exponent - 1; i++) room[at++] = (byte)'0';
        significant.CopyTo(room[at..]);
        return at + significant.Length;
    }

    // Significant digits as d.ddde±x (`signed`: '+' on a positive exponent).
    private static int Exponential(scoped ReadOnlySpan<byte> significant, int exponent, bool signed, Span<byte> room)
    {
        int at = 0;
        room[at++] = significant[0];
        if (significant.Length > 1)
        {
            room[at++] = (byte)'.';
            significant[1..].CopyTo(room[at..]);
            at += significant.Length - 1;
        }
        room[at++] = (byte)'e';
        if (exponent < 0) room[at++] = (byte)'-';
        else if (signed) room[at++] = (byte)'+';
        Math.Abs(exponent).TryFormat(room[at..], out int written, default, CultureInfo.InvariantCulture);
        return at + written;
    }

    // The digits of round_half_even(abs × 10^scale) (abs finite, not negative): their count ("0" for zero).
    private static int Rounded(double abs, int scale, Span<byte> digits)
    {
        ulong bits = BitConverter.DoubleToUInt64Bits(abs);
        int biased = (int)(bits >> 52);
        ulong mantissa = bits & ((1UL << 52) - 1);
        if (biased == 0) biased = 1; // subnormal
        else mantissa |= 1UL << 52;
        int e = biased - 1075; // abs = mantissa × 2^e
        int written;
        // Fast path: mantissa × 10^scale fits 128 bits and is shifted right (the common case: decimals of a value under 2^53).
        if (mantissa == 0) return Copy("0"u8, digits);
        if (scale is >= 0 and <= 19 && e is < 0 and > -128)
        {
            UInt128 scaled = (UInt128)mantissa * Pow10(scale);
            int shift = -e;
            UInt128 quotient = scaled >> shift, remainder = scaled - (quotient << shift), half = (UInt128)1 << (shift - 1);
            if (remainder > half || remainder == half && (quotient & 1) == 1) quotient++;
            quotient.TryFormat(digits, out written, default, CultureInfo.InvariantCulture);
            return written;
        }
        BigInteger numerator = mantissa, denominator = BigInteger.One;
        if (e >= 0) numerator <<= e;
        else denominator <<= -e;
        if (scale >= 0) numerator *= BigInteger.Pow(10, scale);
        else denominator *= BigInteger.Pow(10, -scale);
        BigInteger q = BigInteger.DivRem(numerator, denominator, out BigInteger r);
        int compare = (r << 1).CompareTo(denominator);
        if (compare > 0 || compare == 0 && !q.IsEven) q += 1;
        // BigInteger writes UTF-16 only (on .NET 8): digits narrowed.
        Span<char> text = digits.Length <= 1024 ? stackalloc char[digits.Length] : new char[digits.Length];
        q.TryFormat(text, out written, default, CultureInfo.InvariantCulture);
        for (int i = 0; i < written; i++) digits[i] = (byte)text[i];
        return written;
    }

    private static UInt128 Pow10(int n)
    {
        UInt128 p = 1;
        for (int i = 0; i < n; i++) p *= 10;
        return p;
    }

    private static int Copy(ReadOnlySpan<byte> text, Span<byte> destination)
    {
        text.CopyTo(destination);
        return text.Length;
    }
}
