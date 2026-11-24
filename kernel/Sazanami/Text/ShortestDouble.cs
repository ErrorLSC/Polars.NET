using System.Numerics;

namespace Kaminari.Sazanami.Text;

/// <summary>
/// The shortest decimal digits that read back as a double (and, among those, the closest to it, ties to even):
/// Raffaello Giulietti's Schubfach (as in the JDK's DoubleToDecimal, without its two-digit minimum), about twice as fast
/// as Ryū: three 126 × 64-bit products give the rounding interval at once, no digit removed one at a time. Its table
/// of powers of ten is computed once.
/// </summary>
internal static class ShortestDouble
{
    private const int QMin = -1074, KMin = -324, KMax = 292;
    private const ulong CMin = 1UL << 52, Mask63 = (1UL << 63) - 1;

    // g = floor(10^-k · 2^(125 − flog2pow10(−k))) + 1 for k in [KMin, KMax], as its high and low 63 bits.
    private static readonly ulong[] G = Powers();

    private static ulong[] Powers()
    {
        var g = new ulong[2 * (KMax - KMin + 1)];
        for (int k = KMin; k <= KMax; k++)
        {
            int shift = 125 - Flog2Pow10(-k);
            BigInteger num = k <= 0 ? BigInteger.Pow(10, -k) : BigInteger.One, den = k > 0 ? BigInteger.Pow(10, k) : BigInteger.One;
            if (shift >= 0) num <<= shift;
            else den <<= -shift;
            BigInteger value = num / den + 1;
            g[2 * (k - KMin)] = (ulong)(value >> 63);
            g[2 * (k - KMin) + 1] = (ulong)(value & Mask63);
        }
        return g;
    }

    /// <summary>
    /// The shortest digits of a finite, non-zero <paramref name="value"/>'s magnitude: the value is
    /// <paramref name="digits"/> × 10^<paramref name="exponent"/>, with as few digits as any decimal that reads back
    /// (no trailing zeros).
    /// </summary>
    public static void Digits(double value, out ulong digits, out int exponent)
    {
        Raw(value, out digits, out exponent);
        if (digits % 10 != 0) return;
        // Schubfach's digits carry the zeros of a short decimal (574.59: 574590000000000e-12): taken off 8, 4, 2 and
        // 1 at a time, not one division per zero.
        while (digits % 100_000_000 == 0)
        {
            digits /= 100_000_000;
            exponent += 8;
        }
        if (digits % 10_000 == 0)
        {
            digits /= 10_000;
            exponent += 4;
        }
        if (digits % 100 == 0)
        {
            digits /= 100;
            exponent += 2;
        }
        if (digits % 10 == 0)
        {
            digits /= 10;
            exponent++;
        }
    }

    // The digits, trailing zeros perhaps kept.
    private static void Raw(double value, out ulong digits, out int exponent)
    {
        ulong bits = BitConverter.DoubleToUInt64Bits(value);
        ulong fraction = bits & (CMin - 1);
        int biased = (int)(bits >> 52) & 0x7FF;
        if (biased == 0)
        {
            Decimal(QMin, fraction, out digits, out exponent); // subnormal
            return;
        }
        int shift = -QMin + 1 - biased;
        ulong c = CMin | fraction;
        if (shift is > 0 and < 53)
        {
            // A whole number in [1, 2^53): its own digits.
            ulong whole = c >> shift;
            if (whole << shift == c)
            {
                digits = whole;
                exponent = 0;
                return;
            }
        }
        Decimal(-shift, c, out digits, out exponent);
    }

    // c × 2^q: the shortest decimal of its rounding interval (half-way bounds in when c is even), the closest of two.
    private static void Decimal(int q, ulong c, out ulong digits, out int exponent)
    {
        ulong odd = c & 1;
        ulong cb = c << 2, cbr = cb + 2, cbl;
        int k;
        if (c != CMin || q == QMin)
        {
            cbl = cb - 2;
            k = Flog10Pow2(q);
        }
        else
        {
            // A power of two: the interval below is half as wide.
            cbl = cb - 1;
            k = Flog10ThreeQuartersPow2(q);
        }
        int h = q + Flog2Pow10(-k) + 2;
        ulong g1 = G[2 * (k - KMin)], g0 = G[2 * (k - KMin) + 1];
        ulong vb = Rop(g1, g0, cb << h), vbl = Rop(g1, g0, cbl << h), vbr = Rop(g1, g0, cbr << h);
        ulong s = vb >> 2;
        if (s >= 10)
        {
            // One digit fewer: a multiple of ten in the interval (the JDK asks two digits at least, s >= 100).
            ulong sp10 = 10 * Math.BigMul(s, 115_292_150_460_684_698UL << 4, out _);
            ulong tp10 = sp10 + 10;
            bool upin = vbl + odd <= sp10 << 2;
            bool wpin = (tp10 << 2) + odd <= vbr;
            if (upin != wpin)
            {
                digits = upin ? sp10 : tp10;
                exponent = k;
                return;
            }
        }
        ulong t = s + 1;
        bool uin = vbl + odd <= s << 2;
        bool win = (t << 2) + odd <= vbr;
        if (uin != win)
        {
            digits = uin ? s : t;
            exponent = k;
            return;
        }
        long cmp = (long)(vb - ((s + t) << 1));
        digits = cmp < 0 || cmp == 0 && (s & 1) == 0 ? s : t;
        exponent = k;
    }

    // The 126-bit g times cp, shifted right 127 bits, with a sticky low bit when anything was shifted out.
    private static ulong Rop(ulong g1, ulong g0, ulong cp)
    {
        ulong x1 = Math.BigMul(g0, cp, out _);
        ulong y0 = g1 * cp;
        ulong y1 = Math.BigMul(g1, cp, out _);
        ulong z = (y0 >> 1) + x1;
        ulong vbp = y1 + (z >> 63);
        return vbp | ((z & Mask63) + Mask63) >> 63;
    }

    private static int Flog10Pow2(int q) => (int)((q * 661_971_961_083L) >> 41);

    private static int Flog10ThreeQuartersPow2(int q) => (int)((q * 661_971_961_083L - 274_743_187_321L) >> 41);

    private static int Flog2Pow10(int e) => (int)((e * 913_124_641_741L) >> 38);
}
