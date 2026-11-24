using System.Numerics;
using System.Runtime.CompilerServices;

namespace Kaminari.Sazanami.Text;

/// <summary>
/// The shortest decimal digits that read back as a double (and, among those, the closest to it): Ulf Adams' Ryū
/// (PLDI 2018, d2s), a few times faster than .NET's own formatting. Its tables of powers of five are computed once.
/// </summary>
internal static class ShortestDouble
{
    private const int MantissaBits = 52, Bias = 1023, Pow5InvBits = 125, Pow5Bits = 125;
    private const int InvTableSize = 342, TableSize = 326;

    // 125-bit approximations of 5^i (rounded down) and of 2^k / 5^q (rounded up), as (low, high) halves.
    private static readonly (ulong Low, ulong High)[] Pow5 = new (ulong, ulong)[TableSize];
    private static readonly (ulong Low, ulong High)[] Pow5Inv = new (ulong, ulong)[InvTableSize];

    static ShortestDouble()
    {
        BigInteger mask64 = ulong.MaxValue;
        for (int i = 0; i < InvTableSize; i++)
        {
            BigInteger pow = BigInteger.Pow(5, i);
            int length = (int)pow.GetBitLength();
            if (i < TableSize)
            {
                BigInteger split = length >= Pow5Bits ? pow >> (length - Pow5Bits) : pow << (Pow5Bits - length);
                Pow5[i] = ((ulong)(split & mask64), (ulong)(split >> 64));
            }
            BigInteger inverse = (BigInteger.One << (length - 1 + Pow5InvBits)) / pow + 1;
            Pow5Inv[i] = ((ulong)(inverse & mask64), (ulong)(inverse >> 64));
        }
    }

    /// <summary>
    /// The shortest digits of a finite, non-zero <paramref name="value"/>'s magnitude: the value is
    /// <paramref name="digits"/> × 10^<paramref name="exponent"/>, with as few digits as any decimal that reads back.
    /// </summary>
    public static void Digits(double value, out ulong digits, out int exponent)
    {
        ulong bits = BitConverter.DoubleToUInt64Bits(value);
        ulong ieeeMantissa = bits & ((1UL << MantissaBits) - 1);
        uint ieeeExponent = (uint)(bits >> MantissaBits) & 0x7FF;
        // A whole number in [1, 2^53) (Ryū's small-integer path): its own digits, trailing zeros taken off; the main
        // path would take them off one division at a time.
        int fractionBits = Bias + MantissaBits - (int)ieeeExponent;
        if ((uint)fractionBits <= MantissaBits)
        {
            ulong whole = (1UL << MantissaBits) | ieeeMantissa;
            if ((whole & ((1UL << fractionBits) - 1)) == 0)
            {
                whole >>= fractionBits;
                int zeros = 0;
                while (true)
                {
                    ulong q = whole / 10;
                    if (whole - q * 10 != 0) break;
                    whole = q;
                    zeros++;
                }
                digits = whole;
                exponent = zeros;
                return;
            }
        }
        int e2;
        ulong m2;
        if (ieeeExponent == 0)
        {
            e2 = 1 - Bias - MantissaBits - 2;
            m2 = ieeeMantissa;
        }
        else
        {
            e2 = (int)ieeeExponent - Bias - MantissaBits - 2;
            m2 = (1UL << MantissaBits) | ieeeMantissa;
        }
        bool even = (m2 & 1) == 0, acceptBounds = even;

        // The interval of decimal representations that round to the value, in a power of ten.
        ulong mv = 4 * m2;
        uint mmShift = ieeeMantissa != 0 || ieeeExponent <= 1 ? 1u : 0u;
        ulong vr, vp, vm;
        int e10;
        bool vmIsTrailingZeros = false, vrIsTrailingZeros = false;
        if (e2 >= 0)
        {
            int q = Log10Pow2(e2) - (e2 > 3 ? 1 : 0);
            e10 = q;
            int k = Pow5InvBits + Pow5BitLength(q) - 1;
            int i = -e2 + q + k;
            vr = MulShiftAll(m2, Pow5Inv[q], i, out vp, out vm, mmShift);
            if (q <= 21)
            {
                if (mv % 5 == 0) vrIsTrailingZeros = MultipleOfPowerOf5(mv, q);
                else if (acceptBounds) vmIsTrailingZeros = MultipleOfPowerOf5(mv - 1 - mmShift, q);
                else vp -= MultipleOfPowerOf5(mv + 2, q) ? 1UL : 0;
            }
        }
        else
        {
            int q = Log10Pow5(-e2) - (-e2 > 1 ? 1 : 0);
            e10 = q + e2;
            int i = -e2 - q;
            int k = Pow5BitLength(i) - Pow5Bits;
            int j = q - k;
            vr = MulShiftAll(m2, Pow5[i], j, out vp, out vm, mmShift);
            if (q <= 1)
            {
                vrIsTrailingZeros = true;
                if (acceptBounds) vmIsTrailingZeros = mmShift == 1;
                else vp--;
            }
            else if (q < 63) vrIsTrailingZeros = (mv & ((1UL << q) - 1)) == 0;
        }

        // The shortest representation in the interval.
        int removed = 0;
        int lastRemovedDigit = 0;
        ulong output;
        if (vmIsTrailingZeros || vrIsTrailingZeros)
        {
            while (true)
            {
                ulong vpDiv10 = vp / 10, vmDiv10 = vm / 10;
                if (vpDiv10 <= vmDiv10) break;
                uint vmMod10 = (uint)(vm - 10 * vmDiv10);
                ulong vrDiv10 = vr / 10;
                uint vrMod10 = (uint)(vr - 10 * vrDiv10);
                vmIsTrailingZeros &= vmMod10 == 0;
                vrIsTrailingZeros &= lastRemovedDigit == 0;
                lastRemovedDigit = (int)vrMod10;
                vr = vrDiv10;
                vp = vpDiv10;
                vm = vmDiv10;
                removed++;
            }
            if (vmIsTrailingZeros)
            {
                while (true)
                {
                    ulong vmDiv10 = vm / 10;
                    uint vmMod10 = (uint)(vm - 10 * vmDiv10);
                    if (vmMod10 != 0) break;
                    ulong vpDiv10 = vp / 10, vrDiv10 = vr / 10;
                    uint vrMod10 = (uint)(vr - 10 * vrDiv10);
                    vrIsTrailingZeros &= lastRemovedDigit == 0;
                    lastRemovedDigit = (int)vrMod10;
                    vr = vrDiv10;
                    vp = vpDiv10;
                    vm = vmDiv10;
                    removed++;
                }
            }
            // Round half to even when the exact number ends .....50..0.
            if (vrIsTrailingZeros && lastRemovedDigit == 5 && vr % 2 == 0) lastRemovedDigit = 4;
            output = vr + ((vr == vm && (!acceptBounds || !vmIsTrailingZeros)) || lastRemovedDigit >= 5 ? 1UL : 0);
        }
        else
        {
            // The common case: digits removed 8, 4, 2 and 1 at a time while the interval still holds a shorter
            // number; rounding looks at the digits removed last (the most significant of them), all at once.
            bool roundUp = false;
            Remove(100_000_000, 8, ref vr, ref vp, ref vm, ref removed, ref roundUp);
            Remove(10_000, 4, ref vr, ref vp, ref vm, ref removed, ref roundUp);
            Remove(100, 2, ref vr, ref vp, ref vm, ref removed, ref roundUp);
            Remove(10, 1, ref vr, ref vp, ref vm, ref removed, ref roundUp);
            output = vr + (vr == vm || roundUp ? 1UL : 0);
        }
        digits = output;
        exponent = e10 + removed;
    }

    // Removes digits (pow = 10^count at a time) while the interval holds a number with fewer.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Remove(ulong pow, int count, ref ulong vr, ref ulong vp, ref ulong vm, ref int removed, ref bool roundUp)
    {
        while (true)
        {
            ulong vpDiv = vp / pow, vmDiv = vm / pow;
            if (vpDiv <= vmDiv) return;
            ulong vrDiv = vr / pow;
            roundUp = vr - pow * vrDiv >= pow / 2;
            vr = vrDiv;
            vp = vpDiv;
            vm = vmDiv;
            removed += count;
        }
    }

    private static int Pow5BitLength(int e) => (int)(((uint)e * 1217359) >> 19) + 1;

    private static int Log10Pow2(int e) => (int)(((uint)e * 78913) >> 18);

    private static int Log10Pow5(int e) => (int)(((uint)e * 732923) >> 20);

    private static bool MultipleOfPowerOf5(ulong value, int p)
    {
        int count = 0;
        while (value % 5 == 0 && value != 0)
        {
            value /= 5;
            count++;
        }
        return count >= p;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong MulShift(ulong m, (ulong Low, ulong High) mul, int j)
    {
        // ((m × mul) >> j), the product 192 bits wide; j ≥ 64.
        ulong b0High = Math.BigMul(m, mul.Low, out _);
        ulong b2High = Math.BigMul(m, mul.High, out ulong b2Low);
        ulong sumLow = b2Low + b0High;
        ulong sumHigh = b2High + (sumLow < b2Low ? 1UL : 0);
        int shift = j - 64;
        return shift == 0 ? sumLow : (sumLow >> shift) | (sumHigh << (64 - shift));
    }

    private static ulong MulShiftAll(ulong m, (ulong Low, ulong High) mul, int j, out ulong vp, out ulong vm, uint mmShift)
    {
        vp = MulShift(4 * m + 2, mul, j);
        vm = MulShift(4 * m - 1 - mmShift, mul, j);
        return MulShift(4 * m, mul, j);
    }
}
