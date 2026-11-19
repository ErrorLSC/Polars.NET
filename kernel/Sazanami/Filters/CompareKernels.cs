using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Kaminari.Sazanami.Expressions;

namespace Kaminari.Sazanami.Filters;

/// <summary>A test of a value against a scalar, on single values and on vectors of them.</summary>
internal interface IValueTest<T> where T : unmanaged, INumber<T>
{
    static abstract bool Test(T value, T scalar);
    static abstract Vector128<T> Test(Vector128<T> values, Vector128<T> scalar);
    static abstract Vector256<T> Test(Vector256<T> values, Vector256<T> scalar);
}

/// <summary>
/// The value tests of comparisons in the IR's order: plain comparisons for integers, and for floating values the
/// IEEE comparison put right for NaN (equal to NaN, greater than every number); -0 equals 0 either way.
/// </summary>
internal static class ValueTests
{
    public readonly struct Eq<T> : IValueTest<T> where T : unmanaged, INumber<T>
    {
        public static bool Test(T value, T scalar) => value == scalar;
        public static Vector128<T> Test(Vector128<T> values, Vector128<T> scalar) => Vector128.Equals(values, scalar);
        public static Vector256<T> Test(Vector256<T> values, Vector256<T> scalar) => Vector256.Equals(values, scalar);
    }

    /// <summary>Not equal (a NaN value is not equal to a number).</summary>
    public readonly struct NotEq<T> : IValueTest<T> where T : unmanaged, INumber<T>
    {
        public static bool Test(T value, T scalar) => !(value == scalar);
        public static Vector128<T> Test(Vector128<T> values, Vector128<T> scalar) => ~Vector128.Equals(values, scalar);
        public static Vector256<T> Test(Vector256<T> values, Vector256<T> scalar) => ~Vector256.Equals(values, scalar);
    }

    public readonly struct Lt<T> : IValueTest<T> where T : unmanaged, INumber<T>
    {
        public static bool Test(T value, T scalar) => value < scalar;
        public static Vector128<T> Test(Vector128<T> values, Vector128<T> scalar) => Vector128.LessThan(values, scalar);
        public static Vector256<T> Test(Vector256<T> values, Vector256<T> scalar) => Vector256.LessThan(values, scalar);
    }

    public readonly struct LtEq<T> : IValueTest<T> where T : unmanaged, INumber<T>
    {
        public static bool Test(T value, T scalar) => value <= scalar;
        public static Vector128<T> Test(Vector128<T> values, Vector128<T> scalar) => Vector128.LessThanOrEqual(values, scalar);
        public static Vector256<T> Test(Vector256<T> values, Vector256<T> scalar) => Vector256.LessThanOrEqual(values, scalar);
    }

    public readonly struct Gt<T> : IValueTest<T> where T : unmanaged, INumber<T>
    {
        public static bool Test(T value, T scalar) => value > scalar;
        public static Vector128<T> Test(Vector128<T> values, Vector128<T> scalar) => Vector128.GreaterThan(values, scalar);
        public static Vector256<T> Test(Vector256<T> values, Vector256<T> scalar) => Vector256.GreaterThan(values, scalar);
    }

    public readonly struct GtEq<T> : IValueTest<T> where T : unmanaged, INumber<T>
    {
        public static bool Test(T value, T scalar) => value >= scalar;
        public static Vector128<T> Test(Vector128<T> values, Vector128<T> scalar) => Vector128.GreaterThanOrEqual(values, scalar);
        public static Vector256<T> Test(Vector256<T> values, Vector256<T> scalar) => Vector256.GreaterThanOrEqual(values, scalar);
    }

    /// <summary>Greater than a number, NaN included (NaN is greater than every number).</summary>
    public readonly struct GtOrNaN<T> : IValueTest<T> where T : unmanaged, INumber<T>
    {
        public static bool Test(T value, T scalar) => value > scalar || T.IsNaN(value);
        public static Vector128<T> Test(Vector128<T> values, Vector128<T> scalar) => Vector128.GreaterThan(values, scalar) | ~Vector128.Equals(values, values);
        public static Vector256<T> Test(Vector256<T> values, Vector256<T> scalar) => Vector256.GreaterThan(values, scalar) | ~Vector256.Equals(values, values);
    }

    /// <summary>Greater than or equal to a number, NaN included.</summary>
    public readonly struct GtEqOrNaN<T> : IValueTest<T> where T : unmanaged, INumber<T>
    {
        public static bool Test(T value, T scalar) => value >= scalar || T.IsNaN(value);
        public static Vector128<T> Test(Vector128<T> values, Vector128<T> scalar) => Vector128.GreaterThanOrEqual(values, scalar) | ~Vector128.Equals(values, values);
        public static Vector256<T> Test(Vector256<T> values, Vector256<T> scalar) => Vector256.GreaterThanOrEqual(values, scalar) | ~Vector256.Equals(values, values);
    }

    /// <summary>The value is NaN (the scalar is not used).</summary>
    public readonly struct IsNaN<T> : IValueTest<T> where T : unmanaged, INumber<T>
    {
        public static bool Test(T value, T scalar) => T.IsNaN(value);
        public static Vector128<T> Test(Vector128<T> values, Vector128<T> scalar) => ~Vector128.Equals(values, values);
        public static Vector256<T> Test(Vector256<T> values, Vector256<T> scalar) => ~Vector256.Equals(values, values);
    }

    // The IR's total order of floating values between two computed values (either may be NaN): NaN equals NaN and is
    // greater than every number. NaN(x) is ~Equals(x, x).

    /// <summary>Equal in the total order (NaN equals NaN).</summary>
    public readonly struct TotalEq<T> : IValueTest<T> where T : unmanaged, INumber<T>
    {
        public static bool Test(T value, T other) => value == other || T.IsNaN(value) && T.IsNaN(other);
        public static Vector128<T> Test(Vector128<T> values, Vector128<T> other) =>
            Vector128.Equals(values, other) | ~Vector128.Equals(values, values) & ~Vector128.Equals(other, other);
        public static Vector256<T> Test(Vector256<T> values, Vector256<T> other) =>
            Vector256.Equals(values, other) | ~Vector256.Equals(values, values) & ~Vector256.Equals(other, other);
    }

    public readonly struct TotalNotEq<T> : IValueTest<T> where T : unmanaged, INumber<T>
    {
        public static bool Test(T value, T other) => !TotalEq<T>.Test(value, other);
        public static Vector128<T> Test(Vector128<T> values, Vector128<T> other) => ~TotalEq<T>.Test(values, other);
        public static Vector256<T> Test(Vector256<T> values, Vector256<T> other) => ~TotalEq<T>.Test(values, other);
    }

    /// <summary>Less in the total order: a number below a number, or a number against NaN.</summary>
    public readonly struct TotalLt<T> : IValueTest<T> where T : unmanaged, INumber<T>
    {
        public static bool Test(T value, T other) => value < other || !T.IsNaN(value) && T.IsNaN(other);
        public static Vector128<T> Test(Vector128<T> values, Vector128<T> other) =>
            Vector128.LessThan(values, other) | Vector128.Equals(values, values) & ~Vector128.Equals(other, other);
        public static Vector256<T> Test(Vector256<T> values, Vector256<T> other) =>
            Vector256.LessThan(values, other) | Vector256.Equals(values, values) & ~Vector256.Equals(other, other);
    }

    /// <summary>Less or equal in the total order: anything against NaN.</summary>
    public readonly struct TotalLtEq<T> : IValueTest<T> where T : unmanaged, INumber<T>
    {
        public static bool Test(T value, T other) => value <= other || T.IsNaN(other);
        public static Vector128<T> Test(Vector128<T> values, Vector128<T> other) => Vector128.LessThanOrEqual(values, other) | ~Vector128.Equals(other, other);
        public static Vector256<T> Test(Vector256<T> values, Vector256<T> other) => Vector256.LessThanOrEqual(values, other) | ~Vector256.Equals(other, other);
    }

    /// <summary>Greater in the total order: a number above a number, or NaN against a number.</summary>
    public readonly struct TotalGt<T> : IValueTest<T> where T : unmanaged, INumber<T>
    {
        public static bool Test(T value, T other) => TotalLt<T>.Test(other, value);
        public static Vector128<T> Test(Vector128<T> values, Vector128<T> other) => TotalLt<T>.Test(other, values);
        public static Vector256<T> Test(Vector256<T> values, Vector256<T> other) => TotalLt<T>.Test(other, values);
    }

    /// <summary>Greater or equal in the total order: NaN against anything.</summary>
    public readonly struct TotalGtEq<T> : IValueTest<T> where T : unmanaged, INumber<T>
    {
        public static bool Test(T value, T other) => TotalLtEq<T>.Test(other, value);
        public static Vector128<T> Test(Vector128<T> values, Vector128<T> other) => TotalLtEq<T>.Test(other, values);
        public static Vector256<T> Test(Vector256<T> values, Vector256<T> other) => TotalLtEq<T>.Test(other, values);
    }

    /// <summary>The value is a number (the scalar is not used).</summary>
    public readonly struct NotNaN<T> : IValueTest<T> where T : unmanaged, INumber<T>
    {
        public static bool Test(T value, T scalar) => !T.IsNaN(value);
        public static Vector128<T> Test(Vector128<T> values, Vector128<T> scalar) => Vector128.Equals(values, values);
        public static Vector256<T> Test(Vector256<T> values, Vector256<T> scalar) => Vector256.Equals(values, values);
    }
}

/// <summary>
/// Column-against-scalar kernels: a test per value, written as a row mask, 64 values per word. Vectorized with the
/// portable 256- and 128-bit APIs where the hardware has them and the type is a vector element; a scalar loop
/// otherwise (and for the values after the last whole word).
/// </summary>
internal static class CompareKernels
{
    /// <summary>
    /// Sets bit <c>i</c> of <paramref name="mask"/> where <typeparamref name="TTest"/> holds for
    /// <paramref name="values"/>[i], for every value (nulls included: their slots hold some value), and clears the bits
    /// past the last value.
    /// </summary>
    public static void Run<T, TTest>(ReadOnlySpan<T> values, T scalar, Span<ulong> mask)
        where T : unmanaged, INumber<T>
        where TTest : struct, IValueTest<T>
    {
        int length = values.Length, words = Masks.Words(length);
        if (mask.Length < words) throw new ArgumentException("The mask is shorter than the values.", nameof(mask));
        ref T v = ref MemoryMarshal.GetReference(values);
        int full = length >> 6, w = 0;
        if (Vector256.IsHardwareAccelerated && Vector256<T>.IsSupported) w = Words256<T, TTest>(ref v, full, scalar, mask);
        else if (Vector128.IsHardwareAccelerated && Vector128<T>.IsSupported) w = Words128<T, TTest>(ref v, full, scalar, mask);
        for (; w < words; w++)
        {
            int first = w << 6, n = Math.Min(64, length - first);
            ulong word = 0;
            for (int k = 0; k < n; k++)
            {
                if (TTest.Test(Unsafe.Add(ref v, first + k), scalar)) word |= 1UL << k;
            }
            mask[w] = word;
        }
    }

    /// <summary>
    /// Sets bit <c>i</c> of <paramref name="mask"/> where <typeparamref name="TTest"/> holds for
    /// <paramref name="left"/>[i] against <paramref name="right"/>[i] (two computed values), and clears the bits past
    /// the last value.
    /// </summary>
    public static void Run<T, TTest>(ReadOnlySpan<T> left, ReadOnlySpan<T> right, Span<ulong> mask)
        where T : unmanaged, INumber<T>
        where TTest : struct, IValueTest<T>
    {
        int length = left.Length, words = Masks.Words(length);
        if (right.Length != length) throw new ArgumentException("The values differ in length.", nameof(right));
        if (mask.Length < words) throw new ArgumentException("The mask is shorter than the values.", nameof(mask));
        ref T a = ref MemoryMarshal.GetReference(left);
        ref T b = ref MemoryMarshal.GetReference(right);
        ref ulong m = ref MemoryMarshal.GetReference(mask);
        int full = length >> 6, w = 0;
        if (Vector256.IsHardwareAccelerated && Vector256<T>.IsSupported)
        {
            for (int lanes = Vector256<T>.Count; w < full; w++)
            {
                nuint first = (nuint)(w << 6);
                ulong word = 0;
                for (int k = 0; k < 64; k += lanes)
                    word |= (ulong)TTest.Test(Vector256.LoadUnsafe(ref a, first + (nuint)k), Vector256.LoadUnsafe(ref b, first + (nuint)k)).ExtractMostSignificantBits() << k;
                Unsafe.Add(ref m, w) = word;
            }
        }
        else if (Vector128.IsHardwareAccelerated && Vector128<T>.IsSupported)
        {
            for (int lanes = Vector128<T>.Count; w < full; w++)
            {
                nuint first = (nuint)(w << 6);
                ulong word = 0;
                for (int k = 0; k < 64; k += lanes)
                    word |= (ulong)TTest.Test(Vector128.LoadUnsafe(ref a, first + (nuint)k), Vector128.LoadUnsafe(ref b, first + (nuint)k)).ExtractMostSignificantBits() << k;
                Unsafe.Add(ref m, w) = word;
            }
        }
        for (; w < words; w++)
        {
            int first = w << 6, n = Math.Min(64, length - first);
            ulong word = 0;
            for (int k = 0; k < n; k++)
            {
                if (TTest.Test(Unsafe.Add(ref a, first + k), Unsafe.Add(ref b, first + k))) word |= 1UL << k;
            }
            mask[w] = word;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int Words256<T, TTest>(ref T v, int full, T scalar, Span<ulong> mask)
        where T : unmanaged, INumber<T>
        where TTest : struct, IValueTest<T>
    {
        Vector256<T> s = Vector256.Create(scalar);
        int lanes = Vector256<T>.Count;
        ref ulong m = ref MemoryMarshal.GetReference(mask);
        for (int w = 0; w < full; w++)
        {
            nuint first = (nuint)(w << 6);
            ulong word = 0;
            for (int k = 0; k < 64; k += lanes)
                word |= (ulong)TTest.Test(Vector256.LoadUnsafe(ref v, first + (nuint)k), s).ExtractMostSignificantBits() << k;
            Unsafe.Add(ref m, w) = word;
        }
        return full;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int Words128<T, TTest>(ref T v, int full, T scalar, Span<ulong> mask)
        where T : unmanaged, INumber<T>
        where TTest : struct, IValueTest<T>
    {
        Vector128<T> s = Vector128.Create(scalar);
        int lanes = Vector128<T>.Count;
        ref ulong m = ref MemoryMarshal.GetReference(mask);
        for (int w = 0; w < full; w++)
        {
            nuint first = (nuint)(w << 6);
            ulong word = 0;
            for (int k = 0; k < 64; k += lanes)
                word |= (ulong)TTest.Test(Vector128.LoadUnsafe(ref v, first + (nuint)k), s).ExtractMostSignificantBits() << k;
            Unsafe.Add(ref m, w) = word;
        }
        return full;
    }
}
