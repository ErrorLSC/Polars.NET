using System.Numerics;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace Kaminari.Sazanami.Kernels;

/// <summary>
/// The smallest and largest of a run of numbers, for statistics: portable vectors (<see cref="Vector128{T}"/>, the
/// order of <typeparamref name="T"/>: signed or unsigned integers, IEEE floats), the tail and machines without vector
/// support value by value. Floating values skip NaN (counted); which of -0 and +0 a bound takes is unspecified.
/// </summary>
internal static class MinMax
{
    /// <summary>The bounds of <paramref name="values"/> (at least one).</summary>
    public static void Integers<T>(ReadOnlySpan<T> values, out T min, out T max) where T : unmanaged, IBinaryInteger<T>
    {
        if (values.IsEmpty) throw new ArgumentException("No values.", nameof(values));
        int i;
        if (Vector128.IsHardwareAccelerated && Vector128<T>.IsSupported && values.Length >= 2 * Vector128<T>.Count)
        {
            ref T first = ref MemoryMarshal.GetReference(values);
            Vector128<T> lo = Vector128.LoadUnsafe(ref first), hi = lo;
            int n = Vector128<T>.Count;
            for (i = n; i <= values.Length - n; i += n)
            {
                Vector128<T> v = Vector128.LoadUnsafe(ref first, (nuint)i);
                lo = Vector128.Min(lo, v);
                hi = Vector128.Max(hi, v);
            }
            min = lo.GetElement(0);
            max = hi.GetElement(0);
            for (int k = 1; k < n; k++)
            {
                min = T.Min(min, lo.GetElement(k));
                max = T.Max(max, hi.GetElement(k));
            }
        }
        else
        {
            min = max = values[0];
            i = 1;
        }
        for (; i < values.Length; i++)
        {
            T v = values[i];
            if (v < min) min = v;
            else if (v > max) max = v;
        }
    }

    /// <summary>
    /// The bounds of the values of <paramref name="values"/> that are not NaN; false when there are none. NaNs are
    /// counted in <paramref name="nans"/>.
    /// </summary>
    public static bool Floating<T>(ReadOnlySpan<T> values, out T min, out T max, out long nans) where T : unmanaged, IFloatingPointIeee754<T>
    {
        min = max = T.Zero;
        nans = 0;
        bool found = false;
        int i = 0;
        if (Vector128.IsHardwareAccelerated && Vector128<T>.IsSupported && values.Length >= 2 * Vector128<T>.Count)
        {
            ref T first = ref MemoryMarshal.GetReference(values);
            int n = Vector128<T>.Count;
            Vector128<T> lo = default, hi = default;
            for (; i <= values.Length - n; i += n)
            {
                Vector128<T> v = Vector128.LoadUnsafe(ref first, (nuint)i);
                if (Vector128.EqualsAll(v, v)) // no NaN
                {
                    if (!found)
                    {
                        (lo, hi, found) = (v, v, true);
                        continue;
                    }
                    lo = Vector128.Min(lo, v);
                    hi = Vector128.Max(hi, v);
                    continue;
                }
                for (int k = 0; k < n; k++) Scalar(v.GetElement(k), ref lo, ref hi, ref found, ref nans);
            }
            if (found)
            {
                min = lo.GetElement(0);
                max = hi.GetElement(0);
                for (int k = 1; k < n; k++)
                {
                    min = Smaller(min, lo.GetElement(k));
                    max = Larger(max, hi.GetElement(k));
                }
            }
        }
        for (; i < values.Length; i++)
        {
            T v = values[i];
            if (T.IsNaN(v)) nans++;
            else if (!found) (min, max, found) = (v, v, true);
            else
            {
                min = Smaller(min, v);
                max = Larger(max, v);
            }
        }
        return found;

        // A value of a vector holding a NaN: folded into lane 0 of the running bounds.
        static void Scalar(T v, ref Vector128<T> lo, ref Vector128<T> hi, ref bool found, ref long nans)
        {
            if (T.IsNaN(v))
            {
                nans++;
                return;
            }
            if (!found)
            {
                (lo, hi, found) = (Vector128.Create(v), Vector128.Create(v), true);
                return;
            }
            lo = lo.WithElement(0, Smaller(lo.GetElement(0), v));
            hi = hi.WithElement(0, Larger(hi.GetElement(0), v));
        }

        static T Smaller(T a, T b) => b < a ? b : a;
        static T Larger(T a, T b) => b > a ? b : a;
    }
}
