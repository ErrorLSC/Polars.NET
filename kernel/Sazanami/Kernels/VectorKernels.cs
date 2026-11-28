using System.Numerics;
using System.Runtime.InteropServices;

namespace Kaminari.Sazanami.Kernels;

/// <summary>Small vector kernels over spans, written with the portable vector APIs (the same code on x64 and arm64).</summary>
internal static class VectorKernels
{
    /// <summary>Adds <paramref name="value"/> to every value (wrapping), with vector adds.</summary>
    public static void AddInPlace(Span<int> values, int value)
    {
        int i = 0;
        if (Vector.IsHardwareAccelerated && values.Length >= Vector<int>.Count)
        {
            var add = new Vector<int>(value);
            Span<Vector<int>> vectors = MemoryMarshal.Cast<int, Vector<int>>(values);
            for (int v = 0; v < vectors.Length; v++) vectors[v] += add;
            i = vectors.Length * Vector<int>.Count;
        }
        for (; i < values.Length; i++) values[i] = unchecked(values[i] + value);
    }

    /// <summary>The largest value read as unsigned (a negative index counts as huge), with vector compares.</summary>
    public static uint MaxUnsigned(ReadOnlySpan<int> values)
    {
        ReadOnlySpan<uint> u = MemoryMarshal.Cast<int, uint>(values);
        int i = 0;
        uint max = 0;
        if (Vector.IsHardwareAccelerated && u.Length >= Vector<uint>.Count)
        {
            var acc = Vector<uint>.Zero;
            for (; i + Vector<uint>.Count <= u.Length; i += Vector<uint>.Count) acc = Vector.Max(acc, new Vector<uint>(u[i..]));
            for (int k = 0; k < Vector<uint>.Count; k++) max = Math.Max(max, acc[k]);
        }
        for (; i < u.Length; i++) max = Math.Max(max, u[i]);
        return max;
    }
}
