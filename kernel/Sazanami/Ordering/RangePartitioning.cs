using System.Buffers.Binary;
using System.Numerics;
using Apache.Arrow;
using Kaminari.Sazanami.Curves;

namespace Kaminari.Sazanami.Ordering;

/// <summary>
/// The range boundaries of one column, taken from a sample: values are cut into at most a given number of ranges
/// of about equal row counts, and any value of the column maps to the number of its range (a range id). Equal
/// values always share a range; nulls take range 0, with the smallest values. The order is the column's natural
/// order: numbers by value (NaN above every number, -0.0 equal to 0.0), strings and binary by their bytes,
/// decimals by value (one scale per column), dates and timestamps by their stored count (one unit per column).
/// </summary>
public sealed class RangeBoundaries
{
    private readonly Values _boundaries;

    private RangeBoundaries(Values boundaries) => _boundaries = boundaries;

    /// <summary>The number of ranges: one more than the boundaries.</summary>
    public int RangeCount => _boundaries.Count + 1;

    /// <summary>Boundaries for at most <paramref name="maxRanges"/> ranges, from a sample of the column's values.</summary>
    /// <exception cref="NotSupportedException">The column's type has no order here.</exception>
    public static RangeBoundaries FromSample(IArrowArray sample, int maxRanges)
    {
        ArgumentNullException.ThrowIfNull(sample);
        if (maxRanges < 1) throw new ArgumentOutOfRangeException(nameof(maxRanges), "At least one range.");
        Values values = Values.Of(sample).NonNull().Sorted();
        int n = values.Count;
        var picked = new List<int>();
        for (int r = 1; r < maxRanges && n > 0; r++)
        {
            int at = (int)((long)r * n / maxRanges);
            // A boundary starts a range: the first value of the next range; equal values never straddle one.
            if (at == 0 || (picked.Count > 0 && values.Compare(picked[^1], at) == 0)) continue;
            if (values.Compare(at - 1, at) == 0) continue; // inside a run of equal values: no cut here
            picked.Add(at);
        }
        return new RangeBoundaries(values.Select(picked));
    }

    /// <summary>The range id of every value (nulls: 0).</summary>
    public void Assign(IArrowArray column, Span<uint> ids)
    {
        ArgumentNullException.ThrowIfNull(column);
        if (ids.Length < column.Length) throw new ArgumentException("Fewer ids than values.", nameof(ids));
        Values values = Values.Of(column);
        for (int i = 0; i < column.Length; i++)
            ids[i] = values.IsNull(i) ? 0 : (uint)_boundaries.UpperBound(values, i);
    }

    /// <summary>
    /// A column's values in an order-preserving form: 64-bit integers, doubles, 128-bit unscaled decimals or byte
    /// strings, with nulls.
    /// </summary>
    private sealed class Values
    {
        private long[]? _longs;
        private double[]? _doubles;
        private Int128[]? _wide;
        private byte[][]? _bytes;
        private bool[] _nulls = [];

        public int Count => _nulls.Length;

        public bool IsNull(int i) => _nulls[i];

        public static Values Of(IArrowArray array)
        {
            int n = array.Length;
            var v = new Values { _nulls = new bool[n] };
            for (int i = 0; i < n; i++) v._nulls[i] = array.IsNull(i);
            switch (array)
            {
                case Int8Array a: v._longs = Longs(n, i => a.GetValue(i) ?? 0); break;
                case Int16Array a: v._longs = Longs(n, i => a.GetValue(i) ?? 0); break;
                case Int32Array a: v._longs = Longs(n, i => a.GetValue(i) ?? 0); break;
                case Int64Array a: v._longs = Longs(n, i => a.GetValue(i) ?? 0); break;
                case UInt8Array a: v._longs = Longs(n, i => a.GetValue(i) ?? 0); break;
                case UInt16Array a: v._longs = Longs(n, i => a.GetValue(i) ?? 0); break;
                case UInt32Array a: v._longs = Longs(n, i => a.GetValue(i) ?? 0); break;
                case UInt64Array a: v._wide = Wide(n, i => a.GetValue(i) ?? 0); break;
                case BooleanArray a: v._longs = Longs(n, i => a.GetValue(i) == true ? 1 : 0); break;
                case Date32Array a: v._longs = Longs(n, i => a.Values[i]); break;
                case Date64Array a: v._longs = Longs(n, i => a.Values[i]); break;
                case TimestampArray a: v._longs = Longs(n, i => a.Values[i]); break;
                case Time32Array a: v._longs = Longs(n, i => a.Values[i]); break;
                case Time64Array a: v._longs = Longs(n, i => a.Values[i]); break;
                case DurationArray a: v._longs = Longs(n, i => a.Values[i]); break;
                case FloatArray a: v._doubles = Doubles(n, i => a.GetValue(i) ?? 0); break;
                case DoubleArray a: v._doubles = Doubles(n, i => a.GetValue(i) ?? 0); break;
                case Decimal128Array a:
                {
                    ReadOnlySpan<byte> buffer = a.ValueBuffer.Span;
                    var wide = new Int128[n];
                    for (int i = 0; i < n; i++)
                    {
                        ReadOnlySpan<byte> raw = buffer.Slice((a.Offset + i) * 16, 16);
                        wide[i] = new Int128(BinaryPrimitives.ReadUInt64LittleEndian(raw[8..]), BinaryPrimitives.ReadUInt64LittleEndian(raw));
                    }
                    v._wide = wide;
                    break;
                }
                case BinaryArray a: v._bytes = Bytes(n, i => a.GetBytes(i).ToArray()); break;
                case LargeBinaryArray a: v._bytes = Bytes(n, i => a.GetBytes(i).ToArray()); break;
                case BinaryViewArray a: v._bytes = Bytes(n, i => a.GetBytes(i).ToArray()); break;
                default:
                    throw new NotSupportedException($"Values of type {array.Data.DataType.Name} have no order for clustering.");
            }
            return v;
        }

        private static long[] Longs(int n, Func<int, long> get)
        {
            var r = new long[n];
            for (int i = 0; i < n; i++) r[i] = get(i);
            return r;
        }

        private static Int128[] Wide(int n, Func<int, ulong> get)
        {
            var r = new Int128[n];
            for (int i = 0; i < n; i++) r[i] = get(i);
            return r;
        }

        private static double[] Doubles(int n, Func<int, double> get)
        {
            var r = new double[n];
            for (int i = 0; i < n; i++) r[i] = get(i) is var d && d == 0 ? 0.0 : d; // -0.0 as 0.0
            return r;
        }

        private static byte[][] Bytes(int n, Func<int, byte[]> get)
        {
            var r = new byte[n][];
            for (int i = 0; i < n; i++) r[i] = get(i);
            return r;
        }

        /// <summary>The non-null values, in their order of appearance.</summary>
        public Values NonNull() => Select([.. Enumerable.Range(0, Count).Where(i => !_nulls[i])]);

        /// <summary>The values at the given positions.</summary>
        public Values Select(IReadOnlyList<int> at)
        {
            var v = new Values { _nulls = new bool[at.Count] };
            for (int i = 0; i < at.Count; i++) v._nulls[i] = _nulls[at[i]];
            if (_longs is not null) v._longs = [.. at.Select(i => _longs[i])];
            if (_doubles is not null) v._doubles = [.. at.Select(i => _doubles[i])];
            if (_wide is not null) v._wide = [.. at.Select(i => _wide[i])];
            if (_bytes is not null) v._bytes = [.. at.Select(i => _bytes[i])];
            return v;
        }

        /// <summary>The values sorted (no nulls expected).</summary>
        public Values Sorted()
        {
            int[] order = [.. Enumerable.Range(0, Count)];
            System.Array.Sort(order, Compare);
            return Select(order);
        }

        public int Compare(int a, int b) => CompareAcross(this, a, this, b);

        /// <summary>The number of these (sorted, non-null) values at most <paramref name="other"/>'s value at <paramref name="i"/>.</summary>
        public int UpperBound(Values other, int i)
        {
            int lo = 0, hi = Count;
            while (lo < hi)
            {
                int mid = (lo + hi) >>> 1;
                if (CompareAcross(this, mid, other, i) <= 0) lo = mid + 1;
                else hi = mid;
            }
            return lo;
        }

        private static int CompareAcross(Values x, int a, Values y, int b)
        {
            if (x._longs is not null) return x._longs[a].CompareTo(y._longs![b]);
            if (x._doubles is not null) return CompareDoubles(x._doubles[a], y._doubles![b]);
            if (x._wide is not null) return x._wide[a].CompareTo(y._wide![b]);
            if (x._bytes is not null) return x._bytes[a].AsSpan().SequenceCompareTo(y._bytes![b]);
            return 0; // an empty sample: no values to compare
        }

        // NaN above every number (and equal to itself), as Spark orders doubles.
        private static int CompareDoubles(double a, double b) =>
            double.IsNaN(a) ? (double.IsNaN(b) ? 0 : 1) : double.IsNaN(b) ? -1 : a.CompareTo(b);
    }
}

/// <summary>
/// Keys and buckets of liquid clustering, as Spark's multi-dimensional clustering makes them: each row's range ids
/// (one per clustering column) are placed along a Hilbert curve, a random byte is added to spread rows with equal
/// ids, and rows are assigned to buckets by key range, the bucket boundaries taken from a sample's keys. Rows are
/// not sorted within a bucket (Spark's default).
/// </summary>
public static class ClusteringKeys
{
    /// <summary>The bits a range id takes for <paramref name="ranges"/> ranges.</summary>
    public static int BitsFor(int ranges) => Math.Max(1, 32 - BitOperations.LeadingZeroCount((uint)Math.Max(1, ranges - 1)));

    /// <summary>
    /// The curve key of every row: the Hilbert index of its range ids, shifted up by 8 bits for a noise byte.
    /// </summary>
    /// <param name="rangeIds">One array per clustering column, all of <paramref name="keys"/>' length; each id below 2^bits.</param>
    /// <param name="bits">Bits per range id; columns × bits at most 56.</param>
    /// <param name="noise">The noise bytes' source; null for none (keys then differ only by their ids).</param>
    /// <param name="keys">Receives one key per row.</param>
    public static void Compute(ReadOnlyMemory<uint>[] rangeIds, int bits, Random? noise, Span<ulong> keys)
    {
        ArgumentNullException.ThrowIfNull(rangeIds);
        if (rangeIds.Length == 0 || rangeIds.Length * bits > 56)
            throw new ArgumentException($"{rangeIds.Length} columns of {bits} bits do not fit a key.", nameof(rangeIds));
        int n = keys.Length;
        var coordinates = new ReadOnlyMemory<ulong>[rangeIds.Length];
        for (int d = 0; d < rangeIds.Length; d++)
        {
            ReadOnlySpan<uint> ids = rangeIds[d].Span;
            if (ids.Length < n) throw new ArgumentException("A column has fewer range ids than there are keys.", nameof(rangeIds));
            var wide = new ulong[n];
            for (int p = 0; p < n; p++) wide[p] = ids[p];
            coordinates[d] = wide;
        }
        var curve = new UInt128[n];
        SpaceFillingCurve.Hilbert(coordinates, bits, curve);
        for (int p = 0; p < n; p++) keys[p] = ((ulong)curve[p] << 8) | (byte)(noise?.Next(256) ?? 0);
    }

    /// <summary>
    /// Boundaries for at most <paramref name="buckets"/> buckets of about equal row counts, from a sample's keys.
    /// </summary>
    public static ulong[] BucketBoundaries(ReadOnlySpan<ulong> sampleKeys, int buckets)
    {
        if (buckets < 1) throw new ArgumentOutOfRangeException(nameof(buckets), "At least one bucket.");
        ulong[] sorted = sampleKeys.ToArray();
        System.Array.Sort(sorted);
        var boundaries = new List<ulong>();
        for (int b = 1; b < buckets && sorted.Length > 0; b++)
        {
            ulong at = sorted[(int)((long)b * sorted.Length / buckets)];
            // A boundary starts a bucket: never at the smallest key (an empty first bucket) nor twice.
            if (at > sorted[0] && (boundaries.Count == 0 || boundaries[^1] < at)) boundaries.Add(at);
        }
        return [.. boundaries];
    }

    /// <summary>The bucket of a key: the number of boundaries at most the key.</summary>
    public static uint Bucket(ReadOnlySpan<ulong> boundaries, ulong key)
    {
        int lo = 0, hi = boundaries.Length;
        while (lo < hi)
        {
            int mid = (lo + hi) >>> 1;
            if (boundaries[mid] <= key) lo = mid + 1;
            else hi = mid;
        }
        return (uint)lo;
    }
}
