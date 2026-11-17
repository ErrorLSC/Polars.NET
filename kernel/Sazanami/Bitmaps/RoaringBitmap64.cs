using System.Buffers;
using System.Buffers.Binary;
using System.Collections;
using System.Numerics;
using System.Runtime.InteropServices;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Filters;
using Kaminari.Sazanami.Kernels;

namespace Kaminari.Sazanami.Bitmaps;

/// <summary>
/// A set of row numbers (non-negative 64-bit integers) as a roaring bitmap in native memory: the rows sharing their
/// high 48 bits form a container, held as a sorted array of 16-bit values, a 65536-bit bitset or a list of runs,
/// whichever is smallest. Reads and writes the portable 64-bit serialization (CRoaring's
/// <c>roaring64_bitmap_portable_serialize</c>: a 64-bit count of 32-bit bitmaps, each its high 32 bits and the
/// portable 32-bit format), which Delta deletion vectors and Iceberg deletion vectors wrap in their own framing.
/// </summary>
/// <remarks>
/// Immutable: the set operations return a new bitmap. Every container is kept in the kind the portable writers choose
/// (runs when <c>2 + 4·runs</c> is smaller than the plain form, else an array up to 4096 values, else a bitset), so
/// <see cref="Serialize"/> writes them as they are and two equal sets serialize to the same bytes. Concurrent reads
/// are safe; <see cref="Dispose"/> returns the memory to <see cref="NativeMemoryPool"/>.
/// </remarks>
public sealed class RoaringBitmap64 : IDisposable
{
    private enum Kind : byte { Array, Bitset, Run }

    /// <summary>A container's place in the arena: its values (array), words (bitset) or (start, length − 1) pairs (runs).</summary>
    private struct Container
    {
        public int Offset;
        public int Cardinality;
        public int Runs;
        public Kind Kind;
    }

    private const int ArrayMax = 4096;
    private const int BitsetWords = 1024;
    private const int BitsetShorts = 4 * BitsetWords;
    private const uint CookieNoRuns = 12346;
    private const uint CookieRuns = 12347;
    private const int NoOffsetThreshold = 4;

    private readonly NativeArray<long> _keys;
    private readonly NativeArray<Container> _containers;
    private readonly NativeArray<ushort> _data;
    private readonly int _count;
    private bool _disposed;

    private RoaringBitmap64(NativeArray<long> keys, NativeArray<Container> containers, NativeArray<ushort> data, int count, long cardinality)
    {
        _keys = keys;
        _containers = containers;
        _data = data;
        _count = count;
        Cardinality = cardinality;
    }

    /// <summary>A bitmap holding no rows.</summary>
    public static RoaringBitmap64 Empty() => new Assembler().Finish();

    /// <summary>The number of rows in the set.</summary>
    public long Cardinality { get; }

    /// <summary>Whether the set holds no row.</summary>
    public bool IsEmpty => Cardinality == 0;

    // ------------------------------------------------------------------
    // Building
    // ------------------------------------------------------------------

    /// <summary>The rows of a strictly ascending, non-negative sequence.</summary>
    /// <exception cref="ArgumentException">The rows are not strictly ascending, or one is negative.</exception>
    public static RoaringBitmap64 FromSorted(ReadOnlySpan<long> rows)
    {
        if (rows.Length > 0 && rows[0] < 0) throw new ArgumentException("Row numbers are not negative.", nameof(rows));
        for (int i = 1; i < rows.Length; i++)
        {
            if (rows[i] <= rows[i - 1]) throw new ArgumentException($"The rows are not strictly ascending at index {i}.", nameof(rows));
        }
        var assembler = new Assembler();
        try
        {
            for (int i = 0; i < rows.Length;)
            {
                long key = rows[i] >> 16;
                int start = i;
                while (i < rows.Length && rows[i] >> 16 == key) i++;
                assembler.AddValues(key, rows[start..i]);
            }
            return assembler.Finish();
        }
        catch
        {
            assembler.Dispose();
            throw;
        }
    }

    /// <summary>The rows given in any order, repeats allowed.</summary>
    /// <exception cref="ArgumentException">A row is negative.</exception>
    public static RoaringBitmap64 FromRows(ReadOnlySpan<long> rows)
    {
        using var builder = new Builder(rows.Length);
        builder.AddRange(rows);
        return builder.Build();
    }

    /// <summary>
    /// Collects rows in any order (repeats allowed) in native memory and builds the bitmap of them; reusable after
    /// <see cref="Build"/> (it empties). Not thread-safe.
    /// </summary>
    public sealed class Builder(int initialCapacity = 0) : IDisposable
    {
        private readonly NativeArray<long> _rows = new(initialCapacity);
        private int _length;

        /// <summary>The rows added since the last build, repeats included.</summary>
        public int Count => _length;

        /// <exception cref="ArgumentOutOfRangeException"><paramref name="row"/> is negative.</exception>
        public void Add(long row)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(row);
            _rows.Grow(_length + 1, _length);
            _rows.At(_length++) = row;
        }

        /// <exception cref="ArgumentException">A row is negative.</exception>
        public void AddRange(ReadOnlySpan<long> rows)
        {
            if (rows.Length == 0) return;
            if (rows.IndexOfAnyInRange(long.MinValue, -1) >= 0) throw new ArgumentException("Row numbers are not negative.", nameof(rows));
            _rows.Grow(_length + rows.Length, _length);
            rows.CopyTo(_rows.AsSpan(_length, rows.Length));
            _length += rows.Length;
        }

        /// <summary>The bitmap of the rows added; the builder is empty after.</summary>
        public RoaringBitmap64 Build()
        {
            Span<long> rows = _rows.AsSpan(0, _length);
            rows.Sort();
            int n = 0;
            for (int i = 0; i < rows.Length; i++)
            {
                if (n == 0 || rows[i] != rows[n - 1]) rows[n++] = rows[i];
            }
            _length = 0;
            return FromSorted(rows[..n]);
        }

        public void Dispose() => _rows.Dispose();
    }

    // ------------------------------------------------------------------
    // Queries
    // ------------------------------------------------------------------

    /// <summary>Whether <paramref name="row"/> is in the set.</summary>
    public bool Contains(long row)
    {
        ThrowIfDisposed();
        if (row < 0) return false;
        int index = _keys.AsSpan(0, _count).BinarySearch(row >> 16);
        if (index < 0) return false;
        Container c = _containers[index];
        ushort low = (ushort)row;
        ReadOnlySpan<ushort> payload = Payload(c);
        return c.Kind switch
        {
            Kind.Array => payload.BinarySearch(low) >= 0,
            Kind.Bitset => (Words(payload)[low >> 6] >> (low & 63) & 1) != 0,
            _ => InRuns(payload, c.Runs, low),
        };

        static bool InRuns(ReadOnlySpan<ushort> pairs, int runs, ushort low)
        {
            // The last run starting at or before low.
            int lo = 0, hi = runs - 1, found = -1;
            while (lo <= hi)
            {
                int mid = (lo + hi) >>> 1;
                if (pairs[2 * mid] <= low) { found = mid; lo = mid + 1; }
                else hi = mid - 1;
            }
            return found >= 0 && low - pairs[2 * found] <= pairs[2 * found + 1];
        }
    }

    /// <summary>A copy of this bitmap (its own native memory).</summary>
    public RoaringBitmap64 Clone()
    {
        ThrowIfDisposed();
        var assembler = new Assembler();
        try
        {
            for (int i = 0; i < _count; i++) assembler.AddCopy(_keys[i], _containers[i], Payload(_containers[i]));
            return assembler.Finish();
        }
        catch
        {
            assembler.Dispose();
            throw;
        }
    }

    /// <summary>Whether both bitmaps hold the same rows.</summary>
    public bool SetEquals(RoaringBitmap64 other)
    {
        ArgumentNullException.ThrowIfNull(other);
        ThrowIfDisposed();
        other.ThrowIfDisposed();
        if (Cardinality != other.Cardinality || _count != other._count) return false;
        if (!_keys.AsSpan(0, _count).SequenceEqual(other._keys.AsSpan(0, _count))) return false;
        for (int i = 0; i < _count; i++)
        {
            // Containers are canonical: the same values come in the same kind.
            Container a = _containers[i], b = other._containers[i];
            if (a.Kind != b.Kind || a.Cardinality != b.Cardinality || !Payload(a).SequenceEqual(other.Payload(b))) return false;
        }
        return true;
    }

    /// <summary>Writes the rows in ascending order into the first <see cref="Cardinality"/> values of <paramref name="destination"/>.</summary>
    /// <exception cref="ArgumentException"><paramref name="destination"/> is shorter than <see cref="Cardinality"/>.</exception>
    public void CopyTo(Span<long> destination)
    {
        ThrowIfDisposed();
        if (destination.Length < Cardinality) throw new ArgumentException($"The destination holds {destination.Length} values, the set {Cardinality}.", nameof(destination));
        int at = 0;
        for (int i = 0; i < _count; i++)
        {
            long baseRow = _keys[i] << 16;
            Container c = _containers[i];
            ReadOnlySpan<ushort> payload = Payload(c);
            switch (c.Kind)
            {
                case Kind.Array:
                    foreach (ushort v in payload) destination[at++] = baseRow | v;
                    break;
                case Kind.Run:
                    for (int r = 0; r < c.Runs; r++)
                    {
                        long start = baseRow | payload[2 * r];
                        for (int k = 0, n = payload[2 * r + 1] + 1; k < n; k++) destination[at++] = start + k; // no v <= end: the last run may end at long.MaxValue
                    }
                    break;
                default:
                    ReadOnlySpan<ulong> words = Words(payload);
                    for (int w = 0; w < BitsetWords; w++)
                    {
                        for (ulong word = words[w]; word != 0; word &= word - 1)
                            destination[at++] = baseRow | (uint)(w * 64 + BitOperations.TrailingZeroCount(word));
                    }
                    break;
            }
        }
    }

    /// <summary>The rows as maximal ranges <c>[Start, Start + Count)</c>, ascending.</summary>
    public RangeEnumerable Ranges
    {
        get
        {
            ThrowIfDisposed();
            return new(this);
        }
    }

    /// <summary>
    /// Fills the selection mask of rows <c>[<paramref name="firstRow"/>, <paramref name="firstRow"/> +
    /// <paramref name="count"/>)</c>, LSB-first in 64-bit words (bit <c>i</c> is row <c>firstRow + i</c>): set for the
    /// rows in the set, or with <paramref name="complement"/> for the rows not in it (a deletion vector's kept rows).
    /// Bits past <paramref name="count"/> in the last word are cleared; words after it are left alone.
    /// </summary>
    /// <exception cref="ArgumentException"><paramref name="mask"/> holds fewer than <c>⌈count / 64⌉</c> words.</exception>
    public void FillMask(long firstRow, int count, Span<ulong> mask, bool complement = false)
    {
        ThrowIfDisposed();
        ArgumentOutOfRangeException.ThrowIfNegative(firstRow);
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        int words = Masks.Words(count);
        if (mask.Length < words) throw new ArgumentException($"The mask holds {mask.Length} words, {count} rows need {words}.", nameof(mask));
        mask = mask[..words];
        mask.Clear();
        Span<byte> bits = MemoryMarshal.AsBytes(mask);
        ulong end = (ulong)firstRow + (uint)count; // past long.MaxValue when the window reaches the last rows
        for (int i = LowerBound(firstRow >> 16); i < _count; i++)
        {
            long baseRow = _keys[i] << 16;
            if ((ulong)baseRow >= end) break;
            int lo = (int)Math.Max(0, firstRow - baseRow), hi = (int)Math.Min(1UL << 16, end - (ulong)baseRow);
            long shift = baseRow - firstRow; // mask bit of the container's value 0 (negative when it starts before the window)
            Container c = _containers[i];
            ReadOnlySpan<ushort> payload = Payload(c);
            switch (c.Kind)
            {
                case Kind.Array:
                {
                    int k = payload.BinarySearch((ushort)lo);
                    for (k = k < 0 ? ~k : k; k < payload.Length && payload[k] < hi; k++)
                    {
                        long bit = shift + payload[k];
                        mask[(int)(bit >> 6)] |= 1UL << (int)(bit & 63);
                    }
                    break;
                }
                case Kind.Run:
                    for (int r = 0; r < c.Runs; r++)
                    {
                        int s = Math.Max(payload[2 * r], lo), e = Math.Min(payload[2 * r] + payload[2 * r + 1] + 1, hi);
                        if (s < e) Bits.Fill(bits, shift + s, e - s, true);
                    }
                    break;
                default:
                    Bits.Copy(MemoryMarshal.AsBytes(payload), lo, bits, shift + lo, hi - lo);
                    break;
            }
        }
        if (complement) Masks.Not(mask, count);
    }

    // ------------------------------------------------------------------
    // Set operations
    // ------------------------------------------------------------------

    /// <summary>The rows in this set or in <paramref name="other"/>.</summary>
    public RoaringBitmap64 Union(RoaringBitmap64 other) => Combine(other, Operation.Or);

    /// <summary>The rows in both sets.</summary>
    public RoaringBitmap64 Intersect(RoaringBitmap64 other) => Combine(other, Operation.And);

    /// <summary>The rows in this set but not in <paramref name="other"/>.</summary>
    public RoaringBitmap64 Except(RoaringBitmap64 other) => Combine(other, Operation.AndNot);

    private enum Operation { Or, And, AndNot }

    private RoaringBitmap64 Combine(RoaringBitmap64 other, Operation operation)
    {
        ArgumentNullException.ThrowIfNull(other);
        ThrowIfDisposed();
        other.ThrowIfDisposed();
        var assembler = new Assembler();
        try
        {
            using var scratch = new NativeArray<ulong>(BitsetWords);
            Span<ulong> left = assembler.Words, right = scratch.AsSpan(0, BitsetWords);
            int i = 0, j = 0;
            while (i < _count || j < other._count)
            {
                long a = i < _count ? _keys[i] : long.MaxValue, b = j < other._count ? other._keys[j] : long.MaxValue;
                if (a < b)
                {
                    if (operation != Operation.And) assembler.AddCopy(a, _containers[i], Payload(_containers[i]));
                    i++;
                }
                else if (b < a)
                {
                    if (operation == Operation.Or) assembler.AddCopy(b, other._containers[j], other.Payload(other._containers[j]));
                    j++;
                }
                else
                {
                    ToWords(_containers[i], Payload(_containers[i]), left);
                    ToWords(other._containers[j], other.Payload(other._containers[j]), right);
                    switch (operation)
                    {
                        case Operation.Or: Masks.Or(left, right); break;
                        case Operation.And: Masks.And(left, right); break;
                        default: Masks.AndNot(left, right); break;
                    }
                    assembler.AddWords(a, left);
                    i++;
                    j++;
                }
            }
            return assembler.Finish();
        }
        catch
        {
            assembler.Dispose();
            throw;
        }
    }

    /// <summary>A container as a 65536-bit bitset.</summary>
    private static void ToWords(Container c, ReadOnlySpan<ushort> payload, Span<ulong> words)
    {
        switch (c.Kind)
        {
            case Kind.Bitset:
                Words(payload).CopyTo(words);
                break;
            case Kind.Array:
                words.Clear();
                foreach (ushort v in payload) words[v >> 6] |= 1UL << (v & 63);
                break;
            default:
                words.Clear();
                for (int r = 0; r < c.Runs; r++) Masks.SetRange(words, payload[2 * r], payload[2 * r + 1] + 1);
                break;
        }
    }

    // ------------------------------------------------------------------
    // Portable serialization
    // ------------------------------------------------------------------

    /// <summary>The number of bytes <see cref="Serialize"/> writes.</summary>
    public long SerializedSize
    {
        get
        {
            ThrowIfDisposed();
            long size = 8;
            for (int i = 0; i < _count;)
            {
                int end = BucketEnd(i);
                size += 4 + BucketHeadSize(i, end);
                for (; i < end; i++) size += PayloadBytes(_containers[i]);
            }
            return size;
        }
    }

    /// <summary>Writes the set in the portable 64-bit format.</summary>
    public void Serialize(IBufferWriter<byte> output)
    {
        ArgumentNullException.ThrowIfNull(output);
        ThrowIfDisposed();
        int buckets = 0;
        for (int i = 0; i < _count; i = BucketEnd(i)) buckets++;
        Span<byte> head = output.GetSpan(8);
        BinaryPrimitives.WriteInt64LittleEndian(head, buckets);
        output.Advance(8);

        for (int first = 0; first < _count;)
        {
            int end = BucketEnd(first), n = end - first;
            bool hasRuns = false;
            for (int i = first; i < end; i++) hasRuns |= _containers[i].Kind == Kind.Run;
            bool withOffsets = !hasRuns || n >= NoOffsetThreshold;
            int headSize = BucketHeadSize(first, end);

            // The high 32 bits, the cookie (with the run flags) or the cookie and the count, the (key, cardinality − 1)
            // pairs, then the offsets of the containers from the start of this 32-bit bitmap.
            Span<byte> span = output.GetSpan(4 + headSize)[..(4 + headSize)];
            BinaryPrimitives.WriteUInt32LittleEndian(span, (uint)(_keys[first] >> 16));
            Span<byte> bitmap = span[4..];
            int pos;
            if (hasRuns)
            {
                BinaryPrimitives.WriteUInt32LittleEndian(bitmap, CookieRuns | (uint)(n - 1) << 16);
                Span<byte> flags = bitmap.Slice(4, (n + 7) / 8);
                flags.Clear();
                for (int c = 0; c < n; c++)
                {
                    if (_containers[first + c].Kind == Kind.Run) flags[c / 8] |= (byte)(1 << (c % 8));
                }
                pos = 4 + flags.Length;
            }
            else
            {
                BinaryPrimitives.WriteUInt32LittleEndian(bitmap, CookieNoRuns);
                BinaryPrimitives.WriteInt32LittleEndian(bitmap[4..], n);
                pos = 8;
            }
            for (int c = first; c < end; c++)
            {
                BinaryPrimitives.WriteUInt16LittleEndian(bitmap[pos..], (ushort)_keys[c]);
                BinaryPrimitives.WriteUInt16LittleEndian(bitmap[(pos + 2)..], (ushort)(_containers[c].Cardinality - 1));
                pos += 4;
            }
            if (withOffsets)
            {
                int offset = headSize;
                for (int c = first; c < end; c++)
                {
                    BinaryPrimitives.WriteInt32LittleEndian(bitmap[pos..], offset);
                    pos += 4;
                    offset += PayloadBytes(_containers[c]);
                }
            }
            output.Advance(4 + headSize);

            for (int c = first; c < end; c++)
            {
                Container container = _containers[c];
                ReadOnlySpan<ushort> payload = Payload(container);
                int bytes = PayloadBytes(container);
                Span<byte> data = output.GetSpan(bytes)[..bytes];
                if (container.Kind == Kind.Run)
                {
                    BinaryPrimitives.WriteUInt16LittleEndian(data, (ushort)container.Runs);
                    data = data[2..];
                }
                WriteLittleEndian(payload, data);
                output.Advance(bytes);
            }
            first = end;
        }
    }

    /// <summary>Reads a set in the portable 64-bit format from the start of <paramref name="data"/>.</summary>
    /// <exception cref="InvalidDataException">The bytes are truncated or not a portable 64-bit roaring bitmap.</exception>
    public static RoaringBitmap64 Deserialize(ReadOnlySpan<byte> data) => Deserialize(data, out _);

    /// <summary>Reads a set in the portable 64-bit format from the start of <paramref name="data"/>.</summary>
    /// <param name="data">The bytes; may go on past the bitmap.</param>
    /// <param name="bytesRead">The bytes the bitmap took.</param>
    /// <exception cref="InvalidDataException">The bytes are truncated or not a portable 64-bit roaring bitmap.</exception>
    public static RoaringBitmap64 Deserialize(ReadOnlySpan<byte> data, out int bytesRead)
    {
        var assembler = new Assembler();
        try
        {
            Need(data, 0, 8);
            long buckets = BinaryPrimitives.ReadInt64LittleEndian(data);
            // Each 32-bit bitmap takes at least 12 bytes: its high bits, a cookie and a count (or one run flag and a pair).
            if (buckets < 0 || buckets > (data.Length - 8) / 12) throw Truncated();
            int pos = 8;
            long previousHigh = -1;
            for (long b = 0; b < buckets; b++)
            {
                Need(data, pos, 4);
                uint high = BinaryPrimitives.ReadUInt32LittleEndian(data[pos..]);
                pos += 4;
                if (high > int.MaxValue) throw new InvalidDataException($"The roaring bitmap holds values of 2^63 and more (high bits {high}); row numbers end below.");
                if (high <= previousHigh) throw NotAscending();
                previousHigh = high;
                pos += ReadBitmap32(data[pos..], (long)high << 16, ref assembler);
            }
            bytesRead = pos;
            return assembler.Finish();
        }
        catch
        {
            assembler.Dispose();
            throw;
        }
    }

    /// <summary>Reads one portable 32-bit bitmap whose containers' keys go under <paramref name="highKey"/>; returns its size.</summary>
    private static int ReadBitmap32(ReadOnlySpan<byte> data, long highKey, ref Assembler assembler)
    {
        Need(data, 0, 4);
        uint cookie = BinaryPrimitives.ReadUInt32LittleEndian(data);
        int pos = 4, containers;
        ReadOnlySpan<byte> runFlags = default;
        bool hasRuns = (cookie & 0xFFFF) == CookieRuns;
        if (hasRuns)
        {
            containers = (int)(cookie >> 16) + 1;
            Need(data, pos, (containers + 7) / 8);
            runFlags = data.Slice(pos, (containers + 7) / 8);
            pos += runFlags.Length;
        }
        else if (cookie == CookieNoRuns)
        {
            Need(data, pos, 4);
            containers = BinaryPrimitives.ReadInt32LittleEndian(data[pos..]);
            pos += 4;
            if (containers is < 0 or > 1 << 16) throw new InvalidDataException($"The roaring bitmap claims {containers} containers.");
        }
        else
        {
            throw new InvalidDataException($"Unknown roaring bitmap cookie {cookie}.");
        }

        Need(data, pos, containers * 4);
        ReadOnlySpan<byte> header = data.Slice(pos, containers * 4);
        pos += containers * 4;
        if (!hasRuns || containers >= NoOffsetThreshold)
        {
            // The offsets serve random access; a sequential read does not need them (and some writers leave them 0).
            Need(data, pos, containers * 4);
            pos += containers * 4;
        }

        int previousKey = -1;
        for (int c = 0; c < containers; c++)
        {
            int key = BinaryPrimitives.ReadUInt16LittleEndian(header[(c * 4)..]);
            int cardinality = BinaryPrimitives.ReadUInt16LittleEndian(header[(c * 4 + 2)..]) + 1;
            if (key <= previousKey) throw NotAscending();
            previousKey = key;
            bool isRun = hasRuns && (runFlags[c / 8] & (1 << (c % 8))) != 0;
            long containerKey = highKey | (uint)key;

            if (isRun)
            {
                Need(data, pos, 2);
                int runs = BinaryPrimitives.ReadUInt16LittleEndian(data[pos..]);
                pos += 2;
                Need(data, pos, 4 * runs);
                Span<ushort> pairs = assembler.Values[..(2 * runs)];
                ReadLittleEndian(data.Slice(pos, 4 * runs), pairs);
                pos += 4 * runs;
                long total = 0;
                int next = 0; // the lowest value the next run may start at (runs may touch, never overlap)
                for (int r = 0; r < runs; r++)
                {
                    int start = pairs[2 * r], last = start + pairs[2 * r + 1];
                    if (start < next) throw NotAscending();
                    if (last > ushort.MaxValue) throw new InvalidDataException("A run of the roaring bitmap goes past its container.");
                    total += last - start + 1;
                    next = last + 1;
                }
                if (total != cardinality) throw CardinalityMismatch(cardinality, total);
                assembler.AddRuns(containerKey, pairs);
            }
            else if (cardinality <= ArrayMax)
            {
                Need(data, pos, 2 * cardinality);
                Span<ushort> values = assembler.Values[..cardinality];
                ReadLittleEndian(data.Slice(pos, 2 * cardinality), values);
                pos += 2 * cardinality;
                for (int k = 1; k < values.Length; k++)
                {
                    if (values[k] <= values[k - 1]) throw NotAscending();
                }
                assembler.AddValues(containerKey, values);
            }
            else
            {
                Need(data, pos, 8 * BitsetWords);
                Span<ulong> words = assembler.Words;
                ReadLittleEndian(data.Slice(pos, 8 * BitsetWords), words);
                pos += 8 * BitsetWords;
                int set = 0;
                foreach (ulong w in words) set += BitOperations.PopCount(w);
                if (set != cardinality) throw CardinalityMismatch(cardinality, set);
                assembler.AddWords(containerKey, words);
            }
        }
        return pos;
    }

    private static void Need(ReadOnlySpan<byte> data, int pos, int bytes)
    {
        if (bytes < 0 || pos > data.Length - bytes) throw Truncated();
    }

    private static InvalidDataException Truncated() => new("The roaring bitmap is truncated.");

    private static InvalidDataException NotAscending() => new("The roaring bitmap is not in ascending order.");

    private static InvalidDataException CardinalityMismatch(int header, long actual) =>
        new($"A container of the roaring bitmap holds {actual} values but its header says {header}.");

    // ------------------------------------------------------------------
    // Layout helpers
    // ------------------------------------------------------------------

    private ReadOnlySpan<ushort> Payload(Container c) => _data.AsSpan(c.Offset, PayloadShorts(c));

    private static int PayloadShorts(Container c) => c.Kind switch
    {
        Kind.Array => c.Cardinality,
        Kind.Bitset => BitsetShorts,
        _ => 2 * c.Runs,
    };

    /// <summary>The bytes of a container's payload in the portable format (a run container leads with its run count).</summary>
    private static int PayloadBytes(Container c) => c.Kind == Kind.Run ? 2 + 4 * c.Runs : 2 * PayloadShorts(c);

    private static ReadOnlySpan<ulong> Words(ReadOnlySpan<ushort> payload) => MemoryMarshal.Cast<ushort, ulong>(payload);

    /// <summary>The index after the last container sharing container <paramref name="first"/>'s high 32 bits.</summary>
    private int BucketEnd(int first)
    {
        long high = _keys[first] >> 16;
        int end = first + 1;
        while (end < _count && _keys[end] >> 16 == high) end++;
        return end;
    }

    /// <summary>The bytes of a 32-bit bitmap before its payloads: cookie, run flags or count, key pairs, offsets.</summary>
    private int BucketHeadSize(int first, int end)
    {
        int n = end - first;
        bool hasRuns = false;
        for (int i = first; i < end; i++) hasRuns |= _containers[i].Kind == Kind.Run;
        int size = hasRuns ? 4 + (n + 7) / 8 : 8;
        size += 4 * n;
        if (!hasRuns || n >= NoOffsetThreshold) size += 4 * n;
        return size;
    }

    /// <summary>The first container whose key is at least <paramref name="key"/>.</summary>
    private int LowerBound(long key)
    {
        int index = _keys.AsSpan(0, _count).BinarySearch(key);
        return index < 0 ? ~index : index;
    }

    private static void WriteLittleEndian(ReadOnlySpan<ushort> values, Span<byte> destination)
    {
        if (BitConverter.IsLittleEndian) MemoryMarshal.AsBytes(values).CopyTo(destination);
        else BinaryPrimitives.ReverseEndianness(values, MemoryMarshal.Cast<byte, ushort>(destination));
    }

    private static void ReadLittleEndian(ReadOnlySpan<byte> source, Span<ushort> values)
    {
        if (BitConverter.IsLittleEndian) source.CopyTo(MemoryMarshal.AsBytes(values));
        else BinaryPrimitives.ReverseEndianness(MemoryMarshal.Cast<byte, ushort>(source), values);
    }

    private static void ReadLittleEndian(ReadOnlySpan<byte> source, Span<ulong> words)
    {
        if (BitConverter.IsLittleEndian) source.CopyTo(MemoryMarshal.AsBytes(words));
        else BinaryPrimitives.ReverseEndianness(MemoryMarshal.Cast<byte, ulong>(source), words);
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);

    public void Dispose()
    {
        _disposed = true;
        _keys.Dispose();
        _containers.Dispose();
        _data.Dispose();
    }

    // ------------------------------------------------------------------
    // Assembly: containers appended in ascending key order, each in its canonical kind
    // ------------------------------------------------------------------

    /// <summary>
    /// Appends containers in ascending key order, choosing each one's kind as the portable writers do; empty
    /// containers are dropped. Owns its arrays until <see cref="Finish"/> hands them to a bitmap.
    /// </summary>
    private struct Assembler()
    {
        private NativeArray<long> _keys = new();
        private NativeArray<Container> _containers = new();
        private NativeArray<ushort> _data = new();
        private NativeArray<ushort>? _values;
        private NativeArray<ulong>? _words;
        private int _count;
        private int _used;
        private long _cardinality;

        /// <summary>
        /// Scratch for one container's values or run pairs on the way in: up to 65535 runs, two values each (runs
        /// that touch, one value apiece, are legal).
        /// </summary>
        public Span<ushort> Values => (_values ??= new(1 << 17)).AsSpan(0, 1 << 17);

        /// <summary>Scratch for one container's bitset on the way in.</summary>
        public Span<ulong> Words => (_words ??= new(BitsetWords)).AsSpan(0, BitsetWords);

        private static Kind Choose(int cardinality, int runs)
        {
            int plain = cardinality <= ArrayMax ? 2 * cardinality : 8 * BitsetWords;
            return 2 + 4 * runs < plain ? Kind.Run : cardinality <= ArrayMax ? Kind.Array : Kind.Bitset;
        }

        /// <summary>A container of strictly ascending values: the low 16 bits of each (16-bit or 64-bit values).</summary>
        public void AddValues<T>(long key, ReadOnlySpan<T> values) where T : unmanaged, IBinaryInteger<T>
        {
            if (values.Length == 0) return;
            int runs = 1;
            for (int i = 1; i < values.Length; i++)
            {
                if (values[i] != values[i - 1] + T.One) runs++;
            }
            Kind kind = Choose(values.Length, runs);
            Span<ushort> payload = Append(key, kind, values.Length, runs);
            switch (kind)
            {
                case Kind.Array:
                    for (int i = 0; i < values.Length; i++) payload[i] = ushort.CreateTruncating(values[i]);
                    break;
                case Kind.Run:
                    for (int i = 0, r = 0; i < values.Length; r++)
                    {
                        int start = i;
                        while (i + 1 < values.Length && values[i + 1] == values[i] + T.One) i++;
                        payload[2 * r] = ushort.CreateTruncating(values[start]);
                        payload[2 * r + 1] = (ushort)(i - start);
                        i++;
                    }
                    break;
                default:
                    Span<ulong> words = MemoryMarshal.Cast<ushort, ulong>(payload);
                    words.Clear();
                    foreach (T value in values)
                    {
                        ushort v = ushort.CreateTruncating(value);
                        words[v >> 6] |= 1UL << (v & 63);
                    }
                    break;
            }
        }

        /// <summary>A container of (start, length − 1) runs, ascending and not overlapping (they may touch).</summary>
        public void AddRuns(long key, ReadOnlySpan<ushort> pairs)
        {
            int count = pairs.Length / 2, runs = 0, cardinality = 0;
            for (int r = 0; r < count; r++)
            {
                cardinality += pairs[2 * r + 1] + 1;
                if (r == 0 || pairs[2 * r] != pairs[2 * r - 2] + pairs[2 * r - 1] + 1) runs++;
            }
            if (cardinality == 0) return;
            Kind kind = Choose(cardinality, runs);
            Span<ushort> payload = Append(key, kind, cardinality, runs);
            switch (kind)
            {
                case Kind.Run:
                    // Touching runs merge.
                    for (int r = 0, w = -1; r < count; r++)
                    {
                        int start = pairs[2 * r], last = start + pairs[2 * r + 1];
                        if (w >= 0 && start == payload[2 * w] + payload[2 * w + 1] + 1) payload[2 * w + 1] = (ushort)(last - payload[2 * w]);
                        else
                        {
                            w++;
                            payload[2 * w] = (ushort)start;
                            payload[2 * w + 1] = pairs[2 * r + 1];
                        }
                    }
                    break;
                case Kind.Array:
                    for (int r = 0, i = 0; r < count; r++)
                    {
                        for (int v = pairs[2 * r], last = v + pairs[2 * r + 1]; v <= last; v++) payload[i++] = (ushort)v;
                    }
                    break;
                default:
                    Span<ulong> words = MemoryMarshal.Cast<ushort, ulong>(payload);
                    words.Clear();
                    for (int r = 0; r < count; r++) Masks.SetRange(words, pairs[2 * r], pairs[2 * r + 1] + 1);
                    break;
            }
        }

        /// <summary>A container given as a 65536-bit bitset.</summary>
        public void AddWords(long key, ReadOnlySpan<ulong> words)
        {
            int cardinality = 0, runs = 0;
            ulong carry = 0; // the top bit of the previous word: a run going on across the boundary
            foreach (ulong w in words)
            {
                cardinality += BitOperations.PopCount(w);
                runs += BitOperations.PopCount(w & ~(w << 1 | carry));
                carry = w >> 63;
            }
            if (cardinality == 0) return;
            Kind kind = Choose(cardinality, runs);
            Span<ushort> payload = Append(key, kind, cardinality, runs);
            switch (kind)
            {
                case Kind.Bitset:
                    words.CopyTo(MemoryMarshal.Cast<ushort, ulong>(payload));
                    break;
                case Kind.Array:
                {
                    int i = 0;
                    for (int w = 0; w < BitsetWords; w++)
                    {
                        for (ulong word = words[w]; word != 0; word &= word - 1)
                            payload[i++] = (ushort)(w * 64 + BitOperations.TrailingZeroCount(word));
                    }
                    break;
                }
                default:
                    for (int r = 0, at = NextSet(words, 0); at < 1 << 16; r++)
                    {
                        int end = NextClear(words, at);
                        payload[2 * r] = (ushort)at;
                        payload[2 * r + 1] = (ushort)(end - at - 1);
                        at = NextSet(words, end);
                    }
                    break;
            }
        }

        /// <summary>A container that is canonical already (taken from another bitmap).</summary>
        public void AddCopy(long key, Container c, ReadOnlySpan<ushort> payload) =>
            payload.CopyTo(Append(key, c.Kind, c.Cardinality, c.Runs));

        /// <summary>Makes room for a container's payload in the arena and records the container.</summary>
        private Span<ushort> Append(long key, Kind kind, int cardinality, int runs)
        {
            int size = PayloadShorts(new Container { Kind = kind, Cardinality = cardinality, Runs = runs });
            int offset = kind == Kind.Bitset ? (_used + 3) & ~3 : _used; // bitsets on 8-byte boundaries, read as words
            _data.Grow(offset + size, _used);
            _keys.Grow(_count + 1, _count);
            _containers.Grow(_count + 1, _count);
            _keys[_count] = key;
            _containers[_count] = new Container { Offset = offset, Cardinality = cardinality, Runs = runs, Kind = kind };
            _count++;
            _used = offset + size;
            _cardinality += cardinality;
            return _data.AsSpan(offset, size);
        }

        public readonly RoaringBitmap64 Finish()
        {
            _values?.Dispose();
            _words?.Dispose();
            return new(_keys, _containers, _data, _count, _cardinality);
        }

        public readonly void Dispose()
        {
            _keys.Dispose();
            _containers.Dispose();
            _data.Dispose();
            _values?.Dispose();
            _words?.Dispose();
        }

        /// <summary>The first set bit at or after <paramref name="from"/>, or 65536.</summary>
        internal static int NextSet(ReadOnlySpan<ulong> words, int from)
        {
            if (from >= 1 << 16) return 1 << 16;
            int w = from >> 6;
            ulong word = words[w] & (ulong.MaxValue << (from & 63));
            while (word == 0)
            {
                if (++w == BitsetWords) return 1 << 16;
                word = words[w];
            }
            return w * 64 + BitOperations.TrailingZeroCount(word);
        }

        /// <summary>The first clear bit at or after <paramref name="from"/>, or 65536.</summary>
        internal static int NextClear(ReadOnlySpan<ulong> words, int from)
        {
            if (from >= 1 << 16) return 1 << 16;
            int w = from >> 6;
            ulong word = ~words[w] & (ulong.MaxValue << (from & 63));
            while (word == 0)
            {
                if (++w == BitsetWords) return 1 << 16;
                word = ~words[w];
            }
            return w * 64 + BitOperations.TrailingZeroCount(word);
        }
    }

    // ------------------------------------------------------------------
    // Ranges
    // ------------------------------------------------------------------

    /// <summary>The rows of a bitmap as maximal ascending ranges (ranges touching across containers merge).</summary>
    public readonly struct RangeEnumerable : IEnumerable<(long Start, long Count)>
    {
        private readonly RoaringBitmap64 _bitmap;

        internal RangeEnumerable(RoaringBitmap64 bitmap) => _bitmap = bitmap;

        public RangeEnumerator GetEnumerator() => new(_bitmap);

        IEnumerator<(long Start, long Count)> IEnumerable<(long Start, long Count)>.GetEnumerator() => GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    /// <summary>Walks a bitmap's maximal ranges; the bitmap must stay undisposed meanwhile.</summary>
    public struct RangeEnumerator : IEnumerator<(long Start, long Count)>
    {
        private readonly RoaringBitmap64 _bitmap;
        private int _container;
        private int _position; // within the container: array index, run index, or bit
        private long _pendingStart, _pendingEnd;
        private bool _pending;

        internal RangeEnumerator(RoaringBitmap64 bitmap) => _bitmap = bitmap;

        public (long Start, long Count) Current { get; private set; }

        readonly object IEnumerator.Current => Current;

        public bool MoveNext()
        {
            while (NextInContainer(out long start, out long end))
            {
                if (_pending && start == _pendingEnd)
                {
                    _pendingEnd = end;
                    continue;
                }
                bool had = _pending;
                (long previousStart, long previousEnd) = (_pendingStart, _pendingEnd);
                (_pendingStart, _pendingEnd, _pending) = (start, end, true);
                if (had)
                {
                    Current = (previousStart, previousEnd - previousStart);
                    return true;
                }
            }
            if (!_pending) return false;
            Current = (_pendingStart, _pendingEnd - _pendingStart);
            _pending = false;
            return true;
        }

        /// <summary>The next range as one container stores it (not merged with its neighbors).</summary>
        private bool NextInContainer(out long start, out long end)
        {
            RoaringBitmap64 b = _bitmap;
            b.ThrowIfDisposed();
            for (; _container < b._count; _container++, _position = 0)
            {
                long baseRow = b._keys[_container] << 16;
                Container c = b._containers[_container];
                ReadOnlySpan<ushort> payload = b.Payload(c);
                switch (c.Kind)
                {
                    case Kind.Array when _position < payload.Length:
                    {
                        int first = _position;
                        while (_position + 1 < payload.Length && payload[_position + 1] == payload[_position] + 1) _position++;
                        // An end past long.MaxValue wraps to long.MinValue; Count = end - start still comes out right.
                        start = baseRow | payload[first];
                        end = (baseRow | payload[_position]) + 1;
                        _position++;
                        return true;
                    }
                    case Kind.Run when _position < c.Runs:
                        start = baseRow | payload[2 * _position];
                        end = start + payload[2 * _position + 1] + 1;
                        _position++;
                        return true;
                    case Kind.Bitset:
                    {
                        ReadOnlySpan<ulong> words = Words(payload);
                        int at = Assembler.NextSet(words, _position);
                        if (at >= 1 << 16) break;
                        _position = Assembler.NextClear(words, at);
                        start = baseRow + at;
                        end = baseRow + _position;
                        return true;
                    }
                }
            }
            start = end = 0;
            return false;
        }

        public void Reset()
        {
            _container = _position = 0;
            _pending = false;
        }

        public readonly void Dispose() { }
    }
}
