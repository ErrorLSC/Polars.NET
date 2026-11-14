using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Filters;
using Kaminari.Sazanami.Helpers;

namespace Kaminari.Sazanami.Arrow;

/// <summary>
/// Keeps the rows of a flat Arrow array that a row mask selects (<see cref="Masks"/> layout), in a new array over
/// native memory of its own (freed when it is disposed, as the arrays of <see cref="NativeBatchExport"/>). The input
/// is left as it is; it must stay alive while it is filtered.
/// </summary>
/// <remarks>
/// Values are gathered word by word (whole words of selected rows copied at once, set bits otherwise); no shuffle
/// tables, which the .NET 8 runtime does not accelerate. Long values of views and the dictionaries of dictionary
/// arrays are shared with the input instead of copied when the input's memory is a native batch's
/// (<see cref="NativeBatchExport.TryShare"/>): the result then keeps that memory until it is disposed. Views are
/// shared only when enough rows are kept (<see cref="ShareViewDataFrom"/>): a few rows would hold a whole batch's bytes.
/// </remarks>
internal static unsafe class ArrowFilter
{
    /// <summary>The share of a view array's rows from which its data buffers are shared rather than the kept values copied.</summary>
    internal const double ShareViewDataFrom = 0.125;

    /// <summary>
    /// The rows of <paramref name="array"/> whose bits are set in <paramref name="selected"/> (bits past its length
    /// are ignored). <paramref name="shareViewData"/> forces sharing (true) or copying (false) of long view values;
    /// null decides by the rows kept.
    /// </summary>
    /// <exception cref="NotSupportedException">A nested array (lists, structs, maps) or a type without a native builder.</exception>
    public static IArrowArray Filter(IArrowArray array, ReadOnlySpan<ulong> selected, bool? shareViewData = null)
    {
        ArgumentNullException.ThrowIfNull(array);
        ArrayData data = array.Data;
        int length = data.Length;
        if (selected.Length < Masks.Words(length)) throw new ArgumentException($"The mask holds fewer than {length} bits.", nameof(selected));
        int count = Count(selected, length);
        if (data.DataType is NullType) return new NullArray(count);
        var export = new NativeBatchExport();
        try
        {
            return export.ImportArray(Build(export, data, selected, count, shareViewData), data.DataType);
        }
        catch
        {
            export.FreeAll();
            throw;
        }
    }

    private static int Count(ReadOnlySpan<ulong> selected, int length)
    {
        int words = Masks.Words(length);
        if (words == 0) return 0;
        return Masks.Count(selected[..(words - 1)]) + BitOperations.PopCount(selected[words - 1] & Masks.LastWord(length));
    }

    /// <summary>Word <paramref name="w"/> of the mask, without the bits past the last row.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong Word(ReadOnlySpan<ulong> selected, int w, int length) =>
        (w << 6) + 64 <= length ? selected[w] : selected[w] & Masks.LastWord(length);

    private static NativeCArray* Build(NativeBatchExport export, ArrayData data, ReadOnlySpan<ulong> selected, int count, bool? shareViewData)
    {
        int length = data.Length;
        nint validity = 0;
        long nulls = 0;
        if (data.NullCount != 0 && !data.Buffers[0].IsEmpty)
        {
            var bits = (ulong*)export.Allocate(Masks.Words(count) * 8L);
            int set = GatherBits(data.Buffers[0].Span, data.Offset, selected, length, bits);
            nulls = count - set;
            if (nulls > 0) validity = (nint)bits;
        }

        switch (data.DataType)
        {
            case DictionaryType d:
            {
                if (d.IndexType is not IntegerType index) throw new NotSupportedException($"Dictionary indices of type {d.IndexType.Name}.");
                nint indices = GatherFixed(export, data.Buffers[1].Span, data.Offset, index.BitWidth / 8, selected, length, count);
                NativeCArray* dictionary = Dictionary(export, data.Dictionary!);
                return export.NewArray(count, nulls, [validity, indices], [], dictionary);
            }
            case BooleanType:
            {
                var bits = (ulong*)export.Allocate(Masks.Words(count) * 8L);
                GatherBits(data.Buffers[1].Span, data.Offset, selected, length, bits);
                return export.NewArray(count, nulls, [validity, (nint)bits], []);
            }
            case FixedSizeBinaryType f:
            {
                int width = f.ByteWidth;
                byte* values = (byte*)export.Allocate((long)count * width);
                ReadOnlySpan<byte> source = data.Buffers[1].Span.Slice(data.Offset * width, length * width);
                long at = 0;
                var runs = new Runs(selected, length);
                while (runs.Next(out int start, out int n))
                {
                    source.Slice(start * width, n * width).CopyTo(new Span<byte>(values + at, n * width));
                    at += (long)n * width;
                }
                return export.NewArray(count, nulls, [validity, (nint)values], []);
            }
            case FixedWidthType f:
                return export.NewArray(count, nulls, [validity, GatherFixed(export, data.Buffers[1].Span, data.Offset, f.BitWidth / 8, selected, length, count)], []);
            case StringType or BinaryType:
                return Offsets(export, data, selected, count, validity, nulls);
            case StringViewType or BinaryViewType:
                return Views(export, data, selected, count, validity, nulls, shareViewData ?? count >= length * ShareViewDataFrom);
            default:
                throw new NotSupportedException($"Filtering {data.DataType.Name} arrays is not supported (flat arrays only).");
        }
    }

    /// <summary>A dictionary for the filtered array: the input's, shared, or else copied whole.</summary>
    private static NativeCArray* Dictionary(NativeBatchExport export, ArrayData dictionary)
    {
        if (dictionary.Dictionary is null && dictionary.Children is null or { Length: 0 })
        {
            var pointers = new nint[dictionary.Buffers.Length];
            bool shared = true;
            for (int b = 0; b < pointers.Length && shared; b++)
                shared = dictionary.Buffers[b].IsEmpty || export.TryShare(dictionary.Buffers[b], out pointers[b]);
            if (shared)
            {
                NativeCArray* array = export.NewArray(dictionary.Length, dictionary.NullCount, pointers, []);
                array->Offset = dictionary.Offset;
                return array;
            }
        }
        ulong[] all = new ulong[Masks.Words(dictionary.Length)];
        Masks.Fill(all, dictionary.Length, true);
        return Build(export, dictionary, all, dictionary.Length, shareViewData: true);
    }

    /// <summary>Gathers the selected bits of a bitmap (from <paramref name="firstBit"/>) into <paramref name="target"/>; returns how many are set.</summary>
    private static int GatherBits(ReadOnlySpan<byte> bitmap, long firstBit, ReadOnlySpan<ulong> selected, int length, ulong* target)
    {
        ulong pending = 0;
        int filled = 0, set = 0, words = Masks.Words(length);
        for (int w = 0; w < words; w++)
        {
            ulong mask = Word(selected, w, length);
            if (mask == 0) continue;
            ulong source = Bits.Word(bitmap, firstBit + 64L * w), bits;
            int n;
            if (mask == ulong.MaxValue) (bits, n) = (source, 64);
            else
            {
                (bits, n) = (0, 0);
                for (; mask != 0; mask &= mask - 1, n++) bits |= (source >> BitOperations.TrailingZeroCount(mask) & 1) << n;
            }
            set += BitOperations.PopCount(bits);
            pending |= bits << filled;
            int total = filled + n;
            if (total >= 64)
            {
                *target++ = pending;
                pending = filled == 0 ? 0 : bits >> (64 - filled);
                total -= 64;
            }
            filled = total;
        }
        if (filled > 0) *target = pending;
        return set;
    }

    private static nint GatherFixed(NativeBatchExport export, ReadOnlySpan<byte> buffer, int offset, int width, ReadOnlySpan<ulong> selected, int length, int count)
    {
        void* values = export.Allocate((long)count * width);
        switch (width)
        {
            case 1: Gather(buffer.CastTo<byte>().Slice(offset, length), selected, (byte*)values); break;
            case 2: Gather(buffer.CastTo<ushort>().Slice(offset, length), selected, (ushort*)values); break;
            case 4: Gather(buffer.CastTo<uint>().Slice(offset, length), selected, (uint*)values); break;
            case 8: Gather(buffer.CastTo<ulong>().Slice(offset, length), selected, (ulong*)values); break;
            case 16: Gather(buffer.CastTo<Int128>().Slice(offset, length), selected, (Int128*)values); break;
            case 32: Gather(buffer.CastTo<Int256Bits>().Slice(offset, length), selected, (Int256Bits*)values); break;
            default: throw new NotSupportedException($"Values {width} bytes wide.");
        }
        return (nint)values;
    }

    /// <summary>Copies the selected values to <paramref name="target"/>, in order.</summary>
    private static void Gather<T>(ReadOnlySpan<T> source, ReadOnlySpan<ulong> selected, T* target) where T : unmanaged
    {
        int length = source.Length, words = Masks.Words(length);
        ref T from = ref MemoryMarshal.GetReference(source);
        for (int w = 0; w < words; w++)
        {
            ulong mask = Word(selected, w, length);
            if (mask == 0) continue;
            int first = w << 6;
            if (mask == ulong.MaxValue)
            {
                MemoryMarshal.CreateReadOnlySpan(ref Unsafe.Add(ref from, first), 64).CopyTo(new Span<T>(target, 64));
                target += 64;
                continue;
            }
            for (; mask != 0; mask &= mask - 1) *target++ = Unsafe.Add(ref from, first + BitOperations.TrailingZeroCount(mask));
        }
    }

    private static NativeCArray* Offsets(NativeBatchExport export, ArrayData data, ReadOnlySpan<ulong> selected, int count, nint validity, long nulls)
    {
        int length = data.Length;
        ReadOnlySpan<int> offsets = data.Buffers[1].Span.CastTo<int>().Slice(data.Offset, length + 1);
        ReadOnlySpan<byte> bytes = data.Buffers[2].Span;
        long total = 0;
        var runs = new Runs(selected, length);
        while (runs.Next(out int start, out int n)) total += offsets[start + n] - offsets[start];
        var targetOffsets = (int*)export.Allocate((count + 1L) * sizeof(int));
        byte* targetBytes = (byte*)export.Allocate(total);
        int at = 0, o = 0;
        targetOffsets[o++] = 0;
        runs = new Runs(selected, length);
        while (runs.Next(out int start, out int n))
        {
            int begin = offsets[start], size = offsets[start + n] - begin;
            bytes.Slice(begin, size).CopyTo(new Span<byte>(targetBytes + at, size));
            for (int i = 1; i <= n; i++) targetOffsets[o++] = at + offsets[start + i] - begin;
            at += size;
        }
        return export.NewArray(count, nulls, [validity, (nint)targetOffsets, (nint)targetBytes], []);
    }

    private static NativeCArray* Views(NativeBatchExport export, ArrayData data, ReadOnlySpan<ulong> selected, int count, nint validity, long nulls, bool share)
    {
        int length = data.Length, buffers = data.Buffers.Length - 2;
        var views = (Int128*)GatherFixed(export, data.Buffers[1].Span, data.Offset, 16, selected, length, count);
        if (buffers == 0) return export.NewArray(count, nulls, [validity, (nint)views], []);
        if (share)
        {
            var pointers = new nint[2 + buffers];
            (pointers[0], pointers[1]) = (validity, (nint)views);
            bool shared = true;
            for (int b = 0; b < buffers && shared; b++) shared = export.TryShare(data.Buffers[2 + b], out pointers[2 + b]);
            if (shared) return export.NewArray(count, nulls, pointers, []);
        }

        // Copy the long values kept into buffers of their own (a new one before an offset would pass 2 GB).
        var source = new ReadOnlyMemory<byte>[buffers];
        for (int b = 0; b < buffers; b++) source[b] = data.Buffers[2 + b].Memory;
        var valid = (ulong*)validity;
        if (valid != null)
        {
            // Nulls point nowhere: a foreign null view may name a buffer the result does not have.
            for (int i = 0; i < count; i++)
            {
                if ((valid[i >> 6] >> (i & 63) & 1) == 0) views[i] = default;
            }
        }
        List<long> sizes = [];
        long current = 0;
        for (int i = 0; i < count; i++)
        {
            int size = LongLength(views, valid, i);
            if (size == 0) continue;
            if (sizes.Count == 0 || current + size > int.MaxValue)
            {
                if (sizes.Count > 0) sizes[^1] = current;
                sizes.Add(0);
                current = 0;
            }
            current += size;
        }
        if (sizes.Count == 0) return export.NewArray(count, nulls, [validity, (nint)views], []);
        sizes[^1] = current;
        var targets = new nint[2 + sizes.Count];
        (targets[0], targets[1]) = (validity, (nint)views);
        for (int b = 0; b < sizes.Count; b++) targets[2 + b] = (nint)export.Allocate(sizes[b]);
        int buffer = 0, offset = 0;
        for (int i = 0; i < count; i++)
        {
            int size = LongLength(views, valid, i);
            if (size == 0) continue;
            if (offset + (long)size > sizes[buffer]) (buffer, offset) = (buffer + 1, 0);
            var view = new Span<byte>(views + i, 16);
            BinaryKernels.Long(view, source).CopyTo(new Span<byte>((byte*)targets[2 + buffer] + offset, size));
            MemoryMarshal.Write(view[8..], buffer);
            MemoryMarshal.Write(view[12..], offset);
            offset += size;
        }
        return export.NewArray(count, nulls, targets, []);

        // The length of a valid view's long value (0 for nulls and inline values).
        static int LongLength(Int128* views, ulong* valid, int i)
        {
            if (valid != null && (valid[i >> 6] >> (i & 63) & 1) == 0) return 0;
            int size = *(int*)(views + i);
            return size > BytesTests.MaxInline ? size : 0;
        }
    }

    /// <summary>The runs of set bits of a row mask, in order.</summary>
    private ref struct Runs(ReadOnlySpan<ulong> mask, int length)
    {
        private readonly ReadOnlySpan<ulong> _mask = mask;
        private int _at;

        public bool Next(out int start, out int count)
        {
            start = Find(true, _at);
            if (start >= length)
            {
                count = 0;
                return false;
            }
            int end = Find(false, start);
            count = end - start;
            _at = end;
            return true;
        }

        private readonly int Find(bool set, int from)
        {
            int words = Masks.Words(length), w = from >> 6;
            if (w >= words) return length;
            ulong word = (set ? _mask[w] : ~_mask[w]) & (ulong.MaxValue << (from & 63));
            while (word == 0)
            {
                if (++w >= words) return length;
                word = set ? _mask[w] : ~_mask[w];
            }
            return Math.Min(length, (w << 6) + BitOperations.TrailingZeroCount(word));
        }
    }
}
