using System.Numerics;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.ExceptionServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Interop;
using Kaminari.Sazanami.Arrow.TypeSystem;
using Kaminari.Sazanami.Kernels;

namespace Kaminari.Sazanami.Arrow.Compute;

/// <summary>
/// Concatenates record batches of one schema into one batch over native memory of its own (freed when it is disposed,
/// as the arrays of <see cref="NativeBatchExport"/>): flat columns, and structs, lists, large lists, maps and fixed-size
/// lists of them. The batches are left as they are; they must stay alive while they are concatenated.
/// </summary>
/// <remarks>
/// The output is planned first, on the calling thread: its buffers allocated, each batch's place in them fixed. The
/// copies then run side by side, a piece per batch and buffer, on up to the threads asked (so a few wide columns use
/// every thread, which concatenating column by column does not). Bitmaps (validity, booleans) are copied while planning:
/// batches' bits meet inside words, and they are small. The long values of views are not copied: the output shares the
/// batches' data buffers (when their memory is native: <see cref="NativeBatchExport.TryShare"/>; other data buffers are
/// copied whole), the views renumbered to them.
/// </remarks>
internal static unsafe class ArrowConcat
{
    /// <summary>The longest value a view holds inline (longer ones name a data buffer).</summary>
    private const int MaxInlineView = 12;

    /// <summary>
    /// One batch of every row of <paramref name="batches"/> (one schema), in order, the copies run on up to
    /// <paramref name="maxDegreeOfParallelism"/> threads.
    /// </summary>
    /// <exception cref="ArgumentException">No batches, or batches of other schemas (column counts and types).</exception>
    /// <exception cref="OverflowException">More rows, bytes or list elements than an array takes (2 GB / <see cref="int.MaxValue"/>).</exception>
    /// <exception cref="NotSupportedException">A type without a native layout here (dictionaries, list views, unions, run-end
    /// encoded).</exception>
    public static RecordBatch Concatenate(IReadOnlyList<RecordBatch> batches, int maxDegreeOfParallelism)
    {
        ArgumentNullException.ThrowIfNull(batches);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxDegreeOfParallelism, 1);
        if (batches.Count == 0) throw new ArgumentException("There is no batch to concatenate.", nameof(batches));
        Schema schema = batches[0].Schema;
        int columns = schema.FieldsList.Count;
        long rows = 0;
        foreach (RecordBatch batch in batches)
        {
            if (batch.ColumnCount != columns)
                throw new ArgumentException($"A batch has {batch.ColumnCount} columns, the first {columns}.", nameof(batches));
            for (int c = 0; c < columns; c++)
            {
                if (!ArrowTypes.SameType(batch.Column(c).Data.DataType, schema.FieldsList[c].DataType))
                    throw new ArgumentException($"Column '{schema.FieldsList[c].Name}' is {batch.Column(c).Data.DataType.Name} in a batch, {schema.FieldsList[c].DataType.Name} in the first.", nameof(batches));
            }
            rows += batch.Length;
        }
        if (rows > int.MaxValue) throw new OverflowException($"The batches hold {rows} rows, more than a batch takes.");

        var export = new NativeBatchExport();
        var pieces = new Pieces();
        try
        {
            var children = new nint[columns];
            var parts = new ArrayData[batches.Count];
            for (int c = 0; c < columns; c++)
            {
                for (int b = 0; b < parts.Length; b++) parts[b] = batches[b].Column(c).Data;
                children[c] = (nint)Plan(export, parts, pieces);
            }
            Copy(pieces, maxDegreeOfParallelism);
            return export.Import(export.NewArray(rows, 0, [], children), schema);
        }
        catch
        {
            export.FreeAll();
            throw;
        }
        finally
        {
            pieces.Dispose();
        }
    }

    // ---- plan ---------------------------------------------------------------------------------------------------

    /// <summary>What a piece copies: bytes as they are, offsets moved by a delta, or views renumbered to the shared buffers.</summary>
    private enum Kind : byte { Bytes, Offsets32, Offsets64, Views }

    /// <summary>
    /// A copy of one batch's part of a buffer: <see cref="Count"/> bytes (offsets, views) from <see cref="From"/> to
    /// <see cref="To"/>; offsets get <see cref="Delta"/> added, long views <see cref="Delta"/> added to their buffer
    /// numbers (null views, where <see cref="Validity"/> says so from <see cref="ValidityBit"/>, cleared).
    /// </summary>
    private readonly record struct Piece(Kind Kind, nint From, nint To, long Count, long Delta = 0, nint Validity = 0, long ValidityBit = 0);

    /// <summary>The pieces to copy, and the batches' buffers pinned for them (managed memory included) until they are copied.</summary>
    private sealed class Pieces : List<Piece>, IDisposable
    {
        private readonly List<System.Buffers.MemoryHandle> _pins = [];

        /// <summary>The address of byte <paramref name="at"/> of a buffer, pinned until the pieces are disposed.</summary>
        public nint Start(ArrowBuffer buffer, long at)
        {
            System.Buffers.MemoryHandle pin = buffer.Memory.Pin();
            _pins.Add(pin);
            return (nint)((byte*)pin.Pointer + at);
        }

        public void Dispose()
        {
            foreach (System.Buffers.MemoryHandle pin in _pins) pin.Dispose();
            _pins.Clear();
        }
    }

    /// <summary>The output array of <paramref name="parts"/> (one type), its buffers allocated, its pieces noted.</summary>
    private static NativeCArray* Plan(NativeBatchExport export, ArrayData[] parts, Pieces pieces)
    {
        IArrowType type = parts[0].DataType;
        long total = 0;
        foreach (ArrayData part in parts) total += part.Length;
        if (type is NullType) return export.NewArray(total, total, [], []);
        (nint validity, long nulls) = Validity(export, parts, total);

        switch (type)
        {
            case DictionaryType:
                throw new NotSupportedException("Concatenating dictionary arrays is not supported (their dictionaries would have to be unified).");
            case BooleanType:
            {
                var values = (byte*)export.Allocate(((total + 63) >> 6) * 8);
                long at = 0;
                foreach (ArrayData part in parts)
                {
                    Bits.Copy(part.Buffers[1].Span, part.Offset, new Span<byte>(values, (int)((total + 7) >> 3)), at, part.Length);
                    at += part.Length;
                }
                return export.NewArray(total, nulls, [validity, (nint)values], []);
            }
            case FixedSizeBinaryType f:
                return export.NewArray(total, nulls, [validity, Fixed(export, parts, total, f.ByteWidth, pieces)], []);
            case FixedWidthType f when f.BitWidth % 8 == 0:
                return export.NewArray(total, nulls, [validity, Fixed(export, parts, total, f.BitWidth / 8, pieces)], []);
            case StringType or BinaryType:
                return Offsets<int>(export, parts, total, validity, nulls, pieces, Kind.Offsets32, values: true);
            case LargeStringType or LargeBinaryType:
                return Offsets<long>(export, parts, total, validity, nulls, pieces, Kind.Offsets64, values: true);
            case StringViewType or BinaryViewType:
                return Views(export, parts, total, validity, nulls, pieces);
            case StructType s:
            {
                var children = new nint[s.Fields.Count];
                var childParts = new ArrayData[parts.Length];
                for (int c = 0; c < children.Length; c++)
                {
                    for (int p = 0; p < parts.Length; p++) childParts[p] = parts[p].Children[c].Slice(parts[p].Offset, parts[p].Length);
                    children[c] = (nint)Plan(export, childParts, pieces);
                }
                return export.NewArray(total, nulls, [validity], children);
            }
            case ListType or MapType:
                return Offsets<int>(export, parts, total, validity, nulls, pieces, Kind.Offsets32, values: false);
            case LargeListType:
                return Offsets<long>(export, parts, total, validity, nulls, pieces, Kind.Offsets64, values: false);
            case FixedSizeListType f:
            {
                var childParts = new ArrayData[parts.Length];
                for (int p = 0; p < parts.Length; p++)
                    childParts[p] = parts[p].Children[0].Slice(parts[p].Offset * f.ListSize, parts[p].Length * f.ListSize);
                return export.NewArray(total, nulls, [validity], [(nint)Plan(export, childParts, pieces)]);
            }
            default:
                throw new NotSupportedException($"Concatenating {type.Name} arrays is not supported.");
        }
    }

    /// <summary>The validity bitmap of the parts (copied now) and the nulls; none (0) when no part has a null.</summary>
    private static (nint Bitmap, long Nulls) Validity(NativeBatchExport export, ArrayData[] parts, long total)
    {
        bool any = false;
        foreach (ArrayData part in parts) any |= part.NullCount != 0 && !part.Buffers[0].IsEmpty;
        if (!any) return (0, 0);
        var bitmap = (byte*)export.Allocate(((total + 63) >> 6) * 8);
        var target = new Span<byte>(bitmap, (int)((total + 7) >> 3));
        long at = 0;
        foreach (ArrayData part in parts)
        {
            if (part.NullCount != 0 && !part.Buffers[0].IsEmpty) Bits.Copy(part.Buffers[0].Span, part.Offset, target, at, part.Length);
            else Bits.Fill(target, at, part.Length, true);
            at += part.Length;
        }
        long nulls = total - Bits.Count(target, 0, total);
        return nulls == 0 ? (0, 0) : ((nint)bitmap, nulls);
    }

    /// <summary>Values <paramref name="width"/> bytes wide: a piece per part.</summary>
    private static nint Fixed(NativeBatchExport export, ArrayData[] parts, long total, int width, Pieces pieces)
    {
        var values = (byte*)export.AllocateUninitialized(total * width);
        long at = 0;
        foreach (ArrayData part in parts)
        {
            if (part.Length > 0)
                pieces.Add(new Piece(Kind.Bytes, pieces.Start(part.Buffers[1], (long)part.Offset * width), (nint)(values + at * width), (long)part.Length * width));
            at += part.Length;
        }
        return (nint)values;
    }

    /// <summary>
    /// Offsets (strings and binaries with their bytes, when <paramref name="values"/>; lists, maps and large lists with
    /// their elements, concatenated in turn): each part's offsets moved to where its values land.
    /// </summary>
    private static NativeCArray* Offsets<TOffset>(NativeBatchExport export, ArrayData[] parts, long total, nint validity, long nulls,
        Pieces pieces, Kind kind, bool values) where TOffset : unmanaged, IBinaryInteger<TOffset>
    {
        var offsets = (TOffset*)export.AllocateUninitialized((total + 1) * sizeof(TOffset));
        offsets[0] = TOffset.Zero;
        long rows = 0, at = 0; // rows and values (bytes, elements) before the part
        var ranges = new (long First, long Count)[parts.Length];
        for (int p = 0; p < parts.Length; p++)
        {
            ArrayData part = parts[p];
            ReadOnlySpan<TOffset> own = part.Buffers[1].Span.CastTo<TOffset>().Slice(part.Offset, part.Length + 1);
            long first = long.CreateChecked(own[0]), count = long.CreateChecked(own[^1]) - first;
            ranges[p] = (first, count);
            if (part.Length > 0)
                pieces.Add(new Piece(kind, pieces.Start(part.Buffers[1], ((long)part.Offset + 1) * sizeof(TOffset)), (nint)(offsets + rows + 1),
                    (long)part.Length * sizeof(TOffset), at - first));
            rows += part.Length;
            at += count;
        }
        if (typeof(TOffset) == typeof(int) && at > int.MaxValue)
            throw new OverflowException($"The batches hold {at} {(values ? "bytes" : "list elements")}, more than 32-bit offsets reach.");

        if (values)
        {
            var bytes = (byte*)export.AllocateUninitialized(at);
            long to = 0;
            for (int p = 0; p < parts.Length; p++)
            {
                if (ranges[p].Count > 0) pieces.Add(new Piece(Kind.Bytes, pieces.Start(parts[p].Buffers[2], ranges[p].First), (nint)(bytes + to), ranges[p].Count));
                to += ranges[p].Count;
            }
            return export.NewArray(total, nulls, [validity, (nint)offsets, (nint)bytes], []);
        }
        if (at > int.MaxValue) throw new OverflowException($"The batches hold {at} list elements, more than an array takes.");
        var childParts = new ArrayData[parts.Length];
        for (int p = 0; p < parts.Length; p++) childParts[p] = parts[p].Children[0].Slice((int)ranges[p].First, (int)ranges[p].Count);
        return export.NewArray(total, nulls, [validity, (nint)offsets], [(nint)Plan(export, childParts, pieces)]);
    }

    /// <summary>Views: copied, renumbered to the parts' data buffers, which are shared.</summary>
    private static NativeCArray* Views(NativeBatchExport export, ArrayData[] parts, long total, nint validity, long nulls, Pieces pieces)
    {
        var views = (byte*)export.AllocateUninitialized(total * 16);
        List<nint> buffers = [validity, (nint)views];
        long at = 0;
        foreach (ArrayData part in parts)
        {
            int before = buffers.Count - 2;
            for (int b = 2; b < part.Buffers.Length; b++)
            {
                // Native data is shared; other data (managed memory) copied whole into a buffer of the output's.
                if (!export.TryShare(part.Buffers[b], out nint shared))
                {
                    long bytes = part.Buffers[b].Length;
                    shared = (nint)export.AllocateUninitialized(bytes);
                    if (bytes > 0) pieces.Add(new Piece(Kind.Bytes, pieces.Start(part.Buffers[b], 0), shared, bytes));
                }
                buffers.Add(shared);
            }
            if (part.Length > 0)
            {
                bool hasNulls = part.NullCount != 0 && !part.Buffers[0].IsEmpty;
                pieces.Add(new Piece(Kind.Views, pieces.Start(part.Buffers[1], (long)part.Offset * 16), (nint)(views + at * 16), (long)part.Length * 16, before,
                    hasNulls ? pieces.Start(part.Buffers[0], 0) : 0, part.Offset));
            }
            at += part.Length;
        }
        return export.NewArray(total, nulls, [.. buffers], []);
    }

    // ---- copy ---------------------------------------------------------------------------------------------------

    private static void Copy(Pieces pieces, int maxDegreeOfParallelism)
    {
        if (maxDegreeOfParallelism == 1 || pieces.Count < 2)
        {
            foreach (Piece piece in pieces) Run(piece);
            return;
        }
        try
        {
            Parallel.For(0, pieces.Count, new ParallelOptions { MaxDegreeOfParallelism = maxDegreeOfParallelism }, i => Run(pieces[i]));
        }
        catch (AggregateException e) when (e.InnerExceptions is [Exception first, ..])
        {
            ExceptionDispatchInfo.Throw(first);
        }
    }

    private static void Run(Piece piece)
    {
        switch (piece.Kind)
        {
            case Kind.Bytes:
                Buffer.MemoryCopy((void*)piece.From, (void*)piece.To, piece.Count, piece.Count);
                break;
            case Kind.Offsets32:
                Shift(new ReadOnlySpan<int>((void*)piece.From, (int)(piece.Count / sizeof(int))), new Span<int>((void*)piece.To, (int)(piece.Count / sizeof(int))),
                    (int)piece.Delta);
                break;
            case Kind.Offsets64:
                Shift(new ReadOnlySpan<long>((void*)piece.From, (int)(piece.Count / sizeof(long))), new Span<long>((void*)piece.To, (int)(piece.Count / sizeof(long))),
                    piece.Delta);
                break;
            case Kind.Views:
                Views(piece);
                break;
        }
    }

    /// <summary>Offsets copied with <paramref name="delta"/> added (vectorized where the hardware allows).</summary>
    private static void Shift<T>(ReadOnlySpan<T> from, Span<T> to, T delta) where T : unmanaged, IBinaryInteger<T>
    {
        int i = 0;
        if (Vector256.IsHardwareAccelerated && from.Length >= Vector256<T>.Count)
        {
            var add = Vector256.Create(delta);
            for (; i <= from.Length - Vector256<T>.Count; i += Vector256<T>.Count)
                (Vector256.LoadUnsafe(ref MemoryMarshal.GetReference(from), (nuint)i) + add).StoreUnsafe(ref MemoryMarshal.GetReference(to), (nuint)i);
        }
        else if (Vector128.IsHardwareAccelerated && from.Length >= Vector128<T>.Count)
        {
            var add = Vector128.Create(delta);
            for (; i <= from.Length - Vector128<T>.Count; i += Vector128<T>.Count)
                (Vector128.LoadUnsafe(ref MemoryMarshal.GetReference(from), (nuint)i) + add).StoreUnsafe(ref MemoryMarshal.GetReference(to), (nuint)i);
        }
        for (; i < from.Length; i++) to[i] = from[i] + delta;
    }

    /// <summary>
    /// Views copied whole, then the long ones renumbered (unless the part's buffers come first) and the nulls cleared
    /// (a null names no buffer).
    /// </summary>
    private static void Views(Piece piece)
    {
        Buffer.MemoryCopy((void*)piece.From, (void*)piece.To, piece.Count, piece.Count);
        var views = (int*)piece.To; // four ints a view: length, prefix, buffer, offset
        long n = piece.Count / 16;
        int delta = (int)piece.Delta;
        if (delta != 0)
        {
            for (long i = 0; i < n; i++)
            {
                int* view = views + 4 * i;
                if (view[0] > MaxInlineView) view[2] += delta;
            }
        }
        if (piece.Validity == 0) return;
        var valid = new ReadOnlySpan<byte>((void*)piece.Validity, (int)((piece.ValidityBit + n + 7) >> 3));
        for (long start = 0; start < n; start += 64)
        {
            int count = (int)Math.Min(64, n - start);
            ulong nulls = ~Bits.Word(valid, piece.ValidityBit + start) & (count == 64 ? ulong.MaxValue : (1UL << count) - 1);
            for (; nulls != 0; nulls &= nulls - 1) ((Int128*)views)[start + BitOperations.TrailingZeroCount(nulls)] = default;
        }
    }
}
