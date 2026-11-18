using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.C;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Interop;

namespace Sazanami.Tests;

/// <summary>
/// <see cref="ArrowCImport"/> against Apache.Arrow's importer (the oracle, allowed here only) on every layout: the
/// same export (by <see cref="ArrowCExport"/>) imported both ways must give the same arrays, buffer for buffer, over
/// the exported array's own memory; and the memory's lifetime both ways.
/// </summary>
public unsafe class ArrowCImportTests
{
    // ---- arrays of every layout ----

    private static ArrowBuffer Buf<T>(params T[] values) where T : unmanaged => new(MemoryMarshal.AsBytes(values.AsSpan()).ToArray());

    /// <summary>A validity bitmap: a slot is null where <paramref name="nulls"/> says so.</summary>
    private static ArrowBuffer Validity(int length, Func<int, bool> isNull)
    {
        var bits = new byte[(length + 7) / 8];
        for (int i = 0; i < length; i++)
            if (!isNull(i)) bits[i / 8] |= (byte)(1 << (i % 8));
        return new(bits);
    }

    private static ArrayData Fixed(IArrowType type, int width, int length, bool nulls = true)
    {
        var values = new byte[length * width];
        new Random(width * 31 + length).NextBytes(values);
        return new ArrayData(type, length, nulls ? (length + 2) / 3 : 0, 0,
            [nulls ? Validity(length, i => i % 3 == 0) : ArrowBuffer.Empty, new ArrowBuffer(values)]);
    }

    private static IArrowArray Strings(int n) =>
        new StringArray.Builder().AppendRange(Enumerable.Range(0, n).Select(i => i % 4 == 0 ? null : $"value {i}")).Build();

    private static IArrowArray Views(int n)
    {
        var b = new StringViewArray.Builder();
        // Short ones inline in the view, long ones in the data buffers.
        for (int i = 0; i < n; i++)
        {
            if (i % 5 == 0) b.AppendNull();
            else b.Append(i % 2 == 0 ? $"s{i}" : $"a longer string than twelve bytes, number {i}");
        }
        return b.Build();
    }

    private static IArrowArray Ints(int n) => new Int32Array.Builder().AppendRange(Enumerable.Range(0, n)).Build();

    // Not the null type: Apache.Arrow's importer, the oracle, fails on it (GC memory pressure of 0); see NullArrays_Import.
    public static TheoryData<string> Layouts() => [.. AllArrays().Select(a => a.Name).Where(n => n != "null")];

    private static IEnumerable<(string Name, Func<IArrowArray> Make)> AllArrays()
    {
        const int N = 70; // not a multiple of 8 or 64
        yield return ("int8", () => ArrowArrayFactory.BuildArray(Fixed(Int8Type.Default, 1, N)));
        yield return ("uint16", () => ArrowArrayFactory.BuildArray(Fixed(UInt16Type.Default, 2, N)));
        yield return ("int32", () => ArrowArrayFactory.BuildArray(Fixed(Int32Type.Default, 4, N)));
        yield return ("uint64 no nulls", () => ArrowArrayFactory.BuildArray(Fixed(UInt64Type.Default, 8, N, nulls: false)));
        yield return ("half float", () => ArrowArrayFactory.BuildArray(Fixed(HalfFloatType.Default, 2, N)));
        yield return ("double", () => ArrowArrayFactory.BuildArray(Fixed(DoubleType.Default, 8, N)));
        yield return ("bool", () =>
        {
            var b = new BooleanArray.Builder();
            for (int i = 0; i < N; i++)
            {
                if (i % 3 == 0) b.AppendNull();
                else b.Append(i % 2 == 0);
            }
            return b.Build();
        });
        yield return ("date32", () => ArrowArrayFactory.BuildArray(Fixed(Date32Type.Default, 4, N)));
        yield return ("date64", () => ArrowArrayFactory.BuildArray(Fixed(Date64Type.Default, 8, N)));
        yield return ("time32", () => ArrowArrayFactory.BuildArray(Fixed(new Time32Type(TimeUnit.Millisecond), 4, N)));
        yield return ("time64", () => ArrowArrayFactory.BuildArray(Fixed(new Time64Type(TimeUnit.Nanosecond), 8, N)));
        yield return ("timestamp", () => ArrowArrayFactory.BuildArray(Fixed(new TimestampType(TimeUnit.Microsecond, "UTC"), 8, N)));
        yield return ("duration", () => ArrowArrayFactory.BuildArray(Fixed(DurationType.Millisecond, 8, N)));
        yield return ("interval year-month", () => ArrowArrayFactory.BuildArray(Fixed(new IntervalType(IntervalUnit.YearMonth), 4, N)));
        yield return ("interval day-time", () => ArrowArrayFactory.BuildArray(Fixed(new IntervalType(IntervalUnit.DayTime), 8, N)));
        yield return ("interval month-day-nano", () => ArrowArrayFactory.BuildArray(Fixed(new IntervalType(IntervalUnit.MonthDayNanosecond), 16, N)));
        yield return ("decimal32", () => ArrowArrayFactory.BuildArray(Fixed(new Decimal32Type(9, 2), 4, N)));
        yield return ("decimal64", () => ArrowArrayFactory.BuildArray(Fixed(new Decimal64Type(18, 2), 8, N)));
        yield return ("decimal128", () => ArrowArrayFactory.BuildArray(Fixed(new Decimal128Type(38, 4), 16, N)));
        yield return ("decimal256", () => ArrowArrayFactory.BuildArray(Fixed(new Decimal256Type(76, 4), 32, N)));
        yield return ("fixed size binary", () => ArrowArrayFactory.BuildArray(Fixed(new FixedSizeBinaryType(5), 5, N)));
        yield return ("null", () => new NullArray(N));
        yield return ("string", () => Strings(N));
        yield return ("binary", () => new BinaryArray.Builder().AppendRange(Enumerable.Range(0, N).Select(i => (byte[])[(byte)i, 1, 2])).Build());
        yield return ("large string", () => new LargeStringArray.Builder().AppendRange(Enumerable.Range(0, N).Select(i => i % 4 == 0 ? null : $"large {i}")).Build());
        yield return ("large binary", () => new LargeBinaryArray.Builder().AppendRange(Enumerable.Range(0, N).Select(i => (byte[])[(byte)i])).Build());
        yield return ("string view", () => Views(N));
        yield return ("binary view", () =>
        {
            var b = new BinaryViewArray.Builder();
            for (int i = 0; i < N; i++) b.Append([.. Enumerable.Repeat((byte)i, i % 20)]);
            return b.Build();
        });
        yield return ("empty string", () => new StringArray.Builder().Build());
        yield return ("empty view", () => new StringViewArray.Builder().Build());
        yield return ("list", () =>
        {
            var b = new ListArray.Builder(Int32Type.Default);
            var values = (Int32Array.Builder)b.ValueBuilder;
            for (int i = 0; i < N; i++)
            {
                if (i % 7 == 0) { b.AppendNull(); continue; }
                b.Append();
                for (int k = 0; k < i % 4; k++) values.Append(i * 10 + k);
            }
            return b.Build();
        });
        yield return ("large list", () =>
        {
            IArrowArray values = Strings(30);
            long[] offsets = [.. Enumerable.Range(0, 11).Select(i => (long)i * 3)];
            return new LargeListArray(new ArrayData(new LargeListType(StringType.Default), 10, 0, 0, [ArrowBuffer.Empty, Buf(offsets)], [values.Data]));
        });
        yield return ("list view", () =>
            new ListViewArray(new ListViewType(Int32Type.Default), 4, Buf(0, 5, 2, 7), Buf(3, 0, 4, 1), Ints(10), ArrowBuffer.Empty, 0, 0));
        yield return ("large list view", () => new LargeListViewArray(new ArrayData(new LargeListViewType(Int32Type.Default), 4, 0, 0,
            [ArrowBuffer.Empty, Buf(0L, 5, 2, 7), Buf(3L, 0, 4, 1)], [Ints(10).Data])));
        yield return ("fixed size list", () => new FixedSizeListArray(new ArrayData(new FixedSizeListType(Int32Type.Default, 3), 10, 2, 0,
            [Validity(10, i => i % 5 == 0)], [Ints(30).Data])));
        yield return ("struct", () =>
        {
            var type = new StructType([new Field("a", Int32Type.Default, true), new Field("b", StringViewType.Default, true)]);
            return new StructArray(type, N, [Ints(N), Views(N)], Validity(N, i => i % 9 == 0), (N + 8) / 9);
        });
        yield return ("map", () =>
        {
            var type = new MapType(StringType.Default, Int32Type.Default);
            var entries = new StructArray((StructType)type.KeyValueType, 6, [Strings(6), Ints(6)], ArrowBuffer.Empty, 0);
            return new MapArray(type, 3, Buf(0, 2, 2, 6), entries, ArrowBuffer.Empty, 0, 0);
        });
        yield return ("dictionary", () =>
        {
            var type = new DictionaryType(Int32Type.Default, StringType.Default, ordered: false);
            return new DictionaryArray(type, new Int32Array.Builder().AppendRange(Enumerable.Range(0, N).Select(i => i % 5)).Build(), Strings(5));
        });
        yield return ("dictionary of views", () =>
        {
            var type = new DictionaryType(UInt32Type.Default, StringViewType.Default, ordered: false);
            return new DictionaryArray(type, new UInt32Array.Builder().AppendRange(Enumerable.Range(0, N).Select(i => (uint)(i % 7))).Build(), Views(7));
        });
        yield return ("dense union", () =>
        {
            var type = new UnionType([new Field("i", Int32Type.Default, true), new Field("s", StringType.Default, true)], [0, 1], UnionMode.Dense);
            return new DenseUnionArray(type, 6, [Ints(3), Strings(3)], Buf<byte>(0, 1, 0, 1, 0, 1), Buf(0, 0, 1, 1, 2, 2), 0, 0);
        });
        yield return ("sparse union", () =>
        {
            var type = new UnionType([new Field("i", Int32Type.Default, true), new Field("s", StringType.Default, true)], [0, 1], UnionMode.Sparse);
            return new SparseUnionArray(type, 6, [Ints(6), Strings(6)], Buf<byte>(0, 1, 1, 0, 0, 1), 0, 0);
        });
        yield return ("run end encoded", () => new RunEndEncodedArray(new Int32Array(Buf(3, 4, 9), ArrowBuffer.Empty, 3, 0, 0), Strings(3)));
        yield return ("sliced int32", () => ArrowArrayFactory.Slice(ArrowArrayFactory.BuildArray(Fixed(Int32Type.Default, 4, N)), 5, 40));
        yield return ("sliced bool", () => ArrowArrayFactory.Slice(new BooleanArray.Builder().AppendRange(Enumerable.Range(0, N).Select(i => i % 3 == 0)).Build(), 3, 50));
        yield return ("sliced string", () => ArrowArrayFactory.Slice(Strings(N), 7, 20));
        yield return ("sliced view", () => ArrowArrayFactory.Slice(Views(N), 11, 30));
        yield return ("sliced list", () => ArrowArrayFactory.Slice(new ListArray(new ArrayData(new ListType(Int32Type.Default), 5, 0, 0,
            [ArrowBuffer.Empty, Buf(0, 2, 4, 6, 8, 10)], [Ints(10).Data])), 1, 3));
    }

    private static IArrowArray Make(string name) => AllArrays().Single(a => a.Name == name).Make();

    [Theory]
    [MemberData(nameof(Layouts))]
    public void EveryLayout_ImportsAsApacheArrowsImporterDoes(string layout)
    {
        IArrowArray array = Make(layout);
        CArrowArray* forApache = CArrowArray.Create(), forUs = CArrowArray.Create();
        try
        {
            ArrowCExport.ExportArray(array, forApache);
            ArrowCExport.ExportArray(array, forUs);
#pragma warning disable RS0030 // the oracle
            using IArrowArray expected = CArrowArrayImporter.ImportArray(forApache, array.Data.DataType);
#pragma warning restore RS0030
            using IArrowArray actual = ArrowCImport.ImportArray(forUs, array.Data.DataType);
            Assert.Equal(expected.GetType(), actual.GetType());
            AssertSame(expected.Data, actual.Data, layout);
            AssertExported(array.Data, actual.Data, layout); // the export handed on the array's own memory
        }
        finally
        {
            CArrowArray.Free(forApache);
            CArrowArray.Free(forUs);
            array.Dispose();
        }
    }

    private static void AssertSame(ArrayData expected, ArrayData actual, string where)
    {
        Assert.Equal(expected.DataType.TypeId, actual.DataType.TypeId);
        Assert.Equal(expected.DataType.Name, actual.DataType.Name);
        Assert.Equal(expected.Length, actual.Length);
        Assert.Equal(expected.Offset, actual.Offset);
        Assert.Equal(expected.NullCount, actual.NullCount);
        Assert.True(expected.Buffers.Length == actual.Buffers.Length, $"{where}: {actual.Buffers.Length} buffers, Apache {expected.Buffers.Length}");
        for (int b = 0; b < expected.Buffers.Length; b++)
            Assert.True(expected.Buffers[b].Span.SequenceEqual(actual.Buffers[b].Span),
                $"{where}: buffer {b} differs ({actual.Buffers[b].Length} bytes, Apache {expected.Buffers[b].Length})");
        Assert.Equal(expected.Children?.Length ?? 0, actual.Children?.Length ?? 0);
        for (int c = 0; c < (expected.Children?.Length ?? 0); c++) AssertSame(expected.Children![c], actual.Children![c], $"{where}.child{c}");
        Assert.Equal(expected.Dictionary is null, actual.Dictionary is null);
        if (expected.Dictionary is not null) AssertSame(expected.Dictionary, actual.Dictionary!, $"{where}.dictionary");
    }

    /// <summary>The import reads the original array's memory (no copy), as much of each buffer as the layout covers.</summary>
    private static void AssertExported(ArrayData original, ArrayData imported, string where)
    {
        for (int b = 0; b < imported.Buffers.Length; b++)
        {
            ReadOnlySpan<byte> theirs = imported.Buffers[b].Span, ours = original.Buffers[b].Span;
            if (theirs.IsEmpty) continue;
            Assert.True(Unsafe.AreSame(ref MemoryMarshal.GetReference(theirs), ref MemoryMarshal.GetReference(ours)), $"{where}: buffer {b} was copied");
            Assert.True(theirs.Length <= ours.Length, $"{where}: buffer {b} goes past the original");
        }
        for (int c = 0; c < (imported.Children?.Length ?? 0); c++) AssertExported(original.Children![c], imported.Children![c], $"{where}.child{c}");
        if (imported.Dictionary is not null) AssertExported(original.Dictionary!, imported.Dictionary, $"{where}.dictionary");
    }

    [Fact]
    public void ARecordBatch_ImportsAsApacheArrowsImporterDoes()
    {
        string[] names = ["int32", "string view", "struct", "dictionary", "list"];
        var schema = new Schema([.. names.Select(n => new Field(n, Make(n).Data.DataType, true))], null);
        using var batch = new RecordBatch(schema, [.. names.Select(n => Make(n))], 70);
        CArrowArray* forApache = CArrowArray.Create(), forUs = CArrowArray.Create();
        try
        {
            ArrowCExport.ExportRecordBatch(batch, forApache);
            ArrowCExport.ExportRecordBatch(batch, forUs);
#pragma warning disable RS0030 // the oracle
            using RecordBatch expected = CArrowArrayImporter.ImportRecordBatch(forApache, schema);
#pragma warning restore RS0030
            using RecordBatch actual = ArrowCImport.ImportRecordBatch(forUs, schema);
            Assert.Equal(expected.Length, actual.Length);
            for (int c = 0; c < names.Length; c++) AssertSame(expected.Column(c).Data, actual.Column(c).Data, names[c]);
            // A schema of another width is refused.
            ArrowCExport.ExportRecordBatch(batch, forUs);
            Assert.Throws<InvalidDataException>(() => ArrowCImport.ImportRecordBatch(forUs, new Schema([schema.FieldsList[0]], null)));
        }
        finally
        {
            CArrowArray.Free(forApache);
            CArrowArray.Free(forUs);
        }
    }

    [Fact]
    public void NullArrays_Import()
    {
        s_releases = 0;
        var source = (NativeCArray*)NativeMemory.AllocZeroed((nuint)sizeof(NativeCArray));
        try
        {
            *source = new NativeCArray { Length = 9, NullCount = 9, Release = &CountRelease };
            using IArrowArray array = ArrowCImport.ImportArray((CArrowArray*)source, NullType.Default);
            Assert.IsType<NullArray>(array);
            Assert.Equal(9, array.Length);
            Assert.Equal(9, array.NullCount);
            Assert.Empty(array.Data.Buffers);
        }
        finally
        {
            NativeMemory.Free(source);
        }
        Assert.Equal(1, s_releases);
    }

    // ---- lifetime: a hand-made export counting its releases ----

    private static int s_releases;

    [UnmanagedCallersOnly]
    private static void CountRelease(NativeCArray* array)
    {
        Interlocked.Increment(ref s_releases);
        array->Release = null;
    }

    /// <summary>An int32 array of four values in native memory (the test frees it), released by <see cref="CountRelease"/>.</summary>
    private static (nint Values, nint Buffers) Export(NativeCArray* array, long bufferCount = 2)
    {
        var values = (int*)NativeMemory.Alloc(16);
        for (int i = 0; i < 4; i++) values[i] = i + 1;
        var buffers = (byte**)NativeMemory.AllocZeroed(16);
        buffers[1] = (byte*)values;
        *array = new NativeCArray { Length = 4, BufferCount = bufferCount, Buffers = buffers, Release = &CountRelease };
        return ((nint)values, (nint)buffers);
    }

    private static void Free((nint Values, nint Buffers) memory, NativeCArray* source)
    {
        NativeMemory.Free(source);
        NativeMemory.Free((void*)memory.Values);
        NativeMemory.Free((void*)memory.Buffers);
    }

    [Fact]
    public void TheArray_IsReleasedOnce_AfterItsLastBufferAndPin()
    {
        s_releases = 0;
        var source = (NativeCArray*)NativeMemory.AllocZeroed((nuint)sizeof(NativeCArray));
        var memory = Export(source);
        try
        {
            var array = (Int32Array)ArrowCImport.ImportArray((CArrowArray*)source, Int32Type.Default);
            Assert.True(source->Release == null); // moved
            Assert.Equal([1, 2, 3, 4], array.Values.ToArray());
            Assert.Equal(0, s_releases);

            // A pin (what an exporter handing the memory on takes) keeps it past the array.
            System.Buffers.MemoryHandle pin = array.ValueBuffer.Memory.Pin();
            array.Dispose();
            Assert.Equal(0, s_releases);
            Assert.Equal(3, ((int*)pin.Pointer)[2]);
            pin.Dispose();
            Assert.Equal(1, s_releases);
            array.Dispose();
            Assert.Equal(1, s_releases);
        }
        finally
        {
            Free(memory, source);
        }
    }

    [Fact]
    public void AFailedImport_ReleasesTheArrayAtOnce()
    {
        s_releases = 0;
        var source = (NativeCArray*)NativeMemory.AllocZeroed((nuint)sizeof(NativeCArray));
        var memory = Export(source, bufferCount: 3); // three buffers: not an int32 layout
        try
        {
            Assert.Throws<InvalidDataException>(() => ArrowCImport.ImportArray((CArrowArray*)source, Int32Type.Default));
            Assert.Equal(1, s_releases);
            Assert.True(source->Release == null);
            Assert.Throws<ArgumentException>(() => ArrowCImport.ImportArray((CArrowArray*)source, Int32Type.Default)); // released already
        }
        finally
        {
            Free(memory, source);
        }
    }

    [Fact]
    public void AnArrayDroppedUndisposed_IsReleasedByTheFinalizer()
    {
        s_releases = 0;
        var source = (NativeCArray*)NativeMemory.AllocZeroed((nuint)sizeof(NativeCArray));
        var memory = Export(source);
        try
        {
            ImportAndDrop(source);
            for (int i = 0; i < 10 && Volatile.Read(ref s_releases) == 0; i++)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }
            Assert.Equal(1, s_releases);
        }
        finally
        {
            Free(memory, source);
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ImportAndDrop(NativeCArray* source) => _ = ArrowCImport.ImportArray((CArrowArray*)source, Int32Type.Default).Length;

    // ---- export ----

    [Fact]
    public void AnExport_KeepsTheMemory_PastTheOriginalArray_UntilReleased()
    {
        s_releases = 0;
        var source = (NativeCArray*)NativeMemory.AllocZeroed((nuint)sizeof(NativeCArray));
        var memory = Export(source);
        var exported = (NativeCArray*)NativeMemory.AllocZeroed((nuint)sizeof(NativeCArray));
        try
        {
            IArrowArray imported = ArrowCImport.ImportArray((CArrowArray*)source, Int32Type.Default);
            ArrowCExport.ExportArray(imported, (CArrowArray*)exported);
            imported.Dispose();
            Assert.Equal(0, s_releases); // the export retains the producer's memory
            Assert.Equal(3, ((int*)exported->Buffers[1])[2]);
            exported->Release(exported);
            Assert.True(exported->Release == null);
            Assert.Equal(1, s_releases);
        }
        finally
        {
            NativeMemory.Free(exported);
            Free(memory, source);
        }
    }

    [Fact]
    public void AChildMovedOut_OutlivesItsParent()
    {
        s_releases = 0;
        var source = (NativeCArray*)NativeMemory.AllocZeroed((nuint)sizeof(NativeCArray));
        var memory = Export(source);
        var root = (NativeCArray*)NativeMemory.AllocZeroed((nuint)sizeof(NativeCArray));
        var moved = (NativeCArray*)NativeMemory.AllocZeroed((nuint)sizeof(NativeCArray));
        try
        {
            using (IArrowArray child = ArrowCImport.ImportArray((CArrowArray*)source, Int32Type.Default))
            {
                var type = new StructType([new Field("a", Int32Type.Default, true), new Field("b", StringType.Default, true)]);
                using var parent = new StructArray(type, 4, [child, Strings(4)], ArrowBuffer.Empty, 0);
                ArrowCExport.ExportArray(parent, (CArrowArray*)root);
            }
            Assert.Equal(2, root->ChildCount);
            // The consumer moves the first child out, then releases the parent: the child stays.
            *moved = *root->Children[0];
            root->Children[0]->Release = null;
            root->Release(root);
            Assert.Equal(0, s_releases);
            Assert.Equal(4, ((int*)moved->Buffers[1])[3]);
            moved->Release(moved);
            Assert.Equal(1, s_releases);
        }
        finally
        {
            NativeMemory.Free(root);
            NativeMemory.Free(moved);
            Free(memory, source);
        }
    }

    [Fact]
    public void Views_GoOutWithTheirBufferSizes_AndNullArraysRelease()
    {
        using IArrowArray views = Views(70);
        var exported = (NativeCArray*)NativeMemory.AllocZeroed((nuint)sizeof(NativeCArray));
        try
        {
            ArrowCExport.ExportArray(views, (CArrowArray*)exported);
            int variadic = views.Data.Buffers.Length - 2;
            Assert.True(variadic > 0);
            Assert.Equal(views.Data.Buffers.Length + 1, exported->BufferCount);
            var sizes = (long*)exported->Buffers[exported->BufferCount - 1];
            for (int k = 0; k < variadic; k++) Assert.Equal(views.Data.Buffers[2 + k].Length, sizes[k]);
            exported->Release(exported);

            using var nulls = new NullArray(5);
            ArrowCExport.ExportArray(nulls, (CArrowArray*)exported); // Apache.Arrow's exporter failed to release this
            Assert.Equal(5, exported->NullCount);
            exported->Release(exported);
            Assert.True(exported->Release == null);
        }
        finally
        {
            NativeMemory.Free(exported);
        }
    }

    // ---- streams ----

    [Fact]
    public void AStream_ReadsItsBatches_AndIsReleasedOnDispose()
    {
        var schema = new Schema([new Field("v", StringViewType.Default, true), new Field("i", Int32Type.Default, true)], null);
        RecordBatch[] batches = [.. Enumerable.Range(1, 3).Select(n => new RecordBatch(schema, [Views(n * 10), Ints(n * 10)], n * 10))];
        string?[] expected = [.. batches.Select(b => ((StringViewArray)b.Column(0)).GetString(1))]; // the exporter consumes the batches
        bool ended = false;
        IEnumerable<RecordBatch> Batches()
        {
            try
            {
                foreach (RecordBatch batch in batches) yield return batch;
            }
            finally
            {
                ended = true;
            }
        }
        var exporter = new ArrowStreamExporter(Batches().GetEnumerator(), schema);
        var stream = (Kaminari.Sazanami.Arrow.Interop.CArrowArrayStream*)NativeMemory.AllocZeroed((nuint)sizeof(Kaminari.Sazanami.Arrow.Interop.CArrowArrayStream));
        try
        {
            exporter.Export(stream);
            using (var reader = new ArrowCStreamReader(stream))
            {
                Assert.True(stream->release == null); // moved
                Assert.Equal(["v", "i"], reader.Schema.FieldsList.Select(f => f.Name));
                for (int n = 1; n <= 3; n++)
                {
                    using RecordBatch batch = reader.ReadNextRecordBatchAsync().AsTask().Result!;
                    Assert.Equal(n * 10, batch.Length);
                    Assert.Equal(expected[n - 1], ((StringViewArray)batch.Column(0)).GetString(1));
                }
                Assert.Null(reader.ReadNextRecordBatchAsync().AsTask().Result);

                // Moved on to another consumer: the reader has nothing left, a second move is refused.
                var other = (Kaminari.Sazanami.Arrow.Interop.CArrowArrayStream*)NativeMemory.AllocZeroed((nuint)sizeof(Kaminari.Sazanami.Arrow.Interop.CArrowArrayStream));
                try
                {
                    reader.MoveTo(other);
                    Assert.True(other->release != null);
                    Assert.Throws<InvalidOperationException>(() => reader.MoveTo(other));
                    Assert.Throws<InvalidOperationException>(() => reader.ReadNextRecordBatchAsync().AsTask().GetAwaiter().GetResult());
                    other->release(other);
                }
                finally
                {
                    NativeMemory.Free(other);
                }
            }
            Assert.True(ended); // released (by the consumer it was moved to)
        }
        finally
        {
            NativeMemory.Free(stream);
            foreach (RecordBatch batch in batches) batch.Dispose();
        }
    }
}
