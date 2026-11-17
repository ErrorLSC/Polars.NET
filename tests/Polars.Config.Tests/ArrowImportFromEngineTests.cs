using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;
using Kaminari.Sazanami.Arrow;
using Polars.CSharp;
using Polars.NET.Core;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Native;
using static Polars.CSharp.Polars;

namespace Polars.Config.Tests;

/// <summary>
/// The engine's Arrow exports imported by <see cref="ArrowCImport"/>: as Apache.Arrow's importer (the oracle, allowed
/// here only) imports them, for every type the engine has; and without the forced full collections that importer
/// causes (serial project: GC counts are process-wide).
/// </summary>
public sealed unsafe class ArrowImportFromEngineTests
{
    [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "get_Handle")]
    private static extern DataFrameHandle Handle(DataFrame df);

    /// <summary>
    /// A frame of every type the engine exports to Arrow, with nulls, nesting and a dictionary (not Int128: Apache.Arrow
    /// has no type for it, its schema importer refuses it).
    /// </summary>
    private static DataFrame EveryType(int rows)
    {
        using DataFrame typed = DataFrame.From<Row>([.. Enumerable.Range(0, rows).Select(Row.Of)]);
        return typed.Lazy().WithColumns(
            Col("Id").Cast(DataType.Int8, strict: false).Alias("I8"),
            Col("Id").Cast(DataType.UInt8, strict: false).Alias("U8"),
            Col("Id").Cast(DataType.Int16).Alias("I16"),
            Col("Id").Cast(DataType.UInt16).Alias("U16"),
            Col("Id").Cast(DataType.Int32).Alias("I32"),
            Col("Id").Cast(DataType.UInt32).Alias("U32"),
            Col("Id").Cast(DataType.UInt64).Alias("U64"),
            Col("Id").Cast(DataType.Float16).Alias("F16"),
            Col("Id").Cast(DataType.Float32).Alias("F32"),
            Col("Money").Cast(DataType.Decimal(9, 2)).Alias("D9"),
            Col("Money").Cast(DataType.Decimal(38, 9)).Alias("D38"),
            Col("At").Cast(DataType.Datetime(TimeUnit.Nanoseconds, "Asia/Tokyo")).Alias("Zoned"),
            Col("At").Cast(DataType.Datetime(TimeUnit.Milliseconds)).Alias("Naive"),
            Col("Span").Cast(DataType.Duration(TimeUnit.Milliseconds)).Alias("Ms"),
            Col("Text").Cast(DataType.Categorical()).Alias("Cat"),
            Col("Letter").Cast(DataType.Enum(["a", "b", "c"])).Alias("Enum"),
            Col("Numbers").Cast(DataType.Array(DataType.Int32, 2)).Alias("Pairs"),
            Lit((string?)null).Alias("Nothing")).Collect();
    }

    /// <summary>Exports the frame through the engine twice and imports it both ways.</summary>
    private static (RecordBatch Apache, RecordBatch Ours) BothWays(DataFrame df)
    {
        CArrowArray* forApache = CArrowArray.Create(), forUs = CArrowArray.Create();
        CArrowSchema* schemaA = CArrowSchema.Create(), schemaB = CArrowSchema.Create();
        try
        {
            NativeBindings.pl_to_arrow(Handle(df), forApache, schemaA);
            NativeBindings.pl_to_arrow(Handle(df), forUs, schemaB);
            Schema schema = CArrowSchemaImporter.ImportSchema(schemaA);
#pragma warning disable RS0030 // the oracle
            RecordBatch apache = CArrowArrayImporter.ImportRecordBatch(forApache, schema);
#pragma warning restore RS0030
            return (apache, ArrowCImport.ImportRecordBatch(forUs, CArrowSchemaImporter.ImportSchema(schemaB)));
        }
        finally
        {
            CArrowArray.Free(forApache);
            CArrowArray.Free(forUs);
            CArrowSchema.Free(schemaA);
            CArrowSchema.Free(schemaB);
        }
    }

    [Fact]
    public void EveryTypeTheEngineExports_ImportsAsApacheArrowsImporterDoes()
    {
        using DataFrame df = EveryType(500);
        using DataFrame sliced = df.Slice(7, 300);
        foreach (DataFrame frame in (DataFrame[])[df, sliced])
        {
            var (apache, ours) = BothWays(frame);
            using (apache)
            using (ours)
            {
                Assert.Equal(apache.Length, ours.Length);
                Assert.Equal(apache.Schema.FieldsList.Select(f => f.DataType.Name), ours.Schema.FieldsList.Select(f => f.DataType.Name));
                for (int c = 0; c < apache.ColumnCount; c++)
                {
                    Assert.Equal(apache.Column(c).GetType(), ours.Column(c).GetType());
                    AssertSame(apache.Column(c).Data, ours.Column(c).Data, apache.Schema.FieldsList[c].Name);
                }
            }
        }
    }

    private static void AssertSame(ArrayData expected, ArrayData actual, string where)
    {
        Assert.Equal(expected.DataType.TypeId, actual.DataType.TypeId);
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

    [Fact]
    public void TheEnginesStream_ReadsAsItsFrame()
    {
        using DataFrame df = EveryType(5000);
        using RecordBatch whole = ArrowFfiBridge.ExportDataFrame(Handle(df));
        using IArrowArrayStream stream = ArrowStreamInterop.ExportToStream(Handle(df));
        int rows = 0;
        while (stream.ReadNextRecordBatchAsync().AsTask().Result is { } batch)
        {
            using (batch)
            {
                Assert.Equal(whole.Schema.FieldsList.Select(f => f.Name), batch.Schema.FieldsList.Select(f => f.Name));
                var ids = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("Id"));
                var text = (StringViewArray)batch.Column(batch.Schema.GetFieldIndex("Text"));
                var wholeText = (StringViewArray)whole.Column(whole.Schema.GetFieldIndex("Text"));
                for (int i = 0; i < batch.Length; i++)
                {
                    Assert.Equal(rows + i, ids.GetValue(i));
                    Assert.Equal(wholeText.GetString(rows + i), text.GetString(i));
                }
                rows += batch.Length;
            }
        }
        Assert.Equal(5000, rows);
    }

    [Fact]
    public void Imports_ForceNoFullCollections()
    {
        // Batches of about 20 MiB made by the engine, imported one after another with a small managed heap (the case
        // of a sink): Apache.Arrow's importer reported each to the GC as memory pressure, which forced a gen2
        // collection nearly every batch (the pressure budget follows the managed heap's size).
        using DataFrame seed = DataFrame.From<Wide>([new(0, 0, "")]);
        using DataFrame big = seed.Lazy().Select(
            IntRange(0, 500_000).Alias("Id"),
            (IntRange(0, 500_000).Cast(DataType.Float64) * 0.5).Alias("Value"),
            ConcatString("", false, Lit("value number "), IntRange(0, 500_000).Cast(DataType.String)).Alias("Text")).Collect();
        using (ArrowFfiBridge.ExportDataFrame(Handle(big))) { } // warm
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        int before = GC.CollectionCount(2);
        for (int i = 0; i < 40; i++)
        {
            using RecordBatch batch = ArrowFfiBridge.ExportDataFrame(Handle(big));
            Assert.Equal(500_000, batch.Length);
        }
        Assert.InRange(GC.CollectionCount(2) - before, 0, 2);
    }

    [Fact]
    public void ExportsToTheEngine_RoundTrip_AndTheBatchMayGoFirst()
    {
        // Every type back to the engine through ArrowCExport; the batch disposed before the frame is used (the
        // export retains its buffers).
        using DataFrame df = EveryType(2000);
        DataFrame back;
        using (RecordBatch batch = ArrowFfiBridge.ExportDataFrame(Handle(df))) back = DataFrame.FromArrow(batch);
        using (back) Assert.True(back.Equals(df));
    }

    [Fact]
    public void BuildingFromDotNetData_ForcesNoFullCollections()
    {
        // .NET objects to a frame through Apache.Arrow builders: built with PooledArrowAllocator. Apache.Arrow's default
        // allocator reported every buffer to the GC as memory pressure (a gen2 collection every few batches).
        static IEnumerable<Point> Points(int n)
        {
            for (int i = 0; i < n; i++) yield return new(i, i * 0.5, i % 7 == 0 ? null : "p");
        }
        using (DataFrame.FromEnumerable(Points(10_000), batchSize: 2000)) { } // warm
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        int before = GC.CollectionCount(2);
        for (int r = 0; r < 8; r++)
        {
            using DataFrame df = DataFrame.FromEnumerable(Points(1_000_000), batchSize: 2000);
            Assert.Equal(1_000_000, df.Height);
        }
        Assert.InRange(GC.CollectionCount(2) - before, 0, 2);
    }

    private readonly record struct Point(long Id, double Value, string? Tag);

    private sealed record Wide(long Id, double Value, string Text);

    private sealed record Inner(int A, string? B);

    private sealed record Row(long Id, decimal Money, string? Text, string Letter, bool Flag, DateOnly Day, DateTimeOffset At,
        TimeSpan Span, TimeOnly Clock, byte[]? Bytes, List<int>? Numbers, List<string?>? Words, Inner? Nested)
    {
        public static Row Of(int i) => new(i, i * 1.25m,
            i % 5 == 0 ? null : i % 7 == 0 ? $"{new string('w', 40)}-{i % 11}" : $"t{i % 13}",
            ((string[])["a", "b", "c"])[i % 3], i % 2 == 0, DateOnly.FromDayNumber(738_000 + i % 5000),
            DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_000 + i * 1000L), TimeSpan.FromMilliseconds(i * 17),
            TimeOnly.FromTimeSpan(TimeSpan.FromSeconds(i % 86_400)), i % 4 == 0 ? null : [(byte)i, (byte)(i >> 8)],
            i % 6 == 0 ? null : [i, i + 1], i % 8 == 0 ? null : [$"w{i}", null, $"a much longer word than twelve bytes {i}"],
            i % 9 == 0 ? null : new Inner(i, i % 3 == 0 ? null : $"b{i}"));
    }
}
