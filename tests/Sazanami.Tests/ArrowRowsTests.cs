using System.Data.SqlTypes;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow;

namespace Sazanami.Tests;

public class ArrowRowsTests
{
    private const string IdKey = "PARQUET:field_id";

    // ---- building blocks -----------------------------------------------------------------------------------------

    private static ArrowBuffer Offsets(params int[] offsets) => new ArrowBuffer.Builder<int>().AppendRange(offsets).Build();

    private static ArrowBuffer LongOffsets(params long[] offsets) => new ArrowBuffer.Builder<long>().AppendRange(offsets).Build();

    private static (ArrowBuffer Bitmap, int NullCount) Validity(params bool[] valid)
    {
        var b = new ArrowBuffer.BitmapBuilder();
        foreach (bool v in valid) b.Append(v);
        return (b.Build(), valid.Count(v => !v));
    }

    private static Int32Array Ints(params int?[] values)
    {
        var b = new Int32Array.Builder();
        foreach (int? v in values)
        {
            if (v is int i) b.Append(i);
            else b.AppendNull();
        }
        return b.Build();
    }

    private static Int64Array Longs(params long?[] values)
    {
        var b = new Int64Array.Builder();
        foreach (long? v in values)
        {
            if (v is long l) b.Append(l);
            else b.AppendNull();
        }
        return b.Build();
    }

    private static StringArray Strings(params string?[] values)
    {
        var b = new StringArray.Builder();
        foreach (string? v in values)
        {
            if (v is null) b.AppendNull();
            else b.Append(v);
        }
        return b.Build();
    }

    private static BinaryArray Binaries(params byte[]?[] values)
    {
        var b = new BinaryArray.Builder();
        foreach (byte[]? v in values)
        {
            if (v is null) b.AppendNull();
            else b.Append(v.AsSpan());
        }
        return b.Build();
    }

    private static Field WithId(Field field, int id) =>
        new(field.Name, field.DataType, field.IsNullable, [new(IdKey, id.ToString())]);

    private static StructArray Struct(StructType type, IArrowArray[] children, params bool[] valid)
    {
        (ArrowBuffer bitmap, int nulls) = Validity(valid);
        return new(type, valid.Length, children, bitmap, nulls, 0);
    }

    // ---- the fixture: four rows, row 2 holds the nulls -----------------------------------------------------------

    private static readonly StructType DataFileType = new([
        WithId(new("path", StringType.Default, true), 100),
        WithId(new("size", Int64Type.Default, true), 101),
    ]);

    private static readonly MapType SizesType = new(Int32Type.Default, Int64Type.Default, nullable: false, keySorted: false);

    private static readonly MapType BoundsType = new(StringType.Default, BinaryType.Default, nullable: true, keySorted: false);

    private static readonly StructType EntryType = new([new("k", Int32Type.Default, true), new("v", StringType.Default, true)]);

    private static RecordBatch Fixture()
    {
        // data_file: null at row 2, whose children still hold values (the null parent must hide them).
        StructArray dataFile = Struct(DataFileType,
            [Strings("a.parquet", "b.parquet", "garbage", "d.parquet"), Longs(1000, 2000, 999, null)],
            true, true, false, true);

        (ArrowBuffer tagsValid, int tagsNulls) = Validity(true, true, false, true);
        ListArray tags = new(new ListType(Int64Type.Default), 4, Offsets(0, 2, 2, 2, 3), Longs(1, 2, 3), tagsValid, tagsNulls, 0);

        (ArrowBuffer sizesValid, int sizesNulls) = Validity(true, true, false, true);
        StructArray sizeEntries = Struct((StructType)SizesType.Fields[0].DataType,
            [Ints(1, 2, 3), Longs(100, 200, 300)], true, true, true);
        MapArray sizes = new(SizesType, 4, Offsets(0, 2, 2, 2, 3), sizeEntries, sizesValid, sizesNulls, 0);

        (ArrowBuffer boundsValid, int boundsNulls) = Validity(true, true, true, false);
        StructArray boundEntries = Struct((StructType)BoundsType.Fields[0].DataType,
            [Strings("a", "b", "é"), Binaries([1], [2, 3], null)], true, true, true);
        MapArray bounds = new(BoundsType, 4, Offsets(0, 1, 3, 3, 3), boundEntries, boundsValid, boundsNulls, 0);

        (ArrowBuffer entriesValid, int entriesNulls) = Validity(true, true, true, false);
        StructArray entryItems = Struct(EntryType, [Ints(1, 2, 3), Strings("x", "y", null)], true, true, true);
        ListArray entries = new(new ListType(new Field("element", EntryType, true)), 4, Offsets(0, 1, 3, 3, 3),
            entryItems, entriesValid, entriesNulls, 0);

        DictionaryType dictType = new(Int32Type.Default, StringType.Default, false);
        DictionaryArray dict = new(dictType, Ints(0, 1, 0, null), Strings("red", "blue"));

        FixedSizeListType fslType = new(Int32Type.Default, 2);
        FixedSizeListArray fsl = new(fslType, 4, Ints(1, 2, 3, 4, 5, 6, 7, 8), ArrowBuffer.Empty, 0, 0);

        Guid guid = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");
        byte[] uuidBytes = new byte[64];
        for (int i = 0; i < 4; i++) guid.TryWriteBytes(uuidBytes.AsSpan(i * 16), bigEndian: true, out _);
        FixedSizeBinaryArray uuid = new(new ArrayData(new FixedSizeBinaryType(16), 4, 0, 0,
            [ArrowBuffer.Empty, new ArrowBuffer(uuidBytes)]));

        var ts = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
        for (int i = 0; i < 4; i++) ts.Append(DateTimeOffset.UnixEpoch.AddDays(i));
        var dates = new Date32Array.Builder();
        for (int i = 0; i < 4; i++) dates.Append(new DateTime(2026, 1, 1 + i));

        var large = new LargeListArray(new LargeListType(Int32Type.Default), 4, LongOffsets(0, 1, 1, 1, 2),
            Ints(5, 6), ArrowBuffer.Empty, 0, 0);

        var sv = new StringViewArray.Builder();
        foreach (string s in (string[])["short", "a string longer than twelve bytes", "x", "y"]) sv.Append(s);

        Schema schema = new([
            WithId(new("id", Int32Type.Default, true), 1),
            WithId(new("big", Int64Type.Default, true), 2),
            new("name", StringType.Default, true),
            WithId(new("data_file", DataFileType, true), 3),
            new("tags", tags.Data.DataType, true),
            new("sizes", SizesType, true),
            new("bounds", BoundsType, true),
            new("entries", entries.Data.DataType, true),
            new("dict", dictType, true),
            new("fsl", fslType, true),
            new("uuid", new FixedSizeBinaryType(16), true),
            new("ts", ts.Build().Data.DataType, true),
            new("dt", Date32Type.Default, true),
            new("large", large.Data.DataType, true),
            new("sv", StringViewType.Default, true),
        ], []);
        return new RecordBatch(schema, [
            Ints(1, 2, null, 4), Longs(10, 20, 30, 40), Strings("a", null, "ccc", "d"), dataFile, tags, sizes, bounds,
            entries, dict, fsl, uuid, ts.Build(), dates.Build(), large, sv.Build(),
        ], 4);
    }

    // ---- scalars ------------------------------------------------------------------------------------------------

    [Fact]
    public void Scalars_ReadTyped_ByOrdinalAndName()
    {
        ArrowRows rows = ArrowRows.Over(Fixture());
        Assert.Equal(4, rows.Count);
        int id = rows.Ordinal("id");
        Assert.Equal([1, 2, null, 4], rows.Select(r => r.Get<int?>(id)));
        Assert.Equal([10L, 20, 30, 40], rows.Select(r => r.Get<long>("big")));
        Assert.Equal(["a", null, "ccc", "d"], rows.Select(r => r.Get<string>("name")));
        Assert.Equal(["red", "blue", "red", null], rows.Select(r => r.Get<string?>("dict")));
        Assert.Equal(["short", "a string longer than twelve bytes", "x", "y"], rows.Select(r => r.Get<string>("sv")));
        Assert.Equal(Guid.Parse("00112233-4455-6677-8899-aabbccddeeff"), rows[3].Get<Guid>("uuid"));
        Assert.Equal(DateTimeOffset.UnixEpoch.AddDays(2), rows[2].Get<DateTimeOffset>("ts"));
        Assert.Equal(new DateOnly(2026, 1, 3), rows[2].Get<DateOnly>("dt"));
        Assert.Equal("ccc"u8.ToArray(), rows[2].GetBytes("name").ToArray());
        Assert.Equal((object?)30L, rows[2].GetValue("big"));
        Assert.Null(rows[2].GetValue("id"));
    }

    [Fact]
    public void Integers_Widen_ButDoNotNarrow()
    {
        ArrowRows rows = ArrowRows.Over(Fixture());
        Assert.Equal(1L, rows[0].Get<long>("id"));
        Assert.Equal(1L, rows[0].Get<long?>("id"));
        Assert.Throws<InvalidCastException>(() => rows[0].Get<short>("id"));
        Assert.Throws<InvalidCastException>(() => rows[0].Get<double>("id")); // integers do not become floating point
        Assert.Throws<InvalidCastException>(() => rows[0].Get<ulong>("id")); // nor unsigned
    }

    [Fact]
    public void Mismatches_And_Nulls_Throw_NamingTheField()
    {
        ArrowRows rows = ArrowRows.Over(Fixture());
        var narrow = Assert.Throws<InvalidCastException>(() => rows[0].Get<int>("big"));
        Assert.Contains("'big'", narrow.Message);
        Assert.Throws<InvalidCastException>(() => rows[0].Get<string>("id"));
        Assert.Throws<InvalidCastException>(() => rows[0].GetStruct("tags"));
        var nul = Assert.Throws<InvalidOperationException>(() => rows[2].Get<int>("id"));
        Assert.Contains("'id'", nul.Message);
        Assert.Throws<KeyNotFoundException>(() => rows.Ordinal("nope"));
    }

    // ---- nested -------------------------------------------------------------------------------------------------

    [Fact]
    public void Structs_Read_AndANullParentHidesItsChildren()
    {
        ArrowRows rows = ArrowRows.Over(Fixture());
        ArrowRow file = rows[1].GetStruct("data_file");
        Assert.Equal("b.parquet", file.Get<string>("path"));
        Assert.Equal(2000L, file.Get<long>("size"));
        Assert.Null(rows[3].GetStruct("data_file").Get<long?>("size"));

        Assert.True(rows[2].IsNull("data_file"));
        Assert.False(rows[2].TryGetStruct("data_file", out _));
        Assert.Throws<InvalidOperationException>(() => rows[2].GetStruct("data_file"));

        // Read the struct as rows of its own: the null struct is a null row and its fields read as null.
        StructArray dataFile = (StructArray)Fixture().Column("data_file");
        ArrowRow nullRow = ArrowRows.Over(dataFile)[2];
        Assert.True(nullRow.IsNull());
        Assert.True(nullRow.IsNull("path"));
        Assert.Null(nullRow.Get<string>("path"));
        Assert.Null(nullRow.Get<long?>("size"));
    }

    [Fact]
    public void Lists_Read_IncludingListsOfStructs()
    {
        ArrowRows rows = ArrowRows.Over(Fixture());
        Assert.Equal([1L, 2], rows[0].GetList<long>("tags").ToArray());
        Assert.Equal(0, rows[1].GetList<long>("tags").Count);
        Assert.False(rows[2].TryGetList<long>("tags", out _));
        Assert.Equal([3L], rows[3].GetList<long>("tags").ToArray());

        ArrowList<ArrowRow> entries = rows[1].GetList<ArrowRow>("entries");
        Assert.Equal(2, entries.Count);
        Assert.Equal([2, 3], entries.ToArray().Select(e => e.Get<int>("k")));
        Assert.Equal(["y", null], entries.ToArray().Select(e => e.Get<string>("v")));

        Assert.Equal([5, 6], rows[2].GetList<int>("fsl").ToArray());
        Assert.Equal([6], rows[3].GetList<int>("large").ToArray());
        Assert.True(rows[0].GetList<int>("fsl").Contains(2));
    }

    [Fact]
    public void Maps_Read_WithIntAndStringKeys()
    {
        ArrowRows rows = ArrowRows.Over(Fixture());
        ArrowMap<int, long> sizes = rows[0].GetMap<int, long>("sizes");
        Assert.Equal(2, sizes.Count);
        Assert.Equal(200L, sizes[2]);
        Assert.False(sizes.TryGetValue(9, out _));
        Assert.Equal([1, 2], sizes.Keys.ToArray());
        Assert.Equal([KeyValuePair.Create(3, 300L)], [.. Pairs(rows[3].GetMap<int, long>("sizes"))]);
        Assert.False(rows[2].TryGetMap<int, long>("sizes", out _));

        ArrowMap<string, byte[]?> bounds = rows[1].GetMap<string, byte[]?>("bounds");
        Assert.Equal([2, 3], bounds["b"]);
        Assert.True(bounds.TryGetValue("é", out byte[]? none));
        Assert.Null(none);
        Assert.Equal(0, bounds.IndexOf("b"));
        Assert.Equal([2, 3], bounds.Values.GetBytes(0).ToArray());

        static IEnumerable<KeyValuePair<int, long>> Pairs(ArrowMap<int, long> map)
        {
            List<KeyValuePair<int, long>> list = [];
            foreach (var pair in map) list.Add(pair);
            return list;
        }
    }

    [Fact]
    public void WideDecimals_ReadAsSqlDecimal_OrUnscaledBytes()
    {
        var b = new Decimal128Array.Builder(new Decimal128Type(38, 10));
        SqlDecimal wide = SqlDecimal.Parse("1234567890123456789012345678.0123456789");
        b.Append(wide);
        b.Append(1.5m);
        RecordBatch batch = new(new Schema([new("d", new Decimal128Type(38, 10), true)], []), [b.Build()], 2);
        ArrowRows rows = ArrowRows.Over(batch);
        Assert.Equal(wide, rows[0].Get<SqlDecimal>("d"));
        Assert.Equal(wide, rows[0].GetValue("d")); // boxed by the column's precision: 38 digits → SqlDecimal
        Assert.Throws<OverflowException>(() => rows[0].Get<decimal>("d"));
        Assert.Equal(1.5m, rows[1].Get<decimal>("d"));
        // Unscaled 1.5 at scale 10 = 15_000_000_000, little-endian 16 bytes.
        Assert.Equal(new Int128(0, 15_000_000_000), new Int128(
            BitConverter.ToUInt64(rows[1].GetBytes("d")[8..]), BitConverter.ToUInt64(rows[1].GetBytes("d")[..8])));
    }

    // ---- slices -------------------------------------------------------------------------------------------------

    [Fact]
    public void SlicedBatch_ReadsTheSameValues_AtEveryLevel()
    {
        RecordBatch full = Fixture();
        ArrowRows whole = ArrowRows.Over(full);
        ArrowRows sliced = ArrowRows.Over(full.Slice(1, 3));
        Assert.Equal(3, sliced.Count);
        for (int i = 0; i < 3; i++)
        {
            ArrowRow a = whole[i + 1], b = sliced[i];
            Assert.Equal(a.Get<int?>("id"), b.Get<int?>("id"));
            Assert.Equal(a.Get<string>("name"), b.Get<string>("name"));
            Assert.Equal(a.Get<string>("dict"), b.Get<string>("dict"));
            Assert.Equal(a.Get<string>("sv"), b.Get<string>("sv"));
            Assert.Equal(a.Get<Guid>("uuid"), b.Get<Guid>("uuid"));
            Assert.Equal(a.IsNull("data_file"), b.IsNull("data_file"));
            if (!a.IsNull("data_file"))
                Assert.Equal(a.GetStruct("data_file").Get<long?>("size"), b.GetStruct("data_file").Get<long?>("size"));
            Assert.Equal(a.TryGetList<long>("tags", out var ta) ? ta.ToArray() : null,
                b.TryGetList<long>("tags", out var tb) ? tb.ToArray() : null);
            Assert.Equal(a.GetList<int>("fsl").ToArray(), b.GetList<int>("fsl").ToArray());
            Assert.Equal(a.GetList<int>("large").ToArray(), b.GetList<int>("large").ToArray());
            Assert.Equal(a.TryGetMap<int, long>("sizes", out var sa) ? sa.Keys.ToArray() : null,
                b.TryGetMap<int, long>("sizes", out var sb) ? sb.Keys.ToArray() : null);
            Assert.Equal(a.TryGetMap<string, byte[]?>("bounds", out var ba) ? ba.Keys.ToArray() : null,
                b.TryGetMap<string, byte[]?>("bounds", out var bb) ? bb.Keys.ToArray() : null);
            Assert.Equal(a.TryGetList<ArrowRow>("entries", out var ea) ? ea.ToArray().Select(e => e.Get<string>("v")) : null,
                b.TryGetList<ArrowRow>("entries", out var eb) ? eb.ToArray().Select(e => e.Get<string>("v")) : null);
        }
    }

    [Fact]
    public void SlicedStruct_ReadsItsOwnRows()
    {
        StructArray dataFile = (StructArray)ArrowArrayFactory.Slice(Fixture().Column("data_file"), 1, 3);
        ArrowRows rows = ArrowRows.Over(dataFile);
        Assert.Equal(["b.parquet", null, "d.parquet"], rows.Select(r => r.Get<string>("path")));
        Assert.Equal([false, true, false], rows.Select(r => r.IsNull()));
    }

    // ---- field ids ----------------------------------------------------------------------------------------------

    [Fact]
    public void FieldIds_ResolveOrdinals_AtEveryLevel()
    {
        ArrowRows rows = ArrowRows.Over(Fixture(), IdKey);
        Assert.Equal(0, rows.OrdinalOfId(1));
        Assert.Equal(3, rows.OrdinalOfId(3));
        Assert.Null(rows.FieldId(rows.Ordinal("name")));
        Assert.False(rows.TryGetOrdinalOfId(42, out _));
        ArrowRow file = rows[0].GetStruct(rows.OrdinalOfId(3));
        Assert.Equal(1000L, file.Get<long>(file.Rows.OrdinalOfId(101)));
        Assert.Throws<InvalidOperationException>(() => ArrowRows.Over(Fixture()).OrdinalOfId(1));
    }

    // ---- allocation ---------------------------------------------------------------------------------------------

    [Fact]
    public void ReadingRows_AllocatesNothing()
    {
        ArrowRows rows = ArrowRows.Over(Fixture());
        int id = rows.Ordinal("id"), big = rows.Ordinal("big"), file = rows.Ordinal("data_file"),
            tags = rows.Ordinal("tags"), sizes = rows.Ordinal("sizes"), bounds = rows.Ordinal("bounds"),
            entries = rows.Ordinal("entries"), dict = rows.Ordinal("dict"), uuid = rows.Ordinal("uuid");

        long Pass()
        {
            long sum = 0;
            foreach (ArrowRow row in rows)
            {
                sum += row.Get<int?>(id) ?? 0;
                sum += row.Get<long>(big);
                if (row.TryGetStruct(file, out ArrowRow f)) sum += (f.Get<long?>("size") ?? 0) + f.GetBytes("path").Length;
                if (row.TryGetList(tags, out ArrowList<long> t)) foreach (long v in t) sum += v;
                if (row.TryGetMap(sizes, out ArrowMap<int, long> s) && s.TryGetValue(1, out long one)) sum += one;
                if (row.TryGetMap(bounds, out ArrowMap<string, byte[]?> b))
                {
                    int at = b.IndexOf("b");
                    if (at >= 0) sum += b.Values.GetBytes(at).Length;
                }
                if (row.TryGetList(entries, out ArrowList<ArrowRow> e))
                {
                    foreach (ArrowRow entry in e) sum += entry.Get<int>("k");
                }
                sum += row.GetBytes(dict).Length + row.Get<Guid>(uuid).GetHashCode() % 2;
            }
            return sum;
        }

        long expected = Pass(); // builds the nested layouts
        long before = GC.GetAllocatedBytesForCurrentThread();
        long again = Pass();
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.Equal(expected, again);
        Assert.Equal(0, allocated);
    }
}

internal static class ArrowRowsTestExtensions
{
    public static IEnumerable<T> Select<T>(this ArrowRows rows, Func<ArrowRow, T> selector)
    {
        List<T> result = [];
        foreach (ArrowRow row in rows) result.Add(selector(row));
        return result;
    }
}
