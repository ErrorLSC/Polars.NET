using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Data;
using Kaminari.Sazanami.Marshalling;
using Microsoft.FSharp.Core;

namespace Sazanami.Tests;

public sealed class ArrowRowBuilderTests
{
    [Fact]
    public void Rows_ComeBackThroughTheRowMapper_AcrossBatches()
    {
        Zoo[] rows = [.. Enumerable.Range(0, 40).Select(Row)];
        using var builder = new ArrowRowBuilder<Zoo>();
        foreach (Zoo[] chunk in rows.Chunk(17))
        {
            builder.AppendRange(chunk);
            Assert.Equal(chunk.Length, builder.Count);
            using RecordBatch batch = builder.ExportBatch();
            Assert.Equal(0, builder.Count);
            Assert.Equal(chunk.Length, batch.Length);
            using ArrowBatchCursor<Zoo> cursor = ArrowRowMapper<Zoo>.CreateCursor(batch);
            for (int i = 0; i < chunk.Length; i++)
            {
                Zoo read = cursor.ReadRow(i);
                foreach (var p in typeof(Zoo).GetProperties())
                {
                    object? wrote = p.GetValue(chunk[i]);
                    // Timestamps hold the wall clock in microseconds, cut toward the epoch (as the engine converts them).
                    if (wrote is DateTime at) wrote = new DateTime(DateTime.UnixEpoch.Ticks + (at.Ticks - DateTime.UnixEpoch.Ticks) / 10 * 10);
                    Assert.True(Show(wrote) == Show(p.GetValue(read)), $"{p.Name} of row {i}: wrote {Show(wrote)}, read {Show(p.GetValue(read))}");
                }
            }
        }
    }

    [Fact]
    public void Schema_IsTheEngineLayout()
    {
        Schema schema = ArrowRowBuilder<Zoo>.Schema;
        Assert.Equal(typeof(Zoo).GetProperties().Select(p => p.Name), schema.FieldsList.Select(f => f.Name));

        Field F(string name) => schema.GetFieldByName(name);
        Assert.False(F("I32").IsNullable);
        Assert.True(F("MaybeInt").IsNullable);
        Assert.True(F("Text").IsNullable);
        Assert.IsType<StringViewType>(F("Text").DataType);
        Assert.IsType<BinaryViewType>(F("Bytes").DataType);
        Assert.IsType<BinaryViewType>(F("Id").DataType);
        var dec = Assert.IsType<Decimal128Type>(F("Dec").DataType);
        Assert.Equal((38, 18), (dec.Precision, dec.Scale));
        var at = Assert.IsType<TimestampType>(F("At").DataType);
        Assert.Equal((TimeUnit.Microsecond, (string?)null), (at.Unit, at.Timezone));
        var offset = Assert.IsType<TimestampType>(F("AtOffset").DataType);
        Assert.Equal((TimeUnit.Microsecond, "UTC"), (offset.Unit, offset.Timezone));
        Assert.Equal(TimeUnit.Microsecond, Assert.IsType<DurationType>(F("Span").DataType).Unit);
        Assert.Equal(TimeUnit.Nanosecond, Assert.IsType<Time64Type>(F("Time").DataType).Unit);
        Assert.IsType<Date32Type>(F("Day").DataType);
        Assert.IsType<UInt8Type>(F("Perm").DataType); // [Flags]: the underlying integer
        Assert.False(F("Perm").HasMetadata);

        var color = Assert.IsType<DictionaryType>(F("Color").DataType);
        Assert.IsType<UInt32Type>(color.IndexType);
        Assert.IsType<StringViewType>(color.ValueType);
        Assert.Equal(EnumCategories.TryGetArrowFieldMetadata(typeof(Color)), F("Color").Metadata);
        Assert.Equal(EnumCategories.TryGetArrowFieldMetadata(typeof(Color)), F("MaybeColor").Metadata);
        Assert.True(F("MaybeColor").IsNullable);

        var ints = Assert.IsType<LargeListType>(F("Ints").DataType);
        Assert.IsType<Int32Type>(ints.ValueDataType);
        Assert.False(ints.ValueField.IsNullable);
        Assert.True(Assert.IsType<LargeListType>(F("MaybeInts").DataType).ValueField.IsNullable);
        var nested = Assert.IsType<StructType>(F("Nested").DataType);
        Assert.Equal(["A", "B"], nested.Fields.Select(f => f.Name));
        Assert.False(nested.Fields[0].IsNullable);
    }

    [Fact]
    public void Enums_AreTheirCategoryPositions()
    {
        using var builder = new ArrowRowBuilder<Color>();
        builder.AppendRange([Color.Blue, Color.Red, Color.Green, Color.Blue]);
        using RecordBatch batch = builder.ExportBatch();
        var column = Assert.IsType<DictionaryArray>(batch.Column(0));
        var dictionary = (StringViewArray)column.Dictionary;
        Assert.Equal(EnumCategories.GetCategories(typeof(Color)), Enumerable.Range(0, dictionary.Length).Select(i => dictionary.GetString(i)));
        Assert.Equal([2u, 0u, 1u, 2u], ((UInt32Array)column.Indices).Values.ToArray());

        using var undefined = new ArrowRowBuilder<Color>();
        Assert.Throws<ArgumentException>(() => undefined.Append((Color)3));
    }

    [Fact]
    public void Dictionaries_AreListsOfKeyValueStructs()
    {
        using var builder = new ArrowRowBuilder<Holder>();
        builder.Append(new Holder(new Dictionary<string, int> { ["a"] = 1, ["b"] = 2 }));
        builder.Append(new Holder(null));
        builder.Append(new Holder([]));
        using RecordBatch batch = builder.ExportBatch();
        var lists = Assert.IsType<LargeListArray>(batch.Column(0));
        var entries = (StructArray)lists.Values;
        Assert.Equal(["Key", "Value"], ((StructType)entries.Data.DataType).Fields.Select(f => f.Name));
        Assert.Equal([0L, 2L, 2L, 2L], lists.ValueOffsets.ToArray());
        Assert.True(lists.IsNull(1));
        var keys = (StringViewArray)entries.Fields[0];
        Assert.Equal(["a", "b"], [keys.GetString(0), keys.GetString(1)]);
        Assert.Equal([1, 2], ((Int32Array)entries.Fields[1]).Values.ToArray());
    }

    [Theory]
    [InlineData("0", "0")]
    [InlineData("-1.5", "-1500000000000000000")]
    [InlineData("0.000000000000000001", "1")]
    [InlineData("1.50000000000000000000000000", "1500000000000000000")] // scale 26, trailing zeros: exact
    [InlineData("79228162514.264337593543950335", "79228162514264337593543950335")] // the most digits a decimal holds
    [InlineData("-79228162514.264337593543950335", "-79228162514264337593543950335")]
    [InlineData("99999999999999999999", "99999999999999999999000000000000000000")] // 20 integer digits
    public void Decimals_AreScaledByTenToTheEighteen(string value, string unscaled)
    {
        decimal parsed = decimal.Parse(value, System.Globalization.CultureInfo.InvariantCulture);
        using var builder = new ArrowRowBuilder<decimal>();
        builder.Append(parsed);
        using RecordBatch batch = builder.ExportBatch();
        var column = (Decimal128Array)batch.Column(0);
        Assert.Equal(Int128.Parse(unscaled), System.Runtime.InteropServices.MemoryMarshal.Read<Int128>(column.ValueBuffer.Span));
        Assert.Equal(parsed, column.GetValue(0));
    }

    [Theory]
    [InlineData("0.0000000000000000001")]          // 19 fractional digits
    [InlineData("100000000000000000000")]           // 10^20: 21 integer digits
    [InlineData("79228162514264337593543950335")]   // decimal.MaxValue
    public void Decimals_ThatDoNotFitExactly_AreRefused_AndFaultTheBuilder(string value)
    {
        using var builder = new ArrowRowBuilder<Priced>();
        builder.Append(new Priced(1, 2m));
        var e = Assert.Throws<OverflowException>(() => builder.Append(new Priced(2, decimal.Parse(value, System.Globalization.CultureInfo.InvariantCulture))));
        Assert.Contains("decimal(38, 18)", e.Message);
        Assert.Throws<InvalidOperationException>(() => builder.Append(new Priced(3, 1m)));
        Assert.Throws<InvalidOperationException>(() => builder.ExportBatch());
    }

    [Fact]
    public void Decimals_OneThird_IsRefused()
    {
        using var builder = new ArrowRowBuilder<decimal>();
        Assert.Throws<OverflowException>(() => builder.Append(1m / 3));
    }

    [Fact]
    public void NullRows_AreRefused()
    {
        using var builder = new ArrowRowBuilder<Inner>();
        builder.Append(new Inner(1, "a"));
        var e = Assert.Throws<ArgumentException>(() => builder.Append(null!));
        Assert.Contains("row 1", e.Message);
        builder.Append(new Inner(2, null)); // not faulted: nothing was appended
        Assert.Equal(2, builder.Count);
    }

    [Fact]
    public void NonRecordRows_AreOneValueColumn()
    {
        Assert.Equal(["value"], ArrowRowBuilder<int>.Schema.FieldsList.Select(f => f.Name));
        using var strings = new ArrowRowBuilder<string?>();
        strings.AppendRange(["short", null, "", "a string longer than twelve bytes", "日本語のテキスト", "\ud800lone"]);
        using RecordBatch batch = strings.ExportBatch();
        var column = (StringViewArray)batch.Column(0);
        Assert.Equal(["short", null, "", "a string longer than twelve bytes", "日本語のテキスト", "�lone"],
            Enumerable.Range(0, column.Length).Select(i => column.GetString(i)));

        using var lists = new ArrowRowBuilder<List<int>?>();
        lists.AppendRange([[1, 2], null, []]);
        using RecordBatch listBatch = lists.ExportBatch();
        var listColumn = (LargeListArray)listBatch.Column(0);
        Assert.Equal([0L, 2L, 2L, 2L], listColumn.ValueOffsets.ToArray());
        Assert.Equal(1, listColumn.NullCount);
    }

    [Fact]
    public void CharsObjectsAndOpaqueTypes_AreText()
    {
        using var builder = new ArrowRowBuilder<Odd>();
        builder.Append(new Odd('é', 42, new Opaque(), 'x', FSharpValueOption<int>.Some(5)));
        builder.Append(new Odd('a', null, null, null, FSharpValueOption<int>.None));
        Schema schema = ArrowRowBuilder<Odd>.Schema;
        Assert.All(schema.FieldsList.Take(4), f => Assert.IsType<StringViewType>(f.DataType));
        using RecordBatch batch = builder.ExportBatch();
        string?[] Strings(int c) => [.. Enumerable.Range(0, 2).Select(i => ((StringViewArray)batch.Column(c)).GetString(i))];
        Assert.Equal(["é", "a"], Strings(0));
        Assert.Equal(["42", null], Strings(1));
        Assert.Equal(["opaque", null], Strings(2));
        Assert.Equal(["x", null], Strings(3));
        var options = (Int32Array)batch.Column(4);
        Assert.Equal([5, null], [options.GetValue(0), options.GetValue(1)]);
    }

    [Fact]
    public void RecursiveTypes_AreRefused() =>
        Assert.Throws<NotSupportedException>(() => ArrowRowBuilder<Node>.Schema);

    [Fact]
    public void StructRows_AndLargeBatches()
    {
        using var builder = new ArrowRowBuilder<Point>();
        builder.Reserve(100_000);
        for (int i = 0; i < 100_000; i++) builder.Append(new Point(i, i * 0.5, $"p{i}-{new string('z', i % 20)}"));
        using RecordBatch batch = builder.ExportBatch();
        var labels = (StringViewArray)batch.Column(2);
        Assert.Equal(0, batch.Column(0).NullCount);
        Assert.Equal(99_999, ((Int32Array)batch.Column(0)).GetValue(99_999));
        Assert.Equal($"p77777-{new string('z', 77777 % 20)}", labels.GetString(77_777));
    }

    private static string Show(object? v) => v switch
    {
        null => "null",
        string s => s,
        System.Collections.IEnumerable e => "[" + string.Join(",", e.Cast<object?>().Select(Show)) + "]",
        DateTime d => d.ToString("O"),
        DateTimeOffset d => d.UtcDateTime.ToString("O"),
        _ => v.ToString() ?? "",
    };

    private static Zoo Row(int i)
    {
        bool odd = i % 2 == 1;
        return new Zoo(
            (sbyte)(i - 3), (short)(i * 300 - 7), i * 100_000 - 5, (long)i << 40, (byte)i, (ushort)(i * 1000), (uint)i * 3_000_000_000u / 4, ulong.MaxValue - (ulong)i,
            (Half)(i + 0.5), i + 0.25f, i * 1.5 - 0.125, 1234.5678m * i - 0.000000000000000001m, odd,
            new DateTime(1960 + i, 1 + i % 12, 2, 3, 4, 5, DateTimeKind.Utc).AddTicks(1230 + i),
            new DateTimeOffset(2020, 1, 1 + i % 28, 12, 0, 0, TimeSpan.FromHours(i % 5 - 2)),
            TimeSpan.FromMilliseconds(i * 1234.5 - 2000), new TimeOnly(i % 24, i % 60, 3, 4), new DateOnly(1950 + i * 2, 2, 28),
            odd ? null : $"text-{i}-" + new string('x', i), odd ? null : [.. Enumerable.Range(0, i * 7).Select(b => (byte)b)],
            new Guid(i * 7919, (short)i, (short)(i + 1), [1, 2, 3, 4, 5, 6, 7, (byte)i]),
            (Color)(1 << (i % 3)), odd ? null : Color.Blue, (Perm)(i % 4),
            odd ? null : i, odd ? i * 0.5 : null, odd ? null : new DateTime(2000, 1, 1).AddDays(i), odd ? null : i % 3 == 0, odd ? Guid.Empty : null,
            odd ? null : [.. Enumerable.Range(0, i)], odd ? [null, "b", ""] : null, [i, null],
            odd ? null : new Inner(i, i % 4 == 0 ? null : $"in{i}"), [new Inner(i, "x"), null],
            odd ? null : FSharpOption<int>.Some(i), odd ? FSharpOption<string>.Some($"o{i}") : null);
    }
}

public enum Color { Red = 1, Green = 2, Blue = 4 }

[Flags]
public enum Perm : byte { None = 0, Read = 1, Write = 2 }

public sealed record Inner(int A, string? B);

public sealed record Holder(Dictionary<string, int>? Map);

public sealed record Priced(int Id, decimal Price);

public sealed class Opaque
{
    public override string ToString() => "opaque";
}

public sealed record Odd(char Letter, object? Anything, Opaque? Thing, char? MaybeLetter, FSharpValueOption<int> Count);

public sealed record Node(int Value, Node? Next);

public readonly record struct Point(int X, double Y, string Label);

public sealed record Zoo(
    sbyte I8, short I16, int I32, long I64, byte U8, ushort U16, uint U32, ulong U64,
    Half F16, float F32, double F64, decimal Dec, bool Flag,
    DateTime At, DateTimeOffset AtOffset, TimeSpan Span, TimeOnly Time, DateOnly Day,
    string? Text, byte[]? Bytes, Guid Id,
    Color Color, Color? MaybeColor, Perm Perm,
    int? MaybeInt, double? MaybeDouble, DateTime? MaybeAt, bool? MaybeFlag, Guid? MaybeId,
    int[]? Ints, List<string?>? Words, List<int?>? MaybeInts, Inner? Nested, List<Inner?>? Nesteds,
    FSharpOption<int>? Opt, FSharpOption<string>? OptText);
