using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Encodings;

namespace Sazanami.Tests;

/// <summary>
/// <see cref="ArrowRowKeys"/>: equal values make equal keys whatever array, layout or slice holds them (strings plain,
/// large, as views or through a dictionary), other values other keys; an empty string is the null key only when asked;
/// keys of several columns keep them apart; looked up as spans in a <see cref="ByteArrayDictionary"/>.
/// </summary>
public class ArrowRowKeysTests
{
    private static byte[] Key(IArrowArray[] columns, int row, bool emptyIsNull = true)
    {
        using var key = new NativeBufferWriter(16);
        ArrowRowKeys.Append(columns, row, key, emptyIsNull);
        return key.WrittenSpan.ToArray();
    }

    private static readonly string?[] Values = ["a", "", null, "a long value of more than twelve bytes", "b", "a"];

    [Fact]
    public void Strings_KeyAlike_InEveryLayout()
    {
        IArrowArray[] layouts =
        [
            new StringArray.Builder().AppendRange(Values).Build(),
            new LargeStringArray.Builder().AppendRange(Values).Build(),
            new StringViewArray.Builder().AppendRange(Values).Build(),
            ArrowArrayFactory.Slice(new StringViewArray.Builder().AppendRange(["x", "y", .. Values]).Build(), 2, Values.Length),
        ];
        for (int row = 0; row < Values.Length; row++)
        {
            byte[] expected = Key([layouts[0]], row);
            foreach (IArrowArray layout in layouts) Assert.Equal(expected, Key([layout], row));
        }
        var dictionary = new DictionaryArray(new DictionaryType(Int32Type.Default, StringType.Default, false),
            new Int32Array.Builder().AppendRange([2, 0, 1, 2]).Build(), new StringArray.Builder().AppendRange(["a", "b", "a long value of more than twelve bytes"]).Build());
        Assert.Equal(Key([layouts[0]], 3), Key([dictionary], 0));
        Assert.Equal(Key([layouts[0]], 0), Key([dictionary], 1));
    }

    [Fact]
    public void AnEmptyString_IsTheNullKey_OnlyWhenAsked()
    {
        IArrowArray[] column = [new StringArray.Builder().AppendRange(Values).Build()];
        Assert.Equal(Key(column, 2), Key(column, 1));
        Assert.NotEqual(Key(column, 2, emptyIsNull: false), Key(column, 1, emptyIsNull: false));
        Assert.Equal(Key(column, 0), Key(column, 5));
        Assert.NotEqual(Key(column, 0), Key(column, 4));
    }

    [Fact]
    public void FixedWidthAndBooleans_KeyByValue_AndColumnsStayApart()
    {
        var longs = new Int64Array.Builder().AppendRange([5, 7, 5]).AppendNull().Build();
        var flags = new BooleanArray.Builder().AppendRange([true, true, false]).AppendNull().Build();
        var days = new Date32Array.Builder().Append(new DateTime(2024, 1, 1)).Append(new DateTime(2024, 1, 1)).Append(new DateTime(2024, 1, 2)).Append(new DateTime(2024, 1, 2)).Build();
        Assert.Equal(Key([longs], 0), Key([longs], 2));
        Assert.NotEqual(Key([longs], 0), Key([longs], 1));
        Assert.NotEqual(Key([flags], 0), Key([flags], 2));
        Assert.NotEqual(Key([flags], 2), Key([flags], 3)); // false is no null
        Assert.NotEqual(Key([longs, flags], 0), Key([longs, flags], 2));
        Assert.Equal(Key([longs, days], 0), Key([ArrowArrayFactory.Slice(longs, 2, 2), ArrowArrayFactory.Slice(days, 0, 2)], 0));
        // ("ab", "c") and ("a", "bc") are other keys: the lengths keep the columns apart.
        var first = new StringArray.Builder().AppendRange(["ab", "a"]).Build();
        var second = new StringArray.Builder().AppendRange(["c", "bc"]).Build();
        Assert.NotEqual(Key([first, second], 0), Key([first, second], 1));
    }

    [Fact]
    public void Keys_AreLookedUpAsSpans_InADictionaryOfFirstAppearances()
    {
        IArrowArray[] column = [new StringViewArray.Builder().AppendRange(Values).Build()];
        using var keys = new ByteArrayDictionary();
        using var key = new NativeBufferWriter(16);
        int[] ids = new int[Values.Length];
        for (int row = 0; row < Values.Length; row++)
        {
            key.ResetWrittenCount();
            ArrowRowKeys.Append(column, row, key, emptyIsNull: true);
            ids[row] = keys.GetOrAdd(key.WrittenSpan);
        }
        Assert.Equal([0, 1, 1, 2, 3, 0], ids);
    }
}
