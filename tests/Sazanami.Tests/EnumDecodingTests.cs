using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Builders;
using Kaminari.Sazanami.Data.Mapping;

namespace Sazanami.Tests;

/// <summary>
/// A column of an enum's names (a dictionary of strings, strings) holds member names exactly: an alias reads as its
/// value, any other text (another case, a number, an unknown name) is a <see cref="FormatException"/> naming it, so
/// that a filter comparing names finds every row read as the member. A dictionary entry no row uses is no error. A
/// [Flags] enum's text is .NET's ("Read, Write", any case).
/// </summary>
public class EnumDecodingTests
{
    public enum Shade { Red = 5, Green = 1, Blue = 3, Verde = 1 }

    [Flags]
    public enum Access { None = 0, Read = 1, Write = 2 }

    public sealed record Row(Shade Shade, Shade? Opt);

    public sealed record FlagRow(Access Access);

    private static StringArray Strings(params string?[] values)
    {
        var builder = new StringArray.Builder();
        foreach (string? value in values)
        {
            if (value is null) builder.AppendNull();
            else builder.Append(value);
        }
        return builder.Build(PooledArrowAllocator.Instance);
    }

    private static DictionaryArray Dictionary(string[] entries, params int?[] indices)
    {
        var keys = new Int32Array.Builder();
        foreach (int? index in indices)
        {
            if (index is int i) keys.Append(i);
            else keys.AppendNull();
        }
        return new DictionaryArray(new DictionaryType(Int32Type.Default, StringType.Default, false), keys.Build(PooledArrowAllocator.Instance), Strings(entries));
    }

    private static List<Row> Read(IArrowArray shade, IArrowArray opt)
    {
        Schema schema = new([new Field("Shade", shade.Data.DataType, true), new Field("Opt", opt.Data.DataType, true)], null);
        using var batch = new RecordBatch(schema, [shade, opt], shade.Length);
        using ArrowBatchCursor<Row> cursor = ArrowRowMapper<Row>.CreateCursor(batch);
        return [.. Enumerable.Range(0, batch.Length).Select(cursor.ReadRow)];
    }

    [Fact]
    public void Dictionaries_OfMemberNames_AreRead_AliasesToo()
    {
        string[] entries = ["Red", "Blue", "Verde", "bogus"];
        List<Row> rows = Read(Dictionary(entries, 0, 1, 2), Dictionary(entries, null, 2, 0));
        Assert.Equal([new(Shade.Red, null), new(Shade.Blue, Shade.Green), new(Shade.Green, Shade.Red)], rows);
    }

    [Theory]
    [InlineData("bogus")]
    [InlineData("red")]
    [InlineData("5")]
    public void Dictionaries_OfOtherTexts_AreRefused_WhenARowReadsThem(string text)
    {
        string[] entries = ["Red", text];
        var e = Assert.Throws<FormatException>(() => Read(Dictionary(entries, 0, 1), Dictionary(entries, 0, 0)));
        Assert.Contains($"'{text}' is not a member name of enum", e.Message);
    }

    [Fact]
    public void Strings_OfMemberNames_AreRead_OthersRefused()
    {
        Assert.Equal([new(Shade.Blue, null), new(Shade.Green, Shade.Red)], Read(Strings("Blue", "Verde"), Strings(null, "Red")));
        var e = Assert.Throws<FormatException>(() => Read(Strings("Blue", "BLUE"), Strings(null, null)));
        Assert.Contains("'BLUE' is not a member name of enum", e.Message);
        Assert.Throws<FormatException>(() => Read(Strings("3"), Strings((string?)null)));
    }

    [Fact]
    public void FlagsEnums_ReadDotNetText()
    {
        StringArray access = Strings("Read, Write", "write", "None");
        Schema schema = new([new Field("Access", access.Data.DataType, true)], null);
        using var batch = new RecordBatch(schema, [access], access.Length);
        using ArrowBatchCursor<FlagRow> cursor = ArrowRowMapper<FlagRow>.CreateCursor(batch);
        Assert.Equal([Access.Read | Access.Write, Access.Write, Access.None], Enumerable.Range(0, 3).Select(i => cursor.ReadRow(i).Access));
    }
}
