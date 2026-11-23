using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Builders;

namespace Sazanami.Tests;

/// <summary>
/// <see cref="NativeColumnBuilder.AppendRows"/>: rows of an array appended to a builder (listed, or the first ones), its
/// validity carried over, onto rows already built; builders that do not take rows say so and append nothing.
/// </summary>
public class NativeBuilderAppendRowsTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(100)]
    [InlineData(5000)]
    public void FixedWidth_TakesListedOrFirstRows_WithTheirNulls(int length)
    {
        var random = new Random(length + 7);
        for (int round = 0; round < 6; round++)
        {
            bool nulls = round % 2 == 1;
            var source = new Int64Array.Builder();
            for (int i = 0; i < length + 3; i++)
            {
                if (nulls && random.Next(4) == 0) source.AppendNull();
                else source.Append(random.NextInt64(-1000, 1000));
            }
            int offset = round >= 2 ? random.Next(0, 3) : 0;
            Int64Array array = (Int64Array)ArrowArrayFactory.Slice(source.Build(), offset, length);
            int[] rows = [.. Enumerable.Range(0, length).Where(_ => random.Next(3) != 0)];
            // Rows already in the builder: all valid, or one null (validity materialized before).
            using var builder = new NativeFixedWidthBuilder<long>(Int64Type.Default);
            builder.Append(42);
            if (round >= 4) builder.AppendNull();
            long?[] before = round >= 4 ? [42, null] : [42];
            Assert.True(builder.AppendRows(array.Data, rows, rows.Length));
            int first = random.Next(length + 1);
            Assert.True(builder.AppendRows(array.Data, default, first));
            using var built = (Int64Array)builder.ExportArray();
            long?[] expected = [.. before, .. rows.Select(r => array.GetValue(r)), .. Enumerable.Range(0, first).Select(r => array.GetValue(r))];
            Assert.Equal(expected, Enumerable.Range(0, built.Length).Select(r => built.GetValue(r)));
            Assert.Equal(expected.Count(v => v is null), built.NullCount);
        }
    }

    [Fact]
    public void OtherBuilders_DeclineAndAppendNothing()
    {
        using var strings = new NativeBinaryViewBuilder(StringViewType.Default);
        var source = new StringArray.Builder().Append("a").Append("b").Build();
        Assert.False(strings.AppendRows(source.Data, [1], 1));
        Assert.Equal(0, strings.Length);
    }
}
