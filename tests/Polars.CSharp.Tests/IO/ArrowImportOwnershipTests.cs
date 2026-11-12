using Apache.Arrow;
using Apache.Arrow.Types;
using Polars.NET.Core.Exceptions;

namespace Polars.CSharp.Tests;

/// <summary>
/// Importing Arrow data the engine refuses (here Decimal256, which panics in the engine's FFI import) must fail
/// with an exception: the array is released once, by the engine, and the schema by .NET. It used to be released
/// twice and crashed the process with an access violation.
/// </summary>
public class ArrowImportOwnershipTests
{
    private static RecordBatch Batch(bool withDecimal256)
    {
        var ints = new Int64Array.Builder().Append(1).Append(2).Build();
        if (!withDecimal256)
            return new RecordBatch(new Schema([new Apache.Arrow.Field("i", Int64Type.Default, true)], null), [ints], 2);
        var dec = new Decimal256Array.Builder(new Decimal256Type(50, 0)).Append(1m).Append(2m).Build();
        var schema = new Schema([new Apache.Arrow.Field("i", Int64Type.Default, true), new Apache.Arrow.Field("d", dec.Data.DataType, true)], null);
        return new RecordBatch(schema, [ints, dec], 2);
    }

    [Fact]
    public void DataFrame_FromArrow_of_an_unsupported_type_throws_and_the_process_survives()
    {
        for (int i = 0; i < 5; i++)
        {
            using var bad = Batch(withDecimal256: true);
            Assert.ThrowsAny<PolarsException>(() => DataFrame.FromArrow(bad));
        }
        using var good = Batch(withDecimal256: false);
        using var df = DataFrame.FromArrow(good);
        Assert.Equal(2L, df.GetValue<long>(1, "i"));
    }

    [Fact]
    public void Series_FromArrow_of_an_unsupported_type_throws_and_the_process_survives()
    {
        var dec = new Decimal256Array.Builder(new Decimal256Type(50, 0)).Append(1m).Build();
        for (int i = 0; i < 5; i++) Assert.ThrowsAny<PolarsException>(() => Series.FromArrow("d", dec));
        using var s = Series.FromArrow("i", new Int64Array.Builder().Append(7).Build());
        Assert.Equal(7L, s.GetValue<long>(0));
    }

    [Fact]
    public void Successful_imports_still_work_repeatedly()
    {
        using var batch = Batch(withDecimal256: false);
        for (int i = 0; i < 50; i++)
        {
            using var df = DataFrame.FromArrow(batch);
            Assert.Equal(2, df.Height);
        }
    }
}
