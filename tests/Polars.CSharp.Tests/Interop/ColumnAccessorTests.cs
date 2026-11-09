using Apache.Arrow;
using Apache.Arrow.Types;
using Polars.NET.Core.Data;

namespace Polars.CSharp.Tests;

using TimeUnit = Apache.Arrow.Types.TimeUnit;
using Field = Apache.Arrow.Field;
using Kaminari.Sazanami.Data;

/// <summary>
/// Checks the pointer-based column accessors against Apache.Arrow's own getters,
/// on sliced arrays (non-zero, non-byte-aligned offset) with nulls.
/// </summary>
public class ColumnAccessorTests
{
    private const int Total = 37;
    private const int SliceOffset = 5;
    private const int SliceLength = 23;

    private static bool IsNullRow(int i) => i % 3 == 1;

    private static TArray Sliced<TArray>(IArrowArray full) where TArray : IArrowArray
        => (TArray)ArrowArrayFactory.Slice(full, SliceOffset, SliceLength);

    private static void AssertMatches<T>(IArrowArray array, Type target, Func<int, T> expected)
    {
        var field = new Field("c", array.Data.DataType, nullable: true);
        using var accessor = ColumnAccessorFactory.Create(field, target);
        accessor.SetBatch(array);
        var typed = (ITypedAccessor<T>)accessor;
        // ITypedAccessor<Nullable<T>> (unconstrained T? is just T, so resolve it by reflection).
        var nullableIface = typeof(ITypedAccessor<>).MakeGenericType(typeof(Nullable<>).MakeGenericType(typeof(T)));
        var getNullable = nullableIface.IsInstanceOfType(accessor) ? nullableIface.GetMethod("GetTypedValue")! : null;

        Assert.Equal(SliceLength, array.Length);
        for (int i = 0; i < array.Length; i++)
        {
            Assert.Equal(array.IsNull(i), accessor.IsNull(i));
            Assert.Equal(array.IsNull(i), accessor.IsNullFast(i));
            if (array.IsNull(i))
            {
                if (getNullable != null) Assert.Null(getNullable.Invoke(accessor, [i]));
                Assert.Equal(DBNull.Value, accessor.GetValue(i));
                continue;
            }
            T exp = expected(i);
            T act = typed.GetTypedValue(i);
            Assert.Equal(exp, act);
            if (exp is DateTime de && act is DateTime da) Assert.Equal(de.Kind, da.Kind);
            if (getNullable != null) Assert.Equal(exp, (T)getNullable.Invoke(accessor, [i])!);
        }
    }

    [Fact]
    public void Boolean_Sliced_WithNulls()
    {
        var b = new BooleanArray.Builder();
        for (int i = 0; i < Total; i++)
        {
            if (IsNullRow(i)) b.AppendNull(); else b.Append(i % 5 < 2);
        }
        var arr = Sliced<BooleanArray>(b.Build());
        AssertMatches(arr, typeof(bool), i => arr.GetValue(i)!.Value);
    }

    [Fact]
    public void Boolean_NoNulls_Sliced()
    {
        var b = new BooleanArray.Builder();
        for (int i = 0; i < Total; i++) b.Append(i % 7 == 0 || i % 2 == 1);
        var arr = Sliced<BooleanArray>(b.Build());
        AssertMatches(arr, typeof(bool), i => arr.GetValue(i)!.Value);
    }

    [Theory]
    [InlineData(TimeUnit.Second, null)]
    [InlineData(TimeUnit.Millisecond, null)]
    [InlineData(TimeUnit.Microsecond, null)]
    [InlineData(TimeUnit.Nanosecond, null)]
    [InlineData(TimeUnit.Microsecond, "UTC")]
    public void Timestamp_Sliced_WithNulls(TimeUnit unit, string tz)
    {
        var type = new TimestampType(unit, tz);
        var b = new TimestampArray.Builder(type);
        var start = new DateTimeOffset(2024, 2, 28, 23, 59, 58, TimeSpan.Zero);
        for (int i = 0; i < Total; i++)
        {
            if (IsNullRow(i)) b.AppendNull(); else b.Append(start.AddSeconds(i * 3601));
        }
        var arr = Sliced<TimestampArray>(b.Build());
        var kind = tz is null ? DateTimeKind.Unspecified : DateTimeKind.Utc;
        AssertMatches(arr, typeof(DateTime),
            i => DateTime.SpecifyKind(arr.GetTimestamp(i)!.Value.UtcDateTime, kind));
    }

    [Fact]
    public void Date32_Sliced_WithNulls()
    {
        var b = new Date32Array.Builder();
        for (int i = 0; i < Total; i++)
        {
            if (IsNullRow(i)) b.AppendNull(); else b.Append(new DateTime(1969, 12, 20).AddDays(i));
        }
        var arr = Sliced<Date32Array>(b.Build());
        AssertMatches(arr, typeof(DateOnly), i => DateOnly.FromDateTime(arr.GetDateTime(i)!.Value));
        AssertMatches(arr, typeof(DateTime), i => arr.GetDateTime(i)!.Value);
    }

    [Fact]
    public void Date64_Sliced_WithNulls_MatchesArrowGetDateTime()
    {
        var b = new Date64Array.Builder();
        for (int i = 0; i < Total; i++)
        {
            if (IsNullRow(i)) b.AppendNull(); else b.Append(new DateTime(1969, 12, 20).AddDays(i));
        }
        var arr = Sliced<Date64Array>(b.Build());
        AssertMatches(arr, typeof(DateTime), i => arr.GetDateTime(i)!.Value);
    }

    [Theory]
    [InlineData(TimeUnit.Second)]
    [InlineData(TimeUnit.Millisecond)]
    [InlineData(TimeUnit.Microsecond)]
    [InlineData(TimeUnit.Nanosecond)]
    public void Duration_Sliced_WithNulls(TimeUnit unit)
    {
        var b = new DurationArray.Builder(DurationType.FromTimeUnit(unit));
        for (int i = 0; i < Total; i++)
        {
            if (IsNullRow(i)) b.AppendNull(); else b.Append(TimeSpan.FromSeconds(i * 97 - 1000));
        }
        var arr = Sliced<DurationArray>(b.Build());
        AssertMatches(arr, typeof(TimeSpan), i => arr.GetTimeSpan(i)!.Value);
    }

    [Theory]
    [InlineData(TimeUnit.Second)]
    [InlineData(TimeUnit.Millisecond)]
    public void Time32_Sliced_WithNulls(TimeUnit unit)
    {
        var b = new Time32Array.Builder(unit);
        for (int i = 0; i < Total; i++)
        {
            if (IsNullRow(i)) b.AppendNull(); else b.Append(new TimeOnly(i % 24, i % 60, i % 60));
        }
        var arr = Sliced<Time32Array>(b.Build());
        AssertMatches(arr, typeof(TimeOnly), i => arr.GetTime(i)!.Value);
    }

    [Theory]
    [InlineData(TimeUnit.Microsecond)]
    [InlineData(TimeUnit.Nanosecond)]
    public void Time64_Sliced_WithNulls(TimeUnit unit)
    {
        var b = new Time64Array.Builder(unit);
        for (int i = 0; i < Total; i++)
        {
            if (IsNullRow(i)) b.AppendNull(); else b.Append(new TimeOnly(i % 24, i % 60, i % 60, i));
        }
        var arr = Sliced<Time64Array>(b.Build());
        AssertMatches(arr, typeof(TimeOnly), i => arr.GetTime(i)!.Value);
    }

    [Fact]
    public void Int64_MagicTemporal_Sliced_WithNulls()
    {
        var b = new Int64Array.Builder();
        for (int i = 0; i < Total; i++)
        {
            if (IsNullRow(i)) b.AppendNull(); else b.Append(i * 3_600_000_123L);
        }
        var arr = Sliced<Int64Array>(b.Build());
        AssertMatches(arr, typeof(DateTime), i => DateTime.UnixEpoch.AddMicroseconds(arr.GetValue(i)!.Value));
        AssertMatches(arr, typeof(TimeSpan), i => new TimeSpan(arr.GetValue(i)!.Value * 10));

        var tb = new Int64Array.Builder();
        for (int i = 0; i < Total; i++)
        {
            if (IsNullRow(i)) tb.AppendNull(); else tb.Append(i * 1_800_000_007L); // < 1 day in microseconds
        }
        var timeArr = Sliced<Int64Array>(tb.Build());
        AssertMatches(timeArr, typeof(TimeOnly), i => new TimeOnly(timeArr.GetValue(i)!.Value * 10));
    }

    [Fact]
    public void Accessor_Rebinds_AcrossBatches()
    {
        var b1 = new BooleanArray.Builder().AppendRange([true, false, true]).Build();
        var b2 = new BooleanArray.Builder().AppendNull().Append(false).Append(true).Build();
        using var accessor = ColumnAccessorFactory.Create(new Field("b", BooleanType.Default, true), typeof(bool));
        var typed = (ITypedAccessor<bool>)accessor;

        accessor.SetBatch(b1);
        Assert.False(accessor.IsNull(0));
        Assert.True(typed.GetTypedValue(0));
        Assert.False(typed.GetTypedValue(1));

        accessor.SetBatch(b2);
        Assert.True(accessor.IsNull(0));
        Assert.False(typed.GetTypedValue(1));
        Assert.True(typed.GetTypedValue(2));
    }

    // ---------------------------------------------------------------------
    // NumericAccessor<TSrc>: one conversion matrix for every numeric source
    // ---------------------------------------------------------------------

    private static ColumnAccessor BindNumeric(IArrowArray array)
    {
        var accessor = ColumnAccessorFactory.Create(new Field("n", array.Data.DataType, true), typeof(object));
        accessor.SetBatch(array);
        return accessor;
    }

    private static T Get<T>(ColumnAccessor accessor, int i) => ((ITypedAccessor<T>)accessor).GetTypedValue(i);

    [Fact]
    public void Numeric_Int16_Sliced_WithNulls_Widens()
    {
        var b = new Int16Array.Builder();
        for (int i = 0; i < Total; i++)
        {
            if (IsNullRow(i)) b.AppendNull(); else b.Append((short)(i * 1000 - 15000));
        }
        var arr = Sliced<Int16Array>(b.Build());
        AssertMatches<long>(arr, typeof(long), i => arr.GetValue(i)!.Value);
        AssertMatches<double>(arr, typeof(double), i => arr.GetValue(i)!.Value);
    }

    [Fact]
    public void Numeric_IntegerNarrowing_Wraps()
    {
        using var acc = BindNumeric(new Int64Array.Builder().AppendRange([300L, -1L, long.MaxValue]).Build());
        Assert.Equal((byte)44, Get<byte>(acc, 0));
        Assert.Equal((sbyte)44, Get<sbyte>(acc, 0));
        Assert.Equal((byte)255, Get<byte>(acc, 1));
        Assert.Equal(ulong.MaxValue, Get<ulong>(acc, 1));
        Assert.Equal(-1, Get<int>(acc, 2));
        Assert.Equal((UInt128)long.MaxValue, Get<UInt128>(acc, 2));
    }

    [Fact]
    public void Numeric_FloatToInteger_Saturates()
    {
        using var acc = BindNumeric(new DoubleArray.Builder().AppendRange([1e300, -1e300, 2.9, -2.9]).Build());
        Assert.Equal(int.MaxValue, Get<int>(acc, 0));
        Assert.Equal(int.MinValue, Get<int>(acc, 1));
        Assert.Equal((byte)255, Get<byte>(acc, 0));
        Assert.Equal((byte)0, Get<byte>(acc, 1));
        Assert.Equal(sbyte.MaxValue, Get<sbyte>(acc, 0));
        Assert.Equal(long.MinValue, Get<long?>(acc, 1));
        Assert.Equal(2, Get<int>(acc, 2));
        Assert.Equal(-2, Get<int>(acc, 3));
        // Previously missing on float sources; now part of the shared matrix.
        Assert.Equal((Int128)(-2), Get<Int128>(acc, 3));
        Assert.Equal((char)2, Get<char>(acc, 2));
    }

    [Fact]
    public void Numeric_ToDecimal_IsChecked()
    {
        using var dbl = BindNumeric(new DoubleArray.Builder().AppendRange([1.5, double.NaN, 1e300]).Build());
        Assert.Equal(1.5m, Get<decimal>(dbl, 0));
        Assert.Throws<OverflowException>(() => Get<decimal>(dbl, 1));
        Assert.Throws<OverflowException>(() => Get<decimal>(dbl, 2));
        Assert.Throws<OverflowException>(() => Get<decimal?>(dbl, 2));

        using var half = BindNumeric(new HalfFloatArray.Builder().AppendRange([(Half)2.5f, Half.NaN]).Build());
        Assert.Equal(2.5m, Get<decimal>(half, 0));
        Assert.Throws<OverflowException>(() => Get<decimal>(half, 1));

        using var u64 = BindNumeric(new UInt64Array.Builder().Append(ulong.MaxValue).Build());
        Assert.Equal((decimal)ulong.MaxValue, Get<decimal>(u64, 0));
    }

    [Fact]
    public void Numeric_DoesNotConvertToString()
    {
        using var half = BindNumeric(new HalfFloatArray.Builder().Append((Half)1f).Build());
        using var i32 = BindNumeric(new Int32Array.Builder().Append(1).Build());
        Assert.False(half is ITypedAccessor<string>);
        Assert.False(i32 is ITypedAccessor<string>);
    }
}
