using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Builders;
using Kaminari.Sazanami.Arrow.Compute;
using Kaminari.Sazanami.Arrow.Interop;
using Kaminari.Sazanami.Arrow.TypeSystem;

namespace Sazanami.Tests;

/// <summary><see cref="ArrowWidening"/>: which conversions compile, the common types, and the values converted.</summary>
public class ArrowWideningTests
{
    private static readonly ArrowConversionRules Lossless = default;
    private static readonly ArrowConversionRules LossyRules = new(Lossy: true);

    private static readonly IArrowType[] Integers =
        [Int8Type.Default, UInt8Type.Default, Int16Type.Default, UInt16Type.Default, Int32Type.Default, UInt32Type.Default, Int64Type.Default, UInt64Type.Default];

    private static readonly IArrowType[] Floats = [HalfFloatType.Default, FloatType.Default, DoubleType.Default];

    private static TimestampType Ts(TimeUnit unit, string? zone = null) => new(unit, zone);

    // ---- rules ----------------------------------------------------------------------------------------------------

    public static TheoryData<string, string, string> Pairs => new()
    {
        // from, to, "lossless" | "lossy" | "never"
        { "i8", "i16", "lossless" }, { "i32", "i64", "lossless" }, { "u8", "i16", "lossless" }, { "u32", "i64", "lossless" },
        { "u8", "u16", "lossless" }, { "u32", "u64", "lossless" },
        { "i64", "i32", "never" }, { "i32", "u32", "never" }, { "i8", "u64", "never" }, { "u32", "i32", "never" }, { "u64", "i64", "never" },
        { "i8", "f16", "lossless" }, { "u8", "f16", "lossless" }, { "i16", "f16", "lossy" }, { "i16", "f32", "lossless" }, { "u16", "f32", "lossless" },
        { "i32", "f32", "lossy" }, { "i32", "f64", "lossless" }, { "u32", "f64", "lossless" }, { "i64", "f64", "lossy" }, { "u64", "f64", "lossy" },
        { "f16", "f32", "lossless" }, { "f32", "f64", "lossless" }, { "f64", "f32", "lossy" }, { "f64", "f16", "lossy" },
        { "f64", "i64", "never" }, { "f32", "i32", "never" },
        { "d5,2", "d10,2", "lossless" }, { "d9,2", "d10,3", "lossless" }, { "d10,2", "d10,3", "never" }, { "d10,3", "d10,2", "never" },
        { "d38,0", "d40,2", "lossless" }, { "d10,2", "d9,2", "never" },
        { "i32", "d10,0", "lossless" }, { "i32", "d12,2", "lossless" }, { "i32", "d9,0", "never" }, { "i64", "d19,0", "lossless" },
        { "u64", "d19,0", "never" }, { "i64", "d18,0", "never" }, { "u32", "d9,0", "never" }, { "u64", "d20,0", "lossless" }, { "i8", "d3,0", "lossless" }, { "i16", "d5,0", "lossless" },
        // Decimals of 32 and 64 bits by the same digit rules, into and out of every width.
        { "d32_9,2", "d64_18,3", "lossless" }, { "d32_9,2", "d10,2", "lossless" }, { "d32_5,2", "d32_9,4", "lossless" },
        { "d64_18,0", "d40,2", "lossless" }, { "d10,2", "d64_18,2", "lossless" }, { "d40,0", "d64_18,0", "never" },
        { "d64_18,0", "d32_9,0", "never" }, { "d32_9,2", "d64_18,1", "never" }, { "d32_9,2", "d32_9,3", "never" },
        { "i8", "d32_3,0", "lossless" }, { "i32", "d32_9,0", "never" }, { "i32", "d64_10,0", "lossless" }, { "i64", "d64_18,0", "never" },
        { "d32_7,0", "f32", "lossless" }, { "d32_8,0", "f32", "lossy" }, { "d64_15,0", "f64", "lossless" }, { "d64_18,2", "f64", "lossy" },
        { "f64", "d64_18,2", "never" },
        { "d7,0", "f32", "lossless" }, { "d8,0", "f32", "lossy" }, { "d15,0", "f64", "lossless" }, { "d10,2", "f64", "lossy" }, { "f64", "d38,10", "never" },
        { "date", "ts_us", "lossless" }, { "date", "ts_ns", "lossless" }, { "date", "ts_us_utc", "never" }, { "ts_us", "date", "never" },
        { "ts_ms", "ts_us", "lossless" }, { "ts_us", "ts_ns", "lossless" }, { "ts_ns", "ts_us", "lossy" }, { "ts_us", "ts_ms", "lossy" },
        { "ts_us", "ts_us_utc", "never" }, { "ts_us_utc", "ts_us", "never" }, { "ts_us_utc", "ts_us_tokyo", "lossless" }, { "ts_ms_utc", "ts_ns_tokyo", "lossless" },
        { "t32_ms", "t64_us", "lossless" }, { "t32_s", "t32_ms", "lossless" }, { "t64_ns", "t64_us", "lossy" }, { "t64_us", "t32_ms", "lossy" },
        { "dur_ms", "dur_us", "lossless" }, { "dur_us", "dur_ms", "lossy" },
        { "str", "bin", "never" }, { "bin", "str", "never" }, { "i64", "str", "never" }, { "str", "i64", "never" }, { "bool", "i8", "never" },
        { "str", "dict_str", "lossless" }, { "dict_str", "str", "lossless" }, { "view", "dict_view", "lossless" }, { "str", "view", "lossless" },
        { "view", "dict_str", "lossless" }, { "dict_str", "view", "lossless" }, { "view", "bin", "never" },
        { "null", "i32", "lossless" }, { "null", "list_i64", "lossless" },
        { "list_i32", "list_i64", "lossless" }, { "list_i64", "list_i32", "never" }, { "list_i64", "list_f64", "lossy" },
        { "map_i32", "map_i64", "lossless" },
    };

    private static IArrowType Parse(string name) => name switch
    {
        "i8" => Int8Type.Default, "u8" => UInt8Type.Default, "i16" => Int16Type.Default, "u16" => UInt16Type.Default,
        "i32" => Int32Type.Default, "u32" => UInt32Type.Default, "i64" => Int64Type.Default, "u64" => UInt64Type.Default,
        "f16" => HalfFloatType.Default, "f32" => FloatType.Default, "f64" => DoubleType.Default, "bool" => BooleanType.Default,
        "str" => StringType.Default, "bin" => BinaryType.Default, "view" => StringViewType.Default, "binview" => BinaryViewType.Default, "null" => NullType.Default,
        "date" => Date32Type.Default,
        "ts_ms" => Ts(TimeUnit.Millisecond), "ts_us" => Ts(TimeUnit.Microsecond), "ts_ns" => Ts(TimeUnit.Nanosecond),
        "ts_us_utc" => Ts(TimeUnit.Microsecond, "UTC"), "ts_ms_utc" => Ts(TimeUnit.Millisecond, "UTC"),
        "ts_us_tokyo" => Ts(TimeUnit.Microsecond, "Asia/Tokyo"), "ts_ns_tokyo" => Ts(TimeUnit.Nanosecond, "Asia/Tokyo"),
        "t32_s" => new Time32Type(TimeUnit.Second), "t32_ms" => new Time32Type(TimeUnit.Millisecond),
        "t64_us" => new Time64Type(TimeUnit.Microsecond), "t64_ns" => new Time64Type(TimeUnit.Nanosecond),
        "dur_ms" => DurationType.Millisecond, "dur_us" => DurationType.Microsecond,
        "dict_str" => new DictionaryType(Int32Type.Default, StringType.Default, false),
        "dict_view" => new DictionaryType(Int32Type.Default, StringViewType.Default, false),
        "list_i32" => new ListType(Int32Type.Default), "list_i64" => new ListType(Int64Type.Default), "list_f64" => new ListType(DoubleType.Default),
        "map_i32" => new MapType(StringType.Default, Int32Type.Default), "map_i64" => new MapType(StringType.Default, Int64Type.Default),
        _ when name.StartsWith("d32_", StringComparison.Ordinal) => Decimal(name[4..], 32),
        _ when name.StartsWith("d64_", StringComparison.Ordinal) => Decimal(name[4..], 64),
        _ when name.StartsWith('d') => Decimal(name[1..]),
        _ => throw new ArgumentException(name),
    };

    private static IArrowType Decimal(string parameters, int bits = 0)
    {
        int[] ps = [.. parameters.Split(',').Select(int.Parse)];
        return bits switch
        {
            32 => new Decimal32Type(ps[0], ps[1]),
            64 => new Decimal64Type(ps[0], ps[1]),
            _ => ps[0] <= 38 ? new Decimal128Type(ps[0], ps[1]) : new Decimal256Type(ps[0], ps[1]),
        };
    }

    [Theory]
    [MemberData(nameof(Pairs))]
    public void Conversions_CompileAsTheRulesSay(string from, string to, string kind)
    {
        IArrowType f = Parse(from), t = Parse(to);
        Assert.Equal(kind == "lossless", ArrowWidening.CanConvert(f, t, Lossless));
        Assert.Equal(kind != "never", ArrowWidening.CanConvert(f, t, LossyRules));
        if (kind != "lossless")
        {
            Assert.False(ArrowWidening.TryCompile(f, t, Lossless, "x", out _, out ArrowConversionError? error));
            Assert.Equal(kind == "lossy" ? ArrowConversionProblem.Lossy : ArrowConversionProblem.Incompatible, error.Problem);
            Assert.StartsWith("x", error.Path);
        }
    }

    [Fact]
    public void DeltaTypeWidening_IsCovered()
    {
        // Delta's type widening (byte < short < int < long, float < double, byte/short/int to double, decimals
        // up keeping integer digits, integers to decimals with the digits, date to timestamp_ntz) is lossless here.
        (IArrowType From, IArrowType To)[] delta =
        [
            (Int8Type.Default, Int16Type.Default), (Int16Type.Default, Int32Type.Default), (Int32Type.Default, Int64Type.Default),
            (Int8Type.Default, Int64Type.Default), (FloatType.Default, DoubleType.Default), (Int8Type.Default, DoubleType.Default),
            (Int16Type.Default, DoubleType.Default), (Int32Type.Default, DoubleType.Default),
            (new Decimal128Type(10, 2), new Decimal128Type(12, 4)), (new Decimal128Type(10, 2), new Decimal128Type(20, 2)),
            (Int8Type.Default, new Decimal128Type(10, 7)), (Int16Type.Default, new Decimal128Type(5, 0)), (Int32Type.Default, new Decimal128Type(10, 0)),
            (Int64Type.Default, new Decimal128Type(20, 0)), (Date32Type.Default, Ts(TimeUnit.Microsecond)),
        ];
        foreach (var (from, to) in delta) Assert.True(ArrowWidening.CanConvert(from, to, Lossless), $"{from.Name} -> {to.Name}");
    }

    public static TheoryData<string, string, string?, string?> CommonPairs => new()
    {
        // a, b, common lossless, common lossy (null: none)
        { "i32", "i64", "i64", "i64" }, { "u32", "i32", "i64", "i64" }, { "u8", "i8", "i16", "i16" }, { "u64", "i64", null, "f64" },
        { "u16", "u32", "u32", "u32" }, { "i16", "f32", "f32", "f32" }, { "i32", "f32", "f64", "f64" }, { "i64", "f64", null, "f64" },
        { "f16", "f64", "f64", "f64" }, { "d10,3", "d10,2", "d11,3", "d11,3" }, { "i32", "d5,2", "d12,2", "d12,2" },
        { "d38,0", "d10,5", "d43,5", "d43,5" }, { "date", "ts_us", "ts_us", "ts_us" }, { "ts_ms", "ts_ns", "ts_ns", "ts_ns" },
        { "ts_us", "ts_us_utc", null, null }, { "ts_us_utc", "ts_ms_utc", "ts_us_utc", "ts_us_utc" },
        { "str", "i64", null, null }, { "str", "bin", null, null }, { "dict_str", "str", "str", "str" }, { "null", "i32", "i32", "i32" },
        { "list_i32", "list_i64", "list_i64", "list_i64" }, { "t32_ms", "t64_us", "t64_us", "t64_us" }, { "d15,0", "f32", "f64", "f64" },
        { "d10,2", "f64", null, "f64" }, { "str", "view", "view", "view" }, { "bin", "binview", "binview", "binview" },
        // Decimals as wide as the wider one (integers count as 32 bits), wider when the precision needs it (pyarrow's
        // unify_schemas(promote_options="permissive") gives the same).
        { "d32_9,2", "d64_18,3", "d64_18,3", "d64_18,3" }, { "d32_9,2", "d32_9,3", "d64_10,3", "d64_10,3" },
        { "d32_5,2", "d32_6,3", "d32_6,3", "d32_6,3" }, { "d64_18,2", "d10,2", "d18,2", "d18,2" }, { "d64_18,0", "d32_9,9", "d27,9", "d27,9" },
        { "d32_2,1", "d40,0", "d41,1", "d41,1" }, { "i8", "d32_4,2", "d32_5,2", "d32_5,2" }, { "i32", "d32_9,2", "d64_12,2", "d64_12,2" },
        { "i64", "d32_9,2", "d21,2", "d21,2" }, { "d32_7,0", "f32", "f32", "f32" }, { "d32_9,2", "f64", null, "f64" },
    };

    [Theory]
    [MemberData(nameof(CommonPairs))]
    public void CommonTypes_AreTheNarrowestBothConvertTo(string a, string b, string? lossless, string? lossy)
    {
        IArrowType x = Parse(a), y = Parse(b);
        foreach (var (expected, rules) in ((string?, ArrowConversionRules)[])[(lossless, Lossless), (lossy, LossyRules)])
        {
            foreach (var (first, second) in ((IArrowType, IArrowType)[])[(x, y), (y, x)])
            {
                IArrowType? common = ArrowWidening.Common(first, second, rules.Lossy);
                if (expected is null)
                {
                    Assert.Null(common);
                    continue;
                }
                Assert.NotNull(common);
                Assert.True(ArrowTypes.SameType(Parse(expected), common), $"{a}+{b}: {ArrowTypes.Describe(common)}");
                Assert.True(ArrowWidening.CanConvert(first, common, rules));
                Assert.True(ArrowWidening.CanConvert(second, common, rules));
            }
        }
    }

    [Fact]
    public void CommonStructs_UniteTheirFields_FirstOrderFirst()
    {
        var a = new StructType([new Field("a", Int32Type.Default, true), new Field("c", StringType.Default, true)]);
        var b = new StructType([new Field("b", StringType.Default, false), new Field("a", Int64Type.Default, true)]);
        var common = (StructType)ArrowWidening.Common(a, b, lossy: false)!;
        Assert.Equal(["a", "c", "b"], common.Fields.Select(f => f.Name));
        Assert.IsType<Int64Type>(common.Fields[0].DataType);
        Assert.All(common.Fields, f => Assert.True(f.IsNullable));
        Assert.Null(ArrowWidening.Common(a, new StructType([new Field("a", StringType.Default, true)]), lossy: true));
    }

    // ---- numbers --------------------------------------------------------------------------------------------------

    [Fact]
    public void Numbers_ConvertAsTheClrDoes_AtEveryLengthOffsetAndNullShape()
    {
        var random = new Random(2026_11_16);
        foreach (IArrowType from in Integers.Concat(Floats))
        {
            foreach (IArrowType to in Integers.Concat(Floats))
            {
                if (!ArrowWidening.TryCompile(from, to, LossyRules, "x", out ArrowConversion? conversion, out _)) continue;
                bool lossless = ArrowWidening.CanConvert(from, to, Lossless);
                foreach (int length in (int[])[0, 1, 3, 7, 8, 15, 16, 17, 33, 64, 65, 130])
                {
                    int offset = random.Next(3) == 0 ? random.Next(1, 9) : 0;
                    double?[] values = [.. Enumerable.Range(0, length + offset).Select(_ => random.Next(5) == 0 ? (double?)null : Sample(random, from))];
                    IArrowArray source = ArrowArrayFactory.Slice(Numbers(from, values), offset, length);
                    using var converted = (IArrowArray)conversion.Convert(source);
                    Assert.Equal(length, converted.Length);
                    Assert.Equal(values.Skip(offset).Count(v => v is null), converted.NullCount);
                    for (int i = 0; i < length; i++)
                    {
                        double? expected = values[offset + i] is { } v ? Expected(from, to, v) : null;
                        double? got = Number(converted, i);
                        Assert.True(Same(expected, got), $"{from.Name} -> {to.Name} [{i}]: {expected} vs {got}");
                        if (lossless && got is { } g) Assert.Equal(values[offset + i]!.Value, g);
                    }
                }
            }
        }
    }

    [Fact]
    public void Extremes_SurviveLosslessWidenings()
    {
        Assert.Equal([(long)int.MinValue, int.MaxValue],
            Longs(NumericConversion.Create(Int32Type.Default, Int64Type.Default).Convert(Ints(int.MinValue, int.MaxValue))));
        IArrowArray u = new UInt32Array.Builder().Append(uint.MaxValue).Append(0).Build();
        Assert.Equal([(long)uint.MaxValue, 0], Longs(NumericConversion.Create(UInt32Type.Default, Int64Type.Default).Convert(u)));
        IArrowArray doubles = NumericConversion.Create(Int32Type.Default, DoubleType.Default).Convert(Ints(int.MinValue, int.MaxValue));
        Assert.Equal([int.MinValue, int.MaxValue], ((DoubleArray)doubles).Values.ToArray());
        var big = new Int64Array.Builder().Append(long.MaxValue).Append((1L << 53) + 1).Build();
        Assert.True(ArrowWidening.TryCompile(Int64Type.Default, DoubleType.Default, LossyRules, "x", out ArrowConversion? lossy, out _));
        Assert.Equal([9.223372036854775807E18, 9007199254740992.0], ((DoubleArray)lossy.Convert(big)).Values.ToArray());
    }

    // ---- decimals -------------------------------------------------------------------------------------------------

    [Fact]
    public void Decimals_RescaleExactly_IntoDecimal128And256()
    {
        var source = new Decimal128Array.Builder(new Decimal128Type(9, 2)).Append(1.25m).AppendNull().Append(-9999999.99m).Build();
        ArrowConversion wider = Compile(source.Data.DataType, new Decimal128Type(10, 3));
        var converted = (Decimal128Array)wider.Convert(source);
        Assert.Equal([1.250m, null, -9999999.990m], Enumerable.Range(0, 3).Select(i => converted.GetValue(i)));
        ArrowConversion wide = Compile(source.Data.DataType, new Decimal256Type(45, 10));
        var huge = (Decimal256Array)wide.Convert(source);
        Assert.Equal("1.2500000000", huge.GetString(0));
        Assert.True(huge.IsNull(1));
        Assert.Equal("-9999999.9900000000", huge.GetString(2));

        var ints = new Int64Array.Builder().Append(long.MinValue).Append(long.MaxValue).Build();
        var fromInts = (Decimal128Array)Compile(Int64Type.Default, new Decimal128Type(21, 2)).Convert(ints);
        Assert.Equal("-9223372036854775808.00", fromInts.GetString(0));
        Assert.Equal("9223372036854775807.00", fromInts.GetString(1));
        var unsigned = new UInt64Array.Builder().Append(ulong.MaxValue).Build();
        Assert.Equal("18446744073709551615", ((Decimal128Array)Compile(UInt64Type.Default, new Decimal128Type(20, 0)).Convert(unsigned)).GetString(0));
        // Decimal256 sources too.
        var back = (Decimal256Array)Compile(huge.Data.DataType, new Decimal256Type(50, 12)).Convert(huge);
        Assert.Equal("-9999999.990000000000", back.GetString(2));
        // Into Decimal128 from Decimal256 whose digits fit (fuzz seed 20261127).
        var narrow256 = new Decimal256Array.Builder(new Decimal256Type(30, 4)).Append(1.2345m).AppendNull().Append(-12345678901234567890.1234m).Build();
        var into128 = (Decimal128Array)Compile(narrow256.Data.DataType, new Decimal128Type(38, 6)).Convert(narrow256);
        Assert.Equal(["1.234500", null, "-12345678901234567890.123400"], Enumerable.Range(0, 3).Select(into128.GetString));
    }

    /// <summary>
    /// Decimals of every width into every width the rules allow: the source's extremes, zero, ±1 and random values
    /// rescaled exactly, at an offset; null slots holding the width's extreme convert without an overflow.
    /// </summary>
    [Fact]
    public void Decimals_OfEveryWidth_RescaleExactly()
    {
        IArrowType[] types =
        [
            new Decimal32Type(9, 2), new Decimal32Type(5, 0), new Decimal32Type(9, 9), new Decimal64Type(18, 3), new Decimal64Type(12, 0),
            new Decimal128Type(20, 4), new Decimal128Type(38, 10), new Decimal256Type(40, 5), new Decimal256Type(60, 20),
        ];
        var random = new Random(2026_11_19);
        int compiled = 0;
        foreach (IArrowType from in types)
        {
            foreach (IArrowType to in types)
            {
                if (!ArrowWidening.TryCompile(from, to, Lossless, "x", out ArrowConversion? conversion, out _)) continue;
                compiled++;
                var (p1, s1) = ArrowTypes.DecimalParameters(from);
                BigInteger max = BigInteger.Pow(10, p1) - 1;
                BigInteger?[] values = [null, 0, 1, -1, max, -max, null, .. Enumerable.Range(0, 20).Select(_ => (BigInteger?)Within(random, max))];
                IArrowArray source = ArrowArrayFactory.Slice(ArrowArrayFactory.BuildArray(Decimals(from, values)), 1, values.Length - 1);
                using var converted = (IArrowArray)conversion.Convert(source);
                Assert.True(ArrowTypes.SameType(to, converted.Data.DataType));
                BigInteger factor = BigInteger.Pow(10, ArrowTypes.DecimalParameters(to).Scale - s1);
                for (int i = 0; i < source.Length; i++)
                {
                    BigInteger? expected = values[i + 1] * factor;
                    Assert.Equal(expected, converted.IsNull(i) ? null : Unscaled(converted, i));
                }
            }
        }
        Assert.Equal(37, compiled); // identities included

        // Integers into 32 and 64 bits.
        var bytes = new Int8Array.Builder().Append(sbyte.MinValue).AppendNull().Append(sbyte.MaxValue).Build();
        using var small = (IArrowArray)Compile(Int8Type.Default, new Decimal32Type(3, 0)).Convert(bytes);
        Assert.Equal([-128, null, 127], Enumerable.Range(0, 3).Select(i => small.IsNull(i) ? null : (BigInteger?)Unscaled(small, i)));
        using var ints = (IArrowArray)Compile(Int32Type.Default, new Decimal64Type(12, 2)).Convert(Ints(int.MinValue, null, int.MaxValue));
        Assert.Equal([-214748364800L, null, 214748364700L], Enumerable.Range(0, 3).Select(i => ints.IsNull(i) ? null : (BigInteger?)Unscaled(ints, i)));

        static BigInteger Within(Random random, BigInteger max)
        {
            var bytes = new byte[max.GetByteCount() + 1];
            random.NextBytes(bytes);
            return new BigInteger(bytes) % (max + 1);
        }
    }

    [Fact]
    public void Decimals_IntoFloats_AreTheNearest()
    {
        var source = new Decimal128Array.Builder(new Decimal128Type(10, 2)).Append(0.1m).Append(-12345678.91m).Append(0m).Build();
        Assert.True(ArrowWidening.TryCompile(source.Data.DataType, DoubleType.Default, LossyRules, "x", out ArrowConversion? conversion, out _));
        Assert.Equal([0.1, -12345678.91, 0.0], ((DoubleArray)conversion.Convert(source)).Values.ToArray());
        var exact = new Decimal128Array.Builder(new Decimal128Type(15, 0)).Append(999_999_999_999_999m).Build();
        Assert.Equal(999_999_999_999_999.0, ((DoubleArray)Compile(exact.Data.DataType, DoubleType.Default).Convert(exact)).GetValue(0));

        // 32 and 64 bits.
        IArrowArray d32 = ArrowArrayFactory.BuildArray(Decimals(new Decimal32Type(9, 2), [123_456_789, null, -1]));
        Assert.True(ArrowWidening.TryCompile(d32.Data.DataType, DoubleType.Default, LossyRules, "x", out ArrowConversion? from32, out _));
        var doubles = (DoubleArray)from32.Convert(d32);
        Assert.Equal([1234567.89, null, -0.01], Enumerable.Range(0, 3).Select(doubles.GetValue));
        IArrowArray d64 = ArrowArrayFactory.BuildArray(Decimals(new Decimal64Type(15, 0), [999_999_999_999_999, -999_999_999_999_999]));
        Assert.Equal([999_999_999_999_999.0, -999_999_999_999_999.0], ((DoubleArray)Compile(d64.Data.DataType, DoubleType.Default).Convert(d64)).Values.ToArray());
    }

    // ---- times ----------------------------------------------------------------------------------------------------

    [Fact]
    public void Dates_BecomeMidnights_AndUnitsScale()
    {
        var dates = new Date32Array.Builder().Append(new DateTime(2024, 5, 6)).Append(new DateTime(1969, 12, 31)).AppendNull().Build();
        var stamps = (TimestampArray)Compile(Date32Type.Default, Ts(TimeUnit.Microsecond)).Convert(dates);
        Assert.Equal(new DateTimeOffset(2024, 5, 6, 0, 0, 0, TimeSpan.Zero), stamps.GetTimestamp(0));
        Assert.Equal(-86_400_000_000L, stamps.GetValue(1));
        Assert.True(stamps.IsNull(2));

        var ms = Stamps(Ts(TimeUnit.Millisecond, "UTC"), 1_704_164_645_123, -1);
        var ns = (TimestampArray)Compile(ms.Data.DataType, Ts(TimeUnit.Nanosecond, "Asia/Tokyo")).Convert(ms);
        Assert.Equal([1_704_164_645_123_000_000, -1_000_000], ns.Values.ToArray());
        Assert.Equal("Asia/Tokyo", ((TimestampType)ns.Data.DataType).Timezone);
    }

    [Fact]
    public void FinerUnits_FailBeyondTheRange_ButNotOnNullSlots()
    {
        var far = Stamps(Ts(TimeUnit.Millisecond), long.MaxValue / 10);
        ArrowConversion conversion = Compile(far.Data.DataType, Ts(TimeUnit.Nanosecond));
        Assert.Contains("beyond the range of timestamp[ns]", Assert.Throws<OverflowException>(() => conversion.Convert(far)).Message);

        // A null slot holding a huge value converts to a null.
        var data = new ArrayData(Ts(TimeUnit.Millisecond), 2, 1, 0,
            [new ArrowBuffer((byte[])[0b10]), new ArrowBuffer(BitConverter.GetBytes(long.MaxValue).Concat(BitConverter.GetBytes(5L)).ToArray())]);
        var converted = (TimestampArray)conversion.Convert(ArrowArrayFactory.BuildArray(data));
        Assert.True(converted.IsNull(0));
        Assert.Equal(5_000_000, converted.GetValue(1));
        var days = new Date32Array.Builder().Append(new DateTime(9999, 12, 31)).Build();
        Assert.Throws<OverflowException>(() => Compile(Date32Type.Default, Ts(TimeUnit.Nanosecond)).Convert(days));
    }

    [Fact]
    public void CoarserUnits_RoundDown()
    {
        var ns = Stamps(Ts(TimeUnit.Nanosecond), 1_999, -1, -1_000, -1_001);
        Assert.True(ArrowWidening.TryCompile(ns.Data.DataType, Ts(TimeUnit.Microsecond), LossyRules, "x", out ArrowConversion? conversion, out _));
        Assert.Equal([1, -1, -1, -2], ((TimestampArray)conversion.Convert(ns)).Values.ToArray());
        var time = new Time64Array.Builder(TimeUnit.Microsecond).Append(3_723_004_567L).Build();
        Assert.True(ArrowWidening.TryCompile(time.Data.DataType, new Time32Type(TimeUnit.Millisecond), LossyRules, "x", out ArrowConversion? toMs, out _));
        Assert.Equal(3_723_004, ((Time32Array)toMs.Convert(time)).GetValue(0));
        var ms = new Time32Array.Builder(TimeUnit.Millisecond).Append(3_723_004).Build();
        Assert.Equal(3_723_004_000L, ((Time64Array)Compile(ms.Data.DataType, new Time64Type(TimeUnit.Microsecond)).Convert(ms)).GetValue(0));
    }

    // ---- nested ---------------------------------------------------------------------------------------------------

    [Fact]
    public void Structs_MatchFieldsByName_InsertMissing_DropExtra()
    {
        var source = Struct(
            [new Field("b", StringType.Default, true), new Field("extra", DoubleType.Default, true), new Field("a", Int32Type.Default, true)],
            [Strings("x", null, "z"), Doubles(1, 2, 3), Ints(1, 2, 3)],
            validity: [true, true, false]);
        var target = new StructType([new Field("a", Int64Type.Default, true), new Field("b", StringType.Default, true), new Field("c", DoubleType.Default, true)]);

        Assert.False(ArrowWidening.TryCompile(source.Data.DataType, target, Lossless, "s", out _, out ArrowConversionError? error));
        Assert.Equal("s.c", error.Path);
        Assert.Equal(ArrowConversionProblem.MissingField, error.Problem);

        Assert.True(ArrowWidening.TryCompile(source.Data.DataType, target, new ArrowConversionRules(InsertMissingFields: true), "s", out ArrowConversion? conversion, out _));
        foreach (int offset in (int[])[0, 1])
        {
            var converted = (StructArray)conversion.Convert(source.Slice(offset, 3 - offset));
            Assert.Equal(3 - offset, converted.Length);
            var a = (Int64Array)converted.Fields[0];
            var b = (StringArray)converted.Fields[1];
            Assert.Equal(((long?[])[1, 2, 3]).Skip(offset), Enumerable.Range(0, a.Length).Select(i => a.GetValue(i)));
            Assert.Equal(((string?[])["x", null, "z"]).Skip(offset), Enumerable.Range(0, b.Length).Select(i => b.GetString(i)));
            Assert.Equal(converted.Length, converted.Fields[2].NullCount);
            Assert.Equal(1, converted.NullCount);
            Assert.True(converted.IsNull(2 - offset));
        }
        Assert.Equal("struct<a: int64, b: utf8, c: double>", ArrowTypes.Describe(conversion.Target));
    }

    [Fact]
    public void ListsAndMaps_ConvertTheirElements_FromAnyOffset()
    {
        var builder = new ListArray.Builder(Int32Type.Default);
        var values = (Int32Array.Builder)builder.ValueBuilder;
        builder.Append(); values.Append(1).Append(2);
        builder.AppendNull();
        builder.Append(); values.Append(3);
        builder.Append();
        ListArray list = builder.Build();
        ArrowConversion conversion = Compile(list.Data.DataType, new ListType(Int64Type.Default));
        var converted = (ListArray)conversion.Convert(list.Slice(1, 3));
        Assert.True(converted.IsNull(0));
        Assert.Equal([3L], ((Int64Array)converted.GetSlicedValues(1)).Values.ToArray());
        Assert.Equal(0, converted.GetValueLength(2));
        Assert.Equal(1, converted.Values.Length);

        var map = new MapArray.Builder(new MapType(StringType.Default, Int32Type.Default));
        map.Append();
        ((StringArray.Builder)map.KeyBuilder).Append("k");
        ((Int32Array.Builder)map.ValueBuilder).Append(7);
        MapArray maps = map.Build();
        var widened = (MapArray)Compile(maps.Data.DataType, new MapType(StringType.Default, Int64Type.Default)).Convert(maps);
        Assert.Equal("k", ((StringArray)widened.Keys).GetString(0));
        Assert.Equal(7L, ((Int64Array)widened.Values).GetValue(0));

        var fixedList = new FixedSizeListArray(new FixedSizeListType(new Field("item", Int16Type.Default, true), 2), 2, (IArrowArray)new Int16Array.Builder().AppendRange([1, 2, 3, 4]).Build(), ArrowBuffer.Empty, 0);
        var fixedWide = (FixedSizeListArray)Compile(fixedList.Data.DataType, new FixedSizeListType(new Field("item", Int32Type.Default, true), 2)).Convert(fixedList.Slice(1, 1));
        Assert.Equal([3, 4], ((Int32Array)fixedWide.Values).Values.ToArray());
    }

    [Fact]
    public void Maps_ReadAsListsOfTheirEntries_FromAnyOffset()
    {
        var builder = new MapArray.Builder(new MapType(StringType.Default, Int32Type.Default));
        var keys = (StringArray.Builder)builder.KeyBuilder;
        var values = (Int32Array.Builder)builder.ValueBuilder;
        builder.Append(); keys.Append("skipped"); values.Append(0);
        builder.Append(); keys.Append("a"); values.Append(1); keys.Append("b"); values.AppendNull();
        builder.AppendNull();
        builder.Append();
        using MapArray maps = builder.Build(PooledArrowAllocator.Instance);

        // An engine without a map type: a large list of key/value structs, keys as views, values wider.
        var entry = new StructType([new Field("key", StringViewType.Default, false), new Field("value", Int64Type.Default, true)]);
        var target = new LargeListType(new Field("item", entry, true));
        Assert.True(ArrowWidening.CanConvert(maps.Data.DataType, target, default));
        using var lists = (LargeListArray)Compile(maps.Data.DataType, target).Convert(maps.Slice(1, 3));

        Assert.Equal(3, lists.Length);
        Assert.Equal(2, lists.GetValueLength(0));
        Assert.True(lists.IsNull(1));
        Assert.Equal(0, lists.GetValueLength(2));
        var entries = (StructArray)lists.GetSlicedValues(0);
        Assert.Equal(["a", "b"], [((StringViewArray)entries.Fields[0]).GetString(0), ((StringViewArray)entries.Fields[0]).GetString(1)]);
        var widened = (Int64Array)entries.Fields[1];
        Assert.Equal(1L, widened.GetValue(0));
        Assert.True(widened.IsNull(1));

        // Entries whose fields are not the map's do not convert.
        var other = new ListType(new Field("item", new StructType([new Field("k", StringType.Default, false)]), true));
        Assert.False(ArrowWidening.CanConvert(maps.Data.DataType, other, default));
    }

    [Fact]
    public void Dictionaries_ConvertToAndFromTheirValues()
    {
        var dictionary = new DictionaryArray(new DictionaryType(Int32Type.Default, StringType.Default, false),
            new Int32Array.Builder().Append(1).AppendNull().Append(0).Append(1).Build(), Strings("a", "bb"));
        var decoded = (StringArray)Compile(dictionary.Data.DataType, StringType.Default).Convert(dictionary.Slice(1, 3));
        Assert.Equal([null, "a", "bb"], Enumerable.Range(0, 3).Select(i => decoded.GetString(i)));

        // Views into a dictionary of plain strings (as a reader's dictionaries hold them): the values copied.
        var views = new StringViewArray.Builder().Append("a value longer than twelve bytes").AppendNull().Append("v").Build();
        var fromViews = (DictionaryArray)Compile(StringViewType.Default, new DictionaryType(Int32Type.Default, StringType.Default, false)).Convert(views);
        Assert.IsType<StringArray>(fromViews.Dictionary);
        Assert.Equal(["a value longer than twelve bytes", null, "v"], Enumerable.Range(0, 3).Select(i => fromViews.IsNull(i) ? null
            : ((StringArray)fromViews.Dictionary).GetString(((Int32Array)fromViews.Indices).GetValue(i)!.Value)));

        StringArray plain = Strings("x", null, "y");
        var encoded = (DictionaryArray)Compile(StringType.Default, new DictionaryType(Int32Type.Default, StringType.Default, false)).Convert(plain.Slice(1, 2));
        Assert.True(encoded.IsNull(0));
        Assert.Equal("y", ((StringArray)encoded.Dictionary).GetString(((Int32Array)encoded.Indices).GetValue(1)!.Value));
    }

    [Fact]
    public void NativeSources_AreSharedNotCopied_AndOutliveTheirSource()
    {
        // A struct whose 'b' field is unchanged: its buffers are the source's (a native batch's), kept alive by the result.
        using var builder = NativeColumnBuilder.Create(new StructType([new Field("a", Int32Type.Default, true), new Field("b", StringViewType.Default, true)]));
        var fields = (NativeStructBuilder)builder;
        for (int i = 0; i < 100; i++)
        {
            ((NativeFixedWidthBuilder<int>)fields.Children[0]).Append(i);
            ((NativeBinaryViewBuilder)fields.Children[1]).Append(System.Text.Encoding.UTF8.GetBytes($"a long string value number {i}"));
            fields.EndStruct();
        }
        IArrowArray source = builder.ExportArray();
        var target = new StructType([new Field("b", StringViewType.Default, true), new Field("a", Int64Type.Default, true)]);
        IArrowArray converted = Compile(source.Data.DataType, target).Convert(source);
        Assert.True(source.Data.Children[1].Buffers[2].Span == converted.Data.Children[0].Buffers[2].Span, "the long values are shared");
        source.Dispose();
        GC.Collect();
        GC.WaitForPendingFinalizers();
        var b = (StringViewArray)((StructArray)converted).Fields[0];
        Assert.Equal("a long string value number 99", b.GetString(99));
        Assert.Equal(99L, ((Int64Array)((StructArray)converted).Fields[1]).GetValue(99));
        converted.Dispose();
    }

    [Fact]
    public void NullArrays_OfAnyType_AreAllNull()
    {
        IArrowType[] types =
        [
            Int32Type.Default, BooleanType.Default, StringType.Default, StringViewType.Default, new Decimal256Type(50, 2), Ts(TimeUnit.Nanosecond, "UTC"),
            new StructType([new Field("a", new ListType(Int64Type.Default), true)]), new MapType(StringType.Default, DoubleType.Default),
            new FixedSizeListType(new Field("item", FloatType.Default, true), 3), new DictionaryType(Int32Type.Default, StringType.Default, false),
            NullType.Default,
        ];
        foreach (IArrowType type in types)
        {
            foreach (int length in (int[])[0, 1, 70])
            {
                using IArrowArray nulls = NullArrays.Of(type, length);
                Assert.Equal(length, nulls.Length);
                Assert.Equal(length, nulls.NullCount);
                Assert.True(ArrowTypes.SameType(type, nulls.Data.DataType));
            }
        }
    }

    /// <summary>
    /// Nulls of types no builder makes, alone or nested: dictionaries of any index and value type (over an empty
    /// dictionary), run-end encoded nulls (one run; past what the run ends' type reaches, an error), unions (each slot
    /// a null of the first child).
    /// </summary>
    [Fact]
    public void NullArrays_OfTypesWithoutABuilder_ReadAsNulls()
    {
        static DictionaryType Dictionary(IArrowType index, IArrowType values) => new(index, values, false);
        IArrowType[] counted =
        [
            Dictionary(Int8Type.Default, Int64Type.Default), Dictionary(UInt16Type.Default, StringViewType.Default),
            new StructType([new Field("d", Dictionary(Int16Type.Default, StringType.Default), true)]),
            new ListType(new Field("item", Dictionary(Int8Type.Default, StringType.Default), true)),
            new LargeListType(new Field("item", Dictionary(Int64Type.Default, BinaryType.Default), true)),
            new MapType(new Field("key", StringType.Default, false), new Field("value", Dictionary(Int8Type.Default, StringType.Default), true)),
            new FixedSizeListType(new Field("item", Dictionary(UInt8Type.Default, StringType.Default), true), 2),
            new ListViewType(new Field("item", Dictionary(Int8Type.Default, StringType.Default), true)),
            new IntervalType(IntervalUnit.YearMonth), new IntervalType(IntervalUnit.DayTime), new IntervalType(IntervalUnit.MonthDayNanosecond),
            new Decimal32Type(9, 2), new Decimal64Type(18, 3), new FixedSizeBinaryType(5), BooleanType.Default, DurationType.Microsecond,
            new StructType([new Field("i", new IntervalType(IntervalUnit.MonthDayNanosecond), true)]),
        ];
        foreach (IArrowType type in counted)
        {
            foreach (int length in (int[])[0, 1, 70])
            {
                using IArrowArray nulls = NullArrays.Of(type, length);
                Assert.Equal(length, nulls.Length);
                Assert.Equal(length, nulls.NullCount);
                Assert.True(ArrowTypes.SameType(type, nulls.Data.DataType));
            }
        }

        foreach (IArrowType ends in (IArrowType[])[Int16Type.Default, Int32Type.Default, Int64Type.Default])
        {
            var type = new RunEndEncodedType(ends, Dictionary(Int8Type.Default, StringType.Default));
            foreach (int length in (int[])[0, 1, 32_767, 70_000])
            {
                if (ends is Int16Type && length > short.MaxValue)
                {
                    Assert.Throws<OverflowException>(() => NullArrays.Of(type, length));
                    continue;
                }
                using IArrowArray nulls = NullArrays.Of(type, length);
                var ree = Assert.IsType<RunEndEncodedArray>(nulls);
                Assert.Equal(length, ree.Length);
                long[] runEnds = [.. Enumerable.Range(0, ree.RunEnds.Length).Select(i => ree.RunEnds switch
                {
                    Int16Array a => (long)a.GetValue(i)!.Value,
                    Int32Array a => a.GetValue(i)!.Value,
                    Int64Array a => a.GetValue(i)!.Value,
                    _ => -1,
                })];
                Assert.Equal(length == 0 ? 0 : 1, runEnds.Length);
                if (length > 0) Assert.Equal(length, runEnds[^1]);
                Assert.Equal(runEnds.Length, ree.Values.Length);
                Assert.Equal(ree.Values.Length, ree.Values.NullCount);
            }
        }

        foreach (UnionMode mode in (UnionMode[])[UnionMode.Sparse, UnionMode.Dense])
        {
            var type = new UnionType([new Field("i", Int32Type.Default, true), new Field("d", Dictionary(Int8Type.Default, StringType.Default), true)], [5, 7], mode);
            foreach (int length in (int[])[0, 1, 70])
            {
                using IArrowArray nulls = NullArrays.Of(type, length);
                var union = Assert.IsAssignableFrom<UnionArray>(nulls);
                Assert.Equal(length, union.Length);
                for (int i = 0; i < length; i++)
                {
                    Assert.Equal(5, union.TypeIds[i]);
                    int at = union is DenseUnionArray dense ? dense.ValueOffsets[i] : i;
                    Assert.True(union.Fields[0].IsNull(at));
                }
            }
        }
    }

    [Fact]
    public void Identity_ReturnsTheSourceItself()
    {
        Int32Array source = Ints(1, 2);
        ArrowConversion conversion = Compile(Int32Type.Default, Int32Type.Default);
        Assert.True(conversion.IsIdentity);
        Assert.Same(source, conversion.Convert(source));
    }

    // ---- helpers --------------------------------------------------------------------------------------------------

    private static ArrowConversion Compile(IArrowType from, IArrowType to)
    {
        Assert.True(ArrowWidening.TryCompile(from, to, Lossless, "x", out ArrowConversion? conversion, out ArrowConversionError? error), error?.Message);
        return conversion;
    }

    private static Int32Array Ints(params int?[] values)
    {
        var b = new Int32Array.Builder();
        foreach (int? v in values) b.Append(v);
        return b.Build();
    }

    private static DoubleArray Doubles(params double[] values) => new DoubleArray.Builder().AppendRange(values).Build();

    private static StringArray Strings(params string?[] values)
    {
        var b = new StringArray.Builder();
        foreach (string? v in values) b.Append(v);
        return b.Build();
    }

    private static TimestampArray Stamps(TimestampType type, params long[] values)
    {
        var data = new ArrayData(type, values.Length, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(values.SelectMany(BitConverter.GetBytes).ToArray())]);
        return new TimestampArray(data);
    }

    private static StructArray Struct(Field[] fields, IArrowArray[] children, bool[] validity)
    {
        var bits = new byte[(validity.Length + 7) / 8];
        for (int i = 0; i < validity.Length; i++) if (validity[i]) bits[i / 8] |= (byte)(1 << (i % 8));
        return new StructArray(new StructType(fields), validity.Length, children, new ArrowBuffer(bits), validity.Count(v => !v));
    }

    private static long[] Longs(IArrowArray array) => ((Int64Array)array).Values.ToArray();

    private static int DecimalWidth(IArrowType type) => type switch
    {
        Decimal32Type => 4,
        Decimal64Type => 8,
        Decimal128Type => 16,
        _ => 32,
    };

    /// <summary>A decimal array of unscaled values; null slots hold the width's largest value (anything may be there).</summary>
    private static ArrayData Decimals(IArrowType type, BigInteger?[] unscaled)
    {
        int width = DecimalWidth(type);
        var values = new byte[unscaled.Length * width];
        var bits = new byte[(unscaled.Length + 7) / 8];
        for (int i = 0; i < unscaled.Length; i++)
        {
            Span<byte> slot = values.AsSpan(i * width, width);
            if (unscaled[i] is { } v)
            {
                bits[i / 8] |= (byte)(1 << (i % 8));
                slot.Fill(v.Sign < 0 ? (byte)0xFF : (byte)0);
                Assert.True(v.TryWriteBytes(slot, out _, isUnsigned: false, isBigEndian: false));
            }
            else
            {
                slot.Fill(0xFF);
                slot[^1] = 0x7F;
            }
        }
        return new ArrayData(type, unscaled.Length, unscaled.Count(v => v is null), 0, [new ArrowBuffer(bits), new ArrowBuffer(values)]);
    }

    private static BigInteger Unscaled(IArrowArray array, int i)
    {
        int width = DecimalWidth(array.Data.DataType);
        return new BigInteger(array.Data.Buffers[1].Span.Slice((array.Data.Offset + i) * width, width), isUnsigned: false, isBigEndian: false);
    }

    private static double Sample(Random random, IArrowType type) => type switch
    {
        Int8Type => random.Next(sbyte.MinValue, sbyte.MaxValue + 1),
        UInt8Type => random.Next(256),
        Int16Type => random.Next(short.MinValue, short.MaxValue + 1),
        UInt16Type => random.Next(65536),
        Int32Type => random.Next(4) == 0 ? (random.Next(2) == 0 ? int.MinValue : int.MaxValue) : random.Next(),
        UInt32Type => random.Next(4) == 0 ? uint.MaxValue : (uint)random.NextInt64(uint.MaxValue),
        Int64Type => random.Next(4) == 0 ? (random.Next(2) == 0 ? long.MinValue : long.MaxValue) : random.NextInt64(long.MinValue, long.MaxValue),
        UInt64Type => random.Next(4) == 0 ? ulong.MaxValue : (ulong)random.NextInt64(long.MaxValue) * 2,
        HalfFloatType => (double)(Half)(random.NextDouble() * 1000 - 500),
        FloatType => random.Next(6) == 0 ? float.NaN : (float)(random.NextDouble() * 1e30 - 5e29),
        _ => random.Next(6) == 0 ? double.NegativeInfinity : random.NextDouble() * 1e300 - 5e299,
    };

    /// <summary>A sample as the source holds it, converted by the CLR into the target type, as a double for comparison.</summary>
    private static double Expected(IArrowType from, IArrowType to, double v)
    {
        object typed = from switch
        {
            Int8Type => (sbyte)v, UInt8Type => (byte)v, Int16Type => (short)v, UInt16Type => (ushort)v, Int32Type => (int)v, UInt32Type => (uint)v,
            Int64Type => v >= 9.2233720368547758E18 ? long.MaxValue : (long)v,
            UInt64Type => v >= 1.8446744073709552E19 ? ulong.MaxValue : (ulong)v,
            HalfFloatType => (Half)v, FloatType => (float)v, _ => v,
        };
        return to switch
        {
            HalfFloatType => (double)(typed switch
            {
                sbyte x => (Half)x, byte x => (Half)x, short x => (Half)x, ushort x => (Half)x, int x => (Half)x, uint x => (Half)x,
                long x => (Half)x, ulong x => (Half)x, Half x => x, float x => (Half)x, double x => (Half)x, _ => throw new NotSupportedException(),
            }),
            FloatType => typed switch
            {
                sbyte x => (float)x, byte x => x, short x => x, ushort x => x, int x => x, uint x => x,
                long x => x, ulong x => x, Half x => (float)x, float x => x, double x => (float)x, _ => throw new NotSupportedException(),
            },
            _ => typed switch
            {
                sbyte x => x, byte x => x, short x => x, ushort x => x, int x => x, uint x => x,
                long x => x, ulong x => x, Half x => (double)x, float x => x, double x => x, _ => throw new NotSupportedException(),
            },
        };
    }

    private static bool Same(double? a, double? b) => a is null || b is null ? a is null && b is null : a.Value.Equals(b.Value);

    private static IArrowArray Numbers(IArrowType type, double?[] values)
    {
        IArrowArray Build<T>(Func<double, T> cast) where T : unmanaged
        {
            var data = new T[values.Length];
            var validity = new byte[(values.Length + 7) / 8];
            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] is not { } v) continue;
                data[i] = cast(v);
                validity[i / 8] |= (byte)(1 << (i % 8));
            }
            byte[] bytes = System.Runtime.InteropServices.MemoryMarshal.AsBytes(data.AsSpan()).ToArray();
            int nulls = values.Count(v => v is null);
            return ArrowArrayFactory.BuildArray(new ArrayData(type, values.Length, nulls, 0, [new ArrowBuffer(validity), new ArrowBuffer(bytes)]));
        }
        return type switch
        {
            Int8Type => Build(v => (sbyte)v),
            UInt8Type => Build(v => (byte)v),
            Int16Type => Build(v => (short)v),
            UInt16Type => Build(v => (ushort)v),
            Int32Type => Build(v => (int)v),
            UInt32Type => Build(v => (uint)v),
            Int64Type => Build(v => v >= 9.2233720368547758E18 ? long.MaxValue : (long)v),
            UInt64Type => Build(v => v >= 1.8446744073709552E19 ? ulong.MaxValue : (ulong)v),
            HalfFloatType => Build(v => (Half)v),
            FloatType => Build(v => (float)v),
            _ => Build(v => v),
        };
    }

    private static double? Number(IArrowArray array, int i) => array.IsNull(i) ? null : array switch
    {
        Int8Array a => a.GetValue(i), UInt8Array a => a.GetValue(i), Int16Array a => a.GetValue(i), UInt16Array a => a.GetValue(i),
        Int32Array a => a.GetValue(i), UInt32Array a => a.GetValue(i), Int64Array a => a.GetValue(i), UInt64Array a => a.GetValue(i),
        HalfFloatArray a => (double?)a.GetValue(i), FloatArray a => a.GetValue(i), DoubleArray a => a.GetValue(i),
        _ => throw new NotSupportedException(array.GetType().Name),
    };
}
