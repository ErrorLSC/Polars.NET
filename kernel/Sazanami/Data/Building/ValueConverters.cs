using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.FSharp.Core;
using Kaminari.Sazanami.Arrow.Builders;
using Kaminari.Sazanami.Arrow.Interop;
using Kaminari.Sazanami.Marshalling;

namespace Kaminari.Sazanami.Data.Building;

/// <summary>Converts a .NET value to the fixed-width value Arrow stores.</summary>
internal interface IArrowValueConverter<TIn, TOut>
{
    static abstract TOut Convert(TIn value);
}

internal readonly struct Same<T> : IArrowValueConverter<T, T>
{
    public static T Convert(T value) => value;
}

internal readonly struct UInt128Bits : IArrowValueConverter<UInt128, Int128>
{
    public static Int128 Convert(UInt128 value) => (Int128)value; // the same bits (unchecked)
}

internal readonly struct EnumToUnderlying<TEnum, TUnderlying> : IArrowValueConverter<TEnum, TUnderlying>
    where TEnum : struct, Enum where TUnderlying : unmanaged
{
    public static TUnderlying Convert(TEnum value) => Unsafe.As<TEnum, TUnderlying>(ref value);
}

/// <summary>Wall clock ticks since the Unix epoch, in microseconds (toward zero, as the engine converts them).</summary>
internal readonly struct DateTimeToMicros : IArrowValueConverter<DateTime, long>
{
    public static long Convert(DateTime value) => (value.Ticks - DateTime.UnixEpoch.Ticks) / TimeSpan.TicksPerMicrosecond;
}

internal readonly struct DateTimeOffsetToMicros : IArrowValueConverter<DateTimeOffset, long>
{
    public static long Convert(DateTimeOffset value) => (value.UtcTicks - DateTime.UnixEpoch.Ticks) / TimeSpan.TicksPerMicrosecond;
}

internal readonly struct TimeSpanToMicros : IArrowValueConverter<TimeSpan, long>
{
    public static long Convert(TimeSpan value) => value.Ticks / TimeSpan.TicksPerMicrosecond;
}

internal readonly struct TimeOnlyToNanos : IArrowValueConverter<TimeOnly, long>
{
    public static long Convert(TimeOnly value) => value.Ticks * 100;
}

internal readonly struct DateOnlyToDays : IArrowValueConverter<DateOnly, int>
{
    private static readonly int Epoch = DateOnly.FromDateTime(DateTime.UnixEpoch).DayNumber;

    public static int Convert(DateOnly value) => value.DayNumber - Epoch;
}

/// <summary>A decimal as decimal(38, 18): its value times 10^18, refused when that is not a whole number below 10^38.</summary>
internal readonly struct DecimalToScaled : IArrowValueConverter<decimal, Int128>
{
    private const int Scale = 18;
    private static readonly UInt128 Limit = UInt128.Parse("100000000000000000000000000000000000000"); // 10^38

    public static Int128 Convert(decimal value)
    {
        Span<int> bits = stackalloc int[4];
        decimal.GetBits(value, bits);
        UInt128 mantissa = ((UInt128)(uint)bits[2] << 64) | ((ulong)(uint)bits[1] << 32) | (uint)bits[0];
        int scale = (bits[3] >> 16) & 0xFF;
        if (scale <= Scale)
        {
            UInt128 factor = Pow10(Scale - scale);
            if (mantissa > (Limit - 1) / factor) throw Unfit(value);
            mantissa *= factor;
        }
        else
        {
            UInt128 divisor = Pow10(scale - Scale);
            if (mantissa % divisor != 0) throw Unfit(value);
            mantissa /= divisor;
        }
        Int128 scaled = (Int128)mantissa;
        return bits[3] < 0 ? -scaled : scaled;
    }

    private static UInt128 Pow10(int exponent)
    {
        UInt128 result = 1;
        for (int i = 0; i < exponent; i++) result *= 10;
        return result;
    }

    private static OverflowException Unfit(decimal value) =>
        new($"The decimal {value} does not fit decimal(38, 18) exactly: at most 20 integer and 18 fractional digits (round it, e.g. Math.Round(value, 18)).");
}
