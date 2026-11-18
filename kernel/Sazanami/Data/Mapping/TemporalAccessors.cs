using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Collections;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using Microsoft.FSharp.Collections;
using Microsoft.FSharp.Core;
using Kaminari.Sazanami.Arrow.TypeSystem;
using Kaminari.Sazanami.Json;
using Kaminari.Sazanami.Marshalling;

namespace Kaminari.Sazanami.Data.Mapping;

internal sealed unsafe class Int64ToDateTimeAccessor : TypedColumnAccessor<DateTime>,
    ITypedAccessor<DateTime>, ITypedAccessor<DateTime?>, ITypedAccessor<DateTimeOffset>, ITypedAccessor<DateTimeOffset?>
{
    private long* _ptr;
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => typeof(DateTime);
    public override bool UsesFastNullBitmap => true;
    public override void SetBatch(IArrowArray array) => _ptr = BindPrimitiveBuffer<long>((Int64Array)array);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool IsNull(int index) => IsNullFast(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override DateTime GetTypedValue(int index)
        => DateTime.UnixEpoch.AddMicroseconds(_ptr[index]);

    DateTime ITypedAccessor<DateTime>.GetTypedValue(int index) => GetTypedValue(index);
    DateTimeOffset ITypedAccessor<DateTimeOffset>.GetTypedValue(int index) => new(GetTypedValue(index));

    DateTime? ITypedAccessor<DateTime?>.GetTypedValue(int index) => IsNull(index) ? null : GetTypedValue(index);
    DateTimeOffset? ITypedAccessor<DateTimeOffset?>.GetTypedValue(int index) => IsNull(index) ? null : new DateTimeOffset(GetTypedValue(index));
}

internal sealed unsafe class Int64ToTimeSpanAccessor : TypedColumnAccessor<TimeSpan>,
    ITypedAccessor<TimeSpan>, ITypedAccessor<TimeSpan?>, ITypedAccessor<long>, ITypedAccessor<long?>
{
    private long* _ptr;
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => typeof(TimeSpan);
    public override bool UsesFastNullBitmap => true;
    public override void SetBatch(IArrowArray array) => _ptr = BindPrimitiveBuffer<long>((Int64Array)array);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool IsNull(int index) => IsNullFast(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override TimeSpan GetTypedValue(int index)
        => new(_ptr[index] * 10);

    TimeSpan ITypedAccessor<TimeSpan>.GetTypedValue(int index) => GetTypedValue(index);
    long ITypedAccessor<long>.GetTypedValue(int index) => _ptr[index] * 10;
    TimeSpan? ITypedAccessor<TimeSpan?>.GetTypedValue(int index) => IsNullFast(index) ? null : GetTypedValue(index);
    long? ITypedAccessor<long?>.GetTypedValue(int index) => IsNullFast(index) ? null : _ptr[index] * 10;
}

internal sealed unsafe class TimestampAccessor : TypedColumnAccessor<DateTime>,
    ITypedAccessor<DateTime>, ITypedAccessor<DateTime?>,
    ITypedAccessor<DateTimeOffset>, ITypedAccessor<DateTimeOffset?>,
    ITypedAccessor<DateOnly>, ITypedAccessor<DateOnly?>
{
    private long* _ptr;
    private TimeUnit _unit;
    private bool _hasTimeZone;
    private TimeZoneInfo? _timeZoneInfo;
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    private readonly Type _targetType;

    public TimestampAccessor(Type? targetType = null)
    {
        _targetType = targetType == typeof(DateTimeOffset) ? typeof(DateTimeOffset) : typeof(DateTime);
    }

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => _targetType;
    public override bool UsesFastNullBitmap => true;

    public override void SetBatch(IArrowArray array)
    {
        var tsArray = (TimestampArray)array;
        _ptr = BindPrimitiveBuffer<long>(tsArray);
        var tsType = (TimestampType)tsArray.Data.DataType;
        _unit = tsType.Unit;
        _hasTimeZone = !string.IsNullOrEmpty(tsType.Timezone);
        _timeZoneInfo = _hasTimeZone ? ResolveTimeZoneSafe(tsType.Timezone!) : null;
    }

    private static TimeZoneInfo? ResolveTimeZoneSafe(string tz)
    {
        try
        {
            return TimeZoneInfo.FindSystemTimeZoneById(tz);
        }
        catch (TimeZoneNotFoundException) { }
        catch (InvalidTimeZoneException) { }

        string cleanTz = tz.StartsWith("UTC", StringComparison.OrdinalIgnoreCase)
            ? tz[3..].Trim()
            : tz.Trim();

        if (cleanTz.StartsWith('+'))
        {
            cleanTz = cleanTz[1..];
        }

        if (TimeSpan.TryParse(cleanTz, out TimeSpan offset))
        {
            return TimeZoneInfo.CreateCustomTimeZone(tz, offset, tz, tz);
        }

        return null;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool IsNull(int index) => IsNullFast(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private long GetTicks(int index)
    {
        long val = _ptr[index];
        return _unit switch
        {
            TimeUnit.Nanosecond => val / 100L,
            TimeUnit.Microsecond => val * 10L,
            TimeUnit.Millisecond => val * 10_000L,
            TimeUnit.Second => val * 10_000_000L,
            _ => val
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override DateTime GetTypedValue(int index)
    {
        long ticks = GetTicks(index);
        return new DateTime(
            DateTime.UnixEpoch.Ticks + ticks,
            _hasTimeZone ? DateTimeKind.Utc : DateTimeKind.Unspecified);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private DateTimeOffset GetDateTimeOffsetValue(int index)
    {
        long ticks = GetTicks(index);
        DateTimeOffset dtoUtc = new(DateTime.UnixEpoch.Ticks + ticks, TimeSpan.Zero);
        if (_timeZoneInfo is null)
        {
            return dtoUtc;
        }
        TimeSpan offset = _timeZoneInfo.GetUtcOffset(dtoUtc);
        return dtoUtc.ToOffset(offset);
    }

    public override object GetValue(int index)
    {
        if (IsNull(index)) return DBNull.Value;
        if (_targetType == typeof(DateTimeOffset)) return GetDateTimeOffsetValue(index);
        return GetTypedValue(index);
    }

    DateTime ITypedAccessor<DateTime>.GetTypedValue(int index) => GetTypedValue(index);
    DateTimeOffset ITypedAccessor<DateTimeOffset>.GetTypedValue(int index) => GetDateTimeOffsetValue(index);
    DateOnly ITypedAccessor<DateOnly>.GetTypedValue(int index) => DateOnly.FromDateTime(GetTypedValue(index));

    DateTime? ITypedAccessor<DateTime?>.GetTypedValue(int index) => IsNull(index) ? null : GetTypedValue(index);
    DateTimeOffset? ITypedAccessor<DateTimeOffset?>.GetTypedValue(int index) => IsNull(index) ? null : GetDateTimeOffsetValue(index);
    DateOnly? ITypedAccessor<DateOnly?>.GetTypedValue(int index) => IsNull(index) ? null : DateOnly.FromDateTime(GetTypedValue(index));
}

internal sealed unsafe class Date32ToDateOnlyAccessor : TypedColumnAccessor<DateOnly>,
    ITypedAccessor<DateOnly>, ITypedAccessor<DateTime>, ITypedAccessor<DateTimeOffset>,
    ITypedAccessor<DateOnly?>, ITypedAccessor<DateTime?>, ITypedAccessor<DateTimeOffset?>
{
    private int* _ptr;
    private const int UnixEpochDayNumber = 719162;
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    private readonly Type _targetType;

    public Date32ToDateOnlyAccessor(Type? targetType = null)
    {
        _targetType = targetType == typeof(DateTime) ? typeof(DateTime) : typeof(DateOnly);
    }

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => _targetType;
    public override bool UsesFastNullBitmap => true;
    public override void SetBatch(IArrowArray array) => _ptr = BindPrimitiveBuffer<int>((Date32Array)array);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool IsNull(int index) => IsNullFast(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override DateOnly GetTypedValue(int index)
        => DateOnly.FromDayNumber(_ptr[index] + UnixEpochDayNumber);

    public override object GetValue(int index)
    {
        if (IsNull(index)) return DBNull.Value;
        if (_targetType == typeof(DateTime)) return GetTypedValue(index).ToDateTime(TimeOnly.MinValue);
        return GetTypedValue(index);
    }

    DateOnly ITypedAccessor<DateOnly>.GetTypedValue(int index) => GetTypedValue(index);
    DateTime ITypedAccessor<DateTime>.GetTypedValue(int index)
        => GetTypedValue(index).ToDateTime(TimeOnly.MinValue);
    DateTimeOffset ITypedAccessor<DateTimeOffset>.GetTypedValue(int index)
        => new(GetTypedValue(index).ToDateTime(TimeOnly.MinValue), TimeSpan.Zero);

    DateOnly? ITypedAccessor<DateOnly?>.GetTypedValue(int index) => IsNull(index) ? null : GetTypedValue(index);
    DateTime? ITypedAccessor<DateTime?>.GetTypedValue(int index)
        => IsNull(index) ? null : GetTypedValue(index).ToDateTime(TimeOnly.MinValue);
    DateTimeOffset? ITypedAccessor<DateTimeOffset?>.GetTypedValue(int index)
        => IsNull(index) ? null : new DateTimeOffset(GetTypedValue(index).ToDateTime(TimeOnly.MinValue), TimeSpan.Zero);
}

internal sealed unsafe class Date64Accessor : TypedColumnAccessor<DateTime>,
    ITypedAccessor<DateTime>, ITypedAccessor<DateTimeOffset>, ITypedAccessor<DateOnly>,
    ITypedAccessor<DateTime?>, ITypedAccessor<DateTimeOffset?>, ITypedAccessor<DateOnly?>
{
    private long* _ptr;
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => typeof(DateTime);
    public override bool UsesFastNullBitmap => true;
    public override void SetBatch(IArrowArray array) => _ptr = BindPrimitiveBuffer<long>((Date64Array)array);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool IsNull(int index) => IsNullFast(index);

    // Date64 = milliseconds since epoch; Arrow's GetDateTime returns DateTimeKind.Unspecified.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override DateTime GetTypedValue(int index)
        => new(DateTime.UnixEpoch.Ticks + _ptr[index] * TimeSpan.TicksPerMillisecond, DateTimeKind.Unspecified);

    DateTime ITypedAccessor<DateTime>.GetTypedValue(int index) => GetTypedValue(index);
    DateTimeOffset ITypedAccessor<DateTimeOffset>.GetTypedValue(int index) => new(GetTypedValue(index));
    DateOnly ITypedAccessor<DateOnly>.GetTypedValue(int index) => DateOnly.FromDateTime(GetTypedValue(index));

    DateTime? ITypedAccessor<DateTime?>.GetTypedValue(int index) => IsNull(index) ? null : GetTypedValue(index);
    DateTimeOffset? ITypedAccessor<DateTimeOffset?>.GetTypedValue(int index) => IsNull(index) ? null : new DateTimeOffset(GetTypedValue(index));
    DateOnly? ITypedAccessor<DateOnly?>.GetTypedValue(int index) => IsNull(index) ? null : DateOnly.FromDateTime(GetTypedValue(index));
}

internal sealed unsafe class DurationAccessor : TypedColumnAccessor<TimeSpan>,
    ITypedAccessor<TimeSpan>, ITypedAccessor<TimeSpan?>, ITypedAccessor<long>, ITypedAccessor<long?>
{
    private long* _ptr;
    private TimeUnit _unit;

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => typeof(TimeSpan);
    public override bool UsesFastNullBitmap => true;

    public override void SetBatch(IArrowArray array)
    {
        var durArray = (DurationArray)array;
        _ptr = BindPrimitiveBuffer<long>(durArray);
        _unit = ((DurationType)durArray.Data.DataType).Unit;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool IsNull(int index) => IsNullFast(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override TimeSpan GetTypedValue(int index)
    {
        long val = _ptr[index];
        long ticks = _unit switch
        {
            TimeUnit.Nanosecond => val / 100L,
            TimeUnit.Microsecond => val * 10L,
            TimeUnit.Millisecond => val * 10_000L,
            TimeUnit.Second => val * 10_000_000L,
            _ => val
        };
        return new TimeSpan(ticks);
    }

    TimeSpan ITypedAccessor<TimeSpan>.GetTypedValue(int index) => GetTypedValue(index);
    long ITypedAccessor<long>.GetTypedValue(int index) => GetTypedValue(index).Ticks;

    TimeSpan? ITypedAccessor<TimeSpan?>.GetTypedValue(int index) => IsNull(index) ? null : GetTypedValue(index);
    long? ITypedAccessor<long?>.GetTypedValue(int index) => IsNull(index) ? null : GetTypedValue(index).Ticks;
}

internal sealed unsafe class Int64ToTimeOnlyAccessor : TypedColumnAccessor<TimeOnly>,
    ITypedAccessor<TimeOnly>, ITypedAccessor<TimeOnly?>,
    ITypedAccessor<TimeSpan>, ITypedAccessor<TimeSpan?>
{
    private long* _ptr;
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => typeof(TimeOnly);
    public override bool UsesFastNullBitmap => true;
    public override void SetBatch(IArrowArray array) => _ptr = BindPrimitiveBuffer<long>((Int64Array)array);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool IsNull(int index) => IsNullFast(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override TimeOnly GetTypedValue(int index)
    {
        long ticks = _ptr[index] * 10;
        return new TimeOnly(ticks);
    }

    TimeOnly ITypedAccessor<TimeOnly>.GetTypedValue(int index) => GetTypedValue(index);
    TimeSpan ITypedAccessor<TimeSpan>.GetTypedValue(int index) => GetTypedValue(index).ToTimeSpan();
    TimeOnly? ITypedAccessor<TimeOnly?>.GetTypedValue(int index) => IsNull(index) ? null : GetTypedValue(index);
    TimeSpan? ITypedAccessor<TimeSpan?>.GetTypedValue(int index) => IsNull(index) ? null : GetTypedValue(index).ToTimeSpan();
}

internal sealed unsafe class Time32Accessor : TypedColumnAccessor<TimeOnly>,
    ITypedAccessor<TimeOnly>, ITypedAccessor<TimeOnly?>,
    ITypedAccessor<TimeSpan>, ITypedAccessor<TimeSpan?>
{
    private int* _ptr;
    private bool _isMillisecond;

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => typeof(TimeOnly);
    public override bool UsesFastNullBitmap => true;

    public override void SetBatch(IArrowArray array)
    {
        var t32Array = (Time32Array)array;
        _ptr = BindPrimitiveBuffer<int>(t32Array);
        var unit = ((Time32Type)t32Array.Data.DataType).Unit;
        _isMillisecond = unit == TimeUnit.Millisecond;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool IsNull(int index) => IsNullFast(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override TimeOnly GetTypedValue(int index)
    {
        int val = _ptr[index];
        long ticks = _isMillisecond ? (val * 10_000L) : (val * 10_000_000L);
        return new TimeOnly(ticks);
    }

    TimeOnly ITypedAccessor<TimeOnly>.GetTypedValue(int index) => GetTypedValue(index);
    TimeSpan ITypedAccessor<TimeSpan>.GetTypedValue(int index) => GetTypedValue(index).ToTimeSpan();

    TimeOnly? ITypedAccessor<TimeOnly?>.GetTypedValue(int index) => IsNull(index) ? null : GetTypedValue(index);
    TimeSpan? ITypedAccessor<TimeSpan?>.GetTypedValue(int index) => IsNull(index) ? null : GetTypedValue(index).ToTimeSpan();
}

internal sealed unsafe class Time64Accessor : TypedColumnAccessor<TimeOnly>,
    ITypedAccessor<TimeOnly>, ITypedAccessor<TimeOnly?>,
    ITypedAccessor<TimeSpan>, ITypedAccessor<TimeSpan?>
{
    private long* _ptr;
    private bool _isNanosecond;

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => typeof(TimeOnly);
    public override bool UsesFastNullBitmap => true;

    public override void SetBatch(IArrowArray array)
    {
        var t64Array = (Time64Array)array;
        _ptr = BindPrimitiveBuffer<long>(t64Array);
        var unit = ((Time64Type)t64Array.Data.DataType).Unit;
        _isNanosecond = unit == TimeUnit.Nanosecond;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool IsNull(int index) => IsNullFast(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override TimeOnly GetTypedValue(int index)
    {
        long val = _ptr[index];
        long ticks = _isNanosecond ? (val / 100L) : (val * 10L);
        return new TimeOnly(ticks);
    }

    TimeOnly ITypedAccessor<TimeOnly>.GetTypedValue(int index) => GetTypedValue(index);
    TimeSpan ITypedAccessor<TimeSpan>.GetTypedValue(int index) => GetTypedValue(index).ToTimeSpan();

    TimeOnly? ITypedAccessor<TimeOnly?>.GetTypedValue(int index) => IsNull(index) ? null : GetTypedValue(index);
    TimeSpan? ITypedAccessor<TimeSpan?>.GetTypedValue(int index) => IsNull(index) ? null : GetTypedValue(index).ToTimeSpan();
}
