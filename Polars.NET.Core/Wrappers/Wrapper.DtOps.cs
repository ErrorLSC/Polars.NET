using Polars.NET.Core.Native;

namespace Polars.NET.Core;

public readonly partial struct PolarsWrapper
{
    public static ExprHandle DtMillennium(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_dt_millennium, e);
    public static ExprHandle DtCentury(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_dt_century, e);
    public static ExprHandle DtIsoYear(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_dt_iso_year, e);
    public static ExprHandle DtYear(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_dt_year, e);
    public static ExprHandle DtQuarter(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_dt_quarter, e);
    public static ExprHandle DtMonth(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_dt_month, e);
    public static ExprHandle DtDay(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_dt_day, e);
    public static ExprHandle DtDaysInMonth(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_dt_days_in_month, e);
    public static ExprHandle DtOrdinalDay(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_dt_ordinal_day, e);
    public static ExprHandle DtWeekday(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_dt_weekday, e);
    public static ExprHandle DtHour(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_dt_hour, e);
    public static ExprHandle DtMinute(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_dt_minute, e);
    public static ExprHandle DtSecond(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_dt_second, e);
    public static ExprHandle DtMillisecond(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_dt_millisecond, e);
    public static ExprHandle DtMicrosecond(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_dt_microsecond, e);
    public static ExprHandle DtNanosecond(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_dt_nanosecond, e);
    public static ExprHandle DtIsLeapYear(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_dt_is_leap_year, e);
    public static ExprHandle DtMonthStart(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_dt_month_start, e);
    public static ExprHandle DtMonthEnd(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_dt_month_end, e);
    public static ExprHandle DtBaseUtcOffset(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_dt_base_utc_offset, e);
    public static ExprHandle DtDstOffset(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_dt_dst_offset, e);
    public static ExprHandle DtDatetime(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_dt_datetime, e);
    public static ExprHandle DtDate(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_dt_date, e);
    public static ExprHandle DtTime(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_dt_time, e);
    public static ExprHandle DtTotalDays(ExprHandle e,bool fractional) => UnaryBoolOp(NativeBindings.pl_expr_dt_total_days, e,fractional);
    public static ExprHandle DtTotalHours(ExprHandle e,bool fractional) => UnaryBoolOp(NativeBindings.pl_expr_dt_total_hours, e,fractional);
    public static ExprHandle DtTotalMinutes(ExprHandle e,bool fractional) => UnaryBoolOp(NativeBindings.pl_expr_dt_total_minutes, e,fractional);
    public static ExprHandle DtTotalSeconds(ExprHandle e,bool fractional) => UnaryBoolOp(NativeBindings.pl_expr_dt_total_seconds, e,fractional);
    public static ExprHandle DtTotalMilliseconds(ExprHandle e,bool fractional) => UnaryBoolOp(NativeBindings.pl_expr_dt_total_milliseconds, e,fractional);
    public static ExprHandle DtTotalMicroseconds(ExprHandle e,bool fractional) => UnaryBoolOp(NativeBindings.pl_expr_dt_total_microseconds, e,fractional);
    public static ExprHandle DtTotalNanoseconds(ExprHandle e,bool fractional) => UnaryBoolOp(NativeBindings.pl_expr_dt_total_nanoseconds, e,fractional);
    public static ExprHandle DtToString(ExprHandle e, string format)
    {
        var h = NativeBindings.pl_expr_dt_to_string(e, format);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle DtTruncate(ExprHandle e, ExprHandle every) => BinaryOp(NativeBindings.pl_expr_dt_truncate, e, every);
    public static ExprHandle DtRound(ExprHandle e, ExprHandle every) => BinaryOp(NativeBindings.pl_expr_dt_round, e, every);
    public static ExprHandle DtOffsetBy(ExprHandle e, ExprHandle by) => BinaryOp(NativeBindings.pl_expr_dt_offset_by, e, by);
    public static ExprHandle DtTimestamp(ExprHandle e, PlTimeUnit unitCode)
    {
        var h = NativeBindings.pl_expr_dt_timestamp(e, unitCode);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle DtCombine(ExprHandle expr, ExprHandle time, PlTimeUnit tu)
    {
        var h = NativeBindings.pl_expr_dt_combine(expr, time, tu);
        
        expr.TransferOwnership();
        time.TransferOwnership();
        
        return ErrorHelper.Check(h);
    }
    public static ExprHandle DtConvertTimeZone(ExprHandle e, string timeZone)
    {
        var h = NativeBindings.pl_expr_dt_convert_time_zone(e, timeZone);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle DtReplaceTimeZone(
        ExprHandle e, 
        string? timeZone, 
        ExprHandle ambiguous, 
        PlNonExistent nonExistent)
    {
        var h = NativeBindings.pl_expr_dt_replace_time_zone(e, timeZone, ambiguous, nonExistent);
        e.TransferOwnership();
        ambiguous.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle DtAddBusinessDays(
        ExprHandle expr, 
        ExprHandle n, 
        bool[] weekMask, 
        ExprHandle holidays,
        PlRoll roll) 
    {
        if (weekMask.Length != 7) 
            throw new ArgumentException("Week mask must have length 7.");

        var maskBytes = new byte[7];
        for (int i = 0; i < 7; i++) maskBytes[i] = weekMask[i] ? (byte)1 : (byte)0;

        var h = ErrorHelper.Check(NativeBindings.pl_expr_add_business_days(
                expr,
                n,
                maskBytes,
                holidays,
                roll
            ));
        expr.TransferOwnership();
        n.TransferOwnership();
        holidays.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle DtIsBusinessDay(
        ExprHandle expr,
        bool[] weekMask,
        ExprHandle holidays)
    {
        if (weekMask.Length != 7) 
            throw new ArgumentException("Week mask must have length 7.");

        var maskBytes = new byte[7];
        for (int i = 0; i < 7; i++) maskBytes[i] = weekMask[i] ? (byte)1 : (byte)0;

        var h = ErrorHelper.Check(NativeBindings.pl_expr_is_business_day(
            expr,
            maskBytes,
            holidays
        ));
        holidays.TransferOwnership();
        expr.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle DtBusinessDayCount(
        ExprHandle start, 
        ExprHandle end, 
        bool[] weekMask, 
        ExprHandle holidays) 
    {
        if (weekMask.Length != 7) 
            throw new ArgumentException("Week mask must have length 7.");

        var maskBytes = new byte[7];
        for (int i = 0; i < 7; i++) maskBytes[i] = weekMask[i] ? (byte)1 : (byte)0;

        var h = ErrorHelper.Check(NativeBindings.pl_expr_business_day_count(
                start,
                end,
                maskBytes,
                holidays
            ));
        start.TransferOwnership();
        end.TransferOwnership();
        holidays.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle DtCastTimeUnit(ExprHandle expr,PlTimeUnit unit)
    {
        var h = NativeBindings.pl_expr_dt_cast_time_unit(expr,unit);
        expr.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle DtWithTimeUnit(ExprHandle expr,PlTimeUnit unit)
    {
        var h = NativeBindings.pl_expr_dt_with_time_unit(expr,unit);
        expr.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle DtReplace(
        ExprHandle expr,
        ExprHandle year,
        ExprHandle month,
        ExprHandle day,
        ExprHandle hour,
        ExprHandle minute,
        ExprHandle second,
        ExprHandle microsecond,
        ExprHandle ambiguous
    )
    {
        var h = NativeBindings.pl_expr_dt_replace(expr,year,month,day,hour,minute,second,microsecond,ambiguous);
        expr.TransferOwnership();
        year.TransferOwnership();
        month.TransferOwnership();
        day.TransferOwnership();
        hour.TransferOwnership();
        minute.TransferOwnership();
        second.TransferOwnership();
        microsecond.TransferOwnership();
        ambiguous.TransferOwnership();
        return ErrorHelper.Check(h);   
    }

}