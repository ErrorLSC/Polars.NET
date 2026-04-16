using System.Runtime.InteropServices;

namespace Polars.NET.Core.Native;

internal partial class NativeBindings
{
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_millennium(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_century(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_iso_year(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_year(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_quarter(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_month(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_day(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_days_in_month(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_ordinal_day(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_weekday(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_hour(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_minute(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_second(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_millisecond(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_microsecond(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_nanosecond(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_is_leap_year(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_month_start(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_month_end(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_base_utc_offset(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_dst_offset(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_datetime(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_date(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_time(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_total_days(ExprHandle expr,[MarshalAs(UnmanagedType.U1)]bool fractional);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_total_hours(ExprHandle expr,[MarshalAs(UnmanagedType.U1)]bool fractional);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_total_minutes(ExprHandle expr,[MarshalAs(UnmanagedType.U1)]bool fractional);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_total_seconds(ExprHandle expr,[MarshalAs(UnmanagedType.U1)]bool fractional);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_total_milliseconds(ExprHandle expr,[MarshalAs(UnmanagedType.U1)]bool fractional);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_total_microseconds(ExprHandle expr,[MarshalAs(UnmanagedType.U1)]bool fractional);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_total_nanoseconds(ExprHandle expr,[MarshalAs(UnmanagedType.U1)]bool fractional);
    
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_dt_to_string(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string format);
    
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_dt_truncate(ExprHandle e, ExprHandle every);

    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_dt_round(ExprHandle e, ExprHandle every);

    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_dt_offset_by(ExprHandle e, ExprHandle by);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_dt_combine(ExprHandle e, ExprHandle time,PlTimeUnit timeUnit);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_dt_cast_time_unit(ExprHandle e,PlTimeUnit timeUnit);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_dt_with_time_unit(ExprHandle e,PlTimeUnit timeUnit);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_dt_timestamp(ExprHandle e, PlTimeUnit unitCode);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_dt_convert_time_zone(ExprHandle e, string timeZone);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_dt_replace_time_zone(
        ExprHandle e, 
        string? timeZone, 
        ExprHandle ambiguous, 
        PlNonExistent nonExistent
    );
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_add_business_days(
        ExprHandle expr,
        ExprHandle n,
        [In, MarshalAs(UnmanagedType.LPArray, SizeConst = 7)] byte[] weekMask,
        [In, MarshalAs(UnmanagedType.LPArray)] int[] holidays,     
        UIntPtr holidaysLen,
        PlRoll rollStrategy
    );

    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_is_business_day(
        ExprHandle expr,
        [In, MarshalAs(UnmanagedType.LPArray, SizeConst = 7)] byte[] weekMask,
        [In, MarshalAs(UnmanagedType.LPArray)] int[] holidays,
        UIntPtr holidaysLen
    );
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_dt_replace(
        ExprHandle expr,
        ExprHandle year,
        ExprHandle month,
        ExprHandle day,
        ExprHandle hour,
        ExprHandle minute,
        ExprHandle second,
        ExprHandle microsecond,
        ExprHandle ambiguous
    );
}