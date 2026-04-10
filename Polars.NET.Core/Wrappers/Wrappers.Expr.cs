using System.Runtime.InteropServices;
using Polars.NET.Core.Native;

namespace Polars.NET.Core;

public readonly partial struct PolarsWrapper
{
    // Unary Nodes
    private static ExprHandle UnaryOp(Func<ExprHandle, ExprHandle> op, ExprHandle expr)
    {
        var h = op(expr);
        expr.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    // Binary Nodes
    private static ExprHandle BinaryOp(Func<ExprHandle, ExprHandle, ExprHandle> op, ExprHandle l, ExprHandle r)
    {
        var h = op(l, r);
        l.TransferOwnership();
        r.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    private static ExprHandle UnaryStrOp(Func<ExprHandle, ExprHandle> op, ExprHandle expr) 
    => UnaryOp(op, expr);
    private static ExprHandle UnaryStrOp(Func<ExprHandle, string, ExprHandle> func, ExprHandle e, string arg)
    {
        var h = func(e, arg);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    private static ExprHandle UnaryStrOpNullable(Func<ExprHandle, string?, ExprHandle> func, ExprHandle e, string? arg)
    {
        var h = func(e, arg);
        
        e.TransferOwnership();
        
        return ErrorHelper.Check(h);
    }
    private static ExprHandle UnaryDtOp(Func<ExprHandle, ExprHandle> op, ExprHandle expr) 
        => UnaryOp(op, expr);
    public static ExprHandle RollingOp(Func<ExprHandle, string ,UIntPtr,double[]?,UIntPtr,bool,ExprHandle> op, ExprHandle expr, string windowSize,int minPeriods, double[]? weights, bool center)
    {   
        UIntPtr len = weights != null ? (UIntPtr)weights.Length : UIntPtr.Zero;
        var h = op(expr, windowSize,(UIntPtr)minPeriods,weights,len,center);
        expr.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    private static ExprHandle RollingByOp(Func<ExprHandle, string, UIntPtr,ExprHandle, PlClosedWindow, ExprHandle> op, ExprHandle expr, string windowSize, int minPeriods,ExprHandle by, PlClosedWindow closed)
    {
        var h = op(expr, windowSize,(UIntPtr)minPeriods, by, closed);
        expr.TransferOwnership();
        by.TransferOwnership(); 
        return ErrorHelper.Check(h);
    }
    // --- Expr Ops ---
    // Leaf Nodes
    public static ExprHandle Col(string name) => ErrorHelper.Check(NativeBindings.pl_expr_col(name));
    public static ExprHandle Lit(sbyte val) => ErrorHelper.Check(NativeBindings.pl_expr_lit_i8(val));
    public static ExprHandle Lit(byte val) => ErrorHelper.Check(NativeBindings.pl_expr_lit_u8(val));
    public static ExprHandle Lit(short val) => ErrorHelper.Check(NativeBindings.pl_expr_lit_i16(val));
    public static ExprHandle Lit(ushort val) => ErrorHelper.Check(NativeBindings.pl_expr_lit_u16(val));
    public static ExprHandle Lit(int val) => ErrorHelper.Check(NativeBindings.pl_expr_lit_i32(val));
    public static ExprHandle Lit(uint val) => ErrorHelper.Check(NativeBindings.pl_expr_lit_u32(val));
    public static ExprHandle Lit(long val) => ErrorHelper.Check(NativeBindings.pl_expr_lit_i64(val));
    public static ExprHandle Lit(ulong val) => ErrorHelper.Check(NativeBindings.pl_expr_lit_u64(val));
    public static ExprHandle Lit(Int128 value)
    {
        // Lower 64 bits：
        ulong lowPart = (ulong)value;

        // Upper 64 bits
        long highPart = (long)(value >> 64);
        return ErrorHelper.Check(NativeBindings.pl_expr_lit_i128(lowPart, highPart));
    }

    // public static ExprHandle Lit(UInt128 value)
    // {
    //     ulong low = (ulong)value;        
    //     ulong high = (ulong)(value >> 64); 

    //     var handle = NativeBindings.pl_expr_lit_u128(low, high);
    //     return ErrorHelper.Check(handle);
    // }

    public static ExprHandle Lit(string val) => ErrorHelper.Check(NativeBindings.pl_expr_lit_str(val));
    public static ExprHandle Lit(double val) => ErrorHelper.Check(NativeBindings.pl_expr_lit_f64(val));
    public static ExprHandle Lit(float val) => ErrorHelper.Check(NativeBindings.pl_expr_lit_f32(val));
    public static ExprHandle Lit(Half val) => ErrorHelper.Check(NativeBindings.pl_expr_lit_f16(val));
    public static ExprHandle Lit(bool val) => ErrorHelper.Check(NativeBindings.pl_expr_lit_bool(val));
    public static ExprHandle LitNull() => ErrorHelper.Check(NativeBindings.pl_expr_lit_null());
    private static readonly int UnixEpochDayNumber = new DateOnly(1970, 1, 1).DayNumber;
    private const long UnixEpochTicks = 621_355_968_000_000_000L;
    public static ExprHandle Lit(DateTime dt)
    {
        // C# DateTime.Ticks is from 0001-01-01 
        // Unix Epoch is 1970-01-01
        long ticksSinceEpoch = dt.Ticks - UnixEpochTicks;
        long micros = ticksSinceEpoch / 10; // 100ns -> 1us
        
        return ErrorHelper.Check(NativeBindings.pl_expr_lit_datetime(micros));
    }
    public static ExprHandle Lit(DateTimeOffset value)
    {
        long utcTicks = value.UtcTicks;

        long deltaTicks = utcTicks - UnixEpochTicks;

        long micros = deltaTicks / 10;

        return ErrorHelper.Check(NativeBindings.pl_expr_lit_datetime(micros));
    }
    public static ExprHandle Lit(DateOnly value)
    {
        int daysSinceEpoch = value.DayNumber - UnixEpochDayNumber;

        return ErrorHelper.Check(NativeBindings.pl_expr_lit_date(daysSinceEpoch));
    }
    public static ExprHandle Lit(TimeOnly value)
    {
        long nanoseconds = value.Ticks * 100;

        return ErrorHelper.Check(NativeBindings.pl_expr_lit_time(nanoseconds));
    }
    public static ExprHandle Lit(TimeSpan value)
    {
        // .NET Ticks = 100 ns
        // Target Unit = 1 us = 1000 ns
        // Conversion: Ticks / 10
        long microseconds = value.Ticks / 10;

        return ErrorHelper.Check(NativeBindings.pl_expr_lit_duration(microseconds));
    }
    public static ExprHandle Lit(decimal value)
    {
        // Get decimal binary part
        // bits[0] = low, bits[1] = mid, bits[2] = high, bits[3] = flags (sign & scale)
        int[] bits = decimal.GetBits(value);
        byte scale = (byte)((bits[3] >> 16) & 0x7F);

        ulong low64 = (uint)bits[0] | ((ulong)(uint)bits[1] << 32);
        ulong high32 = (uint)bits[2];
        
        Int128 unscaled = new(high32, low64);

        if ((bits[3] & 0x80000000) != 0)
        {
            unscaled = -unscaled;
        }

        // ==========================================
        // Deassemble Int128 into High/Low
        // ==========================================
        ulong lowPart = (ulong)unscaled;
        long highPart = (long)(unscaled >> 64);
        return ErrorHelper.Check(NativeBindings.pl_expr_lit_decimal(lowPart, highPart, scale));
    }
    public static ExprHandle Lit(SeriesHandle serieshandle)
    {
        var h = NativeBindings.pl_expr_lit_series(serieshandle);
        serieshandle.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    // Alias
    public static ExprHandle Alias(ExprHandle expr, string name) 
    {
        var h = NativeBindings.pl_expr_alias(expr, name);
        expr.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle UndoAlias(ExprHandle expr) => UnaryOp(NativeBindings.pl_expr_meta_undo_aliases,expr);
    public static ExprHandle UpperBound(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_upper_bound, e);
    public static ExprHandle LowerBound(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_lower_bound, e);
    // Reverse
    public static ExprHandle Reverse(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_reverse, e);
    // Aggregate
    public static ExprHandle First(ExprHandle e, bool ignoreNulls)
    {
        if (ignoreNulls == true)
        {
            var h = NativeBindings.pl_expr_first_non_null(e);
            e.TransferOwnership();
            return ErrorHelper.Check(h);
        }
        else
        {
            var h = NativeBindings.pl_expr_first(e);
            e.TransferOwnership();
            return ErrorHelper.Check(h);
        }
    }
    public static ExprHandle Last(ExprHandle e,bool ignoreNulls)
    {
        if (ignoreNulls == true)
        {
            var h = NativeBindings.pl_expr_last_non_null(e);
            e.TransferOwnership();
            return ErrorHelper.Check(h);
        }
        else
        {
            var h = NativeBindings.pl_expr_last(e);
            e.TransferOwnership();
            return ErrorHelper.Check(h);
        }   
    }
    public static ExprHandle Head(ExprHandle e, int n)
    {
        if (n < 0)
            throw new ArgumentOutOfRangeException(nameof(n), "Length must be non-negative.");

        var h = NativeBindings.pl_expr_head(e, (nuint)n);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }

    public static ExprHandle Tail(ExprHandle e, int n)
    {
        if (n < 0)
            throw new ArgumentOutOfRangeException(nameof(n), "Length must be non-negative.");

        var h = NativeBindings.pl_expr_tail(e, (nuint)n);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle All(ExprHandle e, bool ignoreNulls)
    {
        var h = NativeBindings.pl_expr_all(e,ignoreNulls);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle Any(ExprHandle e, bool ignoreNulls)
    {
        var h = NativeBindings.pl_expr_any(e,ignoreNulls);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle Item(ExprHandle e, bool allowEmpty)
    {
        var h = NativeBindings.pl_expr_item(e,allowEmpty);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle Sum(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_sum, e);
    public static ExprHandle Mean(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_mean, e);
    public static ExprHandle Max(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_max, e);
    public static ExprHandle NanMax(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_nan_max, e);
    public static ExprHandle NanMin(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_nan_min, e);
    public static ExprHandle BitwiseAnd(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_bitwise_and, e);
    public static ExprHandle BitwiseOr(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_bitwise_or, e);
    public static ExprHandle BitwiseXor(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_bitwise_xor, e);
    public static ExprHandle BitwiseCountOnes(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_bitwise_count_ones, e);
    public static ExprHandle BitwiseCountZeros(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_bitwise_count_zeros, e);
    public static ExprHandle BitwiseLeadingOnes(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_bitwise_leading_ones, e);
    public static ExprHandle BitwiseLeadingZeros(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_bitwise_leading_zeros, e);
    public static ExprHandle BitwiseTrailingOnes(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_bitwise_trailing_ones, e);
    public static ExprHandle BitwiseTrailingZeros(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_bitwise_trailing_zeros, e);
    public static ExprHandle MaxBy(ExprHandle expr, ExprHandle by) 
        => BinaryOp(NativeBindings.pl_expr_max_by, expr, by);
    public static ExprHandle MinBy(ExprHandle expr, ExprHandle by) 
        => BinaryOp(NativeBindings.pl_expr_min_by, expr, by);
    public static ExprHandle Min(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_min, e);
    public static ExprHandle Abs(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_abs, e);
    public static ExprHandle NullCount(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_null_count, e);
    public static ExprHandle NUnique(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_n_unique, e);
    public static ExprHandle ApproxNUnique(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_approx_n_unique, e);
    public static ExprHandle Product(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_product, e);
    public static ExprHandle Skew(ExprHandle e, bool bias)
    {
        var h = NativeBindings.pl_expr_skew(e,bias);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle Kurtosis(ExprHandle e,bool fisher ,bool bias)
    {
        var h = NativeBindings.pl_expr_kurtosis(e,fisher,bias);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle PctChange(ExprHandle e,long n)
    {
        var h = NativeBindings.pl_expr_pct_change(e,n);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static unsafe ExprHandle Rank(ExprHandle e,PlRankMethod method,bool descending, ulong? seed)
    {
        ulong seedValue = seed.GetValueOrDefault();
        ulong* pSeed = seed.HasValue ? &seedValue : null;
        var h = NativeBindings.pl_expr_rank(e,method,descending,pSeed);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    // Cumulative Functions
    public static ExprHandle CumSum(ExprHandle e, bool reverse)
    {
        var h = NativeBindings.pl_expr_cum_sum(e,reverse);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle CumMax(ExprHandle e, bool reverse)
    {
        var h = NativeBindings.pl_expr_cum_max(e,reverse);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle CumMin(ExprHandle e, bool reverse)
    {
        var h = NativeBindings.pl_expr_cum_min(e,reverse);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle CumProd(ExprHandle e, bool reverse)
    {
        var h = NativeBindings.pl_expr_cum_prod(e,reverse);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle CumCount(ExprHandle e, bool reverse)
    {
        var h = NativeBindings.pl_expr_cum_count(e,reverse);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    // EWM Functions
    public static ExprHandle EwmMean(ExprHandle e, double alpha,bool adjust, bool bias,int minPeriods,bool ignoreNulls)
    {
        var h = NativeBindings.pl_expr_ewm_mean(e,alpha,adjust,bias,(UIntPtr)minPeriods,ignoreNulls);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle EwmStd(ExprHandle e, double alpha,bool adjust, bool bias,int minPeriods,bool ignoreNulls)
    {
        var h = NativeBindings.pl_expr_ewm_std(e,alpha,adjust,bias,(UIntPtr)minPeriods,ignoreNulls);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle EwmVar(ExprHandle e, double alpha,bool adjust, bool bias,int minPeriods,bool ignoreNulls)
    {
        var h = NativeBindings.pl_expr_ewm_var(e,alpha,adjust,bias,(UIntPtr)minPeriods,ignoreNulls);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle EwmMeanBy(ExprHandle e, ExprHandle by,string halfLife)
    {
        var h = NativeBindings.pl_expr_ewm_mean_by(e,by,halfLife);
        e.TransferOwnership();
        by.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    // Is_In
    public static ExprHandle IsIn(ExprHandle expr, ExprHandle otherExpr, bool nullsEqual)
    {
        var h = NativeBindings.pl_expr_is_in(expr, otherExpr, nullsEqual);
        expr.TransferOwnership();
        otherExpr.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle Filter(ExprHandle expr, ExprHandle predicate) 
        => BinaryOp(NativeBindings.pl_expr_filter, expr, predicate);
    // Temporal
    public static ExprHandle DtYear(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_dt_year, e);
    public static ExprHandle DtQuarter(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_dt_quarter, e);
    public static ExprHandle DtMonth(ExprHandle e) => UnaryDtOp(NativeBindings.pl_expr_dt_month, e);
    public static ExprHandle DtDay(ExprHandle e) => UnaryDtOp(NativeBindings.pl_expr_dt_day, e);
    public static ExprHandle DtOrdinalDay(ExprHandle e) => UnaryDtOp(NativeBindings.pl_expr_dt_ordinal_day, e);
    public static ExprHandle DtWeekday(ExprHandle e) => UnaryDtOp(NativeBindings.pl_expr_dt_weekday, e);
    public static ExprHandle DtHour(ExprHandle e) => UnaryDtOp(NativeBindings.pl_expr_dt_hour, e);
    public static ExprHandle DtMinute(ExprHandle e) => UnaryDtOp(NativeBindings.pl_expr_dt_minute, e);
    public static ExprHandle DtSecond(ExprHandle e) => UnaryDtOp(NativeBindings.pl_expr_dt_second, e);
    public static ExprHandle DtMillisecond(ExprHandle e) => UnaryDtOp(NativeBindings.pl_expr_dt_millisecond, e);
    public static ExprHandle DtMicrosecond(ExprHandle e) => UnaryDtOp(NativeBindings.pl_expr_dt_microsecond, e);
    public static ExprHandle DtNanosecond(ExprHandle e) => UnaryDtOp(NativeBindings.pl_expr_dt_nanosecond, e);
    public static ExprHandle DtToString(ExprHandle e, string format)
    {
        var h = NativeBindings.pl_expr_dt_to_string(e, format);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }

    public static ExprHandle DtDate(ExprHandle e) => UnaryDtOp(NativeBindings.pl_expr_dt_date, e);
    public static ExprHandle DtTime(ExprHandle e) => UnaryDtOp(NativeBindings.pl_expr_dt_time, e);
    // Truncate / Round (Expr + String)
    public static ExprHandle DtTruncate(ExprHandle e, string every) 
        => UnaryStrOp(NativeBindings.pl_expr_dt_truncate, e, every);

    public static ExprHandle DtRound(ExprHandle e, string every)
        => UnaryStrOp(NativeBindings.pl_expr_dt_round, e, every);

    // OffsetBy (Expr + Expr)
    public static ExprHandle DtOffsetBy(ExprHandle e, ExprHandle by)
        => BinaryOp(NativeBindings.pl_expr_dt_offset_by, e, by);

    // Timestamp (Expr + Int)
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
    // TimeZone
    public static ExprHandle DtConvertTimeZone(ExprHandle e, string timeZone)
    {
        var h = NativeBindings.pl_expr_dt_convert_time_zone(e, timeZone);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle DtReplaceTimeZone(
        ExprHandle e, 
        string? timeZone, 
        string? ambiguous = null, 
        string? nonExistent = "raise")
    {
        var h = NativeBindings.pl_expr_dt_replace_time_zone(e, timeZone, ambiguous, nonExistent);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle DtAddBusinessDays(
        ExprHandle expr, 
        ExprHandle n, 
        bool[] weekMask, 
        int[] holidays,
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
                (UIntPtr)holidays.Length,
                roll
            ));
        expr.TransferOwnership();
        n.TransferOwnership();
        return ErrorHelper.Check(h);
    }

    public static ExprHandle DtIsBusinessDay(
        ExprHandle expr,
        bool[] weekMask,
        int[] holidays)
    {
        if (weekMask.Length != 7) 
            throw new ArgumentException("Week mask must have length 7.");

        var maskBytes = new byte[7];
        for (int i = 0; i < 7; i++) maskBytes[i] = weekMask[i] ? (byte)1 : (byte)0;

        var h = ErrorHelper.Check(NativeBindings.pl_expr_is_business_day(
            expr,
            maskBytes,
            holidays,
            (UIntPtr)holidays.Length
        ));
        expr.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    // String Ops
    public static ExprHandle ConcatString(ExprHandle[] exprs,string separator, bool ignoreNulls)
    {
        var ptrs = HandlesToPtrs(exprs); 
        return ErrorHelper.Check(NativeBindings.pl_concat_str(
            ptrs,(UIntPtr)exprs.Length,
            separator,ignoreNulls            
        ));
    }
    public static ExprHandle FormatString(string format,ExprHandle[] exprs)
    {
        var ptrs = HandlesToPtrs(exprs); 
        return ErrorHelper.Check(NativeBindings.pl_format_str(
            format,ptrs,(UIntPtr)exprs.Length         
        ));
    }
    public static ExprHandle StrContains(ExprHandle e, string pat) 
    {
        var h = NativeBindings.pl_expr_str_contains(e, pat);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }

    public static ExprHandle StrToUpper(ExprHandle e) => UnaryStrOp(NativeBindings.pl_expr_str_to_uppercase, e);
    public static ExprHandle StrToLower(ExprHandle e) => UnaryStrOp(NativeBindings.pl_expr_str_to_lowercase, e);
    public static ExprHandle StrLenBytes(ExprHandle e) => UnaryStrOp(NativeBindings.pl_expr_str_len_bytes, e);
    
    public static ExprHandle StrSlice(ExprHandle e, long offset, ulong length)
    {
        var h = NativeBindings.pl_expr_str_slice(e, offset, length);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }

    public static ExprHandle StrReplaceAll(ExprHandle e, string pat, string val,bool useRegex = false)
    {
        var h = NativeBindings.pl_expr_str_replace_all(e, pat, val,useRegex);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StrSplit(ExprHandle e, string pat) 
    {
        var h = NativeBindings.pl_expr_str_split(e, pat);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StrExtract(ExprHandle e, string pat, uint groupIndex)
    {
        var h = NativeBindings.pl_expr_str_extract(e, pat, groupIndex);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StrStripChars(ExprHandle e, string? matches = null)
        => UnaryStrOpNullable(NativeBindings.pl_expr_str_strip_chars, e, matches);

    public static ExprHandle StrStripCharsStart(ExprHandle e, string? matches = null)
        => UnaryStrOpNullable(NativeBindings.pl_expr_str_strip_chars_start, e, matches);

    public static ExprHandle StrStripCharsEnd(ExprHandle e, string? matches = null)
        => UnaryStrOpNullable(NativeBindings.pl_expr_str_strip_chars_end, e, matches);
    public static ExprHandle StrStripPrefix(ExprHandle e, string prefix)
        => UnaryStrOp(NativeBindings.pl_expr_str_strip_prefix, e, prefix);

    public static ExprHandle StrStripSuffix(ExprHandle e, string suffix)
        => UnaryStrOp(NativeBindings.pl_expr_str_strip_suffix, e, suffix);
    public static ExprHandle StrStartsWith(ExprHandle e, string prefix)
        => UnaryStrOp(NativeBindings.pl_expr_str_starts_with, e, prefix);

    public static ExprHandle StrEndsWith(ExprHandle e, string suffix)
        => UnaryStrOp(NativeBindings.pl_expr_str_ends_with, e, suffix);

    public static ExprHandle StrToDate(        
        ExprHandle e,
        string? format,
        [MarshalAs(UnmanagedType.U1)] bool strict,
        [MarshalAs(UnmanagedType.U1)] bool exact,
        [MarshalAs(UnmanagedType.U1)] bool cache)
    {
        var h = NativeBindings.pl_expr_str_to_date(e,format,strict,exact,cache);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }

    public static ExprHandle StrToDatetime(        
        ExprHandle e,
        PlTimeUnit unit,
        string? timeZone,
        string? format,
        [MarshalAs(UnmanagedType.U1)] bool strict,
        [MarshalAs(UnmanagedType.U1)] bool exact,
        [MarshalAs(UnmanagedType.U1)] bool cache)
    {
        var h = NativeBindings.pl_expr_str_to_datetime(e,unit,timeZone,format,strict,exact,cache);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    // Compare
    public static ExprHandle Eq(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_eq, l, r);
    public static ExprHandle EqMissing(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_eq_missing, l, r);
    public static ExprHandle Neq(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_neq, l, r);
    public static ExprHandle NeqMissing(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_neq_missing, l, r);
    public static ExprHandle Gt(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_gt, l, r);
    public static ExprHandle GtEq(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_gt_eq, l, r);
    public static ExprHandle Lt(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_lt, l, r);
    public static ExprHandle LtEq(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_lt_eq, l, r);
    // Arithmetic
    public static ExprHandle Add(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_add, l, r);
    public static ExprHandle Sub(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_sub, l, r);
    public static ExprHandle Div(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_div, l, r);
    public static ExprHandle FloorDiv(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_floor_div, l, r);
    public static ExprHandle Rem(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_rem, l, r);
    public static ExprHandle Mul(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_mul, l, r);
    public static ExprHandle BitLeftShift(ExprHandle expr, int n)
    {
        var h = NativeBindings.pl_expr_bit_shl(expr, n);
        expr.TransferOwnership();
        return ErrorHelper.Check(h);
    }

    public static ExprHandle BitRightShift(ExprHandle expr, int n)
    {
        var h = NativeBindings.pl_expr_bit_shr(expr, n);
        expr.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    // Logic
    public static ExprHandle And(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_and, l, r);
    public static ExprHandle Or(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_or, l, r);
    public static ExprHandle Not(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_not, e);
    public static ExprHandle Xor(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_xor, l, r);
    // Top-K & Bottom-K
    public static ExprHandle TopK(ExprHandle e, uint k) 
    {
        var h = NativeBindings.pl_expr_top_k(e, k);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle BottomK(ExprHandle e, uint k) 
    {
        var h = NativeBindings.pl_expr_bottom_k(e, k);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }    
    public static ExprHandle TopKBy(ExprHandle expr, uint k, ExprHandle[] by, bool[] reverse)
    {
        var byPtrs = HandlesToPtrs(by);

        unsafe
        {
            fixed (bool* descPtr = reverse)
            {
                var h = NativeBindings.pl_expr_top_k_by(
                    expr, 
                    k, 
                    byPtrs, 
                    (UIntPtr)byPtrs.Length, 
                    descPtr, 
                    (UIntPtr)reverse.Length
                );
                expr.TransferOwnership();
                return ErrorHelper.Check(h);
            }
        }
    }

    public static ExprHandle BottomKBy(ExprHandle expr, uint k, ExprHandle[] by, bool[] reverse)
    {
        var byPtrs = HandlesToPtrs(by);

        unsafe
        {
            fixed (bool* descPtr = reverse)
            {
                var h = NativeBindings.pl_expr_bottom_k_by(
                    expr, 
                    k, 
                    byPtrs, 
                    (UIntPtr)byPtrs.Length, 
                    descPtr, 
                    (UIntPtr)reverse.Length
                );
                expr.TransferOwnership();
                return ErrorHelper.Check(h);
            }
        }
    }
    public static ExprHandle Coalesce(ExprHandle[] handles)
    {
        if (handles == null || handles.Length == 0)
            throw new ArgumentException("Coalesce requires at least one expression.");

        nint[] ptrs = HandlesToPtrs(handles);

        return NativeBindings.pl_expr_coalesce(ptrs, (nuint)ptrs.Length);
    }
    // Null Handling
    public static ExprHandle FillNull(ExprHandle expr, ExprHandle fillValue) 
        => BinaryOp(NativeBindings.pl_expr_fill_null, expr, fillValue);
    public static ExprHandle FillNan(ExprHandle expr, ExprHandle fillValue) 
        => BinaryOp(NativeBindings.pl_expr_fill_nan, expr, fillValue);
    public static ExprHandle Interpolate(ExprHandle expr, PlInterpolationMethod method)
    {
        var h = NativeBindings.pl_expr_interpolate(expr, method);
        expr.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle InterpolateBy(ExprHandle e, ExprHandle by) => BinaryOp(NativeBindings.pl_expr_interpolate_by, e, by);
    
    public static ExprHandle IsNull(ExprHandle expr) 
        => UnaryOp(NativeBindings.pl_expr_is_null, expr);

    public static ExprHandle IsNotNull(ExprHandle expr) 
        => UnaryOp(NativeBindings.pl_expr_is_not_null, expr);
    public static ExprHandle DropNulls(ExprHandle expr) 
        => UnaryOp(NativeBindings.pl_expr_drop_nulls, expr);
    public static ExprHandle DropNans(ExprHandle expr) 
        => UnaryOp(NativeBindings.pl_expr_drop_nans, expr);
    // Unique and Duplicated
    public static ExprHandle ExprIsUnique(ExprHandle expr)
        => UnaryOp(NativeBindings.pl_expr_is_unique,expr);

    public static ExprHandle ExprIsDuplicated(ExprHandle expr)
        => UnaryOp(NativeBindings.pl_expr_is_duplicated,expr);

    public static ExprHandle ExprUnique(ExprHandle expr)
        => UnaryOp(NativeBindings.pl_expr_unique,expr);

    public static ExprHandle ExprUniqueStable(ExprHandle expr)
        => UnaryOp(NativeBindings.pl_expr_unique_stable,expr);
    // Math
    public static ExprHandle Pow(ExprHandle b, ExprHandle e) => BinaryOp(NativeBindings.pl_expr_pow, b, e);
    public static ExprHandle Dot(ExprHandle left, ExprHandle right) => BinaryOp(NativeBindings.pl_expr_dot, left, right);
    public static ExprHandle Sqrt(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_sqrt, e);
    public static ExprHandle Cbrt(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_cbrt, e);
    public static ExprHandle Exp(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_exp, e);
    public static ExprHandle Sin(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_sin,e);
    public static ExprHandle Cos(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_cos,e);
    public static ExprHandle Tan(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_tan,e);
    
    public static ExprHandle ArcSin(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_arcsin,e);
    public static ExprHandle ArcCos(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_arccos,e);
    public static ExprHandle ArcTan(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_arctan,e);

    public static ExprHandle Sinh(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_sinh,e);
    public static ExprHandle Cosh(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_cosh,e);
    public static ExprHandle Tanh(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_tanh,e);

    public static ExprHandle ArcSinh(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_arcsinh,e);
    public static ExprHandle ArcCosh(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_arccosh,e);
    public static ExprHandle ArcTanh(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_arctanh,e);

    public static ExprHandle Sign(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_sign,e);
    public static ExprHandle Ceil(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_ceil,e);
    public static ExprHandle Floor(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_floor,e);
    public static ExprHandle Log(ExprHandle expr, double baseVal)
    {
        var h = NativeBindings.pl_expr_log(expr, baseVal);
        expr.TransferOwnership(); // 消耗掉 expr
        return ErrorHelper.Check(h);
    }
    public static ExprHandle Round(ExprHandle e, uint decimals)
    {
        var h = NativeBindings.pl_expr_round(e, decimals);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    // Statistics
    public static ExprHandle Count(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_count, e);
    public static ExprHandle Std(ExprHandle e, int ddof) 
    {
        var h = NativeBindings.pl_expr_std(e, (byte)ddof);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle Var(ExprHandle e, int ddof)
    {
        var h = NativeBindings.pl_expr_var(e, (byte)ddof);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle Median(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_median, e);
    public static ExprHandle Mode(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_mode, e);
    
    public static ExprHandle Quantile(ExprHandle e, double quantile, PlQuantileMethod method)
    {
        var h = NativeBindings.pl_expr_quantile(e, quantile, method);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    // IsBetween
    public static ExprHandle IsBetween(ExprHandle expr, ExprHandle lower, ExprHandle upper)
    {
        var h = NativeBindings.pl_expr_is_between(expr, lower, upper);
        expr.TransferOwnership();
        lower.TransferOwnership();
        upper.TransferOwnership();
        return ErrorHelper.Check(h);
    }

    // List
    public static ExprHandle ListFirst(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_list_first, e);
    
    public static ExprHandle ListGet(ExprHandle e, long index)
    {
        var h = NativeBindings.pl_expr_list_get(e, index);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }

    public static ExprHandle Explode(ExprHandle e, bool emptyAsNull,bool keepNulls)
    {
        var h = NativeBindings.pl_expr_explode(e,emptyAsNull,keepNulls);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    } 
    public static ExprHandle Implode(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_implode, e);
    
    public static ExprHandle ListJoin(ExprHandle e, string sep)
    {
        var h = NativeBindings.pl_expr_list_join(e, sep);
        e.TransferOwnership(); 
        return ErrorHelper.Check(h);
    }

    public static ExprHandle ListLen(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_list_len, e);
    // --- List Aggs ---
    public static ExprHandle ListSum(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_list_sum, e);
    public static ExprHandle ListMin(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_list_min, e);
    public static ExprHandle ListMax(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_list_max, e);
    public static ExprHandle ListMean(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_list_mean, e);

    // --- List Other ---
    public static ExprHandle ListSort(ExprHandle e, bool descending, bool nullsLast, bool maintainOrder)
    {
        var h = NativeBindings.pl_expr_list_sort(e, descending, nullsLast, maintainOrder);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }

    public static ExprHandle ListContains(ExprHandle listExpr, ExprHandle itemExpr, bool nullsEqual)
    {
        var h = NativeBindings.pl_expr_list_contains(listExpr, itemExpr, nullsEqual);
        listExpr.TransferOwnership();
        itemExpr.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle ConcatList(ExprHandle[] exprs)
    {
        var ptrs = HandlesToPtrs(exprs); 
        return ErrorHelper.Check(NativeBindings.pl_concat_list(
            ptrs,
            (UIntPtr)exprs.Length
        ));
    }
    public static ExprHandle ListReverse(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_list_reverse, e);
    // --- Array ---
    public static ExprHandle ConcatArray(ExprHandle[] exprs)
    {
        var ptrs = HandlesToPtrs(exprs); 
        return ErrorHelper.Check(NativeBindings.pl_concat_array(
            ptrs,
            (UIntPtr)exprs.Length
        ));
    }
    public static ExprHandle ArraySum(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_array_sum, e);
    public static ExprHandle ArrayMin(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_array_min, e);
    public static ExprHandle ArrayMax(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_array_max, e);
    public static ExprHandle ArrayMean(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_array_mean, e);
    public static ExprHandle ArrayMedian(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_array_median, e);
    public static ExprHandle ArrayLen(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_array_len, e);
    public static ExprHandle ArrayNUnique(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_array_n_unique, e);
    public static ExprHandle ArrayCountMatches(ExprHandle e,ExprHandle item) => BinaryOp(NativeBindings.pl_expr_array_count_matches, e,item);
    public static ExprHandle ArrayShift(ExprHandle e,ExprHandle shift) => BinaryOp(NativeBindings.pl_expr_array_shift, e,shift);
    public static ExprHandle ArrayAgg(ExprHandle e,ExprHandle agg) => BinaryOp(NativeBindings.pl_expr_array_agg, e,agg);
    public static ExprHandle ArrayStd(ExprHandle e,byte ddof)
    {
        var h = NativeBindings.pl_expr_array_std(e,ddof);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle ArrayVar(ExprHandle e,byte ddof)
    {
        var h = NativeBindings.pl_expr_array_var(e,ddof);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }    
    public static ExprHandle ArrayUnique(ExprHandle e,bool stable)   
    {
        var h = NativeBindings.pl_expr_array_unique(e, stable);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle ArrayJoin(ExprHandle e,string sep,bool ignoreNulls)
    {
        var h = NativeBindings.pl_expr_array_join(e, sep,ignoreNulls);
        e.TransferOwnership(); 
        return ErrorHelper.Check(h);
    }
    public static ExprHandle ArrayContains(ExprHandle expr, ExprHandle item, bool nullsEqual)
    {
        var h = NativeBindings.pl_expr_array_contains(expr, item, nullsEqual);
        expr.TransferOwnership();
        item.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle ArrayAny(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_array_any, e);
    public static ExprHandle ArrayAll(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_array_all, e);
    public static ExprHandle ArraySort(ExprHandle e, bool descending, bool nullsLast, bool maintainOrder)
    {
        var h = NativeBindings.pl_expr_array_sort(e, descending, nullsLast, maintainOrder);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle ArrayGet(ExprHandle expr, ExprHandle index, bool nullOnOob)
    {
        var h = NativeBindings.pl_expr_array_get(expr, index, nullOnOob);
        expr.TransferOwnership();
        index.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle ArrayReverse(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_array_reverse, e);
    public static ExprHandle ArrayArgMin(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_array_arg_min, e);
    public static ExprHandle ArrayArgMax(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_array_arg_max, e);
    public static ExprHandle ArrayExplode(ExprHandle e, bool emptyAsNull,bool keepNulls)
    {
        var h = NativeBindings.pl_expr_array_explode(e,emptyAsNull,keepNulls);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle ArrayHead(ExprHandle expr, ExprHandle num, bool asArray)
    {
        var h = NativeBindings.pl_expr_array_head(expr, num, asArray);
        expr.TransferOwnership();
        num.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle ArrayTail(ExprHandle expr, ExprHandle num, bool asArray)
    {
        var h = NativeBindings.pl_expr_array_tail(expr, num, asArray);
        expr.TransferOwnership();
        num.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle ArrayEval(ExprHandle expr, ExprHandle other, bool asList)
    {
        var h = NativeBindings.pl_expr_array_eval(expr, other, asList);
        expr.TransferOwnership();
        other.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle ArraySlice(ExprHandle expr, ExprHandle offset,ExprHandle length, bool asList)
    {
        var h = NativeBindings.pl_expr_array_slice(expr, offset,length, asList);
        expr.TransferOwnership();
        offset.TransferOwnership();
        length.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    
    public static ExprHandle ArrayToList(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_array_to_list, e);
    public static ExprHandle ArrayToStruct(ExprHandle e,string[]? fieldNames)
    {
        nuint len = fieldNames == null ? nuint.Zero : (nuint)fieldNames.Length;
        var h = NativeBindings.pl_expr_array_to_struct(e, fieldNames, len);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    // --- Struct ---
    public static ExprHandle AsStruct(ExprHandle[] exprs)
    {
        var raw = HandlesToPtrs(exprs);
        return ErrorHelper.Check(NativeBindings.pl_expr_as_struct(raw, (UIntPtr)raw.Length));
    }

    public static ExprHandle StructFieldByName(ExprHandle e, string[] name)
    {
        var h = NativeBindings.pl_expr_struct_field_by_names(e, name,(nuint)name.Length);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StructFieldByIndex(ExprHandle e, long index)
        {
            var h = NativeBindings.pl_expr_struct_field_by_index(e, index);
            e.TransferOwnership(); 
            return ErrorHelper.Check(h);
        }

    public static ExprHandle StructRenameFields(ExprHandle e, string[] names)
    {
        var h = NativeBindings.pl_expr_struct_rename_fields(e, names, (UIntPtr)names.Length);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StructJsonEncode(ExprHandle e)
    {
        var h = NativeBindings.pl_expr_struct_json_encode(e);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StructWithFields(ExprHandle e, ExprHandle[] fields)
    {
        var ptrs = HandlesToPtrs(fields);
        var h = NativeBindings.pl_expr_struct_with_fields(e,ptrs,(nuint)ptrs.Length);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    
    // Naming
    public static ExprHandle Prefix(ExprHandle e, string p)
    {
        var h = NativeBindings.pl_expr_prefix(e, p);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    
    public static ExprHandle Suffix(ExprHandle e, string s)
    {
        var h = NativeBindings.pl_expr_suffix(e, s);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    // Window
    public static ExprHandle Over(ExprHandle expr, ExprHandle[] partitionBy)
    {
        var rawPartition = HandlesToPtrs(partitionBy);
        
        var h = NativeBindings.pl_expr_over(expr, rawPartition, (UIntPtr)rawPartition.Length);
        
        expr.TransferOwnership();
        
        return ErrorHelper.Check(h);
    }
    // Expr Length
    public static ExprHandle ExprLen(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_len,e);
    public static ExprHandle Len() => NativeBindings.pl_len();
    // expr clone
    
    public static ExprHandle CloneExpr(ExprHandle expr)
        => ErrorHelper.Check(NativeBindings.pl_expr_clone(expr));
    public static ExprHandle Rechunk(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_len,e);
    public static ExprHandle ExprCast(ExprHandle expr, DataTypeExprHandle dexpr, bool strict,bool wrapNumerical)
    {
        var h = NativeBindings.pl_expr_cast(expr, dexpr, strict,wrapNumerical);
        expr.TransferOwnership();
        dexpr.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    // Shift
    public static ExprHandle Shift(ExprHandle e, long n)
    {
        var h = NativeBindings.pl_expr_shift(e, n);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    // Diff
    public static ExprHandle Diff(ExprHandle e, long n)
    {
        var h = NativeBindings.pl_expr_diff(e, n);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    // Fill
    public static ExprHandle ForwardFill(ExprHandle e, uint limit)
    {
        var h = NativeBindings.pl_expr_forward_fill(e, limit);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle BackwardFill(ExprHandle e, uint limit)
    {
        var h = NativeBindings.pl_expr_backward_fill(e, limit);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle RollingMean(ExprHandle e, string w, int minPeriods, double[]? weights,bool center) => RollingOp(NativeBindings.pl_expr_rolling_mean, e, w, minPeriods,weights,center);
    public static ExprHandle RollingMax(ExprHandle e, string w, int minPeriods, double[]? weights,bool center) => RollingOp(NativeBindings.pl_expr_rolling_max, e, w, minPeriods,weights,center);
    public static ExprHandle RollingMin(ExprHandle e, string w, int minPeriods, double[]? weights,bool center) => RollingOp(NativeBindings.pl_expr_rolling_min, e, w, minPeriods,weights,center);
    public static ExprHandle RollingSum(ExprHandle e, string w, int minPeriods, double[]? weights,bool center) => RollingOp(NativeBindings.pl_expr_rolling_sum, e, w, minPeriods,weights,center);
    public static ExprHandle RollingStd(ExprHandle e, string w, int minPeriods, double[]? weights,bool center) => RollingOp(NativeBindings.pl_expr_rolling_std, e, w, minPeriods,weights,center);
    public static ExprHandle RollingVar(ExprHandle e, string w, int minPeriods, double[]? weights,bool center,byte ddof)
    {   
        UIntPtr len = weights != null ? (UIntPtr)weights.Length : UIntPtr.Zero;
        var h = NativeBindings.pl_expr_rolling_var(e, w, (UIntPtr)minPeriods,weights,len,center, ddof);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    
    public static ExprHandle RollingMedian(ExprHandle e, string w, int minPeriods, double[]? weights,bool center) => RollingOp(NativeBindings.pl_expr_rolling_median, e, w, minPeriods,weights,center);
    public static ExprHandle RollingSkew(ExprHandle e, string w, int minPeriods, double[]? weights,bool center, bool bias)    
    {   
        UIntPtr len = weights != null ? (UIntPtr)weights.Length : UIntPtr.Zero;
        var h = NativeBindings.pl_expr_rolling_skew(e, w, (UIntPtr)minPeriods,weights,len,center,bias);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle RollingKurtosis(ExprHandle e, string w, int minPeriods, double[]? weights,bool center,bool fisher, bool bias)    
    {   
        UIntPtr len = weights != null ? (UIntPtr)weights.Length : UIntPtr.Zero;
        var h = NativeBindings.pl_expr_rolling_kurtosis(e, w, (UIntPtr)minPeriods,weights,len,center,fisher,bias);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }    public unsafe static ExprHandle RollingRank(ExprHandle e, string w, int minPeriods,PlRankMethod method,ulong? seed,double[]? weights, bool center)
    {
        ulong seedValue = seed.GetValueOrDefault();
        ulong* pSeed = seed.HasValue ? &seedValue : null;
        UIntPtr len = weights != null ? (UIntPtr)weights.Length : UIntPtr.Zero;
        var h = NativeBindings.pl_expr_rolling_rank(e,w,(UIntPtr)minPeriods,method,pSeed,weights,len, center);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle RollingQuantile(ExprHandle e,double quantile,PlQuantileMethod method ,string w, int minPeriods,double[]? weights, bool center)
    {
        UIntPtr len = weights != null ? (UIntPtr)weights.Length : UIntPtr.Zero;
        var h = NativeBindings.pl_expr_rolling_quantile(e,quantile,method,w,(UIntPtr)minPeriods,weights,len, center);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle RollingMeanBy(ExprHandle e, string w, int minPeriods, ExprHandle by, PlClosedWindow closed) => RollingByOp(NativeBindings.pl_expr_rolling_mean_by, e, w, minPeriods,by, closed);
    public static ExprHandle RollingSumBy(ExprHandle e, string w, int minPeriods, ExprHandle by, PlClosedWindow closed) => RollingByOp(NativeBindings.pl_expr_rolling_sum_by, e, w, minPeriods,by, closed);
    public static ExprHandle RollingMinBy(ExprHandle e, string w, int minPeriods, ExprHandle by, PlClosedWindow closed) => RollingByOp(NativeBindings.pl_expr_rolling_min_by, e, w, minPeriods,by, closed);
    public static ExprHandle RollingMaxBy(ExprHandle e, string w, int minPeriods, ExprHandle by, PlClosedWindow closed) => RollingByOp(NativeBindings.pl_expr_rolling_max_by, e, w, minPeriods,by, closed);
    public static ExprHandle RollingStdBy(ExprHandle e, string w, int minPeriods, ExprHandle by, PlClosedWindow closed) => RollingByOp(NativeBindings.pl_expr_rolling_std_by, e, w, minPeriods,by, closed);
    public static ExprHandle RollingVarBy(ExprHandle e, string w, int minPeriods, ExprHandle by, PlClosedWindow closed, byte ddof) 
    {
        var h = NativeBindings.pl_expr_rolling_var_by(e, w, (UIntPtr)minPeriods,by, closed,ddof);
        e.TransferOwnership();
        by.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle RollingMedianBy(ExprHandle e, string w, int minPeriods, ExprHandle by, PlClosedWindow closed) => RollingByOp(NativeBindings.pl_expr_rolling_median_by, e, w, minPeriods,by, closed);
    public static unsafe ExprHandle RollingRankBy(
        ExprHandle e,
        string windowSize, 
        ExprHandle by, 
        PlRollingRankMethod method, 
        ulong? seed,
        int minPeriods, 
        PlClosedWindow closed)
    {
        ulong seedValue = seed.GetValueOrDefault();
        ulong* pSeed = seed.HasValue ? &seedValue : null;

        var h = NativeBindings.pl_expr_rolling_rank_by(
            e,
            method,
            pSeed,
            windowSize,
            (UIntPtr)minPeriods,
            by,
            closed
        );
        e.TransferOwnership();
        by.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle RollingQuantileBy(ExprHandle e, double quantile, PlQuantileMethod method, string w, int minPeriods, ExprHandle by, PlClosedWindow closed)
    {
        var h = NativeBindings.pl_expr_rolling_quantile_by(e,quantile,method,w,(UIntPtr)minPeriods,by,closed);
        e.TransferOwnership();
        by.TransferOwnership(); 
        return ErrorHelper.Check(h);
    }
    public static ExprHandle IfElse(ExprHandle pred, ExprHandle ifTrue, ExprHandle ifFalse)
    {
        var h = NativeBindings.pl_expr_ternary(pred, ifTrue, ifFalse);
        
        pred.TransferOwnership();
        ifTrue.TransferOwnership();
        ifFalse.TransferOwnership();
        
        return ErrorHelper.Check(h);
    }
    public static ExprHandle ExtendConstant(ExprHandle e, ExprHandle value, ExprHandle n)
    {
        var h = NativeBindings.pl_expr_extend_constant(e, value, n);
        
        e.TransferOwnership();
        value.TransferOwnership();
        n.TransferOwnership();
        
        return ErrorHelper.Check(h);
    }
    public static ExprHandle SqlExpr(string sql)
        =>ErrorHelper.Check(NativeBindings.pl_expr_sql(sql));
    public static ExprHandle Gather(ExprHandle expr,ExprHandle idx)
        => BinaryOp(NativeBindings.pl_expr_gather, expr, idx);
    public static ExprHandle GatherEvery(ExprHandle expr,nuint n, nuint offset)
    {
        var h = NativeBindings.pl_expr_gather_every(expr,n,offset);
        expr.TransferOwnership();

        return ErrorHelper.Check(h);
    }
    public static ExprHandle Get(ExprHandle expr,ExprHandle idx,bool nullOnOob)
    {
        var h = NativeBindings.pl_expr_get(expr,idx,nullOnOob);
        expr.TransferOwnership();
        idx.TransferOwnership();

        return ErrorHelper.Check(h);
    }
    public static unsafe ExprHandle Sort(
        ExprHandle expr, 
        bool descending, 
        bool nullsLast, 
        bool multithreaded, 
        bool maintainOrder, 
        uint? limit)
    {
        if (expr.IsInvalid) throw new ArgumentException("Expr handle is invalid");

        uint localLimit = limit.GetValueOrDefault();
        uint* limitPtr = limit.HasValue ? &localLimit : null;

        var h = NativeBindings.pl_expr_sort(
            expr, 
            descending, 
            nullsLast, 
            multithreaded, 
            maintainOrder, 
            limitPtr
        );
        expr.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle ArgUnique(ExprHandle expr) => UnaryOp(NativeBindings.pl_expr_arg_unique,expr);
    public static ExprHandle ArgMax(ExprHandle expr) => UnaryOp(NativeBindings.pl_expr_arg_max,expr);
    public static ExprHandle ArgMin(ExprHandle expr) => UnaryOp(NativeBindings.pl_expr_arg_min,expr);
    public static ExprHandle ArgSort(ExprHandle expr, bool descending, bool nullsLast)
    {
        var h = NativeBindings.pl_expr_arg_sort(expr,descending,nullsLast);
        expr.TransferOwnership();

        return ErrorHelper.Check(h);
    }
    public static ExprHandle IndexOf(ExprHandle expr, ExprHandle element) 
        => BinaryOp(NativeBindings.pl_expr_index_of,expr,element);
    public static ExprHandle SearchSorted(ExprHandle expr,ExprHandle element, PlSearchSortedSide side,bool descending)
    {
        var h = NativeBindings.pl_expr_search_sorted(expr,element,side,descending);
        expr.TransferOwnership();
        element.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle ConcatExprs(ExprHandle[] exprs,bool rechunk)
    {
        var ptrs = HandlesToPtrs(exprs); 
        return ErrorHelper.Check(NativeBindings.pl_concat_expr(
            ptrs,(UIntPtr)exprs.Length,
            rechunk           
        ));
    }
    public static SelectorHandle ToSelector(ExprHandle e) 
    {
        var h = NativeBindings.pl_expr_try_into_selector(e);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static string ExprToString(ExprHandle e) 
    {
        var h = NativeBindings.pl_expr_to_string(e);
        return ErrorHelper.CheckString(h);
    }
    public static string FormatTree(ExprHandle expr,bool displayAsDot, SchemaHandle? schema)
    {
        using var schemaLock = new SafeHandleLock<SchemaHandle>(
            schema != null ? [schema] : null
        );
        IntPtr schemaPtr = schema != null ? schemaLock.Pointers[0] : IntPtr.Zero;
        int status = NativeBindings.pl_expr_meta_into_tree_formatter(expr, displayAsDot,schemaPtr, out IntPtr ptr);
        ErrorHelper.CheckStatus(status);

        return ErrorHelper.CheckString(ptr);
    }
    public static ExprHandle[] Pop(ExprHandle handle)
    {
        IntPtr rawPtr = handle.TransferOwnership();

        int status = NativeBindings.pl_expr_meta_pop(rawPtr, out IntPtr ptrs, out nuint len);
        ErrorHelper.CheckStatus(status);

        if (ptrs == IntPtr.Zero || len == 0) return [];

        try
        {
            var handles = new ExprHandle[(int)len];
            for (int i = 0; i < (int)len; i++)
            {
                IntPtr p = Marshal.ReadIntPtr(ptrs, i * IntPtr.Size);
                handles[i] = new ExprHandle(p); 
            }
            return handles;
        }
        finally
        {
            NativeBindings.pl_free_ptr_array(ptrs, len);
        }
    }
    public static string? ExprGetOutputName(ExprHandle expr)
    {
        int status = NativeBindings.pl_expr_get_output_name(expr, out IntPtr strPtr);

        if (status != 0 || strPtr == IntPtr.Zero)
        {
            ErrorHelper.CheckStatus(status);
            return null;
        }

        try
        {
            return Marshal.PtrToStringUTF8(strPtr);
        }
        finally
        {
            NativeBindings.pl_free_string(strPtr);
        }
    }
    public static ExprHandle IntRange(ExprHandle start,ExprHandle end,long step,DataTypeExprHandle dtypeExpr) 
    {
        var h = NativeBindings.pl_expr_int_range(start,end,step,dtypeExpr);
        start.TransferOwnership();
        end.TransferOwnership();
        dtypeExpr.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle IntRanges(ExprHandle start,ExprHandle end,ExprHandle step,DataTypeExprHandle dtypeExpr) 
    {
        var h = NativeBindings.pl_expr_int_ranges(start,end,step,dtypeExpr);
        start.TransferOwnership();
        end.TransferOwnership();
        step.TransferOwnership();
        dtypeExpr.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle DateRange(ExprHandle? start,ExprHandle? end,string? interval,ExprHandle? numSamples,PlClosedWindow closedWindow) 
    {
        IntPtr startHandle = start?.TransferOwnership() ?? IntPtr.Zero;
        IntPtr endHandle = end?.TransferOwnership() ?? IntPtr.Zero;
        IntPtr numSamplesHandle = numSamples?.TransferOwnership() ?? IntPtr.Zero;
        var h = NativeBindings.pl_expr_date_range(startHandle,endHandle,interval,numSamplesHandle,closedWindow);
        return ErrorHelper.Check(h);
    }
    public static ExprHandle DateRanges(ExprHandle? start,ExprHandle? end,string? interval,ExprHandle? numSamples,PlClosedWindow closedWindow) 
    {
        IntPtr startHandle = start?.TransferOwnership() ?? IntPtr.Zero;
        IntPtr endHandle = end?.TransferOwnership() ?? IntPtr.Zero;
        IntPtr numSamplesHandle = numSamples?.TransferOwnership() ?? IntPtr.Zero;
        var h = NativeBindings.pl_expr_date_ranges(startHandle,endHandle,interval,numSamplesHandle,closedWindow);
        return ErrorHelper.Check(h);
    }
    public static ExprHandle DatetimeRange(
        ExprHandle? start,
        ExprHandle? end,
        string? interval,
        ExprHandle? numSamples,
        PlClosedWindow closedWindow,
        PlTimeUnit unit,
        string? timeZone) 
    {
        IntPtr startHandle = start?.TransferOwnership() ?? IntPtr.Zero;
        IntPtr endHandle = end?.TransferOwnership() ?? IntPtr.Zero;
        IntPtr numSamplesHandle = numSamples?.TransferOwnership() ?? IntPtr.Zero;
        var h = NativeBindings.pl_expr_datetime_range(startHandle,endHandle,interval,numSamplesHandle,closedWindow,unit,timeZone);
        return ErrorHelper.Check(h);
    }
    public static ExprHandle DatetimeRanges(
        ExprHandle? start,
        ExprHandle? end,
        string? interval,
        ExprHandle? numSamples,
        PlClosedWindow closedWindow,
        PlTimeUnit unit,
        string? timeZone) 
    {
        IntPtr startHandle = start?.TransferOwnership() ?? IntPtr.Zero;
        IntPtr endHandle = end?.TransferOwnership() ?? IntPtr.Zero;
        IntPtr numSamplesHandle = numSamples?.TransferOwnership() ?? IntPtr.Zero;
        var h = NativeBindings.pl_expr_datetime_ranges(startHandle,endHandle,interval,numSamplesHandle,closedWindow,unit,timeZone);
        return ErrorHelper.Check(h);
    }
    public static ExprHandle TimeRange(ExprHandle start,ExprHandle end,string? interval,PlClosedWindow closedWindow) 
    {
        var h = NativeBindings.pl_expr_time_range(start,end,interval,closedWindow);
        start.TransferOwnership();
        end.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle TimeRanges(ExprHandle start,ExprHandle end,string? interval,PlClosedWindow closedWindow) 
    {
        var h = NativeBindings.pl_expr_time_ranges(start,end,interval,closedWindow);
        start.TransferOwnership();
        end.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle LinearSpace(
        ExprHandle start,
        ExprHandle end,
        ExprHandle numSamples,
        PlClosedInterval closedInterval) 
    {
        var h = NativeBindings.pl_expr_linear_space(start,end,numSamples,closedInterval);
        start.TransferOwnership();
        end.TransferOwnership();
        numSamples.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle LinearSpaces(
        ExprHandle start,
        ExprHandle end,
        ExprHandle numSamples,
        PlClosedInterval closedInterval,
        bool asArray) 
    {
        var h = NativeBindings.pl_expr_linear_spaces(start,end,numSamples,closedInterval,asArray);
        start.TransferOwnership();
        end.TransferOwnership();
        numSamples.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static bool IsColumn(ExprHandle expr)
    {
        int code = NativeBindings.pl_expr_meta_is_column(expr,out bool result);
        ErrorHelper.CheckStatus(code);
        return result;
    }
    public static bool IsColumnSelection(ExprHandle expr,bool allowAliasing)
    {
        int code = NativeBindings.pl_expr_meta_is_column_selection(expr,allowAliasing,out bool result);
        ErrorHelper.CheckStatus(code);
        return result;
    }
    public static bool IsLiteral(ExprHandle expr,bool allowAliasing)
    {
        int code = NativeBindings.pl_expr_meta_is_literal(expr,allowAliasing,out bool result);
        ErrorHelper.CheckStatus(code);
        return result;
    }
    public static bool IsRegexProjection(ExprHandle expr)
    {
        int code = NativeBindings.pl_expr_meta_is_regex_projection(expr,out bool result);
        ErrorHelper.CheckStatus(code);
        return result;
    }
    public static bool HasMultipleOutputs(ExprHandle expr)
    {
        int code = NativeBindings.pl_expr_meta_has_multiple_outputs(expr,out bool result);
        ErrorHelper.CheckStatus(code);
        return result;
    }
    public static string[] RootNames(ExprHandle expr)
    {
        int status = NativeBindings.pl_expr_meta_root_names(expr, out IntPtr ptr);
        ErrorHelper.CheckStatus(status); 

        if (ptr == IntPtr.Zero) return [];

        try
        {
            unsafe
            {
                byte* p = (byte*)ptr;
                
                int len = 0;
                while (p[len] != 0) len++;

                if (len == 0) return [];

                var span = new ReadOnlySpan<byte>(p, len);

                int count = 1;
                foreach (byte b in span)
                {
                    if (b == 0x1F) count++;
                }

                string[] result = new string[count];

                int arrayIndex = 0;
                int sliceStart = 0;
                for (int i = 0; i <= span.Length; i++)
                {
                    if (i == span.Length || span[i] == 0x1F)
                    {
                        var slice = span[sliceStart..i];
                        
                        result[arrayIndex++] = System.Text.Encoding.UTF8.GetString(slice);
                        
                        sliceStart = i + 1;
                    }
                }

                return result;
            }
        }
        finally
        {
            NativeBindings.pl_free_string(ptr); 
        }
    }
    public static bool ExprEquals(ExprHandle expr,ExprHandle other)
    {
        int status = NativeBindings.pl_expr_meta_eq(expr,other, out bool result);
        ErrorHelper.CheckStatus(status);
        return result;
    }
    public static ExprHandle ExprAllHorizontal(ExprHandle[] exprs)
    {
        var ptrs = HandlesToPtrs(exprs); 
        var h = NativeBindings.pl_expr_all_horizontal(ptrs,(nuint)ptrs.Length);
        return ErrorHelper.Check(h);
    }
    public static ExprHandle ExprAnyHorizontal(ExprHandle[] exprs)
    {
        var ptrs = HandlesToPtrs(exprs); 
        var h = NativeBindings.pl_expr_any_horizontal(ptrs,(nuint)ptrs.Length);
        return ErrorHelper.Check(h);
    }
    public static ExprHandle ExprMaxHorizontal(ExprHandle[] exprs)
    {
        var ptrs = HandlesToPtrs(exprs); 
        var h = NativeBindings.pl_expr_max_horizontal(ptrs,(nuint)ptrs.Length);
        return ErrorHelper.Check(h);
    }
    public static ExprHandle ExprMinHorizontal(ExprHandle[] exprs)
    {
        var ptrs = HandlesToPtrs(exprs); 
        var h = NativeBindings.pl_expr_min_horizontal(ptrs,(nuint)ptrs.Length);
        return ErrorHelper.Check(h);
    }
    public static ExprHandle ExprSumHorizontal(ExprHandle[] exprs,bool ignoreNulls)
    {
        var ptrs = HandlesToPtrs(exprs); 
        var h = NativeBindings.pl_expr_sum_horizontal(ptrs,(nuint)ptrs.Length,ignoreNulls);
        return ErrorHelper.Check(h);
    }
    public static ExprHandle ExprMeanHorizontal(ExprHandle[] exprs,bool ignoreNulls)
    {
        var ptrs = HandlesToPtrs(exprs); 
        var h = NativeBindings.pl_expr_mean_horizontal(ptrs,(nuint)ptrs.Length,ignoreNulls);
        return ErrorHelper.Check(h);
    }
    public static ExprHandle ExprSetSorted(ExprHandle expr,bool descending,bool nullsLast)
    {
        var h = NativeBindings.pl_expr_set_sorted_flag(expr,descending,nullsLast);
        expr.TransferOwnership();
        return ErrorHelper.Check(h);
    }
}