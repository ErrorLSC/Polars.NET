using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;

/// <summary>
/// Date Ops Namespace
/// </summary>
public readonly struct SeriesDtOps
{
    private readonly Series _series;
    internal SeriesDtOps(Series series) { _series = series; }

    private Series Apply(Func<Expr, Expr> op) 
    {
        var expr = op(Pl.Col(_series.Name));
        
        return _series.ApplyExpr(expr);
    }
    /// <inheritdoc cref="DtOps.Millennium"/>
    public Series Millennium() => Apply(e => e.Dt.Millennium());
    /// <inheritdoc cref="DtOps.Century"/>
    public Series Century() => Apply(e => e.Dt.Century());
    /// <inheritdoc cref="DtOps.Year"/>
    public Series Year() => Apply(e => e.Dt.Year());
    /// <inheritdoc cref="DtOps.IsoYear"/>
    public Series IsoYear() => Apply(e => e.Dt.IsoYear());
    /// <inheritdoc cref="DtOps.Quarter"/>
    public Series Quarter() => Apply(e => e.Dt.Quarter());
    /// <inheritdoc cref="DtOps.Month"/>
    public Series Month() => Apply(e => e.Dt.Month());
    /// <inheritdoc cref="DtOps.Day"/>
    public Series Day() => Apply(e => e.Dt.Day());
    /// <inheritdoc cref="DtOps.DaysInMonth"/>
    public Series DaysInMonth() => Apply(e => e.Dt.DaysInMonth());
    /// <inheritdoc cref="DtOps.OrdinalDay"/>
    public Series OrdinalDay() => Apply(e => e.Dt.OrdinalDay());
    /// <inheritdoc cref="DtOps.Weekday"/>
    public Series Weekday() => Apply(e => e.Dt.Weekday());
    /// <inheritdoc cref="DtOps.Hour"/>
    public Series Hour() => Apply(e => e.Dt.Hour());
    /// <inheritdoc cref="DtOps.Minute"/>
    public Series Minute() => Apply(e => e.Dt.Minute());
    /// <inheritdoc cref="DtOps.Second"/>
    public Series Second(bool fractional = false) => Apply(e => e.Dt.Second(fractional));
    /// <inheritdoc cref="DtOps.Millisecond"/>
    public Series Millisecond() => Apply(e => e.Dt.Millisecond());
    /// <inheritdoc cref="DtOps.Microsecond"/>
    public Series Microsecond() => Apply(e => e.Dt.Microsecond());
    /// <inheritdoc cref="DtOps.Nanosecond"/>
    public Series Nanosecond() => Apply(e => e.Dt.Nanosecond());
    /// <inheritdoc cref="DtOps.IsLeapYear"/>
    public Series IsLeapYear() => Apply(e => e.Dt.IsLeapYear());
    /// <inheritdoc cref="DtOps.MonthStart"/>
    public Series MonthStart() => Apply(e => e.Dt.MonthStart());
    /// <inheritdoc cref="DtOps.MonthEnd"/>
    public Series MonthEnd() => Apply(e => e.Dt.MonthEnd());
    /// <inheritdoc cref="DtOps.BaseUtcOffset"/>
    public Series BaseUtcOffset() => Apply(e => e.Dt.BaseUtcOffset());
    /// <inheritdoc cref="DtOps.DstOffset"/>
    public Series DstOffset() => Apply(e => e.Dt.DstOffset());
    /// <inheritdoc cref="DtOps.Date"/>
    public Series Date() => Apply(e => e.Dt.Date());
    /// <inheritdoc cref="DtOps.Time"/>
    public Series Time() => Apply(e => e.Dt.Time());
    /// <inheritdoc cref="DtOps.TotalDays"/>
    public Series TotalDays(bool fractional=false) => Apply(e => e.Dt.TotalDays(fractional));
    /// <inheritdoc cref="DtOps.TotalHours"/>
    public Series TotalHours(bool fractional=false) => Apply(e => e.Dt.TotalHours(fractional));
    /// <inheritdoc cref="DtOps.TotalMinutes"/>
    public Series TotalMinutes(bool fractional=false) => Apply(e => e.Dt.TotalMinutes(fractional));
    /// <inheritdoc cref="DtOps.TotalMilliseconds"/>
    public Series TotalMilliseconds(bool fractional=false) => Apply(e => e.Dt.TotalMilliseconds(fractional));
    /// <inheritdoc cref="DtOps.TotalMicroseconds"/>
    public Series TotalMicroseconds(bool fractional=false) => Apply(e => e.Dt.TotalMicroseconds(fractional));
    /// <inheritdoc cref="DtOps.TotalNanoseconds"/>
    public Series TotalNanoseconds(bool fractional=false) => Apply(e => e.Dt.TotalNanoseconds(fractional));
    /// <inheritdoc cref="DtOps.ToString"/>
    public Series ToString(string format="iso") => Apply(e => e.Dt.ToString(format));
    /// <inheritdoc cref="DtOps.Strftime"/>
    public Series Strftime(string format="iso") => Apply(e => e.Dt.Strftime(format));
    // ==========================================
    // Truncate & Round
    // ==========================================

    /// <inheritdoc cref="DtOps.Truncate"/>
    public Series Truncate(DurationOrExpr every) => Apply(e => e.Dt.Truncate(every));
    /// <inheritdoc cref="DtOps.Round"/>
    public Series Round(DurationOrExpr every) => Apply(e => e.Dt.Round(every));
    // ==========================================
    // Offset
    // ==========================================
    /// <inheritdoc cref="DtOps.OffsetBy"/>
    public Series OffsetBy(DurationOrExpr by) => Apply(e => e.Dt.OffsetBy(by));
    // ==========================================
    // Timestamp
    // ==========================================

    /// <inheritdoc cref="DtOps.Timestamp"/>
    public Series Timestamp(TimeUnit timeUnit = TimeUnit.Microseconds) => Apply(e => e.Dt.Timestamp(timeUnit));
    /// <inheritdoc cref="DtOps.Combine"/>
    public Series Combine(Expr time,TimeUnit timeUnit) => Apply(e => e.Dt.Combine(time,timeUnit));
    /// <inheritdoc cref="DtOps.Combine"/>
    public Series Combine(Series time,TimeUnit timeUnit) => Apply(e => e.Dt.Combine(Pl.Lit(time),timeUnit));

    // ==========================================
    // TimeZone
    // ==========================================
    /// <inheritdoc cref="DtOps.ConvertTimeZone"/>
    public Series ConvertTimeZone(string timeZone) => Apply(e => e.Dt.ConvertTimeZone(timeZone));

    /// <inheritdoc cref="DtOps.ReplaceTimeZone"/>
    public Series ReplaceTimeZone(string? timeZone, AmbiguousArg? ambiguous = null, NonExistent nonExistent = NonExistent.Raise)
         =>Apply(e => e.Dt.ReplaceTimeZone(timeZone,ambiguous,nonExistent));
    // ==========================================
    // BusinessDays
    // ==========================================
    /// <inheritdoc cref="DtOps.AddBusinessDays"/>
    public Series AddBusinessDays(
        Expr n, 
        IEnumerable<DateOnly>? holidays = null, 
        bool[]? weekMask = null, 
        Roll roll = Roll.Raise)
        =>Apply(e => e.Dt.AddBusinessDays(n,holidays,weekMask,roll));
    /// <inheritdoc cref="DtOps.IsBusinessDay(IEnumerable{DateOnly},bool[])"/>
    public Series IsBusinessDay(IEnumerable<DateOnly>? holidays = null, bool[]? weekMask = null)
        =>Apply(e => e.Dt.IsBusinessDay(holidays,weekMask));
    /// <inheritdoc cref="DtOps.IsBusinessDay(IEnumerable{DateOnly},bool[])"/>   
    public Series IsBusinessDay(Expr holidays, bool[]? weekMask = null)
        =>Apply(e => e.Dt.IsBusinessDay(holidays,weekMask));
    /// <inheritdoc cref="DtOps.IsBusinessDay(IEnumerable{DateOnly},bool[])"/>
    public Series IsBusinessDay(Series holidays, bool[]? weekMask = null)
        =>Apply(e => e.Dt.IsBusinessDay(holidays,weekMask));
    /// <inheritdoc cref="DtOps.Replace"/>
    public Series Replace(
        IntoExprColumn? year = null,
        IntoExprColumn? month = null,
        IntoExprColumn? day = null,
        IntoExprColumn? hour = null,
        IntoExprColumn? minute = null,
        IntoExprColumn? second = null,
        IntoExprColumn? microsecond = null,
        AmbiguousArg? ambiguous = null)
    => Apply(e => e.Dt.Replace(year,month,day,hour,minute,second,microsecond,ambiguous));
    /// <inheritdoc cref="DtOps.Epoch"/>
    public Series Epoch(TimeUnit timeUnit = TimeUnit.Microseconds) => Apply(e => e.Dt.Epoch(timeUnit));
    
}