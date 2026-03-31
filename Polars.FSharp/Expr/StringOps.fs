namespace Polars.FSharp

open Polars.NET.Core

type [<Struct>] StringOps(handle: ExprHandle) =
    
    /// <summary> Convert to uppercase. </summary>
    member _.ToUpper() = new Expr(PolarsWrapper.StrToUpper handle)
    /// <summary> Convert to lowercase. </summary>
    member _.ToLower() = new Expr(PolarsWrapper.StrToLower handle)
    /// <summary> Get length in bytes. </summary>
    member _.Len() = new Expr(PolarsWrapper.StrLenBytes handle)
    // F# uint64 = C# ulong
    member _.Slice(offset: int64, length: uint64) = 
        new Expr(PolarsWrapper.StrSlice(handle, offset, length))
    member _.ReplaceAll(pattern: string, value: string, ?useRegex: bool) =
        let regex = defaultArg useRegex false
        new Expr(PolarsWrapper.StrReplaceAll(handle, pattern, value,regex))
    member _.Extract(pattern: string, groupIndex: int) =
        new Expr(PolarsWrapper.StrExtract(handle, pattern, uint groupIndex))
    member _.Contains(pat: string) = 
        new Expr(PolarsWrapper.StrContains(handle, pat))
    member _.Split(separator: string) = new Expr(PolarsWrapper.StrSplit(handle, separator))
    /// <summary>
    /// Remove leading and trailing characters.
    /// If 'matches' is omitted, whitespace is removed.
    /// </summary>
    member _.Strip(?matches: string) = 
        // Option.toObj: None -> null, Some s -> s
        new Expr(PolarsWrapper.StrStripChars(handle, Option.toObj matches))

    /// <summary>
    /// Remove leading characters (Left Trim).
    /// If 'matches' is omitted, whitespace is removed.
    /// </summary>
    member _.LStrip(?matches: string) = 
        new Expr(PolarsWrapper.StrStripCharsStart(handle, Option.toObj matches))

    /// <summary>
    /// Remove trailing characters (Right Trim).
    /// If 'matches' is omitted, whitespace is removed.
    /// </summary>
    member _.RStrip(?matches: string) = 
        new Expr(PolarsWrapper.StrStripCharsEnd(handle, Option.toObj matches))

    /// <summary>
    /// Remove a specific prefix string.
    /// </summary>
    member _.StripPrefix(prefix: string) = 
        new Expr(PolarsWrapper.StrStripPrefix(handle, prefix))

    /// <summary>
    /// Remove a specific suffix string.
    /// </summary>
    member _.StripSuffix(suffix: string) = 
        new Expr(PolarsWrapper.StrStripSuffix(handle, suffix))

    /// <summary>
    /// Check if string starts with a specific prefix.
    /// </summary>
    member _.StartsWith(prefix: string) = 
        new Expr(PolarsWrapper.StrStartsWith(handle, prefix))

    /// <summary>
    /// Check if string ends with a specific suffix.
    /// </summary>
    member _.EndsWith(suffix: string) = 
        new Expr(PolarsWrapper.StrEndsWith(handle, suffix))

    /// <summary>
    /// Parse string to Date using a format string (e.g., "%Y-%m-%d").
    /// </summary>
    /// <param name="format">The parsing format (e.g., "%Y-%m-%d"). Null for auto-inference.</param>
    /// <param name="strict">If true, raises an error on parsing failure. If false, returns nulls.</param>
    /// <param name="exact">If true, requires an exact match. If false, allows matching substrings.</param>
    /// <param name="cache">Use a cache of unique converted dates to speed up parsing.</param>
    member _.ToDate(?format: string,?strict:bool,?exact:bool,?cache:bool) = 
        let fmt = Option.toObj format
        let strt = defaultArg strict true
        let ext = defaultArg exact true
        let cac = defaultArg cache true
        new Expr(PolarsWrapper.StrToDate(handle,fmt, strt,ext,cac))

    /// <summary>
    /// Convert string to Datetime. If format is null, Polars will attempt to infer it.
    /// </summary>
    /// <param name="format">The parsing format (e.g., "%Y-%m-%d"). Null for auto-inference.</param>
    /// <param name="timeUnit">Target time unit. Null to use default (usually Microseconds).</param>
    /// <param name="timeZone">Target time zone (e.g., "UTC", "Asia/Shanghai").</param>
    /// <param name="strict">If true, raises an error on parsing failure. If false, returns nulls.</param>
    /// <param name="exact">If true, requires an exact match. If false, allows matching substrings.</param>
    /// <param name="cache">Use a cache of unique converted dates to speed up parsing.</param>
    member this.ToDatetime(
        ?format: string,
        ?timeUnit: TimeUnit,
        ?timeZone: string,
        ?strict: bool,
        ?exact: bool,
        ?cache: bool) = 
        
        let tu = 
            match timeUnit with
            | Some u -> u.ToNative()
            | None -> LanguagePrimitives.EnumOfValue 100uy
        
        let tz = Option.toObj timeZone
        let fmt = Option.toObj format

        let st = defaultArg strict true
        let ex = defaultArg exact true
        let ca = defaultArg cache true
        
        let h = PolarsWrapper.StrToDatetime(
            handle, 
            tu, 
            tz, 
            fmt, 
            st, 
            ex, 
            ca
        )
        
        new Expr(h)