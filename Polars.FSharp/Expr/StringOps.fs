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
        new Expr(PolarsWrapper.StrSlice(handle, PolarsWrapper.Lit offset, PolarsWrapper.Lit length))
    member _.ReplaceAll(pattern: string, value: string, ?literal: bool) =
        let regex = defaultArg literal true
        new Expr(PolarsWrapper.StrReplaceAll(handle,PolarsWrapper.Lit pattern, PolarsWrapper.Lit value,regex))
    member _.Extract(pattern: string, groupIndex: int) =
        new Expr(PolarsWrapper.StrExtract(handle,PolarsWrapper.Lit pattern, int groupIndex))
    member _.Contains(pat: string,?literal:bool,?strict:bool) = 
        let li = defaultArg literal false
        let st = defaultArg strict true 
        new Expr(PolarsWrapper.StrContains(handle,PolarsWrapper.Lit pat,li,st))
    member _.Split(separator: string,?inclusive:bool,?literal:bool,?strict:bool) = 
        let inc = defaultArg inclusive false
        let li = defaultArg literal true
        let st = defaultArg strict true 
        new Expr(PolarsWrapper.StrSplit(handle,PolarsWrapper.Lit separator,inc,li,st))
    /// <summary>
    /// Remove leading and trailing characters.
    /// If 'matches' is omitted, whitespace is removed.
    /// </summary>
    member this.Strip(?matches: string) =
        let charsExprHandle =
            match matches with
            | Some s -> PolarsWrapper.Lit(s)      
            | None   -> PolarsWrapper.LitNull()   
        new Expr(PolarsWrapper.StrStripChars(handle, charsExprHandle))

    /// <summary>
    /// Remove leading characters (Left Trim).
    /// If 'matches' is omitted, whitespace is removed.
    /// </summary>
    member _.LStrip(?matches: string) = 
        let charsExprHandle =
            match matches with
            | Some s -> PolarsWrapper.Lit(s)      
            | None   -> PolarsWrapper.LitNull()  
        new Expr(PolarsWrapper.StrStripCharsStart(handle, charsExprHandle))

    /// <summary>
    /// Remove trailing characters (Right Trim).
    /// If 'matches' is omitted, whitespace is removed.
    /// </summary>
    member _.RStrip(?matches: string) = 
        let charsExprHandle =
            match matches with
            | Some s -> PolarsWrapper.Lit(s)      
            | None   -> PolarsWrapper.LitNull()  
        new Expr(PolarsWrapper.StrStripCharsEnd(handle, charsExprHandle))

    /// <summary>
    /// Remove a specific prefix string.
    /// </summary>
    member _.StripPrefix(prefix: string) = 
        new Expr(PolarsWrapper.StrStripPrefix(handle,PolarsWrapper.Lit prefix))

    /// <summary>
    /// Remove a specific suffix string.
    /// </summary>
    member _.StripSuffix(suffix: string) = 
        new Expr(PolarsWrapper.StrStripSuffix(handle, PolarsWrapper.Lit suffix))

    /// <summary>
    /// Check if string starts with a specific prefix.
    /// </summary>
    member _.StartsWith(prefix: string) = 
        new Expr(PolarsWrapper.StrStartsWith(handle, PolarsWrapper.Lit prefix))

    /// <summary>
    /// Check if string ends with a specific suffix.
    /// </summary>
    member _.EndsWith(suffix: string) = 
        new Expr(PolarsWrapper.StrEndsWith(handle, PolarsWrapper.Lit suffix))

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
        ?cache: bool,
        ?ambiguous: Expr) = 
        
        let tu = 
            match timeUnit with
            | Some u -> u.ToNative()
            | None -> LanguagePrimitives.EnumOfValue 100uy
        
        let tz = Option.toObj timeZone
        let fmt = Option.toObj format

        let st = defaultArg strict true
        let ex = defaultArg exact true
        let ca = defaultArg cache true
        let amb = 
            match ambiguous with
            | Some a -> a.CloneHandle()
            | None -> PolarsWrapper.Lit "raise"
        
        let h = PolarsWrapper.StrToDatetime(
            handle, 
            tu, 
            tz, 
            fmt, 
            st, 
            ex, 
            ca,
            amb
        )
        
        new Expr(h)