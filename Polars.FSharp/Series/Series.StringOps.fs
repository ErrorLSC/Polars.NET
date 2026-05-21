namespace Polars.FSharp

type SeriesStrNameSpace(parent: Series) =
    
    let apply (op: Expr -> Expr) =
        let expr = Expr.Col parent.Name |> op
        parent.ApplyExpr expr

    /// <summary> Convert to uppercase. </summary>
    member _.ToUppercase() = apply (fun e -> e.Str.ToUppercase())

    /// <summary> Convert to lowercase. </summary>
    member _.ToLowercase() = apply (fun e -> e.Str.ToLowercase())
    member _.ToTitlecase() = apply (fun e -> e.Str.ToTitlecase())
    member _.EscapeRegex() = apply (fun e -> e.Str.EscapeRegex())
    /// <summary> Get length in bytes. </summary>
    member _.LenBytes() = apply (fun e -> e.Str.LenBytes())
    member _.LenChars() = apply (fun e -> e.Str.LenChars())
    member _.Reverse() = apply (fun e -> e.Str.Reverse())
    /// <summary> Slice the string. </summary>
    member _.Slice(offset: int64, ?length: uint64) = 
        apply (fun e -> e.Str.Slice(offset, ?length=length))
    member _.Replace(pattern,value,?literal,?n) = 
        apply (fun e -> e.Str.Replace(pattern,value,?literal=literal,?n=n))
    /// <summary> Replace all occurrences of a pattern. </summary>
    member _.ReplaceAll(pattern: string, value: string, ?literal: bool) =
        apply (fun e -> e.Str.ReplaceAll(pattern, value, ?literal=literal))
    member _.ReplaceMany(patterns,replaceWith,?asciiCaseInsensitive,?leftmost) = 
        apply (fun e-> e.Str.ReplaceMany(patterns,replaceWith,?asciiCaseInsensitive=asciiCaseInsensitive,?leftmost=leftmost))
    /// <summary> Extract the target capture group from regex pattern. </summary>
    member _.Extract(pattern: string, ?groupIndex: int) =
        apply (fun e -> e.Str.Extract(pattern, ?groupIndex=groupIndex))
    member _.ExtractMany(patterns,?asciiCaseInsensitive,?overlapping,?leftmost) = 
        apply (fun e -> e.Str.ExtractMany(patterns,?asciiCaseInsensitive=asciiCaseInsensitive,?overlapping=overlapping,?leftmost=leftmost))
    member _.ExtractGroups(pattern) = apply (fun e -> e.Str.ExtractGroups(pattern))
    /// <summary> Check if string contains pattern. </summary>
    member _.Contains(pattern: string,?literal:bool,?strict:bool) = 
        apply (fun e -> e.Str.Contains(pattern,?literal=literal,?strict=strict))
    member _.ContainsAny(pattern,?asciiCaseInsensitive) = 
        apply (fun e-> e.Str.ContainsAny(pattern,?asciiCaseInsensitive=asciiCaseInsensitive))
    /// <summary> Split string by separator. Returns a List column. </summary>
    member _.Split(separator: string,?inclusive:bool,?literal:bool,?strict:bool) = 
        apply (fun e -> e.Str.Split(separator,?inclusive=inclusive,?literal=literal,?strict=strict))
    member _.SplitN(by,n) = apply (fun e -> e.Str.SplitN(by,n))
    member _.SplitExact(by,n,?inclusive) = apply (fun e -> e.Str.SplitExact(by,n,?inclusive=inclusive))
    member _.Find(pattern,?literal,?strict) = apply (fun e -> e.Str.Find(pattern,?literal=literal,?strict=strict))
    member _.FindMany(patterns,?asciiCaseInsensitive,?overlapping,?leftmost) =
        apply (fun e -> e.Str.FindMany(patterns,?asciiCaseInsensitive=asciiCaseInsensitive,?overlapping=overlapping,?leftmost=leftmost))
    member _.CountMatches(pattern,?literal) = apply (fun e -> e.Str.CountMatches(pattern,?literal=literal))
    /// <summary> Remove leading and trailing characters. </summary>
    member _.StripChars(?characters: string) = 
        apply (fun e -> e.Str.StripChars(?characters=characters))

    /// <summary> Remove leading characters. </summary>
    member _.StripCharsStart(?characters: string) = 
        apply (fun e -> e.Str.StripCharsStart(?characters=characters))

    /// <summary> Remove trailing characters. </summary>
    member _.StripCharsEnd(?characters: string) = 
        apply (fun e -> e.Str.StripCharsEnd(?characters=characters))

    member _.StripPrefix(prefix: string) = 
        apply (fun e -> e.Str.StripPrefix prefix)

    member _.StripSuffix(suffix: string) = 
        apply (fun e -> e.Str.StripSuffix suffix)

    // --- Checks ---

    member _.StartsWith(prefix: string) = 
        apply (fun e -> e.Str.StartsWith prefix)

    member _.EndsWith(suffix: string) = 
        apply (fun e -> e.Str.EndsWith suffix)
    member _.Head(n) = apply (fun e -> e.Str.Head n)
    member _.Tail(n) = apply (fun e -> e.Str.Tail n)
    member _.JsonPathMatch(jsonPath) = apply (fun e -> e.Str.JsonPathMatch jsonPath)
    member _.JsonDecode(dtype:DataType) = apply (fun e -> e.Str.JsonDecode dtype)
    member _.Zfill(length) = apply (fun e -> e.Str.Zfill length)
    member _.PadStart(length,?fillChar) = apply (fun e -> e.Str.PadStart(length,?fillChar=fillChar))
    member _.PadEnd(length,?fillChar) = apply (fun e -> e.Str.PadEnd(length,?fillChar=fillChar))
    member _.Join(?delimiter,?ignoreNulls) = apply (fun e -> e.Str.Join(?delimiter=delimiter,?ignoreNulls=ignoreNulls))
    member _.Encode(encoding) = apply (fun e -> e.Str.Encode(encoding))
    member _.Decode(encoding,?strict) = apply (fun e -> e.Str.Decode(encoding,?strict=strict))
    member _.Normalize(form) = apply (fun e -> e.Str.Normalize(form))
    // --- Parsing ---

    /// <summary> Parse string to Date. </summary>
    member _.ToDate(?format: string,?strict:bool,?exact:bool,?cache:bool) = 
        apply (fun e -> e.Str.ToDate(?format=format,?strict=strict,?exact=exact,?cache=cache))
    member _.ToTime(?format: string,?strict:bool,?exact:bool,?cache:bool) = 
        apply (fun e -> e.Str.ToTime(?format=format,?strict=strict,?exact=exact,?cache=cache))

    /// <summary> Parse string to Datetime. </summary>
    member _.ToDatetime(
        ?format: string,
        ?timeUnit: TimeUnit,
        ?timeZone: string,
        ?strict: bool,
        ?exact: bool,
        ?cache: bool,
        ?ambiguous: Expr) = 
        apply (fun e -> e.Str.ToDatetime(?format=format,?timeUnit=timeUnit,?timeZone=timeZone,?strict=strict,
            ?exact=exact,?cache=cache,?ambiguous=ambiguous))
    member _.Strptime(
        dtype:DataType,
        ?format: string,
        ?strict: bool,
        ?exact: bool,
        ?cache: bool,
        ?ambiguous: Expr) =
        apply (fun e -> e.Str.Strptime(dtype,?format=format,?strict=strict,
            ?exact=exact,?cache=cache,?ambiguous=ambiguous))
    member _.ToDecimal(scale) = apply (fun e -> e.Str.ToDecimal(scale))
    member _.ToInteger(?radix,?dtype,?strict) = apply (fun e -> e.Str.ToInteger(?radix=radix,?dtype=dtype,?strict=strict))