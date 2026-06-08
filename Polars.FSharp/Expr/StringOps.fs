namespace Polars.FSharp

open Polars.NET.Core
open System.Text

type [<Struct>] StringOps(handle: ExprHandle) =
    
    /// <summary> Convert to uppercase. </summary>
    member _.ToUppercase() = new Expr(PolarsWrapper.StrToUpper handle)
    /// <summary> Convert to lowercase. </summary>
    member _.ToLowercase() = new Expr(PolarsWrapper.StrToLower handle)
    /// <summary>
    /// Modify strings to their titlecase equivalent.
    /// <para>This is a form of case transform where the first letter of each word is capitalized, with the rest of the word in lowercase.
    /// Non-alphanumeric characters define the word boundaries.
    /// </para>
    /// </summary>
    member _.ToTitlecase() = new Expr(PolarsWrapper.StrToTitlecase handle)
    /// <summary>
    /// Returns string values with all regular expression meta characters escaped.
    /// </summary>
    member _.EscapeRegex() = new Expr(PolarsWrapper.StrEscapeRegex handle)
    /// <summary> Get length in bytes. </summary>
    member _.LenBytes() = new Expr(PolarsWrapper.StrLenBytes handle)
    /// <summary>
    /// Return the length of each string as the number of characters.
    /// </summary>
    /// <returns>Expression of data type UInt32.</returns>
    member _.LenChars() = new Expr(PolarsWrapper.StrLenChars handle)
    /// <summary>
    /// Returns string values in reversed order.
    /// </summary>
    member _.Reverse() = new Expr(PolarsWrapper.StrReverse(handle))
    /// <summary>
    /// Slice the string by offset and length.
    /// </summary>
    /// <param name="offset">Start index.</param>
    /// <param name="length">Length of the slice.</param>
    member _.Slice(offset: int64, ?length: uint64) = 
        let len = 
            match length with
            | Some length -> PolarsWrapper.Lit length
            | None -> PolarsWrapper.LitNull()
        new Expr(PolarsWrapper.StrSlice(handle, PolarsWrapper.Lit offset, len))
    /// <summary>
    /// Replace first matching regex/literal substring with a new string value.
    /// </summary>
    /// <param name="pattern">A valid regular expression pattern, compatible with the regex crate.</param>
    /// <param name="value">String that will replace the matched substring.</param>
    /// <param name="literal">Treat pattern as a literal string, not a regex.</param>
    /// <param name="n">Number of matches to replace.</param>
    member _.Replace(pattern:string,value:string,?literal:bool,?n:int) = 
        let lit = defaultArg literal false
        let nt = defaultArg n 1
        new Expr(PolarsWrapper.StrReplace(handle,PolarsWrapper.Lit pattern,PolarsWrapper.Lit value,lit,nt))
    /// <summary>
    /// Replace all occurrences of a pattern with a value.
    /// </summary>
    /// <param name="pattern">The pattern to search for.</param>
    /// <param name="value">The value to replace with.</param>
    /// <param name="literal">Whether to interpret the pattern as literal value(not Regex).</param>
    member _.ReplaceAll(pattern: string, value: string, ?literal: bool) =
        let regex = defaultArg literal true
        new Expr(PolarsWrapper.StrReplaceAll(handle,PolarsWrapper.Lit pattern, PolarsWrapper.Lit value,regex))
    /// <summary>
    /// Use the Aho-Corasick algorithm to replace many matches.
    /// </summary>
    /// <param name="patterns">Expression yielding string patterns to search and replace.</param>
    /// <param name="replaceWith">Strings to replace where a pattern was a match. Length must match the length of patterns or have length 1. This can be broadcasted, so it supports many:one and many:many.</param>
    /// <param name="asciiCaseInsensitive">Enable ASCII-aware case-insensitive matching.</param>
    /// <param name="leftmost">Guarantees in case there are overlapping matches that the leftmost match is used.</param>
    member _.ReplaceMany(patterns:Expr,replaceWith:Expr,?asciiCaseInsensitive,?leftmost) = 
        let ascii = defaultArg asciiCaseInsensitive false
        let lef = defaultArg leftmost false
        new Expr(PolarsWrapper.StrReplaceMany(handle,patterns.CloneHandle(),replaceWith.CloneHandle(),ascii,lef))
    /// <summary>
    /// Extract the first match of a regex pattern.
    /// </summary>
    /// <param name="pattern">Regex pattern with capture groups.</param>
    /// <param name="groupIndex">The index of the capture group to extract (usually 1).</param>
    member _.Extract(pattern: string, ?groupIndex: int) =
        let gi = defaultArg groupIndex 1
        new Expr(PolarsWrapper.StrExtract(handle,PolarsWrapper.Lit pattern,gi))
    /// <summary>
    /// Use the Aho-Corasick algorithm to extract many matches.This method supports matching on string literals only, and does not support regular expression matching.
    /// </summary>
    /// <param name="patterns">String patterns to search.</param>
    /// <param name="asciiCaseInsensitive">Enable ASCII-aware case-insensitive matching. When this option is enabled, searching will be performed without respect to case for ASCII letters (a-z and A-Z) only.</param>
    /// <param name="overlapping">Whether matches may overlap.</param>
    /// <param name="leftmost">Guarantees in case there are overlapping matches that the leftmost match is used. In case there are multiple candidates for the leftmost match the pattern which comes first in patterns is used. 
    /// May not be used together with overlapping = True.</param>
    member _.ExtractMany(patterns:Expr,?asciiCaseInsensitive,?overlapping,?leftmost) = 
        let asc = defaultArg asciiCaseInsensitive false
        let ovr = defaultArg overlapping false
        let lef = defaultArg leftmost false
        new Expr(PolarsWrapper.StrExtractMany(handle,patterns.CloneHandle(),asc,ovr,lef))
    /// <summary>
    /// Extract all matches for the given regex pattern.
    /// Extract each successive non-overlapping regex match in an individual string as a list. If the haystack string is null, null is returned.
    /// </summary>
    /// <param name="pattern">A valid regular expression pattern, compatible with the regex crate.</param>
    /// <returns>Expression of data type List(String).</returns>
    member _.ExtractAll(pattern:string) = new Expr(PolarsWrapper.StrExtractAll(handle,PolarsWrapper.Lit pattern))
    /// <summary>
    /// Extract all capture groups for the given regex pattern.
    /// </summary>
    /// <param name="pattern">A valid regular expression pattern containing at least one capture group, compatible with the regex crate.</param>
    /// <returns>Expression of data type Struct with fields of data type String.</returns>
    member _.ExtractGroups(pattern:string) = new Expr(PolarsWrapper.StrExtractGroups(handle,pattern))
    /// <summary>
    /// Check if the string contains a substring or regex pattern.
    /// </summary>
    member _.Contains(pattern: string,?literal:bool,?strict:bool) = 
        let li = defaultArg literal false
        let st = defaultArg strict true 
        new Expr(PolarsWrapper.StrContains(handle,PolarsWrapper.Lit pattern,li,st))
    /// <summary>
    /// Use the Aho-Corasick algorithm to find matches.
    /// Determines if any of the patterns are contained in the string.
    /// </summary>
    /// <param name="patterns">String patterns to search.</param>
    /// <param name="asciiCaseInsensitive">Enable ASCII-aware case-insensitive matching. 
    /// When this option is enabled, searching will be performed without respect to case for ASCII letters 
    /// (a-z and A-Z) only.</param>
    member _.ContainsAny(pattern:string,?asciiCaseInsensitive) = 
        let asc = defaultArg asciiCaseInsensitive false
        new Expr(PolarsWrapper.StrContainsAny(handle,PolarsWrapper.Lit pattern,asc))
    /// <summary>
    /// Split the string by a substring.
    /// </summary>
    /// <param name="by">Substring to split by.</param>
    /// <param name="inclusive">If True, include the split character/string in the results.</param>
    /// <param name="literal">Treat by as a literal string, not as a regular expression.</param>
    /// <param name="strict">Raise an error if the underlying pattern is not a valid regex, otherwise mask out with a null value.</param>
    /// <returns>Expression/Series of data type String.</returns>
    member _.Split(separator: string,?inclusive:bool,?literal:bool,?strict:bool) = 
        let inc = defaultArg inclusive false
        let li = defaultArg literal true
        let st = defaultArg strict true 
        new Expr(PolarsWrapper.StrSplit(handle,PolarsWrapper.Lit separator,inc,li,st))
    /// <summary>
    /// Split the string by a substring, restricted to returning at most n items.
    /// <para>If the number of possible splits is less than n-1, the remaining field elements will be null. 
    /// If the number of possible splits is n-1 or greater, the last (nth) substring will contain the remainder of the string.
    /// </para>
    /// </summary>
    /// <param name="by">Substring to split by.</param>
    /// <param name="n">Max number of items to return.</param>
    /// <returns>Expression/Series of data type Struct with fields of data type String.</returns>
    member _.SplitN(by:string,n:int) = new Expr(PolarsWrapper.StrSplitN(handle,PolarsWrapper.Lit by,n))
    /// <summary>
    /// Split the string by a substring using n splits.
    /// Results in a struct of n+1 fields.
    /// If it cannot make n splits, the remaining field elements will be null.
    /// </summary>
    /// <param name="by">Substring to split by.</param>
    /// <param name="n">Number of splits to make.</param>
    /// <param name="inclusive">If True, include the split character/string in the results.</param>
    /// <returns>Expression/Series of data type Struct with fields of data type String.</returns>
    member _.SplitExact(by:string,n:int,?inclusive) = 
        let inc = defaultArg inclusive false
        new Expr(PolarsWrapper.StrSplitExact(handle,PolarsWrapper.Lit by,n,inc))
    /// <summary>
    /// Return the bytes offset of the first substring matching a pattern.
    /// If the pattern is not found, returns None.
    /// </summary>
    /// <param name="pattern">A valid regular expression pattern, compatible with the regex crate.</param>
    /// <param name="literal">Treat pattern as a literal string, not as a regular expression.</param>
    /// <param name="strict">Raise an error if the underlying pattern is not a valid regex, 
    /// otherwise mask out with a null value.</param>
    member _.Find(pattern:string,?literal,?strict) = 
        let lit = defaultArg literal false
        let st = defaultArg strict true
        new Expr(PolarsWrapper.StrFind(handle,PolarsWrapper.Lit pattern,lit,st))
    member _.FindMany(patterns:Expr,?asciiCaseInsensitive,?overlapping,?leftmost) =
        let asc = defaultArg asciiCaseInsensitive false
        let ovr = defaultArg overlapping false
        let lef = defaultArg leftmost false
        new Expr(PolarsWrapper.StrFindMany(handle,patterns.CloneHandle(),asc,ovr,lef))
    /// <summary>
    /// Count all successive non-overlapping regex matches.
    /// </summary>
    /// <param name="pattern">A valid regular expression pattern, compatible with the regex crate.</param>
    /// <param name="literal">Treat pattern as a literal string, not as a regular expression.</param>
    /// <returns>Expression of data type UInt32. Returns null if the original value is null.</returns>
    member _.CountMatches(pattern:string,?literal) = 
        let lit = defaultArg literal false
        new Expr(PolarsWrapper.StrCountMatches(handle,PolarsWrapper.Lit pattern,lit))
    /// <summary>
    /// Remove leading and trailing characters.
    /// If matches is null, whitespace is removed.
    /// </summary>
    member _.StripChars(?characters:string) = 
        let char = 
            match characters with
            | Some c -> PolarsWrapper.Lit c
            | None -> PolarsWrapper.LitNull()
        new Expr(PolarsWrapper.StrStripChars(handle, char))
    /// <summary>
    /// Remove leading characters (Left Trim).
    /// If 'matches' is omitted, whitespace is removed.
    /// </summary>
    member _.StripCharsStart(?characters: string) = 
        let charsExprHandle =
            match characters with
            | Some s -> PolarsWrapper.Lit(s)      
            | None   -> PolarsWrapper.LitNull()  
        new Expr(PolarsWrapper.StrStripCharsStart(handle, charsExprHandle))

    /// <summary>
    /// Remove trailing characters (Right Trim).
    /// If 'matches' is omitted, whitespace is removed.
    /// </summary>
    member _.StripCharsEnd(?characters: string) = 
        let charsExprHandle =
            match characters with
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
    /// Return the first n characters of each string in a String Series.
    /// </summary>
    /// <param name="n">Length of the slice
    /// <para>When the n input is negative, head returns characters up to the n`th from the end of the string.
    /// For example, if n = -3, then all characters except the last three are returned.</para></param>
    /// <returns>Expression of data type String.</returns>
    member _.Head(n:int) = new Expr(PolarsWrapper.StrHead(handle,PolarsWrapper.Lit n))
    /// <summary>
    /// Return the last n characters of each string in a String Series.
    /// </summary>
    /// <param name="n">Length of the slice (integer or expression)
    /// <para>When the n input is negative, head returns characters up to the n`th from the start of the string.
    /// For example, if n = -3, then all characters except the first three are returned.</para></param>
    /// <returns>Expression of data type String.</returns>
    member _.Tail(n:int) = new Expr(PolarsWrapper.StrTail(handle,PolarsWrapper.Lit n))
    /// <summary>
    /// Extract the first match from a JSON string using the provided JSONPath.
    /// Throws errors if invalid JSON strings are encountered. All return values are cast to String, regardless of the original value.
    /// </summary>
    /// <param name="jsonPath">A valid JSONPath query string.</param>
    /// <returns>Expression of data type String. Contains null values if original value is null or the json_path returns nothing.</returns>
    member _.JsonPathMatch(jsonPath:string) = new Expr(PolarsWrapper.StrJsonPathMatch(handle,PolarsWrapper.Lit jsonPath))
    /// <summary>
    /// Parse string values as JSON.
    /// Throws an error if invalid JSON strings are encountered.
    /// </summary>
    /// <param name="dtype">The datatype to cast the extracted value to.</param>
    member _.JsonDecode(dtype:DataTypeExpr) = new Expr(PolarsWrapper.StrJsonDecode(handle,dtype.CloneHandle()))
    member this.JsonDecode(dtype:DataType) = this.JsonDecode(dtype.ToDataTypeExpr())
    /// <summary>
    /// Pad the start of the string with zeros until it reaches the given length.
    /// <para>A sign prefix (-) is handled by inserting the padding after the sign character rather than before.</para>
    /// <para>This method is intended for padding numeric strings. If your data contains non-ASCII characters, use pad_start() instead.</para>
    /// </summary>
    /// <param name="length">Pad the string until it reaches this length. Strings with length equal to or greater than this value are returned as-is.</param>
    member _.Zfill(length:int) = new Expr(PolarsWrapper.StrZfill(handle,PolarsWrapper.Lit length))
    /// <summary>
    /// Pad the start of the string until it reaches the given length.
    /// </summary>
    /// <param name="length">Pad the string until it reaches this length. Strings with length equal to or greater than this value are returned as-is.</param>
    /// <param name="fillChar">The character to pad the string with.</param>
    member _.PadStart(length:int,?fillChar:string) = 
        let fill = defaultArg fillChar " "
        new Expr(PolarsWrapper.StrPadStart(handle,PolarsWrapper.Lit length,fill))
    /// <summary>
    /// Pad the end of the string until it reaches the given length.
    /// </summary>
    /// <param name="length">Pad the string until it reaches this length. Strings with length equal to or greater than this value are returned as-is.</param>
    /// <param name="fillChar">The character to pad the string with.</param>
    member _.PadEnd(length:int,?fillChar:string) = 
        let fill = defaultArg fillChar " "
        new Expr(PolarsWrapper.StrPadEnd(handle,PolarsWrapper.Lit length,fill))
    /// <summary>
    /// Vertically concatenate the string values in the column to a single string value.
    /// </summary>
    /// <param name="delimiter">The delimiter to insert between consecutive string values.</param>
    /// <param name="ignoreNulls">Ignore null values (default). If set to False, null values will be propagated. This means that if the column contains any null values, the output is null.</param>
    /// <returns>Expression of data type String.</returns>
    member _.Join(?delimiter:string,?ignoreNulls) =
        let ign = defaultArg ignoreNulls true
        let del = defaultArg delimiter "" 
        new Expr(PolarsWrapper.StrJoin(handle,del,ign))
    /// <summary>
    /// Encode values using the provided encoding.
    /// </summary>
    /// <param name="encoding">The encoding to use.</param>
    /// <returns>Expression of data type String.</returns>
    member _.Encode(encoding:TransferEncoding) =
        match encoding with
            | TransferEncoding.Base64 -> new Expr(PolarsWrapper.StrBase64Encode(handle))
            | TransferEncoding.Hex -> new Expr(PolarsWrapper.StrHexEncode(handle))
    /// <summary>
    /// Decode values using the provided encoding.
    /// </summary>
    /// <param name="encoding">The encoding to use.</param>
    /// <param name="strict">Raise an error if the underlying value cannot be decoded, otherwise mask out with a null value.</param>
    /// <returns>Expression of data type Binary.</returns>
    member _.Decode(encoding:TransferEncoding,?strict) =
        let st = defaultArg strict true
        match encoding with
            | TransferEncoding.Base64 -> new Expr(PolarsWrapper.StrBase64Decode(handle,st))
            | TransferEncoding.Hex -> new Expr(PolarsWrapper.StrHexDecode(handle,st))
    /// <summary>
    /// Returns the Unicode normal form of the string values.
    /// </summary>
    /// <param name="form">Unicode form to use.</param>
    member _.Normalize(form:NormalizationForm) = new Expr(PolarsWrapper.StrNormalize(handle,form))
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
    /// Convert a String column into a Time column.
    /// </summary>
    /// <param name="format">The parsing format (e.g., "%H:%M:%S"). Null for auto-inference. </param>
    /// <param name="strict">Raise an error if any conversion fails.</param>
    /// <param name="exact">If true, requires an exact match. If false, allows matching substrings.</param>
    /// <param name="cache">Use a cache of unique, converted times to apply the conversion.</param>
    member _.ToTime(?format,?strict,?exact,?cache) = 
        let fmt = Option.toObj format
        let strt = defaultArg strict true
        let ext = defaultArg exact true
        let cac = defaultArg cache true
        new Expr(PolarsWrapper.StrToTime(handle,fmt, strt,ext,cac))
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
    /// <summary>
    /// Convert a String column into a Date/Datetime/Time column.
    /// </summary>
    /// <param name="dtype">The data type to convert into. Can be either Date, Datetime, or Time.</param>
    /// <param name="format">The parsing format (e.g., "%Y-%m-%d"). Null for auto-inference.</param>
    /// <param name="strict">Raise an error if any conversion fails.</param>
    /// <param name="exact">Require an exact format match. If False, allow the format to match anywhere in the target string. Conversion to the Time type is always exact.</param>
    /// <param name="cache">Use a cache of unique, converted dates to apply the datetime conversion.</param>
    /// <param name="ambiguous">Determine how to deal with ambiguous datetimes:
    /// 'raise' (default): raise
    /// 'earliest': use the earliest datetime
    /// 'latest': use the latest datetime
    /// 'null': set to null</param>
    member this.Strptime(
        dtype:DataType,
        ?format: string,
        ?strict: bool,
        ?exact: bool,
        ?cache: bool,
        ?ambiguous: Expr) = 
        
        let fmt = Option.toObj format

        let st = defaultArg strict true
        let ex = defaultArg exact true
        let ca = defaultArg cache true
        let amb = 
            match ambiguous with
            | Some a -> a.CloneHandle()
            | None -> PolarsWrapper.Lit "raise"
        
        let h = PolarsWrapper.Strptime(
            handle, 
            dtype.ToDataTypeExpr().Handle, 
            fmt, 
            st, 
            ex, 
            ca,
            amb
        )
        
        new Expr(h)
    /// <summary>
    /// Convert a String column into a Decimal column.
    /// </summary>
    /// <param name="scale">Number of digits after the comma to use for the decimals.</param>
    member _.ToDecimal(scale:int) = new Expr(PolarsWrapper.StrToDecimal(handle,scale))
    /// <summary>
    /// Convert a String column into an Integer column with the specified radix (base).
    /// </summary>
    /// <param name="radix">Positive integer or expression which is the base of the string we are parsing. Default is 10.</param>
    /// <param name="dtype">Integer data type to cast the result to. Default is Int64.</param>
    /// <param name="strict">If true, raises ComputeError on failure. If false, silently converts to Null.</param>
    /// <returns>Expression of destinated integer data type.</returns>
    member _.ToInteger(?radix:int,?dtype:DataType,?strict:bool) =
        let ra = 
            match radix with
            | Some r -> PolarsWrapper.Lit r
            | None -> PolarsWrapper.Lit 10
        let dt = 
            match dtype with
            | Some d -> d.Handle
            | None -> DataType.Int64.Handle
        let st = defaultArg strict true
        new Expr(PolarsWrapper.StrToInteger(handle,ra,dt,st))