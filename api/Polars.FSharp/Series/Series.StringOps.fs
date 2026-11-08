namespace Polars.FSharp

type SeriesStrNameSpace(parent: Series) =

    let apply (op: Expr -> Expr) =
        let expr = Expr.Col parent.Name |> op
        parent.ApplyExpr expr

    /// <summary> Convert to uppercase. </summary>
    member _.ToUppercase() = apply (fun e -> e.Str.ToUppercase())

    /// <summary> Convert to lowercase. </summary>
    member _.ToLowercase() = apply (fun e -> e.Str.ToLowercase())
    /// <summary>
    /// Modify strings to their titlecase equivalent.
    /// <para>This is a form of case transform where the first letter of each word is capitalized, with the rest of the word in lowercase.
    /// Non-alphanumeric characters define the word boundaries.
    /// </para>
    /// </summary>
    member _.ToTitlecase() = apply (fun e -> e.Str.ToTitlecase())
    /// <summary>
    /// Returns string values with all regular expression meta characters escaped.
    /// </summary>
    member _.EscapeRegex() = apply (fun e -> e.Str.EscapeRegex())
    /// <summary> Get length in bytes. </summary>
    member _.LenBytes() = apply (fun e -> e.Str.LenBytes())
    /// <summary>
    /// Return the length of each string as the number of characters.
    /// </summary>
    member _.LenChars() = apply (fun e -> e.Str.LenChars())
    /// <summary>
    /// Reverses the order of the characters in each string.
    /// </summary>
    member _.Reverse() = apply (fun e -> e.Str.Reverse())

    /// <summary>
    /// Returns an expression that slices the string by an offset and a length.
    /// </summary>
    /// <param name="offset">Start index.</param>
    /// <param name="length">Length of the slice.</param>
    member _.Slice(offset: int64, ?length: uint64) =
        apply (fun e -> e.Str.Slice(offset, ?length = length))

    /// <summary>
    /// Replace the first occurrence of a substring or regex pattern with a new value.
    /// </summary>
    /// <param name="pattern">Pattern to search for. Can be a literal string or a regex pattern.</param>
    /// <param name="value">String to replace the pattern with.</param>
    /// <param name="literal">If true, treat pattern as a literal string (not a regex).</param>
    /// <param name="n">Maximum number of replacements (default is 1).</param>
    member _.Replace(pattern: string, value: string, ?literal: bool, ?n: int) =
        apply (fun e -> e.Str.Replace(pattern, value, ?literal = literal, ?n = n))

    /// <summary>
    /// Replace all occurrences of a pattern with a new value.
    /// </summary>
    /// <param name="pattern">Pattern to search for. Can be a literal string or a regex pattern.</param>
    /// <param name="value">String to replace the pattern with.</param>
    /// <param name="literal">If true, treat pattern as a literal string (not a regex).</param>
    member _.ReplaceAll(pattern: string, value: string, ?literal: bool) =
        apply (fun e -> e.Str.ReplaceAll(pattern, value, ?literal = literal))

    /// <summary>
    /// Replace multiple patterns with different replacement values.
    /// </summary>
    /// <param name="patterns">Expression yielding string patterns to search and replace.</param>
    /// <param name="replaceWith">Strings to replace where a pattern was a match. Length must match the length of patterns or have length 1. This can be broadcasted, so it supports many:one and many:many.</param>
    /// <param name="asciiCaseInsensitive">Enable ASCII-aware case-insensitive matching.</param>
    /// <param name="leftmost">Guarantees in case there are overlapping matches that the leftmost match is used.</param>
    member _.ReplaceMany(patterns: Expr, replaceWith: Expr, ?asciiCaseInsensitive, ?leftmost) =
        apply (fun e ->
            e.Str.ReplaceMany(
                patterns,
                replaceWith,
                ?asciiCaseInsensitive = asciiCaseInsensitive,
                ?leftmost = leftmost
            ))

    /// <summary>
    /// Extract the target capture group from a regex pattern.
    /// </summary>
    /// <param name="pattern">Regex pattern with capture groups.</param>
    /// <param name="groupIndex">The index of the capture group to extract (default is 1).</param>
    member _.Extract(pattern: string, ?groupIndex: int) =
        apply (fun e -> e.Str.Extract(pattern, ?groupIndex = groupIndex))

    /// <summary>
    /// Extracts all successive non-overlapping regex matches in an individual string as a list.
    /// </summary>
    /// <param name="patterns">String patterns to search. This method supports matching on string literals only, and does not support regular expression matching.</param>
    /// <param name="asciiCaseInsensitive">Enable ASCII-aware case-insensitive matching.</param>
    /// <param name="overlapping">Whether matches may overlap.</param>
    /// <param name="leftmost">Guarantees in case there are overlapping matches that the leftmost match is used.</param>
    member _.ExtractMany(patterns: Expr, ?asciiCaseInsensitive, ?overlapping, ?leftmost) =
        apply (fun e ->
            e.Str.ExtractMany(
                patterns,
                ?asciiCaseInsensitive = asciiCaseInsensitive,
                ?overlapping = overlapping,
                ?leftmost = leftmost
            ))

    /// <summary>
    ///Extracts all matches for the given regex pattern.
    /// </summary>
    /// <param name="pattern">A valid regular expression pattern, compatible with the regex crate.</param>
    /// <returns>
    /// An expression that, for each row, returns a list of all non-overlapping matches of the given regex pattern.
    /// If no match is found, an empty list is returned.
    /// If a row contains null, null is returned.
    /// </returns>
    /// <exception cref="Exception">
    /// Throws an exception if the regex pattern is invalid.
    /// </exception>
    member _.ExtractGroups(pattern: string) =
        apply (fun e -> e.Str.ExtractGroups pattern)

    /// <summary>
    /// Checks if the string contains the given pattern.
    /// </summary>
    /// <param name="pattern">The pattern to search for. Can be a literal string or a regex pattern.</param>
    /// <param name="literal">If true, treat the pattern as a literal string (not a regex).</param>
    /// <param name="strict">If true, throw an error if the pattern is not found.</param>
    /// <returns>An expression that returns a boolean indicating whether the pattern was found.</returns>
    /// <exception cref="PolarsException">
    /// Throws an exception if `strict` is true and the pattern is not found in any row.
    /// </exception>
    member _.Contains(pattern: string, ?literal: bool, ?strict: bool) =
        apply (fun e -> e.Str.Contains(pattern, ?literal = literal, ?strict = strict))

    /// <summary>
    /// Checks if the strings contain any of the given patterns.
    /// </summary>
    /// <param name="pattern">The pattern to search for. This is treated as a regex pattern.</param>
    /// <param name="asciiCaseInsensitive">If true, perform case-insensitive matching for ASCII characters.</param>
    /// <returns>An expression that returns a boolean indicating whether any of the patterns were found.</returns>
    member _.ContainsAny(pattern, ?asciiCaseInsensitive) =
        apply (fun e -> e.Str.ContainsAny(pattern, ?asciiCaseInsensitive = asciiCaseInsensitive))

    /// <summary>
    /// Splits the string by separator and returns a List column.
    /// </summary>
    /// <param name="separator">The separator to split by.</param>
    /// <param name="inclusive">If true, include the separator in the result.</param>
    /// <param name="literal">If true, treat the separator as a literal string (not a regex).</param>
    /// <param name="strict">If true, throw an error if the separator is not found.</param>
    /// <returns>An expression that returns a List column of strings.</returns>
    member _.Split(separator: string, ?inclusive: bool, ?literal: bool, ?strict: bool) =
        apply (fun e -> e.Str.Split(separator, ?inclusive = inclusive, ?literal = literal, ?strict = strict))

    /// <summary>
    /// Split the string by a separator.
    /// </summary>
    /// <param name="separator">The separator to split by.</param>
    /// <param name="n">The maximum number of splits to perform.</param>
    /// <returns>An expression that splits the string by the separator.</returns>
    member _.SplitN(separator: string, n: int) =
        apply (fun e -> e.Str.SplitN(separator, n))

    /// <summary>
    /// Split the string by a separator into a fixed number of pieces.
    /// </summary>
    /// <param name="separator">The separator to split by.</param>
    /// <param name="n">The number of pieces to split into.</param>
    /// <param name="inclusive">If true, include the separator in the resulting pieces.</param>
    /// <returns>An expression that splits the string by the separator into a fixed number of pieces.</returns>
    member _.SplitExact(separator: string, n: int, ?inclusive: bool) =
        apply (fun e -> e.Str.SplitExact(separator, n, ?inclusive = inclusive))

    /// <summary>
    /// Return the bytes offset of the first substring matching a pattern.
    /// If the pattern is not found, returns None.
    /// </summary>
    /// <param name="pattern">A valid regular expression pattern, compatible with the regex crate.</param>
    /// <param name="literal">Treat pattern as a literal string, not as a regular expression.</param>
    /// <param name="strict">Raise an error if the underlying pattern is not a valid regex,
    /// otherwise mask out with a null value.</param>
    member _.Find(pattern, ?literal, ?strict) =
        apply (fun e -> e.Str.Find(pattern, ?literal = literal, ?strict = strict))

    /// <summary>
    /// Return the bytes offset of all non-overlapping substrings matching a pattern.
    /// If the pattern is not found, an empty list is returned.
    /// </summary>
    /// <param name="patterns">An expression yielding string patterns to search.
    /// This method supports matching on string literals only, and does not support regular expression matching.</param>
    /// <param name="asciiCaseInsensitive">Enable ASCII-aware case-insensitive matching. When this option is enabled, searching will be performed without respect to case for ASCII letters (a-z and A-Z) only.</param>
    /// <param name="overlapping">Whether matches may overlap. This is only supported when the search pattern contains a wildcard.</param>
    /// <param name="leftmost">Guarantees in case there are overlapping matches that the leftmost match is used.
    /// In case there are multiple candidates for the leftmost match the pattern which comes first in patterns is used.
    /// May not be used together with overlapping = True.</param>
    member _.FindMany(patterns, ?asciiCaseInsensitive, ?overlapping, ?leftmost) =
        apply (fun e ->
            e.Str.FindMany(
                patterns,
                ?asciiCaseInsensitive = asciiCaseInsensitive,
                ?overlapping = overlapping,
                ?leftmost = leftmost
            ))

    /// <summary>
    /// Counts the number of non-overlapping matches of the pattern in the string.
    /// </summary>
    /// <param name="pattern">The pattern to search for. Can be a literal string or a regex pattern.</param>
    /// <param name="literal">If true, treat the pattern as a literal string (not a regex).</param>
    /// <returns>An expression that returns the count of matches.</returns>
    member _.CountMatches(pattern, ?literal) =
        apply (fun e -> e.Str.CountMatches(pattern, ?literal = literal))

    /// <summary> Remove leading and trailing characters. </summary>
    member _.StripChars(?characters: string) =
        apply (fun e -> e.Str.StripChars(?characters = characters))

    /// <summary> Remove leading characters. </summary>
    member _.StripCharsStart(?characters: string) =
        apply (fun e -> e.Str.StripCharsStart(?characters = characters))

    /// <summary> Remove trailing characters. </summary>
    member _.StripCharsEnd(?characters: string) =
        apply (fun e -> e.Str.StripCharsEnd(?characters = characters))

    /// <summary>
    /// Remove the given prefix from the start of the string.
    /// </summary>
    /// <param name="prefix">The prefix to remove.</param>
    /// <returns>An expression that removes the prefix from the string.</returns>
    member _.StripPrefix(prefix: string) =
        apply (fun e -> e.Str.StripPrefix prefix)

    /// <summary>
    /// Remove the given suffix from the end of the string.
    /// </summary>
    /// <param name="suffix">The suffix to remove.</param>
    /// <returns>An expression that removes the suffix from the string.</returns>
    member _.StripSuffix(suffix: string) =
        apply (fun e -> e.Str.StripSuffix suffix)

    /// <summary>
    /// Check if the string starts with the given prefix.
    /// </summary>
    /// <param name="prefix">The prefix to check for.</param>
    /// <returns>An expression that returns true if the string starts with the prefix, false otherwise.</returns>
    member _.StartsWith(prefix: string) =
        apply (fun e -> e.Str.StartsWith prefix)

    /// <summary>
    /// Check if the string ends with the given suffix.
    /// </summary>
    /// <param name="suffix">The suffix to check for.</param>
    /// <returns>An expression that returns true if the string ends with the suffix, false otherwise.</returns>
    member _.EndsWith(suffix: string) = apply (fun e -> e.Str.EndsWith suffix)

    /// <summary>
    /// Return the first n characters of the string.
    /// </summary>
    /// <param name="n">The number of characters to return.</param>
    /// <returns>An expression that returns the first n characters of the string.</returns>
    member _.Head(n) = apply (fun e -> e.Str.Head n)

    /// <summary>
    /// Return the last n characters of the string.
    /// </summary>
    /// <param name="n">The number of characters to return.</param>
    /// <returns>An expression that returns the last n characters of the string.</returns>
    member _.Tail(n) = apply (fun e -> e.Str.Tail n)

    /// <summary>
    /// Extracts all matches for the given JSONPath expression.
    /// </summary>
    /// <param name="jsonPath">The JSONPath expression to use for extraction.</param>
    /// <returns>An expression that returns a list of all matches for the given JSONPath expression.</returns>
    member _.JsonPathMatch(jsonPath) =
        apply (fun e -> e.Str.JsonPathMatch jsonPath)

    /// <summary>
    /// Decode a String Series with JSON data into a structured Polars Series (e.g. List, Dict).
    /// </summary>
    /// <param name="dtype">The target DataType to decode the JSON into. This must be a structured type (e.g. DataType.List, DataType.Dict) as JSON decoding is not supported for primitive types like DataType.String or DataType.Int64.</param>
    /// <returns>A new Series with the decoded JSON data.</returns>
    /// <exception cref="PolarsException">
    /// Throws an exception if the target DataType is not a structured type, or if JSON decoding fails for any element.
    /// </exception>
    member _.JsonDecode(dtype: DataType) = apply (fun e -> e.Str.JsonDecode dtype)

    /// <summary>
    /// Pad strings with leading zeros until they reach the specified length.
    /// </summary>
    /// <param name="length">The target length of the strings.</param>
    /// <returns>An expression that pads the strings with leading zeros.</returns>
    member _.Zfill(length) = apply (fun e -> e.Str.Zfill length)

    /// <summary>
    /// Pad strings to a certain length by adding characters to the start.
    /// </summary>
    /// <param name="length">The target length of the strings.</param>
    /// <param name="fillChar">The character to pad with. Defaults to '0'.</param>
    /// <returns>An expression that pads the strings with leading characters.</returns>
    member _.PadStart(length, ?fillChar) =
        apply (fun e -> e.Str.PadStart(length, ?fillChar = fillChar))

    /// <summary>
    /// Pad strings to a certain length by adding characters to the end.
    /// </summary>
    /// <param name="length">The target length of the strings.</param>
    /// <param name="fillChar">The character to pad with. Defaults to '0'.</param>
    /// <returns>An expression that pads the strings with trailing characters.</returns>
    member _.PadEnd(length, ?fillChar) =
        apply (fun e -> e.Str.PadEnd(length, ?fillChar = fillChar))

    /// <summary>
    /// Join elements of a string series with a separator.
    /// </summary>
    /// <param name="delimiter">The separator to use. Defaults to an empty string.</param>
    /// <param name="ignoreNulls">Whether to ignore null values. Defaults to true.</param>
    /// <returns>An expression that joins the elements of the string series.</returns>
    member _.Join(?delimiter, ?ignoreNulls) =
        apply (fun e -> e.Str.Join(?delimiter = delimiter, ?ignoreNulls = ignoreNulls))

    /// <summary>
    /// Encode a String Series into a Bytes Series, using the specified encoding.
    /// </summary>
    /// <param name="encoding">The Charset encoding to use for the conversion.</param>
    /// <returns>An expression that converts the String Series to a Bytes Series.</returns>
    member _.Encode encoding = apply (fun e -> e.Str.Encode encoding)

    /// <summary>
    /// Decode a Bytes Series into a String Series, using the specified encoding.
    /// </summary>
    /// <param name="encoding">The Charset encoding to use for the conversion.</param>
    /// <param name="strict">If true, invalid bytes will raise an exception. If false, they will be replaced with the replacement character.</param>
    /// <returns>An expression that converts the Bytes Series to a String Series.</returns>
    member _.Decode(encoding, ?strict) =
        apply (fun e -> e.Str.Decode(encoding, ?strict = strict))

    /// <summary>
    /// Returns the Unicode normal form of the string values.
    /// </summary>
    /// <param name="form">Unicode form to use.</param>
    member _.Normalize form = apply (fun e -> e.Str.Normalize form)

    /// <summary> Parse string to Date. </summary>
    member _.ToDate(?format: string, ?strict: bool, ?exact: bool, ?cache: bool) =
        apply (fun e -> e.Str.ToDate(?format = format, ?strict = strict, ?exact = exact, ?cache = cache))

    /// <summary>
    /// Parse string to Time.
    /// </summary>
    /// <param name="format">Date format.</param>
    /// <param name="strict">Return null when parsing fails.</param>
    /// <param name="exact">Return null when parsing fails.</param>
    /// <param name="cache">Use cache.</param>
    /// <returns>Series of Time.</returns>
    member _.ToTime(?format: string, ?strict: bool, ?exact: bool, ?cache: bool) =
        apply (fun e -> e.Str.ToTime(?format = format, ?strict = strict, ?exact = exact, ?cache = cache))

    /// <summary>
    /// Parse string to Datetime.
    /// </summary>
    /// <param name="format">Date format.</param>
    /// <param name="timeUnit">Time unit.</param>
    /// <param name="timeZone">Time zone.</param>
    /// <param name="strict">Return null when parsing fails.</param>
    /// <param name="exact">Return null when parsing fails.</param>
    /// <param name="cache">Use cache.</param>
    /// <param name="ambiguous">Ambiguous dates.</param>
    /// <returns>Series of Datetime.</returns>
    member _.ToDatetime
        (
            ?format: string,
            ?timeUnit: TimeUnit,
            ?timeZone: string,
            ?strict: bool,
            ?exact: bool,
            ?cache: bool,
            ?ambiguous: Expr
        ) =
        apply (fun e ->
            e.Str.ToDatetime(
                ?format = format,
                ?timeUnit = timeUnit,
                ?timeZone = timeZone,
                ?strict = strict,
                ?exact = exact,
                ?cache = cache,
                ?ambiguous = ambiguous
            ))

    /// <summary>
    /// Parse string to datetime with given format.
    /// </summary>
    /// <param name="dtype">Data type.</param>
    /// <param name="format">Date format.</param>
    /// <param name="strict">Return null when parsing fails.</param>
    /// <param name="exact">Return null when parsing fails.</param>
    /// <param name="cache">Use cache.</param>
    /// <param name="ambiguous">Ambiguous dates.</param>
    /// <returns>Series of Datetime.</returns>
    member _.Strptime(dtype: DataType, ?format: string, ?strict: bool, ?exact: bool, ?cache: bool, ?ambiguous: Expr) =
        apply (fun e ->
            e.Str.Strptime(
                dtype,
                ?format = format,
                ?strict = strict,
                ?exact = exact,
                ?cache = cache,
                ?ambiguous = ambiguous
            ))

    /// <summary>
    /// Convert string column to decimal.
    /// </summary>
    /// <param name="scale">Scale of the decimal.</param>
    /// <returns>Series of decimal.</returns>
    member _.ToDecimal(scale) = apply (fun e -> e.Str.ToDecimal scale)

    /// <summary>
    /// Convert string column to integer.
    /// </summary>
    /// <param name="radix">Radix of the integer.</param>
    /// <param name="dtype">Data type of the integer.</param>
    /// <param name="strict">Return null when parsing fails.</param>
    /// <returns>Series of integer.</returns>
    member _.ToInteger(?radix, ?dtype, ?strict) =
        apply (fun e -> e.Str.ToInteger(?radix = radix, ?dtype = dtype, ?strict = strict))
