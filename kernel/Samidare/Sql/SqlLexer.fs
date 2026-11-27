namespace Kaminari.Samidare.Sql

open System
open System.Buffers
open System.Collections.Frozen
open System.Globalization

[<RequireQualifiedAccess>]
type internal SqlTokenKind =
    /// An unquoted word: a keyword or a name.
    | Word
    | QuotedIdentifier
    | Integer
    | Decimal
    | Float
    | String
    /// A number with Spark's type suffix (`10L`, `1.5D`, `2BD`): its text as written.
    | SuffixedNumber
    /// Punctuation or an operator, its text in `Text`.
    | Symbol
    | End

/// A token: for a word or a number its text as written, for a quoted name or a string its value.
[<Struct>]
type internal SqlToken = { Kind: SqlTokenKind; Text: string; Span: SqlSpan }

module internal SqlLexer =

    /// Words that never name a column or table unquoted, so a word after an expression or a table is an alias
    /// only when it is not one of them (`FROM t LEFT JOIN u`, `SELECT x FROM`).
    let reserved: FrozenSet<string> =
        FrozenSet.ToFrozenSet(
            [ "ALL"; "AND"; "AS"; "BETWEEN"; "BY"; "CASE"; "CAST"; "CROSS"; "DISTINCT"; "ELSE"; "END"; "ESCAPE"
              "EXCEPT"; "EXISTS"; "FALSE"; "FETCH"; "FROM"; "FULL"; "GROUP"; "HAVING"; "IN"; "INNER"; "INTERSECT"; "IS"
              "JOIN"; "LATERAL"; "LEFT"; "LIKE"; "LIMIT"; "NATURAL"; "NOT"; "NULL"; "OFFSET"; "ON"; "OR"; "ORDER"
              "OUTER"; "OVER"; "QUALIFY"; "RIGHT"; "SELECT"; "THEN"; "TRUE"; "TRY_CAST"; "UNION"; "USING"; "WHEN"
              "WHERE"; "WINDOW"; "WITH" ],
            StringComparer.OrdinalIgnoreCase)

    let isReserved (word: string) = reserved.Contains word

    let private isWordStart (c: char) = Char.IsLetter c || c = '_'
    let private isWordPart (c: char) = Char.IsLetterOrDigit c || c = '_' || c = '$'

    let private singles = "(),.;*+-/%=<>[]{}:"
    let private commentMarks = SearchValues.Create("*/")

    /// The two-character operator at `i`, or null.
    let private pairAt (dialect: SqlDialect) (text: string) (i: int) =
        if i + 1 >= text.Length then null
        else
            match text.[i], text.[i + 1] with
            | '<', '=' when dialect.IsSpark && i + 2 < text.Length && text.[i + 2] = '>' -> "<=>"
            | '<', '>' -> "<>"
            | '!', '=' -> "!="
            | '<', '=' -> "<="
            | '>', '=' -> ">="
            | '|', '|' -> "||"
            | '=', '=' when dialect.IsSpark -> "=="
            | _ -> null

    /// Spark's numeric literal suffixes: the type a number with one has.
    let private suffixes = [ "BD"; "Y"; "S"; "L"; "D"; "F" ]

    let private isOctal (c: char) = c >= '0' && c <= '7'

    let tokenize (dialect: SqlDialect) (text: string) : SqlToken array =
        let tokens = ResizeArray<SqlToken>(text.Length / 4 + 1)
        let fail (reason: string) (start: int) (length: int) (suggestion: string option) =
            raise (SqlException(reason, text, { Start = start; Length = length }, suggestion))
        let add kind value start stop = tokens.Add { Kind = kind; Text = value; Span = { Start = start; Length = stop - start } }
        let digitsFrom (i: int) =
            let mutable j = i
            while j < text.Length && Char.IsAsciiDigit text.[j] do
                j <- j + 1
            j
        /// A quoted text from `start` (its opening quote) whose quote doubles inside: its value and its end.
        let quoted (start: int) (quote: char) (what: string) =
            let builder = Text.StringBuilder()
            let mutable i = start + 1
            let mutable closed = false
            while not closed do
                match text.IndexOf(quote, i) with
                | -1 -> fail $"Unterminated {what}: no closing {quote}" start 1 (Some $"Close it with {quote}; write {quote}{quote} for a {quote} inside it.")
                | close when close + 1 < text.Length && text.[close + 1] = quote ->
                    builder.Append(text, i, close + 1 - i) |> ignore
                    i <- close + 2
                | close ->
                    builder.Append(text, i, close - i) |> ignore
                    i <- close + 1
                    closed <- true
            builder.ToString(), i
        /// A Spark string from `start` (its opening quote): a doubled quote is one quote, and backslash escapes are read
        /// as Spark reads them: `\ddd` octal, `\uXXXX`, `\UXXXXXXXX`, `\0 \b \n \r \t \Z`, `\%` and `\_` kept whole
        /// (LIKE's escapes), any other character as itself.
        let sparkString (start: int) (quote: char) =
            let builder = Text.StringBuilder()
            let hexAt (from: int) (count: int) =
                if from + count > text.Length then None
                else
                    match Int32.TryParse(text.AsSpan(from, count), NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture) with
                    | true, v -> Some v
                    | _ -> None
            let mutable i = start + 1
            let mutable closed = false
            while not closed do
                if i >= text.Length then
                    fail $"Unterminated string: no closing {quote}" start 1 (Some $"Close it with {quote}; write \\{quote} or {quote}{quote} for a {quote} inside it.")
                let c = text.[i]
                if c = quote then
                    if i + 1 < text.Length && text.[i + 1] = quote then
                        builder.Append quote |> ignore
                        i <- i + 2
                    else
                        closed <- true
                        i <- i + 1
                elif c = '\\' && i + 1 < text.Length then
                    let n = text.[i + 1]
                    if i + 3 < text.Length && n >= '0' && n <= '3' && isOctal text.[i + 2] && isOctal text.[i + 3] then
                        builder.Append(char ((int n - int '0') * 64 + (int text.[i + 2] - int '0') * 8 + (int text.[i + 3] - int '0'))) |> ignore
                        i <- i + 4
                    else
                        match n, hexAt (i + 2) 4, hexAt (i + 2) 8 with
                        | 'u', Some code, _ ->
                            builder.Append(char code) |> ignore
                            i <- i + 6
                        | 'U', _, Some code when code <= 0x10FFFF && (code < 0xD800 || code > 0xDFFF) ->
                            builder.Append(Char.ConvertFromUtf32 code) |> ignore
                            i <- i + 10
                        | _ ->
                            match n with
                            | '0' -> builder.Append '\000' |> ignore
                            | 'b' -> builder.Append '\b' |> ignore
                            | 'n' -> builder.Append '\n' |> ignore
                            | 'r' -> builder.Append '\r' |> ignore
                            | 't' -> builder.Append '\t' |> ignore
                            | 'Z' -> builder.Append '\u001A' |> ignore
                            | '%' | '_' -> builder.Append('\\').Append(n) |> ignore
                            | other -> builder.Append other |> ignore
                            i <- i + 2
                else
                    builder.Append c |> ignore
                    i <- i + 1
            builder.ToString(), i
        /// A Spark raw string (`r'...'`) from its quote: no escapes, up to the next quote.
        let rawString (quoteAt: int) =
            let quote = text.[quoteAt]
            match text.IndexOf(quote, quoteAt + 1) with
            | -1 -> fail $"Unterminated string: no closing {quote}" (quoteAt - 1) 2 None
            | close -> text.Substring(quoteAt + 1, close - quoteAt - 1), close + 1
        let mutable i = 0
        while i < text.Length do
            let c = text.[i]
            if Char.IsWhiteSpace c then
                i <- i + 1
            elif c = '-' && i + 1 < text.Length && text.[i + 1] = '-' then
                i <- match text.IndexOf('\n', i) with -1 -> text.Length | n -> n + 1
            elif c = '/' && i + 1 < text.Length && text.[i + 1] = '*' then
                // Bracketed comments nest (ISO SQL).
                let start = i
                let mutable depth = 1
                i <- i + 2
                while depth > 0 do
                    match text.AsSpan(i).IndexOfAny(commentMarks) with
                    | -1 -> fail "Unterminated comment: no closing */" start 2 None
                    | relative ->
                        let j = i + relative
                        if text.[j] = '*' && j + 1 < text.Length && text.[j + 1] = '/' then
                            depth <- depth - 1
                            i <- j + 2
                        elif text.[j] = '/' && j + 1 < text.Length && text.[j + 1] = '*' then
                            depth <- depth + 1
                            i <- j + 2
                        else i <- j + 1
            elif dialect.IsSpark && (c = 'r' || c = 'R') && i + 1 < text.Length && (text.[i + 1] = '\'' || text.[i + 1] = '"') then
                let value, stop = rawString (i + 1)
                add SqlTokenKind.String value i stop
                i <- stop
            elif isWordStart c then
                let start = i
                i <- i + 1
                while i < text.Length && isWordPart text.[i] do
                    i <- i + 1
                add SqlTokenKind.Word (text.Substring(start, i - start)) start i
            elif dialect.IsSpark && (c = '\'' || c = '"') then
                let value, stop = sparkString i c
                add SqlTokenKind.String value i stop
                i <- stop
            elif c = '"' || (c = '`' && dialect.IsSpark) then
                let value, stop = quoted i c "quoted name"
                if value.Length = 0 then fail "A quoted name cannot be empty" i (stop - i) None
                add SqlTokenKind.QuotedIdentifier value i stop
                i <- stop
            elif c = '\'' then
                let value, stop = quoted i '\'' "string"
                add SqlTokenKind.String value i stop
                i <- stop
            elif Char.IsAsciiDigit c || (c = '.' && i + 1 < text.Length && Char.IsAsciiDigit text.[i + 1]) then
                let start = i
                let mutable kind = SqlTokenKind.Integer
                i <- digitsFrom i
                if i < text.Length && text.[i] = '.' then
                    kind <- SqlTokenKind.Decimal
                    i <- digitsFrom (i + 1)
                if i < text.Length && (text.[i] = 'e' || text.[i] = 'E') then
                    let exponent = if i + 1 < text.Length && (text.[i + 1] = '+' || text.[i + 1] = '-') then i + 2 else i + 1
                    if exponent < text.Length && Char.IsAsciiDigit text.[exponent] then
                        kind <- SqlTokenKind.Float
                        i <- digitsFrom exponent
                let suffix =
                    if not dialect.IsSpark then None
                    else
                        suffixes
                        |> List.tryFind (fun x ->
                            i + x.Length <= text.Length
                            && String.Compare(text, i, x, 0, x.Length, StringComparison.OrdinalIgnoreCase) = 0
                            && (i + x.Length = text.Length || not (isWordPart text.[i + x.Length]))
                            // Integral suffixes only after digits alone.
                            && (kind = SqlTokenKind.Integer || not (x = "Y" || x = "S" || x = "L")))
                match suffix with
                | Some x ->
                    i <- i + x.Length
                    kind <- SqlTokenKind.SuffixedNumber
                | None -> ()
                if i < text.Length && isWordStart text.[i] then
                    fail $"A number cannot run into a name: '{text.Substring(start, i + 1 - start)}'" start (i + 1 - start)
                        (Some "Put a space between them.")
                add kind (text.Substring(start, i - start)) start i
            else
                match pairAt dialect text i with
                | null when dialect.IsSpark && c = ':' && i + 1 < text.Length && text.[i + 1] = ':' ->
                    fail "The :: cast is not supported" i 2 (Some "Write CAST(value AS type).")
                | null when singles.Contains c || (c = '!' && dialect.IsSpark) ->
                    add SqlTokenKind.Symbol (string c) i (i + 1)
                    i <- i + 1
                | null when dialect.IsSpark && (c = '&' || c = '|' || c = '^' || c = '~') ->
                    fail $"Bitwise operators are not supported: '{c}'" i 1 None
                | null ->
                    let hint = if c = '`' then Some "Quote names with double quotes: \"name\"." else None
                    fail $"Unexpected character '{c}'" i 1 hint
                | pair ->
                    add SqlTokenKind.Symbol pair i (i + pair.Length)
                    i <- i + pair.Length
        tokens.Add { Kind = SqlTokenKind.End; Text = ""; Span = { Start = text.Length; Length = 0 } }
        tokens.ToArray()
