namespace Kaminari.Samidare.Sql

open System
open System.Text

/// A range of a SQL text: its first character's offset and its length, in UTF-16 code units.
[<Struct>]
type SqlSpan =
    { Start: int
      Length: int }

    member this.End = this.Start + this.Length

    /// The span from the start of `first` to the end of `last`.
    static member Cover(first: SqlSpan, last: SqlSpan) = { Start = first.Start; Length = max first.Length (last.End - first.Start) }

/// A SQL text that cannot be read: the message names what was expected and what was found, at a line and a column
/// (both from 1), with the line quoted and the place marked, and a suggestion where one helps.
type SqlException(reason: string, text: string, span: SqlSpan, suggestion: string option) =
    inherit Exception(SqlException.Format(reason, text, span, suggestion))

    /// The problem alone, without its place.
    member _.Reason = reason
    member _.Span = span
    member _.Suggestion = suggestion
    member _.Line = fst (SqlException.Position(text, span.Start))
    member _.Column = snd (SqlException.Position(text, span.Start))

    /// The line and column (both from 1) of an offset.
    static member Position(text: string, offset: int) : int * int =
        let offset = min (max offset 0) text.Length
        let mutable line = 1
        let mutable lineStart = 0
        for i in 0 .. offset - 1 do
            if text.[i] = '\n' then
                line <- line + 1
                lineStart <- i + 1
        line, offset - lineStart + 1

    static member private Format(reason: string, text: string, span: SqlSpan, suggestion: string option) =
        let line, column = SqlException.Position(text, span.Start)
        let start = min (max span.Start 0) text.Length
        let lineStart = if start = 0 then 0 else text.LastIndexOf('\n', start - 1) + 1
        let lineEnd = match text.IndexOf('\n', start) with -1 -> text.Length | i -> i
        let source = text.Substring(lineStart, lineEnd - lineStart).TrimEnd('\r').Replace('\t', ' ')
        let gutter = string line
        let width = max 1 (min span.Length (lineEnd - start))
        let builder = StringBuilder()
        builder.Append($"{reason} (line {line}, column {column})").AppendLine() |> ignore
        builder.Append($"  {gutter} | {source}").AppendLine() |> ignore
        builder.Append($"  {String(' ', gutter.Length)} | {String(' ', start - lineStart)}{String('^', width)}") |> ignore
        suggestion |> Option.iter (fun s -> builder.AppendLine().Append(s) |> ignore)
        builder.ToString()
