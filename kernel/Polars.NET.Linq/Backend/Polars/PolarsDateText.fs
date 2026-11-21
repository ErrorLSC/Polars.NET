namespace Polars.NET.Linq.Backend.Polars

open System
open System.Globalization
open System.Text

/// The engine's text of dates and times (strftime, chrono) for .NET custom formats (ScalarFunction.DtToText).
module internal PolarsDateText =

    /// A .NET custom date and time format of the culture named `cultureName` as a strftime pattern, or None where the
    /// engine cannot write it as .NET does: a calendar other than the Gregorian one (th-TH's Buddhist era, fa-IR's
    /// Persian calendar), names (months, days, AM/PM) other than the invariant culture's English ones, fractions of a
    /// second, eras, time zones, one-letter years.
    let tryStrftime (format: string, cultureName: string) : string option =
        let culture = CultureInfo.GetCultureInfo cultureName
        let dtfi = culture.DateTimeFormat
        let english = CultureInfo.InvariantCulture.DateTimeFormat
        let englishMonths = dtfi.MonthNames = english.MonthNames && dtfi.AbbreviatedMonthNames = english.AbbreviatedMonthNames
        let englishDays = dtfi.DayNames = english.DayNames && dtfi.AbbreviatedDayNames = english.AbbreviatedDayNames
        let englishAmPm = dtfi.AMDesignator = "AM" && dtfi.PMDesignator = "PM"
        let out = StringBuilder()
        let literal (text: string) = out.Append(text.Replace("%", "%%")) |> ignore
        let rec walk i =
            if i >= format.Length then
                Some(out.ToString())
            else
                let c = format.[i]
                let mutable run = 1
                while i + run < format.Length && format.[i + run] = c do
                    run <- run + 1
                let specifier (s: string) =
                    out.Append s |> ignore
                    walk (i + run)
                match c, run with
                | 'y', 2 -> specifier "%y"
                | 'y', 4 -> specifier "%Y"
                | 'M', 1 -> specifier "%-m"
                | 'M', 2 -> specifier "%m"
                | 'M', 3 when englishMonths -> specifier "%b"
                | 'M', 4 when englishMonths -> specifier "%B"
                | 'd', 1 -> specifier "%-d"
                | 'd', 2 -> specifier "%d"
                | 'd', 3 when englishDays -> specifier "%a"
                | 'd', 4 when englishDays -> specifier "%A"
                | 'H', 1 -> specifier "%-H"
                | 'H', 2 -> specifier "%H"
                | 'h', 1 -> specifier "%-I"
                | 'h', 2 -> specifier "%I"
                | 'm', 1 -> specifier "%-M"
                | 'm', 2 -> specifier "%M"
                | 's', 1 -> specifier "%-S"
                | 's', 2 -> specifier "%S"
                | 't', 2 when englishAmPm -> specifier "%p"
                | ('y' | 'M' | 'd' | 'H' | 'h' | 'm' | 's' | 't' | 'f' | 'F' | 'g' | 'z' | 'K'), _ -> None
                | '/', _ ->
                    literal (String.replicate run dtfi.DateSeparator)
                    walk (i + run)
                | ':', _ ->
                    literal (String.replicate run dtfi.TimeSeparator)
                    walk (i + run)
                | ('\'' | '"'), _ ->
                    match format.IndexOf(c, i + 1) with
                    | -1 -> None
                    | close ->
                        literal (format.Substring(i + 1, close - i - 1))
                        walk (close + 1)
                | '\\', _ when i + 1 < format.Length ->
                    literal (string format.[i + 1])
                    walk (i + 2)
                | _ ->
                    literal (String(c, run))
                    walk (i + run)
        if dtfi.Calendar :? GregorianCalendar then walk 0 else None
