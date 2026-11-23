using System.Buffers;
using System.Globalization;
using System.Text;

namespace Kaminari.Sazanami.Text;

/// <summary>
/// The kind of value a CSV field holds, inferred as Polars' CSV reader (0.55, <c>infer_field_schema</c>) infers it from
/// the field's UTF-8 text: a text in double quotes is temporal or a string; else <c>(?i:true|false)</c> is a boolean,
/// <c>[-+]?(\d*\.\d+([eE][-+]?\d+)?|inf|NaN|\d+[eE][-+]?\d+|\d+\.)</c> a float (',' for '.' under a decimal comma),
/// <c>-?\d+</c> an Int64 when it parses as one, else an Int128 (whatever its size), then, when dates are parsed, a
/// temporal pattern (<see cref="TemporalPatterns.Infer(ReadOnlySpan{byte})"/>); everything else is a string. The
/// regexes are Unicode-aware as the engine's: <c>\d</c> is any decimal digit ("１２３" is an Int128) and the case of
/// "true"/"false" folds ('ſ' is an 's').
/// </summary>
internal static class ValueInference
{
    /// <summary>
    /// The kind <paramref name="text"/> holds; <paramref name="nanoseconds"/> tells a datetime whose fraction needs
    /// nanoseconds. A fraction finer than nanoseconds is a string (no unit holds it).
    /// </summary>
    public static TextKind Infer(ReadOnlySpan<byte> text, bool parseDates, bool decimalComma, out bool nanoseconds)
    {
        nanoseconds = false;
        if (text.Length >= 2 && text[0] == '"' && text[^1] == '"')
            return parseDates ? Temporal(text[1..^1], out nanoseconds) : TextKind.String;
        if (IsBoolean(text)) return TextKind.Boolean;
        if (IsFloat(text, decimalComma ? (byte)',' : (byte)'.')) return TextKind.Float64;
        if (IsInteger(text))
            return long.TryParse(text, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out _) ? TextKind.Int64 : TextKind.Int128;
        return parseDates ? Temporal(text, out nanoseconds) : TextKind.String;
    }

    private static TextKind Temporal(ReadOnlySpan<byte> text, out bool nanoseconds)
    {
        TemporalPattern? pattern = TemporalPatterns.Infer(text, out int fraction);
        nanoseconds = fraction == 1;
        return fraction == 2 ? TextKind.String : pattern switch
        {
            TemporalPattern.DateDmy or TemporalPattern.DateYmd => TextKind.Date,
            TemporalPattern.Time => TextKind.Time,
            TemporalPattern.DatetimeDmy or TemporalPattern.DatetimeYmd => TextKind.Datetime,
            TemporalPattern.DatetimeYmdZ => TextKind.DatetimeUtc,
            _ => TextKind.String,
        };
    }

    /// <summary><c>(?i:true|false)</c>, Unicode case folding.</summary>
    public static bool IsBoolean(ReadOnlySpan<byte> text) => Folds(text, "true"u8) || Folds(text, "false"u8);

    private static bool Folds(ReadOnlySpan<byte> text, ReadOnlySpan<byte> word)
    {
        int p = 0;
        foreach (byte letter in word)
        {
            if (p < text.Length && (text[p] | 0x20) == letter) p++;
            else if (letter == 's' && text[p..].StartsWith("ſ"u8)) p += 2; // LATIN SMALL LETTER LONG S folds to 's'
            else return false;
        }
        return p == text.Length;
    }

    /// <summary><c>^-?\d+$</c>, Unicode digits.</summary>
    public static bool IsInteger(ReadOnlySpan<byte> text)
    {
        int p = text.Length > 0 && text[0] == '-' ? 1 : 0;
        return Digits(text, ref p) > 0 && p == text.Length;
    }

    /// <summary>The float regex with <paramref name="point"/> as the decimal separator, Unicode digits.</summary>
    public static bool IsFloat(ReadOnlySpan<byte> text, byte point)
    {
        int p = text.Length > 0 && text[0] is (byte)'-' or (byte)'+' ? 1 : 0;
        if (text[p..].SequenceEqual("inf"u8) || text[p..].SequenceEqual("NaN"u8)) return true;
        int whole = Digits(text, ref p);
        if (p < text.Length && text[p] == point)
        {
            p++;
            int fraction = Digits(text, ref p);
            if (fraction == 0) return whole > 0 && p == text.Length; // "1."
            return p == text.Length || Exponent(text, p);
        }
        return whole > 0 && Exponent(text, p);

        static bool Exponent(ReadOnlySpan<byte> text, int p)
        {
            if (p == text.Length || text[p] is not ((byte)'e' or (byte)'E')) return false;
            p++;
            if (p < text.Length && text[p] is (byte)'-' or (byte)'+') p++;
            return Digits(text, ref p) > 0 && p == text.Length;
        }
    }

    /// <summary>Skips decimal digits (any script) from <paramref name="p"/>; returns how many.</summary>
    private static int Digits(ReadOnlySpan<byte> text, ref int p)
    {
        int count = 0;
        while (p < text.Length)
        {
            if (text[p] - (uint)'0' <= 9)
            {
                p++;
                count++;
                continue;
            }
            if (text[p] < 0x80 || Rune.DecodeFromUtf8(text[p..], out Rune rune, out int length) != OperationStatus.Done
                || Rune.GetUnicodeCategory(rune) != UnicodeCategory.DecimalDigitNumber) break;
            p += length;
            count++;
        }
        return count;
    }
}
