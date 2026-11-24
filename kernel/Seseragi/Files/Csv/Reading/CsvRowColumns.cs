using System.Buffers.Text;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Builders;
using Kaminari.Sazanami.Marshalling;
using Kaminari.Sazanami.Text;

namespace Kaminari.Seseragi.Files.Csv.Reading;

/// <summary>
/// The columns rows of a type are read with beyond CSV's own types (<see cref="CsvRowSchema{T}"/> marks them): GUIDs
/// from their text, enums from their members' names or numbers, durations, half-precision floats.
/// </summary>
internal static class CsvRowColumns
{
    /// <summary>The metadata key of a binary column of GUIDs (16 bytes each, as <see cref="Guid.TryWriteBytes(Span{byte})"/> writes them), written and read as text.</summary>
    public const string GuidKey = "kaminari.csv.guid";

    /// <summary>The metadata key of a string column of an enum's member names, the members as <see cref="EnumNameLookup.Members"/> lists them.</summary>
    public const string EnumKey = "kaminari.csv.enum";

    /// <summary>The metadata key of a decimal column written with the fewest digits that hold each value exactly (no trailing zeros).</summary>
    public const string ShortestKey = "kaminari.csv.shortest";
}

/// <summary>GUIDs from their text (32 hex digits, hyphenated or not, braced or in parentheses), as 16 bytes.</summary>
internal sealed class GuidColumn(NativeColumnBuilder b, string name, CsvValueRules rules) : CsvColumn(b, name, rules)
{
    private readonly IBytesBuilder _b = (IBytesBuilder)b;

    public override int Decode(ReadOnlySpan<byte> unit, ReadOnlySpan<CsvField> fields, int count, out CsvValueError? error)
    {
        error = null;
        Span<byte> bytes = stackalloc byte[16];
        for (int r = 0; r < count; r++)
        {
            if (!Text(unit, fields[r], r, out ReadOnlySpan<byte> text))
            {
                Builder.AppendNull();
                continue;
            }
            if (TryParse(Trim(text), out Guid value))
            {
                value.TryWriteBytes(bytes);
                _b.Append(bytes);
            }
            else if (!Refuse(text, "{value} is not a GUID (such as 0f8fad5b-d9cb-469f-a165-70867728950e)", out error, hint: false)) return r;
        }
        return -1;
    }

    private static bool TryParse(ReadOnlySpan<byte> text, out Guid value)
    {
        value = default;
        char format = text.Length switch { 36 => 'D', 32 => 'N', 38 => text[0] == '{' ? 'B' : 'P', _ => '\0' };
        return format != '\0' && Utf8Parser.TryParse(text, out value, out int used, format) && used == text.Length;
    }

    private static ReadOnlySpan<byte> Trim(ReadOnlySpan<byte> text) => text.Trim(" \t"u8);
}

/// <summary>
/// An enum's member names (a string column marked <see cref="CsvRowColumns.EnumKey"/>): a member's name (exactly, else
/// ignoring case) or number, written as the member's name, so that the column holds member names exactly as the rows
/// read them and readers' filters compare them.
/// </summary>
internal sealed class EnumNameColumn(NativeColumnBuilder b, string name, CsvValueRules rules, string members) : CsvColumn(b, name, rules)
{
    private readonly IBytesBuilder _b = (IBytesBuilder)b;
    private readonly EnumNameLookup _members = new(members);

    public override int Decode(ReadOnlySpan<byte> unit, ReadOnlySpan<CsvField> fields, int count, out CsvValueError? error)
    {
        error = null;
        for (int r = 0; r < count; r++)
        {
            if (!Text(unit, fields[r], r, out ReadOnlySpan<byte> text))
            {
                Builder.AppendNull();
                continue;
            }
            int member = _members.Find(text.Trim(" \t"u8));
            if (member >= 0) _b.Append(_members.Name(member));
            else if (!Refuse(text, "{value} is not a member of the enum (read from a member's name, in any case, or its number)", out error, hint: false)) return r;
        }
        return -1;
    }

    public override void Dispose()
    {
        _members.Dispose();
        base.Dispose();
    }
}

/// <summary>Durations: ISO 8601 (<c>PT1.5S</c>, as the writer writes them) or <c>[-][d.]hh:mm:ss[.f]</c>, in the column's unit.</summary>
internal sealed class DurationColumn(NativeColumnBuilder b, string name, CsvValueRules rules, TimeUnit timeUnit) : CsvColumn(b, name, rules)
{
    private readonly NativeFixedWidthBuilder<long> _b = (NativeFixedWidthBuilder<long>)b;

    public override int Decode(ReadOnlySpan<byte> unit, ReadOnlySpan<CsvField> fields, int count, out CsvValueError? error)
    {
        error = null;
        long divisor = timeUnit switch { TimeUnit.Second => 1_000_000_000, TimeUnit.Millisecond => 1_000_000, TimeUnit.Microsecond => 1000, _ => 1 };
        for (int r = 0; r < count; r++)
        {
            if (!Text(unit, fields[r], r, out ReadOnlySpan<byte> text))
            {
                _b.AppendNull();
                continue;
            }
            ReadOnlySpan<byte> trimmed = text.Trim(" \t"u8);
            if (!(IsoTemporal.TryParseDuration(trimmed, out Int128 nanos) || TryClock(trimmed, out nanos)))
            {
                if (!Refuse(text, "{value} is not a duration (ISO 8601 such as PT1.5S, or [-][d.]hh:mm:ss[.f])", out error, hint: false)) return r;
                continue;
            }
            Int128 value = nanos / divisor;
            if (nanos % divisor != 0)
            {
                if (!Refuse(text, $"{{value}} is more precise than the column's unit ({DatetimeColumn.UnitName(timeUnit)}): change the given schema to a finer unit", out error, hint: false)) return r;
                continue;
            }
            if (value < long.MinValue || value > long.MaxValue)
            {
                if (!Refuse(text, "{value} is out of the range of the column's durations", out error, hint: false)) return r;
                continue;
            }
            _b.Append((long)value);
        }
        return -1;
    }

    private static bool TryClock(ReadOnlySpan<byte> text, out Int128 nanos)
    {
        nanos = 0;
        if (text.IndexOf((byte)':') < 0 || !Utf8Parser.TryParse(text, out TimeSpan span, out int used, 'c') || used != text.Length) return false;
        nanos = (Int128)span.Ticks * 100;
        return true;
    }
}

/// <summary>Half-precision floats, parsed as doubles and rounded to half precision.</summary>
internal sealed class HalfColumn(NativeColumnBuilder b, string name, CsvValueRules rules) : CsvColumn(b, name, rules)
{
    private readonly NativeFixedWidthBuilder<Half> _b = (NativeFixedWidthBuilder<Half>)b;

    public override int Decode(ReadOnlySpan<byte> unit, ReadOnlySpan<CsvField> fields, int count, out CsvValueError? error)
    {
        error = null;
        Span<byte> stack = stackalloc byte[StackBytes];
        for (int r = 0; r < count; r++)
        {
            if (!Text(unit, fields[r], r, out ReadOnlySpan<byte> text))
            {
                _b.AppendNull();
                continue;
            }
            if (Number(TrimStart(text), stack, out ReadOnlySpan<byte> number) && FastDouble.TryParse(number, out double value)) _b.Append((Half)value);
            else if (!Refuse(text, "{value} is not a floating-point number", out error)) return r;
        }
        return -1;
    }
}
