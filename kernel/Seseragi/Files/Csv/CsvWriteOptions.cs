namespace Kaminari.Seseragi.Files.Csv;

/// <summary>How <see cref="CsvWriter"/> writes delimited text.</summary>
public sealed record CsvWriteOptions
{
    /// <summary>Whether the first line names the columns.</summary>
    public bool IncludeHeader { get; init; } = true;

    /// <summary>Whether the text starts with a byte order mark (spreadsheet programs take it as the sign of UTF-8).</summary>
    public bool IncludeBom { get; init; }

    /// <summary>The byte between fields (ASCII; not the quote, nor CR or LF).</summary>
    public char Separator { get; init; } = ',';

    /// <summary>The quote around fields that need one (ASCII; a quote inside is doubled).</summary>
    public char Quote { get; init; } = '"';

    /// <summary>Which fields are quoted (<see cref="CsvQuoteStyle.Necessary"/>: those that would not read back as they are).</summary>
    public CsvQuoteStyle QuoteStyle { get; init; } = CsvQuoteStyle.Necessary;

    /// <summary>The text of a null (never quoted, except with <see cref="CsvQuoteStyle.Always"/>).</summary>
    public string NullValue { get; init; } = "";

    /// <summary>What ends each line (may be empty).</summary>
    public string LineTerminator { get; init; } = "\n";

    /// <summary>
    /// How floats are written: null (the default) their shortest digits in decimal notation unless very large or small
    /// (<c>1e+16</c>, <c>1e-7</c>); true always in scientific notation (<c>1.5e3</c>); false always in decimal notation
    /// (every digit written out).
    /// </summary>
    public bool? FloatScientific { get; init; }

    /// <summary>
    /// The decimals floats are rounded to (their exact value, half to even: 2.5 with none is 2); with
    /// <see cref="FloatScientific"/> true, the decimals after the first digit. Null (the default): their shortest digits.
    /// </summary>
    public int? FloatPrecision { get; init; }

    /// <summary>
    /// Whether floats and decimals use ',' as the decimal point. With the separator ',' too, every float and decimal is
    /// quoted (as the reference engine writes them; its nulls there stay the null value).
    /// </summary>
    public bool DecimalComma { get; init; }

    /// <summary>
    /// How dates are written: a .NET format (<c>dd/MM/yyyy</c>, or a standard one such as <c>d</c>), written as
    /// <see cref="DateOnly.ToString(string, IFormatProvider)"/> writes it with the invariant culture; or, holding '%', a
    /// chrono (strftime) format (<c>%d/%m/%Y</c>). Null (the default): <c>yyyy-MM-dd</c>.
    /// </summary>
    public string? DateFormat { get; init; }

    /// <summary>How times of day are written (.NET or chrono, as <see cref="DateFormat"/>); null: <c>HH:mm:ss.fffffffff</c>.</summary>
    public string? TimeFormat { get; init; }

    /// <summary>
    /// How timestamps are written (.NET or chrono, as <see cref="DateFormat"/>), with or without a zone (a zoned one in its
    /// local time; an offset or zone specifier — .NET <c>zzz</c>, <c>K</c>, chrono <c>%z</c>, <c>%Z</c> — only fits
    /// zoned columns). Null: <c>yyyy-MM-ddTHH:mm:ss.ffffff</c> (the digits of the unit), and a zoned column's offset.
    /// </summary>
    public string? DatetimeFormat { get; init; }

    /// <summary>
    /// Text between each three digits of a number's integer part ("1.234,56" with '.' and <see cref="DecimalComma"/>,
    /// "1 234,56" with a space or a no-break space, "1'234.5", "1,234.5"); null (the default): none. Numbers are quoted
    /// when it holds the separator or the quote.
    /// </summary>
    public string? ThousandsSeparator { get; init; }

    /// <summary>
    /// The text's encoding; null (the default) is UTF-8. Any .NET encoding without shift states: legacy code pages
    /// (Shift-JIS 932, GBK 936, Big5 950, Windows-1252 …), UTF-16, GB18030, UTF-32. A character it cannot write is an
    /// error naming its row and column, or '?' (<see cref="InvalidText"/>). Text outputs (<see cref="TextWriter"/>,
    /// strings) take none.
    /// </summary>
    public System.Text.Encoding? Encoding { get; init; }

    /// <summary>
    /// What a character <see cref="Encoding"/> cannot write becomes: an error (the default) or '?' (the encoding's
    /// replacement; never a look-alike, which could be a separator or a quote).
    /// </summary>
    public CsvInvalidText InvalidText { get; init; } = CsvInvalidText.Error;

    /// <summary>
    /// The compression of the bytes written (none by default). The rows are compressed in pieces side by side, each a
    /// gzip member or Zstandard frame of its own: one valid file that every reader decompresses whole. Text outputs
    /// (<see cref="TextWriter"/>, strings) take no compression.
    /// </summary>
    public CsvCompression Compression { get; init; } = CsvCompression.None;

    /// <summary>The compression level (the codec's default when null): gzip 0–9, Zstandard 1–22.</summary>
    public int? CompressionLevel { get; init; }

    /// <summary>
    /// How many pieces of rows are encoded (and compressed) at once, in all (batches the writer owns, on their way side by
    /// side, included): the logical processors by default (<see cref="Environment.ProcessorCount"/>: a second hardware
    /// thread per core helps this encoding on the whole). The bytes written do not depend on it.
    /// </summary>
    public int MaxDegreeOfParallelism { get; init; } = Environment.ProcessorCount;

    /// <summary>
    /// Who owns the batches (<see cref="BatchOwnership.Caller"/> by default: a batch is encoded before Write returns).
    /// With <see cref="BatchOwnership.Writer"/> the writer takes every batch given to it, returns once the batch is
    /// checked, encodes several batches side by side and disposes each once encoded (also when writing fails, or the
    /// writer is aborted or disposed). The bytes written do not depend on it.
    /// </summary>
    public BatchOwnership BatchOwnership { get; init; } = BatchOwnership.Caller;

    /// <summary>Whether the stream (or text writer) stays open once the writer is disposed (the writer of a path always closes its file).</summary>
    public bool LeaveOpen { get; init; }

    /// <summary>The bytes of encoded rows in flight at most (a piece of rows goes beyond alone; tests set less).</summary>
    internal long InFlightBytes { get; init; } = 256L << 20;

    /// <summary>The encoded bytes a piece of rows aims at (tests set less, to cut batches finely).</summary>
    internal int PieceBytes { get; init; } = 256 << 10;

    /// <summary>
    /// The bytes of the pieces encoded and not yet written, at most (by the pieces' target size: as many as fit, two per
    /// thread at least, <see cref="WindowPieces"/>' default at most; set WindowPieces to count pieces instead).
    /// </summary>
    internal long WindowBytes { get; init; } = 8L << 20;

    /// <summary>
    /// The pieces of rows encoded and not yet written, at most (0: eight times <see cref="MaxDegreeOfParallelism"/>; tests set
    /// less): the memory a write holds.
    /// </summary>
    internal int WindowPieces { get; init; }

    internal void Validate()
    {
        if (Separator > 0x7F || Separator is '\r' or '\n')
            throw new ArgumentOutOfRangeException(nameof(Separator), Separator, "The separator must be one ASCII character other than CR and LF.");
        if (Quote > 0x7F || Quote is '\r' or '\n' || Quote == Separator)
            throw new ArgumentOutOfRangeException(nameof(Quote), Quote, "The quote must be one ASCII character other than CR, LF and the separator.");
        if (FloatPrecision is < 0 or > 1000)
            throw new ArgumentOutOfRangeException(nameof(FloatPrecision), FloatPrecision, "The float precision is a number of decimals, from 0 to 1000.");
        if (ThousandsSeparator is { } t)
        {
            if (t.Length == 0 || t.Any(c => char.IsAsciiDigit(c) || c is '\r' or '\n'))
                throw new ArgumentException("The thousands separator is one or more characters: no digits, no line ends.", nameof(ThousandsSeparator));
            if (t == (DecimalComma ? "," : "."))
                throw new ArgumentException($"The thousands separator '{t}' is the decimal point too.", nameof(ThousandsSeparator));
            if (t.Contains(Quote))
                throw new ArgumentException($"The thousands separator '{t}' holds the quote: choose another quote (or separator).", nameof(ThousandsSeparator));
        }
        ArgumentNullException.ThrowIfNull(NullValue);
        ArgumentNullException.ThrowIfNull(LineTerminator);
        if (!Enum.IsDefined(QuoteStyle)) throw new ArgumentOutOfRangeException(nameof(QuoteStyle), QuoteStyle, "Unknown quote style.");
        if (!Enum.IsDefined(Compression)) throw new ArgumentOutOfRangeException(nameof(Compression), Compression, "Unknown CSV compression.");
        if (!Enum.IsDefined(InvalidText)) throw new ArgumentOutOfRangeException(nameof(InvalidText), InvalidText, "Unknown InvalidText.");
        if (!Enum.IsDefined(BatchOwnership)) throw new ArgumentOutOfRangeException(nameof(BatchOwnership), BatchOwnership, "Unknown batch ownership.");
        if (MaxDegreeOfParallelism <= 0)
            throw new ArgumentOutOfRangeException(nameof(MaxDegreeOfParallelism), MaxDegreeOfParallelism, "The degree of parallelism must be positive.");
        if (CompressionLevel is int level && (Compression == CsvCompression.None
            || Compression == CsvCompression.Gzip && level is < 0 or > 9 || Compression == CsvCompression.Zstd && level is < 1 or > 22))
            throw new ArgumentOutOfRangeException(nameof(CompressionLevel), level, "The level is not one of the compression's (gzip 0–9, Zstandard 1–22).");
    }
}

/// <summary>Which fields a <see cref="CsvWriter"/> quotes.</summary>
public enum CsvQuoteStyle
{
    /// <summary>
    /// Those that would not read back as they are: a field holding the separator, the quote, CR or LF, and an empty
    /// string (told from a null).
    /// </summary>
    Necessary,

    /// <summary>Every field, nulls included.</summary>
    Always,

    /// <summary>Every field that is not a number (the header too); nulls are not.</summary>
    NonNumeric,

    /// <summary>None (a field holding the separator or a line end does not read back as it is).</summary>
    Never,
}

/// <summary>The compression of CSV written.</summary>
public enum CsvCompression
{
    /// <summary>None.</summary>
    None,

    /// <summary>gzip (members side by side).</summary>
    Gzip,

    /// <summary>Zstandard (frames side by side).</summary>
    Zstd,
}
