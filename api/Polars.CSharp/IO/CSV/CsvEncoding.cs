using System.Text;
using Polars.NET.Core.Csv;

namespace Polars.CSharp;

/// <summary>
/// The text encoding of CSV read or written: UTF-8 by default (<c>default</c> is <see cref="UTF8"/>), any encoding .NET
/// knows, legacy code pages included (Shift_JIS, GBK, Big5, Windows-1252 …: no provider to register). Given by its
/// static members, a code page (<c>932</c>), a name (<c>"shift_jis"</c>) or a <see cref="System.Text.Encoding"/>.
/// </summary>
/// <remarks>
/// Strict by default: text the encoding does not define is an error naming the row. <see cref="Lossy"/> reads it as
/// U+FFFD and writes a character the encoding cannot hold as <c>?</c>. A byte order mark at the start of input read is
/// honoured (it names UTF-8 or UTF-16 whatever is given, unless another non-UTF-8 encoding is given: then an error).
/// </remarks>
public readonly struct CsvEncoding : IEquatable<CsvEncoding>
{
    private readonly Encoding? _encoding; // null: UTF-8

    private CsvEncoding(Encoding? encoding, bool lossy)
    {
        _encoding = encoding is { CodePage: 65001 } ? null : encoding;
        IsLossy = lossy;
    }

    /// <summary>UTF-8, strict (the default).</summary>
    public static CsvEncoding UTF8 => default;

    /// <summary>UTF-8, invalid bytes read as U+FFFD.</summary>
    public static CsvEncoding LossyUTF8 => new(null, lossy: true);

    /// <summary>UTF-16, little endian (code page 1200).</summary>
    public static CsvEncoding Utf16LE => new(Encoding.Unicode, lossy: false);

    /// <summary>UTF-16, big endian (code page 1201).</summary>
    public static CsvEncoding Utf16BE => new(Encoding.BigEndianUnicode, lossy: false);

    /// <summary>Shift_JIS as Windows writes it (code page 932: NEC and IBM extensions).</summary>
    public static CsvEncoding ShiftJis => FromCodePage(932);

    /// <summary>GBK, simplified Chinese (code page 936).</summary>
    public static CsvEncoding Gbk => FromCodePage(936);

    /// <summary>Big5, traditional Chinese (code page 950).</summary>
    public static CsvEncoding Big5 => FromCodePage(950);

    /// <summary>EUC-KR / Unified Hangul Code, Korean (code page 949).</summary>
    public static CsvEncoding EucKr => FromCodePage(949);

    /// <summary>Windows-1252, Western European (code page 1252; what "ANSI" means on most Western Windows).</summary>
    public static CsvEncoding Windows1252 => FromCodePage(1252);

    /// <summary>ISO-8859-1 (Latin-1).</summary>
    public static CsvEncoding Latin1 => new(Encoding.Latin1, lossy: false);

    /// <summary>The encoding of a code page (932, 936, 1252, 65001 …).</summary>
    /// <exception cref="ArgumentException">.NET knows no encoding of that code page.</exception>
    public static CsvEncoding FromCodePage(int codePage) => new(ManagedCsv.EncodingOf(codePage), lossy: false);

    /// <summary>The encoding of a name ("shift_jis", "gbk", "windows-1252", "utf-16" …; case does not matter).</summary>
    /// <exception cref="ArgumentException">.NET knows no encoding of that name.</exception>
    public static CsvEncoding FromName(string name) => new(ManagedCsv.EncodingOf(name), lossy: false);

    /// <summary>A .NET encoding (its own fallbacks are not used: strict, or <see cref="Lossy"/>).</summary>
    public static CsvEncoding From(Encoding encoding)
    {
        ArgumentNullException.ThrowIfNull(encoding);
        return new(encoding, lossy: false);
    }

    /// <summary>This encoding, lossy: invalid input read as U+FFFD, characters it cannot hold written as <c>?</c>.</summary>
    public CsvEncoding Lossy() => new(_encoding, lossy: true);

    /// <summary>Whether invalid text is replaced (else an error naming the row).</summary>
    public bool IsLossy { get; }

    /// <summary>The .NET encoding.</summary>
    public Encoding Encoding => _encoding ?? System.Text.Encoding.UTF8;

    /// <summary>The encoding given to the readers and writers (null: UTF-8).</summary>
    internal Encoding? Given => _encoding;

    /// <summary>The encoding of a code page (<see cref="FromCodePage"/>).</summary>
    public static implicit operator CsvEncoding(int codePage) => FromCodePage(codePage);

    /// <summary>The encoding of a name (<see cref="FromName"/>).</summary>
    public static implicit operator CsvEncoding(string name) => FromName(name);

    /// <summary>A .NET encoding (<see cref="From"/>).</summary>
    public static implicit operator CsvEncoding(Encoding encoding) => From(encoding);

    /// <summary>The same code page, both lossy or both strict.</summary>
    public bool Equals(CsvEncoding other) => IsLossy == other.IsLossy && Encoding.CodePage == other.Encoding.CodePage;

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is CsvEncoding other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode() => HashCode.Combine(Encoding.CodePage, IsLossy);

    /// <summary>Whether two encodings are the same (<see cref="Equals(CsvEncoding)"/>).</summary>
    public static bool operator ==(CsvEncoding left, CsvEncoding right) => left.Equals(right);

    /// <summary>Whether two encodings differ.</summary>
    public static bool operator !=(CsvEncoding left, CsvEncoding right) => !left.Equals(right);

    /// <summary>The encoding's web name ("utf-8", "shift_jis" …), marked when lossy.</summary>
    public override string ToString() => IsLossy ? $"{Encoding.WebName} (lossy)" : Encoding.WebName;
}
