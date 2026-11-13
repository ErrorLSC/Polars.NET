using System.Buffers;
using Kaminari.Seseragi.Files.Excel.Packaging;
using Kaminari.Seseragi.Files.Excel.Reading;
using Kaminari.Seseragi.Files.Excel.Xml;

namespace Kaminari.Seseragi.Files.Excel.Ods;

/// <summary>
/// An OpenDocument spreadsheet ([ODF 1.3]): a package (.ods: a zip with <c>mimetype</c>, <c>content.xml</c> and
/// <c>styles.xml</c>) or a flat document (.fods: one XML file). The sheets are the <c>table:table</c> elements of
/// <c>office:spreadsheet</c>, one after another in one XML document, so listing them takes a pass over all of it.
/// The workbook therefore finds sheets as it needs them: opening reads the styles and stops at the first sheet's
/// start tag; reading a sheet goes on from there (skipping the sheets before it) on the same stream, so the
/// document is decompressed once; only <see cref="Sheets"/> scans to the end. A sheet the scan has passed is read by
/// starting the document again at its offset (decompressing and discarding what comes before it in a package,
/// seeking in a flat file).
/// </summary>
internal sealed class OdsWorkbook : WorkbookSource
{
    /// <summary>The media types of spreadsheets and spreadsheet templates.</summary>
    private const string Spreadsheet = "application/vnd.oasis.opendocument.spreadsheet";

    private readonly OpcPackage? _package;    // a package
    private readonly RandomAccessSource? _flat; // a flat document
    private readonly OdsStyles _styles;
    private readonly List<ExcelSheetInfo> _sheets = []; // found so far, in order
    private readonly List<long> _offsets = [];
    private bool _complete;      // every sheet found
    private Cursor? _cursor;     // the scan, standing at the start tag of sheet _cursorAt (or at the end)
    private int _cursorAt = -1;

    private OdsWorkbook(OpcPackage? package, RandomAccessSource? flat, OdsStyles styles)
    {
        _package = package;
        _flat = flat;
        _styles = styles;
    }

    public override ExcelFormat Format => ExcelFormat.Ods;

    public override bool Date1904 => _styles.Date1904;

    /// <summary>All sheets (scanning the rest of the document the first time).</summary>
    public override IReadOnlyList<ExcelSheetInfo> Sheets
    {
        get
        {
            while (!_complete) AdvanceAsync(false, default).AsTask().GetAwaiter().GetResult();
            return _sheets;
        }
    }

    public override IReadOnlyList<ExcelTableInfo> Tables => [];

    /// <summary>Whether a media type is a spreadsheet's (or a spreadsheet template's).</summary>
    public static bool IsSpreadsheet(string mediaType) =>
        mediaType is Spreadsheet or Spreadsheet + "-template";

    /// <summary>
    /// The media type of an OpenDocument package: the <c>mimetype</c> part, or the manifest's entry for the package
    /// itself; null when the zip is no OpenDocument package.
    /// </summary>
    public static string? MediaType(OpcPackage package)
    {
        if (package.HasPart("mimetype")) return System.Text.Encoding.ASCII.GetString(XmlText.Trim(package.ReadBinaryPart("mimetype")));
        if (!package.HasPart("META-INF/manifest.xml")) return null;
        var x = new XmlSpanScanner(package.ReadPart("META-INF/manifest.xml"));
        while (x.Read())
        {
            if (x.Token == XmlToken.StartElement && x.Is("file-entry"u8) && x.TryGetAttribute("full-path"u8, out var path)
                && path.SequenceEqual("/"u8) && x.TryGetAttribute("media-type"u8, out var type))
                return XmlText.DecodeString(type, false);
        }
        return null;
    }

    /// <summary>
    /// Whether <paramref name="head"/> (the first bytes of a file) starts a flat OpenDocument document: an
    /// <c>office:document</c> root element; gives its <c>office:mimetype</c>.
    /// </summary>
    public static bool IsFlat(ReadOnlySpan<byte> head, out string? mediaType)
    {
        mediaType = null;
        if (head.StartsWith((ReadOnlySpan<byte>)[0xEF, 0xBB, 0xBF])) head = head[3..];
        int lt = head.IndexOf((byte)'<');
        if (lt < 0 || !XmlText.Trim(head[..lt]).IsEmpty) return false;
        int last = head.LastIndexOf((byte)'<');
        try
        {
            // Complete markup only: up to the last '<' (the root tag must end before it).
            var x = new XmlSpanScanner(head[..last]);
            while (x.Read())
            {
                if (x.Token != XmlToken.StartElement) continue;
                if (!x.Is("document"u8)) return false;
                mediaType = x.TryGetAttribute("mimetype"u8, out var m) ? XmlText.DecodeString(m, false) : null;
                return true;
            }
        }
        catch (ExcelFormatException)
        {
        }
        return false;
    }

    /// <summary>Opens a package (its media type already checked).</summary>
    public static async ValueTask<OdsWorkbook> OpenAsync(OpcPackage package, bool useAsync, CancellationToken ct)
    {
        if (package.HasPart("META-INF/manifest.xml"))
        {
            var x = new XmlSpanScanner(package.ReadPart("META-INF/manifest.xml"));
            while (x.Read())
            {
                if (x.Token == XmlToken.StartElement && x.Is("encryption-data"u8))
                    throw new ExcelFormatException("The workbook is encrypted (a password-protected .ods workbook), which is not supported.");
            }
        }
        if (!package.HasPart("content.xml")) throw new ExcelFormatException("The OpenDocument package has no content.xml.");
        var styles = new OdsStyles();
        if (package.HasPart("styles.xml"))
        {
            var x = new XmlSpanScanner(package.ReadPart("styles.xml"));
            while (x.Read()) styles.Observe(ref x);
        }
        var workbook = new OdsWorkbook(package, null, styles);
        await workbook.AdvanceAsync(useAsync, ct).ConfigureAwait(false); // the styles, settings and first sheet's name
        return workbook;
    }

    /// <summary>Opens a flat document (its media type already checked); the source is owned from here.</summary>
    public static async ValueTask<OdsWorkbook> OpenFlatAsync(RandomAccessSource source, bool useAsync, CancellationToken ct)
    {
        var workbook = new OdsWorkbook(null, source, new OdsStyles());
        try
        {
            await workbook.AdvanceAsync(useAsync, ct).ConfigureAwait(false);
            return workbook;
        }
        catch
        {
            workbook.Dispose();
            throw;
        }
    }

    private Stream OpenContent() => _flat is not null ? new RandomAccessStream(_flat, 0) : _package!.OpenPart("content.xml");

    /// <summary>
    /// Moves the scan to the next sheet (starting it over from the top when it was handed to a reader); records a
    /// sheet seen for the first time. Returns its index, or -1 at the end of the spreadsheet.
    /// </summary>
    private async ValueTask<int> AdvanceAsync(bool useAsync, CancellationToken ct)
    {
        if (_cursor is null)
        {
            if (_complete) return -1;
            _cursor = new Cursor(OpenContent(), _styles);
            _cursorAt = -1;
        }
        var next = await _cursor.NextAsync(useAsync, ct).ConfigureAwait(false);
        if (next is not { } sheet)
        {
            _complete = true;
            _cursor.Dispose();
            _cursor = null;
            _cursorAt = -1;
            return -1;
        }
        int index = _cursor.Found - 1;
        if (index == _sheets.Count)
        {
            _sheets.Add(new(sheet.Name, index, _styles.IsHidden(sheet.Style) ? ExcelSheetVisibility.Hidden : ExcelSheetVisibility.Visible));
            _offsets.Add(sheet.Offset);
        }
        _cursorAt = index;
        return index;
    }

    public override async ValueTask<(ExcelSheetInfo Sheet, SheetSource Source)?> TryOpenSheetDirectAsync(string? name, int index, bool useAsync, CancellationToken ct)
    {
        int target = Find();
        while (target < 0 && !_complete)
        {
            int found = await AdvanceAsync(useAsync, ct).ConfigureAwait(false);
            if (found >= 0 && Matches(found)) target = found;
        }
        if (target < 0) return null;
        if (_cursor is not null && _cursorAt == target)
        {
            // The scan stands at the sheet: its reader goes on from there.
            var (stream, reader) = _cursor.HandOver();
            _cursor = null;
            _cursorAt = -1;
            return (_sheets[target], OdsSheetSource.Continue(stream, reader, _sheets[target].Name, _styles));
        }
        return (_sheets[target], await OpenSheetAsync(target, useAsync, ct).ConfigureAwait(false));

        int Find()
        {
            for (int i = 0; i < _sheets.Count; i++)
                if (Matches(i)) return i;
            return -1;
        }

        bool Matches(int i) => name is null ? i == index : string.Equals(_sheets[i].Name, name, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// A pass over the document from its top: styles and settings outside the sheets are fed to the styles, and it
    /// stops at each sheet's start tag (a <c>table:table</c> child of <c>office:spreadsheet</c>; tables nested in
    /// cells are not sheets). Markup is parsed up to the last '&lt;' buffered, so every tag and text run handed to the
    /// scanner is whole.
    /// </summary>
    private sealed class Cursor(Stream stream, OdsStyles styles) : IDisposable
    {
        private readonly ChunkReader _reader = new(stream);
        private long _consumed;
        private int _depth, _spreadsheet = -1, _table = -1;
        private bool _atSheet; // the buffer starts at the start tag of the sheet found last
        private bool _done, _handedOver;

        /// <summary>Sheets found so far.</summary>
        public int Found { get; private set; }

        /// <summary>The next sheet (name, table style, offset of its start tag); null at the end of the spreadsheet.</summary>
        public async ValueTask<(string Name, string? Style, long Offset)?> NextAsync(bool useAsync, CancellationToken ct)
        {
            while (!_done)
            {
                var buf = _reader.Buffered;
                int end = buf.LastIndexOf((byte)'<');
                if (end > 0 && Scan(buf[..end], out int consume) is { } sheet)
                {
                    _reader.Consume(consume);
                    _consumed += consume;
                    Found++;
                    return sheet;
                }
                if (end > 0)
                {
                    _reader.Consume(end);
                    _consumed += end;
                }
                if (_done) break;
                bool more = useAsync ? await _reader.FillAsync(ct).ConfigureAwait(false) : _reader.Fill();
                if (!more)
                {
                    throw new ExcelFormatException(_spreadsheet < 0
                        ? "The OpenDocument document holds no spreadsheet (office:spreadsheet)."
                        : "The OpenDocument document is truncated.");
                }
            }
            return null;
        }

        // Tokens of whole markup; stops before a sheet's start tag (consume: the bytes before it).
        private (string, string?, long)? Scan(ReadOnlySpan<byte> markup, out int consume)
        {
            consume = markup.Length;
            var x = new XmlSpanScanner(markup);
            while (x.Read())
            {
                if (x.Token == XmlToken.StartElement)
                {
                    if (_atSheet)
                    {
                        _atSheet = false; // the sheet reported last: now skipped
                        if (!x.IsEmptyElement) _table = _depth++;
                        continue;
                    }
                    if (_table < 0)
                    {
                        if (_spreadsheet >= 0 && _depth == _spreadsheet + 1 && x.Is("table"u8))
                        {
                            string name = x.TryGetAttribute("name"u8, out var n) ? XmlText.DecodeString(n, false) : $"Sheet{Found + 1}";
                            string? style = x.TryGetAttribute("style-name"u8, out var s) ? XmlText.DecodeString(s, false) : null;
                            consume = x.TokenStart;
                            _atSheet = true;
                            return (name, style, _consumed + x.TokenStart);
                        }
                        styles.Observe(ref x);
                        if (x.Is("spreadsheet"u8) && _spreadsheet < 0) _spreadsheet = _depth;
                    }
                    if (!x.IsEmptyElement) _depth++;
                }
                else if (x.Token == XmlToken.EndElement)
                {
                    _depth--;
                    if (_table >= 0)
                    {
                        if (_depth == _table) _table = -1;
                    }
                    else
                    {
                        styles.Observe(ref x);
                        if (_depth == _spreadsheet && x.Is("spreadsheet"u8))
                        {
                            _done = true;
                            consume = x.Position;
                            return null;
                        }
                    }
                }
            }
            return null;
        }

        /// <summary>Gives the stream and its buffer (at the sheet's start tag) to a sheet reader.</summary>
        public (Stream, ChunkReader) HandOver()
        {
            _handedOver = true;
            return (stream, _reader);
        }

        public void Dispose()
        {
            if (_handedOver) return;
            _reader.Dispose();
            stream.Dispose();
        }
    }

    public override async ValueTask<SheetSource> OpenSheetAsync(int index, bool useAsync, CancellationToken cancellationToken)
    {
        Stream stream;
        if (_flat is not null)
        {
            stream = new RandomAccessStream(_flat, _offsets[index]);
        }
        else
        {
            stream = _package!.OpenPart("content.xml");
            try
            {
                await DiscardAsync(stream, _offsets[index], useAsync, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                await stream.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }
        try
        {
            return await OdsSheetSource.OpenAsync(stream, _sheets[index].Name, _styles, useAsync, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await stream.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        // A deflate stream does not seek: what comes before the sheet is decompressed and dropped.
        static async ValueTask DiscardAsync(Stream stream, long count, bool useAsync, CancellationToken ct)
        {
            byte[] scratch = ArrayPool<byte>.Shared.Rent(64 * 1024);
            try
            {
                while (count > 0)
                {
                    int want = (int)Math.Min(count, scratch.Length);
                    int n = useAsync ? await stream.ReadAsync(scratch.AsMemory(0, want), ct).ConfigureAwait(false) : stream.Read(scratch, 0, want);
                    if (n <= 0) throw new ExcelFormatException("content.xml ends before the sheet it lists.");
                    count -= n;
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(scratch);
            }
        }
    }

    public override void Dispose()
    {
        _cursor?.Dispose();
        _package?.Dispose();
        _flat?.Dispose();
    }
}
