using System.Text;
using Kaminari.Seseragi.Files.Excel.Reading;
using Kaminari.Seseragi.Files.Excel.Xml;

namespace Kaminari.Seseragi.Files.Excel.Xlsx;

/// <summary>
/// An xlsx workbook: sheets, the 1904 flag, cell formats and tables read at open; the shared string table read on
/// first use (it can be large and a caller may only want the sheet list).
/// </summary>
internal sealed class XlsxWorkbook : WorkbookSource
{
    private readonly XlsxPackage _package;
    private readonly string?[] _sheetParts; // null for sheets without a part we can open
    private readonly string? _sharedStringsPart;
    private readonly ExcelValueKind[] _styleKinds;
    private XlsxSharedStrings? _sharedStrings;
    private IReadOnlyList<ExcelTableInfo>? _tables;

    private XlsxWorkbook(XlsxPackage package, string workbookPart)
    {
        _package = package;
        var rels = package.Relationships(workbookPart);
        var byId = rels.ToDictionary(r => r.Id, StringComparer.Ordinal);
        List<ExcelSheetInfo> sheets = [];
        List<string?> parts = [];
        var x = new XmlSpanScanner(package.ReadPart(workbookPart));
        while (x.Read())
        {
            if (x.Token != XmlToken.StartElement) continue;
            if (x.Is("workbookPr"u8))
            {
                Date1904 = x.TryGetAttribute("date1904"u8, out var d) && XmlText.ParseBoolean(d);
            }
            else if (x.Is("sheet"u8))
            {
                if (!x.TryGetAttribute("name"u8, out var name) || !x.TryGetAttribute("id"u8, out var rid)) continue;
                string id = XmlText.DecodeString(rid, false);
                // Every sheet keeps its tab position; only worksheets hold cells.
                byId.TryGetValue(id, out var rel);
                var kind = rel.Type is null ? ExcelSheetKind.Other
                    : rel.IsType("worksheet") ? ExcelSheetKind.Worksheet
                    : rel.IsType("chartsheet") ? ExcelSheetKind.Chartsheet
                    : ExcelSheetKind.Other;
                var state = x.TryGetAttribute("state"u8, out var st) ? st : default;
                var visibility = state.SequenceEqual("hidden"u8) ? ExcelSheetVisibility.Hidden
                    : state.SequenceEqual("veryHidden"u8) ? ExcelSheetVisibility.VeryHidden
                    : ExcelSheetVisibility.Visible;
                sheets.Add(new(XmlText.DecodeString(name), sheets.Count, visibility, kind));
                parts.Add(kind == ExcelSheetKind.Worksheet ? rel.Target : null);
            }
        }
        Sheets = sheets;
        _sheetParts = [.. parts];
        _sharedStringsPart = rels.FirstOrDefault(r => r.IsType("sharedStrings")).Target;
        string? stylesPart = rels.FirstOrDefault(r => r.IsType("styles")).Target;
        _styleKinds = stylesPart is not null && package.HasPart(stylesPart) ? ReadStyles(package.ReadPart(stylesPart)) : [];

        // cellXfs: the number format of each cell format index, classified.
        static ExcelValueKind[] ReadStyles(byte[] styles)
        {
            Dictionary<int, ExcelValueKind> custom = [];
            List<ExcelValueKind> kinds = [];
            var x = new XmlSpanScanner(styles);
            bool inCellXfs = false, inNumFmts = false;
            while (x.Read())
            {
                if (x.Token == XmlToken.EndElement && x.Is("cellXfs"u8)) inCellXfs = false;
                if (x.Token == XmlToken.EndElement && x.Is("numFmts"u8)) inNumFmts = false;
                if (x.Token != XmlToken.StartElement) continue;
                if (x.Is("numFmts"u8))
                {
                    inNumFmts = !x.IsEmptyElement;
                }
                else if (inNumFmts && x.Is("numFmt"u8))
                {
                    if (x.TryGetAttribute("numFmtId"u8, out var id) && XmlText.TryParseInt(id, out int fmtId)
                        && x.TryGetAttribute("formatCode"u8, out var code))
                        custom[fmtId] = ExcelNumberFormat.Classify(XmlText.DecodeString(code, false));
                }
                else if (x.Is("cellXfs"u8))
                {
                    inCellXfs = !x.IsEmptyElement;
                }
                else if (inCellXfs && x.Is("xf"u8))
                {
                    int fmtId = x.TryGetAttribute("numFmtId"u8, out var id) && XmlText.TryParseInt(id, out int v) ? v : 0;
                    kinds.Add(custom.TryGetValue(fmtId, out var kind) ? kind : ExcelNumberFormat.BuiltIn(fmtId));
                    x.SkipElement();
                }
            }
            return [.. kinds];
        }
    }

    public override ExcelFormat Format => ExcelFormat.Xlsx;

    public override bool Date1904 { get; }

    public override IReadOnlyList<ExcelSheetInfo> Sheets { get; }

    public override IReadOnlyList<ExcelTableInfo> Tables => _tables ??= ReadTables();

    /// <summary>Opens the workbook in a package whose root relationships name an xlsx workbook part.</summary>
    public static XlsxWorkbook Open(XlsxPackage package, string workbookPart) => new(package, workbookPart);

    /// <summary>The workbook part named by the package's office-document relationship, if any.</summary>
    public static string? FindWorkbookPart(XlsxPackage package) =>
        package.Relationships("").FirstOrDefault(r => r.IsType("officeDocument")).Target;

    public override async ValueTask<SheetSource> OpenSheetAsync(int index, bool useAsync, CancellationToken cancellationToken)
    {
        if (_sharedStrings is null && _sharedStringsPart is not null && _package.HasPart(_sharedStringsPart))
        {
            using var part = _package.OpenPart(_sharedStringsPart);
            _sharedStrings = await XlsxSharedStrings.LoadAsync(part, useAsync, cancellationToken).ConfigureAwait(false);
        }
        var stream = _package.OpenPart(_sheetParts[index]
            ?? throw new InvalidOperationException($"Sheet '{Sheets[index].Name}' is not a worksheet."));
        try
        {
            return await XlsxSheetSource.OpenAsync(stream, _styleKinds, _sharedStrings, useAsync, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await stream.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private List<ExcelTableInfo> ReadTables()
    {
        List<ExcelTableInfo> tables = [];
        for (int i = 0; i < _sheetParts.Length; i++)
        {
            if (_sheetParts[i] is not { } part) continue;
            foreach (var rel in _package.Relationships(part))
            {
                if (!rel.IsType("table") || !_package.HasPart(rel.Target)) continue;
                var x = new XmlSpanScanner(_package.ReadPart(rel.Target));
                while (x.Read())
                {
                    if (x.Token != XmlToken.StartElement || !x.Is("table"u8)) continue;
                    var nameAttr = x.TryGetAttribute("displayName"u8, out var dn) ? dn
                        : x.TryGetAttribute("name"u8, out var n) ? n : default;
                    if (nameAttr.IsEmpty || !x.TryGetAttribute("ref"u8, out var range)) break;
                    int header = x.TryGetAttribute("headerRowCount"u8, out var h) && XmlText.TryParseInt(h, out int hv) ? hv : 1;
                    int totals = x.TryGetAttribute("totalsRowCount"u8, out var t) && XmlText.TryParseInt(t, out int tv) ? tv : 0;
                    tables.Add(new(XmlText.DecodeString(nameAttr, false), Sheets[i].Name, Encoding.UTF8.GetString(range), header, totals));
                    break;
                }
            }
        }
        return tables;
    }

    public override void Dispose()
    {
        _sharedStrings?.Dispose();
        _package.Dispose();
    }
}
