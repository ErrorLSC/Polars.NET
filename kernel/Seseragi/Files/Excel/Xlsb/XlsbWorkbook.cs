using Kaminari.Seseragi.Files.Excel.Packaging;
using Kaminari.Seseragi.Files.Excel.Reading;

namespace Kaminari.Seseragi.Files.Excel.Xlsb;

/// <summary>
/// A binary workbook ([MS-XLSB]): the same zip package and relationships as xlsx, with BIFF12 record parts for the
/// workbook, styles, shared strings, sheets and tables. Sheets, the 1904 flag and cell formats are read at open; the
/// shared strings on first use.
/// </summary>
internal sealed class XlsbWorkbook : WorkbookSource
{
    private readonly OpcPackage _package;
    private readonly string?[] _sheetParts; // null for sheets without cells
    private readonly string? _sharedStringsPart;
    private readonly ExcelValueKind[] _styleKinds;
    private SharedStringStore? _sharedStrings;
    private IReadOnlyList<ExcelTableInfo>? _tables;

    private XlsbWorkbook(OpcPackage package, string workbookPart)
    {
        _package = package;
        var rels = package.Relationships(workbookPart);
        var byId = rels.ToDictionary(r => r.Id, StringComparer.Ordinal);
        List<ExcelSheetInfo> sheets = [];
        List<string?> parts = [];
        foreach (var r in Biff12.Records(package.ReadBinaryPart(workbookPart)))
        {
            if (r.Type == Brt.WbProp)
            {
                Date1904 = (new RecordReader(r.Data, r.Type).U32() & 1) != 0;
            }
            else if (r.Type == Brt.BundleSh)
            {
                // BrtBundleSh (2.4.316): hsState, iTabID, strRelID (nullable), strName.
                var x = new RecordReader(r.Data, r.Type);
                uint state = x.U32();
                x.Skip(4);
                string? relId = x.TryNullableWideStringBytes(out var rid) ? WideText.ToString(rid) : null;
                string name = x.WideString();
                OpcRelationship rel = default;
                bool found = relId is not null && byId.TryGetValue(relId, out rel);
                var kind = !found ? ExcelSheetKind.Other
                    : rel.IsType("worksheet") ? ExcelSheetKind.Worksheet
                    : rel.IsType("chartsheet") ? ExcelSheetKind.Chartsheet
                    : ExcelSheetKind.Other;
                var visibility = state switch
                {
                    1 => ExcelSheetVisibility.Hidden,
                    2 => ExcelSheetVisibility.VeryHidden,
                    _ => ExcelSheetVisibility.Visible,
                };
                sheets.Add(new(name, sheets.Count, visibility, kind));
                parts.Add(kind == ExcelSheetKind.Worksheet ? rel.Target : null);
            }
            else if (r.Type == Brt.EndBundleShs)
            {
                break;
            }
        }
        Sheets = sheets;
        _sheetParts = [.. parts];
        _sharedStringsPart = rels.FirstOrDefault(r => r.IsType("sharedStrings")).Target;
        string? stylesPart = rels.FirstOrDefault(r => r.IsType("styles")).Target;
        _styleKinds = stylesPart is not null && package.HasPart(stylesPart) ? ReadStyles(package.ReadBinaryPart(stylesPart)) : [];

        // BrtFmt (number formats) and the BrtXF records between BrtBeginCellXFs and BrtEndCellXFs (cell formats;
        // the cell style XFs before them are not cell formats).
        static ExcelValueKind[] ReadStyles(byte[] styles)
        {
            Dictionary<int, ExcelValueKind> custom = [];
            List<ExcelValueKind> kinds = [];
            bool inCellXfs = false;
            foreach (var r in Biff12.Records(styles))
            {
                switch (r.Type)
                {
                    case Brt.Fmt:
                    {
                        var x = new RecordReader(r.Data, r.Type);
                        int id = x.U16();
                        custom[id] = ExcelNumberFormat.Classify(x.WideString());
                        break;
                    }
                    case Brt.BeginCellXfs:
                        inCellXfs = true;
                        break;
                    case Brt.EndCellXfs:
                        inCellXfs = false;
                        break;
                    case Brt.Xf when inCellXfs:
                    {
                        var x = new RecordReader(r.Data, r.Type);
                        x.Skip(2); // ixfeParent
                        int fmtId = x.U16();
                        kinds.Add(custom.TryGetValue(fmtId, out var kind) ? kind : ExcelNumberFormat.BuiltIn(fmtId));
                        break;
                    }
                }
            }
            return [.. kinds];
        }
    }

    public override ExcelFormat Format => ExcelFormat.Xlsb;

    public override bool Date1904 { get; }

    public override IReadOnlyList<ExcelSheetInfo> Sheets { get; }

    public override IReadOnlyList<ExcelTableInfo> Tables => _tables ??= ReadTables();

    public static XlsbWorkbook Open(OpcPackage package, string workbookPart) => new(package, workbookPart);

    public override async ValueTask<SheetSource> OpenSheetAsync(int index, bool useAsync, CancellationToken cancellationToken)
    {
        if (_sharedStrings is null && _sharedStringsPart is not null && _package.HasPart(_sharedStringsPart))
        {
            using var part = _package.OpenPart(_sharedStringsPart);
            _sharedStrings = await XlsbSharedStrings.LoadAsync(part, useAsync, cancellationToken).ConfigureAwait(false);
        }
        var stream = _package.OpenPart(_sheetParts[index]
            ?? throw new InvalidOperationException($"Sheet '{Sheets[index].Name}' is not a worksheet."));
        try
        {
            return await XlsbSheetSource.OpenAsync(stream, _styleKinds, _sharedStrings, useAsync, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await stream.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    // BrtBeginList (2.4.100): rfxList (rwFirst, rwLast, colFirst, colLast), lt, idList, crwHeader, crwTotals, flags,
    // six DXF ids, dwConnID, then stName and stDisplayName (nullable wide strings).
    private List<ExcelTableInfo> ReadTables()
    {
        List<ExcelTableInfo> tables = [];
        for (int i = 0; i < _sheetParts.Length; i++)
        {
            if (_sheetParts[i] is not { } sheetPart) continue;
            foreach (var rel in _package.Relationships(sheetPart))
            {
                if (!rel.IsType("table") || !_package.HasPart(rel.Target)) continue;
                foreach (var r in Biff12.Records(_package.ReadBinaryPart(rel.Target)))
                {
                    if (r.Type != Brt.BeginList) continue;
                    var x = new RecordReader(r.Data, r.Type);
                    int rwFirst = x.I32(), rwLast = x.I32(), colFirst = x.I32(), colLast = x.I32();
                    if (rwFirst < 0 || rwLast < rwFirst || rwLast >= ExcelCellReference.MaxRows
                        || colFirst < 0 || colLast < colFirst || colLast >= ExcelCellReference.MaxColumns)
                        throw new ExcelFormatException($"A table of sheet '{Sheets[i].Name}' has an invalid range.");
                    x.Skip(8); // lt, idList
                    int header = (int)x.U32(), totals = (int)x.U32();
                    x.Skip(4 + 6 * 4 + 4); // flags, DXF ids, dwConnID
                    string? name = x.TryNullableWideStringBytes(out var n) ? WideText.ToString(n) : null;
                    string? display = x.TryNullableWideStringBytes(out var d) ? WideText.ToString(d) : null;
                    string range = $"{ExcelCellReference.Format(rwFirst, colFirst)}:{ExcelCellReference.Format(rwLast, colLast)}";
                    tables.Add(new(display ?? name ?? "", Sheets[i].Name, range, header, totals));
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
