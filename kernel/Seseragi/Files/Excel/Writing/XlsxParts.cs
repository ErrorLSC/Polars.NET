namespace Kaminari.Seseragi.Files.Excel.Writing;

/// <summary>A cell format: its index in <c>cellXfs</c> and the width its values show at (for autofit).</summary>
internal sealed record ExcelStyle(byte Index, int Width, string FormatCode);

/// <summary>
/// The cell formats of a written workbook: the default (index 0) and one per temporal kind, each with its own number
/// format (custom ids from 164). Built from the write options; written as <c>styles.xml</c>.
/// </summary>
internal sealed class ExcelStyles
{
    private readonly List<ExcelStyle> _formats = [];

    public ExcelStyles(ExcelWriteOptions options)
    {
        Date = Add(options.DateFormat);
        DateTime = Add(options.DatetimeFormat);
        Time = Add(options.TimeFormat);
        Duration = Add(options.DurationFormat);

        ExcelStyle Add(string code)
        {
            if (string.IsNullOrEmpty(code)) throw new ArgumentException("A number format must not be empty.", nameof(options));
            var existing = _formats.FirstOrDefault(f => f.FormatCode == code);
            if (existing is not null) return existing;
            var style = new ExcelStyle((byte)(_formats.Count + 1), DisplayWidth(code), code);
            _formats.Add(style);
            return style;
        }
    }

    public ExcelStyle Date { get; }

    public ExcelStyle DateTime { get; }

    public ExcelStyle Time { get; }

    public ExcelStyle Duration { get; }

    // How wide a format's values show: its length without quotes, escapes and brackets.
    private static int DisplayWidth(string code)
    {
        int width = 0;
        bool quoted = false;
        for (int i = 0; i < code.Length; i++)
        {
            char c = code[i];
            if (c == '"') quoted = !quoted;
            else if (c == '\\' && !quoted) i++;
            else if (c == '[' && !quoted)
            {
                int end = code.IndexOf(']', i);
                width += 2;
                i = end < 0 ? code.Length : end;
                continue;
            }
            else if (c == ';' && !quoted) break;
            width++;
        }
        return width;
    }

    public void Write(XmlOut x)
    {
        x.Raw("""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>"""u8);
        x.Raw("\n"u8);
        x.Raw("""<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">"""u8);
        x.Raw("<numFmts count=\""u8);
        x.Int(_formats.Count);
        x.Raw("\">"u8);
        foreach (var f in _formats)
        {
            x.Raw("<numFmt numFmtId=\""u8);
            x.Int(163 + f.Index);
            x.Raw("\" formatCode=\""u8);
            x.Text(f.FormatCode);
            x.Raw("\"/>"u8);
        }
        x.Raw("</numFmts>"u8);
        x.Raw("""<fonts count="1"><font><sz val="11"/><name val="Calibri"/><family val="2"/><scheme val="minor"/></font></fonts>"""u8);
        x.Raw("""<fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills>"""u8);
        x.Raw("""<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>"""u8);
        x.Raw("""<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>"""u8);
        x.Raw("<cellXfs count=\""u8);
        x.Int(_formats.Count + 1);
        x.Raw("""
            "><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>
            """u8);
        foreach (var f in _formats)
        {
            x.Raw("<xf numFmtId=\""u8);
            x.Int(163 + f.Index);
            x.Raw("""
                " fontId="0" fillId="0" borderId="0" xfId="0" applyNumberFormat="1"/>
                """u8);
        }
        x.Raw("</cellXfs>"u8);
        x.Raw("""<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>"""u8);
        x.Raw("</styleSheet>"u8);
    }
}

/// <summary>The fixed parts of a one-sheet workbook package.</summary>
internal static class XlsxParts
{
    public const string SheetPart = "xl/worksheets/sheet1.xml";
    public const string SharedStringsPart = "xl/sharedStrings.xml";
    public const string StylesPart = "xl/styles.xml";
    public const string WorkbookPart = "xl/workbook.xml";
    public const string WorkbookRelsPart = "xl/_rels/workbook.xml.rels";
    public const string RootRelsPart = "_rels/.rels";
    public const string ContentTypesPart = "[Content_Types].xml";

    public static ReadOnlySpan<byte> XmlDeclaration => """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>"""u8;

    public static ReadOnlySpan<byte> RootRels => """
        <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        <Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>
        """u8;

    public static void WorkbookRels(XmlOut x, bool sharedStrings)
    {
        x.Raw("""
            <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
            <Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
            """u8);
        if (sharedStrings)
            x.Raw("""<Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.xml"/>"""u8);
        x.Raw("</Relationships>"u8);
    }

    public static void Workbook(XmlOut x, string sheetName)
    {
        x.Raw("""
            <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
            <workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><workbookPr/><bookViews><workbookView/></bookViews><sheets><sheet name="
            """u8);
        x.Text(sheetName);
        x.Raw("""
            " sheetId="1" r:id="rId1"/></sheets></workbook>
            """u8);
    }

    public static void ContentTypes(XmlOut x, bool sharedStrings)
    {
        x.Raw("""
            <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
            <Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/><Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
            """u8);
        if (sharedStrings)
            x.Raw("""<Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>"""u8);
        x.Raw("</Types>"u8);
    }

    public static ReadOnlySpan<byte> WorksheetStart => """
        <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        <worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
        """u8;

    public static ReadOnlySpan<byte> FrozenHeaderView =>
        """<sheetViews><sheetView workbookViewId="0"><pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/><selection pane="bottomLeft"/></sheetView></sheetViews>"""u8;
}
