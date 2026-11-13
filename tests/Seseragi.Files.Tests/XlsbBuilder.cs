using System.Buffers.Binary;
using System.IO.Compression;
using System.Text;

namespace Seseragi.Files.Tests;

/// <summary>
/// Builds binary workbooks record by record for the tests: no open-source writer produces .xlsb, and Excel's own
/// files do not use every record kind the reader handles. Only what the reader looks at is written.
/// </summary>
internal sealed class XlsbBuilder
{
    private readonly List<(string Name, int State, MemoryStream Sheet, List<MemoryStream> Tables)> _sheets = [];
    private readonly List<string> _strings = [];
    private readonly List<(int Id, string Code)> _formats = [];
    private readonly List<int> _xfFormats = [0];
    private bool _date1904;
    private MemoryStream? _current;

    public XlsbBuilder Date1904()
    {
        _date1904 = true;
        return this;
    }

    /// <summary>A cell format with a number format (a built-in id, or a custom code given an id ≥ 164); returns its index.</summary>
    public int Style(int formatId, string? code = null)
    {
        if (code is not null) _formats.Add((formatId, code));
        _xfFormats.Add(formatId);
        return _xfFormats.Count - 1;
    }

    public int SharedString(string s)
    {
        _strings.Add(s);
        return _strings.Count - 1;
    }

    /// <summary>Starts a sheet (state 0 visible, 1 hidden, 2 very hidden) with a dimension record and BrtBeginSheetData.</summary>
    public XlsbBuilder Sheet(string name, int state = 0)
    {
        _current = new MemoryStream();
        _sheets.Add((name, state, _current, []));
        Record(_current, 129, []);                                        // BrtBeginSheet
        Record(_current, 148, [.. I32(0), .. I32(0), .. I32(0), .. I32(0)]); // BrtWsDim
        Record(_current, 145, []);                                        // BrtBeginSheetData
        return this;
    }

    public XlsbBuilder Row(int row) => Raw(0, [.. I32(row), .. new byte[21]]);

    private static byte[] CellHead(int col, int style) => [.. I32(col), (byte)style, (byte)(style >> 8), (byte)(style >> 16), 0];

    public XlsbBuilder Blank(int col, int style = 0) => Raw(1, CellHead(col, style));

    public XlsbBuilder Rk(int col, uint raw, int style = 0) => Raw(2, [.. CellHead(col, style), .. U32(raw)]);

    public XlsbBuilder Error(int col, byte code = 0x2A) => Raw(3, [.. CellHead(col, 0), code]);

    public XlsbBuilder Bool(int col, bool value) => Raw(4, [.. CellHead(col, 0), (byte)(value ? 1 : 0)]);

    public XlsbBuilder Real(int col, double value, int style = 0) => Raw(5, [.. CellHead(col, style), .. F64(value)]);

    public XlsbBuilder Text(int col, string value) => Raw(6, [.. CellHead(col, 0), .. Wide(value)]);

    public XlsbBuilder Shared(int col, int index) => Raw(7, [.. CellHead(col, 0), .. I32(index)]);

    // Formula records: the cached value, grbitFlags, then a formula of zero tokens (cce = 0, cb = 0).
    private static readonly byte[] EmptyFormula = [0, 0, .. I32(0), .. I32(0)];

    public XlsbBuilder FormulaText(int col, string value) => Raw(8, [.. CellHead(col, 0), .. Wide(value), .. EmptyFormula]);

    public XlsbBuilder FormulaNumber(int col, double value, int style = 0) => Raw(9, [.. CellHead(col, style), .. F64(value), .. EmptyFormula]);

    public XlsbBuilder FormulaBool(int col, bool value) => Raw(10, [.. CellHead(col, 0), (byte)(value ? 1 : 0), .. EmptyFormula]);

    public XlsbBuilder FormulaError(int col, byte code = 0x07) => Raw(11, [.. CellHead(col, 0), code, .. EmptyFormula]);

    /// <summary>BrtCellRString: a RichStr with one formatting run (fRichStr).</summary>
    public XlsbBuilder RichText(int col, string value) =>
        Raw(62, [.. CellHead(col, 0), 1, .. Wide(value), .. I32(1), 0, 0, 0, 0]);

    /// <summary>Any record (unknown ones included) in the current sheet.</summary>
    public XlsbBuilder Raw(int type, byte[] data)
    {
        Record(_current!, type, data);
        return this;
    }

    /// <summary>Raw bytes (malformed records).</summary>
    public XlsbBuilder Bytes(params byte[] bytes)
    {
        _current!.Write(bytes);
        return this;
    }

    /// <summary>A table on the current sheet (BrtBeginList).</summary>
    public XlsbBuilder Table(string name, int rwFirst, int colFirst, int rwLast, int colLast, int headerRows = 1, int totalsRows = 0)
    {
        var t = new MemoryStream();
        Record(t, 343, [.. I32(rwFirst), .. I32(rwLast), .. I32(colFirst), .. I32(colLast), .. I32(0), .. I32(1),
            .. I32(headerRows), .. I32(totalsRows), .. I32(0), .. Enumerable.Repeat((byte)0xFF, 24), .. I32(0),
            .. Wide(name), .. Wide(name)]);
        Record(t, 344, []);
        _sheets[^1].Tables.Add(t);
        return this;
    }

    /// <summary>Ends the current sheet (BrtEndSheetData, BrtEndSheet) unless <paramref name="close"/> is false.</summary>
    public XlsbBuilder EndSheet(bool close = true)
    {
        if (close)
        {
            Record(_current!, 146, []);
            Record(_current!, 130, []);
        }
        _current = null;
        return this;
    }

    public MemoryStream Build()
    {
        var ms = new MemoryStream();
        using (var zip = new ZipArchive(ms, ZipArchiveMode.Create, true))
        {
            void Part(string name, byte[] bytes)
            {
                using var s = zip.CreateEntry(name).Open();
                s.Write(bytes);
            }
            void Xml(string name, string text) => Part(name, Encoding.UTF8.GetBytes(text));
            const string R = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
            Xml("[Content_Types].xml", """<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="bin" ContentType="application/vnd.ms-excel.sheet.binary.macroEnabled.main"/></Types>""");
            Xml("_rels/.rels", $"""<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="{R}/officeDocument" Target="xl/workbook.bin"/></Relationships>""");

            var wb = new MemoryStream();
            Record(wb, 131, []);                                     // BrtBeginBook
            Record(wb, 153, [.. U32(_date1904 ? 1u : 0u), .. new byte[4], .. Wide("")]); // BrtWbProp
            Record(wb, 143, []);                                     // BrtBeginBundleShs
            var rels = new StringBuilder($"""<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rIdS" Type="{R}/styles" Target="styles.bin"/><Relationship Id="rIdT" Type="{R}/sharedStrings" Target="sharedStrings.bin"/>""");
            for (int i = 0; i < _sheets.Count; i++)
            {
                var (name, state, sheet, tables) = _sheets[i];
                Record(wb, 156, [.. U32((uint)state), .. U32((uint)i + 1), .. Wide($"rId{i + 1}"), .. Wide(name)]);
                rels.Append($"""<Relationship Id="rId{i + 1}" Type="{R}/worksheet" Target="worksheets/sheet{i + 1}.bin"/>""");
                Part($"xl/worksheets/sheet{i + 1}.bin", sheet.ToArray());
                if (tables.Count > 0)
                {
                    var sheetRels = new StringBuilder("""<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">""");
                    for (int t = 0; t < tables.Count; t++)
                    {
                        string target = $"table{i + 1}_{t + 1}.bin";
                        sheetRels.Append($"""<Relationship Id="rIdT{t}" Type="{R}/table" Target="../tables/{target}"/>""");
                        Part($"xl/tables/{target}", tables[t].ToArray());
                    }
                    Xml($"xl/worksheets/_rels/sheet{i + 1}.bin.rels", sheetRels + "</Relationships>");
                }
            }
            Record(wb, 144, []);                                     // BrtEndBundleShs
            Record(wb, 132, []);                                     // BrtEndBook
            Part("xl/workbook.bin", wb.ToArray());
            Xml("xl/_rels/workbook.bin.rels", rels + "</Relationships>");

            var styles = new MemoryStream();
            Record(styles, 278, []);                                 // BrtBeginStyleSheet
            Record(styles, 615, I32(_formats.Count));                // BrtBeginFmts
            foreach (var (id, code) in _formats) Record(styles, 44, [(byte)id, (byte)(id >> 8), .. Wide(code)]);
            Record(styles, 616, []);
            Record(styles, 626, I32(1));                             // BrtBeginCellStyleXFs: a style XF that is not a cell format
            Record(styles, 47, [0xFF, 0xFF, 14, 0, .. new byte[12]]);
            Record(styles, 627, []);
            Record(styles, 617, I32(_xfFormats.Count));              // BrtBeginCellXFs
            foreach (int f in _xfFormats) Record(styles, 47, [0, 0, (byte)f, (byte)(f >> 8), .. new byte[12]]);
            Record(styles, 618, []);
            Record(styles, 279, []);
            Part("xl/styles.bin", styles.ToArray());

            var sst = new MemoryStream();
            Record(sst, 159, [.. I32(_strings.Count), .. I32(_strings.Count)]); // BrtBeginSst
            foreach (var s in _strings) Record(sst, 19, [0, .. Wide(s)]);
            Record(sst, 160, []);
            Part("xl/sharedStrings.bin", sst.ToArray());
        }
        ms.Position = 0;
        return ms;
    }

    public static void Record(Stream s, int type, ReadOnlySpan<byte> data)
    {
        if (type < 128) s.WriteByte((byte)type);
        else
        {
            s.WriteByte((byte)(0x80 | (type & 0x7F)));
            s.WriteByte((byte)(type >> 7));
        }
        int size = data.Length;
        do
        {
            byte b = (byte)(size & 0x7F);
            size >>= 7;
            s.WriteByte(size > 0 ? (byte)(b | 0x80) : b);
        } while (size > 0);
        s.Write(data);
    }

    public static byte[] I32(int v)
    {
        var b = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(b, v);
        return b;
    }

    public static byte[] U32(uint v)
    {
        var b = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(b, v);
        return b;
    }

    public static byte[] F64(double v)
    {
        var b = new byte[8];
        BinaryPrimitives.WriteDoubleLittleEndian(b, v);
        return b;
    }

    public static byte[] Wide(string s) => [.. I32(s.Length), .. Encoding.Unicode.GetBytes(s)];

    /// <summary>An RkNumber: an integer (fInt) or the high 30 bits of a double, optionally ×100 (fx100).</summary>
    public static uint RkInt(int value, bool x100 = false) => ((uint)(value << 2)) | 2u | (x100 ? 1u : 0u);

    public static uint RkFloat(double value, bool x100 = false) =>
        (uint)(BitConverter.DoubleToInt64Bits(value) >>> 32) & 0xFFFFFFFC | (x100 ? 1u : 0u);
}
