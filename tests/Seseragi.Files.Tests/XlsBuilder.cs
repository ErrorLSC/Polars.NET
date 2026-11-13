using System.Buffers.Binary;
using System.Text;

namespace Seseragi.Files.Tests;

/// <summary>
/// Builds Excel 97–2003 workbooks record by record for the tests: the cases no writer at hand produces (strings split
/// across Continue records with a change of width, formula strings, every RK variant, mini-stream and version 4
/// compound files, malformed structures). Only what the reader looks at is written, plus a font so that xlrd and
/// LibreOffice accept the files.
/// </summary>
internal sealed class XlsBuilder
{
    private readonly List<(string Name, int State, int Type, MemoryStream Sheet)> _sheets = [];
    private readonly List<(string Text, int Runs, int Ext)> _strings = [];
    private readonly List<(int Id, string Code)> _formats = [];
    private readonly List<int> _xfFormats = [0];
    private readonly List<byte[]> _globalsExtra = [];
    private bool _date1904;
    private MemoryStream? _current;
    private int _firstRow = int.MaxValue, _lastRow = -1, _firstCol = int.MaxValue, _lastCol = -1;

    /// <summary>The largest record data written before a Continue record (8224 in Excel; small values force splits).</summary>
    public int MaxRecord { get; init; } = 8224;

    public XlsBuilder Date1904()
    {
        _date1904 = true;
        return this;
    }

    /// <summary>A FILEPASS record (RC4) in the globals: the workbook claims to be encrypted.</summary>
    public XlsBuilder Encrypted()
    {
        _globalsExtra.Add(Record(0x002F, [1, 0, 1, 0, 1, 0, .. new byte[48]]));
        return this;
    }

    /// <summary>A cell format with a number format (a built-in id, or a custom code given an id ≥ 164); returns its XF index.</summary>
    public int Style(int formatId, string? code = null)
    {
        if (code is not null) _formats.Add((formatId, code));
        _xfFormats.Add(formatId);
        return _xfFormats.Count - 1;
    }

    /// <summary>A shared string, optionally with formatting runs and phonetic data (skipped by readers).</summary>
    public int SharedString(string s, int runs = 0, int ext = 0)
    {
        _strings.Add((s, runs, ext));
        return _strings.Count - 1;
    }

    /// <summary>Starts a sheet: state 0 visible, 1 hidden, 2 very hidden; type is BoundSheet8.dt (0 worksheet, 2 chart).</summary>
    public XlsBuilder Sheet(string name, int state = 0, int type = 0, bool dialog = false)
    {
        _current = new MemoryStream();
        _sheets.Add((name, state, type, _current));
        _firstRow = _firstCol = int.MaxValue;
        _lastRow = _lastCol = -1;
        Write(Bof(type == 2 ? 0x0020 : 0x0010));
        if (dialog) Write(Record(0x0081, [0x11, 0x04]));
        return this;
    }

    private void Track(int row, int firstCol, int lastCol)
    {
        _firstRow = Math.Min(_firstRow, row);
        _lastRow = Math.Max(_lastRow, row);
        _firstCol = Math.Min(_firstCol, firstCol);
        _lastCol = Math.Max(_lastCol, lastCol);
    }

    private static byte[] CellHead(int row, int col, int xf) => [.. U16(row), .. U16(col), .. U16(xf)];

    private XlsBuilder Cell(int type, int row, int col, byte[] data)
    {
        Track(row, col, col);
        Write(Record(type, data));
        return this;
    }

    public XlsBuilder Number(int row, int col, double value, int xf = 0) => Cell(0x0203, row, col, [.. CellHead(row, col, xf), .. F64(value)]);

    public XlsBuilder Rk(int row, int col, uint raw, int xf = 0) => Cell(0x027E, row, col, [.. CellHead(row, col, xf), .. U32(raw)]);

    public XlsBuilder MulRk(int row, int col, params (uint Raw, int Xf)[] cells)
    {
        Track(row, col, col + cells.Length - 1);
        Write(Record(0x00BD, [.. U16(row), .. U16(col), .. cells.SelectMany(RkRec), .. U16(col + cells.Length - 1)]));
        return this;

        static byte[] RkRec((uint Raw, int Xf) c) => [.. U16(c.Xf), .. U32(c.Raw)];
    }

    public XlsBuilder Blank(int row, int col) => Cell(0x0201, row, col, CellHead(row, col, 0));

    public XlsBuilder MulBlank(int row, int col, int count)
    {
        Track(row, col, col + count - 1);
        Write(Record(0x00BE, [.. U16(row), .. U16(col), .. Enumerable.Repeat((byte)0, 2 * count), .. U16(col + count - 1)]));
        return this;
    }

    public XlsBuilder LabelSst(int row, int col, int index, int xf = 0) => Cell(0x00FD, row, col, [.. CellHead(row, col, xf), .. U32((uint)index)]);

    /// <summary>A Label record: an inline XLUnicodeString (one-byte characters when they all fit).</summary>
    public XlsBuilder Label(int row, int col, string text) => Cell(0x0204, row, col, [.. CellHead(row, col, 0), .. UnicodeString(text)]);

    public XlsBuilder Bool(int row, int col, bool value) => Cell(0x0205, row, col, [.. CellHead(row, col, 0), (byte)(value ? 1 : 0), 0]);

    public XlsBuilder Error(int row, int col, byte code = 0x2A) => Cell(0x0205, row, col, [.. CellHead(row, col, 0), code, 1]);

    // Formula records: the cached value, flags, chn, then a formula of zero tokens (cce = 0).
    private XlsBuilder Formula(int row, int col, byte[] value, int xf) =>
        Cell(0x0006, row, col, [.. CellHead(row, col, xf), .. value, 0, 0, .. U32(0), 0, 0]);

    public XlsBuilder FormulaNumber(int row, int col, double value, int xf = 0) => Formula(row, col, F64(value), xf);

    public XlsBuilder FormulaBool(int row, int col, bool value) => Formula(row, col, [1, 0, (byte)(value ? 1 : 0), 0, 0, 0, 0xFF, 0xFF], 0);

    public XlsBuilder FormulaError(int row, int col, byte code = 0x07) => Formula(row, col, [2, 0, code, 0, 0, 0, 0xFF, 0xFF], 0);

    public XlsBuilder FormulaEmpty(int row, int col) => Formula(row, col, [3, 0, 0, 0, 0, 0, 0xFF, 0xFF], 0);

    /// <summary>A formula with a string result: the Formula record, optionally a ShrFmla record, then the String
    /// record, split into Continue records past <see cref="MaxRecord"/> (each piece picking its own width).</summary>
    public XlsBuilder FormulaString(int row, int col, string text, bool sharedFormula = false, bool omitString = false)
    {
        Formula(row, col, [0, 0, 0, 0, 0, 0, 0xFF, 0xFF], 0);
        if (sharedFormula) Write(Record(0x04BC, new byte[10]));
        if (omitString) return this;
        List<List<byte>> pieces = [[.. U16(text.Length)]];
        AppendChars(pieces, text, flagsAt: 2);
        Write(Record(0x0207, [.. pieces[0]]));
        foreach (var piece in pieces.Skip(1)) Write(Record(0x003C, [.. piece]));
        return this;
    }

    /// <summary>A record as given (unknown or malformed ones).</summary>
    public XlsBuilder Raw(int type, byte[] data)
    {
        Write(Record(type, data));
        return this;
    }

    /// <summary>An embedded chart's substream inside the sheet, with a number in its data cache.</summary>
    public XlsBuilder EmbeddedChart()
    {
        Write(Bof(0x0020));
        Write(Record(0x0203, [.. CellHead(0, 0, 0), .. F64(99)]));
        Write(Record(0x000A, []));
        return this;
    }

    /// <summary>Ends the sheet: Dimensions (unless left out), then EOF. The Dimensions record goes before the cells.</summary>
    public XlsBuilder EndSheet(bool dimensions = true)
    {
        var cells = _current!.ToArray();
        int bof = BinaryPrimitives.ReadUInt16LittleEndian(cells.AsSpan(2)) + 4;
        int head = bof + (cells.Length > bof && BinaryPrimitives.ReadUInt16LittleEndian(cells.AsSpan(bof)) == 0x0081 ? 6 : 0);
        _current.SetLength(0);
        _current.Write(cells.AsSpan(0, head));
        if (dimensions)
        {
            bool empty = _lastRow < 0;
            _current.Write(Record(0x0200, [.. U32(empty ? 0u : (uint)_firstRow), .. U32(empty ? 0u : (uint)_lastRow + 1),
                .. U16(empty ? 0 : _firstCol), .. U16(empty ? 0 : _lastCol + 1), 0, 0]));
        }
        _current.Write(cells.AsSpan(head));
        Write(Record(0x000A, []));
        return this;
    }

    private void Write(byte[] record) => _current!.Write(record);

    /// <summary>The "Workbook" stream: globals, then the sheets.</summary>
    public byte[] WorkbookStream(int biffVersion = 0x0600)
    {
        var globals = new MemoryStream();
        globals.Write(Bof(0x0005, biffVersion));
        foreach (var r in _globalsExtra) globals.Write(r);
        globals.Write(Record(0x0042, U16(1200)));             // CodePage: UTF-16
        globals.Write(Record(0x0022, U16(_date1904 ? 1 : 0))); // Date1904
        // Font (2.4.122): height, flags, colour, weight, escapement, underline, family, charset, reserved, name.
        globals.Write(Record(0x0031, [.. U16(200), 0, 0, .. U16(0x7FFF), .. U16(400), 0, 0, 0, 0, 0, 0, .. ShortString("Arial")]));
        foreach (var (id, code) in _formats) globals.Write(Record(0x041E, [.. U16(id), .. UnicodeString(code)]));
        // XF (2.4.353): ifnt, ifmt, flags (cell XF: fStyle 0), then 14 bytes of alignment, borders and fill.
        foreach (int fmt in _xfFormats) globals.Write(Record(0x00E0, [.. U16(0), .. U16(fmt), .. U16(0x0001), .. new byte[14]]));
        int boundAt = (int)globals.Length;
        foreach (var (name, state, type, _) in _sheets)
            globals.Write(Record(0x0085, [.. U32(0), (byte)state, (byte)type, .. ShortString(name)]));
        if (_strings.Count > 0) WriteSst(globals);
        globals.Write(Record(0x000A, []));

        var stream = new MemoryStream();
        stream.Write(globals.ToArray());
        var bytes = globals.ToArray();
        // lbPlyPos: each sheet's BOF offset, patched into its BoundSheet8 record.
        int at = boundAt;
        foreach (var (name, _, _, sheet) in _sheets)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(at + 4), (uint)stream.Length);
            stream.Write(sheet.ToArray());
            at += 4 + 6 + ShortString(name).Length;
        }
        var result = stream.ToArray();
        bytes.CopyTo(result, 0);
        return result;
    }

    /// <summary>The workbook as a compound file.</summary>
    public MemoryStream Build(int version = 3, string streamName = "Workbook") =>
        new(CompoundFileBuilder.Pack([(streamName, WorkbookStream())], version));

    // SST (2.4.265) and Continue records: a string's header is never split; its characters break into a Continue that
    // starts with a width byte; runs and phonetic data break without one.
    private void WriteSst(Stream into)
    {
        List<List<byte>> pieces = [[.. U32((uint)_strings.Count), .. U32((uint)_strings.Count)]];
        foreach (var (text, runs, ext) in _strings)
        {
            int header = 3 + (runs > 0 ? 2 : 0) + (ext > 0 ? 4 : 0);
            if (pieces[^1].Count + header > MaxRecord) pieces.Add([]);
            var cur = pieces[^1];
            cur.AddRange(U16(text.Length));
            int flagsAt = cur.Count;
            cur.Add((byte)((runs > 0 ? 8 : 0) | (ext > 0 ? 4 : 0)));
            if (runs > 0) cur.AddRange(U16(runs));
            if (ext > 0) cur.AddRange(U32((uint)ext));
            AppendChars(pieces, text, flagsAt);
            // Formatting runs (FormatRun: ich, ifnt) break between runs, as Excel writes them; phonetic data anywhere.
            for (int i = 0; i < runs; i++)
            {
                if (pieces[^1].Count + 4 > MaxRecord) pieces.Add([]);
                pieces[^1].AddRange([.. U16(i), .. U16(0)]);
            }
            for (int i = 0; i < ext; i++)
            {
                if (pieces[^1].Count >= MaxRecord) pieces.Add([]);
                pieces[^1].Add(0x5A);
            }
        }
        into.Write(Record(0x00FC, [.. pieces[0]]));
        foreach (var piece in pieces.Skip(1)) into.Write(Record(0x003C, [.. piece]));
    }

    // The characters of a string whose width byte is at pieces[^1][flagsAt] (OR-ed in); a break starts a Continue
    // with a width byte of its own, each piece one-byte when all its characters fit.
    private void AppendChars(List<List<byte>> pieces, string text, int flagsAt)
    {
        var cur = pieces[^1];
        if (cur.Count == flagsAt) cur.Add(0); // FormulaString: the width byte follows the count
        int i = 0;
        bool first = true;
        while (true)
        {
            int room = MaxRecord - cur.Count;
            int take = 0;
            bool wide = false;
            // Characters that fit in this piece: one byte each while all fit Latin-1, else two.
            for (int k = i; k < text.Length; k++)
            {
                bool w = wide || text[k] > 0xFF;
                if ((k - i + 1) * (w ? 2 : 1) > room) break;
                wide = w;
                take = k - i + 1;
            }
            if (take == 0 && i < text.Length && !first)
                throw new InvalidOperationException("MaxRecord too small for a character.");
            if (wide) cur[flagsAt] |= 1;
            foreach (char c in text.AsSpan(i, take))
            {
                if (wide) cur.AddRange(U16(c));
                else cur.Add((byte)c);
            }
            i += take;
            if (i >= text.Length) return;
            cur = [0];
            pieces.Add(cur);
            flagsAt = 0;
            first = false;
        }
    }

    private static byte[] Bof(int type, int version = 0x0600) =>
        Record(0x0809, [.. U16(version), .. U16(type), .. U16(0x0DBB), .. U16(0x07CC), .. U32(0x000100C1), .. U32(0x00000006)]);

    public static byte[] Record(int type, byte[] data) => [.. U16(type), .. U16(data.Length), .. data];

    public static byte[] UnicodeString(string s) => s.All(c => c <= 0xFF)
        ? [.. U16(s.Length), 0, .. s.Select(c => (byte)c)]
        : [.. U16(s.Length), 1, .. Encoding.Unicode.GetBytes(s)];

    private static byte[] ShortString(string s) => s.All(c => c <= 0xFF)
        ? [(byte)s.Length, 0, .. s.Select(c => (byte)c)]
        : [(byte)s.Length, 1, .. Encoding.Unicode.GetBytes(s)];

    public static uint RkInt(int value, bool x100 = false) => (uint)(value << 2) | 2u | (x100 ? 1u : 0u);

    public static uint RkFloat(double value, bool x100 = false) =>
        (uint)(BitConverter.DoubleToInt64Bits(value) >> 32) & 0xFFFFFFFC | (x100 ? 1u : 0u);

    public static byte[] U16(int v) => [(byte)v, (byte)(v >> 8)];

    public static byte[] U32(uint v) => [(byte)v, (byte)(v >> 8), (byte)(v >> 16), (byte)(v >> 24)];

    private static byte[] F64(double v)
    {
        var b = new byte[8];
        BinaryPrimitives.WriteDoubleLittleEndian(b, v);
        return b;
    }
}

/// <summary>
/// Writes compound files ([MS-CFB]) holding streams at the root: data sectors, the mini stream for streams under
/// 4096 bytes, the mini FAT, the directory (the root's children as a right-leaning tree in name order), the FAT and,
/// past 109 FAT sectors, DIFAT sectors. A hook can change the FAT before it is written (to build broken files).
/// </summary>
internal static class CompoundFileBuilder
{
    private const uint End = 0xFFFFFFFE, Free = 0xFFFFFFFF, FatSect = 0xFFFFFFFD, DifSect = 0xFFFFFFFC;

    public static byte[] Pack(IReadOnlyList<(string Name, byte[] Data)> streams, int version = 3, Action<uint[]>? patchFat = null, bool noMiniStream = false)
    {
        int shift = version == 4 ? 12 : 9;
        int size = 1 << shift;
        List<byte[]> sectors = [];
        List<uint> fat = [];

        uint Allocate(byte[] data)
        {
            if (data.Length == 0) return End;
            uint first = (uint)sectors.Count;
            for (int at = 0; at < data.Length; at += size)
            {
                var s = new byte[size];
                data.AsSpan(at, Math.Min(size, data.Length - at)).CopyTo(s);
                sectors.Add(s);
                fat.Add(at + size >= data.Length ? End : (uint)sectors.Count);
            }
            return first;
        }

        // Small streams in the mini stream (64-byte sectors chained by the mini FAT).
        var mini = new MemoryStream();
        List<uint> miniFat = [];
        var starts = new uint[streams.Count];
        for (int i = 0; i < streams.Count; i++)
        {
            var data = streams[i].Data;
            if (data.Length >= 4096 || noMiniStream)
            {
                starts[i] = Allocate(data);
                continue;
            }
            starts[i] = data.Length == 0 ? End : (uint)miniFat.Count;
            for (int at = 0; at < data.Length; at += 64)
            {
                var s = new byte[64];
                data.AsSpan(at, Math.Min(64, data.Length - at)).CopyTo(s);
                mini.Write(s);
                miniFat.Add(at + 64 >= data.Length ? End : (uint)miniFat.Count + 1);
            }
        }
        uint miniStart = Allocate(mini.ToArray());
        uint miniFatStart = Allocate([.. miniFat.SelectMany(XlsBuilder.U32)]);

        // Directory: the root, then the streams; the root's child is the smallest name, each entry's right sibling
        // the next (CFB order: shorter names first, then by upper case).
        var order = Enumerable.Range(0, streams.Count)
            .OrderBy(i => streams[i].Name.Length).ThenBy(i => streams[i].Name.ToUpperInvariant(), StringComparer.Ordinal).ToArray();
        var dir = new MemoryStream();
        dir.Write(Entry("Root Entry", 5, Free, Free, streams.Count > 0 ? (uint)order[0] + 1 : Free, miniStart, mini.Length));
        for (int i = 0; i < streams.Count; i++)
        {
            int pos = Array.IndexOf(order, i);
            uint right = pos + 1 < order.Length ? (uint)order[pos + 1] + 1 : Free;
            dir.Write(Entry(streams[i].Name, 2, Free, right, Free, starts[i], streams[i].Data.Length));
        }
        while (dir.Length % size != 0) dir.Write(Entry("", 0, Free, Free, Free, 0, 0));
        uint dirStart = Allocate(dir.ToArray());

        // FAT sectors (and DIFAT sectors past 109) cover every sector, themselves included.
        int perFat = size / 4, perDifat = size / 4 - 1;
        int fatCount = 0, difatCount = 0;
        while (true)
        {
            int total = sectors.Count + fatCount + difatCount;
            int needFat = (total + perFat - 1) / perFat;
            int needDifat = Math.Max(0, (needFat - 109 + perDifat - 1) / perDifat);
            if (needFat == fatCount && needDifat == difatCount) break;
            fatCount = needFat;
            difatCount = needDifat;
        }
        uint fatFirst = (uint)sectors.Count;
        for (int i = 0; i < fatCount; i++) fat.Add(FatSect);
        uint difatFirst = (uint)(sectors.Count + fatCount);
        for (int i = 0; i < difatCount; i++) fat.Add(DifSect);
        var table = new uint[fatCount * perFat];
        Array.Fill(table, Free);
        fat.CopyTo(table);
        patchFat?.Invoke(table);
        for (int i = 0; i < fatCount; i++)
        {
            var s = new byte[size];
            for (int k = 0; k < perFat; k++) BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(4 * k), table[i * perFat + k]);
            sectors.Add(s);
        }
        for (int d = 0; d < difatCount; d++)
        {
            var s = new byte[size];
            for (int k = 0; k < perDifat; k++)
            {
                int index = 109 + d * perDifat + k;
                BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(4 * k), index < fatCount ? fatFirst + (uint)index : Free);
            }
            BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(size - 4), d + 1 < difatCount ? difatFirst + (uint)d + 1 : End);
            sectors.Add(s);
        }

        var header = new byte[size];
        ReadOnlySpan<byte> sig = [0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1];
        sig.CopyTo(header);
        void H16(int at, int v) => BinaryPrimitives.WriteUInt16LittleEndian(header.AsSpan(at), (ushort)v);
        void H32(int at, uint v) => BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(at), v);
        H16(24, 0x3E);
        H16(26, version);
        H16(28, 0xFFFE);
        H16(30, shift);
        H16(32, 6);
        H32(40, version == 4 ? (uint)(dir.Length / size) : 0);
        H32(44, (uint)fatCount);
        H32(48, dirStart);
        H32(56, 4096);
        H32(60, miniFat.Count > 0 ? miniFatStart : End);
        H32(64, (uint)((miniFat.Count * 4 + size - 1) / size));
        H32(68, difatCount > 0 ? difatFirst : End);
        H32(72, (uint)difatCount);
        for (int i = 0; i < 109; i++) H32(76 + 4 * i, i < fatCount ? fatFirst + (uint)i : Free);

        var file = new MemoryStream();
        file.Write(header);
        foreach (var s in sectors) file.Write(s);
        return file.ToArray();

        static byte[] Entry(string name, byte type, uint left, uint right, uint child, uint start, long length)
        {
            var e = new byte[128];
            Encoding.Unicode.GetBytes(name).CopyTo(e, 0);
            BinaryPrimitives.WriteUInt16LittleEndian(e.AsSpan(64), (ushort)(name.Length == 0 ? 0 : 2 * name.Length + 2));
            e[66] = type;
            e[67] = 1; // black
            BinaryPrimitives.WriteUInt32LittleEndian(e.AsSpan(68), left);
            BinaryPrimitives.WriteUInt32LittleEndian(e.AsSpan(72), right);
            BinaryPrimitives.WriteUInt32LittleEndian(e.AsSpan(76), child);
            BinaryPrimitives.WriteUInt32LittleEndian(e.AsSpan(116), type == 0 ? 0 : start);
            BinaryPrimitives.WriteUInt64LittleEndian(e.AsSpan(120), (ulong)length);
            return e;
        }
    }
}
