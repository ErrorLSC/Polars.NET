using System.Diagnostics.CodeAnalysis;
using System.Text;
using Kaminari.Seseragi.Files.Excel.Packaging;
using Kaminari.Seseragi.Files.Excel.Reading;
using Kaminari.Seseragi.Files.Excel.Xlsb;

namespace Kaminari.Seseragi.Files.Excel.Xls;

/// <summary>
/// An Excel 97–2003 workbook ([MS-XLS], BIFF8): the "Workbook" stream of a compound file, a globals substream
/// followed by one substream per sheet. Opening reads the globals (sheets, the 1904 flag, number formats and cell
/// formats) and checks which worksheets are dialog sheets; the shared strings are read on first use.
/// </summary>
internal sealed class XlsWorkbook : WorkbookSource
{
    private readonly CompoundFile _file;
    private readonly long[] _offsets;      // stream offset of each sheet's BOF
    private readonly ExcelValueKind[] _kinds; // by XF index (cell and style XFs alike, [MS-XLS] 2.5.168)
    private readonly long _sstOffset;      // -1 without an SST record
    private SharedStringStore? _sharedStrings;

    private XlsWorkbook(CompoundFile file, string streamName, bool date1904, List<ExcelSheetInfo> sheets, long[] offsets, ExcelValueKind[] kinds, long sstOffset)
    {
        _file = file;
        _streamName = streamName;
        Date1904 = date1904;
        Sheets = sheets;
        _offsets = offsets;
        _kinds = kinds;
        _sstOffset = sstOffset;
    }

    public override ExcelFormat Format => ExcelFormat.Xls;

    public override bool Date1904 { get; }

    public override IReadOnlyList<ExcelSheetInfo> Sheets { get; }

    public override IReadOnlyList<ExcelTableInfo> Tables => []; // BIFF8 has no tables (list objects)

    private readonly string _streamName;

    /// <summary>The workbook stream of a compound file: "Workbook" ([MS-XLS] 2.1.7.20), else "Book", the BIFF5 name,
    /// which some writers also give BIFF8 streams (the BOF record tells the version). Null when there is neither.</summary>
    public static string? FindStream(CompoundFile file) =>
        file.HasStream("Workbook") ? "Workbook" : file.HasStream("Book") ? "Book" : null;

    public static async ValueTask<XlsWorkbook> OpenAsync(CompoundFile file, string streamName, bool useAsync, CancellationToken ct)
    {
        using var stream = await file.OpenStreamAsync(streamName, useAsync, ct).ConfigureAwait(false);
        using var records = new BiffRecords(stream);
        if (!await NextAsync(records).ConfigureAwait(false)) throw new ExcelFormatException("The workbook stream is empty.");
        CheckBof(records, 0x0005, "workbook globals");

        bool date1904 = false;
        Dictionary<int, ExcelValueKind> formats = [];
        List<int> xfFormats = [];
        List<(string Name, ExcelSheetVisibility Visibility, int Type, long Offset)> bound = [];
        long sst = -1;
        bool ended = false;
        while (!ended && await NextAsync(records).ConfigureAwait(false))
        {
            var x = new RecordReader(records.Data, records.Type);
            switch (records.Type)
            {
                case Rt.Eof:
                    ended = true;
                    break;
                case Rt.FilePass:
                    throw new ExcelFormatException(
                        "The workbook is encrypted, which is not supported. Excel also encrypts .xls workbooks whose structure or "
                        + "sheets are only protected, with a built-in password: remove the protection or save as .xlsx.");
                case Rt.Date1904:
                    date1904 = (x.U16() & 1) != 0;
                    break;
                case Rt.Format:
                {
                    // A format whose string does not fit its record (some writers garble the count) stays a
                    // plain number format.
                    int id = x.U16();
                    if (TryUnicodeString(ref x, wideCount: true, out string? code)) formats[id] = ExcelNumberFormat.Classify(code);
                    break;
                }
                case Rt.Xf:
                    x.Skip(2); // ifnt
                    xfFormats.Add(x.U16());
                    break;
                case Rt.BoundSheet8:
                {
                    long offset = x.U32();
                    int state = x.U8() & 3;
                    int type = x.U8();
                    if (!TryUnicodeString(ref x, wideCount: false, out string? name)) throw new ExcelFormatException("A sheet name does not fit its BoundSheet8 record.");
                    var visibility = state switch
                    {
                        1 => ExcelSheetVisibility.Hidden,
                        2 => ExcelSheetVisibility.VeryHidden,
                        _ => ExcelSheetVisibility.Visible,
                    };
                    bound.Add((name, visibility, type, offset));
                    break;
                }
                case Rt.Sst when sst < 0:
                    sst = records.Position;
                    break;
            }
        }
        if (!ended) throw new ExcelFormatException("The workbook globals end without an EOF record.");

        ExcelValueKind[] kinds = [.. xfFormats.Select(id => formats.TryGetValue(id, out var kind) ? kind : ExcelNumberFormat.BuiltIn(id))];
        List<ExcelSheetInfo> sheets = [];
        foreach (var (name, visibility, type, offset) in bound)
        {
            // dt (2.4.28): 0 worksheet or dialog sheet (told apart by WsBool), 1 macro sheet, 2 chart sheet, 6 VBA module.
            var kind = type switch
            {
                0 => await IsDialogAsync(stream, offset, name).ConfigureAwait(false) ? ExcelSheetKind.Other : ExcelSheetKind.Worksheet,
                2 => ExcelSheetKind.Chartsheet,
                _ => ExcelSheetKind.Other,
            };
            sheets.Add(new(name, sheets.Count, visibility, kind));
        }
        return new(file, streamName, date1904, sheets, [.. bound.Select(b => b.Offset)], kinds, sst);

        async ValueTask<bool> NextAsync(BiffRecords r) => useAsync ? await r.NextAsync(ct).ConfigureAwait(false) : r.Next();

        // The sheet's WsBool (2.4.351) has fDialog (bit 4) set for a dialog sheet; it comes before the cells.
        async ValueTask<bool> IsDialogAsync(Stream workbook, long offset, string name)
        {
            if (offset >= workbook.Length) throw new ExcelFormatException($"Sheet '{name}' starts past the end of the workbook stream.");
            workbook.Position = offset;
            using var r = new BiffRecords(workbook, 4096); // WsBool comes within the first records
            if (!await NextAsync(r).ConfigureAwait(false)) return false;
            CheckBof(r, 0x0010, $"sheet '{name}'");
            while (await NextAsync(r).ConfigureAwait(false))
            {
                switch (r.Type)
                {
                    case Rt.WsBool:
                        return r.Size >= 1 && (r.Data[0] & 0x10) != 0;
                    case Rt.Dimensions or Rt.Eof or Rt.Bof:
                        return false;
                }
            }
            return false;
        }
    }

    /// <summary>
    /// Checks that the record is a BIFF8 BOF ([MS-XLS] 2.4.21: vers 0x0600) of the given substream type; older BIFF
    /// versions are named in the error.
    /// </summary>
    internal static void CheckBof(BiffRecords records, int substream, string what)
    {
        switch (records.Type)
        {
            case Rt.Bof2 or Rt.Bof3 or Rt.Bof4:
                throw OlderBiff("BIFF2–BIFF4 (Excel 2.x to 4.0)");
            case Rt.Bof:
                var x = new RecordReader(records.Data, records.Type);
                int version = x.U16();
                int type = x.U16();
                if (version == 0x0500) throw OlderBiff("BIFF5 (Excel 5.0 or 95)");
                if (version != 0x0600) throw new ExcelFormatException($"The {what} substream has an unknown BIFF version 0x{version:X4}.");
                if (type != substream) throw new ExcelFormatException($"The {what} substream has the wrong type (0x{type:X4}).");
                return;
            default:
                throw new ExcelFormatException($"The {what} substream does not start with a BOF record.");
        }
    }

    internal static ExcelFormatException OlderBiff(string version) =>
        new($"The workbook is in the {version} format; only Excel 97–2003 (BIFF8) .xls workbooks are supported. Save it in a newer format.");

    /// <summary>
    /// An XLUnicodeString (2.5.294: 16-bit count) or ShortXLUnicodeString (2.5.240: 8-bit count) inside one record:
    /// the count, a flags byte whose low bit means two-byte characters, the characters.
    /// </summary>
    internal static bool TryUnicodeString(ref RecordReader x, bool wideCount, [NotNullWhen(true)] out string? text)
    {
        text = null;
        if (x.Remaining < (wideCount ? 3 : 2)) return false;
        int chars = wideCount ? x.U16() : x.U8();
        bool wide = (x.U8() & 1) != 0;
        int bytes = wide ? 2 * chars : chars;
        if (x.Remaining < bytes) return false;
        var data = x.Bytes(bytes);
        text = wide ? Encoding.Unicode.GetString(data) : Encoding.Latin1.GetString(data);
        return true;
    }

    public override async ValueTask<SheetSource> OpenSheetAsync(int index, bool useAsync, CancellationToken cancellationToken)
    {
        if (_sharedStrings is null && _sstOffset >= 0)
        {
            using var sstStream = await _file.OpenStreamAsync(_streamName, useAsync, cancellationToken).ConfigureAwait(false);
            _sharedStrings = await XlsSharedStrings.LoadAsync(sstStream, _sstOffset, useAsync, cancellationToken).ConfigureAwait(false);
        }
        var stream = await _file.OpenStreamAsync(_streamName, useAsync, cancellationToken).ConfigureAwait(false);
        try
        {
            if (_offsets[index] >= stream.Length) throw new ExcelFormatException($"Sheet '{Sheets[index].Name}' starts past the end of the workbook stream.");
            stream.Position = _offsets[index];
            return await XlsSheetSource.OpenAsync(stream, Sheets[index].Name, _kinds, _sharedStrings, useAsync, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await stream.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    public override void Dispose()
    {
        _sharedStrings?.Dispose();
        _file.Dispose();
    }
}
