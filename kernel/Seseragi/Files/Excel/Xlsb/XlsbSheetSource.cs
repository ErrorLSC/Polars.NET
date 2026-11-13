using Kaminari.Seseragi.Files.Excel.Reading;

namespace Kaminari.Seseragi.Files.Excel.Xlsb;

/// <summary>
/// Streams the cells of a binary worksheet part. Records are parsed once buffered whole; a row is handed out when
/// the next BrtRowHdr (or BrtEndSheetData) is reached, so a refill only happens between records. Cell values: numbers
/// (BrtCellRk, BrtCellReal, BrtFmlaNum), booleans, errors (BrtCellError, BrtFmlaError), strings (BrtCellSt,
/// BrtFmlaString, BrtCellRString, BrtCellIsst); formulas give their cached value. Records the reader does not use
/// (row formatting, column spans, alternate-content and future-record blocks, [MS-XLSB] 2.1.6) are skipped. The
/// one-byte types 12 to 18, unassigned among the cell records, are refused rather than dropped: a cell kind the
/// specification does not describe must not vanish silently.
/// </summary>
internal sealed class XlsbSheetSource : SheetSource
{
    private readonly Stream _stream;
    private readonly ChunkReader _reader;
    private readonly ExcelValueKind[] _styles;
    private readonly SharedStringStore? _sharedStrings;
    private int _row = -1;       // the row whose cells are being collected (-1 before the first BrtRowHdr)
    private bool _collecting;    // the RowBuffer holds the cells of _row
    private bool _done;

    private XlsbSheetSource(Stream stream, ExcelValueKind[] styles, SharedStringStore? sharedStrings)
    {
        _stream = stream;
        _reader = new ChunkReader(stream, xml: false);
        _styles = styles;
        _sharedStrings = sharedStrings;
    }

    public static async ValueTask<XlsbSheetSource> OpenAsync(Stream stream, ExcelValueKind[] styles,
        SharedStringStore? sharedStrings, bool useAsync, CancellationToken cancellationToken)
    {
        var source = new XlsbSheetSource(stream, styles, sharedStrings);
        try
        {
            while (!TryReadHead())
            {
                bool more = useAsync ? await source._reader.FillAsync(cancellationToken).ConfigureAwait(false) : source._reader.Fill();
                if (!more)
                {
                    if (TryReadHead()) break;
                    throw new ExcelFormatException("The binary worksheet has no sheet data.");
                }
            }
            return source;
        }
        catch
        {
            source.Dispose();
            throw;
        }

        // Records up to and including BrtBeginSheetData; BrtWsDim gives the dimension. False when more input is needed.
        bool TryReadHead()
        {
            var buf = source._reader.Buffered;
            int pos = 0;
            while (Biff12.TryReadHeader(buf[pos..], out int type, out int size, out int header))
            {
                var data = buf.Slice(pos + header, size);
                pos += header + size;
                if (type == Brt.WsDim)
                {
                    // UncheckedRfX: rwFirst, rwLast, colFirst, colLast.
                    var x = new RecordReader(data, type);
                    int r1 = x.I32(), r2 = x.I32(), c1 = x.I32(), c2 = x.I32();
                    if (r1 >= 0 && r2 >= r1 && c1 >= 0 && c2 >= c1) source.Dimension = (r1, c1, r2, c2);
                }
                else if (type == Brt.BeginSheetData)
                {
                    source._reader.Consume(pos);
                    return true;
                }
            }
            source._reader.Consume(pos);
            return false;
        }
    }

    private enum Step : byte { Row, NeedMore, End }

    public override bool ReadRow(RowBuffer row)
    {
        while (true)
        {
            var step = TryReadRow(row);
            if (step != Step.NeedMore) return step == Step.Row;
            if (!_reader.Fill()) return Finish(row);
        }
    }

    public override async ValueTask<bool> ReadRowAsync(RowBuffer row, CancellationToken cancellationToken)
    {
        while (true)
        {
            var step = TryReadRow(row);
            if (step != Step.NeedMore) return step == Step.Row;
            if (!await _reader.FillAsync(cancellationToken).ConfigureAwait(false)) return Finish(row);
        }
    }

    // The part ended without BrtEndSheetData: what is left must be a whole number of records.
    private bool Finish(RowBuffer row)
    {
        var step = TryReadRow(row);
        if (step == Step.Row) return true;
        if (!_reader.Buffered.IsEmpty) throw new ExcelFormatException("The binary worksheet ends inside a record.");
        _done = true;
        if (_collecting && row.Count > 0)
        {
            _collecting = false;
            return true;
        }
        return false;
    }

    private Step TryReadRow(RowBuffer row)
    {
        if (_done) return Step.End;
        var buf = _reader.Buffered;
        int pos = 0;
        try
        {
            while (Biff12.TryReadHeader(buf[pos..], out int type, out int size, out int header))
            {
                var data = buf.Slice(pos + header, size);
                switch (type)
                {
                    case Brt.RowHdr:
                    {
                        if (_collecting && row.Count > 0)
                        {
                            _collecting = false;
                            return Step.Row; // this BrtRowHdr is read again for the next row
                        }
                        // BrtRowHdr (2.4.770): rw first. Rows come in increasing order (a repeated header continues
                        // its row).
                        int rw = new RecordReader(data, type).I32();
                        if (rw < 0 || rw >= ExcelCellReference.MaxRows || rw < _row)
                            throw new ExcelFormatException($"Invalid or out-of-order row number {rw} in a binary worksheet.");
                        if (!_collecting || rw != _row)
                        {
                            row.Clear();
                            row.SharedStrings = _sharedStrings;
                            row.Row = rw;
                        }
                        _row = rw;
                        _collecting = true;
                        break;
                    }
                    case Brt.EndSheetData:
                        pos += header + size;
                        _done = true;
                        if (_collecting && row.Count > 0)
                        {
                            _collecting = false;
                            return Step.Row;
                        }
                        return Step.End;
                    case >= 12 and <= 18:
                        throw new ExcelFormatException($"A binary worksheet holds a record of type {type}, which [MS-XLSB] does not assign (a cell kind this reader does not know).");
                    case >= Brt.CellBlank and <= Brt.FmlaError or Brt.CellRString:
                        if (!_collecting) throw new ExcelFormatException("A binary worksheet has a cell before its first row.");
                        ParseCell(type, data, row);
                        break;
                }
                pos += header + size;
            }
            return Step.NeedMore;
        }
        finally
        {
            _reader.Consume(pos);
        }

        // Cell (2.5.10): column (4 bytes), iStyleRef (24 bits), flags (8 bits); then the value.
        void ParseCell(int type, ReadOnlySpan<byte> data, RowBuffer row)
        {
            var x = new RecordReader(data, type);
            uint column = x.U32();
            uint styleAndFlags = x.U32();
            if (column >= ExcelCellReference.MaxColumns)
                throw new ExcelFormatException($"Invalid column {column} in a binary worksheet.");
            int style = (int)(styleAndFlags & 0xFFFFFF);
            var kind = (uint)style < (uint)_styles.Length ? _styles[style] : ExcelValueKind.Number;
            switch (type)
            {
                case Brt.CellBlank:
                    return;
                case Brt.CellRk:
                    Number(row, (int)column, Rk(x.U32()), kind);
                    return;
                case Brt.CellReal or Brt.FmlaNum:
                    Number(row, (int)column, x.F64(), kind);
                    return;
                case Brt.CellBool or Brt.FmlaBool:
                {
                    ref Cell cell = ref row.Add();
                    cell.Column = (int)column;
                    cell.Type = CellType.Boolean;
                    cell.Number = x.U8() != 0 ? 1 : 0;
                    return;
                }
                case Brt.CellError or Brt.FmlaError:
                {
                    x.U8(); // BErr: every error reads as null
                    ref Cell cell = ref row.Add();
                    cell.Column = (int)column;
                    cell.Type = CellType.Error;
                    return;
                }
                case Brt.CellSt or Brt.FmlaString:
                    Text(row, (int)column, x.WideStringBytes());
                    return;
                case Brt.CellRString:
                    x.Skip(1); // fRichStr, fExtStr: runs and phonetic text follow the string
                    Text(row, (int)column, x.WideStringBytes());
                    return;
                case Brt.CellIsst:
                {
                    uint index = x.U32();
                    // Empty text is an empty cell; an index beyond the table fails when the value is read.
                    if (_sharedStrings is not null && index < (uint)_sharedStrings.Count && _sharedStrings.Get((int)index).IsEmpty) return;
                    if (index > int.MaxValue) throw new ExcelFormatException($"Invalid shared string index {index}.");
                    ref Cell cell = ref row.Add();
                    cell.Column = (int)column;
                    cell.Type = CellType.SharedString;
                    cell.Offset = (int)index;
                    return;
                }
            }
        }
    }

    private static void Number(RowBuffer row, int column, double value, ExcelValueKind kind)
    {
        ref Cell cell = ref row.Add();
        cell.Column = column;
        cell.Type = CellType.Number;
        cell.Number = value;
        cell.Kind = kind;
    }

    private static void Text(RowBuffer row, int column, ReadOnlySpan<byte> utf16)
    {
        if (utf16.IsEmpty) return; // empty text is an empty cell
        int start = row.TextLength;
        int n = WideText.ToUtf8(utf16, row.ReserveText(WideText.MaxUtf8(utf16.Length)));
        row.CommitText(n);
        ref Cell cell = ref row.Add();
        cell.Column = column;
        cell.Type = CellType.String;
        cell.Offset = start;
        cell.Length = n;
    }

    /// <summary>
    /// RkNumber (2.5.123): bit 0 fx100 (divide by 100), bit 1 fInt (a 30-bit signed integer, else the high 30 bits of
    /// an IEEE double).
    /// </summary>
    internal static double Rk(uint raw)
    {
        double value = (raw & 2) != 0
            ? (int)raw >> 2
            : BitConverter.Int64BitsToDouble((long)(raw & 0xFFFFFFFC) << 32);
        return (raw & 1) != 0 ? value / 100 : value;
    }

    public override void Dispose()
    {
        _reader.Dispose();
        _stream.Dispose();
    }
}
