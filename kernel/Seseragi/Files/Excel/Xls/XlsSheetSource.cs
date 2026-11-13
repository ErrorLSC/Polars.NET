using Kaminari.Seseragi.Files.Excel.Reading;
using Kaminari.Seseragi.Files.Excel.Xlsb;

namespace Kaminari.Seseragi.Files.Excel.Xls;

/// <summary>
/// Streams the cells of a worksheet substream ([MS-XLS] 2.1.7.20.5). There are no row records to wait for: a row is
/// handed out when a cell of a later row (or the substream's EOF) arrives, and that cell's record is read again for
/// the next row. Cell values: numbers (Number, RK, MulRk), strings (LabelSst, and Label, which [MS-XLS] describes
/// for charts only but other writers put in worksheets), booleans and errors (BoolErr), and the cached results of
/// formulas, whose string results follow in a String record (after an optional Array, Table or ShrFmla record) and
/// may run on into Continue records. Blank and MulBlank add no cells. Cells must come in increasing row order and,
/// within a row, increasing column order, as Excel writes them; cells out of order are refused. Substreams nested
/// in the sheet (embedded charts, whose data cache also holds Number and Label records) are skipped. Other records
/// are skipped too, deliberately: the cells are the records the CELL rule of [MS-XLS] 2.1.7.20.5 lists, and the rest
/// carry formatting, layout and objects. (The xlsb reader, by contrast, refuses the unassigned one-byte types 12 to
/// 18 among its cell records.) RString, a BIFF5/7 rich-text cell that [MS-XLS] does not describe, is refused rather
/// than dropped.
/// </summary>
internal sealed class XlsSheetSource : SheetSource
{
    private readonly Stream _stream;
    private readonly BiffRecords _records;
    private readonly string _sheet;
    private readonly ExcelValueKind[] _kinds;
    private readonly SharedStringStore? _sharedStrings;
    private readonly BiffText _text = new();
    private int _row = -1;          // the row whose cells are being collected (or were last)
    private int _lastColumn = -1;   // the last column seen in that row
    private bool _collecting;       // the RowBuffer holds the cells of _row
    private int _depth;             // nested substreams (embedded charts) being skipped
    private int _pendingString = -1; // the cell (index in the row) of a formula whose String record is due
    private bool _inString;         // that String record's characters run on into a Continue record
    private bool _done;

    private XlsSheetSource(Stream stream, string sheet, ExcelValueKind[] kinds, SharedStringStore? sharedStrings)
    {
        _stream = stream;
        _records = new BiffRecords(stream);
        _sheet = sheet;
        _kinds = kinds;
        _sharedStrings = sharedStrings;
    }

    /// <summary>Opens the sheet substream starting at the stream's position: its BOF, then the records up to the
    /// Dimensions record (or the first cell).</summary>
    public static async ValueTask<XlsSheetSource> OpenAsync(Stream stream, string sheet, ExcelValueKind[] kinds,
        SharedStringStore? sharedStrings, bool useAsync, CancellationToken ct)
    {
        var source = new XlsSheetSource(stream, sheet, kinds, sharedStrings);
        try
        {
            var records = source._records;
            if (!await NextAsync().ConfigureAwait(false)) throw new ExcelFormatException($"Sheet '{sheet}' has no records.");
            XlsWorkbook.CheckBof(records, 0x0010, $"sheet '{sheet}'");
            while (await NextAsync().ConfigureAwait(false))
            {
                if (records.Type == Rt.Dimensions)
                {
                    // rwMic, rwMac (one past the last row), colMic, colMac (one past the last column).
                    var x = new RecordReader(records.Data, records.Type);
                    long r1 = x.U32(), r2 = x.U32();
                    int c1 = x.U16(), c2 = x.U16();
                    if (r2 > r1 && r2 <= ExcelCellReference.MaxRows && c2 > c1 && c2 <= ExcelCellReference.MaxColumns)
                        source.Dimension = ((int)r1, c1, (int)r2 - 1, c2 - 1);
                    break;
                }
                if (IsCell(records.Type) || records.Type is Rt.Eof or Rt.Bof)
                {
                    records.Keep();
                    break;
                }
            }
            return source;
        }
        catch
        {
            source.Dispose();
            throw;
        }

        async ValueTask<bool> NextAsync() => useAsync ? await source._records.NextAsync(ct).ConfigureAwait(false) : source._records.Next();
    }

    private static bool IsCell(int type) => type is Rt.Number or Rt.Rk or Rt.MulRk or Rt.LabelSst or Rt.Label or Rt.BoolErr
        or Rt.Formula or Rt.Blank or Rt.MulBlank or Rt.RString;

    private enum Step : byte { Next, Row, End }

    public override bool ReadRow(RowBuffer row)
    {
        while (!_done)
        {
            if (!_records.Next()) throw Unterminated();
            var step = Handle(row);
            if (step != Step.Next) return step == Step.Row;
        }
        return false;
    }

    public override async ValueTask<bool> ReadRowAsync(RowBuffer row, CancellationToken cancellationToken)
    {
        while (!_done)
        {
            if (!await _records.NextAsync(cancellationToken).ConfigureAwait(false)) throw Unterminated();
            var step = Handle(row);
            if (step != Step.Next) return step == Step.Row;
        }
        return false;
    }

    private ExcelFormatException Unterminated() => new($"Sheet '{_sheet}' ends without an EOF record (the workbook stream is truncated).");

    private Step Handle(RowBuffer row)
    {
        int type = _records.Type;
        var data = _records.Data;
        if (_inString)
        {
            if (type != Rt.Continue || data.IsEmpty) throw MissingString(row);
            _text.Resume(data[0]);
            Text(row, data[1..]);
            return Step.Next;
        }
        if (_pendingString >= 0)
        {
            if (type == Rt.String)
            {
                // XLUnicodeString (2.5.294): cch, fHighByte, the characters.
                var x = new RecordReader(data, type);
                int chars = x.U16();
                _text.Begin(chars, (x.U8() & 1) != 0);
                Text(row, data[3..]);
                return Step.Next;
            }
            if (IsCell(type) || type is Rt.Eof or Rt.Bof) throw MissingString(row);
            return Step.Next; // Array, Table, ShrFmla
        }
        if (_depth > 0)
        {
            if (type == Rt.Bof) _depth++;
            else if (type == Rt.Eof) _depth--;
            return Step.Next;
        }
        switch (type)
        {
            case Rt.Bof:
                _depth = 1;
                return Step.Next;
            case Rt.Eof:
                _done = true;
                if (_collecting && row.Count > 0)
                {
                    _collecting = false;
                    return Step.Row;
                }
                return Step.End;
            case Rt.RString:
                throw new ExcelFormatException($"Sheet '{_sheet}' holds an RString cell record (0x00D6), which [MS-XLS] does not describe (a BIFF5/7 rich-text cell).");
            case Rt.Number or Rt.Rk or Rt.LabelSst or Rt.Label or Rt.BoolErr or Rt.Formula or Rt.Blank:
            {
                // Cell (2.5.19): rw, col, ixfe; then the value.
                var x = new RecordReader(data, type);
                int rw = x.U16(), col = x.U16(), ixfe = x.U16();
                if (!StartCell(row, rw, col)) return Step.Row;
                _lastColumn = col;
                Cell(row, type, col, ixfe, ref x);
                return Step.Next;
            }
            case Rt.MulRk or Rt.MulBlank:
            {
                // rw, colFirst, an array of RkRec (ixfe, RK) or of ixfe, colLast.
                int width = type == Rt.MulRk ? 6 : 2;
                if (data.Length < 6 || (data.Length - 6) % width != 0) throw new ExcelFormatException($"Sheet '{_sheet}' has a malformed MulRk or MulBlank record.");
                var x = new RecordReader(data, type);
                int rw = x.U16(), first = x.U16();
                int count = (data.Length - 6) / width;
                int last = data[^2] | data[^1] << 8;
                if (count == 0 || last != first + count - 1) throw new ExcelFormatException($"Sheet '{_sheet}' has a MulRk or MulBlank record whose column range does not match its cells.");
                if (!StartCell(row, rw, first)) return Step.Row;
                if (last >= ExcelCellReference.MaxColumns) throw BadColumn(last);
                _lastColumn = last;
                if (type == Rt.MulRk)
                {
                    for (int i = 0; i < count; i++)
                    {
                        int ixfe = x.U16();
                        Number(row, first + i, XlsbSheetSource.Rk(x.U32()), Kind(ixfe));
                    }
                }
                return Step.Next;
            }
            default:
                return Step.Next;
        }
    }

    /// <summary>Begins a cell of row <paramref name="rw"/>; false when it starts a new row and the collected one must
    /// be handed out first (the record is kept and read again).</summary>
    private bool StartCell(RowBuffer row, int rw, int col)
    {
        if (rw < _row || rw == _row && _collecting && col <= _lastColumn)
            throw new ExcelFormatException($"Sheet '{_sheet}' has cell {ExcelCellReference.Format(rw, col)} after {ExcelCellReference.Format(_row, Math.Max(_lastColumn, 0))}: "
                + "cells out of order (rows must increase, and columns within a row) are not supported.");
        if (col >= ExcelCellReference.MaxColumns) throw BadColumn(col);
        if (rw > _row || !_collecting)
        {
            if (_collecting && row.Count > 0)
            {
                _collecting = false;
                _records.Keep();
                return false;
            }
            row.Clear();
            row.SharedStrings = _sharedStrings;
            row.Row = rw;
            _row = rw;
            _lastColumn = -1;
            _collecting = true;
        }
        return true;
    }

    private ExcelFormatException BadColumn(int col) => new($"Sheet '{_sheet}' has a cell in column {col}, past the last column.");

    private ExcelValueKind Kind(int ixfe) => (uint)ixfe < (uint)_kinds.Length ? _kinds[ixfe] : ExcelValueKind.Number;

    private void Cell(RowBuffer row, int type, int col, int ixfe, ref RecordReader x)
    {
        switch (type)
        {
            case Rt.Number:
                Number(row, col, x.F64(), Kind(ixfe));
                return;
            case Rt.Rk:
                Number(row, col, XlsbSheetSource.Rk(x.U32()), Kind(ixfe));
                return;
            case Rt.LabelSst:
            {
                uint index = x.U32();
                // Empty text is an empty cell; an index beyond the table fails when the value is read.
                if (_sharedStrings is not null && index < (uint)_sharedStrings.Count && _sharedStrings.Get((int)index).IsEmpty) return;
                if (index > int.MaxValue) throw new ExcelFormatException($"Invalid shared string index {index}.");
                ref Cell cell = ref row.Add();
                cell.Column = col;
                cell.Type = CellType.SharedString;
                cell.Offset = (int)index;
                return;
            }
            case Rt.Label:
            {
                int chars = x.U16();
                bool wide = (x.U8() & 1) != 0;
                var bytes = x.Bytes(wide ? 2 * chars : chars);
                ref Cell cell = ref row.Add();
                cell.Column = col;
                cell.Type = CellType.String;
                _text.Begin(chars, wide);
                _text.Feed(bytes);
                FinishText(row, row.Count - 1);
                return;
            }
            case Rt.BoolErr:
            {
                // Bes (2.5.10): bBoolErr, fError.
                byte value = x.U8();
                bool error = x.U8() != 0;
                ref Cell cell = ref row.Add();
                cell.Column = col;
                cell.Type = error ? CellType.Error : CellType.Boolean;
                cell.Number = !error && value != 0 ? 1 : 0;
                return;
            }
            case Rt.Formula:
            {
                // FormulaValue (2.5.133): an Xnum unless the last two bytes are 0xFFFF; then byte 1 is the type
                // (0 string in a String record, 1 boolean, 2 error, 3 empty string) and byte 3 the boolean or error.
                var value = x.Bytes(8);
                if (value[6] != 0xFF || value[7] != 0xFF)
                {
                    Number(row, col, BitConverter.ToDouble(value), Kind(ixfe));
                    return;
                }
                switch (value[0])
                {
                    case 0:
                    {
                        ref Cell cell = ref row.Add();
                        cell.Column = col;
                        cell.Type = CellType.String;
                        _pendingString = row.Count - 1;
                        return;
                    }
                    case 1 or 2:
                    {
                        ref Cell cell = ref row.Add();
                        cell.Column = col;
                        cell.Type = value[0] == 2 ? CellType.Error : CellType.Boolean;
                        cell.Number = value[0] == 1 && value[2] != 0 ? 1 : 0;
                        return;
                    }
                    case 3:
                        return; // an empty string: an empty cell
                    default:
                        throw new ExcelFormatException($"Sheet '{_sheet}' has a formula in {ExcelCellReference.Format(_row, col)} with an unknown result type {value[0]}.");
                }
            }
        }
    }

    // Characters of a formula's String record (or its continuation); the cell is filled once all are read.
    private void Text(RowBuffer row, ReadOnlySpan<byte> data)
    {
        int used = _text.Feed(data);
        _inString = !_text.Done;
        if (_inString)
        {
            if (used < data.Length) throw new ExcelFormatException($"Sheet '{_sheet}' has a formula string whose two-byte characters are split across records.");
            return;
        }
        FinishText(row, _pendingString);
        _pendingString = -1;
    }

    // Fills the string cell at index from the decoded characters; empty text leaves no cell (it is the last one).
    private void FinishText(RowBuffer row, int index)
    {
        if (_text.Chars.IsEmpty)
        {
            row.RemoveLast();
            return;
        }
        int start = row.TextLength;
        int n = _text.ToUtf8(row.ReserveText(_text.MaxUtf8));
        row.CommitText(n);
        ref Cell cell = ref row[index];
        cell.Offset = start;
        cell.Length = n;
    }

    private ExcelFormatException MissingString(RowBuffer row) =>
        new($"Sheet '{_sheet}' has a formula in {ExcelCellReference.Format(_row, row[_pendingString].Column)} whose string result is missing or cut short.");

    private static void Number(RowBuffer row, int column, double value, ExcelValueKind kind)
    {
        ref Cell cell = ref row.Add();
        cell.Column = column;
        cell.Type = CellType.Number;
        cell.Number = value;
        cell.Kind = kind;
    }

    public override void Dispose()
    {
        _records.Dispose();
        _stream.Dispose();
    }
}
