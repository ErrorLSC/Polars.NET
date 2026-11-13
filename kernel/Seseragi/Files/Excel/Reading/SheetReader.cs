using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow;

namespace Kaminari.Seseragi.Files.Excel.Reading;

/// <summary>The cell range a read is limited to (a table), header and totals rows given separately.</summary>
internal readonly record struct ReadRange(int FirstRow, int FirstColumn, int LastRow, int LastColumn, int HeaderRows, int TotalsRows);

/// <summary>
/// Turns the rows of a sheet into Arrow record batches, whatever the container format: finds the header, infers
/// column types from the first rows (held back meanwhile), applies overrides and the projection, then streams.
/// </summary>
internal sealed class SheetReader : IDisposable
{
    private readonly SheetSource _source;
    private readonly ExcelReadOptions _options;
    private readonly AppendContext _context;
    private readonly ReadRange? _range;
    private readonly RowBuffer _row = new();
    private readonly Queue<RowBuffer> _held = new();

    private int _origin;          // first sheet column of the read
    private int _width;           // columns from the origin
    private int[] _slots = [];    // per column from the origin: output slot, NotSelected, or Dropped
    private ColumnAppender[] _appenders = [];
    private int[] _cellOfSlot = [];
    private NativeStructBuilder? _root;
    private int _lastRow = -1;    // the last sheet row emitted (or the header row)
    private bool _sourceDone;
    private bool _projected;

    private const int NotSelected = -1;
    private const int Dropped = -2;

    private SheetReader(SheetSource source, ExcelReadOptions options, string sheet, bool date1904, ReadRange? range)
    {
        _source = source;
        _options = options;
        _context = new(sheet, date1904);
        _range = range;
    }

    public Schema Schema { get; private set; } = null!;

    public static async ValueTask<SheetReader> OpenAsync(SheetSource source, ExcelReadOptions options, string sheet,
        bool date1904, ReadRange? range, bool useAsync, CancellationToken cancellationToken)
    {
        var reader = new SheetReader(source, options, sheet, date1904, range);
        try
        {
            await reader.PrepareAsync(useAsync, cancellationToken).ConfigureAwait(false);
            return reader;
        }
        catch
        {
            reader.Dispose();
            throw;
        }
    }

    // ------------------------------------------------------------------ opening: header, window, schema

    private async ValueTask<bool> NextRowAsync(bool useAsync, CancellationToken ct)
    {
        while (true)
        {
            if (_sourceDone) return false;
            bool got = useAsync ? await _source.ReadRowAsync(_row, ct).ConfigureAwait(false) : _source.ReadRow(_row);
            if (!got)
            {
                _sourceDone = true;
                return false;
            }
            if (_range is { } r)
            {
                if (_row.Row < r.FirstRow) continue;
                if (_row.Row > r.LastRow - r.TotalsRows)
                {
                    _sourceDone = true;
                    return false;
                }
            }
            return true;
        }
    }

    private bool HasValue(RowBuffer row)
    {
        foreach (ref readonly var c in row.Cells)
            if (c.Type is not (CellType.Empty or CellType.Error) && InRange(c.Column)) return true;
        return false;
    }

    private bool InRange(int column) => _range is not { } r || (column >= r.FirstColumn && column <= r.LastColumn);

    private async ValueTask PrepareAsync(bool useAsync, CancellationToken ct)
    {
        // Header: the first row with a value (a table's first row).
        RowBuffer? header = null;
        bool hasHeader = _range is { } tr ? tr.HeaderRows > 0 : _options.HasHeader;
        if (hasHeader)
        {
            while (await NextRowAsync(useAsync, ct).ConfigureAwait(false))
            {
                if (_range is { } r ? _row.Row == r.FirstRow : HasValue(_row))
                {
                    header = _row.Clone();
                    _lastRow = _row.Row;
                    break;
                }
                if (_range is { } r2 && _row.Row > r2.FirstRow) break; // the table's header row is empty
            }
        }

        // Window: the first data rows, held back until the schema is known.
        int window = _options.InferSchemaLength is { } n ? n : int.MaxValue;
        if (window < 0) throw new ArgumentOutOfRangeException(nameof(_options.InferSchemaLength), "InferSchemaLength must not be negative.");
        int skipFirst = _range is { } rr && hasHeader ? rr.FirstRow : -1;
        while (_held.Count < Math.Max(window, 1) && await NextRowAsync(useAsync, ct).ConfigureAwait(false))
        {
            if (_row.Row == skipFirst && header is null) continue; // empty table header row
            if (_lastRow < 0 && !HasValue(_row)) continue;        // leading empty rows
            _held.Enqueue(_row.Clone());
        }

        // Column range: the range's columns, or from the leftmost value to the rightmost cell seen or declared.
        int first = int.MaxValue, last = -1;
        if (_range is { } t)
        {
            first = t.FirstColumn;
            last = t.LastColumn;
        }
        else
        {
            foreach (var row in Rows(header))
            {
                foreach (ref readonly var c in row.Cells)
                {
                    if (c.Type is CellType.Empty or CellType.Error) continue;
                    first = Math.Min(first, c.Column);
                    last = Math.Max(last, c.Column);
                }
            }
            if (last >= 0 && _source.Dimension is { } dim && dim.LastColumn > last && dim.LastColumn < ExcelCellReference.MaxColumns)
                last = dim.LastColumn;
        }
        if (_options.RaiseIfEmpty && _sourceDone && !_held.Any(HasValue))
            throw new ExcelFormatException($"Sheet '{_context.Sheet}' has no data.");
        if (last < 0)
        {
            first = 0;
            last = -1;
        }
        _origin = first;
        _width = last - first + 1;

        // Names: header text, blank → __UNNAMED__i, duplicates → name_k (first k from 1 not taken).
        string[] names = new string[_width];
        bool[] unnamed = new bool[_width];
        if (header is not null)
        {
            Span<byte> buffer = stackalloc byte[CellText.MaxLength];
            foreach (ref readonly var c in header.Cells)
            {
                int i = c.Column - _origin;
                if ((uint)i >= (uint)_width) continue;
                if (c.Type is CellType.String or CellType.SharedString) names[i] = Encoding.UTF8.GetString(header.GetText(c));
                else if (CellText.TryFormat(c, _context.Date1904, buffer, out int len)) names[i] = Encoding.UTF8.GetString(buffer[..len]);
            }
            for (int i = 0; i < _width; i++)
            {
                if (string.IsNullOrEmpty(names[i]))
                {
                    names[i] = $"__UNNAMED__{i}";
                    unnamed[i] = true;
                }
            }
            HashSet<string> taken = [];
            for (int i = 0; i < _width; i++)
            {
                string name = names[i];
                if (!taken.Add(name))
                {
                    int k = 1;
                    while (!taken.Add($"{name}_{k}")) k++;
                    names[i] = $"{name}_{k}";
                }
            }
        }
        else
        {
            for (int i = 0; i < _width; i++) names[i] = $"column_{i + 1}";
        }

        // Types: overrides by name, else inferred over the window.
        var overrides = _options.SchemaOverrides;
        if (overrides is not null)
        {
            var missing = overrides.Keys.Where(k => !names.Contains(k)).ToList();
            if (missing.Count > 0)
                throw new ArgumentException($"Schema overrides name columns sheet '{_context.Sheet}' does not have: {string.Join(", ", missing)}.");
        }
        var inference = new ColumnInference[_width];
        if (window > 0)
        {
            foreach (var row in _held)
            {
                foreach (ref readonly var c in row.Cells)
                {
                    int i = c.Column - _origin;
                    if ((uint)i < (uint)_width) inference[i].Add(c, _context.Date1904);
                }
            }
        }

        // Which columns: drop unnamed empty ones, then the projection.
        _slots = new int[_width];
        List<int> kept = [];
        for (int i = 0; i < _width; i++)
        {
            if (_options.DropEmptyColumns && unnamed[i] && !HasValueInColumn(i)) _slots[i] = Dropped;
            else kept.Add(i);
        }
        List<int> selected = kept;
        if (_options.Columns is { } wanted)
        {
            _projected = true;
            var byName = kept.ToDictionary(i => names[i]);
            var unknown = wanted.Where(w => !byName.ContainsKey(w)).ToList();
            if (unknown.Count > 0) throw new ArgumentException($"Sheet '{_context.Sheet}' has no columns named {string.Join(", ", unknown)}.");
            var set = wanted.Select(w => byName[w]).ToHashSet();
            selected = [.. kept.Where(set.Contains)];
        }
        else if (_options.ColumnIndices is { } indices)
        {
            _projected = true;
            var bad = indices.Where(i => (uint)i >= (uint)kept.Count).ToList();
            if (bad.Count > 0) throw new ArgumentException($"Column indices out of range (sheet '{_context.Sheet}' has {kept.Count} columns): {string.Join(", ", bad)}.");
            var set = indices.Select(i => kept[i]).ToHashSet();
            selected = [.. kept.Where(set.Contains)];
        }
        foreach (int i in kept) _slots[i] = NotSelected;

        List<Field> fields = [];
        List<ColumnAppender> appenders = [];
        foreach (int i in selected)
        {
            bool inferred = overrides is null || !overrides.ContainsKey(names[i]);
            IArrowType type = inferred ? inference[i].Type : overrides![names[i]];
            var appender = ColumnAppender.Create(type)
                ?? throw new ArgumentException($"Column '{names[i]}' cannot be read as {type.Name}.");
            appender.Inferred = inferred;
            _slots[i] = appenders.Count;
            appenders.Add(appender);
            fields.Add(new Field(names[i], appender.Builder.Type, nullable: true));
        }
        _appenders = [.. appenders];
        _cellOfSlot = new int[_appenders.Length];
        Schema = new Schema(fields, null);
        // A sheet without columns has no batches (an Arrow struct needs at least one field).
        _root = fields.Count == 0 ? null : new NativeStructBuilder(new StructType(fields), [.. _appenders.Select(a => a.Builder)]);

        IEnumerable<RowBuffer> Rows(RowBuffer? h) => h is null ? _held : _held.Prepend(h);

        bool HasValueInColumn(int i)
        {
            foreach (var row in _held)
                foreach (ref readonly var c in row.Cells)
                    if (c.Column - _origin == i && c.Type is not (CellType.Empty or CellType.Error)) return true;
            return false;
        }
    }

    // ------------------------------------------------------------------ streaming

    public IEnumerable<RecordBatch> ReadBatches()
    {
        while (FillBatchAsync(false, default).AsTask().GetAwaiter().GetResult() is { } batch) yield return batch;
    }

    public async IAsyncEnumerable<RecordBatch> ReadBatchesAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        while (await FillBatchAsync(true, cancellationToken).ConfigureAwait(false) is { } batch) yield return batch;
    }

    private async ValueTask<RecordBatch?> FillBatchAsync(bool useAsync, CancellationToken ct)
    {
        if (_root is not { } root) return null;
        int target = _options.BatchSize;
        while (root.Length < target)
        {
            RowBuffer row;
            if (_held.Count > 0) row = _held.Dequeue();
            else if (await NextRowAsync(useAsync, ct).ConfigureAwait(false)) row = _row;
            else break;
            Emit(row);
        }
        return root.Length > 0 ? root.ExportBatch(Schema) : null;
    }

    // One sheet row: the empty rows before it (when they are kept), then its cells. An empty row is only written
    // once a later row has values, so trailing empty rows never are.
    private void Emit(RowBuffer row)
    {
        var root = _root!;
        int slots = _appenders.Length;
        _cellOfSlot.AsSpan().Fill(-1);
        bool any = false;
        var cells = row.Cells;
        for (int k = 0; k < cells.Length; k++)
        {
            ref readonly var c = ref cells[k];
            int i = c.Column - _origin;
            if ((uint)i >= (uint)_width)
            {
                if (c.Type is CellType.Empty or CellType.Error || _projected || !InRange(c.Column)) continue;
                throw _context.Mismatch(row.Row, c,
                    "lies outside the columns found in the first rows; read with a larger InferSchemaLength or select Columns");
            }
            int slot = _slots[i];
            if (slot >= 0)
            {
                _cellOfSlot[slot] = k;
                if (c.Type is not (CellType.Empty or CellType.Error)) any = true;
            }
            else if (slot == Dropped && !_projected && c.Type is not (CellType.Empty or CellType.Error))
            {
                throw _context.Mismatch(row.Row, c,
                    "has a value in a column without a header that was empty in the first rows and dropped; read with DropEmptyColumns off or a larger InferSchemaLength");
            }
        }
        if (!any) return;
        if (!_options.DropEmptyRows && _lastRow >= 0)
        {
            for (int gap = _lastRow + 1; gap < row.Row; gap++) root.AppendNull();
        }
        _lastRow = row.Row;
        for (int s = 0; s < slots; s++)
        {
            int k = _cellOfSlot[s];
            if (k < 0 || cells[k].Type is CellType.Empty or CellType.Error) _appenders[s].AppendNull();
            else _appenders[s].Append(row, cells[k], _context);
        }
        root.EndStruct();
    }

    public void Dispose()
    {
        _root?.Dispose();
        _root = null;
        _source.Dispose();
    }
}

/// <summary>What the inference window saw in one column, and the Arrow type that follows.</summary>
internal struct ColumnInference
{
    [Flags]
    private enum Seen : ushort
    {
        None = 0,
        Integer = 1,
        Fraction = 2,
        Boolean = 4,
        Text = 8,
        Date = 16,
        DateTime = 32,
        Time = 64,
        Duration = 128,
    }

    private Seen _seen;

    public readonly bool IsEmpty => _seen == Seen.None;

    public void Add(in Cell cell, bool date1904)
    {
        _seen |= cell.Type switch
        {
            CellType.Number => cell.Kind switch
            {
                ExcelValueKind.Number => cell.Number == Math.Floor(cell.Number) && Math.Abs(cell.Number) < 9.2e18 ? Seen.Integer : Seen.Fraction,
                ExcelValueKind.Date => ExcelDates.TrySplit(cell.Number, date1904, out _, out long ofDay)
                    ? ofDay == 0 ? Seen.Date : Seen.DateTime
                    : Seen.Fraction,
                ExcelValueKind.DateTime => ExcelDates.TrySplit(cell.Number, date1904, out _, out _) ? Seen.DateTime : Seen.Fraction,
                // A time format over a whole serial shows the clock time of a full date-time.
                ExcelValueKind.Time => cell.Number is >= 0 and < 1 ? Seen.Time
                    : ExcelDates.TrySplit(cell.Number, date1904, out _, out _) ? Seen.DateTime : Seen.Fraction,
                _ => Seen.Duration,
            },
            CellType.Temporal => cell.Kind switch
            {
                ExcelValueKind.Date => cell.Micros % ExcelDates.MicrosPerDay == 0 ? Seen.Date : Seen.DateTime,
                ExcelValueKind.Time => Seen.Time,
                ExcelValueKind.Duration => Seen.Duration,
                _ => Seen.DateTime,
            },
            CellType.Boolean => Seen.Boolean,
            CellType.String or CellType.SharedString => Seen.Text,
            _ => Seen.None,
        };
    }

    public readonly IArrowType Type => _seen switch
    {
        Seen.Integer => Int64Type.Default,
        Seen.Fraction or (Seen.Integer | Seen.Fraction) => DoubleType.Default,
        Seen.Boolean => BooleanType.Default,
        Seen.Date => Date32Type.Default,
        _ when _seen != Seen.None && (_seen & ~(Seen.Date | Seen.DateTime | Seen.Time)) == 0 && (_seen & (Seen.Date | Seen.DateTime)) != 0
            => new TimestampType(TimeUnit.Microsecond, (string?)null),
        Seen.Time => new Time64Type(TimeUnit.Microsecond),
        _ when _seen != Seen.None && (_seen & ~(Seen.Time | Seen.Duration)) == 0 => DurationType.Microsecond,
        _ => StringType.Default,
    };
}
