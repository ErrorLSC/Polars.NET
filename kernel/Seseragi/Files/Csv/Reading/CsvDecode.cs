using System.Text.Unicode;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Builders;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Text;

namespace Kaminari.Seseragi.Files.Csv.Reading;

/// <summary>
/// A failure inside a unit: at a record counted from the unit's first one (0-based), whose start lies
/// <see cref="LinesBefore"/> line ends into the unit (the reader adds the records and lines before the unit).
/// </summary>
internal sealed class CsvUnitError(long row, long linesBefore, string message, CsvValueError? value)
{
    public long Row { get; } = row;

    public long LinesBefore { get; } = linesBefore;

    public string Message { get; } = message;

    /// <summary>The refused value, or null for an input error.</summary>
    public CsvValueError? Value { get; } = value;

    /// <summary>The failure as the reader reports it: at its row (1 = the first data record) and line in the whole input.</summary>
    public Exception At(long rowsBefore, long linesBeforeUnit)
    {
        long row = rowsBefore + Row + 1, line = linesBeforeUnit + LinesBefore + 1;
        string text = $"Row {row} (line {line}): {Message}";
        return Value is { } v ? new CsvValueException(text, v.Column, row, line, v.Value) : new CsvFormatException(text, row, line);
    }
}

/// <summary>What a unit decoded: record batches, its rows (and line ends when counted), its failure, or the end of a stream.</summary>
internal sealed class CsvUnit : IDisposable
{
    public static CsvUnit EndOfInput => new() { End = true };

    public List<RecordBatch?> Batches { get; } = [];

    public long Rows { get; set; }

    /// <summary>The unit's line ends (all of its bytes), or -1 when not counted.</summary>
    public long Lines { get; set; } = -1;

    public CsvUnitError? Error { get; set; }

    public bool End { get; private init; }

    /// <summary>
    /// Per decoded column, for temporal columns decoded without a group: the group the unit took (null: none) — and
    /// whether a value was refused before it was known; null when every temporal column had its group.
    /// </summary>
    public (TemporalPattern? Group, bool Unsure)[]? Guesses { get; set; }

    public void Dispose()
    {
        foreach (RecordBatch? batch in Batches) batch?.Dispose();
        Batches.Clear();
    }
}

/// <summary>What decoding takes: the columns decoded, where they are in the file, the value rules, rows per batch.</summary>
internal sealed class CsvDecodeSettings(Schema schema, int fileColumns, int[] slots, CsvValueRules rules, int batchSize)
{
    /// <summary>The columns decoded, in output order.</summary>
    public Schema Schema { get; } = schema;

    /// <summary>The schema of the batches delivered with a row index (its column first), or null.</summary>
    public Schema? Indexed { get; init; }

    /// <summary>The fields a record has.</summary>
    public int FileColumns { get; } = fileColumns;

    /// <summary>Per file column, its decoded column's index, or -1 (passed over).</summary>
    public int[] Slots { get; } = slots;

    public CsvValueRules Rules { get; } = rules;

    public int BatchSize { get; } = batchSize;

    /// <summary>Per decoded column, the rows its type was inferred from; 0 when given (null: every type given).</summary>
    public int[]? InferredFrom { get; init; }

    /// <summary>The records tokenized at once: about 32 K fields (the table stays in the caches), 64 to 4096 records.</summary>
    public int ChunkRows => Math.Min(BatchSize, Math.Clamp(32_768 / Math.Max(1, Schema.FieldsList.Count), 64, 4096));

    private StructType? Type { get; } = schema.FieldsList.Count == 0 ? null : new StructType(schema.FieldsList);

    /// <summary>The builder of a unit's rows (null without columns).</summary>
    public NativeStructBuilder? Builder() => Type is null ? null : (NativeStructBuilder)NativeColumnBuilder.Create(Type);

    /// <summary>The decoders over <paramref name="builder"/>'s children, temporal ones in <paramref name="groups"/>.</summary>
    public CsvColumn[] Columns(NativeStructBuilder? builder, TemporalPattern?[] groups)
    {
        if (builder is null) return [];
        var columns = new CsvColumn[Schema.FieldsList.Count];
        try
        {
            for (int c = 0; c < columns.Length; c++)
            {
                columns[c] = CsvColumn.For(Schema.FieldsList[c], builder.Children[c], Rules, groups[c]);
                columns[c].InferredFrom = InferredFrom?[c] ?? 0;
            }
        }
        catch
        {
            foreach (CsvColumn? column in columns) column?.Dispose();
            throw;
        }
        return columns;
    }

    /// <summary>The batch of the rows built (a batch without columns has only its length).</summary>
    public RecordBatch Export(NativeStructBuilder? builder, int rows) => builder?.ExportBatch(Schema) ?? new RecordBatch(Schema, [], rows);
}

/// <summary>Decodes a unit of whole records into record batches.</summary>
internal static class CsvDecode
{
    /// <summary>
    /// The records of <paramref name="bytes"/> from <paramref name="from"/> (a record start) as batches, at most
    /// <paramref name="maxRows"/>; temporal columns read in <paramref name="groups"/> (null: the unit's first value that
    /// reads). A failure is the unit's <see cref="CsvUnit.Error"/> (the first in the unit's order).
    /// </summary>
    public static CsvUnit Decode(ReadOnlySpan<byte> bytes, int from, CsvDecodeSettings settings, TemporalPattern?[] groups, long maxRows, bool countLines)
    {
        var unit = new CsvUnit();
        CsvTokenizer tokenizer = settings.Rules.Tokenizer;
        int invalid = FirstInvalidUtf8(bytes[from..]);
        if (invalid >= 0) invalid += from;
        int decoded = settings.Schema.FieldsList.Count, capacity = settings.ChunkRows, batchSize = settings.BatchSize;
        NativeStructBuilder? builder = settings.Builder();
        CsvColumn[] columns = [];
        var table = new NativeArray<CsvField>(Math.Max(1, capacity * decoded));
        var starts = new NativeArray<int>(capacity);
        try
        {
            columns = settings.Columns(builder, groups);
            Span<CsvField> fields = table.AsSpan(0, capacity * decoded);
            Span<int> recordStarts = starts.AsSpan(0, capacity);
            int pos = from, inBatch = 0;
            bool reserved = false;
            while (pos < bytes.Length && unit.Rows < maxRows)
            {
                int cap = (int)Math.Min(Math.Min(capacity, maxRows - unit.Rows), batchSize - inBatch);
                int chunkStart = pos;
                int n = tokenizer.Records(bytes, ref pos, settings.FileColumns, settings.Slots, fields, recordStarts, cap, out FieldEnd end, out int failedColumn);
                // The first failure in record order: an input error (the tokenizer's, or a byte that is not UTF-8), then
                // values refused in earlier records (or earlier columns of the same record).
                int limit = n;
                CsvUnitError? error = null;
                if (end != FieldEnd.Record)
                {
                    error = new CsvUnitError(unit.Rows + n, tokenizer.CountLines(bytes[..pos]), Malformed(end, failedColumn, settings), null);
                }
                if (invalid >= 0 && invalid < pos)
                {
                    // A byte that is not UTF-8 inside a record read: that record fails (before any of its values).
                    int row = RowAt(recordStarts[..n], invalid);
                    error = new CsvUnitError(unit.Rows + row, tokenizer.CountLines(bytes[..recordStarts[row]]), NotUtf8, null);
                    limit = row;
                }
                if (!reserved && n > 0)
                {
                    reserved = true;
                    long expected = Math.Min(Math.Min(batchSize, maxRows - unit.Rows), (long)((bytes.Length - chunkStart) * (double)n / Math.Max(1, pos - chunkStart) * 1.0625) + 1);
                    for (int c = 0; c < columns.Length; c++) columns[c].Reserve(expected, fields.Slice(c * cap, n));
                }
                for (int c = 0; c < columns.Length && limit > 0; c++)
                {
                    int refused = columns[c].Decode(bytes, fields.Slice(c * cap, limit), limit, out CsvValueError? value);
                    if (refused < 0) continue;
                    error = new CsvUnitError(unit.Rows + refused, tokenizer.CountLines(bytes[..recordStarts[refused]]), value!.Message, value);
                    limit = refused;
                }
                if (error is not null)
                {
                    unit.Dispose();
                    unit.Error = error;
                    unit.Guesses = Guesses(columns);
                    return unit;
                }
                builder?.EndStructs(n);
                inBatch += n;
                unit.Rows += n;
                if (inBatch == batchSize)
                {
                    unit.Batches.Add(settings.Export(builder, inBatch));
                    foreach (CsvColumn column in columns) column.EndBatch();
                    inBatch = 0;
                    reserved = false;
                }
            }
            if (inBatch > 0)
            {
                unit.Batches.Add(settings.Export(builder, inBatch));
                foreach (CsvColumn column in columns) column.EndBatch();
            }
            if (countLines) unit.Lines = tokenizer.CountLines(bytes);
            unit.Guesses = Guesses(columns);
            return unit;
        }
        catch
        {
            unit.Dispose();
            throw;
        }
        finally
        {
            foreach (CsvColumn column in columns) column.Dispose();
            builder?.Dispose();
            table.Dispose();
            starts.Dispose();
        }
    }

    // The groups temporal columns decoded without one took, or null when none was.
    private static (TemporalPattern?, bool)[]? Guesses(CsvColumn[] columns)
    {
        (TemporalPattern?, bool)[]? guesses = null;
        for (int c = 0; c < columns.Length; c++)
        {
            if (columns[c] is not TemporalColumnBase { Seeded: false } temporal) continue;
            guesses ??= new (TemporalPattern?, bool)[columns.Length];
            guesses[c] = (temporal.Group, temporal.RefusedUnseeded);
        }
        return guesses;
    }

    /// <summary>The failure of text that is not UTF-8.</summary>
    public const string NotUtf8 = "the text is not UTF-8 (give the input's encoding)";

    // The record holding byte at: the last record start at or before it.
    private static int RowAt(ReadOnlySpan<int> starts, int at)
    {
        int lo = 0, hi = starts.Length - 1;
        while (lo < hi)
        {
            int mid = (lo + hi + 1) >> 1;
            if (starts[mid] <= at) lo = mid;
            else hi = mid - 1;
        }
        return lo;
    }

    /// <summary>The message of a malformed record.</summary>
    public static string Malformed(FieldEnd end, int column, CsvDecodeSettings settings) => Malformed(end, column, settings.FileColumns);

    /// <inheritdoc cref="Malformed(FieldEnd, int, CsvDecodeSettings)"/>
    public static string Malformed(FieldEnd end, int column, int columns) => end switch
    {
        FieldEnd.QuoteInside => $"field {column + 1} holds a quote but does not start with one (quote the whole field and double the quotes inside, or read without quoting)",
        FieldEnd.AfterQuote => $"field {column + 1} has text after its closing quote (a quote inside a quoted field is doubled)",
        FieldEnd.Unterminated => $"field {column + 1} opens a quote that never closes",
        FieldEnd.TooLong => $"field {column + 1} is longer than {CsvField.MaxLength >> 20} MiB",
        FieldEnd.TooManyFields => $"the record has more fields than the {columns} columns (check the separator, or truncate ragged lines)",
        _ => end.ToString(),
    };

    /// <summary>Where the first byte that is not UTF-8 is, or -1.</summary>
    public static int FirstInvalidUtf8(ReadOnlySpan<byte> bytes)
    {
        if (System.Text.Ascii.IsValid(bytes) || Utf8.IsValid(bytes)) return -1;
        int at = 0;
        while (at < bytes.Length)
        {
            if (System.Text.Rune.DecodeFromUtf8(bytes[at..], out _, out int consumed) != System.Buffers.OperationStatus.Done) return at;
            at += consumed;
        }
        return -1;
    }
}
