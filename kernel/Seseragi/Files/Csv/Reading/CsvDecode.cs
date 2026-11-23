using System.Text.Unicode;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Builders;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Filters;
using Kaminari.Sazanami.Text;
using Kaminari.Seseragi.Files.IO;

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

    /// <summary>The rows the unit delivers before the failure: its records before it, or under a row filter those kept.</summary>
    public long Delivered { get; init; } = row;

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

/// <summary>
/// What decoding takes: the columns decoded (those delivered, and under a row filter those it reads), where they are in
/// the file, the value rules, rows per batch.
/// </summary>
internal sealed class CsvDecodeSettings
{
    private readonly StructType? _type, _tests;
    private readonly Schema? _testSchema;

    /// <param name="decoded">The columns decoded, in output order.</param>
    /// <param name="fileColumns">The fields a record has.</param>
    /// <param name="slots">Per file column, its index in <paramref name="decoded"/>, or -1.</param>
    /// <param name="rules">The value rules.</param>
    /// <param name="batchSize">Rows per batch.</param>
    /// <param name="filter">The row filter (with a program) over <paramref name="decoded"/>, or null.</param>
    public CsvDecodeSettings(Schema decoded, int fileColumns, int[] slots, CsvValueRules rules, int batchSize, RowFilter? filter = null)
    {
        Decoded = decoded;
        FileColumns = fileColumns;
        Slots = slots;
        Rules = rules;
        BatchSize = batchSize;
        Filter = filter;
        Schema = filter?.Selected ?? decoded;
        Output = filter is null ? [.. Enumerable.Range(0, decoded.FieldsList.Count)] : filter.Output.ToArray();
        Tests = filter?.Arguments ?? [];
        TestOrder = [.. Enumerable.Range(0, Tests.Length).OrderBy(t => Tests[t])];
        TakenFrom = [.. Output.Select(d => System.Array.IndexOf(Tests, d))];
        FieldsChecked = filter is null && rules.Charset.Strict && rules.Charset.FieldDecoder is not null && slots.All(s => s >= 0);
        _type = Schema.FieldsList.Count == 0 ? null : new StructType(Schema.FieldsList);
        _tests = Tests.Length == 0 ? null : new StructType([.. Tests.Select((d, i) => new Field($"{i}", decoded.FieldsList[d].DataType, true))]);
        _testSchema = _tests is null ? null : new Schema(_tests.Fields, null);
    }

    /// <summary>The columns tokenized and decoded, in output order (the table's columns).</summary>
    public Schema Decoded { get; }

    /// <summary>The columns delivered, in output order.</summary>
    public Schema Schema { get; }

    /// <summary>Per column delivered, its index in <see cref="Decoded"/>.</summary>
    public int[] Output { get; }

    /// <summary>The row filter (two-phase decoding: the columns it reads, then the delivered ones for the rows kept), or null.</summary>
    public RowFilter? Filter { get; }

    /// <summary>Per column the filter's program reads (in its order), its index in <see cref="Decoded"/>.</summary>
    public int[] Tests { get; }

    /// <summary>The indices into <see cref="Tests"/> in output order (the order tested columns are decoded in).</summary>
    public int[] TestOrder { get; }

    /// <summary>
    /// Per column delivered, the index into <see cref="Tests"/> of the same column when the filter reads it (its kept
    /// values are taken from the tested array when its builder can, not decoded again), or -1.
    /// </summary>
    public int[] TakenFrom { get; }

    /// <summary>The schema of the batches delivered with a row index (its column first), or null.</summary>
    public Schema? Indexed { get; init; }

    /// <summary>The fields a record has.</summary>
    public int FileColumns { get; }

    /// <summary>Per file column, its decoded column's index, or -1 (passed over).</summary>
    public int[] Slots { get; }

    public CsvValueRules Rules { get; }

    public int BatchSize { get; }

    /// <summary>
    /// Whether invalid text is found as fields are decoded (strict text read on its bytes, every column decoded for every
    /// row) instead of by checking each unit whole first.
    /// </summary>
    public bool FieldsChecked { get; }

    /// <summary>Per decoded column, the rows its type was inferred from; 0 when given (null: every type given).</summary>
    public int[]? InferredFrom { get; init; }

    /// <summary>The records tokenized at once: about 32 K fields (the table stays in the caches), 64 to 4096 records.</summary>
    public int ChunkRows => Math.Min(BatchSize, Math.Clamp(32_768 / Math.Max(1, Decoded.FieldsList.Count), 64, 4096));

    /// <summary>The builder of a unit's rows (null without columns).</summary>
    public NativeStructBuilder? Builder() => _type is null ? null : (NativeStructBuilder)NativeColumnBuilder.Create(_type);

    /// <summary>The builder of a chunk's tested columns (null when the filter reads none).</summary>
    public NativeStructBuilder? TestBuilder() => _tests is null ? null : (NativeStructBuilder)NativeColumnBuilder.Create(_tests);

    /// <summary>The decoders of the delivered columns over <paramref name="builder"/>'s children; temporal ones in <paramref name="groups"/> (per decoded column).</summary>
    public CsvColumn[] Columns(NativeStructBuilder? builder, TemporalPattern?[] groups) => Columns(builder, Output, groups);

    /// <summary>The decoders of the tested columns over <paramref name="builder"/>'s children.</summary>
    public CsvColumn[] TestColumns(NativeStructBuilder? builder, TemporalPattern?[] groups) => Columns(builder, Tests, groups);

    private CsvColumn[] Columns(NativeStructBuilder? builder, int[] which, TemporalPattern?[] groups)
    {
        if (builder is null) return [];
        var columns = new CsvColumn[which.Length];
        try
        {
            for (int c = 0; c < columns.Length; c++)
            {
                int d = which[c];
                columns[c] = CsvColumn.For(Decoded.FieldsList[d], builder.Children[c], Rules, groups[d]);
                columns[c].InferredFrom = InferredFrom?[d] ?? 0;
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

    /// <summary>The batch of a chunk's tested columns (null when the filter reads none).</summary>
    public RecordBatch? ExportTests(NativeStructBuilder? builder) => builder?.ExportBatch(_testSchema!);
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
        if (settings.Filter is not null) return DecodeFiltered(bytes, from, settings, groups, maxRows, countLines);
        var unit = new CsvUnit();
        CsvTokenizer tokenizer = settings.Rules.Tokenizer;
        CsvCharset charset = settings.Rules.Charset;
        bool clean = charset.Clean(bytes[from..]);
        // Text in another encoding is checked as its fields are decoded when every column is (all of it is in fields);
        // else, and for UTF-8, the unit is checked whole: failures never depend on the columns read.
        bool fieldsChecked = !clean && settings.FieldsChecked;
        int invalid = (clean && charset.FieldDecoder is not null) || fieldsChecked ? -1 : charset.FirstInvalid(bytes[from..]);
        if (invalid >= 0) invalid += from;
        int decoded = settings.Schema.FieldsList.Count, capacity = settings.ChunkRows, batchSize = settings.BatchSize;
        NativeStructBuilder? builder = settings.Builder();
        CsvColumn[] columns = [];
        var table = new NativeArray<CsvField>(Math.Max(1, capacity * decoded));
        var starts = new NativeArray<int>(capacity);
        try
        {
            columns = settings.Columns(builder, groups);
            foreach (CsvColumn column in columns) column.Clean = clean;
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
                    error = new CsvUnitError(unit.Rows + row, tokenizer.CountLines(bytes[..recordStarts[row]]), charset.Invalid, null);
                    limit = row;
                }
                if (!reserved && n > 0)
                {
                    reserved = true;
                    long expected = Math.Min(Math.Min(batchSize, maxRows - unit.Rows), (long)((bytes.Length - chunkStart) * (double)n / Math.Max(1, pos - chunkStart) * 1.0625) + 1);
                    for (int c = 0; c < columns.Length; c++) columns[c].Reserve(expected, fields.Slice(c * cap, n));
                }
                for (int c = 0; c < columns.Length; c++)
                {
                    // A value refused at the limit still lets a later column's invalid text in that record come first.
                    bool valueAtLimit = error?.Value is not null;
                    int rows = Math.Min(n, limit + (valueAtLimit ? 1 : 0));
                    if (rows == 0) break;
                    int refused = columns[c].Decode(bytes, fields.Slice(c * cap, rows), rows, out CsvValueError? value);
                    int bad = fieldsChecked ? columns[c].TakeInvalidRow() : int.MaxValue;
                    // The column's first failure: invalid text before (or in) the record of a refused value comes first.
                    if (bad < rows && (refused < 0 || bad <= refused))
                    {
                        if (bad < limit || (bad == limit && valueAtLimit))
                        {
                            error = new CsvUnitError(unit.Rows + bad, tokenizer.CountLines(bytes[..recordStarts[bad]]), charset.Invalid, null);
                            limit = bad;
                        }
                    }
                    else if (refused >= 0 && refused < limit)
                    {
                        error = new CsvUnitError(unit.Rows + refused, tokenizer.CountLines(bytes[..recordStarts[refused]]), value!.Message, value);
                        limit = refused;
                    }
                }
                if (error is not null)
                {
                    unit.Dispose();
                    unit.Error = error;
                    unit.Guesses = Guesses(settings, columns, []);
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
            unit.Guesses = Guesses(settings, columns, []);
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

    /// <summary>
    /// <see cref="Decode"/> under a row filter, in two phases per chunk of records: the columns the filter reads are
    /// decoded for every record and the program run on them; the delivered columns are decoded for the records it keeps
    /// only (a value a dropped record holds in them is never read). <paramref name="maxRows"/> counts the rows kept.
    /// Failures in record order: an input error, then a value refused in a tested column (any record) or in a delivered
    /// column (a kept record); in one record a tested column's comes first.
    /// </summary>
    private static CsvUnit DecodeFiltered(ReadOnlySpan<byte> bytes, int from, CsvDecodeSettings settings, TemporalPattern?[] groups, long maxRows, bool countLines)
    {
        var unit = new CsvUnit();
        CsvTokenizer tokenizer = settings.Rules.Tokenizer;
        CsvCharset charset = settings.Rules.Charset;
        FilterProgram program = settings.Filter!.Program!;
        bool clean = charset.Clean(bytes[from..]);
        // The unit is checked whole for invalid text: not every field is decoded.
        int invalid = clean && charset.FieldDecoder is not null ? -1 : charset.FirstInvalid(bytes[from..]);
        if (invalid >= 0) invalid += from;
        int[] output = settings.Output;
        int decoded = settings.Decoded.FieldsList.Count, delivered = output.Length, capacity = settings.ChunkRows, batchSize = settings.BatchSize;
        NativeStructBuilder? builder = settings.Builder(), testBuilder = settings.TestBuilder();
        NativeFixedWidthBuilder<long>? numbers = settings.Indexed is null ? null : new NativeFixedWidthBuilder<long>(Int64Type.Default);
        CsvColumn[] columns = [], tests = [];
        var table = new NativeArray<CsvField>(Math.Max(1, capacity * decoded));
        var compact = new NativeArray<CsvField>(Math.Max(1, capacity * delivered));
        var starts = new NativeArray<int>(capacity);
        var kept = new NativeArray<int>(capacity);
        Span<ulong> mask = stackalloc ulong[Masks.Words(capacity)];
        IArrowArray[] arguments = new IArrowArray[program.Columns.Count];
        RecordBatch? testBatch = null;
        try
        {
            columns = settings.Columns(builder, groups);
            tests = settings.TestColumns(testBuilder, groups);
            foreach (CsvColumn column in columns) column.Clean = clean;
            foreach (CsvColumn column in tests) column.Clean = clean;
            Span<CsvField> fields = table.AsSpan(0, capacity * decoded);
            Span<CsvField> chosen = compact.AsSpan(0, capacity * delivered);
            Span<int> recordStarts = starts.AsSpan(0, capacity);
            Span<int> rowsKept = kept.AsSpan(0, capacity);
            int pos = from, inBatch = 0;
            long delivered0 = 0; // rows delivered by the unit (kept, within maxRows)
            bool reserved = false, full = false;
            while (pos < bytes.Length && !full && delivered0 < maxRows)
            {
                testBatch?.Dispose();
                testBatch = null;
                int cap = Math.Min(capacity, batchSize - inBatch);
                int chunkStart = pos;
                int n = tokenizer.Records(bytes, ref pos, settings.FileColumns, settings.Slots, fields, recordStarts, cap, out FieldEnd end, out int failedColumn);
                // Phase 1, the tested columns over the records before the first input error; a refused value moves the
                // limit to its record.
                int limit = n;
                CsvUnitError? error = null;
                if (end != FieldEnd.Record)
                    error = new CsvUnitError(unit.Rows + n, tokenizer.CountLines(bytes[..pos]), Malformed(end, failedColumn, settings), null);
                if (invalid >= 0 && invalid < pos)
                {
                    int row = RowAt(recordStarts[..n], invalid);
                    error = new CsvUnitError(unit.Rows + row, tokenizer.CountLines(bytes[..recordStarts[row]]), charset.Invalid, null);
                    limit = row;
                }
                int tested = limit;
                // The tested columns' builders are emptied by every chunk's export: sized for the chunk at once.
                if (limit > 0)
                    for (int t = 0; t < tests.Length; t++) tests[t].Reserve(limit, fields.Slice(settings.Tests[t] * cap, Math.Min(limit, 64)));
                foreach (int t in settings.TestOrder)
                {
                    // In output order: of two values refused in one record, the first column's is reported (as unfiltered).
                    if (limit == 0) break;
                    int refused = tests[t].Decode(bytes, fields.Slice(settings.Tests[t] * cap, limit), limit, out CsvValueError? value);
                    if (refused >= 0 && refused < limit)
                    {
                        error = new CsvUnitError(unit.Rows + refused, tokenizer.CountLines(bytes[..recordStarts[refused]]), value!.Message, value);
                        limit = refused;
                    }
                }
                int count = 0;
                bool all = false; // every record kept: the kept rows are 0 … count - 1 (also once MaxRows cuts them)
                if (limit > 0)
                {
                    // Columns cut short by a refused value are filled with nulls to the records decoded; rows from the
                    // limit on are not kept.
                    if (testBuilder is not null)
                    {
                        foreach (NativeColumnBuilder child in testBuilder.Children)
                            while (child.Length < tested) child.AppendNull();
                        testBuilder.EndStructs(tested);
                    }
                    // Kept until the delivered columns are built (they may take values from it).
                    testBatch = settings.ExportTests(testBuilder);
                    foreach (CsvColumn test in tests) test.EndBatch();
                    for (int a = 0; a < arguments.Length; a++) arguments[a] = testBatch!.Column(a);
                    count = program.Evaluate(arguments, tested, mask);
                    System.Array.Clear(arguments);
                    if (limit < tested)
                    {
                        Kaminari.Sazanami.Kernels.Bits.Fill(System.Runtime.InteropServices.MemoryMarshal.AsBytes(mask), limit, tested - limit, false);
                        count = Masks.Count(mask[..Masks.Words(tested)]);
                    }
                    all = count == n;
                    int at = all ? count : 0; // the identity is not listed
                    for (int w = 0; w < Masks.Words(tested) && at < count; w++)
                        for (ulong word = mask[w]; word != 0; word &= word - 1)
                            rowsKept[at++] = 64 * w + System.Numerics.BitOperations.TrailingZeroCount(word);
                }
                else if (testBuilder is not null)
                {
                    // Nothing decoded to test: the builder is emptied for the next chunk.
                    foreach (NativeColumnBuilder child in testBuilder.Children)
                        while (child.Length < tested) child.AppendNull();
                    testBuilder.EndStructs(tested);
                    settings.ExportTests(testBuilder)?.Dispose();
                    foreach (CsvColumn test in tests) test.EndBatch();
                }
                // MaxRows: once the rows asked are kept, nothing after the last of them is read (nor fails).
                if (delivered0 + count >= maxRows)
                {
                    count = (int)(maxRows - delivered0);
                    full = true;
                    error = null;
                }
                // Every record kept: the table is read as it is.
                int scanned = full ? (count == 0 ? 0 : (all ? count - 1 : rowsKept[count - 1]) + 1) : limit;
                // Phase 2, the delivered columns for the kept records, from a table of their fields alone.
                for (int c = 0; c < delivered && !all; c++)
                {
                    ReadOnlySpan<CsvField> source = fields.Slice(output[c] * cap, n);
                    Span<CsvField> to = chosen.Slice(c * capacity, count);
                    for (int i = 0; i < to.Length; i++) to[i] = source[rowsKept[i]];
                }
                if (!reserved && count > 0)
                {
                    reserved = true;
                    long expected = Math.Min(Math.Min(batchSize - inBatch, maxRows - delivered0),
                        (long)((bytes.Length - chunkStart) * (double)count / Math.Max(1, pos - chunkStart) * 1.0625) + 1);
                    for (int c = 0; c < columns.Length; c++) columns[c].Reserve(expected, all ? fields.Slice(output[c] * cap, count) : chosen.Slice(c * capacity, count));
                    numbers?.Reserve(expected);
                }
                int limitKept = count;
                for (int c = 0; c < columns.Length; c++)
                {
                    ReadOnlySpan<CsvField> fieldsKept = all ? fields.Slice(output[c] * cap, limitKept) : chosen.Slice(c * capacity, limitKept);
                    int refused;
                    CsvValueError? value;
                    // A column the filter read: its kept values (checked for every record before the limit) taken as
                    // they are, when its builder can.
                    if (limitKept > 0 && settings.TakenFrom[c] is int t and >= 0 && testBatch is not null
                        && columns[c].Builder.AppendRows(testBatch.Column(t).Data, all ? default : rowsKept[..limitKept], limitKept)) continue;
                    if (columns[c] is TemporalColumnBase { Group: null } temporal)
                    {
                        // The group comes from the first value that reads in file order, kept or not (a chunk keeping no
                        // record still finds it): kept records before it are decoded without one, those from it on in it.
                        int first = temporal.FirstReading(bytes, fields.Slice(output[c] * cap, scanned), scanned, out TemporalPattern? group);
                        if (limitKept == 0)
                        {
                            if (group is { } found) temporal.Adopt(found);
                            continue;
                        }
                        int before = 0;
                        while (before < limitKept && (all ? before : rowsKept[before]) < first) before++;
                        refused = temporal.Decode(bytes, fieldsKept[..before], before, out value);
                        if (refused < 0 && group is { } g)
                        {
                            // Taken even when no kept record follows it in the chunk: later chunks read in it.
                            temporal.Adopt(g);
                            if (before < limitKept)
                            {
                                refused = temporal.Decode(bytes, fieldsKept[before..], limitKept - before, out value);
                                if (refused >= 0) refused += before;
                            }
                        }
                    }
                    else if (limitKept == 0) continue;
                    else refused = columns[c].Decode(bytes, fieldsKept, limitKept, out value);
                    if (refused >= 0 && refused < limitKept)
                    {
                        int row = all ? refused : rowsKept[refused];
                        error = new CsvUnitError(unit.Rows + row, tokenizer.CountLines(bytes[..recordStarts[row]]), value!.Message, value);
                        limitKept = refused;
                    }
                }
                if (error is not null)
                {
                    unit.Dispose();
                    unit.Error = new CsvUnitError(error.Row, error.LinesBefore, error.Message, error.Value) { Delivered = delivered0 + limitKept };
                    unit.Guesses = Guesses(settings, columns, tests);
                    return unit;
                }
                builder?.EndStructs(count);
                if (numbers is not null)
                {
                    Span<long> slots = numbers.AppendSlots(count);
                    for (int i = 0; i < count; i++) slots[i] = unit.Rows + (all ? i : rowsKept[i]);
                }
                inBatch += count;
                delivered0 += count;
                unit.Rows += full ? scanned : n;
                if (inBatch == batchSize)
                {
                    unit.Batches.Add(Export(inBatch));
                    inBatch = 0;
                    reserved = false;
                }
            }
            if (inBatch > 0) unit.Batches.Add(Export(inBatch));
            if (countLines) unit.Lines = tokenizer.CountLines(bytes);
            unit.Guesses = Guesses(settings, columns, tests);
            return unit;
        }
        catch
        {
            unit.Dispose();
            throw;
        }
        finally
        {
            testBatch?.Dispose();
            foreach (CsvColumn column in columns) column.Dispose();
            foreach (CsvColumn column in tests) column.Dispose();
            builder?.Dispose();
            testBuilder?.Dispose();
            numbers?.Dispose();
            table.Dispose();
            compact.Dispose();
            starts.Dispose();
            kept.Dispose();
        }

        // The rows built as a batch, numbered first when asked (by record in the unit, shifted as delivered).
        RecordBatch Export(int rows)
        {
            RecordBatch batch = settings.Export(builder, rows);
            foreach (CsvColumn column in columns) column.EndBatch();
            if (numbers is null) return batch;
            IArrowArray index;
            try { index = numbers.ExportArray(); }
            catch
            {
                batch.Dispose();
                throw;
            }
            return new RecordBatch(settings.Indexed!, [index, .. batch.Arrays], rows);
        }
    }

    // Per decoded column, the group a temporal column decoded without one took (a column both tested and delivered: the
    // tested decoder's, unsure when the two differ), or null when none was decoded without one.
    private static (TemporalPattern?, bool)[]? Guesses(CsvDecodeSettings settings, CsvColumn[] columns, CsvColumn[] tests)
    {
        (TemporalPattern? Group, bool Unsure)[]? guesses = null;
        Take(tests, settings.Tests);
        Take(columns, settings.Output);
        return guesses;

        void Take(CsvColumn[] decoders, int[] decoded)
        {
            for (int c = 0; c < decoders.Length; c++)
            {
                if (decoders[c] is not TemporalColumnBase { Seeded: false } temporal) continue;
                guesses ??= new (TemporalPattern?, bool)[settings.Decoded.FieldsList.Count];
                ref (TemporalPattern? Group, bool Unsure) guess = ref guesses[decoded[c]];
                bool differs = guess.Group is { } before && temporal.Group is { } now && before != now;
                guess = (guess.Group ?? temporal.Group, guess.Unsure || temporal.RefusedUnseeded || differs);
            }
        }
    }

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
