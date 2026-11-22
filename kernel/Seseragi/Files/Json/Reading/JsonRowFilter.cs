using Apache.Arrow;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Arrow.Compute;
using Kaminari.Sazanami.Filters;

namespace Kaminari.Seseragi.Files.Json.Reading;

/// <summary>
/// A row filter evaluated on each decoded batch: the batch holds the selected columns and those the filter reads (in
/// schema order); the rows it keeps of the selected columns are delivered.
/// </summary>
internal sealed class JsonRowFilter
{
    private readonly FilterProgram? _program;
    private readonly (int Column, string[] Fields)[] _arguments;
    private readonly int[] _output;

    private JsonRowFilter(FilterProgram? program, (int, string[])[] arguments, int[] output, Schema decoded, Schema selected)
    {
        _program = program;
        _arguments = arguments;
        _output = output;
        Decoded = decoded;
        Selected = selected;
    }

    /// <summary>The columns decoded: the selected ones and those the filter reads, in schema order.</summary>
    public Schema Decoded { get; }

    /// <summary>The columns delivered.</summary>
    public Schema Selected { get; }

    /// <summary>
    /// The filter of <paramref name="filter"/> over <paramref name="full"/>'s columns, delivering
    /// <paramref name="selected"/>'s; <paramref name="exact"/> tells whether every conjunct is evaluated here.
    /// </summary>
    public static JsonRowFilter Create(Schema full, Schema selected, ScalarExpr filter, out bool exact)
    {
        LoweredFilter lowered = FilterLowering.lower(full, filter);
        exact = lowered.Exact;
        FilterProgram? program = Microsoft.FSharp.Core.OptionModule.IsSome(lowered.Program) ? lowered.Program.Value : null;
        HashSet<string> read = [.. selected.FieldsList.Select(f => f.Name)];
        foreach (FilterColumn column in program?.Columns ?? [])
        {
            if (full.GetFieldIndex(column.Path[0]) < 0)
                throw new ArgumentException($"The filter reads column '{column.Path[0]}', which the JSON columns do not have.");
            read.Add(column.Path[0]);
        }
        var decoded = new Schema([.. full.FieldsList.Where(f => read.Contains(f.Name))], full.Metadata);
        (int, string[])[] arguments = [.. (program?.Columns ?? []).Select(c => (decoded.GetFieldIndex(c.Path[0]), c.Path[1..]))];
        int[] output = [.. selected.FieldsList.Select(f => decoded.GetFieldIndex(f.Name))];
        return new JsonRowFilter(program, arguments, output, decoded, selected);
    }

    /// <summary>
    /// The rows of <paramref name="batch"/> (of the decoded columns, disposed here) the filter keeps, of the selected
    /// columns; null when it keeps none.
    /// </summary>
    public RecordBatch? Apply(RecordBatch batch) => Apply(batch, out _);

    /// <inheritdoc cref="Apply(RecordBatch)"/>
    /// <param name="batch">The batch.</param>
    /// <param name="kept">The rows kept (<see cref="Masks"/> layout); null when all of them are.</param>
    public RecordBatch? Apply(RecordBatch batch, out ulong[]? kept)
    {
        kept = null;
        int length = batch.Length;
        ulong[]? mask = null;
        int count = length;
        if (_program is not null && length > 0)
        {
            mask = new ulong[Masks.Words(length)];
            try
            {
                IArrowArray[] arguments = [.. _arguments.Select(a => Argument(batch.Column(a.Column), a.Fields))];
                count = _program.Evaluate(arguments, length, mask);
            }
            catch
            {
                batch.Dispose();
                throw;
            }
        }
        if (count == 0)
        {
            batch.Dispose();
            return null;
        }
        if (count == length) return Select(batch);
        kept = mask;
        return Take(batch, mask!, count);
    }

    /// <summary>The first <paramref name="rows"/> rows of <paramref name="batch"/> (of the selected columns, disposed here).</summary>
    public static RecordBatch First(RecordBatch batch, int rows)
    {
        ulong[] mask = new ulong[Masks.Words(batch.Length)];
        Masks.SetRange(mask, 0, rows);
        return Take(batch, mask, rows, null);
    }

    // The selected columns of a batch keeping all its rows: the others are freed.
    private RecordBatch Select(RecordBatch batch)
    {
        if (_output.Length == batch.ColumnCount) return batch;
        var keep = new bool[batch.ColumnCount];
        foreach (int c in _output) keep[c] = true;
        for (int c = 0; c < keep.Length; c++)
            if (!keep[c]) batch.Column(c).Dispose();
        return new RecordBatch(Selected, [.. _output.Select(batch.Column)], batch.Length);
    }

    private RecordBatch Take(RecordBatch batch, ulong[] mask, int kept) => Take(batch, mask, kept, this);

    // The rows a mask selects, copied out of the batch (disposed).
    private static RecordBatch Take(RecordBatch batch, ulong[] mask, int kept, JsonRowFilter? filter)
    {
        try
        {
            int[] columns = filter?._output ?? [.. Enumerable.Range(0, batch.ColumnCount)];
            List<IArrowArray> output = new(columns.Length);
            try
            {
                foreach (int c in columns) output.Add(ArrowFilter.Filter(batch.Column(c), mask));
            }
            catch
            {
                foreach (IArrowArray array in output) array.Dispose();
                throw;
            }
            return new RecordBatch(filter?.Selected ?? batch.Schema, output, kept);
        }
        finally
        {
            batch.Dispose();
        }
    }

    // A struct field of a column, down the field names.
    private static IArrowArray Argument(IArrowArray column, string[] fields)
    {
        IArrowArray current = column;
        foreach (string name in fields)
        {
            var parent = (StructArray)current;
            current = parent.Fields[((Apache.Arrow.Types.StructType)parent.Data.DataType).GetFieldIndex(name)];
        }
        return current;
    }
}
