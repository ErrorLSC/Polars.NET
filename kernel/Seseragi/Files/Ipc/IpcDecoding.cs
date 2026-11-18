using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Arrow.Compute;
using Kaminari.Sazanami.Arrow.Ipc;
using Kaminari.Sazanami.Filters;

namespace Kaminari.Seseragi.Files.Ipc;

/// <summary>
/// What a reader decodes from each record batch: the selected columns, and (with <see cref="IpcReadOptions.FilterRows"/>)
/// the columns a row filter reads, decoded and evaluated first, the other columns decoded only when it keeps a row.
/// Immutable: batches are decoded with it on several threads at once.
/// </summary>
internal sealed class IpcDecoding
{
    private readonly IpcSchema _schema;
    private readonly bool[]? _selected;
    private readonly bool _validateUtf8;
    private readonly int _parallelism;
    private readonly long _parallelFrom;
    private readonly RowFilter? _filter;

    public IpcDecoding(IpcSchema schema, IpcReadOptions options)
    {
        _schema = schema;
        _selected = IpcFraming.Select(schema, options.Columns);
        Schema = IpcFraming.Project(schema, _selected);
        _validateUtf8 = options.ValidateUtf8;
        _parallelism = options.MaxDegreeOfParallelism;
        _parallelFrom = options.ParallelDecompressFrom;
        FiltersExactly = options.FilterRows;
        bool[]? read = _selected;
        if (options.FilterRows && options.Filter is { } filter)
        {
            LoweredFilter lowered = FilterLowering.lower(schema.Schema, filter);
            FiltersExactly = lowered.Exact;
            if (Microsoft.FSharp.Core.OptionModule.IsSome(lowered.Program))
            {
                _filter = new RowFilter(lowered.Program.Value, schema, _selected);
                if (Schema.FieldsList.FirstOrDefault(f => !Filterable(f.DataType)) is { } field)
                    throw new NotSupportedException($"Rows of column '{field.Name}' ({field.DataType.Name}) cannot be filtered: "
                        + "list views, unions and run-end encoded arrays are read whole only (leave FilterRows off, or the column out).");
                read = Union(_selected, _filter.Reads, schema.Fields.Length);
            }
        }
        Read = read;
        DictionaryIds = IpcFraming.DictionaryIds(schema.Fields, read);
    }

    /// <summary>The schema of the batches (the selected columns).</summary>
    public Schema Schema { get; }

    /// <summary>Whether the batches hold exactly the rows the filter keeps (see <see cref="IpcReader.FiltersExactly"/>).</summary>
    public bool FiltersExactly { get; }

    /// <summary>Whether rows are filtered (batches may then come empty: readers pass over them).</summary>
    public bool Filters => _filter is not null;

    /// <summary>The top-level fields whose buffers are read: the selected ones and those the filter reads (null: all).</summary>
    public bool[]? Read { get; }

    /// <summary>The dictionaries the fields read reach.</summary>
    public HashSet<long> DictionaryIds { get; }

    /// <summary>Which buffers of a message the fields read need.</summary>
    public bool[] Plan(IpcMessage message) => IpcIO.Plan(_schema.Fields, Read, message, message.Version);

    /// <summary>
    /// Decodes a record batch from its body (released once the batch holds what it needs): its first
    /// <paramref name="rows"/> rows, or with a filter the first <paramref name="rows"/> rows it keeps.
    /// </summary>
    /// <exception cref="IpcFormatException">The batch is damaged or invalid.</exception>
    public RecordBatch Decode(IpcMessage message, IpcBody body, IpcDictionaries dictionaries, long rows)
    {
        IpcRecordBatch header = message.RecordBatch!;
        IpcMetadataVersion version = message.Version;
        try
        {
            if (_filter is null)
            {
                body.Prepare(header, Plan(message), _parallelism, _parallelFrom);
                IpcArray?[] arrays = IpcBatchDecoder.Decode(_schema.Fields, _selected, header, version, body, dictionaries, _validateUtf8);
                return IpcFraming.Assemble(Schema, arrays, (int)Math.Min(rows, header.Length));
            }
            return Filtered(message, body, dictionaries, rows);
        }
        catch (InvalidDataException e)
        {
            throw IpcIO.Format(e);
        }
        finally
        {
            body.Memory.Sealed();
        }
    }

    private RecordBatch Filtered(IpcMessage message, IpcBody body, IpcDictionaries dictionaries, long most)
    {
        RowFilter filter = _filter!;
        IpcRecordBatch header = message.RecordBatch!;
        IpcMetadataVersion version = message.Version;
        int length = (int)header.Length;
        int fields = _schema.Fields.Length;
        var columns = new IArrowArray?[fields];
        try
        {
            // The filter's columns first.
            body.Prepare(header, IpcIO.Plan(_schema.Fields, filter.Reads, message, version), _parallelism, _parallelFrom);
            Arrays(IpcBatchDecoder.Decode(_schema.Fields, filter.Reads, header, version, body, dictionaries, _validateUtf8));
            ulong[] mask = new ulong[Masks.Words(length)];
            IArrowArray[] arguments = [.. filter.Arguments.Select(a => Argument(columns[a.Column]!, a.Fields))];
            int kept = filter.Program.Evaluate(arguments, length, mask);
            if (kept > most) kept = Truncate(mask, (int)most);
            if (kept == 0) return Empty();

            // Then the other selected columns.
            bool[] rest = [.. Enumerable.Range(0, fields).Select(i => (_selected is null || _selected[i]) && !filter.Reads[i])];
            body.Prepare(header, IpcIO.Plan(_schema.Fields, rest, message, version), _parallelism, _parallelFrom);
            Arrays(IpcBatchDecoder.Decode(_schema.Fields, rest, header, version, body, dictionaries, _validateUtf8));

            List<IArrowArray> output = new(Schema.FieldsList.Count);
            try
            {
                for (int i = 0; i < fields; i++)
                {
                    if (_selected is not null && !_selected[i]) continue;
                    output.Add(kept == length ? Take(i) : ArrowFilter.Filter(columns[i]!, mask));
                }
            }
            catch
            {
                foreach (IArrowArray array in output) array.Dispose();
                throw;
            }
            return new RecordBatch(Schema, output, kept);
        }
        finally
        {
            foreach (IArrowArray? column in columns) column?.Dispose();
        }

        void Arrays(IpcArray?[] decoded)
        {
            for (int i = 0; i < decoded.Length; i++)
            {
                if (decoded[i] is { } array) columns[i] = ArrowArrayFactory.BuildArray(IpcBatchDecoder.Prefix(array, length).ToArrayData());
            }
        }

        IArrowArray Take(int i)
        {
            IArrowArray array = columns[i]!;
            columns[i] = null;
            return array;
        }
    }

    /// <summary>A batch of no rows.</summary>
    private RecordBatch Empty() =>
        new(Schema, [.. Schema.FieldsList.Select(f => ArrowArrayFactory.BuildArray(IpcBatchDecoder.Empty(f.DataType).ToArrayData()))], 0);

    /// <summary>Keeps the first <paramref name="most"/> set bits of a mask; returns how many it keeps.</summary>
    private static int Truncate(Span<ulong> mask, int most)
    {
        int left = most;
        for (int w = 0; w < mask.Length; w++)
        {
            int set = BitOperations.PopCount(mask[w]);
            if (set <= left)
            {
                left -= set;
                continue;
            }
            ulong word = mask[w];
            ulong keep = 0;
            for (; left > 0; left--)
            {
                ulong lowest = word & (ulong)-(long)word;
                keep |= lowest;
                word ^= lowest;
            }
            mask[w] = keep;
            mask[(w + 1)..].Clear();
            break;
        }
        return most - left;
    }

    /// <summary>A struct field of a column, down <paramref name="fields"/>.</summary>
    private static IArrowArray Argument(IArrowArray column, string[] fields)
    {
        IArrowArray current = column;
        foreach (string name in fields)
        {
            var parent = (StructArray)current;
            current = parent.Fields[((StructType)parent.Data.DataType).GetFieldIndex(name)];
        }
        return current;
    }

    /// <summary>Whether <see cref="ArrowFilter"/> filters arrays of a type.</summary>
    private static bool Filterable(IArrowType type) => type switch
    {
        ListViewType or LargeListViewType or UnionType or RunEndEncodedType => false,
        DictionaryType d => Filterable(d.ValueType),
        NestedType n => n.Fields.All(f => Filterable(f.DataType)),
        _ => true,
    };

    private static bool[]? Union(bool[]? a, bool[] b, int length) =>
        a is null ? null : [.. Enumerable.Range(0, length).Select(i => a[i] || b[i])];

    /// <summary>A lowered row filter: its program, the top-level fields it reads and, per argument, the field and the struct fields below it.</summary>
    private sealed class RowFilter
    {
        public RowFilter(FilterProgram program, IpcSchema schema, bool[]? selected)
        {
            Program = program;
            Reads = new bool[schema.Fields.Length];
            Arguments = new (int, string[])[program.Columns.Count];
            for (int a = 0; a < Arguments.Length; a++)
            {
                string[] path = program.Columns[a].Path;
                int column = schema.Schema.GetFieldIndex(path[0]);
                if (column < 0) throw new ArgumentException($"The filter reads column '{path[0]}', which the Arrow IPC data does not have.");
                Reads[column] = true;
                Arguments[a] = (column, path[1..]);
            }
        }

        public FilterProgram Program { get; }

        public bool[] Reads { get; }

        public (int Column, string[] Fields)[] Arguments { get; }
    }
}
