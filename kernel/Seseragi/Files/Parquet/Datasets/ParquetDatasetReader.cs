using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Arrow;
using Kaminari.Sazanami.Filters;
using Kaminari.Seseragi.Files.Parquet.Schema;
using Microsoft.FSharp.Core;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// Reads a dataset's files one after another, each as <see cref="ParquetFile.OpenReader(ParquetReadOptions?)"/> does
/// with the options given, into batches of one schema: batches never span files. The footers of the next files are
/// read while a file is read.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item><see cref="ParquetReadOptions.Columns"/> and <see cref="ParquetReadOptions.ColumnPaths"/> name the dataset's
/// columns (all of them when both are null): columns from files and partition columns. Batches hold the columns from
/// files in the dataset's order (a partition column of the same name in its place, holding the path's value), then
/// the other partition columns, then the file path.</item>
/// <item>A file's column converts to the dataset's type: as it is when the types agree (nullability aside); else by a
/// lossless widening (wider integers, floats holding the integers, decimals of no fewer digits, finer time units, dates
/// into timestamps, struct fields matched by name — those the file has beyond the dataset's left out —, the elements
/// of lists and maps, dictionaries into their values and back); by an approximating conversion only with
/// <see cref="ParquetDatasetOptions.AllowLossyCasts"/>; else the read fails naming the file and the column. A column
/// or struct field a file lacks is an error, or nulls with <see cref="ParquetDatasetOptions.AllowMissingColumns"/>
/// (always when united by name).</item>
/// <item><see cref="ParquetReadOptions.Filter"/>: its conjuncts over partition columns only decide which files are
/// read (exactly, on the files' partition values); the others prune (and with <see cref="ParquetReadOptions.FilterRows"/>
/// filter) every file read, partition columns replaced by the file's values and columns the file lacks by nulls.
/// Conjuncts over a column a file's values convert into lossily, or with struct fields it lacks, do not prune that
/// file; where the file's own filtering cannot be exact, the dataset filters its rows after the conversion, so that
/// <see cref="FiltersExactly"/> holds for every file.</item>
/// <item><see cref="ParquetReadOptions.RowIndexColumn"/> numbers the rows across the files, in the order listed, as if
/// they were one (the numbers of each file follow the previous files' row counts, rows of files left out included):
/// the first column.</item>
/// <item><see cref="ParquetReadOptions.MaxRows"/> counts the rows delivered across the files.</item>
/// <item><see cref="ParquetReadOptions.RowGroups"/> and <see cref="ParquetReadOptions.Rows"/> number per file: they
/// are refused (rows are selected per file by <see cref="ParquetDatasetFile.Rows"/>).</item>
/// </list>
/// Errors in a file name it.
/// </remarks>
public sealed class ParquetDatasetReader : IDisposable, IAsyncDisposable
{
    /// <summary>How many files ahead of the one read are opened (their footers read) at once.</summary>
    private const int FilesAhead = 16;

    private readonly ParquetDataset _dataset;
    private readonly ParquetReadOptions _options;
    private readonly bool _allowMissing;
    private readonly ArrowConversionRules _rules;
    private readonly Dictionary<string, Field> _columns;   // the dataset's columns from files, whole, as these options read them
    private readonly Target[] _targets;                   // the columns from files selected, in the dataset's order
    private readonly Column[] _layout;
    private readonly bool[] _read;
    private readonly ScalarExpr? _fileFilter;
    private readonly ScalarExpr[] _mixed;
    private readonly ScalarExpr? _rowFilter;              // what the rows delivered must pass (the conjuncts not decided per file)
    private readonly FilterProgram? _program;             // _rowFilter as the dataset applies it after converting (exact)
    private readonly string[] _filterRoots;               // the columns from files the row filter reads

    /// <summary>A column from files selected: whole, or the struct fields on its paths; its type in the batches.</summary>
    private sealed record Target(string Name, string[][]? Paths, Field Field);

    /// <summary>A column of the batches after the row index: a selected column from files, or a partition column.</summary>
    private readonly record struct Column(Field Field, int Target, int Partition);

    internal ParquetDatasetReader(ParquetDataset dataset, ParquetReadOptions options)
    {
        options.Validate();
        if (options.RowGroups is not null)
            throw new ArgumentException("Row groups are numbered per file: a dataset reads them all (select rows per file with ParquetDatasetFile.Rows).", nameof(options));
        if (options.Rows is not null)
            throw new ArgumentException("Rows are numbered per file: select them with ParquetDatasetFile.Rows.", nameof(options));
        if (options.Filter is { } given) options = options with { Filter = FilterShapes.canonical(given) };
        _dataset = dataset;
        _options = options;
        _allowMissing = dataset.Options.AllowMissingColumns || dataset.Options.Schema is null && dataset.Options.SchemaMode == DatasetSchemaMode.UnionByName;
        _rules = new ArrowConversionRules(dataset.Options.AllowLossyCasts, _allowMissing);
        Partitions? partitions = dataset.Partitions;
        bool IsPartition(string name) => partitions?.IndexOf(name) >= 0;

        Apache.Arrow.Schema columns = dataset.ColumnsFor(options);
        _columns = columns.FieldsList.ToDictionary(f => f.Name, StringComparer.Ordinal);

        // The columns selected: from files (not shadowed by a partition column), and partition columns.
        bool all = options.Columns is null && options.ColumnPaths is null;
        foreach (string name in options.Columns ?? [])
        {
            if (!IsPartition(name) && !_columns.ContainsKey(name))
                throw new ArgumentException($"The dataset has no column '{name}' (the columns are those of the files and the partitions).", nameof(options));
        }
        Dictionary<string, List<string[]>> paths = new(StringComparer.Ordinal);
        foreach (string[] selected in options.ColumnPaths ?? [])
        {
            if (selected is null || selected.Length == 0) throw new ArgumentException("A column path is empty.", nameof(options));
            if (IsPartition(selected[0])) throw new ArgumentException($"'{selected[0]}' is a partition column: it has no fields to select.", nameof(options));
            if (!_columns.ContainsKey(selected[0])) throw new ArgumentException($"The dataset has no column '{selected[0]}'.", nameof(options));
            (paths.TryGetValue(selected[0], out var list) ? list : paths[selected[0]] = []).Add(selected[1..]);
        }
        List<Target> targets = [];
        foreach (Field field in columns.FieldsList)
        {
            if (IsPartition(field.Name)) continue;
            Field nullable = field.IsNullable ? field : new Field(field.Name, field.DataType, nullable: true, field.Metadata);
            if (all || options.Columns?.Contains(field.Name) == true) targets.Add(new Target(field.Name, null, nullable));
            else if (paths.TryGetValue(field.Name, out var below))
                targets.Add(new Target(field.Name, [.. below], new Field(field.Name, Prune(field.DataType, below, field.Name), nullable: true, field.Metadata)));
        }
        _targets = [.. targets];
        bool[] partitionsRead = [.. Enumerable.Range(0, partitions?.Fields.Length ?? 0).Select(p => all || options.Columns?.Contains(partitions!.Fields[p].Name) == true)];

        // The filter: conjuncts over partition columns only decide which files are read; the others go to the files,
        // those over both with the partition columns replaced by each file's values.
        _read = [.. Enumerable.Repeat(true, dataset.Files.Count)];
        bool exact = true;
        List<ScalarExpr> onFiles = [], mixed = [];
        if (options.Filter is { } filter)
        {
            List<ScalarExpr> onPartitions = [];
            foreach (ScalarExpr conjunct in FilterShapes.conjuncts(filter))
            {
                string[] names = [.. ScalarExprOps.collectColumns(conjunct)];
                int inPartitions = names.Count(IsPartition);
                (inPartitions == 0 ? onFiles : inPartitions == names.Length ? onPartitions : mixed).Add(conjunct);
            }
            if (onPartitions.Count > 0) exact = Prune(partitions!, onPartitions.Aggregate(And), _read);
        }
        _fileFilter = onFiles.Count == 0 ? null : onFiles.Aggregate(And);
        _mixed = [.. mixed];
        _rowFilter = onFiles.Count + mixed.Count == 0 ? null : onFiles.Concat(mixed).Aggregate(And);

        // Exact when the filter lowers exactly over the dataset's types (each file's rows are filtered again after
        // conversion where its own filtering is not exact).
        _filterRoots = [];
        FiltersExactly = options.FilterRows && exact;
        if (options.FilterRows && _rowFilter is { } rows)
        {
            List<Field> fields = [.. columns.FieldsList.Select(f => partitions?.IndexOf(f.Name) is int p and >= 0 ? dataset.PartitionField(p, options.ViewTypes) : f)];
            for (int p = 0; p < (partitions?.Fields.Length ?? 0); p++)
            {
                if (!_columns.ContainsKey(partitions!.Fields[p].Name)) fields.Add(dataset.PartitionField(p, options.ViewTypes));
            }
            LoweredFilter lowered = FilterLowering.lower(new Apache.Arrow.Schema(fields, null), rows);
            FiltersExactly &= lowered.Exact;
            if (FiltersExactly && OptionModule.IsSome(lowered.Program))
            {
                _program = lowered.Program.Value;
                _filterRoots = [.. _program.Columns.Select(c => c.Path[0]).Where(n => !IsPartition(n)).Distinct()];
            }
        }

        List<Column> layout = [];
        foreach (Field field in columns.FieldsList)
        {
            int p = partitions?.IndexOf(field.Name) ?? -1;
            if (p >= 0)
            {
                if (partitionsRead[p]) layout.Add(new Column(dataset.PartitionField(p, options.ViewTypes), -1, p));
            }
            else if (System.Array.FindIndex(_targets, t => t.Name == field.Name) is int t and >= 0) layout.Add(new Column(_targets[t].Field, t, -1));
        }
        for (int p = 0; p < partitionsRead.Length; p++)
        {
            if (partitionsRead[p] && !_columns.ContainsKey(partitions!.Fields[p].Name))
                layout.Add(new Column(dataset.PartitionField(p, options.ViewTypes), -1, p));
        }
        _layout = [.. layout];
        List<Field> output = [.. _layout.Select(c => c.Field)];
        if (options.RowIndexColumn is { } index)
        {
            if (output.Any(f => f.Name == index)) throw new ArgumentException($"The row index column '{index}' has the name of another column read.", nameof(options));
            output.Insert(0, new Field(index, Int64Type.Default, nullable: false));
        }
        if (dataset.Options.FilePathColumn is { } path)
        {
            if (output.Any(f => f.Name == path)) throw new ArgumentException($"The file path column '{path}' has the name of another column read.", nameof(options));
            output.Add(new Field(path, options.ViewTypes ? StringViewType.Default : StringType.Default, nullable: false));
        }
        Schema = new Apache.Arrow.Schema(output, columns.Metadata);

        static ScalarExpr And(ScalarExpr a, ScalarExpr b) => ScalarExpr.NewBinary(BinaryOp.And, a, b);
    }

    /// <summary>The schema of the batches: the row index if asked, the columns read (nullable), the file path if asked.</summary>
    public Apache.Arrow.Schema Schema { get; }

    /// <summary>
    /// Whether, with <see cref="ParquetReadOptions.FilterRows"/>, the batches hold exactly the rows the filter keeps:
    /// when the filter lowers exactly over the dataset's types and its conjuncts over partition columns are decided
    /// exactly (files whose own filtering is not exact are filtered again after their columns convert).
    /// </summary>
    public bool FiltersExactly { get; }

    /// <summary>The files a filter over partition columns leaves (for tests).</summary>
    internal IReadOnlyList<bool> FilesRead => _read;

    /// <summary>The files whose rows the dataset filtered after converting them (for tests).</summary>
    internal int FilesFilteredAfter => _filteredAfter;

    private int _filteredAfter;

    /// <summary>Reads the batches, file after file.</summary>
    /// <exception cref="ParquetFormatException">
    /// A file is damaged, is not a Parquet file, lacks a column read, holds a column of a type that does not convert
    /// to the dataset's, or a value beyond the range of the dataset's type (the message names the file).
    /// </exception>
    /// <exception cref="FileNotFoundException">A file named does not exist.</exception>
    public IEnumerable<RecordBatch> ReadBatches()
    {
        using var ahead = new Ahead(this, default);
        long? remaining = _options.MaxRows;
        long before = 0;
        for (int i = 0; i < _dataset.Files.Count && remaining != 0; i++)
        {
            if (!_read[i] && _options.RowIndexColumn is null) continue;
            ParquetFile file = ahead.Take(i).GetAwaiter().GetResult();
            try
            {
                if (_read[i])
                {
                    using FilePlan plan = Plan(file, i, before, remaining);
                    using IEnumerator<RecordBatch> batches = plan.Reader.ReadBatches().GetEnumerator();
                    while (remaining != 0 && Next(batches, i) is { } batch)
                    {
                        if (Complete(batch, plan, i, ref remaining) is { } done) yield return done;
                    }
                }
                before += RowCount(file);
            }
            finally
            {
                file.Dispose();
            }
        }
    }

    /// <inheritdoc cref="ReadBatches"/>
    public async IAsyncEnumerable<RecordBatch> ReadBatchesAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var ahead = new Ahead(this, cancellationToken);
        long? remaining = _options.MaxRows;
        long before = 0;
        for (int i = 0; i < _dataset.Files.Count && remaining != 0; i++)
        {
            if (!_read[i] && _options.RowIndexColumn is null) continue;
            ParquetFile file = await ahead.Take(i).ConfigureAwait(false);
            try
            {
                if (_read[i])
                {
                    using FilePlan plan = Plan(file, i, before, remaining);
                    await using IAsyncEnumerator<RecordBatch> batches = plan.Reader.ReadBatchesAsync(cancellationToken).GetAsyncEnumerator(cancellationToken);
                    while (remaining != 0 && await NextAsync(batches, i).ConfigureAwait(false) is { } batch)
                    {
                        if (Complete(batch, plan, i, ref remaining) is { } done) yield return done;
                    }
                }
                before += RowCount(file);
            }
            finally
            {
                await file.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>Reads the batches as an Arrow stream (each batch disposed when the next is read).</summary>
    public Apache.Arrow.Ipc.IArrowArrayStream ReadArrowStream() => new ParquetReader.OwningBatchStream(Schema, ReadBatches());

    /// <summary>Nothing is held between reads.</summary>
    public void Dispose() { }

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private string PathOf(int file) => _dataset.Files[file].Path;

    // ---- one file -------------------------------------------------------------------------------------------------

    /// <summary>
    /// How a file is read: its reader, and per column of the batches (and per column the dataset's filter reads
    /// after converting) the reader's column it comes from (-1: the file lacks it, nulls) and the conversion.
    /// </summary>
    private sealed class FilePlan(ParquetReader reader, (int Source, ArrowConversion Conversion)[] outputs,
        (int Source, ArrowConversion Conversion)[] filtered, bool filterAfter) : IDisposable
    {
        public ParquetReader Reader { get; } = reader;

        /// <summary>Per selected column from files (as <see cref="_targets"/>).</summary>
        public (int Source, ArrowConversion Conversion)[] Outputs { get; } = outputs;

        /// <summary>Per column from files the dataset's filter reads (as <see cref="_filterRoots"/>), when it filters this file.</summary>
        public (int Source, ArrowConversion Conversion)[] Filtered { get; } = filtered;

        /// <summary>Whether the dataset filters the file's rows after converting them.</summary>
        public bool FilterAfter { get; } = filterAfter;

        public void Dispose() => Reader.Dispose();
    }

    /// <summary>The plan of a file: its rows numbered after <paramref name="before"/> rows, up to <paramref name="remaining"/>.</summary>
    private FilePlan Plan(ParquetFile file, int index, long before, long? remaining)
    {
        string path = PathOf(index);
        Dictionary<string, Field> has;
        try
        {
            has = ArrowMapping.Map(file.Tree, file.Thrift, _options).Schema.FieldsList.ToDictionary(f => f.Name, StringComparer.Ordinal);
        }
        catch (ParquetFormatException e)
        {
            throw ParquetDataset.InFile(path, e);
        }

        // What the file's reader reads: whole columns and paths (those the file has).
        HashSet<string> whole = new(StringComparer.Ordinal);
        List<string[]> paths = [];
        foreach (Target target in _targets)
        {
            if (!has.TryGetValue(target.Name, out Field? field))
            {
                if (!_allowMissing) throw Missing(path, target.Name, filter: false);
                continue;
            }
            if (target.Paths is null)
            {
                whole.Add(target.Name);
                continue;
            }
            string[][] kept = [.. target.Paths.Where(p => Exists(field.DataType, p, 0))];
            if (kept.Length < target.Paths.Length && !_allowMissing)
                throw Missing(path, string.Join('.', [target.Name, .. target.Paths.First(p => !Exists(field.DataType, p, 0))]), filter: false);
            // Every field selected missing: the column is read whole (its rows' nulls), its fields then left out.
            if (kept.Length == 0) whole.Add(target.Name);
            else paths.AddRange(kept.Select(p => (string[])[target.Name, .. p]));
        }

        // The filter the file reads: columns it lacks as nulls; conjuncts over columns whose values convert (lossily, or
        // with struct fields it lacks) are left out, the file then filtered after converting.
        HashSet<string> missing = new(StringComparer.Ordinal), dropped = new(StringComparer.Ordinal);
        if (_rowFilter is not null)
        {
            foreach (string root in ScalarExprOps.collectColumns(_rowFilter))
            {
                if (!_columns.TryGetValue(root, out Field? target) || _dataset.Partitions?.IndexOf(root) >= 0) continue;
                if (!has.TryGetValue(root, out Field? field))
                {
                    if (!_allowMissing) throw Missing(path, root, filter: true);
                    missing.Add(root);
                }
                else if (!ArrowWidening.CanConvert(field.DataType, target.DataType, default)) dropped.Add(root);
            }
        }
        var (fileFilter, complete) = FileFilter(index, missing, dropped);

        bool after = FiltersExactly && _program is not null && !complete;
        ParquetReader reader = Open(file, index, before, after ? null : remaining, whole, paths, fileFilter, after);
        if (FiltersExactly && _program is not null && !after && !reader.FiltersExactly)
        {
            // The file's filtering is not exact: read again with what the dataset's filter needs.
            reader.Dispose();
            after = true;
            reader = Open(file, index, before, null, whole, paths, fileFilter, after);
        }
        try
        {
            Apache.Arrow.Schema read = reader.Schema;
            int skip = _options.RowIndexColumn is null ? 0 : 1;
            var outputs = new (int, ArrowConversion)[_targets.Length];
            for (int t = 0; t < _targets.Length; t++)
                outputs[t] = Conversion(read, skip, _targets[t].Name, _targets[t].Field.DataType, path);
            var filtered = after ? new (int, ArrowConversion)[_filterRoots.Length] : [];
            for (int f = 0; f < filtered.Length; f++) filtered[f] = Conversion(read, skip, _filterRoots[f], _columns[_filterRoots[f]].DataType, path);
            if (after) Interlocked.Increment(ref _filteredAfter);
            return new FilePlan(reader, outputs, filtered, after);
        }
        catch
        {
            reader.Dispose();
            throw;
        }
    }

    /// <summary>The file's reader: the columns read, and with <paramref name="after"/> the columns the dataset's filter reads, whole.</summary>
    private ParquetReader Open(ParquetFile file, int index, long before, long? remaining, HashSet<string> whole, List<string[]> paths, ScalarExpr? filter, bool after)
    {
        HashSet<string> columns = new(whole, StringComparer.Ordinal);
        List<string[]> parts = paths;
        if (after)
        {
            Dictionary<string, Field> has = file.Schema.FieldsList.ToDictionary(f => f.Name, StringComparer.Ordinal);
            foreach (string root in _filterRoots)
            {
                if (has.ContainsKey(root)) columns.Add(root);
            }
            parts = [.. paths.Where(p => !columns.Contains(p[0]))];
        }
        try
        {
            return file.OpenReader(_options with
            {
                Columns = [.. columns],
                ColumnPaths = parts.Count == 0 ? null : parts,
                Rows = _dataset.Files[index].Rows,
                RowIndexOffset = before,
                MaxRows = remaining,
                Filter = filter,
            });
        }
        catch (ParquetFormatException e)
        {
            throw ParquetDataset.InFile(PathOf(index), e);
        }
    }

    /// <summary>
    /// Where a column comes from in the reader's batches (-1 when the file lacks it: nulls) and how it converts to
    /// <paramref name="target"/>; a conversion the rules refuse is an error naming the file, the column and what allows it.
    /// </summary>
    private (int Source, ArrowConversion Conversion) Conversion(Apache.Arrow.Schema read, int skip, string name, IArrowType target, string path)
    {
        int at = -1;
        for (int c = skip; c < read.FieldsList.Count; c++)
        {
            if (read.FieldsList[c].Name == name) at = c;
        }
        if (at < 0) return (-1, new NullsConversion(target));
        IArrowType type = read.FieldsList[at].DataType;
        if (ArrowWidening.TryCompile(type, target, _rules, name, out ArrowConversion? conversion, out ArrowConversionError? error)) return (at, conversion);
        string hint = error.Problem switch
        {
            ArrowConversionProblem.Lossy => " (ParquetDatasetOptions.AllowLossyCasts allows lossy conversions)",
            ArrowConversionProblem.MissingField => " (ParquetDatasetOptions.AllowMissingColumns reads missing columns and fields as nulls)",
            _ when _dataset.Options.Schema is null && _dataset.Options.SchemaMode == DatasetSchemaMode.FirstFile && ArrowWidening.CanConvert(target, type, _rules) =>
                " (the dataset's types are the first file's: SchemaMode.UnionByName or a given Schema reads wider types in later files)",
            _ => "",
        };
        string what = error.Problem == ArrowConversionProblem.MissingField
            ? $"the file has no field '{error.Path}', which the dataset reads"
            : $"'{error.Path}': {error.Message} (the dataset's type)";
        throw new ParquetFormatException($"{path}: {what}{hint}.");
    }

    private static ParquetFormatException Missing(string path, string column, bool filter) =>
        new($"{path}: the file has no column '{column}', which the dataset {(filter ? "filters on" : "reads")} "
            + "(ParquetDatasetOptions.AllowMissingColumns reads missing columns and fields as nulls).");

    /// <summary>Whether a path of field names (through lists, and maps by key and value) leads to a field of the type.</summary>
    private static bool Exists(IArrowType type, string[] path, int at)
    {
        if (at == path.Length) return true;
        return type switch
        {
            StructType s => s.GetFieldIndex(path[at], StringComparer.Ordinal) is int i and >= 0 && Exists(s.Fields[i].DataType, path, at + 1),
            ListType l => Exists(l.ValueDataType, path, at),
            FixedSizeListType l => Exists(l.ValueDataType, path, at),
            MapType m => path[at] switch
            {
                "key" => Exists(m.KeyField.DataType, path, at + 1),
                "value" => Exists(m.ValueField.DataType, path, at + 1),
                _ => false,
            },
            _ => false,
        };
    }

    /// <summary>A type with only the struct fields on the paths, as a file's reader prunes a column it reads by paths.</summary>
    /// <exception cref="ArgumentException">A path names a field the type does not have.</exception>
    private static IArrowType Prune(IArrowType type, IReadOnlyList<string[]> paths, string where)
    {
        if (paths.Count == 0 || paths.Any(p => p.Length == 0)) return type;
        switch (type)
        {
            case StructType s:
                foreach (string[] p in paths)
                {
                    if (s.GetFieldIndex(p[0], StringComparer.Ordinal) < 0) throw new ArgumentException($"'{where}' has no field '{p[0]}'.");
                }
                return new StructType([.. s.Fields
                    .Where(f => paths.Any(p => p[0] == f.Name))
                    .Select(f => new Field(f.Name, Prune(f.DataType, [.. paths.Where(p => p[0] == f.Name).Select(p => p[1..])], $"{where}.{f.Name}"), f.IsNullable, f.Metadata))]);
            case ListType l:
                return new ListType(new Field(l.ValueField.Name, Prune(l.ValueDataType, paths, where), l.ValueField.IsNullable, l.ValueField.Metadata));
            case FixedSizeListType l:
                return new FixedSizeListType(new Field(l.ValueField.Name, Prune(l.ValueDataType, paths, where), l.ValueField.IsNullable, l.ValueField.Metadata), l.ListSize);
            case MapType m:
            {
                foreach (string[] p in paths)
                {
                    if (p[0] is not ("key" or "value")) throw new ArgumentException($"'{where}' is a map: paths go through 'key' or 'value', not '{p[0]}'.");
                }
                string[][] values = [.. paths.Where(p => p[0] == "value").Select(p => p[1..])];
                Field value = values.Length == 0 ? m.ValueField
                    : new Field(m.ValueField.Name, Prune(m.ValueField.DataType, values, $"{where}.value"), m.ValueField.IsNullable, m.ValueField.Metadata);
                return new MapType(m.KeyField, value, m.KeySorted);
            }
            default:
                throw new ArgumentException($"'{where}' is not a struct, list or map: no field below it can be selected.");
        }
    }

    /// <summary>
    /// A file's batch in the dataset's schema: its columns converted and in order, partition values and the file
    /// path added; filtered by the dataset when the plan says so; cut to <paramref name="remaining"/> rows. Null when
    /// no row is left.
    /// </summary>
    private RecordBatch? Complete(RecordBatch batch, FilePlan plan, int index, ref long? remaining)
    {
        int length = batch.Length, skip = _options.RowIndexColumn is null ? 0 : 1;
        var columns = new IArrowArray[Schema.FieldsList.Count];
        var used = new bool[batch.ColumnCount]; // the batch's columns handed on as they are
        IArrowArray[] filterArrays = [];
        try
        {
            try
            {
                if (skip == 1)
                {
                    columns[0] = batch.Column(0);
                    used[0] = true;
                }
                for (int c = 0; c < _layout.Length; c++)
                {
                    columns[skip + c] = _layout[c].Partition is int p and >= 0
                        ? Constant(_dataset.Partitions!.Columns[p], index, length, _layout[c].Field.DataType)
                        : Convert(batch, plan.Outputs[_layout[c].Target], length, used);
                }
                if (_dataset.Options.FilePathColumn is not null) columns[^1] = ConstantArrays.String(PathOf(index), length, _options.ViewTypes);
                if (plan.FilterAfter) filterArrays = [.. plan.Filtered.Select(f => Convert(batch, f, length, used))];
            }
            catch (OverflowException e)
            {
                throw new ParquetFormatException($"{PathOf(index)}: {e.Message}", e);
            }
            finally
            {
                for (int c = 0; c < used.Length; c++)
                {
                    if (!used[c]) batch.Column(c).Dispose();
                }
            }

            if (plan.FilterAfter)
            {
                // The dataset's filter over the converted columns (and the file's partition values).
                FilterProgram program = _program!;
                var arguments = new IArrowArray[program.Columns.Count];
                List<IArrowArray> constants = [];
                for (int a = 0; a < arguments.Length; a++)
                {
                    FilterColumn column = program.Columns[a];
                    int f = System.Array.IndexOf(_filterRoots, column.Path[0]);
                    IArrowArray root;
                    if (f >= 0) root = filterArrays[f];
                    else
                    {
                        int p = _dataset.Partitions!.IndexOf(column.Path[0]);
                        root = Constant(_dataset.Partitions.Columns[p], index, length, _dataset.PartitionField(p, _options.ViewTypes).DataType);
                        constants.Add(root);
                    }
                    arguments[a] = Descend(root, column.Path);
                }
                var mask = new ulong[Masks.Words(length)];
                int kept;
                try
                {
                    kept = program.Evaluate(arguments, length, mask);
                }
                finally
                {
                    foreach (IArrowArray constant in constants) constant.Dispose();
                }
                if (kept < length)
                {
                    for (int c = 0; c < columns.Length; c++)
                    {
                        IArrowArray all = columns[c];
                        columns[c] = ArrowFilter.Filter(all, mask);
                        if (!filterArrays.Contains(all)) all.Dispose(); // those are disposed below
                    }
                    length = kept;
                }
            }
        }
        catch
        {
            foreach (IArrowArray? column in columns) column?.Dispose();
            throw;
        }
        finally
        {
            foreach (IArrowArray array in filterArrays)
            {
                if (!columns.Contains(array)) array.Dispose();
            }
        }

        if (length == 0)
        {
            foreach (IArrowArray column in columns) column.Dispose();
            return null;
        }
        var result = new RecordBatch(Schema, columns, length);
        if (remaining is long left && length >= left)
        {
            if (length > left) result = new RecordBatch(Schema, [.. columns.Select(a => ArrowArrayFactory.Slice(a, 0, (int)left))], (int)left);
            remaining = 0;
        }
        else remaining -= length;
        return result;

        static IArrowArray Constant(IArrowArray values, int file, int length, IArrowType type)
        {
            // Inferred strings are kept as plain strings and repeated in the reader's layout.
            if (type is StringViewType && values is StringArray strings)
                return strings.IsNull(file)
                    ? ConstantArrays.Repeat(new StringViewArray.Builder().AppendNull().Build(), 0, length)
                    : ConstantArrays.String(strings.GetString(file), length, view: true);
            return ConstantArrays.Repeat(values, file, length);
        }
    }

    /// <summary>A column of the batch converted (the batch's own when nothing changes, marked used), or nulls when the file lacks it.</summary>
    private static IArrowArray Convert(RecordBatch batch, (int Source, ArrowConversion Conversion) plan, int length, bool[] used)
    {
        if (plan.Source < 0) return NullArrays.Of(plan.Conversion.Target, length);
        IArrowArray source = batch.Column(plan.Source);
        if (plan.Conversion.IsIdentity)
        {
            // A column handed on twice (selected and filtered on) is the same array.
            used[plan.Source] = true;
            return source;
        }
        return plan.Conversion.Convert(source);
    }

    /// <summary>The struct field a filter column names below its top-level column.</summary>
    private static IArrowArray Descend(IArrowArray column, string[] path)
    {
        IArrowArray current = column;
        for (int i = 1; i < path.Length; i++)
        {
            var parent = (StructArray)current;
            current = parent.Fields[((StructType)parent.Data.DataType).GetFieldIndex(path[i])];
        }
        return current;
    }

    // ---- filters --------------------------------------------------------------------------------------------------

    /// <summary>
    /// Clears <paramref name="read"/> for the files whose partition values the filter rules out (false or null), as
    /// the IR's semantics evaluate it; whether every conjunct could be decided (those that cannot keep every file).
    /// </summary>
    private static bool Prune(Partitions partitions, ScalarExpr filter, bool[] read)
    {
        var schema = new Apache.Arrow.Schema(partitions.Fields, null);
        LoweredFilter lowered = FilterLowering.lower(schema, filter);
        if (OptionModule.IsNone(lowered.Program)) return lowered.Exact;
        FilterProgram program = lowered.Program.Value;
        IArrowArray[] columns = [.. program.Columns.Select(c => partitions.Columns[partitions.IndexOf(c.Path[0])])];
        var selected = new ulong[(read.Length + 63) / 64];
        program.Evaluate(columns, read.Length, selected);
        for (int i = 0; i < read.Length; i++) read[i] = (selected[i / 64] >> (i % 64) & 1) != 0;
        return lowered.Exact;
    }

    /// <summary>
    /// The filter a file reads: the conjuncts over its columns, those over partition columns too with its values,
    /// columns it lacks (<paramref name="missing"/>) as nulls; conjuncts over <paramref name="dropped"/> columns are
    /// left out. Whether every conjunct is in (one left out, or one with a partition value of no literal form, makes
    /// the file's rows filtered less).
    /// </summary>
    private (ScalarExpr? Filter, bool Complete) FileFilter(int index, HashSet<string> missing, HashSet<string> dropped)
    {
        if (_mixed.Length == 0 && missing.Count == 0 && dropped.Count == 0) return (_fileFilter, true);
        Partitions? partitions = _dataset.Partitions;
        ScalarExpr? filter = null;
        bool all = true;
        IEnumerable<ScalarExpr> conjuncts = _fileFilter is null ? _mixed : FilterShapes.conjuncts(_fileFilter).Concat(_mixed);
        foreach (ScalarExpr conjunct in conjuncts)
        {
            bool complete = true, changed = false;
            ScalarExpr substituted = ScalarExprOps.substitute(FuncConvert.FromFunc((string name) =>
            {
                if (dropped.Contains(name))
                {
                    complete = false;
                    return FSharpOption<ScalarExpr>.None;
                }
                if (missing.Contains(name))
                {
                    changed = true;
                    return FSharpOption<ScalarExpr>.Some(ScalarExpr.NewLiteral(LiteralValue.NewNull(LogicalTypes.Null)));
                }
                int p = partitions?.IndexOf(name) ?? -1;
                if (p < 0) return FSharpOption<ScalarExpr>.None;
                if (Literal(partitions!.Columns[p], index) is not { } literal)
                {
                    complete = false;
                    return FSharpOption<ScalarExpr>.None;
                }
                changed = true;
                return FSharpOption<ScalarExpr>.Some(ScalarExpr.NewLiteral(literal));
            }), conjunct);
            // A conjunct left out only filters less (the dataset filters after where it promised exact rows).
            if (!complete)
            {
                all = false;
                continue;
            }
            // Comparisons between the values put in are decided, so that what is left reads the file's columns only.
            if (changed) substituted = FilterShapes.foldConstants(substituted);
            if (substituted is ScalarExpr.Literal { value: LiteralValue.Bool { Item: true } }) continue;
            filter = filter is null ? substituted : ScalarExpr.NewBinary(BinaryOp.And, filter, substituted);
        }
        return (filter, all);
    }

    /// <summary>A partition value as an IR literal; null for a null value or a type without one.</summary>
    private static LiteralValue? Literal(IArrowArray column, int index) => column.IsNull(index) ? null : column switch
    {
        BooleanArray a => LiteralValue.NewBool(a.GetValue(index)!.Value),
        Int8Array a => LiteralValue.NewInt8(a.GetValue(index)!.Value),
        Int16Array a => LiteralValue.NewInt16(a.GetValue(index)!.Value),
        Int32Array a => LiteralValue.NewInt32(a.GetValue(index)!.Value),
        Int64Array a => LiteralValue.NewInt64(a.GetValue(index)!.Value),
        UInt8Array a => LiteralValue.NewUInt8(a.GetValue(index)!.Value),
        UInt16Array a => LiteralValue.NewUInt16(a.GetValue(index)!.Value),
        UInt32Array a => LiteralValue.NewUInt32(a.GetValue(index)!.Value),
        UInt64Array a => LiteralValue.NewUInt64(a.GetValue(index)!.Value),
        FloatArray a => LiteralValue.NewFloat32(a.GetValue(index)!.Value),
        DoubleArray a => LiteralValue.NewFloat64(a.GetValue(index)!.Value),
        Date32Array a => LiteralValue.NewDate(a.GetDateOnly(index)!.Value),
        StringArray a => LiteralValue.NewString(a.GetString(index)),
        StringViewArray a => LiteralValue.NewString(a.GetString(index)),
        _ => null,
    };

    // ---- files ----------------------------------------------------------------------------------------------------

    /// <summary>Opens a file (its footer from before when there is one, kept for the next reads otherwise).</summary>
    private async ValueTask<ParquetFile> OpenAsync(int index, bool useAsync, CancellationToken ct)
    {
        ParquetFile file = await ParquetDataset.OpenFileAsync(_dataset.Store, _dataset.Files[index], _dataset.Footer(index), useAsync, ct).ConfigureAwait(false);
        _dataset.KeepFooter(index, file.Metadata);
        return file;
    }

    /// <summary>A file's row count as its reader numbers rows (the row groups' counts).</summary>
    private static long RowCount(ParquetFile file) => file.Thrift.RowGroups.Sum(g => Math.Max(0, g.NumRows));

    private RecordBatch? Next(IEnumerator<RecordBatch> batches, int index)
    {
        try
        {
            return batches.MoveNext() ? batches.Current : null;
        }
        catch (ParquetFormatException e)
        {
            throw ParquetDataset.InFile(PathOf(index), e);
        }
    }

    private async ValueTask<RecordBatch?> NextAsync(IAsyncEnumerator<RecordBatch> batches, int index)
    {
        try
        {
            return await batches.MoveNextAsync().ConfigureAwait(false) ? batches.Current : null;
        }
        catch (ParquetFormatException e)
        {
            throw ParquetDataset.InFile(PathOf(index), e);
        }
    }

    /// <summary>
    /// Files opened ahead of the one read (up to <see cref="FilesAhead"/> of those to open: the files read, and the
    /// others when their row counts are needed); those not taken are disposed once opened.
    /// </summary>
    private sealed class Ahead(ParquetDatasetReader owner, CancellationToken ct) : IDisposable
    {
        private readonly Dictionary<int, Task<ParquetFile>> _started = [];
        private int _next;

        private bool Needed(int index) => owner._read[index] || owner._options.RowIndexColumn is not null;

        /// <summary>The file at <paramref name="index"/> (files are taken in order), the next ones started meanwhile.</summary>
        public Task<ParquetFile> Take(int index)
        {
            int count = owner._dataset.Files.Count;
            _next = Math.Max(_next, index);
            for (; _next < count && _started.Count < FilesAhead; _next++)
            {
                if (!Needed(_next)) continue;
                int file = _next;
                _started[file] = Task.Run(() => owner.OpenAsync(file, useAsync: true, ct).AsTask(), ct);
            }
            return _started.Remove(index, out Task<ParquetFile>? task) ? task : Task.Run(() => owner.OpenAsync(index, useAsync: true, ct).AsTask(), ct);
        }

        public void Dispose()
        {
            foreach (Task<ParquetFile> task in _started.Values)
                task.ContinueWith(static t =>
                {
                    if (t.IsCompletedSuccessfully) t.Result.Dispose();
                    else _ = t.Exception; // observed: a file not read need not open
                }, CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
            _started.Clear();
        }
    }
}
