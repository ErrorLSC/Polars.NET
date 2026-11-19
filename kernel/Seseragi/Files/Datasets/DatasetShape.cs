using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Arrow.Builders;
using Kaminari.Sazanami.Arrow.Compute;
using Kaminari.Sazanami.Arrow.Interop;
using Kaminari.Sazanami.Filters;
using Microsoft.FSharp.Core;

namespace Kaminari.Seseragi.Files;

/// <summary>
/// What a dataset reader of any format does around its files' readers: the columns selected (from files, and
/// partition columns), the filter split (conjuncts over partition columns decide which files are read; the others go to
/// the files, those over both with each file's values), how each file's columns convert to the dataset's types, and
/// each file's batches made the dataset's (converted, partition values and the file path added, filtered again where a
/// file's own filtering is not exact, cut to the rows asked). Safe on any thread once made.
/// </summary>
internal sealed class DatasetShape
{
    private readonly DatasetOptions _dataset;
    private readonly Partitions? _partitions;
    private readonly Func<int, bool, Field> _partitionField;
    private readonly Func<int, string> _pathOf;
    private readonly Func<string, Exception?, Exception> _error;
    private readonly bool _views;
    private readonly string? _rowIndex;
    private readonly bool _allowMissing;
    private readonly ArrowConversionRules _rules;
    private readonly Column[] _layout;
    private readonly ScalarExpr? _fileFilter;
    private readonly ScalarExpr[] _mixed;
    private readonly Dictionary<string, IArrowType> _types;     // the dataset's column types, by name (what the filter means)
    private readonly string[] _filterColumns = [];              // the columns the files' conjuncts read
    private readonly ScalarExpr? _rowFilter;              // what the rows delivered must pass (the conjuncts not decided per file)
    private readonly FilterProgram? _program;             // _rowFilter as the dataset applies it after converting (exact)

    /// <summary>A column from files selected: whole, or the struct fields on its paths; its type in the batches.</summary>
    internal sealed record Target(string Name, string[][]? Paths, Field Field);

    /// <summary>A column of the batches after the row index: a selected column from files, or a partition column.</summary>
    private readonly record struct Column(Field Field, int Target, int Partition);

    /// <summary>The conversions of a file's columns: per selected column, and per column the dataset's filter reads.</summary>
    internal sealed record Converted((int Source, ArrowConversion Conversion)[] Outputs, (int Source, ArrowConversion Conversion)[] Filtered);

    /// <summary>
    /// What a file of given columns reads: the columns the row filter reads that it lacks (nulls), those whose
    /// conjuncts are left out (they convert lossily, or with struct fields it lacks), and the reader's column lists
    /// (whole columns and paths): the columns read, and (second) with the columns the dataset's filter reads, whole.
    /// </summary>
    internal sealed record FileColumns(HashSet<string> Missing, HashSet<string> Dropped, (IReadOnlyList<string> Columns, IReadOnlyList<string[]>? Paths)[] Lists);

    /// <param name="dataset">The dataset's options (missing columns, lossy casts, the file path column; their names in messages).</param>
    /// <param name="partitions">The partition values of the files, when partitioned.</param>
    /// <param name="partitionField">Partition column <c>p</c> as batches hold it, strings as views or not.</param>
    /// <param name="columns">The dataset's columns from files, as these options read them.</param>
    /// <param name="files">How many files the dataset has.</param>
    /// <param name="selected">The columns selected by name (with <paramref name="paths"/>; both null: all).</param>
    /// <param name="paths">The struct fields selected (each path from a top-level column).</param>
    /// <param name="filter">The filter (canonical), or null.</param>
    /// <param name="filterRows">Whether the rows the filter rules out are left out.</param>
    /// <param name="rowIndexColumn">The row index column the files' readers put first, or null.</param>
    /// <param name="views">Whether strings added (partition values, the file path) are views.</param>
    /// <param name="pathOf">File <c>i</c>'s path.</param>
    /// <param name="error">The format's error for a message (naming the file) and its cause.</param>
    public DatasetShape(DatasetOptions dataset, Partitions? partitions, Func<int, bool, Field> partitionField, Schema columns, int files,
        IReadOnlyList<string>? selected, IReadOnlyList<string[]>? paths, ScalarExpr? filter, bool filterRows, string? rowIndexColumn, bool views,
        Func<int, string> pathOf, Func<string, Exception?, Exception> error)
    {
        _dataset = dataset;
        _partitions = partitions;
        _partitionField = partitionField;
        _pathOf = pathOf;
        _error = error;
        _views = views;
        _rowIndex = rowIndexColumn;
        _allowMissing = dataset.AllowMissingColumns || dataset.Schema is null && dataset.SchemaMode == DatasetSchemaMode.UnionByName;
        _rules = new ArrowConversionRules(dataset.AllowLossyCasts, _allowMissing);
        bool IsPartition(string name) => partitions?.IndexOf(name) >= 0;

        Columns = columns.FieldsList.ToDictionary(f => f.Name, StringComparer.Ordinal);

        // The columns selected: from files (not shadowed by a partition column), and partition columns.
        bool all = selected is null && paths is null;
        foreach (string name in selected ?? [])
        {
            if (!IsPartition(name) && !Columns.ContainsKey(name))
                throw new ArgumentException($"The dataset has no column '{name}' (the columns are those of the files and the partitions).", "options");
        }
        Dictionary<string, List<string[]>> below = new(StringComparer.Ordinal);
        foreach (string[] path in paths ?? [])
        {
            if (path is null || path.Length == 0) throw new ArgumentException("A column path is empty.", "options");
            if (IsPartition(path[0])) throw new ArgumentException($"'{path[0]}' is a partition column: it has no fields to select.", "options");
            if (!Columns.ContainsKey(path[0])) throw new ArgumentException($"The dataset has no column '{path[0]}'.", "options");
            (below.TryGetValue(path[0], out var list) ? list : below[path[0]] = []).Add(path[1..]);
        }
        List<Target> targets = [];
        foreach (Field field in columns.FieldsList)
        {
            if (IsPartition(field.Name)) continue;
            Field nullable = field.IsNullable ? field : new Field(field.Name, field.DataType, nullable: true, field.Metadata);
            if (all || selected?.Contains(field.Name) == true) targets.Add(new Target(field.Name, null, nullable));
            else if (below.TryGetValue(field.Name, out var fields))
                targets.Add(new Target(field.Name, [.. fields], new Field(field.Name, Prune(field.DataType, fields, field.Name), nullable: true, field.Metadata)));
        }
        Targets = [.. targets];
        bool[] partitionsRead = [.. Enumerable.Range(0, partitions?.Fields.Length ?? 0).Select(p => all || selected?.Contains(partitions!.Fields[p].Name) == true)];

        // The filter: conjuncts over partition columns only decide which files are read; the others go to the files,
        // those over both with the partition columns replaced by each file's values.
        Read = [.. Enumerable.Repeat(true, files)];
        bool exact = true;
        List<ScalarExpr> onFiles = [], mixed = [];
        if (filter is not null)
        {
            List<ScalarExpr> onPartitions = [];
            foreach (ScalarExpr conjunct in FilterShapes.conjuncts(filter))
            {
                string[] names = [.. ScalarExprOps.collectColumns(conjunct)];
                int inPartitions = names.Count(IsPartition);
                (inPartitions == 0 ? onFiles : inPartitions == names.Length ? onPartitions : mixed).Add(conjunct);
            }
            if (onPartitions.Count > 0) exact = Prune(partitions!, onPartitions.Aggregate(And), Read);
        }
        _types = new(StringComparer.Ordinal);
        foreach (Field field in columns.FieldsList) _types.TryAdd(field.Name, field.DataType);
        _fileFilter = onFiles.Count == 0 ? null : onFiles.Aggregate(And);
        _mixed = [.. mixed];
        _filterColumns = [.. onFiles.Concat(mixed).SelectMany(ScalarExprOps.collectColumns).Distinct()];
        _rowFilter = onFiles.Count + mixed.Count == 0 ? null : onFiles.Concat(mixed).Aggregate(And);

        // Exact when the filter lowers exactly over the dataset's types (each file's rows are filtered again after
        // conversion where its own filtering is not exact).
        FilterRoots = [];
        FiltersExactly = filterRows && exact;
        if (filterRows && _rowFilter is { } rows)
        {
            LoweredFilter lowered = FilterLowering.lower(FilterSchema(columns), rows);
            FiltersExactly &= lowered.Exact;
            if (FiltersExactly && OptionModule.IsSome(lowered.Program))
            {
                _program = lowered.Program.Value;
                FilterRoots = [.. _program.Columns.Select(c => c.Path[0]).Where(n => !IsPartition(n)).Distinct()];
            }
        }

        List<Column> layout = [];
        foreach (Field field in columns.FieldsList)
        {
            int p = partitions?.IndexOf(field.Name) ?? -1;
            if (p >= 0)
            {
                if (partitionsRead[p]) layout.Add(new Column(partitionField(p, views), -1, p));
            }
            else if (System.Array.FindIndex(Targets, t => t.Name == field.Name) is int t and >= 0) layout.Add(new Column(Targets[t].Field, t, -1));
        }
        for (int p = 0; p < partitionsRead.Length; p++)
        {
            if (partitionsRead[p] && !Columns.ContainsKey(partitions!.Fields[p].Name)) layout.Add(new Column(partitionField(p, views), -1, p));
        }
        _layout = [.. layout];
        List<Field> output = [.. _layout.Select(c => c.Field)];
        if (rowIndexColumn is { } index)
        {
            if (output.Any(f => f.Name == index)) throw new ArgumentException($"The row index column '{index}' has the name of another column read.", "options");
            output.Insert(0, new Field(index, Int64Type.Default, nullable: false));
        }
        if (dataset.FilePathColumn is { } file)
        {
            if (output.Any(f => f.Name == file)) throw new ArgumentException($"The file path column '{file}' has the name of another column read.", "options");
            output.Add(new Field(file, views ? StringViewType.Default : StringType.Default, nullable: false));
        }
        Schema = new Schema(output, columns.Metadata);

        static ScalarExpr And(ScalarExpr a, ScalarExpr b) => ScalarExpr.NewBinary(BinaryOp.And, a, b);
    }

    /// <summary>
    /// The columns a filter reads: the columns from files (a partition column of the same name in its place), then the
    /// other partition columns.
    /// </summary>
    private Schema FilterSchema(Schema columns)
    {
        List<Field> fields = [.. columns.FieldsList.Select(f => _partitions?.IndexOf(f.Name) is int p and >= 0 ? _partitionField(p, _views) : f)];
        for (int p = 0; p < (_partitions?.Fields.Length ?? 0); p++)
        {
            if (columns.GetFieldIndex(_partitions!.Fields[p].Name) < 0) fields.Add(_partitionField(p, _views));
        }
        return new Schema(fields, null);
    }

    /// <summary>The schema of the batches: the row index if asked, the columns read (nullable), the file path if asked.</summary>
    public Schema Schema { get; }

    /// <summary>The dataset's columns from files, whole, as the options read them, by name.</summary>
    public Dictionary<string, Field> Columns { get; }

    /// <summary>The columns from files selected, in the dataset's order.</summary>
    public Target[] Targets { get; }

    /// <summary>The columns from files the dataset's filter reads after converting (when it filters exactly).</summary>
    public string[] FilterRoots { get; }

    /// <summary>Which files are read: those a filter over partition columns leaves.</summary>
    public bool[] Read { get; }

    /// <summary>
    /// Whether the rows delivered are exactly those the filter keeps (rows filtered, the filter lowering exactly over
    /// the dataset's types, its conjuncts over partition columns decided exactly).
    /// </summary>
    public bool FiltersExactly { get; }

    /// <summary>Whether the dataset filters some files' rows after converting them (where their own filtering is not exact).</summary>
    public bool FiltersAfter => _program is not null;

    /// <summary>The leaves the columns read hold (a struct's fields, a list's elements, a map's keys and values).</summary>
    public int Leaves => Targets.Sum(t => LeavesOf(t.Field.DataType)) + FilterRoots.Length;

    private static int LeavesOf(IArrowType type) => type switch
    {
        StructType s => s.Fields.Sum(f => LeavesOf(f.DataType)),
        ListType l => LeavesOf(l.ValueDataType),
        FixedSizeListType l => LeavesOf(l.ValueDataType),
        MapType m => LeavesOf(m.KeyField.DataType) + LeavesOf(m.ValueField.DataType),
        _ => 1,
    };

    // ---- one file -------------------------------------------------------------------------------------------------

    /// <summary>What a file whose columns are <paramref name="has"/> reads (errors name <paramref name="path"/>).</summary>
    public FileColumns ColumnsOf(Dictionary<string, Field> has, string path)
    {
        // What the file's reader reads: whole columns and paths (those the file has).
        HashSet<string> whole = new(StringComparer.Ordinal);
        List<string[]> paths = [];
        foreach (Target target in Targets)
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
                if (!Columns.TryGetValue(root, out Field? target) || _partitions?.IndexOf(root) >= 0) continue;
                if (!has.TryGetValue(root, out Field? field))
                {
                    if (!_allowMissing) throw Missing(path, root, filter: true);
                    missing.Add(root);
                }
                else if (!ArrowWidening.CanConvert(field.DataType, target.DataType, default)) dropped.Add(root);
            }
        }

        // With the dataset filtering after converting, the reader reads the columns the filter reads too, whole.
        HashSet<string> withFilter = new(whole, StringComparer.Ordinal);
        foreach (string root in FilterRoots)
        {
            if (has.ContainsKey(root)) withFilter.Add(root);
        }
        List<string[]> pathsWithFilter = [.. paths.Where(p => !withFilter.Contains(p[0]))];
        return new FileColumns(missing, dropped,
            [([.. whole], paths.Count == 0 ? null : paths), ([.. withFilter], pathsWithFilter.Count == 0 ? null : pathsWithFilter)]);
    }

    /// <summary>How the columns of a file's batches (<paramref name="read"/>) convert to the dataset's.</summary>
    public Converted Conversions(Schema read, bool after, string path)
    {
        int skip = _rowIndex is null ? 0 : 1;
        var outputs = new (int, ArrowConversion)[Targets.Length];
        for (int t = 0; t < Targets.Length; t++) outputs[t] = Conversion(read, skip, Targets[t].Name, Targets[t].Field, path);
        var filtered = after ? new (int, ArrowConversion)[FilterRoots.Length] : [];
        for (int f = 0; f < filtered.Length; f++) filtered[f] = Conversion(read, skip, FilterRoots[f], Columns[FilterRoots[f]], path);
        return new Converted(outputs, filtered);
    }

    /// <summary>
    /// Where a column comes from in the reader's batches (-1 when the file lacks it: nulls) and how it converts to
    /// <paramref name="target"/>; a conversion the rules refuse is an error naming the file, the column and what allows it.
    /// </summary>
    private (int Source, ArrowConversion Conversion) Conversion(Schema read, int skip, string name, Field targetField, string path)
    {
        IArrowType target = targetField.DataType;
        int at = -1;
        for (int c = skip; c < read.FieldsList.Count; c++)
        {
            if (read.FieldsList[c].Name == name) at = c;
        }
        if (at < 0) return (-1, new NullsConversion(target));
        IArrowType type = read.FieldsList[at].DataType;
        if (EnumMismatch(read.FieldsList[at], targetField, name, declared: _dataset.Schema is not null) is { } mismatch)
            throw _error($"{path}: {mismatch}", null);
        if (ArrowWidening.TryCompile(type, target, _rules, name, out ArrowConversion? conversion, out ArrowConversionError? error)) return (at, conversion);
        string hint = error.Problem switch
        {
            ArrowConversionProblem.Lossy => $" ({_dataset.Name}.AllowLossyCasts allows lossy conversions)",
            ArrowConversionProblem.MissingField => $" ({_dataset.Name}.AllowMissingColumns reads missing columns and fields as nulls)",
            _ when _dataset.Schema is null && _dataset.SchemaMode == DatasetSchemaMode.FirstFile && ArrowWidening.CanConvert(target, type, _rules) =>
                " (the dataset's types are the first file's: SchemaMode.UnionByName or a given Schema reads wider types in later files)",
            _ => "",
        };
        string what = error.Problem == ArrowConversionProblem.MissingField
            ? $"the file has no field '{error.Path}', which the dataset reads"
            : $"'{error.Path}': {error.Message} (the dataset's type)";
        throw _error($"{path}: {what}{hint}.", null);
    }

    /// <summary>
    /// A Polars Enum (field metadata <c>_PL_ENUM_VALUES2</c>, <c>_PL_ENUM_VALUES</c> before it) of other categories than
    /// the dataset's, or an Enum against a column that is none (or the reverse), at any depth: what is wrong, or null.
    /// An Enum is a declared set of values: as Polars, the dataset does not change it (cast the column, or change the
    /// Enum).
    /// </summary>
    /// <param name="declared">
    /// The dataset's fields are given (<see cref="DatasetOptions.Schema"/>): a field declared without an Enum reads the
    /// file's Enum as its values (the way to downgrade one); only a declared Enum is compared.
    /// </param>
    private static string? EnumMismatch(Field file, Field dataset, string path, bool declared)
    {
        string? mine = EnumOf(file), theirs = EnumOf(dataset);
        if (mine != theirs && !(declared && theirs is null))
        {
            string Describe(string? categories) => categories is null ? "no Enum" : $"an Enum of {Shorten(categories)}";
            return $"'{path}' is {Describe(mine)} in this file but {Describe(theirs)} in the dataset: an Enum's categories are not "
                + "changed (give the dataset's Schema with the column as plain values, or make the Enums alike).";
        }
        return (file.DataType, dataset.DataType) switch
        {
            (StructType f, StructType d) => d.Fields.Select(df => f.Fields.FirstOrDefault(ff => ff.Name == df.Name) is { } ff ? EnumMismatch(ff, df, $"{path}.{df.Name}", declared) : null)
                .FirstOrDefault(m => m is not null),
            (NestedType f, NestedType d) when ListItem(f) is { } fi && ListItem(d) is { } di => EnumMismatch(fi, di, path + "[]", declared),
            (MapType f, MapType d) => EnumMismatch(f.KeyField, d.KeyField, path + "{key}", declared) ?? EnumMismatch(f.ValueField, d.ValueField, path + "{value}", declared),
            _ => null,
        };

        static string? EnumOf(Field field) => field.Metadata is { } metadata
            ? metadata.TryGetValue("_PL_ENUM_VALUES2", out string? values) || metadata.TryGetValue("_PL_ENUM_VALUES", out values) ? values : null
            : null;

        static string Shorten(string text) => text.Length <= 80 ? text : text[..77] + "...";

        static Field? ListItem(NestedType type) => type switch
        {
            ListType l => l.ValueField,
            LargeListType l => l.ValueField,
            ListViewType l => l.ValueField,
            LargeListViewType l => l.ValueField,
            FixedSizeListType l => l.ValueField,
            _ => null,
        };
    }

    private Exception Missing(string path, string column, bool filter) =>
        _error($"{path}: the file has no column '{column}', which the dataset {(filter ? "filters on" : "reads")} "
            + $"({_dataset.Name}.AllowMissingColumns reads missing columns and fields as nulls).", null);

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
    /// File <paramref name="index"/>'s batch in the dataset's schema: its columns converted (<paramref name="converted"/>)
    /// and in order, partition values and the file path added; filtered by the dataset when <paramref name="filterAfter"/>.
    /// Null when no row is left. Safe on any thread.
    /// </summary>
    public RecordBatch? Convert(RecordBatch batch, Converted converted, bool filterAfter, int index)
    {
        int length = batch.Length, skip = _rowIndex is null ? 0 : 1;
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
                        ? Constant(_partitions!.Columns[p], index, length, _layout[c].Field.DataType)
                        : Convert(batch, converted.Outputs[_layout[c].Target], length, used);
                }
                if (_dataset.FilePathColumn is not null) columns[^1] = ConstantArrays.String(_pathOf(index), length, _views);
                if (filterAfter) filterArrays = [.. converted.Filtered.Select(f => Convert(batch, f, length, used))];
            }
            catch (OverflowException e)
            {
                throw _error($"{_pathOf(index)}: {e.Message}", e);
            }
            finally
            {
                for (int c = 0; c < used.Length; c++)
                {
                    if (!used[c]) batch.Column(c).Dispose();
                }
            }

            if (filterAfter)
            {
                // The dataset's filter over the converted columns (and the file's partition values).
                FilterProgram program = _program!;
                var arguments = new IArrowArray[program.Columns.Count];
                List<IArrowArray> constants = [];
                for (int a = 0; a < arguments.Length; a++)
                {
                    FilterColumn column = program.Columns[a];
                    int f = System.Array.IndexOf(FilterRoots, column.Path[0]);
                    IArrowArray root;
                    if (f >= 0) root = filterArrays[f];
                    else
                    {
                        int p = _partitions!.IndexOf(column.Path[0]);
                        root = Constant(_partitions.Columns[p], index, length, _partitionField(p, _views).DataType);
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
        return new RecordBatch(Schema, columns, length);

        static IArrowArray Constant(IArrowArray values, int file, int length, IArrowType type)
        {
            // Inferred strings are kept as plain strings and repeated in the reader's layout.
            if (type is StringViewType && values is StringArray strings)
                return strings.IsNull(file)
                    ? ConstantArrays.Repeat(new StringViewArray.Builder().AppendNull().Build(PooledArrowAllocator.Instance), 0, length)
                    : ConstantArrays.String(strings.GetString(file), length, view: true);
            return ConstantArrays.Repeat(values, file, length);
        }
    }

    /// <summary>A batch cut to the <paramref name="remaining"/> rows still to deliver (over its memory: disposing the cut releases it all).</summary>
    public RecordBatch Cut(RecordBatch batch, ref long? remaining, DictionaryUnifier? unifier)
    {
        if (remaining is long left)
        {
            if (batch.Length < left) remaining = left - batch.Length;
            else
            {
                remaining = 0;
                if (batch.Length > left) batch = new RecordBatch(Schema, [.. batch.Arrays.Select(a => ArrowArrayFactory.Slice(a, 0, (int)left))], (int)left);
            }
        }
        return unifier is null ? batch : unifier.Apply(batch);
    }

    /// <summary>
    /// The unifier of one reading's dictionaries (<see cref="DatasetOptions.UnifyDictionaries"/>), its batches handed to
    /// <see cref="Cut"/> in order; null when not asked or no column holds a dictionary.
    /// </summary>
    public DictionaryUnifier? NewUnifier() => _dataset.UnifyDictionaries ? DictionaryUnifier.For(Schema) : null;

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
        var schema = new Schema(partitions.Fields, null);
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
    /// The filter file <paramref name="index"/> reads: the conjuncts over its columns, those over partition columns
    /// too with its values, columns it lacks (<paramref name="missing"/>) as nulls; conjuncts over
    /// <paramref name="dropped"/> columns are left out. A column the file stores as a narrower number than the
    /// dataset's (<paramref name="has"/>) is cast up where arithmetic reads it, so that the arithmetic is the dataset's
    /// (an Int32 file's column of an Int64 dataset wraps where the dataset does); a conjunct with arithmetic over a column
    /// of another type that cannot be cast up (a nested one, a decimal, …) is left out. Whether every conjunct is in (one left out, or one with a partition value of no literal
    /// form, makes the file's rows filtered less).
    /// </summary>
    public (ScalarExpr? Filter, bool Complete) FileFilter(int index, HashSet<string> missing, HashSet<string> dropped, IReadOnlyDictionary<string, Field> has)
    {
        Dictionary<string, IArrowType>? widened = null;
        HashSet<string>? reshaped = null;
        foreach (string name in _filterColumns)
        {
            if (!has.TryGetValue(name, out Field? stored) || !_types.TryGetValue(name, out IArrowType? meant)) continue;
            if (FilterArithmetic.SameValues(stored.DataType, meant)) continue;
            if (Kaminari.Sazanami.Expressions.ValueTypes.IsArithmetic(Kaminari.Sazanami.Expressions.ValueTypes.ValueType(stored.DataType))
                && Kaminari.Sazanami.Expressions.ValueTypes.IsArithmetic(meant))
                (widened ??= new(StringComparer.Ordinal))[name] = meant;
            else
                (reshaped ??= new(StringComparer.Ordinal)).Add(name); // no cast up: arithmetic over it is left to the rows
        }
        if (_mixed.Length == 0 && missing.Count == 0 && dropped.Count == 0 && widened is null && reshaped is null) return (_fileFilter, true);
        Partitions? partitions = _partitions;
        ScalarExpr? filter = null;
        bool all = true;
        IEnumerable<ScalarExpr> conjuncts = _fileFilter is null ? _mixed : FilterShapes.conjuncts(_fileFilter).Concat(_mixed);
        foreach (ScalarExpr conjunct in conjuncts)
        {
            bool complete = true, changed = false;
            HashSet<string> computed = widened is null && reshaped is null ? [] : FilterArithmetic.Columns(conjunct);
            if (reshaped is not null && computed.Overlaps(reshaped))
            {
                all = false; // evaluated after conversion
                continue;
            }
            ScalarExpr substituted = ScalarExprOps.substitute(FuncConvert.FromFunc((string name) =>
            {
                if (dropped.Contains(name))
                {
                    complete = false;
                    return FSharpOption<ScalarExpr>.None;
                }
                // Cast up only where arithmetic reads it: a plain comparison is the same at any width (and keeps its pruning).
                if (widened is not null && computed.Contains(name) && widened.TryGetValue(name, out IArrowType? meant))
                    return FSharpOption<ScalarExpr>.Some(ScalarExpr.NewCast(ScalarExpr.NewColumn(name), LogicalType.NewArrow(meant), false));
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
}
