using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Arrow.Builders;
using Kaminari.Sazanami.Arrow.Compute;
using Kaminari.Sazanami.Arrow.Interop;
using Kaminari.Sazanami.Filters;
using Kaminari.Sazanami.Rows;
using Microsoft.FSharp.Core;

namespace Kaminari.Seseragi.Files;

/// <summary>
/// What a dataset reader of any format does around its files' readers: the columns selected (from files, and
/// partition columns), the filter split (conjuncts over partition columns and the file path column — values of a file, not
/// of a row — decide which files are read; the others go to the files, those over both with each file's values), how each file's columns convert to the dataset's types, and
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
    private readonly string? _rowIndex;                   // the row index across the files, first (ParquetReadOptions.RowIndexColumn, …)
    private readonly string? _fileRowIndex;               // each row's number in its file, last (DatasetOptions.FileRowIndexColumn)
    private readonly Schema _converted;                   // the batches Convert makes: the readers' numbers first when numbered, no file row index
    private readonly ScalarExpr? _positions;              // the conjuncts over the file row index alone
    private readonly FilterProgram? _positionProgram;     // _positions as rows are filtered by them (numbered in their files)
    private readonly bool _allowMissing;
    private readonly ArrowConversionRules _rules;
    private readonly Column[] _layout;
    private readonly ScalarExpr? _fileFilter;
    private readonly ScalarExpr[] _mixed;
    private readonly Dictionary<string, IArrowType> _types;     // the dataset's column types, by name (what the filter means)
    private readonly string[] _filterColumns = [];              // the columns the files' conjuncts read
    private readonly ScalarExpr? _rowFilter;              // what the rows delivered must pass (the conjuncts not decided per file)
    private readonly FilterProgram? _program;             // _rowFilter as the dataset applies it after converting (exact)
    private readonly IReadOnlyList<DatasetFile> _files;
    private readonly ArrowDefaults? _defaults;            // the dataset's defaults (DatasetOptions.Defaults)
    private readonly ArrowDefaults?[]? _fileDefaults;     // per file with defaults of its own: those over the dataset's

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
    /// <param name="files">The dataset's files (their defaults).</param>
    /// <param name="selected">The columns selected by name (with <paramref name="paths"/>; both null: all).</param>
    /// <param name="paths">The struct fields selected (each path from a top-level column).</param>
    /// <param name="filter">The filter (canonical), or null.</param>
    /// <param name="filterRows">Whether the rows the filter rules out are left out.</param>
    /// <param name="rowIndexColumn">
    /// The row index column (across the files), or null. The files' readers put their rows' numbers first when it or
    /// the file row index is asked (<see cref="NumbersColumn"/>); <see cref="Number"/> makes both of them.
    /// </param>
    /// <param name="views">Whether strings added (partition values, the file path) are views.</param>
    /// <param name="pathOf">File <c>i</c>'s path.</param>
    /// <param name="error">The format's error for a message (naming the file) and its cause.</param>
    public DatasetShape(DatasetOptions dataset, Partitions? partitions, Func<int, bool, Field> partitionField, Schema columns, IReadOnlyList<DatasetFile> files,
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
        _fileRowIndex = dataset.FileRowIndexColumn;
        _allowMissing = dataset.AllowMissingColumns || dataset.Schema is null && dataset.SchemaMode == DatasetSchemaMode.UnionByName;
        _rules = new ArrowConversionRules(dataset.AllowLossyCasts, _allowMissing);
        bool IsPartition(string name) => partitions?.IndexOf(name) >= 0;
        // A value per file: a partition column, or the file path column (when it names no partition column).
        bool PerFile(string name) => IsPartition(name) || name == dataset.FilePathColumn;

        Columns = columns.FieldsList.ToDictionary(f => f.Name, StringComparer.Ordinal);

        // The defaults of missing fields: the dataset's, and files' own over them.
        // Checked against every column when the dataset opened (CheckDefaults): those of columns these options do not
        // read (typed rows) are passed over.
        _files = files;
        (_defaults, _fileDefaults) = DefaultsOf(dataset, partitions, columns, files, onlyColumns: true);

        // The columns selected: from files (not shadowed by a partition column), and partition columns (the file path
        // column, always last when asked, may be named too: the dataset's schema lists it).
        bool all = selected is null && paths is null;
        foreach (string name in selected ?? [])
        {
            if (!IsPartition(name) && !Columns.ContainsKey(name) && name != dataset.FilePathColumn && name != dataset.FileRowIndexColumn)
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

        // The filter: conjuncts over values per file only (partition columns, the file path) decide which files are
        // read; the others go to the files, those over both with each file's values put in.
        Read = [.. Enumerable.Repeat(true, files.Count)];
        bool exact = true;
        List<ScalarExpr> onFiles = [], mixed = [], positions = [];
        if (filter is not null)
        {
            List<ScalarExpr> perFile = [];
            foreach (ScalarExpr conjunct in FilterShapes.conjuncts(filter))
            {
                string[] names = [.. ScalarExprOps.collectColumns(conjunct)];
                // Over the file row index: alone, its rows are selected by number; with other columns, refused when rows
                // are filtered (only pruning otherwise: left out).
                if (_fileRowIndex is { } position && names.Contains(position))
                {
                    if (names.All(n => n == position)) positions.Add(conjunct);
                    else if (filterRows)
                        throw new ArgumentException($"A filter conjunct reads the file row index column '{position}' with other columns "
                            + $"({string.Join(", ", names.Where(n => n != position))}): only conjuncts over it alone are taken (split the conjunct).", "options");
                    continue;
                }
                int inPerFile = names.Count(PerFile);
                (inPerFile == 0 ? onFiles : inPerFile == names.Length ? perFile : mixed).Add(conjunct);
            }
            if (perFile.Count > 0)
            {
                var (fields, values, filePaths) = PerFileColumns();
                using (filePaths) exact = Prune((fields, values), perFile.Aggregate(And), Read);
            }
        }
        _types = new(StringComparer.Ordinal);
        foreach (Field field in columns.FieldsList) _types.TryAdd(field.Name, field.DataType);
        _fileFilter = onFiles.Count == 0 ? null : onFiles.Aggregate(And);
        _mixed = [.. mixed];
        _filterColumns = [.. onFiles.Concat(mixed).SelectMany(ScalarExprOps.collectColumns).Distinct()];
        _rowFilter = onFiles.Count + mixed.Count == 0 ? null : onFiles.Concat(mixed).Aggregate(And);

        _positions = positions.Count == 0 ? null : positions.Aggregate(And);

        // Exact when the filter lowers exactly over the dataset's types (each file's rows are filtered again after
        // conversion where its own filtering is not exact).
        FilterRoots = [];
        FiltersExactly = filterRows && exact;
        if (filterRows && _positions is { } over)
        {
            LoweredFilter lowered = FilterLowering.lower(new Schema([new Field(_fileRowIndex!, Int64Type.Default, nullable: false)], null), over);
            FiltersExactly &= lowered.Exact;
            if (OptionModule.IsSome(lowered.Program)) _positionProgram = lowered.Program.Value;
        }
        if (filterRows && _rowFilter is { } rows)
        {
            LoweredFilter lowered = FilterLowering.lower(FilterSchema(columns), rows);
            FiltersExactly &= lowered.Exact;
            if (FiltersExactly && OptionModule.IsSome(lowered.Program))
            {
                _program = lowered.Program.Value;
                FilterRoots = [.. _program.Columns.Select(c => c.Path[0]).Where(n => !PerFile(n)).Distinct()];
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
        if (dataset.FilePathColumn is { } file)
        {
            if (output.Any(f => f.Name == file)) throw new ArgumentException($"The file path column '{file}' has the name of another column read.", "options");
            output.Add(PathField(file, views));
        }
        // What Convert makes: the readers' numbers first (named as the readers name them), when numbered.
        List<Field> converted = [.. output];
        if (NumbersColumn is { } numbers)
        {
            if (output.Any(f => f.Name == numbers)) throw new ArgumentException($"The row index column '{numbers}' has the name of another column read.", "options");
            converted.Insert(0, new Field(numbers, Int64Type.Default, nullable: false));
        }
        _converted = new Schema(converted, columns.Metadata);
        if (rowIndexColumn is { } index)
        {
            if (output.Any(f => f.Name == index)) throw new ArgumentException($"The row index column '{index}' has the name of another column read.", "options");
            output.Insert(0, new Field(index, Int64Type.Default, nullable: false));
        }
        if (_fileRowIndex is { } fileRow)
        {
            if (output.Any(f => f.Name == fileRow)) throw new ArgumentException($"The file row index column '{fileRow}' has the name of another column read.", "options");
            output.Add(new Field(fileRow, Int64Type.Default, nullable: false));
        }
        Schema = new Schema(output, columns.Metadata);

        static ScalarExpr And(ScalarExpr a, ScalarExpr b) => ScalarExpr.NewBinary(BinaryOp.And, a, b);

        // The values per file a filter decides files by: the partition columns, then the file paths (made here: the
        // caller disposes them).
        (Field[] Fields, IArrowArray[] Columns, StringArray? Paths) PerFileColumns()
        {
            List<Field> fields = [.. partitions?.Fields ?? []];
            List<IArrowArray> values = [.. partitions?.Columns ?? []];
            StringArray? built = null;
            if (dataset.FilePathColumn is { } file && !IsPartition(file))
            {
                var builder = new StringArray.Builder();
                for (int i = 0; i < files.Count; i++) builder.Append(pathOf(i));
                built = builder.Build(PooledArrowAllocator.Instance);
                fields.Add(PathField(file, views: false));
                values.Add(built);
            }
            return ([.. fields], [.. values], built);
        }
    }

    /// <summary>The file path column as batches hold it.</summary>
    private static Field PathField(string name, bool views) => new(name, views ? StringViewType.Default : StringType.Default, nullable: false);

    /// <summary>
    /// The columns a filter reads: the columns from files (a partition column of the same name in its place), then the
    /// other partition columns, then the file path column.
    /// </summary>
    private Schema FilterSchema(Schema columns)
    {
        List<Field> fields = [.. columns.FieldsList.Select(f => _partitions?.IndexOf(f.Name) is int p and >= 0 ? _partitionField(p, _views) : f)];
        for (int p = 0; p < (_partitions?.Fields.Length ?? 0); p++)
        {
            if (columns.GetFieldIndex(_partitions!.Fields[p].Name) < 0) fields.Add(_partitionField(p, _views));
        }
        if (_dataset.FilePathColumn is { } file && !fields.Any(f => f.Name == file)) fields.Add(PathField(file, _views));
        return new Schema(fields, null);
    }

    /// <summary>
    /// Checks the defaults of a dataset that opens (<see cref="DatasetOptions.Defaults"/>, the files' own) against its
    /// columns from files: errors come when it opens, not when it is read.
    /// </summary>
    /// <exception cref="ArgumentException">A default names no field of the dataset, or one whose values come from elsewhere, or its value is not of the field's type.</exception>
    public static void CheckDefaults(DatasetOptions dataset, Partitions? partitions, Schema columns, IReadOnlyList<DatasetFile> files) =>
        DefaultsOf(dataset, partitions, columns, files, onlyColumns: false);

    /// <summary>
    /// The dataset's defaults and, per file with its own, those over the dataset's (null when no file has any);
    /// defaults of top-level columns <c>columns</c> lacks passed over with <c>onlyColumns</c> (else an error).
    /// </summary>
    private static (ArrowDefaults? Dataset, ArrowDefaults?[]? Files) DefaultsOf(DatasetOptions dataset, Partitions? partitions, Schema columns,
        IReadOnlyList<DatasetFile> files, bool onlyColumns)
    {
        string? NotFromFiles(string name) =>
            partitions?.IndexOf(name) >= 0 ? "it is a partition column (its values come with the files)"
            : name == dataset.FilePathColumn ? "it is the file path column"
            : null;
        ArrowDefaults? defaults = ColumnDefaults.Build(dataset.Defaults, columns, NotFromFiles, $"{dataset.Name}.Defaults", onlyColumns);
        ArrowDefaults?[]? own = null;
        for (int i = 0; i < files.Count; i++)
        {
            if (files[i].Defaults is not { } given) continue;
            own ??= new ArrowDefaults?[files.Count];
            own[i] = ArrowDefaults.Merge(ColumnDefaults.Build(given, columns, NotFromFiles, $"The defaults of '{files[i].Path}'", onlyColumns), defaults);
        }
        return (defaults, own);
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

    /// <summary>
    /// The conjuncts of the filter not decided per file (null when none): those that prune row groups and pages, and
    /// with rows filtered leave rows out, so that the rows delivered are not the files' counts.
    /// </summary>
    public ScalarExpr? RowFilter => _rowFilter;

    /// <summary>
    /// Whether the dataset filters some files' rows after converting them (where their own filtering is not exact), or
    /// filters rows by their numbers in their files (<see cref="Number"/>): their readers then read every row.
    /// </summary>
    public bool FiltersAfter => _program is not null || _positionProgram is not null;

    /// <summary>
    /// The name of the column the files' readers put first, numbering their rows (their row index option), when the
    /// row index or the file row index is asked; else null.
    /// </summary>
    public string? NumbersColumn => _rowIndex ?? _fileRowIndex;

    /// <summary>The conjuncts of the filter over the file row index alone (null when none): the rows they select, by number.</summary>
    public ScalarExpr? PositionFilter => _positions;

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

    /// <summary>
    /// What file <paramref name="index"/>, whose columns are <paramref name="has"/>, reads (errors name
    /// <paramref name="path"/>). The same for every file of these columns unless <see cref="OwnDefaultsApply"/>.
    /// </summary>
    public FileColumns ColumnsOf(Dictionary<string, Field> has, string path, int index)
    {
        ArrowDefaults? defaults = DefaultsOf(index);
        // What the file's reader reads: whole columns and paths (those the file has).
        HashSet<string> whole = new(StringComparer.Ordinal);
        List<string[]> paths = [];
        foreach (Target target in Targets)
        {
            if (!has.TryGetValue(target.Name, out Field? field))
            {
                if (!_allowMissing && !MayLack(defaults, null, [target.Name])) throw Missing(path, target.Name, filter: false);
                continue;
            }
            if (target.Paths is null)
            {
                whole.Add(target.Name);
                continue;
            }
            string[][] kept = [.. target.Paths.Where(p => Exists(field.DataType, p, 0))];
            if (kept.Length < target.Paths.Length && !_allowMissing
                && target.Paths.FirstOrDefault(p => !Exists(field.DataType, p, 0) && !MayLack(defaults, field.DataType, [target.Name, .. p])) is { } lacked)
                throw Missing(path, string.Join('.', [target.Name, .. lacked]), filter: false);
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
                    if (!_allowMissing && !MayLack(defaults, null, [root])) throw Missing(path, root, filter: true);
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

    /// <summary>
    /// How the columns of file <paramref name="index"/>'s batches (<paramref name="read"/>) convert to the dataset's: the
    /// same for every file of these columns unless <see cref="OwnDefaultsApply"/>.
    /// </summary>
    public Converted Conversions(Schema read, bool after, string path, int index)
    {
        int skip = NumbersColumn is null ? 0 : 1;
        ArrowDefaults? defaults = DefaultsOf(index);
        var outputs = new (int, ArrowConversion)[Targets.Length];
        for (int t = 0; t < Targets.Length; t++) outputs[t] = Conversion(read, skip, Targets[t].Name, Targets[t].Field, path, defaults?.Field(Targets[t].Name));
        var filtered = after ? new (int, ArrowConversion)[FilterRoots.Length] : [];
        for (int f = 0; f < filtered.Length; f++) filtered[f] = Conversion(read, skip, FilterRoots[f], Columns[FilterRoots[f]], path, defaults?.Field(FilterRoots[f]));
        return new Converted(outputs, filtered);
    }

    /// <summary>The defaults of file <paramref name="index"/>'s missing fields: its own over the dataset's.</summary>
    private ArrowDefaults? DefaultsOf(int index) => _fileDefaults?[index] ?? _defaults;

    /// <summary>
    /// Whether file <paramref name="index"/>, whose columns are <paramref name="has"/>, lacks a field its own defaults
    /// name (or one above it): what it reads (<see cref="ColumnsOf"/>, <see cref="Conversions"/>) is then its own, not
    /// that of the other files of these columns.
    /// </summary>
    public bool OwnDefaultsApply(int index, IReadOnlyDictionary<string, Field> has) =>
        _fileDefaults?[index] is not null
        && _files[index].Defaults!.Any(d => !has.TryGetValue(d.Path[0], out Field? field) || !Exists(field.DataType, [.. d.Path.Skip(1)], 0));

    /// <summary>
    /// Whether a file may lack the first field on <paramref name="path"/> it lacks (the column when
    /// <paramref name="type"/>, the file's column, is null): a default is declared for it.
    /// </summary>
    private static bool MayLack(ArrowDefaults? defaults, IArrowType? type, string[] path)
    {
        ArrowDefaults? node = defaults?.Field(path[0]);
        for (int at = 1; type is not null && at < path.Length; at++)
        {
            type = Below(type, path[at]);
            node = node?.Field(path[at]);
        }
        return type is not null || node is { Declared: true };

        // The field below a type (through lists), or null.
        static IArrowType? Below(IArrowType type, string name)
        {
            while (type is ListType or LargeListType or ListViewType or LargeListViewType or FixedSizeListType) type = ArrowWidening.ListElement(type);
            return type switch
            {
                StructType s => s.GetFieldIndex(name, StringComparer.Ordinal) is int i and >= 0 ? s.Fields[i].DataType : null,
                MapType m => name switch { "key" => m.KeyField.DataType, "value" => m.ValueField.DataType, _ => null },
                _ => null,
            };
        }
    }

    /// <summary>
    /// Where a column comes from in the reader's batches (-1 when the file lacks it: its default, else nulls) and how it
    /// converts to <paramref name="targetField"/> (struct fields it lacks as their <paramref name="defaults"/> say); a
    /// conversion the rules refuse is an error naming the file, the column and what allows it.
    /// </summary>
    private (int Source, ArrowConversion Conversion) Conversion(Schema read, int skip, string name, Field targetField, string path, ArrowDefaults? defaults)
    {
        IArrowType target = targetField.DataType;
        int at = -1;
        for (int c = skip; c < read.FieldsList.Count; c++)
        {
            if (read.FieldsList[c].Name == name) at = c;
        }
        if (at < 0) return (-1, defaults?.ScalarFor(target) is { } value ? new ConstantConversion(value) : new NullsConversion(target));
        IArrowType type = read.FieldsList[at].DataType;
        if (EnumMismatch(read.FieldsList[at], targetField, name, declared: _dataset.Schema is not null) is { } mismatch)
            throw _error($"{path}: {mismatch}", null);
        if (ArrowWidening.TryCompile(type, target, _rules, name, out ArrowConversion? conversion, out ArrowConversionError? error, defaults)) return (at, conversion);
        string hint = error.Problem switch
        {
            ArrowConversionProblem.Lossy => $" ({_dataset.Name}.AllowLossyCasts allows lossy conversions)",
            ArrowConversionProblem.MissingField => $" ({_dataset.Name}.AllowMissingColumns reads missing columns and fields as nulls, a default as its value)",
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
    /// An Enum (a dataframe engine's, in field metadata <c>_PL_ENUM_VALUES2</c>, <c>_PL_ENUM_VALUES</c> before it) of other
    /// categories than the dataset's, or an Enum against a column that is none (or the reverse), at any depth: what is
    /// wrong, or null. An Enum is a declared set of values: as the engines that declare it, the dataset does not change
    /// it (cast the column, or change the Enum).
    /// </summary>
    /// <param name="declared">
    /// The dataset's fields are given (<see cref="DatasetOptions.Schema"/>): a field declared without an Enum reads the
    /// file's Enum as its values (the way to downgrade one); only a declared Enum is compared.
    /// </param>
    /// <param name="file">The file's field.</param>
    /// <param name="dataset">The dataset's field.</param>
    /// <param name="path">The field's path (in messages).</param>
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
            + $"({_dataset.Name}.AllowMissingColumns reads missing columns and fields as nulls, {_dataset.Name}.Defaults as their defaults).", null);

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
    /// Numbered (<see cref="NumbersColumn"/>), the readers' numbers stay first, as they are: <see cref="Number"/> then
    /// makes the batch the dataset's. Null when no row is left. Safe on any thread.
    /// </summary>
    public RecordBatch? Convert(RecordBatch batch, Converted converted, bool filterAfter, int index)
    {
        int length = batch.Length, skip = NumbersColumn is null ? 0 : 1;
        var columns = new IArrowArray[_converted.FieldsList.Count];
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
                if (filterAfter && _program is not null) filterArrays = [.. converted.Filtered.Select(f => Convert(batch, f, length, used))];
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

            if (filterAfter && _program is not null)
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
                    else if (_partitions?.IndexOf(column.Path[0]) is int p and >= 0)
                    {
                        root = Constant(_partitions.Columns[p], index, length, _partitionField(p, _views).DataType);
                        constants.Add(root);
                    }
                    else
                    {
                        root = ConstantArrays.String(_pathOf(index), length, _views); // the file path column
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
        return new RecordBatch(_converted, columns, length);

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

    /// <summary>
    /// A converted batch (<see cref="Convert(RecordBatch, Converted, bool, int)"/>) numbered as the dataset numbers rows, in file order (a file's rows are
    /// numbered once those before it are known): the row index first, <paramref name="toGlobal"/> added to the readers'
    /// numbers; the file row index last, <paramref name="toFile"/> added; with <paramref name="filterPositions"/>, the
    /// rows the conjuncts over the file row index rule out left out (null when none is left). Unnumbered, the batch as
    /// it is.
    /// </summary>
    public RecordBatch? Number(RecordBatch batch, long toFile, long toGlobal, bool filterPositions)
    {
        if (NumbersColumn is null) return batch;
        IArrowArray numbers = batch.Column(0);
        IArrowArray? global = null, file = null;
        if (_fileRowIndex is null)
        {
            global = numbers;
            RowNumbers.Shift(global, toGlobal);
        }
        else
        {
            if (_rowIndex is not null) global = RowNumbers.Shifted(numbers, toGlobal);
            file = numbers;
            RowNumbers.Shift(file, toFile);
        }
        List<IArrowArray> columns = [.. batch.Arrays.Skip(1)];
        if (global is not null) columns.Insert(0, global);
        if (file is not null) columns.Add(file);
        int length = batch.Length;
        if (filterPositions && _positionProgram is { } program && length > 0)
        {
            var mask = new ulong[Masks.Words(length)];
            int kept = program.Evaluate([file!], length, mask);
            if (kept == 0)
            {
                foreach (IArrowArray column in columns) column.Dispose();
                return null;
            }
            if (kept < length)
            {
                for (int c = 0; c < columns.Count; c++)
                {
                    IArrowArray all = columns[c];
                    columns[c] = ArrowFilter.Filter(all, mask);
                    all.Dispose();
                }
                length = kept;
            }
        }
        return new RecordBatch(Schema, columns, length);
    }

    /// <summary>
    /// The rows of a file of <paramref name="rows"/> rows the conjuncts over the file row index select (by number), as
    /// far as their shapes say (comparisons of it with integers, their <c>And</c>, <c>Or</c> and <c>Not</c>, <c>In</c>
    /// lists of integers); null when there are none or none of them has such a shape. <paramref name="exact"/>: every
    /// conjunct had (the rows are exactly those the conjuncts keep).
    /// </summary>
    public RowRanges? PositionRows(long rows, out bool exact)
    {
        exact = false;
        if (_positions is null) return null;
        RowRanges? selected = null;
        exact = true;
        foreach (ScalarExpr conjunct in FilterShapes.conjuncts(_positions))
        {
            if (Ranges(conjunct) is { } ranges) selected = selected is null ? ranges : selected.Intersect(ranges);
            else exact = false;
        }
        return selected;

        // The rows a predicate over the position keeps (it is never null), or null for a shape not modelled.
        RowRanges? Ranges(ScalarExpr e)
        {
            switch (e)
            {
                case ScalarExpr.Binary { op: var op } b when op.IsAnd || op.IsOr:
                    return Ranges(b.left) is { } l && Ranges(b.right) is { } r ? op.IsAnd ? l.Intersect(r) : l.Union(r) : null;
                case ScalarExpr.Unary { op: var op } u when op.IsNot:
                    return Ranges(u.expr)?.Complement(rows);
                case ScalarExpr.Binary b when b.left is ScalarExpr.Column && Integer(b.right) is { } value:
                    return Compare(b.op, value);
                case ScalarExpr.Binary b when b.right is ScalarExpr.Column && Integer(b.left) is { } value:
                    return Flipped(b.op) is { } flipped ? Compare(flipped, value) : null;
                case ScalarExpr.InList { expr: ScalarExpr.Column, source: InListSource.Literals list, negated: var negated }:
                {
                    List<long> at = [];
                    foreach (LiteralValue literal in list.Item)
                    {
                        if (Integer(ScalarExpr.NewLiteral(literal)) is not { } v) return null;
                        if (v >= 0 && v < rows) at.Add((long)v);
                    }
                    at.Sort();
                    RowRanges these = RowRanges.FromIndices([.. at.Distinct()]);
                    return negated ? these.Complement(rows) : these;
                }
                default:
                    return null;
            }
        }

        // A comparison of the position with a value (never null: the missing-aware forms are the same).
        RowRanges? Compare(BinaryOp op, Int128 value)
        {
            if (op.IsNotEq || op.IsNotEqMissing) return Compare(BinaryOp.Eq, value)!.Complement(rows);
            (Int128 From, Int128 To)? range = op switch // [From, To)
            {
                _ when op.IsEq || op.IsEqMissing => (value, value + 1),
                _ when op.IsLt => (Int128.MinValue, value),
                _ when op.IsLtEq => (Int128.MinValue, value + 1),
                _ when op.IsGt => (value + 1, Int128.MaxValue),
                _ when op.IsGtEq => (value, Int128.MaxValue),
                _ => null,
            };
            if (range is not var (from, to)) return null;
            long start = (long)Int128.Clamp(from, 0, rows), end = (long)Int128.Clamp(to, 0, rows);
            return end > start ? RowRanges.Of([(start, end - start)]) : RowRanges.Empty;
        }

        static BinaryOp? Flipped(BinaryOp op) => op switch
        {
            _ when op.IsEq || op.IsNotEq || op.IsEqMissing || op.IsNotEqMissing => op,
            _ when op.IsLt => BinaryOp.Gt,
            _ when op.IsLtEq => BinaryOp.GtEq,
            _ when op.IsGt => BinaryOp.Lt,
            _ when op.IsGtEq => BinaryOp.LtEq,
            _ => null,
        };

        static Int128? Integer(ScalarExpr e) => e is ScalarExpr.Literal { value: var v } ? v switch
        {
            LiteralValue.Int8 x => x.Item,
            LiteralValue.Int16 x => x.Item,
            LiteralValue.Int32 x => x.Item,
            LiteralValue.Int64 x => x.Item,
            LiteralValue.UInt8 x => x.Item,
            LiteralValue.UInt16 x => x.Item,
            LiteralValue.UInt32 x => x.Item,
            LiteralValue.UInt64 x => x.Item,
            _ => null,
        } : null;
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
    public DictionaryUnifier? NewUnifier() => _dataset.UnifyDictionaries ? DictionaryUnifier.For(Output) : null;

    /// <summary>
    /// The schema of the batches delivered: <see cref="Schema"/>, its dictionaries indexed as the given schema declares
    /// them when they are unified (<see cref="DictionaryUnifier.Declared(Schema, Schema)"/>).
    /// </summary>
    public Schema Output => _dataset.UnifyDictionaries && _dataset.Schema is { } given ? DictionaryUnifier.Declared(Schema, given) : Schema;

    /// <summary>A column of the batch converted (the batch's own when nothing changes, marked used), or its default (or nulls) when the file lacks it.</summary>
    private static IArrowArray Convert(RecordBatch batch, (int Source, ArrowConversion Conversion) plan, int length, bool[] used)
    {
        if (plan.Source < 0) return plan.Conversion is ConstantConversion constant ? constant.Of(length) : NullArrays.Of(plan.Conversion.Target, length);
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
    /// Clears <paramref name="read"/> for the files whose values per file (partition values, the file path) the filter
    /// rules out (false or null), as the IR's semantics evaluate it; whether every conjunct could be decided (those that cannot keep every file).
    /// </summary>
    private static bool Prune((Field[] Fields, IArrowArray[] Columns) perFile, ScalarExpr filter, bool[] read)
    {
        var schema = new Schema(perFile.Fields, null);
        LoweredFilter lowered = FilterLowering.lower(schema, filter);
        if (OptionModule.IsNone(lowered.Program)) return lowered.Exact;
        FilterProgram program = lowered.Program.Value;
        IArrowArray[] columns = [.. program.Columns.Select(c => perFile.Columns[System.Array.FindIndex(perFile.Fields, f => f.Name == c.Path[0])])];
        var selected = new ulong[(read.Length + 63) / 64];
        program.Evaluate(columns, read.Length, selected);
        for (int i = 0; i < read.Length; i++) read[i] = (selected[i / 64] >> (i % 64) & 1) != 0;
        return lowered.Exact;
    }

    /// <summary>
    /// The filter file <paramref name="index"/> reads: the conjuncts over its columns, those over partition columns
    /// too with its values, columns it lacks (<paramref name="missing"/>) as their defaults (nulls when none; a default
    /// of no literal form leaves its conjuncts out); conjuncts over
    /// <paramref name="dropped"/> columns are left out. A column the file stores as a narrower number than the
    /// dataset's (<paramref name="has"/>) is cast up where arithmetic reads it, so that the arithmetic is the dataset's
    /// (an Int32 file's column of an Int64 dataset wraps where the dataset does); a conjunct with arithmetic over a column
    /// of another type that cannot be cast up (a nested one, a decimal, …) is left out. Whether every conjunct is in (one left out, or one with a partition value of no literal
    /// form, makes the file's rows filtered less). The conjuncts over the file row index are never in: the file's rows are
    /// filtered by them after (<see cref="Number"/>) unless <paramref name="positionsSelected"/> (the file's reader reads
    /// exactly the rows they keep, <see cref="PositionRows"/>).
    /// </summary>
    public (ScalarExpr? Filter, bool Complete) FileFilter(int index, HashSet<string> missing, HashSet<string> dropped, IReadOnlyDictionary<string, Field> has,
        bool positionsSelected = false)
    {
        var (filter, complete) = FileFilterOf(index, missing, dropped, has);
        return (filter, complete && (_positionProgram is null || positionsSelected));
    }

    private (ScalarExpr? Filter, bool Complete) FileFilterOf(int index, HashSet<string> missing, HashSet<string> dropped, IReadOnlyDictionary<string, Field> has)
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
                    LiteralValue? value = DefaultsOf(index)?.Field(name)?.ScalarFor(_types[name]) is { } scalar ? Literal(scalar) : LiteralValue.NewNull(LogicalTypes.Null);
                    if (value is null)
                    {
                        complete = false;
                        return FSharpOption<ScalarExpr>.None;
                    }
                    changed = true;
                    return FSharpOption<ScalarExpr>.Some(ScalarExpr.NewLiteral(value));
                }
                int p = partitions?.IndexOf(name) ?? -1;
                if (p < 0 && name == _dataset.FilePathColumn)
                {
                    changed = true;
                    return FSharpOption<ScalarExpr>.Some(ScalarExpr.NewLiteral(LiteralValue.NewString(_pathOf(index))));
                }
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

    /// <summary>A default value as an IR literal (a dictionary's as its value's); null for a type without one.</summary>
    private static LiteralValue? Literal(ArrowScalar scalar)
    {
        if (scalar.Value is null) return LiteralValue.NewNull(LogicalTypes.Null);
        if (scalar.Value is ArrowScalar inner) return Literal(inner);
        return scalar.Type switch
        {
            BooleanType => LiteralValue.NewBool((bool)scalar.Value),
            Int8Type => LiteralValue.NewInt8((sbyte)Bytes()[0]),
            Int16Type => LiteralValue.NewInt16(BinaryPrimitives.ReadInt16LittleEndian(Bytes())),
            Int32Type => LiteralValue.NewInt32(BinaryPrimitives.ReadInt32LittleEndian(Bytes())),
            Int64Type => LiteralValue.NewInt64(BinaryPrimitives.ReadInt64LittleEndian(Bytes())),
            UInt8Type => LiteralValue.NewUInt8(Bytes()[0]),
            UInt16Type => LiteralValue.NewUInt16(BinaryPrimitives.ReadUInt16LittleEndian(Bytes())),
            UInt32Type => LiteralValue.NewUInt32(BinaryPrimitives.ReadUInt32LittleEndian(Bytes())),
            UInt64Type => LiteralValue.NewUInt64(BinaryPrimitives.ReadUInt64LittleEndian(Bytes())),
            FloatType => LiteralValue.NewFloat32(BinaryPrimitives.ReadSingleLittleEndian(Bytes())),
            DoubleType => LiteralValue.NewFloat64(BinaryPrimitives.ReadDoubleLittleEndian(Bytes())),
            Date32Type => LiteralValue.NewDate(DateOnly.FromDayNumber(DateOnly.FromDateTime(DateTime.UnixEpoch).DayNumber + BinaryPrimitives.ReadInt32LittleEndian(Bytes()))),
            StringType or LargeStringType or StringViewType => LiteralValue.NewString(System.Text.Encoding.UTF8.GetString(Bytes())),
            _ => null,
        };

        byte[] Bytes() => (byte[])scalar.Value!;
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
