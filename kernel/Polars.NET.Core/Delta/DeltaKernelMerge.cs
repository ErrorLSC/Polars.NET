using System.Globalization;
using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Lakehouse.Skipping;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Exceptions;
using Polars.NET.Core.Wrappers;

namespace Polars.NET.Core.Delta;

/// <summary>What a merge clause does to the rows it takes.</summary>
internal enum MergeClauseKind
{
    /// <summary>A target row with a matching source row becomes the source row (columns the source lacks keep their value).</summary>
    MatchedUpdate,
    /// <summary>A target row with a matching source row is deleted.</summary>
    MatchedDelete,
    /// <summary>A source row without a matching target row is inserted.</summary>
    NotMatchedInsert,
    /// <summary>A target row without a matching source row is deleted.</summary>
    NotMatchedBySourceDelete,
}

/// <summary>One clause of a merge: what to do, and when (over target columns and suffixed source columns).</summary>
internal readonly record struct MergeClause(MergeClauseKind Kind, ExprHandle Condition);

/// <summary>What a merge did.</summary>
internal sealed record DeltaMergeResult(
    long? Version, long SourceRows, long InsertedRows, long UpdatedRows, long DeletedRows, long CopiedRows,
    int RemovedFiles, int AddedFiles, int DeletionVectorsAdded, int ChangeFiles, bool SchemaEvolved);

/// <summary>
/// Merges a source into a Delta table through the managed kernel, by equality of the merge keys. Every row of the
/// full join of target and source takes the first clause, in the order given, whose state (matched, not matched,
/// not matched by source) and condition hold; a row no clause takes stays as it is (a source row is not inserted).
/// Conditions see the target's columns by name and the source's as name + <see cref="SourceSuffix"/>.
/// Not reachable from the public API yet.
/// </summary>
/// <remarks>
/// As SQL MERGE: a source row whose key is null matches nothing; a target row matched by more than one source row
/// that an update or delete clause takes is an error. Only files that can hold a matching key are read, unless a
/// not-matched-by-source clause needs the whole table. Updated and inserted rows go to new files; the rows they
/// replace are marked in deletion vectors when the table has them on, else their files are rewritten.
/// </remarks>
internal static class DeltaKernelMerge
{
    /// <summary>The suffix that tells source columns apart in clause conditions (the public API's <c>MergeContext</c>).</summary>
    public const string SourceSuffix = "_src_tmp";

    private const string FileColumn = "__seseragi_file";
    private const string ActionColumn = "__seseragi_action";
    private const string InTarget = "__seseragi_in_target";
    private const string InSource = "__seseragi_in_source";

    private const int Keep = 0, Insert = 1, Update = 2, Delete = 3, Ignore = 4;

    /// <param name="tablePath">The table location: a local directory or a cloud URL.</param>
    /// <param name="source">The source rows; consumed.</param>
    /// <param name="keys">The merge keys: columns of both, equal for a match.</param>
    /// <param name="clauses">The clauses in order; consumed. None: update matched rows and insert the others.</param>
    /// <param name="canEvolve">Add source columns the table lacks to its schema, instead of failing.</param>
    /// <param name="cloud">Cloud settings for a cloud URL.</param>
    public static DeltaMergeResult Merge(
        string tablePath, LazyFrameHandle source, string[] keys, IReadOnlyList<MergeClause> clauses, bool canEvolve = false,
        CloudArgs? cloud = null)
    {
        var owned = new List<ExprHandle>(clauses.Select(c => c.Condition));
        try
        {
            if (keys.Length == 0) throw new ArgumentException("A merge needs at least one key.", nameof(keys));
            if (clauses.Count == 0)
            {
                clauses = [new(MergeClauseKind.MatchedUpdate, PolarsWrapper.Lit(true)), new(MergeClauseKind.NotMatchedInsert, PolarsWrapper.Lit(true))];
                owned.AddRange(clauses.Select(c => c.Condition));
            }

            using DmlTable table = DmlTable.Open(tablePath, cloud);
            Snapshot snapshot = table.Snapshot;

            using DataFrameHandle sourceRows = PolarsWrapper.LazyCollect(source, PlEngine.Auto);
            source = null!;
            StructType sourceSchema = DeltaKernelWrite.SchemaOf(sourceRows);
            StructType schema = Evolve(snapshot, sourceSchema, keys, canEvolve, out MetadataAction? newMetadata);

            // Files that can hold a matching key; all of them when rows without a source match may be deleted.
            bool bySource = clauses.Any(c => c.Kind == MergeClauseKind.NotMatchedBySourceDelete);
            Predicate? keyRange = bySource ? null : KeyRange(sourceRows, snapshot, keys);
            IReadOnlyList<AddAction> candidates = keyRange is null ? snapshot.Files : snapshot.FilesMatching(keyRange);

            var transaction = Transaction.Start(table.FileSystem, snapshot, "MERGE");
            transaction.ReadPredicate(keyRange).ReadFiles(candidates);
            if (newMetadata is not null) transaction.SetMetadata(newMetadata);
            RecordParameters(transaction, keys, clauses);

            // The rows the clauses change, with their action: one engine query.
            LazyFrameHandle joined = Join(table, candidates, sourceRows, snapshot.Schema, sourceSchema, schema, keys);
            ExprHandle action = ActionOf(clauses);
            LazyFrameHandle changed = PolarsWrapper.LazyFilter(
                PolarsWrapper.LazyWithColumns(joined, [PolarsWrapper.Alias(action, ActionColumn)]),
                PolarsWrapper.IsIn(PolarsWrapper.Col(ActionColumn), Ints(Insert, Update, Delete), nullsEqual: false));
            using DataFrameHandle rows = PolarsWrapper.LazyCollect(changed, PlEngine.Auto);

            var plan = new DmlPlan(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            var counts = Apply(table, candidates, rows, schema, sourceSchema, plan);

            long sourceCount = PolarsWrapper.DataFrameHeight(sourceRows);
            if (plan.IsEmpty && newMetadata is null)
                return new DeltaMergeResult(null, sourceCount, 0, 0, 0, 0, 0, 0, 0, 0, false);

            plan.AddTo(transaction);
            transaction.WithMetric("numSourceRows", sourceCount)
                       .WithMetric("numTargetRowsInserted", counts.Inserted)
                       .WithMetric("numTargetRowsUpdated", counts.Updated)
                       .WithMetric("numTargetRowsDeleted", counts.Deleted)
                       .WithMetric("numTargetRowsCopied", plan.CopiedRows)
                       .WithMetric("numOutputRows", counts.Inserted + counts.Updated + plan.CopiedRows)
                       .WithMetric("numTargetFilesAdded", plan.Adds.Count)
                       .WithMetric("numTargetFilesRemoved", plan.Removes.Count)
                       .WithMetric("numTargetChangeFilesAdded", plan.ChangeFiles.Count)
                       .WithMetric("numTargetDeletionVectorsAdded", plan.VectorsAdded);
            long version = transaction.Commit();
            DeltaKernelWrite.TryCheckpoint(table.FileSystem, tablePath, version, cloud);
            return new DeltaMergeResult(
                version, sourceCount, counts.Inserted, counts.Updated, counts.Deleted, plan.CopiedRows,
                plan.Removes.Count, plan.Adds.Count, plan.VectorsAdded, plan.ChangeFiles.Count, newMetadata is not null);
        }
        catch (DeltaException e)
        {
            throw new PolarsException(e.Message, e);
        }
        finally
        {
            source?.Dispose();
            foreach (ExprHandle condition in owned) condition.Dispose();
        }
    }

    // ------------------------------------------------------------------
    // Schema
    // ------------------------------------------------------------------

    /// <summary>
    /// The schema the merge writes, by the rules of a write (<see cref="SchemaEvolution"/>): with
    /// <paramref name="canEvolve"/> the source's new columns are added (under column mapping too) and, with type
    /// widening on, column types widened. A source column whose type does not fit is cast to the table's as the
    /// rows are merged (a value that does not fit fails). Checks the keys.
    /// </summary>
    private static StructType Evolve(Snapshot snapshot, StructType source, string[] keys, bool canEvolve, out MetadataAction? metadata)
    {
        StructType table = snapshot.Schema;
        foreach (string key in keys)
        {
            if (table.Field(key) is null) throw new PolarsException($"The merge key '{key}' is not a column of the table.");
            if (source.Field(key) is null) throw new PolarsException($"The merge key '{key}' is not a column of the source.");
        }
        // Shared scalar columns that would not widen are taken in the table's type: the merge casts them. Nested
        // columns are merged field by field (new nested fields added with evolution).
        bool widening = canEvolve && SchemaEvolution.WideningEnabled(snapshot.Metadata, snapshot.Protocol);
        var adjusted = new StructType([.. source.Fields.Select(f =>
            table.Field(f.Name) is StructField t && t.Type is PrimitiveType or DecimalType
                && !(widening && SchemaEvolution.CanWiden(t.Type, f.Type))
                ? new StructField { Name = f.Name, Type = t.Type, Nullable = true }
                : f)]);
        try
        {
            return SchemaEvolution.Merge(snapshot.Metadata, snapshot.Protocol, adjusted, canEvolve, out metadata, allowMissingColumns: true);
        }
        catch (DeltaException e) when (e.Message.Contains("Allow schema evolution", StringComparison.Ordinal))
        {
            throw new PolarsException(e.Message.Replace("The data has", "The source has", StringComparison.Ordinal), e);
        }
    }

    // ------------------------------------------------------------------
    // Pruning
    // ------------------------------------------------------------------

    /// <summary>
    /// A predicate every matching target row satisfies: each key between the source's smallest and largest value.
    /// Null when the source has no key values, or no key is of a type the range can be taken over.
    /// </summary>
    private static Predicate? KeyRange(DataFrameHandle sourceRows, Snapshot snapshot, string[] keys)
    {
        var usable = keys.Where(k => snapshot.Schema.Field(k)!.Type is PrimitiveType
        {
            Name: PrimitiveType.Byte or PrimitiveType.Short or PrimitiveType.Integer or PrimitiveType.Long or PrimitiveType.String or PrimitiveType.Date
        }).ToArray();
        if (usable.Length == 0 || PolarsWrapper.DataFrameHeight(sourceRows) == 0) return null;

        var exprs = new List<ExprHandle>();
        foreach (string key in usable)
        {
            exprs.Add(PolarsWrapper.Alias(PolarsWrapper.Min(PolarsWrapper.Col(key)), "lo:" + key));
            exprs.Add(PolarsWrapper.Alias(PolarsWrapper.Max(PolarsWrapper.Col(key)), "hi:" + key));
        }
        using JsonDocument row = DeltaKernelWrite.OneRow(sourceRows, [.. exprs]);
        var parts = new List<Predicate>();
        foreach (string key in usable)
        {
            JsonElement lo = row.RootElement.GetProperty("lo:" + key), hi = row.RootElement.GetProperty("hi:" + key);
            if (lo.ValueKind == JsonValueKind.Null || hi.ValueKind == JsonValueKind.Null) continue; // only null keys here: they match nothing
            var type = (PrimitiveType)snapshot.Schema.Field(key)!.Type;
            object Value(JsonElement v) => type.Name switch
            {
                PrimitiveType.String => v.GetString()!,
                PrimitiveType.Date => DateOnly.ParseExact(v.GetString()!, "yyyy-MM-dd", CultureInfo.InvariantCulture),
                _ => v.GetInt64(),
            };
            var column = new ColumnPath(key);
            parts.Add(Predicate.Ge(column, Value(lo)));
            parts.Add(Predicate.Le(column, Value(hi)));
        }
        return parts.Count == 0 ? null : Predicate.And([.. parts]);
    }

    // ------------------------------------------------------------------
    // Planning in the engine
    // ------------------------------------------------------------------

    /// <summary>
    /// The full join of the candidate files' rows (table columns, row index, file index, a target marker) and the
    /// source (columns suffixed, cast to the table's types, a source marker) on the keys. Null keys do not match.
    /// </summary>
    private static LazyFrameHandle Join(
        DmlTable table, IReadOnlyList<AddAction> candidates, DataFrameHandle sourceRows, StructType tableSchema, StructType sourceSchema,
        StructType writtenSchema, string[] keys)
    {
        var frames = new List<LazyFrameHandle>(candidates.Count);
        for (int i = 0; i < candidates.Count; i++)
        {
            LazyFrameHandle lf = table.ScanFile(candidates[i], withRowIndex: true);
            frames.Add(PolarsWrapper.LazyWithColumns(lf, [PolarsWrapper.Alias(PolarsWrapper.Lit(i), FileColumn)]));
        }
        LazyFrameHandle target = DmlTable.Concat(frames) ?? EmptyTarget(tableSchema);
        target = PolarsWrapper.LazyWithColumns(target, [PolarsWrapper.Alias(PolarsWrapper.Lit(true), InTarget)]);

        var columns = new List<ExprHandle>();
        foreach (StructField field in sourceSchema.Fields)
        {
            // Every source column takes the type it is written as (a value that does not fit fails).
            DeltaType type = writtenSchema.Field(field.Name)!.Type;
            using DataTypeHandle engineType = DeltaKernelScan.EngineType(type);
            columns.Add(PolarsWrapper.Alias(
                PolarsWrapper.ExprCast(PolarsWrapper.Col(field.Name), PolarsWrapper.DataTypeExprFromDataType(engineType), strict: true, wrapNumerical: false),
                field.Name + SourceSuffix));
        }
        columns.Add(PolarsWrapper.Alias(PolarsWrapper.Lit(true), InSource));
        LazyFrameHandle sourceLf = PolarsWrapper.LazySelect(PolarsWrapper.DataFrameToLazy(PolarsWrapper.CloneDataFrame(sourceRows)), [.. columns]);

        return PolarsWrapper.Join(
            target, sourceLf,
            [.. keys.Select(PolarsWrapper.Col)], [.. keys.Select(k => PolarsWrapper.Col(k + SourceSuffix))],
            PlJoinType.Outer, suffix: null, PlJoinValidation.ManyToMany, PlJoinCoalesce.KeepColumns, PlJoinMaintainOrder.None,
            PlJoinSide.None, nullsEqual: false, sliceOffset: null, sliceLen: 0);
    }

    /// <summary>No target rows, with the columns a scanned file would have.</summary>
    private static LazyFrameHandle EmptyTarget(StructType schema)
    {
        SchemaHandle handle = PolarsWrapper.SchemaCreate();
        try
        {
            foreach (StructField field in schema.Fields)
            {
                using DataTypeHandle type = DeltaKernelScan.EngineType(field.Type);
                PolarsWrapper.SchemaAddField(handle, field.Name, type);
            }
            using DataTypeHandle int64 = PolarsWrapper.NewPrimitiveType((int)PlDataType.Int64);
            using DataTypeHandle int32 = PolarsWrapper.NewPrimitiveType((int)PlDataType.Int32);
            PolarsWrapper.SchemaAddField(handle, DeltaKernelScan.RowIndexColumn, int64);
            PolarsWrapper.SchemaAddField(handle, FileColumn, int32);
            using DataFrameHandle empty = PolarsWrapper.DataFrameFromSchema(handle, 0);
            return PolarsWrapper.DataFrameToLazy(PolarsWrapper.CloneDataFrame(empty));
        }
        finally
        {
            handle.Dispose();
        }
    }

    /// <summary>The action of each joined row: the first clause that takes it, in order.</summary>
    private static ExprHandle ActionOf(IReadOnlyList<MergeClause> clauses)
    {
        ExprHandle inTarget() => PolarsWrapper.IsNotNull(PolarsWrapper.Col(InTarget));
        ExprHandle inSource() => PolarsWrapper.IsNotNull(PolarsWrapper.Col(InSource));
        ExprHandle matched() => PolarsWrapper.And(inTarget(), inSource());
        ExprHandle sourceOnly() => PolarsWrapper.And(inSource(), PolarsWrapper.Not(inTarget()));
        ExprHandle targetOnly() => PolarsWrapper.And(inTarget(), PolarsWrapper.Not(inSource()));

        // No clause: a source row is not inserted, a target row stays.
        ExprHandle action = PolarsWrapper.IfElse(sourceOnly(), PolarsWrapper.Lit(Ignore), PolarsWrapper.Lit(Keep));
        for (int i = clauses.Count - 1; i >= 0; i--)
        {
            var (state, result) = clauses[i].Kind switch
            {
                MergeClauseKind.MatchedUpdate => (matched(), Update),
                MergeClauseKind.MatchedDelete => (matched(), Delete),
                MergeClauseKind.NotMatchedInsert => (sourceOnly(), Insert),
                _ => (targetOnly(), Delete),
            };
            // A condition that is null does not take the row.
            ExprHandle condition = PolarsWrapper.FillNull(PolarsWrapper.CloneExpr(clauses[i].Condition), PolarsWrapper.Lit(false));
            action = PolarsWrapper.IfElse(PolarsWrapper.And(state, condition), PolarsWrapper.Lit(result), action);
        }
        return action;
    }

    // ------------------------------------------------------------------
    // Applying the plan
    // ------------------------------------------------------------------

    private readonly record struct Counts(long Inserted, long Updated, long Deleted);

    private static Counts Apply(
        DmlTable table, IReadOnlyList<AddAction> candidates, DataFrameHandle rows, StructType schema, StructType sourceSchema, DmlPlan plan)
    {
        if (PolarsWrapper.DataFrameHeight(rows) == 0) return default;
        Snapshot snapshot = table.Snapshot;
        LazyFrameHandle Rows() => PolarsWrapper.DataFrameToLazy(PolarsWrapper.CloneDataFrame(rows));
        ExprHandle Is(params int[] actions) => PolarsWrapper.IsIn(PolarsWrapper.Col(ActionColumn), Ints(actions), nullsEqual: false);

        // Target rows replaced or deleted, per candidate file.
        Dictionary<int, long[]> hits = TouchedRows(PolarsWrapper.LazyFilter(Rows(), Is(Update, Delete)));
        long updated = Count(rows, Update), deleted = Count(rows, Delete), inserted = Count(rows, Insert);

        var written = new List<LazyFrameHandle>();
        var changes = new List<LazyFrameHandle>();
        var marked = new List<(AddAction File, long[] Rows)>();
        try
        {
            // Updated and inserted rows: the source's values, the target's for columns the source lacks.
            LazyFrameHandle newRows = PolarsWrapper.LazySelect(PolarsWrapper.LazyFilter(Rows(), Is(Insert, Update)),
                [.. NewRowColumns(schema, snapshot.Schema, sourceSchema), PolarsWrapper.Col(ActionColumn)]);
            written.Add(PolarsWrapper.LazySelect(PolarsWrapper.LazyClone(newRows), [.. schema.Fields.Select(f => PolarsWrapper.Col(f.Name))]));

            if (table.ChangeFeedEnabled)
            {
                changes.Add(ChangeRows(PolarsWrapper.LazyFilter(Rows(), Is(Update)), schema, snapshot.Schema, "update_preimage"));
                changes.Add(ChangeRows(PolarsWrapper.LazyFilter(Rows(), Is(Delete)), schema, snapshot.Schema, "delete"));
                changes.Add(DmlTable.WithChangeType(Project(PolarsWrapper.LazyFilter(PolarsWrapper.LazyClone(newRows), Is(Update)), schema), "update_postimage"));
                changes.Add(DmlTable.WithChangeType(Project(PolarsWrapper.LazyFilter(PolarsWrapper.LazyClone(newRows), Is(Insert)), schema), "insert"));
            }
            newRows.Dispose();

            foreach (var (index, touched) in hits.OrderBy(h => h.Key))
            {
                AddAction file = candidates[index];
                long live = table.LiveRows(file);
                if (touched.Length >= live)
                {
                    plan.Remove(file);
                }
                else if (table.DeletionVectorsEnabled)
                {
                    marked.Add((file, touched));
                }
                else
                {
                    LazyFrameHandle kept = table.ScanFile(file, dropRows: touched);
                    written.Add(Conform(kept, schema, snapshot.Schema));
                    plan.CopiedRows += live - touched.Length;
                    plan.Remove(file);
                    plan.Rewritten++;
                }
            }

            plan.MarkDeleted(table, marked);
            if (written.Count > 0 && (inserted > 0 || updated > 0 || plan.Rewritten > 0))
            {
                table.EnsureFilesWritable();
                using DataFrameHandle df = PolarsWrapper.LazyCollect(DmlTable.Concat(written)!, PlEngine.Auto);
                DeltaKernelWrite.EnsureNotNull(df, schema);
                plan.Adds.AddRange(DeltaKernelWrite.WriteFiles(table.FileSystem, df, schema, table.PartitionColumns, plan.Now, table.Snapshot.Metadata.Configuration));
            }

            if (changes.Count > 0)
            {
                using DataFrameHandle df = PolarsWrapper.LazyCollect(DmlTable.Concat(changes)!, PlEngine.Auto);
                plan.ChangeFiles.AddRange(DeltaKernelWrite.WriteChangeFiles(table.FileSystem, df, schema, table.PartitionColumns));
            }
            return new Counts(inserted, updated, deleted);
        }
        finally
        {
            foreach (LazyFrameHandle frame in written.Concat(changes)) frame.Dispose();
        }
    }

    /// <summary>The touched target rows per file; fails when a target row is matched by more than one source row.</summary>
    private static Dictionary<int, long[]> TouchedRows(LazyFrameHandle rows)
    {
        using DataFrameHandle df = PolarsWrapper.LazyCollect(
            PolarsWrapper.LazySelect(rows, [PolarsWrapper.Col(FileColumn), PolarsWrapper.Col(DeltaKernelScan.RowIndexColumn)]), PlEngine.Auto);
        var result = new Dictionary<int, long[]>();
        if (PolarsWrapper.DataFrameHeight(df) == 0) return result;
        int[] files = DmlTable.Int32Column(df, FileColumn);
        long[] positions = DmlTable.Int64Column(df, DeltaKernelScan.RowIndexColumn);
        var byFile = new Dictionary<int, List<long>>();
        for (int k = 0; k < files.Length; k++)
        {
            if (!byFile.TryGetValue(files[k], out List<long>? list)) byFile[files[k]] = list = [];
            list.Add(positions[k]);
        }
        foreach (var (file, list) in byFile)
        {
            DeletionVector distinct = DeletionVector.FromRows(list);
            if (distinct.Cardinality != list.Count)
                throw new PolarsException(
                    "The merge matched a target row with more than one source row and would update or delete it more than once. " +
                    "Make the source's merge keys unique among the rows that match.");
            result[file] = distinct.DeletedRows;
        }
        return result;
    }

    private static long Count(DataFrameHandle rows, int action)
    {
        LazyFrameHandle lf = PolarsWrapper.LazySelect(
            PolarsWrapper.LazyFilter(PolarsWrapper.DataFrameToLazy(PolarsWrapper.CloneDataFrame(rows)),
                PolarsWrapper.Eq(PolarsWrapper.Col(ActionColumn), PolarsWrapper.Lit(action))),
            [PolarsWrapper.Alias(PolarsWrapper.Len(), "n")]);
        using DataFrameHandle df = PolarsWrapper.LazyCollect(lf, PlEngine.Auto);
        using JsonDocument row = DeltaKernelWrite.OneRow(df, [PolarsWrapper.Col("n")]);
        return row.RootElement.GetProperty("n").GetInt64();
    }

    /// <summary>The columns of updated and inserted rows over the written schema.</summary>
    private static IEnumerable<ExprHandle> NewRowColumns(StructType schema, StructType tableSchema, StructType sourceSchema)
    {
        foreach (StructField field in schema.Fields)
        {
            bool inSource = sourceSchema.Field(field.Name) is not null;
            bool inTable = tableSchema.Field(field.Name) is not null;
            ExprHandle value;
            if (inSource)
            {
                value = PolarsWrapper.Col(field.Name + SourceSuffix);
            }
            else
            {
                // Not in the source: an update keeps the target's value, an insert has none.
                ExprHandle kept = inTable ? PolarsWrapper.Col(field.Name) : PolarsWrapper.LitNull();
                value = PolarsWrapper.IfElse(
                    PolarsWrapper.Eq(PolarsWrapper.Col(ActionColumn), PolarsWrapper.Lit(Insert)), PolarsWrapper.LitNull(), kept);
            }
            using DataTypeHandle type = DeltaKernelScan.EngineType(field.Type);
            yield return PolarsWrapper.Alias(
                PolarsWrapper.ExprCast(value, PolarsWrapper.DataTypeExprFromDataType(type), strict: true, wrapNumerical: false), field.Name);
        }
    }

    /// <summary>Target rows (before the merge) over the written schema, with a change type.</summary>
    private static LazyFrameHandle ChangeRows(LazyFrameHandle rows, StructType schema, StructType tableSchema, string changeType) =>
        DmlTable.WithChangeType(Conform(rows, schema, tableSchema), changeType);

    /// <summary>Table rows read under the table's schema, over the written one: widened columns cast, added columns null.</summary>
    private static LazyFrameHandle Conform(LazyFrameHandle rows, StructType schema, StructType tableSchema) =>
        PolarsWrapper.LazySelect(rows, [.. schema.Fields.Select(field =>
        {
            // Columns of the table in the type written (it may have been widened); added ones null.
            using DataTypeHandle type = DeltaKernelScan.EngineType(field.Type);
            ExprHandle value = tableSchema.Field(field.Name) is not null ? PolarsWrapper.Col(field.Name) : PolarsWrapper.LitNull();
            return PolarsWrapper.Alias(
                PolarsWrapper.ExprCast(value, PolarsWrapper.DataTypeExprFromDataType(type), strict: true, wrapNumerical: false), field.Name);
        })]);

    private static LazyFrameHandle Project(LazyFrameHandle rows, StructType schema) =>
        PolarsWrapper.LazySelect(rows, [.. schema.Fields.Select(f => PolarsWrapper.Col(f.Name))]);

    private static ExprHandle Ints(params int[] values) =>
        PolarsWrapper.Implode(PolarsWrapper.Lit(PolarsWrapper.SeriesNew("actions", values)), maintainOrder: false);

    // ------------------------------------------------------------------
    // Commit info
    // ------------------------------------------------------------------

    /// <summary>The operation parameters Spark records for a merge.</summary>
    private static void RecordParameters(Transaction transaction, string[] keys, IReadOnlyList<MergeClause> clauses)
    {
        transaction.WithParameter("predicate", JsonText.StringArray([string.Join(" AND ", keys.Select(k => $"source.{k} = target.{k}"))]));
        string Clauses(Func<MergeClauseKind, bool> which) => JsonText.ObjectArray(clauses.Where(c => which(c.Kind)).Select(c => new Dictionary<string, string>
        {
            ["predicate"] = PolarsWrapper.ExprToString(c.Condition),
            ["actionType"] = c.Kind is MergeClauseKind.MatchedUpdate ? "update" : c.Kind is MergeClauseKind.NotMatchedInsert ? "insert" : "delete",
        }));
        transaction.WithParameter("matchedPredicates", Clauses(k => k is MergeClauseKind.MatchedUpdate or MergeClauseKind.MatchedDelete))
                   .WithParameter("notMatchedPredicates", Clauses(k => k is MergeClauseKind.NotMatchedInsert))
                   .WithParameter("notMatchedBySourcePredicates", Clauses(k => k is MergeClauseKind.NotMatchedBySourceDelete));
    }
}
