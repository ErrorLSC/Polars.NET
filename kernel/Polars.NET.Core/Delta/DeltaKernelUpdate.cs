using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Lakehouse.Skipping;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Exceptions;
using Polars.NET.Core.Wrappers;

namespace Polars.NET.Core.Delta;

/// <summary>What an update did.</summary>
/// <param name="Version">The version committed; null when no row matched and nothing was committed.</param>
/// <param name="UpdatedRows">Rows updated.</param>
/// <param name="CopiedRows">Rows written again unchanged, because their file was rewritten.</param>
/// <param name="RemovedFiles">Data files removed, including those given a new deletion vector.</param>
/// <param name="AddedFiles">Data files added: updated rows, rewrites, and files re-added with a deletion vector.</param>
/// <param name="DeletionVectorsAdded">Files whose old rows were marked in a deletion vector.</param>
/// <param name="RewrittenFiles">Files rewritten whole with the updated rows in place.</param>
/// <param name="ChangeFiles">Change data files written for the change data feed.</param>
internal sealed record DeltaUpdateResult(
    long? Version, long UpdatedRows, long CopiedRows, int RemovedFiles, int AddedFiles, int DeletionVectorsAdded, int RewrittenFiles,
    int ChangeFiles);

/// <summary>
/// Updates rows of a Delta table through the managed kernel. The engine finds the matching rows file by file. With
/// deletion vectors on, the old rows are marked deleted and the updated rows written to new files; otherwise each
/// file with a match is rewritten with the updated rows in place. On a table with the change data feed, the rows
/// before and after are written as change data (update_preimage, update_postimage). Not reachable from the public
/// API yet.
/// </summary>
internal static class DeltaKernelUpdate
{
    /// <summary>Sets columns of the rows matching <paramref name="predicate"/>; with no predicate, of every row.</summary>
    /// <param name="tablePath">The table location: a local directory or a cloud URL.</param>
    /// <param name="predicate">The rows to update, over the table's logical columns; consumed. A row for which it is
    /// null is not updated.</param>
    /// <param name="assignments">Column name and new value, over the row before the update; consumed. Each value is
    /// cast to the column's type; a value that does not fit fails the update.</param>
    /// <param name="fileFilter">Optional: a predicate every updated row satisfies, to leave out files without one.</param>
    /// <param name="cloud">Cloud settings for a cloud URL.</param>
    public static DeltaUpdateResult Update(
        string tablePath, ExprHandle? predicate, IReadOnlyList<(string Column, ExprHandle Value)> assignments,
        Predicate? fileFilter = null, CloudArgs? cloud = null)
    {
        try
        {
            if (assignments.Count == 0) throw new ArgumentException("An update sets at least one column.", nameof(assignments));
            if (predicate is null && fileFilter is not null)
                throw new ArgumentException("A file filter narrows a predicate; without a predicate every row is updated.", nameof(fileFilter));
            var duplicate = assignments.GroupBy(a => a.Column, StringComparer.Ordinal).FirstOrDefault(g => g.Count() > 1);
            if (duplicate is not null) throw new ArgumentException($"The column '{duplicate.Key}' is set twice.", nameof(assignments));

            using DmlTable table = DmlTable.Open(tablePath, cloud);
            Snapshot snapshot = table.Snapshot;
            foreach (var (column, _) in assignments)
            {
                if (snapshot.Schema.Field(column) is null)
                    throw new PolarsException($"The table has no column '{column}'.");
            }
            table.EnsureFilesWritable();

            IReadOnlyList<AddAction> candidates = fileFilter is null ? snapshot.Files : snapshot.FilesMatching(fileFilter);
            var transaction = Transaction.Start(table.FileSystem, snapshot, "UPDATE");
            transaction.ReadPredicate(fileFilter).ReadFiles(candidates);
            string predicateText = predicate is null ? "true" : PolarsWrapper.ExprToString(predicate);
            transaction.WithParameter("predicate", JsonText.StringArray([predicateText]));

            // Every live row matches without a predicate.
            ExprHandle matchAll = predicate ?? PolarsWrapper.Lit(true);
            predicate = null;
            Dictionary<int, long[]> hits = table.MatchingRows(candidates, matchAll);
            long updated = hits.Values.Sum(r => (long)r.Length);
            if (updated == 0) return new DeltaUpdateResult(null, 0, 0, 0, 0, 0, 0, 0);

            var plan = new DmlPlan(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            Apply(table, candidates, hits, assignments, plan);
            plan.AddTo(transaction);
            transaction.WithMetric("numUpdatedRows", updated);

            long version = transaction.Commit();
            DeltaKernelWrite.TryCheckpoint(table.FileSystem, tablePath, version, cloud);
            return new DeltaUpdateResult(
                version, updated, plan.CopiedRows, plan.Removes.Count, plan.Adds.Count, plan.VectorsAdded, plan.Rewritten, plan.ChangeFiles.Count);
        }
        catch (DeltaException e)
        {
            throw new PolarsException(e.Message, e);
        }
        finally
        {
            predicate?.Dispose();
            foreach (var (_, value) in assignments) value.Dispose();
        }
    }

    private static void Apply(
        DmlTable table, IReadOnlyList<AddAction> candidates, Dictionary<int, long[]> hits,
        IReadOnlyList<(string Column, ExprHandle Value)> assignments, DmlPlan plan)
    {
        Snapshot snapshot = table.Snapshot;
        bool useVectors = table.DeletionVectorsEnabled;
        var marked = new List<(AddAction File, long[] Rows)>();
        var written = new List<LazyFrameHandle>();   // rows of new data files
        var preimages = new List<LazyFrameHandle>();
        var postimages = new List<LazyFrameHandle>();
        try
        {
            foreach (var (index, rows) in hits.OrderBy(h => h.Key))
            {
                AddAction file = candidates[index];
                if (table.ChangeFeedEnabled)
                {
                    preimages.Add(table.ScanFile(file, onlyRows: rows));
                    postimages.Add(Assign(table.ScanFile(file, onlyRows: rows), snapshot.Schema, assignments, onlyListed: null));
                }

                if (useVectors && rows.Length < table.LiveRows(file))
                {
                    // The old rows are deleted; the updated ones go to a new file.
                    marked.Add((file, rows));
                    written.Add(Assign(table.ScanFile(file, onlyRows: rows), snapshot.Schema, assignments, onlyListed: null));
                }
                else
                {
                    // The whole file again, the matching rows updated in place.
                    long live = table.LiveRows(file);
                    plan.CopiedRows += live - rows.Length;
                    LazyFrameHandle all = table.ScanFile(file, withRowIndex: true);
                    written.Add(Assign(all, snapshot.Schema, assignments, onlyListed: rows));
                    plan.Remove(file);
                    plan.Rewritten++;
                }
            }

            plan.MarkDeleted(table, marked);

            LazyFrameHandle? newRows = DmlTable.Concat(written);
            if (newRows is not null)
            {
                using DataFrameHandle df = PolarsWrapper.LazyCollect(newRows, PlEngine.Auto);
                DeltaKernelWrite.EnsureNotNull(df, snapshot.Schema);
                plan.Adds.AddRange(DeltaKernelWrite.WriteFiles(table.FileSystem, df, snapshot.Schema, table.PartitionColumns, plan.Now));
            }

            if (preimages.Count > 0)
            {
                var changes = new List<LazyFrameHandle>
                {
                    DmlTable.WithChangeType(DmlTable.Concat(preimages)!, "update_preimage"),
                    DmlTable.WithChangeType(DmlTable.Concat(postimages)!, "update_postimage"),
                };
                using DataFrameHandle df = PolarsWrapper.LazyCollect(DmlTable.Concat(changes)!, PlEngine.Auto);
                plan.ChangeFiles.AddRange(DeltaKernelWrite.WriteChangeFiles(table.FileSystem, df, snapshot.Schema, table.PartitionColumns));
            }
        }
        finally
        {
            foreach (LazyFrameHandle frame in written.Concat(preimages).Concat(postimages)) frame.Dispose();
        }
    }

    /// <summary>
    /// The table's columns of <paramref name="rows"/>, the assigned ones set to their new values (cast to the
    /// column type). With <paramref name="onlyListed"/>, only the rows at those positions are changed; the frame
    /// must then carry the row index column.
    /// </summary>
    private static LazyFrameHandle Assign(
        LazyFrameHandle rows, StructType schema, IReadOnlyList<(string Column, ExprHandle Value)> assignments, long[]? onlyListed)
    {
        var exprs = new ExprHandle[schema.Fields.Count];
        for (int i = 0; i < exprs.Length; i++)
        {
            StructField field = schema.Fields[i];
            int a = Index(assignments, field.Name);
            if (a < 0)
            {
                exprs[i] = PolarsWrapper.Col(field.Name);
                continue;
            }
            using DataTypeHandle type = DeltaKernelScan.EngineType(field.Type);
            ExprHandle value = PolarsWrapper.ExprCast(
                PolarsWrapper.CloneExpr(assignments[a].Value), PolarsWrapper.DataTypeExprFromDataType(type), strict: true, wrapNumerical: false);
            if (onlyListed is not null)
                value = PolarsWrapper.IfElse(DmlTable.RowIsListed(onlyListed), value, PolarsWrapper.Col(field.Name));
            exprs[i] = PolarsWrapper.Alias(value, field.Name);
        }
        return PolarsWrapper.LazySelect(rows, exprs);

        static int Index(IReadOnlyList<(string Column, ExprHandle Value)> list, string name)
        {
            for (int i = 0; i < list.Count; i++)
            {
                if (list[i].Column == name) return i;
            }
            return -1;
        }
    }
}
