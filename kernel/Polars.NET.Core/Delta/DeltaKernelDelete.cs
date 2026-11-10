using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Skipping;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Exceptions;
using Polars.NET.Core.Wrappers;

namespace Polars.NET.Core.Delta;

/// <summary>What a delete did.</summary>
/// <param name="Version">The version committed; null when no row matched and nothing was committed.</param>
/// <param name="DeletedRows">Rows deleted.</param>
/// <param name="RemovedFiles">Data files removed, including those replaced by a rewrite or given a new deletion vector.</param>
/// <param name="AddedFiles">Data files added: rewrites and files re-added with a deletion vector.</param>
/// <param name="DeletionVectorsAdded">Files whose deleted rows were recorded in a deletion vector.</param>
/// <param name="RewrittenFiles">Files rewritten without the deleted rows.</param>
/// <param name="ChangeFiles">Change data files written for the change data feed.</param>
internal sealed record DeltaDeleteResult(
    long? Version, long DeletedRows, int RemovedFiles, int AddedFiles, int DeletionVectorsAdded, int RewrittenFiles, int ChangeFiles);

/// <summary>
/// Deletes rows from a Delta table through the managed kernel. The engine finds the matching rows file by file; per
/// file the kernel removes it (every live row matched), marks the rows in a deletion vector (the table has them
/// enabled), or has the engine rewrite it without them. On a table with the change data feed, the deleted rows are
/// also written as change data. The public Delta API calls it.
/// </summary>
internal static class DeltaKernelDelete
{
    /// <summary>Deletes the rows matching <paramref name="predicate"/>; with no predicate, every row.</summary>
    /// <param name="tablePath">The table location: a local directory or a cloud URL.</param>
    /// <param name="predicate">The rows to delete, over the table's logical columns; consumed. A row for which it is
    /// null is kept. Null deletes every row.</param>
    /// <param name="fileFilter">Optional: a predicate that every deleted row satisfies, used to leave out files that
    /// hold none (and to narrow what counts as a conflicting concurrent write). It does not choose rows.</param>
    /// <param name="cloud">Cloud settings for a cloud URL.</param>
    public static DeltaDeleteResult Delete(string tablePath, ExprHandle? predicate, Predicate? fileFilter = null, CloudArgs? cloud = null)
    {
        // Retried as a whole on a commit conflict, each attempt with its own copy of the predicate.
        try
        {
            return ConflictRetry.Run(() => DeleteOnce(tablePath, predicate is null ? null : PolarsWrapper.CloneExpr(predicate), fileFilter, cloud));
        }
        finally
        {
            predicate?.Dispose();
        }
    }

    private static DeltaDeleteResult DeleteOnce(string tablePath, ExprHandle? predicate, Predicate? fileFilter, CloudArgs? cloud)
    {
        try
        {
            if (predicate is null && fileFilter is not null)
                throw new ArgumentException("A file filter narrows a predicate; without a predicate every row is deleted.", nameof(fileFilter));

            using DmlTable table = DmlTable.Open(tablePath, cloud);
            Snapshot snapshot = table.Snapshot;
            IReadOnlyList<AddAction> candidates = fileFilter is null ? snapshot.Files : snapshot.FilesMatching(fileFilter);

            var transaction = Transaction.Start(table.FileSystem, snapshot, "DELETE");
            transaction.ReadPredicate(fileFilter).ReadFiles(candidates);
            string predicateText = predicate is null ? "true" : PolarsWrapper.ExprToString(predicate);
            transaction.WithParameter("predicate", JsonText.StringArray([predicateText]));

            var plan = new DmlPlan(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            long deleted = 0;
            if (predicate is null)
            {
                // Every row: remove every file. The change data feed derives the deleted rows from the removes.
                foreach (AddAction file in candidates)
                {
                    plan.Remove(file);
                    deleted += table.LiveRows(file);
                }
            }
            else
            {
                Dictionary<int, long[]> hits = table.MatchingRows(candidates, predicate);
                predicate = null; // consumed
                deleted = hits.Values.Sum(r => (long)r.Length);
                PlanFiles(table, candidates, hits, plan);
            }

            if (plan.IsEmpty) return new DeltaDeleteResult(null, 0, 0, 0, 0, 0, 0);

            plan.AddTo(transaction);
            transaction.WithMetric("numDeletedRows", deleted);
            long version = transaction.Commit();
            DeltaKernelWrite.TryCheckpoint(table.FileSystem, tablePath, version, cloud);
            return new DeltaDeleteResult(
                version, deleted, plan.Removes.Count, plan.Adds.Count, plan.VectorsAdded, plan.Rewritten, plan.ChangeFiles.Count);
        }
        catch (DeltaException e)
        {
            throw new PolarsException(e.Message, e);
        }
        finally
        {
            predicate?.Dispose();
        }
    }

    private static void PlanFiles(DmlTable table, IReadOnlyList<AddAction> candidates, Dictionary<int, long[]> hits, DmlPlan plan)
    {
        Snapshot snapshot = table.Snapshot;
        bool useVectors = table.DeletionVectorsEnabled;
        var marked = new List<(AddAction File, long[] Rows)>();
        var deletedFrames = new List<LazyFrameHandle>();
        try
        {
            foreach (var (index, rows) in hits.OrderBy(h => h.Key))
            {
                AddAction file = candidates[index];
                if (table.ChangeFeedEnabled) deletedFrames.Add(table.ScanFile(file, onlyRows: rows));

                if (rows.Length >= table.LiveRows(file))
                {
                    plan.Remove(file);
                }
                else if (useVectors)
                {
                    marked.Add((file, rows));
                }
                else
                {
                    table.EnsureFilesWritable();
                    using DataFrameHandle kept = PolarsWrapper.LazyCollect(table.ScanFile(file, dropRows: rows), PlEngine.Auto);
                    plan.CopiedRows += PolarsWrapper.DataFrameHeight(kept);
                    plan.Remove(file);
                    plan.Adds.AddRange(DeltaKernelWrite.WriteFiles(table.FileSystem, kept, snapshot.Schema, table.PartitionColumns, plan.Now, table.Snapshot.Metadata.Configuration));
                    plan.Rewritten++;
                }
            }

            plan.MarkDeleted(table, marked);

            // On a table with the change data feed, every deleted row is written as change data.
            LazyFrameHandle? all = DmlTable.Concat(deletedFrames);
            if (all is not null)
            {
                using DataFrameHandle df = PolarsWrapper.LazyCollect(DmlTable.WithChangeType(all, "delete"), PlEngine.Auto);
                plan.ChangeFiles.AddRange(DeltaKernelWrite.WriteChangeFiles(table.FileSystem, df, snapshot.Schema, table.PartitionColumns));
            }
        }
        finally
        {
            foreach (LazyFrameHandle frame in deletedFrames) frame.Dispose();
        }
    }
}
