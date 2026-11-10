using Kaminari.Seseragi.Lakehouse.Delta;
using Polars.NET.Core.Exceptions;

namespace Polars.NET.Core.Delta;

/// <summary>
/// Vacuums a Delta table through the managed kernel (see <see cref="Kaminari.Seseragi.Lakehouse.Delta.Vacuum"/>).
/// Not reachable from the public API yet.
/// </summary>
internal static class DeltaKernelVacuum
{
    /// <param name="tablePath">The table location: a local directory or a cloud URL.</param>
    /// <param name="retentionHours">How long removed files are kept; the table's retention when null.</param>
    /// <param name="enforceRetention">Refuse a retention shorter than the table's.</param>
    /// <param name="dryRun">Only report what would be deleted.</param>
    /// <param name="full">List the whole table, so files no commit references go too; else only removed files.</param>
    /// <param name="cloud">Cloud settings for a cloud URL.</param>
    public static VacuumResult Vacuum(
        string tablePath, double? retentionHours = null, bool enforceRetention = true, bool dryRun = false, bool full = false,
        CloudArgs? cloud = null)
    {
        if (retentionHours < 0) throw new ArgumentOutOfRangeException(nameof(retentionHours), "The retention cannot be negative.");
        try
        {
            using DmlTable table = DmlTable.Open(tablePath, cloud);
            return Kaminari.Seseragi.Lakehouse.Delta.Vacuum.Run(
                table.FileSystem, table.Snapshot,
                retentionHours is double hours ? TimeSpan.FromHours(hours) : null,
                enforceRetention, dryRun, full ? VacuumMode.Full : VacuumMode.Lite);
        }
        catch (DeltaException e)
        {
            throw new PolarsException(e.Message, e);
        }
    }
}
