using Kaminari.Seseragi.Lakehouse.Delta;
using Polars.NET.Core.Exceptions;

namespace Polars.NET.Core.Delta;

/// <summary>
/// Table properties and table features of a Delta table through the managed kernel (see
/// <see cref="AlterTable"/>). Not reachable from the public API yet.
/// </summary>
internal static class DeltaKernelAlter
{
    /// <summary>Sets table properties; returns the version committed, or null when nothing changes.</summary>
    /// <param name="tablePath">The table location: a local directory or a cloud URL.</param>
    /// <param name="properties">The properties to set.</param>
    /// <param name="raiseIfNotExists">Refuse a <c>delta.</c> property Delta does not define.</param>
    /// <param name="cloud">Cloud settings for a cloud URL.</param>
    public static long? SetTableProperties(
        string tablePath, IReadOnlyDictionary<string, string> properties, bool raiseIfNotExists = false, CloudArgs? cloud = null)
    {
        ArgumentNullException.ThrowIfNull(properties);
        if (properties.Count == 0) throw new ArgumentException("No property to set.", nameof(properties));
        return Run(tablePath, cloud, table => AlterTable.SetProperties(table.FileSystem, table.Snapshot, properties, raiseIfNotExists));
    }

    /// <summary>
    /// Adds a table feature (deletion vectors, the change data feed and append-only are turned on too); returns the
    /// version committed, or null when the table has it already.
    /// </summary>
    /// <param name="tablePath">The table location: a local directory or a cloud URL.</param>
    /// <param name="feature">The feature name.</param>
    /// <param name="allowProtocolIncrease">Allow raising the table's reader or writer version.</param>
    /// <param name="cloud">Cloud settings for a cloud URL.</param>
    public static long? AddFeature(string tablePath, string feature, bool allowProtocolIncrease = true, CloudArgs? cloud = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(feature);
        return Run(tablePath, cloud, table => AlterTable.AddFeatures(table.FileSystem, table.Snapshot, [feature], allowProtocolIncrease));
    }

    private static long? Run(string tablePath, CloudArgs? cloud, Func<DmlTable, long?> alter)
    {
        try
        {
            using DmlTable table = DmlTable.Open(tablePath, cloud);
            long? version = alter(table);
            if (version is long committed) DeltaKernelWrite.TryCheckpoint(table.FileSystem, tablePath, committed, cloud);
            return version;
        }
        catch (DeltaException e)
        {
            throw new PolarsException(e.Message, e);
        }
    }
}
