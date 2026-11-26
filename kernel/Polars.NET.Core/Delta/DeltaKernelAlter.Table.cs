using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Polars.NET.Core.Exceptions;
using Polars.NET.Core.Wrappers;

namespace Polars.NET.Core.Delta;

/// <summary>A column to add (<see cref="DeltaKernelAlter.AddColumns"/>): its type as the engine gives it.</summary>
internal sealed record KernelNewColumn(string Column, DataTypeHandle Type, string? Comment, string? After, bool First);

internal static partial class DeltaKernelAlter
{
    /// <summary>Adds columns (Spark's <c>ALTER TABLE … ADD COLUMNS</c>); returns the version committed.</summary>
    /// <param name="tablePath">The table location: a local directory or a cloud URL.</param>
    /// <param name="columns">The columns, dotted for nested fields; their types are borrowed.</param>
    /// <param name="cloud">Cloud settings for a cloud URL.</param>
    public static long AddColumns(string tablePath, IReadOnlyList<KernelNewColumn> columns, CloudArgs? cloud = null)
    {
        ArgumentNullException.ThrowIfNull(columns);
        // Types as data of these types would be written (the engine's Int64 is Delta's long, and so on).
        NewColumn[] added =
        [
            .. columns.Select(c => new NewColumn(c.Column, DeltaKernelWrite.DeltaTypeOf(c.Type, c.Column), c.Comment, c.After, c.First)),
        ];
        return Run(tablePath, cloud, table => AlterTable.AddColumns(table.FileSystem, table.Snapshot, added))!.Value;
    }

    /// <summary>Removes table properties (Spark's <c>ALTER TABLE … UNSET TBLPROPERTIES</c>); returns the version
    /// committed, or null when nothing is removed.</summary>
    /// <param name="tablePath">The table location: a local directory or a cloud URL.</param>
    /// <param name="keys">The properties.</param>
    /// <param name="ifExists">Skip properties the table does not have.</param>
    /// <param name="cloud">Cloud settings for a cloud URL.</param>
    public static long? UnsetProperties(string tablePath, IReadOnlyList<string> keys, bool ifExists = false, CloudArgs? cloud = null)
    {
        ArgumentNullException.ThrowIfNull(keys);
        return Run(tablePath, cloud, table => AlterTable.UnsetProperties(table.FileSystem, table.Snapshot, keys, ifExists));
    }

    /// <summary>What Spark's <c>DESCRIBE DETAIL</c> tells about the table (read only: any table that can be read).</summary>
    /// <param name="tablePath">The table location: a local directory or a cloud URL.</param>
    /// <param name="cloud">Cloud settings for a cloud URL.</param>
    /// <exception cref="PolarsException">There is no table at the path.</exception>
    public static TableDetail Detail(string tablePath, CloudArgs? cloud = null)
    {
        var (fileSystem, _) = TableStorage.Open(tablePath, cloud);
        try
        {
            Snapshot table = DeltaKernelWrite.TryLoad(fileSystem, tablePath, cloud)
                ?? throw new PolarsException($"No Delta table at {tablePath}.");
            string location = ManagedStores.IsUrl(tablePath) ? tablePath.TrimEnd('/') : Path.GetFullPath(tablePath).TrimEnd(Path.DirectorySeparatorChar);
            return TableDetail.Of(fileSystem, table, location);
        }
        catch (DeltaException e)
        {
            throw new PolarsException(e.Message, e);
        }
        finally
        {
            (fileSystem as IDisposable)?.Dispose();
        }
    }
}
