using Kaminari.Samidare;
using Kaminari.Samidare.IR;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Exceptions;
using Polars.NET.Core.Linq.Backend;
using Polars.NET.Core.Wrappers;

namespace Polars.NET.Core.Delta;

internal static partial class DeltaKernelAlter
{
    /// <summary>
    /// Adds a CHECK constraint (Spark's <c>ALTER TABLE … ADD CONSTRAINT</c>); returns the version committed. The
    /// expression is bound as writes bind it, then the table's rows are checked: a row for which it is false or null
    /// refuses the constraint. The commit is made on the version checked; a concurrent commit makes it start again.
    /// </summary>
    /// <param name="tablePath">The table location: a local directory or a cloud URL.</param>
    /// <param name="name">The constraint's name, any case (kept lower-cased, as Spark).</param>
    /// <param name="expression">A Boolean Spark SQL expression over the table's columns.</param>
    /// <param name="cloud">Cloud settings for a cloud URL.</param>
    /// <exception cref="PolarsException">The name is taken or empty, the expression cannot be a constraint, or rows violate it.</exception>
    public static long AddConstraint(string tablePath, string name, string expression, CloudArgs? cloud = null)
    {
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(expression);
        string text = expression.Trim();
        return ConflictRetry.Run(() => Run(tablePath, cloud, table =>
        {
            Snapshot snapshot = table.Snapshot;
            // Refused before any row is read: an empty or taken name.
            string key = AlterTable.ConstraintKey(name);
            if (snapshot.Metadata.Configuration.Keys.Any(k => string.Equals(k, key, StringComparison.OrdinalIgnoreCase)))
                throw new DeltaException($"Constraint '{name.ToLowerInvariant()}' already exists. Please delete the old constraint first.");
            BoundSparkExpression bound = DeltaConstraintCheck.BindNew(snapshot.Schema, name, text);
            long violations = Violations(table, bound.Expr);
            if (violations > 0)
                throw new DeltaException($"{violations} rows in {tablePath} violate the new CHECK constraint ({text}).");
            return AlterTable.AddConstraint(table.FileSystem, snapshot, name, text);
        })!.Value);
    }

    /// <summary>Drops a CHECK constraint (Spark's <c>ALTER TABLE … DROP CONSTRAINT</c>); returns the version committed,
    /// or null when the table has none of that name and <paramref name="ifExists"/> is set.</summary>
    /// <param name="tablePath">The table location: a local directory or a cloud URL.</param>
    /// <param name="name">The constraint's name, any case.</param>
    /// <param name="ifExists">Skip a missing constraint instead of failing.</param>
    /// <param name="cloud">Cloud settings for a cloud URL.</param>
    public static long? DropConstraint(string tablePath, string name, bool ifExists = false, CloudArgs? cloud = null)
    {
        ArgumentNullException.ThrowIfNull(name);
        return Run(tablePath, cloud, table => AlterTable.DropConstraint(table.FileSystem, table.Snapshot, name, ifExists));
    }

    /// <summary>
    /// Sets a column's default (Spark's <c>ALTER COLUMN … SET DEFAULT</c>), checked as a default declared at
    /// <c>CREATE TABLE</c> is (see <see cref="DeltaColumnDefaults.Validate"/>); returns the version committed. The
    /// allowColumnDefaults feature comes into the protocol when the table has none.
    /// </summary>
    /// <param name="tablePath">The table location: a local directory or a cloud URL.</param>
    /// <param name="column">A top-level column, any case; not a generated or an identity column.</param>
    /// <param name="expression">The default: a constant Spark SQL expression.</param>
    /// <param name="cloud">Cloud settings for a cloud URL.</param>
    public static long SetColumnDefault(string tablePath, string column, string expression, CloudArgs? cloud = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(column);
        ArgumentNullException.ThrowIfNull(expression);
        string text = expression.Trim();
        return Run(tablePath, cloud, table =>
        {
            StructField field = AlterTable.DefaultableColumn(table.Snapshot.Schema, column);
            if (text.Length == 0) throw new DeltaException($"The default of the column {field.Name} has no expression.");
            DeltaColumnDefaults.Validate(table.Snapshot.Schema, field.Name, text);
            return AlterTable.SetColumnDefault(table.FileSystem, table.Snapshot, field.Name, text);
        })!.Value;
    }

    /// <summary>Drops a column's default (Spark's <c>ALTER COLUMN … DROP DEFAULT</c>); returns the version committed,
    /// or null when the column has none.</summary>
    /// <param name="tablePath">The table location: a local directory or a cloud URL.</param>
    /// <param name="column">A top-level column, any case.</param>
    /// <param name="cloud">Cloud settings for a cloud URL.</param>
    public static long? DropColumnDefault(string tablePath, string column, CloudArgs? cloud = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(column);
        return Run(tablePath, cloud, table => AlterTable.DropColumnDefault(table.FileSystem, table.Snapshot, column));
    }

    /// <summary>The live rows of the table for which the Boolean expression is false or null.</summary>
    private static long Violations(DmlTable table, ScalarExpr check)
    {
        IReadOnlyList<AddAction> files = table.Snapshot.Files;
        if (files.Count == 0) return 0;
        var frames = new List<LazyFrameHandle>(files.Count);
        try
        {
            foreach (AddAction file in files) frames.Add(table.ScanFile(file));
            LazyFrameHandle all = frames.Count == 1
                ? frames[0]
                : PolarsWrapper.LazyConcat([.. frames], PlConcatType.Vertical, rechunk: false, parallel: true, toSupertypes: false, maintainOrder: false);
            frames.Clear(); // owned by the query now
            ExprHandle violated = PolarsWrapper.Not(PolarsWrapper.FillNull(PolarsScalarLowerer.Lower(check), PolarsWrapper.Lit(false)));
            LazyFrameHandle count = PolarsWrapper.LazySelect(PolarsWrapper.LazyFilter(all, violated), [PolarsWrapper.Alias(PolarsWrapper.Len(), "n")]);
            using DataFrameHandle df = PolarsWrapper.LazyCollect(count, PlEngine.Streaming);
            using SeriesHandle n = PolarsWrapper.DataFrameGetColumn(df, "n");
            using DataTypeHandle int64 = PolarsWrapper.NewPrimitiveType((int)PlDataType.Int64);
            using SeriesHandle asLong = PolarsWrapper.SeriesCast(n, int64, strict: true, wrapNumerical: false);
            return PolarsWrapper.SeriesGetInt64Fast(asLong, 0);
        }
        finally
        {
            foreach (LazyFrameHandle frame in frames) frame.Dispose();
        }
    }
}
