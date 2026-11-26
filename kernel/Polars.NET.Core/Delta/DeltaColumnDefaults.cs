using Kaminari.Samidare;
using Kaminari.Samidare.IR;
using Kaminari.Samidare.Sql;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Polars.NET.Core.Linq.Backend;
using Polars.NET.Core.Wrappers;

namespace Polars.NET.Core.Delta;

/// <summary>
/// A table's column defaults (<see cref="Constraints.ColumnDefaults"/>, feature allowColumnDefaults) as engine
/// constants: what a write gives a column its rows lack (an append, a merge's inserted rows), as Spark does. A value
/// given for the column, null included, is kept. Bound once per write, so <c>current_timestamp()</c> is one instant
/// for all its rows.
/// </summary>
internal sealed class DeltaColumnDefaults
{
    /// <summary>The session time zone defaults read the current date in (D2; a write option comes with S7e).</summary>
    private const string SessionTimeZone = "UTC";

    private readonly (string Column, ScalarExpr Value, DeltaType Type)[] _columns;

    private DeltaColumnDefaults((string, ScalarExpr, DeltaType)[] columns) => _columns = columns;

    /// <summary>The columns that have a default.</summary>
    public IEnumerable<string> Names => _columns.Select(c => c.Column);

    /// <summary>The table's column defaults, bound as of <paramref name="now"/> (the write's time).</summary>
    /// <exception cref="DeltaProtocolException">A default is not a constant this kernel can compute, or not of the
    /// column's type.</exception>
    public static DeltaColumnDefaults Of(StructType schema, DateTimeOffset now)
    {
        IReadOnlyList<DeltaColumnDefault> defaults = Constraints.ColumnDefaults(schema);
        if (defaults.Count == 0) return new DeltaColumnDefaults([]);
        // The table's columns, so that a default reading one is refused as such.
        (string, LogicalType)[] columns = [.. schema.Fields.Select(f => (f.Name, DeltaConstraintCheck.LogicalTypeOf(f.Type)))];
        var bound = new List<(string, ScalarExpr, DeltaType)>(defaults.Count);
        foreach (DeltaColumnDefault column in defaults)
        {
            DeltaType type = schema.Field(column.Column)!.Type;
            // As Spark assigns a default: converted to the column's type.
            string text = $"CAST(({column.Expression}) AS {type})";
            try
            {
                bound.Add((column.Column, SqlFrontend.BindSparkExpression(text, [.. columns], SparkExpressionUse.Default, SessionTimeZone, now).Expr, type));
            }
            catch (SqlException e)
            {
                throw new DeltaProtocolException($"The default of the column {column.Column} ({column.Expression}) cannot be computed by this kernel: {e.Reason}");
            }
        }
        return new DeltaColumnDefaults([.. bound]);
    }

    /// <summary>The columns with a default a frame's columns lack.</summary>
    public HashSet<string> Missing(IEnumerable<string> present)
    {
        var have = present.ToHashSet(StringComparer.Ordinal);
        return [.. Names.Where(n => !have.Contains(n))];
    }

    /// <summary>The rows (table columns; consumed) with the <paramref name="columns"/> set to their defaults, on the
    /// rows <paramref name="where"/> holds for (each call one expression) when given, the other rows' values kept.</summary>
    public LazyFrameHandle Fill(LazyFrameHandle rows, IReadOnlySet<string> columns, Func<ExprHandle>? where = null)
    {
        ExprHandle[] filled =
        [
            .. _columns.Where(c => columns.Contains(c.Column)).Select(c =>
            {
                using DataTypeHandle target = DeltaKernelScan.EngineType(c.Type);
                ExprHandle value = PolarsWrapper.ExprCast(PolarsScalarLowerer.Lower(c.Value), PolarsWrapper.DataTypeExprFromDataType(target), strict: false, wrapNumerical: false);
                return PolarsWrapper.Alias(where is null ? value : PolarsWrapper.IfElse(where(), value, PolarsWrapper.Col(c.Column)), c.Column);
            }),
        ];
        return filled.Length == 0 ? rows : PolarsWrapper.LazyWithColumns(rows, filled);
    }
}
