using Kaminari.Samidare;
using Kaminari.Samidare.IR;
using Kaminari.Samidare.Sql;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Polars.NET.Core.Linq.Backend;
using Polars.NET.Core.Wrappers;

namespace Polars.NET.Core.Delta;

/// <summary>
/// A table's generated columns (<see cref="Constraints.GeneratedColumns"/>) as engine expressions over its other
/// columns: what a write computes where the rows lack a generated column (an append, a merge's inserted rows) or where
/// an update changes a row without setting it, as Spark does. A value given for one is checked against it (see
/// <see cref="DeltaConstraintCheck"/>).
/// </summary>
internal sealed class DeltaGeneratedColumns
{
    private readonly (string Column, ScalarExpr Value, DeltaType Type)[] _columns;

    private DeltaGeneratedColumns((string, ScalarExpr, DeltaType)[] columns) => _columns = columns;

    /// <summary>Whether the table has generated columns.</summary>
    public bool Any => _columns.Length > 0;

    /// <summary>The generated columns' names.</summary>
    public IEnumerable<string> Names => _columns.Select(c => c.Column);

    /// <summary>The table's generated columns, bound over its columns with Spark's semantics.</summary>
    /// <param name="schema">The table's columns.</param>
    /// <param name="declaring">The columns are being declared (a table created with them): a refusal says the
    /// expression is not valid rather than that this kernel cannot compute it.</param>
    /// <exception cref="DeltaProtocolException">A generation expression is not one this kernel can compute: an
    /// expression the binder refuses, one reading itself or another generated column, or one whose type is not the column's.</exception>
    public static DeltaGeneratedColumns Of(StructType schema, bool declaring = false)
    {
        IReadOnlyList<DeltaGeneratedColumn> generated = Constraints.GeneratedColumns(schema);
        if (generated.Count == 0) return new DeltaGeneratedColumns([]);
        (string, LogicalType)[] columns = [.. schema.Fields.Select(f => (f.Name, DeltaConstraintCheck.LogicalTypeOf(f.Type)))];
        var names = generated.Select(g => g.Column).ToHashSet(StringComparer.Ordinal);
        var bound = new List<(string, ScalarExpr, DeltaType)>(generated.Count);
        foreach (DeltaGeneratedColumn column in generated)
        {
            string Refused(string why) => declaring
                ? $"The generated column {column.Column} ({column.Expression}) is not valid: {why}"
                : $"The generated column {column.Column} ({column.Expression}) cannot be computed by this kernel: {why}";
            BoundSparkExpression value;
            try
            {
                value = SqlFrontend.BindSparkExpression(
                    column.Expression, [.. columns], SparkExpressionUse.Generated, DeltaSession.TimeZone, DateTimeOffset.UtcNow);
            }
            catch (SqlException e)
            {
                throw new DeltaProtocolException(Refused(e.Reason));
            }
            if (value.Columns.Contains(column.Column, StringComparer.Ordinal))
                throw new DeltaProtocolException(Refused("it reads the column itself."));
            if (value.Columns.FirstOrDefault(names.Contains) is { } other)
                throw new DeltaProtocolException(Refused($"it reads the generated column {other}."));
            DeltaType type = schema.Field(column.Column)!.Type;
            LogicalType declared = DeltaConstraintCheck.LogicalTypeOf(type);
            if (!SameType(value.Type, declared))
                throw new DeltaProtocolException(Refused($"its type {SqlFrontend.SparkTypeName(value.Type)} is not the column's, {SqlFrontend.SparkTypeName(declared)}."));
            bound.Add((column.Column, value.Expr, type));
        }
        return new DeltaGeneratedColumns([.. bound]);
    }

    /// <summary>Whether a value of the expression's type is one of the column's (Spark makes them equal).</summary>
    private static bool SameType(LogicalType value, LogicalType column) =>
        value.Display == column.Display || (IsText(value) && IsText(column));

    private static bool IsText(LogicalType t) =>
        t is LogicalType.Arrow { arrowType: Apache.Arrow.Types.StringType or Apache.Arrow.Types.StringViewType or Apache.Arrow.Types.LargeStringType };

    /// <summary>The generated column's value over the row, as the column's type.</summary>
    private static ExprHandle Value(ScalarExpr expr, DeltaType type)
    {
        using DataTypeHandle target = DeltaKernelScan.EngineType(type);
        return PolarsWrapper.ExprCast(PolarsScalarLowerer.Lower(expr), PolarsWrapper.DataTypeExprFromDataType(target), strict: false, wrapNumerical: false);
    }

    /// <summary>The generated columns a frame's columns lack.</summary>
    public HashSet<string> Missing(IEnumerable<string> present)
    {
        var have = present.ToHashSet(StringComparer.Ordinal);
        return [.. Names.Where(n => !have.Contains(n))];
    }

    /// <summary>The rows (table columns; consumed) with the <paramref name="columns"/> computed.</summary>
    public LazyFrameHandle Compute(LazyFrameHandle rows, IReadOnlySet<string> columns)
    {
        ExprHandle[] computed = [.. _columns.Where(c => columns.Contains(c.Column)).Select(c => PolarsWrapper.Alias(Value(c.Value, c.Type), c.Column))];
        return computed.Length == 0 ? rows : PolarsWrapper.LazyWithColumns(rows, computed);
    }

    /// <summary>
    /// The rows (table columns; consumed) with the generated columns not in <paramref name="kept"/> computed on the
    /// rows <paramref name="where"/> holds for (taken), the other rows' values kept.
    /// </summary>
    public LazyFrameHandle Recompute(LazyFrameHandle rows, IReadOnlySet<string> kept, Func<ExprHandle> where)
    {
        ExprHandle[] computed =
        [
            .. _columns.Where(c => !kept.Contains(c.Column)).Select(c =>
                PolarsWrapper.Alias(PolarsWrapper.IfElse(where(), Value(c.Value, c.Type), PolarsWrapper.Col(c.Column)), c.Column)),
        ];
        return computed.Length == 0 ? rows : PolarsWrapper.LazyWithColumns(rows, computed);
    }
}
