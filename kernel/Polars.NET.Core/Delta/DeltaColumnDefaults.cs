using A = Apache.Arrow.Types;
using Kaminari.Samidare;
using Kaminari.Samidare.IR;
using Kaminari.Samidare.Sql;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Exceptions;
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
                bound.Add((column.Column, SqlFrontend.BindSparkExpression(text, [.. columns], SparkExpressionUse.Default, DeltaSession.TimeZone, now).Expr, type));
            }
            catch (SqlException e)
            {
                throw new DeltaProtocolException($"The default of the column {column.Column} ({column.Expression}) cannot be computed by this kernel: {e.Reason}");
            }
        }
        return new DeltaColumnDefaults([.. bound]);
    }

    /// <summary>
    /// Checks a default about to be declared for a column of the schema, as Spark checks it when it is declared: a
    /// constant this kernel can compute, of a type that can be stored in the column (Spark's store assignment:
    /// numbers into numbers, any value into text and text into any, dates and timestamps into each other, null into
    /// anything), whose value fits the column (computed once, as Spark computes it).
    /// </summary>
    /// <param name="schema">The table's columns.</param>
    /// <param name="column">The column, a top-level column of the schema.</param>
    /// <param name="expression">The default.</param>
    /// <exception cref="PolarsException">The default is not valid for the column.</exception>
    public static void Validate(StructType schema, string column, string expression)
    {
        DeltaType type = schema.Field(column)!.Type;
        LogicalType declared = DeltaConstraintCheck.LogicalTypeOf(type);
        (string, LogicalType)[] columns = [.. schema.Fields.Select(f => (f.Name, DeltaConstraintCheck.LogicalTypeOf(f.Type)))];
        DateTimeOffset now = DateTimeOffset.UtcNow;
        string Refused(string why) => $"The default {expression} of the column {column} is not valid: {why}";

        BoundSparkExpression value, cast;
        try
        {
            value = SqlFrontend.BindSparkExpression(expression, [.. columns], SparkExpressionUse.Default, DeltaSession.TimeZone, now);
        }
        catch (SqlException e)
        {
            throw new PolarsException(Refused(e.Reason), e);
        }
        if (!CanStoreAssign(value.Type, declared))
            throw new PolarsException(Refused(
                $"a value of type {SqlFrontend.SparkTypeName(value.Type)} cannot be stored in a column of type {SqlFrontend.SparkTypeName(declared)}."));
        try
        {
            cast = SqlFrontend.BindSparkExpression($"CAST(({expression}) AS {type})", [.. columns], SparkExpressionUse.Default, DeltaSession.TimeZone, now);
        }
        catch (SqlException e)
        {
            throw new PolarsException(Refused(e.Reason), e);
        }

        // Computed once, as the column's type: a value that does not fit fails here rather than at the first write.
        using DataTypeHandle target = DeltaKernelScan.EngineType(type);
        ExprHandle computed = PolarsWrapper.ExprCast(PolarsScalarLowerer.Lower(cast.Expr), PolarsWrapper.DataTypeExprFromDataType(target), strict: false, wrapNumerical: false);
        try
        {
            using DataFrameHandle none = PolarsWrapper.DataFrameNew([]);
            using DataFrameHandle result = PolarsWrapper.LazyCollect(
                PolarsWrapper.LazySelect(PolarsWrapper.DataFrameToLazy(none), [PolarsWrapper.Alias(computed, "v")]), PlEngine.Auto);
        }
        // The engine relays a failing conversion or arithmetic as .NET's own exception (see ManagedErrorRelay).
        catch (Exception e) when (e is PolarsException or ArithmeticException or InvalidCastException or FormatException)
        {
            throw new PolarsException(Refused($"its value cannot be stored in the column: {(e is PolarsException { InnerException: { } inner } ? inner : e).Message}"), e);
        }
    }

    /// <summary>Whether Spark stores a value of the type in a column of the other (its ANSI store assignment).</summary>
    private static bool CanStoreAssign(LogicalType from, LogicalType to)
    {
        if (from.Display == to.Display) return true;
        Kind source = KindOf(from), target = KindOf(to);
        return (source, target) switch
        {
            (Kind.Null, _) => true,
            (Kind.Numeric, Kind.Numeric) => true,
            (Kind.Datetime, Kind.Datetime) => true,
            (not Kind.Other, Kind.Text) => true,
            (Kind.Text, not Kind.Other) => true,
            _ => false,
        };
    }

    private enum Kind { Null, Numeric, Text, Boolean, Datetime, Binary, Other }

    private static Kind KindOf(LogicalType type) => type switch
    {
        LogicalType.Arrow { arrowType: var t } => t switch
        {
            A.NullType => Kind.Null,
            A.BooleanType => Kind.Boolean,
            A.Int8Type or A.Int16Type or A.Int32Type or A.Int64Type or A.FloatType or A.DoubleType or A.Decimal128Type => Kind.Numeric,
            A.StringType or A.StringViewType or A.LargeStringType => Kind.Text,
            A.Date32Type or A.TimestampType => Kind.Datetime,
            A.BinaryType or A.BinaryViewType or A.LargeBinaryType => Kind.Binary,
            _ => Kind.Other,
        },
        _ => Kind.Other,
    };

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
