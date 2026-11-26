using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare;
using Kaminari.Samidare.IR;
using Kaminari.Samidare.Sql;
using Kaminari.Sazanami.Arrow.Builders;
using Kaminari.Seseragi.Lakehouse.Delta;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Exceptions;
using Polars.NET.Core.Wrappers;
using Polars.NET.Core.Linq.Backend;
using DeltaType = Kaminari.Seseragi.Lakehouse.Delta.Schema.DeltaType;
using PrimitiveType = Kaminari.Seseragi.Lakehouse.Delta.Schema.PrimitiveType;
using DeltaArrayType = Kaminari.Seseragi.Lakehouse.Delta.Schema.ArrayType;
using DeltaDecimalType = Kaminari.Seseragi.Lakehouse.Delta.Schema.DecimalType;
using DeltaMapType = Kaminari.Seseragi.Lakehouse.Delta.Schema.MapType;
using DeltaStructType = Kaminari.Seseragi.Lakehouse.Delta.Schema.StructType;

namespace Polars.NET.Core.Delta;

/// <summary>
/// Enforces a table's CHECK constraints and column invariants (<see cref="Constraints"/>) on the rows a write adds,
/// as Spark does: each expression is bound with Spark's semantics (SqlFrontend.BindSparkExpression), and a row whose
/// result is false or null fails the write with <see cref="DeltaConstraintViolationException"/>. The check streams
/// with the rows: each batch's results are computed by the engine, and only a batch holding a violation is read.
/// </summary>
internal static class DeltaConstraintCheck
{
    /// <summary>
    /// The rows (table columns, logical names; consumed) filtered by a check of every constraint of the table, which
    /// passes every row and fails the query on the first violating one; the rows themselves when the table has none.
    /// </summary>
    /// <exception cref="DeltaProtocolException">A constraint is not an expression this kernel can enforce.</exception>
    public static LazyFrameHandle Apply(LazyFrameHandle rows, DeltaStructType schema, IReadOnlyDictionary<string, string>? configuration)
    {
        IReadOnlyList<DeltaConstraint> constraints = Constraints.Of(schema, configuration ?? new Dictionary<string, string>());
        if (constraints.Count == 0) return rows;
        (string, LogicalType)[] columns = [.. schema.Fields.Select(f => (f.Name, LogicalTypeOf(f.Type)))];
        ExprHandle? passes = null;
        foreach (DeltaConstraint constraint in constraints)
        {
            ExprHandle check = Checked(constraint, columns);
            passes = passes is null ? check : PolarsWrapper.And(passes, check);
        }
        return PolarsWrapper.LazyFilter(rows, passes!);
    }

    /// <summary>
    /// A new CHECK constraint bound over the table's columns, as a write will bind it: refused when it is not a
    /// Boolean expression this kernel can enforce.
    /// </summary>
    /// <exception cref="PolarsException">The expression cannot be a CHECK constraint of the table.</exception>
    public static BoundSparkExpression BindNew(DeltaStructType schema, string name, string expression)
    {
        (string, LogicalType)[] columns = [.. schema.Fields.Select(f => (f.Name, LogicalTypeOf(f.Type)))];
        try
        {
            return SqlFrontend.BindSparkExpression(expression, [.. columns], SparkExpressionUse.Check, DeltaSession.TimeZone, DateTimeOffset.UtcNow);
        }
        catch (SqlException e)
        {
            throw new PolarsException($"The CHECK constraint '{name}' ({expression}) cannot be added: {e.Reason}", e);
        }
    }

    /// <summary>Why a CHECK constraint cannot be bound over the columns of <paramref name="schema"/>; null when it can.</summary>
    public static string? Unenforceable(DeltaStructType schema, string expression)
    {
        (string, LogicalType)[] columns = [.. schema.Fields.Select(f => (f.Name, LogicalTypeOf(f.Type)))];
        try
        {
            SqlFrontend.BindSparkExpression(expression, [.. columns], SparkExpressionUse.Check, DeltaSession.TimeZone, DateTimeOffset.UtcNow);
            return null;
        }
        catch (SqlException e)
        {
            return e.Reason;
        }
    }

    /// <summary>A Boolean expression true on every row, which fails the query on a row violating the constraint.</summary>
    private static ExprHandle Checked(DeltaConstraint constraint, (string, LogicalType)[] columns)
    {
        BoundSparkExpression bound;
        try
        {
            bound = SqlFrontend.BindSparkExpression(
                constraint.Text, [.. columns], SparkExpressionUse.Check, DeltaSession.TimeZone, DateTimeOffset.UtcNow);
        }
        catch (SqlException e)
        {
            throw new DeltaProtocolException(
                $"The table's CHECK constraint {constraint.Name} ({constraint.Expression}) cannot be enforced by this kernel: {e.Reason}");
        }
        // The values a violation reports: the columns and struct fields the expression reads, by name.
        string[] paths = [.. ReadPaths(bound.Expr).Order(StringComparer.Ordinal)];
        ExprHandle violated = PolarsWrapper.Not(PolarsWrapper.FillNull(PolarsScalarLowerer.Lower(bound.Expr), PolarsWrapper.Lit(false)));
        ExprHandle payload = PolarsWrapper.AsStruct(
        [
            PolarsWrapper.Alias(violated, "violated"),
            .. paths.Select((path, i) => PolarsWrapper.Alias(PathExpr(path), $"v{i}")),
        ]);
        using DataTypeHandle boolean = NativeDataTypeResolver.GetDataTypeHandle(typeof(bool));
        return PolarsWrapper.Map(payload, batch => Passes(batch, constraint, paths), boolean);
    }

    /// <summary>True for every row of a batch, or the violation of its first violating row (relayed to the caller).</summary>
    private static IArrowArray Passes(IArrowArray batch, DeltaConstraint constraint, string[] paths)
    {
        var payload = (StructArray)batch;
        var violated = (BooleanArray)payload.Fields[0];
        for (int i = 0; i < violated.Length; i++)
        {
            if (violated.GetValue(i) == true)
            {
                KeyValuePair<string, string>[] values = [.. paths.Select((path, f) => KeyValuePair.Create(path, SparkText(payload.Fields[f + 1], i)))];
                throw ManagedErrorRelay.Capture(new DeltaConstraintViolationException(constraint.Name, constraint.Expression, values));
            }
        }
        var passes = new BooleanArray.Builder();
        passes.AppendRange(Enumerable.Repeat(true, violated.Length));
        return passes.Build(PooledArrowAllocator.Instance);
    }

    /// <summary>The columns an expression reads, a struct's field by its path (<c>st.x</c>).</summary>
    private static HashSet<string> ReadPaths(ScalarExpr expr)
    {
        var paths = new HashSet<string>(StringComparer.Ordinal);
        Visit(expr);
        return paths;

        void Visit(ScalarExpr e)
        {
            if (PathOf(e) is { } path)
            {
                paths.Add(path);
                return;
            }
            foreach (ScalarExpr child in ScalarExprOps.children(e)) Visit(child);
        }

        static string? PathOf(ScalarExpr e) => e switch
        {
            ScalarExpr.Column c => c.name,
            ScalarExpr.Function { func.IsStructFieldByName: true, args: [var inner, ScalarExpr.Literal { value: LiteralValue.String field }] }
                when PathOf(inner) is { } parent => parent + "." + field.Item,
            _ => null,
        };
    }

    private static ExprHandle PathExpr(string path)
    {
        string[] parts = path.Split('.');
        ExprHandle e = PolarsWrapper.Col(parts[0]);
        for (int i = 1; i < parts.Length; i++) e = PolarsWrapper.StructFieldByName(e, [parts[i]]);
        return e;
    }

    /// <summary>The IR type a table column is read as, for the binder (TIMESTAMP an instant, TIMESTAMP_NTZ not).</summary>
    internal static LogicalType LogicalTypeOf(DeltaType type) => ArrowTypeOf(type) is { } arrow ? LogicalType.NewArrow(arrow) : LogicalType.Unknown;

    private static IArrowType? ArrowTypeOf(DeltaType type) => type switch
    {
        PrimitiveType { Name: PrimitiveType.Boolean } => BooleanType.Default,
        PrimitiveType { Name: PrimitiveType.Byte } => Int8Type.Default,
        PrimitiveType { Name: PrimitiveType.Short } => Int16Type.Default,
        PrimitiveType { Name: PrimitiveType.Integer } => Int32Type.Default,
        PrimitiveType { Name: PrimitiveType.Long } => Int64Type.Default,
        PrimitiveType { Name: PrimitiveType.Float } => FloatType.Default,
        PrimitiveType { Name: PrimitiveType.Double } => DoubleType.Default,
        PrimitiveType { Name: PrimitiveType.String } => StringViewType.Default,
        PrimitiveType { Name: PrimitiveType.Binary } => BinaryViewType.Default,
        PrimitiveType { Name: PrimitiveType.Date } => Date32Type.Default,
        PrimitiveType { Name: PrimitiveType.Timestamp } => new TimestampType(TimeUnit.Microsecond, "UTC"),
        PrimitiveType { Name: PrimitiveType.TimestampNtz } => new TimestampType(TimeUnit.Microsecond, (string?)null),
        DeltaDecimalType d => new Decimal128Type(d.Precision, d.Scale),
        DeltaStructType s when s.Fields.Select(f => ArrowTypeOf(f.Type)).ToArray() is var fields && fields.All(f => f is not null) =>
            new StructType([.. s.Fields.Select((f, i) => new Field(f.Name, fields[i]!, f.Nullable))]),
        DeltaArrayType a when ArrowTypeOf(a.ElementType) is { } element => new ListType(new Field("element", element, a.ContainsNull)),
        DeltaMapType => null,
        _ => null,
    };

    /// <summary>A value as Spark writes it in a violation: dates as days and timestamps as microseconds since the
    /// epoch, floating values as Java does, <c>null</c> for null.</summary>
    internal static string SparkText(IArrowArray array, int i)
    {
        if (array.IsNull(i)) return "null";
        return array switch
        {
            BooleanArray a => a.GetValue(i) == true ? "true" : "false",
            Int8Array a => a.GetValue(i)!.Value.ToString(CultureInfo.InvariantCulture),
            Int16Array a => a.GetValue(i)!.Value.ToString(CultureInfo.InvariantCulture),
            Int32Array a => a.GetValue(i)!.Value.ToString(CultureInfo.InvariantCulture),
            Int64Array a => a.GetValue(i)!.Value.ToString(CultureInfo.InvariantCulture),
            DoubleArray a => JavaText(a.GetValue(i)!.Value),
            FloatArray a => JavaText(a.GetValue(i)!.Value),
            Decimal128Array a => a.GetValue(i)!.Value.ToString("F" + ((Decimal128Type)a.Data.DataType).Scale, CultureInfo.InvariantCulture),
            Date32Array a => a.GetValue(i)!.Value.ToString(CultureInfo.InvariantCulture),
            TimestampArray a => a.GetValue(i)!.Value.ToString(CultureInfo.InvariantCulture),
            StringViewArray a => a.GetString(i),
            StringArray a => a.GetString(i),
            _ => array.Data.DataType.Name,
        };
    }

    /// <summary>A floating value as Java's Double.toString / Float.toString write it: the shortest digits that read back,
    /// plain from 10^-3 to 10^7 (<c>1.5</c>, <c>100.0</c>), else as <c>d.dddE±n</c> (<c>1.0E10</c>, <c>1.234E-5</c>).</summary>
    internal static string JavaText(double value) => JavaText(value, value.ToString("R", CultureInfo.InvariantCulture));

    /// <inheritdoc cref="JavaText(double)"/>
    internal static string JavaText(float value) => JavaText(value, value.ToString("R", CultureInfo.InvariantCulture));

    private static string JavaText(double value, string shortest)
    {
        if (double.IsNaN(value)) return "NaN";
        if (double.IsInfinity(value)) return value > 0 ? "Infinity" : "-Infinity";
        string sign = value < 0 || (value == 0 && double.IsNegative(value)) ? "-" : "";
        string text = shortest.TrimStart('-');
        double magnitude = Math.Abs(value);
        if (magnitude == 0) return sign + "0.0";
        // The digits and the exponent of the first one.
        string digits;
        int exponent;
        int e = text.IndexOfAny(['E', 'e']);
        if (e >= 0)
        {
            string mantissa = text[..e];
            exponent = int.Parse(text[(e + 1)..], NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture);
            int point = mantissa.IndexOf('.');
            digits = mantissa.Replace(".", "");
            exponent += (point < 0 ? mantissa.Length : point) - 1;
            int zeros = digits.Length - digits.TrimStart('0').Length;
            digits = digits.TrimStart('0');
            exponent -= zeros;
        }
        else
        {
            int point = text.IndexOf('.');
            string all = text.Replace(".", "");
            int zeros = all.Length - all.TrimStart('0').Length;
            exponent = (point < 0 ? text.Length : point) - 1 - zeros;
            digits = all.TrimStart('0');
        }
        digits = digits.TrimEnd('0');
        if (digits.Length == 0) digits = "0";
        if (magnitude >= 1e-3 && magnitude < 1e7)
        {
            // Plain: the integral digits (at least one), a point, the fraction (at least one digit).
            string integral, fraction;
            if (exponent >= 0)
            {
                integral = digits.Length > exponent + 1 ? digits[..(exponent + 1)] : digits.PadRight(exponent + 1, '0');
                fraction = digits.Length > exponent + 1 ? digits[(exponent + 1)..] : "0";
            }
            else
            {
                integral = "0";
                fraction = new string('0', -exponent - 1) + digits;
            }
            return sign + integral + "." + fraction;
        }
        return sign + digits[0] + "." + (digits.Length > 1 ? digits[1..] : "0") + "E" + exponent.ToString(CultureInfo.InvariantCulture);
    }
}
