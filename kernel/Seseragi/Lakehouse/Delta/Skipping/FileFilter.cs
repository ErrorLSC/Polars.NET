using Kaminari.Samidare.IR;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Lakehouse.Skipping;
using Microsoft.FSharp.Collections;

namespace Kaminari.Seseragi.Lakehouse.Delta.Skipping;

/// <summary>
/// Decides, from a file's partition values and statistics alone, whether the file can hold a row matching a
/// condition. It never drops a file that might: whenever something is unknown the file is kept. The condition is the
/// query compiler's IR (see <see cref="FileSkipping"/>), or a kernel <see cref="Predicate"/>, read as that IR.
/// </summary>
public sealed class FileFilter
{
    private readonly FileSkipping? _skipping;

    /// <summary>Prepares a predicate for a table.</summary>
    /// <param name="schema">The table schema.</param>
    /// <param name="partitionColumns">The partition columns, by logical name.</param>
    /// <param name="predicate">The condition rows must meet.</param>
    public FileFilter(StructType schema, IReadOnlyList<string> partitionColumns, Predicate predicate)
        : this(schema, partitionColumns, ToFilter(predicate ?? throw new ArgumentNullException(nameof(predicate)), schema))
    {
    }

    /// <summary>Prepares a row filter of the query compiler's IR for a table (columns by logical name).</summary>
    /// <param name="schema">The table schema.</param>
    /// <param name="partitionColumns">The partition columns, by logical name.</param>
    /// <param name="filter">The rows a query keeps.</param>
    public FileFilter(StructType schema, IReadOnlyList<string> partitionColumns, ScalarExpr filter)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(partitionColumns);
        ArgumentNullException.ThrowIfNull(filter);
        _skipping = FileSkipping.Create(schema, partitionColumns, filter);
    }

    /// <summary>Whether the condition can rule out any file at all (false: every file is kept).</summary>
    public bool CanSkip => _skipping is not null;

    /// <summary>False only when no row of the file can match.</summary>
    public bool MightMatch(AddAction file)
    {
        ArgumentNullException.ThrowIfNull(file);
        return _skipping?.MightMatch(file) ?? true;
    }

    /// <summary>The files that may hold a matching row, in their order (judged on several threads when many).</summary>
    internal List<AddAction> Matching(IReadOnlyList<AddAction> files) => _skipping?.Matching(files) ?? [.. files];

    // ------------------------------------------------------------------ a kernel predicate as IR

    /// <summary>
    /// The IR of a kernel predicate. Values become literals of their own type; a <see cref="DateTime"/> compared with a
    /// timestamp column is an instant (UTC unless it says it is local, as the log's values), with a timestamp without
    /// time zone a wall-clock value, and a <see cref="DateTimeOffset"/> with the latter its UTC wall clock.
    /// </summary>
    internal static ScalarExpr ToFilter(Predicate predicate, StructType schema) => predicate switch
    {
        ComparePredicate c => Literal(c.Value, TypeOf(schema, c.Column)) is { } value
            ? ScalarExpr.NewBinary(Op(c.Op), Column(c.Column), ScalarExpr.NewLiteral(value))
            : s_unknown,
        NullPredicate n => ScalarExpr.NewUnary(n.WantNull ? UnaryOp.IsNull : UnaryOp.IsNotNull, Column(n.Column)),
        JunctionPredicate { Children.Length: 0 } j => ScalarExpr.NewLiteral(LiteralValue.NewBool(j.IsAnd)),
        JunctionPredicate j => j.Children.Select(c => ToFilter(c, schema)).Aggregate((a, b) => ScalarExpr.NewBinary(j.IsAnd ? BinaryOp.And : BinaryOp.Or, a, b)),
        NotPredicate n => ScalarExpr.NewUnary(UnaryOp.Not, ToFilter(n.Child, schema)),
        _ => throw new ArgumentException($"Unknown predicate {predicate}.", nameof(predicate)),
    };

    /// <summary>
    /// A condition statistics cannot answer (two columns no table has), for a value of a type a predicate does not
    /// take: it keeps every file, also under a negation (a constant would flip).
    /// </summary>
    private static readonly ScalarExpr s_unknown =
        ScalarExpr.NewBinary(BinaryOp.Eq, ScalarExpr.NewColumn("\0"), ScalarExpr.NewColumn("\0"));

    private static BinaryOp Op(CompareOp op) => op switch
    {
        CompareOp.Eq => BinaryOp.Eq,
        CompareOp.Ne => BinaryOp.NotEq,
        CompareOp.Lt => BinaryOp.Lt,
        CompareOp.Le => BinaryOp.LtEq,
        CompareOp.Gt => BinaryOp.Gt,
        _ => BinaryOp.GtEq,
    };

    /// <summary>A column, or a struct field reached through <c>StructFieldByName</c>.</summary>
    private static ScalarExpr Column(ColumnPath path)
    {
        ScalarExpr expr = ScalarExpr.NewColumn(path.Segments[0]);
        foreach (string name in path.Segments.Skip(1))
        {
            expr = ScalarExpr.NewFunction(ScalarFunction.StructFieldByName,
                ListModule.OfSeq([expr, ScalarExpr.NewLiteral(LiteralValue.NewString(name))]), LogicalTypes.Null);
        }
        return expr;
    }

    /// <summary>The Delta type at a path of logical names; null when there is none.</summary>
    private static DeltaType? TypeOf(StructType schema, ColumnPath path)
    {
        DeltaType? type = schema;
        foreach (string name in path.Segments)
        {
            if (type is not StructType s || s.Field(name) is not { } field) return null;
            type = field.Type;
        }
        return type;
    }

    private static LiteralValue? Literal(object value, DeltaType? type)
    {
        bool ntz = type is PrimitiveType { Name: PrimitiveType.TimestampNtz };
        return value switch
        {
            sbyte v => LiteralValue.NewInt8(v),
            short v => LiteralValue.NewInt16(v),
            int v => LiteralValue.NewInt32(v),
            long v => LiteralValue.NewInt64(v),
            byte v => LiteralValue.NewUInt8(v),
            ushort v => LiteralValue.NewUInt16(v),
            uint v => LiteralValue.NewUInt32(v),
            ulong v => LiteralValue.NewUInt64(v),
            float v => LiteralValue.NewFloat32(v),
            double v => LiteralValue.NewFloat64(v),
            decimal v => LiteralValue.NewDecimal(v),
            string v => LiteralValue.NewString(v),
            bool v => LiteralValue.NewBool(v),
            DateOnly v => LiteralValue.NewDate(v),
            DateTime v when ntz => LiteralValue.NewDateTime(DateTime.SpecifyKind(v, DateTimeKind.Unspecified)),
            DateTime v => LiteralValue.NewDateTimeOffset(new DateTimeOffset(v.Kind == DateTimeKind.Local
                ? v.ToUniversalTime() : DateTime.SpecifyKind(v, DateTimeKind.Utc))),
            DateTimeOffset v when ntz => LiteralValue.NewDateTime(v.UtcDateTime),
            DateTimeOffset v => LiteralValue.NewDateTimeOffset(v),
            _ => null,
        };
    }
}
