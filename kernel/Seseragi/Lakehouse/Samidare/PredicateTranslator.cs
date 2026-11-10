using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Seseragi.Lakehouse.Skipping;
using Microsoft.FSharp.Collections;

namespace Kaminari.Seseragi.Lakehouse.Samidare;

/// <summary>
/// Turns a row filter of the query compiler into a file predicate for skipping. The result is implied by the
/// filter: every row the filter keeps satisfies it, so a file the predicate rules out holds no such row. What
/// cannot be expressed is left out, which only makes skipping weaker. The filter itself must still run on the rows.
/// </summary>
/// <remarks>
/// The filter follows the compiler's semantics (IR/SEMANTICS.md): a null result drops the row, and floating values
/// follow a total order in which NaN is greater than every number. The predicate follows SQL: a comparison with a
/// null matches nothing, and statistics may leave NaN out of a maximum. So a floating comparison that a NaN row
/// could satisfy (<c>&gt;</c>, <c>&gt;=</c>, <c>!=</c>) is translated only where the filter also excludes NaN for
/// that column, as the compiler's .NET NaN guard (<c>x &lt;= +inf</c>) does.
/// </remarks>
public static class PredicateTranslator
{
    /// <summary>Translates a row filter; null when it implies nothing a file predicate can say.</summary>
    /// <param name="filter">The filter over the columns of <paramref name="schema"/>.</param>
    /// <param name="schema">The schema of the scanned table, as the compiler sees it.</param>
    public static Predicate? Translate(ScalarExpr filter, LogicalSchema schema)
    {
        ArgumentNullException.ThrowIfNull(filter);
        ArgumentNullException.ThrowIfNull(schema);
        var types = new Dictionary<string, LogicalType>(StringComparer.Ordinal);
        foreach (LogicalField field in schema.Fields) types[field.Name] = field.DataType;
        return new Translator(types).Translate(filter, s_noGuards)?.Predicate;
    }

    private static readonly HashSet<string> s_noGuards = new(StringComparer.Ordinal);

    /// <summary>
    /// A translated piece. <c>Exact</c>: the predicate holds on exactly the rows where the piece is true, and its
    /// negation on exactly the rows where the piece is false (null rows on neither side). Only exact pieces may be
    /// negated.
    /// </summary>
    private sealed record Piece(Predicate Predicate, bool Exact);

    private sealed class Translator(Dictionary<string, LogicalType> types)
    {
        /// <param name="expr">The piece of the filter.</param>
        /// <param name="nonNaN">Floating columns the enclosing conjunction keeps away from NaN.</param>
        public Piece? Translate(ScalarExpr expr, HashSet<string> nonNaN)
        {
            switch (expr)
            {
                case ScalarExpr.Binary b when b.op.IsAnd:
                    return And(Conjuncts(expr), nonNaN);

                case ScalarExpr.Binary b when b.op.IsOr:
                {
                    var children = new List<Piece>();
                    foreach (ScalarExpr child in Disjuncts(expr))
                    {
                        // A disjunct that says nothing makes the whole disjunction say nothing.
                        Piece? piece = Translate(child, nonNaN);
                        if (piece is null) return null;
                        children.Add(piece);
                    }
                    return new Piece(Predicate.Or([.. children.Select(c => c.Predicate)]), children.All(c => c.Exact));
                }

                case ScalarExpr.Binary b:
                    return Comparison(b.op, b.left, b.right, nonNaN);

                case ScalarExpr.Unary u when u.op.IsNot:
                {
                    Piece? inner = Translate(u.expr, nonNaN);
                    return inner is { Exact: true } ? new Piece(Predicate.Not(inner.Predicate), true) : null;
                }

                case ScalarExpr.Unary u when (u.op.IsIsNull || u.op.IsIsNotNull) && ColumnOf(u.expr) is string column:
                    return new Piece(u.op.IsIsNull ? Predicate.IsNull(column) : Predicate.IsNotNull(column), true);

                case ScalarExpr.Column c when IsBoolean(c.name):
                    return new Piece(Predicate.Eq(c.name, true), true);

                case ScalarExpr.InList list when list.source is InListSource.Literals literals:
                    return InList(list.expr, literals.Item, list.negated, list.nullsEqual, nonNaN);

                // The compiler wraps a comparison that is null on a null operand as Coalesce [cmp; false]. True on
                // the same rows, but false (not null) on null rows, so it cannot be negated.
                case ScalarExpr.Coalesce c when c.exprs.Length == 2
                                              && c.exprs.Tail.Head is ScalarExpr.Literal { value: LiteralValue.Bool { Item: false } }:
                    return Translate(c.exprs.Head, nonNaN) is Piece p ? p with { Exact = false } : null;

                default:
                    return null;
            }
        }

        private Piece? And(List<ScalarExpr> conjuncts, HashSet<string> outer)
        {
            // Columns a conjunct keeps away from NaN protect comparisons anywhere in the conjunction: a row the
            // whole conjunction keeps has a number (or null) there.
            HashSet<string> nonNaN = outer;
            foreach (ScalarExpr conjunct in conjuncts)
            {
                if (NaNGuardColumn(conjunct) is string guarded)
                {
                    if (ReferenceEquals(nonNaN, outer)) nonNaN = new HashSet<string>(outer, StringComparer.Ordinal);
                    nonNaN.Add(guarded);
                }
            }

            var pieces = new List<Piece>();
            bool exact = true;
            foreach (ScalarExpr conjunct in conjuncts)
            {
                Piece? piece = Translate(conjunct, nonNaN);
                if (piece is null) { exact = false; continue; }
                pieces.Add(piece);
                exact &= piece.Exact;
            }
            return pieces.Count switch
            {
                0 => null,
                1 => pieces[0] with { Exact = exact },
                _ => new Piece(Predicate.And([.. pieces.Select(p => p.Predicate)]), exact),
            };
        }

        private Piece? Comparison(BinaryOp op, ScalarExpr left, ScalarExpr right, HashSet<string> nonNaN)
        {
            CompareOp? compare = op switch
            {
                _ when op.IsEq => CompareOp.Eq,
                _ when op.IsNotEq => CompareOp.Ne,
                _ when op.IsLt => CompareOp.Lt,
                _ when op.IsLtEq => CompareOp.Le,
                _ when op.IsGt => CompareOp.Gt,
                _ when op.IsGtEq => CompareOp.Ge,
                _ => null,
            };

            // Null-safe equality: true or false, never null.
            if (op.IsEqMissing || op.IsNotEqMissing)
            {
                if (!TrySplit(left, right, out string? column, out LiteralValue? literal, out _)) return null;
                if (literal is LiteralValue.Null)
                    return new Piece(op.IsEqMissing ? Predicate.IsNull(column!) : Predicate.IsNotNull(column!), true);
                // `x <=> 5` is `x = 5` on the rows it keeps, but false where x is null: not negatable.
                // `x <!=> 5` also keeps null rows, which no comparison does.
                return op.IsEqMissing ? Compare(column!, CompareOp.Eq, literal!, nonNaN) is Piece p ? p with { Exact = false } : null : null;
            }

            if (compare is not CompareOp cmp) return null;
            if (!TrySplit(left, right, out string? col, out LiteralValue? lit, out bool flipped)) return null;
            return Compare(col!, flipped ? Flip(cmp) : cmp, lit!, nonNaN);
        }

        private Piece? Compare(string column, CompareOp op, LiteralValue literal, HashSet<string> nonNaN)
        {
            if (literal is LiteralValue.Null) return null; // matches nothing; no predicate says "false"
            if (ToValue(literal) is not object value) return null;

            bool floating = FloatWidth(column) is not null || value is double;
            if (!floating) return new Piece(Predicate.Compare(column, op, value), true);

            if (value is double d && double.IsNaN(d)) return null;
            // A NaN row satisfies >, >= and != under the total order, and statistics may not show it.
            bool guarded = nonNaN.Contains(column);
            if (!guarded && op is not (CompareOp.Eq or CompareOp.Lt or CompareOp.Le)) return null;

            if (FloatWidth(column) is ArrowTypeId.Float)
            {
                // Statistics of a Float32 column are decimal text, read back as a double within half a Float32 step
                // of the stored value. Widen the bound by one Float32 step so a boundary value is never ruled out.
                if (value is not (double or int or long or short or sbyte or byte or ushort or uint)) return null;
                float bound = (float)Convert.ToDouble(value, System.Globalization.CultureInfo.InvariantCulture);
                Predicate widened = op switch
                {
                    CompareOp.Lt or CompareOp.Le => Predicate.Le(column, (double)MathF.BitIncrement(bound)),
                    CompareOp.Gt or CompareOp.Ge => Predicate.Ge(column, (double)MathF.BitDecrement(bound)),
                    CompareOp.Eq => Predicate.And(
                        Predicate.Ge(column, (double)MathF.BitDecrement(bound)),
                        Predicate.Le(column, (double)MathF.BitIncrement(bound))),
                    _ => Predicate.Compare(column, op, value), // != rules a file out only on equal bounds; widening cannot help
                };
                return op == CompareOp.Ne ? null : new Piece(widened, false);
            }
            if (FloatWidth(column) is ArrowTypeId.HalfFloat) return null;
            return new Piece(Predicate.Compare(column, op, value), guarded);
        }

        private ArrowTypeId? FloatWidth(string column) =>
            types.TryGetValue(column, out LogicalType? type) && Arrow(type) is ArrowTypeId id
            && id is ArrowTypeId.HalfFloat or ArrowTypeId.Float or ArrowTypeId.Double ? id : null;

        private Piece? InList(ScalarExpr target, FSharpList<LiteralValue> values, bool negated, bool nullsEqual, HashSet<string> nonNaN)
        {
            if (ColumnOf(target) is not string column) return null;
            var children = new List<Piece>();
            bool hasNull = false;
            foreach (LiteralValue value in values)
            {
                if (value is LiteralValue.Null) { hasNull = true; continue; }
                if (Compare(column, CompareOp.Eq, value, nonNaN) is not Piece eq) return null;
                children.Add(eq);
            }
            if (hasNull && nullsEqual) children.Add(new Piece(Predicate.IsNull(column), true));
            if (children.Count == 0) return null;

            // Under SQL IN a null element turns a miss into null rather than false; with nullsEqual a null value
            // gives false rather than null. Either way the negation is not the plain complement.
            bool exact = children.All(c => c.Exact) && !hasNull && !nullsEqual;
            var piece = children.Count == 1 ? children[0] with { Exact = exact } : new Piece(Predicate.Or([.. children.Select(c => c.Predicate)]), exact);
            if (!negated) return piece;
            return piece.Exact ? new Piece(Predicate.Not(piece.Predicate), true) : null;
        }

        /// <summary>The column a conjunct keeps away from NaN: <c>x &lt;= +inf</c> or <c>NOT coalesce(isnan(x), false)</c>.</summary>
        private string? NaNGuardColumn(ScalarExpr conjunct)
        {
            switch (conjunct)
            {
                case ScalarExpr.Binary { op.IsLtEq: true } b when ColumnOf(b.left) is string column
                                                                && b.right is ScalarExpr.Literal l && IsPositiveInfinity(l.value):
                    return column;
                case ScalarExpr.Binary { op.IsGtEq: true } b when ColumnOf(b.right) is string column
                                                                && b.left is ScalarExpr.Literal l && IsPositiveInfinity(l.value):
                    return column;
                case ScalarExpr.Unary { op.IsNot: true } u:
                {
                    ScalarExpr inner = u.expr is ScalarExpr.Coalesce c && c.exprs.Length == 2 ? c.exprs.Head : u.expr;
                    return inner is ScalarExpr.Unary { op.IsIsNan: true } isNan ? ColumnOf(isNan.expr) : null;
                }
                default:
                    return null;
            }
        }

        private static bool IsPositiveInfinity(LiteralValue value) => value switch
        {
            LiteralValue.Float64 d => double.IsPositiveInfinity(d.Item),
            LiteralValue.Float32 f => float.IsPositiveInfinity(f.Item),
            LiteralValue.Float16 h => Half.IsPositiveInfinity(h.Item),
            _ => false,
        };

        /// <summary>Splits a comparison into its column and literal; <paramref name="flipped"/> when the literal is on the left.</summary>
        private bool TrySplit(ScalarExpr left, ScalarExpr right, out string? column, out LiteralValue? literal, out bool flipped)
        {
            if (ColumnOf(left) is string l && right is ScalarExpr.Literal rl)
            {
                (column, literal, flipped) = (l, rl.value, false);
                return true;
            }
            if (ColumnOf(right) is string r && left is ScalarExpr.Literal ll)
            {
                (column, literal, flipped) = (r, ll.value, true);
                return true;
            }
            (column, literal, flipped) = (null, null, false);
            return false;
        }

        private static CompareOp Flip(CompareOp op) => op switch
        {
            CompareOp.Lt => CompareOp.Gt,
            CompareOp.Le => CompareOp.Ge,
            CompareOp.Gt => CompareOp.Lt,
            CompareOp.Ge => CompareOp.Le,
            _ => op,
        };

        /// <summary>
        /// The table column an expression reads, seeing through casts that keep the order of every value
        /// (integer to a wider integer of the same signedness or to a signed one wider still, Float32 to Float64).
        /// </summary>
        private string? ColumnOf(ScalarExpr expr)
        {
            switch (expr)
            {
                case ScalarExpr.Column c when types.ContainsKey(c.name):
                    return c.name;
                case ScalarExpr.Cast cast when ColumnOf(cast.expr) is string column
                                            && types.TryGetValue(column, out LogicalType? source)
                                            && KeepsOrder(source, cast.targetType):
                    return column;
                default:
                    return null;
            }
        }

        private static bool KeepsOrder(LogicalType source, LogicalType target)
        {
            if (Arrow(source) is not ArrowTypeId from || Arrow(target) is not ArrowTypeId to) return false;
            if (from == to) return true;
            if (from == ArrowTypeId.Float && to == ArrowTypeId.Double) return true;
            if (IntegerWidth(from) is not (int fromBits, bool fromSigned) || IntegerWidth(to) is not (int toBits, bool toSigned)) return false;
            return fromSigned == toSigned ? toBits >= fromBits : !fromSigned && toSigned && toBits > fromBits;
        }

        private static (int Bits, bool Signed)? IntegerWidth(ArrowTypeId id) => id switch
        {
            ArrowTypeId.Int8 => (8, true),
            ArrowTypeId.Int16 => (16, true),
            ArrowTypeId.Int32 => (32, true),
            ArrowTypeId.Int64 => (64, true),
            ArrowTypeId.UInt8 => (8, false),
            ArrowTypeId.UInt16 => (16, false),
            ArrowTypeId.UInt32 => (32, false),
            ArrowTypeId.UInt64 => (64, false),
            _ => null,
        };

        private static ArrowTypeId? Arrow(LogicalType type) => type is LogicalType.Arrow a ? a.arrowType.TypeId : null;

        private bool IsBoolean(string column) =>
            types.TryGetValue(column, out LogicalType? type) && Arrow(type) is ArrowTypeId.Boolean;

        /// <summary>The literal as the predicate takes it; null for kinds statistics cannot compare.</summary>
        private static object? ToValue(LiteralValue literal) => literal switch
        {
            LiteralValue.Bool v => v.Item,
            LiteralValue.Int8 v => v.Item,
            LiteralValue.Int16 v => v.Item,
            LiteralValue.Int32 v => v.Item,
            LiteralValue.Int64 v => v.Item,
            LiteralValue.UInt8 v => v.Item,
            LiteralValue.UInt16 v => v.Item,
            LiteralValue.UInt32 v => v.Item,
            LiteralValue.UInt64 v => v.Item <= long.MaxValue ? (long)v.Item : null,
            LiteralValue.Float16 v => (double)v.Item,
            LiteralValue.Float32 v => (double)v.Item,
            LiteralValue.Float64 v => v.Item,
            LiteralValue.Decimal v => v.Item,
            LiteralValue.String v => v.Item,
            LiteralValue.Date v => v.Item,
            LiteralValue.DateTime v => v.Item,
            LiteralValue.DateTimeOffset v => v.Item,
            _ => null,
        };

        private static List<ScalarExpr> Conjuncts(ScalarExpr expr) => Flatten(expr, isAnd: true, []);

        private static List<ScalarExpr> Disjuncts(ScalarExpr expr) => Flatten(expr, isAnd: false, []);

        private static List<ScalarExpr> Flatten(ScalarExpr expr, bool isAnd, List<ScalarExpr> into)
        {
            if (expr is ScalarExpr.Binary b && (isAnd ? b.op.IsAnd : b.op.IsOr))
            {
                Flatten(b.left, isAnd, into);
                Flatten(b.right, isAnd, into);
            }
            else
            {
                into.Add(expr);
            }
            return into;
        }
    }
}
