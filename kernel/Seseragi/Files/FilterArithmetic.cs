using Kaminari.Samidare.IR;

namespace Kaminari.Seseragi.Files;

/// <summary>
/// Where a filter computes arithmetic, whose result depends on the width it is computed in (comparisons do not): a
/// column stored narrower than the type the filter means must be cast up before, or the conjunct left to the rows.
/// </summary>
internal static class FilterArithmetic
{
    /// <summary>
    /// Whether values stored as <paramref name="stored"/> are values of <paramref name="meant"/> for arithmetic: flat
    /// types as the kernels compare them (dictionaries by their values), nested ones field by field.
    /// </summary>
    public static bool SameValues(Apache.Arrow.Types.IArrowType stored, Apache.Arrow.Types.IArrowType meant) =>
        Kaminari.Sazanami.Expressions.ValueTypes.ValueType(stored) is Apache.Arrow.Types.NestedType || meant is Apache.Arrow.Types.NestedType
            ? LogicalType.NewArrow(stored).Equals(LogicalType.NewArrow(meant))
            : Kaminari.Sazanami.Expressions.ValueTypes.Same(stored, meant);

    /// <summary>The columns read under an arithmetic node of <paramref name="expr"/> (empty when it computes none).</summary>
    public static HashSet<string> Columns(ScalarExpr expr)
    {
        HashSet<string> columns = new(StringComparer.Ordinal);
        Visit(expr, false, columns);
        return columns;

        static void Visit(ScalarExpr e, bool under, HashSet<string> columns)
        {
            switch (e)
            {
                case ScalarExpr.Column c:
                    if (under) columns.Add(c.name);
                    return;
                case ScalarExpr.Binary b:
                {
                    bool arithmetic = under || b.op.IsAdd || b.op.IsSubtract || b.op.IsMultiply || b.op.IsDivide || b.op.IsTruncatedDivide
                                      || b.op.IsFloorDivide || b.op.IsModulo || b.op.IsPower;
                    Visit(b.left, arithmetic, columns);
                    Visit(b.right, arithmetic, columns);
                    return;
                }
                case ScalarExpr.Unary u:
                    Visit(u.expr, under || u.op.IsNegate || u.op.IsAbs, columns);
                    return;
                case ScalarExpr.Cast c:
                    Visit(c.expr, under, columns);
                    return;
                case ScalarExpr.Alias a:
                    Visit(a.expr, under, columns);
                    return;
                case ScalarExpr.Coalesce c:
                    foreach (ScalarExpr x in c.exprs) Visit(x, under, columns);
                    return;
                case ScalarExpr.InList l:
                    Visit(l.expr, under, columns);
                    return;
                case ScalarExpr.Function f:
                    foreach (ScalarExpr x in f.args) Visit(x, under, columns);
                    return;
            }
        }
    }
}
