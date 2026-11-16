using Microsoft.FSharp.Collections;
using Kaminari.Samidare.IR;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// <c>FilterShapes.foldConstants</c>: comparisons between literals decided where the IR's order and the CLR's agree,
/// boolean constants folded in Kleene logic, everything else left as it is.
/// </summary>
public class FilterConstantFoldingTests
{
    private static ScalarExpr Lit(LiteralValue value) => ScalarExpr.NewLiteral(value);

    private static ScalarExpr Bool(bool value) => Lit(LiteralValue.NewBool(value));

    private static ScalarExpr Cmp(BinaryOp op, ScalarExpr a, ScalarExpr b) => ScalarExpr.NewBinary(op, a, b);

    private static readonly ScalarExpr Column = Cmp(BinaryOp.Gt, ScalarExpr.NewColumn("x"), Lit(LiteralValue.NewInt64(1)));

    public static TheoryData<string, bool> Comparisons => new()
    {
        { "Eq 1 1", true }, { "Eq 1 2", false }, { "NotEq 1 2", true }, { "Lt 1 2", true }, { "LtEq 2 2", true },
        { "Gt 1 2", false }, { "GtEq 3 2", true }, { "EqMissing 2 2", true }, { "NotEqMissing 2 2", false },
    };

    [Theory]
    [MemberData(nameof(Comparisons))]
    public void IntegerComparisons_AreDecided(string spec, bool expected)
    {
        string[] parts = spec.Split(' ');
        var op = (BinaryOp)typeof(BinaryOp).GetProperty(parts[0])!.GetValue(null)!;
        ScalarExpr folded = FilterShapes.foldConstants(Cmp(op, Lit(LiteralValue.NewInt64(long.Parse(parts[1]))), Lit(LiteralValue.NewInt64(long.Parse(parts[2])))));
        Assert.Equal(Bool(expected), folded);
    }

    [Fact]
    public void OtherKinds_AreDecidedWhereTheOrdersAgree()
    {
        Assert.Equal(Bool(true), FilterShapes.foldConstants(Cmp(BinaryOp.Eq, Lit(LiteralValue.NewString("a")), Lit(LiteralValue.NewString("a")))));
        Assert.Equal(Bool(true), FilterShapes.foldConstants(Cmp(BinaryOp.NotEq, Lit(LiteralValue.NewString("a")), Lit(LiteralValue.NewString("b")))));
        Assert.Equal(Bool(true), FilterShapes.foldConstants(Cmp(BinaryOp.Lt, Lit(LiteralValue.NewDate(new DateOnly(2024, 1, 1))), Lit(LiteralValue.NewDate(new DateOnly(2024, 1, 2))))));
        Assert.Equal(Bool(false), FilterShapes.foldConstants(Cmp(BinaryOp.Eq, Lit(LiteralValue.NewBool(true)), Lit(LiteralValue.NewBool(false)))));
        // Left as they are: string order (UTF-8 bytes in the IR), floats (NaN ordered), mixed kinds.
        foreach (ScalarExpr kept in (ScalarExpr[])[
                     Cmp(BinaryOp.Lt, Lit(LiteralValue.NewString("a")), Lit(LiteralValue.NewString("b"))),
                     Cmp(BinaryOp.Eq, Lit(LiteralValue.NewFloat64(double.NaN)), Lit(LiteralValue.NewFloat64(double.NaN))),
                     Cmp(BinaryOp.Eq, Lit(LiteralValue.NewInt32(1)), Lit(LiteralValue.NewInt64(1)))])
        {
            Assert.Equal(kept, FilterShapes.foldConstants(kept));
        }
    }

    [Fact]
    public void BooleanConstants_FoldInKleeneLogic()
    {
        ScalarExpr nul = Lit(LiteralValue.NewNull(LogicalType.Unknown));
        Assert.Equal(Bool(false), FilterShapes.foldConstants(Cmp(BinaryOp.And, Column, Bool(false))));
        Assert.Equal(Bool(false), FilterShapes.foldConstants(Cmp(BinaryOp.And, Bool(false), nul)));
        Assert.Equal(Column, FilterShapes.foldConstants(Cmp(BinaryOp.And, Bool(true), Column)));
        Assert.Equal(Bool(true), FilterShapes.foldConstants(Cmp(BinaryOp.Or, Column, Bool(true))));
        Assert.Equal(Bool(true), FilterShapes.foldConstants(Cmp(BinaryOp.Or, nul, Bool(true))));
        Assert.Equal(Column, FilterShapes.foldConstants(Cmp(BinaryOp.Or, Bool(false), Column)));
        Assert.Equal(nul, FilterShapes.foldConstants(Cmp(BinaryOp.And, Bool(true), nul)));
        Assert.Equal(Bool(false), FilterShapes.foldConstants(ScalarExpr.NewUnary(UnaryOp.Not, Bool(true))));
        // Nested: (1 = 1 AND x > 1) OR (1 = 2 AND x < 5) → x > 1.
        ScalarExpr one = Lit(LiteralValue.NewInt64(1)), two = Lit(LiteralValue.NewInt64(2));
        ScalarExpr nested = Cmp(BinaryOp.Or, Cmp(BinaryOp.And, Cmp(BinaryOp.Eq, one, one), Column),
            Cmp(BinaryOp.And, Cmp(BinaryOp.Eq, one, two), Cmp(BinaryOp.Lt, ScalarExpr.NewColumn("x"), Lit(LiteralValue.NewInt64(5)))));
        Assert.Equal(Column, FilterShapes.foldConstants(nested));
        Assert.Equal(Column, FilterShapes.foldConstants(Column));
    }

    [Fact]
    public void NullLiterals_FoldInKleeneLogic()
    {
        // A column a file lacks is put in as a null: comparisons with it are null, null-safe ones and IS NULL decided.
        ScalarExpr nul = Lit(LiteralValue.NewNull(LogicalType.Unknown)), one = Lit(LiteralValue.NewInt64(1)), x = ScalarExpr.NewColumn("x");
        ScalarExpr Null() => Lit(LiteralValue.NewNull(LogicalTypes.Boolean));
        foreach (BinaryOp op in (BinaryOp[])[BinaryOp.Eq, BinaryOp.NotEq, BinaryOp.Lt, BinaryOp.LtEq, BinaryOp.Gt, BinaryOp.GtEq])
        {
            Assert.Equal(Null(), FilterShapes.foldConstants(Cmp(op, nul, one)));
            Assert.Equal(Null(), FilterShapes.foldConstants(Cmp(op, x, nul)));
        }
        Assert.Equal(Bool(true), FilterShapes.foldConstants(Cmp(BinaryOp.EqMissing, nul, nul)));
        Assert.Equal(Bool(false), FilterShapes.foldConstants(Cmp(BinaryOp.EqMissing, nul, one)));
        Assert.Equal(Bool(true), FilterShapes.foldConstants(Cmp(BinaryOp.NotEqMissing, one, nul)));
        Assert.Equal(ScalarExpr.NewUnary(UnaryOp.IsNull, x), FilterShapes.foldConstants(Cmp(BinaryOp.EqMissing, x, nul)));
        Assert.Equal(ScalarExpr.NewUnary(UnaryOp.IsNotNull, x), FilterShapes.foldConstants(Cmp(BinaryOp.NotEqMissing, nul, x)));
        Assert.Equal(Bool(true), FilterShapes.foldConstants(ScalarExpr.NewUnary(UnaryOp.IsNull, nul)));
        Assert.Equal(Bool(false), FilterShapes.foldConstants(ScalarExpr.NewUnary(UnaryOp.IsNull, one)));
        Assert.Equal(Bool(false), FilterShapes.foldConstants(ScalarExpr.NewUnary(UnaryOp.IsNotNull, nul)));
        Assert.Equal(Null(), FilterShapes.foldConstants(ScalarExpr.NewUnary(UnaryOp.Not, Cmp(BinaryOp.Gt, x, nul))));
        // Kleene: false wins an AND, true an OR; two nulls make a null; a null beside a column stays.
        Assert.Equal(Bool(false), FilterShapes.foldConstants(Cmp(BinaryOp.And, Cmp(BinaryOp.Eq, x, nul), Bool(false))));
        Assert.Equal(Null(), FilterShapes.foldConstants(Cmp(BinaryOp.Or, Cmp(BinaryOp.Eq, x, nul), Cmp(BinaryOp.Lt, nul, one))));
        Assert.Equal(Cmp(BinaryOp.And, Null(), Column), FilterShapes.foldConstants(Cmp(BinaryOp.And, Cmp(BinaryOp.Gt, nul, one), Column)));
        // IN of a null: null, or decided when nulls are equal.
        ScalarExpr In(bool negated, bool nullsEqual, params LiteralValue[] values) =>
            ScalarExpr.NewInList(nul, InListSource.NewLiterals(ListModule.OfSeq(values)), negated, nullsEqual);
        Assert.Equal(Null(), FilterShapes.foldConstants(In(false, false, LiteralValue.NewInt64(1))));
        Assert.Equal(Bool(true), FilterShapes.foldConstants(In(false, true, LiteralValue.NewInt64(1), LiteralValue.NewNull(LogicalType.Unknown))));
        Assert.Equal(Bool(true), FilterShapes.foldConstants(In(true, true, LiteralValue.NewInt64(1))));
    }
}
