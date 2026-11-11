using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using StructType = Kaminari.Seseragi.Lakehouse.Delta.Schema.StructType;
using Kaminari.Seseragi.Lakehouse.Delta.Skipping;
using Kaminari.Seseragi.Lakehouse.Samidare;
using Microsoft.FSharp.Collections;

namespace Seseragi.Lakehouse.Tests;

public class PredicateTranslatorTests
{
    private static LogicalField Field(string name, IArrowType type) => new(name, LogicalType.NewArrow(type), true);

    private static readonly LogicalSchema s_schema = new(ListModule.OfSeq([
        Field("id", Int64Type.Default),
        Field("small", Int32Type.Default),
        Field("name", StringType.Default),
        Field("price", DoubleType.Default),
        Field("ratio", FloatType.Default),
        Field("flag", BooleanType.Default),
    ]));

    private static ScalarExpr Col(string name) => ScalarExpr.NewColumn(name);
    private static ScalarExpr Lit(long v) => ScalarExpr.NewLiteral(LiteralValue.NewInt64(v));
    private static ScalarExpr Lit(double v) => ScalarExpr.NewLiteral(LiteralValue.NewFloat64(v));
    private static ScalarExpr Lit(float v) => ScalarExpr.NewLiteral(LiteralValue.NewFloat32(v));
    private static ScalarExpr Lit(string v) => ScalarExpr.NewLiteral(LiteralValue.NewString(v));
    private static ScalarExpr Bin(BinaryOp op, ScalarExpr l, ScalarExpr r) => ScalarExpr.NewBinary(op, l, r);
    private static ScalarExpr And(ScalarExpr l, ScalarExpr r) => Bin(BinaryOp.And, l, r);
    private static ScalarExpr Or(ScalarExpr l, ScalarExpr r) => Bin(BinaryOp.Or, l, r);
    private static ScalarExpr Not(ScalarExpr e) => ScalarExpr.NewUnary(UnaryOp.Not, e);

    /// <summary>Something the translator cannot read: upper(name) = 'X'.</summary>
    private static readonly ScalarExpr s_opaque = Bin(BinaryOp.Eq,
        ScalarExpr.NewFunction(ScalarFunction.ToUpper, ListModule.OfSeq([Col("name")]), LogicalType.NewArrow(StringType.Default)),
        Lit("X"));

    private static string? T(ScalarExpr filter) => PredicateTranslator.Translate(filter, s_schema)?.ToString();

    [Fact]
    public void Comparisons_FlipALiteralOnTheLeft()
    {
        Assert.Equal("id = 5", T(Bin(BinaryOp.Eq, Col("id"), Lit(5L))));
        Assert.Equal("id > 5", T(Bin(BinaryOp.Lt, Lit(5L), Col("id"))));
        Assert.Equal("name <= 'b'", T(Bin(BinaryOp.GtEq, Lit("b"), Col("name"))));
        Assert.Equal("flag = true", T(Col("flag")));
        Assert.Null(T(Bin(BinaryOp.Eq, Col("id"), Col("small"))));
        Assert.Null(T(Bin(BinaryOp.Eq, Col("missing"), Lit(1L))));
        Assert.Null(T(Bin(BinaryOp.Eq, Col("id"), ScalarExpr.NewLiteral(LiteralValue.NewNull(LogicalType.NewArrow(Int64Type.Default))))));
    }

    [Fact]
    public void Conjunctions_DropWhatTheyCannotRead_ButThenCannotBeNegated()
    {
        Assert.Equal("(id > 5 AND name = 'a')", T(And(Bin(BinaryOp.Gt, Col("id"), Lit(5L)), Bin(BinaryOp.Eq, Col("name"), Lit("a")))));
        Assert.Equal("id > 5", T(And(Bin(BinaryOp.Gt, Col("id"), Lit(5L)), s_opaque)));
        Assert.Null(T(Not(And(Bin(BinaryOp.Gt, Col("id"), Lit(5L)), s_opaque))));
        Assert.Equal("NOT (id > 5 AND id < 9)", T(Not(And(Bin(BinaryOp.Gt, Col("id"), Lit(5L)), Bin(BinaryOp.Lt, Col("id"), Lit(9L))))));
    }

    [Fact]
    public void Disjunctions_NeedEveryBranch()
    {
        Assert.Equal("(id = 1 OR id = 2 OR id IS NULL)",
            T(Or(Or(Bin(BinaryOp.Eq, Col("id"), Lit(1L)), Bin(BinaryOp.Eq, Col("id"), Lit(2L))), ScalarExpr.NewUnary(UnaryOp.IsNull, Col("id")))));
        Assert.Null(T(Or(Bin(BinaryOp.Eq, Col("id"), Lit(1L)), s_opaque)));
    }

    [Fact]
    public void Floats_TranslateUpperComparisonsOnlyUnderTheNaNGuard()
    {
        // NaN is greater than every number in the filter, and statistics may leave it out of the maximum.
        Assert.Null(T(Bin(BinaryOp.Gt, Col("price"), Lit(1.5))));
        Assert.Null(T(Bin(BinaryOp.NotEq, Col("price"), Lit(1.5))));
        Assert.Equal("price < 1.5", T(Bin(BinaryOp.Lt, Col("price"), Lit(1.5))));
        Assert.Null(T(Not(Bin(BinaryOp.Lt, Col("price"), Lit(1.5)))));
        Assert.Null(T(Bin(BinaryOp.Eq, Col("price"), Lit(double.NaN))));

        // The compiler's .NET guard: x > 1.5 AND x <= +inf.
        var guarded = And(Bin(BinaryOp.Gt, Col("price"), Lit(1.5)), Bin(BinaryOp.LtEq, Col("price"), Lit(double.PositiveInfinity)));
        Assert.Equal("(price > 1.5 AND price <= Infinity)", T(guarded));
        Assert.NotNull(T(Not(guarded)));

        // The guard protects the whole conjunction, including a disjunction inside it.
        var nested = And(Or(Bin(BinaryOp.Gt, Col("price"), Lit(9.0)), Bin(BinaryOp.Eq, Col("id"), Lit(1L))),
            Not(ScalarExpr.NewCoalesce(ListModule.OfSeq([ScalarExpr.NewUnary(UnaryOp.IsNan, Col("price")), ScalarExpr.NewLiteral(LiteralValue.NewBool(false))]))));
        Assert.Equal("(price > 9 OR id = 1)", T(nested));
    }

    [Fact]
    public void NullWrappersAndNullSafeEquality_AreNotNegated()
    {
        var wrapped = ScalarExpr.NewCoalesce(ListModule.OfSeq([Bin(BinaryOp.Gt, Col("id"), Lit(5L)), ScalarExpr.NewLiteral(LiteralValue.NewBool(false))]));
        Assert.Equal("id > 5", T(wrapped));
        Assert.Null(T(Not(wrapped)));

        var nullLit = ScalarExpr.NewLiteral(LiteralValue.NewNull(LogicalType.NewArrow(Int64Type.Default)));
        Assert.Equal("id IS NULL", T(Bin(BinaryOp.EqMissing, Col("id"), nullLit)));
        Assert.Equal("id IS NOT NULL", T(Bin(BinaryOp.NotEqMissing, Col("id"), nullLit)));
        Assert.Equal("id = 5", T(Bin(BinaryOp.EqMissing, Col("id"), Lit(5L))));
        Assert.Null(T(Not(Bin(BinaryOp.EqMissing, Col("id"), Lit(5L)))));
        Assert.Null(T(Bin(BinaryOp.NotEqMissing, Col("id"), Lit(5L))));
    }

    [Fact]
    public void InLists_BecomeDisjunctions()
    {
        static ScalarExpr In(bool negated, bool nullsEqual, params LiteralValue[] values) =>
            ScalarExpr.NewInList(Col("id"), InListSource.NewLiterals(ListModule.OfSeq(values)), negated, nullsEqual);

        Assert.Equal("(id = 1 OR id = 2)", T(In(false, false, LiteralValue.NewInt64(1), LiteralValue.NewInt64(2))));
        Assert.Equal("NOT (id = 1 OR id = 2)", T(In(true, false, LiteralValue.NewInt64(1), LiteralValue.NewInt64(2))));
        var nul = LiteralValue.NewNull(LogicalType.NewArrow(Int64Type.Default));
        Assert.Equal("(id = 1 OR id IS NULL)", T(In(false, true, LiteralValue.NewInt64(1), nul)));
        Assert.Null(T(In(true, true, LiteralValue.NewInt64(1), nul)));
        Assert.Null(T(In(true, false, LiteralValue.NewInt64(1), nul)));
    }

    [Fact]
    public void Casts_AreSeenThroughOnlyWhenTheyKeepTheOrder()
    {
        static ScalarExpr Cast(string column, IArrowType to) => ScalarExpr.NewCast(Col(column), LogicalType.NewArrow(to), false);
        Assert.Equal("small > 5", T(Bin(BinaryOp.Gt, Cast("small", Int64Type.Default), Lit(5L))));
        Assert.Null(T(Bin(BinaryOp.Gt, Cast("id", Int32Type.Default), Lit(5L))));
        Assert.Null(T(Bin(BinaryOp.Gt, Cast("id", StringType.Default), Lit("5"))));
    }

    [Fact]
    public void Float32Bounds_AreWidened_SoTheStoredValueIsNeverRuledOut()
    {
        // The writer stored 1.1f as the text 1.1, which reads back as a double that differs from (double)1.1f.
        var schema = StructType.Parse("""
            {"type":"struct","fields":[{"name":"ratio","type":"float","nullable":true,"metadata":{}}]}
            """);
        var file = new AddAction
        {
            Path = "f.parquet",
            PartitionValues = new Dictionary<string, string?>(),
            Stats = """{"numRecords":3,"minValues":{"ratio":0.5},"maxValues":{"ratio":1.1},"nullCount":{"ratio":0}}""",
        };
        bool Might(ScalarExpr filter) =>
            new FileFilter(schema, [], PredicateTranslator.Translate(filter, s_schema)!).MightMatch(file);

        Assert.True(Might(Bin(BinaryOp.Eq, Col("ratio"), Lit(1.1f))));
        Assert.True(Might(And(Bin(BinaryOp.GtEq, Col("ratio"), Lit(1.1f)), Bin(BinaryOp.LtEq, Col("ratio"), Lit(float.PositiveInfinity)))));
        Assert.False(Might(And(Bin(BinaryOp.GtEq, Col("ratio"), Lit(1.2f)), Bin(BinaryOp.LtEq, Col("ratio"), Lit(float.PositiveInfinity)))));
        Assert.True(Might(Bin(BinaryOp.LtEq, Col("ratio"), Lit(0.5f))));
        Assert.False(Might(Bin(BinaryOp.Eq, Col("ratio"), Lit(2.0f))));
        Assert.False(Might(Bin(BinaryOp.Lt, Col("ratio"), Lit(0.25f))));
        // Widened bounds are not negatable.
        Assert.Null(T(Not(Bin(BinaryOp.Eq, Col("ratio"), Lit(1.1f)))));
    }
}
