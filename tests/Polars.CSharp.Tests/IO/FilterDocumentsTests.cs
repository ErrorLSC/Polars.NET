#nullable enable
using Kaminari.Samidare.IR;
using Microsoft.FSharp.Collections;
using Polars.NET.Core.IO.Plugins;

namespace Polars.CSharp.Tests;

/// <summary>
/// The engine's pushed filter documents (<c>filter_expr.v1</c>) as Samidare IR: what translates exactly, what only
/// implies the filter, and what has no translation (unknown nodes, literals the IR cannot hold exactly).
/// </summary>
public class FilterDocumentsTests
{
    private static ScalarExpr Col(string name) => ScalarExpr.NewColumn(name);

    private static ScalarExpr Lit(LiteralValue value) => ScalarExpr.NewLiteral(value);

    private static ScalarExpr Bin(BinaryOp op, ScalarExpr left, ScalarExpr right) => ScalarExpr.NewBinary(op, left, right);

    private const string A = """{"col":"a"}""";
    private const string Unknown = """{"unknown":"expression"}""";

    private static string I64(long v) => $$$"""{"lit":{"dtype":"i64","value":"{{{v}}}"}}""";

    private static string Op(string op, params string[] args) => $$"""{"op":"{{op}}","args":[{{string.Join(",", args)}}]}""";

    [Fact]
    public void A_Comparison_Translates_Exactly()
    {
        var (exact, implied) = FilterDocuments.Translate(Op("gt", A, I64(5)));
        var expected = Bin(BinaryOp.Gt, Col("a"), Lit(LiteralValue.NewInt64(5)));
        Assert.Equal(expected, exact);
        Assert.Equal(expected, implied);
    }

    [Theory]
    [InlineData("eq", "Eq")]
    [InlineData("neq", "NotEq")]
    [InlineData("lt", "Lt")]
    [InlineData("lt_eq", "LtEq")]
    [InlineData("gt_eq", "GtEq")]
    [InlineData("eq_missing", "EqMissing")]
    [InlineData("neq_missing", "NotEqMissing")]
    [InlineData("starts_with", "StartsWith")]
    [InlineData("ends_with", "EndsWith")]
    public void Every_Binary_Operator_Maps(string op, string binary)
    {
        var (exact, _) = FilterDocuments.Translate(Op(op, A, I64(1)));
        var node = Assert.IsType<ScalarExpr.Binary>(exact);
        Assert.Equal(binary, node.op.ToString());
    }

    [Fact]
    public void An_And_With_An_Unknown_Side_Only_Implies_Its_Known_Side()
    {
        string known = Op("lt", A, I64(3));
        var (exact, implied) = FilterDocuments.Translate(Op("and", Unknown, known));
        Assert.Null(exact);
        Assert.Equal(Bin(BinaryOp.Lt, Col("a"), Lit(LiteralValue.NewInt64(3))), implied);
    }

    [Fact]
    public void Nested_Ands_Keep_Every_Known_Conjunct()
    {
        string x = Op("lt", A, I64(3)), y = Op("gt", A, I64(1));
        var (exact, implied) = FilterDocuments.Translate(Op("and", Op("and", x, Unknown), y));
        Assert.Null(exact);
        Assert.Equal(
            Bin(BinaryOp.And, Bin(BinaryOp.Lt, Col("a"), Lit(LiteralValue.NewInt64(3))), Bin(BinaryOp.Gt, Col("a"), Lit(LiteralValue.NewInt64(1)))),
            implied);
    }

    [Theory]
    [InlineData("or")]
    [InlineData("not")]
    public void An_Unknown_Under_Or_Or_Not_Leaves_Nothing(string op)
    {
        string document = op == "or" ? Op("or", Unknown, Op("lt", A, I64(3))) : Op("not", Op("and", Unknown, Op("lt", A, I64(3))));
        Assert.Equal((null, null), FilterDocuments.Translate(document));
    }

    [Theory]
    [InlineData("both", "GtEq", "LtEq")]
    [InlineData("left", "GtEq", "Lt")]
    [InlineData("right", "Gt", "LtEq")]
    [InlineData("none", "Gt", "Lt")]
    public void Is_Between_Is_Two_Comparisons(string closed, string lower, string upper)
    {
        string document = $$"""{"op":"is_between","closed":"{{closed}}","args":[{{A}},{{I64(1)}},{{I64(9)}}]}""";
        var (exact, _) = FilterDocuments.Translate(document);
        var and = Assert.IsType<ScalarExpr.Binary>(exact);
        Assert.Equal(BinaryOp.And, and.op);
        Assert.Equal(lower, Assert.IsType<ScalarExpr.Binary>(and.left).op.ToString());
        Assert.Equal(upper, Assert.IsType<ScalarExpr.Binary>(and.right).op.ToString());
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Is_In_Keeps_Its_Values_And_Null_Rule(bool nullsEqual)
    {
        string document = $$$"""{"op":"is_in","args":[{{{A}}}],"values":{"dtype":"i32","values":["1",null,"3"]},"nulls_equal":{{{(nullsEqual ? "true" : "false")}}}}""";
        var (exact, _) = FilterDocuments.Translate(document);
        var expected = ScalarExpr.NewInList(Col("a"),
            InListSource.NewLiterals(ListModule.OfArray([LiteralValue.NewInt32(1), LiteralValue.NewNull(FilterDocuments.DataType("i32")!), LiteralValue.NewInt32(3)])),
            false, nullsEqual);
        Assert.Equal(expected, exact);
    }

    [Fact]
    public void Contains_Literal_Is_A_Literal_String_Search()
    {
        var (exact, _) = FilterDocuments.Translate(Op("contains_literal", A, """{"lit":{"dtype":"str","value":"x.y"}}"""));
        var function = Assert.IsType<ScalarExpr.Function>(exact);
        Assert.Equal(ScalarFunction.StrContains, function.func);
        Assert.Equal(Lit(LiteralValue.NewBool(true)), function.args[2]);
    }

    [Fact]
    public void A_Cast_Is_Read_As_Non_Strict()
    {
        var (exact, _) = FilterDocuments.Translate(Op("gt", """{"op":"cast","dtype":"f64","args":[{"col":"a"}]}""", """{"lit":{"dtype":"f64","value":"1.5"}}"""));
        var cast = Assert.IsType<ScalarExpr.Cast>(Assert.IsType<ScalarExpr.Binary>(exact).left);
        Assert.Equal(LogicalTypes.Float64, cast.targetType);
        Assert.False(cast.isStrict);
    }

    public static TheoryData<string, LiteralValue?> Literals => new()
    {
        { """{"dtype":"bool","value":true}""", LiteralValue.NewBool(true) },
        { """{"dtype":"str","value":"é"}""", LiteralValue.NewString("é") },
        { """{"dtype":"u64","value":"18446744073709551615"}""", LiteralValue.NewUInt64(ulong.MaxValue) },
        { """{"dtype":"f64","value":"NaN"}""", LiteralValue.NewFloat64(double.NaN) },
        { """{"dtype":"f64","value":"-inf"}""", LiteralValue.NewFloat64(double.NegativeInfinity) },
        { """{"dtype":"f32","value":"0.1"}""", LiteralValue.NewFloat32(0.1f) },
        { """{"dtype":"date","value":19723}""", LiteralValue.NewDate(new DateOnly(2024, 1, 1)) },
        { """{"dtype":"time","value":"3600000000000"}""", LiteralValue.NewTimeOnly(new TimeOnly(1, 0)) },
        { """{"dtype":"datetime[us]","value":"1704067200000000"}""", LiteralValue.NewDateTime(new DateTime(2024, 1, 1)) },
        { """{"dtype":"datetime[ms, UTC]","value":"1704067200000"}""", LiteralValue.NewDateTimeOffset(new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero)) },
        { """{"dtype":"duration[ns]","value":"1500"}""", LiteralValue.NewTimeSpan(TimeSpan.FromTicks(15)) },
        { """{"dtype":"decimal[10, 2]","value":"-12345"}""", LiteralValue.NewDecimal(-123.45m) },
        // No exact IR form: below the IR's 100 ns, a zone the literal cannot carry, beyond System.Decimal, 128-bit.
        { """{"dtype":"datetime[ns]","value":"1"}""", null },
        { """{"dtype":"datetime[us, Asia/Tokyo]","value":"0"}""", null },
        { """{"dtype":"decimal[38, 0]","value":"99999999999999999999999999999999999999"}""", null },
        { """{"dtype":"i128","value":"1"}""", null },
    };

    [Theory]
    [MemberData(nameof(Literals))]
    public void Literals_Translate_Exactly_Or_Not_At_All(string literal, LiteralValue? expected)
    {
        var (exact, _) = FilterDocuments.Translate($$$"""{"op":"eq","args":[{{{A}}},{"lit":{{{literal}}}}]}""");
        if (expected is null) Assert.Null(exact);
        else Assert.Equal(Bin(BinaryOp.Eq, Col("a"), Lit(expected)), exact);
    }

    [Fact]
    public void Null_Values_Are_Typed_Nulls()
    {
        var (exact, _) = FilterDocuments.Translate(Op("eq", A, """{"lit":{"dtype":"i16","value":null}}"""));
        Assert.Equal(Bin(BinaryOp.Eq, Col("a"), Lit(LiteralValue.NewNull(FilterDocuments.DataType("i16")!))), exact);
    }

    [Theory]
    [InlineData("not json")]
    [InlineData("""{"op":"eq","args":[{"col":"a"}]}""")]
    [InlineData("""{"op":"frobnicate","args":[{"col":"a"},{"col":"b"}]}""")]
    [InlineData("""{"op":"eq","args":[{"col":"a"},{"lit":{"dtype":"i8","value":"300"}}]}""")]
    public void Documents_It_Cannot_Read_Have_No_Translation(string document)
    {
        Assert.Equal((null, null), FilterDocuments.Translate(document));
    }
}
