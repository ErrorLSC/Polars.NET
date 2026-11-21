using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Skipping;
using Microsoft.FSharp.Collections;
using StructType = Kaminari.Seseragi.Lakehouse.Delta.Schema.StructType;

namespace Seseragi.Lakehouse.Tests;

/// <summary>File skipping from filters of the query compiler's IR, by its semantics (SEMANTICS.md).</summary>
public class IrFileFilterTests
{
    private static readonly StructType s_schema = StructType.Parse("""
        {"type":"struct","fields":[
          {"name":"id","type":"long","nullable":true,"metadata":{}},
          {"name":"small","type":"integer","nullable":true,"metadata":{}},
          {"name":"name","type":"string","nullable":true,"metadata":{}},
          {"name":"price","type":"double","nullable":true,"metadata":{}},
          {"name":"ratio","type":"float","nullable":true,"metadata":{}},
          {"name":"flag","type":"boolean","nullable":true,"metadata":{}},
          {"name":"ts","type":"timestamp_ntz","nullable":true,"metadata":{}},
          {"name":"amount","type":"decimal(10,2)","nullable":true,"metadata":{}},
          {"name":"region","type":"string","nullable":true,"metadata":{}}
        ]}
        """);

    // id 10..20, small 1..3, name bob..dave, price 1.5..9.5, ratio 0.5..1.1 (a float written as text), ts one day,
    // amount 2.50..7.25, flag all null; partition region = east.
    private static readonly AddAction s_file = new()
    {
        Path = "f.parquet",
        PartitionValues = new Dictionary<string, string?> { ["region"] = "east" },
        Stats = """
            {"numRecords":100,
             "minValues":{"id":10,"small":1,"name":"bob","price":1.5,"ratio":0.5,"ts":"2024-01-01T00:00:00.000","amount":2.50},
             "maxValues":{"id":20,"small":3,"name":"dave","price":9.5,"ratio":1.1,"ts":"2024-01-01T23:59:59.999","amount":7.25},
             "nullCount":{"id":0,"small":0,"name":0,"price":0,"ratio":0,"flag":100,"ts":0,"amount":0}}
            """,
    };

    private static bool Might(ScalarExpr filter) => new FileFilter(s_schema, ["region"], filter).MightMatch(s_file);

    private static ScalarExpr Col(string name) => ScalarExpr.NewColumn(name);
    private static ScalarExpr Lit(long v) => ScalarExpr.NewLiteral(LiteralValue.NewInt64(v));
    private static ScalarExpr Lit(int v) => ScalarExpr.NewLiteral(LiteralValue.NewInt32(v));
    private static ScalarExpr Lit(double v) => ScalarExpr.NewLiteral(LiteralValue.NewFloat64(v));
    private static ScalarExpr Lit(float v) => ScalarExpr.NewLiteral(LiteralValue.NewFloat32(v));
    private static ScalarExpr Lit(string v) => ScalarExpr.NewLiteral(LiteralValue.NewString(v));
    private static ScalarExpr Bin(BinaryOp op, ScalarExpr l, ScalarExpr r) => ScalarExpr.NewBinary(op, l, r);
    private static ScalarExpr And(ScalarExpr l, ScalarExpr r) => Bin(BinaryOp.And, l, r);
    private static ScalarExpr Or(ScalarExpr l, ScalarExpr r) => Bin(BinaryOp.Or, l, r);
    private static ScalarExpr Not(ScalarExpr e) => ScalarExpr.NewUnary(UnaryOp.Not, e);
    private static ScalarExpr NullLiteral => ScalarExpr.NewLiteral(LiteralValue.NewNull(LogicalType.NewArrow(Int64Type.Default)));

    /// <summary>Something no statistics answer: upper(name) = 'X'.</summary>
    private static readonly ScalarExpr s_opaque = Bin(BinaryOp.Eq,
        ScalarExpr.NewFunction(ScalarFunction.ToUpper, ListModule.OfSeq([Col("name")]), LogicalType.NewArrow(StringType.Default)),
        Lit("X"));

    /// <summary>The C# front end's NaN guard for x: NOT coalesce(isnan(x), false).</summary>
    private static ScalarExpr NotNaN(string column) => Not(ScalarExpr.NewCoalesce(ListModule.OfSeq([
        ScalarExpr.NewUnary(UnaryOp.IsNan, Col(column)), ScalarExpr.NewLiteral(LiteralValue.NewBool(false))])));

    [Fact]
    public void Comparisons_EitherWayRound()
    {
        Assert.False(Might(Bin(BinaryOp.Eq, Col("id"), Lit(5L))));
        Assert.True(Might(Bin(BinaryOp.Eq, Col("id"), Lit(15L))));
        Assert.False(Might(Bin(BinaryOp.Lt, Lit(20L), Col("id"))));
        Assert.True(Might(Bin(BinaryOp.Lt, Lit(19L), Col("id"))));
        Assert.False(Might(Bin(BinaryOp.GtEq, Lit("a"), Col("name"))));
        Assert.True(Might(Bin(BinaryOp.Eq, Col("missing"), Lit(1L))));
        // A comparison with null is null on every row.
        Assert.False(Might(Bin(BinaryOp.Eq, Col("id"), NullLiteral)));
        // A boolean column as a filter: flag is null in every row.
        Assert.False(Might(Col("flag")));
    }

    [Fact]
    public void PartitionValues_AreExact()
    {
        Assert.True(Might(Bin(BinaryOp.Eq, Col("region"), Lit("east"))));
        Assert.False(Might(Bin(BinaryOp.NotEq, Col("region"), Lit("east"))));
        Assert.False(Might(ScalarExpr.NewUnary(UnaryOp.IsNull, Col("region"))));
    }

    [Fact]
    public void WhatNoStatisticsAnswer_KeepsTheFile_UnlessTheRestRulesItOut()
    {
        Assert.True(Might(s_opaque));
        Assert.False(Might(And(Bin(BinaryOp.Gt, Col("id"), Lit(20L)), s_opaque)));
        Assert.True(Might(Not(And(Bin(BinaryOp.Gt, Col("id"), Lit(20L)), s_opaque))));
        Assert.True(Might(Or(Bin(BinaryOp.Eq, Col("id"), Lit(1L)), s_opaque)));
        Assert.False(Might(Or(Bin(BinaryOp.Eq, Col("id"), Lit(1L)), Bin(BinaryOp.Eq, Col("id"), Lit(2L)))));
        Assert.False(Might(Not(And(Bin(BinaryOp.GtEq, Col("id"), Lit(10L)), Bin(BinaryOp.LtEq, Col("id"), Lit(20L))))));
    }

    [Fact]
    public void FloatingUpperBounds_HoldOnlyUnderANaNGuard()
    {
        // NaN is greater than every number and the log counts no NaN.
        Assert.True(Might(Bin(BinaryOp.Gt, Col("price"), Lit(9.5))));
        Assert.False(Might(And(Bin(BinaryOp.Gt, Col("price"), Lit(9.5)), NotNaN("price"))));
        Assert.False(Might(And(Bin(BinaryOp.Gt, Col("price"), Lit(9.5)), Bin(BinaryOp.LtEq, Col("price"), Lit(double.PositiveInfinity)))));
        Assert.False(Might(Bin(BinaryOp.Lt, Col("price"), Lit(1.5))));
        // The guard protects the whole conjunction, a disjunction inside it too.
        Assert.False(Might(And(Or(Bin(BinaryOp.Gt, Col("price"), Lit(9.5)), Bin(BinaryOp.Eq, Col("id"), Lit(1L))), NotNaN("price"))));
        Assert.True(Might(ScalarExpr.NewUnary(UnaryOp.IsNan, Col("price"))));
    }

    [Fact]
    public void NullSafeEquality_AndInLists()
    {
        Assert.False(Might(Bin(BinaryOp.EqMissing, Col("id"), NullLiteral)));
        Assert.True(Might(Bin(BinaryOp.NotEqMissing, Col("id"), NullLiteral)));
        Assert.False(Might(Bin(BinaryOp.EqMissing, Col("id"), Lit(5L))));
        Assert.True(Might(Bin(BinaryOp.EqMissing, Col("id"), Lit(15L))));

        static ScalarExpr In(bool negated, bool nullsEqual, params LiteralValue[] values) =>
            ScalarExpr.NewInList(Col("id"), InListSource.NewLiterals(ListModule.OfSeq(values)), negated, nullsEqual);
        Assert.False(Might(In(false, false, LiteralValue.NewInt64(1), LiteralValue.NewInt64(2))));
        Assert.True(Might(In(false, false, LiteralValue.NewInt64(1), LiteralValue.NewInt64(12))));
        Assert.True(Might(In(true, false, LiteralValue.NewInt64(1), LiteralValue.NewInt64(2))));
        var nul = LiteralValue.NewNull(LogicalType.NewArrow(Int64Type.Default));
        Assert.False(Might(In(false, true, LiteralValue.NewInt64(1), nul)));
        Assert.False(Might(In(true, false, LiteralValue.NewInt64(1), nul)), "NOT IN with a null is never true");
    }

    [Fact]
    public void Casts_AreSeenThroughOnlyWhenTheyKeepEveryValue()
    {
        static ScalarExpr Cast(string column, IArrowType to) => ScalarExpr.NewCast(Col(column), LogicalType.NewArrow(to), false);
        var dayAfter = ScalarExpr.NewLiteral(LiteralValue.NewDateTime(new DateTime(2024, 1, 2)));
        Assert.False(Might(Bin(BinaryOp.GtEq, Cast("ts", new TimestampType(TimeUnit.Microsecond, (string?)null)), dayAfter)));
        // cast(ts as s) rounds: it is not seen through, so the file stays.
        Assert.True(Might(Bin(BinaryOp.GtEq, Cast("ts", new TimestampType(TimeUnit.Second, (string?)null)), dayAfter)));
        Assert.False(Might(Bin(BinaryOp.Gt, Cast("small", Int64Type.Default), Lit(5L))));
        Assert.True(Might(Bin(BinaryOp.Gt, Cast("id", StringType.Default), Lit("5"))));
        var price = ScalarExpr.NewLiteral(LiteralValue.NewDecimal(9.99m));
        Assert.False(Might(Bin(BinaryOp.Gt, Cast("amount", new Decimal128Type(20, 2)), price)));
    }

    [Fact]
    public void TimestampMaximum_AllowsForTheCutMicroseconds()
    {
        // The maximum 23:59:59.999 may stand for any value up to .999999.
        var almost = ScalarExpr.NewLiteral(LiteralValue.NewDateTime(new DateTime(2024, 1, 1, 23, 59, 59).AddTicks(9_999_990)));
        Assert.True(Might(Bin(BinaryOp.GtEq, Col("ts"), almost)));
        var next = ScalarExpr.NewLiteral(LiteralValue.NewDateTime(new DateTime(2024, 1, 2)));
        Assert.False(Might(Bin(BinaryOp.GtEq, Col("ts"), next)));
    }

    [Fact]
    public void Arithmetic_ColumnPairs_Prefixes()
    {
        // id + 1 lies in 11..21; small * 10 in 10..30.
        Assert.False(Might(Bin(BinaryOp.Gt, Bin(BinaryOp.Add, Col("id"), Lit(1L)), Lit(21L))));
        Assert.True(Might(Bin(BinaryOp.Gt, Bin(BinaryOp.Add, Col("id"), Lit(1L)), Lit(20L))));
        Assert.False(Might(Bin(BinaryOp.Lt, Bin(BinaryOp.Multiply, Col("small"), Lit(10)), Lit(10))));
        // Columns against each other: small (1..3) is never above id (10..20).
        Assert.False(Might(Bin(BinaryOp.Gt, ScalarExpr.NewCast(Col("small"), LogicalType.NewArrow(Int64Type.Default), false), Col("id"))));
        Assert.True(Might(Bin(BinaryOp.Lt, ScalarExpr.NewCast(Col("small"), LogicalType.NewArrow(Int64Type.Default), false), Col("id"))));
        // Prefixes.
        Assert.False(Might(Bin(BinaryOp.StartsWith, Col("name"), Lit("e"))));
        Assert.True(Might(Bin(BinaryOp.StartsWith, Col("name"), Lit("c"))));
        Assert.True(Might(Bin(BinaryOp.StartsWith, Col("name"), Lit("bo"))));
        // As the C# front end writes it: a call, read in its canonical shape.
        Assert.False(Might(ScalarExpr.NewFunction(ScalarFunction.StartsWith, ListModule.OfSeq([Col("name"), Lit("e")]), LogicalType.NewArrow(BooleanType.Default))));
    }

    [Fact]
    public void Float32Bounds_ReadAsTheFloatsTheyWere()
    {
        // The writer stored 1.1f as the text 1.1: the bound is the float 1.1f, not the double 1.1.
        Assert.True(Might(Bin(BinaryOp.Eq, Col("ratio"), Lit(1.1f))));
        Assert.True(Might(And(Bin(BinaryOp.GtEq, Col("ratio"), Lit(1.1f)), NotNaN("ratio"))));
        Assert.False(Might(And(Bin(BinaryOp.GtEq, Col("ratio"), Lit(1.2f)), NotNaN("ratio"))));
        Assert.True(Might(Bin(BinaryOp.LtEq, Col("ratio"), Lit(0.5f))));
        Assert.False(Might(Bin(BinaryOp.Eq, Col("ratio"), Lit(2.0f))));
        Assert.False(Might(Bin(BinaryOp.Lt, Col("ratio"), Lit(0.25f))));
    }
}
