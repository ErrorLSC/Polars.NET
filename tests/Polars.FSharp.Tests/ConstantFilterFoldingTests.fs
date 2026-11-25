namespace Polars.FSharp.Tests

open Xunit
open Kaminari.Samidare.IR
open Kaminari.Samidare.IR.ScalarExprOps
open Kaminari.Samidare.IR.Passes

/// ConstantFilterFolding over hand-built plans: folded predicates, and the UNION ALL branches a constant filter empties.
module ConstantFilterFoldingTests =

    let private salesSchema = LogicalSchema.OfFields [ ("s_k", LogicalTypes.Int32); ("s_v", LogicalTypes.Int64) ]
    let private catalogSchema = LogicalSchema.OfFields [ ("c_k", LogicalTypes.Int32); ("c_v", LogicalTypes.Int32) ]

    let private sales = LogicalPlan.Scan(LogicalPlanSource.Slot 0, salesSchema)
    let private catalog = LogicalPlan.Scan(LogicalPlanSource.Slot 1, catalogSchema)

    let private str (value: string) = ScalarExpr.Literal(LiteralValue.String value)
    let private int32 (value: int) = ScalarExpr.Literal(LiteralValue.Int32 value)
    let private int64 (value: int64) = ScalarExpr.Literal(LiteralValue.Int64 value)
    let private eq left right = ScalarExpr.Binary(BinaryOp.Eq, left, right)

    /// A channel's branch as a SQL union over channels reads after pushdown: its rows, marked by a literal channel.
    let private branch input key value (channel: string) filterChannel =
        LogicalPlan.Project(LogicalPlan.Filter(input, eq (str channel) (str filterChannel)), [ "k", col key; "v", col value ])

    let private fold = ConstantFilterFolding.apply

    [<Fact>]
    let ``Arithmetic between literals becomes a literal bound`` () =
        let bound = ScalarExpr.Cast(ScalarExpr.Binary(BinaryOp.Add, int32 1999, int32 2), LogicalTypes.Int64, false)
        let plan = LogicalPlan.Filter(sales, ScalarExpr.Binary(BinaryOp.LtEq, col "s_v", bound))
        Assert.Equal(LogicalPlan.Filter(sales, ScalarExpr.Binary(BinaryOp.LtEq, col "s_v", int64 2001L)), fold plan)

    [<Fact>]
    let ``A filter folded to true goes away`` () =
        Assert.Equal(sales, fold (LogicalPlan.Filter(sales, eq (str "s") (str "s"))))

    [<Fact>]
    let ``An emptied right branch leaves the left`` () =
        let plan = LogicalPlan.Union(branch sales "s_k" "s_v" "s" "s", branch catalog "c_k" "c_v" "c" "s", true)
        Assert.Equal(LogicalPlan.Project(sales, [ "k", col "s_k"; "v", col "s_v" ]), fold plan)

    [<Fact>]
    let ``An emptied left branch leaves the right with the left's types`` () =
        let plan = LogicalPlan.Union(branch sales "s_k" "s_v" "s" "c", branch catalog "c_k" "c_v" "c" "c", true)
        let right = LogicalPlan.Project(catalog, [ "k", col "c_k"; "v", col "c_v" ])
        let expected = LogicalPlan.Project(right, [ "k", col "k"; "v", ScalarExpr.Cast(col "v", LogicalTypes.Int64, false) ])
        Assert.Equal(expected, fold plan)

    [<Fact>]
    let ``A limit of no rows empties its branch`` () =
        let none = LogicalPlan.Project(LogicalPlan.Limit(catalog, 0L, 0u), [ "k", col "c_k"; "v", col "c_v" ])
        let plan = LogicalPlan.Union(branch sales "s_k" "s_v" "s" "s", none, true)
        Assert.Equal(LogicalPlan.Project(sales, [ "k", col "s_k"; "v", col "s_v" ]), fold plan)

    [<Fact>]
    let ``Branches of nested unions go`` () =
        let plan =
            LogicalPlan.Union(
                LogicalPlan.Union(branch sales "s_k" "s_v" "s" "s", branch catalog "c_k" "c_v" "c" "s", true),
                branch sales "s_k" "s_v" "w" "s",
                true
            )
        Assert.Equal(LogicalPlan.Project(sales, [ "k", col "s_k"; "v", col "s_v" ]), fold plan)

    [<Fact>]
    let ``An aggregate over no rows is one row and stays`` () =
        let total column = ScalarExpr.Aggregation(AggKind.SumOrNull, Some(col column), false)
        let plan = LogicalPlan.Union(LogicalPlan.Project(LogicalPlan.Filter(sales, eq (str "c") (str "s")), [ "n", total "s_v" ]), LogicalPlan.Project(catalog, [ "n", total "c_v" ]), true)
        let folded = LogicalPlan.Project(LogicalPlan.Filter(sales, ScalarExpr.Literal(LiteralValue.Bool false)), [ "n", total "s_v" ])
        Assert.Equal(LogicalPlan.Union(folded, LogicalPlan.Project(catalog, [ "n", total "c_v" ]), true), fold plan)

    [<Fact>]
    let ``A grouping over no rows stays`` () =
        let grouped =
            LogicalPlan.GroupBy(LogicalPlan.Filter(sales, ScalarExpr.Literal(LiteralValue.Bool false)), [], [ "n", ScalarExpr.Aggregation(AggKind.Len, None, false) ], None, true)
        let other = LogicalPlan.Project(catalog, [ "n", ScalarExpr.Aggregation(AggKind.Len, None, false) ])
        let plan = LogicalPlan.Union(grouped, other, true)
        Assert.Equal(plan, fold plan)

    [<Fact>]
    let ``A projection of literals alone is not known to be empty`` () =
        let literal = LogicalPlan.Project(LogicalPlan.Filter(sales, ScalarExpr.Literal(LiteralValue.Bool false)), [ "k", int32 1; "v", int64 2L ])
        let plan = LogicalPlan.Union(literal, branch catalog "c_k" "c_v" "c" "c", true)
        Assert.Equal(LogicalPlan.Union(literal, LogicalPlan.Project(catalog, [ "k", col "c_k"; "v", col "c_v" ]), true), fold plan)

    [<Fact>]
    let ``A union without ALL keeps its branches`` () =
        let plan = LogicalPlan.Union(branch sales "s_k" "s_v" "s" "s", branch catalog "c_k" "c_v" "c" "s", false)
        let expected =
            LogicalPlan.Union(
                LogicalPlan.Project(sales, [ "k", col "s_k"; "v", col "s_v" ]),
                LogicalPlan.Project(LogicalPlan.Filter(catalog, ScalarExpr.Literal(LiteralValue.Bool false)), [ "k", col "c_k"; "v", col "c_v" ]),
                false
            )
        Assert.Equal(expected, fold plan)

    [<Fact>]
    let ``A null filter empties its branch`` () =
        let plan =
            LogicalPlan.Union(
                branch sales "s_k" "s_v" "s" "s",
                LogicalPlan.Project(LogicalPlan.Filter(catalog, eq (col "c_k") (ScalarExpr.Literal(LiteralValue.Null LogicalTypes.Int32))), [ "k", col "c_k"; "v", col "c_v" ]),
                true
            )
        // A comparison with a null is null on every row: the branch keeps nothing.
        Assert.Equal(LogicalPlan.Project(sales, [ "k", col "s_k"; "v", col "s_v" ]), fold plan)
