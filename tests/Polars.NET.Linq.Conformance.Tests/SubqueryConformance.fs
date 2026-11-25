/// Contract: subqueries (SEMANTICS.md, Subqueries). A subquery's plan is evaluated for each row of the node holding
/// it, its OuterColumns reading that row. Scalar gives the single value (null for no row; more rows fail), Exists
/// whether a row exists (never null), In the membership of its target (nullsEqual: .NET Contains; otherwise SQL IN).
/// Aggregates over no rows keep their rules: Len and Sum 0, the rest null, List empty. Correlation is the predicate
/// as written (Eq never matches null, EqMissing does). The Decorrelation pass replaces each subquery by joins and
/// reports the shapes it cannot replace.
module Polars.NET.Linq.Conformance.Tests.SubqueryConformance

open System
open Xunit
open Kaminari.Samidare.IR

type Cust = { Id: int64; State: string; Credit: Nullable<float> }
type Ord = { OrderId: int64; Cust: Nullable<int64>; Amount: Nullable<float>; Day: int64 }
type PK = { Id: int64; K: Nullable<int64>; G: Nullable<int64> }
type QK = { K: Nullable<int64>; G: Nullable<int64> }

let private custs: Cust array =
    [|
        { Id = 1L; State = "CA"; Credit = Nullable 100.0 }
        { Id = 2L; State = "NY"; Credit = Nullable 50.0 }
        { Id = 3L; State = "CA"; Credit = Nullable() }
        { Id = 4L; State = "TX"; Credit = Nullable 10.0 }
        { Id = 5L; State = "NY"; Credit = Nullable 20.0 }
    |]

let private orders: Ord array =
    [|
        { OrderId = 1L; Cust = n 1L; Amount = Nullable 120.0; Day = 1L }
        { OrderId = 2L; Cust = n 1L; Amount = Nullable 30.0; Day = 2L }
        { OrderId = 3L; Cust = n 2L; Amount = Nullable 40.0; Day = 1L }
        { OrderId = 4L; Cust = n 2L; Amount = Nullable(); Day = 3L }
        { OrderId = 5L; Cust = nul; Amount = Nullable 70.0; Day = 1L }
        { OrderId = 6L; Cust = n 4L; Amount = Nullable 5.0; Day = 2L }
    |]

let private outer (name: string) (t: LogicalType) = ScalarExpr.OuterColumn(1, name, t)
let private custId = outer "Id" LogicalTypes.Int64
let private f64 (v: float) = ScalarExpr.Literal(LiteralValue.Float64 v)
let private scalar plan = ScalarExpr.Subquery(1, plan, SubqueryKind.Scalar)
/// A scalar subquery of its own scope, for several in one node.
let private scalarAt scope plan = ScalarExpr.Subquery(scope, plan, SubqueryKind.Scalar)
let private exists plan = ScalarExpr.Subquery(1, plan, SubqueryKind.Exists)
let private isIn target nullsEqual plan = ScalarExpr.Subquery(1, plan, SubqueryKind.In(target, nullsEqual))
let private ids (values: int64 list) = values |> List.map (fun v -> [ box v ])

/// The orders of the customer the subquery is evaluated for.
let private ordersOf (t: Tables) = filter (t.Scan orders) (bin BinaryOp.Eq (col "Cust") custId)

/// The same, for the subquery with the given scope.
let private ordersAt (t: Tables) scope =
    filter (t.Scan orders) (bin BinaryOp.Eq (col "Cust") (ScalarExpr.OuterColumn(scope, "Id", LogicalTypes.Int64)))

let private customersWhere (t: Tables) predicate = project (filter (t.Scan custs) predicate) [ "Id", col "Id" ]

[<Fact>]
let ``Exists and Not Exists test whether the correlated rows exist`` () =
    let t = Tables()
    Conformance.rowsInOrder t (customersWhere t (exists (ordersOf t))) (ids [ 1L; 2L; 4L ])
    let t = Tables()
    Conformance.rowsInOrder t (customersWhere t (not' (exists (ordersOf t)))) (ids [ 3L; 5L ])

[<Fact>]
let ``A correlation reading both rows beyond equality (theta)`` () =
    let theta (t: Tables) =
        filter (t.Scan orders) (bin BinaryOp.And (bin BinaryOp.Eq (col "Cust") custId) (bin BinaryOp.Gt (col "Amount") (outer "Credit" LogicalTypes.Float64)))
    let t = Tables()
    Conformance.rowsInOrder t (customersWhere t (exists (theta t))) (ids [ 1L ])
    let t = Tables()
    Conformance.rowsInOrder t (customersWhere t (not' (exists (theta t)))) (ids [ 2L; 3L; 4L; 5L ])

[<Fact>]
let ``A condition on the outer row alone inside the subquery`` () =
    let caOrders (t: Tables) =
        filter (t.Scan orders) (bin BinaryOp.And (bin BinaryOp.Eq (col "Cust") custId) (bin BinaryOp.Eq (outer "State" LogicalTypes.String) (str "CA")))
    let t = Tables()
    Conformance.rowsInOrder t (customersWhere t (exists (caOrders t))) (ids [ 1L ])
    let t = Tables()
    Conformance.rowsInOrder t (customersWhere t (not' (exists (caOrders t)))) (ids [ 2L; 3L; 4L; 5L ])

[<Fact>]
let ``Exists under OR (TPC-DS Q10, Q35)`` () =
    let t = Tables()
    let day3 = filter (t.Scan orders) (bin BinaryOp.And (bin BinaryOp.Eq (col "Cust") custId) (bin BinaryOp.Eq (col "Day") (i64 3L)))
    Conformance.rowsInOrder t (customersWhere t (bin BinaryOp.Or (exists day3) (bin BinaryOp.Eq (col "State") (str "CA")))) (ids [ 1L; 2L; 3L ])

[<Fact>]
let ``Exists OR Exists over the same outer key (TPC-DS Q10, Q35)`` () =
    let ordersWhere (t: Tables) scope extra =
        filter (t.Scan orders) (bin BinaryOp.And (bin BinaryOp.Eq (col "Cust") (ScalarExpr.OuterColumn(scope, "Id", LogicalTypes.Int64))) extra)
    let either (t: Tables) =
        bin BinaryOp.Or
            (ScalarExpr.Subquery(1, ordersWhere t 1 (bin BinaryOp.Eq (col "Day") (i64 3L)), SubqueryKind.Exists))
            (ScalarExpr.Subquery(2, ordersWhere t 2 (bin BinaryOp.Gt (col "Amount") (f64 100.0)), SubqueryKind.Exists))
    let t = Tables()
    Conformance.rowsInOrder t (customersWhere t (either t)) (ids [ 1L; 2L ])
    // Under NOT, and as a value, the same disjunction is attached instead.
    let t = Tables()
    Conformance.rowsInOrder t (customersWhere t (not' (either t))) (ids [ 3L; 4L; 5L ])
    let t = Tables()
    Conformance.rowsInOrder t
        (project (t.Scan custs) [ "Id", col "Id"; "e", either t ])
        [ [ 1L; true ]; [ 2L; true ]; [ 3L; false ]; [ 4L; false ]; [ 5L; false ] ]

[<Fact>]
let ``In tests membership in the subquery's column`` () =
    let custsOfOrders (t: Tables) = project (t.Scan orders) [ "c", col "Cust" ]
    for nullsEqual in [ true; false ] do
        let t = Tables()
        Conformance.rowsInOrder t (customersWhere t (isIn (col "Id") nullsEqual (custsOfOrders t))) (ids [ 1L; 2L; 4L ])
    let t = Tables()
    Conformance.rowsInOrder t (customersWhere t (not' (isIn (col "Id") true (custsOfOrders t)))) (ids [ 3L; 5L ])
    // As a value: true or false, never null with nullsEqual.
    let t = Tables()
    Conformance.rowsInOrder t
        (project (t.Scan custs) [ "Id", col "Id"; "has", isIn (col "Id") true (custsOfOrders t) ])
        [ [ 1L; true ]; [ 2L; true ]; [ 3L; false ]; [ 4L; true ]; [ 5L; false ] ]

[<Fact>]
let ``A correlated scalar aggregate compared with the outer row (TPC-DS Q6, Q32, Q92)`` () =
    let scaledAverage (t: Tables) =
        bin BinaryOp.Multiply (f64 1.4) (scalar (project (ordersOf t) [ "a", agg AggKind.Mean (col "Amount") ]))
    let t = Tables()
    Conformance.rowsInOrder t (customersWhere t (bin BinaryOp.Gt (col "Credit") (scaledAverage t))) (ids [ 4L ])
    // Negated, the comparison stays as written: a null side keeps it null, and the row is dropped.
    let t = Tables()
    Conformance.rowsInOrder t (customersWhere t (not' (bin BinaryOp.Gt (col "Credit") (scaledAverage t)))) (ids [ 1L; 2L ])

[<Fact>]
let ``Aggregates over no correlated rows keep their rules`` () =
    let t = Tables()
    let o scope = ordersAt t scope
    Conformance.rowsInOrder t
        (project (t.Scan custs) [
            "Id", col "Id"
            "n", scalarAt 1 (project (o 1) [ "n", len ])
            "sum", scalarAt 2 (project (o 2) [ "s", agg AggKind.Sum (col "Amount") ])
            "min", scalarAt 3 (project (o 3) [ "m", agg AggKind.Min (col "Amount") ])
            "days", fn ScalarFunction.ListLen [ scalarAt 4 (project (o 4) [ "l", agg AggKind.List (col "Day") ]) ]
        ])
        [
            [ 1L; 2L; 150.0; 30.0; 2L ]
            [ 2L; 2L; 40.0; 40.0; 2L ]
            [ 3L; 0L; 0.0; null; 0L ]
            [ 4L; 1L; 5.0; 5.0; 1L ]
            [ 5L; 0L; 0.0; null; 0L ]
        ]

/// SQL's SUM (SumOrNull) of a scalar subquery: null for a row without correlated rows and for one whose values are all
/// null, as over no rows uncorrelated.
[<Fact>]
let ``A SumOrNull subquery is null without values`` () =
    let t = Tables()
    let sumOrNull plan = project plan [ "s", agg AggKind.SumOrNull (col "Amount") ]
    let dayThree scope = filter (ordersAt t scope) (bin BinaryOp.Eq (col "Day") (i64 3L))
    let noOrders = filter (t.Scan orders) (bin BinaryOp.Gt (col "Day") (i64 10L))
    Conformance.rowsInOrder t
        (project (t.Scan custs) [
            "Id", col "Id"
            "sum", scalarAt 1 (sumOrNull (ordersAt t 1))
            "day3", scalarAt 2 (sumOrNull (dayThree 2))
            "none", scalarAt 3 (sumOrNull noOrders)
        ])
        [
            [ 1L; 150.0; null; null ]
            [ 2L; 40.0; null; null ]
            [ 3L; null; null; null ]
            [ 4L; 5.0; null; null ]
            [ 5L; null; null; null ]
        ]

[<Fact>]
let ``A count over an OR of correlated conditions (TPC-DS Q41)`` () =
    let t = Tables()
    let either =
        bin BinaryOp.Or
            (bin BinaryOp.And (bin BinaryOp.Eq (col "Cust") custId) (bin BinaryOp.Gt (col "Amount") (f64 100.0)))
            (bin BinaryOp.And (bin BinaryOp.Eq (col "Cust") custId) (bin BinaryOp.Eq (col "Day") (i64 3L)))
    let count = scalar (project (filter (t.Scan orders) either) [ "n", len ])
    Conformance.rowsInOrder t (customersWhere t (bin BinaryOp.Gt count (ScalarExpr.Literal(LiteralValue.UInt32 0u)))) (ids [ 1L; 2L ])

[<Fact>]
let ``Uncorrelated scalar subqueries in a conditional (TPC-DS Q9)`` () =
    let t = Tables()
    let all () = t.Scan orders
    let bucket =
        ScalarExpr.Conditional(
            bin BinaryOp.Gt (scalarAt 1 (project (all ()) [ "n", len ])) (ScalarExpr.Literal(LiteralValue.UInt32 3u)),
            scalarAt 2 (project (all ()) [ "a", agg AggKind.Mean (col "Amount") ]),
            scalarAt 3 (project (all ()) [ "m", agg AggKind.Max (col "Amount") ])
        )
    Conformance.rowsInOrder t (project (t.Scan custs) [ "Id", col "Id"; "b", bucket ]) [ for c in custs -> [ box c.Id; box 53.0 ] ]

[<Fact>]
let ``Uncorrelated scalar subqueries over the same rows share one row`` () =
    let t = Tables()
    let all = t.Scan orders
    let bucket =
        ScalarExpr.Conditional(
            bin BinaryOp.Gt (scalarAt 1 (project all [ "n", len ])) (ScalarExpr.Literal(LiteralValue.UInt32 3u)),
            scalarAt 2 (project all [ "a", agg AggKind.Mean (col "Amount") ]),
            scalarAt 3 (project all [ "m", agg AggKind.Max (col "Amount") ])
        )
    Conformance.rowsInOrder t (project (t.Scan custs) [ "Id", col "Id"; "b", bucket ]) [ for c in custs -> [ box c.Id; box 53.0 ] ]

[<Fact>]
let ``A row-valued scalar subquery gives its one value and fails on more rows`` () =
    let t = Tables()
    let amountOf (t: Tables) predicate = scalar (project (filter (t.Scan orders) predicate) [ "Amount", col "Amount" ])
    Conformance.rowsInOrder t
        (project (t.Scan custs) [ "Id", col "Id"; "v", amountOf t (bin BinaryOp.Eq (col "OrderId") (i64 2L)) ])
        [ for c in custs -> [ box c.Id; box 30.0 ] ]
    let t = Tables()
    Conformance.rowsInOrder t
        (project (t.Scan custs) [ "Id", col "Id"; "v", amountOf t (bin BinaryOp.Eq (col "OrderId") (i64 99L)) ])
        [ for c in custs -> [ box c.Id; null ] ]
    let t = Tables()
    Conformance.raises<InvalidOperationException> t
        (project (t.Scan custs) [ "Id", col "Id"; "v", amountOf t (bin BinaryOp.Eq (col "Day") (i64 1L)) ])
        "A scalar subquery yielded more than one row."

let private ps: PK array =
    [|
        { Id = 1L; K = n 1L; G = n 1L }
        { Id = 2L; K = nul; G = n 1L }
        { Id = 3L; K = nul; G = nul }
        { Id = 4L; K = n 2L; G = n 2L }
    |]

let private qs: QK array =
    [| { K = n 1L; G = n 1L }; { K = nul; G = n 1L }; { K = nul; G = nul }; { K = n 2L; G = n 3L } |]

[<Fact>]
let ``Correlation keys match nulls as their operator says`` () =
    let pkWhere (t: Tables) predicate = project (filter (t.Scan ps) (exists (filter (t.Scan qs) predicate))) [ "Id", col "Id" ]
    let key op name = bin op (col name) (outer name LogicalTypes.Int64)
    let t = Tables()
    Conformance.rowsInOrder t (pkWhere t (key BinaryOp.EqMissing "K")) (ids [ 1L; 2L; 3L; 4L ])
    let t = Tables()
    Conformance.rowsInOrder t (pkWhere t (key BinaryOp.Eq "K")) (ids [ 1L; 4L ])
    // One key matching nulls and one not.
    let t = Tables()
    Conformance.rowsInOrder t (pkWhere t (bin BinaryOp.And (key BinaryOp.EqMissing "K") (key BinaryOp.Eq "G"))) (ids [ 1L; 2L ])
    // The same as values (a left join, not a semi join).
    let t = Tables()
    Conformance.rowsInOrder t
        (project (t.Scan ps) [ "Id", col "Id"; "e", exists (filter (t.Scan qs) (bin BinaryOp.And (key BinaryOp.EqMissing "K") (key BinaryOp.Eq "G"))) ])
        [ [ 1L; true ]; [ 2L; true ]; [ 3L; false ]; [ 4L; false ] ]

[<Fact>]
let ``In matches a null target with nullsEqual only`` () =
    let pkIn (t: Tables) nullsEqual = project (filter (t.Scan ps) (isIn (col "K") nullsEqual (project (t.Scan qs) [ "k", col "K" ]))) [ "Id", col "Id" ]
    let t = Tables()
    Conformance.rowsInOrder t (pkIn t true) (ids [ 1L; 2L; 3L; 4L ])
    let t = Tables()
    Conformance.rowsInOrder t (pkIn t false) (ids [ 1L; 4L ])

[<Fact>]
let ``SQL In as a value is true on a match, false over no rows, else null when the target or a value is null`` () =
    let custsOfOrders (t: Tables) = project (t.Scan orders) [ "c", col "Cust" ]
    // The orders' customers hold a null: no match is unknown.
    let t = Tables()
    Conformance.rowsInOrder t
        (project (t.Scan custs) [ "Id", col "Id"; "in", isIn (col "Id") false (custsOfOrders t) ])
        [ [ 1L; true ]; [ 2L; true ]; [ 3L; null ]; [ 4L; true ]; [ 5L; null ] ]
    // NOT IN then keeps no customer; without the null, those with no order.
    let t = Tables()
    Conformance.rowsInOrder t (customersWhere t (not' (isIn (col "Id") false (custsOfOrders t)))) []
    let t = Tables()
    let known = project (filter (t.Scan orders) (ScalarExpr.Unary(UnaryOp.IsNotNull, col "Cust"))) [ "c", col "Cust" ]
    Conformance.rowsInOrder t (customersWhere t (not' (isIn (col "Id") false known))) (ids [ 3L; 5L ])
    // Correlated: the credit among the customer's order amounts. A null target over no rows is false.
    let t = Tables()
    Conformance.rowsInOrder t
        (project (t.Scan custs) [ "Id", col "Id"; "in", isIn (col "Credit") false (project (ordersOf t) [ "a", col "Amount" ]) ])
        [ [ 1L; false ]; [ 2L; null ]; [ 3L; false ]; [ 4L; false ]; [ 5L; false ] ]

[<Fact>]
let ``Shapes the pass does not replace are reported`` () =
    let t = Tables()
    // Correlation under a limit (a correlated top-N).
    Conformance.unsupported (customersWhere t (exists (LogicalPlan.Limit(ordersOf t, 0L, 1u))))
    // A correlated scalar that is not an aggregate.
    Conformance.unsupported (project (t.Scan custs) [ "v", scalar (project (ordersOf t) [ "Amount", col "Amount" ]) ])
