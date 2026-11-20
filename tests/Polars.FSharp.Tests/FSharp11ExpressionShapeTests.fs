namespace Polars.FSharp.Tests

open System
open System.Linq
open System.Linq.Expressions
open Microsoft.FSharp.Linq.RuntimeHelpers
open Xunit
open Polars.FSharp.Query
open Polars.FSharp

type ShapeRow = { Name: string; DeptId: int }

/// <summary>
/// The expression shapes the F# 11 compiler gives LINQ, built here by hand so that they are covered whichever compiler
/// builds the tests: a tuple pattern bound in blocks (<c>Block[a]{ a = t.Item1; … }</c>, where F# 10 invoked nested
/// lambdas), an anonymous record built without naming its members, and a selection in two steps (an AnonymousObject,
/// then the result from its items). Each runs natively, every step, and agrees with LINQ to Objects.
/// </summary>
module FSharp11ExpressionShapeTests =

    let private rows = [| { Name = "a"; DeptId = 1 }; { Name = "b"; DeptId = 2 }; { Name = "c"; DeptId = 1 } |]

    /// The query built by `shape` over the rows in a frame and over the array: native, and the same rows in order.
    let private assertShape<'T> (shape: IQueryable<ShapeRow> -> Expression) =
        use frame = DataFrame.ofRecords rows
        let native = frame.AsQueryable<ShapeRow>()
        let query = native.Provider.CreateQuery<'T>(shape native)
        let report = (query :?> Kaminari.Samidare.IQueryExplainable).InspectPlan(true)
        Assert.False(report.IsFullFallback, report.FormatReport())
        Assert.True(report.FallbackBoundary.IsNone, report.FormatReport()) // every step native, none in memory
        let source = rows.AsQueryable()
        let expected = source.Provider.CreateQuery<'T>(shape source) |> Seq.map (sprintf "%A") |> List.ofSeq
        Assert.Equal<string list>(expected, query |> Seq.map (sprintf "%A") |> List.ofSeq)

    let private select (source: Expression) (selector: LambdaExpression) =
        let select =
            typeof<Queryable>.GetMethods()
            |> Array.find (fun m -> m.Name = "Select" && (m.GetParameters().[1].ParameterType.GetGenericArguments().[0].GetGenericArguments().Length = 2))
        Expression.Call(select.MakeGenericMethod(selector.Parameters.[0].Type, selector.ReturnType), source, Expression.Quote selector) :> Expression

    let private rowParameter () = Expression.Parameter(typeof<ShapeRow>, "x")

    let private field (row: Expression) (name: string) = Expression.Property(row, name) :> Expression

    [<Fact>]
    let ``A tuple pattern bound in blocks`` () =
        // rows.Select(x => (x.Name, x.DeptId)).Join(rows, fun (name, deptId) -> deptId, y => y.DeptId,
        // fun (name, deptId) y -> deptId * 10 + y.DeptId) as F# 11 builds the two lambdas over the tuple.
        assertShape<int> (fun source ->
            let x = rowParameter ()
            let pair = Expression.Lambda(Expression.New(typeof<Tuple<string, int>>.GetConstructors().[0], field x "Name", field x "DeptId"), x)
            let bound (tupledArg: ParameterExpression) (body: ParameterExpression -> ParameterExpression -> Expression) =
                let name = Expression.Variable(typeof<string>, "name")
                let deptId = Expression.Variable(typeof<int>, "deptId")
                Expression.Block(
                    [ name ],
                    Expression.Assign(name, field tupledArg "Item1"),
                    Expression.Block([ deptId ], Expression.Assign(deptId, field tupledArg "Item2"), body name deptId)) :> Expression
            let outerKey =
                let t = Expression.Parameter(typeof<Tuple<string, int>>, "tupledArg")
                Expression.Lambda(bound t (fun _ deptId -> deptId), t)
            let y = Expression.Parameter(typeof<ShapeRow>, "y")
            let innerKey = Expression.Lambda(field y "DeptId", y)
            let result =
                let t = Expression.Parameter(typeof<Tuple<string, int>>, "tupledArg")
                Expression.Lambda(bound t (fun _ deptId -> Expression.Add(Expression.Multiply(deptId, Expression.Constant 10), field y "DeptId")), t, y)
            let join =
                typeof<Queryable>.GetMethods()
                |> Array.find (fun m -> m.Name = "Join" && m.GetParameters().Length = 5)
                |> fun m -> m.MakeGenericMethod(typeof<Tuple<string, int>>, typeof<ShapeRow>, typeof<int>, typeof<int>)
            Expression.Call(join, select source.Expression pair, source.Expression, Expression.Quote outerKey, Expression.Quote innerKey, Expression.Quote result) :> Expression)

    [<Fact>]
    let ``A block whose binding uses the one before`` () =
        assertShape<int> (fun source ->
            let x = rowParameter ()
            let a = Expression.Variable(typeof<int>, "a")
            let b = Expression.Variable(typeof<int>, "b")
            let body =
                Expression.Block([ a; b ], Expression.Assign(a, field x "DeptId"), Expression.Assign(b, Expression.Add(a, Expression.Constant 1)), Expression.Multiply(a, b))
            select source.Expression (Expression.Lambda(body, x)))

    [<Fact>]
    let ``An anonymous record built without naming its members`` () =
        assertShape<{| dept: int; name: string |}> (fun source ->
            let x = rowParameter ()
            // The constructor's parameters are the fields in name order: dept, name.
            let make = Expression.New(typeof<{| dept: int; name: string |}>.GetConstructors().[0], field x "DeptId", field x "Name")
            select source.Expression (Expression.Lambda(make, x)))

    [<Fact>]
    let ``A selection in two steps through an AnonymousObject`` () =
        assertShape<{| dept: int; name: string |}> (fun source ->
            let x = rowParameter ()
            let item = typeof<AnonymousObject<string, int>>
            let first = Expression.Lambda(Expression.New(item.GetConstructors().[0], field x "Name", field x "DeptId"), x)
            let after = Expression.Parameter(item, "after")
            let make = Expression.New(typeof<{| dept: int; name: string |}>.GetConstructors().[0], field after "Item2", field after "Item1")
            select (select source.Expression first) (Expression.Lambda(make, after)))
