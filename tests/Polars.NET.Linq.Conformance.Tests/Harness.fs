namespace Polars.NET.Linq.Conformance.Tests

open System
open Xunit
open Polars.NET.Core.Enums
open Polars.NET.Core.Wrappers
open Polars.NET.Linq.IR
open Polars.NET.Linq.Backend.Polars

/// A result relation: column names and rows of boxed values (null for a null).
type ResultTable = { Columns: string list; Rows: obj list list }

/// An execution engine for the LINQ IR. The conformance suite runs every plan on every engine; a new backend
/// (the planned managed engine, for instance) joins the suite by implementing this and adding itself to
/// `Engines.all`. Sources are in-memory .NET arrays (SourceBinding.InMemory), which any engine can read.
type IIrEngine =
    abstract Name: string
    abstract Execute: plan: LogicalPlan * sources: SourceBinding array -> ResultTable

/// The Polars backend: lowers the plan and collects it.
type PolarsEngine() =
    interface IIrEngine with
        member _.Name = "Polars"

        member _.Execute(plan, sources) =
            let handle = PolarsWrapper.LazyCollect(PolarsPhysicalLowerer.lowerPlan sources plan, PlEngine.Auto)
            use df = new Polars.FSharp.DataFrame(handle)
            let columns = df.ColumnNames |> List.ofSeq
            let rows =
                [ for r in 0 .. int df.Height - 1 ->
                      [ for c in 0 .. columns.Length - 1 ->
                            match df.[r, c] with
                            | null -> null
                            | v -> v ] ]
            { Columns = columns; Rows = rows }

module Engines =
    let all: IIrEngine list = [ PolarsEngine() ]

/// The in-memory tables a plan reads, each scanned through its slot.
type Tables() =
    let sources = ResizeArray<SourceBinding>()

    /// A Scan of a table of rows; its columns are the element type's public properties, in declaration order.
    member _.Scan<'T>(rows: 'T array) : LogicalPlan =
        let slot = sources.Count
        sources.Add(SourceBinding.InMemory(rows :> Array, typeof<'T>))
        let fields =
            typeof<'T>.GetProperties()
            |> Array.map (fun p -> p.Name, LogicalTypes.tryOfClrType p.PropertyType |> Option.defaultValue LogicalType.Unknown)
        LogicalPlan.Scan(LogicalPlanSource.Slot slot, LogicalSchema.OfFields fields)

    member _.Sources = sources.ToArray()

/// Builders for terse plans.
[<AutoOpen>]
module Ir =
    let col name = ScalarExpr.Column name
    let i64 (v: int64) = ScalarExpr.Literal(LiteralValue.Int64 v)
    let str (v: string) = ScalarExpr.Literal(LiteralValue.String v)
    let bool (v: bool) = ScalarExpr.Literal(LiteralValue.Bool v)
    let nullBool = ScalarExpr.Literal(LiteralValue.Null LogicalTypes.Boolean)
    let bin op l r = ScalarExpr.Binary(op, l, r)
    let not' e = ScalarExpr.Unary(UnaryOp.Not, e)
    let agg kind e = ScalarExpr.Aggregation(kind, Some e, false)
    let len = ScalarExpr.Aggregation(AggKind.Len, None, false)
    let fn f args = ScalarExpr.Function(f, args, LogicalType.Unknown)
    let project input (cols: (string * ScalarExpr) list) = LogicalPlan.Project(input, cols)
    let filter input pred = LogicalPlan.Filter(input, pred)
    let asc e = { SortOrder.Expr = e; Descending = false; NullsLast = false }

    /// A nullable int64 value, for expected rows and row types.
    let n (v: int64) = Nullable v
    let nul: Nullable<int64> = Nullable()

module Conformance =
    /// Integral values compare as int64 and floating values as double: the contract fixes a value's kind
    /// (integer, floating, string, bool), not its width.
    let private normalize (v: obj) : obj =
        match v with
        | null -> null
        | :? sbyte | :? int16 | :? int32 | :? int64 | :? byte | :? uint16 | :? uint32 | :? uint64 -> box (Convert.ToInt64 v)
        | :? single | :? double -> box (Convert.ToDouble v)
        | other -> other

    let private normalizeRows (rows: obj list list) = rows |> List.map (List.map normalize)

    let private sortRows (rows: obj list list) =
        rows |> List.sortBy (fun r -> r |> List.map (fun v -> if isNull v then "\u0000" else string v) |> String.concat "\u0001")

    /// Runs the plan as written and after the logical optimizer on every engine, and checks the result: the
    /// backend must implement each node's semantics, and the passes must preserve them.
    let private run (ordered: bool) (tables: Tables) (plan: LogicalPlan) (expected: obj list list) =
        let expected = normalizeRows expected
        for engine in Engines.all do
            for label, p in [ "as written", plan; "optimized", LogicalOptimizer.optimize plan ] do
                let actual = engine.Execute(p, tables.Sources).Rows |> normalizeRows
                let e, a = if ordered then expected, actual else sortRows expected, sortRows actual
                // Object.Equals, not F# `=`: a boxed NaN equals NaN (F# equality follows IEEE, NaN <> NaN).
                let sameRow (x: obj list) (y: obj list) = x.Length = y.Length && List.forall2 (fun (u: obj) v -> Object.Equals(u, v)) x y
                if not (e.Length = a.Length && List.forall2 sameRow e a) then
                    Assert.Fail(sprintf "%s (%s):\nexpected %A\nactual   %A\nplan %s" engine.Name label e a (LogicalPlanOps.formatPlan p))

    /// The rows, in this order.
    let rowsInOrder tables plan expected = run true tables plan expected

    /// The rows, in any order.
    let rowsInAnyOrder tables plan expected = run false tables plan expected

    /// The column names of the result.
    let columns (tables: Tables) (plan: LogicalPlan) (expected: string list) =
        for engine in Engines.all do
            Assert.Equal<string list>(expected, engine.Execute(plan, tables.Sources).Columns)

    /// Running the plan raises the exception `'E` with this message, on every engine.
    let raises<'E when 'E :> exn> (tables: Tables) (plan: LogicalPlan) (message: string) =
        for engine in Engines.all do
            let ex = Assert.Throws<'E>(fun () -> engine.Execute(plan, tables.Sources) |> ignore)
            Assert.Equal(message, ex.Message)
