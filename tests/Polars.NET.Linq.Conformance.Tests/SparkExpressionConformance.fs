/// Contract (sql-s7 Spark expression profile): a Delta expression bound by SqlFrontend.BindSparkExpression gives, row by
/// row, what Spark 4 gives with ANSI on: the same type, the same value, and a failure (Overflow or DivideByZero) where
/// Spark fails, as written and after the optimizer. The oracle is Spark/spark-expressions.json, written by
/// Spark/spark_expressions.py from the same table as `rows` below.
module Polars.NET.Linq.Conformance.Tests.SparkExpressionConformance

open System
open System.Globalization
open System.IO
open System.Text
open System.Text.Json
open Xunit
open Kaminari.Samidare
open Kaminari.Samidare.IR
open Kaminari.Samidare.Sql

/// The oracle's table (spark_expressions.py, ROWS).
type SR =
    { k: int64
      i: Nullable<int>
      l: Nullable<int64>
      t: Nullable<sbyte>
      sm: Nullable<int16>
      d: Nullable<decimal>
      f: Nullable<float>
      fl: Nullable<float32>
      s: string
      b: Nullable<bool>
      dt: Nullable<DateOnly>
      ts: Nullable<DateTimeOffset>
      tn: Nullable<DateTime>
      z: Nullable<int>
      p: string }

let private utc (y, mo, d, h, mi, s, us) = Nullable(DateTimeOffset(DateTime(y, mo, d, h, mi, s).AddTicks(int64 us * 10L), TimeSpan.Zero))

let rows: SR array =
    [|
        { k = 1L; i = Nullable 1; l = Nullable 10L; t = Nullable 1y; sm = Nullable 1s; d = Nullable 12.34m; f = Nullable 1.5; fl = Nullable 1.5f
          s = "Ab"; b = Nullable true; dt = Nullable(DateOnly(2021, 3, 5)); ts = utc (2021, 3, 5, 4, 0, 0, 0)
          tn = Nullable(DateTime(2021, 3, 5, 4, 0, 0)); z = Nullable 0; p = "a_c" }
        { k = 2L; i = Nullable Int32.MaxValue; l = Nullable Int64.MaxValue; t = Nullable 127y; sm = Nullable 32767s; d = Nullable -99.99m
          f = Nullable -2.5; fl = Nullable Single.NaN; s = "é xyz"; b = Nullable false; dt = Nullable(DateOnly(2020, 2, 29))
          ts = utc (2021, 7, 1, 6, 30, 0, 500000); tn = Nullable(DateTime(1999, 12, 31, 23, 59, 59).AddTicks(1234560L)); z = Nullable 2; p = "%" }
        { k = 3L; i = Nullable(); l = Nullable(); t = Nullable(); sm = Nullable(); d = Nullable(); f = Nullable(); fl = Nullable(); s = null
          b = Nullable(); dt = Nullable(); ts = Nullable(); tn = Nullable(); z = Nullable(); p = null }
        { k = 4L; i = Nullable Int32.MinValue; l = Nullable Int64.MinValue; t = Nullable -128y; sm = Nullable -32768s; d = Nullable 0.05m
          f = Nullable Double.NaN; fl = Nullable -0.0f; s = ""; b = Nullable(); dt = Nullable(DateOnly(1969, 12, 31))
          ts = utc (1969, 12, 31, 23, 59, 59, 0); tn = Nullable(DateTime(1970, 1, 1)); z = Nullable -1; p = "abc" }
    |]

let private decimal42 = LogicalTypes.decimalOf 4 2

/// The table's columns as Delta declares them (d is DECIMAL(4,2)).
let columns: (string * LogicalType) list =
    typeof<SR>.GetProperties()
    |> Array.map (fun p -> p.Name, (if p.Name = "d" then decimal42 else (LogicalTypes.tryOfClrType p.PropertyType).Value))
    |> List.ofArray

/// The table as a plan: the scan, d cast to its declared type.
let table (t: Tables) (row: SR) =
    project (t.Scan [| row |]) [ for name, ty in columns -> name, (if name = "d" then ScalarExpr.Cast(col "d", ty, false) else col name) ]

let bindAt (zone: string) (text: string) =
    SqlFrontend.BindSparkExpression(text, [ for name, t in columns -> struct (name, t) ], SparkExpressionUse.Generated, zone, DateTimeOffset.UnixEpoch)

/// Spark's text of a timestamp: `yyyy-MM-dd HH:mm:ss`, and the fraction of a second without trailing zeros.
let private timestampText (v: DateTime) =
    let whole = v.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)
    let fraction = (v.Ticks % TimeSpan.TicksPerSecond / 10L).ToString("D6", CultureInfo.InvariantCulture).TrimEnd('0')
    if fraction = "" then whole else whole + "." + fraction

/// A value as Spark's CAST(... AS STRING) writes it, for a value of the Spark type `kind`; floating values as numbers.
let private sparkText (zone: TimeZoneInfo) (kind: string) (v: obj) : string =
    match v with
    | null -> null
    | :? bool as b -> if b then "true" else "false"
    | :? decimal as m ->
        let scale = kind.Substring(kind.IndexOf(',') + 1).TrimEnd(')') |> int
        m.ToString("F" + string scale, CultureInfo.InvariantCulture)
    | :? double as x -> x.ToString("R", CultureInfo.InvariantCulture)
    | :? single as x -> x.ToString("R", CultureInfo.InvariantCulture)
    | :? DateOnly as d -> d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)
    | :? DateTimeOffset as o -> timestampText (TimeZoneInfo.ConvertTime(o, zone).DateTime)
    | :? DateTime as d when kind = "date" -> d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)
    | :? DateTime as d when kind = "timestamp" ->
        timestampText (TimeZoneInfo.ConvertTime(DateTimeOffset(DateTime.SpecifyKind(d, DateTimeKind.Utc)), zone).DateTime)
    | :? DateTime as d -> timestampText d
    | :? IFormattable as n -> n.ToString(null, CultureInfo.InvariantCulture)
    | other -> string other

/// Whether our text matches Spark's: floating values compare as numbers (Spark writes them as Java does).
let private sameText (kind: string) (spark: string) (ours: string) =
    match spark, ours with
    | null, null -> true
    | null, _
    | _, null -> false
    | _ when kind = "double" || kind = "float" ->
        let parse (x: string) = Double.Parse(x, NumberStyles.Float, CultureInfo.InvariantCulture)
        let a, b = parse spark, parse ours
        (Double.IsNaN a && Double.IsNaN b) || (if kind = "float" then float32 a = float32 b else a = b)
    | _ -> spark = ours

/// What running the plan gives: the value's text, or the failure's kind.
let private outcome (zone: TimeZoneInfo) (kind: string) (t: Tables) (plan: LogicalPlan) =
    try
        match (PolarsEngine() :> IIrEngine).Execute(plan, t.Sources).Rows with
        | [ [ v ] ] -> Ok(sparkText zone kind v)
        | other -> Error $"rows {other}"
    with
    | :? OverflowException -> Error "OVERFLOW"
    | :? DivideByZeroException -> Error "DIVIDE_BY_ZERO"
    | ex -> Error $"{ex.GetType().Name}: {ex.Message}"

/// Spark's error class as the failure we raise for it.
let private failureOf (sparkError: string) =
    if sparkError.Contains "BY_ZERO" then "DIVIDE_BY_ZERO"
    elif sparkError.Contains "OVERFLOW" || sparkError.Contains "OUT_OF_RANGE" then "OVERFLOW"
    else sparkError

let private golden () =
    use doc = JsonDocument.Parse(File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Spark", "spark-expressions.json")))
    [ for case in doc.RootElement.GetProperty("cases").EnumerateArray() ->
          let expr = case.GetProperty("expr").GetString()
          let zone = case.GetProperty("zone").GetString()
          let kind = case.GetProperty("type").GetString()
          let expected = [ for r in case.GetProperty("rows").EnumerateArray() -> if r.ValueKind = JsonValueKind.Null then null else r.GetString() ]
          expr, zone, kind, expected ]

[<Fact>]
let ``Every expression gives Spark's type, values and failures`` () =
    let problems = StringBuilder()
    let cases = golden ()
    Assert.True(cases.Length > 200, "the oracle holds the cases")
    for expr, zoneName, kind, expected in cases do
        let zone = TimeZoneInfo.FindSystemTimeZoneById zoneName
        match (try Ok(bindAt zoneName expr) with ex -> Error ex.Message) with
        | Error message -> problems.AppendLine($"{expr} [{zoneName}]: not bound: {message}") |> ignore
        | Ok bound ->
            let ourKind = SparkExpressions.display bound.Type
            if not (String.Equals(ourKind, kind, StringComparison.OrdinalIgnoreCase)) then
                problems.AppendLine($"{expr} [{zoneName}]: type {ourKind}, Spark {kind}") |> ignore
            for row, spark in List.zip (List.ofArray rows) expected do
                for label, optimize in [ "as written", id; "optimized", LogicalOptimizer.optimize ] do
                    let t = Tables()
                    let plan = optimize (project (table t row) [ "r", bound.Expr ])
                    let problem =
                        match outcome zone kind t plan with
                        | Ok ours when not (isNull spark) && spark.StartsWith "ERROR:" -> Some ours
                        | Ok ours when not (sameText kind spark ours) -> Some ours
                        | Ok _ -> None
                        | Error failure ->
                            let wanted = if isNull spark || not (spark.StartsWith "ERROR:") then "a value" else failureOf (spark.Substring 6)
                            if failure = wanted then None else Some failure
                    problem |> Option.iter (fun ours -> problems.AppendLine($"{expr} [{zoneName}] k={row.k} {label}: {ours}, Spark {spark}") |> ignore)
    if problems.Length > 0 then Assert.Fail(problems.ToString())

type SInner = { X: Nullable<int>; y: string }
type SS = { k: int64; st: SInner }

[<Fact>]
let ``A struct's fields are read by name, ignoring case`` () =
    let t = Tables()
    let structRows: SS array = [| { k = 1L; st = { X = Nullable 41; y = "a" } }; { k = 2L; st = { X = Nullable(); y = null } } |]
    let scan = t.Scan structRows
    let schema = (IrOps.InferSchema scan).Fields |> Seq.map (fun (f: LogicalField) -> struct (f.Name, f.DataType)) |> List.ofSeq
    let bound text = SqlFrontend.BindSparkExpression(text, schema, SparkExpressionUse.Generated, "UTC", DateTimeOffset.UnixEpoch).Expr
    Conformance.rowsInOrder t (project scan [ "x", bound "ST.x + 1"; "y", bound "st.Y || '!'" ]) [ [ 42L; "a!" ]; [ null; null ] ]
