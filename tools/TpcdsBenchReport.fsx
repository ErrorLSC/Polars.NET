// Turns the BenchmarkDotNet results of the TPC-DS benchmarks (Polars.Benchmarks/Tpcds) into charts and a Markdown report.
//
//   dotnet fsi tools/TpcdsBenchReport.fsx [results-dir-or-json ...] [--out <dir>]
//
// Inputs default to the newest *-report-full.json in BenchmarkDotNet.Artifacts/results; several files (or
// directories) are merged, the later file winning for the same benchmark. Output (default
// BenchmarkDotNet.Artifacts/tpcds-report): e2e.png, translate.png, phases.png and report.md.
#r "nuget: ScottPlot, 5.1.58"

open System
open System.IO
open System.Text.Json
open ScottPlot

// ---------------------------------------------------------------- input

type Measure =
    { Type: string
      Method: string
      Query: string
      PlanCache: bool option
      MeanNs: float option
      StdDevNs: float
      AllocBytes: int64 option }

let argv = fsi.CommandLineArgs |> Array.skip 1

let outDir, inputs =
    let rec go (out: string option) acc args =
        match args with
        | "--out" :: d :: rest -> go (Some d) acc rest
        | x :: rest -> go out (x :: acc) rest
        | [] -> out, List.rev acc
    let out, ins = go None [] (List.ofArray argv)
    defaultArg out "BenchmarkDotNet.Artifacts/tpcds-report", ins

let jsonFiles =
    let ofDir (dir: string) =
        Directory.GetFiles(dir, "*-report-full.json") |> Array.sortBy File.GetLastWriteTimeUtc |> Array.toList
    match inputs with
    | [] ->
        match ofDir "BenchmarkDotNet.Artifacts/results" with
        | [] -> failwith "No *-report-full.json in BenchmarkDotNet.Artifacts/results."
        | files -> [ List.last files ]
    | ins -> ins |> List.collect (fun p -> if Directory.Exists p then ofDir p else [ p ])

let parameters (s: string) =
    s.Split('&', StringSplitOptions.RemoveEmptyEntries)
    |> Array.map (fun kv -> let i = kv.IndexOf '=' in kv.Substring(0, i), kv.Substring(i + 1))
    |> Map.ofArray

let mutable host: JsonElement option = None

let measures =
    [ for file in jsonFiles do
          use doc = JsonDocument.Parse(File.ReadAllText file)
          let root = doc.RootElement
          host <- Some(root.GetProperty("HostEnvironmentInfo").Clone())
          for b in root.GetProperty("Benchmarks").EnumerateArray() do
              let ty = b.GetProperty("Type").GetString()
              if ty.StartsWith "Tpcds" then
                  let ps = parameters (b.GetProperty("Parameters").GetString())
                  let stats = b.GetProperty("Statistics")
                  let mem = b.GetProperty("Memory")
                  let hasStats = stats.ValueKind = JsonValueKind.Object
                  yield
                      { Type = ty
                        Method = b.GetProperty("Method").GetString()
                        Query = ps.["Query"]
                        PlanCache = ps |> Map.tryFind "PlanCache" |> Option.map bool.Parse
                        MeanNs = if hasStats then Some(stats.GetProperty("Mean").GetDouble()) else None
                        StdDevNs = if hasStats then stats.GetProperty("StandardDeviation").GetDouble() else 0.0
                        AllocBytes =
                          if hasStats && mem.ValueKind = JsonValueKind.Object then
                              Some(mem.GetProperty("BytesAllocatedPerOperation").GetInt64())
                          else None } ]
    // The same benchmark from several files (or jobs): keep the last one that has a measurement.
    |> List.groupBy (fun m -> m.Type, m.Method, m.Query, m.PlanCache)
    |> List.map (fun (_, ms) -> ms |> List.filter (fun m -> m.MeanNs.IsSome) |> List.tryLast |> Option.defaultValue (List.last ms))

let queryOrder (q: string) =
    if q.StartsWith "Q" then (0, int (q.Substring 1)) else (1, 0)

let queries = measures |> List.map _.Query |> List.distinct |> List.sortBy queryOrder

let find ty meth q cache =
    measures
    |> List.tryFind (fun m -> m.Type = ty && m.Method = meth && m.Query = q && m.PlanCache = cache)
    |> Option.bind (fun m -> m.MeanNs |> Option.map (fun mean -> mean, m))

let exec meth q = find "TpcdsExecutionBenchmarks" meth q None
let trans meth q cache = find "TpcdsTranslationBenchmarks" meth q (Some cache)

// ---------------------------------------------------------------- charts

Directory.CreateDirectory outDir |> ignore

let palette =
    {| duck = Color.FromHex "#3B6FB6"
       l2db = Color.FromHex "#D9822B"
       polars = Color.FromHex "#2E9E6B"
       light = Color.FromHex "#9CCFB5"
       grey = Color.FromHex "#8A8F98"
       collect = Color.FromHex "#2E9E6B"
       translate = Color.FromHex "#C2463D"
       materialize = Color.FromHex "#7A5BB5" |}

let legend (plt: Plot) (items: (string * Color) list) =
    for label, color in items do
        plt.Legend.ManualItems.Add(LegendItem(LabelText = label, FillColor = color))
    plt.ShowLegend(Edge.Right) |> ignore

let groupedBars (plt: Plot) (series: (string * Color * (string -> float option)) list) (labels: string list) =
    let n = float series.Length
    let width = 0.8 / n
    let bars = ResizeArray<Bar>()
    labels
    |> List.iteri (fun i q ->
        series
        |> List.iteri (fun s (_, color, value) ->
            match value q with
            | Some v ->
                bars.Add(Bar(Position = float i + (float s - (n - 1.0) / 2.0) * width, Value = v, Size = width * 0.95, FillColor = color))
            | None -> ()))
    plt.Add.Bars(bars) |> ignore
    plt.Axes.Bottom.SetTicks(labels |> List.mapi (fun i _ -> float i) |> Array.ofList, Array.ofList labels)
    plt.Axes.Margins(bottom = 0.0) |> ignore
    legend plt (series |> List.map (fun (l, c, _) -> l, c))

// 1. End to end, relative to DuckDB + Dapper.
let e2eChart () =
    use plt = new Plot()
    let rel meth q =
        match exec "DuckDB_Dapper" q, exec meth q with
        | Some(d, _), Some(v, _) -> Some(v / d)
        | _ -> None
    groupedBars plt
        [ "DuckDB + Dapper (= 1.0)", palette.duck, rel "DuckDB_Dapper"
          "DuckDB + Linq2DB", palette.l2db, rel "DuckDB_Linq2DB"
          "Polars.NET LINQ", palette.polars, rel "Polars_Linq" ]
        queries
    plt.Add.HorizontalLine(1.0, 1.0f, palette.grey, LinePattern.Dashed) |> ignore
    plt.Title "End to end: Parquet -> List<T>, time relative to DuckDB + Dapper (lower is faster)"
    plt.YLabel "× DuckDB + Dapper"
    plt.SavePng(Path.Combine(outDir, "e2e.png"), 1400, 560) |> ignore

// 2. Translation, cold and warm plan cache, in microseconds.
let translateChart () =
    use plt = new Plot()
    // Log scale: bars are drawn at log10(µs) and the axis labels map back to µs.
    let us meth cache q = trans meth q cache |> Option.map (fun (v, _) -> log10 (v / 1e3))
    groupedBars plt
        [ "Polars.NET LINQ, cold", palette.polars, us "Polars_Translate" false
          "Polars.NET LINQ, plan cache", palette.light, us "Polars_Translate" true
          "Linq2DB, cold", palette.l2db, us "Linq2DB_Translate" false
          "Linq2DB, query cache", Color.FromHex "#EDB97E", us "Linq2DB_Translate" true ]
        (queries |> List.filter (fun q -> trans "Polars_Translate" q false |> Option.isSome))
    plt.Axes.Left.TickGenerator <-
        TickGenerators.NumericAutomatic(
            MinorTickGenerator = TickGenerators.LogMinorTickGenerator(),
            IntegerTicksOnly = true,
            LabelFormatter = fun y -> $"{10.0 ** y:N0}")
    plt.Title "Translation: LINQ expression -> native plan (Polars) / SQL text (Linq2DB), log scale"
    plt.YLabel "µs (log scale)"
    plt.SavePng(Path.Combine(outDir, "translate.png"), 1400, 560) |> ignore

// 3. Where Polars.NET spends an end-to-end run: translate (plan cache on), collect, materialize, as shares.
let phases q =
    match trans "Polars_Translate" q true, exec "Polars_Collect" q with
    | Some(t, _), Some(c, _) ->
        let m = trans "Polars_Materialize" q true |> Option.map fst |> Option.defaultValue 0.0
        Some(t, c, m)
    | _ -> None

let phasesChart () =
    use plt = new Plot()
    let labels = queries |> List.filter (phases >> Option.isSome)
    let bars = ResizeArray<Bar>()
    labels
    |> List.iteri (fun i q ->
        let t, c, m = (phases q).Value
        let total = t + c + m
        let mutable baseValue = 0.0
        for v, color in [ t, palette.translate; c, palette.collect; m, palette.materialize ] do
            let share = 100.0 * v / total
            bars.Add(Bar(Position = float i, ValueBase = baseValue, Value = baseValue + share, FillColor = color, Size = 0.7))
            baseValue <- baseValue + share)
    let bp = plt.Add.Bars(bars)
    bp.Horizontal <- true
    plt.Axes.Left.SetTicks(labels |> List.mapi (fun i _ -> float i) |> Array.ofList, Array.ofList labels)
    plt.Axes.SetLimitsX(0.0, 100.0)
    legend plt [ "translate (plan cache)", palette.translate; "collect (scan + execute)", palette.collect; "materialize List<T>", palette.materialize ]
    plt.Title "Polars.NET: share of each phase"
    plt.XLabel "% of translate + collect + materialize"
    plt.SavePng(Path.Combine(outDir, "phases.png"), 1200, 40 * labels.Length + 160) |> ignore

e2eChart ()
translateChart ()
phasesChart ()

// ---------------------------------------------------------------- report

let ms (ns: float) = if ns >= 1e8 then $"%.0f{ns / 1e6}" else $"%.1f{ns / 1e6}"
let us (ns: float) = $"%.0f{ns / 1e3}"
let kb (b: int64 option) = b |> Option.map (fun b -> if b >= 1048576L then $"%.1f{float b / 1048576.0} MB" else $"%.0f{float b / 1024.0} KB") |> Option.defaultValue "–"
let cell f = function Some(v, (m: Measure)) -> f v m | None -> "NA"

let sb = Text.StringBuilder()
let line (s: string) = sb.AppendLine s |> ignore

line "# TPC-DS benchmark: Polars.NET LINQ vs DuckDB"
line ""
match host with
| Some h ->
    let str (k: string) = h.GetProperty(k).ToString()
    line $"""- **Machine:** {str "ProcessorName"}, {str "PhysicalCoreCount"} cores / {str "LogicalCoreCount"} threads, {str "OsVersion"}"""
    line $"""- **Runtime:** {str "RuntimeVersion"}, BenchmarkDotNet {str "BenchmarkDotNetVersion"}"""
| None -> ()
line $"""- **Data:** TPC-DS SF {Environment.GetEnvironmentVariable "POLARS_TPCDS_SF" |> Option.ofObj |> Option.defaultValue "2"}, Parquet files scanned on every run by both engines (OS page cache warm)"""
line $"""- **Sources:** {jsonFiles |> List.map Path.GetFileName |> String.concat ", "}"""
line ""
line "## End to end (Parquet → `List<T>`)"
line ""
line "Mean of 10 runs after 2 warm-ups. Each run builds the query, translates it (plan caches on), scans, executes and materializes. Results of all implementations are checked row by row before timing."
line ""
line "![End to end](e2e.png)"
line ""
line "| Query | DuckDB+Dapper ms | DuckDB+Linq2DB ms | Polars.NET ms | Polars / DuckDB | Polars alloc |"
line "|---|---:|---:|---:|---:|---:|"
for q in queries do
    let d = exec "DuckDB_Dapper" q
    let l = exec "DuckDB_Linq2DB" q
    let p = exec "Polars_Linq" q
    let ratio = match d, p with Some(dv, _), Some(pv, _) -> $"%.2f{pv / dv}×" | _ -> "NA"
    let f = cell (fun v m -> $"{ms v} ± {ms m.StdDevNs}")
    line $"| {q} | {f d} | {f l} | {f p} | {ratio} | {cell (fun _ m -> kb m.AllocBytes) p} |"
line ""
line "## Translation"
line ""
line "Building the expression tree and translating it: to a native Polars plan (IR, optimizer, lowering), or to SQL text for Linq2DB. Cold = plan cache off; cached = the library's default cache, warm."
line ""
line "![Translation](translate.png)"
line ""
line "| Query | Polars cold µs | Polars cached µs | Linq2DB cold µs | Linq2DB cached µs | Polars cold alloc |"
line "|---|---:|---:|---:|---:|---:|"
for q in queries do
    let pc = trans "Polars_Translate" q false
    if pc.IsSome then
        let f = cell (fun v _ -> us v)
        line $"""| {q} | {f pc} | {f (trans "Polars_Translate" q true)} | {f (trans "Linq2DB_Translate" q false)} | {f (trans "Linq2DB_Translate" q true)} | {cell (fun _ m -> kb m.AllocBytes) pc} |"""
line ""
line "## Polars.NET phases"
line ""
line "![Phases](phases.png)"
line ""
line "| Query | translate µs | collect ms | materialize µs | sum ms | measured end to end ms |"
line "|---|---:|---:|---:|---:|---:|"
for q in queries do
    match phases q with
    | Some(t, c, m) ->
        line $"""| {q} | {us t} | {ms c} | {us m} | {ms (t + c + m)} | {cell (fun v _ -> ms v) (exec "Polars_Linq" q)} |"""
    | None -> ()
line ""
line "NA: not applicable (no Linq2DB version written, or a count query with nothing to materialize) or the benchmark failed; see the BenchmarkDotNet log."

File.WriteAllText(Path.Combine(outDir, "report.md"), sb.ToString())
printfn $"Report written to {Path.GetFullPath outDir}"
