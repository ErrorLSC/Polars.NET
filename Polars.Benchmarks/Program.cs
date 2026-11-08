using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Exporters.Json;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains.InProcess.Emit;

// Run from the repository root:
//
//   dotnet run -c Release --project Polars.Benchmarks -p:SkipRust=true -p:RustTargetDir=debug -- --filter '*'
//
// SkipRust / RustTargetDir reuse the debug native library instead of building Rust with --release.
// Benchmarks run in-process so that BenchmarkDotNet does not rebuild the project (and Rust) itself.
// Native execution is therefore slower than in a release build; compile-path numbers are unaffected.
//
// Native timings (TpchBenchmarks) need an optimized native library. A release build without LTO is much quicker
// to produce than the full release profile and close enough for comparisons; from native_shim/:
//
//   CARGO_PROFILE_RELEASE_LTO=false CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 cargo build --release --target-dir target/bench
//
// then run the benchmarks with -p:RustTargetDir=bench/release. TPC-H data: Polars.Benchmarks/generate_tpch_data.sh.
//
// TPC-DS against DuckDB (Tpcds/): data from Polars.Benchmarks/generate_tpcds_data.sh (SF 2 by default), then
//
//   dotnet run -c Release --project Polars.Benchmarks -p:SkipRust=true -p:RustTargetDir=release -- --filter '*Tpcds*'
//
// POLARS_TPCDS_QUERIES=Q3,Q98 narrows the queries. Results (Markdown, CSV, full JSON) land in BenchmarkDotNet.Artifacts/results;
// tools/bench_report.py turns the JSON into the report.
var config = DefaultConfig.Instance
    .AddJob(Job.Default.WithToolchain(InProcessEmitToolchain.Instance))
    .AddExporter(JsonExporter.Full)
    .WithOptions(ConfigOptions.JoinSummary);

if (args is ["tpcds-explain", var explainQuery, ..])
{
    // Prints the IR and the optimized Polars plan of one TPC-DS query, then times one end-to-end run.
    Polars.Benchmarks.Tpcds.TpcdsBenchCatalog.Explain(explainQuery);
    return;
}

if (args is ["zz-cold-loop", var loopQuery, ..]) { Polars.Benchmarks.Tpcds.ZzColdCompile.Loop(loopQuery); return; }

if (args is ["zz-steps", var stepsQuery, ..]) { Polars.Benchmarks.Tpcds.ZzCacheSteps.Run(stepsQuery); return; }

if (args is ["zz-cache", var cacheQuery, ..]) { Polars.Benchmarks.Tpcds.ZzCacheProbe.Run(cacheQuery); return; }

if (args is ["zz-materialize", ..]) { Polars.Benchmarks.Tpcds.ZzMaterializeProbe.Run(); return; }

if (args is ["tpcds-validate", ..])
{
    // Checks that every implementation of every selected TPC-DS query returns the same rows, without timing.
    Polars.Benchmarks.Tpcds.TpcdsBenchCatalog.ValidateAll();
    return;
}

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, config);
