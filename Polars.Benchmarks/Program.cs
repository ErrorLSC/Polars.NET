using BenchmarkDotNet.Configs;
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
var config = DefaultConfig.Instance
    .AddJob(Job.Default.WithToolchain(InProcessEmitToolchain.Instance))
    .WithOptions(ConfigOptions.JoinSummary);

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, config);
