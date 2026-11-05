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
var config = DefaultConfig.Instance
    .AddJob(Job.Default.WithToolchain(InProcessEmitToolchain.Instance))
    .WithOptions(ConfigOptions.JoinSummary);

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, config);
