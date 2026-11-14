using System.Diagnostics;
using Kaminari.Sazanami.Helpers;
using Seseragi.Files.Fuzz;

// Mutation fuzzer for the managed Parquet reader and its unsafe kernels. Contract: any input reads, or is refused with
// ParquetFormatException (codecs: InvalidDataException); anything else — another exception, a crash (a guard page
// hit by an over-read), a hang — is a finding. Cases run in child processes (a crash takes only the child) with core
// dumps off; run the whole thing inside a memory- and CPU-capped scope (see README.md).
//
//   run [seed] [minutes] [out dir] [corpus dir]   parent: batches of cases in workers, findings saved under out dir
//   worker <seed> <first> <count> <corpus dir>    child: guarded memory on, one line per case on stdout
//   case <seed> <n> [corpus dir]                  one case in this process (guarded), its outcome and details
//   save <seed> <n> <file> [corpus dir]           the mutated file of a file case
//   selftest                                      reads one byte past a guarded block: must die with SIGSEGV

return args.FirstOrDefault() switch
{
    "selftest" => SelfTest(),
    "worker" => Worker(long.Parse(args[1]), long.Parse(args[2]), int.Parse(args[3]), args[4]),
    "case" => OneCase(long.Parse(args[1]), long.Parse(args[2]), Corpus(args.ElementAtOrDefault(3))),
    "save" => Save(long.Parse(args[1]), long.Parse(args[2]), args[3], Corpus(args.ElementAtOrDefault(4))),
    "run" => Parent.Run(args.Length > 1 ? long.Parse(args[1]) : 1, args.Length > 2 ? double.Parse(args[2]) : 10,
        args.ElementAtOrDefault(3) ?? Path.Combine(Environment.CurrentDirectory, "fuzz-out"), Corpus(args.ElementAtOrDefault(4))),
    _ => Usage(),
};

static unsafe int SelfTest()
{
    GuardedMemory.Enabled = true;
    using var block = new GuardedMemory.Block(100);
    block.GetSpan().Fill(1);
    Console.WriteLine($"last byte {block.GetSpan()[^1]}; now one past the end…");
    fixed (byte* p = block.GetSpan()) Console.WriteLine(p[100]);
    Console.WriteLine("NOT CAUGHT: the guard page did not fault");
    return 1;
}

static int Usage()
{
    Console.Error.WriteLine("run [seed] [minutes] [out] [corpus] | worker <seed> <first> <count> <corpus> | case <seed> <n> [corpus] | save <seed> <n> <file> [corpus]");
    return 2;
}

static string Corpus(string? given)
{
    if (given is not null) return given;
    for (DirectoryInfo? dir = new(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
    {
        string candidate = Path.Combine(dir.FullName, "tests", "Seseragi.Files.Tests", "TestData", "parquet");
        if (Directory.Exists(candidate)) return candidate;
    }
    throw new DirectoryNotFoundException("No corpus: pass the TestData/parquet directory.");
}

static string[] CorpusFiles(string dir) => Parent.CorpusFilesOf(dir);

static int Worker(long seed, long first, int count, string corpus)
{
    GuardedMemory.Enabled = true;
    var cases = new Cases(CorpusFiles(corpus));
    for (long n = first; n < first + count; n++)
    {
        Console.WriteLine($"C {n}");
        Outcome outcome = cases.Run(seed, n);
        Console.WriteLine(outcome.Finding
            ? $"F {n}\t{outcome.Signature}\t{outcome.Detail.ReplaceLineEndings(" ⏎ ")}"
            : $"R {n} {outcome.Signature}");
    }
    return 0;
}

static int OneCase(long seed, long n, string corpus)
{
    GuardedMemory.Enabled = true;
    var cases = new Cases(CorpusFiles(corpus));
    if (Cases.IsFileCase(n))
    {
        cases.FileOf(seed, n, out string source);
        Console.WriteLine($"file case from {source}");
    }
    Outcome outcome = cases.Run(seed, n);
    Console.WriteLine($"{(outcome.Finding ? "FINDING" : "ok")}: {outcome.Signature}");
    if (outcome.Detail.Length > 0) Console.WriteLine(outcome.Detail);
    return outcome.Finding ? 1 : 0;
}

static int Save(long seed, long n, string file, string corpus)
{
    if (!Cases.IsFileCase(n)) throw new ArgumentException($"Case {n} is a kernel case: it has no file.");
    File.WriteAllBytes(file, new Cases(CorpusFiles(corpus)).FileOf(seed, n, out string source));
    Console.WriteLine($"{file} (from {source})");
    return 0;
}

/// <summary>The parent: workers in batches, a watchdog per case, findings deduplicated and saved, the CPU kept cool.</summary>
internal static class Parent
{
    private const int Batch = 300;
    private static readonly TimeSpan CaseTimeout = TimeSpan.FromSeconds(20);
    private const int SavedPerSignature = 3, SavedAtMost = 100;
    private const double HotCelsius = 80, CoolCelsius = 60;

    public static int Run(long seed, double minutes, string outDir, string corpus)
    {
        Directory.CreateDirectory(outDir);
        DateTime deadline = DateTime.UtcNow.AddMinutes(minutes);
        var signatures = new Dictionary<string, (int Count, long FirstCase)>();
        long n = 0, read = 0, refused = 0;
        int saved = 0;
        using var log = new StreamWriter(Path.Combine(outDir, "log.txt"), append: true) { AutoFlush = true };
        void Say(string line)
        {
            string stamped = $"{DateTime.Now:HH:mm:ss} {line}";
            Console.WriteLine(stamped);
            log.WriteLine(stamped);
        }
        Say($"seed {seed}, {minutes} min, corpus {corpus}, out {outDir}, AVX2 {Environment.GetEnvironmentVariable("DOTNET_EnableAVX2") ?? "default"}");

        while (DateTime.UtcNow < deadline)
        {
            CoolDown(Say);
            var (next, results) = RunWorker(seed, n, Batch, corpus);
            foreach (var (c, kind, signature, detail) in results)
            {
                if (kind == "read") read++;
                else if (kind == "refused") refused++;
                else
                {
                    signatures.TryGetValue(signature, out var known);
                    signatures[signature] = (known.Count + 1, known.Count == 0 ? c : known.FirstCase);
                    if (known.Count == 0) Say($"NEW finding at case {c}: {signature}");
                    if (known.Count < SavedPerSignature && saved < SavedAtMost)
                    {
                        saved++;
                        string name = Path.Combine(outDir, $"{seed}-{c}");
                        File.WriteAllText(name + ".txt", $"seed {seed} case {c}\n{signature}\n\n{detail.Replace(" ⏎ ", "\n")}\n");
                        if (Cases.IsFileCase(c))
                            File.WriteAllBytes(name + ".parquet", new Cases(CorpusFilesOf(corpus)).FileOf(seed, c, out _));
                    }
                }
            }
            n = next;
            if (n % (Batch * 10) == 0) Say($"{n} cases: {read} read, {refused} refused, {signatures.Values.Sum(s => s.Count)} findings ({signatures.Count} kinds)");
        }

        string summary = $"seed {seed}: {n} cases, {read} read, {refused} refused\n"
            + string.Join("\n", signatures.OrderByDescending(s => s.Value.Count).Select(s => $"{s.Value.Count,6} × {s.Key} (first: case {s.Value.FirstCase})"));
        File.WriteAllText(Path.Combine(outDir, $"summary-{seed}.txt"), summary + "\n");
        Say(summary);
        return 0;
    }

    // Legitimately huge: 4 KB of Brotli that is a dictionary of more than 1 GiB (parquet-testing's large-string test);
    // read, it only exhausts the worker's memory cap.
    private static readonly HashSet<string> Excluded = ["large_string_map.brotli.parquet"];

    public static string[] CorpusFilesOf(string dir) =>
        [.. Directory.EnumerateFiles(dir, "*.parquet", SearchOption.AllDirectories).Where(f => !Excluded.Contains(Path.GetFileName(f))).Order(StringComparer.Ordinal)];

    /// <summary>Runs cases [first, first + count) in a child; a crash or hang ends the batch there (the next starts after it).</summary>
    private static (long Next, List<(long Case, string Kind, string Signature, string Detail)> Results) RunWorker(long seed, long first, int count, string corpus)
    {
        var start = new ProcessStartInfo("prlimit")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        // No core files: a guard-page hit is a SIGSEGV, and dumps would fill the disk.
        foreach (string a in (string[])["--core=0", "--", Environment.ProcessPath!, typeof(Parent).Assembly.Location, "worker", $"{seed}", $"{first}", $"{count}", corpus])
            start.ArgumentList.Add(a);
        start.Environment["DOTNET_DbgEnableMiniDump"] = "0";
        start.Environment["DOTNET_EnableCrashReport"] = "0";
        // Managed allocations past 1.5 GiB throw OutOfMemoryException (a finding) instead of waking the OOM killer.
        start.Environment["DOTNET_GCHeapHardLimit"] = "0x60000000";

        List<(long, string, string, string)> results = [];
        using var process = Process.Start(start)!;
        var stderr = new List<string>();
        process.ErrorDataReceived += (_, e) =>
        {
            if (e.Data is null) return;
            lock (stderr)
            {
                stderr.Add(e.Data);
                if (stderr.Count > 200) stderr.RemoveAt(0);
            }
        };
        process.BeginErrorReadLine();

        long pending = -1;
        long next = first + count;
        while (true)
        {
            Task<string?> line = process.StandardOutput.ReadLineAsync();
            if (!line.Wait(CaseTimeout))
            {
                process.Kill(entireProcessTree: true);
                process.WaitForExit();
                results.Add((pending, "finding", "hang (no progress in 20 s)", ""));
                next = pending + 1;
                break;
            }
            string? text = line.Result;
            if (text is null) break;
            if (text.StartsWith("C ", StringComparison.Ordinal)) pending = long.Parse(text[2..]);
            else if (text.StartsWith("R ", StringComparison.Ordinal))
            {
                string[] parts = text.Split(' ');
                results.Add((long.Parse(parts[1]), parts[2], "", ""));
                pending = -1;
            }
            else if (text.StartsWith("F ", StringComparison.Ordinal))
            {
                string[] parts = text.Split('\t', 3);
                results.Add((long.Parse(parts[0][2..]), "finding", parts[1], parts.ElementAtOrDefault(2) ?? ""));
                pending = -1;
            }
        }
        process.WaitForExit();
        if (pending >= 0 && !results.Any(r => r.Item1 == pending))
        {
            // The child died in this case: a fault (a guard page hit), an abort or the OOM killer.
            string[] tail;
            lock (stderr) tail = [.. stderr];
            string frame = tail.Select(l => l.Trim()).FirstOrDefault(l => l.StartsWith("at Kaminari.", StringComparison.Ordinal)) is { } at
                ? at[3..(at.IndexOf('(') is > 3 and var p ? p : at.Length)] : "?";
            string first2 = tail.FirstOrDefault(l => l.Contains("Exception", StringComparison.Ordinal) || l.Contains("Fatal", StringComparison.Ordinal))?.Trim() ?? "";
            string how = process.ExitCode == 137 ? "killed (SIGKILL: the OOM killer?)" : $"crash (exit {process.ExitCode}) {first2[..Math.Min(first2.Length, 80)]} at {frame}";
            results.Add((pending, "finding", how, string.Join("\n", tail)));
            next = pending + 1;
        }
        return (next, results);
    }

    /// <summary>Waits while the package is hot (above 80 °C) until it is below 60 °C.</summary>
    private static void CoolDown(Action<string> say)
    {
        double hot = Celsius();
        if (hot < HotCelsius) return;
        say($"CPU at {hot:F0} °C: pausing");
        while ((hot = Celsius()) > CoolCelsius) Thread.Sleep(5000);
        say($"CPU at {hot:F0} °C: resuming");
    }

    private static double Celsius()
    {
        double max = 0;
        foreach (string zone in Directory.Exists("/sys/class/thermal") ? Directory.GetDirectories("/sys/class/thermal", "thermal_zone*") : [])
        {
            try
            {
                string type = File.ReadAllText(Path.Combine(zone, "type")).Trim();
                if (type is not ("x86_pkg_temp" or "acpitz" or "cpu-thermal")) continue;
                max = Math.Max(max, double.Parse(File.ReadAllText(Path.Combine(zone, "temp")).Trim()) / 1000);
            }
            catch (IOException)
            {
            }
        }
        return max;
    }
}
