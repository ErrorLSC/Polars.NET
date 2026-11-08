using System.Diagnostics;

namespace Polars.CSharp.Tests;

/// <summary>
/// Runs tools/AbiAudit.fsx, which pairs every P/Invoke declaration in Polars.NET.Core with its Rust
/// export in native_shim and fails on parameter count mismatches or imports with no export.
/// </summary>
public class NativeAbiTests
{
    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir != null && !File.Exists(Path.Combine(dir.FullName, "Polars.NET.slnx")))
            dir = dir.Parent;
        return dir?.FullName ?? throw new InvalidOperationException("Repository root (Polars.NET.slnx) not found.");
    }

    [Fact]
    public async Task PInvoke_Declarations_Match_Rust_Exports()
    {
        var root = FindRepoRoot();
        var psi = new ProcessStartInfo("dotnet")
        {
            WorkingDirectory = root,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        psi.ArgumentList.Add("fsi");
        psi.ArgumentList.Add(Path.Combine(root, "tools", "AbiAudit.fsx"));

        using var process = Process.Start(psi)!;
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var stdout = process.StandardOutput.ReadToEndAsync(timeout.Token);
        var stderr = process.StandardError.ReadToEndAsync(timeout.Token);
        await process.WaitForExitAsync(timeout.Token);

        Assert.True(process.ExitCode == 0, $"ABI audit failed:\n{await stdout}\n{await stderr}");
    }
}
