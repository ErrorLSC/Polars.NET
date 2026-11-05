using System.Text.RegularExpressions;
using System.Xml.Linq;

namespace Polars.CSharp.Tests;

/// <summary>
/// Guards the Polars.NET.Linq layering: the frontend and IR are pure symbolic code and may use
/// Polars.NET.Core enums/helpers as vocabulary, but must never touch PolarsWrapper or native handles.
/// The F# compile order keeps the frontend from referencing Backend/Polars modules; this test covers
/// what compile order cannot (Polars.NET.Core is a separate assembly compiled earlier).
/// </summary>
public class LinqArchitectureTests
{
    private static readonly Regex ForbiddenInFrontend = new(
        @"^\s*open\s+Polars\.NET\.Core\s*$"        // Core root namespace: handles + materializer interfaces
        + @"|Polars\.NET\.Core\.Wrappers"          // native wrapper namespace
        + @"|\bPolarsWrapper\b"
        + @"|\b[A-Z]\w*Handle\b"                    // LazyFrameHandle, ExprHandle, DataFrameHandle, ...
        + @"|\bIDataFrameMaterializer\b"
        + @"|Backend\.Polars",
        RegexOptions.Multiline | RegexOptions.Compiled);

    private static bool IsBackendFile(string path) =>
        path.StartsWith("Backend/", StringComparison.Ordinal) || path == "PolarsQueryable.fs";

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir != null && !File.Exists(Path.Combine(dir.FullName, "Polars.NET.slnx")))
            dir = dir.Parent;
        return dir?.FullName ?? throw new InvalidOperationException("Repository root (Polars.NET.slnx) not found.");
    }

    private static List<string> GetCompileOrder(string linqDir) =>
        XDocument.Load(Path.Combine(linqDir, "Polars.NET.Linq.fsproj"))
            .Descendants("Compile")
            .Select(e => e.Attribute("Include")!.Value.Replace('\\', '/'))
            .ToList();

    private static string StripComments(string source)
    {
        var noBlock = Regex.Replace(source, @"\(\*[\s\S]*?\*\)", m => new string('\n', m.Value.Count(c => c == '\n')));
        return string.Join('\n', noBlock.Split('\n').Select(line =>
        {
            int idx = line.IndexOf("//", StringComparison.Ordinal);
            return idx >= 0 ? line[..idx] : line;
        }));
    }

    [Fact]
    [Trait("Architecture", "LinqLayering")]
    public void Linq_Backend_Files_Are_Compiled_After_Frontend()
    {
        var files = GetCompileOrder(Path.Combine(FindRepoRoot(), "Polars.NET.Linq"));
        int firstBackend = files.FindIndex(IsBackendFile);

        Assert.True(firstBackend > 0, "No backend files found in Polars.NET.Linq.fsproj.");
        var misplaced = files.Skip(firstBackend).Where(f => !IsBackendFile(f)).ToList();
        Assert.True(misplaced.Count == 0,
            "Frontend/IR files compiled after the backend boundary: " + string.Join(", ", misplaced));
    }

    [Fact]
    [Trait("Architecture", "LinqLayering")]
    public void Linq_Frontend_And_IR_Do_Not_Reference_Native_Handles()
    {
        var linqDir = Path.Combine(FindRepoRoot(), "Polars.NET.Linq");
        var violations = new List<string>();

        foreach (var file in GetCompileOrder(linqDir).Where(f => !IsBackendFile(f)))
        {
            var lines = StripComments(File.ReadAllText(Path.Combine(linqDir, file))).Split('\n');
            for (int i = 0; i < lines.Length; i++)
            {
                var match = ForbiddenInFrontend.Match(lines[i]);
                if (match.Success)
                    violations.Add($"{file}:{i + 1}: '{match.Value.Trim()}'  ->  {lines[i].Trim()}");
            }
        }

        Assert.True(violations.Count == 0,
            "Frontend/IR must not reference PolarsWrapper, native handles or Backend.Polars:\n" + string.Join('\n', violations));
    }
}
