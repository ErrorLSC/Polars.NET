using System.Text.RegularExpressions;
using System.Xml.Linq;

namespace Polars.CSharp.Tests;

/// <summary>
/// Guards the layering of the compiler: Samidare is pure symbolic code that must not reference Polars.NET.Core
/// or the Polars backend, and Sazanami sits below it.
/// </summary>
public class LinqArchitectureTests
{
    private static readonly Regex ForbiddenInFrontend = new(
        @"\bPolars\.NET\.Core\b"
        + @"|^\s*(open|namespace)\s+Polars\."     // Samidare has its own namespaces
        + @"|\bPolarsWrapper\b"
        + @"|\b[A-Z]\w*Handle\b"                    // LazyFrameHandle, ExprHandle, DataFrameHandle, ...
        + @"|\bIDataFrameMaterializer\b"
        + @"|Backend\.Polars",
        RegexOptions.Multiline | RegexOptions.Compiled);

    // IR files carry their own vocabulary (LogicalTypes, JoinKind, RankMethod, ...) and must not use Polars enums.
    private static readonly Regex ForbiddenInIr = new(
        @"Polars\.NET\.Core\.Enums|\bPl[A-Z]\w*\b",
        RegexOptions.Compiled);

    // The frontend reads method-enum arguments by value and maps them to the IR vocabulary.
    private static readonly Regex ForbiddenVocabularyInFrontend = new(
        @"\bPl[A-Z]\w*\b",
        RegexOptions.Compiled);

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir != null && !File.Exists(Path.Combine(dir.FullName, "Polars.NET.slnx")))
            dir = dir.Parent;
        return dir?.FullName ?? throw new InvalidOperationException("Repository root (Polars.NET.slnx) not found.");
    }

    private static string SamidareDir => Path.Combine(FindRepoRoot(), "kernel", "Samidare");

    private static List<string> SamidareSources() =>
        XDocument.Load(Path.Combine(SamidareDir, "Samidare.fsproj"))
            .Descendants("Compile")
            .Select(e => e.Attribute("Include")!.Value.Replace('\\', '/'))
            .ToList();

    private static List<string> ProjectReferences(params string[] projectPath) =>
        XDocument.Load(Path.Combine([FindRepoRoot(), .. projectPath]))
            .Descendants("ProjectReference")
            .Select(e => Path.GetFileNameWithoutExtension(e.Attribute("Include")!.Value.Replace('\\', '/')))
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
    public void Projects_Follow_The_Dependency_Direction()
    {
        Assert.Empty(ProjectReferences("kernel", "Sazanami", "Sazanami.csproj"));
        Assert.Equal(["Sazanami"], ProjectReferences("kernel", "Samidare", "Samidare.fsproj"));
    }

    [Theory]
    [Trait("Architecture", "LinqLayering")]
    [InlineData("Samidare")]
    [InlineData("Sazanami")]
    public void Backend_Neutral_Projects_Do_Not_Mention_The_Backend(string project)
    {
        var root = Path.Combine(FindRepoRoot(), "kernel", project);
        var mentions = Directory.EnumerateFiles(root, "*", SearchOption.AllDirectories)
            .Where(path => path.EndsWith(".cs", StringComparison.Ordinal) || path.EndsWith(".fs", StringComparison.Ordinal))
            .Where(path => !path.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                        && !path.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .SelectMany(path => File.ReadLines(path).Select((line, i) => (path, line, number: i + 1)))
            .Where(x => x.line.Contains("polars", StringComparison.OrdinalIgnoreCase))
            .Select(x => $"{Path.GetRelativePath(root, x.path)}:{x.number}: {x.line.Trim()}")
            .ToList();

        Assert.True(mentions.Count == 0, $"{project} must stay backend-neutral:\n" + string.Join('\n', mentions));
    }

    [Fact]
    [Trait("Architecture", "LinqLayering")]
    public void Samidare_Does_Not_Reference_Polars_Or_Native_Handles()
    {
        var violations = new List<string>();

        foreach (var file in SamidareSources())
        {
            var lines = StripComments(File.ReadAllText(Path.Combine(SamidareDir, file))).Split('\n');
            for (int i = 0; i < lines.Length; i++)
            {
                var match = ForbiddenInFrontend.Match(lines[i]);
                if (match.Success)
                    violations.Add($"{file}:{i + 1}: '{match.Value.Trim()}'  ->  {lines[i].Trim()}");
            }
        }

        Assert.True(violations.Count == 0,
            "Samidare must not reference Polars.NET.Core, PolarsWrapper, native handles or Backend.Polars:\n" + string.Join('\n', violations));
    }

    [Fact]
    [Trait("Architecture", "LinqLayering")]
    public void Samidare_Uses_Its_Own_Vocabulary_Instead_Of_Polars_Enums()
    {
        var violations = new List<string>();

        foreach (var file in SamidareSources())
        {
            var rule = file.StartsWith("IR/", StringComparison.Ordinal) ? ForbiddenInIr : ForbiddenVocabularyInFrontend;
            var lines = StripComments(File.ReadAllText(Path.Combine(SamidareDir, file))).Split('\n');
            for (int i = 0; i < lines.Length; i++)
            {
                var match = rule.Match(lines[i]);
                if (match.Success)
                    violations.Add($"{file}:{i + 1}: '{match.Value}'  ->  {lines[i].Trim()}");
            }
        }

        Assert.True(violations.Count == 0,
            "IR must use its own vocabulary (LogicalType, JoinKind, RankMethod, ...); Polars enums are mapped in Backend/Polars/PolarsVocabulary.fs:\n"
            + string.Join('\n', violations));
    }
}
