using Kaminari.Sazanami.Text;

namespace Sazanami.Tests;

/// <summary><see cref="GlobPattern"/>: prefixes, recursion, and matching as the <c>glob</c> crate Polars uses for local paths.</summary>
public class GlobPatternTests
{
    [Theory]
    [InlineData("data/*.parquet", "data/", false)]
    [InlineData("data/year=*/x.parquet", "data/", true)]
    [InlineData("data/**/*.parquet", "data/", true)]
    [InlineData("data/**", "data/", true)]
    [InlineData("*.parquet", "", false)]
    [InlineData("/abs/dir/part-?.parquet", "/abs/dir/", false)]
    [InlineData("s3://bucket/a/[0-9]/f", "s3://bucket/a/", true)]
    public void ThePrefix_IsTheLiteralDirectories(string pattern, string prefix, bool recursive)
    {
        GlobPattern glob = GlobPattern.Parse(pattern);
        Assert.Equal(prefix, glob.Prefix);
        Assert.Equal(recursive, glob.Recursive);
    }

    [Theory]
    // '*' within a segment, a leading dot included, never across '/'.
    [InlineData("*.parquet", "a.parquet", true)]
    [InlineData("*.parquet", ".hidden.parquet", true)]
    [InlineData("*.parquet", "_tmp.parquet", true)]
    [InlineData("*.parquet", "sub/a.parquet", false)]
    [InlineData("*", "", true)]
    [InlineData("a*b*c", "abc", true)]
    [InlineData("a*b*c", "aXbYbZc", true)]
    [InlineData("a*b*c", "aXbYcZ", false)]
    [InlineData("*.parquet", "a.parquet.crc", false)]
    // '?' one character.
    [InlineData("part-?.parquet", "part-1.parquet", true)]
    [InlineData("part-?.parquet", "part-12.parquet", false)]
    // Sets and ranges, negated with '!' or '^'; a ']' first is literal.
    [InlineData("[ab].x", "a.x", true)]
    [InlineData("[ab].x", "c.x", false)]
    [InlineData("[0-9][0-9]", "42", true)]
    [InlineData("[0-9][0-9]", "4a", false)]
    [InlineData("[!0-9]", "x", true)]
    [InlineData("[!0-9]", "5", false)]
    [InlineData("[^0-9]", "x", true)]
    [InlineData("[]]x", "]x", true)]
    [InlineData("[*]", "*", true)]
    [InlineData("[*]", "a", false)]
    // '**' takes none or more segments.
    [InlineData("**/*.parquet", "a.parquet", true)]
    [InlineData("**/*.parquet", "y=1/m=2/a.parquet", true)]
    [InlineData("**/*.parquet", "y=1/m=2/a.csv", false)]
    [InlineData("y=*/**/a.parquet", "y=1/a.parquet", true)]
    [InlineData("y=*/**/a.parquet", "y=1/m=2/d=3/a.parquet", true)]
    [InlineData("y=*/**/a.parquet", "z=1/m=2/a.parquet", false)]
    [InlineData("**", "a/b/c", true)]
    [InlineData("*/**", "y/z", true)]
    [InlineData("*/**", "y", false)]
    [InlineData("**/**/x", "x", true)]
    [InlineData("a/**/b/**/c", "a/b/c", true)]
    [InlineData("a/**/b/**/c", "a/1/2/b/3/c", true)]
    [InlineData("a/**/b/**/c", "a/1/2/3/c", false)]
    // Case-sensitive; the segment count matters.
    [InlineData("*.Parquet", "a.parquet", false)]
    [InlineData("*/*", "a", false)]
    [InlineData("*", "a/b", false)]
    public void Paths_MatchAsTheGlobCrateMatches(string pattern, string relative, bool expected) =>
        Assert.Equal(expected, GlobPattern.Parse(pattern).IsMatch(relative));

    [Fact]
    public void ThePrefix_IsMatchedOnlyBelowIt()
    {
        GlobPattern glob = GlobPattern.Parse("root/dir/*.parquet");
        Assert.True(glob.IsMatch("x.parquet"));
        Assert.False(glob.IsMatch("dir/x.parquet"));
    }

    [Theory]
    [InlineData("data/a**/x")]
    [InlineData("data/**b")]
    [InlineData("data/[ab")]
    public void BadPatterns_AreRefused(string pattern) => Assert.Throws<ArgumentException>(() => GlobPattern.Parse(pattern));

    [Theory]
    [InlineData("data/x.parquet", false)]
    [InlineData("data/*.parquet", true)]
    [InlineData("data/x?.parquet", true)]
    [InlineData("data/[0-9].parquet", true)]
    public void Wildcards_AreRecognized(string path, bool glob) => Assert.Equal(glob, GlobPattern.IsGlob(path));

    /// <summary>A path with many segments (beyond the stack buffer) still matches.</summary>
    [Fact]
    public void DeepPaths_Match()
    {
        string deep = string.Join('/', Enumerable.Range(0, 100).Select(i => $"d{i}")) + "/f.parquet";
        Assert.True(GlobPattern.Parse("**/f.parquet").IsMatch(deep));
        Assert.False(GlobPattern.Parse("**/g.parquet").IsMatch(deep));
    }
}
