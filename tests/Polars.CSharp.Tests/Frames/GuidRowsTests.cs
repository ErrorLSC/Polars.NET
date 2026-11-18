namespace Polars.CSharp.Tests;

/// <summary>Guids keep their value through the engine: as top-level and nested members, nullable or not.</summary>
public class GuidRowsTests
{
    private static readonly Guid A = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");
    private static readonly Guid B = Guid.Parse("fedcba98-7654-3210-0123-456789abcdef");

    [Fact]
    public void FromThenRows_KeepsTheGuids()
    {
        GuidRow[] rows = [new(A, null, new GuidHolder(B)), new(B, A, null)];
        using var df = DataFrame.From(rows);
        Assert.Equal(rows, df.Rows<GuidRow>().ToArray());
    }

    public sealed record GuidHolder(Guid Inner);

    public sealed record GuidRow(Guid Id, Guid? Maybe, GuidHolder? Nested);
}
