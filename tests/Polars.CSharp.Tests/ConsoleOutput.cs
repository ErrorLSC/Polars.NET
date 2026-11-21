namespace Polars.CSharp.Tests;

/// <summary>
/// Tests that swap the process-wide <see cref="Console.Out"/> for a writer of their own: they run on their own, after
/// the parallel tests, so no other test writes to their writer (or to it after it is disposed).
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class ConsoleOutput
{
    public const string Name = "Console output";
}
