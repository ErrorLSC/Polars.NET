using System.Collections.Concurrent;

namespace Polars.NET.Core.Exceptions;

/// <summary>
/// Carries a .NET exception that a managed callback raises on purpose while Polars evaluates a plan (a LINQ
/// cardinality check, for instance) back to the caller as that exception, rather than as a
/// <see cref="PolarsException"/> holding its text. The callback throws the token <see cref="Capture"/> returns;
/// the native error that comes back names the token, and the error check rethrows the captured exception.
/// A native kernel that implements .NET checked arithmetic names <see cref="OverflowMarker"/> instead, which is
/// raised as an <see cref="OverflowException"/>.
/// </summary>
internal static class ManagedErrorRelay
{
    private const string Prefix = "[[polars-net-relay:";
    /// <summary>The marker a native kernel raises for .NET's OverflowException (checked arithmetic).</summary>
    private const string OverflowMarker = "[[polars-net-overflow]]";
    private const string Suffix = "]]";
    private const int MaxPending = 1024;

    private static readonly ConcurrentDictionary<string, Exception> Pending = new();

    /// <summary>
    /// Registers the exception the caller should see and returns the exception the callback throws instead.
    /// </summary>
    public static Exception Capture(Exception exception)
    {
        // A token whose native error never surfaced (the query was abandoned) is dropped eventually.
        if (Pending.Count >= MaxPending)
            Pending.Clear();

        string id = Guid.NewGuid().ToString("N");
        Pending[id] = exception;
        return new InvalidOperationException(Prefix + id + Suffix);
    }

    /// <summary>The captured exception a native error message names, if any (taken out of the relay).</summary>
    public static Exception? TryTake(string message)
    {
        if (message.Contains(OverflowMarker, StringComparison.Ordinal))
            return new OverflowException();

        int start = message.IndexOf(Prefix, StringComparison.Ordinal);
        if (start < 0)
            return null;

        start += Prefix.Length;
        int end = message.IndexOf(Suffix, start, StringComparison.Ordinal);
        if (end < 0)
            return null;

        return Pending.TryRemove(message[start..end], out var exception) ? exception : null;
    }
}
