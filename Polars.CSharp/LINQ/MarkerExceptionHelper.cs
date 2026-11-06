using System.Runtime.CompilerServices;

namespace Polars.CSharp.Linq;

internal static class MarkerExceptionHelper
{
    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static InvalidOperationException Throw()
    {
        var frame = new System.Diagnostics.StackFrame(1);
        string methodName = frame.GetMethod()?.Name ?? "Marker";

        string message =
            $"The Polars LINQ method '{methodName}' is an expression marker and cannot be evaluated on client-side collections.\n" +
            "Cause: An earlier operation in the query could not be translated natively to Polars, causing the query to fall back to client-side evaluation.\n" +
            "Fix: Ensure native Polars operations run first, or isolate unsupported client-side methods by materializing data first (e.g., calling .ToList() / .ToDataFrame()).";

        return new InvalidOperationException(message);
    }
}
