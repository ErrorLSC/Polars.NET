using System.Runtime.CompilerServices;

namespace Polars.CSharp.Linq;

internal static class MarkerExceptionHelper
{
    /// <summary>
    /// Builds the exception thrown when a Polars LINQ marker method is evaluated client-side.
    /// The marker's name comes from <see cref="CallerMemberNameAttribute"/> (compile time; works under Native AOT,
    /// where stack-frame method lookup is unavailable).
    /// </summary>
    internal static InvalidOperationException Throw([CallerMemberName] string methodName = "Marker")
    {
        string message =
            $"The Polars LINQ method '{methodName}' is an expression marker and cannot be evaluated on client-side collections.\n" +
            "Cause: An earlier operation in the query could not be translated natively to Polars, causing the query to fall back to client-side evaluation.\n" +
            "Fix: Ensure native Polars operations run first, or isolate unsupported client-side methods by materializing data first (e.g., calling .ToList() / .ToDataFrame()).";

        return new InvalidOperationException(message);
    }
}
