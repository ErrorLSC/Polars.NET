using Microsoft.FSharp.Core;

namespace Polars.NET.Core.Linq;

/// <summary>
/// F# options from C# values, nullability included (Samidare's IrOption cannot carry C# annotations). Never let a
/// nullable value convert implicitly: FSharpOption's implicit conversion turns null into Some(null).
/// </summary>
internal static class FSharpInterop
{
    /// <summary>Some(value), or None for null.</summary>
    public static FSharpOption<T>? ToOption<T>(this T? value) where T : class => value is null ? null : FSharpOption<T>.Some(value);

    /// <summary>Some(value), or None without one.</summary>
    public static FSharpOption<T>? ToOption<T>(this T? value) where T : struct => value is { } v ? FSharpOption<T>.Some(v) : null;
}
