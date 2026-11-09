namespace Kaminari.Samidare

open System

/// <summary>
/// A public as-of join tolerance argument. At most one member has a value; none means no tolerance.
/// </summary>
type IAsOfToleranceValue =
    /// <summary>A fixed time span.</summary>
    abstract Duration: Nullable<TimeSpan>
    /// <summary>Text in the syntax of the execution engine, passed to it as is.</summary>
    abstract Text: string
    /// <summary>An integer distance.</summary>
    abstract Integer: Nullable<int64>
    /// <summary>A floating-point distance.</summary>
    abstract Float: Nullable<double>
