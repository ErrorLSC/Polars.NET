namespace Polars.NET.Linq.IR

open System

/// Translation-time capability queries answered by the execution backend.
/// Lets the frontend decide between native pushdown and managed fallback without referencing the backend.
type internal IBackendCapabilities =
    /// Whether the backend's native regex engine can compile the pattern (inline flags already applied).
    abstract IsRegexPatternSupported: pattern: string -> bool

/// Binds backend-native runtime objects (frames, compiled queryables) as opaque table sources.
type internal ISourceBinder =
    /// Returns an opaque table source when the value is a backend-native object, otherwise None.
    /// In-memory collections are not handled here: the frontend binds them symbolically as LogicalPlanSource.InMemory.
    abstract TryBindExternalSource: value: obj -> ILogicalTableSource option

/// Services an execution backend provides to the frontend during IR construction.
type internal IQueryBackend =
    inherit IBackendCapabilities
    inherit ISourceBinder

/// Thread-local, nestable ambient backend scope for expression translation.
/// Entry points (IR construction, standalone expression translation) open it via BackendScope.Enter.
type internal BackendScope =
    [<ThreadStatic; DefaultValue>]
    static val mutable private current: IQueryBackend option

    /// The backend of the innermost active scope. Throws if translation runs outside any scope.
    static member Current: IQueryBackend =
        match BackendScope.current with
        | Some backend -> backend
        | None ->
            invalidOp
                "[Polars.NET.Linq Bug] Expression translation ran outside a backend scope. The translation entry point must call BackendScope.Enter."

    /// Activates the backend for the current thread until the returned scope is disposed (restores the previous one).
    static member Enter(backend: IQueryBackend) : IDisposable =
        let previous = BackendScope.current
        BackendScope.current <- Some backend
        { new IDisposable with
            member _.Dispose() = BackendScope.current <- previous }
