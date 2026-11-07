namespace Polars.NET.Linq.IR

open System

/// Translation-time capability queries answered by the execution backend.
/// Lets the frontend decide between native pushdown and managed fallback without referencing the backend.
type internal IBackendCapabilities =
    /// Whether the backend's native regex engine can compile the pattern (inline flags already applied).
    abstract IsRegexPatternSupported: pattern: string -> bool
    /// Whether the backend can evaluate ScalarFunction.AssertCount (fail the query while it runs).
    abstract SupportsAssertions: bool

/// Binds backend-native runtime objects (frames, compiled queryables) as opaque table sources.
type internal ISourceBinder =
    /// Returns an opaque table source when the value is a backend-native object, otherwise None.
    /// In-memory collections are not handled here: the frontend binds them symbolically as LogicalPlanSource.InMemory.
    abstract TryBindExternalSource: value: obj -> ILogicalTableSource option

/// Services an execution backend provides to the frontend during IR construction.
type internal IQueryBackend =
    inherit IBackendCapabilities
    inherit ISourceBinder

/// The state one compilation shares with the translators it runs: the backend, the diagnostics collected
/// while translating (the full-fallback reason, the UDFs pushed down) and per-projection translation hints.
///
/// Every entry point that translates expressions opens a scope with `CompilationScope.Enter` and reads the
/// diagnostics from it when done. Compilations nest (a subquery grafted into its outer query compiles while
/// the outer one is compiling): each opens its own scope, so nothing leaks between them, and an outer
/// compilation that wants a nested one's diagnostics takes them with `Absorb`. The innermost open scope is
/// found through a thread-local reference, because translators are deeply recursive functions sharing one
/// evaluator signature.
[<AllowNullLiteral>]
type internal CompilationScope private (backend: IQueryBackend, enclosing: CompilationScope) =
    [<ThreadStatic; DefaultValue>]
    static val mutable private current: CompilationScope

    let udfs = ResizeArray<string>()

    member _.Backend = backend

    /// Why the query needs the full LINQ-to-Objects fallback, when a stage recorded it.
    member val FallbackReason: string option = None with get, set

    /// Highest index read per (target, separator) for the `s.Split(sep)[i]` accesses of the projection
    /// being translated, so that every access splits into the same number of parts.
    member val SplitIndexHints: Map<string * string, int> option = None with get, set

    /// Whether translation evaluated part of the query against its data (an uncorrelated subquery folded
    /// into a constant), which ties the result to the data read at compile time.
    member val EvaluatedAtCompile = false with get, set

    /// Conditions under which the expression being translated runs in C# (the left side of `&&`, the test of a
    /// `?:` branch, ...), each with the translation context (boxed) it was translated in. A UDF translated under
    /// guards of its own context is only invoked where they all hold, as C# short-circuit evaluation would.
    member val UdfGuards: (obj * ScalarExpr) list = [] with get, set

    /// UDFs pushed down to the backend, in the order first recorded.
    member _.Udfs: string list = List.ofSeq udfs

    member _.RecordUdf(description: string) =
        if not (udfs.Contains description) then
            udfs.Add description

    /// Takes the diagnostics of a compilation nested in this one; its fallback reason, when it has one,
    /// is the more specific.
    member this.Absorb(fallbackReason: string option, nestedUdfs: string list, evaluatedAtCompile: bool) =
        this.FallbackReason <- fallbackReason |> Option.orElse this.FallbackReason
        this.EvaluatedAtCompile <- this.EvaluatedAtCompile || evaluatedAtCompile
        for udf in nestedUdfs do
            this.RecordUdf udf

    /// The innermost open scope. Throws if translation runs outside any scope.
    static member Current: CompilationScope =
        match CompilationScope.current with
        | null ->
            invalidOp
                "[Polars.NET.Linq Bug] Expression translation ran outside a compilation scope. The translation entry point must call CompilationScope.Enter."
        | scope -> scope

    /// The innermost open scope, if any.
    static member TryCurrent: CompilationScope option = Option.ofObj CompilationScope.current

    /// Opens a scope for the current thread until it is disposed, which reopens the enclosing one.
    static member Enter(backend: IQueryBackend) : CompilationScope =
        let scope = new CompilationScope(backend, CompilationScope.current)
        CompilationScope.current <- scope
        scope

    interface IDisposable with
        member _.Dispose() = CompilationScope.current <- enclosing
