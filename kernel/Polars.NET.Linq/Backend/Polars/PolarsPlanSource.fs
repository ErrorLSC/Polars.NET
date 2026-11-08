namespace Polars.NET.Linq.Backend.Polars

open Polars.NET.Core

/// Uniform contract to expose the compiled native LazyFrameHandle of a Polars queryable without leaking generic parameters.
type internal IPolarsPlanSource =
    /// Compiles the queryable pipeline stages and returns the final optimized LazyFrameHandle
    abstract member GetCompiledLazyFrameHandle: unit -> LazyFrameHandle
