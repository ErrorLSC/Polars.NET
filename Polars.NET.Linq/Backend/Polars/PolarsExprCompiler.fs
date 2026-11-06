namespace Polars.NET.Linq.Backend.Polars

open System.Linq.Expressions
open Polars.NET.Core
open Polars.NET.Linq
open Polars.NET.Linq.IR

/// Compiles standalone LINQ expressions (outside a query pipeline) into native Polars expressions,
/// e.g. for TypedMerge conditions/setters and DataFrame-level key selectors.
module internal PolarsExprCompiler =

    /// Translates the expression to a symbolic ScalarExpr in a compilation scope for the Polars backend, then lowers it to an ExprHandle.
    /// The frame's column types are not known here, so enum conversions take columns to hold underlying values.
    let tryCompile (ctx: TranslationContext) (expr: Expression) : ExprHandle option =
        use _scope = CompilationScope.Enter PolarsQueryBackend.Instance
        tryTranslateToScalar ctx expr
        |> Option.map (EnumResolution.resolveScalar LogicalSchema.Empty >> PolarsScalarLowerer.lower)
