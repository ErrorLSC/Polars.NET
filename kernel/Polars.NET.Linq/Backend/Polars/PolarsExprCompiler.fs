namespace Polars.NET.Linq.Backend.Polars

open System.Linq.Expressions
open Polars.NET.Core
open Kaminari.Samidare.IR
open Kaminari.Samidare

/// Compiles standalone LINQ expressions (outside a query pipeline) into native Polars expressions,
/// e.g. for TypedMerge conditions/setters and DataFrame-level key selectors.
module internal PolarsExprCompiler =

    let private compileWith (ctx: TranslationContext) (resolve: ScalarExpr -> ScalarExpr) (expr: Expression) =
        use _scope = CompilationScope.Enter PolarsQueryBackend.Instance
        tryTranslateToScalar ctx expr |> Option.map (resolve >> PolarsScalarLowerer.lower)

    /// Translates the expression to a symbolic ScalarExpr in a compilation scope for the Polars backend, then lowers
    /// it to an ExprHandle. `schema` holds the columns the expression reads (enum conversions depend on their types).
    let tryCompile (ctx: TranslationContext) (schema: LogicalSchema) (expr: Expression) : ExprHandle option =
        compileWith ctx (EnumResolution.resolveScalar schema) expr

    /// Like tryCompile, for an expression whose value is stored into a column of `columnType`.
    let tryCompileAssignment
        (ctx: TranslationContext)
        (schema: LogicalSchema)
        (columnType: LogicalType)
        (expr: Expression)
        : ExprHandle option =
        compileWith ctx (EnumResolution.resolveAssignment schema columnType) expr

    /// The schema, with column types, of a LazyFrame.
    let schemaOf (lf: LazyFrameHandle) : LogicalSchema = PolarsPhysicalLowerer.schemaOfLazyFrame lf

    /// The columns a merge expression reads: the target's under their names and the source's with `sourceSuffix`
    /// (as TranslationContext.Merge resolves them).
    let mergeSchema (target: LogicalSchema) (source: LogicalSchema) (sourceSuffix: string) : LogicalSchema =
        {
            Fields = target.Fields @ (source.Fields |> List.map (fun f -> { f with Name = f.Name + sourceSuffix }))
        }

    /// The type of `column` in `schema`, or Unknown.
    let columnType (schema: LogicalSchema) (column: string) : LogicalType =
        schema.TryFindField column |> Option.map (fun f -> f.DataType) |> Option.defaultValue LogicalType.Unknown
