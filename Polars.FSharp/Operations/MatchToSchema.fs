namespace Polars.FSharp

open Polars.NET.Core
open System

/// <summary>
/// Represents the action to take when a column from the target schema is missing in the current frame.
/// </summary>
type MissingColumnAction private (policyType: PlMissingColumnsPolicyType, expr: Expr option) =
    member internal this.Type = policyType
    member internal this.Expression = expr

    /// <summary>
    /// Insert the missing column with null values.
    /// </summary>
    static member Insert() = MissingColumnAction(PlMissingColumnsPolicyType.Insert, None)

    /// <summary>
    /// Raise an error if the column is missing.
    /// </summary>
    static member Raise() = MissingColumnAction(PlMissingColumnsPolicyType.Raise, None)

    /// <summary>
    /// Insert the missing column and fill it with the evaluated expression or literal value.
    /// </summary>
    /// <param name="expr">The expression to fill the missing column with.</param>
    static member InsertWith(expr: Expr) = 
        MissingColumnAction(PlMissingColumnsPolicyType.InsertWith, Some (expr.Clone()))

/// <summary>
/// Configuration for schema matching policies during a MatchToSchema operation.
/// Provides fine-grained control over how missing columns, struct fields, and type castings are handled.
/// </summary>
type MatchSchemaConfig =
    { /// Policy for handling top-level columns present in the target schema but missing in the frame.
      MissingColumns: MissingColumnAction
      /// Policy for handling missing fields within nested Struct columns.
      MissingStructFields: MissingColumnsPolicy
      /// Policy for handling extra fields within nested Struct columns that are not in the target schema.
      ExtraStructFields: ExtraColumnsPolicy
      /// Policy for handling integer type casting.
      IntegerCast: UpcastOrForbid
      /// Policy for handling floating-point type casting.
      FloatCast: UpcastOrForbid }

    /// <summary>
    /// Creates a default schema matching configuration.
    /// </summary>
    static member Default =
        { MissingColumns = MissingColumnAction.Raise()
          MissingStructFields = MissingColumnsPolicy.Raise
          ExtraStructFields = ExtraColumnsPolicy.Raise
          IntegerCast = UpcastOrForbid.Forbid
          FloatCast = UpcastOrForbid.Forbid }

    member internal this.ToCoreConfig() : PolarsWrapper.PlMatchToSchemaConfig =
        let mutable c = PolarsWrapper.PlMatchToSchemaConfig()
        c.MissingColumnsType <- this.MissingColumns.Type
        c.MissingColumnsExpr <- 
            match this.MissingColumns.Expression with
            | Some e -> e.CloneHandle()
            | None -> null
        c.MissingStructFields <- this.MissingStructFields.ToNative()
        c.ExtraStructFields <- this.ExtraStructFields.ToNative()
        c.IntegerCast <- this.IntegerCast.ToNative()
        c.FloatCast <- this.FloatCast.ToNative()
        c


[<AutoOpen>]
module MatchToSchemaOps =
    open System.Collections.Generic
    type LazyFrame with
        member this.MatchToSchema(schema: PolarsSchema, 
                                ?extraColumns: ExtraColumnsPolicy,
                                ?defaultConfig: MatchSchemaConfig,
                                ?columnOverrides: Map<string, MatchSchemaConfig>) : LazyFrame =
                                
            let exa = defaultArg extraColumns ExtraColumnsPolicy.Raise
            let coreDefault = (defaultArg defaultConfig MatchSchemaConfig.Default).ToCoreConfig()

            let coreOverrides = 
                columnOverrides
                |> Option.filter (fun m -> not m.IsEmpty)
                |> Option.map (fun m ->
                    let d = Dictionary<_,_>()
                    m |> Map.iter (fun k v -> d.[k] <- v.ToCoreConfig())
                    d
                )
                |> Option.toObj

            let handle = 
                PolarsWrapper.MatchToSchema(
                    this.CloneHandle(),
                    schema.Handle,
                    exa.ToNative(),
                    coreDefault,
                    coreOverrides
                )

            new LazyFrame(handle)