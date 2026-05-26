namespace Polars.FSharp

open Polars.NET.Core

type [<Struct>] MetaOps(handle: ExprHandle) = 
    member this.OutputName() =
        PolarsWrapper.ExprGetOutputName handle
    /// <summary>Return the original expression.</summary>
    member this.AsExpression() =
        new Expr(handle)  

    /// <summary>Indicate if this expression is a basic (non-regex) unaliased column.</summary>
    member this.IsColumn() =
        PolarsWrapper.IsColumn(handle)

    /// <summary>Indicate if this expression expands to columns that match a regex pattern.</summary>
    member this.IsRegexProjection() =
        PolarsWrapper.IsRegexProjection(handle)

    /// <summary>
    /// Indicate if this expression only selects columns (optionally with aliasing).
    /// </summary>
    /// <param name="allowAliasing">If False (default), any aliasing is not considered to be column selection.</param>
    member this.IsColumnSelection(?allowAliasing: bool) =
        let allowAliasing = defaultArg allowAliasing false
        PolarsWrapper.IsColumnSelection(handle, allowAliasing)

    /// <summary>Indicate if this expression is a literal value (optionally aliased).</summary>
    member this.IsLiteral(?allowAliasing: bool) =
        let allowAliasing = defaultArg allowAliasing false
        PolarsWrapper.IsLiteral(handle, allowAliasing)

    /// <summary>Indicate if this expression expands into multiple expressions.</summary>
    member this.HasMultipleOutputs() =
        PolarsWrapper.HasMultipleOutputs(handle)

    /// <summary>Remove any aliases from this expression and return the inner expression.</summary>
    member this.UndoAliases() =
        new Expr(PolarsWrapper.UndoAlias(PolarsWrapper.CloneExpr handle))  

    /// <summary>Get a list with the root column name(s).</summary>
    member this.RootNames() =
        PolarsWrapper.RootNames(handle)

    // /// <summary>Format the expression as a tree (plain or Graphviz dot).</summary>
    // member this.FormatTree(?displayAsDot: bool, ?schema: PolarsSchema) =
    //     let displayAsDot = defaultArg displayAsDot false
    //     let schemaHandle = schema |> Option.map (fun s -> s.Handle) |> Option.toObj
    //     PolarsWrapper.FormatTree(handle, displayAsDot, schemaHandle)

    /// <summary>Pop the latest expression and return the input(s) as an array of Expr.</summary>
    member this.Pop() =
        let handles = PolarsWrapper.Pop(PolarsWrapper.CloneExpr handle)
        handles |> Array.map (fun h -> new Expr(h)) 

    /// <summary>Indicate if this expression is the same as another expression.</summary>
    member this.Equals(other: Expr) =
        PolarsWrapper.ExprEquals(handle, other.Handle)  
    /// <summary>Indicate if this expression is NOT the same as another expression.</summary>
    member this.NotEquals(other: Expr) =
        this.Equals(other) |> not