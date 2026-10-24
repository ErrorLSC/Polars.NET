namespace Polars.FSharp

open System
open System.IO
open Microsoft.DotNet.Interactive.Formatting

/// <summary>
/// Display utilities for DataFrame, LazyFrame, Series, PolarsSchema, and Expr in interactive environments.
/// </summary>
[<AutoOpen>]
module Display =

    /// <summary>
    /// Display DataFrame as Html Table.
    /// Respects global Polars configurations from CoreConfig.
    /// </summary>
    let toHtml (df: DataFrame) = df.ToHtml()

    /// <summary>
    /// Init notebook support
    /// </summary>
    let init () =
        Formatter.Register<DataFrame>(
            Action<DataFrame, TextWriter>(fun df writer -> writer.Write(df.ToHtml())),
            "text/html"
        )

        Formatter.Register<Series>(
            Action<Series, TextWriter>(fun s writer -> writer.Write(s.ToHtml())),
            "text/html"
        )

        Formatter.Register<LazyFrame>(
            Action<LazyFrame, TextWriter>(fun lf writer -> writer.Write(lf.ToHtml())),
            "text/html"
        )

        Formatter.Register<PolarsSchema>(
            Action<PolarsSchema, TextWriter>(fun schema writer -> writer.Write(schema.ToHtml())),
            "text/html"
        )

        Formatter.Register<Expr>(
            Action<Expr, TextWriter>(fun expr writer -> writer.Write(expr.ToHtml())),
            "text/html"
        )

