namespace Polars.FSharp

open System
open System.IO
open System.Reflection
open System.Diagnostics.CodeAnalysis

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

    /// .NET Interactive's non-generic Formatter.Register(Type, Action&lt;obj, TextWriter&gt;, string),
    /// looked up late-bound in the already-loaded kernel assemblies (no compile-time dependency).
    [<UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification = "Notebook hook only: .NET Interactive kernels load assemblies dynamically and never run trimmed or Native AOT. Outside a kernel the lookup finds nothing.")>]
    [<UnconditionalSuppressMessage("Trimming", "IL2075",
        Justification = "Notebook hook only; see IL2026. Formatter lives in the kernel's own (never trimmed) assembly.")>]
    let private tryFindFormatterRegister () : MethodInfo option =
        AppDomain.CurrentDomain.GetAssemblies()
        |> Array.tryPick (fun a ->
            if a.GetName().Name = "Microsoft.DotNet.Interactive.Formatting" then
                Option.ofObj (a.GetType "Microsoft.DotNet.Interactive.Formatting.Formatter")
            else
                None)
        |> Option.bind (fun formatter ->
            Option.ofObj (
                formatter.GetMethod(
                    "Register",
                    BindingFlags.Public ||| BindingFlags.Static,
                    null,
                    [| typeof<Type>; typeof<Action<obj, TextWriter>>; typeof<string> |],
                    null
                )
            ))

    /// <summary>
    /// Init notebook support: registers text/html formatters for DataFrame, Series, LazyFrame,
    /// PolarsSchema and Expr with .NET Interactive (Polyglot / Jupyter) when its kernel is loaded.
    /// Outside a notebook kernel this does nothing.
    /// </summary>
    let init () =
        match tryFindFormatterRegister () with
        | None -> ()
        | Some register ->
            let registerHtml (t: Type) (render: obj -> string) =
                let write = Action<obj, TextWriter>(fun value writer -> writer.Write(render value))
                register.Invoke(null, [| box t; box write; box "text/html" |]) |> ignore

            registerHtml typeof<DataFrame> (fun o -> (o :?> DataFrame).ToHtml())
            registerHtml typeof<Series> (fun o -> (o :?> Series).ToHtml())
            registerHtml typeof<LazyFrame> (fun o -> (o :?> LazyFrame).ToHtml())
            registerHtml typeof<PolarsSchema> (fun o -> (o :?> PolarsSchema).ToHtml())
            registerHtml typeof<Expr> (fun o -> (o :?> Expr).ToHtml())
