namespace Polars.FSharp.Tests

open Xunit
open Polars.FSharp
open Microsoft.DotNet.Interactive.Formatting

type DisplayTests() =
    [<Fact>]
    [<Trait("Display", "Notebook")>]
    member _.``Display.init registers html formatters with .NET Interactive`` () =
        // Touch the Formatter type so the kernel assembly is loaded, as it is inside a notebook.
        Assert.NotNull(typeof<Formatter>)
        Display.init ()

        use df = pl.dataframe [ Series.create ("a", [| 1; 2; 3 |]) ]
        use s = Series.create ("b", [| "x"; "y" |])
        use lf = df.Lazy()

        Assert.Equal(df.ToHtml(), df.ToDisplayString "text/html")
        Assert.Equal(s.ToHtml(), s.ToDisplayString "text/html")
        Assert.Equal(lf.ToHtml(), lf.ToDisplayString "text/html")
