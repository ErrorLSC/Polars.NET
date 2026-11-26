namespace Polars.FSharp.Tests

open System.Threading
open Apache.Arrow
open Xunit
open Polars.FSharp

/// Eager DataFrame operations run on the in-memory engine, as Python Polars runs its eager methods; lazy queries keep
/// the default engine, which streams the input in morsels.
type EagerEngineTests() =
    let frame () = DataFrame.create [ Series.create ("v", [| 0..399_999 |]) ]

    /// The number of batches the UDF of the expression `run` uses is called with.
    let batches (run: Expr -> DataFrame) =
        let calls = ref 0
        let mapped = Expr.Col("v").MapBatchesArrow(fun (a: IArrowArray) -> Interlocked.Increment(&calls.contents) |> ignore; a)
        use _ = run (mapped.Alias "m")
        calls.Value

    [<Fact>]
    member _.``Eager operations run on the in-memory engine`` () =
        use df = frame ()
        Assert.Equal(1, batches (fun e -> df.Select [ e ]))
        Assert.Equal(1, batches (fun e -> df.WithColumns [ e ]))
        Assert.True(batches (fun e -> df.Lazy().Select([ e ]).Collect()) > 1)
