namespace Polars.FSharp.Tests

open Xunit
open Polars.FSharp

type ConfigTests () =
    [<Fact>]
    [<Trait("Config", "UsingBlock")>]
    member _.``Test Config TableFormat`` () =
        Config.restoreDefaults()
        
        use sInitial = Series.create("nihao", [| "123"; "321" |])
        use df = [sInitial] |> pl.dataframe
        
        Config.withConfig [ Config.tableFormatting (Some TableFormatting.Nothing, None) ] (fun () ->
            let dfS1 = df.ToString()
            Config.withConfig [ Config.tableHideDataFrameShape (Some true) ] (fun () ->
                let isHidden = Config.tryGet "POLARS_FMT_TABLE_HIDE_DATAFRAME_SHAPE_INFORMATION" |> Option.defaultValue "0"
                Assert.Equal("1", isHidden)
                
                Config.tableFormatting (Some TableFormatting.AsciiFull, None) ()
                
                use sInner = Series.create("byebye", [114514; 1919810])
                let dfString = sInner.ToFrame().ToString()
                
                Assert.DoesNotContain("shape", dfString)
                Assert.Contains("+", dfString)
            )
            
            Assert.Equal(dfS1, df.ToString())
        )
        
        Config.restoreDefaults()
        Assert.Contains("┘", df.ToString())


    [<Fact>]
    [<Trait("Config", "SingletonGhostContamination")>]
    member _.``Test Config Singleton Snapshot Isolation`` () =
        Config.restoreDefaults()

        try
            Config.withConfig [ Config.floatPrecision (Some 9L) ] (fun () ->

                let precision = Config.getOr "" "float_precision"
                Assert.Equal("9", precision)

                Config.withConfig [ Config.tableRows (Some 3) ] (fun () ->
                    let currentStatus = Config.status()
                    
                    Assert.Equal("3", currentStatus.["POLARS_FMT_MAX_ROWS"])
                    Assert.Equal("9", currentStatus.["float_precision"])
                )

                let postStatus = Config.status()
                
                Assert.Null(postStatus["POLARS_FMT_MAX_ROWS"])
                
                Assert.Equal("9", Config.getOr "" "float_precision")
            )
        finally
            Config.restoreDefaults()