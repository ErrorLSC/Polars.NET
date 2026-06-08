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
        
        Config.withConfig [ Config.tableFormatting (TableFormatting.Nothing, Active.ResetToDefault) ] (fun () ->
            let dfS1 = df.ToString()
            Config.withConfig [ Config.tableHideDataFrameShape (Active.Set true) ] (fun () ->
                let isHidden = Config.tryGet "POLARS_FMT_TABLE_HIDE_DATAFRAME_SHAPE_INFORMATION" |> Option.defaultValue "0"
                Assert.Equal("1", isHidden)
                
                Config.tableFormatting (TableFormatting.AsciiFull, Active.ResetToDefault) ()
                
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
            Config.withConfig [ Config.floatPrecision (NumSet.Set 9) ] (fun () ->

                let precision = Config.getOr "" "float_precision"
                Assert.Equal("9", precision)

                Config.withConfig [ Config.tableRows (NumSet.Set 3) ] (fun () ->
                    let currentStatus = Config.status ConfigScope.All
                    
                    Assert.Equal("3", currentStatus.["POLARS_FMT_MAX_ROWS"])
                    Assert.Equal("9", currentStatus.["float_precision"])
                )

                let postStatus = Config.status ConfigScope.All
                
                Assert.Null(postStatus["POLARS_FMT_MAX_ROWS"])
                
                Assert.Equal("9", Config.getOr "" "float_precision")
            )
        finally
            Config.restoreDefaults()