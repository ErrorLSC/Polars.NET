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

    [<Fact>]
    member _.``Test ToHtml and ToDump for DataFrame, Series, LazyFrame, PolarsSchema, Expr`` () =
        use s1 = Series.create("id", [Some 1; None; Some 3])
        use s2 = Series.create("name", ["Alice"; "Bob"; "Charlie"])
        use df = DataFrame.create [s1; s2]

        let invokeToDump (obj: obj) =
            let m =
                obj.GetType().GetMethod(
                    "ToDump",
                    System.Reflection.BindingFlags.Instance
                    ||| System.Reflection.BindingFlags.NonPublic
                    ||| System.Reflection.BindingFlags.Public
                )
            Assert.NotNull(m)
            m.Invoke(obj, null)

        // 1. DataFrame.ToHtml & ToDump
        let dfHtml = df.ToHtml()
        Assert.Contains("pl-dataframe", dfHtml)
        Assert.Contains("(3 rows, 2 columns)", dfHtml)
        Assert.Contains("Alice", dfHtml)
        Assert.Contains("<td class='pl-null'>null</td>", dfHtml)
        Assert.Equal(dfHtml, Display.toHtml df)
        let dfDump = invokeToDump df
        Assert.Equal(box dfHtml, dfDump)

        // 2. Series.ToHtml & ToDump
        let sHtml = s1.ToHtml()
        Assert.Contains("pl-dataframe", sHtml)
        Assert.Contains("(3 rows, 1 columns)", sHtml)
        let sDump = invokeToDump s1
        Assert.Equal(box sHtml, sDump)

        // 3. LazyFrame.ToHtml & ToDump
        use lf = df.Lazy() |> LazyFrame.filter (pl.col "id" .> pl.lit 1)
        let lfHtml = lf.ToHtml()
        Assert.Contains("pl-lazy-container", lfHtml)
        Assert.Contains("UNCOLLECTED", lfHtml)
        let lfDump = invokeToDump lf
        Assert.NotNull(lfDump)

        // 4. PolarsSchema.ToHtml & ToDump
        use schema = df.Schema
        let schemaHtml = schema.ToHtml()
        Assert.Contains("pl-schema-table", schemaHtml)
        Assert.Contains("(2 fields)", schemaHtml)
        Assert.Contains("id", schemaHtml)
        Assert.Contains("name", schemaHtml)
        let schemaDump = invokeToDump schema
        Assert.NotNull(schemaDump)

        // 5. Expr.ToHtml & ToDump
        let expr = (pl.col "id" + pl.lit 10).Alias("id_plus_10")
        let treeWithSchema = expr.Meta.FormatTree(schema = schema)
        Assert.NotEmpty(treeWithSchema)
        let exprHtml = expr.ToHtml(schema)
        Assert.Contains("pl-expr-box", exprHtml)
        Assert.Contains("pl-expr-formula", exprHtml)
        Assert.Contains("pl-expr-tree", exprHtml)
        let exprDump = invokeToDump expr
        Assert.NotNull(exprDump)