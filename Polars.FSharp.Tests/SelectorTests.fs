namespace Polars.FSharp.Tests

open System
open Xunit
open Polars.FSharp

type SelectorTests() =

    let mkDf () =
        let data = [
            {| Name = "Alice"; Age = 30; Salary = 5000.0; IsActive = true;  JoinDate = DateTime(2020, 1, 1) |}
            {| Name = "Bob";   Age = 25; Salary = 6000.0; IsActive = false; JoinDate = DateTime(2021, 5, 20) |}
        ]
        DataFrame.ofRecords data

    [<Fact>]
    member _.``Selector: Basic Type & Pattern Matching`` () =
        let df = mkDf()

        // --- Numeric (Age, Salary) ---
        // Python: cs.numeric()
        let numSel = pl.cs.numeric()

        let dfNum = df.Select [numSel]
        
        Assert.Equal(2L, dfNum.Width)
        Assert.Contains("Age", dfNum.Columns)
        Assert.Contains("Salary", dfNum.Columns)
        Assert.DoesNotContain("Name", dfNum.Columns)

        // --- IsActive ---
        let boolSel = pl.cs.byType pl.boolean
        let dfBool = df.Select [boolSel]
        
        Assert.Equal(1L, dfBool.Width)
        Assert.Equal("IsActive", dfBool.Columns.[0])

        // --- Starts With ---
        let nameSel = pl.cs.startsWith "Na"
        let dfName = df.Select [nameSel]
        
        Assert.Equal("Name", dfName.Columns.[0])

    [<Fact>]
    member _.``Selector: Set Operations (AND, OR, NOT)`` () =
        let df = mkDf()

        // --- Intersection (&&&) ---
        let selAnd = pl.cs.numeric() &&& pl.cs.contains "Ag"
        let dfAnd = df.Select [selAnd]

        Assert.Single dfAnd.Columns |> ignore 
        Assert.Equal("Age", dfAnd.Columns.[0])

        // --- Union (|||) ---
        let selOr = pl.cs.numeric() ||| pl.cs.byType pl.boolean
        let dfOr = df.Select [selOr]
        
        Assert.Equal(3L, dfOr.Width)
        Assert.Contains("Age", dfOr.Columns)
        Assert.Contains("Salary", dfOr.Columns)
        Assert.Contains("IsActive", dfOr.Columns)

        // --- Inversion (~~~) ---
        let selNot = ~~~(pl.cs.numeric())
        let dfNot = df.Select [selNot]
        
        Assert.Equal(3L, dfNot.Width)
        Assert.Contains("Name", dfNot.Columns)
        Assert.Contains("IsActive", dfNot.Columns)
        Assert.Contains("JoinDate", dfNot.Columns)
        Assert.DoesNotContain("Age", dfNot.Columns)

    [<Fact>]
    member _.``Selector: Exclusion and Arithmetic`` () =
        let df = mkDf()

        let selExc = pl.cs.all().Exclude ["Salary"; "JoinDate"]
        let dfExc = df.Select [selExc]
        
        Assert.DoesNotContain("Salary", dfExc.Columns)
        Assert.DoesNotContain("JoinDate", dfExc.Columns)
        Assert.Contains("Name", dfExc.Columns)

        let selDiff = pl.cs.numeric() - pl.cs.byType pl.float64
        let dfDiff = df.Select [selDiff]
        
        Assert.Single dfDiff.Columns |> ignore
        Assert.Equal("Age", dfDiff.Columns.[0])

    [<Fact>]
    member _.``Selector: Regex Matching`` () =
        let df = mkDf()

        let selRegex = pl.cs.matches "^Is.*|.*me$"
        let dfRegex = df.Select [selRegex]
        
        Assert.Equal(2L, dfRegex.Width)
        Assert.Contains("IsActive", dfRegex.Columns)
        Assert.Contains("Name", dfRegex.Columns)

    [<Fact>]
    member _.``Selector: Complex ETL Pipeline`` () =

        let df = mkDf()
        
        let dfTransformed = 
            df.Select([
                pl.cs.numeric()
                    .ToExpr()
                    .Truediv(pl.lit 100.0)
                    .Name.Suffix("_pct")
                
                pl.cs.byType(pl.string).ToExpr().Str.ToUpper()
                
                (~~~(pl.cs.numeric() ||| pl.cs.byType pl.string)).ToExpr()
            ])

        Assert.Contains("Age_pct", dfTransformed.Columns)
        Assert.Contains("Salary_pct", dfTransformed.Columns)
        
        // Alice Age 30 -> 0.3
        Assert.Equal(0.3, dfTransformed.Cell<double>("Age_pct",0))
        
        Assert.Contains("IsActive", dfTransformed.Columns)
        
    [<Fact>]
    member _.``Integration: GroupBy, Explode with Selectors`` () =
        let data = [
            {| Region = "US";  Tag1 = ["A"; "B"]; Tag2 = ["X";"Q"]; Sales = 100; Profit = 20 |}
            {| Region = "EU";  Tag1 = ["C"];      Tag2 = ["Y"]; Sales = 200; Profit = 40 |}
            {| Region = "US";  Tag1 = ["A"];      Tag2 = ["Z"]; Sales = 150; Profit = 30 |}
        ]
        let df = DataFrame.ofRecords data

        // ==========================================
        // Case A: Explode
        // ==========================================
        
        let dfTag = df.Explode(pl.cs.startsWith "Tag")
        Assert.Equal(4L, dfTag.Height) // 2 + 1 + 1

        // ==========================================
        // Case B: GroupBy & Agg
        // ==========================================
        let dfAgg =
            df
            |> pl.groupBy [pl.col "Region"] [pl.cs.numeric().ToExpr().Sum()]
            |> pl.sort (pl.col "Region",false)

        Assert.Equal(2L, dfAgg.Height)
        // US Sum: 100 + 150 = 250
        Assert.Equal(250, dfAgg.Cell<int>( "Sales",1)) 
        // US Profit: 20 + 30 = 50
        Assert.Equal(50, dfAgg.Cell<int>( "Profit",1))
    [<Fact>]
    [<Trait("Selector","Types")>]
    member _.``Selector: Advanced Regex Patterns and Generic Types`` () =
        let data = [
            {|
                pureAlpha = 1                   // int
                alphaNum123 = "test"            // string
                原神启动 = 3.14                     // float
                日文カタカナ = true                // bool
                한글 = DateTime(2023, 1, 1)      // DateTime
                Mix中英123 = 100L                 // int64
                ``has space`` = 42              // int (with space)
            |}
        ]
        let df = DataFrame.ofRecords data

        // ==========================================
        // Regex Selectors 
        // ==========================================

        let dfAlpha = df.Select [ pl.cs.alpha (Some true) None ]
        Assert.Equal(1L, dfAlpha.Width)
        Assert.Contains("pureAlpha", dfAlpha.Columns)

        // A-2. alpha (Unicode):  CJK included 
        let dfAlphaUnicode = df.Select [ pl.cs.alpha (Some false) None ]
        Assert.Equal(4L, dfAlphaUnicode.Width) // pureAlpha, 原神启动, 日文カタカナ, 한글

        // alphanumeric: 
        let dfAlphaNum = df.Select [ pl.cs.alphanumeric (Some true) None ]
        Assert.Equal(2L, dfAlphaNum.Width)
        Assert.Contains("pureAlpha", dfAlphaNum.Columns)
        Assert.Contains("alphaNum123", dfAlphaNum.Columns)

        // cjk
        let dfCjk = df.Select [ pl.cs.cjk None None None None ]
        Assert.Equal(3L, dfCjk.Width)
        Assert.Contains("原神启动", dfCjk.Columns)
        Assert.Contains("日文カタカナ", dfCjk.Columns)
        Assert.Contains("한글", dfCjk.Columns)

        // cjkAlphanumeric
        let dfCjkMix = df.Select [ pl.cs.cjkAlphanumeric None None None None None ]
        Assert.Equal(6L, dfCjkMix.Width) 
        Assert.DoesNotContain("has space", dfCjkMix.Columns)

        // IgnoreSpaces 
        let dfSpace = df.Select [ pl.cs.alpha (Some true) (Some true) ]
        Assert.Equal(2L, dfSpace.Width)
        Assert.Contains("pureAlpha", dfSpace.Columns)
        Assert.Contains("has space", dfSpace.Columns)

        // ==========================================
        // Generic Type Selector
        // ==========================================

        let dfInt = df.Select [ pl.cs.byGenericType<int>() ]
        Assert.Equal(2L, dfInt.Width)
        Assert.Contains("pureAlpha", dfInt.Columns)
        Assert.Contains("has space", dfInt.Columns)

        let dfStr = df.Select [ pl.cs.byGenericType<string>() ]
        Assert.Equal(1L, dfStr.Width)
        Assert.Contains("alphaNum123", dfStr.Columns)
        
        let dfDt = df.Select [ pl.cs.byGenericType<DateTime>() ]
        Assert.Equal(1L, dfDt.Width)
        Assert.Contains("한글", dfDt.Columns)

        let dfWithoutBoolOrLong = 
            df.Select [ 
                pl.cs.all() 
                  - pl.cs.byGenericType<bool>() 
                  - pl.cs.byGenericType<int64>() 
            ]
        Assert.DoesNotContain("日文カタカナ", dfWithoutBoolOrLong.Columns)
        Assert.DoesNotContain("Mix中英123", dfWithoutBoolOrLong.Columns)