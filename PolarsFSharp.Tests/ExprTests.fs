namespace PolarsFSharp.Tests

open Xunit
open PolarsFSharp

type ``Expression Logic Tests`` () =
    [<Fact>]
        member _.``Select inline style (Pythonic)`` () =
            use csv = new TempCsv("name,birthdate,weight,height\nQinglei,2025-11-25,70,1.80")
            let df = Polars.readCsv csv.Path None

            // 像 Python 一样写在 list 里面！
            let res = 
                df
                |> Polars.select [
                    Polars.col "name"
                    
                    // Inline 1: 简单的 alias
                    Polars.col "birthdate" |> Polars.alias "b_date"
                    
                    // Inline 2: 链式调用
                    (Polars.col "birthdate").Dt.Year().Alias("year")
                    
                    // Inline 3: 算术表达式
                    (Polars.col "weight" / (Polars.col "height" * Polars.col "height"))
                    |> Polars.alias "bmi"
                ]

            // 验证
            Assert.Equal(4L, res.Columns) // name, b_date, year, bmi
            
            // 使用新的 Option 取值 API 验证
            // Qinglei
            Assert.Equal("Qinglei", res.String("name", 0)) 
            // BMI ≈ 21.6
            Assert.True(res.Float("bmi", 0).Value > 21.6)
    [<Fact>]
    member _.``Filter by numeric value (> operator)`` () =
        use csv = new TempCsv("val\n10\n20\n30")
        let df = Polars.readCsv csv.Path None
        
        let res = df |> Polars.filter (Polars.col "val" .> Polars.lit 15)
        
        Assert.Equal(2L, res.Rows)
    [<Fact>]
    member _.``Filter by numeric value (< operator)`` () =
        use csv = new TempCsv("name,birthdate,weight,height\nBen Brown,1985-02-15,72.5,1.77\nQinglei,2025-11-25,70.0,1.80\nZhang,2025-10-31,55,1.75")
        let df = Polars.readCsv csv.Path (Some true)

        let res = df |> Polars.filter ((Polars.col "birthdate").Dt.Year() .< Polars.lit 1990 )

        Assert.Equal(1L,res.Rows)

    [<Fact>]
    member _.``Filter by string value (== operator)`` () =
        use csv = new TempCsv("name\nAlice\nBob\nAlice")
        let df = Polars.readCsv csv.Path None
        
        // SRTP 魔法测试
        let res = df |> Polars.filter (Polars.col "name" .== Polars.lit "Alice")
        
        Assert.Equal(2L, res.Rows)

    [<Fact>]
    member _.``Filter by double value (== operator)`` () =
        use csv = new TempCsv("value\n3.36\n4.2\n5\n3.36")
        let df = Polars.readCsv csv.Path None
        
        // SRTP 魔法测试
        let res = df |> Polars.filter (Polars.col "value" .== Polars.lit 3.36)
        
        Assert.Equal(2L, res.Rows)

    [<Fact>]
    member _.``Null handling works`` () =
        // 造一个带 null 的 CSV
        // age: 10, null, 30
        use csv = new TempCsv("age\n10\n\n30") 
        let lf = Polars.scanCsv csv.Path None

        // 测试 1: fill_null
        // 把 null 填成 0，然后筛选 age > 0
        // 结果应该是 3 行 (10, 0, 30)
        let res = 
            lf 
            |> Polars.withColumn (
                Polars.col "age" 
                |> Polars.fillNull (Polars.lit 0) 
                |> Polars.alias "age_filled"
            )
            |> Polars.filterLazy (Polars.col "age_filled" .>= Polars.lit 0)
            |> Polars.collect
        Assert.Equal(3L, res.Rows)
        
        // 测试 2: is_null
        // 筛选出 null 的行
        let df= Polars.readCsv csv.Path None 
        let nulls = df |> Polars.filter (Polars.col "age" |> Polars.isNull)
        Assert.Equal(1L, nulls.Rows)
