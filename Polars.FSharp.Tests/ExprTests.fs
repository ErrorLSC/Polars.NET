namespace Polars.FSharp.Tests

open System
open Xunit
open Polars.FSharp

type ``Expression Logic Tests`` () =
    [<Fact>]
        member _.``Select inline style (Pythonic)`` () =
            use csv = new TempCsv "name,birthdate,weight,height\nQinglei,2025-11-25,70,1.80"
            let df = Polars.readCsv csv.Path None

            // 像 Python 一样写在 list 里面！
            let res = 
                df
                |> Polars.select [
                    Polars.col "name"
                    
                    // Inline 1: 简单的 alias
                    Polars.col "birthdate" |> Polars.alias "b_date"
                    
                    // Inline 2: 链式调用
                    (Polars.col "birthdate").Dt.Year().Alias "year"
                    
                    // Inline 3: 算术表达式
                    Polars.col "weight" / (Polars.col "height" * Polars.col "height")
                    |> Polars.alias "bmi"
                ]

            // 验证
            Assert.Equal(4L, res.Columns) // name, b_date, year, bmi
            
            // 使用新的 Option 取值 API 验证
            // Qinglei
            Assert.Equal("Qinglei", res.String("name", 0).Value) 
            // BMI ≈ 21.6
            Assert.True(res.Float("bmi", 0).Value > 21.6)
    [<Fact>]
    member _.``Filter by numeric value (> operator)`` () =
        use csv = new TempCsv "val\n10\n20\n30"
        let df = Polars.readCsv csv.Path None
        
        let res = df |> Polars.filter (Polars.col "val" .> Polars.lit 15)
        
        Assert.Equal(2L, res.Rows)
    [<Fact>]
    member _.``Filter by numeric value (< operator)`` () =
        use csv = new TempCsv "name,birthdate,weight,height\nBen Brown,1985-02-15,72.5,1.77\nQinglei,2025-11-25,70.0,1.80\nZhang,2025-10-31,55,1.75"
        let df = Polars.readCsv csv.Path (Some true)

        let res = df |> Polars.filter ((Polars.col "birthdate").Dt.Year() .< Polars.lit 1990 )

        Assert.Equal(1L,res.Rows)

    [<Fact>]
    member _.``Filter by string value (== operator)`` () =
        use csv = new TempCsv "name\nAlice\nBob\nAlice"
        let df = Polars.readCsv csv.Path None
        
        // SRTP 魔法测试
        let res = df |> Polars.filter (Polars.col "name" .== Polars.lit "Alice")
        
        Assert.Equal(2L, res.Rows)

    [<Fact>]
    member _.``Filter by double value (== operator)`` () =
        use csv = new TempCsv "value\n3.36\n4.2\n5\n3.36"
        let df = Polars.readCsv csv.Path None
        
        // SRTP 魔法测试
        let res = df |> Polars.filter (Polars.col "value" .== Polars.lit 3.36)
        
        Assert.Equal(2L, res.Rows)

    [<Fact>]
    member _.``Null handling works`` () =
        // 造一个带 null 的 CSV
        // age: 10, null, 30
        use csv = new TempCsv "age\n10\n\n30" 
        let lf = Polars.scanCsv csv.Path None

        // 测试 1: fill_null
        // 把 null 填成 0，然后筛选 age > 0
        // 结果应该是 3 行 (10, 0, 30)
        let res = 
            lf 
            |> Polars.withColumnLazy (
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
    [<Fact>]
    member _.``IsBetween with DateTime Literals`` () =
        // 构造数据: Qinglei 的生日
        use csv = new TempCsv "name,birthdate,height\nQinglei,1990-05-20,1.80\nTooOld,1980-01-01,1.80\nTooShort,1990-05-20,1.60"
        
        // 必须开启日期解析
        let df = Polars.readCsv csv.Path (Some true)

        // Python logic translation:
        // filter(
        //    col("birthdate").is_between(date(1982,12,31), date(1996,1,1)),
        //    col("height") > 1.7
        // )
        
        // 定义边界
        let startDt = DateTime(1982, 12, 31)
        let endDt = DateTime(1996, 1, 1)

        let res = 
            df 
            |> Polars.filter (
                // 条件 1: 生日区间
                (Polars.col "birthdate").IsBetween(Polars.lit startDt, Polars.lit endDt)
                .&& // 条件 2: AND
                // 条件 3: 身高
                (Polars.col "height" .> Polars.lit 1.7)
            )

        // 验证: 只有 Qinglei 符合
        Assert.Equal(1L, res.Rows)
        Assert.Equal("Qinglei", res.String("name", 0).Value)

type ``String Logic Tests`` () =

    [<Fact>]
    member _.``String operations (Case, Slice, Replace)`` () =
        // 脏数据: "  Hello World  ", "foo BAR"
        use csv = new TempCsv "text\nHello World\nfoo BAR"
        let df = Polars.readCsv csv.Path None

        let res = 
            df 
            |> Polars.select [
                Polars.col "text"
                
                // 1. 转大写
                (Polars.col "text").Str.ToUpper().Alias "upper"
                
                // 2. 切片 (取前 3 个字符)
                (Polars.col "text").Str.Slice(0L, 3UL).Alias "slice"
                
                // 3. 替换 (把 'o' 换成 '0')
                (Polars.col "text").Str.ReplaceAll("o", "0").Alias "replaced"
                
                // 4. 长度
                (Polars.col "text").Str.Len().Alias "len"
            ]

        // 验证 Row 0: "Hello World"
        Assert.Equal("HELLO WORLD", res.String("upper", 0).Value)
        Assert.Equal("Hel", res.String("slice", 0).Value)
        Assert.Equal("Hell0 W0rld", res.String("replaced", 0).Value)
        Assert.Equal(11L, int64 (res.Int("len", 0).Value)) // u32 -> i64

        // 验证 Row 1: "foo BAR"
        Assert.Equal("FOO BAR", res.String("upper", 1).Value)
        Assert.Equal("foo", res.String("slice", 1).Value)
    [<Fact>]
    member _.``Math Ops (BMI Calculation with Pow)`` () =
        // 构造数据: 身高(m), 体重(kg)
        use csv = new TempCsv "name,height,weight\nAlice,1.65,60\nBob,1.80,80"
        let df = Polars.readCsv csv.Path None

        // 目标逻辑: weight / (height ^ 2)
        let bmiExpr = 
            Polars.col "weight" / Polars.col "height" .** Polars.lit 2
            |> Polars.alias "bmi"

        let res = 
            df 
            |> Polars.select [
                Polars.col "name"
                bmiExpr
                // 顺便测一下 sqrt: sqrt(height)
                (Polars.col "height").Sqrt().Alias "sqrt_h"
            ]

        // 验证 Bob 的 BMI: 80 / 1.8^2 = 24.691358...
        let bobBmi = res.Float("bmi", 1).Value
        Assert.True(bobBmi > 24.69 && bobBmi < 24.70)

        // 验证 Alice 的 Sqrt: sqrt(1.65) = 1.2845...
        let aliceSqrt = res.Float("sqrt_h", 0).Value
        Assert.True(aliceSqrt > 1.28 && aliceSqrt < 1.29)

    [<Fact>]
    member _.``Temporal Ops (Components, Format, Cast)`` () =
        // 构造数据: 包含日期和时间的字符串
        // Row 0: 2023年圣诞节下午3点半 (周一)
        // Row 1: 2024年元旦零点 (周一)
        let csvContent = "ts\n2023-12-25 15:30:00\n2024-01-01 00:00:00"
        use csv = new TempCsv(csvContent)
        
        // [关键] 开启 tryParseDates=true，让 Polars 自动解析为 Datetime 类型
        let df = Polars.readCsv csv.Path (Some true)

        let res =
            df
            |> Polars.select [
                Polars.col "ts"

                // 1. 提取组件 (Components)
                (Polars.col "ts").Dt.Year().Alias "y"
                (Polars.col "ts").Dt.Month().Alias "m"
                (Polars.col "ts").Dt.Day().Alias "d"
                (Polars.col "ts").Dt.Hour().Alias "h"
                
                // Polars 定义: Monday=1, Sunday=7
                (Polars.col "ts").Dt.Weekday().Alias "w_day"
                
                // 2. 格式化 (Format to String)
                // 测试自定义格式: "2023/12/25"
                (Polars.col "ts").Dt.ToString("%Y/%m/%d").Alias "fmt_custom"
                
                // 3. 类型转换 (Cast to Date)
                // Datetime (含时分秒) -> Date (只含日期)
                (Polars.col "ts").Dt.Date().Alias "date_only"
            ]
        // --- 验证 Row 0: 2023-12-25 15:30:00 ---
        
        // 年月日
        Assert.Equal(2023L, res.Int("y", 0).Value)
        Assert.Equal(12L, res.Int("m", 0).Value)
        Assert.Equal(25L, res.Int("d", 0).Value)
        
        // 小时
        Assert.Equal(15L, res.Int("h", 0).Value)
        
        // 星期 (2023-12-25 是周一)
        Assert.Equal(1L, res.Int("w_day", 0).Value)

        // 格式化字符串验证
        Assert.Equal("2023/12/25", res.String("fmt_custom", 0).Value)

        // Date 类型验证
        // 我们的 formatValue 辅助函数会将 Date32 渲染为 "yyyy-MM-dd"
        // 如果转换成功，时分秒应该消失
        Assert.Equal("2023-12-25", res.String("date_only", 0).Value)

        // --- 验证 Row 1: 2024-01-01 00:00:00 ---
        Assert.Equal(2024L, res.Int("y", 1).Value)
        Assert.Equal(1L, res.Int("m", 1).Value)
        Assert.Equal(0L, res.Int("h", 1).Value) // 零点

    [<Fact>]
    member _.``Cast Ops: Int to Float, String to Int`` () =
        use csv = new TempCsv "val_str,val_int\n100,10\n200,20"
        let df = Polars.readCsv csv.Path None

        let res = 
            df 
            |> Polars.select [
                // 1. String -> Int64
                (Polars.col "val_str").Cast(DataType.Int64).Alias "str_to_int"
                
                // 2. Int64 -> Float64
                (Polars.col "val_int").Cast(DataType.Float64).Alias "int_to_float"
            ]

        // 验证
        let v1 = res.Int("str_to_int", 0).Value
        Assert.Equal(100L, v1)

        let v2 = res.Float("int_to_float", 1).Value
        Assert.Equal(20.0, v2)
    [<Fact>]
    member _.``Control Flow: IfElse (When/Then/Otherwise)`` () =
        // 构造成绩数据
        use csv = new TempCsv "student,score\nAlice,95\nBob,70\nCharlie,50"
        let df = Polars.readCsv csv.Path None

        // 逻辑:
        // if score >= 90 then "A"
        // else if score >= 60 then "Pass"
        // else "Fail"
        
        let gradeExpr = 
            Polars.ifElse 
                (Polars.col "score" .>= Polars.lit 90) 
                (Polars.lit "A") 
                (
                    // 嵌套 IfElse
                    Polars.ifElse 
                        (Polars.col "score" .>= Polars.lit 60)
                        (Polars.lit "Pass")
                        (Polars.lit "Fail")
                )
            |> Polars.alias "grade"

        let res = 
            df 
            |> Polars.withColumn gradeExpr
            |> Polars.sort (Polars.col "score") true // 降序

        // 验证
        // Alice (95) -> A
        Assert.Equal("A", res.String("grade", 0).Value)
        // Bob (70) -> Pass
        Assert.Equal("Pass", res.String("grade", 1).Value)
        // Charlie (50) -> Fail
        Assert.Equal("Fail", res.String("grade", 2).Value)

    [<Fact>]
    member _.``String Regex: Replace and Extract`` () =
        use csv = new TempCsv "text\nUser: 12345\nID: 999"
        let df = Polars.readCsv csv.Path None

        let res = 
            df 
            |> Polars.select [
                // 1. Regex Replace: 把数字换成 #
                // \d+ 是正则
                (Polars.col "text").Str.ReplaceAll("\d+", "#", useRegex=true).Alias "masked"
                
                // 2. Regex Extract: 提取数字部分
                // (\d+) 是第 1 组
                (Polars.col "text").Str.Extract("(\d+)", 1).Alias "extracted_id"
            ]

        // 验证 Replace
        // "User: 12345" -> "User: #"
        Assert.Equal("User: #", res.String("masked", 0).Value)
        
        // 验证 Extract
        // "User: 12345" -> "12345"
        Assert.Equal("12345", res.String("extracted_id", 0).Value)
        Assert.Equal("999", res.String("extracted_id", 1).Value)