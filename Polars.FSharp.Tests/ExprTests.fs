namespace Polars.FSharp.Tests

open System
open Xunit
open Polars.FSharp

type ``Expression Logic Tests`` () =
    [<Fact>]
        member _.``Select inline style (Pythonic)`` () =
            use csv = new TempCsv "name,birthdate,weight,height\nQinglei,2025-11-25,70,1.80"
            let df = DataFrame.ReadCsv csv.Path

            // 像 Python 一样写在 list 里面！
            let res = 
                df
                |> pl.select [
                    col "name"
                    
                    // Inline 1: 简单的 alias
                    col "birthdate" |> alias "b_date"
                    
                    // Inline 2: 链式调用
                    (col "birthdate").Dt.Year().Alias "year"
                    
                    // Inline 3: 算术表达式
                    col "weight" / (col "height" * col "height")
                    |> alias "bmi"
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
        let df = DataFrame.ReadCsv csv.Path
        
        let res = df |> pl.filter (col "val" .> lit 15)
        
        Assert.Equal(2L, res.Rows)
    [<Fact>]
    member _.``Filter by numeric value (< operator)`` () =
        use csv = new TempCsv "name,birthdate,weight,height\nBen Brown,1985-02-15,72.5,1.77\nQinglei,2025-11-25,70.0,1.80\nZhang,2025-10-31,55,1.75"
        let df = DataFrame.ReadCsv (path=csv.Path,tryParseDates=true)

        let res = df |> pl.filter ((col "birthdate").Dt.Year() .< lit 1990 )

        Assert.Equal(1L,res.Rows)

    [<Fact>]
    member _.``Filter by string value (== operator)`` () =
        use csv = new TempCsv "name\nAlice\nBob\nAlice"
        let df = DataFrame.ReadCsv csv.Path
        
        // SRTP 魔法测试
        let res = df |> pl.filter (pl.col "name" .== pl.lit "Alice")
        
        Assert.Equal(2L, res.Rows)

    [<Fact>]
    member _.``Filter by double value (== operator)`` () =
        use csv = new TempCsv "value\n3.36\n4.2\n5\n3.36"
        let df = DataFrame.ReadCsv csv.Path
        
        // SRTP 魔法测试
        let res = df |> pl.filter (col "value" .== lit 3.36)
        
        Assert.Equal(2L, res.Rows)

    [<Fact>]
    member _.``Null handling works`` () =
        // 造一个带 null 的 CSV
        // age: 10, null, 30
        use csv = new TempCsv "age\n10\n\n30" 
        let lf = LazyFrame.ScanCsv csv.Path

        // 测试 1: fill_null
        // 把 null 填成 0，然后筛选 age > 0
        // 结果应该是 3 行 (10, 0, 30)
        let res = 
            lf 
            |> pl.withColumnLazy (
                col "age" 
                |> pl.fillNull (pl.lit 0) 
                |> pl.alias "age_filled"
            )
            |> pl.filterLazy (col "age_filled" .>= lit 0)
            |> pl.collect
        Assert.Equal(3L, res.Rows)
        
        // 测试 2: is_null
        // 筛选出 null 的行
        let df= DataFrame.ReadCsv csv.Path 
        let nulls = df |> pl.filter (pl.col "age" |> pl.isNull)
        Assert.Equal(1L, nulls.Rows)
    [<Fact>]
    member _.``IsBetween with DateTime Literals`` () =
        // 构造数据: Qinglei 的生日
        use csv = new TempCsv "name,birthdate,height\nQinglei,1990-05-20,1.80\nTooOld,1980-01-01,1.80\nTooShort,1990-05-20,1.60"
        
        // 必须开启日期解析
        let df = DataFrame.ReadCsv (path=csv.Path,tryParseDates=true)

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
            |> pl.filter (
                // 条件 1: 生日区间
                (pl.col "birthdate").IsBetween(pl.lit startDt, pl.lit endDt)
                .&& // 条件 2: AND
                // 条件 3: 身高
                (pl.col "height" .> pl.lit 1.7)
            )

        // 验证: 只有 Qinglei 符合
        Assert.Equal(1L, res.Rows)
        Assert.Equal("Qinglei", res.String("name", 0).Value)
    [<Fact>]
    member _.``Expr: DateTime Ops (Truncate, Offset, Timestamp)`` () =
        // 数据: ["2023-01-01 10:15:00", "2023-01-01 10:45:00"]
        let s = Series.create("ts", ["2023-01-01 10:15:00"; "2023-01-01 10:45:00"])
        // 先解析成 Datetime
        use df_origin = DataFrame.create [s]
        let df =
            df_origin 
            |> pl.select([
            pl.col("ts").Str.ToDatetime("%Y-%m-%d %H:%M:%S").Alias "ts"
            ])

        let res = 
            df
            |> pl.select([
                pl.col "ts"

                // 1. Truncate to 1 hour (10:15 -> 10:00)
                pl.col("ts").Dt.Truncate("1h").Alias "truncated"

                // 2. Round to 1 hour (10:45 -> 11:00)
                pl.col("ts").Dt.Round("1h").Alias "rounded"

                // 3. Offset by 30m (10:15 -> 10:45)
                pl.col("ts").Dt.OffsetBy("30m").Alias "offset"

                // 4. Timestamp (Micros)
                pl.col("ts").Dt.TimestampMicros().Alias "micros"
            ])
            |> pl.show
        // 验证 Row 0: 10:15
        // let row0 = res.Row(0) // 假设你以后会实现 Row 访问，或者用 .Date(..).Value
        // 这里用原来的列式访问
        
        // Truncate: 10:15 -> 10:00
        let t0 = res.Datetime("truncated", 0).Value
        Assert.Equal(10, t0.Hour)
        Assert.Equal(0, t0.Minute)

        // Round: 10:45 (Row 1) -> 11:00
        let r1 = res.Datetime("rounded", 1).Value
        Assert.Equal(11, r1.Hour)
        Assert.Equal(0, r1.Minute)

        // Offset: 10:15 -> 10:45
        let o0 = res.Datetime("offset", 0).Value
        Assert.Equal(10, o0.Hour)
        Assert.Equal(45, o0.Minute)
        
        // Timestamp should be > 0
        Assert.True(res.Int("micros", 0).Value > 0L)

type ``String Logic Tests`` () =

    [<Fact>]
    member _.``Expr: String Cleaning & Parsing (Strip, Anchor, Date)`` () =
        // 1. 准备测试数据，覆盖多种场景
        // Row 0: 空格脏数据 -> 测试 Strip
        // Row 1: URL -> 测试 Prefix/StartsWith
        // Row 2: 文件名 -> 测试 Suffix/EndsWith
        // Row 3: 自定义字符脏数据 -> 测试 Strip(matches)
        // Row 4: 标准日期 -> 测试 ToDate
        // Row 5: 脏日期 -> 测试 链式调用 Strip().ToDate()
        let s = Series.create("raw", [
            "  abc  "           // 0
            "https://pl.rs" // 1
            "data.csv"          // 2
            "__key__"           // 3
            "20250101"          // 4
            "  2025-12-31  "    // 5
        ])
        
        use df = DataFrame.create [s]

        let res = 
            df
            |> pl.select([
                pl.col "raw"

                // 1. Strip 测试 (默认去空格)
                // "  abc  " -> "abc"
                pl.col("raw").Str.Strip().Alias "strip_default"
                
                // 2. LStrip / RStrip 测试
                // "  abc  " -> "abc  " / "  abc"
                pl.col("raw").Str.LStrip().Alias "lstrip"
                pl.col("raw").Str.RStrip().Alias "rstrip"

                // 3. Strip Matches 测试 (去自定义字符)
                // "__key__" -> "key"
                pl.col("raw").Str.Strip(matches="_").Alias "strip_custom"

                // 4. Prefix / Suffix 测试
                // "https://pl.rs" -> "pl.rs"
                // "data.csv" -> "data"
                pl.col("raw").Str.StripPrefix("https://").Alias "strip_prefix"
                pl.col("raw").Str.StripSuffix(".csv").Alias "strip_suffix"

                // 5. Anchors 测试 (StartsWith / EndsWith) -> Boolean
                pl.col("raw").Str.StartsWith("https").Alias "is_url"
                pl.col("raw").Str.EndsWith(".csv").Alias "is_csv"

                // 6. ToDate 测试 (解析)
                // "20250101" -> Date
                pl.col("raw").Str.ToDate("%Y%m%d").Alias "parsed_date"

                // 7. 链式调用测试 (清洗 + 解析)
                // "  2025-12-31  " -> "2025-12-31" -> Date
                pl.col("raw").Str.Strip().Str.ToDate("%Y-%m-%d").Alias "chain_date"
            ])

        // --- 验证结果 ---

        // 1. Strip
        Assert.Equal("abc", res.String("strip_default", 0).Value)
        Assert.Equal("abc  ", res.String("lstrip", 0).Value)
        Assert.Equal("  abc", res.String("rstrip", 0).Value)

        // 2. Custom Strip
        Assert.Equal("key", res.String("strip_custom", 3).Value) // __key__ -> key

        // 3. Prefix / Suffix
        Assert.Equal("pl.rs", res.String("strip_prefix", 1).Value)
        Assert.Equal("data", res.String("strip_suffix", 2).Value) // data.csv -> data

        // 4. Anchors (Boolean)
        Assert.Equal(Some true, res.Bool("is_url", 1)) // https://...
        Assert.Equal(Some false, res.Bool("is_url", 0))
        Assert.Equal(Some true, res.Bool("is_csv", 2)) // ...csv

        // 5. ToDate (解析成功)
        // Row 4: "20250101"
        let d1 = res.Date("parsed_date", 4).Value
        Assert.Equal(2025, d1.Year)
        Assert.Equal(1, d1.Month)
        Assert.Equal(1, d1.Day)

        // 6. ToDate (解析失败 - Strict=false 默认返回 Null)
        // Row 0: "  abc  " 无法解析为日期
        Assert.True(res.IsNullAt("parsed_date", 0))

        // 7. 链式调用 (Strip + ToDate)
        // Row 5: "  2025-12-31  "
        let d2 = res.Date("chain_date", 5).Value
        Assert.Equal(2025, d2.Year)
        Assert.Equal(12, d2.Month)
        Assert.Equal(31, d2.Day)

        // 打印 Schema 确认类型正确
        // parsed_date 应该是 Date 类型
        Assert.Equal(DataType.Date, res.Schema.["parsed_date"])
    [<Fact>]
    member _.``Math Ops (BMI Calculation with Pow)`` () =
        // 构造数据: 身高(m), 体重(kg)
        use csv = new TempCsv "name,height,weight\nAlice,1.65,60\nBob,1.80,80"
        let df = DataFrame.ReadCsv csv.Path

        // 目标逻辑: weight / (height ^ 2)
        let bmiExpr = 
            pl.col "weight" / pl.col "height" .** pl.lit 2
            |> pl.alias "bmi"

        let res = 
            df 
            |> pl.select [
                pl.col "name"
                bmiExpr
                // 顺便测一下 sqrt: sqrt(height)
                (pl.col "height").Sqrt().Alias "sqrt_h"
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
        let df = DataFrame.ReadCsv (path=csv.Path,tryParseDates=true)

        let res =
            df
            |> pl.select [
                pl.col "ts"

                // 1. 提取组件 (Components)
                (pl.col "ts").Dt.Year().Alias "y"
                (pl.col "ts").Dt.Month().Alias "m"
                (pl.col "ts").Dt.Day().Alias "d"
                (pl.col "ts").Dt.Hour().Alias "h"
                
                // Polars 定义: Monday=1, Sunday=7
                (pl.col "ts").Dt.Weekday().Alias "w_day"
                
                // 2. 格式化 (Format to String)
                // 测试自定义格式: "2023/12/25"
                (pl.col "ts").Dt.ToString("%Y/%m/%d").Alias "fmt_custom"
                
                // 3. 类型转换 (Cast to Date)
                // Datetime (含时分秒) -> Date (只含日期)
                (pl.col "ts").Dt.Date().Alias "date_only"
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
        // [修改] 使用更大的数字，避免 Polars 推断为 UInt8
        // 同时给 val_str 加引号，确保它像个 String
        use csv = new TempCsv "val_str,val_int\n\"100\",1000\n\"200\",2000"
        
        // [修改] 显式指定 Schema，确保 val_str 是 String，val_int 是 Int64
        // 这样测试的就是纯粹的 Cast 逻辑，而不是 CSV 推断逻辑
        // (由于 ReadCsv 还没有 Schema 参数，我们依赖数据本身让推断正确)
        // 1000 肯定超过了 UInt8 (max 255)，会被推断为 Int64
        
        let df = DataFrame.ReadCsv csv.Path

        let res = 
            df 
            |> pl.select [
                // 1. String -> Int64
                (pl.col "val_str").Cast(DataType.Int64).Alias "str_to_int"
                
                // 2. Int64 -> Float64
                (pl.col "val_int").Cast(DataType.Float64).Alias "int_to_float"
            ]

        // 验证
        // "100" -> 100
        let v1 = res.Int("str_to_int", 0).Value
        Assert.Equal(100L, v1)

        // 1000 -> 1000.0
        let v2 = res.Float("int_to_float", 0).Value
        Assert.Equal(1000.0, v2)
    [<Fact>]
    member _.``Control Flow: IfElse (When/Then/Otherwise)`` () =
        // 构造成绩数据
        use csv = new TempCsv "student,score\nAlice,95\nBob,70\nCharlie,50"
        let df = DataFrame.ReadCsv csv.Path

        // 逻辑:
        // if score >= 90 then "A"
        // else if score >= 60 then "Pass"
        // else "Fail"
        
        let gradeExpr = 
            pl.ifElse 
                (pl.col "score" .>= pl.lit 90) 
                (pl.lit "A") 
                (
                    // 嵌套 IfElse
                    pl.ifElse 
                        (pl.col "score" .>= pl.lit 60)
                        (pl.lit "Pass")
                        (pl.lit "Fail")
                )
            |> pl.alias "grade"

        let res = 
            df 
            |> pl.withColumn gradeExpr
            |> pl.sort (pl.col "score") true // 降序

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
        let df = DataFrame.ReadCsv csv.Path

        let res = 
            df 
            |> pl.select [
                // 1. Regex Replace: 把数字换成 #
                // \d+ 是正则
                (pl.col "text").Str.ReplaceAll("\d+", "#", useRegex=true).Alias "masked"
                
                // 2. Regex Extract: 提取数字部分
                // (\d+) 是第 1 组
                (pl.col "text").Str.Extract("(\d+)", 1).Alias "extracted_id"
            ]

        // 验证 Replace
        // "User: 12345" -> "User: #"
        Assert.Equal("User: #", res.String("masked", 0).Value)
        
        // 验证 Extract
        // "User: 12345" -> "12345"
        Assert.Equal("12345", res.String("extracted_id", 0).Value)
        Assert.Equal("999", res.String("extracted_id", 1).Value)