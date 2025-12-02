namespace PolarsFSharp.Tests

open Xunit
open PolarsFSharp

type ``Complex Query Tests`` () =
    
    [<Fact>]
    member _.``Join execution (Eager)`` () =
        use users = new TempCsv("id,name\n1,A\n2,B")
        use sales = new TempCsv("uid,amt\n1,100\n1,200\n3,50")

        let uDf = Polars.readCsv users.Path None
        let sDf = Polars.readCsv sales.Path None

        let res = 
            uDf 
            |> Polars.join sDf [Polars.col "id"] [Polars.col "uid"] "left"
        
        // Left join: id 1 (2 rows), id 2 (1 row null match) -> Total 3
        Assert.Equal(3L, res.Rows)

    [<Fact>]
    member _.``Lazy API Chain (Filter -> Collect)`` () =
        use csv = new TempCsv("a,b\n1,10\n2,20\n3,30")
        let lf = Polars.scanCsv csv.Path None
        
        let df = 
            lf
            |> Polars.filterLazy (Polars.col "a" .> Polars.lit 1)
            |> Polars.limit 1u
            |> Polars.collect

        Assert.Equal(1L, df.Rows)

    [<Fact>]
    member _.``GroupBy Queries`` () =
        use csv = new TempCsv("name,birthdate,weight,height\nBen Brown,1985-02-15,72.5,1.77\nQinglei,2025-11-25,70.0,1.80\nZhang,2025-10-31,55,1.75")
        let lf = Polars.scanCsv csv.Path (Some true)

        let res = 
            lf 
            |> Polars.groupByLazy
                [(Polars.col "birthdate").Dt.Year() / Polars.lit 10 * Polars.lit 10 |> Polars.alias "decade" ]
                [ Polars.count().Alias("cnt")] 
            |> Polars.sortLazy (Polars.col "decade") false
            |> Polars.collect

        // 验证
        // Row 0: 1980 -> 2
        Assert.Equal(1980L, res.Int("decade", 0).Value)
        // 注意：count() 返回通常是 UInt32 或 UInt64，我们用 Int64 读取是安全的
        Assert.Equal(1L, int64 (res.Int("cnt", 0).Value)) 

        // Row 1: 1990 -> 1
        Assert.Equal(2020L, res.Int("decade", 1).Value)
        Assert.Equal(2L, int64 (res.Int("cnt", 1).Value))

    [<Fact>]
    member _.``Complex Transformation (Selector Exclude)`` () =
        let csvContent = 
            "name,birthdate,weight,height\n" +
            "Zhang San,1985-01-01,70.1234,1.755\n" +
            "Li Si,1988-05-20,60.5678,1.604\n" +
            "Wang Wu,1996-12-31,80.9999,1.859"
        use csv = new TempCsv(csvContent)
        let lf = Polars.scanCsv csv.Path None

        let res = 
            lf
            |> Polars.withColumns (
                // 1. String Split -> List -> First
                [
                (Polars.col "name").Str.Split(" ").List.First()
                (Polars.col "birthdate").Dt.Year() / Polars.lit 10 * Polars.lit 10 |> Polars.alias "decade"
            ])
            |> Polars.selectLazy [
                // 2. Exclude (all except "ignore_me")
                Polars.all() |> Polars.exclude ["birthdate"] |> Polars.asExpr
            ]
            |> Polars.groupByLazy 
                // Keys
                [ Polars.col "decade" ] 
                // Aggs
                [
                    // Agg A: 名字列表 (Polars 默认行为：非 Key 列在 agg 中会聚合成 List)
                    // 但 Rust 原例写的是 col("name")，如果 context 是 agg，它就是 list
                    // 这里我们显式一点，或者直接传 col "name" 让 Polars 处理
                    Polars.col "name"
                    // Agg B: Weight & Height 的均值 + 四舍五入 + 重命名
                    // Rust 原例: cols(["weight", "height"]).mean().round(2).prefix("avg_")
                    // F# 复刻: 展开写两个 Expr (效果等价)
                    (Polars.col "weight").Mean().Round(2).Name.Prefix("avg_")
                    (Polars.col "height").Mean().Round(2).Name.Prefix("avg_")
                ]
            |> Polars.collect
            |> Polars.sort (Polars.col "decade") false

    // 验证列名 (birthdate 应该没了，新增了 decade 和 avg_ 前缀)
        let cols = res.ColumnNames
        Assert.DoesNotContain("birthdate", cols)
        Assert.Contains("decade", cols)
        Assert.Contains("avg_weight", cols)
        Assert.Contains("avg_height", cols)

        // 验证 Row 0 (1980年代: Zhang, Li)
        Assert.Equal(1980L, res.Int("decade", 0).Value)
        
        // 验证数学运算 (Mean + Round)
        // Weight: (70.1234 + 60.5678) / 2 = 65.3456 -> Round(2) -> 65.35
        let w80 = res.Float("avg_weight", 0).Value
        Assert.Equal(65.35, w80)

        // 验证字符串处理
        // 在 GroupBy 结果中，"name" 列变成了 List<String>
        // 但目前我们的 C# Wrapper 转 Arrow 时，List 列会变成 String (JSON representation) 或者 ListArray
        // 这里我们可以简单验证一下 ToArrow 的行为，或者只验证 Schema
        // (由于我们还没做 List 类型的读取 API，这里暂时跳过内容验证，只要不崩就行)
        
        // 验证 Row 1 (1990年代: Wang)
        Assert.Equal(1990L, res.Int("decade", 1).Value)
        // Weight: 80.9999 -> 81.00
        let w90 = res.Float("avg_weight", 1).Value
        Assert.Equal(81.00, w90)

    [<Fact>]
    member _.``List Ops: Cols, Explode, Join and Read`` () =
        // 数据: 一个人有多个 Tag (空格分隔)
        use csv = new TempCsv("name,tags\nAlice,coding reading\nBob,gaming")
        let lf = Polars.scanCsv csv.Path None

        let res = 
            lf
            |> Polars.withColumn (
                // 1. Split 变成 List
                (Polars.col "tags").Str.Split(" ").Alias("tag_list")
            )
            |> Polars.withColumn (
                // 2. 演示 cols([...]): 同时选中 name 和 tag_list，加上前缀
                // 虽然这里只是演示，通常用于批量数学运算
                Polars.cols ["name"; "tag_list"]
                |> fun e -> e.Name.Prefix("my_")
            )
            |> Polars.withColumn (
                // 3. List Join (还原回去)
                (Polars.col "my_tag_list").List.Join("-").Alias("joined_tags")
            )
            |> Polars.collect

        // 验证 1: cols 产生的前缀
        let cols = res.ColumnNames
        Assert.Contains("my_name", cols)
        Assert.Contains("my_tag_list", cols)

        // 验证 2: List Join
        // coding reading -> coding-reading
        Assert.Equal("coding-reading", res.String("joined_tags", 0).Value)

        // 验证 3: 读取 List (使用新加的 API)
        let aliceTags = res.StringList("my_tag_list", 0)
        Assert.True aliceTags.IsSome
        Assert.Equal<string list>(["coding"; "reading"], aliceTags.Value)

        // 验证 4: Explode (炸裂)
        // Alice 有 2 个 tag，Bob 有 1 个 -> Explode 后应该是 3 行
        let exploded = 
            res 
            |> Polars.select [ Polars.col "my_name"; Polars.col "my_tag_list" ]
            // [修改] 加上列表括号 []
            |> Polars.explode [ Polars.col "my_tag_list" ] 
        
        Assert.Equal(3L, exploded.Rows)
        Assert.Equal("coding", exploded.String("my_tag_list", 0).Value)
        Assert.Equal("reading", exploded.String("my_tag_list", 1).Value)
        Assert.Equal("gaming", exploded.String("my_tag_list", 2).Value)

    [<Fact>]
    member _.``Struct and Advanced List Ops`` () =
        // 构造数据: Alice 考了两次试
        use csv = new TempCsv("name,score1,score2\nAlice,80,90\nBob,60,70")
        let lf = Polars.scanCsv csv.Path None
        let maxCharExpr = 
            (Polars.col "raw_nums").Str.Split(" ")
                .List.Sort(true) // Descending
                .List.First()
                .Alias("max_char")
        let res = 
            lf
            // 1. Struct 测试: 把 score1, score2 打包成 "scores_struct"
            |> Polars.withColumn (
                Polars.asStruct [Polars.col "score1"; Polars.col "score2"]
                |> Polars.alias "scores_struct"
            )
            // 2. List 测试: 
            // 假设我们把 struct 里的字段取出来，做一个计算 (演示 Struct.Field)
            |> Polars.withColumn (
                (Polars.col "scores_struct").Struct.Field("score1").Alias("s1_extracted")
            )
            // 3. List Agg 测试 (既然没有 concat_list，我们造一个伪需求：如果 split 后的 list)
            // 我们手动 split 一个字符串 "1 5 2"
            |> Polars.withColumn (
                Polars.lit "1 5 2"
                |> Polars.alias "raw_nums"
            )
            // 4. 处理 List: Split -> Sort(Desc) -> First
            // "1 5 2" -> ["1", "5", "2"] -> ["5", "2", "1"] -> "5"
            // 注意：Split 出来是 String，Sort 默认按字典序，"5" > "2" > "1"
            |> Polars.withColumn maxCharExpr
            |> Polars.collect

        // 验证 Struct Field
        // Alice score1 = 80
        Assert.Equal(80L, res.Int("s1_extracted", 0).Value)

        // 验证 List Sort + First
        Assert.Equal("5", res.String("max_char", 0).Value)

    [<Fact>]
    member _.``Window Function (Over)`` () =
        use csv = new TempCsv("name,dept,salary\nAlice,IT,1000\nBob,IT,2000\nCharlie,HR,3000")
        let lf = Polars.scanCsv csv.Path None

        let res = 
            lf
            |> Polars.withColumn (
                // 逻辑: col("salary") - col("salary").mean().over([col("dept")])
                (
                    Polars.col "salary" - 
                    (Polars.col "salary").Mean().Over([Polars.col "dept"])
                )
                |> Polars.alias "diff_from_avg"
            )
            |> Polars.collect
            |> Polars.sort (Polars.col "name") false

        // 验证
        // Alice (IT): 1000 - 1500 = -500
        Assert.Equal("Alice", res.String("name", 0).Value)
        Assert.Equal(-500.0, res.Float("diff_from_avg", 0).Value)

        // Bob (IT): 2000 - 1500 = 500
        Assert.Equal("Bob", res.String("name", 1).Value)
        Assert.Equal(500.0, res.Float("diff_from_avg", 1).Value)

        // Charlie (HR): 3000 - 3000 = 0
        Assert.Equal("Charlie", res.String("name", 2).Value)
        Assert.Equal(0.0, res.Float("diff_from_avg", 2).Value)
