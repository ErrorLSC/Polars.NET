namespace Polars.FSharp.Tests

open Xunit
open Polars.FSharp
open Apache.Arrow

type ``Series Tests`` () =

    [<Fact>]
    member _.``Series: Create Int32 with Nulls`` () =
        let data = [Some 1; None; Some 3; Some 42]
        use s = Series.create("nums", data)
        
        Assert.Equal("nums", s.Name)
        Assert.Equal(4L, s.Length)
        
        // 转 Arrow 验证
        let arrow = s.ToArrow() :?> Int32Array
        Assert.Equal(4, arrow.Length)
        Assert.Equal(1, arrow.GetValue(0).Value)
        Assert.True(arrow.IsNull 1) // Null Check
        Assert.Equal(3, arrow.GetValue(2).Value)

    [<Fact>]
    member _.``Series: Create Strings with Nulls`` () =
        let data = [Some "hello"; None; Some "world"]
        use s = Series.create("strings", data)
        
        let arrow = s.ToArrow() 
        // Polars 0.50+ 默认 StringViewArray，或者 LargeStringArray
        // 这里做个类型匹配
        match arrow with
        | :? StringViewArray as sa ->
            Assert.Equal("hello", sa.GetString 0)
            Assert.True(sa.IsNull 1)
            Assert.Equal("world", sa.GetString 2)
        | :? StringArray as sa -> // Fallback logic
            Assert.Equal("hello", sa.GetString 0)
            Assert.True(sa.IsNull 1)
        | _ -> failwithf "Unexpected arrow type: %s" (arrow.GetType().Name)

    [<Fact>]
    member _.``Series: Rename`` () =
        use s = Series.create("a", [1;2])
        Assert.Equal("a", s.Name)
        
        s.Rename("b") |> ignore
        Assert.Equal("b", s.Name)

    [<Fact>]
    member _.``Series: Float with Nulls`` () =
        let data = [Some 1.5; None; Some 3.14]
        use s = Series.create("floats", data)
        
        let arrow = s.ToArrow() :?> DoubleArray
        Assert.Equal(1.5, arrow.GetValue(0).Value)
        Assert.True(arrow.IsNull(1))
        Assert.Equal(3.14, arrow.GetValue(2).Value)
    [<Fact>]
    member _.``Interop: DataFrame <-> Series`` () =
        // 1. 创建 DataFrame
        use csv = new TempCsv "name,age\nalice,10\nbob,20"
        let df = DataFrame.ReadCsv csv.Path
                
        // 2. 获取 Series (ByName)
        use sName = df.Column "name"
        Assert.Equal("name", sName.Name)
        Assert.Equal(2L, sName.Length)
        sName |> pl.showSeries |> ignore
        // 3. 获取 Series (ByIndex)
        use sAge = df.Column 1
        Assert.Equal("age", sAge.Name)

        // 4. 索引器语法
        use sAge2 = df.[1]
        Assert.Equal("age", sAge2.Name)

        // 5. Series -> DataFrame
        let dfNew = sAge.ToFrame()
        Assert.Equal(1L, dfNew.Columns)
        Assert.Equal(2L, dfNew.Rows)
        Assert.Equal("age", dfNew.ColumnNames.[0])
    [<Fact>]
    member _.``Series: Cast to Categorical`` () =
        // 1. 创建字符串 Series (高重复)
        let data = ["apple"; "banana"; "apple"; "apple"; "banana"]
        use s = Series.create("fruits", data)
        
        // 2. 转换为 Categorical
        use sCat = s.Cast DataType.Categorical
        
        // 3. 验证 Arrow 类型
        let arrow = sCat.ToArrow()
        
        // [修复] 使用 DictionaryArray 基类
        Assert.IsAssignableFrom<Apache.Arrow.DictionaryArray> arrow |> ignore
        
        // 进一步验证内部结构
        let dictArr = arrow :?> Apache.Arrow.DictionaryArray
        
        // 验证索引类型 (Polars 通常使用 UInt32 作为物理索引)
        // 注意：Indices 也是一个 IArrowArray
        let indices = dictArr.Indices
        Assert.IsAssignableFrom<Apache.Arrow.UInt32Array> indices |> ignore
        
        // 验证字典值 (应该是去重后的字符串)
        let values = dictArr.Dictionary
        // 可能是 StringArray 或 StringViewArray (取决于 Polars 兼容性设置)
        Assert.True(values :? Apache.Arrow.StringArray || values :? Apache.Arrow.StringViewArray)
        
        // 验证值内容 (apple, banana)
        Assert.Equal(2, values.Length)

    [<Fact>]
    member _.``Series: Cast to Decimal (From String)`` () =
        // 1. 使用字符串源数据，保证精度
        let data = ["1.23"; "4.56"; "7.89"]
        use s = Series.create("money", data)
        
        // 2. String -> Decimal (Precision=10, Scale=2)
        // Polars 解析字符串 "4.56" -> 456 (int128) -> 正确
        use sDec = s.Cast(Decimal(Some 10, 2))
        
        // 3. 验证
        let arrow = sDec.ToArrow()
        let decArr = arrow :?> Decimal128Array
        
        Assert.Equal(1.23m, decArr.GetValue(0).Value)
        Assert.Equal(4.56m, decArr.GetValue(1).Value) // 完美通过
        Assert.Equal(7.89m, decArr.GetValue(2).Value)
    [<Fact>]
    member _.``Series: Create Decimal (High Performance)`` () =
        // 数据: 1.23, 4.56
        // 我们指定 Scale = 2
        let data = [1.23m; 4.56m; 7.89m] 
        
        // 使用新加的 create 方法
        use s = Series.create("money", data, 2)
        
        // 验证
        let arrow = s.ToArrow() :?> Apache.Arrow.Decimal128Array
        
        // 这次 4.56m 进去，出来的必定是 4.56m
        // 因为我们在 C# 端做了 * 100 操作：4.56m * 100m = 456m -> (Int128)456
        // 绝对没有浮点数中间商赚差价
        Assert.Equal(4.56m, arrow.GetValue(1).Value)
    [<Fact>]
    member _.``Scalar Access: Series & DataFrame`` () =
        // Series 验证
        use s = Series.create("d", [1.23m; 4.56m], 2)
        Assert.Equal(Some 1.23m, s.Decimal 0)
        Assert.Equal(Some 4.56m, s.Decimal 1)
        
        // DataFrame 验证 (Redirect)
        use df = DataFrame.create [s]
        Assert.Equal(Some 1.23m, df.Decimal("d", 0))
    [<Fact>]
    member _.``Series: IsNull / IsNotNull`` () =
        // 数据: 1, null, 3
        let s = Series.create("a", [Some 1; None; Some 3])

        // 1. IsNull -> [false, true, false]
        let maskNull = s.IsNull()
        Assert.Equal("bool", maskNull.DtypeStr)
        Assert.Equal(Some false, maskNull.Bool(0))
        Assert.Equal(Some true, maskNull.Bool(1))

        // 2. IsNotNull -> [true, false, true]
        let maskNotNull = s.IsNotNull()
        Assert.Equal(Some true, maskNotNull.Bool(0))
        Assert.Equal(Some false, maskNotNull.Bool(1))