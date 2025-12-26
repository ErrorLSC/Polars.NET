namespace Polars.FSharp.Tests

open System
open Xunit
open Polars.FSharp

type SelectorTests() =

    // 1. 准备测试数据
    // 包含多种类型：String, Int, Float, Bool, Date
    let mkDf () =
        let data = [
            {| Name = "Alice"; Age = 30; Salary = 5000.0; IsActive = true;  JoinDate = DateTime(2020, 1, 1) |}
            {| Name = "Bob";   Age = 25; Salary = 6000.0; IsActive = false; JoinDate = DateTime(2021, 5, 20) |}
        ]
        DataFrame.ofRecords data

    [<Fact>]
    member _.``Selector: Basic Type & Pattern Matching`` () =
        let df = mkDf()

        // --- 场景 A: 选所有数值列 (Age, Salary) ---
        // Python: cs.numeric()
        let numSel = pl.cs.numeric()
        
        // 注意：Select 接收 Expr list，Selector.ToExpr() 转为 Expr
        let dfNum = df.Select [numSel.ToExpr()]
        
        Assert.Equal(2L, dfNum.Width)
        Assert.Contains("Age", dfNum.Columns)
        Assert.Contains("Salary", dfNum.Columns)
        Assert.DoesNotContain("Name", dfNum.Columns)

        // --- 场景 B: 选布尔列 (IsActive) ---
        let boolSel = pl.cs.byType pl.Boolean
        let dfBool = df.Select [boolSel.ToExpr()]
        
        Assert.Equal(1L, dfBool.Width)
        Assert.Equal("IsActive", dfBool.Columns.[0])

        // --- 场景 C: 字符串模式 (Starts With) ---
        let nameSel = pl.cs.startsWith "Na"
        let dfName = df.Select [nameSel.ToExpr()]
        
        Assert.Equal("Name", dfName.Columns.[0])

    [<Fact>]
    member _.``Selector: Set Operations (AND, OR, NOT)`` () =
        let df = mkDf()

        // --- 场景 A: Intersection (&&&) ---
        // 需求：既是数值类型，名字又包含 "Ag" (即 Age，排除 Salary)
        let selAnd = pl.cs.numeric() &&& pl.cs.contains "Ag"
        let dfAnd = df.Select [selAnd.ToExpr()]

        Assert.Single dfAnd.Columns |> ignore // 只有 1 列
        Assert.Equal("Age", dfAnd.Columns.[0])

        // --- 场景 B: Union (|||) ---
        // 需求：数值列 OR 布尔列 (Age, Salary, IsActive)
        let selOr = pl.cs.numeric() ||| pl.cs.byType pl.Boolean
        let dfOr = df.Select([selOr.ToExpr()])
        
        Assert.Equal(3L, dfOr.Width)
        Assert.Contains("Age", dfOr.Columns)
        Assert.Contains("Salary", dfOr.Columns)
        Assert.Contains("IsActive", dfOr.Columns)

        // --- 场景 C: Inversion (~~~) ---
        // 需求：非数值列 (Name, IsActive, JoinDate)
        let selNot = ~~~(pl.cs.numeric())
        let dfNot = df.Select([selNot.ToExpr()])
        
        Assert.Equal(3L, dfNot.Width)
        Assert.Contains("Name", dfNot.Columns)
        Assert.Contains("IsActive", dfNot.Columns)
        Assert.Contains("JoinDate", dfNot.Columns)
        Assert.DoesNotContain("Age", dfNot.Columns)

    [<Fact>]
    member _.``Selector: Exclusion and Arithmetic`` () =
        let df = mkDf()

        // --- 场景 A: 显式 Exclude ---
        // 选所有列，但排除 "Salary" 和 "JoinDate"
        let selExc = pl.cs.all().Exclude(["Salary"; "JoinDate"])
        let dfExc = df.Select([selExc.ToExpr()])
        
        Assert.DoesNotContain("Salary", dfExc.Columns)
        Assert.DoesNotContain("JoinDate", dfExc.Columns)
        Assert.Contains("Name", dfExc.Columns)

        // --- 场景 B: 减法操作符 (-) ---
        // Numeric - Float64 (只剩 Int: Age)
        let selDiff = pl.cs.numeric() - pl.cs.byType pl.Float64
        let dfDiff = df.Select [selDiff.ToExpr()]
        
        Assert.Single dfDiff.Columns |> ignore
        Assert.Equal("Age", dfDiff.Columns.[0])

    [<Fact>]
    member _.``Selector: Regex Matching`` () =
        let df = mkDf()

        // 匹配以 "Is" 开头或以 "me" 结尾的列 (IsActive, Name)
        let selRegex = pl.cs.matches "^Is.*|.*me$"
        let dfRegex = df.Select [selRegex.ToExpr()]
        
        Assert.Equal(2L, dfRegex.Width)
        Assert.Contains("IsActive", dfRegex.Columns)
        Assert.Contains("Name", dfRegex.Columns)

    [<Fact>]
    member _.``Selector: Complex ETL Pipeline`` () =
        // 一个贴近真实的复杂场景
        let df = mkDf()
        
        // 需求：
        // 1. 对所有数值列由原来的数值 -> 归一化 (除以 100)
        // 2. 对所有字符串列 -> 转大写
        // 3. 保持其他列不变
        
        let dfTransformed = 
            df.Select([
                // 1. 数值列处理
                pl.cs.numeric()
                    .ToExpr()
                    .Truediv(pl.lit 100.0) // 这里演示 Selector 转 Expr 后可以直接链式调用计算！
                    .Name.Suffix "_pct" // 重命名后缀
                
                // 2. 字符串列处理 (假设有 Str.ToUpper)
                // pl.cs.byType pl.String
                //     .ToExpr()
                //     .Str.ToUpper()
                
                // 3. 保留其他列 (JoinDate, IsActive)
                // 排除 numeric 和 string
                (~~~(pl.cs.numeric() ||| pl.cs.byType pl.String)).ToExpr()
            ])
            
        // 验证数值列变了
        Assert.Contains("Age_pct", dfTransformed.Columns)
        Assert.Contains("Salary_pct", dfTransformed.Columns)
        
        // 验证计算结果
        // Alice Age 30 -> 0.3
        Assert.Equal(0.3, dfTransformed.Cell<double>(0, "Age_pct"))
        
        // 验证保留列还在
        Assert.Contains("IsActive", dfTransformed.Columns)