namespace PolarsFSharp

open System
open Polars.Native
open Apache.Arrow

type DataType =
    | Boolean
    | Int8 | Int16 | Int32 | Int64
    | UInt8 | UInt16 | UInt32 | UInt64
    | Float32 | Float64
    | String
    | Date | Datetime | Time
    | Duration
    | Binary
    | Unknown

    // 转换 helper
    member internal this.ToNative() =
        match this with
        | Boolean -> PlDataType.Boolean
        | Int8 -> PlDataType.Int8
        | Int16 -> PlDataType.Int16
        | Int32 -> PlDataType.Int32
        | Int64 -> PlDataType.Int64
        | UInt8 -> PlDataType.UInt8
        | UInt16 -> PlDataType.UInt16
        | UInt32 -> PlDataType.UInt32
        | UInt64 -> PlDataType.UInt64
        | Float32 -> PlDataType.Float32
        | Float64 -> PlDataType.Float64
        | String -> PlDataType.String
        | Date -> PlDataType.Date
        | Datetime -> PlDataType.Datetime
        | Time -> PlDataType.Time
        | Duration -> PlDataType.Duration
        | Binary -> PlDataType.Binary
        | Unknown -> PlDataType.Unknown

type JoinType =
    | Inner
    | Left
    | Outer
    | Cross
    | Semi
    | Anti
    
    // 内部转换 helper
    member internal this.ToNative() =
        match this with
        | Inner -> PlJoinType.Inner
        | Left -> PlJoinType.Left
        | Outer -> PlJoinType.Outer
        | Cross -> PlJoinType.Cross
        | Semi -> PlJoinType.Semi
        | Anti -> PlJoinType.Anti

// F# 风格的 PivotAgg
type PivotAgg =
    | First | Sum | Min | Max | Mean | Median | Count | Last
    
    member internal this.ToNative() =
        match this with
        | First -> PlPivotAgg.First
        | Sum -> PlPivotAgg.Sum
        | Min -> PlPivotAgg.Min
        | Max -> PlPivotAgg.Max
        | Mean -> PlPivotAgg.Mean
        | Median -> PlPivotAgg.Median
        | Count -> PlPivotAgg.Count
        | Last -> PlPivotAgg.Last
// ==========================================
// Expr 类型封装
// ==========================================
type Expr(handle: ExprHandle) =
    member _.Handle = handle
    member internal this.CloneHandle() = PolarsWrapper.CloneExpr handle
    // [新增] Cast
    // 用法: col("age").Cast(DataType.Float64)
    member this.Cast(dtype: DataType, ?strict: bool) =
        let isStrict = defaultArg strict false
        new Expr(PolarsWrapper.Cast(this.CloneHandle(), dtype.ToNative(), isStrict))
    // --- Helpers ---
    member this.Round(decimals: int) = new Expr(PolarsWrapper.Round(this.CloneHandle(), uint decimals))
    // 运算符重载, Compare
    static member (.>) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Gt(lhs.Handle, rhs.Handle))
    static member (.<) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Lt(lhs.Handle, rhs.Handle))
    static member (.>=) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.GtEq(lhs.Handle, rhs.Handle))
    static member (.<=) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.LtEq(lhs.Handle, rhs.Handle))
    static member (.==) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Eq(lhs.Handle, rhs.Handle))
    static member (.!=) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Neq(lhs.Handle, rhs.Handle))
    // 运算符重载, Arithmetic
    static member ( + ) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Add(lhs.Handle, rhs.Handle))
    static member ( - ) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Sub(lhs.Handle, rhs.Handle))
    static member ( * ) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Mul(lhs.Handle, rhs.Handle))
    static member ( / ) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Div(lhs.Handle, rhs.Handle))
    static member ( % ) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Rem(lhs.Handle, rhs.Handle))
    static member (.**) (baseExpr: Expr, exponent: Expr) = baseExpr.Pow(exponent)
    // --- 逻辑运算符 ---
    // 使用 .&& 和 .|| 避免与 F# 的短路逻辑 && 冲突
    static member (.&&) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.And(lhs.Handle, rhs.Handle))
    static member (.||) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Or(lhs.Handle, rhs.Handle))
    // 逻辑非 (unary not) -> !expr
    // F# 中一元非可以用 ~~ 或者自定义
    static member (!!) (e: Expr) = new Expr(PolarsWrapper.Not(e.Handle))
    // 方法
    member this.Alias(name: string) = new Expr(PolarsWrapper.Alias(handle, name))
    member this.Sum() = new Expr(PolarsWrapper.Sum(handle))
    member this.Mean() = new Expr(PolarsWrapper.Mean(handle))
    member this.Max() = new Expr(PolarsWrapper.Max(handle))
    member this.Min() = new Expr(PolarsWrapper.Min(handle))
    member this.Abs() = new Expr(PolarsWrapper.Abs(handle))
    // FillNull (填充空值)
    // 
    member this.FillNull(fillValue: Expr) = 
        new Expr(PolarsWrapper.FillNull(this.CloneHandle(), fillValue.CloneHandle()))

    // IsNull (检查是否为空)
    member this.IsNull() = 
        new Expr(PolarsWrapper.IsNull(this.CloneHandle()))

    // IsNotNull
    member this.IsNotNull() = 
        new Expr(PolarsWrapper.IsNotNull(this.CloneHandle()))
    // 基础 Pow: 接受 Expr
    member this.Pow(exponent: Expr) = 
        new Expr(PolarsWrapper.Pow(this.CloneHandle(), exponent.CloneHandle()))

    // 重载 Pow: 方便用户直接传数字 (pow(2))
    // 利用万能 lit 转换
    member this.Pow(exponent: double) = 
        this.Pow(PolarsWrapper.Lit(exponent) |> fun h -> new Expr(h)) // 这里偷懒直接调Wrapper构造临时Expr
    member this.Pow(exponent: int) = 
        this.Pow(PolarsWrapper.Lit(exponent) |> fun h -> new Expr(h))
    member this.Sqrt() = new Expr(PolarsWrapper.Sqrt(this.CloneHandle()))
    member this.Exp() = new Expr(PolarsWrapper.Exp(this.CloneHandle()))
    // [场景 1] 常数底数 (性能最好，直接调 Rust)
    member this.Log(baseVal: double) = 
        new Expr(PolarsWrapper.Log(this.CloneHandle(), baseVal))

    // [场景 2] 动态底数 (Rust 的 log 不支持 Expr，我们用数学公式模拟)
    // log_b(x) = ln(x) / ln(b)
    member this.Log(baseExpr: Expr) = 
        this.Ln() / baseExpr.Ln()

    // 自然对数 (快捷方式)
    member this.Ln() = 
        this.Log(Math.E)

    // --- Namespaces ---
    member this.Name = new NameOps(this.CloneHandle())
    member this.List = new ListOps(this.CloneHandle())
    member this.Struct = new StructOps(this.CloneHandle())
    // Explode
    member this.Explode() = new Expr(PolarsWrapper.Explode(this.CloneHandle()))
    // IsBetween
    member this.IsBetween(lower: Expr, upper: Expr) =
        new Expr(PolarsWrapper.IsBetween(this.CloneHandle(), lower.CloneHandle(), upper.CloneHandle()))

    member this.Map(func: Func<IArrowArray, IArrowArray>) =
        new Expr(PolarsWrapper.Map(this.CloneHandle(), func))
    member this.Map(func: Func<IArrowArray, IArrowArray>, outputType: PlDataType) =
        new Expr(PolarsWrapper.Map(this.CloneHandle(), func, outputType))
    member this.Dt = new DtOps(handle)
    member this.Str = new StringOps(this.CloneHandle())

    // Over
    // 用法: col("salary").Sum().Over([col("dept")])
    member this.Over(partitionBy: Expr list) =
        // 1. 克隆主表达式
        let mainHandle = this.CloneHandle()
        
        // 2. 克隆分组列表
        let partHandles = partitionBy |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        
        // 3. 调用 Wrapper
        new Expr(PolarsWrapper.Over(mainHandle, partHandles))

    // 重载：方便只传一个分组列的情况
    member this.Over(partitionCol: Expr) =
        this.Over [partitionCol]
    // Shift (平移)
    member this.Shift(n: int64) = new Expr(PolarsWrapper.Shift(this.CloneHandle(), n))
    // 默认 shift 1
    member this.Shift() = this.Shift(1L)

    // Diff (差分)
    member this.Diff(n: int64) = new Expr(PolarsWrapper.Diff(this.CloneHandle(), n))
    // 默认 diff 1
    member this.Diff() = this.Diff 1L

    // Fill (填充)
    // limit: 0 表示无限填充
    member this.ForwardFill(?limit: int) = 
        let l = defaultArg limit 0
        new Expr(PolarsWrapper.ForwardFill(this.CloneHandle(), uint l))

    member this.BackwardFill(?limit: int) = 
        let l = defaultArg limit 0
        new Expr(PolarsWrapper.BackwardFill(this.CloneHandle(), uint l))
    
    // 别名
    member this.FillNullStrategy(strategy: string) =
        match strategy.ToLower() with
        | "forward" | "ffill" -> this.ForwardFill()
        | "backward" | "bfill" -> this.BackwardFill()
        | _ -> failwith "Unsupported strategy"

    member this.RollingMin(windowSize: string) = 
        new Expr(PolarsWrapper.RollingMin(this.CloneHandle(), windowSize))
        
    member this.RollingMax(windowSize: string) = 
        new Expr(PolarsWrapper.RollingMax(this.CloneHandle(), windowSize))

    member this.RollingMean(windowSize: string) = 
        new Expr(PolarsWrapper.RollingMean(this.CloneHandle(), windowSize))
        
    member this.RollingSum(windowSize: string) = 
        new Expr(PolarsWrapper.RollingSum(this.CloneHandle(), windowSize))
    // 用法: col("price").RollingMeanBy("1d", col("date"))
    member this.RollingMeanBy(windowSize: string, by: Expr, ?closed: string) =
        let c = defaultArg closed "left"
        new Expr(PolarsWrapper.RollingMeanBy(this.CloneHandle(), windowSize, by.CloneHandle(), c))

    member this.RollingSumBy(windowSize: string, by: Expr, ?closed: string) =
        let c = defaultArg closed "left"
        new Expr(PolarsWrapper.RollingSumBy(this.CloneHandle(), windowSize, by.CloneHandle(), c))
    // 用法: col("price").RollingMeanBy("1d", col("date"))
    member this.RollingMaxBy(windowSize: string, by: Expr, ?closed: string) =
        let c = defaultArg closed "left"
        new Expr(PolarsWrapper.RollingMaxBy(this.CloneHandle(), windowSize, by.CloneHandle(), c))

    member this.RollingMinBy(windowSize: string, by: Expr, ?closed: string) =
        let c = defaultArg closed "left"
        new Expr(PolarsWrapper.RollingMinBy(this.CloneHandle(), windowSize, by.CloneHandle(), c))


and DtOps(handle: ExprHandle) =
    let wrap op = new Expr(op handle)
    member _.Year() = wrap PolarsWrapper.DtYear
    member _.Month() = wrap PolarsWrapper.DtMonth
    member _.Day() = wrap PolarsWrapper.DtDay
    member _.Hour() = wrap PolarsWrapper.DtHour
    member _.Minute() = wrap PolarsWrapper.DtMinute
    member _.Second() = wrap PolarsWrapper.DtSecond
    member _.Millisecond() = wrap PolarsWrapper.DtMillisecond
    member _.Microsecond() = wrap PolarsWrapper.DtMicrosecond
    member _.Nanosecond() = wrap PolarsWrapper.DtNanosecond
    member _.OrdinalDay() = wrap PolarsWrapper.DtOrdinalDay
    member _.Weekday() = wrap PolarsWrapper.DtWeekday
    member _.Date() = wrap PolarsWrapper.DtDate
    member _.Time() = wrap PolarsWrapper.DtTime

    // 1. 指定格式转换
    // 用法: col("date").Dt.ToString("%Y-%m-%d")
    member _.ToString(format: string) = 
        new Expr(PolarsWrapper.DtToString(handle, format)) // 注意这里 handle 是 Clone 进来的，Wrapper 会消耗它

    // 2. [重载] 默认格式转换 (ISO 8601)
    // 用法: col("date").Dt.ToString()
    member this.ToString() = 
        // 这是一个常见的 ISO 格式，或者你可以选择其他默认值
        this.ToString("%Y-%m-%dT%H:%M:%S%.f")
and StringOps(handle: ExprHandle) =
    // 内部帮助函数
    let wrap op = new Expr(op handle)
    
    // 大小写
    member _.ToUpper() = wrap PolarsWrapper.StrToUpper
    member _.ToLower() = wrap PolarsWrapper.StrToLower
    
    // 长度
    member _.Len() = wrap PolarsWrapper.StrLenBytes
    
    // 切片
    // F# uint64 = C# ulong
    member _.Slice(offset: int64, length: uint64) = 
        new Expr(PolarsWrapper.StrSlice(handle, offset, length))
        
    // 替换 (Replace All)
    member _.ReplaceAll(pattern: string, value: string, ?useRegex: bool) =
        let regex = defaultArg useRegex false
        new Expr(PolarsWrapper.StrReplaceAll(handle, pattern, value,regex))

    member _.Extract(pattern: string, groupIndex: int) =
        new Expr(PolarsWrapper.StrExtract(handle, pattern, uint groupIndex))

    // 包含 (之前做的)
    member _.Contains(pat: string) = 
        new Expr(PolarsWrapper.StrContains(handle, pat))
    member _.Split(separator: string) = new Expr(PolarsWrapper.StrSplit(handle, separator))
and NameOps(handle: ExprHandle) =
    let wrap op arg = new Expr(op(handle, arg))
    member _.Prefix(p: string) = wrap PolarsWrapper.Prefix p
    member _.Suffix(s: string) = wrap PolarsWrapper.Suffix s

and ListOps(handle: ExprHandle) =
    member _.First() = new Expr(PolarsWrapper.ListFirst(handle))
    member _.Get(index: int) = new Expr(PolarsWrapper.ListGet(handle, int64 index))
    member _.Join(separator: string) = new Expr(PolarsWrapper.ListJoin(handle, separator))
    member _.Len() = new Expr(PolarsWrapper.ListLen(handle))
    // Aggregations within list
    member _.Sum() = new Expr(PolarsWrapper.ListSum(handle))
    member _.Min() = new Expr(PolarsWrapper.ListMin(handle))
    member _.Max() = new Expr(PolarsWrapper.ListMax(handle))
    member _.Mean() = new Expr(PolarsWrapper.ListMean(handle))
    
    // Sort
    member _.Sort(descending: bool) = new Expr(PolarsWrapper.ListSort(handle, descending))
    
    // Contains
    member _.Contains(item: Expr) : Expr = 
        // 注意：item 也要 clone
        new Expr(PolarsWrapper.ListContains(handle, item.CloneHandle()))
    // Contains 重载 (方便传字面量)
    member _.Contains(item: int) = 
        // [修复] 变量名定义为 itemHandle，以便下一行使用
        let itemHandle = PolarsWrapper.Lit(item)
        
        // 为了最大安全性，建议 clone handle (列表本身)，消耗 itemHandle (元素)
        new Expr(PolarsWrapper.ListContains(PolarsWrapper.CloneExpr(handle), itemHandle))

    // 3. 针对 string 的重载
    member _.Contains(item: string) = 
        // [修复] 变量名一致
        let itemHandle = PolarsWrapper.Lit(item)
        new Expr(PolarsWrapper.ListContains(PolarsWrapper.CloneExpr(handle), itemHandle))

and StructOps(handle: ExprHandle) =
    // 取字段
    member _.Field(name: string) = 
        new Expr(PolarsWrapper.StructFieldByName(handle, name))
type Selector(handle: SelectorHandle) =
    member _.Handle = handle
    
    member internal this.CloneHandle() = 
        PolarsWrapper.CloneSelector(handle)

    // Exclude 方法
    member this.Exclude(names: string list) =
        let arr = List.toArray names
        // 使用 CloneHandle，防止 this.Handle 被消耗
        new Selector(PolarsWrapper.SelectorExclude(this.CloneHandle(), arr))

    member this.ToExpr() =
        // 转换也会消耗 Selector，所以要 Clone
        new Expr(PolarsWrapper.SelectorToExpr(this.CloneHandle()))

// DataFrame 封装
type DataFrame(handle: DataFrameHandle) =
    interface IDisposable with
        member _.Dispose() = handle.Dispose()
    member this.Clone() = new DataFrame(PolarsWrapper.CloneDataFrame handle)
    member internal this.CloneHandle() = PolarsWrapper.CloneDataFrame handle
    member _.Handle = handle
    
    // 依然保留，用于 Show 或者用户真的需要 Arrow 数据时
    member this.ToArrow() = PolarsWrapper.Collect(handle)

    // 零拷贝获取行数
    member _.Rows = PolarsWrapper.DataFrameHeight(handle)

    // 零拷贝获取列数
    member _.Columns = PolarsWrapper.DataFrameWidth(handle)
    member _.ColumnNames = PolarsWrapper.GetColumnNames(handle) |> Array.toList

    member this.Item 
        with get(colName: string, rowIndex: int) =
            // 默认返回 float (double)，因为最通用。
            // 如果需要其他类型，可以使用下面的专用方法
            PolarsWrapper.GetDouble(handle, colName, int64 rowIndex)

    // 专用取值方法
    member this.Int(colName: string, rowIndex: int) : int64 option = 
        let nullableVal = PolarsWrapper.GetInt(handle, colName, int64 rowIndex)
        if nullableVal.HasValue then Some nullableVal.Value else None

    member this.Float(colName: string, rowIndex: int) : float option = 
        let nullableVal = PolarsWrapper.GetDouble(handle, colName, int64 rowIndex)
        if nullableVal.HasValue then Some nullableVal.Value else None
    member this.String(colName: string, rowIndex: int) = PolarsWrapper.GetString(handle, colName, int64 rowIndex) |> Option.ofObj

    member this.StringList(colName: string, rowIndex: int) : string list option =
        // 1. 获取该列的 Arrow Array
        use colHandle = PolarsWrapper.Select(handle, [| PolarsWrapper.Col(colName) |])
        use tempDf = new DataFrame(colHandle)
        use arrowBatch = tempDf.ToArrow()
        
        let col = arrowBatch.Column(colName)
        
        // 内部辅助函数：从 Values 数组中提取字符串
        let extractStrings (valuesArr: IArrowArray) (startIdx: int) (endIdx: int) =
            match valuesArr with
            | :? StringArray as sa ->
                [ for i in startIdx .. endIdx - 1 -> sa.GetString(i) ]
            | :? StringViewArray as sva ->
                [ for i in startIdx .. endIdx - 1 -> sva.GetString(i) ]
            | _ -> [] // 类型不匹配

        // 2. 解析 ListArray 或 LargeListArray
        match col with
        // Case A: 标准 List (32-bit offsets)
        | :? Apache.Arrow.ListArray as listArr ->
            if listArr.IsNull(rowIndex) then None
            else
                let start = listArr.ValueOffsets.[rowIndex]
                let end_ = listArr.ValueOffsets.[rowIndex + 1]
                Some (extractStrings listArr.Values start end_)

        // Case B: [关键修复] Large List (64-bit offsets) - Polars 通常输出这个
        | :? Apache.Arrow.LargeListArray as listArr ->
            if listArr.IsNull(rowIndex) then None
            else
                // Offset 是 long，强转 int (单行 List 长度通常不会超过 20 亿)
                let start = int (listArr.ValueOffsets.[rowIndex])
                let end_ = int (listArr.ValueOffsets.[rowIndex + 1])
                Some (extractStrings listArr.Values start end_)

        | _ -> 
            // 调试信息：如果未来 Polars 改用 ListViewArray，这里能看出来
            // System.Console.WriteLine($"[Debug] Mismatched Array Type: {col.GetType().Name}")
            None

// LazyFrame 封装
// 它依赖 DataFrame (Collect 返回 DataFrame)，所以必须定义在 DataFrame 后面
type LazyFrame(handle: LazyFrameHandle) =
    member _.Handle = handle
    member internal this.CloneHandle() = PolarsWrapper.LazyClone(handle)
    member this.Collect() = 
        let dfHandle = PolarsWrapper.LazyCollect(handle)
        new DataFrame(dfHandle)
    // 返回 JSON 字符串
    member _.SchemaRaw = PolarsWrapper.GetSchemaString(handle)

    // 返回 Map<string, string>
    member _.Schema = 
        let dict = PolarsWrapper.GetSchema(handle)
        dict |> Seq.map (fun kv -> kv.Key, kv.Value) |> Map.ofSeq

    member this.Explain(?optimized: bool) = 
        let opt = defaultArg optimized true
        PolarsWrapper.Explain(handle, opt)

type SqlContext() =
    let handle = PolarsWrapper.SqlContextNew()
    
    interface IDisposable with
        member _.Dispose() = handle.Dispose()

    // 注册表
    member _.Register(name: string, lf: LazyFrame) =
        // 同样，注册是 Move 操作，需要 CloneHandle
        PolarsWrapper.SqlRegister(handle, name, lf.CloneHandle())

    // 执行查询
    member _.Execute(query: string) =
        new LazyFrame(PolarsWrapper.SqlExecute(handle, query))