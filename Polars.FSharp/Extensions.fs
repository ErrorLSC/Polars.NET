namespace Polars.FSharp

open System
open Apache.Arrow
open Polars.NET.Core
open Polars.NET.Core.Arrow

// =========================================================================================
// MODULE: Series Extensions (Data Conversion & Computation)
// =========================================================================================
[<AutoOpen>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module SeriesExtensions =
    // -----------------------------------------------------------
    // 1. Data Conversion (Series <-> Seq)
    // -----------------------------------------------------------
    type Series with
        /// <summary>
        /// High-performance creation from any sequence. 
        /// Supports nested lists, structs, and F# Options.
        /// </summary>
        static member ofSeq<'T>(name: string, data: seq<'T>) : Series =
        // 1. 调用 C# 黑魔法构建 Arrow Array
        // 这里会自动处理 Option<'T> -> Nullable 逻辑
            let arrowArray = ArrowConverter.Build data
            
            // 2. 零拷贝导入 Polars
            let handle = ArrowFfiBridge.ImportSeries(name, arrowArray)
            
            new Series(handle)

        /// <summary>
        /// Convert Series to a typed sequence of Options.
        /// Uses high-performance Arrow reader (Zero-Copy).
        /// Supports: Primitives, String, DateTime, DateOnly, TimeOnly, List, Struct.
        /// </summary>
        member this.AsSeq<'T>() : seq<'T option> =
        // 注意：必须在 seq 内部进行 Export，否则 seq 还没开始跑，Array 就被 Dispose 了
            seq {
                // 1. 导出为 Arrow Array (C# IArrowArray 实现了 IDisposable)
                // 当 seq 迭代结束或被中断时，Dispose 会自动被调用，释放非托管内存
                use cArray = PolarsWrapper.SeriesToArrow this.Handle
                
                // 2. 获取 C# 高性能 Accessor (预编译的委托)
                // accessor 签名: int -> object?
                let accessor = ArrowReader.GetSeriesAccessor<'T>(cArray)
                let len = cArray.Length

                // 3. 高速循环
                for i in 0 .. len - 1 do
                    let valObj = accessor.Invoke(i)
                    // 这里的 isNull 既能判断 null 引用，也能判断 Nullable<T> 的无值状态
                    if isNull valObj then 
                        None 
                    else 
                        // Unbox: 将 object 强转回 'T
                        Some(unbox<'T> valObj)
            }
        /// <summary>
        /// Get values as a list (forces evaluation).
        /// </summary>
        member this.ToList<'T>() = this.AsSeq<'T>() |> Seq.toList

    // -----------------------------------------------------------
    // 3. UDF Support (Direct Map on Series)
    // -----------------------------------------------------------
    type Series with
        /// <summary>
        /// Apply a C# UDF (Arrow->Arrow) directly to this Series.
        /// Returns a new Series.
        /// </summary>
        member this.Map(func: Func<IArrowArray, IArrowArray>) : Series =
            // 1. 获取输入 (Zero-Copy)
            let inputArrow = this.ToArrow()
            
            // 2. 执行运算
            let outputArrow = func.Invoke inputArrow
            
            // 3. 将结果封装回 Series
            // 这里的巧妙之处：Extensions.fs 可以看到 pl.fromArrow
            
            // 3.1 构建 RecordBatch 包装纸
            let field = new Field(this.Name, outputArrow.Data.DataType, true)
            let schema = new Schema([| field |], null)
            use batch = new RecordBatch(schema, [| outputArrow |], outputArrow.Length)
            
            // 3.2 借道 DataFrame (Zero-Copy import)
            use df = DataFrame.FromArrow batch
            
            // 3.3 提取 Series (Clone Handle)
            let res = df.Column 0
            
            // 显式重命名以防万一
            res.Rename this.Name

// =========================================================================================
// MODULE: DataFrame Serialization (Record <-> DataFrame)
// =========================================================================================
[<AutoOpen>]
module Serialization =

    // ==========================================
    // Extensions (Exposed Methods)
    // ==========================================
    
    type DataFrame with
        
        /// <summary>
        /// [ToRecords] 将 DataFrame 转换为 F# Record 列表
        /// </summary>
        member this.ToRecords<'T>() : seq<'T> =
            // 1. 导出为 RecordBatch
            use batch = ArrowFfiBridge.ExportDataFrame this.Handle
            
            // 2. 高性能读取
            // ToList 是为了立即执行读取，避免 batch 被 Dispose 后再延迟枚举导致 AccessViolation
            // 如果数据量巨大，可以考虑不 ToList，但要小心 batch 的生命周期
            ArrowReader.ReadRecordBatch<'T> batch |> Seq.toList |> List.toSeq

        /// <summary>
        /// Create a DataFrame from a sequence of F# Records or Objects.
        /// Uses high-performance Apache Arrow interop.
        /// Supports: F# Option, Nested Lists, DateTime, etc.
        /// </summary>
        static member ofRecords<'T>(data: seq<'T>) : DataFrame =
            // 1. 数据判空保护 (可选，ArrowConverter 其实能处理空列表，但 Schema 会为空)
            // 如果你希望空列表也能根据 'T 推断 Schema，ArrowConverter 需要 T 的反射信息
            // 我们的 ArrowConverter 已经实现了这一点，所以直接传也没问题。
            
            // 2. C# Build RecordBatch
            // 这一步完成了所有的反射、类型转换、内存填充
            let batch = ArrowFfiBridge.BuildRecordBatch data
            
            // 3. Import via FFI
            // 注意：ImportDataFrame 成功后，Rust 会接管 batch 的内存
            // 如果这里抛出异常，我们需要 dispose batch
            // 但 ArrowFfiBridge.ImportDataFrame 内部已经处理了 Dispose 逻辑
            
            let handle = ArrowFfiBridge.ImportDataFrame batch
            new DataFrame(handle)
        member this.Describe() : DataFrame =
            // 1. 筛选数值列 (Int/Float)
            // 我们利用 Schema 来判断
            let numericCols = 
                this.Schema 
                |> Map.filter (fun _ dtype -> dtype.IsNumeric)
                |> Map.keys
                |> Seq.toList

            if numericCols.IsEmpty then
                failwith "No numeric columns to describe."

            // 2. 定义统计指标
            // 每个指标生成一行数据
            let metrics = [
                "count",      fun (c: string) -> pl.col(c).Count().Cast Float64
                "null_count", fun c -> pl.col(c).IsNull().Sum().Cast Float64
                "mean",       fun c -> pl.col(c).Mean()
                "std",        fun c -> pl.col(c).Std()
                "min",        fun c -> pl.col(c).Min().Cast Float64
                "25%",        fun c -> pl.col(c).Quantile 0.25
                "50%",        fun c -> pl.col(c).Median().Cast Float64 
                "75%",        fun c -> pl.col(c).Quantile 0.75
                "max",        fun c -> pl.col(c).Max().Cast Float64
            ]

            // 3. 构建聚合查询
            // 结果将是:
            // statistic | col1 | col2 ...
            // count     | ...  | ...
            // mean      | ...  | ...
            
            // 这里的策略是：先计算所有值，然后转置？
            // 不，Polars 推荐的方式是：构建一个包含 "statistic" 列和其他列的 List of DataFrames，然后 Concat。
            // 每一行（比如 mean）是一个小的 DataFrame：[statistic="mean", col1=mean1, col2=mean2...]
            
            let rowFrames = 
                metrics 
                |> List.map (fun (statName, op) ->
                    // 构造 Select 列表: [ Lit(statName).Alias("statistic"), op(col1), op(col2)... ]
                    let exprs = 
                        [ pl.lit(statName).Alias "statistic" ] @
                        (numericCols |> List.map (fun c -> op c))
                    
                    // 对原 DF 执行 Select -> 得到 1 行 N 列的 DF
                    this |> pl.select exprs
                )

            // 4. 垂直拼接 (Concat Vertical)
            pl.concat rowFrames