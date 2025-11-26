namespace PolarsFSharp

open System
open Apache.Arrow
open Apache.Arrow.Types
open Polars.Native

module Polars =
    
    // --- 积木工厂 ---
    let col (name: string) = new Expr(PolarsWrapper.Col(name))
    let alias (name: string) (expr: Expr) = expr.Alias(name)

    // --- 黑魔法：万能 lit ---
    type LitMechanism = LitMechanism with
        static member ($) (LitMechanism, v: int) = new Expr(PolarsWrapper.Lit(v))
        static member ($) (LitMechanism, v: string) = new Expr(PolarsWrapper.Lit(v))
        static member ($) (LitMechanism, v: double) = new Expr(PolarsWrapper.Lit(v))

    let inline lit (value: ^T) : Expr = 
        ((^T or LitMechanism) : (static member ($) : LitMechanism * ^T -> Expr) (LitMechanism, value))

    // --- IO ---
    let readCsv (path: string) (tryParseDates: bool option): DataFrame =
        let parseDates = defaultArg tryParseDates true
        let handle = PolarsWrapper.ReadCsv(path, parseDates)
        new DataFrame(handle)

    let readParquet (path: string) = new DataFrame(PolarsWrapper.ReadParquet(path))

    let writeCsv (path: string) (df: DataFrame) = 
        PolarsWrapper.WriteCsv(df.Handle, path)
        df 

    let writeParquet (path: string) (df: DataFrame) = 
        PolarsWrapper.WriteParquet(df.Handle, path)
        df

    // --- Eager Ops ---
    let filter (expr: Expr) (df: DataFrame) : DataFrame =
        let h = PolarsWrapper.Filter(df.Handle, expr.Handle)
        new DataFrame(h)

    let select (exprs: Expr list) (df: DataFrame) : DataFrame =
        let handles = exprs |> List.map (fun e -> e.Handle) |> List.toArray
        let h = PolarsWrapper.Select(df.Handle, handles)
        new DataFrame(h)

    let groupBy (keys: Expr list) (aggs: Expr list) (df: DataFrame) : DataFrame =
        let kHandles = keys |> List.map (fun e -> e.Handle) |> List.toArray
        let aHandles = aggs |> List.map (fun e -> e.Handle) |> List.toArray
        let h = PolarsWrapper.GroupByAgg(df.Handle, kHandles, aHandles)
        new DataFrame(h)

    let join (other: DataFrame) (leftOn: Expr list) (rightOn: Expr list) (how: string) (left: DataFrame) : DataFrame =
        let lHandles = leftOn |> List.map (fun e -> e.Handle) |> List.toArray
        let rHandles = rightOn |> List.map (fun e -> e.Handle) |> List.toArray
        let h = PolarsWrapper.Join(left.Handle, other.Handle, lHandles, rHandles, how)
        new DataFrame(h)

    let sum (e: Expr) = e.Sum()
    let mean (e: Expr) = e.Mean()
    let max (e: Expr) = e.Max()
    let min (e: Expr) = e.Min()

    // --- Lazy API ---
    let scanCsv (path: string) (tryParseDates: bool option) = 
        let parseDates = defaultArg tryParseDates true
        new LazyFrame(PolarsWrapper.ScanCsv(path, parseDates))

    let scanParquet (path: string) = new LazyFrame(PolarsWrapper.ScanParquet(path))

    let filterLazy (expr: Expr) (lf: LazyFrame) : LazyFrame =
        let h = PolarsWrapper.LazyFilter(lf.Handle, expr.Handle)
        new LazyFrame(h)

    let selectLazy (exprs: Expr list) (lf: LazyFrame) : LazyFrame =
        let handles = exprs |> List.map (fun e -> e.Handle) |> List.toArray
        let h = PolarsWrapper.LazySelect(lf.Handle, handles)
        new LazyFrame(h)

    let orderBy (expr: Expr) (desc: bool) (lf: LazyFrame) : LazyFrame =
        let h = PolarsWrapper.LazySort(lf.Handle, expr.Handle, desc)
        new LazyFrame(h)

    let limit (n: uint) (lf: LazyFrame) : LazyFrame =
        let h = PolarsWrapper.LazyLimit(lf.Handle, n)
        new LazyFrame(h)

    let withColumn (expr: Expr) (lf: LazyFrame) : LazyFrame =
        let handles = [| expr.Handle |]
        let h = PolarsWrapper.LazyWithColumns(lf.Handle, handles)
        new LazyFrame(h)

    let withColumns (exprs: Expr list) (lf: LazyFrame) : LazyFrame =
        let handles = exprs |> List.map (fun e -> e.Handle) |> List.toArray
        let h = PolarsWrapper.LazyWithColumns(lf.Handle, handles)
        new LazyFrame(h)

    let collect (lf: LazyFrame) : DataFrame = lf.Collect()

    // --- Show / Helper ---
    // 为了保持文件整洁，formatValue 可以设为 private
    let private formatValue (col: IArrowArray) (index: int) : string =
        if col.IsNull(index) then "(null)"
        else
            match col with
            | :? Int32Array as arr -> string (arr.GetValue(index))
            | :? Int64Array as arr -> string (arr.GetValue(index))
            | :? DoubleArray as arr -> string (arr.GetValue(index))
            | :? StringArray as arr -> arr.GetString(index)
            | :? StringViewArray as arr -> arr.GetString(index)
            | :? LargeStringArray as arr -> arr.GetString(index)
            | :? BooleanArray as arr -> string (arr.GetValue(index))
            | :? Date32Array as arr -> 
                let v = arr.GetValue(index)
                if v.HasValue then DateTime(1970, 1, 1).AddDays(float v.Value).ToString("yyyy-MM-dd")
                else "(null)"
            | :? TimestampArray as arr -> 
                let v = arr.GetValue(index) // Nullable<long>
                if v.HasValue then v.Value.ToString() else "(null)"
            | _ -> sprintf "[%s]" (col.GetType().Name)

    let show (df: DataFrame) : DataFrame =
        use batch = df.ToArrow()
        printfn "\n--- Polars DataFrame (Rows: %d) ---" batch.Length
        let fields = batch.Schema.FieldsList
        for field in fields do
            let col = batch.Column(field.Name)
            let typeName = if isNull col.Data then "Unknown" else col.Data.DataType.Name
            printfn "[%s: %s]" field.Name typeName
            
            let limit = Math.Min(batch.Length, 5)
            for i in 0 .. limit - 1 do
                printfn "  %s" (formatValue col i)
            if batch.Length > 5 then printfn "  ..."
        df