open System
open System.IO
open PolarsFSharp // 引用 namespace

module App =
    [<EntryPoint>]
    let main argv =
        printfn "F# Polars App Starting..."

        try
            // 构造测试数据路径
            let rootDir = AppDomain.CurrentDomain.BaseDirectory
            let csvPath = Path.GetFullPath(Path.Combine(rootDir, "test.csv"))
            if not (File.Exists(csvPath)) then 
                File.WriteAllText(csvPath, "name,age,salary\nAlice,25,5000\nBob,30,6000\nCharlie,35,7000")

            // 你的业务代码
            printfn "--- Reading CSV ---"
            let df = Polars.readCsv csvPath None
            
            df 
            |> Polars.filter (Polars.col "age" .> Polars.lit 25)
            |> Polars.show 10
            |> ignore

            printfn "--- Lazy API Test ---"
            let lf = Polars.scanCsv csvPath None
            
            lf
            |> Polars.filterLazy (Polars.col "salary" .> Polars.lit 5500)
            |> Polars.withColumn (Polars.col "age" * Polars.lit 2 |> Polars.alias "age_doubled")
            |> Polars.collect
            |> Polars.show 10
            |> ignore

        with ex -> 
            printfn "Error: %s" ex.Message

        0