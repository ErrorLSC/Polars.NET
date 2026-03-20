namespace Polars.FSharp.Tests

open System
open System.IO

type TempCsv(content: string) =
    let path = Path.GetTempFileName()
    do File.WriteAllText(path, content)
    
    member _.Path = path
    
    interface IDisposable with
        member _.Dispose() = 
            if File.Exists path then 
                try File.Delete path with _ -> () 