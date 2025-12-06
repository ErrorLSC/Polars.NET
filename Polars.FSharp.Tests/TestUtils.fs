namespace PolarsFSharp.Tests

open System
open System.IO

// 辅助：创建临时 CSV，用完自动删除
type TempCsv(content: string) =
    let path = Path.GetTempFileName()
    do File.WriteAllText(path, content)
    
    member _.Path = path
    
    interface IDisposable with
        member _.Dispose() = 
            if File.Exists path then 
                try File.Delete path with _ -> () // 防止文件占用报错