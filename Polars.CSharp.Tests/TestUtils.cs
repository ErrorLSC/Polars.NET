namespace Polars.CSharp.Tests;

/// <summary>
/// 辅助类：自动创建并清理临时 CSV 文件。
/// </summary>
public class DisposableFile : IDisposable
{
    public string Path { get; }

    // 模式 A: 文本模式 (用于 CSV, JSON, NDJSON)
    public DisposableFile(string content, string extension = ".csv")
    {
        // 确保扩展名带点
        if (!extension.StartsWith(".")) extension = "." + extension;
        
        Path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"{Guid.NewGuid()}{extension}");
        File.WriteAllText(Path, content);
    }

    // 模式 B: 仅占位路径 (用于 Parquet/IPC Round-Trip 测试)
    // 我们只生成一个随机路径，让测试代码自己去 Write 文件
    public DisposableFile(string extension = ".parquet")
    {
        if (!extension.StartsWith(".")) extension = "." + extension;
        Path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"{Guid.NewGuid()}{extension}");
    }

    public void Dispose()
    {
        if (File.Exists(Path))
        {
            try 
            {
                File.Delete(Path);
            }
            catch 
            {
                // 忽略占用/删除失败
            }
        }
    }
}