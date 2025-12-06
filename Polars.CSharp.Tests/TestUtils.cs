using System;
using System.IO;

namespace Polars.CSharp.Tests;

/// <summary>
/// 辅助类：自动创建并清理临时 CSV 文件。
/// </summary>
public class DisposableCsv : IDisposable
{
    public string Path { get; }

    public DisposableCsv(string content)
    {
        Path = System.IO.Path.GetTempFileName();
        File.WriteAllText(Path, content);
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
                // 忽略删除失败，避免测试变 flaky
            }
        }
    }
}