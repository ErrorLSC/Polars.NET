namespace Polars.CSharp.Tests;

public class DisposableFile : IDisposable
{
    public string Path { get; }

    public DisposableFile(string content, string extension = ".csv")
    {
        if (!extension.StartsWith(".")) extension = "." + extension;
        
        Path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"{Guid.NewGuid()}{extension}");
        File.WriteAllText(Path, content);
    }

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
            }
        }
    }
}


// Copied from Polars.CSharp.Linq.Tests/Linq/LinqTests.cs (LINQ tests moved to their own net11 project); used by Frames/StreamingTests.cs.
public record SensorReading(int SensorId, double Temperature, string Location);
