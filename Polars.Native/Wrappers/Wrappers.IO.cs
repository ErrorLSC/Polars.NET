namespace Polars.Native;

public static partial class PolarsWrapper
{
    public static DataFrameHandle ReadCsv(string path, bool tryParseDates)
    {
        if (!File.Exists(path)) throw new FileNotFoundException($"CSV not found: {path}");
        return ErrorHelper.Check(NativeBindings.pl_read_csv(path, tryParseDates));
    }

    public static LazyFrameHandle ScanCsv(string path, bool tryParseDates)
    {
        if (!File.Exists(path)) throw new FileNotFoundException($"CSV not found: {path}");
        return ErrorHelper.Check(NativeBindings.pl_scan_csv(path, tryParseDates));
    }

    public static DataFrameHandle ReadParquet(string path)
    {
         if (!File.Exists(path)) throw new FileNotFoundException($"Parquet not found: {path}");
         return ErrorHelper.Check(NativeBindings.pl_read_parquet(path));
    }

    public static LazyFrameHandle ScanParquet(string path) {
        if (!File.Exists(path)) throw new FileNotFoundException($"Parquet not found: {path}");
        return ErrorHelper.Check(NativeBindings.pl_scan_parquet(path));
    } 

    public static void WriteCsv(DataFrameHandle df, string path) => NativeBindings.pl_write_csv(df, path);
    public static void WriteParquet(DataFrameHandle df, string path) => NativeBindings.pl_write_parquet(df, path);
}