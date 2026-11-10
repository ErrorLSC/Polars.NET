using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Text;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Native;
using Polars.NET.Core.Exceptions;

namespace Polars.NET.Core.Delta;

/// <summary>
/// Sinks a lazy frame as Parquet files with the engine's streaming partitioned sink: split by partition keys (not
/// written into the files) and by size, each file at a path the caller chooses. Nothing is collected.
/// </summary>
internal static class HostPathSink
{
    /// <summary>Names the file <paramref name="index"/> of the partition whose key values are the one row of <paramref name="keys"/>.</summary>
    /// <returns>The path, relative to the base path, with <c>/</c> separators.</returns>
    public delegate string PathFor(DataFrameHandle keys, long index);

    /// <summary>Sinks <paramref name="lf"/> (consumed) under <paramref name="basePath"/>.</summary>
    /// <param name="maxRowsPerFile">The most rows in one file; 0 for no limit.</param>
    /// <param name="approxBytesPerFile">The approximate in-memory size of the rows of one file; 0 for no limit.</param>
    /// <param name="pathFor">Called for every file, possibly from several threads (calls are serialized); its
    /// exceptions are rethrown here.</param>
    public static unsafe void Sink(
        LazyFrameHandle lf, string basePath, string[] partitionKeys, bool keysPreGrouped, ulong maxRowsPerFile, ulong approxBytesPerFile,
        PathFor pathFor, PlParquetCompression compression, int compressionLevel, int rowGroupSize, int dataPageSize,
        bool maintainOrder, PlSyncOnClose syncOnClose, CloudArgs? cloud)
    {
        ArgumentNullException.ThrowIfNull(pathFor);
        cloud ??= CloudArgs.Local;
        var state = new State(pathFor);
        GCHandle handle = GCHandle.Alloc(state);
        try
        {
            try
            {
                NativeBindings.pl_lazyframe_sink_parquet_host_paths(
                    lf, basePath, partitionKeys, (nuint)partitionKeys.Length, keysPreGrouped, (nuint)maxRowsPerFile, approxBytesPerFile,
                    (nint)(delegate* unmanaged[Cdecl]<nint, nint, nuint, byte*, nuint, nuint*, int>)&OnPath, GCHandle.ToIntPtr(handle),
                    compression, compressionLevel, rowGroupSize > 0 ? (nuint)rowGroupSize : 0, dataPageSize > 0 ? (nuint)dataPageSize : 0,
                    maintainOrder, syncOnClose,
                    cloud.Provider, cloud.Retries, cloud.RetryTimeoutMs, cloud.RetryInitBackoffMs, cloud.RetryMaxBackoffMs, cloud.CacheTtl,
                    cloud.Keys, cloud.Values, (nuint)(cloud.Keys?.Length ?? 0));
                lf.TransferOwnership();
                ErrorHelper.CheckVoid();
            }
            catch (Exception) when (state.Error is not null)
            {
                // The native error only follows from the host-side failure: report that one below.
            }
            if (state.Error is not null) ExceptionDispatchInfo.Throw(state.Error);
        }
        finally
        {
            // Safe: the native sink disables the callback before it returns.
            handle.Free();
        }
    }

    private sealed class State(PathFor pathFor)
    {
        public readonly PathFor PathFor = pathFor;
        public readonly object Lock = new();
        public Exception? Error;
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static unsafe int OnPath(nint user, nint keys, nuint index, byte* outPath, nuint capacity, nuint* outLength)
    {
        // The keys frame is ours from here on, also when this fails.
        using var keysHandle = new DataFrameHandle(keys);
        State? state = null;
        try
        {
            state = (State)GCHandle.FromIntPtr(user).Target!;
            lock (state.Lock)
            {
                if (state.Error is not null) return 1;
                string path = state.PathFor(keysHandle, (long)index);
                int length = Encoding.UTF8.GetByteCount(path);
                if ((nuint)length > capacity) throw new PathTooLongException($"The file path {path} is longer than {capacity} bytes.");
                Encoding.UTF8.GetBytes(path, new Span<byte>(outPath, length));
                *outLength = (nuint)length;
                return 0;
            }
        }
        catch (Exception e)
        {
            if (state is not null)
            {
                lock (state.Lock) state.Error ??= e;
            }
            return 1;
        }
    }
}
