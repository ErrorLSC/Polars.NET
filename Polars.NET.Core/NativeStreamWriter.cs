using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;

namespace Polars.NET.Core;

/// <summary>
/// Callback target handed to the native <c>pl_*_callback</c> writers.
/// The native side writes its output in chunks to <see cref="Callback"/>, passing <see cref="UserData"/> back.
/// </summary>
public readonly struct NativeWriteTarget
{
    /// <summary>Function pointer: <c>int (*)(void* user, const byte* data, nuint len)</c>, returns 0 on success.</summary>
    public readonly nint Callback;
    /// <summary>Opaque host state passed back to <see cref="Callback"/>.</summary>
    public readonly nint UserData;
    /// <summary>Native-side buffer size in bytes (0 = default, 64 KB).</summary>
    public readonly nuint BufferSize;

    internal NativeWriteTarget(nint callback, nint userData, nuint bufferSize)
    {
        Callback = callback;
        UserData = userData;
        BufferSize = bufferSize;
    }
}

/// <summary>
/// Runs a native writer that streams its output through a <see cref="NativeWriteTarget"/> into a
/// .NET <see cref="Stream"/> or <see cref="PipeWriter"/>, without materializing the whole output in memory.
/// <para>
/// The native writer may invoke the callback from Polars worker threads; calls are serialized on the native side.
/// Exceptions thrown by the destination are captured and rethrown to the caller unchanged.
/// The destination is flushed but never disposed / completed.
/// </para>
/// </summary>
public static class NativeStreamWriter
{
    // Async Stream path backpressure window.
    private const long PauseWriterThreshold = 1024 * 1024;
    private const long ResumeWriterThreshold = 512 * 1024;

    /// <summary>
    /// Synchronously streams native output into <paramref name="destination"/>.
    /// </summary>
    /// <param name="destination">Writable stream. Flushed at the end, not disposed.</param>
    /// <param name="invoke">Calls the native writer with the given target (and checks its error).</param>
    public static void Write(Stream destination, Action<NativeWriteTarget> invoke)
    {
        ValidateStream(destination);
        ArgumentNullException.ThrowIfNull(invoke);

        Run(new StreamState(destination), invoke);
        destination.Flush();
    }

    /// <summary>
    /// Asynchronously streams native output into <paramref name="destination"/>.
    /// <para>
    /// The native writer runs on a thread-pool thread and hands chunks over a bounded queue (~1 MB);
    /// a pump copies them to the destination with <see cref="Stream.WriteAsync(ReadOnlyMemory{byte}, CancellationToken)"/>.
    /// Safe for destinations that disallow synchronous I/O (e.g. ASP.NET Core response bodies).
    /// </para>
    /// </summary>
    /// <param name="destination">Writable stream. Flushed at the end, not disposed.</param>
    /// <param name="invoke">Calls the native writer with the given target (and checks its error).</param>
    /// <param name="cancellationToken">
    /// Cancels the copy to <paramref name="destination"/> and aborts the native writer at its next write.
    /// </param>
    public static async Task WriteAsync(Stream destination, Action<NativeWriteTarget> invoke, CancellationToken cancellationToken = default)
    {
        ValidateStream(destination);
        ArgumentNullException.ThrowIfNull(invoke);
        cancellationToken.ThrowIfCancellationRequested();

        var pipe = new Pipe(new PipeOptions(
            pauseWriterThreshold: PauseWriterThreshold,
            resumeWriterThreshold: ResumeWriterThreshold,
            useSynchronizationContext: false));

        Task<Exception?> pump = PumpAsync(pipe.Reader, destination, cancellationToken);

        Exception? producerError = null;
        try
        {
            await Task.Run(() => Run(new PipeState(pipe.Writer, cancellationToken), invoke), CancellationToken.None)
                      .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            producerError = ex;
        }
        finally
        {
            await pipe.Writer.CompleteAsync(producerError).ConfigureAwait(false);
        }

        // A destination failure is the root cause of the producer failing (it sees the reader completed),
        // so it wins over the producer's error.
        Exception? destinationError = await pump.ConfigureAwait(false);
        if (destinationError is not null)
            ExceptionDispatchInfo.Throw(destinationError);
        if (producerError is not null)
            ExceptionDispatchInfo.Throw(producerError);

        await destination.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously streams native output into <paramref name="destination"/>
    /// (e.g. ASP.NET Core <c>HttpResponse.BodyWriter</c>).
    /// <para>
    /// The native writer runs on a thread-pool thread and writes directly into the pipe, blocking that
    /// thread while the pipe applies backpressure. The writer is flushed at the end but not completed.
    /// </para>
    /// </summary>
    /// <param name="destination">Destination pipe writer. Flushed at the end, not completed.</param>
    /// <param name="invoke">Calls the native writer with the given target (and checks its error).</param>
    /// <param name="cancellationToken">Cancels flushing and aborts the native writer at its next write.</param>
    public static async Task WriteAsync(PipeWriter destination, Action<NativeWriteTarget> invoke, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentNullException.ThrowIfNull(invoke);
        cancellationToken.ThrowIfCancellationRequested();

        await Task.Run(() => Run(new PipeState(destination, cancellationToken), invoke), CancellationToken.None)
                  .ConfigureAwait(false);

        FlushResult result = await destination.FlushAsync(cancellationToken).ConfigureAwait(false);
        if (result.IsCanceled)
            throw new OperationCanceledException(cancellationToken);
    }

    // ------------------------------------------------------------------

    private static void ValidateStream(Stream destination)
    {
        ArgumentNullException.ThrowIfNull(destination);
        if (!destination.CanWrite)
            throw new ArgumentException("The destination stream is not writable.", nameof(destination));
    }

    /// <summary>Copies piped data to the destination. Returns the destination's error, if any.</summary>
    private static async Task<Exception?> PumpAsync(PipeReader reader, Stream destination, CancellationToken cancellationToken)
    {
        try
        {
            while (true)
            {
                ReadResult result;
                try
                {
                    result = await reader.ReadAsync(CancellationToken.None).ConfigureAwait(false);
                }
                catch
                {
                    // The producer completed the pipe with its own error; it is reported by the caller.
                    await reader.CompleteAsync().ConfigureAwait(false);
                    return null;
                }

                var buffer = result.Buffer;
                try
                {
                    foreach (ReadOnlyMemory<byte> segment in buffer)
                        await destination.WriteAsync(segment, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    // Unblocks the producer: its next flush reports IsCompleted and it aborts the native writer.
                    await reader.CompleteAsync(ex).ConfigureAwait(false);
                    return ex;
                }

                reader.AdvanceTo(buffer.End);
                if (result.IsCompleted)
                {
                    await reader.CompleteAsync().ConfigureAwait(false);
                    return null;
                }
            }
        }
        catch (Exception ex)
        {
            await reader.CompleteAsync(ex).ConfigureAwait(false);
            return ex;
        }
    }

    private static unsafe void Run(WriteState state, Action<NativeWriteTarget> invoke)
    {
        var gch = GCHandle.Alloc(state);
        try
        {
            var target = new NativeWriteTarget(
                (nint)(delegate* unmanaged[Cdecl]<nint, byte*, nuint, int>)&OnWrite,
                GCHandle.ToIntPtr(gch),
                0);

            try
            {
                invoke(target);
            }
            catch (Exception) when (state.Error is not null)
            {
                // The native error is only a consequence of the host-side failure; report the original below.
            }

            if (state.Error is not null)
                ExceptionDispatchInfo.Throw(state.Error);
        }
        finally
        {
            // Safe: the native writer disables the callback before returning.
            gch.Free();
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static unsafe int OnWrite(nint userData, byte* data, nuint length)
    {
        WriteState? state = null;
        try
        {
            state = (WriteState)GCHandle.FromIntPtr(userData).Target!;
            if (state.Error is not null) return 1;
            if (length > int.MaxValue)
                throw new IOException($"Native write chunk of {length} bytes is too large.");

            state.Write(new ReadOnlySpan<byte>(data, (int)length));
            return 0;
        }
        catch (Exception ex)
        {
            if (state is not null) state.Error ??= ex;
            return 1;
        }
    }

    private abstract class WriteState
    {
        public Exception? Error;
        public abstract void Write(ReadOnlySpan<byte> data);
    }

    private sealed class StreamState(Stream stream) : WriteState
    {
        public override void Write(ReadOnlySpan<byte> data) => stream.Write(data);
    }

    private sealed class PipeState(PipeWriter writer, CancellationToken cancellationToken) : WriteState
    {
        public override void Write(ReadOnlySpan<byte> data)
        {
            cancellationToken.ThrowIfCancellationRequested();

            while (!data.IsEmpty)
            {
                Span<byte> span = writer.GetSpan(data.Length);
                int n = Math.Min(span.Length, data.Length);
                data[..n].CopyTo(span);
                writer.Advance(n);
                data = data[n..];
            }

            // Blocks this (native / thread-pool) thread while the pipe is over its pause threshold.
            ValueTask<FlushResult> flush = writer.FlushAsync(cancellationToken);
            FlushResult result = flush.IsCompletedSuccessfully
                ? flush.Result
                : flush.AsTask().GetAwaiter().GetResult();

            if (result.IsCanceled)
                throw new OperationCanceledException(cancellationToken);
            if (result.IsCompleted)
                throw new IOException("The destination stopped accepting data.");
        }
    }
}
