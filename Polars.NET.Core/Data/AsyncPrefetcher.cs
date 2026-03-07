using System.Threading.Channels;
using Apache.Arrow;

namespace Polars.NET.Core.Data;

/// <summary>
/// For RecordBatch DataFlow, enable background async prefetch
/// </summary>
public static class PrefetchExtensions
{
    public static IEnumerable<RecordBatch> Prefetch(this IEnumerable<RecordBatch> source, int? bufferSize = null)
    {
        int actualSize = bufferSize ?? PolarsNetConfig.DefaultPrefetchBufferSize;
        
        return new PrefetchingEnumerable(source, actualSize);
    }
    private sealed class PrefetchingEnumerable(IEnumerable<RecordBatch> source, int bufferSize) : IEnumerable<RecordBatch>
    {
        public IEnumerator<RecordBatch> GetEnumerator() => new PrefetchingEnumerator(source.GetEnumerator(), bufferSize);
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private sealed class PrefetchingEnumerator : IEnumerator<RecordBatch>
    {
        private readonly IEnumerator<RecordBatch> _source;
        private readonly Channel<RecordBatch> _channel;
        private readonly CancellationTokenSource _cts;
        private readonly Task _producerTask;
        
        private RecordBatch? _current;
        private Exception? _backgroundException;
        private bool _isDisposed;

        public PrefetchingEnumerator(IEnumerator<RecordBatch> source, int bufferSize)
        {
            _source = source; 
            _cts = new CancellationTokenSource();
            _channel = Channel.CreateBounded<RecordBatch>(new BoundedChannelOptions(bufferSize)
            {
                SingleWriter = true, 
                SingleReader = true, 
                FullMode = BoundedChannelFullMode.Wait
            });

            _producerTask = Task.Factory.StartNew(
                ProducerLoopAsync, 
                _cts.Token, 
                TaskCreationOptions.LongRunning, 
                TaskScheduler.Default).Unwrap();
        }

        private async Task ProducerLoopAsync()
        {
            try
            {
                while (_source.MoveNext())
                {
                    await _channel.Writer.WriteAsync(_source.Current, _cts.Token);
                }
            }
            catch (OperationCanceledException) { /* Normal Cancellation */ }
            catch (Exception ex) { _backgroundException = ex; }
            finally { _channel.Writer.Complete(); }
        }

        public bool MoveNext()
        {
            ObjectDisposedException.ThrowIf(_isDisposed, this);
            if (_backgroundException != null) 
                throw new InvalidOperationException("Prefetch Pipeline Crashed.", _backgroundException);

            _current?.Dispose();
            _current = null;

            bool canRead;
            try 
            {
                var wait = _channel.Reader.WaitToReadAsync(_cts.Token);
                
                if (wait.IsCompleted)
                {
                    // Fast Path
                    canRead = wait.Result; 
                }
                else
                {
                    // Slow Path
                    canRead = wait.AsTask().GetAwaiter().GetResult();
                }
            } 
            catch (OperationCanceledException) 
            { 
                return false; 
            }

            if (canRead && _channel.Reader.TryRead(out var batch))
            {
                _current = batch;
                return true;
            }

            if (_backgroundException != null) 
                throw new InvalidOperationException("Prefetch Pipeline Crashed.", _backgroundException);
            
            return false;
        }

        public RecordBatch Current => _current ?? throw new InvalidOperationException("No current batch.");
        object System.Collections.IEnumerator.Current => Current;
        public void Reset() => throw new NotSupportedException();

        public void Dispose()
        {
            if (_isDisposed) return;
            _isDisposed = true;

            // Cancel pipeline
            _cts.Cancel(); 
            
            // Dispose current batch
            _current?.Dispose();
            
            // Dispose other batch
            while (_channel.Reader.TryRead(out var batch)) 
            {
                batch.Dispose();
            }
            
            // Release source
            try 
            { 
                _source.Dispose(); 
            } 
            catch 
            { 
                // catch potential exceptions to make sure dispose be done
            }
            
            _cts.Dispose();
        }
    }
}