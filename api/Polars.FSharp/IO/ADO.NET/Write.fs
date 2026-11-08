namespace Polars.FSharp

[<AutoOpen>]
module DataReaderWrite =
    open System
    open System.Collections.Generic
    open System.Data
    open System.Collections.Concurrent
    open System.Threading.Tasks
    open Apache.Arrow
    open Polars.NET.Core.Data
    open System.Threading.Channels
    open System.Data.Common
    open System.Threading
    type LazyFrame with
        /// <summary>
        /// Stream query results directly to a database or other IDataReader consumer.
        /// Uses a producer-consumer pattern with bounded capacity for memory efficiency.
        /// </summary>
        /// <param name="writerAction">Callback to consume the IDataReader (e.g., using SqlBulkCopy).</param>
        /// <param name="bufferSize">Max number of batches to buffer in memory (default: 5).</param>
        /// <param name="typeOverrides">Force specific C# types for columns (e.g. map Date32 to DateTime).</param>
        member this.SinkTo(writerAction: Action<IDataReader>, ?bufferSize: int, ?typeOverrides: IDictionary<string, Type>) : unit =
            let capacity = defaultArg bufferSize 5
            
            use buffer = new BlockingCollection<RecordBatch>(boundedCapacity = capacity)

            let consumerTask = Task.Run(fun () ->
                let stream = buffer.GetConsumingEnumerable()
                
                let overrides = 
                        match typeOverrides with 
                        | Some d -> new Dictionary<string, Type>(d) 
                        | None -> null
                
                use reader = new ArrowToDbStream(stream, overrides)
                
                writerAction.Invoke reader
            )

            try
                try
                    this.SinkBatches(fun batch -> buffer.Add batch)
                finally
                    buffer.CompleteAdding()
            with
            | _ -> 
                reraise()

            try
                consumerTask.Wait()
            with
            | :? AggregateException as aggEx ->
                raise (aggEx.Flatten().InnerException)
    type DataFrame with
       /// <summary>
        /// Stream the DataFrame directly to a database or other IDataReader consumer.
        /// <para>
        /// Uses a producer-consumer pattern. This method blocks until the consumer finishes reading.
        /// Ideal for <c>SqlBulkCopy.WriteToServer</c> or <c>NpgsqlBinaryImporter</c>.
        /// </para>
        /// </summary>
        /// <param name="writerAction">Callback that receives an IDataReader.</param>
        /// <param name="bufferSize">Max number of batches to buffer in memory (default: 5).</param>
        /// <param name="typeOverrides">Dictionary to force specific C# types for columns (optional).</param>
        member this.WriteTo(writerAction: Action<IDataReader>, ?bufferSize: int, ?typeOverrides: IDictionary<string, Type>) : unit =
            let capacity = defaultArg bufferSize 5
            
            use buffer = new BlockingCollection<RecordBatch>(capacity)

            let consumerTask = Task.Run(fun () ->
                let stream = buffer.GetConsumingEnumerable()
                
                let overrides = 
                    match typeOverrides with 
                    | Some d -> new Dictionary<string, Type>(d) 
                    | None -> null
                
                use reader = new ArrowToDbStream(stream, overrides)
                
                writerAction.Invoke reader
            )

            try
                try
                    this.ExportBatches(fun batch -> buffer.Add batch)
                finally
                    buffer.CompleteAdding()
            with
            | _ -> 
                reraise()

            try
                consumerTask.Wait()
            with
            | :? AggregateException as aggEx ->
                raise (aggEx.Flatten().InnerException)
        /// <summary>
        /// Export DataFrame As DbDataReader (Zero-Copy Enabled)
        /// </summary>
        member this.AsDataReader(?bufferSize: int, ?typeOverrides: Dictionary<string, Type>) : DbDataReader =

            let bufferSize = defaultArg bufferSize 5
            let overrides = defaultArg typeOverrides null

            let options = BoundedChannelOptions(bufferSize, 
                FullMode = BoundedChannelFullMode.Wait,
                SingleWriter = true,
                SingleReader = true
            )

            let channel = Channel.CreateBounded<RecordBatch> options
            let (cts: Threading.CancellationTokenSource) = new CancellationTokenSource()

            let producerTask = Task.Run(fun () ->
                try
                    this.ExportBatches(fun batch ->
                        channel.Writer.WriteAsync(batch, cts.Token).AsTask().Wait cts.Token
                    )
                    channel.Writer.Complete()
                with
                | :? OperationCanceledException -> 
                    channel.Writer.Complete()
                | ex -> 
                    channel.Writer.Complete ex
            )

            let stream = channel.Reader.ReadAllAsync(cts.Token).ToBlockingEnumerable cts.Token
            
            let innerReader = new ArrowToDbStream(stream, overrides)

            new PolarsDataReader(innerReader, cts, producerTask) :> DbDataReader