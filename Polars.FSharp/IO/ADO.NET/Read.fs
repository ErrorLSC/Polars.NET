namespace Polars.FSharp

[<AutoOpen>]
module DataReaderRead =
    open System
    open Apache.Arrow
    open System.Collections.Generic
    open Polars.NET.Core.Arrow
    open Polars.NET.Core.Data
    open System.Data
    type LazyFrame with
        /// <summary>
        /// Lazily scan a sequence of objects using Apache Arrow Stream Interface.
        /// This supports predicate pushdown and streaming execution.
        /// Data is pulled from the sequence only when needed.
        /// </summary>
        /// <param name="data">The data source sequence.</param>
        /// <param name="batchSize">Rows per Arrow batch (default: 100,000).</param>
        /// <param name="useBuffered">Choose whether disk buffer file needed (for big data) <param>
        static member scanSeq<'T>(data: seq<'T>, ?batchSize: int, ?useBuffered: bool) : LazyFrame =
                let size = defaultArg batchSize 100_000
                let buffered = defaultArg useBuffered false

                // =========================================================
                // 1. Buffered Mode (Disk IPC)
                // =========================================================
                if buffered then
                    let scope = new IpcStreamService.TempIpcScope<'T>(data, size)
                    
                    // Get FileHandle
                    let handle = LazyFrame.ScanIpc(scope.FilePath).Handle
                    
                    { new LazyFrame(handle) with
                        member this.Dispose() =
                            base.Dispose()
                            scope.Dispose()
                    }

                // =========================================================
                // 2. Streaming Mode (Memory Safety & Lazy Fallback)
                // =========================================================
                else
                    let schema = ArrowConverter.GetSchemaFromType<'T>()

                    let streamFactory = Func<IEnumerable<RecordBatch>>(fun () ->
                        seq {
                            let mutable hasYielded = false
                            
                            let batches = ArrowConverter.ToArrowBatches(data, size).Prefetch()

                            for batch in batches do
                                hasYielded <- true
                                yield batch
                                batch.Dispose()
                            
                            if not hasYielded then
                                let emptyBatch = ArrowConverter.GetEmptyBatch<'T>()
                                yield emptyBatch
                                emptyBatch.Dispose()
                        }
                    )

                    let handle = ArrowStreamInterop.ScanStream(streamFactory, schema)
                    new LazyFrame(handle)

        /// <summary>
        /// Scan a database query lazily.
        /// Requires a factory function to create new IDataReaders for potential multi-pass scans.
        /// </summary>
        static member scanDb(readerFactory: unit -> IDataReader, ?batchSize: int, ?useBuffered: bool) : LazyFrame =
            let size = defaultArg batchSize 50_000
            let buffered = defaultArg useBuffered false

            // =========================================================
            // 1. Buffered Mode (Disk IPC)
            // =========================================================
            if buffered then
                let runBuffer () =
                    use reader = readerFactory()
                    new IpcStreamService.TempIpcScopeReader(reader, size)

                let scope = runBuffer()
                let handle = LazyFrame.ScanIpc(scope.FilePath).Handle

                { new LazyFrame(handle) with
                    member this.Dispose() =
                        base.Dispose()
                        scope.Dispose()
                }

            // =========================================================
            // 2. Streaming Mode (Memory)
            // =========================================================
            else
                // Probe Schema
                let schema = 
                    use reader = readerFactory()
                    ArrowTypeResolver.GetSchemaFromDataReader reader

                // Stream Factory
                let factory = Func<IEnumerable<RecordBatch>>(fun () ->
                    seq {
                        use reader = readerFactory()
                        let batches = DbToArrowStream.ToArrowBatches(reader, size).Prefetch()
                        
                        for batch in batches do
                            yield batch
                            batch.Dispose()
                    }
                )

                let handle = ArrowStreamInterop.ScanStream(factory, schema)
                new LazyFrame(handle)
        /// <summary>
        /// [Lazy][Streaming] Scan a database DataReader directly.
        /// <para>Upgraded to pure memory Streaming Mode using Arrow C-Data FFI!</para>
        /// </summary>
        static member scanDb(reader: IDataReader, ?batchSize: int) : LazyFrame =
            let size = defaultArg batchSize 50_000
            let schema = reader.GetArrowSchema()
            
            let stream = reader.ToArrowBatches(size).Prefetch() 
            
            let factory = Func<IEnumerable<RecordBatch>>(fun () -> stream)
            let handle = ArrowStreamInterop.ScanStream(factory, schema)
            
            new LazyFrame(handle)
    type DataFrame with
        /// <summary>
        /// [Eager] Create a DataFrame from an IDataReader (e.g. SqlDataReader).
        /// <para>
        /// Uses high-performance streaming ingestion via Apache Arrow.
        /// </para>
        /// </summary>
        /// <param name="reader">The open IDataReader instance.</param>
        /// <param name="batchSize">Number of rows per Arrow batch (default 50,000).</param>
        static member ReadDb(reader: IDataReader, ?batchSize: int) : DataFrame =
            let schema = reader.GetArrowSchema()

            let size = defaultArg batchSize 50_000
            
            let batchStream = reader.ToArrowBatches(size).Prefetch()
            
            let handle = ArrowStreamInterop.ImportEager(batchStream,schema)
            
            if handle.IsInvalid then

                let emptyBatch = new RecordBatch(schema, System.Array.Empty<IArrowArray>(), 0)

                let safeHandle = ArrowFfiBridge.ImportDataFrame emptyBatch
                new DataFrame(safeHandle)
            else
                new DataFrame(handle)