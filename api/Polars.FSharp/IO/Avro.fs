namespace Polars.FSharp

open System.IO
open System.IO.Pipelines
open System.Threading
open System.Threading.Tasks
open Polars.NET.Core.Wrappers

[<AutoOpen>]
module Avro =
    type DataFrame with
        /// <summary>
        /// Read an Avro file into a DataFrame.
        /// </summary>
        /// <param name="path">The path to the Avro file.</param>
        /// <param name="nRows">Stop reading when `nRows` are read.</param>
        /// <param name="columns">Columns to select/project by name.</param>
        /// <param name="projection">Columns to select/project by index.</param>
        /// <param name="paralleling">Decode the file's blocks on several threads (row order is kept). Default true.</param>
        /// <returns>A new DataFrame.</returns>
        static member ReadAvro(
            path: string, 
            ?nRows: uint64, 
            ?columns: seq<string>, 
            ?projection: seq<int>,
            ?paralleling: bool
        ) =
            let pNRows = Option.toNullable nRows
            let pCols = match columns with Some c -> Seq.toArray c | None -> null
            let pProj = match projection with Some p -> Seq.toArray p | None -> null
            new DataFrame(PolarsWrapper.ReadAvro(path, pNRows, pCols, pProj, defaultArg paralleling true))

        /// <summary>
        /// Read an Avro memory buffer into a DataFrame.
        /// </summary>
        /// <param name="buffer">The byte array containing Avro data.</param>
        /// <param name="nRows">Stop reading when `nRows` are read.</param>
        /// <param name="columns">Columns to select/project by name.</param>
        /// <param name="projection">Columns to select/project by index.</param>
        /// <param name="paralleling">Decode the file's blocks on several threads (row order is kept). Default true.</param>
        /// <returns>A new DataFrame.</returns>
        static member ReadAvro(
            buffer: byte[], 
            ?nRows: uint64, 
            ?columns: seq<string>, 
            ?projection: seq<int>,
            ?paralleling: bool
        ) =
            let pNRows = Option.toNullable nRows
            let pCols = match columns with Some c -> Seq.toArray c | None -> null
            let pProj = match projection with Some p -> Seq.toArray p | None -> null
            new DataFrame(PolarsWrapper.ReadAvro(System.ReadOnlySpan<byte>(buffer), pNRows, pCols, pProj, defaultArg paralleling true))

        /// <summary>
        /// Read Avro data from a stream into a DataFrame. The stream is read block by block and left open.
        /// </summary>
        /// <param name="stream">The stream containing Avro data.</param>
        /// <param name="nRows">Stop reading when `nRows` are read.</param>
        /// <param name="columns">Columns to select/project by name.</param>
        /// <param name="projection">Columns to select/project by index.</param>
        /// <param name="paralleling">Decode the file's blocks on several threads (row order is kept). Default true.</param>
        /// <returns>A new DataFrame.</returns>
        static member ReadAvro(
            stream: System.IO.Stream, 
            ?nRows: uint64, 
            ?columns: seq<string>, 
            ?projection: seq<int>,
            ?paralleling: bool
        ) =
            let pNRows = Option.toNullable nRows
            let pCols = match columns with Some c -> Seq.toArray c | None -> null
            let pProj = match projection with Some p -> Seq.toArray p | None -> null
            new DataFrame(PolarsWrapper.ReadAvro(stream, pNRows, pCols, pProj, defaultArg paralleling true))
        /// <summary>
        /// Write the DataFrame to an Apache Avro file.
        /// </summary>
        /// <param name="path">The file path to write to.</param>
        /// <param name="compression">The compression algorithm to use.</param>
        /// <param name="name">The name of the Avro record.</param>
        /// <returns>The same DataFrame, for chaining.</returns>
        member this.WriteAvro(
            path: string, 
            ?compression: AvroCompression, 
            ?name: string
        ) =
            let pComp = defaultArg compression AvroCompression.Uncompressed
            let pName = defaultArg name ""
            PolarsWrapper.WriteAvro(this.Handle, path, pComp.ToNative(), pName)
            this

        /// <summary>
        /// Write the DataFrame to an Apache Avro format written into a <see cref="T:System.IO.Stream"/>.
        /// Output is written block by block as the frame is exported. The stream is flushed but not disposed.
        /// </summary>
        /// <param name="stream">Writable destination stream.</param>
        /// <param name="compression">The compression algorithm to use.</param>
        /// <param name="name">The name of the Avro record.</param>
        /// <returns>The same DataFrame, for chaining.</returns>
        member this.WriteAvro(
            stream: Stream,
            ?compression: AvroCompression,
            ?name: string
        ) : DataFrame =
            let pComp = defaultArg compression AvroCompression.Uncompressed
            let pName = defaultArg name ""
            PolarsWrapper.WriteAvro(this.Handle, stream, pComp.ToNative(), pName) // completing the file flushes the stream
            this

        /// <summary>
        /// Asynchronously write the DataFrame to an Apache Avro format written into a <see cref="T:System.IO.Stream"/>.
        /// Safe for destinations that disallow synchronous writes (e.g. ASP.NET Core response bodies).
        /// The stream is flushed but not disposed.
        /// </summary>
        member this.WriteAvroAsync(
            stream: Stream,
            ?compression: AvroCompression,
            ?name: string,
            ?cancellationToken: CancellationToken
        ) : Task =
            let pComp = defaultArg compression AvroCompression.Uncompressed
            let pName = defaultArg name ""
            let ct = defaultArg cancellationToken CancellationToken.None
            PolarsWrapper.WriteAvroAsync(this.Handle, stream, pComp.ToNative(), pName, ct)

        /// <summary>
        /// Asynchronously write the DataFrame to an Apache Avro format written into a <see cref="T:System.IO.Pipelines.PipeWriter"/>.
        /// The writer is flushed but not completed.
        /// </summary>
        member this.WriteAvroAsync(
            writer: PipeWriter,
            ?compression: AvroCompression,
            ?name: string,
            ?cancellationToken: CancellationToken
        ) : Task =
            let pComp = defaultArg compression AvroCompression.Uncompressed
            let pName = defaultArg name ""
            let ct = defaultArg cancellationToken CancellationToken.None
            PolarsWrapper.WriteAvroAsync(this.Handle, writer.AsStream(true), pComp.ToNative(), pName, ct)

        /// <summary>
        /// Write the DataFrame to an Apache Avro memory buffer.
        /// </summary>
        /// <param name="compression">The compression algorithm to use.</param>
        /// <param name="name">The name of the Avro record.</param>
        /// <returns>A byte array containing the Avro data.</returns>
        member this.WriteAvroMemory(
            ?compression: AvroCompression, 
            ?name: string
        ) : byte[] =
            let pComp = defaultArg compression AvroCompression.Uncompressed
            let pName = defaultArg name ""
            PolarsWrapper.WriteAvroToMemory(this.Handle, pComp.ToNative(), pName)

        /// <summary>
        /// Write the DataFrame to an Apache Avro memory buffer in native memory, without copying into the managed heap.
        /// </summary>
        /// <param name="compression">The compression algorithm to use.</param>
        /// <param name="name">The name of the Avro record.</param>
        /// <returns>A <see cref="T:Polars.NET.Core.NativeBuffer"/> owning the Avro data. Dispose it to release the native memory.</returns>
        member this.WriteAvroNativeMemory(
            ?compression: AvroCompression,
            ?name: string
        ) : Polars.NET.Core.NativeBuffer =
            let pComp = defaultArg compression AvroCompression.Uncompressed
            let pName = defaultArg name ""
            PolarsWrapper.WriteAvroNativeMemory(this.Handle, pComp.ToNative(), pName)

    type LazyFrame with
        /// <summary>
        /// Lazily scan an Avro file. The header is read now for the schema; the data is streamed block by block each
        /// time the query runs.
        /// </summary>
        /// <param name="path">The path to the Avro file.</param>
        /// <param name="nRows">Stop reading when `nRows` are read.</param>
        /// <param name="columns">Columns to select/project by name.</param>
        /// <param name="projection">Columns to select/project by index.</param>
        /// <param name="paralleling">Decode the file's blocks on several threads (row order is kept). Default true.</param>
        static member ScanAvro(
            path: string,
            ?nRows: uint64,
            ?columns: seq<string>,
            ?projection: seq<int>,
            ?paralleling: bool
        ) : LazyFrame =
            let pNRows = Option.toNullable nRows
            let pCols = match columns with Some c -> Seq.toArray c | None -> null
            let pProj = match projection with Some p -> Seq.toArray p | None -> null
            new LazyFrame(PolarsWrapper.ScanAvro(path, pNRows, pCols, pProj, defaultArg paralleling true))

        /// <summary>
        /// Run the query on the streaming engine and write the result as Avro into a stream as batches arrive.
        /// The stream is flushed but not disposed.
        /// </summary>
        member this.SinkAvro(stream: Stream, ?compression: AvroCompression, ?name: string) : unit =
            let pComp = defaultArg compression AvroCompression.Uncompressed
            use sink = new PolarsWrapper.AvroBatchSink(stream, pComp.ToNative(), defaultArg name "")
            let newHandle = PolarsWrapper.SinkBatches(this.CloneHandle(), System.Action<Apache.Arrow.RecordBatch>(sink.Write))
            use lfRes = new LazyFrame(newHandle)
            use _ = lfRes.Collect(Engine.Streaming)
            sink.Complete(this.Handle) // completing the file flushes the stream

        /// <summary>
        /// Run the query on the streaming engine and write the result to an Avro file as batches arrive
        /// (a failure deletes the partial file).
        /// </summary>
        member this.SinkAvro(path: string, ?compression: AvroCompression, ?name: string) : unit =
            try
                use stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None)
                this.SinkAvro(stream, defaultArg compression AvroCompression.Uncompressed, defaultArg name "")
            with _ ->
                File.Delete path
                reraise ()
