namespace Polars.FSharp

open Polars.NET.Core

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
        /// <returns>A new DataFrame.</returns>
        static member ReadAvro(
            path: string, 
            ?nRows: uint64, 
            ?columns: seq<string>, 
            ?projection: seq<int>
        ) =
            let pNRows = Option.toNullable nRows
            let pCols = match columns with Some c -> Seq.toArray c | None -> null
            let pProj = match projection with Some p -> Seq.toArray p | None -> null
            
            let handle = PolarsWrapper.ReadAvro(path, pNRows, pCols, pProj)
            new DataFrame(handle)

        /// <summary>
        /// Read an Avro memory buffer into a DataFrame.
        /// </summary>
        /// <param name="buffer">The byte array containing Avro data.</param>
        /// <param name="nRows">Stop reading when `nRows` are read.</param>
        /// <param name="columns">Columns to select/project by name.</param>
        /// <param name="projection">Columns to select/project by index.</param>
        /// <returns>A new DataFrame.</returns>
        static member ReadAvro(
            buffer: byte[], 
            ?nRows: uint64, 
            ?columns: seq<string>, 
            ?projection: seq<int>
        ) =
            let pNRows = Option.toNullable nRows
            let pCols = match columns with Some c -> Seq.toArray c | None -> null
            let pProj = match projection with Some p -> Seq.toArray p | None -> null
            
            let handle = PolarsWrapper.ReadAvro(buffer, pNRows, pCols, pProj)
            new DataFrame(handle)

        /// <summary>
        /// Read an Avro memory Stream into a DataFrame.
        /// </summary>
        /// <param name="stream">The stream containing Avro data.</param>
        /// <param name="nRows">Stop reading when `nRows` are read.</param>
        /// <param name="columns">Columns to select/project by name.</param>
        /// <param name="projection">Columns to select/project by index.</param>
        /// <returns>A new DataFrame.</returns>
        static member ReadAvro(
            stream: System.IO.Stream, 
            ?nRows: uint64, 
            ?columns: seq<string>, 
            ?projection: seq<int>
        ) =
            use ms = new System.IO.MemoryStream()
            stream.CopyTo(ms)
            
            let pNRows = Option.toNullable nRows
            let pCols = match columns with Some c -> Seq.toArray c | None -> null
            let pProj = match projection with Some p -> Seq.toArray p | None -> null
            
            let handle = PolarsWrapper.ReadAvro(ms.ToArray(), pNRows, pCols, pProj)
            new DataFrame(handle)
        /// <summary>
        /// Write the DataFrame to an Apache Avro file.
        /// </summary>
        /// <param name="path">The file path to write to.</param>
        /// <param name="compression">The compression algorithm to use.</param>
        /// <param name="name">The name of the Avro record.</param>
        member this.WriteAvro(
            path: string, 
            ?compression: AvroCompression, 
            ?name: string
        ) =
            let pComp = defaultArg compression AvroCompression.Uncompressed
            let pName = defaultArg name ""
            PolarsWrapper.WriteAvro(this.Handle, path, pComp.ToNative(), pName)

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