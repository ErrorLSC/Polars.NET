namespace Polars.FSharp

open Polars.NET.Core

[<AutoOpen>]
module IPCStream =
    type DataFrame with 
        /// <summary>
        /// Read into a DataFrame from Arrow IPC record batch stream.
        /// </summary>
        /// <param name="path">Path to the IPC stream file or a byte buffer</param>
        /// <param name="columns">Columns to select.</param>
        /// <param name="projection">Column indices to select.</param>
        /// <param name="nRows">Stop reading from IPC stream after reading n rows</param>
        /// <param name="rowIndexName">Insert a row index column with the given name into the DataFrame as the first column. 
        /// If set to None (default), no row index column is created.</param>
        /// <param name="rowIndexOffset">Start the row index at this offset.Only used if row_index_name is set.</param>
        /// <param name="rechunk">Make sure that all data is contiguous.</param>
        static member ReadIpcStream(
            path:string,?columns:seq<string>,?projection:seq<uint>,?nRows:uint64,?rowIndexName:string,?rowIndexOffset,?rechunk
        ):DataFrame =
            let col = match columns with Some s -> Seq.toArray s | None -> null
            let pro = 
                match projection with
                    | Some p -> p |> Seq.toArray 
                    | None -> null
            let rows = Option.toNullable nRows
            let index = defaultArg rowIndexName null
            let offset = defaultArg rowIndexOffset 0u
            let re = defaultArg rechunk true
            let h = PolarsWrapper.ReadIpcStream(
                path,col,pro,rows,index,offset,re)
            new DataFrame(h)
        /// <summary>
        /// Read into a DataFrame from Arrow IPC record batch stream.
        /// </summary>
        /// <param name="path">Path to the IPC stream file or a byte buffer</param>
        /// <param name="columns">Columns to select.</param>
        /// <param name="projection">Column indices to select.</param>
        /// <param name="nRows">Stop reading from IPC stream after reading n rows</param>
        /// <param name="rowIndexName">Insert a row index column with the given name into the DataFrame as the first column. 
        /// If set to None (default), no row index column is created.</param>
        /// <param name="rowIndexOffset">Start the row index at this offset.Only used if row_index_name is set.</param>
        /// <param name="rechunk">Make sure that all data is contiguous.</param>
        static member ReadIpcStream(
            buffer: System.ReadOnlySpan<byte>, 
            ?columns: seq<string>, 
            ?projection: seq<uint>, 
            ?nRows: uint64, 
            ?rowIndexName: string, 
            ?rowIndexOffset: uint32, 
            ?rechunk: bool
        ) : DataFrame =
            let col = match columns with Some s -> Seq.toArray s | None -> null
            let pro = 
                match projection with
                    | Some p -> p |> Seq.toArray 
                    | None -> null
            let rows = Option.toNullable nRows
            let index = defaultArg rowIndexName null
            let offset = defaultArg rowIndexOffset 0u
            let re = defaultArg rechunk true
            
            let h = PolarsWrapper.ReadIpcStream(buffer, col, pro, rows, index, offset, re)
            new DataFrame(h)
        /// <summary>
        /// Get the schema of an IPC file without reading data.
        /// </summary>
        static member ReadIpcStreamSchema(path:string):PolarsSchema =
            new PolarsSchema(PolarsWrapper.ReadIpcStreamSchema(path))
        /// <summary>
        /// Get the schema of an IPC file without reading data.
        /// </summary>
        static member ReadIpcStreamSchema(buffer:System.ReadOnlySpan<byte>):PolarsSchema =
            new PolarsSchema(PolarsWrapper.ReadIpcStreamSchema(buffer))
        /// <summary>
        /// Write to Arrow IPC record batch stream.
        /// </summary>
        /// <param name="path">Path to the IPC stream file or a byte buffer</param>
        /// <param name="compression">Compression method. Defaults to None.</param>
        /// <param name="compatLevel">Use a specific compatibility level 
        /// when exporting Polars’ internal data structures.</param>
        member this.WriteIpcStream(path,compression,compatLevel) =
            let com = defaultArg compression IpcCompression.NoCompression |> _.ToNative()
            let cop = defaultArg compatLevel -1
            PolarsWrapper.WriteIpcStream(this.Handle,path,com,cop)
        /// <summary>
        /// Write to Arrow IPC record batch stream.
        /// </summary>
        /// <param name="path">Path to the IPC stream file or a byte buffer</param>
        /// <param name="compression">Compression method. Defaults to None.</param>
        /// <param name="compatLevel">Use a specific compatibility level 
        /// when exporting Polars’ internal data structures.</param>
        member this.WriteIpcStreamMemory(compression,compatLevel) =
            let com = defaultArg compression IpcCompression.NoCompression |> _.ToNative()
            let cop = defaultArg compatLevel -1
            PolarsWrapper.WriteIpcStreamMemory(this.Handle,com,cop)
