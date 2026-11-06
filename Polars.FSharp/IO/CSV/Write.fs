namespace Polars.FSharp

open System.IO
open System.IO.Pipelines
open System.Threading
open System.Threading.Tasks
open Polars.NET.Core
open Polars.NET.Core.Wrappers

[<AutoOpen>]
module CsvWrite =
    type LazyFrame with
        /// <summary>
        /// Execute the LazyFrame and sink the result to a CSV file.
        /// <para>
        /// This operation allows processing datasets larger than memory by streaming results
        /// directly to the file system.
        /// </para>
        /// </summary>
        member this.SinkCsv
            (
                path: string,
                ?includeHeader: bool,
                ?includeBom: bool,
                ?separator: char,
                ?quoteChar: char,
                ?quoteStyle: QuoteStyle,
                ?nullValue: string,
                ?lineTerminator: string,
                ?floatScientific: bool,
                ?floatPrecision: int,
                ?decimalComma: bool,
                ?dateFormat: string,
                ?timeFormat: string,
                ?datetimeFormat: string,
                ?checkExtension: bool,
                ?compression: ExternalCompression,
                ?compressionLevel: int,
                ?maintainOrder: bool,
                ?syncOnClose: SyncOnClose,
                ?mkdir: bool,
                ?batchSize: int,
                ?cloudOptions: CloudOptions
            ) =
            // 1. Resolve Defaults
            let incHdr = defaultArg includeHeader true
            let incBom = defaultArg includeBom false
            let sep = defaultArg separator ','
            let qChar = defaultArg quoteChar '"'
            let qStyle = defaultArg quoteStyle QuoteStyle.Necessary
            let nVal = defaultArg nullValue null
            let lTerm = defaultArg lineTerminator "\n"
            let fSci = Option.toNullable floatScientific
            let fPrec = Option.toNullable floatPrecision
            let dComma = defaultArg decimalComma false
            let dFmt = defaultArg dateFormat null
            let tFmt = defaultArg timeFormat null
            let dtFmt = defaultArg datetimeFormat null
            let chkExt = defaultArg checkExtension true
            let comp = defaultArg compression ExternalCompression.Uncompressed
            let compLvl = defaultArg compressionLevel -1
            let mo = defaultArg maintainOrder true
            let sync = defaultArg syncOnClose SyncOnClose.NoSync
            let mkd = defaultArg mkdir false
            let bSize = defaultArg batchSize 0

            // 2. Unpack Cloud Options via external static helper
            let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals =
                CloudOptions.ParseCloudOptions cloudOptions

            // 3. Native Call
            PolarsWrapper.SinkCsv(
                this.CloneHandle(),
                path,
                incHdr,
                incBom,
                bSize,
                chkExt,
                comp.ToNative(),
                compLvl,
                sep,
                qChar,
                qStyle.ToNative(),
                nVal,
                lTerm,
                dFmt,
                tFmt,
                dtFmt,
                fSci,
                fPrec,
                dComma,
                mo,
                sync.ToNative(),
                mkd,
                cProv,
                cRet,
                cToMs,
                cInitMs,
                cMaxMs,
                cCache,
                cKeys,
                cVals
            )

        /// <summary>
        /// Execute the LazyFrame and sink the result to a CSV file, partitioned by the given selector.
        /// </summary>
        member this.SinkCsvPartitioned
            (
                path: string,
                partitionBy: Selector,
                ?includeKeys: bool,
                ?keysPreGrouped: bool,
                ?maxRowsPerFile: int,
                ?approxBytesPerFile: int64,
                ?includeHeader: bool,
                ?includeBom: bool,
                ?separator: char,
                ?quoteChar: char,
                ?quoteStyle: QuoteStyle,
                ?nullValue: string,
                ?lineTerminator: string,
                ?floatScientific: bool,
                ?floatPrecision: int,
                ?decimalComma: bool,
                ?dateFormat: string,
                ?timeFormat: string,
                ?datetimeFormat: string,
                ?checkExtension: bool,
                ?compression: ExternalCompression,
                ?compressionLevel: int,
                ?maintainOrder: bool,
                ?syncOnClose: SyncOnClose,
                ?mkdir: bool,
                ?batchSize: int,
                ?cloudOptions: CloudOptions
            ) =
            // 1. Resolve Defaults & Limits
            let incKeys = defaultArg includeKeys true
            let preGrouped = defaultArg keysPreGrouped false
            let maxRows = defaultArg maxRowsPerFile 0
            let approxBytes = defaultArg approxBytesPerFile 0L

            let maxRowsNuint = if maxRows > 0 then unativeint maxRows else 0un
            let approxBytesUlong = if approxBytes > 0L then uint64 approxBytes else 0UL

            let incHdr = defaultArg includeHeader true
            let incBom = defaultArg includeBom false
            let sep = defaultArg separator ','
            let qChar = defaultArg quoteChar '"'
            let qStyle = defaultArg quoteStyle QuoteStyle.Necessary
            let nVal = defaultArg nullValue null
            let lTerm = defaultArg lineTerminator "\n"
            let fSci = Option.toNullable floatScientific
            let fPrec = Option.toNullable floatPrecision
            let dComma = defaultArg decimalComma false
            let dFmt = defaultArg dateFormat null
            let tFmt = defaultArg timeFormat null
            let dtFmt = defaultArg datetimeFormat null
            let chkExt = defaultArg checkExtension true
            let comp = defaultArg compression ExternalCompression.Uncompressed
            let compLvl = defaultArg compressionLevel -1
            let mo = defaultArg maintainOrder true
            let sync = defaultArg syncOnClose SyncOnClose.NoSync
            let mkd = defaultArg mkdir false
            let bSize = defaultArg batchSize 0

            // 2. Unpack Cloud Options
            let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals =
                CloudOptions.ParseCloudOptions cloudOptions

            // 3. Native Call
            PolarsWrapper.SinkCsvPartitioned(
                this.CloneHandle(),
                path,
                partitionBy.CloneHandle(),
                incKeys,
                preGrouped,
                maxRowsNuint,
                approxBytesUlong,
                incHdr,
                incBom,
                bSize,
                chkExt,
                comp.ToNative(),
                compLvl,
                sep,
                qChar,
                qStyle.ToNative(),
                nVal,
                lTerm,
                dFmt,
                tFmt,
                dtFmt,
                fSci,
                fPrec,
                dComma,
                mo,
                sync.ToNative(),
                mkd,
                cProv,
                cRet,
                cToMs,
                cInitMs,
                cMaxMs,
                cCache,
                cKeys,
                cVals
            )

        /// <summary>
        /// Sink the LazyFrame to a CSV format in memory.
        /// <para>
        /// This allows for streaming execution directly into a byte array without writing to disk.
        /// </para>
        /// </summary>
        /// <returns>A byte array containing the serialized CSV data.</returns>
        member this.SinkCsvMemory
            (
                ?includeBom: bool,
                ?includeHeader: bool,
                ?batchSize: int,
                ?checkExtension: bool,
                ?compressionCode: ExternalCompression,
                ?compressionLevel: int,
                ?dateFormat: string,
                ?timeFormat: string,
                ?datetimeFormat: string,
                ?floatScientific: int,
                ?floatPrecision: int,
                ?decimalComma: bool,
                ?separator: byte,
                ?quoteChar: byte,
                ?nullValue: string,
                ?lineTerminator: string,
                ?quoteStyle: QuoteStyle,
                ?maintainOrder: bool
            ) =
            let incBom = defaultArg includeBom false
            let incHdr = defaultArg includeHeader true
            let bSize = defaultArg batchSize 1024
            let chkExt = defaultArg checkExtension false
            let comp = defaultArg compressionCode ExternalCompression.Uncompressed
            let compLvl = defaultArg compressionLevel 0
            let dFmt = defaultArg dateFormat null
            let tFmt = defaultArg timeFormat null
            let dtFmt = defaultArg datetimeFormat null
            let fSci = defaultArg floatScientific -1
            let fPrec = defaultArg floatPrecision -1
            let dComma = defaultArg decimalComma false
            let sep = defaultArg separator (byte ',')
            let qChar = defaultArg quoteChar (byte '"')
            let nVal = defaultArg nullValue null
            let lTerm = defaultArg lineTerminator "\n"
            let qStyle = defaultArg quoteStyle QuoteStyle.Necessary
            let mo = defaultArg maintainOrder true

            PolarsWrapper.SinkCsvMemory(
                this.CloneHandle(),
                incBom,
                incHdr,
                bSize,
                chkExt,
                comp.ToNative(),
                compLvl,
                dFmt,
                tFmt,
                dtFmt,
                fSci,
                fPrec,
                dComma,
                sep,
                qChar,
                nVal,
                lTerm,
                qStyle.ToNative(),
                mo
            )

        /// <summary>
        /// Sink the LazyFrame to a CSV format in native memory, without copying into the managed heap.
        /// <para>
        /// This allows for streaming execution directly into a native buffer without writing to disk.
        /// </para>
        /// </summary>
        /// <returns>A <see cref="T:Polars.NET.Core.NativeBuffer"/> owning the serialized CSV data. Dispose it to release the native memory.</returns>
        member this.SinkCsvNativeMemory
            (
                ?includeBom: bool,
                ?includeHeader: bool,
                ?batchSize: int,
                ?checkExtension: bool,
                ?compressionCode: ExternalCompression,
                ?compressionLevel: int,
                ?dateFormat: string,
                ?timeFormat: string,
                ?datetimeFormat: string,
                ?floatScientific: int,
                ?floatPrecision: int,
                ?decimalComma: bool,
                ?separator: byte,
                ?quoteChar: byte,
                ?nullValue: string,
                ?lineTerminator: string,
                ?quoteStyle: QuoteStyle,
                ?maintainOrder: bool
            ) : Polars.NET.Core.NativeBuffer =
            let incBom = defaultArg includeBom false
            let incHdr = defaultArg includeHeader true
            let bSize = defaultArg batchSize 1024
            let chkExt = defaultArg checkExtension false
            let comp = defaultArg compressionCode ExternalCompression.Uncompressed
            let compLvl = defaultArg compressionLevel 0
            let dFmt = defaultArg dateFormat null
            let tFmt = defaultArg timeFormat null
            let dtFmt = defaultArg datetimeFormat null
            let fSci = defaultArg floatScientific -1
            let fPrec = defaultArg floatPrecision -1
            let dComma = defaultArg decimalComma false
            let sep = defaultArg separator (byte ',')
            let qChar = defaultArg quoteChar (byte '"')
            let nVal = defaultArg nullValue null
            let lTerm = defaultArg lineTerminator "\n"
            let qStyle = defaultArg quoteStyle QuoteStyle.Necessary
            let mo = defaultArg maintainOrder true

            PolarsWrapper.SinkCsvNativeMemory(
                this.CloneHandle(),
                incBom,
                incHdr,
                bSize,
                chkExt,
                comp.ToNative(),
                compLvl,
                dFmt,
                tFmt,
                dtFmt,
                fSci,
                fPrec,
                dComma,
                sep,
                qChar,
                nVal,
                lTerm,
                qStyle.ToNative(),
                mo
            )

        /// <summary>
        /// Sink the LazyFrame to a CSV format written into a <see cref="T:System.IO.Stream"/>.
        /// Output is streamed in chunks as it is produced. The stream is flushed but not disposed.
        /// </summary>
        member this.SinkCsv
            (
                stream: Stream,
                ?includeBom: bool,
                ?includeHeader: bool,
                ?batchSize: int,
                ?checkExtension: bool,
                ?compressionCode: ExternalCompression,
                ?compressionLevel: int,
                ?dateFormat: string,
                ?timeFormat: string,
                ?datetimeFormat: string,
                ?floatScientific: int,
                ?floatPrecision: int,
                ?decimalComma: bool,
                ?separator: byte,
                ?quoteChar: byte,
                ?nullValue: string,
                ?lineTerminator: string,
                ?quoteStyle: QuoteStyle,
                ?maintainOrder: bool
            ) : unit =
            let incBom = defaultArg includeBom false
            let incHdr = defaultArg includeHeader true
            let bSize = defaultArg batchSize 1024
            let chkExt = defaultArg checkExtension false
            let comp = defaultArg compressionCode ExternalCompression.Uncompressed
            let compLvl = defaultArg compressionLevel 0
            let dFmt = defaultArg dateFormat null
            let tFmt = defaultArg timeFormat null
            let dtFmt = defaultArg datetimeFormat null
            let fSci = defaultArg floatScientific -1
            let fPrec = defaultArg floatPrecision -1
            let dComma = defaultArg decimalComma false
            let sep = defaultArg separator (byte ',')
            let qChar = defaultArg quoteChar (byte '"')
            let nVal = defaultArg nullValue null
            let lTerm = defaultArg lineTerminator "\n"
            let qStyle = defaultArg quoteStyle QuoteStyle.Necessary
            let mo = defaultArg maintainOrder true
            let handle = this.CloneHandle()
            NativeStreamWriter.Write(stream, fun target ->
                PolarsWrapper.SinkCsvCallback(handle, target, incBom, incHdr, bSize, chkExt, comp.ToNative(), compLvl,
                    dFmt, tFmt, dtFmt, fSci, fPrec, dComma, sep, qChar, nVal, lTerm, qStyle.ToNative(), mo))

        /// <summary>
        /// Asynchronously sink the LazyFrame to a CSV format written into a <see cref="T:System.IO.Stream"/>.
        /// Safe for destinations that disallow synchronous writes (e.g. ASP.NET Core response bodies).
        /// The stream is flushed but not disposed.
        /// </summary>
        member this.SinkCsvAsync
            (
                stream: Stream,
                ?includeBom: bool,
                ?includeHeader: bool,
                ?batchSize: int,
                ?checkExtension: bool,
                ?compressionCode: ExternalCompression,
                ?compressionLevel: int,
                ?dateFormat: string,
                ?timeFormat: string,
                ?datetimeFormat: string,
                ?floatScientific: int,
                ?floatPrecision: int,
                ?decimalComma: bool,
                ?separator: byte,
                ?quoteChar: byte,
                ?nullValue: string,
                ?lineTerminator: string,
                ?quoteStyle: QuoteStyle,
                ?maintainOrder: bool,
                ?cancellationToken: CancellationToken
            ) : Task =
            let incBom = defaultArg includeBom false
            let incHdr = defaultArg includeHeader true
            let bSize = defaultArg batchSize 1024
            let chkExt = defaultArg checkExtension false
            let comp = defaultArg compressionCode ExternalCompression.Uncompressed
            let compLvl = defaultArg compressionLevel 0
            let dFmt = defaultArg dateFormat null
            let tFmt = defaultArg timeFormat null
            let dtFmt = defaultArg datetimeFormat null
            let fSci = defaultArg floatScientific -1
            let fPrec = defaultArg floatPrecision -1
            let dComma = defaultArg decimalComma false
            let sep = defaultArg separator (byte ',')
            let qChar = defaultArg quoteChar (byte '"')
            let nVal = defaultArg nullValue null
            let lTerm = defaultArg lineTerminator "\n"
            let qStyle = defaultArg quoteStyle QuoteStyle.Necessary
            let mo = defaultArg maintainOrder true
            let ct = defaultArg cancellationToken CancellationToken.None
            let handle = this.CloneHandle()
            NativeStreamWriter.WriteAsync(stream,(fun target ->
                PolarsWrapper.SinkCsvCallback(handle, target, incBom, incHdr, bSize, chkExt, comp.ToNative(), compLvl,
                    dFmt, tFmt, dtFmt, fSci, fPrec, dComma, sep, qChar, nVal, lTerm, qStyle.ToNative(), mo)), ct)

        /// <summary>
        /// Asynchronously sink the LazyFrame to a CSV format written into a <see cref="T:System.IO.Pipelines.PipeWriter"/>.
        /// The writer is flushed but not completed.
        /// </summary>
        member this.SinkCsvAsync
            (
                writer: PipeWriter,
                ?includeBom: bool,
                ?includeHeader: bool,
                ?batchSize: int,
                ?checkExtension: bool,
                ?compressionCode: ExternalCompression,
                ?compressionLevel: int,
                ?dateFormat: string,
                ?timeFormat: string,
                ?datetimeFormat: string,
                ?floatScientific: int,
                ?floatPrecision: int,
                ?decimalComma: bool,
                ?separator: byte,
                ?quoteChar: byte,
                ?nullValue: string,
                ?lineTerminator: string,
                ?quoteStyle: QuoteStyle,
                ?maintainOrder: bool,
                ?cancellationToken: CancellationToken
            ) : Task =
            let incBom = defaultArg includeBom false
            let incHdr = defaultArg includeHeader true
            let bSize = defaultArg batchSize 1024
            let chkExt = defaultArg checkExtension false
            let comp = defaultArg compressionCode ExternalCompression.Uncompressed
            let compLvl = defaultArg compressionLevel 0
            let dFmt = defaultArg dateFormat null
            let tFmt = defaultArg timeFormat null
            let dtFmt = defaultArg datetimeFormat null
            let fSci = defaultArg floatScientific -1
            let fPrec = defaultArg floatPrecision -1
            let dComma = defaultArg decimalComma false
            let sep = defaultArg separator (byte ',')
            let qChar = defaultArg quoteChar (byte '"')
            let nVal = defaultArg nullValue null
            let lTerm = defaultArg lineTerminator "\n"
            let qStyle = defaultArg quoteStyle QuoteStyle.Necessary
            let mo = defaultArg maintainOrder true
            let ct = defaultArg cancellationToken CancellationToken.None
            let handle = this.CloneHandle()
            NativeStreamWriter.WriteAsync(writer,(fun target ->
                PolarsWrapper.SinkCsvCallback(handle, target, incBom, incHdr, bSize, chkExt, comp.ToNative(), compLvl,
                    dFmt, tFmt, dtFmt, fSci, fPrec, dComma, sep, qChar, nVal, lTerm, qStyle.ToNative(), mo)), ct)

    type DataFrame with
        /// <summary>
        /// Write DataFrame to a comma-separated values (CSV) file.
        /// <para>
        /// This uses the Lazy execution engine internally to support streaming and cloud storage.
        /// </para>
        /// </summary>
        member this.WriteCsv
            (
                path: string,
                ?includeHeader: bool,
                ?includeBom: bool,
                ?separator: char,
                ?quoteChar: char,
                ?quoteStyle: QuoteStyle,
                ?nullValue: string,
                ?lineTerminator: string,
                ?floatScientific: bool,
                ?floatPrecision: int,
                ?decimalComma: bool,
                ?dateFormat: string,
                ?timeFormat: string,
                ?datetimeFormat: string,
                ?checkExtension: bool,
                ?compression: ExternalCompression,
                ?compressionLevel: int,
                ?maintainOrder: bool,
                ?syncOnClose: SyncOnClose,
                ?mkdir: bool,
                ?batchSize: int,
                ?cloudOptions: CloudOptions
            ) =
            this
                .Lazy()
                .SinkCsv(
                    path,
                    ?includeHeader = includeHeader,
                    ?includeBom = includeBom,
                    ?separator = separator,
                    ?quoteChar = quoteChar,
                    ?quoteStyle = quoteStyle,
                    ?nullValue = nullValue,
                    ?lineTerminator = lineTerminator,
                    ?floatScientific = floatScientific,
                    ?floatPrecision = floatPrecision,
                    ?decimalComma = decimalComma,
                    ?dateFormat = dateFormat,
                    ?timeFormat = timeFormat,
                    ?datetimeFormat = datetimeFormat,
                    ?checkExtension = checkExtension,
                    ?compression = compression,
                    ?compressionLevel = compressionLevel,
                    ?maintainOrder = maintainOrder,
                    ?syncOnClose = syncOnClose,
                    ?mkdir = mkdir,
                    ?batchSize = batchSize,
                    ?cloudOptions = cloudOptions
                )

            this

        /// <summary>
        /// Write DataFrame to a partitioned comma-separated values (CSV) file.
        /// <para>
        /// This uses the Lazy execution engine internally to support streaming and cloud storage.
        /// </para>
        /// </summary>
        member this.WriteCsvPartitioned
            (
                path: string,
                partitionBy: Selector,
                ?includeKeys: bool,
                ?keysPreGrouped: bool,
                ?maxRowsPerFile: int,
                ?approxBytesPerFile: int64,
                ?includeHeader: bool,
                ?includeBom: bool,
                ?separator: char,
                ?quoteChar: char,
                ?quoteStyle: QuoteStyle,
                ?nullValue: string,
                ?lineTerminator: string,
                ?floatScientific: bool,
                ?floatPrecision: int,
                ?decimalComma: bool,
                ?dateFormat: string,
                ?timeFormat: string,
                ?datetimeFormat: string,
                ?checkExtension: bool,
                ?compression: ExternalCompression,
                ?compressionLevel: int,
                ?maintainOrder: bool,
                ?syncOnClose: SyncOnClose,
                ?mkdir: bool,
                ?batchSize: int,
                ?cloudOptions: CloudOptions
            ) =
            this
                .Lazy()
                .SinkCsvPartitioned(
                    path,
                    partitionBy,
                    ?includeKeys = includeKeys,
                    ?keysPreGrouped = keysPreGrouped,
                    ?maxRowsPerFile = maxRowsPerFile,
                    ?approxBytesPerFile = approxBytesPerFile,
                    ?includeHeader = includeHeader,
                    ?includeBom = includeBom,
                    ?separator = separator,
                    ?quoteChar = quoteChar,
                    ?quoteStyle = quoteStyle,
                    ?nullValue = nullValue,
                    ?lineTerminator = lineTerminator,
                    ?floatScientific = floatScientific,
                    ?floatPrecision = floatPrecision,
                    ?decimalComma = decimalComma,
                    ?dateFormat = dateFormat,
                    ?timeFormat = timeFormat,
                    ?datetimeFormat = datetimeFormat,
                    ?checkExtension = checkExtension,
                    ?compression = compression,
                    ?compressionLevel = compressionLevel,
                    ?maintainOrder = maintainOrder,
                    ?syncOnClose = syncOnClose,
                    ?mkdir = mkdir,
                    ?batchSize = batchSize,
                    ?cloudOptions = cloudOptions
                )

            this

        /// <summary>
        /// Write DataFrame to a CSV format in memory.
        /// </summary>
        /// <returns>A byte array containing the serialized CSV data.</returns>
        member this.WriteCsvMemory
            (
                ?includeBom: bool,
                ?includeHeader: bool,
                ?batchSize: int,
                ?checkExtension: bool,
                ?compressionCode: ExternalCompression,
                ?compressionLevel: int,
                ?dateFormat: string,
                ?timeFormat: string,
                ?datetimeFormat: string,
                ?floatScientific: int,
                ?floatPrecision: int,
                ?decimalComma: bool,
                ?separator: byte,
                ?quoteChar: byte,
                ?nullValue: string,
                ?lineTerminator: string,
                ?quoteStyle: QuoteStyle,
                ?maintainOrder: bool
            ) : byte[] =
            this
                .Lazy()
                .SinkCsvMemory(
                    ?includeBom = includeBom,
                    ?includeHeader = includeHeader,
                    ?batchSize = batchSize,
                    ?checkExtension = checkExtension,
                    ?compressionCode = compressionCode,
                    ?compressionLevel = compressionLevel,
                    ?dateFormat = dateFormat,
                    ?timeFormat = timeFormat,
                    ?datetimeFormat = datetimeFormat,
                    ?floatScientific = floatScientific,
                    ?floatPrecision = floatPrecision,
                    ?decimalComma = decimalComma,
                    ?separator = separator,
                    ?quoteChar = quoteChar,
                    ?nullValue = nullValue,
                    ?lineTerminator = lineTerminator,
                    ?quoteStyle = quoteStyle,
                    ?maintainOrder = maintainOrder
                )

        /// <summary>
        /// Write DataFrame to a CSV format in native memory, without copying into the managed heap.
        /// </summary>
        /// <returns>A <see cref="T:Polars.NET.Core.NativeBuffer"/> owning the serialized CSV data. Dispose it to release the native memory.</returns>
        member this.WriteCsvNativeMemory
            (
                ?includeBom: bool,
                ?includeHeader: bool,
                ?batchSize: int,
                ?checkExtension: bool,
                ?compressionCode: ExternalCompression,
                ?compressionLevel: int,
                ?dateFormat: string,
                ?timeFormat: string,
                ?datetimeFormat: string,
                ?floatScientific: int,
                ?floatPrecision: int,
                ?decimalComma: bool,
                ?separator: byte,
                ?quoteChar: byte,
                ?nullValue: string,
                ?lineTerminator: string,
                ?quoteStyle: QuoteStyle,
                ?maintainOrder: bool
            ) : Polars.NET.Core.NativeBuffer =
            this
                .Lazy()
                .SinkCsvNativeMemory(
                    ?includeBom = includeBom,
                    ?includeHeader = includeHeader,
                    ?batchSize = batchSize,
                    ?checkExtension = checkExtension,
                    ?compressionCode = compressionCode,
                    ?compressionLevel = compressionLevel,
                    ?dateFormat = dateFormat,
                    ?timeFormat = timeFormat,
                    ?datetimeFormat = datetimeFormat,
                    ?floatScientific = floatScientific,
                    ?floatPrecision = floatPrecision,
                    ?decimalComma = decimalComma,
                    ?separator = separator,
                    ?quoteChar = quoteChar,
                    ?nullValue = nullValue,
                    ?lineTerminator = lineTerminator,
                    ?quoteStyle = quoteStyle,
                    ?maintainOrder = maintainOrder
                )

        /// <summary>
        /// Write DataFrame to a comma-separated values (CSV) format written into a <see cref="T:System.IO.Stream"/>.
        /// The stream is flushed but not disposed.
        /// </summary>
        /// <returns>The same DataFrame, for chaining.</returns>
        member this.WriteCsv(
            stream: Stream,
            ?includeBom: bool,
            ?includeHeader: bool,
            ?batchSize: int,
            ?checkExtension: bool,
            ?compressionCode: ExternalCompression,
            ?compressionLevel: int,
            ?dateFormat: string,
            ?timeFormat: string,
            ?datetimeFormat: string,
            ?floatScientific: int,
            ?floatPrecision: int,
            ?decimalComma: bool,
            ?separator: byte,
            ?quoteChar: byte,
            ?nullValue: string,
            ?lineTerminator: string,
            ?quoteStyle: QuoteStyle,
            ?maintainOrder: bool
        ) : DataFrame =
            this.Lazy().SinkCsv(
                stream,
                ?includeBom = includeBom,
                ?includeHeader = includeHeader,
                ?batchSize = batchSize,
                ?checkExtension = checkExtension,
                ?compressionCode = compressionCode,
                ?compressionLevel = compressionLevel,
                ?dateFormat = dateFormat,
                ?timeFormat = timeFormat,
                ?datetimeFormat = datetimeFormat,
                ?floatScientific = floatScientific,
                ?floatPrecision = floatPrecision,
                ?decimalComma = decimalComma,
                ?separator = separator,
                ?quoteChar = quoteChar,
                ?nullValue = nullValue,
                ?lineTerminator = lineTerminator,
                ?quoteStyle = quoteStyle,
                ?maintainOrder = maintainOrder
            )
            this

        /// <summary>
        /// Asynchronously write DataFrame to a comma-separated values (CSV) format written into a <see cref="T:System.IO.Stream"/>.
        /// The stream is flushed but not disposed.
        /// </summary>
        member this.WriteCsvAsync(
            stream: Stream,
            ?includeBom: bool,
            ?includeHeader: bool,
            ?batchSize: int,
            ?checkExtension: bool,
            ?compressionCode: ExternalCompression,
            ?compressionLevel: int,
            ?dateFormat: string,
            ?timeFormat: string,
            ?datetimeFormat: string,
            ?floatScientific: int,
            ?floatPrecision: int,
            ?decimalComma: bool,
            ?separator: byte,
            ?quoteChar: byte,
            ?nullValue: string,
            ?lineTerminator: string,
            ?quoteStyle: QuoteStyle,
            ?maintainOrder: bool,
            ?cancellationToken: CancellationToken
        ) : Task =
            this.Lazy().SinkCsvAsync(
                stream,
                ?includeBom = includeBom,
                ?includeHeader = includeHeader,
                ?batchSize = batchSize,
                ?checkExtension = checkExtension,
                ?compressionCode = compressionCode,
                ?compressionLevel = compressionLevel,
                ?dateFormat = dateFormat,
                ?timeFormat = timeFormat,
                ?datetimeFormat = datetimeFormat,
                ?floatScientific = floatScientific,
                ?floatPrecision = floatPrecision,
                ?decimalComma = decimalComma,
                ?separator = separator,
                ?quoteChar = quoteChar,
                ?nullValue = nullValue,
                ?lineTerminator = lineTerminator,
                ?quoteStyle = quoteStyle,
                ?maintainOrder = maintainOrder,
                ?cancellationToken = cancellationToken
            )

        /// <summary>
        /// Asynchronously write DataFrame to a comma-separated values (CSV) format written into a <see cref="T:System.IO.Pipelines.PipeWriter"/>.
        /// The writer is flushed but not completed.
        /// </summary>
        member this.WriteCsvAsync(
            writer: PipeWriter,
            ?includeBom: bool,
            ?includeHeader: bool,
            ?batchSize: int,
            ?checkExtension: bool,
            ?compressionCode: ExternalCompression,
            ?compressionLevel: int,
            ?dateFormat: string,
            ?timeFormat: string,
            ?datetimeFormat: string,
            ?floatScientific: int,
            ?floatPrecision: int,
            ?decimalComma: bool,
            ?separator: byte,
            ?quoteChar: byte,
            ?nullValue: string,
            ?lineTerminator: string,
            ?quoteStyle: QuoteStyle,
            ?maintainOrder: bool,
            ?cancellationToken: CancellationToken
        ) : Task =
            this.Lazy().SinkCsvAsync(
                writer,
                ?includeBom = includeBom,
                ?includeHeader = includeHeader,
                ?batchSize = batchSize,
                ?checkExtension = checkExtension,
                ?compressionCode = compressionCode,
                ?compressionLevel = compressionLevel,
                ?dateFormat = dateFormat,
                ?timeFormat = timeFormat,
                ?datetimeFormat = datetimeFormat,
                ?floatScientific = floatScientific,
                ?floatPrecision = floatPrecision,
                ?decimalComma = decimalComma,
                ?separator = separator,
                ?quoteChar = quoteChar,
                ?nullValue = nullValue,
                ?lineTerminator = lineTerminator,
                ?quoteStyle = quoteStyle,
                ?maintainOrder = maintainOrder,
                ?cancellationToken = cancellationToken
            )
