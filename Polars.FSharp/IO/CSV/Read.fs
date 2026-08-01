namespace Polars.FSharp

open Polars.NET.Core

[<AutoOpen>]
module CsvRead =
    open System.IO
    type LazyFrame with
        /// <summary>
        /// Lazily scans a CSV file into a LazyFrame.
        /// <para>
        /// This allows for query optimization (predicate pushdown, projection pushdown) 
        /// and streaming processing of datasets larger than memory.
        /// </para>
        /// </summary>
        static member ScanCsv(
            path: string,
            ?schema: PolarsSchema,
            ?dtypeOverride:PolarsSchema,
            ?hasHeader: bool,
            ?separator: char,
            ?quoteChar: char,
            ?eolChar: char,
            ?ignoreErrors: bool,
            ?tryParseDates: bool,
            ?lowMemory: bool,
            ?cache: bool,
            ?glob: bool,
            ?rechunk: bool,
            ?raiseIfEmpty: bool,
            ?skipRows: uint64,
            ?skipRowsAfterHeader: uint64,
            ?skipLines: uint64,
            ?nRows: uint64,
            ?inferSchemaLength: uint64,
            ?nThreads: uint64,
            ?chunkSize: uint64,
            ?rowIndexName: string,
            ?rowIndexOffset: uint64,
            ?includeFilePaths: string,
            ?encoding: CsvEncoding,
            ?nullValues: seq<string>,
            ?missingIsNull: bool,
            ?commentPrefix: string,
            ?decimalComma: bool,
            ?truncateRaggedLines: bool,
            ?cloudOptions: CloudOptions
        ) : LazyFrame =
            
            let schemaHandle = match schema with Some s -> s.Handle | None -> null
            let dtypeOverrideHandle = match dtypeOverride with Some s -> s.Handle | None -> null
            let pHasHdr = defaultArg hasHeader true
            let pSep = defaultArg separator ','
            let pQuote = match quoteChar with Some c -> System.Nullable c | None -> System.Nullable '"'
            let pEol = defaultArg eolChar '\n'
            let pIgnoreErr = defaultArg ignoreErrors false
            let pTryDates = defaultArg tryParseDates true
            let pLowMem = defaultArg lowMemory false
            let pCache = defaultArg cache true
            let pGlob = defaultArg glob true
            let pRechunk = defaultArg rechunk false
            let pRaiseEmpty = defaultArg raiseIfEmpty true
            let pSkipR = defaultArg skipRows 0UL
            let pSkipRAH = defaultArg skipRowsAfterHeader 0UL
            let pSkipL = defaultArg skipLines 0UL
            let pNRows = Option.toNullable nRows
            let pInferLen = inferSchemaLength |> Option.toNullable
            let pNTh = Option.toNullable nThreads
            let pChunkSz = Option.toNullable chunkSize
            let pRowIdxName = Option.toObj rowIndexName
            let pRowIdxOff = defaultArg rowIndexOffset 0UL
            let pIncPaths = Option.toObj includeFilePaths
            let pEnc = defaultArg encoding CsvEncoding.UTF8
            let pNullVals = nullValues |> Option.map Seq.toArray |> Option.toObj
            let pMissNull = defaultArg missingIsNull true
            let pComment = Option.toObj commentPrefix
            let pDecComma = defaultArg decimalComma false
            let pTruncRagged = defaultArg truncateRaggedLines false

            let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals =
                CloudOptions.ParseCloudOptions cloudOptions

            let h = PolarsWrapper.ScanCsv(
                path,
                schemaHandle,
                dtypeOverrideHandle,
                pHasHdr,
                pSep,
                pQuote,
                pEol,
                pIgnoreErr,
                pTryDates,
                pLowMem,
                pCache,
                pGlob,
                pRechunk,
                pRaiseEmpty,
                pSkipR,
                pSkipRAH,
                pSkipL,
                pNRows,
                pInferLen,
                pNTh,
                pChunkSz,
                pRowIdxName,
                pRowIdxOff,
                pIncPaths,
                pEnc.ToNative(),
                pNullVals,
                pMissNull,
                pComment,
                pDecComma,
                pTruncRagged,
                cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals
            )
            new LazyFrame(h)

        // ---------------------------------------------------------
        // Scan CSV (Memory / Bytes)
        // ---------------------------------------------------------

        /// <summary>
        /// Lazily scans a CSV from an in-memory byte array.
        /// </summary>
        static member ScanCsv(
            buffer: byte[],
            ?schema: PolarsSchema,
            ?dtypeOverride:PolarsSchema,
            ?hasHeader: bool,
            ?separator: char,
            ?quoteChar: char,
            ?eolChar: char,
            ?ignoreErrors: bool,
            ?tryParseDates: bool,
            ?lowMemory: bool,
            ?cache: bool,
            ?glob: bool,
            ?rechunk: bool,
            ?raiseIfEmpty: bool,
            ?skipRows: uint64,
            ?skipRowsAfterHeader: uint64,
            ?skipLines: uint64,
            ?nRows: uint64,
            ?inferSchemaLength: uint64,
            ?nThreads: uint64,
            ?chunkSize: uint64,
            ?rowIndexName: string,
            ?rowIndexOffset: uint64,
            ?includeFilePaths: string,
            ?encoding: CsvEncoding,
            ?nullValues: seq<string>,
            ?missingIsNull: bool,
            ?commentPrefix: string,
            ?decimalComma: bool,
            ?truncateRaggedLines: bool
        ) : LazyFrame =
            
            let schemaHandle = match schema with Some s -> s.Handle | None -> null
            let dtypeOverrideHandle = match dtypeOverride with Some s -> s.Handle | None -> null
            let pHasHdr = defaultArg hasHeader true
            let pSep = defaultArg separator ','
            let pQuote = match quoteChar with Some c -> System.Nullable c | None -> System.Nullable '"'
            let pEol = defaultArg eolChar '\n'
            let pIgnoreErr = defaultArg ignoreErrors false
            let pTryDates = defaultArg tryParseDates true
            let pLowMem = defaultArg lowMemory false
            let pCache = defaultArg cache true
            let pGlob = defaultArg glob true
            let pRechunk = defaultArg rechunk false
            let pRaiseEmpty = defaultArg raiseIfEmpty true
            let pSkipR = defaultArg skipRows 0UL
            let pSkipRAH = defaultArg skipRowsAfterHeader 0UL
            let pSkipL = defaultArg skipLines 0UL
            let pNRows = Option.toNullable nRows
            let pInferLen = inferSchemaLength |> Option.toNullable
            let pNTh = Option.toNullable nThreads
            let pChunkSz = Option.toNullable chunkSize
            let pRowIdxName = Option.toObj rowIndexName
            let pRowIdxOff = defaultArg rowIndexOffset 0UL
            let pIncPaths = Option.toObj includeFilePaths
            let pEnc = defaultArg encoding CsvEncoding.UTF8
            let pNullVals = nullValues |> Option.map Seq.toArray |> Option.toObj
            let pMissNull = defaultArg missingIsNull true
            let pComment = Option.toObj commentPrefix
            let pDecComma = defaultArg decimalComma false
            let pTruncRagged = defaultArg truncateRaggedLines false

            let h = PolarsWrapper.ScanCsv(
                buffer,
                schemaHandle,
                dtypeOverrideHandle,
                pHasHdr,
                pSep,
                pQuote,
                pEol,
                pIgnoreErr,
                pTryDates,
                pLowMem,
                pCache,
                pGlob,
                pRechunk,
                pRaiseEmpty,
                pSkipR,
                pSkipRAH,
                pSkipL,
                pNRows,
                pInferLen,
                pNTh,
                pChunkSz,
                pRowIdxName,
                pRowIdxOff,
                pIncPaths,
                pEnc.ToNative(),
                pNullVals,
                pMissNull,
                pComment,
                pDecComma,
                pTruncRagged
            )
            new LazyFrame(h)

        // ---------------------------------------------------------
        // Scan CSV (Stream)
        // ---------------------------------------------------------

        /// <summary>
        /// Lazily scans a CSV from a Stream.
        /// <para>
        /// This reads the stream fully into memory to construct the Lazy execution plan.
        /// </para>
        /// </summary>
        static member ScanCsv(
            stream: System.IO.Stream,
            ?schema: PolarsSchema,
            ?hasHeader: bool,
            ?separator: char,
            ?quoteChar: char,
            ?eolChar: char,
            ?ignoreErrors: bool,
            ?tryParseDates: bool,
            ?lowMemory: bool,
            ?cache: bool,
            ?glob: bool,
            ?rechunk: bool,
            ?raiseIfEmpty: bool,
            ?skipRows: uint64,
            ?skipRowsAfterHeader: uint64,
            ?skipLines: uint64,
            ?nRows: uint64,
            ?inferSchemaLength: uint64,
            ?nThreads: uint64,
            ?chunkSize: uint64,
            ?rowIndexName: string,
            ?rowIndexOffset: uint64,
            ?includeFilePaths: string,
            ?encoding: CsvEncoding,
            ?nullValues: seq<string>,
            ?missingIsNull: bool,
            ?commentPrefix: string,
            ?decimalComma: bool,
            ?truncateRaggedLines: bool
        ) : LazyFrame =
            
            use ms = new System.IO.MemoryStream()
            stream.CopyTo(ms)
            let bytes = ms.ToArray()

            LazyFrame.ScanCsv(
                bytes,
                ?schema = schema,
                ?hasHeader = hasHeader,
                ?separator = separator,
                ?quoteChar = quoteChar,
                ?eolChar = eolChar,
                ?ignoreErrors = ignoreErrors,
                ?tryParseDates = tryParseDates,
                ?lowMemory = lowMemory,
                ?cache = cache,
                ?glob = glob,
                ?rechunk = rechunk,
                ?raiseIfEmpty = raiseIfEmpty,
                ?skipRows = skipRows,
                ?skipRowsAfterHeader = skipRowsAfterHeader,
                ?skipLines = skipLines,
                ?nRows = nRows,
                ?inferSchemaLength = inferSchemaLength,
                ?nThreads = nThreads,
                ?chunkSize = chunkSize,
                ?rowIndexName = rowIndexName,
                ?rowIndexOffset = rowIndexOffset,
                ?includeFilePaths = includeFilePaths,
                ?encoding = encoding,
                ?nullValues = nullValues,
                ?missingIsNull = missingIsNull,
                ?commentPrefix = commentPrefix,
                ?decimalComma = decimalComma,
                ?truncateRaggedLines = truncateRaggedLines
            )    /// <summary> Helper: Scan CSV with default settings </summary>
        static member ScanCsv(path: string) = 
            LazyFrame.ScanCsv(path, hasHeader=true)
    type DataFrame with
        /// <summary>
        /// Read a DataFrame from a CSV file.
        /// <para>
        /// Note: This method internally uses LazyFrame.ScanCsv and collects the result. 
        /// For larger-than-memory datasets or better query optimization, consider using LazyFrame.ScanCsv directly.
        /// </para>
        /// </summary>
        static member ReadCsv(
            path: string,
            ?columns: seq<string>,
            ?hasHeader: bool,
            ?separator: char,
            ?quoteChar: char,
            ?eolChar: char,
            ?ignoreErrors: bool,
            ?tryParseDates: bool,
            ?lowMemory: bool,
            ?skipRows: uint64,
            ?nRows: uint64,
            ?inferSchemaLength: uint64,
            ?schema: PolarsSchema,
            ?dtypeOverride:PolarsSchema,
            ?encoding: CsvEncoding,
            ?nullValues: seq<string>,
            ?missingIsNull: bool,
            ?commentPrefix: string,
            ?decimalComma: bool,
            ?truncateRaggedLines: bool,
            ?rowIndexName: string,
            ?rowIndexOffset: uint64,
            ?cloudOptions: CloudOptions
        ) : DataFrame =
            
            let mutable lf = LazyFrame.ScanCsv(
                path,
                ?schema = schema,
                ?dtypeOverride = dtypeOverride,
                ?hasHeader = hasHeader,
                ?separator = separator,
                ?quoteChar = quoteChar,
                ?eolChar = eolChar,
                ?ignoreErrors = ignoreErrors,
                ?tryParseDates = tryParseDates,
                ?lowMemory = lowMemory,
                ?skipRows = skipRows,
                ?nRows = nRows,
                ?inferSchemaLength = inferSchemaLength,
                ?rowIndexName = rowIndexName,
                ?rowIndexOffset = rowIndexOffset,
                ?encoding = encoding,
                ?nullValues = nullValues,
                ?missingIsNull = missingIsNull,
                ?commentPrefix = commentPrefix,
                ?decimalComma = decimalComma,
                ?truncateRaggedLines = truncateRaggedLines,
                ?cloudOptions = cloudOptions
            )

            match columns with
            | Some cols -> 
                let colArray = cols |> Seq.toArray
                if colArray.Length > 0 then
                    use sel = new Selector(PolarsWrapper.SelectorCols colArray)
                    lf <- lf.Select [sel :> IColumnExpr]
            | None -> ()

            lf.Collect()

        // ---------------------------------------------------------
        // Read CSV (Memory / Bytes)
        // ---------------------------------------------------------

        /// <summary>
        /// Read a DataFrame from a CSV memory buffer.
        /// </summary>
        static member ReadCsv(
            buffer: byte[],
            ?columns: seq<string>,
            ?hasHeader: bool,
            ?separator: char,
            ?quoteChar: char,
            ?eolChar: char,
            ?ignoreErrors: bool,
            ?tryParseDates: bool,
            ?lowMemory: bool,
            ?skipRows: uint64,
            ?nRows: uint64,
            ?inferSchemaLength: uint64,
            ?schema: PolarsSchema,
            ?dtypeOverride:PolarsSchema,
            ?encoding: CsvEncoding,
            ?nullValues: seq<string>,
            ?missingIsNull: bool,
            ?commentPrefix: string,
            ?decimalComma: bool,
            ?truncateRaggedLines: bool,
            ?rowIndexName: string,
            ?rowIndexOffset: uint64
        ) : DataFrame =
            
            let mutable lf = LazyFrame.ScanCsv(
                buffer,
                ?schema = schema,
                ?dtypeOverride = dtypeOverride,
                ?hasHeader = hasHeader,
                ?separator = separator,
                ?quoteChar = quoteChar,
                ?eolChar = eolChar,
                ?ignoreErrors = ignoreErrors,
                ?tryParseDates = tryParseDates,
                ?lowMemory = lowMemory,
                ?skipRows = skipRows,
                ?nRows = nRows,
                ?inferSchemaLength = inferSchemaLength,
                ?rowIndexName = rowIndexName,
                ?rowIndexOffset = rowIndexOffset,
                ?encoding = encoding,
                ?nullValues = nullValues ,
                ?missingIsNull = missingIsNull,
                ?commentPrefix = commentPrefix,
                ?decimalComma = decimalComma,
                ?truncateRaggedLines = truncateRaggedLines
            )

            match columns with
            | Some cols -> 
                let colArray = cols |> Seq.toArray
                if colArray.Length > 0 then
                    use sel = new Selector(PolarsWrapper.SelectorCols colArray)
                    lf <- lf.Select [sel :> IColumnExpr]
            | None -> ()

            lf.Collect()

        // ---------------------------------------------------------
        // Read CSV (Stream)
        // ---------------------------------------------------------

        /// <summary>
        /// Read a DataFrame from a CSV memory stream.
        /// </summary>
        static member ReadCsv(
            stream: System.IO.Stream,
            ?columns: seq<string>,
            ?hasHeader: bool,
            ?separator: char,
            ?quoteChar: char,
            ?eolChar: char,
            ?ignoreErrors: bool,
            ?tryParseDates: bool,
            ?lowMemory: bool,
            ?skipRows: uint64,
            ?nRows: uint64,
            ?inferSchemaLength: uint64,
            ?schema: PolarsSchema,
            ?encoding: CsvEncoding,
            ?nullValues: seq<string>,
            ?missingIsNull: bool,
            ?commentPrefix: string,
            ?decimalComma: bool,
            ?truncateRaggedLines: bool,
            ?rowIndexName: string,
            ?rowIndexOffset: uint64
        ) : DataFrame =
            
            use ms = new MemoryStream()
            stream.CopyTo ms
            let bytes = ms.ToArray()

            DataFrame.ReadCsv(
                bytes,
                ?columns = columns,
                ?hasHeader = hasHeader,
                ?separator = separator,
                ?quoteChar = quoteChar,
                ?eolChar = eolChar,
                ?ignoreErrors = ignoreErrors,
                ?tryParseDates = tryParseDates,
                ?lowMemory = lowMemory,
                ?skipRows = skipRows,
                ?nRows = nRows,
                ?inferSchemaLength = inferSchemaLength,
                ?schema = schema,
                ?encoding = encoding,
                ?nullValues = nullValues,
                ?missingIsNull = missingIsNull,
                ?commentPrefix = commentPrefix,
                ?decimalComma = decimalComma,
                ?truncateRaggedLines = truncateRaggedLines,
                ?rowIndexName = rowIndexName,
                ?rowIndexOffset = rowIndexOffset
            )
        /// <summary>
        /// Read a CSV file asynchronously into a DataFrame.
        /// </summary>
        static member ReadCsvAsync
            (
                path: string,
                ?columns: seq<string>,
                ?hasHeader: bool,
                ?separator: char,
                ?quoteChar: char,
                ?eolChar: char,
                ?ignoreErrors: bool,
                ?tryParseDates: bool,
                ?lowMemory: bool,
                ?skipRows: uint64,
                ?nRows: uint64,
                ?inferSchemaLength: uint64,
                ?schema: PolarsSchema,
                ?encoding: CsvEncoding,
                ?nullValues: seq<string>,
                ?missingIsNull: bool,
                ?commentPrefix: string,
                ?decimalComma: bool,
                ?truncateRaggedLines: bool,
                ?rowIndexName: string,
                ?rowIndexOffset: uint64,
                ?cloudOptions: CloudOptions
            ) =
            task {
                return DataFrame.ReadCsv(
                    path,
                    ?columns = columns,
                    ?schema = schema,
                    ?hasHeader = hasHeader,
                    ?separator = separator,
                    ?quoteChar = quoteChar,
                    ?eolChar = eolChar,
                    ?ignoreErrors = ignoreErrors,
                    ?tryParseDates = tryParseDates,
                    ?lowMemory = lowMemory,
                    ?skipRows = skipRows,
                    ?nRows = nRows,
                    ?inferSchemaLength = inferSchemaLength,
                    ?encoding = encoding,
                    ?nullValues = nullValues,
                    ?missingIsNull = missingIsNull,
                    ?commentPrefix = commentPrefix,
                    ?decimalComma = decimalComma,
                    ?truncateRaggedLines = truncateRaggedLines,
                    ?rowIndexName = rowIndexName,
                    ?rowIndexOffset = rowIndexOffset,
                    ?cloudOptions = cloudOptions
                )
            }
